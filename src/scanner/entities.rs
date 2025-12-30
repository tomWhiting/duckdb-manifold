//! Entity scanner for ManifoldDB
//!
//! Implements a table function that scans all entities from a ManifoldDB database.
//!
//! ## Usage
//! ```sql
//! SELECT * FROM manifold_entities('/path/to/database.redb');
//! SELECT id, labels, prop_name FROM manifold_entities('/path/to/database.redb')
//!     WHERE prop_age > 25;
//! ```
//!
//! ## Schema Discovery
//! The schema is discovered dynamically by sampling entities at bind time.
//! Columns are generated for:
//! - `id` - Entity ID (VARCHAR)
//! - `labels` - JSON array of labels (VARCHAR)
//! - `prop_*` - Each discovered property gets a prefixed column
//!
//! ## Scanning Strategy
//!
//! This scanner uses cursor-based streaming to efficiently scan entities:
//! - The storage engine is cached globally (opened once per path, reused)
//! - No upfront ID collection - entities are scanned directly via cursor
//! - Each batch continues from the last key seen, avoiding redundant work

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    collections::HashMap,
    error::Error,
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use manifoldb_core::encoding::Decoder;
use manifoldb_core::types::{Entity, Value};
use manifoldb_storage::backends::RedbEngine;
use manifoldb_storage::{Cursor, StorageEngine, Transaction};

use crate::schema::{DiscoveredColumn, SchemaDiscovery};
use super::{get_cached_engine, BATCH_SIZE, SCHEMA_SAMPLE_SIZE};

/// Bind data for entity scanner - holds schema and database path
#[repr(C)]
pub struct ManifoldEntitiesBindData {
    /// Path to the ManifoldDB database
    pub db_path: String,
    /// Discovered schema columns
    pub columns: Vec<DiscoveredColumn>,
    /// Map from column name to index for fast lookup
    pub column_index: HashMap<String, usize>,
}

/// Init data for entity scanner - holds scan state
#[repr(C)]
pub struct ManifoldEntitiesInitData {
    /// Flag indicating scan is complete
    pub done: AtomicBool,
    /// Last key seen - used as continuation marker for cursor-based scanning
    /// None means we haven't started yet, Some(key) means continue after this key
    pub last_key: Mutex<Option<Vec<u8>>>,
}

/// Entity scanner VTab implementation
pub struct ManifoldEntitiesVTab;

impl VTab for ManifoldEntitiesVTab {
    type InitData = ManifoldEntitiesInitData;
    type BindData = ManifoldEntitiesBindData;

    /// Bind phase: discover schema, set up columns
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        // Get database path from first parameter
        let db_path = bind.get_parameter(0).to_string();

        // Get cached engine (opens once, reused)
        let engine = get_cached_engine(&db_path)?;

        // Discover schema using the engine
        let (columns, column_index) = discover_entity_schema(&engine)?;

        // Register discovered columns with DuckDB
        for col in &columns {
            bind.add_result_column(&col.name, col.to_logical_type_handle());
        }

        Ok(ManifoldEntitiesBindData {
            db_path,
            columns,
            column_index,
        })
    }

    /// Init phase: prepare for scanning (no data loading - we use cursor streaming)
    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        // No upfront data collection - we'll scan directly via cursor in func()
        Ok(ManifoldEntitiesInitData {
            done: AtomicBool::new(false),
            last_key: Mutex::new(None),
        })
    }

    /// Func phase: produce output batches using cursor-based streaming
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        // Wrap in catch_unwind to prevent panics from crossing FFI boundary
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::func_inner(func, output)
        }));

        match result {
            Ok(r) => r,
            Err(_) => Err("Internal panic in manifold_entities".into()),
        }
    }

    /// Define input parameters
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // db_path
        ])
    }
}

impl ManifoldEntitiesVTab {
    fn func_inner(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        // Check if we're done
        if init_data.done.load(Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        // Get the cached engine
        let engine = get_cached_engine(&bind_data.db_path)?;

        // Get the continuation key
        let start_after_key = init_data.last_key.lock().unwrap().clone();

        // Scan the next batch using cursor-based streaming
        let (entities, next_key) = scan_entity_batch(&engine, start_after_key.as_deref(), BATCH_SIZE)?;

        if entities.is_empty() {
            // No more entities - we're done
            init_data.done.store(true, Ordering::Relaxed);
            output.set_len(0);
            return Ok(());
        }

        let batch_size = entities.len();

        // Update the continuation marker for the next batch
        *init_data.last_key.lock().unwrap() = next_key;

        // Populate the output with entity data
        populate_entity_output(&entities, &bind_data.column_index, output)?;

        output.set_len(batch_size);

        Ok(())
    }
}

/// Discover entity schema by sampling the database
fn discover_entity_schema(
    engine: &Arc<RedbEngine>,
) -> Result<(Vec<DiscoveredColumn>, HashMap<String, usize>), Box<dyn Error>> {
    let tx = engine.begin_read()?;

    // Sample entities to discover schema
    let mut discovery = SchemaDiscovery::new();

    // Try to get a cursor on the nodes table
    match tx.cursor("nodes") {
        Ok(mut cursor) => {
            // Iterate through first SCHEMA_SAMPLE_SIZE entities
            if let Some((_key, value)) = cursor.seek_first()? {
                if let Ok(entity) = Entity::decode(&value) {
                    discovery.observe_entity(&entity.properties);
                }

                let mut count = 1;
                while count < SCHEMA_SAMPLE_SIZE {
                    match cursor.next()? {
                        Some((_key, value)) => {
                            if let Ok(entity) = Entity::decode(&value) {
                                discovery.observe_entity(&entity.properties);
                            }
                            count += 1;
                        }
                        None => break,
                    }
                }
            }
        }
        Err(_) => {
            // Table doesn't exist yet - return base schema
        }
    }

    // Finalize schema
    let columns = discovery.finalize();

    let mut column_index = HashMap::new();
    for (i, col) in columns.iter().enumerate() {
        column_index.insert(col.name.clone(), i);
    }

    Ok((columns, column_index))
}

/// Scan a batch of entities using cursor-based streaming
///
/// Returns (entities, next_key) where next_key is the continuation marker
/// for the next batch (the last key we read)
fn scan_entity_batch(
    engine: &Arc<RedbEngine>,
    start_after_key: Option<&[u8]>,
    batch_size: usize,
) -> Result<(Vec<Entity>, Option<Vec<u8>>), Box<dyn Error>> {
    let tx = engine.begin_read()?;
    let mut entities = Vec::with_capacity(batch_size);
    let mut last_key: Option<Vec<u8>> = None;

    match tx.cursor("nodes") {
        Ok(mut cursor) => {
            // Position cursor at the starting point
            let first_entry = if let Some(after_key) = start_after_key {
                // Seek to the key after our continuation marker
                cursor.seek(after_key)?;
                // Skip the key we already processed
                cursor.next()?
            } else {
                // Start from the beginning
                cursor.seek_first()?
            };

            // Process first entry if we have one
            let Some((key, value)) = first_entry else {
                // No more entries - return empty
                return Ok((entities, last_key));
            };

            if let Ok(entity) = Entity::decode(&value) {
                last_key = Some(key.clone());
                entities.push(entity);
            }

            // Continue reading until we have a full batch
            while entities.len() < batch_size {
                match cursor.next()? {
                    Some((key, value)) => {
                        if let Ok(entity) = Entity::decode(&value) {
                            last_key = Some(key.clone());
                            entities.push(entity);
                        }
                    }
                    None => break,
                }
            }
        }
        Err(_) => {
            // Table doesn't exist - return empty
        }
    }

    Ok((entities, last_key))
}

/// Populate DuckDB output chunk with entity data
fn populate_entity_output(
    entities: &[Entity],
    column_index: &HashMap<String, usize>,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    for (row_idx, entity) in entities.iter().enumerate() {
        // Populate id column
        if let Some(&col_idx) = column_index.get("id") {
            let vector = output.flat_vector(col_idx);
            let value = CString::new(entity.id.as_u64().to_string())?;
            vector.insert(row_idx, value);
        }

        // Populate labels column (JSON array)
        if let Some(&col_idx) = column_index.get("labels") {
            let vector = output.flat_vector(col_idx);
            let labels: Vec<&str> = entity.labels.iter().map(|l| l.as_str()).collect();
            let labels_json = serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string());
            let value = CString::new(labels_json)?;
            vector.insert(row_idx, value);
        }

        // Populate property columns
        for (prop_name, prop_value) in &entity.properties {
            let col_name = format!("prop_{}", prop_name);
            if let Some(&col_idx) = column_index.get(&col_name) {
                let vector = output.flat_vector(col_idx);
                let value_str = value_to_duckdb_string(prop_value);
                let value = CString::new(value_str)?;
                vector.insert(row_idx, value);
            }
        }
    }

    Ok(())
}

/// Convert a Manifold Value to a JSON string for DuckDB
fn value_to_json_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s)),
        Value::Bytes(b) => serde_json::to_string(&base64_encode(b)).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string()),
        Value::Vector(v) => serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()),
        Value::SparseVector(sv) => serde_json::to_string(sv).unwrap_or_else(|_| "{}".to_string()),
        Value::MultiVector(mv) => serde_json::to_string(mv).unwrap_or_else(|_| "[]".to_string()),
    }
}

/// Simple base64 encoding for bytes
fn base64_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        let _ = write!(result, "{}", ALPHABET[(b0 >> 2) & 0x3F] as char);
        let _ = write!(result, "{}", ALPHABET[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);

        if chunk.len() > 1 {
            let _ = write!(result, "{}", ALPHABET[((b1 << 2) | (b2 >> 6)) & 0x3F] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            let _ = write!(result, "{}", ALPHABET[b2 & 0x3F] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Convert a Manifold Value to the appropriate DuckDB string representation
fn value_to_duckdb_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        // For complex types, use JSON
        _ => value_to_json_string(value),
    }
}
