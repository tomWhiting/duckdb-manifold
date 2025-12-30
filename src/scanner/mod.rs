//! Scanner implementations for ManifoldDB
//!
//! Each scanner implements the DuckDB VTab trait to expose
//! Manifold data as queryable tables.

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex, OnceLock};
use manifoldb_storage::backends::RedbEngine;
use manifoldb_storage::StorageEngine;

pub mod entities;
pub mod edges;

/// Batch size for reading from Manifold
/// Chosen to balance memory usage and throughput
pub const BATCH_SIZE: usize = 1024;

/// Maximum entities to sample for schema discovery
/// Balance between accuracy and startup time
pub const SCHEMA_SAMPLE_SIZE: usize = 100;

/// Global engine cache - maps db_path to Arc<RedbEngine>
/// Shared between all scanners to avoid multiple opens of the same database
static ENGINE_CACHE: OnceLock<Mutex<HashMap<String, Arc<RedbEngine>>>> = OnceLock::new();

fn get_engine_cache() -> &'static Mutex<HashMap<String, Arc<RedbEngine>>> {
    ENGINE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Get or create a cached engine for the given path
pub fn get_cached_engine(db_path: &str) -> Result<Arc<RedbEngine>, Box<dyn Error>> {
    let mut cache = get_engine_cache().lock().map_err(|e| format!("Lock error: {}", e))?;

    if let Some(engine) = cache.get(db_path) {
        return Ok(Arc::clone(engine));
    }

    let engine = Arc::new(RedbEngine::open(db_path)?);
    cache.insert(db_path.to_string(), Arc::clone(&engine));
    Ok(engine)
}
