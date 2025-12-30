//! Dynamic schema discovery for ManifoldDB
//!
//! This module handles discovering the schema of entities and edges
//! at query time, enabling fully dynamic column generation.
//!
//! ## Design
//! - Pre-scan a sample of entities to discover property keys
//! - Infer DuckDB types from Manifold Value types
//! - Support schema evolution (new properties don't break queries)

use duckdb::core::{LogicalTypeHandle, LogicalTypeId};
use std::collections::HashMap;

/// Our own type ID enum that implements Clone/Copy (LogicalTypeId from duckdb doesn't)
/// This allows us to store type information in thread-safe structures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Boolean,
    Bigint,
    Double,
    Varchar,
    Blob,
}

impl ColumnType {
    /// Convert to DuckDB LogicalTypeId
    pub fn to_logical_type_id(self) -> LogicalTypeId {
        match self {
            ColumnType::Boolean => LogicalTypeId::Boolean,
            ColumnType::Bigint => LogicalTypeId::Bigint,
            ColumnType::Double => LogicalTypeId::Double,
            ColumnType::Varchar => LogicalTypeId::Varchar,
            ColumnType::Blob => LogicalTypeId::Blob,
        }
    }

    /// Convert to DuckDB LogicalTypeHandle
    pub fn to_logical_type_handle(self) -> LogicalTypeHandle {
        LogicalTypeHandle::from(self.to_logical_type_id())
    }

    /// Convert from DuckDB LogicalTypeId (defaults to Varchar for unknown types)
    pub fn from_logical_type_id(id: LogicalTypeId) -> Self {
        match id {
            LogicalTypeId::Boolean => ColumnType::Boolean,
            LogicalTypeId::Bigint => ColumnType::Bigint,
            LogicalTypeId::Double => ColumnType::Double,
            LogicalTypeId::Blob => ColumnType::Blob,
            _ => ColumnType::Varchar, // Default unknown types to varchar
        }
    }
}

/// Represents a discovered column from entity/edge properties
#[derive(Debug, Clone)]
pub struct DiscoveredColumn {
    pub name: String,
    /// Column type (using our own Clone-able enum)
    pub column_type: ColumnType,
    pub nullable: bool,
}

impl DiscoveredColumn {
    /// Convert the stored type to a LogicalTypeHandle for DuckDB registration
    pub fn to_logical_type_handle(&self) -> LogicalTypeHandle {
        self.column_type.to_logical_type_handle()
    }
}

/// Maps Manifold Value types to ColumnType
pub fn manifold_value_to_column_type(value: &manifoldb_core::types::Value) -> ColumnType {
    use manifoldb_core::types::Value;

    match value {
        Value::Null => ColumnType::Varchar, // Nullable, default to varchar
        Value::Bool(_) => ColumnType::Boolean,
        Value::Int(_) => ColumnType::Bigint,
        Value::Float(_) => ColumnType::Double,
        Value::String(_) => ColumnType::Varchar,
        Value::Bytes(_) => ColumnType::Blob,
        Value::Array(_) => ColumnType::Varchar, // JSON-encode arrays for now
        Value::Vector(_) => ColumnType::Varchar, // JSON-encode vectors
        Value::SparseVector(_) => ColumnType::Varchar,
        Value::MultiVector(_) => ColumnType::Varchar,
    }
}

/// Discovers schema from a collection of entities
///
/// Scans entities to find all unique property keys and infer their types.
/// If a property has multiple types across entities, defaults to VARCHAR.
pub struct SchemaDiscovery {
    /// Property name -> observed types
    property_types: HashMap<String, Vec<ColumnType>>,
    /// Number of entities scanned
    sample_count: usize,
}

impl SchemaDiscovery {
    pub fn new() -> Self {
        Self {
            property_types: HashMap::new(),
            sample_count: 0,
        }
    }

    /// Add an entity's properties to the discovery
    pub fn observe_entity(&mut self, properties: &HashMap<String, manifoldb_core::types::Value>) {
        self.sample_count += 1;

        for (key, value) in properties {
            let col_type = manifold_value_to_column_type(value);

            let types = self
                .property_types
                .entry(key.clone())
                .or_insert_with(Vec::new);

            // Only add if not already present
            if !types.contains(&col_type) {
                types.push(col_type);
            }
        }
    }

    /// Generate the final schema based on observations
    ///
    /// Returns columns for:
    /// - id (always VARCHAR)
    /// - labels (always VARCHAR, JSON array)
    /// - All discovered property columns
    pub fn finalize(self) -> Vec<DiscoveredColumn> {
        let mut columns = Vec::new();

        // Fixed columns that always exist
        columns.push(DiscoveredColumn {
            name: "id".to_string(),
            column_type: ColumnType::Varchar,
            nullable: false,
        });

        columns.push(DiscoveredColumn {
            name: "labels".to_string(),
            column_type: ColumnType::Varchar, // JSON array
            nullable: false,
        });

        // Dynamic property columns - always use VARCHAR for simplicity
        // DuckDB can cast to other types as needed in queries
        let mut property_names: Vec<_> = self.property_types.keys().cloned().collect();
        property_names.sort(); // Consistent column ordering

        for name in property_names {
            columns.push(DiscoveredColumn {
                name: format!("prop_{}", name), // Prefix to avoid conflicts with fixed columns
                column_type: ColumnType::Varchar, // Always VARCHAR - simpler and DuckDB can cast
                nullable: true,                 // Properties may not exist on all entities
            });
        }

        columns
    }

    /// Get sample count for diagnostics
    pub fn sample_count(&self) -> usize {
        self.sample_count
    }
}

/// Edge schema discovery (similar but for edges)
pub struct EdgeSchemaDiscovery {
    property_types: HashMap<String, Vec<ColumnType>>,
    sample_count: usize,
}

impl EdgeSchemaDiscovery {
    pub fn new() -> Self {
        Self {
            property_types: HashMap::new(),
            sample_count: 0,
        }
    }

    pub fn observe_edge(&mut self, properties: &HashMap<String, manifoldb_core::types::Value>) {
        self.sample_count += 1;

        for (key, value) in properties {
            let col_type = manifold_value_to_column_type(value);

            let types = self
                .property_types
                .entry(key.clone())
                .or_insert_with(Vec::new);

            // Only add if not already present
            if !types.contains(&col_type) {
                types.push(col_type);
            }
        }
    }

    /// Generate edge schema
    ///
    /// Returns columns for:
    /// - id (VARCHAR)
    /// - source (VARCHAR) - source entity ID
    /// - target (VARCHAR) - target entity ID
    /// - edge_type (VARCHAR)
    /// - All discovered property columns
    pub fn finalize(self) -> Vec<DiscoveredColumn> {
        let mut columns = Vec::new();

        // Fixed columns for edges
        columns.push(DiscoveredColumn {
            name: "id".to_string(),
            column_type: ColumnType::Varchar,
            nullable: false,
        });

        columns.push(DiscoveredColumn {
            name: "source".to_string(),
            column_type: ColumnType::Varchar,
            nullable: false,
        });

        columns.push(DiscoveredColumn {
            name: "target".to_string(),
            column_type: ColumnType::Varchar,
            nullable: false,
        });

        columns.push(DiscoveredColumn {
            name: "edge_type".to_string(),
            column_type: ColumnType::Varchar,
            nullable: false,
        });

        // Dynamic property columns - always use VARCHAR for simplicity
        let mut property_names: Vec<_> = self.property_types.keys().cloned().collect();
        property_names.sort();

        for name in property_names {
            columns.push(DiscoveredColumn {
                name: format!("prop_{}", name),
                column_type: ColumnType::Varchar, // Always VARCHAR - DuckDB can cast
                nullable: true,
            });
        }

        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_discovery_basic() {
        let mut discovery = SchemaDiscovery::new();

        let mut props1 = HashMap::new();
        props1.insert(
            "name".to_string(),
            manifoldb_core::types::Value::String("Alice".to_string()),
        );
        props1.insert("age".to_string(), manifoldb_core::types::Value::Int(30));

        discovery.observe_entity(&props1);

        let schema = discovery.finalize();

        // Should have id, labels, prop_age, prop_name
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "labels");
    }
}
