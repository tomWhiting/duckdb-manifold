//! ManifoldDB Scanner Extension for DuckDB
//!
//! This extension enables DuckDB to query ManifoldDB databases directly,
//! exposing entities, edges, and specialized functions for graph traversal
//! and vector similarity search.
//!
//! ## Philosophy
//! - Manifold handles what it's good at: graph traversal, vector ANN, OLTP
//! - DuckDB handles what it's good at: vectorized analytics, aggregations, window functions
//! - This extension bridges them with zero-copy where possible
//!
//! ## Design Principles
//! - Dynamic schema discovery (no hardcoded schemas)
//! - Batch operations (no row-at-a-time iteration where batches are possible)
//! - Push down filters and projections to Manifold where beneficial

extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod error;
mod scanner;
mod schema;

use duckdb::{ffi, Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::error::Error;

// Re-export scanner implementations
pub use scanner::entities::ManifoldEntitiesVTab;
pub use scanner::edges::ManifoldEdgesVTab;

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

/// Extension entrypoint - registers all table functions with DuckDB
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Register entity scanner
    // Usage: SELECT * FROM manifold_entities('/path/to/db')
    con.register_table_function::<ManifoldEntitiesVTab>("manifold_entities")
        .expect("Failed to register manifold_entities table function");

    // Register edge scanner
    // Usage: SELECT * FROM manifold_edges('/path/to/db')
    con.register_table_function::<ManifoldEdgesVTab>("manifold_edges")
        .expect("Failed to register manifold_edges table function");

    // TODO: Register graph traversal function
    // Usage: SELECT * FROM manifold_traverse('/path/to/db', start_id, edge_type, depth)

    // TODO: Register vector search function
    // Usage: SELECT * FROM manifold_vector_search('/path/to/db', collection, query_vector, k)

    Ok(())
}
