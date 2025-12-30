//! Scanner implementations for ManifoldDB
//!
//! Each scanner implements the DuckDB VTab trait to expose
//! Manifold data as queryable tables.

pub mod entities;
pub mod edges;

/// Batch size for reading from Manifold
/// Chosen to balance memory usage and throughput
pub const BATCH_SIZE: usize = 1024;

/// Maximum entities to sample for schema discovery
/// Balance between accuracy and startup time
pub const SCHEMA_SAMPLE_SIZE: usize = 100;
