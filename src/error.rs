//! Error types for the ManifoldDB scanner extension

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ManifoldScannerError {
    #[error("Failed to open database at path: {path}")]
    DatabaseOpenError { path: String, source: Box<dyn std::error::Error + Send + Sync> },

    #[error("Failed to read entity: {0}")]
    EntityReadError(String),

    #[error("Failed to read edge: {0}")]
    EdgeReadError(String),

    #[error("Schema discovery failed: {0}")]
    SchemaDiscoveryError(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

// Note: thiserror implements std::error::Error, so ManifoldScannerError
// can be converted to Box<dyn Error> via the blanket impl in alloc
