use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Branch not found: {0}")]
    BranchNotFound(String),

    #[error("Branch already exists: {0}")]
    BranchExists(String),

    #[error("Commit not found: {0}")]
    CommitNotFound(String),

    #[error("Empty database — no commits yet")]
    EmptyDatabase,

    #[error("Corruption: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, IcebergError>;
