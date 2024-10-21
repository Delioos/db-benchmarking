use thiserror::Error;
use tokio_postgres::{Error, NoTls};

#[derive(Error, Debug)]
pub enum BenchmarkError {
    #[error("Environment variable not found: {0}")]
    EnvVarError(#[from] std::env::VarError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] tokio_postgres::Error),

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unknown error occurred")]
    Unknown,
}

pub type Result<T> = std::result::Result<T, BenchmarkError>;
