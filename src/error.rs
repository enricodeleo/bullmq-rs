use std::fmt;

/// Errors that can occur when using bullmq-rs.
#[derive(Debug)]
pub enum BullmqError {
    /// Redis connection or command error.
    Redis(redis::RedisError),
    /// Serialization or deserialization error.
    Serialization(serde_json::Error),
    /// Job not found with the given ID.
    JobNotFound(String),
    /// Worker has been shut down.
    WorkerClosed,
    /// Generic error.
    Other(String),
}

impl fmt::Display for BullmqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BullmqError::Redis(e) => write!(f, "Redis error: {}", e),
            BullmqError::Serialization(e) => write!(f, "Serialization error: {}", e),
            BullmqError::JobNotFound(id) => write!(f, "Job not found: {}", id),
            BullmqError::WorkerClosed => write!(f, "Worker has been shut down"),
            BullmqError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for BullmqError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BullmqError::Redis(e) => Some(e),
            BullmqError::Serialization(e) => Some(e),
            _ => None,
        }
    }
}

impl From<redis::RedisError> for BullmqError {
    fn from(err: redis::RedisError) -> Self {
        BullmqError::Redis(err)
    }
}

impl From<serde_json::Error> for BullmqError {
    fn from(err: serde_json::Error) -> Self {
        BullmqError::Serialization(err)
    }
}

/// Result type alias for bullmq-rs operations.
pub type BullmqResult<T> = Result<T, BullmqError>;
