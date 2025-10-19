use bytes::Bytes;
use std::path::PathBuf;
use std::result::Result;

/// Queue record containing id and payload
#[derive(Debug, Clone)]
pub struct QueueRecord {
    pub id: u64,
    pub payload: Bytes,
}

/// Error types for DiskQueue operations
#[derive(Debug)]
pub enum DiskQueueError {
    IoError(std::io::Error),
    SerializationError(String),
    QueueNotFound(String),
    RecordNotFound(String),
    InvalidOperation(String),
}

impl std::fmt::Display for DiskQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskQueueError::IoError(e) => write!(f, "IO error: {}", e),
            DiskQueueError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            DiskQueueError::QueueNotFound(name) => write!(f, "Queue not found: {}", name),
            DiskQueueError::RecordNotFound(id) => write!(f, "Record not found: {}", id),
            DiskQueueError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
    }
}

impl std::error::Error for DiskQueueError {}

impl From<std::io::Error> for DiskQueueError {
    fn from(err: std::io::Error) -> Self {
        DiskQueueError::IoError(err)
    }
}

impl From<fjall::Error> for DiskQueueError {
    fn from(err: fjall::Error) -> Self {
        DiskQueueError::IoError(std::io::Error::other(err.to_string()))
    }
}

/// ThreadSafeDiskQueue trait for persistent queue operations
/// All methods are thread-safe and can be called concurrently
pub trait ThreadSafeDiskQueue: Send + Sync {
    /// Open or create a disk queue at the specified path with the given name
    fn open(path: PathBuf, name: String) -> Result<Self, DiskQueueError>
    where
        Self: Sized;

    /// Enqueue a record and return its id (thread-safe)
    fn enqueue(&self, record: QueueRecord) -> Result<u64, DiskQueueError>;

    /// Dequeue a record by id (thread-safe)
    fn dequeue(&self, id: u64) -> Result<QueueRecord, DiskQueueError>;

    /// Remove multiple records by their ids (thread-safe)
    fn batch_remove(&self, ids: Vec<u64>) -> Result<(), DiskQueueError>;

    /// Get the first N bytes worth of records from the queue starting from offset
    fn first_n_bytes(
        &self,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<QueueRecord>, DiskQueueError>;

    /// Shutdown the queue and release resources
    fn shutdown(&self) -> Result<(), DiskQueueError>;
}
