use crate::disk_queue::{DiskQueueError, QueueRecord, ThreadSafeDiskQueue};
use crate::fjall_queue::FjallDiskQueue;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Evaq - A queue system with WAL and dead letter queue support
pub struct Evaq<Q: ThreadSafeDiskQueue> {
    wal_queue: Q,
    dead_letter_queue: Q,
    wal_next_id: AtomicU64,
    retry_next_id: AtomicU64,
}

// Default implementation using FjallDiskQueue
impl Evaq<FjallDiskQueue> {
    /// Create a new Evaq instance with the default FjallDiskQueue backend
    pub fn new(path: PathBuf) -> Result<Self, DiskQueueError> {
        Self::open(path)
    }
}

impl<Q: ThreadSafeDiskQueue> Evaq<Q> {
    /// Create a new Evaq instance with WAL and dead letter queue
    pub fn open(path: PathBuf) -> Result<Self, DiskQueueError> {
        let wal_queue = Q::open(path.clone(), "wal-queue".to_string())?;
        let dead_letter_queue = Q::open(path, "dead-letter-queue".to_string())?;

        Ok(Self {
            wal_queue,
            dead_letter_queue,
            wal_next_id: AtomicU64::new(1),
            retry_next_id: AtomicU64::new(1),
        })
    }

    /// Push a record to the WAL queue
    pub fn push_wal(&self, payload: Bytes) -> Result<u64, DiskQueueError> {
        let id = self.wal_next_id.fetch_add(1, Ordering::Relaxed);
        let record = QueueRecord { id, payload };
        self.wal_queue.enqueue(record)?;
        Ok(id)
    }

    /// Remove a record from the WAL queue
    pub fn remove_wal(&self, id: u64) -> Result<(), DiskQueueError> {
        self.wal_queue.dequeue(id)?;
        Ok(())
    }

    pub fn pop_wal(&self, id: u64) -> Result<QueueRecord, DiskQueueError> {
        self.wal_queue.dequeue(id)
    }

    /// Push a record to the dead letter queue for retry
    pub fn push_retry_item(&self, payload: Bytes) -> Result<u64, DiskQueueError> {
        let id = self.retry_next_id.fetch_add(1, Ordering::Relaxed);
        let record = QueueRecord { id, payload };
        self.dead_letter_queue.enqueue(record)?;
        Ok(id)
    }

    pub fn pop_retry_item(&self, id: u64) -> Result<QueueRecord, DiskQueueError> {
        self.dead_letter_queue.dequeue(id)
    }

    /// Batch remove retry items from the dead letter queue and return the last record id
    pub fn done_retry_items(&self, ids: Vec<u64>) -> Result<Option<u64>, DiskQueueError> {
        if ids.is_empty() {
            return Ok(None);
        }

        let last_id = *ids.last().unwrap();
        self.dead_letter_queue.batch_remove(ids)?;
        Ok(Some(last_id))
    }

    /// Get the first N bytes worth of records from the dead letter queue
    pub fn first_n_bytes_from_dead_letter_queue(
        &self,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<QueueRecord>, DiskQueueError> {
        self.dead_letter_queue.first_n_bytes(offset, max_bytes)
    }

    /// Shutdown the Evaq instance and persist all data
    pub fn shutdown(self) -> Result<(), DiskQueueError> {
        self.wal_queue.shutdown()?;
        self.dead_letter_queue.shutdown()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fjall_queue::FjallDiskQueue;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[test]
    fn test_push_and_remove_wal() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();
        let id = evaq.push_wal(Bytes::from("test wal data")).unwrap();
        let item = evaq.pop_wal(id);
        assert!(item.is_ok());
        assert_eq!(item.unwrap().payload, Bytes::from("test wal data"));
        let result = evaq.remove_wal(id);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_retry_item() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();
        let id = evaq
            .push_retry_item(Bytes::from("test retry data"))
            .unwrap();
        let item = evaq.pop_retry_item(id);
        assert!(item.is_ok());
        assert_eq!(item.unwrap().payload, Bytes::from("test retry data"));
    }

    #[test]
    fn test_done_retry_items() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        let id1 = evaq.push_retry_item(Bytes::from("retry 1")).unwrap();
        let id2 = evaq.push_retry_item(Bytes::from("retry 2")).unwrap();
        let id3 = evaq.push_retry_item(Bytes::from("retry 3")).unwrap();

        let last_id = evaq.done_retry_items(vec![id1, id2, id3]).unwrap();
        assert_eq!(last_id, Some(id3));
        let item = evaq.pop_retry_item(last_id.unwrap());
        assert!(item.is_err());
    }

    #[test]
    fn test_done_retry_items_empty() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        let last_id = evaq.done_retry_items(vec![]).unwrap();
        assert_eq!(last_id, None);
    }

    #[test]
    fn test_first_n_bytes_from_dead_letter_queue() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        // Add multiple items to the dead letter queue
        let id1 = evaq.push_retry_item(Bytes::from("item1")).unwrap();
        let id2 = evaq.push_retry_item(Bytes::from("item2")).unwrap();
        let id3 = evaq.push_retry_item(Bytes::from("item3")).unwrap();

        // Get first N bytes from offset 0
        let records = evaq.first_n_bytes_from_dead_letter_queue(0, 100).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].id, id1);
        assert_eq!(records[0].payload, Bytes::from("item1"));
        assert_eq!(records[1].id, id2);
        assert_eq!(records[1].payload, Bytes::from("item2"));
        assert_eq!(records[2].id, id3);
        assert_eq!(records[2].payload, Bytes::from("item3"));
    }

    #[test]
    fn test_first_n_bytes_with_offset() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        evaq.push_retry_item(Bytes::from("item1")).unwrap();
        let id2 = evaq.push_retry_item(Bytes::from("item2")).unwrap();
        evaq.push_retry_item(Bytes::from("item3")).unwrap();

        // Get records starting from id2
        let records = evaq.first_n_bytes_from_dead_letter_queue(id2, 100).unwrap();

        // Should get id2 and id3
        assert!(records.len() == 2);
        assert!(records[0].id == id2);
    }

    #[test]
    fn test_first_n_bytes_with_limit() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        evaq.push_retry_item(Bytes::from("item1")).unwrap();
        evaq.push_retry_item(Bytes::from("item2")).unwrap();
        evaq.push_retry_item(Bytes::from("item3")).unwrap();

        // Limit to only 6 bytes (should get only first item "item1" = 5 bytes)
        let records = evaq.first_n_bytes_from_dead_letter_queue(0, 6).unwrap();

        // Should get 1 record
        assert!(records.len() == 1);
    }
}
