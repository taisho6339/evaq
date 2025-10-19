use crate::disk_queue::{DiskQueueError, QueueRecord, ThreadSafeDiskQueue};
use bytes::Bytes;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

// Global keyspace cache to allow multiple partitions to share the same keyspace
lazy_static::lazy_static! {
    static ref KEYSPACE_CACHE: StdMutex<HashMap<PathBuf, Arc<Keyspace>>> = StdMutex::new(HashMap::new());
}

/// FjallDiskQueue implementation using fjall LSM-tree storage
pub struct FjallDiskQueue {
    keyspace: Arc<Keyspace>,
    partition: PartitionHandle,
}

impl ThreadSafeDiskQueue for FjallDiskQueue {
    fn open(path: PathBuf, name: String) -> Result<FjallDiskQueue, DiskQueueError> {
        let mut cache = KEYSPACE_CACHE.lock().unwrap();

        // Get or create keyspace for this path
        let keyspace = cache
            .entry(path.clone())
            .or_insert_with(|| {
                let config = Config::new(path)
                    .max_write_buffer_size(512 * 1024 * 1024)
                    .flush_workers(4);
                Arc::new(config.open().unwrap())
            })
            .clone();

        drop(cache);

        // let fifo = Fifo::new(u64::MAX, None);
        let partition = keyspace.open_partition(
            &name,
            PartitionCreateOptions::default()
                // .compaction_strategy(CompactionStrategy::Fifo(fifo))
                .max_memtable_size(64 * 1024 * 1024),
        )?;

        Ok(Self {
            keyspace,
            partition,
        })
    }

    fn enqueue(&self, record: QueueRecord) -> Result<u64, DiskQueueError> {
        let key = record.id.to_be_bytes();
        self.partition.insert(key, record.payload.as_ref())?;
        self.keyspace.persist(PersistMode::Buffer)?;
        Ok(record.id)
    }

    fn dequeue(&self, id: u64) -> Result<QueueRecord, DiskQueueError> {
        let key = id.to_be_bytes();
        let value = self
            .partition
            .get(key)?
            .ok_or_else(|| DiskQueueError::RecordNotFound(id.to_string()))?;

        self.partition.remove(key)?;
        self.keyspace.persist(PersistMode::Buffer)?;

        Ok(QueueRecord {
            id,
            payload: Bytes::copy_from_slice(&value),
        })
    }

    fn batch_remove(&self, ids: Vec<u64>) -> Result<(), DiskQueueError> {
        let mut batch = self.keyspace.batch();

        for id in ids {
            let key = id.to_be_bytes();
            batch.remove(&self.partition, key);
        }

        batch.commit()?;
        self.keyspace.persist(PersistMode::Buffer)?;
        Ok(())
    }

    fn first_n_bytes(
        &self,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<QueueRecord>, DiskQueueError> {
        let mut records = Vec::new();
        let mut total_bytes = 0;

        let start_key = offset.to_be_bytes();

        for item in self.partition.range(start_key..) {
            let (key, value) = item?;

            let id = u64::from_be_bytes(key.as_ref().try_into().map_err(|_| {
                DiskQueueError::SerializationError("Invalid key format".to_string())
            })?);

            let payload = Bytes::copy_from_slice(&value);
            let record_size = payload.len();

            if total_bytes + record_size > max_bytes && !records.is_empty() {
                break;
            }

            total_bytes += record_size;
            records.push(QueueRecord { id, payload });

            if total_bytes >= max_bytes {
                break;
            }
        }

        Ok(records)
    }

    fn shutdown(self) -> Result<(), DiskQueueError> {
        self.keyspace.persist(PersistMode::SyncAll)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_fjall_queue_open() {
        let temp_dir = TempDir::new().unwrap();
        let queue = FjallDiskQueue::open(temp_dir.path().to_path_buf(), "test_queue".to_string());
        assert!(queue.is_ok());
    }

    #[test]
    fn test_enqueue_dequeue() {
        let temp_dir = TempDir::new().unwrap();
        let queue =
            FjallDiskQueue::open(temp_dir.path().to_path_buf(), "test_queue".to_string()).unwrap();

        let payload = Bytes::from("test data");
        let record = QueueRecord {
            id: 1,
            payload: payload.clone(),
        };
        let id = queue.enqueue(record.clone()).unwrap();

        let retrieved = queue.dequeue(id).unwrap();
        assert_eq!(retrieved.payload, payload);
    }
}
