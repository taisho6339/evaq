use crate::disk_queue::{ThreadSafeDiskQueue, DiskQueueError, QueueRecord};
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

    /// Push a record to the dead letter queue for retry
    pub fn push_retry_item(&self, payload: Bytes) -> Result<u64, DiskQueueError> {
        let id = self.retry_next_id.fetch_add(1, Ordering::Relaxed);
        let record = QueueRecord { id, payload };
        self.dead_letter_queue.enqueue(record)?;
        Ok(id)
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
    fn test_evaq_open() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf());
        assert!(evaq.is_ok());
    }

    #[test]
    fn test_push_and_remove_wal() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        let id = evaq.push_wal(Bytes::from("test wal data")).unwrap();

        let result = evaq.remove_wal(id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_push_retry_item() {
        let temp_dir = TempDir::new().unwrap();
        let evaq = Evaq::<FjallDiskQueue>::open(temp_dir.path().to_path_buf()).unwrap();

        let id = evaq
            .push_retry_item(Bytes::from("test retry data"))
            .unwrap();

        assert!(id > 0);
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
        assert!(records.len() >= 2);
        assert!(records.iter().any(|r| r.id == id2));
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

        // Should get at least 1 record
        assert!(!records.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore] // This is a long-running performance test, run with: cargo test -- --ignored
    async fn test_long_running_performance() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use tokio::time::sleep;
        use tracing::{info, warn};

        // Initialize tracing
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        // Get workspace path from environment variable
        let workspace_path = std::env::var("CARGO_WORKSPACE").unwrap_or_else(|_| {
            std::env::current_dir()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        });
        let test_path = PathBuf::from(workspace_path).join("evaq_perf_test_data");

        // Clean up any existing test data
        let _ = std::fs::remove_dir_all(&test_path);

        info!("Opening Evaq at path: {:?}", test_path);
        let evaq = Arc::new(Evaq::<FjallDiskQueue>::open(test_path.clone()).unwrap());

        info!("Starting initial data population: 10,000,000 records of 2KB each");
        let populate_start = Instant::now();
        let data_2kb = Bytes::from("x".repeat(2000));

        // Populate initial data
        for i in 0..1_000_000 {
            if i % 1_000_000 == 0 {
                info!("Populated {} million records", i / 1_000_000);
            }
            evaq.push_wal(data_2kb.clone()).unwrap();
            evaq.push_retry_item(data_2kb.clone()).unwrap();
        }
        info!(
            "Initial population completed in {:?}",
            populate_start.elapsed()
        );

        // Spawn WAL worker thread
        let evaq_wal = evaq.clone();
        let wal_handle = tokio::spawn(async move {
            info!("WAL worker thread started");
            let interval_duration = Duration::from_millis(1000);
            let records_per_interval = 3000;
            let interval_between_records =
                interval_duration.as_micros() / records_per_interval as u128;

            let mut total_records = 0u64;
            let mut interval_count = 0u64;

            loop {
                let start = Instant::now();
                let mut handles = Vec::new();

                // Spawn all record processing tasks concurrently
                for i in 0..records_per_interval {
                    let evaq_clone = evaq_wal.clone();
                    let delay = Duration::from_micros((i as u128 * interval_between_records) as u64);

                    let handle = tokio::spawn(async move {
                        // Stagger the start times to achieve ~3000 records per second
                        sleep(delay).await;

                        let id = evaq_clone.push_wal(Bytes::from("x".repeat(2000))).unwrap();

                        // Wait 5ms before remove
                        sleep(Duration::from_millis(5)).await;

                        evaq_clone.remove_wal(id).unwrap();
                    });

                    handles.push(handle);
                }

                // Wait for all tasks to complete
                futures::future::join_all(handles).await;

                let actual_records = records_per_interval;
                total_records += actual_records as u64;
                let elapsed = start.elapsed();
                interval_count += 1;

                // Calculate throughput
                let throughput = actual_records as f64 / elapsed.as_secs_f64();
                let avg_throughput = total_records as f64
                    / (interval_count as f64 * interval_duration.as_secs_f64());

                info!(
                    "WAL throughput: {:.2} records/sec (current), {:.2} records/sec (average), total: {} records",
                    throughput, avg_throughput, total_records
                );

                if elapsed < interval_duration {
                    sleep(interval_duration - elapsed).await;
                }
            }
        });

        // Spawn retry worker thread
        let evaq_retry = evaq.clone();
        let retry_handle = tokio::spawn(async move {
            info!("Retry worker thread started");
            let mut offset = 0u64;

            loop {
                // Wait 10 seconds
                sleep(Duration::from_secs(10)).await;

                // Fetch records from dead letter queue
                let fetch_start = Instant::now();
                let evaq_clone = evaq_retry.clone();
                let offset_copy = offset;
                let records = tokio::task::spawn_blocking(move || {
                    evaq_clone.first_n_bytes_from_dead_letter_queue(offset_copy, 64 * 1024 * 1024)
                })
                .await
                .unwrap()
                .unwrap();
                let fetch_duration = fetch_start.elapsed();
                info!(
                    "first_n_bytes_from_dead_letter_queue took {:?}, fetched {} records, offset: {}",
                    fetch_duration,
                    records.len(),
                    offset
                );

                if fetch_duration > Duration::from_secs(1) {
                    warn!(
                        "first_n_bytes_from_dead_letter_queue is slow: {:?}",
                        fetch_duration
                    );
                }

                // Wait 1 second
                sleep(Duration::from_secs(1)).await;

                // Remove processed records
                if !records.is_empty() {
                    let ids: Vec<u64> = records.iter().map(|r| r.id).collect();
                    let last_id = evaq_retry.done_retry_items(ids).unwrap();

                    // Update offset to one more than the last processed ID
                    if let Some(id) = last_id {
                        offset = id + 1;
                    }
                }
            }
        });

        // Run for 10 minutes
        info!("Running performance test for 10 minutes...");
        sleep(Duration::from_secs(600)).await;

        info!("Test duration completed, stopping workers...");
        wal_handle.abort();
        retry_handle.abort();

        // Clean up test data
        drop(evaq);
        sleep(Duration::from_secs(1)).await;

        info!("Cleaning up test data...");
        if let Err(e) = std::fs::remove_dir_all(&test_path) {
            warn!("Failed to clean up test data: {}", e);
        }

        info!("Performance test completed");
    }
}
