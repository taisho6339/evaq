use bytes::Bytes;
use evaq::Evaq;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};

async fn initialize_data(evaq: Arc<Evaq>, total_records: u64, record_size: usize) {
    info!(
        "Starting initial data population: {} records of {} bytes each",
        total_records, record_size
    );
    let populate_start = Instant::now();
    let data = Bytes::from("x".repeat(record_size));

    for i in 0..total_records {
        if i % 100_000 == 0 && i > 0 {
            info!("Populated {} records", i);
        }
        evaq.push_wal(data.clone()).unwrap();
        evaq.push_retry_item(data.clone()).unwrap();
    }
    info!(
        "Initial population completed in {:?}",
        populate_start.elapsed()
    );
}

fn spawn_wal_worker(evaq: Arc<Evaq>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("WAL worker thread started");
        let interval_duration = Duration::from_millis(1000);
        let records_per_interval = 3000;
        let interval_between_records = interval_duration.as_micros() / records_per_interval as u128;

        let mut total_records = 0u64;

        loop {
            let start = Instant::now();
            let mut handles = Vec::new();

            // Spawn all record processing tasks concurrently
            for i in 0..records_per_interval {
                let evaq = evaq.clone();
                let delay = Duration::from_micros((i as u128 * interval_between_records) as u64);

                let handle = tokio::spawn(async move {
                    // Stagger the start times to achieve ~3000 records per second
                    sleep(delay).await;

                    let id = evaq.push_wal(Bytes::from("x".repeat(2000))).unwrap();

                    // Wait 5ms before remove
                    sleep(Duration::from_millis(5)).await;

                    evaq.remove_wal(id).unwrap();
                });

                handles.push(handle);
            }

            // Wait for all tasks to complete
            futures::future::join_all(handles).await;

            let actual_records = records_per_interval;
            total_records += actual_records as u64;
            let elapsed = start.elapsed();

            // Calculate throughput
            let throughput = actual_records as f64 / elapsed.as_secs_f64();

            info!(
                "WAL throughput: {:.2} records/sec (current), total: {} records",
                throughput, total_records
            );

            if elapsed < interval_duration {
                sleep(interval_duration - elapsed).await;
            }
        }
    })
}

fn spawn_retry_worker(evaq: Arc<Evaq>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("Retry worker thread started");
        let mut offset = 0u64;
        loop {
            let evaq_fetch = evaq.clone();
            let evaq_done = evaq.clone();
            // Wait 10 seconds
            sleep(Duration::from_secs(10)).await;

            // Fetch records from dead letter queue
            let fetch_start = Instant::now();

            let offset_copy = offset;
            let records = match tokio::task::spawn_blocking(move || {
                evaq_fetch.first_n_bytes_from_dead_letter_queue(offset_copy, 64 * 1024 * 1024)
            })
            .await
            {
                Ok(Ok(records)) => records,
                Ok(Err(e)) => {
                    error!("Failed to fetch from dead letter queue: {:?}", e);
                    continue;
                }
                Err(e) => {
                    error!("Spawn blocking task failed: {:?}", e);
                    continue;
                }
            };
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

            if records.is_empty() {
                info!(
                    "No records to process from dead letter queue at offset {}",
                    offset
                );
                continue;
            }

            let ids: Vec<u64> = records.iter().map(|r| r.id).collect();
            let last_id =
                match tokio::task::spawn_blocking(move || evaq_done.done_retry_items(ids)).await {
                    Ok(Ok(last_id)) => last_id,
                    Ok(Err(e)) => {
                        error!("Failed to mark retry items as done: {:?}", e);
                        continue;
                    }
                    Err(e) => {
                        error!("Spawn blocking task failed: {:?}", e);
                        continue;
                    }
                };

            // Update offset to one more than the last processed ID
            if let Some(id) = last_id {
                offset = id + 1;
            }
        }
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
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

    let evaq = Arc::new(Evaq::new(test_path.clone()).unwrap());
    initialize_data(evaq.clone(), 1_000_000, 2000).await;

    let wal_handle = spawn_wal_worker(evaq.clone());
    let retry_handle = spawn_retry_worker(evaq.clone());

    // Run for 10 minutes
    info!("Running performance test for 10 minutes...");
    sleep(Duration::from_secs(600)).await;

    info!("Test duration completed, stopping workers...");
    wal_handle.abort();
    retry_handle.abort();
    let _ = evaq.shutdown();

    info!("Cleaning up test data...");
    if let Err(e) = std::fs::remove_dir_all(&test_path) {
        warn!("Failed to clean up test data: {}", e);
    }

    info!("Performance test completed");
}
