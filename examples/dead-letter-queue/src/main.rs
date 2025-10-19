use bytes::Bytes;
use evaq::Evaq;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

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
    // let _ = std::fs::remove_dir_all(&test_path);

    info!("Opening Evaq at path: {:?}", test_path);
    let evaq = Arc::new(Evaq::new(test_path.clone()).unwrap());

    info!("Starting initial data population: 1,000,000 records of 2KB each");
    let populate_start = Instant::now();
    let data_2kb = Bytes::from("x".repeat(2000));

    // Populate initial data
    for i in 0..1_000_000 {
        if i % 100_000 == 0 && i > 0 {
            info!("Populated {} records", i);
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
        let interval_between_records = interval_duration.as_micros() / records_per_interval as u128;

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
            let records = match tokio::task::spawn_blocking(move || {
                evaq_clone.first_n_bytes_from_dead_letter_queue(offset_copy, 64 * 1024 * 1024)
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

            // Remove processed records
            if !records.is_empty() {
                let ids: Vec<u64> = records.iter().map(|r| r.id).collect();
                let evaq_done = evaq_retry.clone();
                let last_id =
                    match tokio::task::spawn_blocking(move || evaq_done.done_retry_items(ids))
                        .await
                    {
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
