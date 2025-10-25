use bytes::Bytes;
use evaq::Evaq;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, warn};

const INITIAL_RECORDS: u64 = 1000;

const EVENT_MESSAGE_SIZE: usize = 512;
const EVENT_PRODUCE_RECORDS_PER_SECOND: usize = 6000;
const EVENT_PRODUCE_INTERVAL_DURATION_MS: u64 = 1000;
const EVENT_PRODUCE_SIMULATED_TASK_LATECY_MS: u64 = 5;

const RETRY_INTERVAL_DURATION_SEC: u64 = 10;
const RETRY_CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64 MB

async fn initialize_data(evaq: Arc<Evaq>, total_records: u64, record_size: usize) {
    info!(
        "Starting initial data population: {} records of {} bytes each",
        total_records, record_size
    );
    let populate_start = Instant::now();
    let payload = Bytes::from("x".repeat(record_size));

    for i in 0..total_records {
        if i % 100_000 == 0 && i > 0 {
            info!("Populated {} records", i);
        }
        evaq.push_wal(payload.clone()).unwrap();
        evaq.push_retry_item(payload.clone()).unwrap();
    }
    info!(
        "Initial population completed in {:?}",
        populate_start.elapsed()
    );
}

fn spawn_wal_worker(evaq: Arc<Evaq>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("WAL worker thread started");
        let interval_duration = Duration::from_millis(EVENT_PRODUCE_INTERVAL_DURATION_MS);
        let records_per_interval = EVENT_PRODUCE_RECORDS_PER_SECOND;
        let interval_between_records = interval_duration.as_micros() / records_per_interval as u128;

        let mut total_records = 0u64;

        loop {
            let start = Instant::now();
            let mut handles = Vec::new();
            let payload = Bytes::from("x".repeat(EVENT_MESSAGE_SIZE));

            // Spawn all record processing tasks concurrently
            for i in 0..records_per_interval {
                let evaq = evaq.clone();
                let delay = Duration::from_micros((i as u128 * interval_between_records) as u64);
                let payload = payload.clone();
                let handle = tokio::spawn(async move {
                    // Stagger the start times to achieve ~3000 records per second
                    sleep(delay).await;

                    let id = evaq.push_wal(payload).unwrap();

                    sleep(Duration::from_millis(
                        EVENT_PRODUCE_SIMULATED_TASK_LATECY_MS,
                    ))
                    .await;

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

fn spawn_retry_worker(
    evaq: Arc<Evaq>,
    processed_ids: Arc<Mutex<HashSet<u64>>>,
    output_file: Arc<Mutex<std::fs::File>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("Retry worker thread started");
        let mut offset = 0u64;
        loop {
            let evaq_fetch = evaq.clone();
            let evaq_done = evaq.clone();
            // Wait 10 seconds
            sleep(Duration::from_secs(RETRY_INTERVAL_DURATION_SEC)).await;

            // Fetch records from dead letter queue
            let fetch_start = Instant::now();

            let offset_copy = offset;
            let records = match tokio::task::spawn_blocking(move || {
                evaq_fetch.first_n_bytes_from_dead_letter_queue(offset_copy, RETRY_CHUNK_SIZE)
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

            if records.is_empty() {
                info!(
                    "No records to process from dead letter queue at offset {}",
                    offset
                );
                continue;
            }

            let ids: Vec<u64> = records.iter().map(|r| r.id).collect();

            // Record successfully processed IDs
            let ids_for_logging = ids.clone();
            let mut processed = processed_ids.lock().await;
            let mut file = output_file.lock().await;
            for id in ids_for_logging {
                processed.insert(id);
                if let Err(e) = writeln!(file, "{}", id) {
                    error!("Failed to write ID to file: {:?}", e);
                }
            }
            if let Err(e) = file.flush() {
                error!("Failed to flush file: {:?}", e);
            }
            drop(file);
            drop(processed);

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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let workspace_path = std::env::var("CARGO_WORKSPACE").unwrap_or_else(|_| {
        std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    });
    let test_path = PathBuf::from(workspace_path.clone()).join("evaq_perf_test_data");
    // Clean up any existing test data
    let _ = std::fs::remove_dir_all(&test_path);

    // Create output file for recording processed retry IDs
    let output_file_path = PathBuf::from(&workspace_path)
        .join("examples/dead-letter-queue")
        .join("processed_retry_ids.txt");

    // Ensure parent directory exists
    if let Some(parent) = output_file_path.parent() {
        std::fs::create_dir_all(parent).expect("Failed to create output directory");
    }

    let output_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&output_file_path)
            .expect("Failed to create output file"),
    ));
    info!("Recording processed retry IDs to: {:?}", output_file_path);

    let processed_ids = Arc::new(Mutex::new(HashSet::new()));

    let evaq = Arc::new(Evaq::new(test_path.clone()).unwrap());
    initialize_data(evaq.clone(), INITIAL_RECORDS, EVENT_MESSAGE_SIZE).await;

    let wal_handle = spawn_wal_worker(evaq.clone());
    let retry_handle = spawn_retry_worker(evaq.clone(), processed_ids.clone(), output_file.clone());

    // Run for 10 minute
    info!("Running performance test for 1 minute...");
    sleep(Duration::from_secs(60)).await;

    info!("Test duration completed, stopping workers...");
    wal_handle.abort();
    retry_handle.abort();

    // Wait a bit for final writes
    sleep(Duration::from_secs(2)).await;

    let _ = evaq.shutdown();

    // Verify all retry items were processed
    let processed = processed_ids.lock().await;
    info!("Total unique retry IDs processed: {}", processed.len());
    info!("Processed retry IDs saved to: {:?}", output_file_path);

    info!("Cleaning up test data...");
    if let Err(e) = std::fs::remove_dir_all(&test_path) {
        warn!("Failed to clean up test data: {}", e);
    }

    info!("Performance test completed");
}
