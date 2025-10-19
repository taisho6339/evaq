# Evaq

A high-performance, LSM-tree based on-disk queue implementation for data evacuation and recovery in Rust.

## Overview

Evaq (Evacuation Queue) is a persistent queue system designed for reliable data evacuation and recovery scenarios. It provides a Write-Ahead Log (WAL) for temporary data persistence and a Dead Letter Queue (DLQ) for retry mechanisms, making it ideal for building resilient data pipelines.

## Use Cases

- **Message Queue Persistence**: Persist messages before sending to Kafka, RabbitMQ, or other message brokers
- **Retry Mechanism**: Store failed operations in the Dead Letter Queue for later retry
- **Data Recovery**: Recover from application crashes or network failures
- **Rate Limiting Buffer**: Buffer high-throughput data before processing
- **Reliable Data Pipeline**: Ensure no data loss in distributed systems

## Features

- **LSM-Tree Storage**: Built on [fjall](https://github.com/fjall-rs/fjall), a high-performance LSM-tree storage engine
- **Thread-Safe**: Safe for concurrent access from multiple threads
- **WAL Support**: Write-Ahead Log for temporary data persistence
- **Dead Letter Queue**: Separate queue for failed items that need retry
- **Batch Operations**: Efficient batch removal for high-throughput scenarios
- **Configurable Memory**: Tunable memory buffers and flush workers
- **Zero-Copy**: Uses `Bytes` for efficient memory management

## Architecture

```
┌─────────────────────────────────────────┐
│              Your Application            │
└─────────────────┬───────────────────────┘
                  │
                  ▼
         ┌────────────────┐
         │     Evaq       │
         └────────┬───────┘
                  │
         ┌────────┴────────┐
         │                 │
    ┌────▼─────┐    ┌─────▼──────┐
    │ WAL Queue│    │  DLQ Queue │
    └────┬─────┘    └─────┬──────┘
         │                │
         └────────┬───────┘
                  │
         ┌────────▼─────────┐
         │  LSM-Tree (fjall) │
         │  ┌──────────┐    │
         │  │ MemTable │    │
         │  └─────┬────┘    │
         │        │         │
         │  ┌─────▼────┐   │
         │  │ SSTables │   │
         │  └──────────┘   │
         └──────────────────┘
                  │
         ┌────────▼─────────┐
         │    Disk Storage   │
         └───────────────────┘
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
evaq = "0.1.0"
```

## Usage

### Basic Example

```rust
use evaq::evaq::Evaq;
use bytes::Bytes;
use std::path::PathBuf;

// Open an Evaq instance
let evaq = Evaq::open(PathBuf::from("/tmp/evaq-data"))?;

// Write to WAL (e.g., before sending to Kafka)
let data = Bytes::from("important message");
let id = evaq.push_wal(data.clone())?;

// Successfully sent to Kafka - remove from WAL
evaq.remove_wal(id)?;
```

### Retry Pattern with Dead Letter Queue

```rust
use evaq::evaq::Evaq;
use bytes::Bytes;
use std::path::PathBuf;

let evaq = Evaq::open(PathBuf::from("/tmp/evaq-data"))?;

// Write to WAL
let data = Bytes::from("message to send");
let wal_id = evaq.push_wal(data.clone())?;

// Try to send to external system
match send_to_kafka(&data) {
    Ok(_) => {
        // Success - remove from WAL
        evaq.remove_wal(wal_id)?;
    }
    Err(_) => {
        // Failed - move to Dead Letter Queue for retry
        evaq.push_retry_item(data)?;
    }
}
```

### Batch Recovery from Dead Letter Queue

```rust
use evaq::evaq::Evaq;
use std::path::PathBuf;

let evaq = Evaq::open(PathBuf::from("/tmp/evaq-data"))?;

// Fetch up to 64MB of records starting from offset 0
let records = evaq.first_n_bytes_from_dead_letter_queue(0, 64 * 1024 * 1024)?;

let mut successful_ids = Vec::new();

for record in records {
    match retry_send(&record.payload) {
        Ok(_) => {
            successful_ids.push(record.id);
        }
        Err(e) => {
            eprintln!("Retry failed for record {}: {}", record.id, e);
        }
    }
}

// Remove successfully processed records
if !successful_ids.is_empty() {
    evaq.done_retry_items(successful_ids)?;
}
```

### Graceful Shutdown

```rust
use evaq::evaq::Evaq;
use std::path::PathBuf;

let evaq = Evaq::open(PathBuf::from("/tmp/evaq-data"))?;

// ... do work ...

// Ensure all data is persisted before shutdown
evaq.shutdown()?;
```

## API Reference

### `Evaq::open(path: PathBuf) -> Result<Self, DiskQueueError>`

Creates a new Evaq instance with WAL and Dead Letter Queue at the specified path.

### `push_wal(payload: Bytes) -> Result<u64, DiskQueueError>`

Pushes data to the Write-Ahead Log and returns a unique ID.

### `remove_wal(id: u64) -> Result<(), DiskQueueError>`

Removes a record from the WAL by ID (typically after successful processing).

### `push_retry_item(payload: Bytes) -> Result<u64, DiskQueueError>`

Pushes a failed item to the Dead Letter Queue for later retry.

### `done_retry_items(ids: Vec<u64>) -> Result<Option<u64>, DiskQueueError>`

Batch removes successfully retried items from the Dead Letter Queue.

### `first_n_bytes_from_dead_letter_queue(offset: u64, max_bytes: usize) -> Result<Vec<QueueRecord>, DiskQueueError>`

Retrieves records from the Dead Letter Queue starting at the given offset, up to `max_bytes` in total size.

### `shutdown(self) -> Result<(), DiskQueueError>`

Gracefully shuts down and persists all pending data to disk.

## Performance

Evaq is designed for high-throughput scenarios:

- **Write Performance**: Leverages LSM-tree architecture for fast sequential writes
- **Memory Efficiency**: Configurable write buffers (default: 512MB for keyspace, 64MB per partition)
- **Batch Operations**: Efficient batch removal reduces I/O overhead
- **Flush Workers**: Multiple workers (default: 4) for parallel flushing

See [src/evaq.rs](src/evaq.rs) for the performance test implementation.

## Thread Safety

All operations in Evaq are thread-safe. Multiple threads can safely:
- Push to WAL concurrently
- Remove from WAL concurrently
- Push to Dead Letter Queue concurrently
- Read from Dead Letter Queue concurrently

Atomic counters ensure unique IDs are generated safely across threads.

## Storage Backend

Evaq uses [fjall](https://github.com/fjall-rs/fjall) as its storage backend, which provides:
- **LSM-Tree Architecture**: Optimized for write-heavy workloads
- **Persistence**: All data is persisted to disk
- **Crash Recovery**: Automatic recovery from crashes
- **Configurable Compaction**: Tunable compaction strategies

## Error Handling

Evaq uses the `DiskQueueError` type for all errors:

```rust
pub enum DiskQueueError {
    IoError(std::io::Error),
    SerializationError(String),
    RecordNotFound(String),
    // ... other variants
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Built on [fjall](https://github.com/fjall-rs/fjall) - A high-performance LSM-tree storage engine
- Inspired by the need for reliable data evacuation in distributed systems
