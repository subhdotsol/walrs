# WALRS

A write-ahead log (WAL) implemented in Rust to explore durable writes, binary record encoding, crash recovery, and segmented storage.

## Features

- Binary record encoding and decoding
- Append-only log writing
- Monotonically increasing sequence numbers
- Record integrity validation with checksums
- Recovery from existing log files
- Handling of incomplete trailing records
- Segmented logs with automatic rotation
- Configurable segment size
- Unit and integration tests
- Benchmarks for write throughput and recovery performance

## Design

The implementation is organized into the following components:

- `record.rs`: Record types, binary serialization, and validation.
- `writer.rs`: Appending records to the log and syncing data.
- `recovery.rs`: Reading and recovering records from existing logs.
- `segment.rs`: Segment management and log rotation.
- `lib.rs`: Public library interface.
- `tests/wal_tests.rs`: Integration and recovery tests.

## Getting Started

Clone the repository and run the tests:

```bash
git clone https://github.com/walrs
cd walrs
cargo test
```

Run formatting and lint checks:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Run benchmarks if configured:

```bash
cargo bench
```

## Status

A systems programming project focused on implementing and testing WAL fundamentals in Rust. Production readiness, crash consistency, and durability guarantees require further validation against filesystem and storagedevice behavior.
