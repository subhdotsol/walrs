
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rust_wal::{Operation, SegmentedWal};

fn benchmark_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");

    for batch_size in [1usize, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("append_then_sync", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let wal = SegmentedWal::open(dir.path(), 16 * 1024 * 1024)
                            .unwrap();
                        (dir, wal)
                    },
                    |(dir, mut wal)| {
                        for index in 0..batch_size {
                            let key = index.to_le_bytes().to_vec();
                            wal.append(Operation::Put {
                                key,
                                value: black_box(vec![b'x'; 100]),
                            })
                            .unwrap();
                        }

                        wal.sync().unwrap();
                        black_box(dir);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn benchmark_append_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append_only");

    for batch_size in [1usize, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("append", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let wal = SegmentedWal::open(dir.path(), 16 * 1024 * 1024)
                            .unwrap();
                        (dir, wal)
                    },
                    |(dir, mut wal)| {
                        for index in 0..batch_size {
                            wal.append(Operation::Put {
                                key: index.to_le_bytes().to_vec(),
                                value: black_box(vec![b'x'; 100]),
                            })
                            .unwrap();
                        }

                        black_box(&wal);
                        black_box(dir);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_append,
    benchmark_append_only
);
criterion_main!(benches);
