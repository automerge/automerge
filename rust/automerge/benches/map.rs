use automerge::{transaction::Transactable, Automerge, ScalarValue, ROOT};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn repeated_increment(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", ScalarValue::counter(0)).unwrap();
    for _ in 0..n {
        tx.increment(ROOT, "counter", 1).unwrap();
    }
    tx.commit();
    doc
}

fn repeated_put(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, "0", i).unwrap();
    }
    tx.commit();
    doc
}

fn increasing_put(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
    tx.commit();
    doc
}

fn decreasing_put(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in (0..n).rev() {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
    tx.commit();
    doc
}

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = [100, 1_000, 10_000];

    let mut group = c.benchmark_group("map");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(BenchmarkId::new("repeated put", size), size, |b, &size| {
            b.iter(|| repeated_put(size))
        });
        group.bench_with_input(
            BenchmarkId::new("repeated increment", size),
            size,
            |b, &size| b.iter(|| repeated_increment(size)),
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| b.iter(|| increasing_put(size)),
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("decreasing put", size),
            size,
            |b, &size| b.iter(|| decreasing_put(size)),
        );
    }
    group.finish();

    let mut group = c.benchmark_group("map save");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(BenchmarkId::new("repeated put", size), size, |b, &size| {
            b.iter_batched(
                || repeated_put(size),
                |doc| doc.save(),
                criterion::BatchSize::LargeInput,
            )
        });
        group.bench_with_input(
            BenchmarkId::new("repeated increment", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || repeated_increment(size),
                    |doc| doc.save(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || increasing_put(size),
                    |doc| doc.save(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("decreasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || decreasing_put(size),
                    |doc| doc.save(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("map load");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(BenchmarkId::new("repeated put", size), size, |b, &size| {
            b.iter_batched(
                || repeated_put(size).save(),
                |bytes| Automerge::load(&bytes).unwrap(),
                criterion::BatchSize::LargeInput,
            )
        });
        group.bench_with_input(
            BenchmarkId::new("repeated increment", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || repeated_increment(size).save(),
                    |bytes| Automerge::load(&bytes).unwrap(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || increasing_put(size).save(),
                    |bytes| Automerge::load(&bytes).unwrap(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("decreasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || decreasing_put(size).save(),
                    |bytes| Automerge::load(&bytes).unwrap(),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("map apply");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(BenchmarkId::new("repeated put", size), size, |b, &size| {
            b.iter_batched(
                || {
                    repeated_put(size)
                        .get_changes(&[])
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>()
                },
                |changes| {
                    let mut doc = Automerge::new();
                    doc.apply_changes(changes)
                },
                criterion::BatchSize::LargeInput,
            )
        });
        group.bench_with_input(
            BenchmarkId::new("repeated increment", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        repeated_increment(size)
                            .get_changes(&[])
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>()
                    },
                    |changes| {
                        let mut doc = Automerge::new();
                        doc.apply_changes(changes)
                    },
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        increasing_put(size)
                            .get_changes(&[])
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>()
                    },
                    |changes| {
                        let mut doc = Automerge::new();
                        doc.apply_changes(changes)
                    },
                    criterion::BatchSize::LargeInput,
                )
            },
        );

        group.throughput(criterion::Throughput::Elements(*size));
        group.bench_with_input(
            BenchmarkId::new("decreasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        decreasing_put(size)
                            .get_changes(&[])
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>()
                    },
                    |changes| {
                        let mut doc = Automerge::new();
                        doc.apply_changes(changes)
                    },
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
