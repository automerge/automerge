use automerge::{
    sync::{self, SyncDoc},
    transaction::Transactable,
    Automerge, ROOT,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

#[derive(Default)]
struct DocWithSync {
    doc: Automerge,
    peer_state: sync::State,
}

impl From<Automerge> for DocWithSync {
    fn from(doc: Automerge) -> Self {
        Self {
            doc,
            peer_state: sync::State::default(),
        }
    }
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

// keep syncing until doc1 no longer generates a sync message for doc2.
fn sync(doc1: &mut DocWithSync, doc2: &mut DocWithSync) {
    while let Some(message1) = doc1.doc.generate_sync_message(&mut doc1.peer_state) {
        doc2.doc
            .receive_sync_message(&mut doc2.peer_state, message1)
            .unwrap();

        if let Some(message2) = doc2.doc.generate_sync_message(&mut doc2.peer_state) {
            doc1.doc
                .receive_sync_message(&mut doc1.peer_state, message2)
                .unwrap()
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = [100, 1_000, 10_000];

    let mut group = c.benchmark_group("sync unidirectional");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));

        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || (increasing_put(size), DocWithSync::default()),
                    |(doc1, mut doc2)| sync(&mut doc1.into(), &mut doc2),
                    criterion::BatchSize::LargeInput,
                )
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("sync unidirectional every change");
    for size in &sizes {
        group.throughput(criterion::Throughput::Elements(*size));

        group.bench_with_input(
            BenchmarkId::new("increasing put", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut doc1 = DocWithSync::default();
                    let mut doc2 = DocWithSync::default();

                    for i in 0..size {
                        let mut tx = doc1.doc.transaction();
                        tx.put(ROOT, i.to_string(), i).unwrap();
                        tx.commit();
                        sync(&mut doc1, &mut doc2);
                    }
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
