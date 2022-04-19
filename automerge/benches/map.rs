use automerge::{transaction::Transactable, Automerge, ROOT};
use criterion::{criterion_group, criterion_main, Criterion};

fn query_single(doc: &Automerge, rounds: u32) {
    for _ in 0..rounds {
        // repeatedly get the last key
        doc.get(ROOT, (rounds - 1).to_string()).unwrap();
    }
}

fn query_range(doc: &Automerge, rounds: u32) {
    for i in 0..rounds {
        doc.get(ROOT, i.to_string()).unwrap();
    }
}

fn put_doc(doc: &mut Automerge, rounds: u32) {
    for i in 0..rounds {
        let mut tx = doc.transaction();
        tx.put(ROOT, i.to_string(), "value").unwrap();
        tx.commit();
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("map");

    let rounds = 10_000;
    let mut doc = Automerge::new();
    put_doc(&mut doc, rounds);

    group.bench_function("query single", |b| b.iter(|| query_single(&doc, rounds)));

    group.bench_function("query range", |b| b.iter(|| query_range(&doc, rounds)));

    group.bench_function("put", |b| {
        b.iter_batched(
            Automerge::new,
            |mut doc| put_doc(&mut doc, rounds),
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
