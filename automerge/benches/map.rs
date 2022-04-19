use automerge::{transaction::Transactable, Automerge, ROOT};
use criterion::{criterion_group, criterion_main, Criterion};

fn query_doc(doc: &Automerge, key: &str, rounds: u32) {
    for _ in 0..rounds {
        doc.get(ROOT, key).unwrap();
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("map");

    let rounds = 10_000;
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..rounds {
        tx.put(ROOT, i.to_string(), vec![0, 1, 2, 3, 4, 5]).unwrap();
    }
    tx.commit();

    group.bench_function("query", |b| {
        b.iter(|| query_doc(&doc, &(rounds - 1).to_string(), rounds))
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
