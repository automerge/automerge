use automerge::{transaction::Transactable, Automerge, ReadDoc, ROOT};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, i.to_string(), i.to_string()).unwrap();
    }
    tx.commit();
    doc
}

fn range(doc: &Automerge) {
    let range = doc.values(ROOT);
    range.for_each(drop);
}

fn range_at(doc: &Automerge) {
    let range = doc.values_at(ROOT, &doc.get_heads());
    range.for_each(drop);
}

fn criterion_benchmark(c: &mut Criterion) {
    let n = 100_000;
    let doc = doc(n);
    c.bench_function(&format!("range {}", n), |b| {
        b.iter(|| range(black_box(&doc)))
    });
    c.bench_function(&format!("range_at {}", n), |b| {
        b.iter(|| range_at(black_box(&doc)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
