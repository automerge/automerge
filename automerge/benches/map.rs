use automerge::{transaction::Transactable, Automerge, ScalarValue, ROOT};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

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
    let small = 1_000;

    c.bench_function(&format!("repeated increment {}", small), |b| {
        b.iter(|| repeated_increment(black_box(small)))
    });

    c.bench_function(&format!("repeated put {}", small), |b| {
        b.iter(|| repeated_put(black_box(small)))
    });

    c.bench_function(&format!("increasing put {}", small), |b| {
        b.iter(|| increasing_put(black_box(small)))
    });

    c.bench_function(&format!("decreasing put {}", small), |b| {
        b.iter(|| decreasing_put(black_box(small)))
    });

    let large = 10_000;

    c.bench_function(&format!("repeated increment {}", large), |b| {
        b.iter(|| repeated_increment(black_box(large)))
    });

    c.bench_function(&format!("repeated put {}", large), |b| {
        b.iter(|| repeated_put(black_box(large)))
    });

    c.bench_function(&format!("increasing put {}", large), |b| {
        b.iter(|| increasing_put(black_box(large)))
    });

    c.bench_function(&format!("decreasing put {}", large), |b| {
        b.iter(|| decreasing_put(black_box(large)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
