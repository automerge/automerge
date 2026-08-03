use super::{Benchmark, SampledBenchmark};
use benchmark_battery::automerge::{
    transaction::Transactable, Automerge, Change, ScalarValue, ROOT,
};
use std::hint::black_box;

const SIZES: [u64; 3] = [100, 1_000, 10_000];
pub fn benchmarks() -> Vec<Benchmark> {
    let mut benchmarks = Vec::new();
    for n in SIZES {
        add_case(
            &mut benchmarks,
            "repeated_increment",
            n,
            repeated_increment_doc,
        );
        add_case(&mut benchmarks, "repeated_put", n, repeated_put_doc);
        add_case(&mut benchmarks, "increasing_put", n, increasing_put_doc);
        add_case(&mut benchmarks, "decreasing_put", n, decreasing_put_doc);

        benchmarks.push(
            SampledBenchmark::no_setup("map", name("map", "deep_history", n), move || {
                build_setup(n, deep_history_doc)
            })
            .into(),
        );
    }
    benchmarks
}

type DocBuilder = fn(u64) -> Automerge;

fn add_case(benchmarks: &mut Vec<Benchmark>, operation: &'static str, n: u64, builder: DocBuilder) {
    benchmarks.extend([
        SampledBenchmark::no_setup("map", name("map", operation, n), move || {
            build_setup(n, builder)
        })
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/save", operation, n),
            move || builder(n),
            save_doc,
        )
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/load", operation, n),
            move || builder(n).save(),
            load_doc,
        )
        .into(),
        SampledBenchmark::batched(
            "map",
            name("map/apply", operation, n),
            move || owned_changes(&builder(n)),
            apply_changes,
        )
        .into(),
    ]);
}

fn name(group: &str, operation: &str, n: u64) -> &'static str {
    Box::leak(format!("{group}/{operation}/{n}").into_boxed_str())
}

fn build_setup(n: u64, builder: DocBuilder) -> Box<dyn FnMut()> {
    Box::new(move || {
        black_box(builder(n));
    })
}

fn save_doc(doc: Automerge) -> (Automerge, Vec<u8>) {
    let bytes = doc.save();
    (doc, bytes)
}

fn load_doc(bytes: Vec<u8>) -> (Vec<u8>, Automerge) {
    let doc = Automerge::load(&bytes).unwrap();
    (bytes, doc)
}

fn apply_changes(changes: Vec<Change>) -> Automerge {
    let mut doc = Automerge::new();
    doc.apply_changes(changes).unwrap();
    doc
}

fn owned_changes(doc: &Automerge) -> Vec<Change> {
    doc.get_changes(&[]).unwrap()
}

fn repeated_increment_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", ScalarValue::counter(0)).unwrap();
    for _ in 0..n {
        tx.increment(ROOT, "counter", 1).unwrap();
    }
    tx.commit();
    doc
}

fn repeated_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, "0", i).unwrap();
    }
    tx.commit();
    doc
}

fn increasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
    tx.commit();
    doc
}

fn decreasing_put_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in (0..n).rev() {
        tx.put(ROOT, i.to_string(), i).unwrap();
    }
    tx.commit();
    doc
}

fn deep_history_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    for i in 0..n {
        let mut tx = doc.transaction();
        tx.put(ROOT, "x", i.to_string()).unwrap();
        tx.put(ROOT, "y", i.to_string()).unwrap();
        tx.commit();
    }
    doc
}
