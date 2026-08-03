use super::SampledBenchmark;
use benchmark_battery::automerge::{transaction::Transactable, Automerge, ReadDoc, ROOT};
use std::hint::black_box;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("range", "range/range/10000", range_10000),
        SampledBenchmark::no_setup("range", "range/range_at/10000", range_at_10000),
        SampledBenchmark::no_setup("range", "range/range/100000", range_100000),
        SampledBenchmark::no_setup("range", "range/range_at/100000", range_at_100000),
    ]
}

fn doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..n {
        tx.put(ROOT, i.to_string(), i.to_string()).unwrap();
    }
    tx.commit();
    doc
}

fn range_10000() -> Box<dyn FnMut()> {
    range(10_000)
}

fn range_at_10000() -> Box<dyn FnMut()> {
    range_at(10_000)
}

fn range_100000() -> Box<dyn FnMut()> {
    range(100_000)
}

fn range_at_100000() -> Box<dyn FnMut()> {
    range_at(100_000)
}

fn range(n: u64) -> Box<dyn FnMut()> {
    let doc = doc(n);
    Box::new(move || {
        black_box(&doc).values(ROOT).for_each(drop);
    })
}

fn range_at(n: u64) -> Box<dyn FnMut()> {
    let doc = doc(n);
    let heads = doc.get_heads();
    Box::new(move || {
        black_box(&doc).values_at(ROOT, &heads).unwrap().for_each(drop);
    })
}
