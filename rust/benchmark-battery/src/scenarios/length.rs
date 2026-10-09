use super::SampledBenchmark;
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{Automerge, ReadDoc, ROOT};
use benchmark_battery::{list_splice_100, rand, text_splice_100};
use std::hint::black_box;

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("length", "length/text_len_now", text_len_now),
        SampledBenchmark::no_setup("length", "length/list_len_now", list_len_now),
        SampledBenchmark::no_setup("length", "length/text_len_at", text_len_at),
        SampledBenchmark::no_setup("length", "length/map_len_now", map_len_now),
        SampledBenchmark::no_setup("length", "length/map_len_at", map_len_at),
    ]
}

fn text_len_now() -> Box<dyn FnMut()> {
    let doc = text_splice_100(N);
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move || {
        black_box(doc.length(&text));
    })
}

fn list_len_now() -> Box<dyn FnMut()> {
    let doc = list_splice_100(N);
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move || {
        black_box(doc.length(&list));
    })
}

fn text_len_at() -> Box<dyn FnMut()> {
    let mut doc = text_splice_100(N);
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
    let heads = doc.get_heads();
    let mut tx = doc.transaction();
    let pos = rand() % (N as usize - 10);
    tx.splice_text(&text, pos, 9, "01234567890").unwrap();
    tx.commit();
    Box::new(move || {
        black_box(doc.length_at(&text, &heads));
    })
}

fn map_doc() -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    for i in 0..N {
        tx.put(&ROOT, format!("key{}", i), format!("value{}", i))
            .unwrap();
    }
    tx.commit();
    doc
}

fn map_len_now() -> Box<dyn FnMut()> {
    let doc = map_doc();
    Box::new(move || {
        black_box(doc.length(&ROOT));
    })
}

fn map_len_at() -> Box<dyn FnMut()> {
    let mut doc = map_doc();
    let heads = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(&ROOT, "next", "value").unwrap();
    tx.commit();
    Box::new(move || {
        black_box(doc.length_at(&ROOT, &heads));
    })
}
