use super::SampledBenchmark;
use benchmark_battery::automerge::marks::{ExpandMark, Mark};
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{Automerge, ObjId, ReadDoc, ROOT};
use benchmark_battery::{rand, text_splice_100};
use std::cmp::{max, min};

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("marks", "marks/add_mark", add_mark),
        splice_with_marks(),
        splice_without_marks(),
    ]
}

fn add_mark() -> Box<dyn FnMut()> {
    let mut doc = text_splice_100(N);
    Box::new(move || {
        add_random_marks(&mut doc, 1);
    })
}

fn add_random_marks(d: &mut Automerge, num: usize) {
    let (_, text) = d.get(ROOT, "content").unwrap().unwrap();
    let mut tx = d.transaction();
    let len = tx.length(&text);
    for i in 0..num {
        let a = rand() % len;
        let b = rand() % len;
        let mark = Mark::new(format!("mark{}", i), true, min(a, b), max(a, b));
        tx.mark(&text, mark, ExpandMark::Both).unwrap();
    }
    tx.commit();
}

fn splice_with_marks() -> SampledBenchmark {
    SampledBenchmark::batched(
        "marks",
        "marks/splice_with_marks",
        || doc_and_text(1_000),
        |(mut doc, text)| {
            for _ in 0..1000 {
                let mut tx = doc.transaction();
                let pos = rand() % tx.length(&text);
                tx.splice_text(&text, pos, 0, "XXX").unwrap();
                tx.commit();
            }
            (doc, text)
        },
    )
}

fn splice_without_marks() -> SampledBenchmark {
    SampledBenchmark::batched(
        "marks",
        "marks/splice_without_marks",
        || doc_and_text(0),
        |(mut doc, text)| {
            for _ in 0..1000 {
                let mut tx = doc.transaction();
                let pos = rand() % tx.length(&text);
                tx.splice_text(&text, pos, 1, "XXX").unwrap();
                tx.commit();
            }
            (doc, text)
        },
    )
}

fn doc_and_text(marks: usize) -> (Automerge, ObjId) {
    let mut doc = text_splice_100(N);
    add_random_marks(&mut doc, marks);
    let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
    (doc, text)
}
