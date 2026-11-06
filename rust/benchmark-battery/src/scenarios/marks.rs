use super::SampledBenchmark;
use benchmark_battery::automerge::iter::Span;
use benchmark_battery::automerge::marks::{ExpandMark, Mark, MarkSet, UpdateSpansConfig};
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{
    AutoCommit, Automerge, ObjId, ObjType, ReadDoc, ScalarValue, ROOT,
};
use benchmark_battery::{rand, text_splice_100};
use std::cmp::{max, min};
use std::hint::black_box;
use std::sync::Arc;

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("marks", "marks/add_mark", add_mark),
        SampledBenchmark::no_setup("marks", "marks/get_marks_at_end", get_marks_at_end),
        splice_with_marks(),
        splice_without_marks(),
        update_spans_existing(1_000),
        update_spans_fresh(1_000),
    ]
}

/// `runs` text spans alternating bold and a link with a distinct href, so the
/// text holds about `runs` marks.
fn marked_spans(runs: usize) -> Vec<Span> {
    (0..runs)
        .map(|i| {
            let marks: MarkSet = if i % 2 == 0 {
                [("bold".to_string(), ScalarValue::from(true))]
                    .into_iter()
                    .collect()
            } else {
                [(
                    "link".to_string(),
                    ScalarValue::from(format!("https://example.org/{i}")),
                )]
                .into_iter()
                .collect()
            };
            Span::Text {
                text: format!("run {i} "),
                marks: Some(Arc::new(marks)),
            }
        })
        .collect()
}

fn marked_text(runs: usize) -> (AutoCommit, ObjId) {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.update_spans(&text, UpdateSpansConfig::default(), marked_spans(runs))
        .unwrap();
    doc.commit();
    (doc, text)
}

/// Rewrites a marked text with one unmarked run appended: most marks are
/// unchanged, as when an editor writes back a document it read.
fn update_spans_existing(runs: usize) -> SampledBenchmark {
    let mut edited = marked_spans(runs);
    edited.push(Span::Text {
        text: "appended".into(),
        marks: None,
    });
    SampledBenchmark::batched(
        "marks",
        "marks/update_spans_existing",
        move || marked_text(runs),
        move |(mut doc, text)| {
            doc.update_spans(&text, UpdateSpansConfig::default(), edited.clone())
                .unwrap();
            (doc, text)
        },
    )
}

/// Writes marked spans into an empty text.
fn update_spans_fresh(runs: usize) -> SampledBenchmark {
    let spans = marked_spans(runs);
    SampledBenchmark::batched(
        "marks",
        "marks/update_spans_fresh",
        || {
            let mut doc = AutoCommit::new();
            let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
            (doc, text)
        },
        move |(mut doc, text)| {
            doc.update_spans(&text, UpdateSpansConfig::default(), spans.clone())
                .unwrap();
            (doc, text)
        },
    )
}

fn get_marks_at_end() -> Box<dyn FnMut()> {
    let (doc, text) = doc_and_text(10);
    let index = doc.length(&text) - 1;
    Box::new(move || {
        black_box(doc.get_marks(&text, index, None).unwrap());
    })
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
