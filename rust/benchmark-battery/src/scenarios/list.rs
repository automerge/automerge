use super::SampledBenchmark;
use benchmark_battery::automerge::transaction::Transactable;
use benchmark_battery::automerge::{ReadDoc, ScalarValue, ROOT};
use benchmark_battery::{list_splice_100, rand};
use std::hint::black_box;

const N: u64 = 100_000;

pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("list", "list/list_cursor_now", list_cursor_now),
        SampledBenchmark::no_setup("list", "list/list_cursor_at", list_cursor_at),
        SampledBenchmark::no_setup("list", "list/list_update_at", list_update_at),
        SampledBenchmark::no_setup("list", "list/list_update_now", list_update_now),
        SampledBenchmark::no_setup("list", "list/list_splice_index_now", list_splice_index_now),
        SampledBenchmark::no_setup("list", "list/list_splice_index_at", list_splice_index_at),
    ]
}

fn list_cursor_now() -> Box<dyn FnMut()> {
    let doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move || {
        let pos = rand() % len;
        let cursor = doc.get_cursor(&list, pos, None).unwrap();
        black_box(doc.get_cursor_position(&list, &cursor, None).unwrap());
    })
}

fn list_cursor_at() -> Box<dyn FnMut()> {
    let mut doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    let heads = doc.get_heads();
    let mut tx = doc.transaction();
    tx.splice(
        &list,
        len / 2,
        1,
        vec![
            ScalarValue::from("x"),
            ScalarValue::from("y"),
            ScalarValue::from("z"),
        ],
    )
    .unwrap();
    tx.commit();
    Box::new(move || {
        let pos = rand() % len;
        let cursor = doc.get_cursor(&list, pos, Some(&heads)).unwrap();
        black_box(
            doc.get_cursor_position(&list, &cursor, Some(&heads))
                .unwrap(),
        );
    })
}

fn list_update_at() -> Box<dyn FnMut()> {
    let mut doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    let head = doc.get_heads();
    let mut heads = vec![head];
    Box::new(move || {
        let pos = rand() % len;
        let h = rand() % heads.len();
        let mut tx = doc.transaction_at(&heads[h]).unwrap();
        tx.put(&list, pos, "x").unwrap();
        tx.commit();
        heads.push(doc.get_heads());
    })
}

fn list_update_now() -> Box<dyn FnMut()> {
    let mut doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move || {
        let pos = rand() % len;
        let mut tx = doc.transaction();
        tx.put(&list, pos, "x").unwrap();
        tx.commit();
    })
}

fn list_splice_index_now() -> Box<dyn FnMut()> {
    let mut doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    Box::new(move || {
        let pos = rand() % len;
        let mut tx = doc.transaction();
        tx.insert(&list, pos, "x").unwrap();
        tx.commit();
    })
}

fn list_splice_index_at() -> Box<dyn FnMut()> {
    let mut doc = list_splice_100(N);
    let len = N as usize;
    let (_, list) = doc.get(ROOT, "content").unwrap().unwrap();
    let heads = doc.get_heads();
    let mut tx = doc.transaction();
    tx.splice(
        &list,
        len / 2,
        1,
        vec![
            ScalarValue::from("x"),
            ScalarValue::from("y"),
            ScalarValue::from("z"),
        ],
    )
    .unwrap();
    tx.commit();
    Box::new(move || {
        let pos = rand() % len;
        let mut tx = doc.transaction_at(&heads).unwrap();
        tx.insert(&list, pos, "x").unwrap();
        tx.commit();
    })
}
