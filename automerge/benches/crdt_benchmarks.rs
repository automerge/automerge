use std::{collections::HashMap, convert::TryInto, default::Default};

use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use smol_str::SmolStr;
use unicode_segmentation::UnicodeSegmentation;

pub fn b1_1(c: &mut Criterion) {
    c.bench_function("B1.1 Append N characters", move |b| {
        b.iter_batched(
            || {
                let mut doc1 = Frontend::new();
                let changedoc1 = doc1
                    .change::<_, _, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(Vec::new()),
                        ))?;
                        Ok(())
                    })
                    .unwrap()
                    .1
                    .unwrap();
                let mut backend1 = Backend::new();
                let (patch1, _) = backend1.apply_local_change(changedoc1).unwrap();
                doc1.apply_patch(patch1).unwrap();

                let mut doc2 = Frontend::new();
                let changedoc2 = backend1.get_changes(&[]);
                let mut backend2 = Backend::new();
                let patch2 = backend2
                    .apply_changes(changedoc2.into_iter().cloned().collect())
                    .unwrap();
                doc2.apply_patch(patch2).unwrap();

                let random_string: String = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(6000)
                    .map(char::from)
                    .collect();
                (doc1, backend1, doc2, backend2, random_string)
            },
            |(mut doc1, mut backend1, mut doc2, mut backend2, random_string)| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (index, c) in random_string.chars().enumerate() {
                        let index: u32 = index.try_into().unwrap();
                        let doc1_insert_change = doc1
                            .change::<_, _, InvalidChangeRequest>(None, |d| {
                                d.add_change(LocalChange::insert(
                                    Path::root().key("text").index(index),
                                    c.into(),
                                ))?;
                                Ok(())
                            })
                            .unwrap()
                            .1
                            .unwrap();
                        let (patch, change_to_send) =
                            backend1.apply_local_change(doc1_insert_change).unwrap();
                        doc1.apply_patch(patch).unwrap();

                        let patch2 = backend2
                            .apply_changes(vec![(change_to_send).clone()])
                            .unwrap();
                        doc2.apply_patch(patch2).unwrap()
                    }
                })
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

pub fn b1_2(c: &mut Criterion) {
    c.bench_function("B1.2 Append string of length N", move |b| {
        b.iter_batched(
            || {
                let mut doc1 = Frontend::new();
                let changedoc1 = doc1
                    .change::<_, _, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(Vec::new()),
                        ))?;
                        Ok(())
                    })
                    .unwrap()
                    .1
                    .unwrap();
                let mut backend1 = Backend::new();
                let (patch1, _) = backend1.apply_local_change(changedoc1).unwrap();
                doc1.apply_patch(patch1).unwrap();

                let mut doc2 = Frontend::new();
                let changedoc2 = backend1.get_changes(&[]);
                let mut backend2 = Backend::new();
                let patch2 = backend2
                    .apply_changes(changedoc2.into_iter().cloned().collect())
                    .unwrap();
                doc2.apply_patch(patch2).unwrap();

                let random_string: SmolStr = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(6000)
                    .map(char::from)
                    .collect();
                let chars: Vec<_> = random_string.graphemes(true).map(|s| s.into()).collect();
                let text = Value::Text(chars);
                (doc1, backend1, doc2, backend2, text)
            },
            |(mut doc1, mut backend1, mut doc2, mut backend2, text)| {
                #[allow(clippy::unit_arg)]
                black_box({
                    let doc1_insert_change = doc1
                        .change::<_, _, InvalidChangeRequest>(None, |d| {
                            d.add_change(LocalChange::set(Path::root().key("text"), text))
                        })
                        .unwrap()
                        .1
                        .unwrap();
                    let (patch, change_to_send) =
                        backend1.apply_local_change(doc1_insert_change).unwrap();
                    doc1.apply_patch(patch).unwrap();

                    let patch2 = backend2
                        .apply_changes(vec![change_to_send.clone()])
                        .unwrap();
                    doc2.apply_patch(patch2).unwrap();
                    (doc1, backend1, doc2, backend2)
                })
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

pub fn b3_1(c: &mut Criterion) {
    c.bench_function("B1.3 20√N clients concurrently set number in Map", |b| {
        b.iter_batched(
            || {
                let n: f64 = 6000.0;
                let root_n: i64 = n.sqrt().floor() as i64;
                let mut local_doc = Frontend::new();
                let mut local_backend = Backend::new();
                let init_change = local_doc
                    .change::<_, _, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("map"),
                            Value::Map(HashMap::new()),
                        ))
                    })
                    .unwrap()
                    .1
                    .unwrap();
                let (patch, init_binchange) =
                    local_backend.apply_local_change(init_change).unwrap();
                local_doc.apply_patch(patch).unwrap();

                let other_docs = (1..root_n).map(|_| Frontend::new());
                let updates: Vec<automerge_backend::Change> = other_docs
                    .enumerate()
                    .map(|(index, mut doc)| {
                        let mut backend = Backend::new();
                        let patch = backend.apply_changes(vec![init_binchange.clone()]).unwrap();
                        doc.apply_patch(patch).unwrap();
                        let change = doc
                            .change(None, |d| {
                                d.add_change(LocalChange::set(
                                    Path::root().key("map").key("v"),
                                    Value::Primitive(Primitive::Int(index as i64 + 1)),
                                ))
                            })
                            .unwrap()
                            .1
                            .unwrap();
                        backend.apply_local_change(change).unwrap().1.clone()
                    })
                    .collect();
                (local_doc, local_backend, updates)
            },
            |(mut local_doc, mut local_backend, updates)| {
                let patch = local_backend.apply_changes(updates).unwrap();
                local_doc.apply_patch(patch)
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = b1_1, b1_2, b3_1
}
criterion_main!(benches);
