use automerge::{Backend, Primitive};
use automerge_frontend::{Frontend, InvalidChangeRequest, LocalChange, Path, Value};
use automerge_protocol::MapType;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use maplit::hashmap;
use rand::{thread_rng, Rng};
use unicode_segmentation::UnicodeSegmentation;

fn apply_small_patch(c: &mut Criterion) {
    c.bench_function("Frontend::apply_patch, small patch", |b| {
        b.iter_batched(
            || {
                let mut doc = Frontend::new();
                let random_string: String = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(1000)
                    .map(char::from)
                    .collect();
                let change = doc
                    .change::<_, _, InvalidChangeRequest>(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(
                                random_string
                                    .graphemes(true)
                                    .map(|s| s.to_owned())
                                    .collect(),
                            ),
                        ))
                    })
                    .unwrap()
                    .1
                    .unwrap();
                let mut backend = Backend::new();
                let (patch, _) = backend.apply_local_change(change).unwrap();

                (Frontend::new(), patch)
            },
            |(mut doc, patch)| {
                #[allow(clippy::unit_arg)]
                black_box(doc.apply_patch(patch).unwrap())
            },
            BatchSize::SmallInput,
        )
    });
}

fn apply_small_patch_many_changes(c: &mut Criterion) {
    c.bench_function("Frontend::apply_patch, small patch, many changes", |b| {
        b.iter_batched(
            || {
                let mut doc = Frontend::new();
                let random_string: String = thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(1000)
                    .map(char::from)
                    .collect();
                let mut backend = Backend::new();
                let change = doc
                    .change(None, |d| {
                        d.add_change(LocalChange::set(
                            Path::root().key("text"),
                            Value::Text(vec![]),
                        ))
                    })
                    .unwrap()
                    .1
                    .unwrap();
                let (patch, _) = backend.apply_local_change(change).unwrap();
                doc.apply_patch(patch).unwrap();

                for (i, c) in random_string.graphemes(true).enumerate() {
                    let change = doc
                        .change::<_, _, InvalidChangeRequest>(None, |d| {
                            d.add_change(LocalChange::insert(
                                Path::root().key("text").index(i as u32),
                                Value::Primitive(Primitive::Str(c.to_owned())),
                            ))
                        })
                        .unwrap()
                        .1
                        .unwrap();
                    let (patch, _) = backend.apply_local_change(change).unwrap();
                    doc.apply_patch(patch).unwrap();
                }
                let patch = backend.get_patch().unwrap();

                (Frontend::new(), patch)
            },
            |(mut doc, patch)| {
                #[allow(clippy::unit_arg)]
                black_box(doc.apply_patch(patch).unwrap())
            },
            BatchSize::SmallInput,
        )
    });
}

fn apply_patch_nested_maps(c: &mut Criterion) {
    c.bench_function("Frontend::apply_patch, nested maps", |b| {
        b.iter_batched(
            || {
                let mut doc = Frontend::new();
                let mut backend = Backend::new();

                let m = hashmap! {
                    "a".to_owned() =>
                    Value::Map(hashmap!{
                        "b".to_owned()=>
                        Value::Map(
                            hashmap! {
                                "abc".to_owned() => Value::Primitive(Primitive::Str("hello world".to_owned()))
                            },
                            MapType::Map,
                        ),
                        "d".to_owned() => Value::Primitive(Primitive::Uint(20)),
                    },MapType::Map)
                };

                for _ in 0..400 {
                    let random_string: String = thread_rng()
                        .sample_iter(&rand::distributions::Alphanumeric)
                        .take(10)
                        .map(char::from)
                        .collect();
                    let change = doc
                        .change::<_,_, InvalidChangeRequest>(None, |d| {
                            d.add_change(LocalChange::set(
                                Path::root().key(random_string),
                                Value::Map(m.clone(), MapType::Map),
                            ))
                        })
                        .unwrap().1
                        .unwrap();
                    let (patch, _) = backend.apply_local_change(change).unwrap();
                    doc.apply_patch(patch).unwrap();
                }

                let patch = backend.get_patch().unwrap();

                (Frontend::new(), patch)
            },
            |(mut doc, patch)| {
                #[allow(clippy::unit_arg)]
                black_box(doc.apply_patch(patch).unwrap())
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = apply_small_patch, apply_small_patch_many_changes, apply_patch_nested_maps
}
criterion_main!(benches);
