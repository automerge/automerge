use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn small_change_backend() -> Backend {
    let mut frontend = Frontend::new();
    let mut backend = Backend::new();
    let (_, change) = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("a"),
                Value::Primitive(Primitive::Str("hello world".to_owned())),
            ))?;
            Ok(())
        })
        .unwrap();
    backend.apply_local_change(change.unwrap()).unwrap();
    backend
}

fn medium_change_backend() -> Backend {
    let mut change1s = Vec::new();
    let mut change2s = Vec::new();

    let actor_id = uuid::Uuid::new_v4();

    let changes1 = vec![LocalChange::set(
        Path::root(),
        Value::Map(
            vec![
                (
                    "\u{0}\u{0}".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::F64(0.0)),
                    ]),
                ),
                (
                    "\u{2}".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                    ]),
                ),
                (
                    "\u{0}".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F32(0.0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::F32(0.0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                    ]),
                ),
                (
                    "".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F32(0.0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Boolean(false)),
                    ]),
                ),
                (
                    "\u{1}".to_owned(),
                    Value::Table(
                        vec![("".to_owned(), Value::Primitive(Primitive::F64(0.0)))]
                            .into_iter()
                            .collect(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        ),
    )];
    let changes2 = vec![
        LocalChange::delete(Path::root().key("\u{0}\u{0}")),
        LocalChange::delete(Path::root().key("\u{2}")),
        LocalChange::delete(Path::root().key("\u{0}")),
        LocalChange::delete(Path::root().key("")),
        LocalChange::delete(Path::root().key("\u{1}")),
    ];

    let mut backend = Backend::new();
    let mut frontend = Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id);
    let patch = backend.get_patch().unwrap();
    frontend.apply_patch(patch).unwrap();

    let c = frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            for change in &changes1 {
                d.add_change(change.clone())?
            }
            Ok(())
        })
        .unwrap();
    if let (_, Some(change)) = c {
        change1s.push(change.clone());
        backend.apply_local_change(change).unwrap();
    }

    let mut frontend = Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id);
    let patch = backend.get_patch().unwrap();
    frontend.apply_patch(patch).unwrap();

    let c = frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            for change in &changes2 {
                d.add_change(change.clone())?
            }
            Ok(())
        })
        .unwrap();
    if let (_, Some(change)) = c {
        change2s.push(change.clone());
        backend.apply_local_change(change).unwrap();
    }
    backend
}

fn save_empty(c: &mut Criterion) {
    c.bench_function("save an empty backend", |b| {
        b.iter_batched(
            Backend::new,
            |b| black_box(b.save().unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn save_small(c: &mut Criterion) {
    c.bench_function("save a small history backend", |b| {
        b.iter_batched(
            small_change_backend,
            |b| black_box(b.save().unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn save_medium(c: &mut Criterion) {
    c.bench_function("save a medium history backend", |b| {
        b.iter_batched(
            medium_change_backend,
            |b| black_box(b.save().unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn load_empty(c: &mut Criterion) {
    c.bench_function("load an empty backend", |b| {
        b.iter_batched(
            || Backend::new().save().unwrap(),
            |v| black_box(Backend::load(v).unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn load_small(c: &mut Criterion) {
    c.bench_function("load a small history backend", |b| {
        b.iter_batched(
            || {
                let backend = small_change_backend();
                backend.save().unwrap()
            },
            |v| black_box(Backend::load(v).unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn load_medium(c: &mut Criterion) {
    c.bench_function("load a medium history backend", |b| {
        b.iter_batched(
            || {
                let backend = medium_change_backend();
                backend.save().unwrap()
            },
            |v| black_box(Backend::load(v).unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = save_empty, save_small, save_medium, load_empty, load_small, load_medium
}
criterion_main!(benches);
