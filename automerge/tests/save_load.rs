use automerge::Backend;
use automerge::Frontend;
use automerge::LocalChange;
use automerge::MapType;
use automerge::Path;
use automerge::Value;
use automerge::{InvalidChangeRequest, Primitive};

#[test]
fn missing_object_error() {
    let mut change1s = Vec::new();
    let mut change2s = Vec::new();

    let actor_id = uuid::Uuid::new_v4();

    for _ in 0..100 {
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
                        Value::Map(
                            vec![("".to_owned(), Value::Primitive(Primitive::F64(0.0)))]
                                .into_iter()
                                .collect(),
                            MapType::Table,
                        ),
                    ),
                ]
                .into_iter()
                .collect(),
                MapType::Map,
            ),
        )];
        let changes2 = vec![
            LocalChange::delete(Path::root().key("\u{0}\u{0}")),
            LocalChange::delete(Path::root().key("\u{2}")),
            LocalChange::delete(Path::root().key("\u{0}")),
            LocalChange::delete(Path::root().key("")),
            LocalChange::delete(Path::root().key("\u{1}")),
        ];

        let mut backend = Backend::init();
        let mut frontend = Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id);
        let patch = backend.get_patch().unwrap();
        frontend.apply_patch(patch).unwrap();

        let c = frontend
            .change::<_, InvalidChangeRequest>(None, |d| {
                for change in &changes1 {
                    d.add_change(change.clone())?
                }
                Ok(())
            })
            .unwrap();
        if let Some(change) = c {
            change1s.push(change.clone());
            backend.apply_local_change(change).unwrap();
        }
        if change1s.len() >= 2 {
            println!(
                "{}",
                pretty_assertions::Comparison::new(
                    &change1s[change1s.len() - 2],
                    &change1s[change1s.len() - 1],
                )
            )
        }

        let backend_bytes = backend.save().unwrap();
        println!("{:?}", backend_bytes);

        let backend = Backend::load(backend_bytes);
        match backend {
            Err(e) => {
                panic!("failed loading backend: {:?}", e)
            }
            Ok(mut backend) => {
                let mut frontend =
                    Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id);
                let patch = backend.get_patch().unwrap();
                frontend.apply_patch(patch).unwrap();

                let c = frontend
                    .change::<_, InvalidChangeRequest>(None, |d| {
                        for change in &changes2 {
                            d.add_change(change.clone())?
                        }
                        Ok(())
                    })
                    .unwrap();
                if let Some(change) = c {
                    change2s.push(change.clone());
                    if change2s.len() >= 2 {
                        println!(
                            "{}",
                            pretty_assertions::Comparison::new(
                                &change2s[change2s.len() - 2],
                                &change2s[change2s.len() - 1]
                            )
                        )
                    }
                    backend.apply_local_change(change).unwrap();
                }
            }
        }
    }
}

#[test]
fn missing_object_error_2() {
    let actor_id = uuid::Uuid::new_v4();

    let changes1 = vec![LocalChange::set(
        Path::root(),
        Value::Map(
            vec![
                (
                    "\u{0}".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::F32(0.0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::F32(0.0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::F64(0.0)),
                    ]),
                ),
                (
                    "".to_owned(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Boolean(false)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                        Value::Primitive(Primitive::Str("".to_owned())),
                    ]),
                ),
                (
                    "\u{1}".to_owned(),
                    Value::Map(
                        vec![("".to_owned(), Value::Primitive(Primitive::Null))]
                            .into_iter()
                            .collect(),
                        MapType::Map,
                    ),
                ),
            ]
            .into_iter()
            .collect(),
            MapType::Map,
        ),
    )];

    let mut backend = Backend::init();
    let mut frontend = Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id);
    let patch = backend.get_patch().unwrap();
    frontend.apply_patch(patch).unwrap();

    let c = frontend
        .change::<_, InvalidChangeRequest>(None, |d| {
            for change in &changes1 {
                d.add_change(change.clone())?
            }
            Ok(())
        })
        .unwrap();
    if let Some(change) = c {
        backend.apply_local_change(change).unwrap();
    }

    let backend_bytes = backend.save().unwrap();
    println!("{:?}", backend_bytes);

    let backend = Backend::load(backend_bytes);
    if let Err(e) = backend {
        panic!("failed loading backend: {:?}", e)
    }
}
