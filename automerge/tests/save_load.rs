use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorId, ElementId, Key, ObjType, ObjectId, Op, OpId, OpType, ScalarValue,
};
use test_env_log::test;

#[test]
fn missing_object_error_flaky_null_rle_decoding() {
    let mut change1s = Vec::new();
    let mut change2s = Vec::new();

    let actor_id = uuid::Uuid::new_v4();

    let changes1 = vec![LocalChange::set(
        Path::root(),
        Value::Map(
            vec![
                (
                    "\u{0}\u{0}".into(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Str("".into())),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".into())),
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
                    "\u{2}".into(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".into())),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".into())),
                    ]),
                ),
                (
                    "\u{0}".into(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".into())),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".into())),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".to_owned())),
                    ]),
                ),
                (
                    "".into(),
                    Value::Sequence(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Int(0)),
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::F64(0.0)),
                        Value::Primitive(Primitive::Timestamp(0)),
                        Value::Primitive(Primitive::Str("".into())),
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
                    "\u{1}".into(),
                    Value::Table(
                        vec![("".into(), Value::Primitive(Primitive::F64(0.0)))]
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
        .unwrap()
        .1;
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
                .change::<_, _, InvalidChangeRequest>(None, |d| {
                    for change in &changes2 {
                        d.add_change(change.clone())?
                    }
                    Ok(())
                })
                .unwrap()
                .1;
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

#[test]
fn missing_object_error_null_rle_decoding() {
    let actor_uuid = uuid::Uuid::new_v4();
    let actor_id = ActorId::from_bytes(actor_uuid.as_bytes());

    let raw_change = amp::Change {
        operations: vec![
            Op {
                action: OpType::Make(ObjType::List),
                obj: ObjectId::Root,
                key: Key::Map("b".into()),
                pred: vec![].into(),
                insert: false,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Head),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(2, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(3, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(4, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(5, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(6, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(7, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(8, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(9, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(10, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(11, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(12, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(13, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(14, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(1, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(15, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Make(ObjType::List),
                obj: ObjectId::Root,
                key: Key::Map("\u{0}".into()),
                pred: vec![].into(),
                insert: false,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Head),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(18, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(19, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(20, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(21, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(22, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(23, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(24, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(25, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(26, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(27, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(28, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(29, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(30, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(31, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(32, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(33, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(34, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(35, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(36, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(37, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(38, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(39, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(40, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(41, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(42, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(43, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(44, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(45, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(46, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(47, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(48, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(49, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(50, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(51, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(52, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(53, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(54, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(55, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(56, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(57, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(58, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(59, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(60, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(61, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(62, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(63, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(64, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(17, actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(65, actor_id.clone()))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Make(ObjType::Map),
                obj: ObjectId::Root,
                key: Key::Map("\u{1}".into()),
                pred: vec![].into(),
                insert: false,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(67, actor_id.clone())),
                key: Key::Map("a".into()),
                pred: vec![].into(),
                insert: false,
            },
        ],
        actor_id,
        hash: None,
        seq: 1,
        start_op: 1,
        time: 0,
        message: Some("".into()),
        deps: vec![],
        extra_bytes: vec![],
    };

    let mut backend = Backend::new();
    backend.apply_local_change(raw_change).unwrap();

    let backend_bytes = backend.save().unwrap();
    println!("{:?}", backend_bytes);

    let backend = Backend::load(backend_bytes);
    if let Err(e) = backend {
        panic!("failed loading backend: {:?}", e)
    }
}
