use std::num::NonZeroU64;

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
                    Value::List(vec![
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
                    Value::List(vec![
                        Value::Primitive(Primitive::Null),
                        Value::Primitive(Primitive::Uint(0)),
                        Value::Primitive(Primitive::Str("".into())),
                        Value::Primitive(Primitive::Counter(0)),
                        Value::Primitive(Primitive::Str("".into())),
                    ]),
                ),
                (
                    "\u{0}".into(),
                    Value::List(vec![
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
                        Value::Primitive(Primitive::Str("".into())),
                    ]),
                ),
                (
                    "".into(),
                    Value::List(vec![
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
    let mut frontend =
        Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id.as_bytes());
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
                Frontend::new_with_timestamper_and_actor_id(Box::new(|| None), actor_id.as_bytes());
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
    let actor_id = ActorId::from(actor_uuid);

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
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Head),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(2).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(3).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(4).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(5).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(6).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(7).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(8).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(9).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(10).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(11).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(12).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(13).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(14).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(1).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(15).unwrap(),
                    actor_id.clone(),
                ))),
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
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Head),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(18).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(19).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(20).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(21).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(22).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(23).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(24).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(25).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(26).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(27).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(28).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(29).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(30).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(31).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(32).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(33).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(34).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(35).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(36).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(37).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(38).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(39).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(40).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(41).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(42).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(43).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(44).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(45).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(46).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(47).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(48).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(49).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(50).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(51).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(52).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(53).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(54).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(55).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(56).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(57).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(58).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(59).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(60).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(61).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(62).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(63).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(64).unwrap(),
                    actor_id.clone(),
                ))),
                pred: vec![].into(),
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: ObjectId::Id(OpId(NonZeroU64::new(17).unwrap(), actor_id.clone())),
                key: Key::Seq(ElementId::Id(OpId(
                    NonZeroU64::new(65).unwrap(),
                    actor_id.clone(),
                ))),
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
                obj: ObjectId::Id(OpId(NonZeroU64::new(67).unwrap(), actor_id.clone())),
                key: Key::Map("a".into()),
                pred: vec![].into(),
                insert: false,
            },
        ],
        actor_id,
        hash: None,
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
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
