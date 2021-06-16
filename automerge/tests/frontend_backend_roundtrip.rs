use std::collections::HashMap;

use automerge::{
    Backend, InvalidChangeRequest, LocalChange, ObjType, Path, Primitive, ScalarValue,
    SequenceType, Value,
};
use automerge_protocol as amp;
use automerge_protocol::{ActorId, ElementId, Key, ObjectId, Op, OpType};
use maplit::hashmap;
use pretty_assertions::assert_eq;
use test_env_log::test;

#[test]
fn test_frontend_uses_correct_elem_ids() {
    let mut hm = HashMap::new();
    hm.insert(
        "a".to_owned(),
        automerge::Value::Sequence(vec![automerge::Value::Primitive(Primitive::Null)]),
    );
    let mut backend = automerge::Backend::new();

    let (mut frontend, change) =
        automerge::Frontend::new_with_initial_state(Value::Map(hm)).unwrap();

    println!("change1 {:?}", change);

    let (patch, _) = backend.apply_local_change(change).unwrap();
    frontend.apply_patch(patch).unwrap();

    let ((), c) = frontend
        .change::<_, _, automerge::InvalidChangeRequest>(None, |d| {
            d.add_change(automerge::LocalChange::set(
                automerge::Path::root().key("a").index(0),
                automerge::Value::Primitive(automerge::Primitive::Int(0)),
            ))
            .unwrap();
            d.add_change(automerge::LocalChange::insert(
                automerge::Path::root().key("a").index(1),
                automerge::Value::Primitive(automerge::Primitive::Boolean(false)),
            ))
            .unwrap();
            Ok(())
        })
        .unwrap();

    let mut ehm = HashMap::new();
    ehm.insert(
        "a".to_owned(),
        automerge::Value::Sequence(vec![
            automerge::Value::Primitive(automerge::Primitive::Int(0)),
            automerge::Value::Primitive(automerge::Primitive::Boolean(false)),
        ]),
    );
    let expected = automerge::Value::Map(ehm.clone());

    assert_eq!(expected, frontend.get_value(&Path::root()).unwrap());

    if let Some(c) = c {
        println!("change2 {:?}", c);
        let (p, _) = backend.apply_local_change(c).unwrap();
        frontend.apply_patch(p).unwrap();
    }
    let v = frontend.get_value(&Path::root()).unwrap();

    let expected = automerge::Value::Map(ehm);
    assert_eq!(expected, v);
}

#[test]
fn test_multi_insert_expands_to_correct_indices() {
    let uuid = uuid::Uuid::new_v4();
    let actor = ActorId::from_bytes(uuid.as_bytes());

    let change = amp::Change {
        operations: vec![
            Op {
                action: OpType::Make(ObjType::Sequence(SequenceType::List)),
                obj: ObjectId::Root,
                key: Key::Map("a".to_owned()),
                pred: vec![],
                insert: false,
            },
            Op {
                action: OpType::Make(ObjType::Sequence(SequenceType::List)),
                obj: actor.op_id_at(1).into(),
                key: Key::Seq(ElementId::Head),
                pred: vec![],
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Null),
                obj: actor.op_id_at(1).into(),
                key: Key::Seq(actor.op_id_at(2).into()),
                pred: vec![],
                insert: true,
            },
            Op {
                action: OpType::Set(ScalarValue::Uint(0)),
                obj: actor.op_id_at(1).into(),
                key: Key::Seq(actor.op_id_at(3).into()),
                pred: vec![],
                insert: true,
            },
        ],
        actor_id: actor,
        hash: None,
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: vec![],
        extra_bytes: vec![],
    };

    let val = Value::Map(hashmap! {
        "a".to_owned() => Value::Sequence(
            vec![
                Value::Sequence(
                    vec![],
                ),
                Value::Primitive(
                    Primitive::Null,
                ),
                Value::Primitive(
                    Primitive::Uint(
                        0
                    ),
                ),
            ],
        ),
    });

    let mut doc = automerge::Frontend::new_with_actor_id(uuid);

    let ((), c) = doc
        .change::<_, _, InvalidChangeRequest>(None, |old| {
            old.add_change(LocalChange::set(
                Path::root().key("a"),
                Value::Sequence(vec![
                    Value::Sequence(vec![]),
                    Value::Primitive(Primitive::Null),
                    Value::Primitive(Primitive::Uint(0)),
                ]),
            ))
            .unwrap();
            Ok(())
        })
        .unwrap();
    let mut c = c.unwrap();

    assert_eq!(doc.get_value(&Path::root()).unwrap(), val);
    c.time = 0;
    assert_eq!(c, change);

    let mut b = automerge::Backend::new();
    let (patch, _) = b.apply_local_change(c).unwrap();
    doc.apply_patch(patch).unwrap();
    assert_eq!(doc.get_value(&Path::root()).unwrap(), val);
}

#[test]
fn test_frontend_doesnt_wait_for_empty_changes() {
    let vals = vec![
        Value::Map(hashmap! {}),
        Value::Map(hashmap! {
            "0".to_owned() => Value::Map(
                hashmap! {},
            ),
            "a".to_owned() => Value::Map(
                hashmap!{
                    "b".to_owned() => Value::Map(
                        hashmap!{},
                    ),
                },
            ),
        }),
        Value::Map(hashmap! {}),
    ];

    let changes = vec![
        vec![],
        vec![
            LocalChange::set(Path::root().key("0"), Value::Map(HashMap::new())),
            LocalChange::set(
                Path::root().key("a"),
                Value::Map(hashmap! {"b".to_owned() => Value::Map(HashMap::new() )}),
            ),
        ],
        vec![
            LocalChange::delete(Path::root().key("a")),
            LocalChange::delete(Path::root().key("0")),
        ],
    ];

    let mut doc = automerge::Frontend::new();

    let mut backend = Backend::new();

    for (val, changes) in vals.iter().zip(changes.into_iter()) {
        let ((), c) = doc
            .change::<_, _, InvalidChangeRequest>(None, |old| {
                for change in changes {
                    old.add_change(change).unwrap()
                }
                Ok(())
            })
            .unwrap();
        if let Some(c) = c {
            assert_eq!(doc.get_value(&Path::root()).unwrap(), *val);

            let (patch, _) = backend.apply_local_change(c).unwrap();
            doc.apply_patch(patch).unwrap();

            assert_eq!(doc.get_value(&Path::root()).unwrap(), *val);
        }
    }
}
