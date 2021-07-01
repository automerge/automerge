use std::{collections::HashMap, convert::TryInto};

use amp::SortedVec;
use automerge_frontend::{Frontend, InvalidChangeRequest, LocalChange, Path, Value};
use automerge_protocol as amp;
use maplit::hashmap;

#[test]
fn test_delete_index_in_mutation() {
    let mut frontend = Frontend::new();
    let _cr = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("vals"),
                Value::Sequence(Vec::new()),
            ))?;
            Ok(())
        })
        .unwrap();

    frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("vals").index(0),
                "0".into(),
            ))?;
            Ok(())
        })
        .unwrap();

    frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("vals").index(1),
                "1".into(),
            ))?;
            Ok(())
        })
        .unwrap();

    frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::delete(Path::root().key("vals").index(1)))?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_multiple_primitive_inserts() {
    let mut frontend = Frontend::new();
    let cr = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("vals"),
                Value::Sequence(Vec::new()),
            ))?;
            doc.add_change(LocalChange::insert_many(
                Path::root().key("vals").index(0),
                vec!["one".into(), "two".into()],
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        cr,
        amp::Change {
            message: None,
            seq: 1,
            actor_id: frontend.actor_id.clone(),
            hash: None,
            start_op: 1,
            deps: Vec::new(),
            time: cr.time,
            extra_bytes: Vec::new(),
            operations: vec![
                amp::Op {
                    key: "vals".into(),
                    insert: false,
                    pred: SortedVec::new(),
                    obj: amp::ObjectId::Root,
                    action: amp::OpType::Make(amp::ObjType::List),
                },
                amp::Op {
                    key: amp::ElementId::Head.into(),
                    action: amp::OpType::MultiSet(
                        vec![
                            amp::ScalarValue::Str("one".into()),
                            amp::ScalarValue::Str("two".into())
                        ]
                        .try_into()
                        .unwrap()
                    ),
                    obj: frontend.actor_id.op_id_at(1).into(),
                    pred: SortedVec::new(),
                    insert: true,
                }
            ]
        }
    );
}

#[test]
fn test_multiple_non_primitive_inserts() {
    let mut frontend = Frontend::new();
    let actor = frontend.actor_id.clone();
    let cr = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("vals"),
                Value::Sequence(Vec::new()),
            ))?;
            doc.add_change(LocalChange::insert_many(
                Path::root().key("vals").index(0),
                vec![
                    hashmap! {"test" => "test1"}.into(),
                    hashmap! {"test" => "test2"}.into(),
                ],
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        cr,
        amp::Change {
            message: None,
            seq: 1,
            actor_id: actor.clone(),
            hash: None,
            start_op: 1,
            deps: Vec::new(),
            time: cr.time,
            extra_bytes: Vec::new(),
            operations: vec![
                amp::Op {
                    key: "vals".into(),
                    insert: false,
                    pred: SortedVec::new(),
                    obj: amp::ObjectId::Root,
                    action: amp::OpType::Make(amp::ObjType::List),
                },
                amp::Op {
                    key: amp::ElementId::Head.into(),
                    obj: actor.op_id_at(1).into(),
                    pred: SortedVec::new(),
                    insert: true,
                    action: amp::OpType::Make(amp::ObjType::Map),
                },
                amp::Op {
                    key: "test".into(),
                    obj: actor.op_id_at(2).into(),
                    pred: SortedVec::new(),
                    insert: false,
                    action: amp::OpType::Set("test1".into()),
                },
                amp::Op {
                    key: actor.op_id_at(2).into(),
                    obj: actor.op_id_at(1).into(),
                    pred: SortedVec::new(),
                    insert: true,
                    action: amp::OpType::Make(amp::ObjType::Map),
                },
                amp::Op {
                    key: "test".into(),
                    obj: actor.op_id_at(4).into(),
                    pred: SortedVec::new(),
                    insert: false,
                    action: amp::OpType::Set("test2".into()),
                }
            ]
        }
    );
}

#[test]
fn test_delete_non_existent_root_key() {
    let mut frontend = Frontend::new();
    let path = Path::root().key("missing");
    let cr = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::delete(path.clone()))?;
            Ok(())
        })
        .unwrap_err();

    assert_eq!(cr, InvalidChangeRequest::NoSuchPathError { path })
}

#[test]
fn test_delete_non_existent_map_key() {
    let mut frontend = Frontend::new();
    let path = Path::root().key("a").key("missing");
    let cr = frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("a"),
                Value::Map(HashMap::new()),
            ))?;
            doc.add_change(LocalChange::delete(path.clone()))?;
            Ok(())
        })
        .unwrap_err();

    assert_eq!(cr, InvalidChangeRequest::NoSuchPathError { path })
}
