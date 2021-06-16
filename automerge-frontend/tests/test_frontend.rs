use std::{convert::TryInto, num::NonZeroU32};

use automerge_frontend::{Frontend, InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use automerge_protocol as amp;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use unicode_segmentation::UnicodeSegmentation;

#[test]
fn test_should_be_empty_after_init() {
    let mut frontend = Frontend::new();
    let result_state = frontend.state().to_json();
    let expected_state: serde_json::Value = serde_json::from_str("{}").unwrap();
    assert_eq!(result_state, expected_state);
}

#[test]
fn test_init_with_state() {
    let initial_state_json: serde_json::Value = serde_json::from_str(
        r#"
        {
            "birds": {
                "wrens": 3.0,
                "magpies": 4.0
            },
            "alist": ["one", 2.0]
        }
    "#,
    )
    .unwrap();
    let value = Value::from_json(&initial_state_json);
    let (mut frontend, _) = Frontend::new_with_initial_state(value).unwrap();
    let result_state = frontend.state().to_json();
    assert_eq!(initial_state_json, result_state);
}

#[test]
fn test_init_with_empty_state() {
    let initial_state_json: serde_json::Value = serde_json::from_str("{}").unwrap();
    let value = Value::from_json(&initial_state_json);
    let (mut frontend, _) = Frontend::new_with_initial_state(value).unwrap();
    let result_state = frontend.state().to_json();
    assert_eq!(initial_state_json, result_state);
}

#[test]
fn test_set_root_object_properties() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change::<_, _, InvalidChangeRequest>(Some("set root object".into()), |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("bird"),
                Value::Primitive(Primitive::Str("magpie".to_string())),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        // Remove timestamp which is irrelevant to test
        .map(|mut cr| {
            cr.time = 0;
            cr
        });
    let expected_change = amp::Change {
        actor_id: doc.actor_id,
        start_op: 1,
        seq: 1,
        time: 0,
        message: Some("set root object".into()),
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Str("magpie".to_string())),
            obj: "_root".try_into().unwrap(),
            key: "bird".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change_request, Some(expected_change));
}

#[test]
fn test_set_bytes() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change::<_, _, InvalidChangeRequest>(Some("set root object".into()), |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("bird"),
                Value::Primitive(Primitive::Bytes(vec![1, 2, 3])),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        // Remove timestamp which is irrelevant to test
        .map(|mut cr| {
            cr.time = 0;
            cr
        });
    let expected_change = amp::Change {
        actor_id: doc.actor_id,
        start_op: 1,
        seq: 1,
        time: 0,
        message: Some("set root object".into()),
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Bytes(vec![1, 2, 3])),
            obj: "_root".try_into().unwrap(),
            key: "bird".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change_request, Some(expected_change));
}

#[test]
fn it_should_return_no_changes_if_nothing_was_changed() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change::<_, _, InvalidChangeRequest>(Some("do nothing".into()), |_| Ok(()))
        .unwrap()
        .1;
    assert!(change_request.is_none())
}

#[test]
fn it_should_create_nested_maps() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::from_json(&serde_json::json!({
                    "wrens": 3
                })),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();
    let expected_change = amp::Change {
        actor_id: doc.actor_id,
        start_op: 1,
        seq: 1,
        time: change_request.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            amp::Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: amp::ObjectId::Root,
                key: "birds".into(),
                insert: false,
                pred: Vec::new(),
            },
            amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::F64(3.0)),
                obj: birds_id,
                key: "wrens".into(),
                insert: false,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change_request, expected_change);
}

#[test]
fn apply_updates_inside_nested_maps() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::from_json(&serde_json::json!({
                    "wrens": 3,
                })),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_first_change = doc.state().clone();
    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").key("sparrows"),
                Value::Primitive(Primitive::F64(15.0)),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_second_change = doc.state().clone();

    assert_eq!(
        state_after_first_change,
        Value::from_json(&serde_json::json!({
            "birds": { "wrens": 3.0}
        }))
    );
    assert_eq!(
        state_after_second_change,
        Value::from_json(&serde_json::json!({
            "birds": {
                "wrens": 3.0,
                "sparrows": 15.0
            }
        }))
    );
    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id,
        seq: 2,
        start_op: 3,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::F64(15.0)),
            obj: birds_id,
            key: "sparrows".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn delete_keys_in_a_map() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root(),
                Value::from_json(&serde_json::json!({
                    "magpies": 2,
                })),
            ))?;
            doc.add_change(LocalChange::set(
                Path::root(),
                Value::from_json(&serde_json::json!({
                    "sparrows": 15,
                })),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::delete(Path::root().key("magpies")))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "sparrows": 15.0
        }))
    );

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 3,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
            obj: amp::ObjectId::Root,
            key: "magpies".into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(1)],
        }],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn create_lists() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::Sequence(vec!["chaffinch".into()]),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let _req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").index(0),
                "chaffinch",
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["chaffinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id,
        seq: 1,
        start_op: 1,
        time: req1.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            amp::Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                key: "birds".into(),
                obj: amp::ObjectId::Root,
                insert: false,
                pred: Vec::new(),
            },
            amp::Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: birds_id,
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req1, expected_change_request);
}

#[test]
fn apply_updates_inside_lists() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::Sequence(vec!["chaffinch".into()]),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").index(0),
                "greenfinch",
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["greenfinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 3,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set("greenfinch".into()),
            obj: birds_id,
            key: doc.actor_id.op_id_at(2).into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(2)],
        }],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn delete_list_elements() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                vec!["chaffinch", "goldfinch"],
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::delete(Path::root().key("birds").index(0)))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["goldfinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 4,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
            obj: birds_id,
            key: doc.actor_id.op_id_at(2).into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(2)],
        }],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn handle_counters_inside_maps() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("wrens"),
                Value::Primitive(Primitive::Counter(0)),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_first_change = doc.state().clone();

    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::increment(Path::root().key("wrens")))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_second_change = doc.state().clone();

    assert_eq!(
        state_after_first_change,
        Value::Map(hashmap! {
            "wrens".into() => Value::Primitive(Primitive::Counter(0))
        },)
    );

    assert_eq!(
        state_after_second_change,
        Value::Map(hashmap! {
            "wrens".into() => Value::Primitive(Primitive::Counter(1))
        },)
    );

    let expected_change_request_1 = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 1,
        start_op: 1,
        time: req1.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Counter(0)),
            obj: amp::ObjectId::Root,
            key: "wrens".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(req1, expected_change_request_1);

    let expected_change_request_2 = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 2,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Inc(1),
            obj: amp::ObjectId::Root,
            key: "wrens".into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(1)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(req2, expected_change_request_2);
}

#[test]
fn handle_counters_inside_lists() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("counts"),
                vec![Value::Primitive(Primitive::Counter(1))],
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_first_change = doc.state().clone();

    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::increment_by(
                Path::root().key("counts").index(0),
                2,
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let state_after_second_change = doc.state().clone();

    assert_eq!(
        state_after_first_change,
        Value::Map(hashmap! {
            "counts".into() => vec![Value::Primitive(Primitive::Counter(1))].into()
        },)
    );

    assert_eq!(
        state_after_second_change,
        Value::Map(hashmap! {
            "counts".into() => vec![Value::Primitive(Primitive::Counter(3))].into()
        },)
    );

    let counts_id = doc.get_object_id(&Path::root().key("counts")).unwrap();

    let expected_change_request_1 = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 1,
        time: req1.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        start_op: 1,
        operations: vec![
            amp::Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: amp::ObjectId::Root,
                key: "counts".into(),
                insert: false,
                pred: Vec::new(),
            },
            amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Counter(1)),
                obj: counts_id.clone(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    assert_eq!(req1, expected_change_request_1);

    let expected_change_request_2 = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 3,
        time: req2.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Inc(2),
            obj: counts_id,
            key: doc.actor_id.op_id_at(2).into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(2)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(req2, expected_change_request_2);
}

#[test]
fn refuse_to_overwrite_counter_value() {
    let mut doc = Frontend::new();
    doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("counts"),
            Value::Primitive(Primitive::Counter(1)),
        ))?;
        Ok(())
    })
    .unwrap()
    .1
    .unwrap();

    let result = doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("counts"),
            "somethingelse",
        ))?;
        Ok(())
    });

    assert_eq!(
        result,
        Err(InvalidChangeRequest::CannotOverwriteCounter {
            path: Path::root().key("counts")
        })
    );
}

#[test]
fn test_sets_characters_in_text() {
    let mut doc = Frontend::new();
    doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("text"),
            Value::Text("some".graphemes(true).map(|s| s.to_owned()).collect()),
        ))?;
        Ok(())
    })
    .unwrap()
    .1
    .unwrap();

    let request = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(Path::root().key("text").index(1), "a"))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let text_id = doc.get_object_id(&Path::root().key("text")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 6,
        time: request.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Str("a".into())),
            obj: text_id,
            key: doc.actor_id.op_id_at(3).into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(3)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(request, expected_change_request);

    let value = doc.get_value(&Path::root()).unwrap();
    let expected_value: Value = Value::Map(hashmap! {
        "text".into() => Value::Text(vec!["s".to_owned(), "a".to_owned(), "m".to_owned(), "e".to_owned()]),
    });
    assert_eq!(value, expected_value);
}

#[test]
fn test_inserts_characters_in_text() {
    let mut doc = Frontend::new();
    doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("text"),
            Value::Text("same".graphemes(true).map(|s| s.to_owned()).collect()),
        ))?;
        Ok(())
    })
    .unwrap()
    .1
    .unwrap();

    let request = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("text").index(1),
                "h".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let text_id = doc.get_object_id(&Path::root().key("text")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 6,
        time: request.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Str("h".into())),
            obj: text_id,
            key: doc.actor_id.op_id_at(2).into(),
            insert: true,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(request, expected_change_request);

    let value = doc.get_value(&Path::root()).unwrap();
    let expected_value: Value = Value::Map(hashmap! {
        "text".into() => Value::Text(vec!["s".to_owned(), "h".to_owned(), "a".to_owned(), "m".to_owned(), "e".to_owned()]),
    });
    assert_eq!(value, expected_value);
}

#[test]
fn test_inserts_characters_at_start_of_text() {
    let mut doc = Frontend::new();
    doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("text"),
            Value::Text(Vec::new()),
        ))?;
        Ok(())
    })
    .unwrap()
    .1
    .unwrap();

    let request = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("text").index(0),
                "i".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let text_id = doc.get_object_id(&Path::root().key("text")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 2,
        time: request.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Str("i".into())),
            obj: text_id,
            key: amp::ElementId::Head.into(),
            insert: true,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(request, expected_change_request);

    let value = doc.get_value(&Path::root()).unwrap();
    let expected_value: Value = Value::Map(hashmap! {
        "text".into() => Value::Text(vec!["i".to_owned()]),
    });
    assert_eq!(value, expected_value);
}

#[test]
fn test_inserts_at_end_of_lists() {
    let mut doc = Frontend::new();
    doc.change::<_, _, InvalidChangeRequest>(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("birds"),
            Value::Sequence(Vec::new()),
        ))?;
        Ok(())
    })
    .unwrap()
    .1
    .unwrap();

    let request = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("birds").index(0),
                "greenfinch".into(),
            ))?;
            doc.add_change(LocalChange::insert(
                Path::root().key("birds").index(1),
                "bullfinch".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let list_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id.clone(),
        seq: 2,
        start_op: 2,
        time: request.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str("greenfinch".into())),
                obj: list_id.clone(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str("bullfinch".into())),
                obj: list_id,
                key: doc.actor_id.op_id_at(2).into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    assert_eq!(request, expected_change_request);

    let value = doc.get_value(&Path::root()).unwrap();
    let expected_value: Value = Value::Map(hashmap! {
        "birds".into() => Value::Sequence(vec!["greenfinch".into(), "bullfinch".into()]),
    });
    assert_eq!(value, expected_value);
}
