use automerge_frontend::{AutomergeFrontendError, Frontend, LocalChange, Path, Value};
use automerge_protocol as amp;
use maplit::hashmap;

const ROOT_ID: &str = "00000000-0000-0000-0000-000000000000";

#[test]
fn test_should_be_empty_after_init() {
    let frontend = Frontend::new();
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
    let (frontend, _) = Frontend::new_with_initial_state(value).unwrap();
    let result_state = frontend.state().to_json();
    assert_eq!(initial_state_json, result_state);
}

#[test]
fn test_init_with_empty_state() {
    let initial_state_json: serde_json::Value = serde_json::from_str("{}").unwrap();
    let value = Value::from_json(&initial_state_json);
    let (frontend, _) = Frontend::new_with_initial_state(value).unwrap();
    let result_state = frontend.state().to_json();
    assert_eq!(initial_state_json, result_state);
}

#[test]
fn test_set_root_object_properties() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change(Some("set root object".into()), |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("bird"),
                Value::Primitive(amp::Value::Str("magpie".to_string())),
            ))?;
            Ok(())
        })
        .unwrap()
        // Remove timestamp which is irrelevant to test
        .map(|mut cr| {
            cr.time = None;
            cr
        });
    let expected_change = amp::Request {
        actor: doc.actor_id,
        seq: 1,
        version: 0,
        time: None,
        message: Some("set root object".into()),
        undoable: true,
        deps: None,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Set,
            obj: ROOT_ID.to_string(),
            key: amp::RequestKey::Str("bird".to_string()),
            child: None,
            value: Some(amp::Value::Str("magpie".to_string())),
            datatype: Some(amp::DataType::Undefined),
            insert: false,
        }]),
        request_type: amp::RequestType::Change,
    };
    assert_eq!(change_request, Some(expected_change));
}

#[test]
fn it_should_return_no_changes_if_nothing_was_changed() {
    let mut doc = Frontend::new();
    let change_request = doc.change(Some("do nothing".into()), |_| Ok(())).unwrap();
    assert!(change_request.is_none())
}

#[test]
fn it_should_create_nested_maps() {
    let mut doc = Frontend::new();
    let change_request = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::from_json(&serde_json::json!({
                    "wrens": 3
                })),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();
    let expected_change = amp::Request {
        actor: doc.actor_id,
        seq: 1,
        time: change_request.time,
        message: None,
        version: 0,
        undoable: true,
        request_type: amp::RequestType::Change,
        deps: None,
        ops: Some(vec![
            amp::Op {
                action: amp::OpType::MakeMap,
                obj: amp::ObjectID::Root.to_string(),
                key: amp::RequestKey::Str("birds".into()),
                child: Some(birds_id.to_string()),
                datatype: None,
                value: None,
                insert: false,
            },
            amp::Op {
                action: amp::OpType::Set,
                obj: birds_id.to_string(),
                key: amp::RequestKey::Str("wrens".into()),
                child: None,
                datatype: Some(amp::DataType::Undefined),
                value: Some(amp::Value::F64(3.0)),
                insert: false,
            },
        ]),
    };
    assert_eq!(change_request, expected_change);
}

#[test]
fn apply_updates_inside_nested_maps() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::from_json(&serde_json::json!({
                    "wrens": 3,
                })),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let state_after_first_change = doc.state().clone();
    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").key("sparrows"),
                Value::Primitive(amp::Value::F64(15.0)),
            ))?;
            Ok(())
        })
        .unwrap()
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

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Set,
            obj: birds_id.to_string(),
            key: "sparrows".into(),
            child: None,
            value: Some(amp::Value::F64(15.0)),
            insert: false,
            datatype: Some(amp::DataType::Undefined),
        }]),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn delete_keys_in_a_map() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
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
        .unwrap();
    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::delete(Path::root().key("magpies")))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "sparrows": 15.0
        }))
    );

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Del,
            obj: amp::ObjectID::Root.to_string(),
            key: "magpies".into(),
            child: None,
            value: None,
            insert: false,
            datatype: None,
        }]),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn create_lists() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::Sequence(vec!["chaffinch".into()], amp::SequenceType::List),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let _req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").index(0),
                "chaffinch".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["chaffinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 1,
        version: 0,
        time: req1.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![
            amp::Op {
                action: amp::OpType::MakeList,
                key: "birds".into(),
                obj: amp::ObjectID::Root.to_string(),
                child: Some(birds_id.to_string()),
                value: None,
                datatype: None,
                insert: false,
            },
            amp::Op {
                action: amp::OpType::Set,
                obj: birds_id.to_string(),
                key: 0.into(),
                child: None,
                value: Some("chaffinch".into()),
                insert: true,
                datatype: Some(amp::DataType::Undefined),
            },
        ]),
    };

    assert_eq!(req1, expected_change_request);
}

#[test]
fn apply_updates_inside_lists() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                Value::Sequence(vec!["chaffinch".into()], amp::SequenceType::List),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds").index(0),
                "greenfinch".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["greenfinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Set,
            obj: birds_id.to_string(),
            key: 0.into(),
            child: None,
            value: Some("greenfinch".into()),
            insert: false,
            datatype: Some(amp::DataType::Undefined),
        }]),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn delete_list_elements() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                vec!["chaffinch", "goldfinch"].into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::delete(Path::root().key("birds").index(0)))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        doc.state(),
        &Value::from_json(&serde_json::json!({
            "birds": ["goldfinch"],
        }))
    );

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Del,
            obj: birds_id.to_string(),
            key: 0.into(),
            child: None,
            value: None,
            insert: false,
            datatype: None,
        }]),
    };

    assert_eq!(req2, expected_change_request);
}

#[test]
fn handle_counters_inside_maps() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("wrens"),
                Value::Primitive(amp::Value::Counter(0)),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let state_after_first_change = doc.state().clone();

    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::increment(Path::root().key("wrens")))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let state_after_second_change = doc.state().clone();

    assert_eq!(
        state_after_first_change,
        Value::Map(
            hashmap! {
                "wrens".into() => Value::Primitive(amp::Value::Counter(0))
            },
            amp::MapType::Map
        )
    );

    assert_eq!(
        state_after_second_change,
        Value::Map(
            hashmap! {
                "wrens".into() => Value::Primitive(amp::Value::Counter(1))
            },
            amp::MapType::Map
        )
    );

    let expected_change_request_1 = amp::Request {
        actor: doc.actor_id.clone(),
        seq: 1,
        version: 0,
        time: req1.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Set,
            obj: amp::ObjectID::Root.to_string(),
            key: "wrens".into(),
            child: None,
            value: Some(amp::Value::Counter(0)),
            insert: false,
            datatype: Some(amp::DataType::Counter),
        }]),
    };
    assert_eq!(req1, expected_change_request_1);

    let expected_change_request_2 = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Inc,
            obj: amp::ObjectID::Root.to_string(),
            key: "wrens".into(),
            child: None,
            value: Some(amp::Value::Int(1)),
            insert: false,
            datatype: Some(amp::DataType::Counter),
        }]),
    };
    assert_eq!(req2, expected_change_request_2);
}

#[test]
fn handle_counters_inside_lists() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("counts"),
                vec![Value::Primitive(amp::Value::Counter(1))].into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let state_after_first_change = doc.state().clone();

    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::increment_by(
                Path::root().key("counts").index(0),
                2,
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();
    let state_after_second_change = doc.state().clone();

    assert_eq!(
        state_after_first_change,
        Value::Map(
            hashmap! {
                "counts".into() => vec![Value::Primitive(amp::Value::Counter(1))].into()
            },
            amp::MapType::Map
        )
    );

    assert_eq!(
        state_after_second_change,
        Value::Map(
            hashmap! {
                "counts".into() => vec![Value::Primitive(amp::Value::Counter(3))].into()
            },
            amp::MapType::Map
        )
    );

    let counts_id = doc.get_object_id(&Path::root().key("counts")).unwrap();

    let expected_change_request_1 = amp::Request {
        actor: doc.actor_id.clone(),
        seq: 1,
        version: 0,
        time: req1.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![
            amp::Op {
                action: amp::OpType::MakeList,
                obj: amp::ObjectID::Root.to_string(),
                key: "counts".into(),
                child: Some(counts_id.to_string()),
                insert: false,
                value: None,
                datatype: None,
            },
            amp::Op {
                action: amp::OpType::Set,
                obj: counts_id.to_string(),
                key: 0.into(),
                child: None,
                value: Some(amp::Value::Counter(1)),
                insert: true,
                datatype: Some(amp::DataType::Counter),
            },
        ]),
    };
    assert_eq!(req1, expected_change_request_1);

    let expected_change_request_2 = amp::Request {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Inc,
            obj: counts_id.to_string(),
            key: 0.into(),
            child: None,
            value: Some(amp::Value::Int(2)),
            insert: false,
            datatype: Some(amp::DataType::Counter),
        }]),
    };
    assert_eq!(req2, expected_change_request_2);
}

#[test]
fn refuse_to_overwrite_counter_value() {
    let mut doc = Frontend::new();
    doc.change(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("counts"),
            Value::Primitive(amp::Value::Counter(1)),
        ))?;
        Ok(())
    })
    .unwrap()
    .unwrap();

    let result = doc.change(None, |doc| {
        doc.add_change(LocalChange::set(
            Path::root().key("counts"),
            Value::Primitive("somethingelse".into()),
        ))?;
        Ok(())
    });

    assert_eq!(result, Err(AutomergeFrontendError::CannotOverwriteCounter));
}
