use automerge_frontend::{Frontend, LocalChange, Path, Value, SequenceType};
use automerge_protocol as amp;

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
    let expected_change = amp::ChangeRequest {
        actor: doc.actor_id,
        seq: 1,
        version: 0,
        time: None,
        message: Some("set root object".into()),
        undoable: true,
        deps: None,
        ops: Some(vec![amp::OpRequest {
            action: amp::ReqOpType::Set,
            obj: ROOT_ID.to_string(),
            key: amp::RequestKey::Str("bird".to_string()),
            child: None,
            value: Some(amp::Value::Str("magpie".to_string())),
            datatype: Some(amp::DataType::Undefined),
            insert: false,
        }]),
        request_type: amp::ChangeRequestType::Change,
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
    let expected_change = amp::ChangeRequest {
        actor: doc.actor_id,
        seq: 1,
        time: change_request.time,
        message: None,
        version: 0,
        undoable: true,
        request_type: amp::ChangeRequestType::Change,
        deps: None,
        ops: Some(vec![
            amp::OpRequest {
                action: amp::ReqOpType::MakeMap,
                obj: amp::ObjectID::Root.to_string(),
                key: amp::RequestKey::Str("birds".into()),
                child: Some(birds_id.to_string()),
                datatype: None,
                value: None,
                insert: false,
            },
            amp::OpRequest {
                action: amp::ReqOpType::Set,
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

    let expected_change_request = amp::ChangeRequest {
        actor: doc.actor_id,
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::ChangeRequestType::Change,
        ops: Some(vec![amp::OpRequest {
            action: amp::ReqOpType::Set,
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

    let expected_change_request = amp::ChangeRequest {
        actor: doc.actor_id.clone(),
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::ChangeRequestType::Change,
        ops: Some(vec![amp::OpRequest {
            action: amp::ReqOpType::Del,
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
                Value::Sequence(vec!["chaffinch".into()], SequenceType::List),
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

    assert_eq!(doc.state(), &Value::from_json(&serde_json::json!({
        "birds": ["chaffinch"],
    })));

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::ChangeRequest {
        actor: doc.actor_id.clone(),
        seq: 1,
        version: 0,
        time: req1.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::ChangeRequestType::Change,
        ops: Some(vec![
            amp::OpRequest {
                action: amp::ReqOpType::MakeList,
                key: "birds".into(),
                obj: amp::ObjectID::Root.to_string(),
                child: Some(birds_id.to_string()),
                value: None,
                datatype: None,
                insert: false,
            },
            amp::OpRequest {
                action: amp::ReqOpType::Set,
                obj: birds_id.to_string(),
                key: 0.into(),
                child: None,
                value: Some("chaffinch".into()),
                insert: true,
                datatype: Some(amp::DataType::Undefined),
            }
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
                Value::Sequence(vec!["chaffinch".into()], SequenceType::List),
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

    assert_eq!(doc.state(), &Value::from_json(&serde_json::json!({
        "birds": ["greenfinch"],
    })));

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    let expected_change_request = amp::ChangeRequest {
        actor: doc.actor_id.clone(),
        seq: 2,
        version: 0,
        time: req2.time,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::ChangeRequestType::Change,
        ops: Some(vec![
            amp::OpRequest {
                action: amp::ReqOpType::Set,
                obj: birds_id.to_string(),
                key: 0.into(),
                child: None,
                value: Some("greenfinch".into()),
                insert: false,
                datatype: Some(amp::DataType::Undefined),
            }
        ]),
    };

    assert_eq!(req2, expected_change_request);
}
