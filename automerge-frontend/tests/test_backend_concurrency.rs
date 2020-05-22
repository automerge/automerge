use automerge_backend::Backend;
use automerge_frontend::{AutomergeFrontendError, Frontend, LocalChange, Path, Value};
use automerge_protocol as amp;
use maplit::hashmap;

fn random_op_id() -> String {
    amp::OpID::new(1, &amp::ActorID::random()).to_string()
}

#[test]
fn use_version_and_sequence_number_from_backend() {
    let mut doc = Frontend::new();
    let remote_actor1 = amp::ActorID::random();
    let remote_actor2 = amp::ActorID::random();

    // This is a remote patch
    let patch = amp::Patch {
        actor: None,
        seq: None,
        version: 3,
        can_undo: false,
        can_redo: false,
        clock: hashmap! {
            doc.actor_id.to_string() => 4,
            remote_actor1.to_string() => 11,
            remote_actor2.to_string() => 41,
        },
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::F64(24.0))
                }
            },
        })),
    };

    // There were no in flight requests so the doc state should be reconciled
    // and should reflect the above patch
    doc.apply_patch(patch).unwrap();

    // Now apply a local patch, this will move the doc into the "waiting for
    // in flight requests" state, which should reflect the change just made.
    let req = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("partridges"),
                Value::Primitive(amp::Value::Int(1)),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let expected_change_request = amp::Request {
        actor: doc.actor_id,
        seq: 5,
        time: req.time,
        version: 3,
        message: None,
        undoable: true,
        deps: None,
        request_type: amp::RequestType::Change,
        ops: Some(vec![amp::Op {
            action: amp::OpType::Set,
            obj: amp::ObjectID::Root.to_string(),
            key: "partridges".into(),
            insert: false,
            value: Some(amp::Value::Int(1)),
            datatype: Some(amp::DataType::Undefined),
            child: None,
        }]),
    };

    assert_eq!(req, expected_change_request);
}

#[test]
fn remove_pending_requests_once_handled() {
    let mut doc = Frontend::new();

    // First we add two local changes
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                amp::Value::Int(24).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let _req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("partridges"),
                amp::Value::Int(1).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    // The doc is waiting for those changes to be applied
    assert_eq!(doc.in_flight_requests(), vec![1, 2]);

    // Apply a patch corresponding (via actor ID and seq) to the first change
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(1),
        clock: hashmap! {
            doc.actor_id.to_string() => 1,
        },
        can_undo: true,
        can_redo: false,
        version: 1,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::Int(24))
                }
            },
        })),
    })
    .unwrap();

    // The doc state should still reflect both local changes as we're still
    // waiting  for the last in flight request to be fulfilled
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => amp::Value::Int(24),
            "partridges".to_string() => amp::Value::Int(1),
        })
    );
    assert_eq!(doc.in_flight_requests(), vec![2]);

    // Apply a patch corresponding (via actor ID and seq) to the second change
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(2),
        clock: hashmap! {
            doc.actor_id.to_string() => 2,
        },
        can_undo: true,
        can_redo: false,
        version: 2,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "partridges".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::Int(1))
                }
            },
        })),
    })
    .unwrap();

    // The doc state should have switched to reconciled
    assert!(doc.in_flight_requests().is_empty());

    // The doc state should still reflect the local changes as they have now
    // been reconciled
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => amp::Value::Int(24),
            "partridges".to_string() => amp::Value::Int(1),
        })
    );

    assert_eq!(doc.version, 2);
    assert_eq!(doc.seq, 2);
}

#[test]
fn leave_request_queue_unchanged_on_remote_changes() {
    let remote = amp::ActorID::random();
    let mut doc = Frontend::new();
    // Enqueue a local change, moving the document into the "waiting for in
    // flight requests" state
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                amp::Value::Int(24).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    // The document is now waiting for the above request
    assert_eq!(doc.in_flight_requests(), vec![1]);

    // Apply a remote patch (due to actor ID and seq missing)
    doc.apply_patch(amp::Patch {
        actor: None,
        seq: None,
        version: 1,
        clock: hashmap! {
            remote.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "pheasants".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::Int(2))
                }
            },
        })),
    })
    .unwrap();

    // The doc state should reflect outstanding in flight request and not the
    // remote patch (because we're still waiting for in flight requests)
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => amp::Value::Int(24),
        })
    );
    assert_eq!(doc.in_flight_requests(), vec![1]);

    // Now apply a patch corresponding to the outstanding in flight request
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(1),
        clock: hashmap! {
            doc.actor_id.to_string() => 2,
            remote.to_string() => 1,
        },
        can_undo: true,
        can_redo: false,
        version: 2,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::Int(24))
                }
            },
        })),
    })
    .unwrap();

    // The doc state should now reflect both the local and remote changes
    // as the doc is now reconciled (all in flight requests have received a
    // patch)
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => amp::Value::Int(24),
            "pheasants".to_string() => amp::Value::Int(2),
        })
    );

    assert!(doc.in_flight_requests().is_empty());
    assert_eq!(doc.version, 2);
    assert_eq!(doc.seq, 2);
}

#[test]
fn dont_allow_out_of_order_request_patches() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                amp::Value::Int(24).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let result = doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(2),
        version: 2,
        clock: hashmap! {
            doc.actor_id.to_string() => 2,
        },
        deps: Vec::new(),
        can_undo: true,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "partridges".to_string() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::Value::Int(1))
                }
            },
        })),
    });

    assert_eq!(
        result,
        Err(AutomergeFrontendError::MismatchedSequenceNumber)
    );
}

#[test]
fn handle_concurrent_insertions_into_lists() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                vec!["goldfinch"].into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    // Apply the corresponding backend patch for the above state, document
    // shoudl be reconciled after this
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(1),
        version: 1,
        clock: hashmap! {
            doc.actor_id.to_string() => 1,
        },
        can_undo: true,
        can_redo: false,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".to_string() => hashmap!{
                    doc.actor_id.op_id_at(1).to_string() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: birds_id.to_string(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert{ index: 0 }],
                        props: hashmap!{
                            0 => hashmap!{
                                random_op_id() => amp::Diff::Value("goldfinch".into())
                            }
                        }
                    })
                }
            },
        })),
    })
    .unwrap();

    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {"birds".to_string() => vec!["goldfinch"]})
    );
    assert!(doc.in_flight_requests().is_empty());

    // Now add another change which updates the same list, this results in an
    // in flight reuest
    let _req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("birds").index(0),
                "chaffinch".into(),
            ))?;
            doc.add_change(LocalChange::insert(
                Path::root().key("birds").index(2),
                "greenfinch".into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch"]}
        )
    );

    let remote = amp::ActorID::random();

    // Apply a patch which does not take effect because we're still waiting
    // for the in flight requests to be responded to
    doc.apply_patch(amp::Patch{
        version: 3,
        clock: hashmap!{
            doc.actor_id.to_string() => 1,
            remote.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff{
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap!{
                "birds".into() => hashmap!{
                    doc.actor_id.op_id_at(1).to_string() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: birds_id.to_string(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert{ index: 1 }],
                        props: hashmap!{
                            1 => hashmap!{
                                remote.op_id_at(1).to_string() => amp::Diff::Value("bullfinch".into())
                            }
                        }
                    })
                }
            }
        }))
    }).unwrap();

    // Check that the doc state hasn't been updated yet
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch"]}
        )
    );

    // Now apply a patch acknowledging the in flight request
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.to_string()),
        seq: Some(2),
        version: 3,
        clock: hashmap!{
            doc.actor_id.to_string() => 2,
            remote.to_string() => 1,
        },
        can_undo: true,
        can_redo: false,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff{
            object_id: amp::ObjectID::Root.to_string(),
            obj_type: amp::ObjType::Map,
            props: hashmap!{
                "birds".to_string() => hashmap!{
                    doc.actor_id.op_id_at(1).to_string() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: birds_id.to_string(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert { index: 0 }, amp::DiffEdit::Insert{ index: 2 }],
                        props: hashmap!{
                            0 => hashmap!{
                                doc.actor_id.op_id_at(2).to_string() => amp::Diff::Value("chaffinch".into()),
                            },
                            2 => hashmap!{
                                doc.actor_id.op_id_at(3).to_string() => amp::Diff::Value("greenfinch".into()),
                            }
                        }
                    })
                }
            }
        }))
    }).unwrap();

    assert!(doc.in_flight_requests().is_empty());
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch", "bullfinch"]}
        )
    )
}

#[test]
fn allow_interleacing_of_patches_and_changes() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                amp::Value::Int(1).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    let req2 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                amp::Value::Int(2).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        req1,
        amp::Request {
            actor: doc.actor_id.clone(),
            seq: 1,
            version: 0,
            message: None,
            time: req1.time,
            undoable: true,
            deps: None,
            request_type: amp::RequestType::Change,
            ops: Some(vec![amp::Op {
                action: amp::OpType::Set,
                obj: amp::ObjectID::Root.to_string(),
                key: "number".into(),
                value: Some(amp::Value::Int(1)),
                child: None,
                datatype: Some(amp::DataType::Undefined),
                insert: false,
            }])
        }
    );

    assert_eq!(
        req2,
        amp::Request {
            actor: doc.actor_id.clone(),
            seq: 2,
            version: 0,
            message: None,
            time: req2.time,
            undoable: true,
            deps: None,
            request_type: amp::RequestType::Change,
            ops: Some(vec![amp::Op {
                action: amp::OpType::Set,
                obj: amp::ObjectID::Root.to_string(),
                key: "number".into(),
                value: Some(amp::Value::Int(2)),
                child: None,
                datatype: Some(amp::DataType::Undefined),
                insert: false,
            }])
        }
    );

    let mut backend = Backend::init();
    let patch1 = backend.apply_local_change(req1).unwrap();
    doc.apply_patch(patch1).unwrap();

    let req3 = doc
        .change(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                amp::Value::Int(3).into(),
            ))?;
            Ok(())
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        req3,
        amp::Request {
            actor: doc.actor_id,
            seq: 3,
            version: 1,
            message: None,
            time: req3.time,
            undoable: true,
            deps: None,
            request_type: amp::RequestType::Change,
            ops: Some(vec![amp::Op {
                action: amp::OpType::Set,
                obj: amp::ObjectID::Root.to_string(),
                key: "number".into(),
                value: Some(amp::Value::Int(3)),
                child: None,
                datatype: Some(amp::DataType::Undefined),
                insert: false,
            }])
        }
    );
}
