use amp::RootDiff;
use automerge_backend::Backend;
use automerge_frontend::{
    Frontend, InvalidChangeRequest, InvalidPatch, LocalChange, Path, Primitive, Value,
};
use automerge_protocol as amp;
use maplit::hashmap;
use pretty_assertions::assert_eq;

fn random_op_id() -> amp::OpId {
    amp::OpId::new(1, &amp::ActorId::random())
}

#[test]
fn use_version_and_sequence_number_from_backend() {
    let mut doc = Frontend::new();
    let remote_actor1 = amp::ActorId::random();
    let remote_actor2 = amp::ActorId::random();

    // This is a remote patch
    let patch = amp::Patch {
        actor: None,
        seq: None,
        clock: hashmap! {
            doc.actor_id.clone() => 4,
            remote_actor1 => 11,
            remote_actor2 => 41,
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::F64(24.0))
                }
            },
        },
        max_op: 4,
        pending_changes: 0,
    };

    // There were no in flight requests so the doc state should be reconciled
    // and should reflect the above patch
    doc.apply_patch(patch).unwrap();

    // Now apply a local patch, this will move the doc into the "waiting for
    // in flight requests" state, which should reflect the change just made.
    let req = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("partridges"),
                Value::Primitive(Primitive::Int(1)),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let expected_change_request = amp::Change {
        actor_id: doc.actor_id,
        seq: 5,
        start_op: 5,
        time: req.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::Int(1)),
            obj: amp::ObjectId::Root,
            key: "partridges".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    assert_eq!(req, expected_change_request);
}

#[test]
fn remove_pending_requests_once_handled() {
    let mut doc = Frontend::new();

    // First we add two local changes
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                Primitive::Int(24),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let _req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("partridges"),
                Primitive::Int(1),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    // The doc is waiting for those changes to be applied
    assert_eq!(doc.in_flight_requests(), vec![1, 2]);

    // Apply a patch corresponding (via actor ID and seq) to the first change
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(1),
        clock: hashmap! {
            doc.actor_id.clone() => 1,
        },
        max_op: 4,
        pending_changes: 0,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::Int(24))
                }
            },
        },
    })
    .unwrap();

    // The doc state should still reflect both local changes as we're still
    // waiting  for the last in flight request to be fulfilled
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => Primitive::Int(24),
            "partridges".to_string() => Primitive::Int(1),
        })
    );
    assert_eq!(doc.in_flight_requests(), vec![2]);

    // Apply a patch corresponding (via actor ID and seq) to the second change
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(2),
        clock: hashmap! {
            doc.actor_id.clone() => 2,
        },
        max_op: 5,
        pending_changes: 0,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "partridges".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::Int(1))
                }
            },
        },
    })
    .unwrap();

    // The doc state should have switched to reconciled
    assert!(doc.in_flight_requests().is_empty());

    // The doc state should still reflect the local changes as they have now
    // been reconciled
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => Primitive::Int(24),
            "partridges".to_string() => Primitive::Int(1),
        })
    );

    assert_eq!(doc.seq, 2);
}

#[test]
fn leave_request_queue_unchanged_on_remote_changes() {
    let remote = amp::ActorId::random();
    let mut doc = Frontend::new();
    // Enqueue a local change, moving the document into the "waiting for in
    // flight requests" state
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                Primitive::Int(24),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    // The document is now waiting for the above request
    assert_eq!(doc.in_flight_requests(), vec![1]);

    // Apply a remote patch (due to actor ID and seq missing)
    doc.apply_patch(amp::Patch {
        actor: None,
        seq: None,
        max_op: 10,
        pending_changes: 0,
        clock: hashmap! {
            remote.clone() => 1,
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "pheasants".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::Int(2))
                }
            },
        },
    })
    .unwrap();

    // The doc state should reflect outstanding in flight request and not the
    // remote patch (because we're still waiting for in flight requests)
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => Primitive::Int(24),
        })
    );
    assert_eq!(doc.in_flight_requests(), vec![1]);

    // Now apply a patch corresponding to the outstanding in flight request
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(1),
        clock: hashmap! {
            doc.actor_id.clone() => 2,
            remote => 1,
        },
        max_op: 11,
        pending_changes: 0,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "blackbirds".into() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::Int(24))
                }
            },
        },
    })
    .unwrap();

    // The doc state should now reflect both the local and remote changes
    // as the doc is now reconciled (all in flight requests have received a
    // patch)
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {
            "blackbirds".to_string() => Primitive::Int(24),
            "pheasants".to_string() => Primitive::Int(2),
        })
    );

    assert!(doc.in_flight_requests().is_empty());
    assert_eq!(doc.seq, 2);
}

#[test]
fn dont_allow_out_of_order_request_patches() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("blackbirds"),
                Primitive::Int(24),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let result = doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(2),
        max_op: 8,
        pending_changes: 0,
        clock: hashmap! {
            doc.actor_id.clone() => 2,
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "partridges".to_string() => hashmap!{
                    random_op_id() => amp::Diff::Value(amp::ScalarValue::Int(1))
                }
            },
        },
    });

    assert_eq!(
        result,
        Err(InvalidPatch::MismatchedSequenceNumber {
            expected: 1,
            actual: 2
        })
    );
}

#[test]
fn handle_concurrent_insertions_into_lists() {
    let mut doc = Frontend::new();
    let _req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("birds"),
                vec!["goldfinch"],
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let birds_id = doc.get_object_id(&Path::root().key("birds")).unwrap();

    // Apply the corresponding backend patch for the above state, document
    // shoudl be reconciled after this
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(1),
        max_op: 1,
        pending_changes: 0,
        clock: hashmap! {
            doc.actor_id.clone() => 1,
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "birds".to_string() => hashmap!{
                    doc.actor_id.op_id_at(1) => amp::Diff::List(amp::ListDiff{
                        object_id: birds_id.clone(),
                        edits: vec![amp::DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: doc.actor_id.op_id_at(1).into(),
                            op_id: doc.actor_id.op_id_at(1),
                            value: amp::Diff::Value("goldfinch".into()),
                        }],
                    })
                }
            },
        },
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
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
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
        .1
        .unwrap();

    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch"]}
        )
    );

    let remote = amp::ActorId::random();

    // Apply a patch which does not take effect because we're still waiting
    // for the in flight requests to be responded to
    doc.apply_patch(amp::Patch {
        clock: hashmap! {
            doc.actor_id.clone() => 1,
            remote.clone() => 1,
        },
        max_op: 3,
        pending_changes: 0,
        actor: None,
        seq: None,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    doc.actor_id.op_id_at(1) => amp::Diff::List(amp::ListDiff{
                        object_id: birds_id.clone(),
                        edits: vec![amp::DiffEdit::SingleElementInsert{
                            index: 1,
                            elem_id: remote.op_id_at(1).into(),
                            op_id: doc.actor_id.op_id_at(1),
                            value: amp::Diff::Value("bullfinch".into()),
                        }],
                    })
                }
            },
        },
    })
    .unwrap();

    // Check that the doc state hasn't been updated yet
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch"]}
        )
    );

    // Now apply a patch acknowledging the in flight request
    doc.apply_patch(amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(2),
        max_op: 3,
        pending_changes: 0,
        clock: hashmap! {
            doc.actor_id.clone() => 2,
            remote => 1,
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "birds".to_string() => hashmap!{
                    doc.actor_id.op_id_at(1) => amp::Diff::List(amp::ListDiff{
                        object_id: birds_id,
                        edits: vec![
                            amp::DiffEdit::SingleElementInsert {
                                index: 0,
                                elem_id: doc.actor_id.op_id_at(2).into(),
                                op_id: doc.actor_id.op_id_at(2),
                                value: amp::Diff::Value("chaffinch".into()),
                            },
                            amp::DiffEdit::SingleElementInsert{
                                index: 2,
                                elem_id: doc.actor_id.op_id_at(3).into(),
                                op_id: doc.actor_id.op_id_at(3),
                                value: amp::Diff::Value("greenfinch".into()),
                            },
                        ],
                    })
                }
            },
        },
    })
    .unwrap();

    assert!(doc.in_flight_requests().is_empty());
    assert_eq!(
        doc.state(),
        &Into::<Value>::into(
            hashmap! {"birds".to_string() => vec!["chaffinch", "goldfinch", "greenfinch", "bullfinch"]}
        )
    )
}

#[test]
fn allow_interleaving_of_patches_and_changes() {
    let mut doc = Frontend::new();
    let req1 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(1),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let req2 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(2),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        req1,
        amp::Change {
            actor_id: doc.actor_id.clone(),
            seq: 1,
            start_op: 1,
            message: None,
            hash: None,
            time: req1.time,
            deps: Vec::new(),
            operations: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Int(1)),
                obj: amp::ObjectId::Root,
                key: "number".into(),
                insert: false,
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
    );

    assert_eq!(
        req2,
        amp::Change {
            actor_id: doc.actor_id.clone(),
            seq: 2,
            start_op: 2,
            message: None,
            hash: None,
            time: req2.time,
            deps: Vec::new(),
            operations: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Int(2)),
                obj: amp::ObjectId::Root,
                key: "number".into(),
                insert: false,
                pred: vec![doc.actor_id.op_id_at(1)],
            }],
            extra_bytes: Vec::new(),
        }
    );

    let mut backend = Backend::new();
    let (patch1, _) = backend.apply_local_change(req1).unwrap();
    doc.apply_patch(patch1).unwrap();

    let req3 = doc
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(3),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    assert_eq!(
        req3,
        amp::Change {
            actor_id: doc.actor_id.clone(),
            seq: 3,
            start_op: 3,
            message: None,
            hash: None,
            time: req3.time,
            deps: Vec::new(),
            operations: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Int(3)),
                obj: amp::ObjectId::Root,
                key: "number".into(),
                insert: false,
                pred: vec![doc.actor_id.op_id_at(2)],
            }],
            extra_bytes: Vec::new(),
        }
    );
}

//it('deps are filled in if the frontend does not have the latest patch', () => {
//const actor1 = uuid(), actor2 = uuid()
//const [doc1, change1] = Frontend.change(Frontend.init(actor1), doc => doc.number = 1)
//const [state1, patch1, binChange1] = Backend.applyLocalChange(Backend.init(), change1)

//const [state1a, patch1a] = Backend.applyChanges(Backend.init(), [binChange1])
//const doc1a = Frontend.applyPatch(Frontend.init(actor2), patch1a)
//const [doc2, change2] = Frontend.change(doc1a, doc => doc.number = 2)
//const [doc3, change3] = Frontend.change(doc2, doc => doc.number = 3)
//assert.deepStrictEqual(change2, {
//actor: actor2, seq: 1, startOp: 2, deps: [decodeChange(binChange1).hash], time: change2.time, message: '', ops: [
//{obj: '_root', action: 'set', key: 'number', insert: false, value: 2, pred: [`1@${actor1}`]}
//]
//})
//assert.deepStrictEqual(change3, {
//actor: actor2, seq: 2, startOp: 3, deps: [], time: change3.time, message: '', ops: [
//{obj: '_root', action: 'set', key: 'number', insert: false, value: 3, pred: [`2@${actor2}`]}
//]
//})

//const [state2, patch2, binChange2] = Backend.applyLocalChange(state1a, change2)
//const [state3, patch3, binChange3] = Backend.applyLocalChange(state2, change3)
//assert.deepStrictEqual(decodeChange(binChange2).deps, [decodeChange(binChange1).hash])
//assert.deepStrictEqual(decodeChange(binChange3).deps, [decodeChange(binChange2).hash])
//assert.deepStrictEqual(patch1a.deps, [decodeChange(binChange1).hash])
//assert.deepStrictEqual(patch2.deps, [])

//const doc2a = Frontend.applyPatch(doc3, patch2)
//const doc3a = Frontend.applyPatch(doc2a, patch3)
//const [doc4, change4] = Frontend.change(doc3a, doc => doc.number = 4)
//assert.deepStrictEqual(change4, {
//actor: actor2, seq: 3, startOp: 4, time: change4.time, message: '', deps: [], ops: [
//{obj: '_root', action: 'set', key: 'number', insert: false, value: 4, pred: [`3@${actor2}`]}
//]
//})
//const [state4, patch4, binChange4] = Backend.applyLocalChange(state3, change4)
//assert.deepStrictEqual(decodeChange(binChange4).deps, [decodeChange(binChange3).hash])
//})
#[test]
fn test_deps_are_filled_in_if_frontend_does_not_have_latest_patch() {
    let (doc, change1) =
        Frontend::new_with_initial_state(hashmap! {"number" => Primitive::Int(1)}.into()).unwrap();

    let mut backend1 = Backend::new();
    let (_, binchange1) = backend1.apply_local_change(change1).unwrap();

    let mut doc2 = Frontend::new();
    let mut backend2 = Backend::new();
    let patch1 = backend2.apply_changes(vec![binchange1.clone()]).unwrap();
    doc2.apply_patch(patch1.clone()).unwrap();

    let change2 = doc2
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(2),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let change3 = doc2
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(3),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let expected_change2 = amp::Change {
        actor_id: doc2.actor_id.clone(),
        start_op: 2,
        seq: 1,
        time: change2.time,
        message: None,
        hash: None,
        deps: vec![binchange1.hash],
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::from(2)),
            obj: amp::ObjectId::Root,
            key: "number".into(),
            insert: false,
            pred: vec![doc.actor_id.op_id_at(1)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change2, expected_change2);

    let expected_change3 = amp::Change {
        actor_id: doc2.actor_id.clone(),
        start_op: 3,
        seq: 2,
        time: change3.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::from(3)),
            obj: amp::ObjectId::Root,
            key: "number".into(),
            insert: false,
            pred: vec![doc2.actor_id.op_id_at(2)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change3, expected_change3);

    let (patch2, binchange2) = backend2.apply_local_change(change2).unwrap();
    let (patch3, binchange3) = backend2.apply_local_change(change3).unwrap();

    assert_eq!(binchange2.deps, vec![binchange1.hash]);
    assert_eq!(binchange3.deps, vec![binchange2.hash]);
    assert_eq!(patch1.deps, vec![binchange1.hash]);
    assert_eq!(patch2.deps, Vec::new());

    doc2.apply_patch(patch2).unwrap();
    doc2.apply_patch(patch3).unwrap();

    let change4 = doc2
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("number"),
                Primitive::Int(4),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();

    let expected_change4 = amp::Change {
        actor_id: doc2.actor_id.clone(),
        start_op: 4,
        seq: 3,
        time: change4.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![amp::Op {
            action: amp::OpType::Set(amp::ScalarValue::from(4)),
            obj: amp::ObjectId::Root,
            key: "number".into(),
            insert: false,
            pred: vec![doc2.actor_id.op_id_at(3)],
        }],
        extra_bytes: Vec::new(),
    };
    assert_eq!(change4, expected_change4);
}
