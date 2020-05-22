extern crate automerge_backend;
use automerge_backend::{Backend, UnencodedChange};
use automerge_backend::{OpType, Operation};
use automerge_protocol::{
    ActorID, ChangeHash, ChangeRequest, ChangeRequestType, DataType, Diff, DiffEdit, ElementID,
    MapDiff, ObjType, ObjectID, OpRequest, Patch, ReqOpType, SeqDiff,
};
use maplit::hashmap;
use std::convert::TryInto;
use std::{collections::HashSet, str::FromStr};

#[test]
fn test_apply_local_change() {
    let actor: ActorID = "eb738e04ef8848ce8b77309b6c7f7e39".try_into().unwrap();
    let change_request = ChangeRequest {
        actor: actor.clone(),
        seq: 1,
        version: 0,
        message: None,
        undoable: false,
        time: None,
        deps: None,
        ops: Some(vec![OpRequest {
            action: ReqOpType::Set,
            value: Some("magpie".into()),
            datatype: Some(DataType::Undefined),
            key: "bird".into(),
            obj: ObjectID::Root.to_string(),
            child: None,
            insert: false,
        }]),
        request_type: ChangeRequestType::Change,
    };

    let mut backend = Backend::init();
    let patch = backend.apply_local_change(change_request).unwrap();

    let changes = backend.get_changes(&[]);
    let expected_change = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: changes[0].time,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set("magpie".into()),
            obj: ObjectID::Root,
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();
    assert_eq!(changes[0], &expected_change);

    let expected_patch = Patch {
        actor: Some(actor.to_string()),
        seq: Some(1),
        version: 1,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        deps: vec![changes[0].hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    "1@eb738e04ef8848ce8b77309b6c7f7e39".into() => Diff::Value("magpie".into())
                }
            },
        })),
    };
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_error_on_duplicate_requests() {
    let actor: ActorID = "37704788917a499cb0206fa8519ac4d9".try_into().unwrap();
    let change_request1 = ChangeRequest {
        actor: actor.clone(),
        seq: 1,
        version: 0,
        message: None,
        undoable: false,
        time: None,
        deps: None,
        ops: Some(vec![OpRequest {
            action: ReqOpType::Set,
            obj: ObjectID::Root.to_string(),
            key: "bird".into(),
            child: None,
            value: Some("magpie".into()),
            datatype: Some(DataType::Undefined),
            insert: false,
        }]),
        request_type: ChangeRequestType::Change,
    };

    let change_request2 = ChangeRequest {
        actor,
        seq: 2,
        version: 0,
        message: None,
        undoable: false,
        time: None,
        deps: None,
        ops: Some(vec![OpRequest {
            action: ReqOpType::Set,
            obj: ObjectID::Root.to_string(),
            key: "bird".into(),
            value: Some("jay".into()),
            child: None,
            insert: false,
            datatype: Some(DataType::Undefined),
        }]),
        request_type: ChangeRequestType::Change,
    };
    let mut backend = Backend::init();
    backend.apply_local_change(change_request1.clone()).unwrap();
    backend.apply_local_change(change_request2.clone()).unwrap();
    assert!(backend.apply_local_change(change_request1).is_err());
    assert!(backend.apply_local_change(change_request2).is_err());
}

#[test]
fn test_handle_concurrent_frontend_and_backend_changes() {
    let actor: ActorID = "cb55260e9d7e457886a4fc73fd949202".try_into().unwrap();
    let local1 = ChangeRequest {
        actor: actor.clone(),
        seq: 1,
        version: 0,
        time: None,
        deps: None,
        message: None,
        undoable: false,
        request_type: ChangeRequestType::Change,
        ops: Some(vec![OpRequest {
            action: ReqOpType::Set,
            obj: ObjectID::Root.to_string(),
            key: "bird".into(),
            value: Some("magpie".into()),
            child: None,
            datatype: Some(DataType::Undefined),
            insert: false,
        }]),
    };

    let local2 = ChangeRequest {
        actor: actor.clone(),
        seq: 2,
        version: 0,
        time: None,
        deps: None,
        message: None,
        request_type: ChangeRequestType::Change,
        undoable: false,
        ops: Some(vec![OpRequest {
            action: ReqOpType::Set,
            obj: ObjectID::Root.to_string(),
            key: "bird".into(),
            value: Some("jay".into()),
            child: None,
            datatype: Some(DataType::Undefined),
            insert: false,
        }]),
    };
    let remote_actor: ActorID = "6d48a01318644eed90455d2cb68ac657".try_into().unwrap();
    let remote1 = UnencodedChange {
        actor_id: remote_actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Operation {
            action: OpType::Set("goldfish".into()),
            obj: ObjectID::Root,
            key: "fish".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let mut expected_change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set("magpie".into()),
            obj: ObjectID::Root,
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
    };

    let mut expected_change2 = UnencodedChange {
        actor_id: remote_actor,
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set("goldfish".into()),
            key: "fish".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
    };

    let mut expected_change3 = UnencodedChange {
        actor_id: actor,
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set("jay".into()),
            obj: ObjectID::Root,
            key: "bird".into(),
            pred: vec!["1@cb55260e9d7e457886a4fc73fd949202".try_into().unwrap()],
            insert: false,
        }],
    };
    let mut backend = Backend::init();
    backend.apply_local_change(local1).unwrap();
    let backend_after_first = backend.clone();
    let changes1 = backend_after_first.get_changes(&[]);
    let change01 = changes1.get(0).unwrap();

    backend.apply_changes(vec![remote1]).unwrap();
    let backend_after_second = backend.clone();
    let changes2 = backend_after_second.get_changes(&[change01.hash]);
    let change12 = *changes2.get(0).unwrap();

    backend.apply_local_change(local2).unwrap();
    let changes3 = backend.get_changes(&[change01.hash, change12.hash]);
    let change23 = changes3.get(0).unwrap();

    expected_change1.time = change01.time;
    expected_change2.time = change12.time;
    expected_change3.time = change23.time;
    expected_change3.deps = vec![change01.hash];

    assert_eq!(change01, &&expected_change1.encode());
    assert_eq!(change12, &expected_change2.encode());
    assert_eq!(change23, &&expected_change3.encode());
}

#[test]
fn test_transform_list_indexes_into_element_ids() {
    let actor: ActorID = "8f389df8fecb4ddc989102321af3578e".try_into().unwrap();
    let remote_actor: ActorID = "9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap();
    let remote1 = UnencodedChange {
        actor_id: remote_actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Make(ObjType::List),
            key: "birds".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let remote2 = UnencodedChange {
        actor_id: remote_actor,
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        deps: vec![remote1.hash],
        operations: vec![Operation {
            action: OpType::Set("magpie".into()),
            obj: "1@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap(),
            key: ElementID::Head.into(),
            insert: true,
            pred: Vec::new(),
        }],
    }
    .encode();

    let local1 = ChangeRequest {
        actor: actor.clone(),
        seq: 1,
        version: 1,
        message: None,
        time: None,
        deps: None,
        undoable: false,
        request_type: ChangeRequestType::Change,
        ops: Some(vec![OpRequest {
            obj: "1@9ba21574dc44411b8ce37bc6037a9687".into(),
            action: ReqOpType::Set,
            value: Some("goldfinch".into()),
            key: 0.into(),
            datatype: Some(DataType::Undefined),
            insert: true,
            child: None,
        }]),
    };
    let local2 = ChangeRequest {
        actor: actor.clone(),
        seq: 2,
        version: 1,
        message: None,
        deps: None,
        time: None,
        undoable: false,
        request_type: ChangeRequestType::Change,
        ops: Some(vec![OpRequest {
            obj: "1@9ba21574dc44411b8ce37bc6037a9687".into(),
            action: ReqOpType::Set,
            value: Some("wagtail".into()),
            key: 1.into(),
            insert: true,
            datatype: Some(DataType::Undefined),
            child: None,
        }]),
    };

    let local3 = ChangeRequest {
        actor: actor.clone(),
        seq: 3,
        version: 4,
        message: None,
        deps: None,
        time: None,
        undoable: false,
        request_type: ChangeRequestType::Change,
        ops: Some(vec![
            OpRequest {
                obj: "1@9ba21574dc44411b8ce37bc6037a9687".into(),
                action: ReqOpType::Set,
                key: 0.into(),
                value: Some("Magpie".into()),
                insert: false,
                child: None,
                datatype: Some(DataType::Undefined),
            },
            OpRequest {
                obj: "1@9ba21574dc44411b8ce37bc6037a9687".into(),
                action: ReqOpType::Set,
                key: 1.into(),
                value: Some("Goldfinch".into()),
                child: None,
                insert: false,
                datatype: Some(DataType::Undefined),
            },
        ]),
    };

    let mut expected_change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 2,
        time: 0,
        message: None,
        deps: vec![remote1.hash],
        operations: vec![Operation {
            obj: "1@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap(),
            action: OpType::Set("goldfinch".into()),
            key: ElementID::Head.into(),
            insert: true,
            pred: Vec::new(),
        }],
    };
    let mut expected_change2 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            obj: "1@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap(),
            action: OpType::Set("wagtail".into()),
            key: ElementID::from_str("2@8f389df8fecb4ddc989102321af3578e")
                .unwrap()
                .into(),
            insert: true,
            pred: Vec::new(),
        }],
    };
    let mut expected_change3 = UnencodedChange {
        actor_id: actor,
        seq: 3,
        start_op: 4,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Operation {
                obj: "1@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap(),
                action: OpType::Set("Magpie".into()),
                key: ElementID::from_str("2@9ba21574dc44411b8ce37bc6037a9687")
                    .unwrap()
                    .into(),
                pred: vec!["2@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap()],
                insert: false,
            },
            Operation {
                obj: "1@9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap(),
                action: OpType::Set("Goldfinch".into()),
                key: ElementID::from_str("2@8f389df8fecb4ddc989102321af3578e")
                    .unwrap()
                    .into(),
                pred: vec!["2@8f389df8fecb4ddc989102321af3578e".try_into().unwrap()],
                insert: false,
            },
        ],
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![remote1.clone()]).unwrap();
    backend.apply_local_change(local1).unwrap();
    let backend_after_first = backend.clone();
    let changes1 = backend_after_first.get_changes(&[remote1.hash]);
    let change12 = *changes1.get(0).unwrap();

    backend.apply_changes(vec![remote2.clone()]).unwrap();
    backend.apply_local_change(local2).unwrap();
    let backend_after_second = backend.clone();
    let changes2 = backend_after_second.get_changes(&[remote2.hash, change12.hash]);
    let change23 = *changes2.get(0).unwrap();

    backend.apply_local_change(local3).unwrap();
    let changes3 = backend.get_changes(&[remote2.hash, change23.hash]);
    let change34 = changes3.get(0).unwrap().decode();

    expected_change1.time = change12.time;
    expected_change2.time = change23.time;
    expected_change2.deps = vec![change12.hash];
    expected_change3.time = change34.time;
    expected_change3.deps = vec![remote2.hash, change23.hash];

    assert_eq!(change12, &expected_change1.encode());
    assert_eq!(change23, &expected_change2.encode());
    assert_changes_equal(change34, expected_change3);
}

#[test]
fn test_handle_list_insertion_and_deletion_in_same_change() {
    let actor: ActorID = "0723d2a1940744868ffd6b294ada813f".try_into().unwrap();
    let local1 = ChangeRequest {
        actor: actor.clone(),
        seq: 1,
        version: 0,
        request_type: ChangeRequestType::Change,
        message: None,
        time: None,
        undoable: false,
        deps: None,
        ops: Some(vec![OpRequest {
            obj: ObjectID::Root.to_string(),
            action: ReqOpType::MakeList,
            key: "birds".into(),
            child: None,
            datatype: None,
            value: None,
            insert: false,
        }]),
    };

    let local2 = ChangeRequest {
        actor: actor.clone(),
        seq: 2,
        version: 0,
        request_type: ChangeRequestType::Change,
        message: None,
        time: None,
        undoable: false,
        deps: None,
        ops: Some(vec![
            OpRequest {
                obj: "1@0723d2a1940744868ffd6b294ada813f".into(),
                action: ReqOpType::Set,
                key: 0.into(),
                insert: true,
                value: Some("magpie".into()),
                child: None,
                datatype: Some(DataType::Undefined),
            },
            OpRequest {
                obj: "1@0723d2a1940744868ffd6b294ada813f".into(),
                action: ReqOpType::Del,
                key: 0.into(),
                child: None,
                insert: false,
                value: None,
                datatype: None,
            },
        ]),
    };

    let mut expected_patch = Patch {
        actor: Some(actor.to_string()),
        seq: Some(2),
        version: 2,
        clock: hashmap! {
            "0723d2a1940744868ffd6b294ada813f".into() => 2
        },
        can_undo: false,
        can_redo: false,
        deps: Vec::new(),
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@0723d2a1940744868ffd6b294ada813f".into() => Diff::Seq(SeqDiff{
                        object_id: "1@0723d2a1940744868ffd6b294ada813f".into(),
                        obj_type: ObjType::List,
                        edits: vec![
                            DiffEdit::Insert{index: 0},
                            DiffEdit::Remove{index: 0},
                        ],
                        props: hashmap!{},
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.apply_local_change(local1).unwrap();
    let patch = backend.apply_local_change(local2).unwrap();
    expected_patch.deps = patch.deps.clone();
    assert_eq!(patch, expected_patch);

    let changes = backend.get_changes(&[]);
    assert_eq!(changes.len(), 2);
    let change1 = changes[0].clone();
    let change2 = changes[1].clone();

    let expected_change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: change1.time,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            obj: ObjectID::Root,
            action: OpType::Make(ObjType::List),
            key: "birds".into(),
            insert: false,
            pred: Vec::new(),
        }],
    }
    .encode();

    let expected_change2 = UnencodedChange {
        actor_id: actor,
        seq: 2,
        start_op: 2,
        time: change2.time,
        message: None,
        deps: vec![change1.hash],
        operations: vec![
            Operation {
                obj: "1@0723d2a1940744868ffd6b294ada813f".try_into().unwrap(),
                action: OpType::Set("magpie".into()),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Operation {
                obj: "1@0723d2a1940744868ffd6b294ada813f".try_into().unwrap(),
                action: OpType::Del,
                key: ElementID::from_str("2@0723d2a1940744868ffd6b294ada813f")
                    .unwrap()
                    .into(),
                pred: vec!["2@0723d2a1940744868ffd6b294ada813f".try_into().unwrap()],
                insert: false,
            },
        ],
    }
    .encode();

    assert_eq!(change1, expected_change1);
    assert_eq!(change2, expected_change2);
}

/// Asserts that the changes are equal without respect to order of the hashes
/// in the change dependencies
fn assert_changes_equal(mut change1: UnencodedChange, change2: UnencodedChange) {
    let change2_clone = change2.clone();
    let deps1: HashSet<&ChangeHash> = change1.deps.iter().collect();
    let deps2: HashSet<&ChangeHash> = change2.deps.iter().collect();
    assert_eq!(
        deps1, deps2,
        "The two changes did not have equal dependencies, left: {:?}, right: {:?}",
        deps1, deps2
    );
    change1.deps = change2.deps;
    assert_eq!(change1, change2_clone)
}
