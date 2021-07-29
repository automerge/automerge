extern crate automerge_backend;
use std::{
    collections::HashSet,
    convert::TryInto,
    num::{NonZeroU32, NonZeroU64},
};

use amp::{RootDiff, SortedVec};
use automerge_backend::{Backend, Change};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorId, ChangeHash, Diff, DiffEdit, ElementId, ListDiff, ObjType, ObjectId, Op, OpType, Patch,
};
use maplit::hashmap;

#[test]
fn test_apply_local_change() {
    let actor: ActorId = "eb738e04ef8848ce8b77309b6c7f7e39".try_into().unwrap();
    let change_request = amp::Change {
        actor_id: actor.clone(),
        time: 0,
        message: None,
        hash: None,
        seq: NonZeroU64::new(1).unwrap(),
        deps: Vec::new(),
        start_op: NonZeroU64::new(1).unwrap(),
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            obj: ObjectId::Root,
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    let mut backend = Backend::new();
    let patch = backend.apply_local_change(change_request).unwrap().0;

    let changes = backend.get_changes(&[]);
    let expected_change = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: changes[0].time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: OpType::Set("magpie".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();
    assert_eq!(changes[0], &expected_change);

    let expected_patch = Patch {
        actor: Some(actor.clone()),
        max_op: 1,
        pending_changes: 0,
        seq: Some(NonZeroU64::new(1).unwrap()),
        clock: hashmap! {
            actor => NonZeroU64::new(1).unwrap(),
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "bird".into() => hashmap!{
                    "1@eb738e04ef8848ce8b77309b6c7f7e39".try_into().unwrap() => Diff::Value("magpie".into())
                }
            },
        },
    };
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_error_on_duplicate_requests() {
    let actor: ActorId = "37704788917a499cb0206fa8519ac4d9".try_into().unwrap();
    let change_request1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        message: None,
        hash: None,
        time: 0,
        deps: Vec::new(),
        start_op: NonZeroU64::new(1).unwrap(),
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    let change_request2 = amp::Change {
        actor_id: actor,
        seq: NonZeroU64::new(2).unwrap(),
        message: None,
        hash: None,
        time: 0,
        deps: Vec::new(),
        start_op: NonZeroU64::new(2).unwrap(),
        operations: vec![Op {
            action: amp::OpType::Set("jay".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let mut backend = Backend::new();
    backend.apply_local_change(change_request1.clone()).unwrap();
    backend.apply_local_change(change_request2.clone()).unwrap();
    assert!(backend.apply_local_change(change_request1).is_err());
    assert!(backend.apply_local_change(change_request2).is_err());
}

#[test]
fn test_handle_concurrent_frontend_and_backend_changes() {
    let actor: ActorId = "cb55260e9d7e457886a4fc73fd949202".try_into().unwrap();
    let local1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        start_op: NonZeroU64::new(1).unwrap(),
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    let local2 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        start_op: NonZeroU64::new(2).unwrap(),
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set("jay".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            insert: false,
            pred: vec![actor.op_id_at(NonZeroU64::new(1).unwrap())].into(),
        }],
        extra_bytes: Vec::new(),
    };
    let remote_actor: ActorId = "6d48a01318644eed90455d2cb68ac657".try_into().unwrap();
    let remote1 = amp::Change {
        actor_id: remote_actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set("goldfish".into()),
            obj: ObjectId::Root,
            key: "fish".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let mut expected_change1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    };

    let mut expected_change2 = amp::Change {
        actor_id: remote_actor,
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("goldfish".into()),
            key: "fish".into(),
            obj: ObjectId::Root,
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    };

    let mut expected_change3 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        start_op: NonZeroU64::new(2).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("jay".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            pred: vec![actor.op_id_at(NonZeroU64::new(1).unwrap())].into(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    };
    let mut backend = Backend::new();
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

    assert_eq!(change01, &&expected_change1.try_into().unwrap());
    assert_eq!(change12, &expected_change2.try_into().unwrap());
    assert_changes_equal(change23.decode(), expected_change3.clone());
    assert_eq!(change23, &&expected_change3.try_into().unwrap());
}

#[test]
fn test_transform_list_indexes_into_element_ids() {
    let actor: ActorId = "8f389df8fecb4ddc989102321af3578e".try_into().unwrap();
    let remote_actor: ActorId = "9ba21574dc44411b8ce37bc6037a9687".try_into().unwrap();
    let remote1: Change = amp::Change {
        actor_id: remote_actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(ObjType::List),
            key: "birds".into(),
            obj: ObjectId::Root,
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let remote2: Change = amp::Change {
        actor_id: remote_actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        start_op: NonZeroU64::new(2).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: vec![remote1.hash],
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
            key: ElementId::Head.into(),
            insert: true,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let local1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        message: None,
        hash: None,
        time: 0,
        deps: vec![remote1.hash],
        start_op: NonZeroU64::new(2).unwrap(),
        operations: vec![Op {
            obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
            action: amp::OpType::Set("goldfinch".into()),
            key: ElementId::Head.into(),
            insert: true,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let local2 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        message: None,
        hash: None,
        deps: Vec::new(),
        time: 0,
        start_op: NonZeroU64::new(3).unwrap(),
        operations: vec![Op {
            obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
            action: amp::OpType::Set("wagtail".into()),
            key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
            insert: true,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    let local3 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(3).unwrap(),
        message: None,
        hash: None,
        deps: vec![remote2.hash],
        time: 0,
        start_op: NonZeroU64::new(4).unwrap(),
        operations: vec![
            Op {
                obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("Magpie".into()),
                key: remote_actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                insert: false,
                pred: vec![remote_actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
            },
            Op {
                obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("Goldfinch".into()),
                key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                insert: false,
                pred: vec![actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
            },
        ],
        extra_bytes: Vec::new(),
    };

    let mut expected_change1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(2).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: vec![remote1.hash],
        operations: vec![Op {
            obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
            action: amp::OpType::Set("goldfinch".into()),
            key: ElementId::Head.into(),
            insert: true,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let mut expected_change2 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        start_op: NonZeroU64::new(3).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
            action: amp::OpType::Set("wagtail".into()),
            key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
            insert: true,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let mut expected_change3 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(3).unwrap(),
        start_op: NonZeroU64::new(4).unwrap(),
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("Magpie".into()),
                key: remote_actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                pred: vec![remote_actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(remote_actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("Goldfinch".into()),
                key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                pred: vec![actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    };

    let mut backend = Backend::new();
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

    assert_changes_equal(change34, expected_change3);
    assert_eq!(change12, &expected_change1.try_into().unwrap());
    assert_changes_equal(change23.decode(), expected_change2.clone());
    assert_eq!(change23, &expected_change2.try_into().unwrap());
}

#[test]
fn test_handle_list_insertion_and_deletion_in_same_change() {
    let actor: ActorId = "0723d2a1940744868ffd6b294ada813f".try_into().unwrap();
    let local1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        message: None,
        hash: None,
        time: 0,
        deps: Vec::new(),
        start_op: NonZeroU64::new(1).unwrap(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Make(ObjType::List),
            key: "birds".into(),
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    };

    let local2 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        message: None,
        hash: None,
        time: 0,
        deps: Vec::new(),
        start_op: NonZeroU64::new(2).unwrap(),
        operations: vec![
            Op {
                obj: ObjectId::from(actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("magpie".into()),
                key: ElementId::Head.into(),
                insert: true,
                pred: SortedVec::new(),
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: OpType::Del(NonZeroU32::new(1).unwrap()),
                key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                insert: false,
                pred: vec![actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
            },
        ],
        extra_bytes: Vec::new(),
    };

    let mut expected_patch = Patch {
        actor: Some(actor.clone()),
        seq: Some(NonZeroU64::new(2).unwrap()),
        max_op: 3,
        pending_changes: 0,
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap()
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => Diff::List(ListDiff{
                        object_id: ObjectId::from(actor.op_id_at(NonZeroU64::new(1).unwrap())),
                        edits: vec![
                            DiffEdit::SingleElementInsert{
                                index: 0,
                                elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: Diff::Value("magpie".into()),
                            },
                            DiffEdit::Remove{index: 0, count: NonZeroU64::new(1).unwrap()},
                        ],
                    })
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.apply_local_change(local1).unwrap();
    let patch = backend.apply_local_change(local2).unwrap().0;
    expected_patch.deps = patch.deps.clone();
    assert_eq!(patch, expected_patch);

    let changes = backend.get_changes(&[]);
    assert_eq!(changes.len(), 2);
    let change1 = changes[0].clone();
    let change2 = changes[1].clone();

    let expected_change1 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(1).unwrap(),
        start_op: NonZeroU64::new(1).unwrap(),
        time: change1.time,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Make(ObjType::List),
            key: "birds".into(),
            insert: false,
            pred: SortedVec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_change2 = amp::Change {
        actor_id: actor.clone(),
        seq: NonZeroU64::new(2).unwrap(),
        start_op: NonZeroU64::new(2).unwrap(),
        time: change2.time,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![
            Op {
                obj: ObjectId::from(actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: amp::OpType::Set("magpie".into()),
                key: ElementId::Head.into(),
                insert: true,
                pred: SortedVec::new(),
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(NonZeroU64::new(1).unwrap())),
                action: OpType::Del(NonZeroU32::new(1).unwrap()),
                key: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                pred: vec![actor.op_id_at(NonZeroU64::new(2).unwrap())].into(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    assert_eq!(change1, expected_change1);
    assert_eq!(change2, expected_change2);
}

/// Asserts that the changes are equal without respect to order of the hashes
/// in the change dependencies
fn assert_changes_equal(mut change1: amp::Change, change2: amp::Change) {
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
