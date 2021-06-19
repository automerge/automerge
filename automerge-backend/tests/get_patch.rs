extern crate automerge_backend;
use std::{convert::TryInto, num::NonZeroU32};

use amp::{RootDiff, SortedVec};
use automerge_backend::{Backend, Change};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorId, Diff, DiffEdit, ElementId, ListDiff, MapDiff, ObjectId, Op, Patch, ScalarValue,
};
use maplit::hashmap;
use pretty_assertions::assert_eq;

#[test]
fn test_include_most_recent_value_for_key() {
    let actor: ActorId = "ec28cfbcdb9e4f32ad24b3c776e651b0".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            obj: ObjectId::Root,
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set("blackbird".into()),
            key: "bird".into(),
            pred: vec![actor.op_id_at(1)].into(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        clock: hashmap! {
            actor.clone() => 2,
        },
        deps: vec![change2.hash],
        diffs: RootDiff {
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor.op_id_at(2) => Diff::Value("blackbird".into()),
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_conflicting_values_for_key() {
    let actor1: ActorId = "111111".try_into().unwrap();
    let actor2: ActorId = "222222".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectId::Root,
            key: "bird".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = amp::Change {
        actor_id: actor2.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("blackbird".into()),
            key: "bird".into(),
            obj: ObjectId::Root,
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor1.clone() => 1,
            actor2.clone() => 1,
        },
        max_op: 1,
        pending_changes: 0,
        seq: None,
        actor: None,
        deps: vec![change1.hash, change2.hash],
        diffs: RootDiff {
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor1.op_id_at(1) => Diff::Value("magpie".into()),
                    actor2.op_id_at(1) => Diff::Value("blackbird".into()),
                },
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handles_counter_increment_at_keys_in_a_map() {
    let actor: ActorId = "46c92088e4484ae5945dc63bf606a4a5".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Counter(1)),
            obj: ObjectId::Root,
            key: "counter".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Inc(2),
            obj: ObjectId::Root,
            key: "counter".into(),
            pred: vec![actor.op_id_at(1)].into(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        seq: None,
        actor: None,
        clock: hashmap! {
            actor.clone() => 2,
        },
        max_op: 2,
        pending_changes: 0,
        deps: vec![change2.hash],
        diffs: RootDiff {
            props: hashmap! {
                "counter".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Counter(3))
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_creates_nested_maps() {
    let actor: ActorId = "06148f9422cb40579fd02f1975c34a51".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::Map),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::F64(3.0)),
                key: "wrens".into(),
                obj: ObjectId::from(actor.op_id_at(1)),
                pred: SortedVec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        hash: None,
        operations: vec![
            Op {
                obj: ObjectId::from(actor.op_id_at(1)),
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                key: "wrens".into(),
                pred: vec![actor.op_id_at(2)].into(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(1)),
                action: amp::OpType::Set(ScalarValue::F64(15.0)),
                key: "sparrows".into(),
                pred: SortedVec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 2,
        },
        actor: None,
        seq: None,
        max_op: 4,
        pending_changes: 0,
        deps: vec![change2.hash],
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Map(MapDiff{
                        object_id: ObjectId::from(actor.op_id_at(1)),
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                actor.op_id_at(4) => Diff::Value(ScalarValue::F64(15.0))
                            }
                        }
                    })
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let actor: ActorId = "90bf7df682f747fa82ac604b35010906".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::List),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(1)),
                action: amp::OpType::Set("chaffinch".into()),
                key: ElementId::Head.into(),
                insert: true,
                pred: SortedVec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1,
        },
        max_op: 2,
        pending_changes: 0,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::List(ListDiff{
                        object_id: ObjectId::from(actor.op_id_at(1)),
                        edits: vec![DiffEdit::SingleElementInsert {
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Value("chaffinch".into()),
                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_latests_state_of_list() {
    let actor: ActorId = "6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::List),
                obj: ObjectId::Root,
                key: "todos".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Make(amp::ObjType::Map),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                insert: true,
                pred: SortedVec::new(),
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(2)),
                action: amp::OpType::Set("water plants".into()),
                key: "title".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(2)),
                action: amp::OpType::Set(false.into()),
                key: "done".into(),
                pred: SortedVec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1
        },
        max_op: 4,
        pending_changes: 0,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: RootDiff {
            props: hashmap! {
                "todos".into() => hashmap!{
                    actor.op_id_at(1) => Diff::List(ListDiff{
                        object_id: ObjectId::from(actor.op_id_at(1)),
                        edits: vec![DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Map(MapDiff{
                                object_id: actor.op_id_at(2).into(),
                                props: hashmap!{
                                    "title".into() => hashmap!{
                                        actor.op_id_at(3) => Diff::Value("water plants".into()),
                                    },
                                    "done".into() => hashmap!{
                                        actor.op_id_at(4) => Diff::Value(false.into())
                                    }
                                }
                            })
                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_date_objects_at_root() {
    let actor: ActorId = "90f5dd5d4f524e95ad5929e08d1194f1".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set(ScalarValue::Timestamp(1_586_541_033_457)),
            key: "now".into(),
            pred: SortedVec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1,
        },
        max_op: 1,
        pending_changes: 0,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: RootDiff {
            props: hashmap! {
                "now".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Timestamp(1_586_541_033_457))
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_date_objects_in_a_list() {
    let actor: ActorId = "08b050f976a249349021a2e63d99c8e8".try_into().unwrap();
    let change1: Change = amp::Change {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                obj: ObjectId::Root,
                action: amp::OpType::Make(amp::ObjType::List),
                key: "list".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(1)),
                action: amp::OpType::Set(ScalarValue::Timestamp(1_586_541_089_595)),
                key: ElementId::Head.into(),
                insert: true,
                pred: SortedVec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1,
        },
        max_op: 2,
        pending_changes: 0,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: RootDiff {
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::List(ListDiff{
                        object_id: ObjectId::from(actor.op_id_at(1)),
                        edits: vec![DiffEdit::SingleElementInsert {
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Value(ScalarValue::Timestamp(1_586_541_089_595)),
                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::new();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_updates_for_conflicting_list_elements() {
    let local_actor = ActorId::random();
    let actor1: ActorId = "da45d93f2b18456f8318c723d1430563".try_into().unwrap();
    let actor2: ActorId = "6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap();
    let local_change = amp::Change {
        actor_id: local_actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::List),
                obj: ObjectId::Root,
                key: "list".into(),
                pred: SortedVec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("local".into()),
                obj: local_actor.op_id_at(1).into(),
                key: amp::ElementId::Head.into(),
                pred: SortedVec::new(),
                insert: true,
            },
        ],
        extra_bytes: Vec::new(),
    };
    let binchange: Change = local_change.clone().try_into().unwrap();

    let remote_change_1: Change = amp::Change {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: vec![binchange.hash],
        extra_bytes: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("remote1".into()),
            obj: local_actor.op_id_at(1).into(),
            key: local_actor.op_id_at(2).into(),
            pred: vec![local_actor.op_id_at(2)].into(),
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let remote_change_2: Change = amp::Change {
        actor_id: actor2.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: vec![binchange.hash],
        extra_bytes: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set("remote2".into()),
            obj: local_actor.op_id_at(1).into(),
            key: local_actor.op_id_at(2).into(),
            pred: vec![local_actor.op_id_at(2)].into(),
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let mut deps = vec![remote_change_2.hash, remote_change_1.hash];
    deps.sort();

    let expected_patch = Patch {
        clock: hashmap! {
            local_actor.clone() => 1,
            actor1.clone() => 1,
            actor2.clone() => 1,
        },
        max_op: 2,
        actor: None,
        seq: None,
        deps,
        diffs: RootDiff {
            props: hashmap! {
                "list".into() => hashmap!{
                    local_actor.op_id_at(1) => Diff::List(ListDiff{
                        object_id: ObjectId::from(local_actor.op_id_at(1)),
                        edits: vec![
                            DiffEdit::SingleElementInsert{
                                index: 0,
                                elem_id: local_actor.op_id_at(2).into(),
                                op_id: actor1.op_id_at(1),
                                value: Diff::Value("remote1".into()),
                            },
                            DiffEdit::Update{
                                index: 0,
                                op_id: actor2.op_id_at(1),
                                value: Diff::Value("remote2".into())
                            },
                        ],
                    })
                }
            },
        },
        pending_changes: 0,
    };

    let mut backend = Backend::new();
    backend.apply_local_change(local_change).unwrap();
    backend
        .load_changes(vec![remote_change_1, remote_change_2])
        .unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}
