extern crate automerge_backend;
use std::{convert::TryInto, num::NonZeroU32, str::FromStr};

use automerge_backend::{AutomergeError, Backend, Change};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorId, CursorDiff, Diff, DiffEdit, ElementId, MapDiff, MapType, ObjectId, Op, Patch,
    ScalarValue, SeqDiff, SequenceType, UncompressedChange,
};
use maplit::hashmap;
use pretty_assertions::assert_eq;

#[test]
fn test_incremental_diffs_in_a_map() {
    let actor: ActorId = "7b7723afd9e6480397a4d467b7693156".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change.clone()]).unwrap();
    let expected_patch = Patch {
        actor: None,
        seq: None,
        deps: vec![change.hash],
        clock: hashmap! {actor.clone() => 1},
        max_op: 1,
        pending_changes: 0,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap!( "bird".into() => hashmap!( actor.op_id_at(1) => "magpie".into() )),
        },
    };
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_increment_key_in_map() {
    let actor: ActorId = "cdee6963c1664645920be8b41a933c2b".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set(ScalarValue::Counter(1)),
            key: "counter".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 2,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Inc(2),
            key: "counter".into(),
            insert: false,
            pred: vec![actor.op_id_at(1)],
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        clock: hashmap! {actor.clone() => 2},
        max_op: 2,
        pending_changes: 0,
        deps: vec![change2.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap!(
            "counter".into() => hashmap!{
                actor.op_id_at(1) =>  ScalarValue::Counter(3).into(),
            }),
        },
    };
    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_conflict_on_assignment_to_same_map_key() {
    let actor_1 = ActorId::from_str("ac11").unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor_1.clone(),
        seq: 1,
        message: None,
        hash: None,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let actor_2 = ActorId::from_str("ac22").unwrap();
    let change2: Change = UncompressedChange {
        actor_id: actor_2.clone(),
        start_op: 2,
        seq: 1,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        time: 0,
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set("blackbird".into()),
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        clock: hashmap! {
            actor_1.clone() => 1,
            actor_2.clone() => 1,
        },
        deps: vec![change2.hash],
        max_op: 2,
        pending_changes: 0,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor_1.op_id_at(1) => "magpie".into(),
                    actor_2.op_id_at(2) => "blackbird".into(),
                }
            },
        },
    };
    let mut backend = Backend::init();
    let _patch1 = backend.apply_changes(vec![change1]).unwrap();
    let patch2 = backend.apply_changes(vec![change2]).unwrap();
    //let patch = backend.get_patch().unwrap();
    assert_eq!(patch2, expected_patch);
}

#[test]
fn delete_key_from_map() {
    let actor: ActorId = "cd86c07f109348f494af5be30fdc4c71".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Set(ScalarValue::Str("magpie".into())),
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![Op {
            obj: ObjectId::Root,
            action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
            key: "bird".into(),
            pred: vec![actor.op_id_at(1)],
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        clock: hashmap! {actor => 2},
        deps: vec![change2.hash],
        max_op: 2,
        pending_changes: 0,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{}
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn create_nested_maps() {
    let actor: ActorId = "d6226fcd55204b82b396f2473da3e26f".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::F64(3.0)),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: "wrens".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch: Patch = Patch {
        actor: None,
        max_op: 2,
        pending_changes: 0,
        deps: vec![change.hash],
        seq: None,
        clock: hashmap! {actor.clone() => 1},
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Map(MapDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: MapType::Map,
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor.op_id_at(2) => Diff::Value(ScalarValue::F64(3.0))
                            }
                        }
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_assign_to_nested_keys_in_map() {
    let actor: ActorId = "3c39c994039042778f4779a01a59a917".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                obj: ObjectId::from(actor.op_id_at(1)),
                action: amp::OpType::Set(ScalarValue::F64(3.0)),
                key: "wrens".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        hash: None,
        operations: vec![Op {
            obj: ObjectId::from(actor.op_id_at(1)),
            action: amp::OpType::Set(ScalarValue::F64(15.0)),
            key: "sparrows".into(),
            pred: Vec::new(),
            insert: false,
        }],
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
        max_op: 3,
        pending_changes: 0,
        deps: vec![change2.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Map(MapDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: MapType::Map,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                actor.op_id_at(3) => Diff::Value(ScalarValue::F64(15.0))
                            }
                        }
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let actor: ActorId = "f82cb62dabe64372ab87466b77792010".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
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
        deps: vec![change.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Value(ScalarValue::Str("chaffinch".into())),

                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_apply_updates_inside_lists() {
    let actor: ActorId = "4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                pred: Vec::new(),
                insert: true,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set("greenfinch".into()),
            obj: ObjectId::from(actor.op_id_at(1)),
            key: actor.op_id_at(2).into(),
            pred: vec![actor.op_id_at(2)],
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        deps: vec![change2.hash],
        clock: hashmap! {
            actor.clone() => 2
        },
        max_op: 3,
        pending_changes: 0,
        seq: None,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Update{
                            index: 0,
                            op_id: actor.op_id_at(3),
                            value: Diff::Value("greenfinch".into()),
                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_delete_list_elements() {
    let actor: ActorId = "8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![Op {
            action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
            obj: ObjectId::from(actor.op_id_at(1)),
            key: actor.op_id_at(2).into(),
            pred: vec![actor.op_id_at(2)],
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        seq: None,
        actor: None,
        max_op: 3,
        pending_changes: 0,
        clock: hashmap! {
            actor.clone() => 2
        },
        deps: vec![change2.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id:  actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Remove{index: 0, count: 1}]
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_list_element_insertion_and_deletion_in_same_change() {
    let actor: ActorId = "ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::list()),
            obj: ObjectId::Root,
            key: "birds".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: actor.op_id_at(2).into(),
                pred: vec![actor.op_id_at(2)],
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 2
        },
        seq: None,
        actor: None,
        max_op: 3,
        pending_changes: 0,
        deps: vec![change2.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![
                            DiffEdit::SingleElementInsert{
                                index: 0,
                                elem_id: actor.op_id_at(2).into(),
                                op_id: actor.op_id_at(2),
                                value: amp::Diff::Value("chaffinch".into()),
                            },
                            DiffEdit::Remove{index: 0, count: 1},
                        ],
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_changes_within_conflicted_objects() {
    let actor1: ActorId = "9f17517523e54ee888e9cd51dfd7a572".try_into().unwrap();
    let actor2: ActorId = "83768a19a13842beb6dde8c68a662fad".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::list()),
            obj: ObjectId::Root,
            key: "conflict".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor2.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::map()),
            obj: ObjectId::Root,
            key: "conflict".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change3: Change = UncompressedChange {
        actor_id: actor2.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change2.hash],
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::F64(12.0)),
            obj: ObjectId::from(actor2.op_id_at(1)),
            key: "sparrow".into(),
            pred: Vec::new(),
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        clock: hashmap! {
            actor1.clone() => 1,
            actor2.clone() => 2,
        },
        max_op: 2,
        pending_changes: 0,
        deps: vec![change1.hash, change3.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "conflict".into() => hashmap!{
                    actor1.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor1.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: Vec::new(),
                    }),
                    actor2.op_id_at(1) => Diff::Map(MapDiff{
                        object_id: actor2.op_id_at(1).into(),
                        obj_type: MapType::Map,
                        props: hashmap!{
                            "sparrow".into() => hashmap!{
                                actor2.op_id_at(2) => Diff::Value(ScalarValue::F64(12.0))
                            }
                        }
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    backend.apply_changes(vec![change2]).unwrap();
    let patch = backend.apply_changes(vec![change3]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test_env_log::test]
fn test_handle_changes_within_conflicted_lists() {
    let actor1: ActorId = "01234567".try_into().unwrap();
    let actor2: ActorId = "89abcdef".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        hash: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "todos".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: actor1.op_id_at(1).into(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: actor1.op_id_at(1).into(),
                key: actor1.op_id_at(2).into(),
                pred: vec![actor1.op_id_at(2)],
                insert: false,
            },
            Op {
                action: amp::OpType::Set("buy milk".into()),
                obj: actor1.op_id_at(3).into(),
                key: "title".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(false.into()),
                obj: actor1.op_id_at(3).into(),
                key: "done".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let change3: Change = UncompressedChange {
        actor_id: actor2.clone(),
        seq: 1,
        start_op: 3,
        time: 0,
        message: None,
        hash: None,
        deps: vec![change1.hash],
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: actor1.op_id_at(1).into(),
                key: actor1.op_id_at(2).into(),
                pred: vec![actor1.op_id_at(2)],
                insert: false,
            },
            Op {
                action: amp::OpType::Set("water plants".into()),
                obj: actor2.op_id_at(3).into(),
                key: "title".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(false.into()),
                obj: actor2.op_id_at(3).into(),
                key: "done".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let mut change4_deps = vec![change2.hash, change3.hash];
    change4_deps.sort();

    let change4: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 3,
        start_op: 6,
        time: 0,
        message: None,
        hash: None,
        deps: change4_deps,
        operations: vec![Op {
            action: amp::OpType::Set(true.into()),
            obj: actor1.op_id_at(3).into(),
            key: "done".into(),
            pred: vec![actor1.op_id_at(5)],
            insert: false,
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        clock: hashmap! {
            actor2.clone() => 1,
            actor1.clone() => 3,
        },
        max_op: 6,
        pending_changes: 0,
        deps: vec![change4.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "todos".into() => hashmap!{
                    actor1.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor1.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor1.op_id_at(3),
                                value: Diff::Map(MapDiff{
                                    object_id: actor1.op_id_at(3).into(),
                                    obj_type: MapType::Map,
                                    props: hashmap!{
                                        "done".to_string() => hashmap!{
                                            actor1.op_id_at(6) => Diff::Value(true.into())
                                        }
                                    }
                                })
                            },
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor2.op_id_at(3),
                                value: Diff::Map(MapDiff{
                                    obj_type: MapType::Map,
                                    object_id: actor2.op_id_at(3).into(),
                                    props: hashmap!{},
                                })
                            }
                        ]
                    }),
                }
            },
        },
    };

    let mut backend = Backend::init();
    let patch = backend
        .apply_changes(vec![change1, change2, change3])
        .unwrap();
    println!("patch {:#?}", patch);
    let patch = backend.apply_changes(vec![change4]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_at_root() {
    let actor: ActorId = "955afa3bbcc140b3b4bac8836479d650".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Timestamp(1_586_528_122_277)),
            obj: ObjectId::Root,
            key: "now".into(),
            pred: Vec::new(),
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
        seq: None,
        actor: None,
        deps: vec![change.hash],
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "now".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Timestamp(1_586_528_122_277))
                }
            },
        },
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_in_a_list() {
    let actor: ActorId = "27d467ecb1a640fb9bed448ce7cf6a44".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Timestamp(1_586_528_191_421)),
                obj: ObjectId::from(actor.op_id_at(1)),
                key: ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
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
        deps: vec![change.hash],
        actor: None,
        seq: None,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Value(ScalarValue::Timestamp(1_586_528_191_421))
                        }],
                    })
                }
            },
        },
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_cursor_objects() {
    let actor = ActorId::random();
    let change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Str("something".into())),
                obj: actor.op_id_at(1).into(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Cursor(actor.op_id_at(2))),
                obj: ObjectId::Root,
                key: "cursor".into(),
                insert: false,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    let binchange: Change = (&change).try_into().unwrap();
    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![Change::from(change)]).unwrap();
    let expected_patch = amp::Patch {
        clock: hashmap! {
            actor.clone() => 1,
        },
        max_op: 3,
        pending_changes: 0,
        deps: vec![binchange.hash],
        actor: None,
        seq: None,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(2).into(),
                            op_id: actor.op_id_at(2),
                            value: Diff::Value(ScalarValue::Str("something".into())),
                        }],
                    })
                },
                "cursor".into() => hashmap!{
                    actor.op_id_at(3) => Diff::Cursor(CursorDiff{
                        object_id: actor.op_id_at(1).into(),
                        elem_id: actor.op_id_at(2),
                        index: 0,
                    }),
                },
            },
        },
    };
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_throws_on_attempt_to_create_missing_cursor() {
    let actor = ActorId::random();
    let change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Cursor(actor.op_id_at(2))),
            obj: ObjectId::Root,
            key: "cursor".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let mut backend = Backend::init();
    let err = backend
        .apply_changes(vec![Change::from(change)])
        .expect_err("Should be an error");
    assert_eq!(
        err,
        AutomergeError::InvalidCursor {
            opid: actor.op_id_at(2)
        }
    );
}

#[test]
fn test_updating_sequences_updates_referring_cursors() {
    let actor = ActorId::random();
    let change1 = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Str("something".into())),
                obj: actor.op_id_at(1).into(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Cursor(actor.op_id_at(2))),
                obj: ObjectId::Root,
                key: "cursor".into(),
                insert: false,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    let binchange1: Change = (&change1).try_into().unwrap();
    let change2 = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 4,
        time: 0,
        deps: vec![binchange1.hash],
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Str("something else".into())),
            obj: actor.op_id_at(1).into(),
            key: amp::ElementId::Head.into(),
            insert: true,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    };
    let binchange2: Change = change2.try_into().unwrap();
    let mut backend = Backend::init();
    backend.apply_changes(vec![binchange1]).unwrap();
    let patch = backend.apply_changes(vec![binchange2.clone()]).unwrap();
    let expected_patch = amp::Patch {
        clock: hashmap! {
            actor.clone() => 2,
        },
        max_op: 4,
        pending_changes: 0,
        deps: vec![binchange2.hash],
        actor: None,
        seq: None,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(4).into(),
                            op_id: actor.op_id_at(4),
                            value: Diff::Value(ScalarValue::Str("something else".into())),
                        }],
                    })
                },
                "cursor".into() => hashmap!{
                    actor.op_id_at(3) => Diff::Cursor(CursorDiff{
                        object_id: actor.op_id_at(1).into(),
                        elem_id: actor.op_id_at(2),
                        index: 1,
                    }),
                },
            },
        },
    };
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_updating_sequences_updates_referring_cursors_with_deleted_items() {
    let actor = ActorId::random();
    let change1 = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        hash: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectId::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Str("something".into())),
                obj: actor.op_id_at(1).into(),
                key: amp::ElementId::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Str("something else".into())),
                obj: actor.op_id_at(1).into(),
                key: actor.op_id_at(2).into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Cursor(actor.op_id_at(3))),
                obj: ObjectId::Root,
                key: "cursor".into(),
                insert: false,
                pred: Vec::new(),
            },
        ],
        extra_bytes: Vec::new(),
    };
    let binchange1: Change = (&change1).try_into().unwrap();
    let change2 = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 5,
        time: 0,
        deps: vec![binchange1.hash],
        message: None,
        hash: None,
        operations: vec![Op {
            action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
            obj: actor.op_id_at(1).into(),
            key: actor.op_id_at(2).into(),
            insert: false,
            pred: vec![actor.op_id_at(2)],
        }],
        extra_bytes: Vec::new(),
    };
    let binchange2: Change = change2.try_into().unwrap();
    let mut backend = Backend::init();
    backend.apply_changes(vec![binchange1]).unwrap();
    let patch = backend.apply_changes(vec![binchange2.clone()]).unwrap();
    let expected_patch = amp::Patch {
        clock: hashmap! {
            actor.clone() => 2,
        },
        max_op: 5,
        pending_changes: 0,
        deps: vec![binchange2.hash],
        actor: None,
        seq: None,
        diffs: MapDiff {
            object_id: ObjectId::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Remove{index: 0, count: 1}],
                    })
                },
                "cursor".into() => hashmap!{
                    actor.op_id_at(4) => Diff::Cursor(CursorDiff{
                        object_id: actor.op_id_at(1).into(),
                        elem_id: actor.op_id_at(3),
                        index: 0,
                    }),
                },
            },
        },
    };
    assert_eq!(patch, expected_patch);
}
