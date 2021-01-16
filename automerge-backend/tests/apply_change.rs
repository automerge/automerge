extern crate automerge_backend;
use automerge_backend::{Backend, Change};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorID, Diff, DiffEdit, ElementID, MapDiff, MapType, ObjDiff, ObjType, ObjectID, Op, Patch,
    ScalarValue, SeqDiff, SequenceType, UncompressedChange,
};
use maplit::hashmap;
use std::convert::TryInto;
use std::str::FromStr;

#[test]
fn test_incremental_diffs_in_a_map() {
    let actor: ActorID = "7b7723afd9e6480397a4d467b7693156".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectID::Root,
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
        diffs: Some(
            MapDiff {
                object_id: ObjectID::Root,
                obj_type: MapType::Map,
                props: hashmap!( "bird".into() => hashmap!( actor.op_id_at(1) => "magpie".into() )),
            }
            .into(),
        ),
    };
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_increment_key_in_map() {
    let actor: ActorID = "cdee6963c1664645920be8b41a933c2b".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectID::Root,
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
        deps: vec![change1.hash],
        operations: vec![Op {
            obj: ObjectID::Root,
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
        deps: vec![change2.hash],
        diffs: Some(
            MapDiff {
                object_id: ObjectID::Root,
                obj_type: MapType::Map,
                props: hashmap!(
                "counter".into() => hashmap!{
                    actor.op_id_at(1) =>  ScalarValue::Counter(3).into(),
                }),
            }
            .into(),
        ),
    };
    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_conflict_on_assignment_to_same_map_key() {
    let actor_1 = ActorID::from_str("ac11").unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor_1.clone(),
        seq: 1,
        message: None,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectID::Root,
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            insert: false,
            pred: Vec::new(),
        }],
        extra_bytes: Vec::new(),
    }
    .try_into()
    .unwrap();

    let actor_2 = ActorID::from_str("ac22").unwrap();
    let change2: Change = UncompressedChange {
        actor_id: actor_2.clone(),
        start_op: 2,
        seq: 1,
        message: None,
        deps: vec![change1.hash],
        time: 0,
        operations: vec![Op {
            obj: ObjectID::Root,
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
        diffs: Some(
            MapDiff {
                object_id: ObjectID::Root,
                obj_type: MapType::Map,
                props: hashmap!( "bird".into() => hashmap!(
                            actor_1.op_id_at(1) => "magpie".into(),
                            actor_2.op_id_at(2) => "blackbird".into(),
                )),
            }
            .into(),
        ),
    };
    let mut backend = Backend::init();
    let _patch1 = backend.apply_changes(vec![change1]).unwrap();
    let patch2 = backend.apply_changes(vec![change2]).unwrap();
    //let patch = backend.get_patch().unwrap();
    assert_eq!(patch2, expected_patch);
}

#[test]
fn delete_key_from_map() {
    let actor: ActorID = "cd86c07f109348f494af5be30fdc4c71".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectID::Root,
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
        deps: vec![change1.hash],
        operations: vec![Op {
            obj: ObjectID::Root,
            action: amp::OpType::Del,
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
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{}
            },
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn create_nested_maps() {
    let actor: ActorID = "d6226fcd55204b82b396f2473da3e26f".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::F64(3.0)),
                obj: ObjectID::from(actor.op_id_at(1)),
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
        deps: vec![change.hash],
        seq: None,
        clock: hashmap! {actor.clone() => 1},
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
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
        })),
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_assign_to_nested_keys_in_map() {
    let actor: ActorID = "3c39c994039042778f4779a01a59a917".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                obj: ObjectID::from(actor.op_id_at(1)),
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
        operations: vec![Op {
            obj: ObjectID::from(actor.op_id_at(1)),
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
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
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
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let actor: ActorID = "f82cb62dabe64372ab87466b77792010".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
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
        actor: None,
        seq: None,
        deps: vec![change.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Insert{ index: 0, elem_id: actor.op_id_at(2).into() }],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2) => Diff::Value(ScalarValue::Str("chaffinch".into()))
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_apply_updates_inside_lists() {
    let actor: ActorID = "4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
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
        operations: vec![Op {
            action: amp::OpType::Set("greenfinch".into()),
            obj: ObjectID::from(actor.op_id_at(1)),
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
        seq: None,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: Vec::new(),
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(3) => Diff::Value("greenfinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_delete_list_elements() {
    let actor: ActorID = "8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
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
        deps: vec![change1.hash],
        operations: vec![Op {
            action: amp::OpType::Del,
            obj: ObjectID::from(actor.op_id_at(1)),
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
        clock: hashmap! {
            actor.clone() => 2
        },
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id:  actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        props: hashmap!{},
                        edits: vec![DiffEdit::Remove{index: 0}]
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_list_element_insertion_and_deletion_in_same_change() {
    let actor: ActorID = "ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::list()),
            obj: ObjectID::Root,
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
        deps: Vec::new(),
        operations: vec![
            Op {
                action: amp::OpType::Set("chaffinch".into()),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                action: amp::OpType::Del,
                obj: ObjectID::from(actor.op_id_at(1)),
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
        deps: vec![change2.hash, change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![
                            DiffEdit::Insert{index: 0, elem_id: actor.op_id_at(2).into()},
                            DiffEdit::Remove{index: 0},
                        ],
                        props: hashmap!{}
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_changes_within_conflicted_objects() {
    let actor1: ActorID = "9f17517523e54ee888e9cd51dfd7a572".try_into().unwrap();
    let actor2: ActorID = "83768a19a13842beb6dde8c68a662fad".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::list()),
            obj: ObjectID::Root,
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
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Make(amp::ObjType::map()),
            obj: ObjectID::Root,
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
        deps: vec![change2.hash],
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::F64(12.0)),
            obj: ObjectID::from(actor2.op_id_at(1)),
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
        deps: vec![change3.hash, change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "conflict".into() => hashmap!{
                    actor1.op_id_at(1) => Diff::Unchanged(ObjDiff{
                       object_id: actor1.op_id_at(1).into(),
                       obj_type: ObjType::Sequence(SequenceType::List),
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
        })),
    };

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    backend.apply_changes(vec![change2]).unwrap();
    let patch = backend.apply_changes(vec![change3]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_at_root() {
    let actor: ActorID = "955afa3bbcc140b3b4bac8836479d650".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Timestamp(1_586_528_122_277)),
            obj: ObjectID::Root,
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
        seq: None,
        actor: None,
        deps: vec![change.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "now".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Timestamp(1_586_528_122_277))
                }
            },
        })),
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_in_a_list() {
    let actor: ActorID = "27d467ecb1a640fb9bed448ce7cf6a44".try_into().unwrap();
    let change: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![
            Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: ObjectID::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Set(ScalarValue::Timestamp(1_586_528_191_421)),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
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
        deps: vec![change.hash],
        actor: None,
        seq: None,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Insert{index: 0, elem_id: actor.op_id_at(2).into()}],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2) => Diff::Value(ScalarValue::Timestamp(1_586_528_191_421))
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}
