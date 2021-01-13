extern crate automerge_backend;
use automerge_backend::{Backend, Change};
use automerge_protocol as amp;
use automerge_protocol::{
    ActorID, Diff, DiffEdit, ElementID, MapDiff, MapType, ObjectID, Op, Patch, ScalarValue,
    SeqDiff, SequenceType, UncompressedChange,
};
use maplit::hashmap;
use std::convert::TryInto;

#[test]
fn test_include_most_recent_value_for_key() {
    let actor: ActorID = "ec28cfbcdb9e4f32ad24b3c776e651b0".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            key: "bird".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
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
            action: amp::OpType::Set("blackbird".into()),
            key: "bird".into(),
            pred: vec![actor.op_id_at(1)],
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        max_op: 2,
        clock: hashmap! {
            actor.clone() => 2,
        },
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor.op_id_at(2) => Diff::Value("blackbird".into()),
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_conflicting_values_for_key() {
    let actor1: ActorID = "111111".try_into().unwrap();
    let actor2: ActorID = "222222".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Op {
            action: amp::OpType::Set("magpie".into()),
            obj: ObjectID::Root,
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
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
            action: amp::OpType::Set("blackbird".into()),
            key: "bird".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor1.clone() => 1,
            actor2.clone() => 1,
        },
        max_op: 1,
        seq: None,
        actor: None,
        deps: vec![change1.hash, change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor1.op_id_at(1) => Diff::Value("magpie".into()),
                    actor2.op_id_at(1) => Diff::Value("blackbird".into()),
                },
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handles_counter_increment_at_keys_in_a_map() {
    let actor: ActorID = "46c92088e4484ae5945dc63bf606a4a5".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            action: amp::OpType::Set(ScalarValue::Counter(1)),
            obj: ObjectID::Root,
            key: "counter".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let change2: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        operations: vec![Op {
            action: amp::OpType::Inc(2),
            obj: ObjectID::Root,
            key: "counter".into(),
            pred: vec![actor.op_id_at(1)],
            insert: false,
        }],
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
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "counter".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Counter(3))
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_creates_nested_maps() {
    let actor: ActorID = "06148f9422cb40579fd02f1975c34a51".try_into().unwrap();
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
                action: amp::OpType::Set(ScalarValue::F64(3.0)),
                key: "wrens".into(),
                obj: ObjectID::from(actor.op_id_at(1)),
                pred: Vec::new(),
                insert: false,
            },
        ],
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
        operations: vec![
            Op {
                obj: ObjectID::from(actor.op_id_at(1)),
                action: amp::OpType::Del,
                key: "wrens".into(),
                pred: vec![actor.op_id_at(2)],
                insert: false,
            },
            Op {
                obj: ObjectID::from(actor.op_id_at(1)),
                action: amp::OpType::Set(ScalarValue::F64(15.0)),
                key: "sparrows".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
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
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Map(MapDiff{
                        object_id: ObjectID::from(actor.op_id_at(1)),
                        obj_type: MapType::Map,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                actor.op_id_at(4) => Diff::Value(ScalarValue::F64(15.0))
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let actor: ActorID = "90bf7df682f747fa82ac604b35010906".try_into().unwrap();
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
                obj: ObjectID::from(actor.op_id_at(1)),
                action: amp::OpType::Set("chaffinch".into()),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
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
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: ObjectID::from(actor.op_id_at(1)),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Insert {
                            index: 0,
                            elem_id: actor.op_id_at(2).into()
                        }],
                        props: hashmap!{
                            0 => hashmap!{
                                "2@90bf7df682f747fa82ac604b35010906".try_into().unwrap() => Diff::Value("chaffinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_latests_state_of_list() {
    let actor: ActorID = "6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap();
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
                key: "todos".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                action: amp::OpType::Make(amp::ObjType::map()),
                obj: ObjectID::from(actor.op_id_at(1)),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Op {
                obj: ObjectID::from(actor.op_id_at(2)),
                action: amp::OpType::Set("water plants".into()),
                key: "title".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                obj: ObjectID::from(actor.op_id_at(2)),
                action: amp::OpType::Set(false.into()),
                key: "done".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1
        },
        max_op: 4,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "todos".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: ObjectID::from(actor.op_id_at(1)),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Insert{index: 0, elem_id: actor.op_id_at(2).into()}],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2) => Diff::Map(MapDiff{
                                    object_id: "2@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                                    obj_type: MapType::Map,
                                    props: hashmap!{
                                        "title".into() => hashmap!{
                                            actor.op_id_at(3) => Diff::Value("water plants".into()),
                                        },
                                        "done".into() => hashmap!{
                                            actor.op_id_at(4) => Diff::Value(false.into())
                                        }
                                    }
                                })
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_date_objects_at_root() {
    let actor: ActorID = "90f5dd5d4f524e95ad5929e08d1194f1".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Op {
            obj: ObjectID::Root,
            action: amp::OpType::Set(ScalarValue::Timestamp(1_586_541_033_457)),
            key: "now".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .try_into()
    .unwrap();

    let expected_patch = Patch {
        clock: hashmap! {
            actor.clone() => 1,
        },
        max_op: 1,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "now".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Value(ScalarValue::Timestamp(1_586_541_033_457))
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_date_objects_in_a_list() {
    let actor: ActorID = "08b050f976a249349021a2e63d99c8e8".try_into().unwrap();
    let change1: Change = UncompressedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Op {
                obj: ObjectID::Root,
                action: amp::OpType::Make(amp::ObjType::list()),
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Op {
                obj: ObjectID::from(actor.op_id_at(1)),
                action: amp::OpType::Set(ScalarValue::Timestamp(1_586_541_089_595)),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
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
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: MapType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    actor.op_id_at(1) => Diff::Seq(SeqDiff{
                        object_id: ObjectID::from(actor.op_id_at(1)),
                        obj_type: SequenceType::List,
                        edits: vec![DiffEdit::Insert {index: 0, elem_id: actor.op_id_at(2).into()}],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2) => Diff::Value(ScalarValue::Timestamp(1_586_541_089_595))
                            }
                        }
                    })
                }
            },
        })),
    };

    let mut backend = Backend::init();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}
