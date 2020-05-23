extern crate automerge_backend;
use automerge_backend::{Backend, UnencodedChange};
use automerge_backend::{OpType, Operation};
use automerge_protocol::{
    ActorID, Diff, DiffEdit, ElementID, MapDiff, ObjType, ObjectID, Patch, SeqDiff, Value,
};
use maplit::hashmap;
use std::convert::TryInto;

#[test]
fn test_include_most_recent_value_for_key() {
    let actor: ActorID = "ec28cfbcdb9e4f32ad24b3c776e651b0".try_into().unwrap();
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Operation {
            action: OpType::Set("magpie".into()),
            key: "bird".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        deps: vec![change1.hash],
        operations: vec![Operation {
            obj: ObjectID::Root,
            action: OpType::Set("blackbird".into()),
            key: "bird".into(),
            pred: vec!["1@ec28cfbcdb9e4f32ad24b3c776e651b0".try_into().unwrap()],
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        version: 0,
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    "2@ec28cfbcdb9e4f32ad24b3c776e651b0".try_into().unwrap() => Diff::Value("blackbird".into())
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
    let change1 = UnencodedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Operation {
            action: OpType::Set("magpie".into()),
            obj: ObjectID::Root,
            key: "bird".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: actor2.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set("blackbird".into()),
            key: "bird".into(),
            obj: ObjectID::Root,
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor1.to_string() => 1,
            actor2.to_string() => 1,
        },
        seq: None,
        actor: None,
        can_undo: false,
        can_redo: false,
        deps: vec![change1.hash, change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    "1@111111".try_into().unwrap() => Diff::Value("magpie".into()),
                    "1@222222".try_into().unwrap() => Diff::Value("blackbird".into()),
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set(Value::Counter(1)),
            obj: ObjectID::Root,
            key: "counter".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        operations: vec![Operation {
            action: OpType::Inc(2),
            obj: ObjectID::Root,
            key: "counter".into(),
            pred: vec!["1@46c92088e4484ae5945dc63bf606a4a5".try_into().unwrap()],
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        seq: None,
        actor: None,
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "counter".into() => hashmap!{
                    "1@46c92088e4484ae5945dc63bf606a4a5".try_into().unwrap() => Diff::Value(Value::Counter(3))
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::Map),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set(Value::F64(3.0)),
                key: "wrens".into(),
                obj: "1@06148f9422cb40579fd02f1975c34a51".try_into().unwrap(),
                pred: Vec::new(),
                insert: false,
            },
        ],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        deps: vec![change1.hash],
        message: None,
        operations: vec![
            Operation {
                action: OpType::Del,
                obj: "1@06148f9422cb40579fd02f1975c34a51".try_into().unwrap(),
                key: "wrens".into(),
                pred: vec!["2@06148f9422cb40579fd02f1975c34a51".try_into().unwrap()],
                insert: false,
            },
            Operation {
                action: OpType::Set(Value::F64(15.0)),
                obj: "1@06148f9422cb40579fd02f1975c34a51".try_into().unwrap(),
                key: "sparrows".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor.to_string() => 2,
        },
        actor: None,
        seq: None,
        can_undo: false,
        can_redo: false,
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@06148f9422cb40579fd02f1975c34a51".try_into().unwrap() => Diff::Map(MapDiff{
                        object_id: "1@06148f9422cb40579fd02f1975c34a51".try_into().unwrap(),
                        obj_type: ObjType::Map,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                "4@06148f9422cb40579fd02f1975c34a51".try_into().unwrap() => Diff::Value(Value::F64(15.0))
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::List),
                obj: ObjectID::Root,
                key: "birds".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set("chaffinch".into()),
                obj: "1@90bf7df682f747fa82ac604b35010906".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@90bf7df682f747fa82ac604b35010906".try_into().unwrap() => Diff::Seq(SeqDiff{
                        object_id: "1@90bf7df682f747fa82ac604b35010906".try_into().unwrap(),
                        obj_type: ObjType::List,
                        edits: vec![DiffEdit::Insert { index :0 }],
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::List),
                obj: ObjectID::Root,
                key: "todos".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Make(ObjType::Map),
                obj: "1@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Operation {
                action: OpType::Set("water plants".into()),
                obj: "2@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                key: "title".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set(false.into()),
                obj: "2@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                key: "done".into(),
                pred: Vec::new(),
                insert: false,
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor.to_string() => 1
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "todos".into() => hashmap!{
                    "1@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap() => Diff::Seq(SeqDiff{
                        object_id: "1@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                        obj_type: ObjType::List,
                        edits: vec![DiffEdit::Insert{index: 0}],
                        props: hashmap!{
                            0 => hashmap!{
                                "2@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap() => Diff::Map(MapDiff{
                                    object_id: "2@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap(),
                                    obj_type: ObjType::Map,
                                    props: hashmap!{
                                        "title".into() => hashmap!{
                                            "3@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap() => Diff::Value("water plants".into()),
                                        },
                                        "done".into() => hashmap!{
                                            "4@6caaa2e433de42ae9c3fa65c9ff3f03e".try_into().unwrap() => Diff::Value(false.into())
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set(Value::Timestamp(1_586_541_033_457)),
            obj: ObjectID::Root,
            key: "now".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "now".into() => hashmap!{
                    "1@90f5dd5d4f524e95ad5929e08d1194f1".try_into().unwrap() => Diff::Value(Value::Timestamp(1_586_541_033_457))
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::List),
                obj: ObjectID::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set(Value::Timestamp(1_586_541_089_595)),
                obj: "1@08b050f976a249349021a2e63d99c8e8".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 0,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: vec![change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    "1@08b050f976a249349021a2e63d99c8e8".try_into().unwrap() => Diff::Seq(SeqDiff{
                        object_id: "1@08b050f976a249349021a2e63d99c8e8".try_into().unwrap(),
                        obj_type: ObjType::List,
                        edits: vec![DiffEdit::Insert {index: 0}],
                        props: hashmap!{
                            0 => hashmap!{
                                "2@08b050f976a249349021a2e63d99c8e8".try_into().unwrap() => Diff::Value(Value::Timestamp(1_586_541_089_595))
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
