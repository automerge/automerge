extern crate automerge_backend;
use automerge_backend::{AutomergeError, Backend, UnencodedChange};
use automerge_backend::{OpType, Operation};
use automerge_protocol::{
    ActorID, Diff, DiffEdit, ElementID, Key, MapDiff, ObjDiff, ObjType, ObjectID, Patch, SeqDiff,
    Value,
};
use maplit::hashmap;
use std::convert::TryInto;
use std::str::FromStr;

#[test]
fn test_incremental_diffs_in_a_map() {
    let change = UnencodedChange {
        actor_id: "7b7723afd9e6480397a4d467b7693156".try_into().unwrap(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation::set(
            ObjectID::Root,
            "bird".into(),
            "magpie".into(),
            vec![],
        )],
    }
    .encode();

    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change.clone()]).unwrap();
    let expected_patch = Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: vec![change.hash],
        clock: hashmap!{"7b7723afd9e6480397a4d467b7693156".into() => 1},
        can_undo: false,
        can_redo: false,
        diffs: Some(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap!( "bird".into() => hashmap!( "1@7b7723afd9e6480397a4d467b7693156".into() => "magpie".into() ))
        }.into()),
    };
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_increment_key_in_map() -> Result<(), AutomergeError> {
    let change1 = UnencodedChange {
        actor_id: "cdee6963c1664645920be8b41a933c2b".try_into().unwrap(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation::set(
            ObjectID::Root,
            "counter".into(),
            Value::Counter(1),
            vec![],
        )],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: "cdee6963c1664645920be8b41a933c2b".try_into().unwrap(),
        seq: 2,
        start_op: 2,
        time: 2,
        message: None,
        deps: vec![change1.hash],
        operations: vec![Operation::inc(
            ObjectID::Root,
            "counter".into(),
            2,
            vec!["1@cdee6963c1664645920be8b41a933c2b".try_into().unwrap()],
        )],
    }
    .encode();

    let expected_patch = Patch {
        version: 2,
        actor: None,
        seq: None,
        clock: hashmap! {"cdee6963c1664645920be8b41a933c2b".into() => 2},
        can_undo: false,
        can_redo: false,
        deps: vec![change2.hash],
        diffs: Some(
            MapDiff {
                object_id: ObjectID::Root.to_string(),
                obj_type: ObjType::Map,
                props: hashmap!(
                "counter".into() => hashmap!{
                    "1@cdee6963c1664645920be8b41a933c2b".into() =>  Value::Counter(3).into(),
                }),
            }
            .into(),
        ),
    };
    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch);
    Ok(())
}

#[test]
fn test_conflict_on_assignment_to_same_map_key() {
    let change1 = UnencodedChange {
        actor_id: ActorID::from_str("ac11").unwrap(),
        seq: 1,
        message: None,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        operations: vec![Operation::set(
            ObjectID::Root,
            "bird".into(),
            "magpie".into(),
            vec![],
        )],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: ActorID::from_str("ac22").unwrap(),
        start_op: 2,
        seq: 1,
        message: None,
        deps: vec![change1.hash],
        time: 0,
        operations: vec![Operation::set(
            ObjectID::Root,
            "bird".into(),
            "blackbird".into(),
            vec![],
        )],
    }
    .encode();

    let expected_patch = Patch {
        version: 2,
        actor: None,
        seq: None,
        clock: hashmap! {
            "ac11".into() => 1,
            "ac22".into() => 1,
        },
        deps: vec![change2.hash],
        can_undo: false,
        can_redo: false,
        diffs: Some(
            MapDiff {
                object_id: ObjectID::Root.to_string(),
                obj_type: ObjType::Map,
                props: hashmap!( "bird".into() => hashmap!(
                            "1@ac11".into() => "magpie".into(),
                            "2@ac22".into() => "blackbird".into(),
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Set(Value::Str("magpie".into())),
            obj: ObjectID::Root,
            key: Key::Map("bird".into()),
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
            action: OpType::Del,
            obj: ObjectID::Root,
            key: Key::Map("bird".into()),
            pred: vec!["1@cd86c07f109348f494af5be30fdc4c71".try_into().unwrap()],
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        actor: None,
        seq: None,
        version: 2,
        clock: hashmap! {actor.to_string() => 2},
        deps: vec![change2.hash],
        can_undo: false,
        can_redo: false,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
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
    let change = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::Map),
                obj: ObjectID::Root,
                key: Key::Map("birds".into()),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set(Value::F64(3.0)),
                obj: "1@d6226fcd55204b82b396f2473da3e26f".try_into().unwrap(),
                key: Key::Map("wrens".into()),
                pred: Vec::new(),
                insert: false,
            },
        ],
    }
    .encode();

    let expected_patch: Patch = Patch {
        actor: None,
        deps: vec![change.hash],
        seq: None,
        clock: hashmap! {actor.to_string() => 1},
        can_undo: false,
        can_redo: false,
        version: 1,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@d6226fcd55204b82b396f2473da3e26f".into() => Diff::Map(MapDiff{
                        object_id: "1@d6226fcd55204b82b396f2473da3e26f".try_into().unwrap(),
                        obj_type: ObjType::Map,
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                 "2@d6226fcd55204b82b396f2473da3e26f".into() => Diff::Value(Value::F64(3.0))
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
                obj: "1@3c39c994039042778f4779a01a59a917".try_into().unwrap(),
                key: "wrens".into(),
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
        operations: vec![Operation {
            action: OpType::Set(Value::F64(15.0)),
            obj: "1@3c39c994039042778f4779a01a59a917".try_into().unwrap(),
            key: "sparrows".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 2,
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_redo: false,
        can_undo: false,
        actor: None,
        seq: None,
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@3c39c994039042778f4779a01a59a917".into() => Diff::Map(MapDiff{
                        object_id: "1@3c39c994039042778f4779a01a59a917".into(),
                        obj_type: ObjType::Map,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                "3@3c39c994039042778f4779a01a59a917".into() => Diff::Value(Value::F64(15.0))
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
    let change = UnencodedChange {
        actor_id: "f82cb62dabe64372ab87466b77792010".try_into().unwrap(),
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
                action: OpType::Set(Value::Str("chaffinch".into())),
                obj: "1@f82cb62dabe64372ab87466b77792010".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 1,
        clock: hashmap! {
            "f82cb62dabe64372ab87466b77792010".into() => 1,
        },
        can_undo: false,
        can_redo: false,
        actor: None,
        seq: None,
        deps: vec![change.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@f82cb62dabe64372ab87466b77792010".into() => Diff::Seq(SeqDiff{
                        object_id: "1@f82cb62dabe64372ab87466b77792010".into(),
                        obj_type: ObjType::List,
                        edits: vec![DiffEdit::Insert{ index: 0 }],
                        props: hashmap!{
                            0 => hashmap!{
                                "2@f82cb62dabe64372ab87466b77792010".into() => Diff::Value(Value::Str("chaffinch".into()))
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
                action: OpType::Set(Value::Str("chaffinch".into())),
                obj: "1@4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap(),
                key: ElementID::Head.into(),
                pred: Vec::new(),
                insert: true,
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
        operations: vec![Operation {
            action: OpType::Set(Value::Str("greenfinch".into())),
            obj: "1@4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap(),
            key: Key::Seq("2@4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap()),
            pred: vec!["2@4ee4a0d033b841c4b26d73d70a879547".try_into().unwrap()],
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        actor: None,
        version: 2,
        deps: vec![change2.hash],
        clock: hashmap! {
            actor.to_string() => 2
        },
        can_undo: false,
        can_redo: false,
        seq: None,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@4ee4a0d033b841c4b26d73d70a879547".into() => Diff::Seq(SeqDiff{
                        object_id: "1@4ee4a0d033b841c4b26d73d70a879547".into(),
                        obj_type: ObjType::List,
                        edits: Vec::new(),
                        props: hashmap!{
                            0 => hashmap!{
                                "3@4ee4a0d033b841c4b26d73d70a879547".into() => Diff::Value(Value::Str("greenfinch".into()))
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
                action: OpType::Set(Value::Str("chaffinch".into())),
                obj: "1@8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
    }
    .encode();

    let change2 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 2,
        start_op: 3,
        time: 0,
        message: None,
        deps: vec![change1.hash],
        operations: vec![Operation {
            action: OpType::Del,
            obj: "1@8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap(),
            key: ElementID::from_str("2@8a3d4716fdca49f4aa5835901f2034c7")
                .unwrap()
                .into(),
            pred: vec!["2@8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap()],
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 2,
        seq: None,
        actor: None,
        can_undo: false,
        can_redo: false,
        clock: hashmap! {
            actor.to_string() => 2
        },
        deps: vec![change2.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@8a3d4716fdca49f4aa5835901f2034c7".into() => Diff::Seq(SeqDiff{
                        object_id:  "1@8a3d4716fdca49f4aa5835901f2034c7".try_into().unwrap(),
                        obj_type: ObjType::List,
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
    let change1 = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Make(ObjType::List),
            obj: ObjectID::Root,
            key: "birds".into(),
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
        deps: Vec::new(),
        operations: vec![
            Operation {
                action: OpType::Set(Value::Str("chaffinch".into())),
                obj: "1@ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
            Operation {
                action: OpType::Del,
                obj: "1@ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap(),
                key: ElementID::from_str("2@ca95bc759404486bbe7b9dd2be779fa8")
                    .unwrap()
                    .into(),
                pred: vec!["2@ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap()],
                insert: false,
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 2,
        clock: hashmap! {
            actor.to_string() => 2
        },
        can_redo: false,
        can_undo: false,
        seq: None,
        actor: None,
        deps: vec![change2.hash, change1.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    "1@ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap() => Diff::Seq(SeqDiff{
                        object_id: "1@ca95bc759404486bbe7b9dd2be779fa8".try_into().unwrap(),
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
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_changes_within_conflicted_objects() {
    let actor1: ActorID = "9f17517523e54ee888e9cd51dfd7a572".try_into().unwrap();
    let actor2: ActorID = "83768a19a13842beb6dde8c68a662fad".try_into().unwrap();
    let change1 = UnencodedChange {
        actor_id: actor1.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        message: None,
        deps: Vec::new(),
        operations: vec![Operation {
            action: OpType::Make(ObjType::List),
            obj: ObjectID::Root,
            key: "conflict".into(),
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
            action: OpType::Make(ObjType::Map),
            obj: ObjectID::Root,
            key: "conflict".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let change3 = UnencodedChange {
        actor_id: actor2.clone(),
        seq: 2,
        start_op: 2,
        time: 0,
        message: None,
        deps: vec![change2.hash],
        operations: vec![Operation {
            action: OpType::Set(Value::F64(12.0)),
            obj: "1@83768a19a13842beb6dde8c68a662fad".try_into().unwrap(),
            key: "sparrow".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 3,
        actor: None,
        seq: None,
        clock: hashmap! {
            actor1.to_string() => 1,
            actor2.to_string() => 2,
        },
        can_redo: false,
        can_undo: false,
        deps: vec![change1.hash, change3.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "conflict".into() => hashmap!{
                    "1@9f17517523e54ee888e9cd51dfd7a572".into() => Diff::Unchanged(ObjDiff{
                       object_id: "1@9f17517523e54ee888e9cd51dfd7a572".try_into().unwrap(),
                       obj_type: ObjType::List,
                    }),
                    "1@83768a19a13842beb6dde8c68a662fad".into() => Diff::Map(MapDiff{
                       object_id: "1@83768a19a13842beb6dde8c68a662fad".try_into().unwrap(),
                       obj_type: ObjType::Map,
                       props: hashmap!{
                           "sparrow".into() => hashmap!{
                             "2@83768a19a13842beb6dde8c68a662fad".into() => Diff::Value(Value::F64(12.0))
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
    let change = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![Operation {
            action: OpType::Set(Value::Timestamp(1_586_528_122_277)),
            obj: ObjectID::Root,
            key: "now".into(),
            pred: Vec::new(),
            insert: false,
        }],
    }
    .encode();

    let expected_patch = Patch {
        version: 1,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        seq: None,
        actor: None,
        deps: vec![change.hash],
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "now".into() => hashmap!{
                    "1@955afa3bbcc140b3b4bac8836479d650".into() => Diff::Value(Value::Timestamp(1_586_528_122_277))
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
    let change = UnencodedChange {
        actor_id: actor.clone(),
        seq: 1,
        start_op: 1,
        time: 0,
        deps: Vec::new(),
        message: None,
        operations: vec![
            Operation {
                action: OpType::Make(ObjType::List),
                obj: ObjectID::Root,
                key: "list".into(),
                pred: Vec::new(),
                insert: false,
            },
            Operation {
                action: OpType::Set(Value::Timestamp(1_586_528_191_421)),
                obj: "1@27d467ecb1a640fb9bed448ce7cf6a44".try_into().unwrap(),
                key: ElementID::Head.into(),
                insert: true,
                pred: Vec::new(),
            },
        ],
    }
    .encode();

    let expected_patch = Patch {
        version: 1,
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        deps: vec![change.hash],
        actor: None,
        seq: None,
        diffs: Some(Diff::Map(MapDiff {
            object_id: ObjectID::Root.to_string(),
            obj_type: ObjType::Map,
            props: hashmap! {
                "list".into() => hashmap!{
                    "1@27d467ecb1a640fb9bed448ce7cf6a44".into() => Diff::Seq(SeqDiff{
                        object_id: "1@27d467ecb1a640fb9bed448ce7cf6a44".into(),
                        obj_type: ObjType::List,
                        edits: vec![DiffEdit::Insert{index: 0}],
                        props: hashmap!{
                            0 => hashmap!{
                                "2@27d467ecb1a640fb9bed448ce7cf6a44".into() => Diff::Value(Value::Timestamp(1_586_528_191_421))
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
