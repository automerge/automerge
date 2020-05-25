use automerge_frontend::{Frontend, Path, Value};
use automerge_protocol as amp;
use maplit::hashmap;

#[test]
fn set_object_root_properties() {
    let actor = amp::ActorID::random();
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor.op_id_at(1).into() => "magpie".into()
                }
            },
        })),
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"bird" => "magpie"})
    );
}

#[test]
fn reveal_conflicts_on_root_properties() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 > actor2)
    let actor1 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43")
            .unwrap()
            .as_bytes(),
    );
    let actor2 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd")
            .unwrap()
            .as_bytes(),
    );
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        clock: hashmap! {
            actor1.to_string() => 1,
            actor2.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "favouriteBird".into() => hashmap!{
                    actor1.op_id_at(1).into() => amp::Diff::Value("robin".into()),
                    actor2.op_id_at(1).into() => amp::Diff::Value("wagtail".into()),
                }
            },
        })),
    };
    let mut doc = Frontend::new();
    doc.apply_patch(patch).unwrap();

    assert_eq!(
        doc.state(),
        &Into::<Value>::into(hashmap! {"favouriteBird" => "wagtail"})
    );

    let conflicts = doc.get_conflicts(&Path::root().key("favouriteBird"));

    assert_eq!(
        conflicts,
        Some(hashmap! {
            actor1.op_id_at(1) => "robin".into(),
            actor2.op_id_at(1) => "wagtail".into(),
        })
    )
}

#[test]
fn create_nested_maps() {
    let actor = amp::ActorID::random();
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(2).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(3))
                            }
                        }
                    })
                }
            },
        })),
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => hashmap!{"wrens" => amp::Value::Int(3)}})
    );
}

#[test]
fn apply_updates_inside_nested_maps() {
    let actor = amp::ActorID::random();
    let patch1 = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(2).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(3))
                            }
                        }
                    })
                }
            },
        })),
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    let birds_id = frontend.get_object_id(&Path::root().key("birds")).unwrap();

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: birds_id.into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                actor.op_id_at(3).into() => amp::Diff::Value(amp::Value::Int(15))
                            }
                        }
                    })
                }
            },
        })),
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => hashmap!{"wrens" => amp::Value::Int(3), "sparrows" => amp::Value::Int(15)}}
        )
    );
}

#[test]
fn apply_updates_inside_map_conflicts() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 < actor2)
    let actor1 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43")
            .unwrap()
            .as_bytes(),
    );
    let actor2 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd")
            .unwrap()
            .as_bytes(),
    );
    let patch1 = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.to_string() => 1,
            actor2.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "favouriteBirds".into() => hashmap!{
                    actor1.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor1.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "blackbirds".into() => hashmap!{
                                actor1.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(1)),
                            }
                        },
                    }),
                    actor2.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor2.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor2.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(3)),
                            }
                        },
                    })
                }
            },
        })),
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"favouriteBirds" => hashmap!{"wrens" => amp::Value::Int(3)}}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("favouriteBirds"))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(1) => hashmap!{"blackbirds" => amp::Value::Int(1)}.into(),
            actor2.op_id_at(1) => hashmap!{"wrens" => amp::Value::Int(3)}.into(),
        }
    );

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.to_string() => 2,
            actor2.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "favouriteBirds".into() => hashmap!{
                    actor1.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor1.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "blackbirds".into() => hashmap!{
                                actor1.op_id_at(3).into() => amp::Diff::Value(amp::Value::Int(2)),
                            }
                        },
                    }),
                    actor2.op_id_at(1).into() => amp::Diff::Unchanged(amp::ObjDiff{
                        object_id: actor2.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                    })
                }
            },
        })),
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"favouriteBirds" => hashmap!{"wrens" => amp::Value::Int(3)}}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("favouriteBirds"))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(1) => hashmap!{"blackbirds" => amp::Value::Int(2)}.into(),
            actor2.op_id_at(1) => hashmap!{"wrens" => amp::Value::Int(3)}.into(),
        }
    );
}

#[test]
fn delete_keys_in_maps() {
    let actor = amp::ActorID::random();
    let mut frontend = Frontend::new();
    let patch1 = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "magpies".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Value(amp::Value::Int(2))
                },
                "sparrows".into() => hashmap!{
                    actor.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(15))
                }
            },
        })),
    };
    frontend.apply_patch(patch1).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"magpies" => amp::Value::Int(2), "sparrows" => amp::Value::Int(15)}
        )
    );

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "magpies".into() => hashmap!{}
            },
        })),
    };

    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"sparrows" => amp::Value::Int(15)})
    );
}

#[test]
fn create_lists() {
    let actor = amp::ActorID::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert { index: 0 }],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value("chaffinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };
    frontend.apply_patch(patch).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["chaffinch"]})
    )
}

#[test]
fn apply_updates_inside_lists() {
    let actor = amp::ActorID::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert { index: 0 }],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value("chaffinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };
    frontend.apply_patch(patch).unwrap();

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(3).into() => amp::Diff::Value("greenfinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };
    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["greenfinch"]})
    )
}

#[test]
fn apply_updates_inside_list_conflicts() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 < actor2)
    let actor1 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43")
            .unwrap()
            .as_bytes(),
    );
    let actor2 = amp::ActorID::from_bytes(
        uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd")
            .unwrap()
            .as_bytes(),
    );

    let other_actor = amp::ActorID::random();

    let patch1 = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            other_actor.to_string() => 1,
            actor1.to_string() => 1,
            actor2.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    other_actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: other_actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert{ index: 0}],
                        props: hashmap!{
                            0 => hashmap!{
                                actor1.op_id_at(2).into() => amp::Diff::Map(amp::MapDiff{
                                    object_id: actor1.op_id_at(2).into(),
                                    obj_type: amp::ObjType::Map,
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor1.op_id_at(3).into() => amp::Diff::Value("woodpecker".into()),
                                        },
                                        "numSeen".into() => hashmap!{
                                            actor1.op_id_at(4).into() => amp::Diff::Value(amp::Value::Int(1)),
                                        },
                                    }
                                }),
                                actor2.op_id_at(2).into() => amp::Diff::Map(amp::MapDiff{
                                    object_id: actor2.op_id_at(2).into(),
                                    obj_type: amp::ObjType::Map,
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor2.op_id_at(3).into() => amp::Diff::Value("lapwing".into()),
                                        },
                                        "numSeen".into() => hashmap!{
                                            actor2.op_id_at(4).into() => amp::Diff::Value(amp::Value::Int(2)),
                                        },
                                    }
                                }),
                            },
                        }
                    })
                }
            },
        })),
    };

    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => vec![hashmap!{"species" => amp::Value::Str("lapwing".to_string()), "numSeen" => amp::Value::Int(2)}]}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("birds").index(0))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(2) => hashmap!{
                "species" => amp::Value::Str("woodpecker".into()),
                "numSeen" => amp::Value::Int(1),
            }.into(),
            actor2.op_id_at(2) => hashmap!{
                "species" => amp::Value::Str("lapwing".into()),
                "numSeen" => amp::Value::Int(2),
            }.into(),
        }
    );

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.to_string() => 2,
            actor2.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    other_actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: other_actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: Vec::new(),
                        props: hashmap!{
                            0 => hashmap!{
                                actor1.op_id_at(2).into() => amp::Diff::Map(amp::MapDiff{
                                    object_id: actor1.op_id_at(2).into(),
                                    obj_type: amp::ObjType::Map,
                                    props: hashmap!{
                                        "numSeen".into() => hashmap!{
                                            actor1.op_id_at(5).into() => amp::Diff::Value(amp::Value::Int(2)),
                                        },
                                    }
                                }),
                                actor2.op_id_at(2).into() => amp::Diff::Unchanged(amp::ObjDiff{
                                    object_id: actor2.op_id_at(2).into(),
                                    obj_type: amp::ObjType::Map,
                                }),
                            },
                        }
                    })
                }
            },
        })),
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => vec![hashmap!{"species" => amp::Value::Str("lapwing".to_string()), "numSeen" => amp::Value::Int(2)}]}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("birds").index(0))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(2) => hashmap!{
                "species" => amp::Value::Str("woodpecker".into()),
                "numSeen" => amp::Value::Int(2),
            }.into(),
            actor2.op_id_at(2) => hashmap!{
                "species" => amp::Value::Str("lapwing".into()),
                "numSeen" => amp::Value::Int(2),
            }.into(),
        }
    );
}

#[test]
fn delete_list_elements() {
    let actor = amp::ActorID::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        version: 1,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 1,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert { index: 0 }, amp::DiffEdit::Insert { index: 1 }],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value("chaffinch".into())
                            },
                            1 => hashmap!{
                                actor.op_id_at(3).into() => amp::Diff::Value("goldfinch".into())
                            }
                        }
                    })
                }
            },
        })),
    };
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["chaffinch", "goldfinch"]})
    );

    let patch2 = amp::Patch {
        version: 2,
        actor: None,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.to_string() => 2,
        },
        can_undo: false,
        can_redo: false,
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Remove{ index: 0 }],
                        props: hashmap!{}
                    })
                }
            },
        })),
    };
    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["goldfinch"]})
    );
}

#[test]
fn apply_updates_at_different_levels_of_object_tree() {
    let actor = amp::ActorID::random();
    let patch1 = amp::Patch {
        version: 1,
        clock: hashmap! {actor.to_string() => 1},
        can_undo: false,
        can_redo: false,
        seq: None,
        actor: None,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "counts".into() => hashmap!{
                    actor.op_id_at(1).into() => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "magpie".into() => hashmap!{
                                actor.op_id_at(2).into() => amp::Diff::Value(amp::Value::Int(2))
                            }
                        }
                    })
                },
                "details".into() => hashmap!{
                    actor.op_id_at(3).into() => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(3).into(),
                        obj_type: amp::ObjType::List,
                        edits: vec![amp::DiffEdit::Insert{ index: 0 }],
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(4).into() => amp::Diff::Map(amp::MapDiff{
                                    object_id: actor.op_id_at(4).into(),
                                    obj_type: amp::ObjType::Map,
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor.op_id_at(5).into() => amp::Diff::Value("magpie".into())
                                        },
                                        "family".into() => hashmap!{
                                            actor.op_id_at(6).into() => amp::Diff::Value("Corvidae".into())
                                        }
                                    }
                                })
                            }
                        }
                    })
                },
            },
        })),
    };

    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {
            "counts" => Into::<Value>::into(hashmap!{"magpie".to_string() => amp::Value::Int(2)}),
            "details" => vec![Into::<Value>::into(hashmap!{
                "species" => "magpie",
                "family" => "Corvidae",
            })].into()
        })
    );

    let patch2 = amp::Patch {
        version: 2,
        clock: hashmap! {actor.to_string() => 2},
        can_undo: false,
        can_redo: false,
        seq: None,
        actor: None,
        deps: Vec::new(),
        diffs: Some(amp::Diff::Map(amp::MapDiff {
            object_id: amp::ObjectID::Root,
            obj_type: amp::ObjType::Map,
            props: hashmap! {
                "counts".into() => hashmap!{
                    actor.op_id_at(1) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(1).into(),
                        obj_type: amp::ObjType::Map,
                        props: hashmap!{
                            "magpie".into() => hashmap!{
                                actor.op_id_at(7) => amp::Diff::Value(amp::Value::Int(3))
                            }
                        }
                    })
                },
                "details".into() => hashmap!{
                    actor.op_id_at(3) => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(3).into(),
                        obj_type: amp::ObjType::List,
                        edits: Vec::new(),
                        props: hashmap!{
                            0 => hashmap!{
                                actor.op_id_at(4) => amp::Diff::Map(amp::MapDiff{
                                    object_id: actor.op_id_at(4).into(),
                                    obj_type: amp::ObjType::Map,
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor.op_id_at(8) => amp::Diff::Value("Eurasian magpie".into())
                                        },
                                    }
                                })
                            }
                        }
                    })
                },
            },
        })),
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {
            "counts" => Into::<Value>::into(hashmap!{"magpie".to_string() => amp::Value::Int(3)}),
            "details" => vec![Into::<Value>::into(hashmap!{
                "species" => "Eurasian magpie",
                "family" => "Corvidae",
            })].into()
        })
    );
}
