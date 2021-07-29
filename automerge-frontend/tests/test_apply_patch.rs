use std::{collections::HashMap, convert::TryInto, num::NonZeroU64};

use amp::RootDiff;
use automerge_frontend::{Frontend, Path, Primitive, Value};
use automerge_protocol as amp;
use maplit::hashmap;
use unicode_segmentation::UnicodeSegmentation;

#[test]
fn set_object_root_properties() {
    let actor = amp::ActorId::random();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 1,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => "magpie".into()
                }
            },
        },
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"bird" => "magpie"})
    );
}

#[test]
fn set_bytes_value() {
    let actor = amp::ActorId::random();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 1,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: amp::RootDiff {
            props: hashmap! {
                "bird".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Value(amp::ScalarValue::Bytes(vec![1, 2, 3])),
                }
            },
        },
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"bird" => Primitive::Bytes(vec![1, 2, 3])})
    );
}

#[test]
fn reveal_conflicts_on_root_properties() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 > actor2)
    let actor1 =
        amp::ActorId::from(uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43").unwrap());
    let actor2 =
        amp::ActorId::from(uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd").unwrap());
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        clock: hashmap! {
            actor1.clone() => NonZeroU64::new(1).unwrap(),
            actor2.clone() => NonZeroU64::new(2).unwrap(),
        },
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "favouriteBird".into() => hashmap!{
                    actor1.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Value("robin".into()),
                    actor2.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Value("wagtail".into()),
                }
            },
        },
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
            actor1.op_id_at(NonZeroU64::new(1).unwrap()) => "robin".into(),
            actor2.op_id_at(NonZeroU64::new(1).unwrap()) => "wagtail".into(),
        })
    )
}

#[test]
fn create_nested_maps() {
    let actor = amp::ActorId::random();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 3,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(3))
                            }
                        }
                    })
                }
            },
        },
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => hashmap!{"wrens" => Primitive::Int(3)}})
    );
}

#[test]
fn apply_updates_inside_nested_maps() {
    let actor = amp::ActorId::random();
    let patch1 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(3))
                            }
                        }
                    })
                }
            },
        },
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    let birds_id = frontend.get_object_id(&Path::root().key("birds")).unwrap();

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 3,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: birds_id,
                        props: hashmap!{
                            "sparrows".into() => hashmap!{
                                actor.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(15))
                            }
                        }
                    })
                }
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => hashmap!{"wrens" => Primitive::Int(3), "sparrows" => Primitive::Int(15)}}
        )
    );
}

#[test]
fn apply_updates_inside_map_conflicts() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 < actor2)
    let actor1 =
        amp::ActorId::from(uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43").unwrap());
    let actor2 =
        amp::ActorId::from(uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd").unwrap());
    let patch1 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.clone() => NonZeroU64::new(1).unwrap(),
            actor2.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "favouriteBirds".into() => hashmap!{
                    actor1.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor1.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: hashmap!{
                            "blackbirds".into() => hashmap!{
                                actor1.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(1)),
                            }
                        },
                    }),
                    actor2.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor2.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: hashmap!{
                            "wrens".into() => hashmap!{
                                actor2.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(3)),
                            }
                        },
                    })
                }
            },
        },
    };
    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"favouriteBirds" => hashmap!{"wrens" => Primitive::Int(3)}})
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("favouriteBirds"))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(NonZeroU64::new(1).unwrap()) => hashmap!{"blackbirds" => Primitive::Int(1)}.into(),
            actor2.op_id_at(NonZeroU64::new(1).unwrap()) => hashmap!{"wrens" => Primitive::Int(3)}.into(),
        }
    );

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 1,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.clone() => NonZeroU64::new(2).unwrap(),
            actor2.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "favouriteBirds".into() => hashmap!{
                    actor1.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor1.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: hashmap!{
                            "blackbirds".into() => hashmap!{
                                actor1.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(2)),
                            }
                        },
                    }),
                    actor2.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor2.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: HashMap::new(),
                    })
                }
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"favouriteBirds" => hashmap!{"wrens" => Primitive::Int(3)}})
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("favouriteBirds"))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(NonZeroU64::new(1).unwrap()) => hashmap!{"blackbirds" => Primitive::Int(2)}.into(),
            actor2.op_id_at(NonZeroU64::new(1).unwrap()) => hashmap!{"wrens" => Primitive::Int(3)}.into(),
        }
    );
}

#[test]
fn delete_keys_in_maps() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch1 = amp::Patch {
        actor: None,
        max_op: 2,
        pending_changes: 0,
        seq: None,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "magpies".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(2))
                },
                "sparrows".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(15))
                }
            },
        },
    };
    frontend.apply_patch(patch1).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"magpies" => Primitive::Int(2), "sparrows" => Primitive::Int(15)}
        )
    );

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 3,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "magpies".into() => hashmap!{}
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"sparrows" => Primitive::Int(15)})
    );
}

#[test]
fn create_lists() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![amp::DiffEdit::SingleElementInsert {
                            index: 0,
                            elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                            op_id: actor.op_id_at(NonZeroU64::new(2).unwrap()),
                            value: amp::Diff::Value("chaffinch".into()),
                        }],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["chaffinch"]})
    )
}

#[test]
fn apply_updates_inside_lists() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 1,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![amp::DiffEdit::SingleElementInsert {
                            index: 0,
                            elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                            op_id: actor.op_id_at(NonZeroU64::new(2).unwrap()),
                            value: amp::Diff::Value("chaffinch".into()),
                        }],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch).unwrap();

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 3,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![amp::DiffEdit::Update{
                            index: 0,
                            op_id: actor.op_id_at(NonZeroU64::new(3).unwrap()),
                            value: amp::Diff::Value("greenfinch".into()),
                        }],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["greenfinch"]})
    )
}

#[test]
fn apply_multi_insert_updates_inside_lists() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 1,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff {
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![amp::DiffEdit::MultiElementInsert(amp::MultiElementInsert {
                            index: 0,
                            elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                            values: vec![
                                amp::ScalarValue::Str("greenfinch".into()),
                                amp::ScalarValue::Str("bullfinch".into()),
                            ].try_into().unwrap()
                        })],
                    })
                }
            },
        },
        pending_changes: 0,
    };
    frontend.apply_patch(patch).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["greenfinch", "bullfinch"]})
    )
}

#[test]
fn apply_updates_inside_list_conflicts() {
    // We don't just use random actor IDs because we need to have a specific
    // ordering (actor1 < actor2)
    let actor1 =
        amp::ActorId::from(uuid::Uuid::parse_str("02ef21f3-c9eb-4087-880e-bedd7c4bbe43").unwrap());
    let actor2 =
        amp::ActorId::from(uuid::Uuid::parse_str("2a1d376b-24f7-4400-8d4a-f58252d644dd").unwrap());

    let other_actor = amp::ActorId::random();

    // A patch to create conflicting value at index 0
    let patch1 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 2,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            other_actor.clone() => NonZeroU64::new(1).unwrap(),
            actor1.clone() => NonZeroU64::new(1).unwrap(),
            actor2.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    other_actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: other_actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::SingleElementInsert{
                                index: 0,
                                elem_id: actor1.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                op_id: actor1.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Map(amp::MapDiff{
                                    object_id: actor1.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor1.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::Value("woodpecker".into()),
                                        },
                                        "numSeen".into() => hashmap!{
                                            actor1.op_id_at(NonZeroU64::new(4).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(1)),
                                        },
                                    }
                                }),
                            },
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Map(amp::MapDiff{
                                    object_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                    props: hashmap!{
                                        "species".into() => hashmap!{
                                            actor2.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::Value("lapwing".into()),
                                        },
                                        "numSeen".into() => hashmap!{
                                            actor2.op_id_at(NonZeroU64::new(4).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(2)),
                                        },
                                    }
                                }),
                            },
                            ],
                    })
                }
            },
        },
    };

    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => vec![hashmap!{"species" => Primitive::Str("lapwing".into()), "numSeen" => Primitive::Int(2)}]}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("birds").index(0))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(NonZeroU64::new(2).unwrap()) => hashmap!{
                "species" => Primitive::Str("woodpecker".into()),
                "numSeen" => Primitive::Int(1),
            }.into(),
            actor2.op_id_at(NonZeroU64::new(2).unwrap()) => hashmap!{
                "species" => Primitive::Str("lapwing".into()),
                "numSeen" => Primitive::Int(2),
            }.into(),
        }
    );

    // Update the conflicting values
    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 5,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor1.clone() => NonZeroU64::new(2).unwrap(),
            actor2.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    other_actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: other_actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor1.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Map(amp::MapDiff{
                                    object_id: actor1.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                    props: hashmap!{
                                        "numSeen".into() => hashmap!{
                                            actor1.op_id_at(NonZeroU64::new(5).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(2)),
                                        },
                                    }
                                })
                            },
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Map(amp::MapDiff{
                                    object_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                    props: HashMap::new(),
                                })
                            }
                        ],
                    })
                }
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => vec![hashmap!{"species" => Primitive::Str("lapwing".into()), "numSeen" => Primitive::Int(2)}]}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("birds").index(0))
            .unwrap(),
        hashmap! {
            actor1.op_id_at(NonZeroU64::new(2).unwrap()) => hashmap!{
                "species" => Primitive::Str("woodpecker".into()),
                "numSeen" => Primitive::Int(2),
            }.into(),
            actor2.op_id_at(NonZeroU64::new(2).unwrap()) => hashmap!{
                "species" => Primitive::Str("lapwing".into()),
                "numSeen" => Primitive::Int(2),
            }.into(),
        }
    );

    // Remove one of the conflicting values
    let patch3 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 5,
        deps: Vec::new(),
        clock: hashmap! {
            actor1 => NonZeroU64::new(2).unwrap(),
            actor2.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    other_actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: other_actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::Update{
                                index: 0,
                                op_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Map(amp::MapDiff{
                                    object_id: actor2.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                    props: HashMap::new(),
                                })
                            }
                        ],
                    })
                }
            },
        },
        pending_changes: 0,
    };

    frontend.apply_patch(patch3).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"birds" => vec![hashmap!{"species" => Primitive::Str("lapwing".into()), "numSeen" => Primitive::Int(2)}]}
        )
    );

    assert_eq!(
        frontend
            .get_conflicts(&Path::root().key("birds").index(0))
            .unwrap(),
        hashmap! {
            actor2.op_id_at(NonZeroU64::new(2).unwrap()) => hashmap!{
                "species" => Primitive::Str("lapwing".into()),
                "numSeen" => Primitive::Int(2),
            }.into(),
        }
    );
}

#[test]
fn delete_list_elements() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 3,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(1).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::SingleElementInsert {
                                index: 0,
                                elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Value("chaffinch".into())
                            },
                            amp::DiffEdit::SingleElementInsert {
                                index: 1,
                                elem_id: actor.op_id_at(NonZeroU64::new(3).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(3).unwrap()),
                                value: amp::Diff::Value("goldfinch".into()),
                            },
                        ],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["chaffinch", "goldfinch"]})
    );

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 4,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "birds".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![amp::DiffEdit::Remove{ index: 0, count: 1 }],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch2).unwrap();
    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {"birds" => vec!["goldfinch"]})
    );
}

#[test]
fn apply_updates_at_different_levels_of_object_tree() {
    let actor = amp::ActorId::random();
    let patch1 = amp::Patch {
        clock: hashmap! {actor.clone() => NonZeroU64::new(1).unwrap()},
        seq: None,
        max_op: 6,
        pending_changes: 0,
        actor: None,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "counts".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: hashmap!{
                            "magpie".into() => hashmap!{
                                actor.op_id_at(NonZeroU64::new(2).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(2))
                            }
                        }
                    })
                },
                "details".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(3).unwrap()).into(),
                        edits: vec![amp::DiffEdit::SingleElementInsert{
                            index: 0,
                            elem_id: actor.op_id_at(NonZeroU64::new(4).unwrap()).into(),
                            op_id: actor.op_id_at(NonZeroU64::new(4).unwrap()),
                            value:  amp::Diff::Map(amp::MapDiff{
                                object_id: actor.op_id_at(NonZeroU64::new(4).unwrap()).into(),
                                props: hashmap!{
                                    "species".into() => hashmap!{
                                        actor.op_id_at(NonZeroU64::new(5).unwrap()) => amp::Diff::Value("magpie".into())
                                    },
                                    "family".into() => hashmap!{
                                        actor.op_id_at(NonZeroU64::new(6).unwrap()) => amp::Diff::Value("Corvidae".into())
                                    }
                                }
                            })
                        }],
                    })
                },
            },
        },
    };

    let mut frontend = Frontend::new();
    frontend.apply_patch(patch1).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {
            "counts" => Into::<Value>::into(hashmap!{"magpie".to_string() => Primitive::Int(2)}),
            "details" => vec![Into::<Value>::into(hashmap!{
                "species" => "magpie",
                "family" => "Corvidae",
            })].into()
        })
    );

    let patch2 = amp::Patch {
        clock: hashmap! {actor.clone() => NonZeroU64::new(2).unwrap()},
        seq: None,
        max_op: 7,
        pending_changes: 0,
        actor: None,
        deps: Vec::new(),
        diffs: RootDiff {
            props: hashmap! {
                "counts".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Map(amp::MapDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        props: hashmap!{
                            "magpie".into() => hashmap!{
                                actor.op_id_at(NonZeroU64::new(7).unwrap()) => amp::Diff::Value(amp::ScalarValue::Int(3))
                            }
                        }
                    })
                },
                "details".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(3).unwrap()) => amp::Diff::List(amp::ListDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(3).unwrap()).into(),
                        edits: vec![amp::DiffEdit::Update{
                            index: 0,
                            op_id: actor.op_id_at(NonZeroU64::new(4).unwrap()),
                            value: amp::Diff::Map(amp::MapDiff{
                                object_id: actor.op_id_at(NonZeroU64::new(4).unwrap()).into(),
                                props: hashmap!{
                                    "species".into() => hashmap!{
                                        actor.op_id_at(NonZeroU64::new(8).unwrap()) => amp::Diff::Value("Eurasian magpie".into())
                                    },
                                }
                            }),
                        }],
                    })
                },
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(hashmap! {
            "counts" => Into::<Value>::into(hashmap!{"magpie".to_string() => Primitive::Int(3)}),
            "details" => vec![Into::<Value>::into(hashmap!{
                "species" => "Eurasian magpie",
                "family" => "Corvidae",
            })].into()
        })
    );
}

#[test]
fn test_text_objects() {
    let actor = amp::ActorId::random();
    let mut frontend = Frontend::new();
    let patch = amp::Patch {
        actor: None,
        seq: None,
        max_op: 4,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(2).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "name".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Text(amp::TextDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::SingleElementInsert {
                                index: 0,
                                elem_id: actor.op_id_at(NonZeroU64::new(2).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(2).unwrap()),
                                value: amp::Diff::Value("b".into()),
                            },
                            amp::DiffEdit::SingleElementInsert {
                                index: 1,
                                elem_id: actor.op_id_at(NonZeroU64::new(3).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(3).unwrap()),
                                value: amp::Diff::Value("e".into()),
                            },
                            amp::DiffEdit::SingleElementInsert {
                                index: 2,
                                elem_id: actor.op_id_at(NonZeroU64::new(4).unwrap()).into(),
                                op_id: actor.op_id_at(NonZeroU64::new(4).unwrap()),
                                value: amp::Diff::Value("n".into()),
                            },
                        ],
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"name" => Value::Text("ben".graphemes(true).map(|s|s.into()).collect())}
        )
    );

    let patch2 = amp::Patch {
        actor: None,
        seq: None,
        max_op: 5,
        pending_changes: 0,
        deps: Vec::new(),
        clock: hashmap! {
            actor.clone() => NonZeroU64::new(3).unwrap(),
        },
        diffs: RootDiff {
            props: hashmap! {
                "name".into() => hashmap!{
                    actor.op_id_at(NonZeroU64::new(1).unwrap()) => amp::Diff::Text(amp::TextDiff{
                        object_id: actor.op_id_at(NonZeroU64::new(1).unwrap()).into(),
                        edits: vec![
                            amp::DiffEdit::Remove { index: 1, count: 1 },
                            amp::DiffEdit::Update{
                                index: 1,
                                op_id: actor.op_id_at(NonZeroU64::new(5).unwrap()),
                                value: amp::Diff::Value(amp::ScalarValue::Str("i".into())),
                            }
                        ],
                    })
                }
            },
        },
    };

    frontend.apply_patch(patch2).unwrap();

    assert_eq!(
        frontend.state(),
        &Into::<Value>::into(
            hashmap! {"name" => Value::Text("bi".graphemes(true).map(|s|s.into()).collect())}
        )
    );
}

#[test]
fn test_unchanged_diff_creates_empty_objects() {
    let mut doc = Frontend::new();
    let patch = amp::Patch {
        actor: Some(doc.actor_id.clone()),
        seq: Some(NonZeroU64::new(1).unwrap()),
        clock: hashmap! {doc.actor_id.clone() => NonZeroU64::new(1).unwrap()},
        deps: Vec::new(),
        max_op: 1,
        pending_changes: 0,
        diffs: RootDiff {
            props: hashmap! {
                "text".into() => hashmap!{
                    "1@cfe5fefb771f4c15a716d488012cbf40".try_into().unwrap() =>  amp::Diff::Text(amp::TextDiff{
                        object_id: "1@cfe5fefb771f4c15a716d488012cbf40".try_into().unwrap(),
                        edits: Vec::new(),
                    })
                }
            },
        },
    };
    doc.apply_patch(patch).unwrap();
    assert_eq!(
        doc.state(),
        &Value::Map(hashmap! {"text".into() => Value::Text(Vec::new())},),
    );
}
