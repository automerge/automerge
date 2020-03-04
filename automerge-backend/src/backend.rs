use crate::{
    ActorID, AutomergeError, Change, ChangeRequest, ChangeRequestType, Clock, Diff, OpSet, Patch,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    op_set: OpSet,
}

impl Backend {
    pub fn init() -> Backend {
        Backend {
            op_set: OpSet::init(),
        }
    }

    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<Patch, AutomergeError> {
        let nested_diffs = changes
            .into_iter()
            .map(|c| self.op_set.apply_change(c, false))
            .collect::<Result<Vec<Vec<Diff>>, AutomergeError>>()?;
        let diffs = nested_diffs.into_iter().flatten().collect();
        Ok(Patch {
            actor: None,
            can_undo: self.op_set.can_undo(),
            can_redo: self.op_set.can_redo(),
            clock: self.op_set.clock.clone(),
            deps: self.op_set.clock.clone(),
            diffs,
            seq: None,
        })
    }

    pub fn apply_local_change(&mut self, change: ChangeRequest) -> Result<Patch, AutomergeError> {
        let actor_id = change.actor_id.clone();
        let seq = change.seq;
        if self.op_set.clock.seq_for(&actor_id) >= seq {
            return Err(AutomergeError::DuplicateChange(format!(
                "Change request has already been applied {} {}",
                actor_id.0, seq
            )));
        }
        match change.request_type {
            ChangeRequestType::Change(ops) => {
                let diffs = self.op_set.apply_change(
                    Change {
                        actor_id: change.actor_id,
                        operations: ops,
                        seq,
                        message: change.message,
                        dependencies: change.dependencies,
                    },
                    true,
                )?;
                Ok(Patch {
                    actor: Some(actor_id),
                    can_undo: self.op_set.can_undo(),
                    can_redo: self.op_set.can_redo(),
                    clock: self.op_set.clock.clone(),
                    deps: self.op_set.clock.clone(),
                    diffs,
                    seq: Some(seq),
                })
            }
            ChangeRequestType::Undo => {
                let diffs = self.op_set.do_undo(
                    change.actor_id.clone(),
                    change.seq,
                    change.message,
                    change.dependencies,
                )?;
                Ok(Patch {
                    actor: Some(actor_id),
                    can_undo: self.op_set.can_undo(),
                    can_redo: self.op_set.can_redo(),
                    clock: self.op_set.clock.clone(),
                    deps: self.op_set.clock.clone(),
                    diffs,
                    seq: Some(seq),
                })
            }
            ChangeRequestType::Redo => {
                let diffs = self.op_set.do_redo(
                    change.actor_id.clone(),
                    change.seq,
                    change.message,
                    change.dependencies,
                )?;
                Ok(Patch {
                    actor: Some(actor_id),
                    can_undo: self.op_set.can_undo(),
                    can_redo: self.op_set.can_redo(),
                    clock: self.op_set.clock.clone(),
                    deps: self.op_set.clock.clone(),
                    diffs,
                    seq: Some(seq),
                })
            }
        }
    }

    pub fn get_patch(&self) -> Patch {
        Patch {
            can_undo: false,
            can_redo: false,
            clock: self.op_set.clock.clone(),
            deps: self.op_set.clock.clone(),
            diffs: self.op_set.object_store.generate_diffs(),
            actor: None,
            seq: None,
        }
    }

    /// Get changes which are in `other` but not in this backend
    pub fn get_changes(&self, other: &Backend) -> Vec<Change> {
        other.op_set.get_missing_changes(&self.op_set.clock)
    }

    pub fn get_changes_for_actor_id(&self, actor_id: ActorID) -> Vec<Change> {
        self.op_set.get_changes_for_actor_id(&actor_id)
    }

    pub fn get_missing_changes(&self, clock: Clock) -> Vec<Change> {
        self.op_set.get_missing_changes(&clock)
    }

    pub fn get_missing_deps(&self) -> Clock {
        self.op_set.get_missing_deps()
    }

    pub fn merge(&mut self, remote: &Backend) -> Result<Patch, AutomergeError> {
        let missing_changes = remote.get_missing_changes(self.op_set.clock.clone());
        self.apply_changes(missing_changes)
    }

    pub fn clock(&self) -> Clock {
        self.op_set.clock.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorID, Backend, Change, ChangeRequest, ChangeRequestType, Clock, Conflict, DataType,
        Diff, DiffAction, ElementID, ElementValue, Key, MapType, ObjectID, Operation, Patch,
        PrimitiveValue, SequenceType,
    };

    struct ApplyChangeTestCase {
        name: &'static str,
        changes: Vec<Change>,
        expected_patch: Patch,
    }

    #[test]
    fn test_diffs() {
        let actor1 = ActorID::from_string("actor1".to_string());
        let actor2 = ActorID::from_string("actor2".to_string());
        let testcases = vec![
            ApplyChangeTestCase {
                name: "Assign to key in map",
                changes: vec![Change {
                    actor_id: actor1.clone(),
                    seq: 1,
                    dependencies: Clock::empty(),
                    message: None,
                    operations: vec![Operation::Set {
                        object_id: ObjectID::Root,
                        key: Key("bird".to_string()),
                        value: PrimitiveValue::Str("magpie".to_string()),
                        datatype: None,
                    }],
                }],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 1),
                    deps: Clock::empty().with_dependency(&actor1, 1),
                    diffs: vec![Diff {
                        action: DiffAction::SetMapKey(
                            ObjectID::Root,
                            MapType::Map,
                            Key("bird".to_string()),
                            ElementValue::Primitive(PrimitiveValue::Str("magpie".to_string())),
                            None,
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "Increment a key in a map",
                changes: vec![
                    Change {
                        actor_id: actor1.clone(),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Set {
                            object_id: ObjectID::Root,
                            key: Key("counter".to_string()),
                            value: PrimitiveValue::Number(1.0),
                            datatype: Some(DataType::Counter),
                        }],
                    },
                    Change {
                        actor_id: actor1.clone(),
                        seq: 2,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Increment {
                            object_id: ObjectID::Root,
                            key: Key("counter".to_string()),
                            value: 2.0,
                        }],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 2),
                    deps: Clock::empty().with_dependency(&actor1, 2),
                    diffs: vec![Diff {
                        action: DiffAction::SetMapKey(
                            ObjectID::Root,
                            MapType::Map,
                            Key("counter".to_string()),
                            ElementValue::Primitive(PrimitiveValue::Number(3.0)),
                            Some(DataType::Counter),
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "should make a conflict on assignment to the same key",
                changes: vec![
                    Change {
                        actor_id: ActorID::from_string("actor1".to_string()),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Set {
                            object_id: ObjectID::Root,
                            key: Key("bird".to_string()),
                            value: PrimitiveValue::Str("magpie".to_string()),
                            datatype: None,
                        }],
                    },
                    Change {
                        actor_id: ActorID::from_string("actor2".to_string()),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Set {
                            object_id: ObjectID::Root,
                            key: Key("bird".to_string()),
                            value: PrimitiveValue::Str("blackbird".to_string()),
                            datatype: None,
                        }],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty()
                        .with_dependency(&actor1, 1)
                        .with_dependency(&actor2, 1),
                    deps: Clock::empty()
                        .with_dependency(&actor1, 1)
                        .with_dependency(&actor2, 1),
                    diffs: vec![Diff {
                        action: DiffAction::SetMapKey(
                            ObjectID::Root,
                            MapType::Map,
                            Key("bird".to_string()),
                            ElementValue::Primitive(PrimitiveValue::Str("blackbird".to_string())),
                            None,
                        ),
                        conflicts: vec![Conflict {
                            actor: actor1.clone(),
                            value: ElementValue::Primitive(PrimitiveValue::Str(
                                "magpie".to_string(),
                            )),
                            datatype: None,
                        }],
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "delete a key from a map",
                changes: vec![
                    Change {
                        actor_id: actor1.clone(),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Set {
                            object_id: ObjectID::Root,
                            key: Key("bird".to_string()),
                            value: PrimitiveValue::Str("magpie".to_string()),
                            datatype: None,
                        }],
                    },
                    Change {
                        actor_id: actor1.clone(),
                        seq: 2,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Delete {
                            object_id: ObjectID::Root,
                            key: Key("bird".to_string()),
                        }],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 2),
                    deps: Clock::empty().with_dependency(&actor1, 2),
                    diffs: vec![Diff {
                        action: DiffAction::RemoveMapKey(
                            ObjectID::Root,
                            MapType::Map,
                            Key("bird".to_string()),
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "create nested maps",
                changes: vec![Change {
                    actor_id: actor1.clone(),
                    seq: 1,
                    dependencies: Clock::empty(),
                    message: None,
                    operations: vec![
                        Operation::MakeMap {
                            object_id: ObjectID::ID("birds".to_string()),
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("birds".to_string()),
                            key: Key("wrens".to_string()),
                            value: PrimitiveValue::Number(3.0),
                            datatype: None,
                        },
                        Operation::Link {
                            object_id: ObjectID::Root,
                            key: Key("birds".to_string()),
                            value: ObjectID::ID("birds".to_string()),
                        },
                    ],
                }],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 1),
                    deps: Clock::empty().with_dependency(&actor1, 1),
                    diffs: vec![
                        Diff {
                            action: DiffAction::CreateMap(
                                ObjectID::ID("birds".to_string()),
                                MapType::Map,
                            ),
                            conflicts: Vec::new(),
                        },
                        Diff {
                            action: DiffAction::SetMapKey(
                                ObjectID::ID("birds".to_string()),
                                MapType::Map,
                                Key("wrens".to_string()),
                                ElementValue::Primitive(PrimitiveValue::Number(3.0)),
                                None,
                            ),
                            conflicts: Vec::new(),
                        },
                        Diff {
                            action: DiffAction::SetMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                                ElementValue::Link(ObjectID::ID("birds".to_string())),
                                None,
                            ),
                            conflicts: Vec::new(),
                        },
                    ],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "create lists",
                changes: vec![Change {
                    actor_id: actor1.clone(),
                    seq: 1,
                    dependencies: Clock::empty(),
                    message: None,
                    operations: vec![
                        Operation::MakeList {
                            object_id: ObjectID::ID("birds".to_string()),
                        },
                        Operation::Insert {
                            list_id: ObjectID::ID("birds".to_string()),
                            key: ElementID::Head,
                            elem: 1,
                        },
                        Operation::Set {
                            object_id: ObjectID::ID("birds".to_string()),
                            key: ElementID::from_actor_and_elem(actor1.clone(), 1).as_key(),
                            value: PrimitiveValue::Str("chaffinch".to_string()),
                            datatype: None,
                        },
                        Operation::Link {
                            object_id: ObjectID::Root,
                            key: Key("birds".to_string()),
                            value: ObjectID::ID("birds".to_string()),
                        },
                    ],
                }],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 1),
                    deps: Clock::empty().with_dependency(&actor1, 1),
                    diffs: vec![
                        Diff {
                            action: DiffAction::CreateList(
                                ObjectID::ID("birds".to_string()),
                                SequenceType::List,
                            ),
                            conflicts: Vec::new(),
                        },
                        Diff {
                            action: DiffAction::InsertSequenceElement(
                                ObjectID::ID("birds".to_string()),
                                SequenceType::List,
                                0,
                                ElementValue::Primitive(PrimitiveValue::Str(
                                    "chaffinch".to_string(),
                                )),
                                None,
                                ElementID::from_actor_and_elem(actor1.clone(), 1),
                            ),
                            conflicts: Vec::new(),
                        },
                        Diff {
                            action: DiffAction::SetMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                                ElementValue::Link(ObjectID::ID("birds".to_string())),
                                None,
                            ),
                            conflicts: Vec::new(),
                        },
                    ],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "apply update inside lists",
                changes: vec![
                    Change {
                        actor_id: actor1.clone(),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![
                            Operation::MakeList {
                                object_id: ObjectID::ID("birds".to_string()),
                            },
                            Operation::Insert {
                                list_id: ObjectID::ID("birds".to_string()),
                                key: ElementID::Head,
                                elem: 1,
                            },
                            Operation::Set {
                                object_id: ObjectID::ID("birds".to_string()),
                                key: Key("actor1:1".to_string()),
                                value: PrimitiveValue::Str("chaffinch".to_string()),
                                datatype: None,
                            },
                            Operation::Link {
                                object_id: ObjectID::Root,
                                key: Key("birds".to_string()),
                                value: ObjectID::ID("birds".to_string()),
                            },
                        ],
                    },
                    Change {
                        actor_id: actor1.clone(),
                        seq: 2,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Set {
                            object_id: ObjectID::ID("birds".to_string()),
                            key: Key("actor1:1".to_string()),
                            value: PrimitiveValue::Str("greenfinch".to_string()),
                            datatype: None,
                        }],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 2),
                    deps: Clock::empty().with_dependency(&actor1, 2),
                    diffs: vec![Diff {
                        action: DiffAction::SetSequenceElement(
                            ObjectID::ID("birds".to_string()),
                            SequenceType::List,
                            0,
                            ElementValue::Primitive(PrimitiveValue::Str("greenfinch".to_string())),
                            None,
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "delete list elements",
                changes: vec![
                    Change {
                        actor_id: actor1.clone(),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![
                            Operation::MakeList {
                                object_id: ObjectID::ID("birds".to_string()),
                            },
                            Operation::Insert {
                                list_id: ObjectID::ID("birds".to_string()),
                                key: ElementID::Head,
                                elem: 1,
                            },
                            Operation::Set {
                                object_id: ObjectID::ID("birds".to_string()),
                                key: Key("actor1:1".to_string()),
                                value: PrimitiveValue::Str("chaffinch".to_string()),
                                datatype: None,
                            },
                            Operation::Link {
                                object_id: ObjectID::Root,
                                key: Key("birds".to_string()),
                                value: ObjectID::ID("birds".to_string()),
                            },
                        ],
                    },
                    Change {
                        actor_id: actor1.clone(),
                        seq: 2,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![Operation::Delete {
                            object_id: ObjectID::ID("birds".to_string()),
                            key: Key("actor1:1".to_string()),
                        }],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 2),
                    deps: Clock::empty().with_dependency(&actor1, 2),
                    diffs: vec![Diff {
                        action: DiffAction::RemoveSequenceElement(
                            ObjectID::ID("birds".to_string()),
                            SequenceType::List,
                            0,
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
            ApplyChangeTestCase {
                name: "Handle list element insertion and deletion in the same change",
                changes: vec![
                    Change {
                        actor_id: actor1.clone(),
                        seq: 1,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![
                            Operation::MakeList {
                                object_id: ObjectID::ID("birds".to_string()),
                            },
                            Operation::Link {
                                object_id: ObjectID::Root,
                                key: Key("birds".to_string()),
                                value: ObjectID::ID("birds".to_string()),
                            },
                        ],
                    },
                    Change {
                        actor_id: actor1.clone(),
                        seq: 2,
                        dependencies: Clock::empty(),
                        message: None,
                        operations: vec![
                            Operation::Insert {
                                list_id: ObjectID::ID("birds".to_string()),
                                key: ElementID::Head,
                                elem: 1,
                            },
                            Operation::Delete {
                                object_id: ObjectID::ID("birds".to_string()),
                                key: Key("actor:1".to_string()),
                            },
                        ],
                    },
                ],
                expected_patch: Patch {
                    can_undo: false,
                    can_redo: false,
                    clock: Clock::empty().with_dependency(&actor1, 2),
                    deps: Clock::empty().with_dependency(&actor1, 2),
                    diffs: vec![Diff {
                        action: DiffAction::MaxElem(
                            ObjectID::ID("birds".to_string()),
                            1,
                            SequenceType::List,
                        ),
                        conflicts: Vec::new(),
                    }],
                    seq: None,
                    actor: None,
                },
            },
        ];

        for testcase in testcases {
            let mut backend = Backend::init();
            let patches = testcase
                .changes
                .into_iter()
                .map(|c| backend.apply_changes(vec![c]).unwrap());
            assert_eq!(
                testcase.expected_patch,
                patches.last().unwrap(),
                "Patches not equal for {}",
                testcase.name
            );
        }
    }

    struct ApplyLocalChangeTestCase {
        name: &'static str,
        change_requests: Vec<ChangeRequest>,
        expected_patches: Vec<Patch>,
    }

    #[test]
    fn test_apply_local_change() {
        let actor1 = ActorID("actor1".to_string());
        let birds = ObjectID::ID("birds".to_string());
        let testcases = vec![
            ApplyLocalChangeTestCase {
                name: "Should undo",
                change_requests: vec![
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 1,
                        message: None,
                        dependencies: Clock::empty(),
                        request_type: ChangeRequestType::Change(vec![
                            Operation::MakeMap {
                                object_id: birds.clone(),
                            },
                            Operation::Link {
                                object_id: ObjectID::Root,
                                key: Key("birds".to_string()),
                                value: birds.clone(),
                            },
                            Operation::Set {
                                object_id: birds.clone(),
                                key: Key("chaffinch".to_string()),
                                value: PrimitiveValue::Boolean(true),
                                datatype: None,
                            },
                        ]),
                    },
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 2,
                        message: None,
                        dependencies: Clock::empty().with_dependency(&actor1, 1),
                        request_type: ChangeRequestType::Undo,
                    },
                ],
                expected_patches: vec![
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: false,
                        can_undo: true,
                        seq: Some(1),
                        clock: Clock::empty().with_dependency(&actor1, 1),
                        deps: Clock::empty().with_dependency(&actor1, 1),
                        diffs: vec![
                            Diff {
                                action: DiffAction::CreateMap(birds.clone(), MapType::Map),
                                conflicts: Vec::new(),
                            },
                            Diff {
                                action: DiffAction::SetMapKey(
                                    ObjectID::Root,
                                    MapType::Map,
                                    Key("birds".to_string()),
                                    ElementValue::Link(birds.clone()),
                                    None,
                                ),
                                conflicts: Vec::new(),
                            },
                            Diff {
                                action: DiffAction::SetMapKey(
                                    birds.clone(),
                                    MapType::Map,
                                    Key("chaffinch".to_string()),
                                    ElementValue::Primitive(PrimitiveValue::Boolean(true)),
                                    None,
                                ),
                                conflicts: Vec::new(),
                            },
                        ],
                    },
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: true,
                        can_undo: false,
                        seq: Some(2),
                        clock: Clock::empty().with_dependency(&actor1, 2),
                        deps: Clock::empty().with_dependency(&actor1, 2),
                        diffs: vec![Diff {
                            action: DiffAction::RemoveMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                            ),
                            conflicts: Vec::new(),
                        }],
                    },
                ],
            },
            ApplyLocalChangeTestCase {
                name: "Should redo",
                change_requests: vec![
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 1,
                        message: None,
                        dependencies: Clock::empty(),
                        request_type: ChangeRequestType::Change(vec![
                            Operation::MakeMap {
                                object_id: birds.clone(),
                            },
                            Operation::Link {
                                object_id: ObjectID::Root,
                                key: Key("birds".to_string()),
                                value: birds.clone(),
                            },
                            Operation::Set {
                                object_id: birds.clone(),
                                key: Key("chaffinch".to_string()),
                                value: PrimitiveValue::Boolean(true),
                                datatype: None,
                            },
                        ]),
                    },
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 2,
                        message: None,
                        dependencies: Clock::empty().with_dependency(&actor1, 1),
                        request_type: ChangeRequestType::Undo,
                    },
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 3,
                        message: None,
                        dependencies: Clock::empty().with_dependency(&actor1, 2),
                        request_type: ChangeRequestType::Redo,
                    },
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 4,
                        message: None,
                        dependencies: Clock::empty().with_dependency(&actor1, 3),
                        request_type: ChangeRequestType::Undo,
                    },
                    ChangeRequest {
                        actor_id: actor1.clone(),
                        seq: 5,
                        message: None,
                        dependencies: Clock::empty().with_dependency(&actor1, 4),
                        request_type: ChangeRequestType::Redo,
                    },
                ],
                expected_patches: vec![
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: false,
                        can_undo: true,
                        seq: Some(1),
                        clock: Clock::empty().with_dependency(&actor1, 1),
                        deps: Clock::empty().with_dependency(&actor1, 1),
                        diffs: vec![
                            Diff {
                                action: DiffAction::CreateMap(birds.clone(), MapType::Map),
                                conflicts: Vec::new(),
                            },
                            Diff {
                                action: DiffAction::SetMapKey(
                                    ObjectID::Root,
                                    MapType::Map,
                                    Key("birds".to_string()),
                                    ElementValue::Link(birds.clone()),
                                    None,
                                ),
                                conflicts: Vec::new(),
                            },
                            Diff {
                                action: DiffAction::SetMapKey(
                                    birds.clone(),
                                    MapType::Map,
                                    Key("chaffinch".to_string()),
                                    ElementValue::Primitive(PrimitiveValue::Boolean(true)),
                                    None,
                                ),
                                conflicts: Vec::new(),
                            },
                        ],
                    },
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: true,
                        can_undo: false,
                        seq: Some(2),
                        clock: Clock::empty().with_dependency(&actor1, 2),
                        deps: Clock::empty().with_dependency(&actor1, 2),
                        diffs: vec![Diff {
                            action: DiffAction::RemoveMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                            ),
                            conflicts: Vec::new(),
                        }],
                    },
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: false,
                        can_undo: true,
                        seq: Some(3),
                        clock: Clock::empty().with_dependency(&actor1, 3),
                        deps: Clock::empty().with_dependency(&actor1, 3),
                        diffs: vec![Diff {
                            action: DiffAction::SetMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                                ElementValue::Link(birds.clone()),
                                None,
                            ),
                            conflicts: Vec::new(),
                        }],
                    },
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: true,
                        can_undo: false,
                        seq: Some(4),
                        clock: Clock::empty().with_dependency(&actor1, 4),
                        deps: Clock::empty().with_dependency(&actor1, 4),
                        diffs: vec![Diff {
                            action: DiffAction::RemoveMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                            ),
                            conflicts: Vec::new(),
                        }],
                    },
                    Patch {
                        actor: Some(actor1.clone()),
                        can_redo: false,
                        can_undo: true,
                        seq: Some(5),
                        clock: Clock::empty().with_dependency(&actor1, 5),
                        deps: Clock::empty().with_dependency(&actor1, 5),
                        diffs: vec![Diff {
                            action: DiffAction::SetMapKey(
                                ObjectID::Root,
                                MapType::Map,
                                Key("birds".to_string()),
                                ElementValue::Link(birds),
                                None,
                            ),
                            conflicts: Vec::new(),
                        }],
                    },
                ],
            },
        ];

        for testcase in testcases {
            let mut backend = Backend::init();
            let patches = testcase
                .change_requests
                .into_iter()
                .map(|c| backend.apply_local_change(c).unwrap());
            for (index, (patch, expected_patch)) in
                patches.zip(testcase.expected_patches).enumerate()
            {
                assert_eq!(
                    patch, expected_patch,
                    "Pathes no equal for testcase: {}, patch: {}",
                    testcase.name, index
                );
            }
        }
    }

    #[test]
    fn test_get_patch() {
        let mut backend = Backend::init();
        let actor = ActorID::from_string("actor1".to_string());
        let change1 = Change {
            actor_id: actor.clone(),
            seq: 1,
            dependencies: Clock::empty(),
            message: None,
            operations: vec![Operation::Set {
                object_id: ObjectID::Root,
                key: Key("bird".to_string()),
                value: PrimitiveValue::Str("magpie".to_string()),
                datatype: None,
            }],
        };
        let change2 = Change {
            actor_id: actor.clone(),
            seq: 2,
            dependencies: Clock::empty(),
            message: None,
            operations: vec![Operation::Set {
                object_id: ObjectID::Root,
                key: Key("bird".to_string()),
                value: PrimitiveValue::Str("blackbird".to_string()),
                datatype: None,
            }],
        };
        let _patch1 = backend.apply_changes(vec![change1, change2]).unwrap();
        let patch2 = backend.get_patch();
        let patch3 = Patch {
            can_undo: false,
            can_redo: false,
            clock: Clock::empty().with_dependency(&actor, 2),
            deps: Clock::empty().with_dependency(&actor, 2),
            seq: None,
            actor: None,
            diffs: vec![Diff {
                action: DiffAction::SetMapKey(
                    ObjectID::Root,
                    MapType::Map,
                    Key("bird".to_string()),
                    ElementValue::Primitive(PrimitiveValue::Str("blackbird".to_string())),
                    None,
                ),
                conflicts: Vec::new(),
            }],
        };
        assert_eq!(patch2, patch3, "Patches not equal test_get_patch");
    }
}
