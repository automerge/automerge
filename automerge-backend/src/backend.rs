use crate::{ActorID, AutomergeError, Change, Clock, Diff, OpSet, Patch};

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
            .map(|c| self.op_set.apply_change(c))
            .collect::<Result<Vec<Vec<Diff>>, AutomergeError>>()?;
        let diffs = nested_diffs.into_iter().flatten().collect();
        Ok(Patch {
            can_undo: false,
            can_redo: false,
            clock: self.op_set.clock.clone(),
            deps: self.op_set.clock.clone(),
            diffs,
        })
    }

    pub fn apply_local_change(&mut self, _change: Change) -> Result<Patch, AutomergeError> {
        Ok(Patch::empty())
    }

    pub fn get_patch(&self) -> Patch {
        Patch::empty()
    }

    pub fn get_changes(&self) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_changes_for_actor_id(&self, _actor_id: ActorID) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_missing_changes(&self, _clock: Clock) -> Vec<Change> {
        Vec::new()
    }

    pub fn get_missing_deps(&self) -> Clock {
        Clock::empty()
    }

    pub fn merge(&mut self, _remote: &Backend) -> Result<Patch, AutomergeError> {
        Ok(Patch::empty())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorID, Backend, Change, Clock, DataType, Diff, DiffAction, ElementValue, Key, MapType,
        ObjectID, Operation, Patch, PrimitiveValue,
    };

    struct TestCase {
        name: &'static str,
        changes: Vec<Change>,
        expected_patch: Patch,
    }

    #[test]
    fn test_diffs() {
        let actor1 = ActorID::new();
        let testcases = vec![
            TestCase {
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
                },
            },
            TestCase {
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
}
