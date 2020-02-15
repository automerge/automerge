use crate::{Clock, Patch, Change, OpSet, ActorID};

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    op_set: OpSet
}

impl Backend {
    pub fn init() -> Backend {
        Backend {
            op_set: OpSet::init()
        }
    }

    pub fn apply_changes(&mut self, _changes: Vec<Change>) -> Patch {
        Patch::empty()
    }

    pub fn apply_local_change(&mut self, _change: Change) -> Patch {
        Patch::empty()
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

    pub fn merge(&mut self, _remote: &Backend) -> Patch {
        Patch::empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorID, Backend, Change, Clock, Diff, DiffAction, ElementValue, Key, MapType, ObjectID, Operation,
        Patch, PrimitiveValue,
    };

    struct TestCase {
        name: &'static str,
        changes: Vec<Change>,
        expected_patch: Patch,
    }

    #[test]
    fn test_diffs() {
        let actor1 = ActorID::new();
        let testcases = vec![TestCase {
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
        }];

        for testcase in testcases {
            let mut backend = Backend::init();
            let patch = backend.apply_changes(testcase.changes);
            assert_eq!(testcase.expected_patch, patch, "Patches not equal for {}", testcase.name); 
        }
    }
}
