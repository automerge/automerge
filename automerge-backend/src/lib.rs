mod error;
mod value;
mod actor_histories;
mod concurrent_operations;
mod operation_with_metadata;
mod object_store;
mod op_set;
mod patch_serialization;
mod protocol;
use serde::Serialize;

pub use crate::protocol::{
    ActorID, Change, Clock, DataType, Key, ObjectID, Operation, PrimitiveValue, ElementID
};
pub use actor_histories::ActorHistories;
pub use object_store::{ObjectHistory, ObjectStore};
pub use concurrent_operations::ConcurrentOperations;
pub use value::Value;
pub use op_set::{OpSet, list_ops_in_order};
pub use error::AutomergeError;
pub use operation_with_metadata::OperationWithMetadata;

#[derive(Debug, PartialEq, Clone)]
pub enum ElementValue {
    Primitive(PrimitiveValue),
    Link(ObjectID),
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SequenceType {
    #[serde(rename = "list")]
    List,
    #[serde(rename = "text")]
    Text,
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum MapType {
    #[serde(rename = "map")]
    Map,
    #[serde(rename = "table")]
    Table,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DiffAction {
    CreateMap(ObjectID, MapType),
    CreateList(ObjectID, SequenceType),
    MaxElem(ObjectID, u32, SequenceType),
    RemoveMapKey(ObjectID, MapType, Key),
    SetMapKey(ObjectID, MapType, Key, ElementValue, Option<DataType>),
    RemoveSequenceElement(ObjectID, SequenceType, u32),
    InsertSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>),
    SetSequenceElement(ObjectID, SequenceType, u32, ElementValue, Option<DataType>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Conflict {
    pub actor: ActorID,
    pub value: ElementValue,
    pub datatype: Option<DataType>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Diff {
    pub action: DiffAction,
    pub conflicts: Vec<Conflict>,
}

#[derive(Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Patch {
    pub can_undo: bool,
    pub can_redo: bool,
    pub clock: Clock,
    pub deps: Clock,
    pub diffs: Vec<Diff>,
}

impl Patch {
    fn empty() -> Patch {
        Patch {
            can_undo: false,
            can_redo: false,
            clock: Clock::empty(),
            deps: Clock::empty(),
            diffs: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    op_set: op_set::OpSet
}

impl Backend {
    pub fn init() -> Backend {
        Backend {
            op_set: op_set::OpSet::init()
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
