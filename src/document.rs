use super::op_set::OpSet;
use super::AutomergeError;
use crate::protocol::Change;
use serde_json;

pub struct Document {
    op_set: OpSet,
}

impl Document {
    /// Create a new, empty document
    pub fn init() -> Document {
        Document {
            op_set: OpSet::init(),
        }
    }

    /// Create a new document from a set of changes
    pub fn load(changes: Vec<Change>) -> Result<Document, AutomergeError> {
        let mut doc = Document::init();
        for change in changes {
            doc.apply_change(change)?
        }
        Ok(doc)
    }

    /// Get the current state of the document as a serde_json value
    pub fn state(&self) -> Result<serde_json::Value, AutomergeError> {
        self.op_set.root_value().map(|v| v.to_json())
    }

    /// Add a single change to the document
    pub fn apply_change(&mut self, change: Change) -> Result<(), AutomergeError> {
        self.op_set.apply_change(change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        ActorID, Clock, DataType, ElementID, Key, ObjectID, Operation, PrimitiveValue,
    };
    use std::collections::HashMap;

    #[test]
    fn test_loading_from_changes() {
        let mut actor1_deps = HashMap::new();
        actor1_deps.insert(ActorID("id1".to_string()), 1);
        let changes = vec![
            Change {
                actor_id: ActorID("id1".to_string()),
                operations: vec![
                    Operation::MakeMap {
                        object_id: ObjectID::ID("2ce778e4-d23f-426f-98d7-e97fea47181c".to_string()),
                    },
                    Operation::Link {
                        object_id: ObjectID::Root,
                        key: Key("cards_by_id".to_string()),
                        value: ObjectID::ID("2ce778e4-d23f-426f-98d7-e97fea47181c".to_string()),
                    },
                    Operation::Set {
                        object_id: ObjectID::Root,
                        key: Key("numRounds".to_string()),
                        value: PrimitiveValue::Number(0.0),
                        datatype: Some(DataType::Counter),
                    },
                    Operation::Set {
                        object_id: ObjectID::Root,
                        key: Key("size_of_cards".to_string()),
                        value: PrimitiveValue::Number(10.0),
                        datatype: None,
                    },
                    Operation::Set {
                        object_id: ObjectID::ID("2ce778e4-d23f-426f-98d7-e97fea47181c".to_string()),
                        key: Key("deleted_key".to_string()),
                        value: PrimitiveValue::Boolean(false),
                        datatype: None,
                    },
                    Operation::Delete {
                        object_id: ObjectID::ID("2ce778e4-d23f-426f-98d7-e97fea47181c".to_string()),
                        key: Key("deleted_key".to_string()),
                    },
                    Operation::MakeList {
                        object_id: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                    },
                    Operation::Link {
                        object_id: ObjectID::Root,
                        key: Key("cards".to_string()),
                        value: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                    },
                    Operation::Insert {
                        list_id: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                        key: ElementID::Head,
                        elem: 1,
                    },
                    Operation::Set {
                        object_id: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                        key: Key("id1:1".to_string()),
                        value: PrimitiveValue::Number(1.0),
                        datatype: None,
                    },
                    Operation::Insert {
                        list_id: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                        key: ElementID::SpecificElementID(ActorID("id1".to_string()), 1),
                        elem: 2,
                    },
                    Operation::Set {
                        object_id: ObjectID::ID("87cef98c-246d-42b8-ada5-28524f5aefb3".to_string()),
                        key: Key("id1:2".to_string()),
                        value: PrimitiveValue::Boolean(false),
                        datatype: None,
                    },
                ],
                seq: 1,
                message: Some("initialization".to_string()),
                dependencies: Clock::empty(),
            },
            Change {
                actor_id: ActorID("id1".to_string()),
                operations: vec![
                    Operation::Increment {
                        object_id: ObjectID::Root,
                        key: Key("numRounds".to_string()),
                        value: 5.0,
                    },
                    Operation::Set {
                        object_id: ObjectID::Root,
                        key: Key("size_of_cards".to_string()),
                        value: PrimitiveValue::Number(12.0),
                        datatype: None,
                    },
                ],
                seq: 2,
                message: Some("incrementation".to_string()),
                dependencies: Clock(actor1_deps.clone()),
            },
            Change {
                actor_id: ActorID("id2".to_string()),
                operations: vec![
                    Operation::Increment {
                        object_id: ObjectID::Root,
                        key: Key("numRounds".to_string()),
                        value: 6.0,
                    },
                    Operation::Set {
                        object_id: ObjectID::Root,
                        key: Key("size_of_cards".to_string()),
                        value: PrimitiveValue::Number(13.0),
                        datatype: None,
                    },
                ],
                seq: 1,
                message: Some("actor 2 incrementation".to_string()),
                dependencies: Clock(actor1_deps.clone()),
            },
        ];
        let doc = Document::load(changes).unwrap();
        let expected: serde_json::Value = serde_json::from_str(
            r#"
            {
                "cards_by_id": {},
                "size_of_cards": 12.0,
                "numRounds": 11.0,
                "cards": [1.0, false]
            }
        "#,
        )
        .unwrap();
        let actual_state = doc.state().unwrap();
        assert_eq!(actual_state, expected)
    }
}
