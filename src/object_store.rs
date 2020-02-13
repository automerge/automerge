use crate::actor_histories::ActorHistories;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{ElementID, ObjectID, Operation};
use std::collections::HashMap;
use std::str::FromStr;

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).
#[derive(Debug, Clone)]
pub(crate) enum ObjectHistory {
    Map {
        operations_by_key: HashMap<String, ConcurrentOperations>,
    },
    List {
        operations_by_elemid: HashMap<ElementID, ConcurrentOperations>,
        insertions: HashMap<ElementID, ElementID>,
        following: HashMap<ElementID, Vec<ElementID>>,
        max_elem: u32,
    },
}

/// The ObjectStore is responsible for storing the concurrent operations seen
/// for each object ID and for the logic of incorporating a new operation.
#[derive(Debug, Clone)]
pub struct ObjectStore {
    operations_by_object_id: HashMap<ObjectID, ObjectHistory>,
}

impl ObjectStore {
    pub(crate) fn new() -> ObjectStore {
        let root = ObjectHistory::Map {
            operations_by_key: HashMap::new(),
        };
        let mut ops_by_id = HashMap::new();
        ops_by_id.insert(ObjectID::Root, root);
        ObjectStore {
            operations_by_object_id: ops_by_id,
        }
    }

    pub(crate) fn history_for_object_id(&self, object_id: &ObjectID) -> Option<&ObjectHistory> {
        self.operations_by_object_id.get(object_id)
    }

    /// Incorporates a new operation into the object store. The caller is
    /// responsible for ensuring that all causal dependencies of the new
    /// operation have already been applied.
    pub(crate) fn apply_operation(
        &mut self,
        actor_histories: &ActorHistories,
        op_with_metadata: OperationWithMetadata,
    ) -> Result<(), AutomergeError> {
        match op_with_metadata.operation {
            Operation::MakeMap { object_id } | Operation::MakeTable { object_id } => {
                let object = ObjectHistory::Map {
                    operations_by_key: HashMap::new(),
                };
                self.operations_by_object_id.insert(object_id, object);
            }
            Operation::MakeList { object_id } | Operation::MakeText { object_id } => {
                let object = ObjectHistory::List {
                    operations_by_elemid: HashMap::new(),
                    insertions: HashMap::new(),
                    following: HashMap::new(),
                    max_elem: 0,
                };
                self.operations_by_object_id.insert(object_id, object);
            }
            Operation::Link {
                ref object_id,
                ref key,
                ..
            }
            | Operation::Delete {
                ref object_id,
                ref key,
            }
            | Operation::Set {
                ref object_id,
                ref key,
                ..
            }
            | Operation::Increment {
                ref object_id,
                ref key,
                ..
            } => {
                let object = self
                    .operations_by_object_id
                    .get_mut(&object_id)
                    .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))?;
                let prior_ops = match object {
                    ObjectHistory::Map {
                        ref mut operations_by_key,
                    } => operations_by_key
                        .entry(key.0.clone())
                        .or_insert_with(ConcurrentOperations::new),
                    ObjectHistory::List {
                        ref mut operations_by_elemid,
                        ..
                    } => {
                        let elem_id = ElementID::from_str(&key.0).map_err(|_| AutomergeError::InvalidChange(format!("Attempted to link, set, delete, or increment an object in a list with invalid element ID {:?}", key.0)))?;
                        operations_by_elemid
                            .entry(elem_id.clone())
                            .or_insert_with(ConcurrentOperations::new)
                    }
                };
                prior_ops.incorporate_new_op(op_with_metadata, actor_histories)?;
            }
            Operation::Insert {
                ref list_id,
                ref key,
                ref elem,
            } => {
                let list = self
                    .operations_by_object_id
                    .get_mut(&list_id)
                    .ok_or_else(|| AutomergeError::MissingObjectError(list_id.clone()))?;
                match list {
                    ObjectHistory::Map { .. } => {
                        return Err(AutomergeError::InvalidChange(format!(
                            "Insert operation received for object key (object ID: {:?}, key: {:?}",
                            list_id, key
                        )))
                    }
                    ObjectHistory::List {
                        insertions,
                        following,
                        operations_by_elemid,
                        max_elem,
                    } => {
                        let inserted_elemid =
                            ElementID::SpecificElementID(op_with_metadata.actor_id.clone(), *elem);
                        if insertions.contains_key(&inserted_elemid) {
                            return Err(AutomergeError::InvalidChange(format!(
                                "Received an insertion for already present key: {:?}",
                                inserted_elemid
                            )));
                        }
                        insertions.insert(inserted_elemid.clone(), inserted_elemid.clone());
                        let following_ops = following.entry(key.clone()).or_insert_with(Vec::new);
                        following_ops.push(inserted_elemid.clone());

                        operations_by_elemid
                            .entry(inserted_elemid)
                            .or_insert_with(ConcurrentOperations::new);
                        *max_elem = std::cmp::max(*max_elem, *elem);
                    }
                }
            }
        }
        Ok(())
    }
}
