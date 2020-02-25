use crate::actor_histories::ActorHistories;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::{
    list_ops_in_order, Diff, DiffAction, ElementID, ElementValue, Key, MapType, ObjectID,
    Operation, SequenceType,
};
use std::collections::HashMap;
use std::str::FromStr;

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectHistory {
    Map {
        operations_by_key: HashMap<String, ConcurrentOperations>,
        map_type: MapType,
        object_id: ObjectID,
    },
    List {
        operations_by_elemid: HashMap<ElementID, ConcurrentOperations>,
        insertions: HashMap<ElementID, ElementID>,
        following: HashMap<ElementID, Vec<ElementID>>,
        max_elem: u32,
        sequence_type: SequenceType,
        object_id: ObjectID,
    },
}

impl ObjectHistory {
    fn handle_mutating_op(
        &mut self,
        op_with_metadata: OperationWithMetadata,
        actor_histories: &ActorHistories,
        key: &Key,
    ) -> Result<(), AutomergeError> {
        let prior_ops = match self {
            ObjectHistory::Map {
                ref mut operations_by_key,
                ..
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
        prior_ops.incorporate_new_op(op_with_metadata, actor_histories)
    }

    fn diff_for_key(&self, key: &Key) -> Result<Option<Diff>, AutomergeError> {
        let action = match self {
            ObjectHistory::Map {
                object_id,
                operations_by_key,
                map_type,
                ..
            } => Some(
                operations_by_key
                    .get(&key.0)
                    .and_then(|ops| {
                        ops.active_op().map(|op| match &op.operation {
                            Operation::Set {
                                object_id,
                                key,
                                value,
                                datatype,
                            } => DiffAction::SetMapKey(
                                object_id.clone(),
                                map_type.clone(),
                                key.clone(),
                                ElementValue::Primitive(value.clone()),
                                datatype.clone(),
                            ),
                            Operation::Link {
                                object_id,
                                key,
                                value,
                            } => DiffAction::SetMapKey(
                                object_id.clone(),
                                map_type.clone(),
                                key.clone(),
                                ElementValue::Link(value.clone()),
                                None,
                            ),
                            _ => panic!("Should not happen for objects"),
                        })
                    })
                    .unwrap_or(DiffAction::RemoveMapKey(
                        object_id.clone(),
                        map_type.clone(),
                        key.clone(),
                    )),
            ),
            ObjectHistory::List {
                object_id,
                operations_by_elemid,
                following,
                sequence_type,
                ..
            } => {
                let ops_in_order = list_ops_in_order(operations_by_elemid, following)
                    .expect("Internal error: corrupted list");
                let element_id = key.as_element_id().map_err(|_| {
                    AutomergeError::InvalidObjectType(
                        "Received list operation with invalid element ID".to_string(),
                    )
                })?;
                let maybe_existing_index: Option<u32> = ops_in_order
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (elem_id, ops))| ops.active_op().map(|_|(i, elem_id)))
                    .find(|(_, elem_id)| elem_id == &&element_id)
                    .map(|(index, _)| index as u32);
                let maybe_ops = operations_by_elemid.get(&element_id);
                match maybe_existing_index {
                    Some(index) => Some(
                        maybe_ops
                            .and_then(|cops| cops.active_op())
                            .map(|op| match &op.operation {
                                Operation::Set {
                                    object_id,
                                    key: _,
                                    value,
                                    datatype,
                                } => DiffAction::SetSequenceElement(
                                    object_id.clone(),
                                    sequence_type.clone(),
                                    index,
                                    ElementValue::Primitive(value.clone()),
                                    datatype.clone(),
                                ),
                                Operation::Link {
                                    object_id,
                                    key: _,
                                    value,
                                } => DiffAction::SetSequenceElement(
                                    object_id.clone(),
                                    sequence_type.clone(),
                                    index,
                                    ElementValue::Link(value.clone()),
                                    None,
                                ),
                                _ => panic!("Should not happen for lists"),
                            })
                            .unwrap_or(DiffAction::RemoveSequenceElement(
                                object_id.clone(),
                                sequence_type.clone(),
                                index,
                            )),
                    ),
                    None => maybe_ops.and_then(|cops| cops.active_op()).map(|op| {
                        let (elem_value, datatype) = match &op.operation {
                            Operation::Set {
                                value, datatype, ..
                            } => (ElementValue::Primitive(value.clone()), datatype.clone()),
                            Operation::Link { value, .. } => {
                                (ElementValue::Link(value.clone()), None)
                            }
                            _ => panic!("Should never happen"),
                        };
                        let insertion_index = ops_in_order
                            .iter()
                            .take_while(|(e, _)| e != &element_id)
                            .enumerate()
                            .filter_map(|(i, (_, ops))| ops.active_op().map(|_|i as u32))
                            .last()
                            .unwrap_or(0);
                        DiffAction::InsertSequenceElement(
                            object_id.clone(),
                            sequence_type.clone(),
                            insertion_index + 1,
                            elem_value,
                            datatype,
                        )
                    }),
                }
            }
        };
        Ok(action.map(|a| Diff {
            action: a,
            conflicts: Vec::new(),
        }))
    }
}

/// The ObjectStore is responsible for storing the concurrent operations seen
/// for each object ID and for the logic of incorporating a new operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectStore {
    operations_by_object_id: HashMap<ObjectID, ObjectHistory>,
}

impl ObjectStore {
    pub(crate) fn new() -> ObjectStore {
        let root = ObjectHistory::Map {
            operations_by_key: HashMap::new(),
            map_type: MapType::Map,
            object_id: ObjectID::Root,
        };
        let mut ops_by_id = HashMap::new();
        ops_by_id.insert(ObjectID::Root, root);
        ObjectStore {
            operations_by_object_id: ops_by_id,
        }
    }

    pub fn history_for_object_id(&self, object_id: &ObjectID) -> Option<&ObjectHistory> {
        self.operations_by_object_id.get(object_id)
    }

    /// Incorporates a new operation into the object store. The caller is
    /// responsible for ensuring that all causal dependencies of the new
    /// operation have already been applied.
    pub fn apply_operation(
        &mut self,
        actor_histories: &ActorHistories,
        op_with_metadata: OperationWithMetadata,
    ) -> Result<Option<Diff>, AutomergeError> {
        //let mut diff = Diff {
        //action: DiffAction::CreateMap(ObjectID::Root, MapType::Map),
        //conflicts: Vec::new(),
        //};
        let diff = match op_with_metadata.operation {
            Operation::MakeMap { object_id } => {
                let object = ObjectHistory::Map {
                    operations_by_key: HashMap::new(),
                    map_type: MapType::Map,
                    object_id: object_id.clone(),
                };
                self.operations_by_object_id.insert(object_id, object);
                None
            }
            Operation::MakeTable { object_id } => {
                let object = ObjectHistory::Map {
                    operations_by_key: HashMap::new(),
                    map_type: MapType::Table,
                    object_id: object_id.clone(),
                };
                self.operations_by_object_id.insert(object_id, object);
                None
            }
            Operation::MakeList { object_id } => {
                let object = ObjectHistory::List {
                    operations_by_elemid: HashMap::new(),
                    insertions: HashMap::new(),
                    following: HashMap::new(),
                    max_elem: 0,
                    sequence_type: SequenceType::Text,
                    object_id: object_id.clone(),
                };
                self.operations_by_object_id.insert(object_id, object);
                None
            }
            Operation::MakeText { object_id } => {
                let object = ObjectHistory::List {
                    operations_by_elemid: HashMap::new(),
                    insertions: HashMap::new(),
                    following: HashMap::new(),
                    max_elem: 0,
                    sequence_type: SequenceType::Text,
                    object_id: object_id.clone(),
                };
                self.operations_by_object_id.insert(object_id, object);
                None
            }
            Operation::Link {
                ref object_id,
                ref key,
                ..
            }
            | Operation::Set {
                ref object_id,
                ref key,
                ..
            }
            | Operation::Delete {
                ref object_id,
                ref key,
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
                object.handle_mutating_op(op_with_metadata.clone(), actor_histories, key)?;
                object.diff_for_key(key)?
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
                        ..
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
                None
            }
        };
        Ok(diff)
    }
}
