use crate::actor_histories::ActorHistories;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::ActorID;
use crate::{
    list_ops_in_order, DataType, Diff, DiffAction, ElementID, ElementValue, Key, MapType, ObjectID,
    Operation, SequenceType,
};
use std::collections::{HashMap, HashSet};

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectState {
    Map(MapState),
    List(ListState),
}

impl ObjectState {
    fn new_map(map_type: MapType, object_id: ObjectID) -> ObjectState {
        ObjectState::Map(MapState::new(map_type, object_id))
    }

    fn new_sequence(sequence_type: SequenceType, object_id: ObjectID) -> ObjectState {
        ObjectState::List(ListState::new(sequence_type, object_id))
    }

    // this feels like we should have a trait or something
    fn generate_diffs(&self) -> Vec<Diff> {
        match self {
            ObjectState::Map(map_state) => map_state.generate_diffs(),
            ObjectState::List(list_state) => list_state.generate_diffs(),
        }
    }

    fn handle_assign_op(
        &mut self,
        op_with_metadata: OperationWithMetadata,
        actor_histories: &ActorHistories,
        key: &Key,
    ) -> Result<(Option<Diff>, Vec<Operation>), AutomergeError> {
        let (diff, mut undo_ops) = match self {
            ObjectState::Map(mapstate) => {
                mapstate.handle_assign_op(op_with_metadata.clone(), actor_histories, key)
            }
            ObjectState::List(liststate) => {
                liststate.handle_assign_op(op_with_metadata.clone(), actor_histories, key)
            }
        }?;

        if let Operation::Increment {
            object_id,
            key,
            value,
        } = &op_with_metadata.operation
        {
            undo_ops = vec![Operation::Increment {
                object_id: object_id.clone(),
                key: key.clone(),
                value: -value,
            }]
        };

        if undo_ops.is_empty() {
            undo_ops.push(Operation::Delete {
                object_id: op_with_metadata.operation.object_id().clone(),
                key: key.clone(),
            })
        }

        Ok((diff, undo_ops))
    }
}

/// Stores operations on list objects
#[derive(Debug, Clone, PartialEq)]
pub struct ListState {
    pub operations_by_elemid: HashMap<ElementID, ConcurrentOperations>,
    pub insertions: HashMap<ElementID, ElementID>,
    pub following: HashMap<ElementID, Vec<ElementID>>,
    pub max_elem: u32,
    pub sequence_type: SequenceType,
    pub object_id: ObjectID,
}

impl ListState {
    fn new(sequence_type: SequenceType, object_id: ObjectID) -> ListState {
        ListState {
            operations_by_elemid: HashMap::new(),
            following: HashMap::new(),
            insertions: HashMap::new(),
            max_elem: 0,
            sequence_type,
            object_id,
        }
    }

    fn generate_diffs(&self) -> Vec<Diff> {
        let mut diffs = Vec::new();

        let head = Diff {
            action: DiffAction::CreateList(self.object_id.clone(), self.sequence_type.clone()),
            conflicts: Vec::new(),
        };

        let ops_in_order = list_ops_in_order(&self.operations_by_elemid, &self.following)
            .ok()
            .unwrap_or_default();

        let inserts = ops_in_order
            .iter()
            .filter_map(|(_, ops)| {
                ops.active_op()
                    .map(|active_op| (active_op, ops.conflicts()))
            })
            .enumerate()
            .map(|(after, (active_op, conflicts))| Diff {
                action: list_op_to_assign_diff(
                    &active_op.operation,
                    &self.sequence_type,
                    after as u32,
                )
                .unwrap(),
                conflicts,
            });

        let tail = Diff {
            action: DiffAction::MaxElem(
                self.object_id.clone(),
                self.max_elem,
                self.sequence_type.clone(),
            ),
            conflicts: Vec::new(),
        };

        diffs.push(head);
        diffs.extend(inserts);
        diffs.push(tail);

        diffs
    }

    fn handle_assign_op(
        &mut self,
        op: OperationWithMetadata,
        actor_histories: &ActorHistories,
        key: &Key,
    ) -> Result<(Option<Diff>, Vec<Operation>), AutomergeError> {
        let elem_id = key.as_element_id().map_err(|_| AutomergeError::InvalidChange(format!("Attempted to link, set, delete, or increment an object in a list with invalid element ID {:?}", key.0)))?;

        // We have to clone this here in order to avoid holding a reference to
        // self which makes the borrow checker choke when adding an op to the
        // operations_by_elemid map later
        let ops_clone = self.operations_by_elemid.clone();
        let ops_in_order_before_this_op = list_ops_in_order(&ops_clone, &self.following)?;

        // This is a hack to avoid holding on to a mutable reference to self
        // when adding a new operation
        let (undo_ops, ops) = {
            let mutable_ops = self
                .operations_by_elemid
                .entry(elem_id.clone())
                .or_insert_with(ConcurrentOperations::new);
            let undo_ops = mutable_ops.incorporate_new_op(op, actor_histories)?;
            (undo_ops, mutable_ops.clone())
        };

        let ops_in_order_after_this_op =
            list_ops_in_order(&self.operations_by_elemid, &self.following)?;

        let index_before_op = ops_in_order_before_this_op
            .iter()
            .filter_map(|(elem_id, ops)| ops.active_op().map(|_| elem_id))
            .enumerate()
            .find(|(_, op_elem_id)| &&elem_id == op_elem_id)
            .map(|(index, _)| index as u32);

        let index_and_value_after_op: Option<(u32, ElementValue, Option<DataType>)> =
            ops_in_order_after_this_op
                .iter()
                .filter_map(|(elem_id, ops)| ops.active_op().map(|op| (op, elem_id)))
                .enumerate()
                .find(|(_, (_, op_elem_id))| &&elem_id == op_elem_id)
                .map(|(index, (op, _))| {
                    let (value, datatype) = match &op.operation {
                        Operation::Set {
                            ref value,
                            ref datatype,
                            ..
                        } => (ElementValue::Primitive(value.clone()), datatype),
                        Operation::Link { value, .. } => (ElementValue::Link(value.clone()), &None),
                        _ => panic!("Should not happen"),
                    };
                    (index as u32, value, datatype.clone())
                });

        let action: Option<DiffAction> = match (index_before_op, index_and_value_after_op) {
            (Some(_), Some((after, value, datatype))) => Some(DiffAction::SetSequenceElement(
                self.object_id.clone(),
                self.sequence_type.clone(),
                after,
                value,
                datatype,
            )),
            (Some(before), None) => Some(DiffAction::RemoveSequenceElement(
                self.object_id.clone(),
                self.sequence_type.clone(),
                before,
            )),
            (None, Some((after, value, datatype))) => Some(DiffAction::InsertSequenceElement(
                self.object_id.clone(),
                self.sequence_type.clone(),
                after,
                value,
                datatype,
                elem_id,
            )),
            (None, None) => None,
        };
        Ok((
            action.map(|action| Diff {
                action,
                conflicts: ops.conflicts(),
            }),
            undo_ops,
        ))
    }

    fn add_insertion(
        &mut self,
        actor_id: &ActorID,
        elem_id: &ElementID,
        elem: u32,
    ) -> Result<Diff, AutomergeError> {
        let inserted_elemid = ElementID::SpecificElementID(actor_id.clone(), elem);
        if self.insertions.contains_key(&inserted_elemid) {
            return Err(AutomergeError::InvalidChange(format!(
                "Received an insertion for already present key: {:?}",
                inserted_elemid
            )));
        }
        self.insertions
            .insert(inserted_elemid.clone(), inserted_elemid.clone());
        let following_ops = self
            .following
            .entry(elem_id.clone())
            .or_insert_with(Vec::new);
        following_ops.push(inserted_elemid.clone());

        let ops = self
            .operations_by_elemid
            .entry(inserted_elemid)
            .or_insert_with(ConcurrentOperations::new);
        self.max_elem = std::cmp::max(self.max_elem, elem);
        Ok(Diff {
            action: DiffAction::MaxElem(
                self.object_id.clone(),
                self.max_elem,
                self.sequence_type.clone(),
            ),
            conflicts: ops.conflicts(),
        })
    }
}

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub struct MapState {
    pub operations_by_key: HashMap<Key, ConcurrentOperations>,
    pub map_type: MapType,
    pub object_id: ObjectID,
}

impl MapState {
    fn new(map_type: MapType, object_id: ObjectID) -> MapState {
        MapState {
            operations_by_key: HashMap::new(),
            map_type,
            object_id,
        }
    }

    fn generate_diffs(&self) -> Vec<Diff> {
        let mut diffs = Vec::new();
        if self.object_id != ObjectID::Root {
            diffs.push(Diff {
                action: DiffAction::CreateMap(self.object_id.clone(), self.map_type.clone()),
                conflicts: Vec::new(),
            })
        }
        diffs.extend(self.operations_by_key.iter().filter_map(|(_, ops)| {
            ops.active_op()
                .and_then(|op| map_op_to_assign_diff(&op.operation, &self.map_type))
                .map(|action| Diff {
                    action,
                    conflicts: ops.conflicts(),
                })
        }));
        diffs
    }

    fn handle_assign_op(
        &mut self,
        op_with_metadata: OperationWithMetadata,
        actor_histories: &ActorHistories,
        key: &Key,
    ) -> Result<(Option<Diff>, Vec<Operation>), AutomergeError> {
        let (undo_ops, ops) = {
            let mutable_ops = self
                .operations_by_key
                .entry(key.clone())
                .or_insert_with(ConcurrentOperations::new);
            let undo_ops = mutable_ops.incorporate_new_op(op_with_metadata, actor_histories)?;
            (undo_ops, mutable_ops.clone())
        };
        Ok((
            Some(
                ops.active_op()
                    .map(|op| {
                        let action = match &op.operation {
                            Operation::Set {
                                object_id,
                                key,
                                value,
                                datatype,
                            } => DiffAction::SetMapKey(
                                object_id.clone(),
                                self.map_type.clone(),
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
                                self.map_type.clone(),
                                key.clone(),
                                ElementValue::Link(value.clone()),
                                None,
                            ),
                            _ => panic!("Should not happen for objects"),
                        };
                        Diff {
                            action,
                            conflicts: ops.conflicts(),
                        }
                    })
                    .unwrap_or_else(|| Diff {
                        action: DiffAction::RemoveMapKey(
                            self.object_id.clone(),
                            self.map_type.clone(),
                            key.clone(),
                        ),
                        conflicts: ops.conflicts(),
                    }),
            ),
            undo_ops,
        ))
    }
}

/// The ObjectStore is responsible for storing the concurrent operations seen
/// for each object ID and for the logic of incorporating a new operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectStore {
    operations_by_object_id: HashMap<ObjectID, ObjectState>,
}

impl ObjectStore {
    pub(crate) fn new() -> ObjectStore {
        let root = ObjectState::new_map(MapType::Map, ObjectID::Root);
        let mut ops_by_id = HashMap::new();
        ops_by_id.insert(ObjectID::Root, root);
        ObjectStore {
            operations_by_object_id: ops_by_id,
        }
    }

    pub fn state_for_object_id(&self, object_id: &ObjectID) -> Option<&ObjectState> {
        self.operations_by_object_id.get(object_id)
    }

    pub fn generate_diffs(&self) -> Vec<Diff> {
        let mut diffs = Vec::new();
        let mut seen = HashSet::new();
        let mut next = vec![ObjectID::Root];

        while !next.is_empty() {
            let oid = next.pop().unwrap();
            if let Some(object_state) = self.operations_by_object_id.get(&oid) {
                let new_diffs = object_state.generate_diffs();
                for diff in new_diffs.iter() {
                    for link in diff.links() {
                        if !seen.contains(&link) {
                            next.push(link)
                        }
                    }
                }
                diffs.push(new_diffs);
                seen.insert(oid);
            }
        }

        diffs.iter().rev().flatten().cloned().collect()
    }

    /// Get the ConcurrentOperations instance corresponding to a key in an
    /// object. If the object is a list this function will attempt to convert
    /// the key into an element ID
    pub fn concurrent_operations_for_field(
        &self,
        object_id: &ObjectID,
        key: &Key,
    ) -> Option<ConcurrentOperations> {
        self.operations_by_object_id
            .get(object_id)
            .and_then(|state| match state {
                ObjectState::Map(mapstate) => mapstate.operations_by_key.get(&key),
                ObjectState::List(liststate) => key
                    .as_element_id()
                    .ok()
                    .and_then(|elem_id| liststate.operations_by_elemid.get(&elem_id)),
            })
            .cloned()
    }

    /// Incorporates a new operation into the object store. The caller is
    /// responsible for ensuring that all causal dependencies of the new
    /// operation have already been applied.
    ///
    /// The return value is a tuple of a diff to send to the frontend, and
    /// a (possibly empty) vector of operations which will undo the operation
    /// later.
    pub fn apply_operation(
        &mut self,
        actor_histories: &ActorHistories,
        op_with_metadata: OperationWithMetadata,
    ) -> Result<(Option<Diff>, Vec<Operation>), AutomergeError> {
        let (diff, undo_ops) = match op_with_metadata.operation {
            Operation::MakeMap { object_id } => {
                let object = ObjectState::new_map(MapType::Map, object_id.clone());
                self.operations_by_object_id
                    .insert(object_id.clone(), object);
                (
                    Some(Diff {
                        action: DiffAction::CreateMap(object_id, MapType::Map),
                        conflicts: Vec::new(),
                    }),
                    Vec::new(),
                )
            }
            Operation::MakeTable { object_id } => {
                let object = ObjectState::new_map(MapType::Table, object_id.clone());
                self.operations_by_object_id
                    .insert(object_id.clone(), object);
                (
                    Some(Diff {
                        action: DiffAction::CreateMap(object_id, MapType::Table),
                        conflicts: Vec::new(),
                    }),
                    Vec::new(),
                )
            }
            Operation::MakeList { object_id } => {
                let object = ObjectState::new_sequence(SequenceType::List, object_id.clone());
                self.operations_by_object_id
                    .insert(object_id.clone(), object);
                (
                    Some(Diff {
                        action: DiffAction::CreateList(object_id, SequenceType::List),
                        conflicts: Vec::new(),
                    }),
                    Vec::new(),
                )
            }
            Operation::MakeText { object_id } => {
                let object = ObjectState::new_sequence(SequenceType::Text, object_id.clone());
                self.operations_by_object_id
                    .insert(object_id.clone(), object);
                (
                    Some(Diff {
                        action: DiffAction::CreateList(object_id, SequenceType::Text),
                        conflicts: Vec::new(),
                    }),
                    Vec::new(),
                )
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
                object.handle_assign_op(op_with_metadata.clone(), actor_histories, key)?
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
                    ObjectState::Map { .. } => {
                        return Err(AutomergeError::InvalidChange(format!(
                            "Insert operation received for object key (object ID: {:?}, key: {:?}",
                            list_id, key
                        )))
                    }
                    ObjectState::List(liststate) => (
                        Some(liststate.add_insertion(&op_with_metadata.actor_id, key, *elem)?),
                        Vec::new(),
                    ),
                }
            }
        };
        Ok((diff, undo_ops))
    }
}

fn map_op_to_assign_diff(op: &Operation, map_type: &MapType) -> Option<DiffAction> {
    match op {
        Operation::Set {
            object_id,
            key,
            value,
            datatype,
        } => Some(DiffAction::SetMapKey(
            object_id.clone(),
            map_type.clone(),
            key.clone(),
            ElementValue::Primitive(value.clone()),
            datatype.clone(),
        )),
        Operation::Link {
            object_id,
            key,
            value,
        } => Some(DiffAction::SetMapKey(
            object_id.clone(),
            map_type.clone(),
            key.clone(),
            ElementValue::Link(value.clone()),
            None,
        )),
        _ => None,
    }
}

fn list_op_to_assign_diff(
    op: &Operation,
    sequence_type: &SequenceType,
    after: u32,
) -> Option<DiffAction> {
    match op {
        Operation::Set {
            ref object_id,
            ref key,
            ref value,
            ref datatype,
            ..
        } => key
            .as_element_id()
            .map(|eid| {
                DiffAction::InsertSequenceElement(
                    object_id.clone(),
                    sequence_type.clone(),
                    after,
                    ElementValue::Primitive(value.clone()),
                    datatype.clone(),
                    eid,
                )
            })
            .ok(),
        Operation::Link {
            value,
            object_id,
            key,
            ..
        } => key
            .as_element_id()
            .map(|eid| {
                DiffAction::InsertSequenceElement(
                    object_id.clone(),
                    sequence_type.clone(),
                    after,
                    ElementValue::Link(value.clone()),
                    None,
                    eid,
                )
            })
            .ok(),
        _ => None,
    }
}
