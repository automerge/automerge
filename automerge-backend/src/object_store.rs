use crate::actor_states::ActorStates;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::ActorID;
use crate::{
    list_ops_in_order, DataType, DiffAction, ElementID, ElementValue, Key, MapType, OpID,
    Operation, SequenceType,
};
use std::collections::HashMap;

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
    pub fn new_map(map_type: MapType, object_id: OpID) -> ObjectState {
        ObjectState::Map(MapState::new(map_type, object_id))
    }

    pub fn new_sequence(sequence_type: SequenceType, object_id: OpID) -> ObjectState {
        ObjectState::List(ListState::new(sequence_type, object_id))
    }

    pub fn set_inbound(&mut self, metaop: OperationWithMetadata) {
        match self {
            ObjectState::Map(state) => state.inbound = Some(metaop),
            ObjectState::List(state) => state.inbound = Some(metaop),
        }
    }

    pub fn inbound(&self) -> &Option<OperationWithMetadata> {
        match self {
            ObjectState::Map(state) => &state.inbound,
            ObjectState::List(state) => &state.inbound,
        }
    }

    pub fn ops_for_key(&self, key: &Key) -> &Vec<OperationWithMetadata> {
        match self {
            ObjectState::Map(mapstate) => {
                // FIXME
                &mapstate.operations_by_key.get(key).unwrap().operations
            }
            ObjectState::List(liststate) => {
                // FIXME
                &liststate
                    .operations_by_elemid
                    .get(&key.as_element_id().ok().unwrap())
                    .unwrap()
                    .operations
            }
        }
    }

    pub fn handle_assign_op(
        &mut self,
        op_with_metadata: OperationWithMetadata,
        actor_states: &ActorStates,
        key: &Key,
    ) -> Result<Vec<Operation>, AutomergeError> {
        let mut undo_ops = match self {
            ObjectState::Map(mapstate) => {
                mapstate.handle_assign_op(op_with_metadata.clone(), actor_states, key)
            }
            ObjectState::List(liststate) => {
                liststate.handle_assign_op(op_with_metadata.clone(), actor_states, key)
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
                object_id: op_with_metadata.operation.obj().clone(),
                key: key.clone(),
            })
        }

        Ok(undo_ops)
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
    pub object_id: OpID,
    pub inbound: Option<OperationWithMetadata>,
}

impl ListState {
    fn new(sequence_type: SequenceType, object_id: OpID) -> ListState {
        ListState {
            operations_by_elemid: HashMap::new(),
            following: HashMap::new(),
            insertions: HashMap::new(),
            max_elem: 0,
            sequence_type,
            object_id,
            inbound: None,
        }
    }

    fn handle_assign_op(
        &mut self,
        op: OperationWithMetadata,
        actor_states: &ActorStates,
        key: &Key,
    ) -> Result<Vec<Operation>, AutomergeError> {
        let elem_id = key.as_element_id()?;

        // We have to clone this here in order to avoid holding a reference to
        // self which makes the borrow checker choke when adding an op to the
        // operations_by_elemid map later
        let ops_clone = self.operations_by_elemid.clone();
        let ops_in_order_before_this_op = list_ops_in_order(&ops_clone, &self.following)?;

        // This is a hack to avoid holding on to a mutable reference to self
        // when adding a new operation
        let undo_ops = self
            .operations_by_elemid
            .entry(elem_id.clone())
            .or_insert_with(ConcurrentOperations::new)
            .incorporate_new_op(op, actor_states)?;

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

        let _action: Option<DiffAction> = match (index_before_op, index_and_value_after_op) {
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
        Ok(undo_ops)
    }

    pub fn add_insertion(
        &mut self,
        actor_id: &ActorID,
        elem_id: &ElementID,
        elem: u32,
    ) -> Result<(), AutomergeError> {
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
        following_ops.push(inserted_elemid);

        self.max_elem = std::cmp::max(self.max_elem, elem);
        Ok(())
    }
}

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub struct MapState {
    pub operations_by_key: HashMap<Key, ConcurrentOperations>,
    pub map_type: MapType,
    pub object_id: OpID,
    pub inbound: Option<OperationWithMetadata>,
}

impl MapState {
    fn new(map_type: MapType, object_id: OpID) -> MapState {
        MapState {
            operations_by_key: HashMap::new(),
            map_type,
            object_id,
            inbound: None,
        }
    }

    fn handle_assign_op(
        &mut self,
        op_with_metadata: OperationWithMetadata,
        actor_states: &ActorStates,
        key: &Key,
    ) -> Result<Vec<Operation>, AutomergeError> {
        let mutable_ops = self
            .operations_by_key
            .entry(key.clone())
            .or_insert_with(ConcurrentOperations::new);
        mutable_ops.incorporate_new_op(op_with_metadata, actor_states)
    }
}
