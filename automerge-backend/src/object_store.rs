use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::{ConcurrentOperations, ElementID, Key, ObjType, OpID};
use std::collections::{HashMap, HashSet};
use std::slice::Iter;

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).

/// Stores operations on map objects
#[derive(Debug, Clone, PartialEq)]
pub struct ObjState {
    pub props: HashMap<Key, ConcurrentOperations>, //Vec<OperationWithMetadata>>,
    pub obj_type: ObjType,
    //    pub op_id: OpID,
    pub inbound: HashSet<OperationWithMetadata>,
    //    pub creator: Option<OperationWithMetadata>,
    pub following: HashMap<ElementID, Vec<OperationWithMetadata>>,
    //    pub insertion: HashMap<ElementID, OperationWithMetadata>,
}

impl ObjState {
    pub fn new(obj_type: ObjType) -> ObjState {
        let mut following = HashMap::new();
        following.insert(ElementID::Head, Vec::new());
        ObjState {
            props: HashMap::new(),
            following,
            //           insertion: HashMap::new(),
            obj_type,
            inbound: HashSet::new(),
            //           creator,
        }
    }

    pub fn is_seq(&self) -> bool {
        match self.obj_type {
            ObjType::Text | ObjType::List => true,
            _ => false,
        }
    }

    pub fn get_index_for(&self, target: &OpID) -> Result<usize, AutomergeError> {
        self.ops_in_order()
            .scan(0, |n, oid| {
                let last = *n;
                let key = Key(oid.to_string());
                if let Some(ops) = self.props.get(&key) {
                    if !ops.is_empty() {
                        *n += 1;
                    }
                }
                Some((last, oid))
            })
            .find_map(|(last, oid)| if oid == target { Some(last) } else { None })
            .ok_or_else(|| AutomergeError::MissingObjectError(target.clone()))
    }

    pub fn ops_in_order(&self) -> ElementIterator {
        ElementIterator {
            following: &self.following,
            stack: vec![self.following.get(&ElementID::Head).unwrap().iter()],
        }
    }

    pub fn insert_after(&mut self, elem: ElementID, op: OperationWithMetadata) {
        let following = self.following.entry(elem).or_default();
        following.push(op);
        following.sort_unstable();
        //        let key = metaop.key().as_element_id()?;
        //        let my_id = ElementID::ID(metaop.opid.clone());
        //        following.push(metaop.clone());
        //        let ops = self.insertion.insert(my_id, metaop);
    }

    /*
        pub fn add_op(&mut self, op: OperationWithMetadata) -> Result<(), AutomergeError> {
            let key = op.key();

            let active_ops = self.props.entry(key.clone()).or_insert_with(Vec::new);

            if op.is_inc() {
                active_ops
                    .iter_mut()
                    .for_each(|other| other.maybe_increment(&op))
            } else {
                let mut overwritten_ops = Vec::new();
                let mut i = 0;
                while i != active_ops.len() {
                    if op.pred().contains(&active_ops[i].opid()) {
                        overwritten_ops.push(active_ops.swap_remove(i));
                    } else {
                        i += 1;
                    }
                };

                overwritten_ops.iter().for_each(|o| {
                    self.get_obj(&child).inbound.remove(o);
                });

                if let Some(child) = op.child() {
                    self.get_obj(&child).inbound.add(op.clone());
                }
            }


            if let Operation::Set { .. } = op.operation {
              active_ops.push(op);
            }

            //        if op.is_insert() {
            //            self.apply_insert(op)?;
            //        }

            Ok(())
        }
    */
    /*
        fn handle_assign_op(
            &mut self,
            op: OperationWithMetadata,
        ) -> Result<Vec<Operation>, AutomergeError> {
            let key = op.key();
            let elem_id = key.as_element_id()?;

            // We have to clone this here in order to avoid holding a reference to
            // self which makes the borrow checker choke when adding an op to the
            // operations_by_elemid map later
            let ops_clone = self.keys.clone();
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
    */
}

pub struct ElementIterator<'a> {
    pub following: &'a HashMap<ElementID, Vec<OperationWithMetadata>>,
    pub stack: Vec<Iter<'a, OperationWithMetadata>>,
}

impl<'a> Iterator for ElementIterator<'a> {
    type Item = &'a OpID;

    // I feel like I could be clever here and use iter.chain()
    // FIXME
    fn next(&mut self) -> Option<&'a OpID> {
        if let Some(mut last) = self.stack.pop() {
            if let Some(next) = last.next() {
                self.stack.push(last);
                if let Some(more) = self.following.get(&ElementID::ID(next.opid.clone())) {
                    self.stack.push(more.iter());
                }
                Some(&next.opid)
            } else {
                self.stack.pop();
                None
            }
        } else {
            None
        }
    }
}

/*
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
*/
