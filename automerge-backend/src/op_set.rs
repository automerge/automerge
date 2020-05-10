//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::object_store::ObjState;
use crate::actor_map::ActorMap;
use crate::op_handle::OpHandle;
use crate::ordered_set::OrderedSet;
use crate::patch::{Diff, DiffEdit, MapDiff, ObjDiff, PendingDiff, SeqDiff};
use crate::protocol::{OpType, UndoOperation};
use core::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::AsRef;
use std::rc::Rc;
use automerge_protocol::{ChangeHash, ObjType, OpID, ObjectID, Key};

/// The OpSet manages an ObjectStore, and a queue of incoming changes in order
/// to ensure that operations are delivered to the object store in causal order
///
/// Whenever a new change is received we iterate through any causally ready
/// changes in the queue and apply them to the object store, then repeat until
/// there are no causally ready changes left. The end result of this is that
/// the object store will contain sets of concurrent operations for each object
/// ID or element ID.
///
/// When we want to get the state of the CRDT we walk through the
/// object store, starting with the root object ID and constructing the value
/// at each node by examining the concurrent operationsi which are active for
/// that node.
///

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct OpSet {
    pub objs: im_rc::HashMap<ObjectID, Rc<ObjState>>,
    pub deps: HashSet<ChangeHash>,
    pub max_op: u64,
}

impl OpSet {
    pub fn init() -> OpSet {
        let mut objs = im_rc::HashMap::new();
        objs.insert(ObjectID::Root, Rc::new(ObjState::new(ObjType::Map)));

        OpSet {
            objs,
            max_op: 0,
            deps: HashSet::new(),
        }
    }

    pub(crate) fn apply_ops(
        &mut self,
        mut ops: Vec<OpHandle>,
        undoable: bool,
        diffs: &mut HashMap<ObjectID,Vec<PendingDiff>>,
        actors: &ActorMap
    ) -> Result<Vec<UndoOperation>, AutomergeError> {
        let mut all_undo_ops = Vec::new();
        let mut new_objects: HashSet<ObjectID> = HashSet::new();
        for op in ops.drain(..) {
            if op.is_make() {
                new_objects.insert(ObjectID::from(&op.id));
            }
            let use_undo = undoable && !(new_objects.contains(&op.obj));

            let obj_id = op.obj.clone();

            let (pending_diff, undo_ops) = self.apply_op(op, actors)?;

            if let Some(d) = pending_diff {
                diffs.entry(obj_id).or_default().push(d);
            }

            if use_undo {
                all_undo_ops.extend(undo_ops);
            }
        }
        Ok(all_undo_ops)
    }

    fn apply_op(
        &mut self,
        op: OpHandle,
        actors: &ActorMap,
    ) -> Result<(Option<PendingDiff>, Vec<UndoOperation>), AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.obj_type()) {
            self.objs.insert(child, Rc::new(ObjState::new(obj_type)));
        }

        let object_id = &op.obj;
        let object = self.get_obj_mut(&object_id)?;

        if object.is_seq() {
            if op.insert {
                object.insert_after(op.key.as_element_id().ok_or(AutomergeError::MapKeyInSeq)?, op.clone(), actors);
            }

            let ops = object.props.entry(op.operation_key()).or_default();
            let before = !ops.is_empty();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let after = !ops.is_empty();

            let undo_ops = op.generate_undos(&overwritten_ops);

            let diff = match (before, after) {
                (true, true) => Some(PendingDiff::Set(op.clone())),
                (true, false) => {
                    let opid = op.operation_key().to_opid().ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.seq.remove_key(&opid).unwrap();
                    Some(PendingDiff::SeqRemove(op.clone(), index))
                }
                (false, true) => {
                    let id = op.operation_key().to_opid().ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.index_of(&id)?;
                    object.seq.insert_index(index, id);
                    Some(PendingDiff::SeqInsert(op.clone(), index))
                }
                (false, false) => None,
            };

            self.unlink(&op, &overwritten_ops)?;

            Ok((diff, undo_ops))
        } else {
            let ops = object.props.entry(op.key.clone()).or_default();
            let before = !ops.is_empty();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let after = !ops.is_empty();
            let undo_ops = op.generate_undos(&overwritten_ops);
            self.unlink(&op, &overwritten_ops)?;

            if before || after {
                Ok((Some(PendingDiff::Set(op)), undo_ops))
            } else {
                Ok((None, undo_ops))
            }
        }
    }

    fn unlink(&mut self, op: &OpHandle, overwritten: &[OpHandle]) -> Result<(), AutomergeError> {
        if let Some(child) = op.child() {
            self.get_obj_mut(&child)?.inbound.insert(op.clone());
        }

        for old in overwritten.iter() {
            if let Some(child) = old.child() {
                self.get_obj_mut(&child)?.inbound.remove(&old);
            }
        }
        Ok(())
    }

    pub fn get_field_ops(&self, object_id: &ObjectID, key: &Key) -> Option<&ConcurrentOperations> {
        self.objs.get(object_id).and_then(|obj| obj.props.get(key))
    }

    pub fn get_field_opids(&self, object_id: &ObjectID, key: &Key) -> Option<Vec<OpID>> {
        self.get_field_ops(object_id, key)
            .map(|con_ops| con_ops.iter().map(|op| op.id.clone()).collect())
    }

    pub fn get_obj(&self, object_id: &ObjectID) -> Result<&ObjState, AutomergeError> {
        self.objs
            .get(&object_id)
            .map(|o| o.as_ref())
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))
    }

    fn get_obj_mut(&mut self, object_id: &ObjectID) -> Result<&mut ObjState, AutomergeError> {
        self.objs
            .get_mut(&object_id)
            .map(|rc| Rc::make_mut(rc))
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))
    }

    pub fn get_pred(&self, object_id: &ObjectID, key: &Key, insert: bool) -> Vec<OpID> {
        if insert {
            Vec::new()
        } else if let Some(ops) = self.get_field_opids(&object_id, &key) {
            ops
        } else if let Some(opid) = key.to_opid() {
            vec![opid]
        } else {
            Vec::new()
        }
    }

    pub fn construct_map(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
        actors: &ActorMap,
    ) -> Result<Diff, AutomergeError> {
        let mut props = HashMap::new();

        for (key, ops) in object.props.iter() {
            if !ops.is_empty() {
                let mut opid_to_value = HashMap::new();
                for op in ops.iter() {
                    let opid_string = String::from(&op.id);
                    if let Some(child_id) = op.child() {
                        opid_to_value.insert(opid_string, self.construct_object(&child_id, actors)?);
                    } else {
                        opid_to_value.insert(opid_string, (&op.adjusted_value()).into());
                    }
                }
                props.insert(actors.key_to_string(key), opid_to_value);
            }
        }
        Ok(MapDiff {
            object_id: actors.object_to_string(object_id),
            obj_type: object.obj_type,
            props,
        }
        .into())
    }

    pub fn construct_list(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
        actors: &ActorMap,
    ) -> Result<Diff, AutomergeError> {
        let mut edits = Vec::new();
        let mut props = HashMap::new();
        let mut index = 0;
        let mut max_counter = 0;

        for opid in object.seq.into_iter() {
            max_counter = max(max_counter, opid.counter());
            let key = opid.into(); // FIXME - something is wrong here
            if let Some(ops) = object.props.get(&key) {
                if !ops.is_empty() {
                    edits.push(DiffEdit::Insert { index });
                    //let key = DiffKey::Seq(index);
                    let mut opid_to_value = HashMap::new();
                    for op in ops.iter() {
                        let opid_string = String::from(&op.id);
                        if let Some(child_id) = op.child() {
                            opid_to_value.insert(opid_string, self.construct_object(&child_id, actors)?);
                        } else {
                            opid_to_value.insert(opid_string, (&op.adjusted_value()).into());
                        }
                    }
                    props.insert(index, opid_to_value);
                    index += 1;
                }
            }
        }
        Ok(SeqDiff {
            object_id: actors.object_to_string(object_id),
            obj_type: object.obj_type,
            edits,
            props,
        }
        .into())
    }

    pub fn construct_object(&self, object_id: &ObjectID, actors: &ActorMap) -> Result<Diff, AutomergeError> {
        let object = self.get_obj(&object_id)?;
        if object.is_seq() {
            self.construct_list(object_id, object, actors)
        } else {
            self.construct_map(object_id, object, actors)
        }
    }

    // this recursivly walks through all the objects touched by the changes
    // to generate a diff in a single pass
    pub fn finalize_diffs(&self, mut pending: HashMap<ObjectID,Vec<PendingDiff>>, actors: &ActorMap) -> Result<Option<Diff>, AutomergeError> {
        if pending.is_empty() {
            return Ok(None);
        }

        let mut objs: Vec<_> = pending.keys().cloned().collect();
        while let Some(obj_id) = objs.pop() {
            let obj = self.get_obj(&obj_id)?;
            if let Some(inbound) = obj.inbound.iter().next() {
                if let Some(diffs) = pending.get_mut(&inbound.obj) {
                    diffs.push(PendingDiff::Set(inbound.clone()))
                } else {
                    objs.push(inbound.obj.clone());
                    pending.insert(inbound.obj.clone(), vec![PendingDiff::Set(inbound.clone())]);
                }
            }
        }

        Ok(Some(self.gen_obj_diff(&ObjectID::Root, &mut pending, actors)?))
    }

    fn gen_seq_diff(
        &self,
        obj_id: &ObjectID,
        obj: &ObjState,
        pending: &[PendingDiff],
        pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<Diff, AutomergeError> {
        let mut props = HashMap::new();
        let edits = pending.iter().filter_map(|p| p.edit()).collect();
        // i may have duplicate keys - this makes sure I hit each one only once
        let keys: HashSet<_> = pending.iter().map(|p| p.operation_key()).collect();
        for key in keys.iter() {
            let mut opid_to_value = HashMap::new();
            for op in obj.props.get(&key).iter().flat_map(|i| i.iter()) {
                let link = match op.action {
                    OpType::Set(_) => (&op.adjusted_value()).into(),
                    OpType::Make(_) => self.gen_obj_diff(&op.id.clone().into(), pending_diffs, actors)?,
                    OpType::Link(ref child) => self.construct_object(&child, actors)?,
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(String::from(&op.id), link);
            }
            if let Some(index) = obj.seq.index_of(&key.to_opid().ok_or(AutomergeError::HeadToOpID)?) {
                props.insert(index, opid_to_value);
            }
        }
        Ok(SeqDiff {
            object_id: actors.object_to_string(obj_id),
            obj_type: obj.obj_type,
            edits,
            props,
        }
        .into())
    }

    fn gen_map_diff(
        &self,
        obj_id: &ObjectID,
        obj: &ObjState,
        pending: &[PendingDiff],
        pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<Diff, AutomergeError> {
        let mut props = HashMap::new();
        // I may have duplicate keys - I do this to make sure I visit each one only once
        let keys: HashSet<_> = pending.iter().map(|p| p.operation_key()).collect();
        for key in keys.iter() {
            let key_string = actors.key_to_string(key);
            let mut opid_to_value = HashMap::new();
            for op in obj.props.get(&key).iter().flat_map(|i| i.iter()) {
                let link = match op.action {
                    OpType::Set(_) => (&op.adjusted_value()).into(),
                    OpType::Make(_) => self.gen_obj_diff(&op.id.clone().into(), pending_diffs, actors)?,
                    OpType::Link(ref child_id) => self.construct_object(&child_id, actors)?,
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(String::from(&op.id), link);
            }
            props.insert(key_string, opid_to_value);
        }
        Ok(MapDiff {
            object_id: actors.object_to_string(obj_id),
            obj_type: obj.obj_type,
            props,
        }
        .into())
    }

    fn gen_obj_diff(
        &self,
        obj_id: &ObjectID,
        pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<Diff, AutomergeError> {
        let obj = self.get_obj(obj_id)?;
        if let Some(pending) = pending_diffs.remove(obj_id) {
            if obj.is_seq() {
                self.gen_seq_diff(obj_id, obj, &pending, pending_diffs, actors)
            } else {
                self.gen_map_diff(obj_id, obj, &pending, pending_diffs, actors)
            }
        } else {
            Ok(Diff::Unchanged(ObjDiff {
                object_id: actors.object_to_string(obj_id),
                obj_type: obj.obj_type,
            }))
        }
    }
}
