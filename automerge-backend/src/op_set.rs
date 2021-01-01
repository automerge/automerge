//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::actor_map::ActorMap;
use crate::error::AutomergeError;
use crate::internal::{InternalOpType, ObjectID};
use crate::object_store::ObjState;
use crate::op_handle::OpHandle;
use crate::ordered_set::OrderedSet;
use crate::pending_diff::PendingDiff;
use crate::Change;
use automerge_protocol as amp;
use core::cmp::max;
use fxhash::FxBuildHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::AsRef;
use std::rc::Rc;

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
    pub objs: HashMap<ObjectID, Rc<ObjState>, FxBuildHasher>,
    pub deps: HashSet<amp::ChangeHash>,
    pub max_op: u64,
}

impl OpSet {
    pub fn init() -> OpSet {
        let mut objs = HashMap::default();
        objs.insert(ObjectID::Root, Rc::new(ObjState::new(amp::ObjType::map())));

        OpSet {
            objs,
            max_op: 0,
            deps: HashSet::default(),
        }
    }

    pub(crate) fn apply_ops(
        &mut self,
        mut ops: Vec<OpHandle>,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<(), AutomergeError> {
        for op in ops.drain(..) {
            let obj_id = op.obj;

            let pending_diff = self.apply_op(op, actors)?;

            if let Some(d) = pending_diff {
                diffs.entry(obj_id).or_default().push(d);
            }
        }
        Ok(())
    }

    pub fn heads(&self) -> Vec<amp::ChangeHash> {
      let mut deps: Vec<_> = self.deps.iter().cloned().collect();
      deps.sort_unstable();
      deps
    }

    pub fn apply_op(
        &mut self,
        op: OpHandle,
        actors: &ActorMap,
    ) -> Result<Option<PendingDiff>, AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.obj_type()) {
            //let child = actors.import_obj(child);
            self.objs.insert(child, Rc::new(ObjState::new(obj_type)));
        }

        let object_id = &op.obj;
        let object = self.get_obj_mut(&object_id)?;

        if object.is_seq() {
            if op.insert {
                object.insert_after(
                    op.key.as_element_id().ok_or(AutomergeError::MapKeyInSeq)?,
                    op.clone(),
                    actors,
                );
            }

            let ops = object.props.entry(op.operation_key()).or_default();
            let before = !ops.is_empty();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let after = !ops.is_empty();

            let diff = match (before, after) {
                (true, true) => Some(PendingDiff::Set(op.clone())),
                (true, false) => {
                    let opid = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.seq.remove_key(&opid).unwrap();
                    Some(PendingDiff::SeqRemove(op.clone(), index))
                }
                (false, true) => {
                    let id = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.index_of(id)?;
                    object.seq.insert_index(index, id);
                    Some(PendingDiff::SeqInsert(op.clone(), index, op.id ))
                }
                (false, false) => None,
            };

            self.unlink(&op, &overwritten_ops)?;

            Ok(diff)
        } else {
            let ops = object.props.entry(op.key.clone()).or_default();
            let before = !ops.is_empty();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let after = !ops.is_empty();
            self.unlink(&op, &overwritten_ops)?;

            if before || after {
                Ok(Some(PendingDiff::Set(op)))
            } else {
                Ok(None)
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

    pub fn get_obj(&self, object_id: &ObjectID) -> Result<&ObjState, AutomergeError> {
        self.objs
            .get(&object_id)
            .map(|o| o.as_ref())
            .ok_or(AutomergeError::MissingObjectError)
    }

    fn get_obj_mut(&mut self, object_id: &ObjectID) -> Result<&mut ObjState, AutomergeError> {
        self.objs
            .get_mut(&object_id)
            .map(|rc| Rc::make_mut(rc))
            .ok_or(AutomergeError::MissingObjectError)
    }

    pub fn construct_map(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
        actors: &ActorMap,
        map_type: amp::MapType,
    ) -> Result<amp::Diff, AutomergeError> {
        let mut props = HashMap::new();

        for (key, ops) in object.props.iter() {
            if !ops.is_empty() {
                let mut opid_to_value = HashMap::new();
                for op in ops.iter() {
                    let amp_opid = actors.export_opid(&op.id);
                    if let Some(child_id) = op.child() {
                        opid_to_value.insert(amp_opid, self.construct_object(&child_id, actors)?);
                    } else {
                        opid_to_value.insert(amp_opid, (&op.adjusted_value()).into());
                    }
                }
                props.insert(actors.key_to_string(key), opid_to_value);
            }
        }
        Ok(amp::MapDiff {
            object_id: actors.export_obj(object_id),
            obj_type: map_type,
            props,
        }
        .into())
    }

    pub fn construct_list(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
        actors: &ActorMap,
        seq_type: amp::SequenceType,
    ) -> Result<amp::Diff, AutomergeError> {
        let mut edits = Vec::new();
        let mut props = HashMap::new();
        let mut index = 0;
        let mut max_counter = 0;

        for opid in object.seq.into_iter() {
            max_counter = max(max_counter, opid.0);
            let key = (*opid).into(); // FIXME - something is wrong here
            let elem_id = actors.export_opid(opid).into();
            if let Some(ops) = object.props.get(&key) {
                if !ops.is_empty() {
                    edits.push(amp::DiffEdit::Insert { index, elem_id });
                    let mut opid_to_value = HashMap::new();
                    for op in ops.iter() {
                        let amp_opid = actors.export_opid(&op.id);
                        if let Some(child_id) = op.child() {
                            opid_to_value
                                .insert(amp_opid, self.construct_object(&child_id, actors)?);
                        } else {
                            opid_to_value.insert(amp_opid, (&op.adjusted_value()).into());
                        }
                    }
                    props.insert(index, opid_to_value);
                    index += 1;
                }
            }
        }
        Ok(amp::SeqDiff {
            object_id: actors.export_obj(object_id),
            obj_type: seq_type,
            edits,
            props,
        }
        .into())
    }

    pub fn construct_object(
        &self,
        object_id: &ObjectID,
        actors: &ActorMap,
    ) -> Result<amp::Diff, AutomergeError> {
        let object = self.get_obj(&object_id)?;
        match object.obj_type {
            amp::ObjType::Map(map_type) => self.construct_map(object_id, object, actors, map_type),
            amp::ObjType::Sequence(seq_type) => {
                self.construct_list(object_id, object, actors, seq_type)
            }
        }
    }

    // this recursivly walks through all the objects touched by the changes
    // to generate a diff in a single pass
    pub fn finalize_diffs(
        &self,
        mut pending: HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<Option<amp::Diff>, AutomergeError> {
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
                    objs.push(inbound.obj);
                    pending.insert(inbound.obj, vec![PendingDiff::Set(inbound.clone())]);
                }
            }
        }

        Ok(Some(self.gen_obj_diff(
            &ObjectID::Root,
            &mut pending,
            actors,
        )?))
    }

    fn gen_seq_diff(
        &self,
        obj_id: &ObjectID,
        obj: &ObjState,
        pending: &[PendingDiff],
        pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
        seq_type: amp::SequenceType,
    ) -> Result<amp::Diff, AutomergeError> {
        let mut props = HashMap::new();
        let edits = pending.iter().filter_map(|p| p.edit(actors)).collect();
        // i may have duplicate keys - this makes sure I hit each one only once
        let keys: HashSet<_> = pending.iter().map(|p| p.operation_key()).collect();
        for key in keys.iter() {
            let mut opid_to_value = HashMap::new();
            for op in obj.props.get(&key).iter().flat_map(|i| i.iter()) {
                let link = match op.action {
                    InternalOpType::Set(_) => (&op.adjusted_value()).into(),
                    InternalOpType::Make(_) => {
                        self.gen_obj_diff(&op.id.into(), pending_diffs, actors)?
                    }
                    InternalOpType::Link(ref child) => self.construct_object(&child, actors)?,
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(actors.export_opid(&op.id), link);
            }
            if let Some(index) = obj
                .seq
                .index_of(&key.to_opid().ok_or(AutomergeError::HeadToOpID)?)
            {
                props.insert(index, opid_to_value);
            }
        }
        Ok(amp::SeqDiff {
            object_id: actors.export_obj(obj_id),
            obj_type: seq_type,
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
        map_type: amp::MapType,
    ) -> Result<amp::Diff, AutomergeError> {
        let mut props = HashMap::new();
        // I may have duplicate keys - I do this to make sure I visit each one only once
        let keys: HashSet<_> = pending.iter().map(|p| p.operation_key()).collect();
        for key in keys.iter() {
            let key_string = actors.key_to_string(key);
            let mut opid_to_value = HashMap::new();
            for op in obj.props.get(&key).iter().flat_map(|i| i.iter()) {
                let link = match op.action {
                    InternalOpType::Set(_) => (&op.adjusted_value()).into(),
                    InternalOpType::Make(_) => {
                        // FIXME
                        self.gen_obj_diff(&op.id.into(), pending_diffs, actors)?
                    }
                    InternalOpType::Link(ref child_id) => {
                        self.construct_object(&child_id, actors)?
                    }
                    _ => panic!("del or inc found in field_operations"),
                };
                opid_to_value.insert(actors.export_opid(&op.id), link);
            }
            props.insert(key_string, opid_to_value);
        }
        Ok(amp::MapDiff {
            object_id: actors.export_obj(obj_id),
            obj_type: map_type,
            props,
        }
        .into())
    }

    pub fn update_deps(&mut self, change: &Change) {
        //self.max_op = max(self.max_op, change.max_op());

        for d in change.deps.iter() {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash);
    }

    fn gen_obj_diff(
        &self,
        obj_id: &ObjectID,
        pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<amp::Diff, AutomergeError> {
        let obj = self.get_obj(obj_id)?;
        if let Some(pending) = pending_diffs.remove(obj_id) {
            match obj.obj_type {
                amp::ObjType::Sequence(seq_type) => {
                    self.gen_seq_diff(obj_id, obj, &pending, pending_diffs, actors, seq_type)
                }
                amp::ObjType::Map(map_type) => {
                    self.gen_map_diff(obj_id, obj, &pending, pending_diffs, actors, map_type)
                }
            }
        } else {
            Ok(amp::Diff::Unchanged(amp::ObjDiff {
                object_id: actors.export_obj(obj_id),
                obj_type: obj.obj_type,
            }))
        }
    }
}
