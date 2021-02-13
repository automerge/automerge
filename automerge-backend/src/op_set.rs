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
use tracing::instrument;

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
/// at each node by examining the concurrent operations which are active for
/// that node.
///

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct OpSet {
    pub objs: HashMap<ObjectID, Rc<ObjState>, FxBuildHasher>,
    pub deps: HashSet<amp::ChangeHash>,
    pub max_op: u64,
    cursors: HashMap<ObjectID, Vec<CursorState>>,
}

impl OpSet {
    pub fn init() -> OpSet {
        let mut objs = HashMap::default();
        objs.insert(ObjectID::Root, Rc::new(ObjState::new(amp::ObjType::map())));

        OpSet {
            objs,
            max_op: 0,
            deps: HashSet::default(),
            cursors: HashMap::new(),
        }
    }

    pub(crate) fn apply_ops(
        &mut self,
        mut ops: Vec<OpHandle>,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &mut ActorMap,
    ) -> Result<(), AutomergeError> {
        for op in ops.drain(..) {
            let obj_id = op.obj;

            let pending_diff = self.apply_op(op, actors)?;

            if let Some(diff) = pending_diff {
                diffs.entry(obj_id).or_default().push(diff);
            }
        }
        Ok(())
    }

    pub fn heads(&self) -> Vec<amp::ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().cloned().collect();
        deps.sort_unstable();
        deps
    }

    #[instrument(skip(self))]
    fn apply_op(
        &mut self,
        op: OpHandle,
        actors: &mut ActorMap,
    ) -> Result<Option<PendingDiff>, AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.obj_type()) {
            //let child = actors.import_obj(child);
            self.objs.insert(child, Rc::new(ObjState::new(obj_type)));
        }

        if let InternalOpType::Set(amp::ScalarValue::Cursor(ref oid)) = op.op.action {
            tracing::debug!(referred_opid=?oid, "Adding cursor");
            let internal_opid = actors.import_opid(oid);
            let mut target_found = false;
            for (obj_id, obj) in self.objs.iter() {
                if obj.insertions.contains_key(&internal_opid.into()) {
                    target_found = true;
                    self.cursors.entry(*obj_id).or_default().push(CursorState {
                        referring_object_id: actors.export_obj(&op.obj),
                        internal_referring_object_id: op.obj,
                        key: op.key.clone(),
                        element_opid: oid.clone(),
                        internal_element_opid: internal_opid,
                        index: obj.index_of(internal_opid).unwrap_or(0),
                        referred_object_id: actors.export_obj(obj_id),
                        internal_referred_object_id: *obj_id,
                    });
                }
            }
            if !target_found {
                return Err(AutomergeError::InvalidCursor { opid: oid.clone() });
            }
        }

        let object_id = &op.obj;
        let object = self.get_obj_mut(&object_id)?;

        let (diff, overwritten) = if object.is_seq() {
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
                (true, true) => {
                    tracing::debug!("updating existing element");
                    Some(PendingDiff::Set(op.clone()))
                }
                (true, false) => {
                    let opid = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.seq.remove_key(&opid).unwrap();
                    tracing::debug!(opid=?opid, index=%index, "deleting element");
                    Some(PendingDiff::SeqRemove(op.clone(), index))
                }
                (false, true) => {
                    let id = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpID)?;
                    let index = object.index_of(id).unwrap_or(0);
                    tracing::debug!(new_id=?id, index=%index, after=?op.operation_key(), "inserting new element");
                    object.seq.insert_index(index, id);
                    Some(PendingDiff::SeqInsert(op.clone(), index, op.id))
                }
                (false, false) => None,
            };

            self.unlink(&op, &overwritten_ops)?;

            (diff, overwritten_ops)
        } else {
            let ops = object.props.entry(op.key.clone()).or_default();
            let before = !ops.is_empty();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let after = !ops.is_empty();
            self.unlink(&op, &overwritten_ops)?;

            if before || after {
                tracing::debug!(overwritten_ops=?overwritten_ops, "setting new value");
                (Some(PendingDiff::Set(op)), overwritten_ops)
            } else {
                tracing::debug!(overwritten_ops=?overwritten_ops, "deleting value");
                (None, overwritten_ops)
            }
        };

        for op in overwritten {
            if let InternalOpType::Set(amp::ScalarValue::Cursor(ref oid)) = op.op.action {
                if let Some(opids) = self.cursors.get_mut(&op.op.obj) {
                    opids.retain(|o| o.element_opid != *oid);
                }
            }
        }
        Ok(diff)
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
                        opid_to_value
                            .insert(amp_opid, self.gen_value_diff(op, &op.adjusted_value()));
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
                            opid_to_value
                                .insert(amp_opid, self.gen_value_diff(op, &op.adjusted_value()));
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

    // this recursively walks through all the objects touched by the changes
    // to generate a diff in a single pass
    pub fn finalize_diffs(
        &mut self,
        mut pending: HashMap<ObjectID, Vec<PendingDiff>>,
        actors: &ActorMap,
    ) -> Result<Option<amp::Diff>, AutomergeError> {
        if pending.is_empty() {
            return Ok(None);
        }

        // For each cursor, if the cursor references an object which has been changed we generate a
        // diff for the cursor
        let mut cursor_changes: HashMap<ObjectID, Vec<PendingDiff>> = HashMap::new();
        for obj_id in pending.keys() {
            if let Some(cursors) = self.cursors.get_mut(&obj_id) {
                for cursor in cursors.iter_mut() {
                    if let Some(obj) = self.objs.get(&cursor.internal_referred_object_id) {
                        cursor.index = obj.index_of(cursor.internal_element_opid).unwrap_or(0);
                        cursor_changes
                            .entry(cursor.internal_referring_object_id)
                            .or_default()
                            .push(PendingDiff::CursorChange(cursor.key.clone()))
                    }
                }
            }
        }
        for (obj_id, cursor_change) in cursor_changes {
            pending.entry(obj_id).or_default().extend(cursor_change)
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
                    InternalOpType::Set(ref value) => self.gen_value_diff(op, value),
                    InternalOpType::Make(_) => {
                        self.gen_obj_diff(&op.id.into(), pending_diffs, actors)?
                    }
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
                    InternalOpType::Set(ref value) => self.gen_value_diff(op, value),
                    InternalOpType::Make(_) => {
                        // FIXME
                        self.gen_obj_diff(&op.id.into(), pending_diffs, actors)?
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

    fn gen_value_diff(&self, op: &OpHandle, value: &amp::ScalarValue) -> amp::Diff {
        match value {
            amp::ScalarValue::Cursor(oid) => {
                // .expect() is okay here because we check that the cursr exists at the start of
                // `OpSet::apply_op()`
                let cursor_state = self
                    .cursors
                    .values()
                    .flatten()
                    .find(|c| c.element_opid == *oid)
                    .expect("missing cursor");
                amp::Diff::Cursor(amp::CursorDiff {
                    object_id: cursor_state.referred_object_id.clone(),
                    index: cursor_state.index as u32,
                    elem_id: oid.clone(),
                })
            }
            _ => op.adjusted_value().into(),
        }
    }
}

/// `CursorState` is the information we need to track in order to update cursors as changes come
/// in. Cursors are created by `Set` operations and therefore live in a particular object (the
/// "referring object") and point at an element in a sequence (the "referred" object). For example
/// this operation:
///
/// ```json
/// {
///     "action": "set",
///     "obj": "_root",
///     "key": "a_cursor",
///     "refObjectId": "1@222"
/// }
/// ```
///
/// Creates a cursor in the root object under the "a_cursor" key which points at element "1@222".
/// When we process a set operation which is a cursor we find the object which contains "1@222" and
/// populate this `CursorState`.
///
/// Note that several fields are duplicated for internal and `automerge_protocol` types. This is
/// because we need to compare those fields against internal types when processing cursors, but we
/// need to create patches which use the `automerge_protocol` types.
#[derive(Debug, PartialEq, Clone)]
struct CursorState {
    /// The id of the object this cursor lives in
    referring_object_id: amp::ObjectID,
    /// The same as `referring_object_id` but as an internal::ObjectID
    internal_referring_object_id: ObjectID,
    /// The key withing the referring object this cursor lives at
    key: crate::internal::Key,
    /// The id of the sequence this cursor refers
    referred_object_id: amp::ObjectID,
    /// The same as the `referred_object_id` but as an internal::ObjectID
    internal_referred_object_id: ObjectID,
    /// The OpID of the element within the sequence this cursor refers to
    element_opid: amp::OpID,
    /// The same as the `element_opid` but as an internal::OpID,
    internal_element_opid: crate::internal::OpID,
    index: usize,
}
