//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use std::collections::{HashMap, HashSet};

use automerge_protocol as amp;
use fxhash::FxBuildHasher;
use smol_str::SmolStr;
use tracing::instrument;

use crate::{
    actor_map::ActorMap,
    error::AutomergeError,
    internal::{InternalOpType, Key, ObjectId},
    object_store::ObjState,
    op_handle::OpHandle,
    ordered_set::OrderedSet,
    patches::{IncrementalPatch, PatchWorkshop},
    Change,
};

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
    pub objs: HashMap<ObjectId, ObjState, FxBuildHasher>,
    pub deps: HashSet<amp::ChangeHash>,
    pub max_op: u64,
    cursors: HashMap<ObjectId, Vec<CursorState>>,
}

impl Default for OpSet {
    fn default() -> Self {
        Self::new()
    }
}

impl OpSet {
    pub fn new() -> OpSet {
        let mut objs = HashMap::default();
        objs.insert(ObjectId::Root, ObjState::new(amp::ObjType::Map));

        OpSet {
            objs,
            max_op: 0,
            deps: HashSet::default(),
            cursors: HashMap::new(),
        }
    }

    pub(crate) fn apply_ops(
        &mut self,
        ops: Vec<OpHandle>,
        patch: &mut IncrementalPatch,
        actors: &mut ActorMap,
    ) -> Result<(), AutomergeError> {
        for op in ops {
            self.apply_op(op, actors, patch)?;
        }
        self.update_cursors(patch);
        Ok(())
    }

    pub fn heads(&self) -> Vec<amp::ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    #[instrument(level = "debug", skip(self))]
    fn apply_op(
        &mut self,
        op: OpHandle,
        actors: &mut ActorMap,
        patch: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.obj_type()) {
            //let child = actors.import_obj(child);
            self.objs.insert(child, ObjState::new(obj_type));
        }

        if let InternalOpType::Set(amp::ScalarValue::Cursor(ref oid)) = op.op.action {
            tracing::debug!(referred_opid=?oid, "Adding cursor");
            let internal_opid = actors.import_opid(oid);
            let mut target_found = false;
            for (obj_id, obj) in &self.objs {
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

        let object_id = op.obj;
        let object = self.get_obj_mut(&object_id)?;

        let overwritten = if object.is_seq() {
            if op.insert {
                object.insert_after(
                    op.key.as_element_id().ok_or(AutomergeError::MapKeyInSeq)?,
                    op.clone(),
                    actors,
                );
            }

            let ops = object
                .props
                .entry(op.operation_key().into_owned())
                .or_default();
            let before = !ops.is_empty();
            let (op, overwritten_ops) = ops.incorporate_new_op(op)?;
            let after = !ops.is_empty();

            match (before, after) {
                (true, true) => {
                    tracing::debug!("updating existing element");
                    let opid = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpId)?;
                    let ops = ops.clone();
                    let index = object.index_of(opid).unwrap_or(0);

                    patch.record_seq_updates(&object_id, object, index, ops.iter(), actors);
                }
                (true, false) => {
                    let opid = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpId)?;
                    let index = object.seq.remove_key(&opid).unwrap();
                    tracing::debug!(opid=?opid, index=%index, "deleting element");
                    patch.record_seq_remove(&object_id, op.clone(), index);
                }
                (false, true) => {
                    let id = op
                        .operation_key()
                        .to_opid()
                        .ok_or(AutomergeError::HeadToOpId)?;
                    let index = object.index_of(id).unwrap_or(0);
                    tracing::debug!(new_id=?id, index=%index, after=?op.operation_key(), "inserting new element");
                    object.seq.insert_index(index, id);
                    patch.record_seq_insert(&object_id, op.clone(), index, op.id);
                }
                (false, false) => {}
            };

            self.unlink(&op, &overwritten_ops)?;

            overwritten_ops
        } else {
            let ops = object.props.entry(op.key.clone()).or_default();
            let before = !ops.is_empty();
            let (op, overwritten_ops) = ops.incorporate_new_op(op)?;
            let after = !ops.is_empty();
            self.unlink(&op, &overwritten_ops)?;

            if before || after {
                patch.record_set(&object_id, op);
            }
            overwritten_ops
        };

        for op in overwritten {
            if let InternalOpType::Set(amp::ScalarValue::Cursor(ref oid)) = op.op.action {
                if let Some(opids) = self.cursors.get_mut(&op.op.obj) {
                    opids.retain(|o| o.element_opid != *oid);
                }
            }
        }
        Ok(())
    }

    fn unlink(&mut self, op: &OpHandle, overwritten: &[OpHandle]) -> Result<(), AutomergeError> {
        if let Some(child) = op.child() {
            self.get_obj_mut(&child)?.inbound = Some(op.clone());
        }

        for old in overwritten.iter() {
            if let Some(child) = old.child() {
                self.get_obj_mut(&child)?.inbound = None;
            }
        }
        Ok(())
    }

    pub fn get_obj(&self, object_id: &ObjectId) -> Result<&ObjState, AutomergeError> {
        self.objs
            .get(object_id)
            .ok_or(AutomergeError::MissingObjectError)
    }

    fn get_obj_mut(&mut self, object_id: &ObjectId) -> Result<&mut ObjState, AutomergeError> {
        self.objs
            .get_mut(object_id)
            .ok_or(AutomergeError::MissingObjectError)
    }

    /// Update any cursors which will be affected by the changes in `pending`
    /// and add the changed cursors to `pending`
    fn update_cursors(&mut self, patch: &mut IncrementalPatch) {
        // For each cursor, if the cursor references an object which has been changed we generate a
        // diff for the cursor
        if self.cursors.is_empty() {
            return;
        }

        let mut cursor_changes: HashMap<ObjectId, Vec<Key>> = HashMap::new();
        for obj_id in patch.changed_object_ids() {
            if let Some(cursors) = self.cursors.get_mut(obj_id) {
                for cursor in cursors.iter_mut() {
                    if let Some(obj) = self.objs.get(&cursor.internal_referred_object_id) {
                        cursor.index = obj.index_of(cursor.internal_element_opid).unwrap_or(0);
                        cursor_changes
                            .entry(cursor.internal_referring_object_id)
                            .or_default()
                            .push(cursor.key.clone())
                    }
                }
            }
        }
        for (obj_id, keys) in cursor_changes {
            for key in keys {
                patch.record_cursor_change(&obj_id, key)
            }
        }
    }

    pub fn update_deps(&mut self, change: &Change) {
        //self.max_op = max(self.max_op, change.max_op());

        for d in &change.deps {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash);
    }

    pub(crate) fn patch_workshop<'a>(&'a self, actors: &'a ActorMap) -> impl PatchWorkshop + 'a {
        PatchWorkshopImpl {
            opset: self,
            actors,
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
    referring_object_id: amp::ObjectId,
    /// The same as `referring_object_id` but as an internal::ObjectID
    internal_referring_object_id: ObjectId,
    /// The key withing the referring object this cursor lives at
    key: crate::internal::Key,
    /// The id of the sequence this cursor refers
    referred_object_id: amp::ObjectId,
    /// The same as the `referred_object_id` but as an internal::ObjectID
    internal_referred_object_id: ObjectId,
    /// The OpID of the element within the sequence this cursor refers to
    element_opid: amp::OpId,
    /// The same as the `element_opid` but as an internal::OpID,
    internal_element_opid: crate::internal::OpId,
    index: usize,
}

/// Implementation of `patches::PatchWorkshop` to pass to the various patch
/// generation mechanisms, defined here to avoid having to make members of the
/// OpSet public.
struct PatchWorkshopImpl<'a> {
    opset: &'a OpSet,
    actors: &'a ActorMap,
}

impl<'a> PatchWorkshop for PatchWorkshopImpl<'a> {
    fn get_obj(&self, object_id: &ObjectId) -> Option<&ObjState> {
        self.opset.get_obj(object_id).ok()
    }

    fn find_cursor(&self, opid: &amp::OpId) -> Option<amp::CursorDiff> {
        self.opset
            .cursors
            .values()
            .flatten()
            .find(|c| c.element_opid == *opid)
            .map(|c| amp::CursorDiff {
                object_id: c.referred_object_id.clone(),
                index: c.index as u32,
                elem_id: opid.clone(),
            })
    }

    fn key_to_string(&self, key: &crate::internal::Key) -> SmolStr {
        self.actors.key_to_string(key)
    }

    fn make_external_opid(&self, opid: &crate::internal::OpId) -> amp::OpId {
        self.actors.export_opid(opid)
    }

    fn make_external_objid(&self, object_id: &ObjectId) -> amp::ObjectId {
        self.actors.export_obj(object_id)
    }
}
