//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::actor_states::ActorStates;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::object_store::ObjState;
use crate::op_handle::OpHandle;
use crate::patch::{Diff, DiffEdit, PendingDiff};
use crate::protocol::{Change, ChangeRequest, Clock, Key, ObjType, ObjectID, OpID, UndoOperation};
use core::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;
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
pub(crate) struct Version {
    pub version: u64,
    pub local_only: bool,
    pub op_set: OpSet,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct OpSet {
    pub objs: HashMap<ObjectID, ObjState>,
    queue: Vec<Change>,
    pub clock: Clock,
    pub deps: Clock,
    pub undo_pos: usize,
    pub undo_stack: Vec<Vec<UndoOperation>>,
    pub redo_stack: Vec<Vec<UndoOperation>>,
    pub states: ActorStates,
    pub max_op: u64,
}

impl OpSet {
    pub fn init() -> OpSet {
        let mut objs = HashMap::new();
        objs.insert(ObjectID::Root, ObjState::new(ObjType::Map));

        OpSet {
            objs,
            queue: Vec::new(),
            clock: Clock::empty(),
            deps: Clock::empty(),
            undo_pos: 0,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            states: ActorStates::new(),
            max_op: 0,
        }
    }

    /// Adds a change to the internal queue of operations, then iteratively
    /// applies all causally ready changes until there are none remaining
    ///
    /// If `make_undoable` is true, the op set will store a set of operations
    /// which can be used to undo this change.

    pub(crate) fn add_change(
        &mut self,
        change: Change,
        local: bool,
        undoable: bool,
        diffs: &mut Vec<PendingDiff>,
    ) -> Result<(), AutomergeError> {
        if local {
            self.apply_change(change, local, undoable, diffs)
        } else {
            self.queue.push(change);
            self.apply_queued_ops(diffs)
        }
    }

    fn apply_queued_ops(&mut self, diffs: &mut Vec<PendingDiff>) -> Result<(), AutomergeError> {
        // TODO: drain_filter
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, false, false, diffs)?;
        }
        Ok(())
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            let deps = change.deps.with(&change.actor_id, change.seq - 1);
            if deps <= self.clock {
                return Some(self.queue.remove(index));
            }
            index += 1
        }
        None
    }

    fn apply_ops(
        &mut self,
        change: &Rc<Change>,
        undoable: bool,
        diffs: &mut Vec<PendingDiff>,
    ) -> Result<Vec<UndoOperation>, AutomergeError> {
        let mut all_undo_ops = Vec::new();
        let mut new_objects: HashSet<OpID> = HashSet::new();
        for op in OpHandle::extract(change).iter() {
            if op.is_make() {
                new_objects.insert(op.id.clone());
            }

            let (diff, undo_ops) = self.apply_op(op)?;

            diffs.push(diff);

            if undoable && !(new_objects.contains(&op.id)) {
                all_undo_ops.extend(undo_ops);
            }
        }
        Ok(all_undo_ops)
    }

    fn apply_change(
        &mut self,
        change: Change,
        _local: bool,
        undoable: bool,
        diffs: &mut Vec<PendingDiff>,
    ) -> Result<(), AutomergeError> {
        let change = Rc::new(change);

        if let Some(all_deps) = self.states.add_change(&change)? {
            self.clock.set(&change.actor_id, change.seq);
            self.deps.subtract(&all_deps);
            self.deps.set(&change.actor_id, change.seq);
        } else {
            // duplicate change - ignore
            return Ok(());
        }

        let all_undo_ops = self.apply_ops(&change, undoable, diffs)?;

        self.max_op = max(self.max_op, change.max_op());

        if undoable {
            let (new_undo_stack_slice, _) = self.undo_stack.split_at(self.undo_pos);
            let mut new_undo_stack: Vec<Vec<UndoOperation>> = new_undo_stack_slice.to_vec();
            new_undo_stack.push(all_undo_ops);
            self.undo_stack = new_undo_stack;
            self.undo_pos += 1;
        };

        Ok(())
    }

    /// Incorporates a new operation into the object store. The caller is
    /// responsible for ensuring that all causal dependencies of the new
    /// operation have already been applied.
    ///
    /// The return value is a tuple of a diff to send to the frontend, and
    /// a (possibly empty) vector of operations which will undo the operation
    /// later.
    fn apply_op(
        &mut self,
        op: &OpHandle,
    ) -> Result<(PendingDiff, Vec<UndoOperation>), AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.obj_type()) {
            self.objs.insert(child, ObjState::new(obj_type));
        }

        let object_id = &op.obj;
        let object = self.get_obj_mut(&object_id)?;

        if object.is_seq() {
            if op.insert {
                object.insert_after(op.key.as_element_id()?, op.clone());
            }

            let ops = object.props.entry(op.operation_key()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;

            let undo_ops = op.generate_undos(&overwritten_ops);

            let index = object.get_index_for(&op.operation_key().to_opid()?)?;

            self.unlink(&op, &overwritten_ops)?;

            Ok((PendingDiff::Seq(op.clone(), index), undo_ops))
        } else {
            let ops = object.props.entry(op.key.clone()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            let undo_ops = op.generate_undos(&overwritten_ops);
            self.unlink(&op, &overwritten_ops)?;

            Ok((PendingDiff::Map(op.clone()), undo_ops))
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

    fn extract(
        &self,
        op: &OpHandle,
    ) -> Result<(Vec<&OpHandle>, &ConcurrentOperations), AutomergeError> {
        let mut object_id = &op.obj;
        let mut path = Vec::new();
        let object = self
            .objs
            .get(&op.obj)
            .ok_or_else(|| AutomergeError::CantExtractObject(op.obj.clone()))?;
        let ops = object
            .props
            .get(&op.operation_key())
            .ok_or_else(|| AutomergeError::CantExtractObject(op.obj.clone()))?;

        while object_id != &ObjectID::Root {
            if let Some(inbound) = self
                .objs
                .get(object_id)
                .and_then(|obj| obj.inbound.iter().next())
            {
                path.insert(0, inbound);
                object_id = &inbound.obj;
            } else {
                return Err(AutomergeError::NoPathToObject(object_id.clone()));
            }
        }
        Ok((path, ops))
    }

    pub fn finalize_diffs(
        &self,
        pending: Vec<PendingDiff>,
    ) -> Result<Option<Diff>, AutomergeError> {
        if pending.is_empty() {
            Ok(None)
        } else {
            let mut diff = Diff::new();
            for action in pending.iter() {
                match action {
                    PendingDiff::Seq(op, index) => {
                        let (path, ops) = self.extract(&op)?;

                        let node = diff.expand_path(&path, self)?;

                        if op.insert {
                            node.add_insert(*index);
                        } else if ops.is_empty() {
                            node.add_remove(*index);
                        }

                        node.add_values(&op.operation_key(), &ops);
                    }
                    PendingDiff::Map(op) => {
                        let (path, ops) = self.extract(&op)?;
                        diff.expand_path(&path, self)?.add_values(&op.key, &ops);
                    }
                }
            }

            diff.remap_list_keys(&self)?;

            Ok(Some(diff))
        }
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
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))
    }

    fn get_obj_mut(&mut self, object_id: &ObjectID) -> Result<&mut ObjState, AutomergeError> {
        self.objs
            .get_mut(&object_id)
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))
    }

    pub fn check_for_duplicate(&self, request: &ChangeRequest) -> Result<(), AutomergeError> {
        if self.clock.get(&request.actor) >= request.seq {
            return Err(AutomergeError::DuplicateChange(format!(
                "Change request has already been applied {}:{}",
                request.actor.0, request.seq
            )));
        }
        Ok(())
    }

    pub fn can_undo(&self) -> bool {
        self.undo_pos > 0
    }

    pub fn can_redo(&self) -> bool {
        !self.redo_stack.is_empty()
    }

    /// Get all the changes we have that are not in `since`
    pub fn get_missing_changes(&self, since: &Clock) -> Vec<&Change> {
        self.states
            .history
            .iter()
            .map(|rc| rc.as_ref())
            .filter(|change| change.seq > since.get(&change.actor_id))
            .collect()
    }

    pub fn get_elem_ids(&self, object_id: &ObjectID) -> Vec<OpID> {
        self.objs
            .get(object_id)
            .map(|obj| obj.ops_in_order().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_missing_deps(&self) -> Clock {
        let mut clock = Clock::empty();
        for change in self.queue.iter() {
            clock.merge(&change.deps.with(&change.actor_id, change.seq - 1))
        }
        clock
    }

    pub fn get_pred(&self, object_id: &ObjectID, key: &Key, insert: bool) -> Vec<OpID> {
        if insert {
            Vec::new()
        } else if let Some(ops) = self.get_field_opids(&object_id, &key) {
            ops
        } else if let Ok(opid) = key.to_opid() {
            vec![opid]
        } else {
            Vec::new()
        }
    }

    pub fn construct_map(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
    ) -> Result<Diff, AutomergeError> {
        let mut diff = Diff {
            object_id: object_id.clone(),
            edits: None,
            props: Some(HashMap::new()),
            obj_type: object.obj_type,
        };
        for (key, ops) in object.props.iter() {
            for op in ops.iter() {
                if let Some(child_id) = op.child() {
                    diff.add_child(&key, &op.id, self.construct_object(&child_id)?);
                } else {
                    diff.add_value(&key, &op);
                }
            }
        }
        Ok(diff)
    }

    pub fn construct_list(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
    ) -> Result<Diff, AutomergeError> {
        let mut diff = Diff {
            object_id: object_id.clone(),
            obj_type: object.obj_type,
            edits: Some(Vec::new()),
            props: Some(HashMap::new()),
        };
        let mut index = 0;
        let mut max_counter = 0;

        for opid in object.ops_in_order() {
            max_counter = max(max_counter, opid.counter());
            if let Some(ops) = object.props.get(&opid.to_key()) {
                if !ops.is_empty() {
                    diff.edits
                        .get_or_insert_with(Vec::new)
                        .push(DiffEdit::Insert { index });
                    let key = Key(index.to_string());
                    for op in ops.iter() {
                        if let Some(child_id) = op.child() {
                            diff.add_child(&key, &op.id, self.construct_object(&child_id)?);
                        } else {
                            diff.add_value(&key, &op);
                        }
                    }
                    index += 1;
                }
            }
        }
        Ok(diff)
    }

    pub fn construct_object(&self, object_id: &ObjectID) -> Result<Diff, AutomergeError> {
        let object = self.get_obj(&object_id)?;
        if object.is_seq() {
            self.construct_list(object_id, object)
        } else {
            self.construct_map(object_id, object)
        }
    }
}
