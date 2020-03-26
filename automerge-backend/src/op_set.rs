//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::actor_states::ActorStates;
use crate::error::AutomergeError;
use crate::object_store::ObjState;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{Change, Clock, ObjectID, OpID, Operation, RequestKey};
use crate::{ChangeRequest, Diff2, DiffEdit, Key, ObjType, PendingDiff};
use core::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;

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
pub struct Version {
    pub version: u64,
    pub local_only: bool,
    pub op_set: OpSet,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OpSet {
    pub objs: HashMap<ObjectID, ObjState>,
    queue: Vec<Change>,
    pub clock: Clock,
    pub deps: Clock,
    pub undo_pos: usize,
    pub undo_stack: Vec<Vec<Operation>>,
    pub redo_stack: Vec<Vec<Operation>>,
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
    ///

    pub fn add_change(
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

    fn apply_change(
        &mut self,
        change: Change,
        _local: bool,
        undoable: bool,
        diffs: &mut Vec<PendingDiff>,
    ) -> Result<(), AutomergeError> {
        // This method is a little more complicated than it intuitively should
        // be due to the bookkeeping required for undo. If we're asked to make
        // this operation undoable we have to store the undo operations for
        // each operation and then add them to the undo stack at the end of the
        // method. However, it's unnecessary to store undo operations for
        // objects which are created by this change (e.g if there's an insert
        // operation for a list which was created in this operation we only
        // need the undo operation for the creation of the list to achieve
        // the undo), so we track newly created objects and only store undo
        // operations which don't operate on them.
        let actor_id = change.actor_id.clone();
        let seq = change.seq;
        let start_op = change.start_op;
        let num_ops = change.operations.len() as u64;
        let operations = change.operations.clone(); // FIXME - shouldnt need this clone
        let all_deps = self
            .states
            .transitive_deps(&change.deps.with(&change.actor_id, change.seq - 1));

        if !self.states.add_change(change, all_deps.clone())? {
            return Ok(());
        }

        let mut all_undo_ops = Vec::new();
        let mut new_object_ids: HashSet<ObjectID> = HashSet::new();

        for (n, operation) in operations.iter().enumerate() {
            // Store newly created object IDs so we can decide whether we need
            // undo ops later
            let metaop = OperationWithMetadata {
                opid: OpID::ID(start_op + (n as u64), actor_id.0.clone()),
                seq,
                actor_id: actor_id.clone(),
                operation: operation.clone(),
            };

            if metaop.is_make() {
                new_object_ids.insert(metaop.opid.to_object_id());
            }

            let (diff, undo_ops) = self.apply_op(metaop.clone())?;

            // FIXME - this should be Option<Vec<..>> but I couldnt get it to work
            diffs.push(diff);

            if undoable && !(new_object_ids.contains(metaop.object_id())) {
                all_undo_ops.extend(undo_ops);
            }
        }

        self.max_op = max(self.max_op, start_op + num_ops - 1);

        self.clock.set(&actor_id, seq);

        self.deps.subtract(&all_deps);
        self.deps.set(&actor_id, seq);

        if undoable {
            let (new_undo_stack_slice, _) = self.undo_stack.split_at(self.undo_pos);
            let mut new_undo_stack: Vec<Vec<Operation>> = new_undo_stack_slice.to_vec();
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
    pub fn apply_op(
        &mut self,
        op: OperationWithMetadata,
    ) -> Result<(PendingDiff, Vec<Operation>), AutomergeError> {
        if let (Some(child), Some(obj_type)) = (op.child(), op.make_type()) {
            self.objs.insert(child, ObjState::new(obj_type));
        }

        let undo_ops = Vec::new();

        let object_id = op.object_id();
        let object = self.get_obj_mut(&object_id)?;

        if object.is_seq() {
            if op.insert() {
                object.insert_after(op.key().as_element_id()?, op.clone());
            }

            let ops = object.props.entry(op.list_key()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;

            let index = object.get_index_for(&op.list_key().to_opid()?)?;

            self.unlink(&op, &overwritten_ops)?;

            Ok((PendingDiff::Seq(op.clone(), index), undo_ops))
        } else {
            let ops = object.props.entry(op.key().clone()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            self.unlink(&op, &overwritten_ops)?;

            Ok((PendingDiff::Map(op.clone()), undo_ops))
        }
    }

    fn unlink(
        &mut self,
        op: &OperationWithMetadata,
        overwritten: &[OperationWithMetadata],
    ) -> Result<(), AutomergeError> {
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

    fn get_path(
        &self,
        object_id: &ObjectID,
    ) -> Result<Vec<(ObjectID, &Key, ObjectID)>, AutomergeError> {
        let mut o = object_id;
        let mut path = Vec::new();
        while let Some(inbound) = self.objs.get(o).and_then(|o| o.inbound.iter().next()) {
            o = inbound.object_id();
            path.insert(
                0,
                (
                    inbound.object_id().clone(),
                    inbound.key(),
                    inbound.child().unwrap(),
                ),
            );
        }
        Ok(path)
    }

    pub fn finalize_diffs(&self, pending: Vec<PendingDiff>) -> Result<Diff2, AutomergeError> {
        let mut diff2 = Diff2::new();

        for diff in pending.iter() {
            match diff {
                PendingDiff::Seq(op, index) => {
                    let object_id = op.object_id();
                    let path = self.get_path(object_id)?;
                    let object = self.objs.get(object_id).unwrap();
                    let ops = object.props.get(&op.list_key()).unwrap();

                    let node = diff2.expand_path(&path, self);

                    if op.insert() {
                        node.add_insert(*index);
                    } else if ops.is_empty() {
                        node.add_remove(*index);
                    }

                    if !ops.is_empty() {
                        let final_index = object.get_index_for(&op.list_key().to_opid()?)?;
                        let key = Key(final_index.to_string());
                        node.add_values(&key, &ops);
                    } else {
                        node.touch();
                    }
                }
                PendingDiff::Map(op) => {
                    let object_id = op.object_id();
                    let path = self.get_path(object_id)?;
                    let object = self.objs.get(object_id).unwrap();
                    let key = op.key();
                    let ops = object.props.get(&key).unwrap();
                    diff2.expand_path(&path, self).add_values(&key, &ops);
                }
                PendingDiff::NoOp => {}
            }
        }

        Ok(diff2)
    }

    pub fn get_ops(&self, object_id: &ObjectID, key: &Key) -> Option<Vec<OpID>> {
        self.objs
            .get(object_id)
            .and_then(|obj| obj.props.get(key))
            .map(|con_ops| con_ops.iter().map(|op| op.opid.clone()).collect())
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
        // TODO: there's a lot of internal copying going on in here for something kinda simple
        self.queue.iter().fold(Clock::empty(), |clock, change| {
            clock
                .union(&change.deps)
                .with(&change.actor_id, change.seq - 1)
        })
    }

    pub fn get_pred(&self, object_id: &ObjectID, key: &Key, insert: bool) -> Vec<OpID> {
        if insert {
            Vec::new()
        } else if let Some(ops) = self.get_ops(&object_id, &key) {
            ops
        } else if let Ok(opid) = key.to_opid() {
            vec![opid]
        } else {
            Vec::new()
        }
    }

    // FIXME - omg - this function is so bad :(
    pub fn resolve_key(
        &self,
        id: &OpID,
        object_id: &ObjectID,
        key: &RequestKey,
        elemids: &mut HashMap<ObjectID, Vec<OpID>>,
        insert: bool,
        del: bool,
    ) -> Result<Key, AutomergeError> {
        match key {
            RequestKey::Str(s) => Ok(Key(s.clone())),
            RequestKey::Num(n) => {
                let n: usize = *n as usize;
                let ids = elemids
                    .entry(object_id.clone())
                    .or_insert_with(|| self.get_elem_ids(&object_id));
                (if insert {
                    if n == 0 {
                        ids.insert(0, id.clone());
                        Some(Key("_head".to_string()))
                    } else {
                        ids.insert(n, id.clone());
                        ids.get(n - 1).map(|i| i.to_key())
                    }
                } else if del {
                    if n < ids.len() {
                        Some(ids.remove(n).to_key())
                    } else {
                        None
                    }
                } else {
                    ids.get(n).map(|i| i.to_key())
                })
                .ok_or(AutomergeError::IndexOutOfBounds(n))
            }
        }
    }

    pub fn construct_map(
        &self,
        object_id: &ObjectID,
        object: &ObjState,
    ) -> Result<Diff2, AutomergeError> {
        let mut diff = Diff2 {
            object_id: object_id.clone(),
            edits: None,
            props: Some(HashMap::new()),
            obj_type: object.obj_type,
        };
        for (key, ops) in object.props.iter() {
            for op in ops.iter() {
                if let Some(child_id) = op.child() {
                    diff.add_child(&key, &op.opid, self.construct_object(&child_id)?);
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
    ) -> Result<Diff2, AutomergeError> {
        let mut diff = Diff2 {
            object_id: object_id.clone(),
            edits: Some(Vec::new()),
            props: Some(HashMap::new()),
            obj_type: object.obj_type,
        };
        //let elemid = ElementID::Head;
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
                            diff.add_child(&key, &op.opid, self.construct_object(&child_id)?);
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

    pub fn construct_object(&self, object_id: &ObjectID) -> Result<Diff2, AutomergeError> {
        let object = self.get_obj(&object_id)?;
        if object.is_seq() {
            self.construct_list(object_id, object)
        } else {
            self.construct_map(object_id, object)
        }
    }
}
