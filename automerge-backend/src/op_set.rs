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
use crate::protocol::{Change, Clock, OpID, Operation};
use crate::{ActorID, Diff2, Key, ObjType, PendingDiff};
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
#[derive(Debug, PartialEq, Clone)]
pub struct OpSet {
    pub objs: HashMap<OpID, ObjState>,
    queue: Vec<Change>,
    pub clock: Clock,
    undo_pos: usize,
    pub undo_stack: Vec<Vec<Operation>>,
    pub redo_stack: Vec<Vec<Operation>>,
    pub states: ActorStates,
    pub max_op: u64,
}

impl OpSet {
    pub fn init() -> OpSet {
        let mut objs = HashMap::new();
        objs.insert(OpID::Root, ObjState::new(ObjType::Map));

        OpSet {
            objs,
            queue: Vec::new(),
            clock: Clock::empty(),
            undo_pos: 0,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            states: ActorStates::new(),
            max_op: 0,
        }
    }

    pub fn do_redo(
        &mut self,
        actor_id: ActorID,
        seq: u32,
        message: Option<String>,
        dependencies: Clock,
        diffs: &mut Vec<PendingDiff>,
        diff2: &mut Diff2,
    ) -> Result<(), AutomergeError> {
        if let Some(redo_ops) = self.redo_stack.pop() {
            let change = Change {
                actor_id,
                start_op: 0, // FIXME
                time: 0,     // FIXME
                seq,
                message,
                dependencies,
                operations: redo_ops,
            };
            self.undo_pos += 1;
            self.add_change(change, false, diffs, diff2)
        } else {
            Err(AutomergeError::InvalidChange("no redo ops".to_string()))
        }
    }

    pub fn do_undo(
        &mut self,
        actor_id: ActorID,
        seq: u32,
        message: Option<String>,
        dependencies: Clock,
        diffs: &mut Vec<PendingDiff>,
        diff2: &mut Diff2,
    ) -> Result<(), AutomergeError> {
        if let Some(undo_ops) = self.undo_stack.get(self.undo_pos - 1) {
            let redo_ops = undo_ops
                .iter()
                .filter_map(|op| match &op {
                    Operation::Increment {
                        object_id: oid,
                        key,
                        value,
                        pred,
                    } => Some(vec![Operation::Increment {
                        object_id: oid.clone(),
                        key: key.clone(),
                        value: -value,
                        pred: pred.clone(),
                    }]),
                    Operation::Set { .. } | Operation::Link { .. } | Operation::Delete { .. } => {
                        panic!("not implemented")
                    }
                    /*
                                            self
                                            .concurrent_operations_for_field(object_id, key)
                                            .map(|cops| {
                                                if cops.active_op().is_some() {
                                                    cops.pure_operations()
                                                } else {
                                                    vec![Operation::Delete {
                                                        object_id: object_id.clone(),
                                                        key: key.clone(),
                                                        pred: pred.clone(),
                                                    }]
                                                }
                                            }),
                    */
                    _ => None,
                })
                .flatten()
                .collect();
            self.redo_stack.push(redo_ops);
            let change = Change {
                start_op: 0, // FIXME
                time: 0,     // FIXME
                actor_id,
                seq,
                message,
                dependencies,
                operations: undo_ops.clone(),
            };
            self.undo_pos -= 1;
            self.add_change(change, false, diffs, diff2)
        } else {
            Err(AutomergeError::InvalidChange(
                "No undo ops to execute".to_string(),
            ))
        }
    }

    /// Adds a change to the internal queue of operations, then iteratively
    /// applies all causally ready changes until there are none remaining
    ///
    /// If `make_undoable` is true, the op set will store a set of operations
    /// which can be used to undo this change.
    pub fn add_change(
        &mut self,
        change: Change,
        make_undoable: bool,
        diffs: &mut Vec<PendingDiff>,
        diff2: &mut Diff2,
    ) -> Result<(), AutomergeError> {
        self.queue.push(change);
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, make_undoable, diffs, diff2)?;
        }
        Ok(())
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            let deps = change.dependencies.with(&change.actor_id, change.seq - 1);
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
        make_undoable: bool,
        diffs: &mut Vec<PendingDiff>,
        diff2: &mut Diff2,
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

        if !self.states.add_change(change)? {
            return Ok(());
        }

        let all_undo_ops = Vec::new();
        let mut new_object_ids: HashSet<OpID> = HashSet::new();

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
                new_object_ids.insert(metaop.opid.clone());
            }

            let diff = self.apply_op(metaop, diff2)?;

            diffs.push(diff);

            // If this object is not created in this change then we need to
            // store the undo ops for it (if we're storing undo ops at all)

            if make_undoable && !(new_object_ids.contains(operation.obj())) {
                //all_undo_ops.extend(undo_ops);
            }
        }

        self.max_op = max(self.max_op, start_op + num_ops - 1);
        self.clock = self.clock.with(&actor_id, seq);

        if make_undoable {
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
        _diff2: &mut Diff2,
    ) -> Result<PendingDiff, AutomergeError> {
        if let Some(obj_type) = op.make_type() {
            self.objs.insert(op.opid.clone(), ObjState::new(obj_type));
        }

        let object_id = op.object_id();
        let object = self.get_obj(&object_id)?;

        if object.is_seq() {
            if op.is_insert() {
                object.insert_after(op.key().as_element_id()?, op.clone());
            }

            let ops = object.props.entry(op.list_key()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;

            let index = object.get_index_for(&op.list_key().to_opid()?)?;

            self.unlink(&op, &overwritten_ops)?;

            Ok(PendingDiff::Seq(op.clone(), index))
        } else {
            let ops = object.props.entry(op.key().clone()).or_default();
            let overwritten_ops = ops.incorporate_new_op(&op)?;
            self.unlink(&op, &overwritten_ops)?;

            Ok(PendingDiff::Map(op.clone()))
        }
    }

    fn unlink(
        &mut self,
        op: &OperationWithMetadata,
        overwritten: &[OperationWithMetadata],
    ) -> Result<(), AutomergeError> {
        if let Some(child) = op.child() {
            self.get_obj(&child)?.inbound.insert(op.clone());
        }

        for old in overwritten.iter() {
            if let Some(child) = old.child() {
                self.get_obj(&child)?.inbound.remove(&old);
            }
        }
        Ok(())
    }

    fn get_path(&self, object_id: &OpID) -> Result<Vec<(OpID, &Key, OpID)>, AutomergeError> {
        let mut oid = object_id;
        let mut path = Vec::new();
        while let Some(inbound) = self.objs.get(oid).and_then(|o| o.inbound.iter().next()) {
            oid = inbound.object_id();
            path.insert(
                0,
                (
                    inbound.object_id().clone(),
                    inbound.key(),
                    inbound.opid.clone(),
                ),
            );
            /*
                        let ops = self
                            .objs
                            .get(inbound.object_id())
                            .and_then(|parent| parent.props.get(inbound.key()))
                            .unwrap(); // FIXME
            */
            //let tmp = (inbound.key(), ops, oid.clone());
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
                    let is_insert = op.is_insert();

                    let node = diff2.expand_path(&path, self);

                    if is_insert {
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

    /*
        fn get_list(&mut self, list_id: &OpID) -> Result<&mut ListState, AutomergeError> {
            let list = self.get_obj(list_id)?;
            match list {
                ObjectState::Map { .. } => Err(AutomergeError::InvalidChange(format!(
                    "Insert operation received for object (object ID: {:?}",
                    list_id
                ))),
                ObjectState::List(liststate) => Ok(liststate),
            }
        }
    */

    fn get_obj(&mut self, object_id: &OpID) -> Result<&mut ObjState, AutomergeError> {
        let object = self
            .objs
            .get_mut(&object_id)
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))?;
        Ok(object)
    }

    pub fn can_undo(&self) -> bool {
        self.undo_pos > 0
    }

    pub fn can_redo(&self) -> bool {
        !self.redo_stack.is_empty()
    }

    /*
        pub fn concurrent_operations_for_field(
            &self,
            object_id: &OpID,
            key: &Key,
        ) -> Result<&[OperationWithMetadata], AutomergeError> {
            Ok(self
                .objs
                .get(object_id)
                .and_then(|state| state.props.get(&key))
                .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))?)
        }
    */

    /// Get all the changes we have that are not in `since`
    pub fn get_missing_changes(&self, since: &Clock) -> Vec<&Change> {
        self.states
            .history
            .iter()
            .map(|rc| rc.as_ref())
            .filter(|change| change.seq > since.get(&change.actor_id))
            .collect()
    }

    pub fn get_missing_deps(&self) -> Clock {
        // TODO: there's a lot of internal copying going on in here for something kinda simple
        self.queue.iter().fold(Clock::empty(), |clock, change| {
            clock
                .union(&change.dependencies)
                .with(&change.actor_id, change.seq - 1)
        })
    }
}

/*
pub fn list_ops_in_order<'a, S: BuildHasher>(
    operations_by_elemid: &'a HashMap<ElementID, ConcurrentOperations, S>,
    following: &HashMap<ElementID, Vec<ElementID>, S>,
) -> Result<Vec<(ElementID, &'a ConcurrentOperations)>, AutomergeError> {
    // First we construct a vector of operations to process in order based
    // on the insertion orders of the operations we've received
    let mut ops_in_order: Vec<(ElementID, &ConcurrentOperations)> = Vec::new();
    // start with everything that was inserted after _head
    let mut to_process: Vec<ElementID> = following
        .get(&ElementID::Head)
        .map(|heads| {
            let mut sorted = heads.to_vec();
            sorted.sort();
            sorted
        })
        .unwrap_or_else(Vec::new);

    // for each element ID, add the operation to the ops_in_order list,
    // then find all the following element IDs, sort them and add them to
    // the list of element IDs still to process.
    while let Some(next_element_id) = to_process.pop() {
        let ops = operations_by_elemid.get(&next_element_id).ok_or_else(|| {
            AutomergeError::InvalidChange(format!(
                "Missing element ID {:?} when interpreting list ops",
                next_element_id
            ))
        })?;
        ops_in_order.push((next_element_id.clone(), ops));
        if let Some(followers) = following.get(&next_element_id) {
            let mut sorted = followers.to_vec();
            sorted.sort();
            to_process.extend(sorted);
        }
    }
    Ok(ops_in_order)
}
*/
