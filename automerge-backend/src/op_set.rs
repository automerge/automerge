//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::actor_states::ActorStates;
//use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::object_store::ObjState;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{Change, Clock, ElementID, OpID, Operation};
use crate::{ActorID, Diff2, Key, MapType, ObjType, SequenceType};
use core::cmp::max;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::BuildHasher;

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
            self.add_change(change, false, diff2)
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
                    Operation::Set {
                        object_id,
                        key,
                        pred,
                        ..
                    }
                    | Operation::Link {
                        object_id,
                        key,
                        pred,
                        ..
                    }
                    | Operation::Delete {
                        object_id,
                        key,
                        pred,
                        ..
                    } => panic!("not implemented"),
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
            self.add_change(change, false, diff2)
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
        diff2: &mut Diff2,
    ) -> Result<(), AutomergeError> {
        self.queue.push(change);
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, make_undoable, diff2)?;
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

        let mut all_undo_ops = Vec::new();
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

            self.apply_op(metaop, diff2)?;

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

    fn apply_link(&mut self, metaop: &OperationWithMetadata) -> Result<(), AutomergeError> {
        match metaop.operation {
            Operation::MakeMap { .. }
            | Operation::MakeTable { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::Link { .. } => {
                let opid = metaop.opid.clone();
                let obj = self
                    .objs
                    .get_mut(&opid)
                    .ok_or_else(|| AutomergeError::MissingObjectError(opid))?;
                obj.inbound.insert(metaop.clone());
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_make(&mut self, metaop: &OperationWithMetadata) {}

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
        diff2: &mut Diff2,
    ) -> Result<(), AutomergeError> {

        if let Some(obj_type) = op.make_type() {
            self.objs.insert(op.opid.clone(), ObjState::new(obj_type));
        }

        let object_id = op.object_id();

        let path = self.get_path(&object_id)?;

        let object = self.get_obj(&object_id)?;

        let key = match op.insert() {
          Some(elem) => {
            object.insert_after(elem, op.clone())?;
            Key(op.opid.to_string())
          },
          None => op.key().clone(),
        };

        let ops = object.props.entry(key.clone()).or_default();

        let overwritten_ops = ops.incorporate_new_op(&op)?;

        diff2.op(&key, &path, &ops.ops);

        if let Some(child) = op.child() {
          self.get_obj(&child)?.inbound.insert(op.clone());
        }

        for old in overwritten_ops.iter() {
            if let Some(child) = old.child() {
                self.get_obj(&child)?.inbound.remove(&old);
            }
        }

        Ok(())
    }

    fn get_index_for_oid(&self, object: &ObjState, target: &OpID) -> Option<usize> {
        object.ops_in_order().filter(|oid| {
            object.props
                .get(&Key(oid.to_string()))
                .map(|ops| !ops.is_empty())
                            .unwrap_or(false)
        }).position(|o| target == o)
    }

    fn get_path(&self, object_id: &OpID) -> Result<Vec<OperationWithMetadata>, AutomergeError> {
        let mut oid = object_id;
        let mut path = Vec::new();
        while let Some(metaop) = self.objs.get(oid).and_then(|os| os.inbound.iter().next()) {
            oid = metaop.object_id();
            path.insert(0, metaop.clone());
        }
        Ok(path)
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
