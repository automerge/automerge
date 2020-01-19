//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::change_request::{ListIndex, Path, PathElement};
use crate::error::{AutomergeError, InvalidChangeRequest};
use crate::protocol::{
    ActorID, Change, Clock, DataType, ElementID, Key, ObjectID, Operation, PrimitiveValue,
};
use crate::value::Value;
use std::cmp::{Ordering, PartialOrd};
use std::collections::HashMap;
use std::convert::TryInto;
use std::str::FromStr;

/// We deserialize individual operations as part of the `Change` structure, but
/// we need access to the actor ID and sequence when applying each individual
/// operation, so we copy the operation, actor ID, and sequence into this
/// struct.
#[derive(PartialEq, Debug, Clone)]
pub struct OperationWithMetadata {
    sequence: u32,
    actor_id: ActorID,
    operation: Operation,
}

/// Note, we can't implement Ord because the Operation contains floating point
/// elements
impl PartialOrd for OperationWithMetadata {
    fn partial_cmp(&self, other: &OperationWithMetadata) -> Option<Ordering> {
        if self.sequence == other.sequence {
            Some(self.actor_id.cmp(&other.actor_id))
        } else {
            Some(self.sequence.cmp(&other.sequence))
        }
    }
}

/// Represents a set of operations which are relevant to either an element ID
/// or object ID and which occurred without knowledge of each other
#[derive(Debug)]
struct ConcurrentOperations {
    operations: Vec<OperationWithMetadata>,
}

impl ConcurrentOperations {
    fn new() -> ConcurrentOperations {
        ConcurrentOperations {
            operations: Vec::new(),
        }
    }

    fn active_op(&self) -> Option<&OperationWithMetadata> {
        // operations are sorted in incorporate_new_op, so the first op is the
        // active one
        self.operations.first()
    }

    fn incorporate_new_op(
        &mut self,
        new_op: OperationWithMetadata,
        actor_histories: &ActorHistories,
    ) -> Result<(), AutomergeError> {
        let mut concurrent: Vec<OperationWithMetadata> = match new_op.operation {
            // If the operation is an increment op, then we are going to modify
            // any Set operations to reflect the increment ops in the next
            // part of this function
            Operation::Increment { .. } => self.operations.clone(),
            // Otherwise we filter out any operations that are not concurrent
            // with the new one (i.e ones which causally precede the new one)
            _ => self
                .operations
                .iter()
                .filter(|op| actor_histories.are_concurrent(op, &new_op))
                .cloned()
                .collect(),
        };
        let this_op = new_op.clone();
        match &new_op.operation {
            // For Set or Link ops, we add them to the concurrent ops list, to
            // be interpreted later as part of the document::walk
            // implementation
            Operation::Set { .. } | Operation::Link { .. } => {
                concurrent.push(this_op);
            }
            // Increment ops are not stored in the op set, instead we update
            // any Set operations which are a counter containing a number to
            // reflect the increment operation
            Operation::Increment {
                value: inc_value, ..
            } => concurrent.iter_mut().for_each(|op| {
                if let Operation::Set {
                    value: PrimitiveValue::Number(ref mut n),
                    datatype: Some(DataType::Counter),
                    ..
                } = op.operation
                {
                    *n += inc_value
                }
            }),
            // All other operations are not relevant (e.g a concurrent
            // operation set containing just a delete operation actually is an
            // empty set, in document::walk we interpret this into a
            // nonexistent part of the state)
            _ => {}
        }
        // the partial_cmp implementation for `OperationWithMetadata` ensures
        // that the operations are in the deterministic order required by
        // automerge.
        //
        // Note we can unwrap because the partial_cmp definition never returns
        // None
        concurrent.sort_by(|a, b| a.partial_cmp(b).unwrap());
        concurrent.reverse();
        self.operations = concurrent;
        Ok(())
    }
}

/// ObjectHistory is what the OpSet uses to store operations for a particular
/// key, they represent the two possible container types in automerge, a map or
/// a sequence (tables and text are effectively the maps and sequences
/// respectively).
#[derive(Debug)]
enum ObjectHistory {
    Map {
        operations_by_key: HashMap<String, ConcurrentOperations>,
    },
    List {
        operations_by_elemid: HashMap<ElementID, ConcurrentOperations>,
        insertions: HashMap<ElementID, ElementID>,
        following: HashMap<ElementID, Vec<ElementID>>,
        max_elem: u32,
    },
}

/// ActorHistories is a cache for the transitive dependencies of each change
/// received from each actor. This is necessary because a change only ships its
/// direct dependencies in `deps` but we need all dependencies to determine
/// whether two operations occurrred concurrently.
#[derive(Debug)]
pub struct ActorHistories(HashMap<ActorID, HashMap<u32, Clock>>);

impl ActorHistories {
    /// Return the latest sequence required by `op` for actor `actor`
    fn dependency_for(&self, op: &OperationWithMetadata, actor: &ActorID) -> u32 {
        self.0
            .get(&op.actor_id)
            .and_then(|clocks| clocks.get(&op.sequence))
            .map(|c| c.seq_for(actor))
            .unwrap_or(0)
    }

    /// Whether or not `change` is already part of this `ActorHistories`
    fn is_applied(&self, change: &Change) -> bool {
        self.0
            .get(&change.actor_id)
            .and_then(|clocks| clocks.get(&change.seq))
            .map(|c| c.seq_for(&change.actor_id) >= change.seq)
            .unwrap_or(false)
    }

    /// Update this ActorHistories to include the changes in `change`
    fn add_change(&mut self, change: &Change) {
        let change_deps = change
            .dependencies
            .with_dependency(&change.actor_id, change.seq - 1);
        let transitive = self.transitive_dependencies(&change.actor_id, change.seq);
        let all_deps = transitive.upper_bound(&change_deps);
        let state = self
            .0
            .entry(change.actor_id.clone())
            .or_insert_with(HashMap::new);
        state.insert(change.seq, all_deps);
    }

    fn transitive_dependencies(&mut self, actor_id: &ActorID, seq: u32) -> Clock {
        self.0
            .get(actor_id)
            .and_then(|deps| deps.get(&seq))
            .cloned()
            .unwrap_or_else(Clock::empty)
    }

    /// Whether the two operations in question are concurrent
    fn are_concurrent(&self, op1: &OperationWithMetadata, op2: &OperationWithMetadata) -> bool {
        if op1.sequence == op2.sequence && op1.actor_id == op2.actor_id {
            return false;
        }
        self.dependency_for(op1, &op2.actor_id) < op2.sequence
            && self.dependency_for(op2, &op1.actor_id) < op1.sequence
    }
}

/// The core logic of the whole libary. Combines operations and allows querying
/// the current state.
///
/// Whenever a new change is received we iterate through any causally ready
/// changes in the queue and apply them, then repeat until there are no
/// causally ready changes left. The end result of this is that
/// `operations_by_object_id` will contain sets of concurrent operations
/// for each object ID or element ID.
///
/// When we want to get the state of the CRDT we walk through the
/// `operations_by_object_id` map, starting with the root object ID and
/// constructing the value at each node by examining the concurrent operations
/// which are active for that node.
#[derive(Debug)]
pub struct OpSet {
    operations_by_object_id: HashMap<ObjectID, ObjectHistory>,
    actor_histories: ActorHistories,
    queue: Vec<Change>,
    pub clock: Clock,
    state: Value,
}

impl OpSet {
    pub fn init() -> OpSet {
        let root = ObjectHistory::Map {
            operations_by_key: HashMap::new(),
        };
        let mut ops_by_id = HashMap::new();
        ops_by_id.insert(ObjectID::Root, root);
        OpSet {
            operations_by_object_id: ops_by_id,
            actor_histories: ActorHistories(HashMap::new()),
            queue: Vec::new(),
            clock: Clock::empty(),
            state: Value::Map(HashMap::new()),
        }
    }

    pub fn apply_change(&mut self, change: Change) -> Result<(), AutomergeError> {
        self.queue.push(change);
        self.apply_causally_ready_changes()?;
        self.state = self.walk(&ObjectID::Root)?;
        Ok(())
    }

    fn apply_causally_ready_changes(&mut self) -> Result<(), AutomergeError> {
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_causally_ready_change(next_change)?
        }
        Ok(())
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            let deps = change
                .dependencies
                .with_dependency(&change.actor_id, change.seq - 1);
            if self.clock.is_before_or_concurrent_with(&deps) {
                return Some(self.queue.remove(index));
            }
            index += 1
        }
        None
    }

    fn apply_causally_ready_change(&mut self, change: Change) -> Result<(), AutomergeError> {
        if self.actor_histories.is_applied(&change) {
            return Ok(());
        }
        self.actor_histories.add_change(&change);
        let actor_id = change.actor_id.clone();
        let seq = change.seq;
        for operation in change.operations {
            let operation_copy = operation.clone();
            let op_with_metadata = OperationWithMetadata {
                sequence: seq,
                actor_id: actor_id.clone(),
                operation: operation_copy,
            };
            match operation {
                Operation::MakeMap { object_id } | Operation::MakeTable { object_id } => {
                    let object = ObjectHistory::Map {
                        operations_by_key: HashMap::new(),
                    };
                    self.operations_by_object_id.insert(object_id, object);
                }
                Operation::MakeList { object_id } | Operation::MakeText { object_id } => {
                    let object = ObjectHistory::List {
                        operations_by_elemid: HashMap::new(),
                        insertions: HashMap::new(),
                        following: HashMap::new(),
                        max_elem: 0,
                    };
                    self.operations_by_object_id.insert(object_id, object);
                }
                Operation::Link { object_id, key, .. }
                | Operation::Delete { object_id, key }
                | Operation::Set { object_id, key, .. }
                | Operation::Increment { object_id, key, .. } => {
                    let object = self
                        .operations_by_object_id
                        .get_mut(&object_id)
                        .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))?;
                    let prior_ops = match object {
                        ObjectHistory::Map {
                            ref mut operations_by_key,
                        } => operations_by_key
                            .entry(key.0.clone())
                            .or_insert_with(ConcurrentOperations::new),
                        ObjectHistory::List {
                            ref mut operations_by_elemid,
                            ..
                        } => {
                            let elem_id = ElementID::from_str(&key.0).map_err(|_| AutomergeError::InvalidChange(format!("Attempted to link, set, delete, or increment an object in a list with invalid element ID {:?}", key.0)))?;
                            operations_by_elemid
                                .entry(elem_id.clone())
                                .or_insert_with(ConcurrentOperations::new)
                        }
                    };
                    prior_ops.incorporate_new_op(op_with_metadata, &self.actor_histories)?;
                }
                Operation::Insert {
                    ref list_id,
                    ref key,
                    ref elem,
                } => {
                    let list = self
                        .operations_by_object_id
                        .get_mut(&list_id)
                        .ok_or_else(|| AutomergeError::MissingObjectError(list_id.clone()))?;
                    match list {
                        ObjectHistory::Map{..} => return Err(AutomergeError::InvalidChange(format!("Insert operation received for object key (object ID: {:?}, key: {:?}", list_id, key))),
                        ObjectHistory::List{insertions, following, operations_by_elemid, max_elem} => {
                            let inserted_elemid = ElementID::SpecificElementID(actor_id.clone(), *elem);
                            if insertions.contains_key(&inserted_elemid) {
                                return Err(AutomergeError::InvalidChange(format!("Received an insertion for already present key: {:?}", inserted_elemid)));
                            }
                            insertions.insert(inserted_elemid.clone(), inserted_elemid.clone());
                            let following_ops = following.entry(key.clone()).or_insert_with(Vec::new);
                            following_ops.push(inserted_elemid.clone());

                            operations_by_elemid.entry(inserted_elemid).or_insert_with(ConcurrentOperations::new);
                            *max_elem = std::cmp::max(*max_elem, *elem);
                        }
                    }
                }
            }
        }
        self.clock = self
            .clock
            .with_dependency(&change.actor_id.clone(), change.seq);
        Ok(())
    }

    pub fn root_value(&self) -> &Value {
        return &self.state
    }

    /// This is where we actually interpret the concurrent operations for each
    /// part of the object and construct the value.
    fn walk(&self, object_id: &ObjectID) -> Result<Value, AutomergeError> {
        let object_history = self
            .operations_by_object_id
            .get(object_id)
            .ok_or_else(|| AutomergeError::MissingObjectError(object_id.clone()))?;
        match object_history {
            ObjectHistory::Map { operations_by_key } => self.interpret_map_ops(operations_by_key),
            ObjectHistory::List {
                operations_by_elemid,
                insertions,
                following,
                ..
            } => self.interpret_list_ops(operations_by_elemid, insertions, following),
        }
    }

    fn interpret_map_ops(
        &self,
        ops_by_key: &HashMap<String, ConcurrentOperations>,
    ) -> Result<Value, AutomergeError> {
        let mut result: HashMap<String, Value> = HashMap::new();
        for (_, ops) in ops_by_key.iter() {
            match ops.active_op() {
                None => {}
                Some(OperationWithMetadata { operation, .. }) => match operation {
                    Operation::Set {
                        key: Key(str_key),
                        value,
                        ..
                    } => {
                        result.insert(
                            str_key.to_string(),
                            match value {
                                PrimitiveValue::Null => Value::Null,
                                PrimitiveValue::Boolean(b) => Value::Boolean(*b),
                                PrimitiveValue::Number(n) => Value::Number(*n),
                                PrimitiveValue::Str(s) => Value::Str(s.to_string()),
                            },
                        );
                    }
                    Operation::Link {
                        key: Key(str_key),
                        value,
                        ..
                    } => {
                        let linked_value = self.walk(value)?;
                        result.insert(str_key.to_string(), linked_value);
                    }
                    Operation::Increment { .. } => {}
                    op => {
                        return Err(AutomergeError::NotImplemented(format!(
                            "Interpret operation not implemented: {:?}",
                            op
                        )))
                    }
                },
            }
        }
        Ok(Value::Map(result))
    }

    fn interpret_list_ops(
        &self,
        operations_by_elemid: &HashMap<ElementID, ConcurrentOperations>,
        _insertions: &HashMap<ElementID, ElementID>,
        following: &HashMap<ElementID, Vec<ElementID>>,
    ) -> Result<Value, AutomergeError> {
        let ops_in_order = self.list_ops_in_order(operations_by_elemid, following)?;

        // Now that we have a list of `ConcurrentOperations` in the correct
        // order, we need to interpret each one to construct the value that
        // should appear at that position in the resulting sequence.
        let result_with_errs =
            ops_in_order
                .iter()
                .filter_map(|(_, ops)| -> Option<Result<Value, AutomergeError>> {
                    ops.active_op().map(|op| match &op.operation {
                        Operation::Set { value, .. } => Ok(match value {
                            PrimitiveValue::Null => Value::Null,
                            PrimitiveValue::Boolean(b) => Value::Boolean(*b),
                            PrimitiveValue::Number(n) => Value::Number(*n),
                            PrimitiveValue::Str(s) => Value::Str(s.to_string()),
                        }),
                        Operation::Link { value, .. } => self.walk(&value),
                        op => Err(AutomergeError::NotImplemented(format!(
                            "Interpret operation not implemented for list ops: {:?}",
                            op
                        ))),
                    })
                });

        let result = result_with_errs.collect::<Result<Vec<Value>, AutomergeError>>()?;

        Ok(Value::List(result))
    }

    fn list_ops_in_order<'a>(
        &'a self,
        operations_by_elemid: &'a HashMap<ElementID, ConcurrentOperations>,
        following: &HashMap<ElementID, Vec<ElementID>>,
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
                sorted.reverse();
                for follower in sorted {
                    to_process.push(follower.clone())
                }
            }
        }
        Ok(ops_in_order)
    }

    pub(crate) fn create_set_operations(
        &self,
        actor_id: &ActorID,
        path: &Path,
        value: &Value,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        // If we're setting a map as the root object we actually want to set
        // each key of the map to the corresponding key in the root object
        if let Value::Map(kvs) = value.clone() {
            if path.is_root() {
                let mut ops = Vec::new();
                for (key, value) in kvs.into_iter() {
                    let key_path = path.key(key);
                    let mut this_key_ops =
                        self.create_set_operations(actor_id, &key_path, &value)?;
                    ops.append(&mut this_key_ops)
                }
                return Ok(ops);
            }
        };
        self.resolve_path(path)
            .and_then(|r| r.as_set_target())
            .map(|path_resolution| match value {
                Value::Map { .. } | Value::List { .. } => {
                    let (new_object_id, mut create_ops) = value_to_ops(actor_id, &value);
                    let link_op = Operation::Link {
                        object_id: path_resolution.containing_object_id.clone(),
                        key: path_resolution.key.clone(),
                        value: new_object_id,
                    };
                    create_ops.push(link_op);
                    create_ops
                }
                Value::Str { .. } | Value::Number { .. } | Value::Boolean { .. } | Value::Null => {
                    vec![create_prim(
                        path_resolution.containing_object_id.clone(),
                        path_resolution.key,
                        &value,
                    )]
                }
            })
            .ok_or(InvalidChangeRequest(format!("Missing path: {:?}", path)))
    }

    pub(crate) fn create_move_operations(
        &self,
        from: &Path,
        to: &Path,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        let resolved_from = self.resolve_path(from).ok_or(InvalidChangeRequest(format!(
            "Missing from path: {:?}",
            from
        )))?;
        let resolved_to = self
            .resolve_path(to)
            .ok_or(InvalidChangeRequest(format!("Missing to path: {:?}", to)))?;

        let move_source = resolved_from
            .as_move_source()
            .ok_or(InvalidChangeRequest(format!(
                "Invalid move source path: {:?}",
                from
            )))?;
        let target = resolved_to
            .as_set_target()
            .ok_or(InvalidChangeRequest(format!("Invalid to path: {:?}", to)))?;

        let delete_op = move_source.delete_op();

        let insert_op = match (move_source, target) {
            (
                MoveSource::Value { value: v, .. },
                SetTarget {
                    containing_object_id,
                    key,
                },
            ) => Operation::Set {
                object_id: containing_object_id,
                key,
                value: v,
                datatype: None,
            },
            (
                MoveSource::Reference {
                    contained_object_id,
                    ..
                },
                SetTarget {
                    containing_object_id: target_container_id,
                    key: target_key,
                },
            ) => Operation::Link {
                object_id: target_container_id,
                key: target_key,
                value: contained_object_id,
            },
        };

        Ok(vec![delete_op, insert_op])
    }

    pub(crate) fn create_delete_operation(
        &self,
        path: &Path,
    ) -> Result<Operation, InvalidChangeRequest> {
        self.resolve_path(path)
            .and_then(|r| r.as_move_source())
            .map(|source| source.delete_op())
            .ok_or(InvalidChangeRequest(format!(
                "Invalid delete path: {:?}",
                path
            )))
    }

    pub(crate) fn create_increment_operation(
        &self,
        _path: &Path,
        _value: f64,
    ) -> Result<Operation, InvalidChangeRequest> {
        Err(InvalidChangeRequest(
            "create_increment_operation not implemented".to_string(),
        ))
    }

    pub(crate) fn create_insert_operation(
        &self,
        actor_id: &ActorID,
        after: &Path,
        value: &Value,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        let after_target = self
            .resolve_path(after)
            .and_then(|p| p.as_insert_after_target())
            .ok_or(InvalidChangeRequest(format!(
                "Invalid insert after path: {:?}",
                after
            )))?;
        let next_elem_id =
            ElementID::SpecificElementID(actor_id.clone(), after_target.max_elem + 1);
        let insert_op = Operation::Insert {
            list_id: after_target.list_id.clone(),
            key: after_target.element_id,
            elem: after_target.max_elem + 1,
        };
        let mut ops = vec![insert_op];
        match value {
            Value::Map { .. } | Value::List { .. } => {
                let (new_object_id, create_ops) = value_to_ops(actor_id, &value);
                ops.extend(create_ops);
                let link_op = Operation::Link {
                    object_id: after_target.list_id.clone(),
                    key: next_elem_id.as_key(),
                    value: new_object_id,
                };
                ops.push(link_op);
            }
            Value::Str { .. } | Value::Number { .. } | Value::Boolean { .. } | Value::Null => {
                ops.push(create_prim(
                    after_target.list_id.clone(),
                    next_elem_id.as_key(),
                    &value,
                ));
            }
        };
        Ok(ops)
    }

    fn resolve_path(&self, path: &Path) -> Option<ResolvedPath> {
        let mut resolved_elements: Vec<ResolvedPathElement> = Vec::new();
        let mut containing_object_id = ObjectID::Root;
        for next_elem in path {
            match resolved_elements.last() {
                Some(ResolvedPathElement::MissingKey(_)) => return None,
                Some(ResolvedPathElement::Index(ElementID::Head)) => return None,
                _ => {}
            }
            match next_elem {
                PathElement::Root => {
                    resolved_elements.push(ResolvedPathElement::Map(ObjectID::Root))
                }
                PathElement::Key(key) => {
                    resolved_elements.push(ResolvedPathElement::Key(Key(key.to_string())));
                    let op = self
                        .operations_by_object_id
                        .get(&containing_object_id)
                        .and_then(|history| match history {
                            ObjectHistory::Map { operations_by_key } => Some(operations_by_key),
                            ObjectHistory::List { .. } => None,
                        })
                        .and_then(|kvs| kvs.get(key))
                        .and_then(|cops| cops.active_op())
                        .map(|o| o.operation.clone());
                    match op {
                        Some(Operation::Set { value, .. }) => {
                            resolved_elements.push(ResolvedPathElement::Value(value))
                        }
                        Some(Operation::Link { value, .. }) => {
                            match self.operations_by_object_id.get(&value) {
                                None => return None,
                                Some(ObjectHistory::Map { .. }) => {
                                    resolved_elements.push(ResolvedPathElement::Map(value.clone()));
                                    containing_object_id = value.clone()
                                }
                                Some(ObjectHistory::List { max_elem, .. }) => {
                                    resolved_elements
                                        .push(ResolvedPathElement::List(value.clone(), *max_elem));
                                    containing_object_id = value.clone()
                                }
                            }
                        }
                        None => resolved_elements
                            .push(ResolvedPathElement::MissingKey(Key(key.to_string()))),
                        _ => return None,
                    }
                }
                PathElement::Index(index) => match index {
                    ListIndex::Head => {
                        match self.operations_by_object_id.get(&containing_object_id) {
                            Some(ObjectHistory::List { .. }) => {
                                resolved_elements.push(ResolvedPathElement::Index(ElementID::Head))
                            }
                            _ => return None,
                        };
                    }
                    ListIndex::Index(i) => {
                        let op = self
                            .operations_by_object_id
                            .get(&containing_object_id)
                            .and_then(|history| match history {
                                ObjectHistory::List {
                                    operations_by_elemid,
                                    following,
                                    ..
                                } => self.list_ops_in_order(operations_by_elemid, following).ok(),
                                ObjectHistory::Map { .. } => None,
                            })
                            .and_then(|ops| ops.get(*i).map(|o| o.clone()))
                            .and_then(|(element_id, cops)| {
                                cops.active_op().map(|o| (element_id, o.operation.clone()))
                            });
                        match op {
                            Some((elem_id, Operation::Set { value, .. })) => {
                                resolved_elements.push(ResolvedPathElement::Index(elem_id));
                                resolved_elements.push(ResolvedPathElement::Value(value));
                            }
                            Some((_, Operation::Link { value, .. })) => {
                                match self.operations_by_object_id.get(&value) {
                                    None => return None,
                                    Some(ObjectHistory::Map { .. }) => {
                                        resolved_elements
                                            .push(ResolvedPathElement::Map(value.clone()));
                                        containing_object_id = value
                                    }
                                    Some(ObjectHistory::List { max_elem, .. }) => {
                                        resolved_elements.push(ResolvedPathElement::List(
                                            value.clone(),
                                            *max_elem,
                                        ));
                                        containing_object_id = value
                                    }
                                }
                            }
                            _ => return None,
                        }
                    }
                },
            }
        }
        Some(ResolvedPath::new(resolved_elements))
    }
}

#[derive(Clone, Debug)]
enum ResolvedPathElement {
    Map(ObjectID),
    List(ObjectID, u32),
    Key(Key),
    Index(ElementID),
    Value(PrimitiveValue),
    MissingKey(Key),
}

#[derive(Debug, Clone)]
struct ResolvedPath(Vec<ResolvedPathElement>);

#[derive(Debug, Clone)]
struct SetTarget {
    containing_object_id: ObjectID,
    key: Key,
}

enum MoveSource {
    Reference {
        containing_object_id: ObjectID,
        key: Key,
        contained_object_id: ObjectID,
    },
    Value {
        containing_object_id: ObjectID,
        key: Key,
        value: PrimitiveValue,
    },
}

impl MoveSource {
    fn delete_op(&self) -> Operation {
        match self {
            MoveSource::Reference {
                containing_object_id,
                key,
                ..
            }
            | MoveSource::Value {
                containing_object_id,
                key,
                ..
            } => Operation::Delete {
                object_id: containing_object_id.clone(),
                key: key.clone(),
            },
        }
    }
}

#[derive(Debug)]
struct InsertAfterTarget {
    list_id: ObjectID,
    element_id: ElementID,
    max_elem: u32,
}

impl ResolvedPath {
    fn new(elements: Vec<ResolvedPathElement>) -> ResolvedPath {
        ResolvedPath(elements)
    }

    fn as_set_target(&self) -> Option<SetTarget> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Value(_)] => Some(SetTarget{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::MissingKey(_)] => Some(SetTarget{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::Value(_)] => Some(SetTarget{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                }),
                _ => None
            }
        })
    }

    fn as_move_source(&self) -> Option<MoveSource> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Map(c)] => Some(MoveSource::Reference{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                    contained_object_id: c.clone()
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::List(l, _)] => Some(MoveSource::Reference{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                    contained_object_id: l.clone()
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Value(v)] => Some(MoveSource::Value{
                    containing_object_id: o.clone(),
                    value: v.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::Map(m)] => Some(MoveSource::Reference{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                    contained_object_id: m.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::List(l2, _)] => Some(MoveSource::Reference{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                    contained_object_id: l2.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(i), ResolvedPathElement::Value(v)] => Some(MoveSource::Value{
                    containing_object_id: l.clone(),
                    value: v.clone(),
                    key: i.as_key(),
                }),
                _ => None
            }
        })
    }

    fn as_insert_after_target(&self) -> Option<InsertAfterTarget> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::List(l, m), ResolvedPathElement::Index(e), ResolvedPathElement::Value(_)] => Some(InsertAfterTarget{
                    list_id: l.clone(),
                    element_id: e.clone(),
                    max_elem: *m,
                }),
                [_, ResolvedPathElement::List(l, m), ResolvedPathElement::Index(e)] => Some(InsertAfterTarget{
                    list_id: l.clone(),
                    element_id: e.clone(),
                    max_elem: *m,
                }),
                _ => None,
            }
        })
    }

    fn last_n(&self, n: usize) -> Option<Box<[ResolvedPathElement]>> {
        if self.0.len() < n {
            None
        } else {
            Some(
                self.0
                    .iter()
                    .skip(self.0.len() - n)
                    .cloned()
                    .collect::<Vec<ResolvedPathElement>>()
                    .into_boxed_slice(),
            )
        }
    }
}

fn value_to_ops(actor_id: &ActorID, v: &Value) -> (ObjectID, Vec<Operation>) {
    match v {
        Value::List(vs) => {
            let list_id = ObjectID::ID(uuid::Uuid::new_v4().to_string());
            let mut ops = vec![Operation::MakeList {
                object_id: list_id.clone(),
            }];
            let mut elem_ops: Vec<Operation> = vs
                .into_iter()
                .enumerate()
                .map(|(index, elem_value)| {
                    let elem: u32 = (index + 1).try_into().unwrap();
                    let previous_elemid = match index {
                        0 => ElementID::Head,
                        _ => ElementID::SpecificElementID(actor_id.clone(), elem - 1),
                    };
                    let insert_op = Operation::Insert {
                        list_id: list_id.clone(),
                        elem,
                        key: previous_elemid,
                    };
                    let elem_id = ElementID::SpecificElementID(actor_id.clone(), elem);
                    let mut elem_value_ops: Vec<Operation> = match elem_value {
                        Value::Boolean { .. }
                        | Value::Str { .. }
                        | Value::Number { .. }
                        | Value::Null { .. } => {
                            vec![create_prim(list_id.clone(), elem_id.as_key(), elem_value)]
                        }
                        Value::Map { .. } | Value::List { .. } => {
                            let (linked_object_id, mut value_ops) =
                                value_to_ops(actor_id, elem_value);
                            value_ops.push(Operation::Link {
                                object_id: list_id.clone(),
                                key: elem_id.as_key(),
                                value: linked_object_id,
                            });
                            value_ops
                        }
                    };
                    let mut result = Vec::new();
                    result.push(insert_op);
                    result.append(&mut elem_value_ops);
                    result
                })
                .flatten()
                .collect();
            ops.append(&mut elem_ops);
            (list_id, ops)
        }
        Value::Map(kvs) => {
            let object_id = ObjectID::ID(uuid::Uuid::new_v4().to_string());
            let mut ops = vec![Operation::MakeMap {
                object_id: object_id.clone(),
            }];
            let mut key_ops: Vec<Operation> = kvs
                .iter()
                .map(|(k, v)| match v {
                    Value::Boolean { .. }
                    | Value::Str { .. }
                    | Value::Number { .. }
                    | Value::Null { .. } => vec![create_prim(object_id.clone(), Key(k.clone()), v)],
                    Value::Map { .. } | Value::List { .. } => {
                        let (linked_object_id, mut value_ops) = value_to_ops(actor_id, v);
                        value_ops.push(Operation::Link {
                            object_id: object_id.clone(),
                            key: Key(k.clone()),
                            value: linked_object_id,
                        });
                        value_ops
                    }
                })
                .flatten()
                .collect();
            ops.append(&mut key_ops);
            (object_id, ops)
        }
        _ => panic!("Only a map or list can be the top level object in value_to_ops".to_string()),
    }
}

fn create_prim(object_id: ObjectID, key: Key, value: &Value) -> Operation {
    let prim_value = match value {
        Value::Number(n) => PrimitiveValue::Number(*n),
        Value::Boolean(b) => PrimitiveValue::Boolean(*b),
        Value::Str(s) => PrimitiveValue::Str(s.to_string()),
        Value::Null => PrimitiveValue::Null,
        _ => panic!("Non primitive value passed to create_prim"),
    };
    Operation::Set {
        object_id,
        key,
        value: prim_value,
        datatype: None,
    }
}
