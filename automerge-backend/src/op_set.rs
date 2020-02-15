//! The OpSet is where most of the interesting work is done in this library.
//! It maintains a mapping from each object ID to a set of concurrent
//! operations which have been seen for that object ID.
//!
//! When the client requests the value of the CRDT (via
//! document::state) the implementation fetches the root object ID's history
//! and then recursively walks through the tree of histories constructing the
//! state. Obviously this is not very efficient.
use crate::actor_histories::ActorHistories;
use crate::concurrent_operations::ConcurrentOperations;
use crate::error::AutomergeError;
use crate::object_store::{ObjectHistory, ObjectStore};
use crate::operation_with_metadata::OperationWithMetadata;
use crate::protocol::{Change, Clock, ElementID, Key, ObjectID, Operation, PrimitiveValue};
use crate::value::Value;
use crate::Diff;
use std::collections::HashMap;

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
    pub object_store: ObjectStore,
    pub actor_histories: ActorHistories,
    queue: Vec<Change>,
    pub clock: Clock,
    state: Value,
}

impl OpSet {
    pub fn init() -> OpSet {
        OpSet {
            object_store: ObjectStore::new(),
            actor_histories: ActorHistories::new(),
            queue: Vec::new(),
            clock: Clock::empty(),
            state: Value::Map(HashMap::new()),
        }
    }

    /// Adds a change to the internal queue of operations, then iteratively
    /// applies all causally ready changes until there are none remaining
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
            let op_with_metadata = OperationWithMetadata {
                sequence: seq,
                actor_id: actor_id.clone(),
                operation: operation.clone(),
            };
            self.object_store
                .apply_operation(&self.actor_histories, op_with_metadata)?;
        }
        self.clock = self
            .clock
            .with_dependency(&change.actor_id.clone(), change.seq);
        Ok(())
    }

    pub fn root_value(&self) -> &Value {
        return &self.state;
    }

    /// This is where we actually interpret the concurrent operations for each
    /// part of the object and construct the current state.
    fn walk(&self, object_id: &ObjectID) -> Result<Value, AutomergeError> {
        let object_history = self
            .object_store
            .history_for_object_id(object_id)
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
        let ops_in_order = list_ops_in_order(operations_by_elemid, following)?;

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
}

pub fn list_ops_in_order<'a>(
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
