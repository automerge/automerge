use crate::patch::Diff;
use crate::actor_histories::ActorHistories;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::patch::{Conflict, ElementValue};
use crate::{DataType, Operation, PrimitiveValue};
use std::cmp::PartialOrd;

/// Represents a set of operations which are relevant to either an element ID
/// or object ID and which occurred without knowledge of each other
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrentOperations {
    operations: Vec<OperationWithMetadata>,
}

impl ConcurrentOperations {
    pub(crate) fn new() -> ConcurrentOperations {
        ConcurrentOperations {
            operations: Vec::new(),
        }
    }

    pub fn active_op(&self) -> Option<&OperationWithMetadata> {
        // operations are sorted in incorporate_new_op, so the first op is the
        // active one
        self.operations.first()
    }

    pub fn conflicts(&self) -> Vec<Conflict> {
        self.operations
            .split_first()
            .map(|(_, tail)| {
                tail.iter()
                    .map(|op| match &op.operation {
                        Operation::Set {
                            value, datatype, ..
                        } => Conflict {
                            actor: op.actor_id.clone(),
                            value: ElementValue::Primitive(value.clone()),
                            datatype: datatype.clone(),
                        },
                        Operation::Link { value, .. } => Conflict {
                            actor: op.actor_id.clone(),
                            value: ElementValue::Link(value.clone()),
                            datatype: None,
                        },
                        _ => panic!("Invalid operation in concurrent ops"),
                    })
                    .collect()
            })
            .unwrap_or_else(|| Vec::new())
    }

    /// Updates this set of operations based on a new operation. 
    pub(crate) fn incorporate_new_op(
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
