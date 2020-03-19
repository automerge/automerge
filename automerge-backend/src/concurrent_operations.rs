use crate::actor_states::ActorStates;
use crate::error::AutomergeError;
use crate::operation_with_metadata::OperationWithMetadata;
use crate::patch::{Conflict, ElementValue};
use crate::{DataType, Operation, PrimitiveValue};
use std::cmp::PartialOrd;

/// Represents a set of operations which are relevant to either an element ID
/// or object ID and which occurred without knowledge of each other
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrentOperations {
    pub ops: Vec<OperationWithMetadata>,
}

impl Default for ConcurrentOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentOperations {
    pub(crate) fn new() -> ConcurrentOperations {
        ConcurrentOperations {
            ops: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
      self.ops.is_empty()
    }

/*
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
            .unwrap_or_default()
    }

*/
    /// Updates this set of operations based on a new operation.
    ///
    /// Returns the previous operations that this op
    /// replaces

    pub(crate) fn incorporate_new_op(
        &mut self,
        new_op: &OperationWithMetadata,
    ) -> Result<Vec<OperationWithMetadata>, AutomergeError> {
        let mut overwritten_ops = Vec::new();
        if new_op.is_inc() {
            self.ops
                .iter_mut()
                .for_each(|other| other.maybe_increment(new_op))
        } else {
            let mut i = 0;
            while i != self.ops.len() {
                if new_op.pred().contains(&self.ops[i].opid.clone()) {
                    overwritten_ops.push(self.ops.swap_remove(i));
                } else {
                    i += 1;
                }
            }
        }

        if let Operation::Set { .. } = new_op.operation {
            self.ops.push(new_op.clone());
        }

        Ok(overwritten_ops)
    }
}
