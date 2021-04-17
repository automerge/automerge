use std::ops::Deref;

use crate::{error::AutomergeError, internal::InternalOpType, op_handle::OpHandle};

/// Represents a set of operations which are relevant to either an element ID
/// or object ID and which occurred without knowledge of each other
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConcurrentOperations {
    pub ops: Vec<OpHandle>,
}

impl Deref for ConcurrentOperations {
    type Target = Vec<OpHandle>;

    fn deref(&self) -> &Self::Target {
        &self.ops
    }
}

impl Default for ConcurrentOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentOperations {
    pub fn new() -> ConcurrentOperations {
        ConcurrentOperations { ops: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Updates this set of operations based on a new operation.
    ///
    /// Returns the previous operations that this op
    /// replaces

    pub fn incorporate_new_op(
        &mut self,
        new_op: &OpHandle,
    ) -> Result<Vec<OpHandle>, AutomergeError> {
        let mut overwritten_ops = Vec::new();
        if new_op.is_inc() {
            self.ops
                .iter_mut()
                .for_each(|other| other.maybe_increment(new_op))
        } else {
            let mut i = 0;
            while i != self.ops.len() {
                if new_op.pred.contains(&self.ops[i].id) {
                    overwritten_ops.push(self.ops.swap_remove(i));
                } else {
                    i += 1;
                }
            }
        }

        match new_op.action {
            InternalOpType::Set(_) | InternalOpType::Make(_) => {
                self.ops.push(new_op.clone());
            }
            _ => {}
        }

        Ok(overwritten_ops)
    }
}
