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
    /// Returns the operation representing the update and the previous operations that this op
    /// replaces.
    /// This is to cover the case of increment operations actually being reflected as Sets on
    /// counters.
    pub fn incorporate_new_op(
        &mut self,
        new_op: OpHandle,
    ) -> Result<(OpHandle, Vec<OpHandle>), AutomergeError> {
        if new_op.is_inc() {
            for op in &mut self.ops {
                if op.maybe_increment(&new_op) {
                    return Ok((op.clone(), Vec::new()));
                }
            }
            Ok((new_op, Vec::new()))
        } else {
            let mut overwritten_ops = Vec::new();
            let mut i = 0;
            while i != self.ops.len() {
                if new_op.pred.contains(&self.ops[i].id) {
                    overwritten_ops.push(self.ops.swap_remove(i));
                } else {
                    i += 1;
                }
            }

            match new_op.action {
                InternalOpType::Set(_) | InternalOpType::Make(_) => {
                    self.ops.push(new_op.clone());
                }
                _ => {}
            }

            Ok((new_op, overwritten_ops))
        }
    }
}
