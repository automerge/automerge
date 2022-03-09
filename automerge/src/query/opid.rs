use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Op, OpId};

/// Search for an OpId in a tree.
/// Returns the index of the operation in the tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdSearch {
    target: OpId,
    pos: usize,
    found: bool,
}

impl OpIdSearch {
    pub fn new(target: OpId) -> Self {
        OpIdSearch {
            target,
            pos: 0,
            found: false,
        }
    }

    /// Get the index of the operation, if found.
    pub fn index(&self) -> Option<usize> {
        if self.found {
            Some(self.pos)
        } else {
            None
        }
    }
}

impl<const B: usize> TreeQuery<B> for OpIdSearch {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if child.index.ops.contains(&self.target) {
            QueryResult::Descend
        } else {
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.id == self.target {
            self.found = true;
            QueryResult::Finish
        } else {
            self.pos += 1;
            QueryResult::Next
        }
    }
}
