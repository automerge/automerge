use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len {
    pub len: usize,
}

impl Len {
    pub fn new() -> Self {
        Len { len: 0 }
    }
}

impl<const B: usize> TreeQuery<B> for Len {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        self.len = child.index.len;
        QueryResult::Finish
    }
}
