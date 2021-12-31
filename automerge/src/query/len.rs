use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len<const B: usize> {
    pub len: usize,
}

impl<const B: usize> Len<B> {
    pub fn new() -> Self {
        Len { len: 0 }
    }
}

impl<const B: usize> TreeQuery<B> for Len<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        self.len = child.index.len;
        QueryResult::Finish
    }
}
