use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::ObjId;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len<const B: usize> {
    obj: ObjId,
    pub len: usize,
}

impl<const B: usize> Len<B> {
    pub fn new(obj: ObjId) -> Self {
        Len { obj, len: 0 }
    }
}

impl<const B: usize> TreeQuery<B> for Len<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        self.len = child.index.len;
        QueryResult::Finish
    }
}
