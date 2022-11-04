use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::ListEncoding;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len {
    pub(crate) len: usize,
    encoding: ListEncoding,
}

impl Len {
    pub(crate) fn new(encoding: ListEncoding) -> Self {
        Len { len: 0, encoding }
    }
}

impl<'a> TreeQuery<'a> for Len {
    fn query_node(&mut self, child: &OpTreeNode) -> QueryResult {
        self.len = child.index.visible_len(self.encoding);
        QueryResult::Finish
    }
}
