use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Len {
    pub(crate) len: usize,
    utf16: bool,
}

impl Len {
    pub(crate) fn new() -> Self {
        Len {
            len: 0,
            utf16: false,
        }
    }
}

impl<'a> TreeQuery<'a> for Len {
    fn query_node(&mut self, child: &OpTreeNode) -> QueryResult {
        self.len = child.index.visible_len(self.utf16);
        QueryResult::Finish
    }
}
