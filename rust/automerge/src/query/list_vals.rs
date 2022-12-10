use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListVals {
    last_elem: Option<ElemId>,
    pub(crate) ops: Vec<Op>,
}

impl ListVals {
    pub(crate) fn new() -> Self {
        ListVals {
            last_elem: None,
            ops: vec![],
        }
    }
}

impl<'a> TreeQuery<'a> for ListVals {
    fn query_node(&mut self, child: &OpTreeNode, ops: &[Op]) -> QueryResult {
        let start = 0;
        for pos in start..child.len() {
            let op = &ops[child.get(pos).unwrap()];
            if op.insert {
                self.last_elem = None;
            }
            if self.last_elem.is_none() && op.visible() {
                self.last_elem = op.elemid();
                self.ops.push(op.clone());
            }
        }
        QueryResult::Finish
    }
}
