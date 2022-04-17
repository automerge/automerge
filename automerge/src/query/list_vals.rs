use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListVals<'a> {
    last_elem: Option<ElemId>,
    pub ops: Vec<&'a Op>,
}

impl<'a> ListVals<'a> {
    pub fn new() -> Self {
        ListVals {
            last_elem: None,
            ops: vec![],
        }
    }
}

impl<'a> TreeQuery<'a> for ListVals<'a> {
    fn query_node(&mut self, child: &'a OpTreeNode) -> QueryResult {
        let start = 0;
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if op.insert {
                self.last_elem = None;
            }
            if self.last_elem.is_none() && op.visible() {
                self.last_elem = op.elemid();
                self.ops.push(op);
            }
        }
        QueryResult::Finish
    }
}
