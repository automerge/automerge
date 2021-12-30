use crate::op_tree::OpTreeNode;
use crate::query::{is_visible, visible_op, QueryResult, TreeQuery};
use crate::{ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListVals {
    last_elem: Option<ElemId>,
    pub ops: Vec<Op>,
}

impl ListVals {
    pub fn new() -> Self {
        ListVals {
            last_elem: None,
            ops: vec![],
        }
    }
}

impl<const B: usize> TreeQuery<B> for ListVals {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut counters = Default::default();
        for pos in 0..child.len() {
            let op = child.get(pos).unwrap();
            if op.insert {
                self.last_elem = None;
            }
            if self.last_elem.is_none() && is_visible(op, pos, &mut counters) {
                for (_, vop) in visible_op(op, pos, &counters) {
                    self.last_elem = vop.elemid();
                    self.ops.push(vop);
                }
            }
        }
        QueryResult::Finish
    }
}
