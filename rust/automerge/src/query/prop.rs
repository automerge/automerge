use crate::op_set::Op;
use crate::op_tree::OpTreeNode;
use crate::query::{Index, OpSetData, QueryResult, TreeQuery};
use crate::types::{Clock, Key};

use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Prop<'a> {
    clock: Option<Clock>,
    key: Key,
    pub(crate) pos: usize,
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) ops_pos: Vec<usize>,
}

impl<'a> Prop<'a> {
    pub(crate) fn new(key: Key, clock: Option<Clock>) -> Self {
        Prop {
            clock,
            key,
            pos: 0,
            ops: vec![],
            ops_pos: vec![],
        }
    }
}

impl<'a> TreeQuery<'a> for Prop<'a> {
    fn query_node(&mut self, child: &OpTreeNode, index: &Index, osd: &OpSetData) -> QueryResult {
        let cmp = child.last().as_op(osd).key_cmp(&self.key);
        if cmp == Ordering::Less
            || (cmp == Ordering::Equal && self.clock.is_none() && !index.has_visible(&self.key))
        {
            self.pos += child.len();
            QueryResult::Next
        } else {
            QueryResult::Descend
        }
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        match op.key_cmp(&self.key) {
            Ordering::Greater => QueryResult::Finish,
            Ordering::Equal => {
                if op.visible_at(self.clock.as_ref()) {
                    self.ops.push(op);
                    self.ops_pos.push(self.pos);
                }
                self.pos += 1;
                QueryResult::Next
            }
            Ordering::Less => {
                self.pos += 1;
                QueryResult::Next
            }
        }
    }
}
