use crate::op_tree::OpTreeNode;
use crate::query::{binary_search_by, is_visible, visible_op, QueryResult, TreeQuery};
use crate::{Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Prop {
    key: Key,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl Prop {
    pub fn new(prop: String) -> Self {
        Prop {
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }
}

impl<const B: usize> TreeQuery<B> for Prop {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let start = binary_search_by(child, |op| op.key.cmp(&self.key));
        let mut counters = Default::default();
        self.pos = start;
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if op.key != self.key {
                break;
            }
            if is_visible(op, pos, &mut counters) {
                for (vpos, vop) in visible_op(op, pos, &counters) {
                    self.ops.push(vop);
                    self.ops_pos.push(vpos);
                }
            }
            self.pos += 1;
        }
        QueryResult::Finish
    }
}
