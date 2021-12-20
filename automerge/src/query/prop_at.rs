use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery, VisWindow};
use crate::{Clock, Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PropAt {
    clock: Clock,
    key: Key,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl PropAt {
    pub fn new(prop: usize, clock: Clock) -> Self {
        PropAt {
            clock,
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }
}

impl<const B: usize> TreeQuery<B> for PropAt {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        let start = binary_search_by(child, |op| m.key_cmp(&op.key, &self.key));
        let mut window: VisWindow = Default::default();
        self.pos = start;
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if op.key != self.key {
                break;
            }
            if window.visible_at(op, pos, &self.clock) {
                for (vpos, vop) in window.seen_op(op, pos) {
                    self.ops.push(vop);
                    self.ops_pos.push(vpos);
                }
            }
            self.pos += 1;
        }
        QueryResult::Finish
    }
}
