use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, is_visible, visible_op, QueryResult, TreeQuery};
use crate::{Key, types::ObjId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Prop {
    obj: ObjId,
    key: Key,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl Prop {
    pub fn new(obj: ObjId, prop: usize) -> Self {
        Prop {
            obj,
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }
}

impl<const B: usize> TreeQuery<B> for Prop {
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        m: &OpSetMetadata,
    ) -> QueryResult {
        let start = binary_search_by(child, |op| {
            m.lamport_cmp(op.obj, self.obj)
                .then_with(|| m.key_cmp(&op.key, &self.key))
        });
        let mut counters = Default::default();
        self.pos = start;
        for pos in start..child.len() {
            let op = child.get(pos).unwrap();
            if !(op.obj == self.obj && op.key == self.key) {
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
