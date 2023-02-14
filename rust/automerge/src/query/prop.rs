use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::query::{binary_search_by, QueryResult, TreeQuery};
use crate::types::{Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Prop<'a> {
    key: Key,
    pub(crate) ops: Vec<&'a Op>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) pos: usize,
}

impl<'a> Prop<'a> {
    pub(crate) fn new(prop: usize) -> Self {
        Prop {
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }
}

impl<'a> TreeQuery<'a> for Prop<'a> {
    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        m: &OpSetMetadata,
        ops: &[Op],
    ) -> QueryResult {
        let start = binary_search_by(child, ops, |op| m.key_cmp(&op.key, &self.key));
        self.pos = start;
        QueryResult::Skip(start)
    }

    fn query_element(&mut self, op: &'a Op) -> QueryResult {
        // don't bother looking at things past our key
        if op.key != self.key {
            return QueryResult::Finish;
        }
        if op.visible() {
            self.ops.push(op);
            self.ops_pos.push(self.pos);
        }
        self.pos += 1;
        QueryResult::Next
    }
}
