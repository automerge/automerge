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
    start: Option<usize>,
}

impl<'a> Prop<'a> {
    pub(crate) fn new(prop: usize) -> Self {
        Prop {
            key: Key::Map(prop),
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
            start: None,
        }
    }
}

impl<'a> TreeQuery<'a> for Prop<'a> {
    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        m: &OpSetMetadata,
    ) -> QueryResult {
        // in the root node find the first op position for the key
        if self.start.is_none() {
            let start = binary_search_by(child, |op| m.key_cmp(&op.key, &self.key));
            self.start = Some(start);
        };
        QueryResult::Descend
    }

    fn query_element(&mut self, op: &'a Op) -> QueryResult {
        // skip to our start
        if self.pos < self.start.unwrap() {
            self.pos += 1;
            return QueryResult::Next;
        }
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
