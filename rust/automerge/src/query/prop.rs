use crate::op_tree::OpTreeNode;
use crate::query::OpSetMetadata;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Clock, Key, Op};
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Prop<'a> {
    clock: Option<Clock>,
    key: Key,
    pub(crate) pos: usize,
    pub(crate) ops: Vec<&'a Op>,
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
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode,
        m: &OpSetMetadata,
        ops: &[Op],
    ) -> QueryResult {
        let cmp = m.key_cmp(&ops[child.last()].key, &self.key);
        if cmp == Ordering::Less
            || (cmp == Ordering::Equal
                && self.clock.is_none()
                && !child.index.has_visible(&self.key))
        {
            self.pos += child.len();
            QueryResult::Next
        } else {
            QueryResult::Descend
        }
    }

    fn query_element_with_metadata(&mut self, element: &'a Op, m: &OpSetMetadata) -> QueryResult {
        match m.key_cmp(&element.key, &self.key) {
            Ordering::Greater => QueryResult::Finish,
            Ordering::Equal => {
                if element.visible_at(self.clock.as_ref()) {
                    self.ops.push(element);
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
