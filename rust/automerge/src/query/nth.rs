use crate::error::AutomergeError;
use crate::op_set::OpSet;
use crate::op_tree::{OpTree, OpTreeNode};
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, Op, OpIds};
use std::fmt::Debug;

/// The Nth query walks the tree to find the n-th Node. It skips parts of the tree where it knows
/// that the nth node can not be in them
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Nth<'a> {
    target: usize,
    seen: usize,
    pub(crate) ops: Vec<&'a Op>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) pos: usize,
}

impl<'a> Nth<'a> {
    pub(crate) fn new(target: usize, _encoding: ListEncoding) -> Self {
        Nth {
            target,
            seen: 0,
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }

    pub(crate) fn pred(&self, ops: &OpSet) -> OpIds {
        ops.m.sorted_opids(self.ops.iter().map(|o| o.id))
    }

    /// Get the key
    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        // the query collects the ops so we can use that to get the key they all use
        if let Some(e) = self.ops.first().and_then(|op| op.elemid()) {
            Ok(Key::Seq(e))
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
    }

    pub(crate) fn index(&self) -> usize {
        self.seen - 1
    }
}

impl<'a> TreeQuery<'a> for Nth<'a> {
    fn equiv(&mut self, other: &Self) -> bool {
        self.index() == other.index() && self.key() == other.key()
    }

    fn can_shortcut_search(&mut self, tree: &'a OpTree) -> bool {
        if let Some((index, pos)) = &tree.last_insert {
            if *index == self.target {
                if let Some(op) = tree.internal.get(*pos) {
                    self.seen = *index + 1;
                    self.ops.push(op);
                    self.ops_pos.push(*pos);
                    self.pos = *pos + 1;
                    return true;
                }
            }
        }
        false
    }

    fn query_node(&mut self, node: &OpTreeNode, _ops: &[Op]) -> QueryResult {
        let num_vis = node.index.visible.len();
        if self.seen + num_vis >= self.target {
            return QueryResult::Descend;
        }
        self.seen += num_vis;
        self.pos += node.len();
        QueryResult::Next
    }

    fn query_element(&mut self, element: &'a Op) -> QueryResult {
        if element.insert && self.seen > self.target {
            QueryResult::Finish
        } else {
            if element.visible() {
                self.seen += 1;
                if self.seen > self.target {
                    self.ops.push(element);
                    self.ops_pos.push(self.pos);
                }
            }
            self.pos += 1;
            QueryResult::Next
        }
    }
}
