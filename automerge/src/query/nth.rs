use crate::error::AutomergeError;
use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Nth<const B: usize> {
    target: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl<const B: usize> Nth<B> {
    pub fn new(target: usize) -> Self {
        Nth {
            target,
            seen: 0,
            last_seen: None,
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
        }
    }

    /// Get the key
    pub fn key(&self) -> Result<Key, AutomergeError> {
        // the query collects the ops so we can use that to get the key they all use
        if let Some(e) = self.ops.first().and_then(|op| op.elemid()) {
            Ok(Key::Seq(e))
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
    }
}

impl<const B: usize> TreeQuery<B> for Nth<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut num_vis = child.index.len;
        if num_vis > 0 {
            // num vis is the number of keys in the index
            // minus one if we're counting last_seen
            // let mut num_vis = s.keys().count();
            if child.index.has(&self.last_seen) {
                num_vis -= 1;
            }
            if self.seen + num_vis > self.target {
                QueryResult::Descend
            } else {
                self.pos += child.len();
                self.seen += num_vis;
                self.last_seen = child.last().elemid();
                QueryResult::Next
            }
        } else {
            // skip this node as no useful ops in it
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.insert {
            if self.seen > self.target {
                return QueryResult::Finish;
            };
            self.last_seen = None
        }
        let visible = element.visible();
        if visible && self.last_seen.is_none() {
            self.seen += 1;
            self.last_seen = element.elemid()
        }
        if self.seen == self.target + 1 && visible {
            self.ops.push(element.clone());
            self.ops_pos.push(self.pos);
        }
        self.pos += 1;
        QueryResult::Next
    }
}
