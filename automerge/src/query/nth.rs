use crate::error::AutomergeError;
use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Nth {
    target: usize,
    seen: usize,
    /// last_seen is the target elemid of the last `seen` operation.
    /// It is used to avoid double counting visible elements (which arise through conflicts) that are split across nodes.
    last_seen: Option<ElemId>,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl Nth {
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

impl<const B: usize> TreeQuery<B> for Nth {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut num_vis = child.index.visible_len();
        if child.index.has_visible(&self.last_seen) {
            num_vis -= 1;
        }

        if self.seen + num_vis > self.target {
            QueryResult::Descend
        } else {
            // skip this node as no useful ops in it
            self.pos += child.len();
            self.seen += num_vis;

            // We have updated seen by the number of visible elements in this index, before we skip it.
            // We also need to keep track of the last elemid that we have seen (and counted as seen).
            // We can just use the elemid of the last op in this node as either:
            // - the insert was at a previous node and this is a long run of overwrites so last_seen should already be set correctly
            // - the visible op is in this node and the elemid references it so it can be set here
            // - the visible op is in a future node and so it will be counted as seen there
            let last_elemid = child.last().elemid();
            if child.index.has_visible(&last_elemid) {
                self.last_seen = last_elemid;
            }
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.insert {
            if self.seen > self.target {
                return QueryResult::Finish;
            }
            // we have a new potentially visible element so reset last_seen
            self.last_seen = None
        }
        let visible = element.visible();
        if visible && self.last_seen.is_none() {
            self.seen += 1;
            // we have a new visible element
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
