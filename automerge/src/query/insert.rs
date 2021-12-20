use crate::op_tree::OpTreeNode;
use crate::query::{is_visible, CounterData, QueryResult, TreeQuery};
use crate::{AutomergeError, ElemId, Key, Op, OpId, HEAD};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth<const B: usize> {
    target: usize,
    seen: usize,
    pub pos: usize,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    counters: HashMap<OpId, CounterData>,
}

impl<const B: usize> InsertNth<B> {
    pub fn new(target: usize) -> Self {
        InsertNth {
            target,
            seen: 0,
            pos: 0,
            last_seen: None,
            last_insert: None,
            counters: Default::default(),
        }
    }

    pub fn key(&self) -> Result<Key, AutomergeError> {
        if self.target == 0 {
            Ok(HEAD.into())
        } else if self.seen == self.target && self.last_insert.is_some() {
            Ok(Key::Seq(self.last_insert.unwrap()))
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
    }

    pub fn is_visible(&mut self, element: &Op) -> bool {
        is_visible(element, self.pos, &mut self.counters)
    }
}

impl<const B: usize> TreeQuery<B> for InsertNth<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if self.target == 0 {
            // insert at the start of the obj all inserts are lesser b/c this is local
            self.pos = 0;
            return QueryResult::Finish;
        }
        let mut num_vis = child.index.len;
        if num_vis > 0 {
            if child.index.has(&self.last_seen) {
                num_vis -= 1;
            }
            if self.seen + num_vis >= self.target {
                QueryResult::Decend
            } else {
                self.pos += child.len();
                self.seen += num_vis;
                self.last_seen = child.last().elemid();
                QueryResult::Next
            }
        } else {
            self.pos += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.insert {
            if self.seen >= self.target {
                return QueryResult::Finish;
            };
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.last_seen.is_none() && self.is_visible(element) {
            self.seen += 1;
            self.last_seen = element.elemid()
        }
        self.pos += 1;
        QueryResult::Next
    }
}
