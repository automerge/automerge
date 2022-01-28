use crate::error::AutomergeError;
use crate::op_tree::OpTreeNode;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Key, Op, HEAD};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth<const B: usize> {
    target: usize,
    seen: usize,
    //pub pos: usize,
    n: usize,
    valid: Option<usize>,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    last_valid_insert: Option<ElemId>,
}

impl<const B: usize> InsertNth<B> {
    pub fn new(target: usize) -> Self {
        let (valid, last_valid_insert) = if target == 0 {
            (Some(0), Some(HEAD))
        } else {
            (None, None)
        };
        InsertNth {
            target,
            seen: 0,
            n: 0,
            valid,
            last_seen: None,
            last_insert: None,
            last_valid_insert,
        }
    }

    pub fn pos(&self) -> usize {
        self.valid.unwrap_or(self.n)
    }

    pub fn key(&self) -> Result<Key, AutomergeError> {
        Ok(self
            .last_valid_insert
            .ok_or(AutomergeError::InvalidIndex(self.target))?
            .into())
        //if self.target == 0 {
        /*
        if self.last_insert.is_none() {
            Ok(HEAD.into())
        } else if self.seen == self.target && self.last_insert.is_some() {
            Ok(Key::Seq(self.last_insert.unwrap()))
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
        */
    }
}

impl<const B: usize> TreeQuery<B> for InsertNth<B> {
    fn query_node(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        let mut num_vis = child.index.len;
        if num_vis > 0 {
            if child.index.has(&self.last_seen) {
                num_vis -= 1;
            }
            if self.seen + num_vis >= self.target {
                QueryResult::Decend
            } else {
                self.n += child.len();
                self.seen += num_vis;
                self.last_seen = child.last().elemid();
                QueryResult::Next
            }
        } else {
            self.n += child.len();
            QueryResult::Next
        }
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.insert {
            if self.valid.is_none() && self.seen >= self.target {
                self.valid = Some(self.n);
            }
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.valid.is_some() && element.valid_mark_anchor() {
            self.last_valid_insert = element.elemid();
            self.valid = None;
        }
        if self.last_seen.is_none() && element.visible() {
            if self.seen >= self.target {
                return QueryResult::Finish;
            }
            self.seen += 1;
            self.last_seen = element.elemid();
            self.last_valid_insert = self.last_seen
        }
        self.n += 1;
        QueryResult::Next
    }
}
