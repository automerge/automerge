use crate::AutomergeError;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{ElemId, Key, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mark<const B: usize> {
    start: usize,
    end: usize,
    pos: usize,
    seen: usize,
    _ops: Vec<(usize, Key)>,
    count: usize,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
}

impl<const B: usize> Mark<B> {
    pub fn new(start: usize, end: usize)  -> Self {
        Mark {
            start,
            end,
            pos: 0,
            seen: 0,
            _ops: Vec::new(),
            count: 0,
            last_seen: None,
            last_insert: None,
        }
    }

    pub fn ops(&self) -> Result<((usize,Key),(usize,Key)),AutomergeError> {
        if self._ops.len() == 2 {
            Ok((self._ops[0], self._ops[1]))
        } else {
            Err(AutomergeError::Fail)
        }
    }
}

impl<const B: usize> TreeQuery<B> for Mark<B> {
    /*
    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        unimplemented!()
    }
    */

    fn query_element(&mut self, element: &Op) -> QueryResult {
        // find location to insert
        // mark or set
        if element.insert {
            if self.seen >= self.end {
               self._ops.push((self.pos + 1, self.last_insert.into()));
               return QueryResult::Finish;
            }
            if self.seen >= self.start && self._ops.is_empty() {
               self._ops.push((self.pos, self.last_insert.into()));
            }
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.last_seen.is_none() && element.visible() {
            self.seen += 1;
            self.last_seen = element.elemid()
        }
        self.pos += 1;
        QueryResult::Next
    }
}
