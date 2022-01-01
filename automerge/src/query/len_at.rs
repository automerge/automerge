use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::types::{Clock, ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LenAt<const B: usize> {
    pub len: usize,
    clock: Clock,
    pos: usize,
    last: Option<ElemId>,
    window: VisWindow,
}

impl<const B: usize> LenAt<B> {
    pub fn new(clock: Clock) -> Self {
        LenAt {
            clock,
            pos: 0,
            len: 0,
            last: None,
            window: Default::default(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for LenAt<B> {
    fn query_element(&mut self, op: &Op) -> QueryResult {
        if op.insert {
            self.last = None;
        }
        let elem = op.elemid();
        let visible = self.window.visible_at(op, self.pos, &self.clock);
        if elem != self.last && visible {
            self.len += 1;
            self.last = elem;
        }
        self.pos += 1;
        QueryResult::Next
    }
}
