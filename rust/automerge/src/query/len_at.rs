use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::types::{Clock, ElemId, ListEncoding, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LenAt {
    pub(crate) len: usize,
    clock: Clock,
    pos: usize,
    encoding: ListEncoding,
    last: Option<ElemId>,
    window: VisWindow,
}

impl LenAt {
    pub(crate) fn new(clock: Clock, encoding: ListEncoding) -> Self {
        LenAt {
            clock,
            pos: 0,
            len: 0,
            encoding,
            last: None,
            window: Default::default(),
        }
    }
}

impl<'a> TreeQuery<'a> for LenAt {
    fn query_element(&mut self, op: &'a Op) -> QueryResult {
        if op.insert {
            self.last = None;
        }
        let elem = op.elemid();
        let visible = self.window.visible_at(op, self.pos, &self.clock);
        if elem != self.last && visible {
            self.len += op.width(self.encoding);
            self.last = elem;
        }
        self.pos += 1;
        QueryResult::Next
    }
}
