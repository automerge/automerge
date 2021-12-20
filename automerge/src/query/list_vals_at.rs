use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::{Clock, ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListValsAt {
    clock: Clock,
    last_elem: Option<ElemId>,
    pub ops: Vec<Op>,
    window: VisWindow,
    pos: usize,
}

impl ListValsAt {
    pub fn new(clock: Clock) -> Self {
        ListValsAt {
            clock,
            last_elem: None,
            ops: vec![],
            window: Default::default(),
            pos: 0,
        }
    }
}

impl<const B: usize> TreeQuery<B> for ListValsAt {
    fn query_element(&mut self, op: &Op) -> QueryResult {
        if op.insert {
            self.last_elem = None;
        }
        if self.last_elem.is_none() && self.window.visible_at(op, self.pos, &self.clock) {
            for (_, vop) in self.window.seen_op(op, self.pos) {
                self.last_elem = vop.elemid();
                self.ops.push(vop);
            }
        }
        self.pos += 1;
        QueryResult::Next
    }
}
