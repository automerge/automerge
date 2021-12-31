use crate::query::{QueryResult, TreeQuery, VisWindow};
use crate::types::{Clock, ElemId, Op};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NthAt<const B: usize> {
    clock: Clock,
    target: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    last_elem: Option<ElemId>,
    window: VisWindow,
    pub ops: Vec<Op>,
    pub ops_pos: Vec<usize>,
    pub pos: usize,
}

impl<const B: usize> NthAt<B> {
    pub fn new(target: usize, clock: Clock) -> Self {
        NthAt {
            clock,
            target,
            seen: 0,
            last_seen: None,
            ops: vec![],
            ops_pos: vec![],
            pos: 0,
            last_elem: None,
            window: Default::default(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for NthAt<B> {
    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.insert {
            if self.seen > self.target {
                return QueryResult::Finish;
            };
            self.last_elem = element.elemid();
            self.last_seen = None
        }
        let visible = self.window.visible_at(element, self.pos, &self.clock);
        if visible && self.last_seen.is_none() {
            self.seen += 1;
            self.last_seen = element.elemid()
        }
        if self.seen == self.target + 1 && visible {
            for (vpos, vop) in self.window.seen_op(element, self.pos) {
                self.ops.push(vop);
                self.ops_pos.push(vpos);
            }
        }
        self.pos += 1;
        QueryResult::Next
    }
}
