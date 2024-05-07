use crate::marks::{MarkSet, MarkStateMachine};
use crate::op_set::{Op, OpIter};
use crate::types::{Clock, Key};
use std::sync::Arc;

#[derive(Default, Clone)]
pub(crate) enum TopOps<'a> {
    #[default]
    Empty,
    Ops(TopOpsInner<'a>),
}

impl<'a> TopOps<'a> {
    pub(crate) fn new(iter: OpIter<'a>, clock: Option<Clock>) -> Self {
        TopOps::Ops(TopOpsInner::new(iter, clock))
    }
}

#[derive(Clone)]
pub(crate) struct TopOpsInner<'a> {
    iter: OpIter<'a>,
    pos: usize,
    start_pos: usize,
    num_ops: usize,
    clock: Option<Clock>,
    key: Option<Key>,
    last_op: Option<(usize, Op<'a>, Option<Arc<MarkSet>>)>,
    marks: MarkStateMachine<'a>,
}

#[derive(Debug)]
pub(crate) struct TopOp<'a> {
    pub(crate) op: Op<'a>,
    pub(crate) conflict: bool,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

impl<'a> TopOpsInner<'a> {
    pub(crate) fn new(iter: OpIter<'a>, clock: Option<Clock>) -> Self {
        Self {
            iter,
            pos: 0,
            start_pos: 0,
            num_ops: 0,
            clock,
            key: None,
            last_op: None,
            marks: Default::default(),
        }
    }
}

impl<'a> Iterator for TopOps<'a> {
    type Item = TopOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::Ops(top) => top.next(),
        }
    }
}

impl<'a> Iterator for TopOpsInner<'a> {
    type Item = TopOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result_op = None;
        loop {
            if let Some(op) = self.iter.next() {
                let key = op.elemid_or_key();
                let visible = op.visible_at(self.clock.as_ref());
                match &self.clock {
                    Some(c) if c.covers(op.id()) => {
                        self.marks.process(*op.id(), op.action(), self.iter.osd);
                    }
                    _ => {}
                }
                match &self.key {
                    Some(k) if k == &key => {
                        if visible {
                            self.last_op = Some((self.pos, op, self.marks.current().cloned()));
                            self.num_ops += 1;
                        }
                    }
                    Some(_) => {
                        result_op = self.last_op.take().map(|(_op_pos, op, marks)| (op, marks));
                        if visible {
                            self.last_op = Some((self.pos, op, self.marks.current().cloned()));
                            self.num_ops = 1;
                        } else {
                            self.num_ops = 0;
                        }
                        self.key = Some(key);
                        self.start_pos = self.pos;
                    }
                    None => {
                        self.key = Some(key);
                        self.start_pos = self.pos;
                        if visible {
                            self.last_op = Some((self.pos, op, self.marks.current().cloned()));
                            self.num_ops = 1;
                        } else {
                            self.num_ops = 0;
                        }
                    }
                }
                self.pos += 1;
                if result_op.is_some() {
                    break;
                }
            } else {
                result_op = self.last_op.take().map(|(_op_pos, op, marks)| (op, marks));
                break;
            }
        }
        result_op.map(|(op, marks)| TopOp {
            op,
            conflict: self.num_ops > 1,
            marks,
        })
    }
}
