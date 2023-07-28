use crate::marks::{RichText, RichTextStateMachine};
use crate::op_tree::OpSetMetadata;
use crate::op_tree::OpTreeIter;
use crate::types::{Clock, Key, Op};
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct TopOps<'a> {
    iter: OpTreeIter<'a>,
    pos: usize,
    start_pos: usize,
    num_ops: usize,
    clock: Option<Clock>,
    key: Option<Key>,
    last_op: Option<(usize, &'a Op, Option<Arc<RichText>>)>,
    marks: RichTextStateMachine<'a>,
    meta: Option<&'a OpSetMetadata>,
}

#[derive(Debug)]
pub(crate) struct TopOp<'a> {
    pub(crate) op: &'a Op,
    pub(crate) conflict: bool,
    pub(crate) marks: Option<Arc<RichText>>,
}

impl<'a> TopOps<'a> {
    pub(crate) fn new(iter: OpTreeIter<'a>, clock: Option<Clock>, meta: &'a OpSetMetadata) -> Self {
        Self {
            iter,
            pos: 0,
            start_pos: 0,
            num_ops: 0,
            clock,
            key: None,
            last_op: None,
            marks: Default::default(),
            meta: Some(meta),
        }
    }
}

impl<'a> Iterator for TopOps<'a> {
    type Item = TopOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result_op = None;
        loop {
            if let Some(op) = self.iter.next() {
                let key = op.elemid_or_key();
                let visible = op.visible_at(self.clock.as_ref());
                match (&self.clock, &self.meta) {
                    (Some(c), Some(m)) if c.covers(&op.id) => {
                        self.marks.process(op, m);
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
