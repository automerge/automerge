use crate::{
    marks::{MarkSet, MarkStateMachine},
    types::Clock,
};

use super::{HasOpScope, Op, OpIter, OpScope, OpType, Verified};

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct MarkIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
    marks: MarkStateMachine<'a>,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> MarkIter<'a, I> {
    pub(crate) fn new(iter: I) -> Self {
        let marks = MarkStateMachine::default();
        Self { iter, marks }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for MarkIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            match op.action() {
                OpType::MarkBegin(_, data) => {
                    self.marks.mark_begin(op.id, data);
                    continue;
                }
                OpType::MarkEnd(_) => {
                    self.marks.mark_end(op.id);
                    continue;
                }
                _ => (),
            }
            return Some(op);
        }
        None
    }
}

impl<'a, I: HasOpScope<'a> + Clone> HasOpScope<'a> for MarkIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.marks.current()
    }
}
