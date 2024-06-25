use crate::{
    marks::{MarkSet, MarkStateMachine},
    types::{Clock },
};

use super::{Op, OpScope , OpIter, Verified};

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Default, Debug)]
pub(crate) struct MarkIter<'a, I: Iterator<Item = Op<'a>> + Clone + Default> {
    iter: I,
    marks: MarkStateMachine<'a>,
    clock: Option<Clock>,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone + Default> MarkIter<'a, I> {
    pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
        let marks = MarkStateMachine::default();
        Self { iter, clock, marks }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone + Default> Iterator for MarkIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.next()?;
        if (self
            .clock
            .as_ref()
            .map(|c| c.covers(&op.id))
            .unwrap_or(true))
        {
            self.marks.process(op.id, op.action());
        }
        Some(op)
    }
}

impl<'a, I: OpScope<'a>> OpScope<'a> for MarkIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<Arc<MarkSet>> {
        self.marks.current().cloned()
    }
}
