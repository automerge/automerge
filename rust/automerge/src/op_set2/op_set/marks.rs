use crate::{
    marks::{MarkSet, MarkStateMachine},
    types::Clock,
};

use super::{Action, MarkData, Op, OpIter, OpQueryTerm, OpType};

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
            if op.action == Action::Mark {
                if let Some(name) = op.mark_name {
                    let value = op.value;
                    self.marks.mark_begin(op.id, MarkData { name, value });
                    continue;
                } else {
                    self.marks.mark_end(op.id);
                    continue;
                }
            }
            return Some(op);
        }
        None
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQueryTerm<'a> for MarkIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.marks.current()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoMarkIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> NoMarkIter<'a, I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for NoMarkIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            if op.action == Action::Mark {
                continue;
            }
            return Some(op);
        }
        None
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQueryTerm<'a> for NoMarkIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        None
    }
}
