use crate::{
    marks::{MarkSet, MarkStateMachine},
    types::Clock,
};

use super::{Op, OpIter, OpQueryTerm, OpType};

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct ElemIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
    current: usize,
    last: Option<Range<usize>>,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> ElemIter<'a, I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { iter, start: 0, end: 0 }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for ElemIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            return Some(op);
        }
        None
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQueryTerm<'a> for ElemIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }
}
