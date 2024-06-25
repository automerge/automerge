use crate::{
    marks::{MarkSet},
    types::{Clock},
};

use super::{Op, OpScope, OpIter, Verified};

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub(crate) struct VisibleOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    clock: Option<Clock>,
    iter: I,
}

impl<'a, I: OpScope<'a>> VisibleOpIter<'a, I> {
  pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
    Self { iter, clock }
  }
}

impl<'a, I: OpScope<'a>> Iterator for VisibleOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            if op.visible_at(self.clock.as_ref()) {
                return Some(op);
            }
        }
        None
    }
}

impl<'a, I: OpScope<'a>> OpScope<'a> for VisibleOpIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<Arc<MarkSet>> {
        self.iter.get_marks()
    }
}

