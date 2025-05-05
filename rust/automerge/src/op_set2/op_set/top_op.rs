use crate::marks::MarkSet;

use super::{Op, OpQueryTerm};

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct TopOpIter<'a, I: Iterator<Item = Op<'a>>> {
    iter: I,
    last_op: Option<Op<'a>>,
}

impl<'a, I: Iterator<Item = Op<'a>>> TopOpIter<'a, I> {
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            last_op: None,
        }
    }
}

impl<'a, I: Iterator<Item = Op<'a>>> Iterator for TopOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for mut next in self.iter.by_ref() {
            if let Some(last) = self.last_op.take() {
                if next.obj != last.obj || next.elemid_or_key() != last.elemid_or_key() {
                    self.last_op = Some(next);
                    return Some(last);
                } else {
                    next.conflict = true;
                }
            }
            self.last_op = Some(next);
        }
        self.last_op.take()
    }
}

impl<'a, I: OpQueryTerm<'a>> OpQueryTerm<'a> for TopOpIter<'a, I> {
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }
    fn range(&self) -> Range<usize> {
        self.iter.range()
    }
}
