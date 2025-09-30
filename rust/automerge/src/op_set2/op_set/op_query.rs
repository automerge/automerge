use super::visible::VisIter;
use super::{MarkIter, NoMarkIter, Op, OpIter, OpSet, TopOpIter, VisibleOpIter};
use crate::iter::tools::SkipIter;
use crate::marks::MarkSet;
use crate::types::Clock;

#[cfg(test)]
use crate::iter::KeyOpIter;

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

pub(crate) trait OpQueryTerm<'a>: Iterator<Item = Op<'a>> + Debug {
    fn get_marks(&self) -> Option<&Arc<MarkSet>>;

    fn range(&self) -> Range<usize>;
}

pub(crate) trait OpQuery<'a>: OpQueryTerm<'a> + Clone {
    fn marks(self) -> MarkIter<'a, Self> {
        MarkIter::new(self)
    }

    fn no_marks(self) -> NoMarkIter<'a, Self> {
        NoMarkIter::new(self)
    }

    fn top_ops(self) -> TopOpIter<'a, Self> {
        TopOpIter::new(self)
    }

    #[cfg(test)]
    fn key_ops(self) -> KeyOpIter<'a, Self> {
        KeyOpIter::new(self)
    }

    fn visible_slow(self, clock: Option<Clock>) -> VisibleOpIter<'a, Self> {
        VisibleOpIter::new(self, clock)
    }

    fn visible(self, op_set: &'a OpSet, clock: Option<&Clock>) -> FixCounters<'a, Self> {
        let vis = VisIter::new(op_set, clock, self.range());
        FixCounters::new(SkipIter::new(self, vis), clock)
    }
}

impl<'a> OpQueryTerm<'a> for OpIter<'a> {
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        None
    }

    fn range(&self) -> Range<usize> {
        self.range()
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQuery<'a> for I {}

pub(crate) struct FixCounters<'a, T: OpQuery<'a>> {
    iter: SkipIter<T, VisIter<'a>>,
    clock: Option<Clock>,
}

impl<'a, T: OpQuery<'a>> FixCounters<'a, T> {
    fn new(iter: SkipIter<T, VisIter<'a>>, clock: Option<&Clock>) -> Self {
        Self {
            iter,
            clock: clock.cloned(),
        }
    }
}

impl<'a, T: OpQuery<'a>> Iterator for FixCounters<'a, T> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut op = self.iter.next()?;
        op.fix_counter(self.clock.as_ref());
        Some(op)
    }
}
