use super::visible::VisIter;
use super::{MarkIter, NoMarkIter, Op, OpIter, OpSet, VisibleOpIter};
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

    #[cfg(test)]
    fn key_ops(self) -> KeyOpIter<'a, Self> {
        KeyOpIter::new(self)
    }

    fn visible_slow(self, clock: Option<Clock>) -> VisibleOpIter<'a, Self> {
        VisibleOpIter::new(self, clock)
    }

    fn visible(
        self,
        op_set: &'a OpSet,
        clock: Option<&Clock>,
    ) -> FixCounters<'a, SkipIter<Self, VisIter<'a>>> {
        let vis = VisIter::new(op_set, clock, self.range());
        FixCounters::new(SkipIter::new(self, vis), clock.cloned())
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

impl<'a, I, S> OpQueryTerm<'a> for SkipIter<I, S>
where
    I: OpQueryTerm<'a> + Clone,
    S: crate::iter::tools::Skipper + std::fmt::Debug,
{
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        None
    }

    fn range(&self) -> Range<usize> {
        self.inner().range()
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQuery<'a> for I {}

#[derive(Clone, Debug)]
pub(crate) struct FixCounters<'a, I> {
    iter: I,
    clock: Option<Clock>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a, I> FixCounters<'a, I> {
    pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
        Self {
            iter,
            clock,
            _phantom: Default::default(),
        }
    }
}

impl<I: Default> Default for FixCounters<'_, I> {
    fn default() -> Self {
        Self::new(I::default(), None)
    }
}

impl<'a, I> Iterator for FixCounters<'a, I>
where
    I: Iterator<Item = Op<'a>>,
{
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut op = self.iter.next()?;
        op.fix_counter(self.clock.as_ref());
        Some(op)
    }
}

impl<'a, I> OpQueryTerm<'a> for FixCounters<'a, I>
where
    I: OpQueryTerm<'a>,
{
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }

    fn range(&self) -> Range<usize> {
        self.iter.range()
    }
}
