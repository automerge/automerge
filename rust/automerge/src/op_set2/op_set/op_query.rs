use super::visible::VisIter;
use super::{MarkIter, NoMarkIter, Op, OpIter, OpSet, VisibleOpIter};
use crate::automerge::view::ReadAt;
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

    /// Restrict this query to the ops visible under `read`, with counter
    /// values summed accordingly.
    ///
    /// As in [`super::TopOps::new`], a current read uses the visibility index
    /// whether or not a mask is present, a historical read scans, and counter
    /// values are fixed up against [`ReadAt::filter`] on either path.
    fn visible(
        self,
        op_set: &'a OpSet,
        read: &ReadAt<'_>,
    ) -> FixCounters<'a, SkipIter<Self, VisIter<'a>>> {
        let vis = VisIter::new(op_set, read.historical(), self.range());
        FixCounters::new(SkipIter::new(self, vis), read.filter().cloned())
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

#[cfg(test)]
impl<'a, I> FixCounters<'a, I> {
    pub(crate) fn inner(&self) -> &I {
        &self.iter
    }
}

#[cfg(test)]
mod tests {
    //! `OpQuery::visible` must use the visibility index for any current
    //! read, masked or not, and scan only for a historical read. See the
    //! tests on `TopOps` for the same invariant on the top-op iterator.

    use super::OpQuery;
    use crate::op_set2::op_set::VisIter;
    use crate::transaction::Transactable;
    use crate::{Author, AutoCommit, ROOT};

    fn author(n: u8) -> Author<'static> {
        Author::from(vec![n])
    }

    /// A two-author map, masked at its heads so that a mask is present but
    /// nothing is hidden.
    fn masked_doc() -> AutoCommit {
        let mut doc = AutoCommit::new().with_author(Some(author(1)));
        doc.put(ROOT, "k", 1).unwrap();
        doc.commit();
        doc.set_author(Some(author(2)));
        doc.put(ROOT, "k", 2).unwrap();
        doc.commit();
        let heads = doc.get_heads();
        doc.mask_author(author(2), &heads);
        doc
    }

    #[test]
    fn masked_current_read_uses_the_indexes() {
        let mut doc = masked_doc();
        assert!(doc.is_author_masked(&author(2)));
        let am = doc.document();
        let obj = am.exid_to_obj(&ROOT).unwrap();
        let range = am.ops().prop_range(&obj.id, "k");
        let read = am.read_current();
        let visible = am.ops().iter_range(&range).visible(am.ops(), &read);
        assert!(matches!(visible.inner().skipper(), VisIter::Indexed(_)));
    }

    #[test]
    fn historical_read_scans() {
        let mut doc = masked_doc();
        let heads = vec![doc.get_changes(&[])[0].hash()];
        let am = doc.document();
        let obj = am.exid_to_obj(&ROOT).unwrap();
        let range = am.ops().prop_range(&obj.id, "k");
        let read = am.read_at(Some(&heads));
        let visible = am.ops().iter_range(&range).visible(am.ops(), &read);
        assert!(matches!(visible.inner().skipper(), VisIter::Scan(_)));
    }
}
