use crate::marks::MarkSet;
use crate::types::{Clock, ListEncoding};

use super::{
    DiffOpIter, IndexIter, KeyOpIter, MarkIter, NoMarkIter, Op, OpIter, TopOpIter, VisibleOpIter,
};

use std::fmt::Debug;
use std::sync::Arc;

// is visible needs to compute counter
// op_iter().is_visible(clock)
//    id, succ, action, *value
// op_iter().visible(clock)

pub(crate) trait OpQueryTerm<'a>: Iterator<Item = Op<'a>> + Debug {
    fn get_opiter(&self) -> &OpIter<'a>;

    fn get_marks(&self) -> Option<&Arc<MarkSet>>;
}

pub(crate) trait OpQuery<'a>: OpQueryTerm<'a> + Clone {
    fn marks(self) -> MarkIter<'a, Self> {
        MarkIter::new(self)
    }

    fn no_marks(self) -> NoMarkIter<'a, Self> {
        NoMarkIter::new(self)
    }

    fn index(self, encoding: ListEncoding) -> IndexIter<'a, Self> {
        IndexIter::new(self, encoding)
    }

    fn top_ops(self) -> TopOpIter<'a, Self> {
        TopOpIter::new(self)
    }

    fn key_ops(self) -> KeyOpIter<'a, Self> {
        KeyOpIter::new(self)
    }

    fn visible(self, clock: Option<Clock>) -> VisibleOpIter<'a, Self> {
        VisibleOpIter::new(self, clock)
    }

    fn diff<'b>(self, before: &'b Clock, after: &'b Clock) -> DiffOpIter<'a, 'b, Self> {
        DiffOpIter::new(self, before, after)
    }
}

impl<'a> OpQueryTerm<'a> for OpIter<'a> {
    fn get_opiter(&self) -> &OpIter<'a> {
        self
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        None
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQuery<'a> for I {}
