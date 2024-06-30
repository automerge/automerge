use crate::{marks::MarkSet, types::Clock};

use super::{KeyOpIter, MarkIter, Op, OpIter, TopOpIter, Verified, VisibleOpIter};

use std::sync::Arc;

// is visible needs to compute counter
// op_iter().is_visible(clock)
//    id, succ, action, *value
// op_iter().visible(clock)

pub(crate) trait HasOpScope<'a>: Iterator<Item = Op<'a>> {
    fn get_opiter(&self) -> &OpIter<'a, Verified>;

    fn get_marks(&self) -> Option<&Arc<MarkSet>>;
}

pub(crate) trait OpScope<'a>: HasOpScope<'a> + Clone {
    fn marks(self) -> MarkIter<'a, Self> {
        MarkIter::new(self)
    }

    fn top_ops(self) -> TopOpIter<'a, Self> {
        TopOpIter::new(self)
    }

    fn key_ops(self) -> KeyOpIter<'a, Self> {
        KeyOpIter::new(self)
    }

    fn visible_ops(self, clock: Option<Clock>) -> VisibleOpIter<'a, Self> {
        VisibleOpIter::new(self, clock)
    }
}

impl<'a> HasOpScope<'a> for OpIter<'a, Verified> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        None
    }
}

//impl<'a> OpScope<'a> for OpIter<'a, Verified> {}

impl<'a, I: HasOpScope<'a> + Clone> OpScope<'a> for I {}
