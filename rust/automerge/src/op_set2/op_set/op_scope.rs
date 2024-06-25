use crate::{
    marks::{MarkSet},
    types::{Clock},
};

use super::{Op, OpIter, Verified, VisibleOpIter, TopOpIter, MarkIter, KeyOpIter};

use std::sync::Arc;

pub(crate) trait OpScope<'a>: Iterator<Item = Op<'a>> + Clone + Default {
    fn get_opiter(&self) -> &OpIter<'a, Verified>;

    fn get_marks(&self) -> Option<Arc<MarkSet>>;

    fn marks(self, clock: Option<Clock>) -> MarkIter<'a, Self> {
        MarkIter::new(self, clock)
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

impl<'a> OpScope<'a> for OpIter<'a, Verified> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self
    }

    fn get_marks(&self) -> Option<Arc<MarkSet>> {
        None
    }
}


