use super::{HasOpScope, MarkIter, Op, OpIter, TopOpIter, Value, Verified, VisibleOpIter};
use crate::{exid::ExId, marks::MarkSet, types::ListEncoding};

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

#[derive(Debug)]
pub struct ListRangeItem<'a> {
    pub index: usize,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

pub struct ListRange<'a, R: RangeBounds<usize>> {
    iter: Option<Box<dyn HasOpScope<'a> + 'a>>,
    range: Option<R>,
    state: usize,
    encoding: ListEncoding,
}

impl<'a, R: RangeBounds<usize>> Default for ListRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: None,
            range: None,
            state: 0,
            encoding: ListEncoding::default(),
        }
    }
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(
        iter: TopOpIter<'a, MarkIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>>,
        range: R,
        encoding: ListEncoding,
    ) -> Self {
        Self {
            iter: Some(Box::new(iter)),
            range: Some(range),
            state: 0,
            encoding,
        }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = ListRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.as_mut()?;
        while let Some(op) = iter.next() {
            let index = self.state;
            self.state += op.width(self.encoding);
            if !self.range.as_ref()?.contains(&index) {
                // stop if past end
                continue;
            }
            let conflict = op.conflict;
            let value = op.value(); // value_at()
            let id = op.exid();
            let marks = iter.get_marks().cloned();
            // todo : need a value_at (vis?) iterator!!
            // let marks = self.marks.current().cloned()
            return Some(ListRangeItem {
                index,
                value,
                id,
                conflict,
                marks,
            });
        }
        None
    }
}