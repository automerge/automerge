use crate::{
    exid::ExId,
    marks::{MarkSet},
    types::{ListEncoding},
};
use super::{ Value, Op, OpScope, VisibleOpIter, MarkIter, TopOpIter, OpIter, Verified };

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
    iter: TopOpIter<'a, VisibleOpIter<'a, MarkIter<'a, OpIter<'a, Verified>>>>,
    range: Option<R>,
    state: usize,
    encoding: ListEncoding,
    op_set: Option<&'a super::OpSet>,
}

impl<'a, R: RangeBounds<usize>> Default for ListRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: Default::default(),
            range: None,
            state: 0,
            encoding: ListEncoding::default(),
            op_set: None,
        }
    }
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(
        iter: TopOpIter<'a, VisibleOpIter<'a, MarkIter<'a, OpIter<'a, Verified>>>>,
        range: R,
        encoding: ListEncoding,
        op_set: &'a super::OpSet,
    ) -> Self {
        Self {
            iter,
            range: Some(range),
            state: 0,
            encoding,
            op_set: Some(op_set),
        }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = ListRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op_set = self.op_set?;
        while let Some(op) = self.iter.next() {
            let index = self.state;
            self.state += op.width(self.encoding);
            if !self.range.as_ref()?.contains(&index) {
                // stop if past end
                continue;
            }
            let conflict = op.conflict;
            let value = op.value(); // value_at()
            let id = op_set.id_to_exid(op.id);
            let marks = self.iter.get_marks();
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

