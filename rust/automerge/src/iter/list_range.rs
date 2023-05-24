use std::fmt;
use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::Clock;
use crate::types::ListEncoding;
use crate::value::Value;

use super::TopOps;

/// Iterator created by the [`crate::ReadDoc::list_range()`] and [`crate::ReadDoc::list_range_at()`] methods
pub struct ListRange<'a, R: RangeBounds<usize>> {
    iter: Option<ListRangeInner<'a, R>>,
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(
        iter: TopOps<'a>,
        op_set: &'a OpSet,
        encoding: ListEncoding,
        range: R,
        clock: Option<Clock>,
    ) -> Self {
        Self {
            iter: Some(ListRangeInner {
                iter,
                op_set,
                state: 0,
                encoding,
                range,
                clock,
            }),
        }
    }
}

struct ListRangeInner<'a, R: RangeBounds<usize>> {
    iter: TopOps<'a>,
    op_set: &'a OpSet,
    state: usize,
    encoding: ListEncoding,
    range: R,
    clock: Option<Clock>,
}

impl<'a, R: RangeBounds<usize>> Default for ListRange<'a, R> {
    fn default() -> Self {
        ListRange { iter: None }
    }
}

impl<'a, R: RangeBounds<usize>> fmt::Debug for ListRange<'a, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListRange").finish()
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = (usize, Value<'a>, ExId, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|inner| {
            for top in inner.iter.by_ref() {
                let index = inner.state;
                inner.state += top.op.width(inner.encoding);
                if inner.range.contains(&index) {
                    return Some((
                        index,
                        top.op.value_at(inner.clock.as_ref()),
                        inner.op_set.id_to_exid(top.op.id),
                        top.conflict,
                    ));
                }
            }
            None
        })
    }
}
