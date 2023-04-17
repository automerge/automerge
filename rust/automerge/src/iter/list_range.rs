use std::fmt;
use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::Clock;
use crate::types::ListEncoding;
use crate::value::Value;

use super::TopOps;

pub struct ListRange<'a, R: RangeBounds<usize>> {
    pub(crate) iter: Option<ListRangeInner<'a, R>>,
}

pub(crate) struct ListRangeInner<'a, R: RangeBounds<usize>> {
    pub(crate) iter: TopOps<'a>,
    pub(crate) op_set: &'a OpSet,
    pub(crate) state: usize,
    pub(crate) encoding: ListEncoding,
    pub(crate) range: R,
    pub(crate) clock: Option<Clock>,
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
    type Item = (usize, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|inner| {
            for op in inner.iter.by_ref() {
                let index = inner.state;
                inner.state += op.width(inner.encoding);
                if inner.range.contains(&index) {
                    return Some((
                        index,
                        op.value_at(inner.clock.as_ref()),
                        inner.op_set.id_to_exid(op.id),
                    ));
                }
            }
            None
        })
    }
}
