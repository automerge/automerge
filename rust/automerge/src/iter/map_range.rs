use std::fmt;
use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::{Clock, Key};
use crate::value::Value;

use super::TopOps;

// this iterator is created by the Automerge::map_range() and
// Automerge::map_range_at() methods

pub struct MapRange<'a, R: RangeBounds<String>> {
    iter: Option<MapRangeInner<'a, R>>,
}

struct MapRangeInner<'a, R: RangeBounds<String>> {
    iter: TopOps<'a>,
    op_set: &'a OpSet,
    range: R,
    clock: Option<Clock>,
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new(iter: TopOps<'a>, op_set: &'a OpSet, range: R, clock: Option<Clock>) -> Self {
        MapRange {
            iter: Some(MapRangeInner {
                iter,
                op_set,
                range,
                clock,
            }),
        }
    }
}

impl<'a, R: RangeBounds<String>> Default for MapRange<'a, R> {
    fn default() -> Self {
        MapRange { iter: None }
    }
}

impl<'a, R: RangeBounds<String>> fmt::Debug for MapRange<'a, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapRange").finish()
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = (&'a str, Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|inner| {
            for op in inner.iter.by_ref() {
                if let Key::Map(n) = &op.key {
                    if let Some(prop) = inner.op_set.m.props.safe_get(*n) {
                        if inner.range.contains(prop) {
                            return Some((
                                prop.as_str(),
                                op.value_at(inner.clock.as_ref()),
                                inner.op_set.id_to_exid(op.id),
                            ));
                        }
                    }
                }
            }
            None
        })
    }
}
