use std::fmt;
use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::{Clock, Key};
use crate::value::Value;

use super::TopOps;

/// Iterator created by the [`crate::ReadDoc::map_range()`] and [`crate::ReadDoc::map_range_at()`] methods
#[derive(Clone)]
pub struct MapRange<'a, R: RangeBounds<String>> {
    iter: Option<MapRangeInner<'a, R>>,
}

#[derive(Clone)]
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
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|inner| {
            for top in inner.iter.by_ref() {
                if let Key::Map(n) = top.op.key() {
                    if let Some(prop) = inner.op_set.osd.props.safe_get(*n) {
                        if inner.range.contains(prop) {
                            return Some(MapRangeItem {
                                key: prop.as_str(),
                                value: top.op.value_at(inner.clock.as_ref()),
                                id: top.op.exid(),
                                conflict: top.conflict,
                            });
                        }
                    }
                }
            }
            None
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: &'a str,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
}

impl<'a> MapRangeItem<'a> {
    pub fn new(key: &'a str, value: Value<'a>, id: ExId, conflict: bool) -> MapRangeItem<'a> {
        MapRangeItem {
            key,
            value,
            id,
            conflict,
        }
    }
}
