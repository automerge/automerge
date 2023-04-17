use std::fmt;
use std::ops::RangeBounds;

use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::{Clock, Key};
use crate::value::Value;

use super::TopOps;

pub struct MapRange<'a, R: RangeBounds<String>> {
    pub(crate) iter: Option<MapRangeInner<'a, R>>,
}

pub(crate) struct MapRangeInner<'a, R: RangeBounds<String>> {
    pub(crate) iter: TopOps<'a>,
    pub(crate) op_set: &'a OpSet,
    pub(crate) range: R,
    pub(crate) clock: Option<Clock>,
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
