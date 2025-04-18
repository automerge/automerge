use super::tools::{ExIdPromise, Shiftable, SkipIter, Unshift};
use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set2::hexane::{ColumnDataIter, StrCursor};
use crate::op_set2::op_set::{ActionIter, OpIdIter, OpSet, ValueIter, VisIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::types::OpId;

use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: Cow<'a, str>,
    pub value: ValueRef<'a>,
    pub conflict: bool,
    pub(crate) pos: usize,
    pub(crate) maybe_exid: ExIdPromise<'a>,
}

impl MapRangeItem<'_> {
    pub fn id(&self) -> ExId {
        self.maybe_exid.exid()
    }

    pub fn into_owned(self) -> MapRangeItem<'static> {
        MapRangeItem {
            key: Cow::Owned(self.key.into_owned()),
            value: self.value.into_owned(),
            conflict: self.conflict,
            pos: self.pos,
            maybe_exid: self.maybe_exid.into_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MapRangeInner<'a> {
    iter: Unshift<SkipIter<MapIter<'a>, VisIter<'a>>>,
    clock: Option<Clock>,
    op_set: &'a OpSet,
}

#[derive(Debug, Clone, Default)]
pub struct MapRange<'a> {
    inner: Option<MapRangeInner<'a>>,
}

impl<'a> Iterator for MapRange<'a> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut conflict = false;
        let inner = self.inner.as_mut()?;

        while let Some((key, action, value, _id, pos)) = inner.iter.next() {
            if let Some((next_key, _, _, _, _)) = inner.iter.peek() {
                if next_key == &key {
                    conflict = true;
                    continue;
                }
            }
            let value = if let ScalarValue::Counter(c) = &value {
                let inc = inner.op_set.get_increment_at_pos(pos, inner.clock.as_ref());
                ValueRef::from_action_value(action, ScalarValue::Counter(*c + inc))
            } else {
                ValueRef::from_action_value(action, value)
            };
            let maybe_exid = ExIdPromise::new(inner.op_set, _id);
            return Some(MapRangeItem {
                key,
                value,
                conflict,
                maybe_exid,
                pos,
            });
        }
        None
    }
}

#[derive(Clone, Debug)]
struct MapIter<'a> {
    id: OpIdIter<'a>,
    key_str: ColumnDataIter<'a, StrCursor>,
    action: ActionIter<'a>,
    value: ValueIter<'a>,
}

impl Shiftable for MapIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let id = self.id.shift_next(range.clone());
        let key_str = self.key_str.shift_next(range.clone());
        let action = self.action.shift_next(range.clone());
        let value = self.value.shift_next(range);
        let pos = self.id.pos() - 1;
        Some((key_str??, action?, value?, id?, pos))
    }
}

impl<'a> Iterator for MapIter<'a> {
    type Item = (Cow<'a, str>, Action, ScalarValue<'a>, OpId, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.next()?;
        let key_str = self.key_str.next()??;
        let action = self.action.next()?;
        let value = self.value.next()?;
        let pos = self.id.pos() - 1;
        Some((key_str, action, value, id, pos))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let id = self.id.nth(n)?;
        let key_str = self.key_str.nth(n)??;
        let action = self.action.nth(n)?;
        let value = self.value.nth(n)?;
        let pos = self.id.pos() - 1;
        Some((key_str, action, value, id, pos))
    }
}

impl<'a> MapRange<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: Option<Clock>) -> Self {
        let key_str = op_set.key_str_iter_range(&range);
        let action = op_set.action_iter_range(&range);
        let value = op_set.value_iter_range(&range);
        let id = op_set.id_iter_range(&range);

        let map_iter = MapIter {
            id,
            key_str,
            value,
            action,
        };

        let vis = VisIter::new(op_set, clock.as_ref(), range);
        let skip = SkipIter::new(map_iter, vis);
        let iter = Unshift::new(skip);

        Self {
            inner: Some(MapRangeInner {
                iter,
                op_set,
                clock,
            }),
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let inner = self.inner.as_mut()?;
        inner.iter.shift(range);
        self.next()
    }
}
