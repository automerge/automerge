use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set2::op_set::{ActionIter, OpIdIter, OpSet, SkipIter, ValueIter, VisIter};
use crate::op_set2::packer::{ColumnDataIter, StrCursor};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::types::OpId;

use std::borrow::Cow;
use std::fmt::Debug;
use std::iter::Peekable;
use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: Cow<'a, str>,
    pub value: ValueRef<'a>,
    pub id: ExId,
    pub conflict: bool,
}

#[derive(Debug)]
struct MapRangeInner<'a> {
    iter: Peekable<SkipIter<MapIter<'a>, VisIter<'a>>>,
    clock: Option<Clock>,
    op_set: &'a OpSet,
}

#[derive(Debug, Default)]
pub struct MapRange<'a> {
    inner: Option<MapRangeInner<'a>>,
}

impl<'a> Iterator for MapRange<'a> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut conflict = false;
        let inner = self.inner.as_mut()?;

        while let Some((key, action, value, id, pos)) = inner.iter.next() {
            if let Some((next_key, _, _, _, _)) = inner.iter.peek() {
                if next_key == &key {
                    conflict = true;
                    continue;
                }
            }
            let id = inner.op_set.id_to_exid(id);
            let value = if let ScalarValue::Counter(c) = &value {
                let inc = inner.op_set.get_increment_at_pos(pos, inner.clock.as_ref());
                ValueRef::from_action_value(action, ScalarValue::Counter(*c + inc))
            } else {
                ValueRef::from_action_value(action, value)
            };
            return Some(MapRangeItem {
                key,
                value,
                id,
                conflict,
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
        let start = range.start;

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
        let skip = SkipIter::new_with_offset(map_iter, vis, start);
        let iter = skip.peekable();

        Self {
            inner: Some(MapRangeInner {
                iter,
                op_set,
                clock,
            }),
        }
    }
}
