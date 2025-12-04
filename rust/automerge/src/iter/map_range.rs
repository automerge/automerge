use super::tools::{Diff, DiffIter, ExIdPromise, Shiftable, Unshift};
use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::op_set2::op_set::{ActionIter, OpIdIter, OpSet, ValueIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::patches::PatchLog;
use crate::types::{ObjId, OpId, TextEncoding};

use hexane::{ColumnDataIter, StrCursor};

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
    pub(crate) fn op_id(&self) -> OpId {
        self.maybe_exid.id
    }

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MapDiffItem<'a> {
    pub(crate) diff: Diff,
    pub(crate) key: Cow<'a, str>,
    pub(crate) value: ValueRef<'a>,
    pub(crate) inc: i64,
    pub(crate) conflict: bool,
    pub(crate) expose: bool,
    pub(crate) pos: usize,
    pub(crate) id: OpId,
}

impl<'a> MapDiffItem<'a> {
    pub(crate) fn export(self, op_set: &'a OpSet) -> MapRangeItem<'a> {
        let maybe_exid = ExIdPromise::new(op_set, self.id);
        MapRangeItem {
            key: self.key,
            value: self.value,
            conflict: self.conflict,
            pos: self.pos,
            maybe_exid,
        }
    }
    fn update(&mut self, expose: bool) {
        self.expose |= expose;
        if self.expose && self.diff == Diff::Same {
            self.diff = Diff::Add;
        }
    }

    pub(crate) fn log(self, obj: ObjId, log: &mut PatchLog, encoding: TextEncoding) {
        match self.diff {
            Diff::Add => log.put_map(
                obj,
                &self.key,
                self.value.hydrate(encoding),
                self.id,
                self.conflict,
                self.expose,
            ),
            Diff::Same => {
                if self.inc != 0 {
                    log.increment_map(obj, &self.key, self.inc, self.id);
                } else if self.conflict {
                    log.flag_conflict_map(obj, &self.key);
                }
            }
            Diff::Del => log.delete_map(obj, &self.key),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MapRange<'a> {
    iter: MapDiff<'a>,
}

impl<'a> Iterator for MapRange<'a> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.export(self.iter.op_set?))
    }
}

#[derive(Clone, Default, Debug)]
struct MapIter<'a> {
    id: OpIdIter<'a>,
    key_str: ColumnDataIter<'a, StrCursor>,
    action: ActionIter<'a>,
    value: ValueIter<'a>,
}

impl Shiftable for MapIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let id = self.id.shift_next(range.clone());
        let key = self.key_str.shift_next(range.clone());
        let action = self.action.shift_next(range.clone());
        let value = self.value.shift_next(range);
        let pos = self.id.pos() - 1;
        Some(MapEntry {
            key: key??,
            action: action?,
            value: value?,
            id: id?,
            pos,
        })
    }
}

#[derive(Clone, Debug)]
struct MapEntry<'a> {
    id: OpId,
    pos: usize,
    key: Cow<'a, str>,
    action: Action,
    value: ScalarValue<'a>,
}

impl<'a> MapEntry<'a> {
    fn diff_item(
        self,
        value: ValueRef<'a>,
        inc: i64,
        mut diff: Diff,
        conflict: bool,
        expose: bool,
    ) -> MapDiffItem<'a> {
        let key = self.key;
        let pos = self.pos;
        let id = self.id;
        if expose && diff == Diff::Same {
            diff = Diff::Add;
        }
        MapDiffItem {
            diff,
            key,
            value,
            inc,
            conflict,
            expose,
            id,
            pos,
        }
    }
}

impl<'a> Iterator for MapIter<'a> {
    type Item = MapEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.next()?;
        let key = self.key_str.next()??;
        let action = self.action.next()?;
        let value = self.value.next()?;
        let pos = self.id.pos() - 1;
        Some(MapEntry {
            id,
            pos,
            key,
            action,
            value,
        })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let id = self.id.nth(n)?;
        let key = self.key_str.nth(n)??;
        let action = self.action.nth(n)?;
        let value = self.value.nth(n)?;
        let pos = self.id.pos() - 1;
        Some(MapEntry {
            key,
            action,
            value,
            id,
            pos,
        })
    }
}

impl<'a> MapRange<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: Option<Clock>) -> Self {
        let iter = MapDiff::new(op_set, range, ClockRange::current(clock));
        Self { iter }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.iter.shift(range);
        self.next()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MapDiff<'a> {
    op_set: Option<&'a OpSet>,
    iter: Unshift<DiffIter<'a, MapIter<'a>>>,
    clock: ClockRange,
}

impl<'a> Iterator for MapDiff<'a> {
    type Item = MapDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op_set = self.op_set.as_mut()?;
        let mut last_is_same = false;
        let mut num_new = 0;
        let mut num_old = 0;
        let mut expose;
        let mut last_visible: Option<Self::Item> = None;

        while let Some((diff, map)) = self.iter.next() {
            match diff {
                Diff::Del => {
                    expose = last_is_same;
                    num_old += 1;
                }
                Diff::Same => {
                    last_is_same = true;
                    num_new += 1;
                    num_old += 1;
                    expose = false;
                }
                Diff::Add => {
                    last_is_same = false;
                    num_new += 1;
                    expose = self.clock.predates(&map.id);
                }
            }
            let value;
            let inc;
            if let ScalarValue::Counter(c) = &map.value {
                let (inc1, inc2) = op_set.get_increment_diff_at_pos(map.pos, &self.clock);
                inc = inc2 - inc1;
                value = ValueRef::from_action_value(map.action, ScalarValue::Counter(*c + inc2));
            } else {
                value = ValueRef::from_action_value(map.action, map.value.clone());
                inc = 0;
            }

            let old_conflict = diff == Diff::Same && num_old > 1;
            let conflict = num_new > 1 && !old_conflict;

            if let Some((next_diff, next_map)) = self.iter.peek() {
                if next_map.key == map.key {
                    if diff.is_visible() && next_diff.is_del() {
                        last_visible = Some(map.diff_item(value, inc, diff, conflict, expose));
                    }
                    continue;
                }
            }

            if diff.is_del() {
                if let Some(mut last) = last_visible.take() {
                    last.update(expose);
                    return Some(last);
                }
            }
            return Some(map.diff_item(value, inc, diff, conflict, expose));
        }
        None
    }
}

impl<'a> MapDiff<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: ClockRange) -> Self {
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

        let skip = DiffIter::new(op_set, map_iter, clock.clone(), range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            clock,
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.shift(range);
        self.next()
    }
}
