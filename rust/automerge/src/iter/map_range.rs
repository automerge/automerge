use super::tools::{Diff, DiffIter, ExIdPromise, Shiftable, Unshift};
use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::op_set2::op_set::{ActionIter, OpIdIter, OpSet, ValueIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::patches::PatchAccumulator;
use crate::types::{ObjId, OpId, TextEncoding};

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
    pub(crate) key: &'a str,
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
            key: Cow::Borrowed(self.key),
            value: self.value,
            conflict: self.conflict,
            pos: self.pos,
            maybe_exid,
        }
    }
    pub(crate) fn log(self, obj: ObjId, log: &mut PatchAccumulator, encoding: TextEncoding) {
        match self.diff {
            Diff::Add => log.put_map(
                obj,
                self.key,
                self.value.hydrate(encoding),
                self.id,
                self.conflict,
                self.expose,
            ),
            Diff::Same => {
                if self.inc != 0 {
                    log.increment_map(obj, self.key, self.inc, self.id);
                } else if self.conflict {
                    log.flag_conflict_map(obj, self.key);
                }
            }
            Diff::Del => log.delete_map(obj, self.key),
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
    key_str: hexane::Iter<'a, Option<String>>,
    action: ActionIter<'a>,
}

impl Shiftable for MapIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let key = self.key_str.shift_next(range.clone());
        let action = self.action.shift_next(range);
        let pos = self.action.pos() - 1;
        Some(MapEntry {
            key: key??,
            action: action?,
            pos,
        })
    }
}

#[derive(Clone, Debug)]
struct MapEntry<'a> {
    pos: usize,
    key: &'a str,
    action: Action,
}

#[derive(Clone, Debug)]
struct PendingMapDiffItem<'a> {
    diff: Diff,
    map: MapEntry<'a>,
    conflict: bool,
    expose: bool,
}

impl PendingMapDiffItem<'_> {
    fn update(&mut self, expose: bool) {
        self.expose |= expose;
        if self.expose && self.diff == Diff::Same {
            self.diff = Diff::Add;
        }
    }
}

impl<'a> Iterator for MapIter<'a> {
    type Item = MapEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.key_str.next()??;
        let action = self.action.next()?;
        let pos = self.action.pos() - 1;
        Some(MapEntry { pos, key, action })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let key = self.key_str.nth(n)??;
        let action = self.action.nth(n)?;
        let pos = self.action.pos() - 1;
        Some(MapEntry { key, action, pos })
    }
}

impl<'a> MapRange<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: Option<Clock>) -> Self {
        let iter = MapDiff::new(op_set, range, ClockRange::current(clock));
        Self { iter }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        Some(self.iter.shift_next(range)?.export(self.iter.op_set?))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MapDiff<'a> {
    op_set: Option<&'a OpSet>,
    iter: Unshift<DiffIter<'a, MapIter<'a>>>,
    id: OpIdIter<'a>,
    id_pos: usize,
    value: ValueIter<'a>,
    value_pos: usize,
    clock: ClockRange,
}

impl<'a> Iterator for MapDiff<'a> {
    type Item = MapDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.op_set?;
        let mut last_is_same = false;
        let mut num_new = 0;
        let mut num_old = 0;
        let mut expose;
        let mut last_visible: Option<PendingMapDiffItem<'a>> = None;

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
                    expose = false;
                }
            }

            let old_conflict = diff == Diff::Same && num_old > 1;
            let conflict = num_new > 1 && !old_conflict;

            if let Some((next_diff, next_map)) = self.iter.peek() {
                if next_map.key == map.key {
                    if diff.is_visible() && next_diff.is_del() {
                        last_visible = Some(PendingMapDiffItem {
                            diff,
                            map,
                            conflict,
                            expose,
                        });
                    }
                    continue;
                }
            }

            if diff.is_del() {
                if let Some(mut last) = last_visible.take() {
                    last.update(expose);
                    return Some(self.materialize(last));
                }
            }
            return Some(self.materialize(PendingMapDiffItem {
                diff,
                map,
                conflict,
                expose,
            }));
        }
        None
    }
}

impl<'a> MapDiff<'a> {
    fn materialize(&mut self, pending: PendingMapDiffItem<'a>) -> MapDiffItem<'a> {
        let PendingMapDiffItem {
            mut diff,
            map,
            conflict,
            mut expose,
        } = pending;
        if expose && diff == Diff::Same {
            diff = Diff::Add;
        }
        let MapEntry { pos, key, action } = map;
        let id = self.id_at(pos);
        if diff == Diff::Add && self.clock.predates(&id) {
            expose = true;
        }
        let (value, inc) = self.value_at(pos, action);
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

    fn id_at(&mut self, pos: usize) -> OpId {
        let skip = pos.saturating_sub(self.id_pos);
        let id = self.id.nth(skip).unwrap_or(crate::types::ROOT);
        self.id_pos = pos + 1;
        id
    }

    fn value_at(&mut self, pos: usize, action: Action) -> (ValueRef<'a>, i64) {
        let Some(op_set) = self.op_set else {
            return (ValueRef::Scalar(ScalarValue::Null), 0);
        };
        let skip = pos.saturating_sub(self.value_pos);
        let value = self.value.nth(skip).unwrap_or(ScalarValue::Null);
        self.value_pos = pos + 1;
        match action {
            Action::MakeMap | Action::MakeList | Action::MakeText | Action::MakeTable => {
                (ValueRef::from_action_value(action, ScalarValue::Null), 0)
            }
            Action::Delete => (ValueRef::Scalar(ScalarValue::Null), 0),
            _ => {
                if let ScalarValue::Counter(c) = &value {
                    let (inc1, inc2) = op_set.get_increment_diff_at_pos(pos, &self.clock);
                    (
                        ValueRef::from_action_value(action, ScalarValue::Counter(*c + inc2)),
                        inc2 - inc1,
                    )
                } else {
                    (ValueRef::from_action_value(action, value), 0)
                }
            }
        }
    }

    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: ClockRange) -> Self {
        let key_str = op_set.key_str_iter_range(&range);
        let action = op_set.action_iter_range(&range);
        let id = op_set.id_iter_range(&range);
        let id_pos = range.start;
        let value = op_set.value_iter_range(&range);
        let value_pos = range.start;

        let map_iter = MapIter { key_str, action };

        let skip = DiffIter::new(op_set, map_iter, clock.clone(), range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            id,
            id_pos,
            value,
            value_pos,
            clock,
        }
    }

    pub(crate) fn new_with_baseline_before(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: ClockRange,
    ) -> Self {
        let key_str = op_set.key_str_iter_range(&range);
        let action = op_set.action_iter_range(&range);
        let id = op_set.id_iter_range(&range);
        let id_pos = range.start;
        let value = op_set.value_iter_range(&range);
        let value_pos = range.start;

        let map_iter = MapIter { key_str, action };

        let skip = DiffIter::new_with_baseline_before(op_set, map_iter, range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            id,
            id_pos,
            value,
            value_pos,
            clock,
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.shift(range.clone());
        if let Some(op_set) = self.op_set {
            self.id = op_set.id_iter_range(&range);
            self.id_pos = range.start;
            self.value = op_set.value_iter_range(&range);
            self.value_pos = range.start;
        }
        self.next()
    }
}
