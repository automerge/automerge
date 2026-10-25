use super::tools::{value_at, Diff, DiffIter, ExIdPromise, Shiftable, Unshift};
use crate::automerge::view::{ClockRange, ReadAt};
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
            // a counter both incremented and newly conflicted is put whole:
            // the conflict flag only travels on a put
            Diff::Same if self.inc != 0 && self.conflict => log.put_map(
                obj,
                self.key,
                self.value.hydrate(encoding),
                self.id,
                true,
                false,
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
    fn get_pos(&self) -> usize {
        self.action.get_pos()
    }

    fn get_max(&self) -> usize {
        self.action.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.action.set_max(pos);
        self.key_str.set_max(pos);
    }

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
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, read: ReadAt<'a>) -> Self {
        let iter = MapDiff::new(op_set, range, read.into_range());
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
    value: ValueIter<'a>,
    clock: ClockRange<'a>,
}

impl<'a> Iterator for MapDiff<'a> {
    type Item = MapDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op_set = self.op_set?;
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
                    let newly_visible = last.diff == Diff::Same;
                    last.expose |= expose || newly_visible;
                    // Deleting the winning value exposes `last`, so its put
                    // patch must describe the conflict state after deletion,
                    // not merely whether the conflict is newly created.
                    last.conflict = num_new > 1;
                    return Some(self.materialize(op_set, last));
                }
            }
            let mut item = PendingMapDiffItem {
                diff,
                map,
                conflict,
                expose,
            };
            if diff == Diff::Same && num_old > 1 && num_new == 1 {
                // The surviving value is unchanged, but removing the other
                // visible values clears its conflict flag. Emit a Put so a
                // hydrated-state consumer can observe that metadata change.
                item.expose = true;
            }
            return Some(self.materialize(op_set, item));
        }
        None
    }
}

impl<'a> MapDiff<'a> {
    fn materialize(
        &mut self,
        op_set: &'a OpSet,
        pending: PendingMapDiffItem<'a>,
    ) -> MapDiffItem<'a> {
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
        let id = self.id.seek_to(pos).unwrap();
        if diff == Diff::Add && self.clock.predates(&id) {
            expose = true;
        }
        let value = self.value.seek_to(pos).unwrap_or(ScalarValue::Null);
        let (value, inc) = value_at(op_set, value, pos, action, &self.clock);
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

    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: ClockRange<'a>) -> Self {
        let key_str = op_set.key_str_iter_range(&range);
        let action = op_set.action_iter_range(&range);
        let id = op_set.id_iter();
        let value = op_set.value_iter();

        let map_iter = MapIter { key_str, action };

        let skip = DiffIter::new(op_set, map_iter, clock.clone(), range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            id,
            value,
            clock,
        }
    }

    /// Forward only.
    pub(crate) fn shift(&mut self, range: Range<usize>) {
        self.iter.shift(range);
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.shift(range);
        self.next()
    }
}
