use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::iter::tools::{DiffIter, ExIdPromise, Shiftable, Unshift};
use crate::iter::Diff;
use crate::op_set2::op_set::{ActionIter, InsertAcc, OpIdIter, ValueIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::op_set2::OpSet;
use crate::patches::PatchLog;
use crate::types::{ObjId, OpId, TextEncoding};

use std::fmt::Debug;
use std::ops::{Bound, Range, RangeBounds};

#[derive(PartialEq, Clone, Debug)]
pub struct ListRangeItem<'a> {
    pub index: usize,
    pub value: ValueRef<'a>,
    pub conflict: bool,
    pub(crate) maybe_exid: ExIdPromise<'a>,
}

impl ListRangeItem<'_> {
    pub(crate) fn op_id(&self) -> OpId {
        self.maybe_exid.id
    }

    pub fn id(&self) -> ExId {
        self.maybe_exid.exid()
    }
    pub fn into_owned(self) -> ListRangeItem<'static> {
        ListRangeItem {
            index: self.index,
            value: self.value.into_owned(),
            conflict: self.conflict,
            maybe_exid: self.maybe_exid.into_owned(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ListDiff<'a> {
    op_set: Option<&'a OpSet>,
    iter: Unshift<DiffIter<'a, ListIter<'a>>>,
    index: usize,
    clock: ClockRange,
}

impl<'a> ListDiff<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: ClockRange) -> Self {
        let inserts = op_set.insert_acc_range(&range);
        let action = op_set.action_iter_range(&range);
        let value = op_set.value_iter_range(&range);
        let id = op_set.id_iter_range(&range);

        let list_iter = ListIter {
            action,
            inserts,
            value,
            id,
        };

        let skip = DiffIter::new(op_set, list_iter, clock.clone(), range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            clock,
            index: 0,
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.shift(range);
        self.index = 0;
        self.next()
    }
}

#[derive(Debug, Clone, Default)]
struct ListState {
    num_old: usize,
    num_new: usize,
    conflict: bool,
    expose: bool,
    inc: i64,
}
impl ListState {
    fn diff_item<'a>(
        &self,
        id: OpId,
        value: ValueRef<'a>,
        index: usize,
        mut diff: Diff,
    ) -> ListDiffItem<'a> {
        if self.expose && diff == Diff::Same {
            diff = Diff::Add;
        }
        let update = diff == Diff::Add && self.num_old > 0;
        ListDiffItem {
            diff,
            value,
            inc: self.inc,
            index,
            update,
            conflict: self.conflict,
            expose: self.expose,
            id,
        }
    }
}

impl<'a> Iterator for ListDiff<'a> {
    type Item = ListDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op_set = self.op_set.as_mut()?;
        let mut last_is_same = false;
        //let mut expose;
        let mut last_visible: Option<Self::Item> = None;
        //let mut num_new = 0;
        //let mut num_old = 0;
        let mut state = ListState::default();

        while let Some((diff, list)) = self.iter.next() {
            match diff {
                Diff::Del => {
                    state.num_old += 1;
                    state.expose = last_is_same;
                }
                Diff::Same => {
                    last_is_same = true;
                    state.num_old += 1;
                    state.num_new += 1;
                    state.expose = false;
                }
                Diff::Add => {
                    last_is_same = false;
                    state.num_new += 1;
                    state.expose = self.clock.predates(&list.id);
                }
            }

            let value = if let ScalarValue::Counter(c) = &list.value {
                let (inc1, inc2) = op_set.get_increment_diff_at_pos(list.pos, &self.clock);
                state.inc = inc2 - inc1;
                ValueRef::from_action_value(list.action, ScalarValue::Counter(*c + inc2))
            } else {
                state.inc = 0;
                ValueRef::from_action_value(list.action, list.value.clone())
            };

            let old_conflict = diff == Diff::Same && state.num_old > 1;
            state.conflict = state.num_new > 1 && !old_conflict;

            if let Some((next_diff, next_list)) = self.iter.peek() {
                if next_list.inserts == list.inserts {
                    if diff.is_visible() && next_diff.is_del() {
                        last_visible = Some(state.diff_item(list.id, value, self.index, diff));
                    }
                    continue;
                }
            }

            if let Some(mut last) = last_visible {
                last.update(state.expose);
                if last.diff.is_visible() {
                    self.index += 1;
                }
                return Some(last);
            } else {
                let list = state.diff_item(list.id, value, self.index, diff);
                if diff.is_visible() {
                    self.index += 1;
                }
                return Some(list);
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListDiffItem<'a> {
    pub(crate) diff: Diff,
    pub(crate) value: ValueRef<'a>,
    pub(crate) inc: i64,
    pub(crate) index: usize,
    pub(crate) conflict: bool,
    pub(crate) update: bool,
    pub(crate) expose: bool,
    pub(crate) id: OpId,
}

impl<'a> ListDiffItem<'a> {
    pub(crate) fn export(self, op_set: &'a OpSet) -> ListRangeItem<'a> {
        let maybe_exid = ExIdPromise::new(op_set, self.id);
        ListRangeItem {
            index: self.index,
            value: self.value,
            conflict: self.conflict,
            maybe_exid,
        }
    }
    pub(crate) fn log(self, obj: ObjId, log: &mut PatchLog, encoding: TextEncoding) {
        let Self {
            diff,
            update,
            index,
            value,
            inc,
            id,
            conflict,
            expose,
        } = self;
        match diff {
            Diff::Add => {
                let value = value.hydrate(encoding);
                if update {
                    log.put_seq(obj, index, value, id, conflict, expose);
                } else {
                    log.insert_and_maybe_expose(obj, index, value, id, conflict, expose);
                }
            }
            Diff::Same => {
                if inc != 0 {
                    log.increment_seq(obj, index, inc, id);
                } else if conflict {
                    log.flag_conflict_seq(obj, index);
                }
            }
            Diff::Del => log.delete_seq(obj, index, 1),
        }
    }

    fn update(&mut self, expose: bool) {
        self.expose |= expose;
        if self.expose && self.diff == Diff::Same {
            self.diff = Diff::Add;
        }
        self.update = true;
    }
}

#[derive(Clone, Default, Debug)]
pub struct ListRange<'a> {
    iter: ListDiff<'a>,
    range: Range<usize>,
}

#[derive(Clone, Default, Debug)]
struct ListIter<'a> {
    id: OpIdIter<'a>,
    inserts: InsertAcc<'a>,
    action: ActionIter<'a>,
    value: ValueIter<'a>,
}

impl Shiftable for ListIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let id = self.id.shift_next(range.clone());
        let action = self.action.shift_next(range.clone());
        let value = self.value.shift_next(range.clone());
        let inserts = self.inserts.shift_next(range);

        let inserts = inserts?.as_usize();
        let pos = self.id.pos() - 1;
        Some(List::new(inserts, action?, value?, id?, pos))
    }
}

#[derive(Clone, Debug)]
struct List<'a> {
    inserts: usize,
    action: Action,
    value: ScalarValue<'a>,
    id: OpId,
    pos: usize,
}

impl<'a> List<'a> {
    fn new(inserts: usize, action: Action, value: ScalarValue<'a>, id: OpId, pos: usize) -> Self {
        Self {
            inserts,
            action,
            value,
            id,
            pos,
        }
    }
}

impl<'a> Iterator for ListIter<'a> {
    type Item = List<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.next()?;
        let inserts = self.inserts.next()?.as_usize();
        let action = self.action.next()?;
        let value = self.value.next()?;
        let pos = self.id.pos() - 1;
        Some(List::new(inserts, action, value, id, pos))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let id = self.id.nth(n)?;
        let inserts = self.inserts.nth(n)?.as_usize();
        let action = self.action.nth(n)?;
        let value = self.value.nth(n)?;
        let pos = self.id.pos() - 1;
        Some(List::new(inserts, action, value, id, pos))
    }
}

impl<'a> ListRange<'a> {
    pub(crate) fn new<R: RangeBounds<usize>>(
        op_set: &'a OpSet,
        obj_range: Range<usize>,
        clock: Option<Clock>,
        range: R,
    ) -> Self {
        let (start, end) = normalize_range(range);
        let range = start..end;

        let clock = ClockRange::Current(clock);
        let iter = ListDiff::new(op_set, obj_range, clock);
        Self { range, iter }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.iter.shift(range);
        self.iter.index = 0;
        self.next()
    }
}

impl<'a> Iterator for ListRange<'a> {
    type Item = ListRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next()?;
            if !self.range.contains(&item.index) {
                continue;
            }
            return Some(item.export(self.iter.op_set?));
        }
    }
}

fn normalize_range<R: RangeBounds<usize>>(range: R) -> (usize, usize) {
    let start = match range.start_bound() {
        Bound::Unbounded => usize::MIN,
        Bound::Included(n) => *n,
        Bound::Excluded(n) => *n - 1,
    };

    let end = match range.end_bound() {
        Bound::Unbounded => usize::MAX,
        Bound::Included(n) => *n + 1,
        Bound::Excluded(n) => *n,
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Transactable;
    use crate::types;
    use crate::{Automerge, ObjType, ReadDoc, ROOT};

    #[test]
    fn list_range_bounds() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.put_object(&ROOT, "list", ObjType::List).unwrap();
        let values = [1, 2, 3, 4, 5]
            .into_iter()
            .map(types::ScalarValue::Int)
            .collect::<Vec<_>>();
        tx.splice(&list, 0, 0, values.clone()).unwrap();
        tx.commit();
        let values = values.into_iter().map(ValueRef::from).collect::<Vec<_>>();
        let v: Vec<_> = doc.list_range(&list, ..).map(|v| v.value).collect();
        assert_eq!(&v, &values[..]);
        let v: Vec<_> = doc.list_range(&list, 2..).map(|v| v.value).collect();
        assert_eq!(&v, &values[2..]);
        let v: Vec<_> = doc.list_range(&list, 1..4).map(|v| v.value).collect();
        assert_eq!(&v, &values[1..4]);
        let v: Vec<_> = doc.list_range(&list, ..3).map(|v| v.value).collect();
        assert_eq!(&v, &values[..3]);
        let v: Vec<_> = doc.list_range(&list, ..=3).map(|v| v.value).collect();
        assert_eq!(&v, &values[..=3]);
        let v: Vec<_> = doc.list_range(&list, 1..=3).map(|v| v.value).collect();
        assert_eq!(&v, &values[1..=3]);
    }

    #[test]
    fn list_range_conflict() {
        let actor1 = "aaaaaaaa".try_into().unwrap();
        let actor2 = "bbbbbbbb".try_into().unwrap();
        let mut doc1 = Automerge::new().with_actor(actor1);
        let mut tx1 = doc1.transaction();
        let list = tx1.put_object(&ROOT, "list", ObjType::List).unwrap();
        let values = [1, 2, 3, 4, 5]
            .into_iter()
            .map(types::ScalarValue::Int)
            .collect::<Vec<_>>();
        tx1.splice(&list, 0, 0, values.clone()).unwrap();
        tx1.commit();

        let mut doc2 = doc1.fork().with_actor(actor2);

        let mut tx2 = doc2.transaction();
        tx2.put(&list, 3, 11).unwrap();
        tx2.commit();

        let mut tx1 = doc1.transaction();
        tx1.put(&list, 3, 10).unwrap();
        tx1.commit();

        doc2.merge(&mut doc1).unwrap();

        let list_vals: Vec<_> = doc2.list_range(&list, ..).collect();
        let conflict: Vec<_> = list_vals.iter().map(|v| v.conflict).collect();
        let vals: Vec<_> = list_vals.into_iter().map(|v| v.value).collect();

        assert_eq!(vals.len(), values.len());
        assert_eq!(vals[3], 11.into());
        assert_eq!(conflict, vec![false, false, false, true, false]);
    }
}
