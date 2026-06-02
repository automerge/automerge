use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::iter::tools::{DiffIter, ExIdPromise, Shiftable, Unshift};
use crate::iter::Diff;
use crate::op_set2::op_set::{ActionIter, InsertAcc, OpIdIter, ValueIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::op_set2::OpSet;
use crate::patches::PatchAccumulator;
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
    id: OpIdIter<'a>,
    id_pos: usize,
    value: ValueIter<'a>,
    value_pos: usize,
    index: usize,
    clock: ClockRange,
}

impl<'a> ListDiff<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>, clock: ClockRange) -> Self {
        Self::new_with_index(op_set, range, clock, 0)
    }

    pub(crate) fn new_with_index(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: ClockRange,
        index: usize,
    ) -> Self {
        let inserts = op_set.insert_acc_range(&range);
        let action = op_set.action_iter_range(&range);
        let id = op_set.id_iter_range(&range);
        let id_pos = range.start;
        let value = op_set.value_iter_range(&range);
        let value_pos = range.start;

        let list_iter = ListIter { action, inserts };

        let skip = DiffIter::new(op_set, list_iter, clock.clone(), range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            id,
            id_pos,
            value,
            value_pos,
            clock,
            index,
        }
    }

    pub(crate) fn new_with_baseline_before(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: ClockRange,
        index: usize,
    ) -> Self {
        let inserts = op_set.insert_acc_range(&range);
        let action = op_set.action_iter_range(&range);
        let id = op_set.id_iter_range(&range);
        let id_pos = range.start;
        let value = op_set.value_iter_range(&range);
        let value_pos = range.start;

        let list_iter = ListIter { action, inserts };

        let skip = DiffIter::new_with_baseline_before(op_set, list_iter, range);
        let iter = Unshift::new(skip);

        Self {
            op_set: Some(op_set),
            iter,
            id,
            id_pos,
            value,
            value_pos,
            clock,
            index,
        }
    }

    fn materialize(&mut self, pending: PendingListDiffItem) -> ListDiffItem<'a> {
        let PendingListDiffItem {
            mut diff,
            list,
            mut state,
            index,
        } = pending;
        if state.expose && diff == Diff::Same {
            diff = Diff::Add;
        }
        let List { action, pos, .. } = list;
        let id = self.id_at(pos);
        if diff == Diff::Add && self.clock.predates(&id) {
            state.expose = true;
        }
        let (value, inc) = self.value_at(pos, action);
        ListDiffItem {
            diff,
            value,
            inc,
            index,
            update: diff == Diff::Add && state.num_old > 0,
            conflict: state.conflict,
            expose: state.expose,
            id,
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

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.shift_next_with_index(range, 0)
    }

    pub(crate) fn shift_next_with_index(
        &mut self,
        range: Range<usize>,
        index: usize,
    ) -> Option<<Self as Iterator>::Item> {
        self.iter.shift(range.clone());
        if let Some(op_set) = self.op_set {
            self.id = op_set.id_iter_range(&range);
            self.id_pos = range.start;
            self.value = op_set.value_iter_range(&range);
            self.value_pos = range.start;
        }
        self.index = index;
        self.next()
    }
}

#[derive(Debug, Clone, Default)]
struct ListState {
    num_old: usize,
    num_new: usize,
    conflict: bool,
    expose: bool,
}

#[derive(Debug, Clone)]
struct PendingListDiffItem {
    diff: Diff,
    list: List,
    state: ListState,
    index: usize,
}

impl<'a> Iterator for ListDiff<'a> {
    type Item = ListDiffItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.op_set?;
        let mut last_is_same = false;
        let mut last_visible: Option<PendingListDiffItem> = None;
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
                    state.expose = false;
                }
            }

            let old_conflict = diff == Diff::Same && state.num_old > 1;
            state.conflict = state.num_new > 1 && !old_conflict;

            if let Some((next_diff, next_list)) = self.iter.peek() {
                if next_list.inserts == list.inserts {
                    if diff.is_visible() && next_diff.is_del() {
                        last_visible = Some(PendingListDiffItem {
                            diff,
                            list,
                            state: state.clone(),
                            index: self.index,
                        });
                    }
                    continue;
                }
            }

            let item = if let Some(mut last) = last_visible {
                last.state.expose |= state.expose;
                if last.state.expose && last.diff == Diff::Same {
                    last.diff = Diff::Add;
                }
                self.materialize(last)
            } else {
                self.materialize(PendingListDiffItem {
                    diff,
                    list,
                    state,
                    index: self.index,
                })
            };
            if item.diff.is_visible() {
                self.index += 1;
            }
            return Some(item);
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
    pub(crate) fn log(self, obj: ObjId, log: &mut PatchAccumulator, encoding: TextEncoding) {
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
}

#[derive(Clone, Default, Debug)]
pub struct ListRange<'a> {
    iter: ListDiff<'a>,
    range: Range<usize>,
}

#[derive(Clone, Default, Debug)]
struct ListIter<'a> {
    inserts: InsertAcc<'a>,
    action: ActionIter<'a>,
}

impl Shiftable for ListIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let action = self.action.shift_next(range.clone());
        let inserts = self.inserts.shift_next(range);

        let inserts = inserts?.total();
        let pos = self.action.pos() - 1;
        Some(List::new(inserts, action?, pos))
    }
}

#[derive(Clone, Debug)]
struct List {
    inserts: usize,
    action: Action,
    pos: usize,
}

impl List {
    fn new(inserts: usize, action: Action, pos: usize) -> Self {
        Self {
            inserts,
            action,
            pos,
        }
    }
}

impl Iterator for ListIter<'_> {
    type Item = List;

    fn next(&mut self) -> Option<Self::Item> {
        let inserts = self.inserts.next()?.total();
        let action = self.action.next()?;
        let pos = self.action.pos() - 1;
        Some(List::new(inserts, action, pos))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let inserts = self.inserts.nth(n)?.total();
        let action = self.action.nth(n)?;
        let pos = self.action.pos() - 1;
        Some(List::new(inserts, action, pos))
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
        Some(self.iter.shift_next(range)?.export(self.iter.op_set?))
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
