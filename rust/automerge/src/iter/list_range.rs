use crate::clock::Clock;
use crate::exid::ExId;
use crate::iter::tools::{Peek, Shiftable, SkipIter};
use crate::op_set2::op_set::{ActionIter, InsertAcc, OpIdIter, ValueIter, VisIter};
use crate::op_set2::types::{Action, ScalarValue, ValueRef};
use crate::op_set2::OpSet;
use crate::types::OpId;
use crate::Value;

use std::fmt::Debug;
use std::ops::{Bound, Range, RangeBounds};

#[derive(Clone, Debug)]
pub struct ListRangeItem<'a> {
    pub index: usize,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
    pub(crate) _id: OpId,
}

#[derive(Clone, Default, Debug)]
pub struct ListRange<'a> {
    inner: Option<ListRangeInner<'a>>,
}

#[derive(Clone, Debug)]
struct ListIter<'a> {
    id: OpIdIter<'a>,
    inserts: InsertAcc<'a>,
    action: ActionIter<'a>,
    value: ValueIter<'a>,
}

impl Shiftable for ListIter<'_> {
    fn shift_range(&mut self, range: Range<usize>) {
        self.id.shift_range(range.clone());
        self.inserts.shift_range(range.clone());
        self.action.shift_range(range.clone());
        self.value.shift_range(range);
    }
}

impl<'a> Iterator for ListIter<'a> {
    type Item = (usize, Action, ScalarValue<'a>, OpId, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.id.next()?;
        let inserts = self.inserts.next()?.as_usize();
        let action = self.action.next()?;
        let value = self.value.next()?;
        let pos = self.id.pos() - 1;
        Some((inserts, action, value, id, pos))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let id = self.id.nth(n)?;
        let inserts = self.inserts.nth(n)?.as_usize();
        let action = self.action.nth(n)?;
        let value = self.value.nth(n)?;
        let pos = self.id.pos() - 1;
        Some((inserts, action, value, id, pos))
    }
}

#[derive(Clone, Debug)]
struct ListRangeInner<'a> {
    op_set: &'a OpSet,
    iter: Peek<SkipIter<ListIter<'a>, VisIter<'a>>>,
    clock: Option<Clock>,
    range: Range<usize>,
    state: usize,
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

        let inserts = op_set.insert_acc_range(&obj_range);
        let action = op_set.action_iter_range(&obj_range);
        let value = op_set.value_iter_range(&obj_range);
        let id = op_set.id_iter_range(&obj_range);

        let list_iter = ListIter {
            action,
            inserts,
            value,
            id,
        };

        let start = obj_range.start;
        let vis = VisIter::new(op_set, clock.as_ref(), obj_range);
        let skip = SkipIter::new_with_offset(list_iter, vis, start);
        let iter = Peek::new(skip);

        let inner = ListRangeInner {
            op_set,
            range,
            iter,
            clock,
            state: 0,
        };
        Self { inner: Some(inner) }
    }

    pub(crate) fn shift_range(&mut self, range: Range<usize>) {
        if let Some(inner) = self.inner.as_mut() {
            inner.iter.shift_range(range);
            inner.state = 0;
        }
    }
}

impl<'a> Iterator for ListRange<'a> {
    type Item = ListRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let inner = self.inner.as_mut()?;
        let mut conflict = false;
        while let Some((insert, action, value, _id, pos)) = inner.iter.next() {
            if inner.iter.peek().map(|next| next.0) == Some(insert) {
                conflict = true;
                continue;
            }
            let index = inner.state;
            inner.state += 1;
            if !inner.range.contains(&index) {
                continue;
            }
            let id = inner.op_set.id_to_exid(_id);
            let value = if let ScalarValue::Counter(c) = &value {
                let inc = inner.op_set.get_increment_at_pos(pos, inner.clock.as_ref());
                ValueRef::from_action_value(action, ScalarValue::Counter(*c + inc))
            } else {
                ValueRef::from_action_value(action, value)
            };
            return Some(ListRangeItem {
                index,
                value: value.into_owned(),
                id,
                _id,
                conflict,
            });
        }
        None
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
        let values = values.into_iter().map(Value::from).collect::<Vec<_>>();
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
