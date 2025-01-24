use crate::{marks::MarkSet, types::Clock};

use super::{Op, OpId, OpQueryTerm};
use crate::op_set2::types::{Action, ActionCursor, ActorCursor, ScalarValue};
use crate::op_set2::OpSet;

use packer::{BooleanCursor, ColumnDataIter, DeltaCursor, IntCursor, UIntCursor};

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct SkipIter<I: Iterator, S: Iterator<Item = usize>> {
    iter: I,
    skip: S,
}

impl<I: Iterator, S: Iterator<Item = usize>> SkipIter<I, S> {
    pub(crate) fn new(iter: I, skip: S) -> Self {
        Self { iter, skip }
    }
}

impl<I: Iterator, S: Iterator<Item = usize>> Iterator for SkipIter<I, S> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let skip = self.skip.next()?;
        self.iter.nth(skip)
    }
}

pub(crate) enum VisIter<'a> {
    Indexed(IndexedVisIter<'a>),
    Scan(ScanVisIter<'a>),
}

impl<'a> VisIter<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Option<&Clock>, range: Range<usize>) -> Self {
        if let Some(clock) = clock {
            let scan = ScanVisIter::new(op_set, range, clock.clone());
            Self::Scan(scan)
        } else {
            let indexed = IndexedVisIter::new(op_set, range);
            Self::Indexed(indexed)
        }
    }
}

impl<'a> Iterator for VisIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Scan(s) => s.next(),
            Self::Indexed(i) => i.next(),
        }
    }
}

pub(crate) struct IndexedVisIter<'a> {
    iter: ColumnDataIter<'a, BooleanCursor>,
    vis: usize,
}

impl<'a> IndexedVisIter<'a> {
    fn new(op_set: &'a OpSet, range: Range<usize>) -> Self {
        let iter = op_set.visible_index.iter_range(range);
        Self { iter, vis: 0 }
    }
}

impl<'a> Iterator for IndexedVisIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.vis > 0 {
            self.vis -= 1;
            Some(0)
        } else {
            let mut skip = 0;
            // next_run can produce zero length runs so
            // we loop
            while let Some(run) = self.iter.next_run() {
                if run.value.as_deref() == Some(&true) && run.count > 0 {
                    self.vis = run.count - 1;
                    break;
                } else {
                    skip += run.count;
                }
            }
            Some(skip)
        }
    }
}

pub(crate) struct ScanVisIter<'a> {
    id_actor: ColumnDataIter<'a, ActorCursor>,
    id_ctr: ColumnDataIter<'a, DeltaCursor>,
    action: ColumnDataIter<'a, ActionCursor>,
    succ_count: ColumnDataIter<'a, UIntCursor>,
    succ_actor: ColumnDataIter<'a, ActorCursor>,
    succ_ctr: ColumnDataIter<'a, DeltaCursor>,
    succ_inc: ColumnDataIter<'a, IntCursor>,
    clock: Clock,
}

impl<'a> ScanVisIter<'a> {
    fn new(op_set: &'a OpSet, range: Range<usize>, clock: Clock) -> Self {
        let id_actor = op_set.cols.id_actor.iter_range(range.clone());
        let id_ctr = op_set.cols.id_ctr.iter_range(range.clone());
        let action = op_set.cols.action.iter_range(range.clone());
        let succ_count = op_set.cols.succ_count.iter_range(range);
        let succ_range = succ_count.calculate_acc().as_usize()..usize::MAX;
        let succ_actor = op_set.cols.succ_actor.iter_range(succ_range.clone());
        let succ_ctr = op_set.cols.succ_ctr.iter_range(succ_range.clone());
        let succ_inc = op_set.inc_index.iter_range(succ_range);
        Self {
            id_actor,
            id_ctr,
            action,
            succ_count,
            succ_actor,
            succ_ctr,
            succ_inc,
            clock,
        }
    }

    fn next_visible(&mut self) -> Option<bool> {
        let id = OpId::load(self.id_actor.next(), self.id_ctr.next())?;
        let action = self.action.next()?;
        let is_inc = action.as_deref() == Some(&Action::Increment);
        let succ_count = self.succ_count.next()?.unwrap_or_default();
        let mut deleted = false;
        for _ in 0..*succ_count {
            let succ_id = OpId::load(self.succ_actor.next(), self.succ_ctr.next())?;
            let inc = self.succ_inc.next()?;
            if inc.is_none() && self.clock.covers(&succ_id) {
                deleted = true;
            }
        }
        if deleted || !self.clock.covers(&id) || is_inc {
            Some(false)
        } else {
            Some(true)
        }
    }
}

impl<'a> Iterator for ScanVisIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let mut skip = 0;
        while let Some(vis) = self.next_visible() {
            if vis {
                break;
            } else {
                skip += 1;
            }
        }
        Some(skip)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VisibleOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    clock: Option<Clock>,
    iter: I,
}

impl<'a, I: OpQueryTerm<'a> + Clone> VisibleOpIter<'a, I> {
    pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
        Self { iter, clock }
    }
}

fn vis(clock: Option<&Clock>, id: &OpId) -> bool {
    if let Some(c) = clock {
        c.covers(id)
    } else {
        true
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> Iterator for VisibleOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let clock = self.clock.as_ref();
        for mut op in self.iter.by_ref() {
            if op.action == Action::Increment {
                continue;
            }
            if op.scope_to_clock(clock) {
                return Some(op);
            }
        }
        None
    }
}

impl<'a> Op<'a> {
    fn maybe_scope_counter_to_clock(
        &self,
        counter: i64,
        clock: Option<&Clock>,
    ) -> (bool, Option<ScalarValue<'a>>) {
        let mut inc = 0;
        let mut deleted = false;
        for (i, val) in self.succ_inc() {
            if vis(clock, &i) {
                if let Some(v) = val {
                    inc += v;
                } else {
                    deleted = true;
                }
            }
        }
        (deleted, Some(ScalarValue::Counter(counter + inc)))
    }

    pub(crate) fn scope_to_clock(&mut self, clock: Option<&Clock>) -> bool {
        let visibility = self.maybe_scope_to_clock(clock);
        let result = visibility.visible();
        if let Some(v) = visibility.value {
            self.value = v;
        }
        result
    }

    fn maybe_scope_to_clock(&mut self, clock: Option<&Clock>) -> Visibility<'a> {
        let predates = vis(clock, &self.id);
        if let ScalarValue::Counter(n) = self.value {
            let (deleted, value) = self.maybe_scope_counter_to_clock(n, clock);
            Visibility {
                predates,
                deleted,
                value,
            }
        } else {
            let deleted = self.succ().any(|i| vis(clock, &i));
            let value = None;
            Visibility {
                predates,
                deleted,
                value,
            }
        }
    }
}

#[derive(Debug)]
struct Visibility<'a> {
    predates: bool,
    deleted: bool,
    value: Option<ScalarValue<'a>>,
}

impl<'a> Visibility<'a> {
    fn visible(&self) -> bool {
        self.predates && !self.deleted
    }
}

impl<'a, I: OpQueryTerm<'a> + Clone> OpQueryTerm<'a> for VisibleOpIter<'a, I> {
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }

    fn range(&self) -> Range<usize> {
        self.iter.range()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DiffOpIter<'a, 'b, I: Iterator<Item = Op<'a>> + Clone> {
    before: &'b Clock,
    after: &'b Clock,
    iter: I,
}

impl<'a, 'b, I: OpQueryTerm<'a> + Clone> DiffOpIter<'a, 'b, I> {
    pub(crate) fn new(iter: I, before: &'b Clock, after: &'b Clock) -> Self {
        Self {
            iter,
            before,
            after,
        }
    }
}

pub(crate) struct DiffOp<'a> {
    pub(crate) op: Op<'a>,
    pub(crate) predates_before: bool,
    pub(crate) predates_after: bool,
    pub(crate) was_deleted_before: bool,
    pub(crate) was_deleted_after: bool,
    pub(crate) value_before: Option<ScalarValue<'a>>,
    pub(crate) value_after: Option<ScalarValue<'a>>,
}

impl<'a, 'b, I: OpQueryTerm<'a> + Clone> Iterator for DiffOpIter<'a, 'b, I> {
    type Item = DiffOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for mut op in self.iter.by_ref() {
            if op.action == Action::Increment {
                continue;
            }
            let before = op.maybe_scope_to_clock(Some(self.before));
            let after = op.maybe_scope_to_clock(Some(self.after));
            if before.visible() || after.visible() {
                let value_before = before.value;
                let value_after = after.value;
                let predates_before = before.predates;
                let predates_after = after.predates;
                let was_deleted_before = before.deleted;
                let was_deleted_after = after.deleted;
                return Some(DiffOp {
                    op,
                    predates_before,
                    predates_after,
                    was_deleted_before,
                    was_deleted_after,
                    value_before,
                    value_after,
                });
            }
        }
        None
    }
}
