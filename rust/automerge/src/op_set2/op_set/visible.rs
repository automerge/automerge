use crate::{marks::MarkSet, types::Clock};

use super::{ActionIter, Op, OpId, OpIdIter, OpQueryTerm, SuccIterIter};
use crate::op_set2::op::SuccCursors;
use crate::op_set2::types::{Action, ScalarValue};
use crate::op_set2::OpSet;

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use crate::iter::tools::{BoolColumnSkipper, Shiftable, Skipper};

impl Shiftable for VisIter<'_> {
    fn get_pos(&self) -> usize {
        match self {
            Self::Indexed(index) => index.get_pos(),
            Self::Scan(scan) => scan.get_pos(),
        }
    }

    fn get_max(&self) -> usize {
        match self {
            Self::Indexed(index) => index.get_max(),
            Self::Scan(scan) => scan.get_max(),
        }
    }

    fn set_max(&mut self, pos: usize) {
        match self {
            Self::Indexed(index) => index.set_max(pos),
            Self::Scan(scan) => scan.set_max(pos),
        }
    }

    fn shift(&mut self, range: Range<usize>) {
        match self {
            Self::Indexed(index) => index.shift(range),
            Self::Scan(scan) => scan.shift(range),
        }
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<usize> {
        match self {
            Self::Indexed(index) => index.shift_next(range),
            Self::Scan(scan) => scan.shift_next(range),
        }
    }
}

impl Shiftable for ScanVisIter<'_> {
    fn get_pos(&self) -> usize {
        self.id.get_pos()
    }

    fn get_max(&self) -> usize {
        self.id.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.id.set_max(pos);
        self.action.set_max(pos);
        self.succ.set_max(pos);
    }

    // this iterator yields skip counts, so the nth-walking default
    // would consume items instead of repositioning — shift the columns
    fn shift(&mut self, range: Range<usize>) {
        self.id.shift(range.clone());
        self.action.shift(range.clone());
        self.succ.shift(range);
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<usize> {
        let id = self.id.shift_next(range.clone());
        let action = self.action.shift_next(range.clone());
        let succ = self.succ.shift_next(range);
        let vis = Self::is_visible(id?, action?, succ?, &self.clock);
        if vis {
            Some(0)
        } else {
            let skip = self.next().unwrap_or(0);
            Some(skip + 1)
        }
    }
}

impl Shiftable for IndexedVisIter<'_> {
    fn get_pos(&self) -> usize {
        self.iter.get_pos()
    }

    fn get_max(&self) -> usize {
        self.iter.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.iter.set_max(pos);
    }

    fn shift(&mut self, range: Range<usize>) {
        self.iter.shift(range);
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<usize> {
        self.iter.shift_next(range)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum VisIter<'a> {
    Indexed(IndexedVisIter<'a>),
    Scan(Box<ScanVisIter<'a>>),
}

impl Default for VisIter<'_> {
    fn default() -> Self {
        Self::Indexed(IndexedVisIter::default())
    }
}

impl Skipper for VisIter<'_> {}

impl<'a> VisIter<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Option<&Clock>, range: Range<usize>) -> Self {
        if let Some(clock) = clock {
            let scan = ScanVisIter::new(op_set, range, clock.clone());
            Self::Scan(Box::new(scan))
        } else {
            let indexed = IndexedVisIter::new(op_set, range);
            Self::Indexed(indexed)
        }
    }
}

impl Iterator for VisIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Scan(s) => s.next(),
            Self::Indexed(i) => i.next(),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct IndexedVisIter<'a> {
    iter: BoolColumnSkipper<'a>,
}

impl<'a> IndexedVisIter<'a> {
    fn new(op_set: &'a OpSet, range: Range<usize>) -> Self {
        let iter = op_set.cols.index.visible.iter_range(range.clone());
        Self {
            iter: BoolColumnSkipper::new(iter),
        }
    }
}

impl Iterator for IndexedVisIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScanVisIter<'a> {
    id: OpIdIter<'a>,
    action: ActionIter<'a>,
    succ: SuccIterIter<'a>,
    clock: Clock,
}

impl<'a> ScanVisIter<'a> {
    fn new(op_set: &'a OpSet, range: Range<usize>, clock: Clock) -> Self {
        let id = op_set.id_iter_range(&range);
        let action = op_set.action_iter_range(&range);
        let succ = op_set.succ_iter_range(&range);
        Self {
            id,
            succ,
            action,
            clock,
        }
    }

    fn is_visible(id: OpId, action: Action, succ: SuccCursors<'_>, clock: &Clock) -> bool {
        let is_inc = action == Action::Increment;
        let mut deleted = false;
        for (id, inc) in succ.with_inc() {
            if inc.is_none() && clock.covers(&id) {
                deleted = true;
            }
        }
        !(deleted || !clock.covers(&id) || is_inc)
    }

    fn next_visible(&mut self) -> Option<bool> {
        let id = self.id.next()?;
        let action = self.action.next()?;
        let succ = self.succ.next()?;
        Some(Self::is_visible(id, action, succ, &self.clock))
    }
}

impl Iterator for ScanVisIter<'_> {
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

impl Visibility<'_> {
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
