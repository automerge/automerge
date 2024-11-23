use crate::{marks::MarkSet, types::Clock};

use super::{Op, OpId, OpIter, OpQueryTerm};
use crate::op_set2::types::{Action, ScalarValue};

use std::fmt::Debug;
use std::sync::Arc;

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
    fn get_opiter(&self) -> &OpIter<'a> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
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
