use crate::{marks::MarkSet, types::Clock};

use super::{KeyIter, Op, OpId, OpIter, OpQueryTerm};
use crate::op_set2::types::{Action, ScalarValue};

use std::collections::HashSet;
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
        while let Some(mut op) = self.iter.next() {
            if op.action == Action::Increment {
                continue;
            }
            if op.scope_to_clock(clock, self.get_opiter()) {
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
        iter: &OpIter<'a>,
    ) -> (bool, Option<ScalarValue<'a>>) {
        let mut succ = self
            .succ()
            .filter(|i| vis(clock, i))
            .collect::<HashSet<_>>();
        // shouldnt need to clone Op
        let key_iter = KeyIter::new(*self, iter.clone());
        let mut inc = 0;
        for op in key_iter {
            if op.action == Action::Increment && succ.contains(&op.id) {
                if vis(clock, &op.id) {
                    inc += op.get_increment_value().unwrap_or(0);
                }
                succ.remove(&op.id);
            }
        }
        (!succ.is_empty(), Some(ScalarValue::Counter(counter + inc)))
    }

    pub(crate) fn scope_to_clock(&mut self, clock: Option<&Clock>, iter: &OpIter<'a>) -> bool {
        let visibility = self.maybe_scope_to_clock(clock, iter);
        let result = visibility.visible();
        if let Some(v) = visibility.value {
            self.value = v;
        }
        result
    }

    fn maybe_scope_to_clock(&mut self, clock: Option<&Clock>, iter: &OpIter<'a>) -> Visibility<'a> {
        let predates = vis(clock, &self.id);
        if let ScalarValue::Counter(n) = self.value {
            let (deleted, value) = self.maybe_scope_counter_to_clock(n, clock, iter);
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
        while let Some(mut op) = self.iter.next() {
            let iter = self.iter.get_opiter();
            if op.action == Action::Increment {
                continue;
            }
            let before = op.maybe_scope_to_clock(Some(self.before), iter);
            let after = op.maybe_scope_to_clock(Some(self.after), iter);
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
