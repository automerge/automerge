use crate::{marks::MarkSet, types::Clock};

use super::{HasOpScope, KeyIter, Op, OpId, OpIter, OpScope, Verified};
use crate::op_set2::types::{Action, ScalarValue};

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct VisibleOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    clock: Option<Clock>,
    iter: I,
}

impl<'a, I: HasOpScope<'a> + Clone> VisibleOpIter<'a, I> {
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

impl<'a, I: HasOpScope<'a> + Clone> Iterator for VisibleOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let clock = self.clock.as_ref();
        while let Some(mut op) = self.iter.next() {
            if op.action == Action::Increment {
                continue;
            }
            if let ScalarValue::Counter(n) = op.value {
                // because the ops are in order we could do this without an allocation
                let mut succ = op.succ().filter(|i| vis(clock, &i)).collect::<HashSet<_>>();
                let mut key_iter = KeyIter::new(op, self.iter.get_opiter().clone());
                let mut inc = 0;
                for op in key_iter {
                    if op.action == Action::Increment && succ.contains(&op.id) {
                        if vis(clock, &op.id) {
                            inc += op.get_increment_value().unwrap_or(0);
                        }
                        succ.remove(&op.id);
                    }
                }
                if succ.len() == 0 {
                    op.value = ScalarValue::Counter(n + inc);
                    return Some(op);
                }
            } else {
                if vis(clock, &op.id) && !op.succ().any(|i| vis(clock, &i)) {
                    return Some(op);
                }
            }
        }
        None
    }
}

impl<'a, I: HasOpScope<'a> + Clone> HasOpScope<'a> for VisibleOpIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }
}
