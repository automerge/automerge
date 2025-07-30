use crate::types::Clock;

use super::{Action, KeyRef, Op, OpQueryTerm, OpsFound};

use std::fmt::Debug;

#[derive(Clone, Debug)]
pub(crate) struct OpsFoundIter<'a, I: Iterator<Item = Op<'a>>> {
    iter: I,
    start_pos: usize,
    clock: Option<Clock>,
    last_key: Option<KeyRef<'a>>,
    found: Option<OpsFound<'a>>,
}

impl<'a, I: Iterator<Item = Op<'a>>> OpsFoundIter<'a, I> {
    pub(crate) fn new(iter: I, clock: Option<Clock>) -> Self {
        Self {
            iter,
            clock,
            start_pos: 0,
            found: None,
            last_key: None,
        }
    }
}

impl<'a, I: OpQueryTerm<'a>> Iterator for OpsFoundIter<'a, I> {
    type Item = OpsFound<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;
        for mut op in self.iter.by_ref() {
            let key = op.elemid_or_key();
            if Some(&key) != self.last_key.as_ref() {
                result = self.found.take();
                self.last_key = Some(key);
                self.found = Some(OpsFound::default());
                self.start_pos = op.pos;
            }
            if let Some(found) = &mut self.found {
                found.end_pos = op.pos + 1;
                found.range = self.start_pos..(op.pos + 1);
                if op.action != Action::Increment && op.scope_to_clock(self.clock.as_ref()) {
                    found.ops.push(op);
                }
            }
            match &result {
                Some(f) if !f.ops.is_empty() => return result,
                _ => (),
            }
        }
        let found = self.found.take()?;
        if found.ops.is_empty() {
            None
        } else {
            Some(found)
        }
    }
}
