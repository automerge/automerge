use crate::types::Clock;

use super::{Action, KeyRef, Op, OpsFound};

use std::fmt::Debug;

/// Iterate over a series of [`Op`]s to return [`OpsFound`].
///
/// The [`Iterator`] is expected to receive [`Op`]s in a contiguous order, where
/// they are grouped by their [`KeyRef`] (given by [`Op::elemid_or_key`]).
///
/// An [`Op`] is kept as a found operation if it is:
///   1. Not an [`Action::Increment`], as these are not stand alone values,
///   2. And the operation is considered visible; that is within the scope of
///      the given [`Clock`], the [`Op`] occurs in and is not deleted with the
///      timeframe of the [`Clock`].
///
/// The resulting [`OpsFound`] will either have:
///   1. A singleton [`Op`] for a given key,
///   2. Or many [`Op`]s, which indicates a conflict for the given key.
///
/// Notably, if a given key's operations are all filtered out, this key is
/// skipped, and never returned as an [`OpsFound`].
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

impl<'a, I: Iterator<Item = Op<'a>>> Iterator for OpsFoundIter<'a, I> {
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
