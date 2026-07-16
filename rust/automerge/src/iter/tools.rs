use crate::clock::ClockRange;
use crate::exid::ExId;
use crate::op_set2::op_set::{TopIter, VisIter};
use crate::op_set2::OpSet;
use crate::types::OpId;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::Peekable;
use std::ops::Range;

pub(crate) use hexane::{Shiftable, Unshift};

pub(crate) trait Skipper: Iterator<Item = usize> {}

/// A peekable iterator that also supports repositioning.
///
/// Unlike [`std::iter::Peekable`] the lookahead can be discarded: [`Self::shift`]
/// repositions the inner iterator to a new range and stores the first item
/// of that range as the new lookahead.
#[derive(Clone, Debug, Default)]
pub(crate) struct PeekShift<I: Iterator> {
    iter: I,
    peeked: Option<I::Item>,
}

impl<I: Iterator> PeekShift<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self { iter, peeked: None }
    }

    pub(crate) fn peek(&mut self) -> Option<&I::Item> {
        if self.peeked.is_none() {
            self.peeked = self.iter.next();
        }
        self.peeked.as_ref()
    }

    /// The next item if it satisfies `f`, leaving it as the lookahead
    /// otherwise.
    pub(crate) fn next_if(&mut self, f: impl FnOnce(&I::Item) -> bool) -> Option<I::Item> {
        match self.peek() {
            Some(item) if f(item) => self.peeked.take(),
            _ => None,
        }
    }
}

impl<I: Iterator + Shiftable> PeekShift<I> {
    /// Reposition to `range`, discarding the current lookahead; the first
    /// item of the new range (if any) becomes the lookahead.
    pub(crate) fn shift(&mut self, range: Range<usize>) {
        self.peeked = self.iter.shift_next(range);
    }

    /// Truncate the inner window; an already-drawn lookahead survives.
    pub(crate) fn set_max(&mut self, pos: usize) {
        self.iter.set_max(pos);
    }
}

impl<I: Iterator> Iterator for PeekShift<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.peeked.take().or_else(|| self.iter.next())
    }
}

#[derive(Debug)]
pub(crate) struct SkipWrap<I: Iterator<Item = usize>> {
    pub(crate) pos: usize,
    iter: I,
}

impl<I: Iterator<Item = usize>> Skipper for SkipWrap<I> {}

impl<I: Iterator<Item = usize>> SkipWrap<I> {
    pub(crate) fn new(pos: usize, iter: I) -> Self {
        Self { pos, iter }
    }
}

impl<I: Iterator<Item = usize>> Iterator for SkipWrap<I> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.iter.next()?;
        let delta = pos - self.pos;
        self.pos = pos + 1;
        Some(delta)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SkipIter<I: Iterator + Debug + Clone, S: Skipper> {
    iter: I,
    skip: S,
}

impl<I: Iterator + Debug + Clone + Default, S: Skipper + Default> Default for SkipIter<I, S> {
    fn default() -> Self {
        Self {
            iter: I::default(),
            skip: S::default(),
        }
    }
}

impl<I: Iterator + Debug + Clone, S: Skipper> SkipIter<I, S> {
    pub(crate) fn new(iter: I, skip: S) -> Self {
        Self { iter, skip }
    }

    pub(crate) fn inner(&self) -> &I {
        &self.iter
    }
}

impl<I: Iterator + Debug + Clone + Shiftable, S: Skipper + Shiftable> Shiftable for SkipIter<I, S> {
    fn get_pos(&self) -> usize {
        self.skip.get_pos()
    }

    fn get_max(&self) -> usize {
        self.skip.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.skip.set_max(pos);
        self.iter.set_max(pos);
    }

    // the skipper's first item decides how far `iter` jumps, so a
    // position-only shift moves both to the range start and lets the
    // next `next()` consume the first skip
    fn shift(&mut self, range: Range<usize>) {
        self.skip.shift(range.clone());
        self.iter.shift(range);
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let skip = self.skip.shift_next(range.clone());
        let start = range.start + skip.unwrap_or(0);
        let end = range.end;
        self.iter.shift_next(start..end)
    }
}

impl<I: Iterator + Debug + Clone, S: Skipper> Iterator for SkipIter<I, S> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let skip = self.skip.next()?;
        if skip == 0 {
            self.iter.next()
        } else {
            self.iter.nth(skip)
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExIdPromise<'a> {
    pub(crate) id: OpId,
    promise: OpSetOrExId<'a>,
}

impl PartialEq for ExIdPromise<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Clone)]
enum OpSetOrExId<'a> {
    OpSet(&'a OpSet),
    ExId(ExId),
}

impl std::fmt::Debug for OpSetOrExId<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpSetOrExId::OpSet(_) => write!(f, "OpSetOrExId::OpSet"),
            OpSetOrExId::ExId(e) => write!(f, "OpSetOrExId::ExId({:?})", e),
        }
    }
}

impl OpSetOrExId<'_> {
    fn into_owned(self, id: OpId) -> OpSetOrExId<'static> {
        match self {
            Self::OpSet(o) => OpSetOrExId::ExId(o.id_to_exid(id)),
            Self::ExId(e) => OpSetOrExId::ExId(e),
        }
    }
}

impl<'a> ExIdPromise<'a> {
    pub(crate) fn new(op_set: &'a OpSet, id: OpId) -> Self {
        Self {
            id,
            promise: OpSetOrExId::OpSet(op_set),
        }
    }

    pub(crate) fn exid(&self) -> ExId {
        match &self.promise {
            OpSetOrExId::OpSet(o) => o.id_to_exid(self.id),
            OpSetOrExId::ExId(e) => e.clone(),
        }
    }

    pub(crate) fn into_owned(self) -> ExIdPromise<'static> {
        ExIdPromise {
            id: self.id,
            promise: self.promise.into_owned(self.id),
        }
    }
}

// merges two iterators of only increasing usize and throw out dedupes

pub(crate) struct MergeIter<I, J>
where
    I: Iterator<Item = usize>,
    J: Iterator<Item = usize>,
{
    left: Peekable<I>,
    right: Peekable<J>,
    last_yielded: Option<usize>,
}

impl<I, J> MergeIter<I, J>
where
    I: Iterator<Item = usize>,
    J: Iterator<Item = usize>,
{
    pub(crate) fn new(left: I, right: J) -> Self {
        Self {
            left: left.peekable(),
            right: right.peekable(),
            last_yielded: None,
        }
    }

    pub(crate) fn skip(self) -> SkipWrap<Self> {
        let pos = self.last_yielded.map(|n| n + 1).unwrap_or(0);
        SkipWrap::new(pos, self)
    }
}

impl<I, J> Iterator for MergeIter<I, J>
where
    I: Iterator<Item = usize>,
    J: Iterator<Item = usize>,
{
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_val = match (self.left.peek(), self.right.peek()) {
                (Some(&l), Some(&r)) if l < r => self.left.next()?,
                (Some(&l), Some(&r)) if r < l => self.right.next()?,
                (Some(_), Some(_)) => {
                    self.left.next();
                    self.right.next()?
                }
                (Some(_), None) => self.left.next()?,
                (None, Some(_)) => self.right.next()?,
                (None, None) => return None,
            };

            if Some(next_val) != self.last_yielded {
                self.last_yielded = Some(next_val);
                return Some(next_val);
            }
        }
    }
}

/// A [`BoolColumnSkipper`] is an iterator that iterates over usize values
/// represnting ranges of `false` values in a boolean column. This can then
/// be used by other iterators to skip past elements for which the boolean
/// column is false
///
/// Only `pending` (trues still owed from a consumed run) and the
/// exhaustion flag are real state — the cursor is derived as
/// `iter.pos() - pending`, and the window end is the iter's own max.
#[derive(Clone, Default, Debug)]
pub(crate) struct BoolColumnSkipper<'a> {
    iter: hexane::Iter<'a, bool>,
    pending: usize,
    exhausted: bool,
}

impl<'a> BoolColumnSkipper<'a> {
    pub(crate) fn new(iter: hexane::Iter<'a, bool>) -> Self {
        Self {
            iter,
            pending: 0,
            exhausted: false,
        }
    }

    /// The position of the next item this skipper will account for.
    fn cursor(&self) -> usize {
        self.iter.pos() - self.pending
    }
}

impl Skipper for BoolColumnSkipper<'_> {}

impl Shiftable for BoolColumnSkipper<'_> {
    fn get_pos(&self) -> usize {
        self.cursor()
    }

    fn get_max(&self) -> usize {
        self.iter.end_pos()
    }

    fn set_max(&mut self, pos: usize) {
        self.iter.set_max(pos);
    }

    // yields skip counts — reposition the state, don't consume.
    // Entering a true-run consumes it whole from the inner iter, owing
    // the tail as `pending`; a shift landing inside the owed span trims
    // it and leaves the iter alone (it is already past). Only when
    // nothing is owed does the iter really move.
    fn shift(&mut self, range: Range<usize>) {
        self.pending = self
            .pending
            .saturating_sub(range.start.saturating_sub(self.cursor()));
        self.exhausted = false;
        if self.pending > 0 {
            self.iter.set_max(range.end);
        } else {
            self.iter.shift(range);
        }
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.shift(range);
        self.next()
    }
}

impl Iterator for BoolColumnSkipper<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        if self.pending > 0 {
            self.pending -= 1;
            return Some(0);
        }
        let mut skipped = 0;
        while let Some(run) = self.iter.next_run() {
            if run.value && run.count > 0 {
                self.pending = run.count - 1;
                return Some(skipped);
            }
            skipped += run.count;
        }
        // no more trues: one final skip covering the remaining falses
        self.exhausted = true;
        Some(skipped)
    }
}

#[derive(Clone, Default, Debug)]
struct PastSkipper<S> {
    before: S,
    after: S,
    state: (Diff, usize),
}

#[derive(Clone, Debug)]
enum DiffSkipper<S> {
    Diff(PastSkipper<S>),
    Current(S),
}

impl<S: Default> Default for DiffSkipper<S> {
    fn default() -> Self {
        Self::Current(S::default())
    }
}

impl<'a> DiffSkipper<VisIter<'a>> {
    fn new(op_set: &'a OpSet, clock: ClockRange, range: Range<usize>) -> Self {
        match clock {
            ClockRange::Current(clock) => {
                DiffSkipper::Current(VisIter::new(op_set, clock.as_ref(), range))
            }
            ClockRange::Diff(before, after) => {
                let before = VisIter::new(op_set, Some(&before), range.clone());
                let after = VisIter::new(op_set, Some(&after), range);
                DiffSkipper::Diff(PastSkipper::new(before, after))
            }
        }
    }
}

impl<'a> DiffSkipper<TopIter<'a>> {
    fn new_top(op_set: &'a OpSet, clock: ClockRange, range: Range<usize>) -> Self {
        match clock {
            ClockRange::Current(clock) => DiffSkipper::Current(TopIter::new(op_set, clock, range)),
            ClockRange::Diff(before, after) => {
                let before = TopIter::new(op_set, Some(before), range.clone());
                let after = TopIter::new(op_set, Some(after), range);
                DiffSkipper::Diff(PastSkipper::new(before, after))
            }
        }
    }
}

impl<S: Skipper> PastSkipper<S> {
    fn new(before: S, after: S) -> Self {
        Self {
            before,
            after,
            state: Default::default(),
        }
    }

    fn progress(&mut self, before: usize, after: usize) -> (Diff, usize) {
        match before.cmp(&after) {
            Ordering::Equal => {
                self.state = (Diff::Same, 0);
                (Diff::Same, before)
            }
            Ordering::Less => {
                self.state = (Diff::Del, after - before - 1);
                (Diff::Del, before)
            }
            Ordering::Greater => {
                self.state = (Diff::Add, before - after - 1);
                (Diff::Add, after)
            }
        }
    }

    fn next_skip(&mut self) -> Option<(usize, usize)> {
        match self.state {
            (Diff::Same, _) => {
                let before = self.before.next();
                let after = self.after.next();
                Some((before?, after?))
            }
            (Diff::Del, delta) => {
                let before = self.before.next()?;
                Some((before, delta))
            }
            (Diff::Add, delta) => {
                let after = self.after.next()?;
                Some((delta, after))
            }
        }
    }
}

impl<S> Iterator for DiffSkipper<S>
where
    S: Skipper,
{
    type Item = (Diff, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DiffSkipper::Current(iter) => Some((Diff::Add, iter.next()?)),
            DiffSkipper::Diff(iter) => iter.next(),
        }
    }
}

impl<S> Iterator for PastSkipper<S>
where
    S: Skipper,
{
    type Item = (Diff, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (before, after) = self.next_skip()?;

        Some(self.progress(before, after))
    }
}

impl<S> Shiftable for DiffSkipper<S>
where
    S: Skipper + Shiftable + Debug,
{
    fn get_pos(&self) -> usize {
        match self {
            Self::Current(iter) => iter.get_pos(),
            Self::Diff(iter) => iter.get_pos(),
        }
    }

    fn get_max(&self) -> usize {
        match self {
            Self::Current(iter) => iter.get_max(),
            Self::Diff(iter) => iter.get_max(),
        }
    }

    fn set_max(&mut self, pos: usize) {
        match self {
            Self::Current(iter) => iter.set_max(pos),
            Self::Diff(iter) => iter.set_max(pos),
        }
    }

    fn shift(&mut self, range: Range<usize>) {
        match self {
            Self::Current(iter) => iter.shift(range),
            Self::Diff(iter) => iter.shift(range),
        }
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<(Diff, usize)> {
        match self {
            Self::Current(iter) => Some((Diff::Add, iter.shift_next(range)?)),
            Self::Diff(iter) => iter.shift_next(range),
        }
    }
}

impl<S> Shiftable for PastSkipper<S>
where
    S: Skipper + Shiftable + Debug,
{
    // the two streams sit at different positions while a run of
    // adds/deletes is pending; the `after` stream is the document
    // position
    fn get_pos(&self) -> usize {
        self.after.get_pos()
    }

    fn get_max(&self) -> usize {
        self.after.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.before.set_max(pos);
        self.after.set_max(pos);
    }

    fn shift(&mut self, range: Range<usize>) {
        self.before.shift(range.clone());
        self.after.shift(range);
        self.state = Default::default();
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<(Diff, usize)> {
        let skip_before = self.before.shift_next(range.clone());
        let skip_after = self.after.shift_next(range.clone());

        Some(self.progress(skip_before?, skip_after?))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DiffIter<'a, I: Iterator + Debug + Clone, S = VisIter<'a>>
where
    S: Skipper,
{
    iter: I,
    skipper: DiffSkipper<S>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<I, S> Default for DiffIter<'_, I, S>
where
    I: Iterator + Debug + Clone + Default,
    S: Skipper + Default,
    DiffSkipper<S>: Default,
{
    fn default() -> Self {
        Self {
            iter: I::default(),
            skipper: DiffSkipper::default(),
            _phantom: Default::default(),
        }
    }
}

impl<'a, I: Iterator + Debug + Clone> DiffIter<'a, I, VisIter<'a>> {
    pub(crate) fn new(op_set: &'a OpSet, iter: I, clock: ClockRange, range: Range<usize>) -> Self {
        Self {
            iter,
            skipper: DiffSkipper::new(op_set, clock, range),
            _phantom: Default::default(),
        }
    }
}

impl<'a, I: Iterator + Debug + Clone> DiffIter<'a, I, TopIter<'a>> {
    pub(crate) fn new_top(
        op_set: &'a OpSet,
        iter: I,
        clock: ClockRange,
        range: Range<usize>,
    ) -> Self {
        Self {
            iter,
            skipper: DiffSkipper::new_top(op_set, clock, range),
            _phantom: Default::default(),
        }
    }
}

impl<I, S> Shiftable for DiffIter<'_, I, S>
where
    I: Iterator + Debug + Clone + Shiftable,
    S: Skipper + Shiftable + Debug,
{
    fn get_pos(&self) -> usize {
        self.skipper.get_pos()
    }

    fn get_max(&self) -> usize {
        self.skipper.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.skipper.set_max(pos);
        self.iter.set_max(pos);
    }

    fn shift(&mut self, range: Range<usize>) {
        self.skipper.shift(range.clone());
        self.iter.shift(range);
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let (diff, skip) = self.skipper.shift_next(range.clone())?;
        let start = range.start + skip;
        let item = self.iter.shift_next(start..range.end)?;
        Some((diff, item))
    }
}

impl<I, S> Iterator for DiffIter<'_, I, S>
where
    I: Iterator + Debug + Clone,
    S: Skipper,
{
    type Item = (Diff, I::Item);

    fn next(&mut self) -> Option<Self::Item> {
        let (diff, skip) = self.skipper.next()?;
        // nth(0) is noticeably slower for column iterators than next().
        let item = if skip == 0 {
            self.iter.next()
        } else {
            self.iter.nth(skip)
        };
        Some((diff, item?))
    }
}

#[derive(Clone, Default, Copy, Debug, PartialEq)]
pub(crate) enum Diff {
    #[default]
    Same,
    Add,
    Del,
}

impl Diff {
    pub(crate) fn is_del(&self) -> bool {
        matches!(self, Diff::Del)
    }

    pub(crate) fn is_visible(&self) -> bool {
        !self.is_del()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_skipper_shift_into_consumed_run() {
        // 5 trues, 3 falses, 4 trues
        let mut vals = vec![true; 5];
        vals.extend([false; 3]);
        vals.extend([true; 4]);
        let col: hexane::Column<bool> = hexane::Column::from_values(vals);

        // reference: a fresh skipper built directly over 2..8
        let fresh: Vec<usize> = BoolColumnSkipper::new(col.iter_range(2..8)).collect();

        // entering the first true-run consumes it whole from the inner
        // iter (pending = 4 after one next)...
        let mut skipper = BoolColumnSkipper::new(col.iter_range(0..12));
        assert_eq!(skipper.next(), Some(0));

        // ...so a shift landing inside the consumed run must trim the
        // owed trues, not discard them
        skipper.shift(2..8);
        let shifted: Vec<usize> = skipper.collect();

        assert_eq!(shifted, fresh, "shift into a consumed true-run");
    }

    #[test]
    fn bool_skipper_shift_next_into_consumed_run() {
        let mut vals = vec![true; 5];
        vals.extend([false; 3]);
        vals.extend([true; 4]);
        let col: hexane::Column<bool> = hexane::Column::from_values(vals);

        let mut fresh = BoolColumnSkipper::new(col.iter_range(2..12));
        let mut skipper = BoolColumnSkipper::new(col.iter_range(0..12));
        assert_eq!(skipper.next(), Some(0)); // pending = 4

        // previously panicked: the inner iter is already past 2
        assert_eq!(skipper.shift_next(2..12), fresh.next());
        assert_eq!(
            skipper.collect::<Vec<_>>(),
            fresh.collect::<Vec<_>>(),
            "shift_next into a consumed true-run"
        );
    }
}
