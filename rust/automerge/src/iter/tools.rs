use crate::clock::ClockRange;
use crate::exid::ExId;
use crate::op_set2::op_set::{TopIter, VisIter};
use crate::op_set2::OpSet;
use crate::types::OpId;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::Peekable;
use std::ops::Range;

#[derive(Clone, Debug)]
pub(crate) struct Unshift<T: Iterator> {
    inner: T,
    next: Option<T::Item>,
}

impl<T: Iterator + Default> Default for Unshift<T> {
    fn default() -> Self {
        Self {
            inner: T::default(),
            next: None,
        }
    }
}

impl<T: Iterator> Unshift<T> {
    pub(crate) fn new(mut inner: T) -> Self {
        let next = inner.next();
        Self { inner, next }
    }

    pub(crate) fn peek(&self) -> Option<&T::Item> {
        self.next.as_ref()
    }
}

impl<T: Shiftable + Iterator> Unshift<T> {
    pub(crate) fn shift(&mut self, range: Range<usize>) {
        self.next = self.inner.shift_next(range);
    }
}

impl<T: Iterator> Iterator for Unshift<T> {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.inner.next();
        std::mem::swap(&mut next, &mut self.next);
        next
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut next = self.inner.nth(n);
        std::mem::swap(&mut next, &mut self.next);
        next
    }
}

pub(crate) trait Skipper: Iterator<Item = usize> {}

pub(crate) trait Shiftable: Iterator + Debug {
    fn shift_next(&mut self, _range: Range<usize>) -> Option<<Self as Iterator>::Item>;
}

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
}

impl<I: Iterator> Iterator for PeekShift<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.peeked.take().or_else(|| self.iter.next())
    }
}

impl<T: hexane::PrefixValue> Shiftable for hexane::PrefixIter<'_, T> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        hexane::PrefixIter::shift_next(self, range)
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
#[derive(Clone, Default, Debug)]
pub(crate) struct BoolColumnSkipper<'a> {
    iter: hexane::Iter<'a, bool>,
    range: Range<usize>,
    cursor: usize,
    pending: usize,
    exhausted: bool,
}

impl<'a> BoolColumnSkipper<'a> {
    pub(crate) fn new(iter: hexane::Iter<'a, bool>, range: Range<usize>) -> Self {
        let cursor = range.start;
        Self {
            iter,
            range,
            cursor,
            pending: 0,
            exhausted: false,
        }
    }
}

impl Skipper for BoolColumnSkipper<'_> {}

impl Shiftable for BoolColumnSkipper<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.range = range.clone();
        self.cursor = range.start;
        self.pending = 0;
        self.exhausted = false;

        let Some(value) = self.iter.shift_next(range.clone()) else {
            self.exhausted = true;
            return Some(range.end.saturating_sub(range.start));
        };

        self.cursor = range.start + 1;
        if value {
            Some(0)
        } else {
            Some(1 + self.next().unwrap_or(0))
        }
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
            self.cursor += 1;
            Some(0)
        } else {
            let mut skipped = 0;
            while let Some(run) = self.iter.next_run() {
                if run.value && run.count > 0 {
                    self.pending = run.count - 1;
                    let pos = self.cursor + skipped;
                    self.cursor = pos + 1;
                    return Some(skipped);
                }
                skipped += run.count;
            }
            self.exhausted = true;
            let skip = self.range.end.saturating_sub(self.cursor);
            self.cursor = self.range.end.saturating_add(1);
            Some(skip)
        }
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
