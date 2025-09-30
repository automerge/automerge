use crate::clock::ClockRange;
use crate::exid::ExId;
use crate::op_set2::op_set::VisIter;
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
        let item = self.iter.nth(skip)?;
        Some(item)
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

#[derive(Clone, Default, Debug)]
struct PastSkipper<'a> {
    before: VisIter<'a>,
    after: VisIter<'a>,
    state: (Diff, usize),
}

#[derive(Clone, Debug)]
enum DiffSkipper<'a> {
    Diff(PastSkipper<'a>),
    Current(VisIter<'a>),
}

impl Default for DiffSkipper<'_> {
    fn default() -> Self {
        Self::Current(VisIter::default())
    }
}

impl<'a> DiffSkipper<'a> {
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

impl<'a> PastSkipper<'a> {
    fn new(before: VisIter<'a>, after: VisIter<'a>) -> Self {
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

impl Iterator for DiffSkipper<'_> {
    type Item = (Diff, usize);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DiffSkipper::Current(iter) => Some((Diff::Add, iter.next()?)),
            DiffSkipper::Diff(iter) => iter.next(),
        }
    }
}

impl Iterator for PastSkipper<'_> {
    type Item = (Diff, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let (before, after) = self.next_skip()?;

        Some(self.progress(before, after))
    }
}

impl Shiftable for DiffSkipper<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<(Diff, usize)> {
        match self {
            Self::Current(iter) => Some((Diff::Add, iter.shift_next(range)?)),
            Self::Diff(iter) => iter.shift_next(range),
        }
    }
}

impl Shiftable for PastSkipper<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<(Diff, usize)> {
        let skip_before = self.before.shift_next(range.clone());
        let skip_after = self.after.shift_next(range.clone());

        Some(self.progress(skip_before?, skip_after?))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DiffIter<'a, I: Iterator + Debug + Clone> {
    iter: I,
    skipper: DiffSkipper<'a>,
}

impl<I: Iterator + Debug + Clone + Default> Default for DiffIter<'_, I> {
    fn default() -> Self {
        Self {
            iter: I::default(),
            skipper: DiffSkipper::default(),
        }
    }
}

impl<'a, I: Iterator + Debug + Clone> DiffIter<'a, I> {
    pub(crate) fn new(op_set: &'a OpSet, iter: I, clock: ClockRange, range: Range<usize>) -> Self {
        Self {
            iter,
            skipper: DiffSkipper::new(op_set, clock, range),
        }
    }
}

impl<I: Iterator + Debug + Clone + Shiftable> Shiftable for DiffIter<'_, I> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let (diff, skip) = self.skipper.shift_next(range.clone())?;
        let start = range.start + skip;
        let item = self.iter.shift_next(start..range.end)?;
        Some((diff, item))
    }
}

impl<I: Iterator + Debug + Clone> Iterator for DiffIter<'_, I> {
    type Item = (Diff, I::Item);

    fn next(&mut self) -> Option<Self::Item> {
        let (diff, skip) = self.skipper.next()?;
        let item = self.iter.nth(skip);
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
