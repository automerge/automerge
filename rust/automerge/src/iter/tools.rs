use crate::exid::ExId;
use crate::op_set2::OpSet;
use crate::types::OpId;

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

pub(crate) trait Shiftable: Iterator {
    fn shift_next(&mut self, _range: Range<usize>) -> Option<<Self as Iterator>::Item>;
}

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

#[derive(Clone, Debug)]
enum OpSetOrExId<'a> {
    OpSet(&'a OpSet),
    ExId(ExId),
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
