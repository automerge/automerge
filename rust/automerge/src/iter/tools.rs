use std::fmt::Debug;
use std::ops::Range;

#[derive(Clone, Debug)]
pub(crate) struct Peek<T: Iterator> {
    inner: T,
    next: Option<T::Item>,
}

impl<T: Iterator> Peek<T> {
    pub(crate) fn new(mut inner: T) -> Self {
        let next = inner.next();
        Self { inner, next }
    }

    pub(crate) fn peek(&self) -> Option<&T::Item> {
        self.next.as_ref()
    }
}

impl<T: Shiftable + Iterator> Shiftable for Peek<T> {
    fn shift_range(&mut self, range: Range<usize>) {
        self.inner.shift_range(range);
        self.next = self.inner.next();
    }
}

impl<T: Iterator> Iterator for Peek<T> {
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

pub(crate) trait Skipper: Iterator<Item = usize> + Debug + Clone {}

pub(crate) trait Shiftable {
    fn shift_range(&mut self, range: Range<usize>);
}

#[derive(Clone, Debug)]
pub(crate) struct SkipIter<I: Iterator + Debug + Clone, S: Skipper> {
    pos: usize,
    iter: I,
    skip: S,
}

impl<I: Iterator + Debug + Clone + Default, S: Skipper + Default> Default for SkipIter<I, S> {
    fn default() -> Self {
        Self {
            pos: 0,
            iter: I::default(),
            skip: S::default(),
        }
    }
}

impl<I: Iterator + Debug + Clone, S: Skipper> SkipIter<I, S> {
    pub(crate) fn new(iter: I, skip: S) -> Self {
        Self { iter, skip, pos: 0 }
    }
    pub(crate) fn new_with_offset(iter: I, skip: S, pos: usize) -> Self {
        Self { iter, skip, pos }
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }
}

impl<I: Iterator + Debug + Clone + Shiftable, S: Skipper + Shiftable> Shiftable for SkipIter<I, S> {
    fn shift_range(&mut self, range: Range<usize>) {
        self.pos = range.start;
        self.skip.shift_range(range.clone());
        self.iter.shift_range(range)
    }
}

impl<I: Iterator + Debug + Clone, S: Skipper> Iterator for SkipIter<I, S> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let skip = self.skip.next()?;
        self.pos += skip;
        let item = self.iter.nth(skip)?;
        self.pos += 1;
        Some(item)
    }
}
