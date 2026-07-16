use std::fmt::Debug;
use std::ops::Range;

use crate::column::Iter;
use crate::delta::{DeltaIter, DeltaValue};
use crate::prefix::{PrefixIter, PrefixValue};
use crate::ColumnValueRef;

/// An iterator over a positional window that can be repositioned.
///
/// Implementations provide the kernel — [`Self::get_pos`],
/// [`Self::get_max`] and [`Self::set_max`] — and inherit the rest.
/// Repositioning only moves forward: a new range must start at or
/// after the current position.
///
/// The default `advance_to`/`advance_by` (and therefore `shift` and
/// `shift_next`) walk via [`Iterator::nth`]; iterators with a faster
/// way to move (multi-column wrappers, skippers) should override.
pub trait Shiftable: Iterator + Debug {
    /// The position of the item the next call to `next()` returns.
    fn get_pos(&self) -> usize;

    /// The end of the iterator's window (exclusive).
    fn get_max(&self) -> usize;

    /// Truncate the window: iteration stops at `pos`.
    fn set_max(&mut self, pos: usize);

    /// The remaining window: `get_pos()..get_max()`.
    fn get_range(&self) -> Range<usize> {
        self.get_pos()..self.get_max()
    }

    /// Advance so the next item returned is the one at `target` (no-op
    /// if the iterator is already at or past it).
    fn advance_to(&mut self, target: usize) {
        if target > self.get_pos() {
            self.nth(target - self.get_pos() - 1);
        }
    }

    /// Advance past `amount` items.
    fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
    }

    /// Reposition the window to `range`: iteration yields the items in
    /// `range` and then stops.
    fn shift(&mut self, range: Range<usize>) {
        self.set_max(range.end);
        self.advance_to(range.start);
    }

    /// [`Self::shift`], returning the item at `range.start`.
    ///
    /// One `nth` call, not a reposition-then-`next` pair.
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        assert!(range.start >= self.get_pos());
        self.set_max(range.end);
        self.nth(range.start - self.get_pos())
    }

    /// Consume through `pos`, returning the item there. Equivalent to
    /// `nth(pos - get_pos())`; `pos` must be at or ahead of the
    /// current position.
    fn scan_to_pos(&mut self, pos: usize) -> Option<<Self as Iterator>::Item> {
        self.nth(pos - self.get_pos())
    }

    /// Wrap in an [`Unshift`], pulling the first item as the lookahead.
    fn unshift(self) -> Unshift<Self>
    where
        Self: Sized,
    {
        Unshift::new(self)
    }
}

impl<T: ColumnValueRef> Shiftable for Iter<'_, T> {
    fn get_pos(&self) -> usize {
        Iter::pos(self)
    }

    fn get_max(&self) -> usize {
        Iter::end_pos(self)
    }

    fn set_max(&mut self, pos: usize) {
        Iter::set_max(self, pos)
    }
}

impl<T: PrefixValue> Shiftable for PrefixIter<'_, T> {
    fn get_pos(&self) -> usize {
        PrefixIter::pos(self)
    }

    fn get_max(&self) -> usize {
        PrefixIter::end_pos(self)
    }

    fn set_max(&mut self, pos: usize) {
        PrefixIter::set_max(self, pos)
    }
}

impl<T: DeltaValue> Shiftable for DeltaIter<'_, T> {
    fn get_pos(&self) -> usize {
        DeltaIter::pos(self)
    }

    fn get_max(&self) -> usize {
        DeltaIter::end_pos(self)
    }

    fn set_max(&mut self, pos: usize) {
        DeltaIter::set_max(self, pos)
    }
}

/// An iterator with its lookahead already pulled.
///
/// `new` immediately draws the first item, so [`Self::peek`] is free
/// and needs no `&mut`. [`Self::shift`] repositions the inner
/// [`Shiftable`] iterator and refills the lookahead from the start of
/// the new range.
#[derive(Clone, Debug)]
pub struct Unshift<T: Iterator> {
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
    pub fn new(mut inner: T) -> Self {
        let next = inner.next();
        Self { inner, next }
    }

    pub fn peek(&self) -> Option<&T::Item> {
        self.next.as_ref()
    }
}

impl<T: Shiftable + Iterator> Unshift<T> {
    pub fn shift(&mut self, range: Range<usize>) {
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
