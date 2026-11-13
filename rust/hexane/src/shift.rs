use std::fmt::Debug;
use std::ops::Range;

use crate::column::Iter;
use crate::delta::{DeltaIter, DeltaValue};
use crate::prefix::{PrefixIter, PrefixValue, PrefixedValue};
use crate::window::{self, RangeList, Stream, Window};
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
///
/// # Contract
///
/// An implementation gives one item per position, from a fixed sequence
/// of them: `next()` gives the item at [`get_pos`](Self::get_pos) and
/// moves on by one, while the position is inside the window
/// (before [`get_max`](Self::get_max) and before the items end), and
/// otherwise gives `None`; `nth(n)` gives what `n + 1` calls to `next()`
/// would give the last of; [`set_max`](Self::set_max) moves only the
/// window's end; and an implementation's own `shift` and `shift_next`
/// do what the provided ones do. The provided methods, [`Unshift`] and
/// [`Ranges`] are verified with Verus against this contract
/// (`window`); the column iterators keep it, by their own verified
/// methods. The run adapters (`Runs`, `DeltaRuns`) give a run per step,
/// so they are not covered, and neither is an [`Unshift`] over one.
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
        // verified (`window::advance_to`)
        window::advance_to(self, target)
    }

    /// Advance past `amount` items.
    fn advance_by(&mut self, amount: usize) {
        // verified (`window::advance_by`)
        window::advance_by(self, amount)
    }

    /// Reposition the window to `range`: iteration yields the items in
    /// `range` and then stops. It moves only forwards: if `range` starts
    /// before [`get_pos`](Self::get_pos), iteration goes on from
    /// `get_pos()` to `range.end`.
    fn shift(&mut self, range: Range<usize>) {
        // verified (`window::shift`): exactly the items in the window are
        // left
        window::shift(self, range.start, range.end)
    }

    /// [`Self::shift`], returning the item at `range.start`.
    ///
    /// One `nth` call, not a reposition-then-`next` pair.
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        assert!(range.start >= self.get_pos());
        // verified (`window::shift_next`)
        window::shift_next(self, range.start, range.end)
    }

    /// Consume through `pos`, returning the item there. Equivalent to
    /// `nth(pos - get_pos())`; `pos` must be at or ahead of the
    /// current position, otherwise returns None
    fn seek_to(&mut self, pos: usize) -> Option<<Self as Iterator>::Item> {
        // verified (`window::seek_to`)
        window::seek_to(self, pos)
    }

    /// Wrap in an [`Unshift`], pulling the first item as the lookahead.
    fn unshift(self) -> Unshift<Self>
    where
        Self: Sized,
    {
        Unshift::new(self)
    }

    /// Chain multiple windows: yield the items of each range in turn.
    /// Whatever remains of the current window is discarded; ranges
    /// must be ascending and non-overlapping (touching is fine).
    fn ranges<R>(self, ranges: R) -> Ranges<Self, R::IntoIter>
    where
        Self: Sized,
        R: IntoIterator<Item = Range<usize>>,
    {
        let mut iter = self;
        // empty the window so the first next() pulls the first range
        // (verified, `window::empty_window`)
        window::empty_window(&mut iter);
        Ranges {
            iter,
            ranges: ranges.into_iter(),
        }
    }
}

/// Iterator adapter chaining several positional windows of a
/// [`Shiftable`] iterator; created by [`Shiftable::ranges`].
#[derive(Clone, Debug)]
pub struct Ranges<I, R> {
    iter: I,
    ranges: R,
}

impl<I: Shiftable, R: Iterator<Item = Range<usize>>> Iterator for Ranges<I, R> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // verified (`window::ranges_next`): the items of each range in turn
        window::ranges_next(&mut self.iter, &mut self.ranges)
    }
}

// The crate's iterators move by their own verified methods.

impl<'a, T: ColumnValueRef> Shiftable for Iter<'a, T> {
    fn get_pos(&self) -> usize {
        Iter::pos(self)
    }

    fn get_max(&self) -> usize {
        Iter::end_pos(self)
    }

    fn set_max(&mut self, pos: usize) {
        Iter::set_max(self, pos)
    }

    fn advance_to(&mut self, target: usize) {
        Iter::advance_to(self, target)
    }

    fn advance_by(&mut self, amount: usize) {
        Iter::advance_by(self, amount)
    }

    fn shift(&mut self, range: Range<usize>) {
        Iter::shift(self, range)
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        Iter::shift_next(self, range)
    }

    fn seek_to(&mut self, pos: usize) -> Option<T::Get<'a>> {
        Iter::seek_item(self, pos)
    }
}

impl<'a, T: PrefixValue> Shiftable for PrefixIter<'a, T> {
    fn get_pos(&self) -> usize {
        PrefixIter::pos(self)
    }

    fn get_max(&self) -> usize {
        PrefixIter::end_pos(self)
    }

    fn set_max(&mut self, pos: usize) {
        PrefixIter::set_max(self, pos)
    }

    fn advance_to(&mut self, target: usize) {
        PrefixIter::advance_to(self, target)
    }

    fn advance_by(&mut self, amount: usize) {
        PrefixIter::advance_by(self, amount)
    }

    fn shift(&mut self, range: Range<usize>) {
        PrefixIter::shift(self, range)
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<PrefixedValue<'a, T>> {
        PrefixIter::shift_next(self, range)
    }

    fn seek_to(&mut self, pos: usize) -> Option<PrefixedValue<'a, T>> {
        PrefixIter::seek_item(self, pos)
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

    fn advance_to(&mut self, target: usize) {
        DeltaIter::advance_to(self, target)
    }

    fn advance_by(&mut self, amount: usize) {
        DeltaIter::advance_by(self, amount)
    }

    fn shift(&mut self, range: Range<usize>) {
        DeltaIter::shift(self, range)
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<T> {
        DeltaIter::shift_next(self, range)
    }

    fn seek_to(&mut self, pos: usize) -> Option<T> {
        DeltaIter::seek_item(self, pos)
    }
}

// The run adapters position by *item*, not by run — the trait's
// `nth`-based defaults would skip a run per step — so everything
// positional delegates to the inner iterator.

impl<'a, T: ColumnValueRef, C: crate::Codec + Debug> Shiftable for crate::column::Runs<'a, T, C> {
    fn get_pos(&self) -> usize {
        self.0.pos()
    }

    fn get_max(&self) -> usize {
        self.0.end_pos()
    }

    fn set_max(&mut self, pos: usize) {
        self.0.set_max(pos)
    }

    fn advance_to(&mut self, target: usize) {
        self.0.advance_to(target)
    }

    fn advance_by(&mut self, amount: usize) {
        self.0.advance_by(amount)
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<crate::Run<T::Get<'a>>> {
        self.0.shift_run(range)
    }

    fn seek_to(&mut self, pos: usize) -> Option<crate::Run<T::Get<'a>>> {
        self.0.seek_run(pos)
    }
}

impl<T: DeltaValue, C: crate::Codec + Debug> Shiftable for crate::delta::DeltaRuns<'_, T, C> {
    fn get_pos(&self) -> usize {
        self.0.pos()
    }

    fn get_max(&self) -> usize {
        self.0.end_pos()
    }

    fn set_max(&mut self, pos: usize) {
        self.0.set_max(pos)
    }

    fn advance_to(&mut self, target: usize) {
        self.0.advance_to(target)
    }

    fn advance_by(&mut self, amount: usize) {
        self.0.advance_by(amount)
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<crate::delta::DeltaRun> {
        self.0.shift_run(range)
    }

    fn seek_to(&mut self, pos: usize) -> Option<crate::delta::DeltaRun> {
        self.0.seek_run(pos)
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
    /// the lookahead is outside the window (an empty shift at it): kept,
    /// for a later shift that starts there, but not yielded
    hidden: bool,
}

impl<T: Iterator + Default> Default for Unshift<T> {
    /// An `Unshift` over the default iterator, as [`Unshift::new`] makes.
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Iterator> Unshift<T> {
    pub fn new(mut inner: T) -> Self {
        // verified (`window::u_new`)
        let next = window::u_new(&mut inner);
        Self {
            inner,
            next,
            hidden: false,
        }
    }

    pub fn peek(&self) -> Option<&T::Item> {
        // verified (`window::u_peek`)
        window::u_peek(&self.inner, &self.next, self.hidden)
    }
}

impl<T: Shiftable + Iterator> Unshift<T> {
    /// Reposition to `range`: the lookahead becomes the item at
    /// `range.start`, and iteration stops at `range.end`. The range may
    /// start at the lookahead's own item, which is then kept (hidden while
    /// the range is empty, for a later one starting there), or after it.
    pub fn shift(&mut self, range: Range<usize>) {
        // verified (`window::u_shift`): exactly the items in `range` are
        // left
        window::u_shift(
            &mut self.inner,
            &mut self.next,
            &mut self.hidden,
            range.start,
            range.end,
        )
    }
}

impl<T: Iterator> Iterator for Unshift<T> {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // verified (`window::u_next`)
        window::u_next(&mut self.inner, &mut self.next, self.hidden)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // verified (`window::u_nth`): the lookahead is item 0, so item `n`
        // is the inner's `n - 1`
        window::u_nth(&mut self.inner, &mut self.next, self.hidden, n)
    }
}

// ── The contract's bridges (trusted) ────────────────────────────────────────

/// Every iterator, as the adapters see it (`window::Stream`): trusted to
/// give its items one per position from a fixed sequence, `nth(n)` being
/// `n + 1` calls to `next`.
impl<I: Iterator + ?Sized> Stream for I {
    type Item = I::Item;

    #[cfg(verus_keep_ghost)]
    fn vals(&self) -> vstd::seq::Seq<I::Item> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn at(&self) -> vstd::prelude::int {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn left(&self) -> vstd::seq::Seq<I::Item> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn lemma_left(&self) {
        unimplemented!()
    }

    #[inline(always)]
    fn s_next(&mut self) -> Option<I::Item> {
        self.next()
    }

    #[inline(always)]
    fn s_nth(&mut self, n: usize) -> Option<I::Item> {
        self.nth(n)
    }
}

/// Every [`Shiftable`] iterator (`window::Window`): trusted to keep the
/// contract on [`Shiftable`].
impl<S: Shiftable + ?Sized> Window for S {
    #[cfg(verus_keep_ghost)]
    fn end(&self) -> vstd::prelude::int {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn lemma_window(&self) {
        unimplemented!()
    }

    #[inline(always)]
    fn w_pos(&self) -> usize {
        self.get_pos()
    }

    #[inline(always)]
    fn w_set_max(&mut self, end: usize) {
        self.set_max(end)
    }

    #[inline(always)]
    fn w_advance_to(&mut self, target: usize) {
        self.advance_to(target)
    }

    #[inline(always)]
    fn w_shift(&mut self, start: usize, end: usize) {
        self.shift(start..end)
    }

    #[inline(always)]
    fn w_shift_next(&mut self, start: usize, end: usize) -> Option<<S as Iterator>::Item> {
        self.shift_next(start..end)
    }
}

/// The ranges a caller gives [`Shiftable::ranges`] (`window::RangeList`):
/// nothing is assumed of them.
impl<R: Iterator<Item = Range<usize>>> RangeList for R {
    #[cfg(verus_keep_ghost)]
    fn given(&self) -> vstd::seq::Seq<Range<usize>> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn ended(&self) -> bool {
        unimplemented!()
    }

    #[inline(always)]
    fn next_range(&mut self) -> Option<Range<usize>> {
        self.next()
    }
}
