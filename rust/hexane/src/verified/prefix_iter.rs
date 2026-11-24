//! The iterator of a prefix column: its values, each with what the
//! values up to it add up to.
//!
//! [`Column::prefix_iter`] gives a [`PrefixIter`]. It
//! moves as a [`Iter`] does, and keeps as it goes the sum of
//! the values it has passed ([`total`](PrefixIter::total)):
//! of those from the column's start, or from where
//! [`reset_prefix`](PrefixIter::reset_prefix) was last called
//! (`base_pos`). Each value it gives comes as a
//! [`PrefixedValue`]: the value, the sum through it and the sum
//! before it. A move that finds a value ([`delta_nth`], [`advance_prefix`])
//! gives a [`PrefixSeek`]: the value with its sums, where it is,
//! and what the values passed over on the way add up to.
//!
//! [`delta_nth`]: PrefixIter::delta_nth
//! [`advance_prefix`]: PrefixIter::advance_prefix
//!
//! As with the sums a prefix column answers with, what is proved is
//! said of integers: the running sum *is* what the column's values from
//! `base_pos()` up to the iterator add up to (`sum_to`), for the sums
//! of a prefix column fit the type they are kept in.
//!
//! The moves are [`PrefixIter`]'s, the shared ones of
//! `prefix_core.rs` (`p_next`, `p_nth`, `p_next_run_from`, `p_delta_nth`,
//! `p_reset`) and `prefix_search.rs` (`p_advance`), instantiated with
//! the verified sums: a long move asks the column's index for the slab
//! to go to, and for what the slabs passed over add up to.
//!
//! An iterator's place is kept without the borrow of its column in a
//! [`PrefixIterState`] ([`PrefixIter::suspend`]), from
//! which [`PrefixIterState::try_resume`] gives an iterator
//! again: one whose running sum counts from where the suspended one's
//! did.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`prefix_iter/imp.rs`), which has
//! its contract word for word and is proved there.

use super::prefix::SumIndex;
#[cfg(verus_keep_ghost)]
use super::sums::{summed, sums_fit};
use super::sums::{PrefixValue, Sum, SumOf, SumWeight, UnsignedPrefix};
use super::Get;
#[cfg(verus_keep_ghost)]
use super::{head_run, VGet, VSpec};
use super::{Codec, Column, ColumnValueRef, Iter, IterState, Runs};
use super::{Ranges, Shiftable, Unshift};
#[cfg(verus_keep_ghost)]
use crate::btree::PrefixAggregate;
use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decoders;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, vals_of, window};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{before, PrefixWeight};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{p_delta_nth, p_next, p_next_run_from, p_nth, p_reset};
#[cfg(verus_keep_ghost)]
use crate::prefix_search::p_advance;
#[cfg(verus_keep_ghost)]
use crate::prefix_search::PrefixOrd;
#[cfg(verus_keep_ghost)]
use crate::verified::range_in;
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{Leb128, PackError, Run};
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

// ── A value with its sums ───────────────────────────────────────────────────

/// A value of a prefix column with the running sum through it, as a
/// [`PrefixIter`] gives it.
///
/// * [`prefix()`](Self::prefix) is what the values before this one add
///   up to;
/// * [`total()`](Self::total) is what the values through this one do.
///
/// Both count from where the iterator's running sum does: the column's
/// start, unless the iterator's sum was reset
/// ([`PrefixIter::reset_prefix`]). With values `[1, 1, 1]`, the
/// one at index 1 has `prefix() == 1` and `total() == 2`.
pub struct PrefixedValue<'a, V: PrefixValue> {
    /// The value.
    pub value: Get<'a, V>,
    /// The running sum through the value: what [`total()`](Self::total)
    /// returns. (Verus lets a verified client read the fields of a type
    /// only if it may read them all, so this one is public too; nothing
    /// is promised of a value a client makes itself.)
    pub total: V::Sum,
}

impl<'a, V: PrefixValue> PrefixedValue<'a, V> {
    /// [`total`](Self::total), as a specification sees it: there it is
    /// written `total()`.
    pub open spec fn spec_total(&self) -> V::Sum {
        self.total
    }

    /// [`prefix`](Self::prefix), as a specification sees it: there it is
    /// written `prefix()`. It is the sum through the value less what the
    /// value adds.
    pub open spec fn spec_prefix(&self) -> V::Sum {
        V::Sum::of_int(self.spec_total().as_int() - V::amount(self.value.sv()))
    }

    /// The running sum through this value.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline(always)]
    pub fn total(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
    {
        self.total_impl()
    }

    /// The running sum of the values before this one: `total()` less
    /// what this value adds.
    #[verifier::when_used_as_spec(spec_prefix)]
    #[inline(always)]
    pub fn prefix(&self) -> (r: V::Sum)
        ensures
            r == self.prefix(),
    {
        self.prefix_impl()
    }
}

impl<'a, V: PrefixValue> Clone for PrefixedValue<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

// ── A value found, and how far it was ───────────────────────────────────────
/// What [`PrefixIter::delta_nth`],
/// [`PrefixIter::advance_prefix`] and [`Column::delta`]
/// find: a value with its sums, where it is, and what the values passed
/// over on the way to it add up to.
///
/// With values `[1, 2, 3, 4]`, `delta_nth(2)` of an iterator at the
/// start finds the `3`: `pos == 2`, `delta == 3` (the `1` and the `2`
/// passed over), `pv.prefix() == 3` and `pv.total() == 6`.
pub struct PrefixSeek<'a, V: PrefixValue> {
    /// The index in the column of the value found. The iterator stands
    /// just after it.
    pub pos: usize,
    /// What the values the move passed over before the value found add
    /// up to: the running sum before the value, less the running sum as
    /// it was before the move.
    pub delta: V::Sum,
    /// The value found, with the running sums through it and before it.
    pub pv: PrefixedValue<'a, V>,
}

impl<'a, V: PrefixValue> Clone for PrefixSeek<'a, V> {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

// ── The iterator ────────────────────────────────────────────────────────────
/// An iterator over a prefix column's values that keeps their running
/// sum, which [`Column::prefix_iter`] and
/// [`Column::prefix_iter_range`] give.
///
/// It is a `std::iter::Iterator` of [`PrefixedValue`]s. The
/// values it has left to give are in a specification `rest()`, as a
/// [`Iter`]'s are, and what it gives of each is in `items()`.
/// Verus's specifications of the standard library's iterator functions
/// speak of `remaining()`, which is `items()`.
///
/// The running sum, [`total`](Self::total), is what the column's values
/// from `base_pos()` up to the iterator add up to: `sum_to(pos())`.
/// `base_pos()` is the column's start until
/// [`reset_prefix`](Self::reset_prefix) makes it where the iterator
/// stands.
pub struct PrefixIter<'a, V: PrefixValue + ColumnValueRef<C> + 'a, C: Codec + 'a = Leb128> {
    /// The column's index, for long moves; a default iterator has none.
    ix: Option<&'a SlabBTree<SumWeight<V::Sum>>>,
    inner: Iter<'a, V, C, SumIndex>,
    /// What the values from `base_at` up to the iterator add up to.
    total: V::Sum,
    /// What the values before `base_at` add up to.
    base: V::Sum,
    /// Where the running sum counts from: 0 until a `reset_prefix`.
    base_at: usize,
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> PrefixIter<'a, V, C> {
    /// The iterator without its sums: a [`Iter`] that stands
    /// where this one does, over the same column, with the same values
    /// left.
    pub closed spec fn plain(&self) -> Iter<'a, V, C, SumIndex> {
        self.inner
    }

    /// The values the iterator has left to give.
    pub open spec fn rest(&self) -> Seq<V::S> {
        self.plain().rest()
    }

    /// The values of the column the iterator reads: all of them, whatever
    /// the iterator's window.
    pub open spec fn column(&self) -> Seq<V::S> {
        self.plain().column()
    }

    /// The column the iterator reads, as it was when the iterator was
    /// made of it. No method of the iterator changes it.
    pub open spec fn source(&self) -> Option<Column<V, C, SumIndex>> {
        self.plain().source()
    }

    /// [`pos`](Self::pos), as a specification sees it: there it is
    /// written `pos()`.
    pub open spec fn spec_pos(&self) -> usize {
        self.plain().spec_pos()
    }

    /// [`end_pos`](Self::end_pos), as a specification sees it: there it
    /// is written `end_pos()`.
    pub open spec fn spec_end_pos(&self) -> usize {
        self.plain().spec_end_pos()
    }

    /// [`total`](Self::total), as a specification sees it: there it is
    /// written `total()`.
    pub closed spec fn spec_total(&self) -> V::Sum {
        self.total
    }

    /// The index in the column that the running sum counts from: 0,
    /// until a [`reset_prefix`](Self::reset_prefix) makes it where the
    /// iterator stands then.
    pub closed spec fn base_pos(&self) -> usize {
        self.base_at
    }

    /// What the column's values from `base_pos()` up to index `to` add
    /// up to. The running sum is `sum_to(pos())`.
    pub open spec fn sum_to(&self, to: int) -> int {
        summed::<V>(self.column().subrange(self.base_pos() as int, to))
    }

    /// What the iterator has left to give: each of the values left, with
    /// the running sum through it.
    pub closed spec fn items(&self) -> Seq<PrefixedValue<'a, V>> {
        let o = self.inner.items();
        Seq::new(
            o.len(),
            |i: int|
                PrefixedValue { value: o[i], total: V::Sum::of_int(self.sum_to(self.inner.it.pos + i + 1)) },
        )
    }

    /// The iterator stands in the column, at `base_pos()` or after it,
    /// and its running sum is what the column's values from there up to
    /// it add up to. The sums of the column's values fit.
    pub proof fn lemma_total(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
            sums_fit::<V>(self.column()),
    {
        self.lemma_total_impl()
    }

    /// What the iterator gives of each value left: the value, and the
    /// sums through it and before it.
    pub proof fn lemma_items(tracked &self)
        ensures
            self.items().len() == self.rest().len(),
            forall|i: int|
                0 <= i < self.items().len() ==> {
                    &&& (#[trigger] self.items()[i]).value.sv() == self.rest()[i]
                    &&& self.items()[i].total().as_int() == self.sum_to(self.pos() + i + 1)
                    &&& self.items()[i].prefix().as_int() == self.sum_to(self.pos() + i)
                    &&& self.items()[i].total().as_int() == self.items()[i].prefix().as_int() + V::amount(
                        self.rest()[i],
                    )
                },
    {
        self.lemma_items_impl()
    }

    /// What the iterator reads is what its column holds: of
    /// [`source`](Self::source) and [`column`](Self::column), the first
    /// tells the second.
    pub proof fn lemma_source(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        self.lemma_source_impl()
    }

    /// Where the iterator stands, and what it has left: its column's
    /// values from [`pos`](Self::pos) up to [`end_pos`](Self::end_pos), as
    /// far as the column goes (`window`). It stands in the column or at
    /// its end.
    pub proof fn lemma_rest(tracked &self)
        ensures
            self.base_pos() <= self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
            self.rest() == window(self.column(), self.pos() as int, self.end_pos() as int),
    {
        self.lemma_rest_impl()
    }

    /// The index in the column of the value `next` gives, if it gives
    /// one.
    ///
    /// What the iterator has left is the column's values from here up to
    /// [`end_pos`](Self::end_pos), as far as the column goes. The
    /// iterator stands in the column or at its end, and at `base_pos()`
    /// or after it (`lemma_rest`).
    #[verifier::when_used_as_spec(spec_pos)]
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.pos(),
    {
        self.pos_impl()
    }

    /// Where the iterator's window ends: the index in the column that
    /// its last value comes before ([`Iter::end_pos`]).
    #[verifier::when_used_as_spec(spec_end_pos)]
    #[inline(always)]
    pub fn end_pos(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
    {
        self.end_pos_impl()
    }

    /// How far the window reaches from where the iterator stands:
    /// `end_pos() - pos()`. That is how many values are left unless the
    /// window runs past the column's end; `len()` is how many are left.
    #[inline(always)]
    pub fn items_left(&self) -> (r: usize)
        ensures
            r == self.end_pos() - self.pos(),
    {
        self.items_left_impl()
    }

    /// The running sum: what the column's values from `base_pos()` up to
    /// the iterator add up to. It is the sum before the value `next`
    /// gives, if it gives one.
    #[verifier::when_used_as_spec(spec_total)]
    #[inline(always)]
    pub fn total(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
    {
        self.total_impl()
    }

    /// The running sum: [`total`](Self::total), by another name.
    #[inline(always)]
    pub fn prefix(&self) -> (r: V::Sum)
        ensures
            r == self.total(),
            self.base_pos() <= self.pos() <= self.column().len(),
            self.total().as_int() == self.sum_to(self.pos() as int),
    {
        self.prefix_impl()
    }

    /// Start the running sum again from where the iterator stands:
    /// [`total`](Self::total) and the sums of the values given from
    /// here on count from this position. The iterator does not move,
    /// and its window stays.
    #[inline(always)]
    pub fn reset_prefix(&mut self)
        ensures
            final(self).rest() == old(self).rest(),
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == old(self).end_pos(),
            final(self).base_pos() == old(self).pos(),
            final(self).total().as_int() == 0,
    {
        self.reset_prefix_impl()
    }

    /// Make the window end at `end`: the iterator then gives the
    /// column's values from where it stands up to `end`, and none if it
    /// stands at `end` or past it ([`Iter::set_max`]). The
    /// running sum stays.
    #[inline(always)]
    pub fn set_max(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total() == old(self).total(),
            final(self).pos() == old(self).pos(),
            final(self).end_pos() == if end >= old(self).pos() {
                end
            } else {
                old(self).pos()
            },
            final(self).rest() == window(old(self).column(), old(self).pos() as int, end as int),
    {
        self.set_max_impl(end)
    }

    /// Move on so that the value at index `target` of the column comes
    /// next, keeping the running sum. The iterator moves only forwards:
    /// at `target` or past it already, it stays. With a `target` past
    /// what it has left, it gives all it has, and stands at the end of
    /// that with nothing left.
    ///
    /// Few of the values passed over are looked at ([`nth`](Self::nth)).
    #[inline(always)]
    pub fn advance_to(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == past(old(self).rest(), target - old(self).pos()),
            target <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == old(self).end_pos()
            },
            old(self).pos() < target <= old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == target
                &&& final(self).end_pos() == old(self).end_pos()
            },
            target > old(self).pos() + old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.advance_to_impl(target)
    }

    /// Move `amount` values on, keeping the running sum: past as many as
    /// the iterator has left, if it has fewer.
    #[inline(always)]
    pub fn advance_by(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == past(old(self).rest(), amount as int),
            amount <= old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + amount
                &&& final(self).end_pos() == old(self).end_pos()
            },
            amount > old(self).rest().len() ==> {
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.advance_by_impl(amount)
    }

    /// Move the window to `range`, keeping the running sum: the iterator
    /// then gives the column's values there. It moves only forwards: if
    /// `range` starts before the iterator, the window starts where the
    /// iterator stands.
    ///
    /// If `range` starts past its own end or the column's, the iterator
    /// has nothing left, and stands where `range` ends or the column
    /// does, whichever comes first, if that is ahead of it.
    #[inline(always)]
    pub fn shift(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            final(self).rest() == window(
                old(self).column(),
                if range.start <= old(self).pos() {
                    old(self).pos() as int
                } else {
                    range.start as int
                },
                range.end as int,
            ),
            range.start <= old(self).pos() ==> {
                &&& final(self).pos() == old(self).pos()
                &&& final(self).end_pos() == if range.end >= old(self).pos() {
                    range.end
                } else {
                    old(self).pos()
                }
            },
            old(self).pos() < range.start <= range.end && range.start <= old(self).column().len() ==> {
                &&& final(self).pos() == range.start
                &&& final(self).end_pos() == range.end
            },
            old(self).pos() < range.start && (range.start > range.end || range.start > old(self).column().len()) ==> {
                &&& final(self).pos() == if range.end <= old(self).pos() {
                    old(self).pos() as int
                } else if range.end <= old(self).column().len() {
                    range.end as int
                } else {
                    old(self).column().len() as int
                }
                &&& final(self).end_pos() == final(self).pos()
            },
    {
        self.shift_impl(range)
    }

    /// Move the window to `range` and give its first value, if it has
    /// one, with the running sum through it. `range` is to start at the
    /// iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    #[inline(always)]
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<PrefixedValue<'a, V>>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r matches Some(pv)
                    &&& pv.value.sv() == w[0]
                    &&& pv.total() == final(self).total()
                    &&& pv.total().as_int() == old(self).sum_to(range.start + 1)
                    &&& pv.prefix().as_int() == old(self).sum_to(range.start as int)
                    &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                    &&& final(self).rest() == w.skip(1)
                    &&& final(self).pos() == range.start + 1
                    &&& final(self).end_pos() == range.end
                }
                &&& w.len() == 0 ==> {
                    &&& r is None
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == if range.end <= old(self).pos() {
                        old(self).pos() as int
                    } else if range.end <= old(self).column().len() {
                        range.end as int
                    } else {
                        old(self).column().len() as int
                    }
                    &&& final(self).end_pos() == final(self).pos()
                }
            }),
    {
        self.shift_next_impl(range)
    }

    /// The next run: the value the iterator gives next, and how many
    /// times it comes before another value does or the iterator has no
    /// more ([`Iter::next_run`]). The iterator moves past the
    /// whole run. The sums given are those of the run's last value:
    /// `total()` is the running sum through the run, and `prefix()` the
    /// sum before its last value.
    #[inline(always)]
    pub fn next_run(&mut self) -> (r: Option<Run<PrefixedValue<'a, V>>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            match r {
                Some(run) => {
                    &&& head_run(old(self).rest(), run.value.value.sv(), run.count as int)
                    &&& run.value == old(self).items()[run.count - 1]
                    &&& run.value.total() == final(self).total()
                    &&& run.value.total().as_int() == old(self).sum_to(old(self).pos() + run.count)
                    &&& run.value.prefix().as_int() == old(self).sum_to(old(self).pos() + run.count - 1)
                    &&& run.value.total().as_int() == old(self).total().as_int() + V::amount(run.value.value.sv())
                        * run.count
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                    &&& final(self).total() == old(self).total()
                },
            },
    {
        self.next_run_impl()
    }

    /// An iterator of the runs the iterator has left to give, without
    /// their sums ([`Iter::runs`]): what
    /// [`splice_runs`](Column::splice_runs) takes.
    #[inline(always)]
    pub fn runs(self) -> (r: Runs<'a, V, C, SumIndex>)
        ensures
            r.iter() == self.plain(),
    {
        self.runs_impl()
    }

    /// The `n`th of the values left, counted from 0, as
    /// [`nth`](Self::nth) gives it, with where it is and what the `n`
    /// values passed over before it add up to (`delta`). It and the
    /// values before it are then given; if there are not that many,
    /// none is left.
    #[inline(always)]
    pub fn delta_nth(&mut self, n: usize) -> (r: Option<PrefixSeek<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == old(self).pos() + n
                &&& seek.pv == old(self).items()[n as int]
                &&& seek.pv.value.sv() == old(self).rest()[n as int]
                &&& seek.delta.as_int() == summed::<V>(old(self).rest().take(n as int))
                &&& seek.pv.total() == final(self).total()
                &&& seek.pv.total().as_int() == old(self).sum_to(old(self).pos() + n + 1)
                &&& seek.pv.prefix().as_int() == old(self).sum_to(old(self).pos() + n)
                &&& seek.pv.prefix().as_int() == old(self).total().as_int() + seek.delta.as_int()
                &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).rest().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
    {
        self.delta_nth_impl(n)
    }

    /// Where the iterator stands, where its window ends and where its
    /// running sum counts from, to keep without the borrow of the
    /// column: [`PrefixIterState::try_resume`] gives an iterator
    /// from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does, with the same values left and the same running sum.
    #[inline(always)]
    pub fn suspend(&self) -> (s: PrefixIterState<V>)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            s.base_pos() == self.base_pos(),
            self.rest() == window(self.column(), s.pos() as int, s.end_pos() as int),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        self.suspend_impl()
    }
}

/// Reading on until the running sum is past a limit: for sums that are
/// counts, which no value takes down.
impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> PrefixIter<'a, V, C> where V::Sum: UnsignedPrefix {
    /// Move on to the first of the values left with which the values
    /// given from here on add up to more than `n`, and past it: it is
    /// given with its sums, where it is, and what the values passed
    /// over before it add up to (`delta`), which is `n` at most. If all
    /// the values left add up to `n` or less, the iterator gives them
    /// all, and none is found.
    ///
    /// Few of the values passed over are looked at: the column's index
    /// is asked for the slab the sum is passed in.
    ///
    /// An iterator of no column (`default()`) has no index to ask: it
    /// finds nothing, and its window is closed where it stands.
    /// ([`PrefixIter`]'s leaves the window of such
    /// an iterator as it is.)
    #[inline(always)]
    pub fn advance_prefix(&mut self, n: V::Sum) -> (r: Option<PrefixSeek<'a, V>>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
            match r {
                Some(seek) => {
                    let k = seek.pos - old(self).pos();
                    &&& 0 <= k < old(self).rest().len()
                    &&& seek.pv == old(self).items()[k]
                    &&& seek.pv.value.sv() == old(self).rest()[k]
                    &&& n.as_int() < summed::<V>(old(self).rest().take(k + 1))
                    &&& forall|j: int| 0 <= j <= k ==> summed::<V>(#[trigger] old(self).rest().take(j)) <= n.as_int()
                    &&& seek.delta.as_int() == summed::<V>(old(self).rest().take(k))
                    &&& seek.pv.total() == final(self).total()
                    &&& seek.pv.total().as_int() == old(self).sum_to(seek.pos + 1)
                    &&& seek.pv.prefix().as_int() == old(self).sum_to(seek.pos as int)
                    &&& seek.pv.prefix().as_int() == old(self).total().as_int() + seek.delta.as_int()
                    &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
                    &&& final(self).items() == old(self).items().skip(k + 1)
                    &&& final(self).rest() == old(self).rest().skip(k + 1)
                    &&& final(self).pos() == seek.pos + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& summed::<V>(old(self).rest()) <= n.as_int()
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        self.advance_prefix_impl(n)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> vstd::std_specs::iter::IteratorSpecImpl for PrefixIter<'a, V, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<PrefixedValue<'a, V>> {
        self.items()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.items().len())
    }

    open spec fn peek(&self, index: int) -> Option<PrefixedValue<'a, V>> {
        if 0 <= index < self.items().len() {
            Some(self.items()[index])
        } else {
            None
        }
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> Iterator for PrefixIter<'a, V, C> {
    type Item = PrefixedValue<'a, V>;

    /// The next value, if one is left, with the running sum through it.
    /// The sum before it is the iterator's running sum as it was.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<PrefixedValue<'a, V>>)
        ensures
            match r {
                Some(pv) => {
                    &&& old(self).rest().len() > 0
                    &&& pv.value.sv() == old(self).rest()[0]
                    &&& pv.prefix() == old(self).total()
                    &&& pv.total() == final(self).total()
                    &&& pv.prefix().as_int() == old(self).sum_to(old(self).pos() as int)
                    &&& pv.total().as_int() == old(self).sum_to(old(self).pos() + 1)
                    &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                    &&& final(self).total() == old(self).total()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
    {
        self.next_impl()
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many, with the running sum through it: it and the values before
    /// it are then given. If there are not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to and for what the slabs
    /// passed over add up to, and within a slab a whole run is passed
    /// over at once.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<PrefixedValue<'a, V>>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& r matches Some(pv)
                &&& pv.value.sv() == old(self).rest()[n as int]
                &&& pv.total() == final(self).total()
                &&& pv.total().as_int() == old(self).sum_to(old(self).pos() + n + 1)
                &&& pv.prefix().as_int() == old(self).sum_to(old(self).pos() + n)
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& final(self).rest() == old(self).rest().skip(n + 1)
                &&& final(self).pos() == old(self).pos() + n + 1
                &&& final(self).end_pos() == old(self).end_pos()
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).rest().len() == 0
                &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                &&& final(self).end_pos() == final(self).pos()
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            final(self).base_pos() == old(self).base_pos(),
            final(self).total().as_int() == old(self).sum_to(final(self).pos() as int),
    {
        self.nth_impl(n)
    }

    /// How many values are left: exactly.
    #[inline]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.items().len(),
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        self.inner.size_hint()
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for PrefixIter<'a, V, C> {
    open spec fn exact_len(&self) -> usize {
        self.items().len() as usize
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> ExactSizeIterator for PrefixIter<'a, V, C> {
    /// How many values are left.
    #[inline]
    fn len(&self) -> (len: usize)
        ensures
            len == self.items().len(),
            len == self.rest().len(),
    {
        self.size_hint().0
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> Clone for PrefixIter<'a, V, C> {
    /// An iterator of its own that stands where this one does, with the
    /// same values left and the same running sum.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
            r.rest() == self.rest(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
            r.base_pos() == self.base_pos(),
            r.total() == self.total(),
    {
        self.clone_impl()
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> Default for PrefixIter<'a, V, C> {
    /// An iterator with nothing to give.
    #[inline(always)]
    fn default() -> (r: Self)
        ensures
            r.items().len() == 0,
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
            r.base_pos() == 0,
            r.total().as_int() == 0,
    {
        Self::default_impl()
    }
}

// ── A place to come back to ─────────────────────────────────────────────────
/// Where a [`PrefixIter`] stood, where its window ended and
/// where its running sum counted from, kept without the borrow of the
/// column: [`PrefixIter::suspend`] gives it, and
/// [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds a [`IterState`] (`plain()`) and `base_pos()`. The
/// sums themselves are not kept: `try_resume` asks the column for them.
pub struct PrefixIterState<V: PrefixValue> {
    inner: IterState,
    /// Where the running sum counts from.
    base_at: usize,
    _phantom: PhantomData<V>,
}

impl<V: PrefixValue> PrefixIterState<V> {
    /// The state without where the sum counts from: that of a
    /// [`Iter`] standing where the iterator stood, with its
    /// window.
    pub closed spec fn plain(&self) -> IterState {
        self.inner
    }

    /// Where the iterator stood: its `pos()`.
    pub open spec fn pos(&self) -> usize {
        self.plain().pos()
    }

    /// Where the iterator's window ended: its `end_pos()`.
    pub open spec fn end_pos(&self) -> usize {
        self.plain().end_pos()
    }

    /// The count of edits of the column the state was taken of
    /// (`Column::edits`).
    pub open spec fn edits(&self) -> usize {
        self.plain().edits()
    }

    /// Where the iterator's running sum counted from: its `base_pos()`.
    pub closed spec fn base_pos(&self) -> usize {
        self.base_at
    }

    /// The state is a place in `column` as it is
    /// ([`IterState`]'s `fits`):
    /// [`try_resume`](Self::try_resume) gives an iterator exactly then.
    pub open spec fn fits<C: Codec>(&self, column: &Column<V, C, SumIndex>) -> bool where V: ColumnValueRef<C> {
        self.plain().fits(column)
    }

    /// A state fits only a column that counts the edits it does: one
    /// edited since the state was taken is refused
    /// ([`IterState`]'s `lemma_fits`).
    pub proof fn lemma_fits<C: Codec>(&self, column: &Column<V, C, SumIndex>) where V: ColumnValueRef<C>
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
        self.lemma_fits_impl::<C>(column)
    }

    /// The running sum counts from the iterator's place or before it.
    pub proof fn lemma_base(tracked &self)
        ensures
            self.base_pos() <= self.pos(),
    {
        self.lemma_base_impl()
    }

    /// An iterator of `column` that stands where the suspended one
    /// stood, with its window, and whose running sum counts from where
    /// that one's did, if the state fits the column (`fits`); an error
    /// ([`PackError::InvalidResume`]) if it does not
    /// ([`IterState::try_resume`]).
    ///
    /// The iterator given is one of `column` whatever column the state
    /// was taken of, and its running sum is `column`'s: what the values
    /// of `column` from `base_pos()` up to the iterator add up to, which
    /// the column's index gives.
    ///
    /// Panics, as [`PrefixColumn`](crate::PrefixColumn)'s does, where an
    /// edit's panic left `column` without its parts and the state would
    /// fit it.
    #[inline(always)]
    pub fn try_resume<'a, C: Codec>(&self, column: &'a Column<V, C, SumIndex>) -> (r: Result<
        PrefixIter<'a, V, C>,
        PackError,
    >) where V: ColumnValueRef<C>
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
                &&& it.base_pos() == self.base_pos()
                &&& it.total().as_int() == summed::<V>(column@.subrange(self.base_pos() as int, self.pos() as int))
            },
    {
        self.try_resume_impl::<C>(column)
    }
}

// ── The column's ────────────────────────────────────────────────────────────
/// A prefix column's values with their running sums.
impl<V: PrefixValue + ColumnValueRef<C>, C: Codec> Column<V, C, SumIndex> {
    /// An iterator over the values that keeps their running sum: each
    /// value comes with what the values through it add up to, and what
    /// those before it do.
    #[inline(always)]
    pub fn prefix_iter(&self) -> (it: PrefixIter<'_, V, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
            it.base_pos() == 0,
            it.total().as_int() == 0,
    {
        self.prefix_iter_impl()
    }

    /// An iterator over the values of `range`, clamped to the column
    /// (`range_in`) as [`Column::iter_range`](super::Column::iter_range)
    /// clamps it, that keeps the running sum, which counts from the
    /// column's start: the sum before the first value it gives is what
    /// the values before `start` add up to. The index gives that; the values before `start` are not all
    /// read.
    #[inline(always)]
    pub fn prefix_iter_range(&self, range: Range<usize>) -> (it: PrefixIter<'_, V, C>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
                &&& it.base_pos() == 0
                &&& it.total().as_int() == summed::<V>(self@.take(start))
            }),
    {
        self.prefix_iter_range_impl(range)
    }

    /// The value at `index` with what the values through it add up to,
    /// and what those before it do; `None` if `index` is past the end.
    #[inline(always)]
    pub fn get_prefixed(&self, index: usize) -> (r: Option<PrefixedValue<'_, V>>)
        ensures
            index < self@.len() ==> {
                &&& r matches Some(pv)
                &&& pv.value.sv() == self@[index as int]
                &&& pv.total().as_int() == summed::<V>(self@.take(index + 1))
                &&& pv.prefix().as_int() == summed::<V>(self@.take(index as int))
                &&& pv.total().as_int() == pv.prefix().as_int() + V::amount(pv.value.sv())
            },
            index >= self@.len() ==> r is None,
    {
        self.get_prefixed_impl(index)
    }

    /// The value at `to` with its sums, as
    /// [`get_prefixed`](Self::get_prefixed) gives them, and what the
    /// values `from..to` before it add up to (`delta`); `None` if `to`
    /// is past the end. `from` is to be `to` at most.
    #[inline(always)]
    pub fn delta(&self, from: usize, to: usize) -> (r: Option<PrefixSeek<'_, V>>)
        requires
            from <= to,
        ensures
            to < self@.len() ==> {
                &&& r matches Some(seek)
                &&& seek.pos == to
                &&& seek.pv.value.sv() == self@[to as int]
                &&& seek.delta.as_int() == summed::<V>(self@.subrange(from as int, to as int))
                &&& seek.pv.total().as_int() == summed::<V>(self@.take(to + 1))
                &&& seek.pv.prefix().as_int() == summed::<V>(self@.take(to as int))
                &&& seek.pv.total().as_int() == seek.pv.prefix().as_int() + V::amount(seek.pv.value.sv())
            },
            to >= self@.len() ==> r is None,
    {
        self.delta_impl(from, to)
    }
}

// ── Repositioning (`Shiftable`) ───────────────────────────────────────────

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> PrefixIter<'a, V, C> {
    /// The items the iterator ranges over: at each place of the column,
    /// the value there with the running sum through it (from
    /// `base_pos()`).
    pub closed spec fn prefixed(&self) -> Seq<PrefixedValue<'a, V>> {
        let o = vals_of::<V::X>(self.inner.slabs@);
        Seq::new(o.len(), |p: int| PrefixedValue { value: o[p], total: V::Sum::of_int(self.sum_to(p + 1)) })
    }
}

/// The iterator repositioned as any of the verified ones is: its model
/// is each value of the column with the running sum through it, each an
/// item.
impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> Shiftable for PrefixIter<'a, V, C> {
    closed spec fn shift_vals(&self) -> Seq<PrefixedValue<'a, V>> {
        self.prefixed()
    }

    closed spec fn shift_at(&self) -> int {
        self.inner.it.pos as int
    }

    closed spec fn shift_end(&self) -> int {
        self.inner.it.pos + self.inner.it.items_left
    }

    closed spec fn shift_left(&self) -> Seq<PrefixedValue<'a, V>> {
        self.items()
    }

    closed spec fn shift_items(&self) -> Seq<PrefixedValue<'a, V>> {
        self.items()
    }

    open spec fn shift_spans(x: PrefixedValue<'a, V>) -> Seq<PrefixedValue<'a, V>> {
        seq![x]
    }

    open spec fn shift_unit() -> bool {
        true
    }

    proof fn lemma_shift(&self) {
        self.lemma_shift_impl()
    }

    proof fn lemma_shift_unit(x: PrefixedValue<'a, V>) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<PrefixedValue<'a, V>>) {
        self.shift_step_impl()
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<PrefixedValue<'a, V>>) {
        self.shift_nth_impl(n)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        self.dup_impl()
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        self.get_pos_impl()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        self.get_max_impl()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        self.set_max_v_shiftable_impl(end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        self.advance_to_v_shiftable_impl(target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        self.advance_by_v_shiftable_impl(amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<PrefixedValue<'a, V>>) {
        self.seek_to_impl(at)
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        self.shift_v_shiftable_impl(range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<PrefixedValue<'a, V>>) {
        self.shift_next_v_shiftable_impl(range)
    }
}

impl<'a, V: PrefixValue + ColumnValueRef<C>, C: Codec> PrefixIter<'a, V, C> {
    /// The iterator's repositioning model ([`Shiftable`]) in the terms
    /// of its own specification: at each place of the column, its value
    /// and what the values come to through it; it stands at `pos()`;
    /// what it has left is `items()`, each item a value.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals() == self.prefixed(),
            self.prefixed().len() == self.column().len(),
            forall|p: int| 0 <= p < self.column().len() ==> {
                &&& (#[trigger] self.prefixed()[p]).value.sv() == self.column()[p]
                &&& self.prefixed()[p].total == V::Sum::of_int(self.sum_to(p + 1))
            },
            self.shift_at() == self.pos(),
            self.shift_items() == self.items(),
            self.shift_left() == self.items(),
            Self::shift_unit(),
    {
        self.lemma_shift_model_impl()
    }

    /// A lookahead: the iterator wrapped so that the item it gives next
    /// can be looked at first ([`Unshift::peek`]).
    #[inline(always)]
    pub fn unshift(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.prefixed(),
            r.unshift_left() == self.items(),
    {
        self.unshift_impl()
    }

    /// The items of each of `ranges` of the column, one range after
    /// another (what is
    /// left of the iterator's own window is dropped). The ranges ascend
    /// and do not overlap (they may touch), and start where the iterator
    /// stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.prefixed(),
            r.ranges_left() == windows(self.prefixed(), ranges@),
    {
        self.ranges_impl(ranges)
    }
}

} // verus!
