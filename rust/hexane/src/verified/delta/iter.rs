//! The iterator of a delta column: the column's values, each the sum
//! of the differences the column keeps up to it.
//!
//! [`iter`](DeltaColumn::iter) and
//! [`iter_range`](DeltaColumn::iter_range) of a
//! [`DeltaColumn`] give a [`DeltaIter`]. It moves as a
//! [`Iter`](crate::verified::Iter) does and says the
//! same of each move: what it has left (`rest`) is the column's values
//! from where it stands ([`pos`](DeltaIter::pos)) to where its
//! window ends ([`end_pos`](DeltaIter::end_pos)), as far as the
//! column goes.
//!
//! It keeps as it goes the sum of the differences before it, so that
//! the next value is one addition away. The moves are
//! [`DeltaIter`]'s, the shared ones of
//! `delta/diter.rs` (`d_next`, `d_step`): a long move asks the column's
//! index for the slab to go to and for the sum of the differences
//! passed over, and within a slab a whole run is passed over at once.
//!
//! What is proved of every value the iterator gives is that it is the
//! column's value at that place. That it is one of the type's is the
//! column's invariant, which the iterator keeps with the borrow of the
//! column: no value is checked as it is read.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`iter/imp.rs`), which has
//! its contract word for word and is proved there.

#[cfg(verus_keep_ghost)]
use super::diffs::{delta_run_units, delta_units};
use super::{bounds_of, DCore, DIt, DKind, DeltaColumn, DeltaValue, DE, DX};
#[cfg(verus_keep_ghost)]
use super::{
    delta_prefix, delta_run_fits, delta_run_values, delta_runs_at, delta_within, deltas, vals,
};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{d_next, d_step};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{past, resumes, window, Decode};
use crate::iter_core::{Decoders, ICore};
use crate::verified::codec::Codec;
use crate::verified::IterState;
#[cfg(verus_keep_ghost)]
use crate::verified::{bound_end, bound_start, bounds_in, head_run, range_in, runs_of, sorted};
use crate::verified::{Ranges, Shiftable, Unshift};
#[cfg(verus_keep_ghost)]
use crate::window::windows;
use crate::{DeltaRun, Leb128, PackError};
use std::ops::{Range, RangeBounds};
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

mod imp;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(super) use imp::{kept_in, lemma_kept_hit};

verus! {

/// An iterator over the values of a [`DeltaColumn`], or over
/// those in a range of it.
///
/// It stands at an index of the column ([`pos`](Self::pos)) and has a
/// window that ends at another ([`end_pos`](Self::end_pos)): what it has
/// left to give (`rest`) is the column's values from the one to the
/// other, as far as the column goes. Each method says what
/// it gives, what is left after it, and where the iterator then stands.
pub struct DeltaIter<'a, T: DeltaValue<C>, C: Codec = Leb128> {
    it: ICore<<DX<T, C> as Decoders<'a>>::D>,
    slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>],
    /// The sum of the differences before the iterator, wrapping as an
    /// `i64` does.
    running: i64,
    /// The column's index, for long moves; a default iterator has none.
    ix: Option<&'a SlabBTree<SlabAgg>>,
    /// The column the slabs are of; a default iterator has none.
    col: Option<&'a DCore<T, C>>,
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    /// The values of the column the iterator reads: all of them,
    /// whatever the iterator's window.
    pub closed spec fn column(&self) -> Seq<T> {
        vals::<T>(self.kept())
    }

    /// The values the iterator has left to give: the column's from
    /// where it stands to where its window ends, as far as the column
    /// goes.
    pub open spec fn rest(&self) -> Seq<T> {
        window(self.column(), self.pos() as int, self.end_pos() as int)
    }

    /// The column the iterator reads, as it was when the iterator was
    /// made of it; a default iterator reads none. No method of the
    /// iterator changes it.
    pub closed spec fn source(&self) -> Option<DeltaColumn<T, C>> {
        match self.col {
            Some(c) => Some(DeltaColumn { core: *c }),
            None => None,
        }
    }

    /// [`pos`](Self::pos), as a specification sees it: there it is
    /// written `pos()`.
    pub closed spec fn spec_pos(&self) -> usize {
        self.it.pos
    }

    /// [`end_pos`](Self::end_pos), as a specification sees it: there it
    /// is written `end_pos()`.
    pub closed spec fn spec_end_pos(&self) -> usize {
        (self.it.pos + self.it.items_left) as usize
    }

    /// What the iterator reads is what its column holds: of `source`
    /// and `column`, the first tells the second.
    pub proof fn lemma_source(tracked &self)
        ensures
            match self.source() {
                Some(c) => self.column() == c@,
                None => self.column().len() == 0,
            },
    {
        self.lemma_source_impl()
    }

    /// The index in the column of the value `next` gives, if it gives
    /// one.
    ///
    /// What the iterator has left is the column's values from here up to
    /// [`end_pos`](Self::end_pos), as far as the column goes. The
    /// iterator stands in the column or at its end.
    #[verifier::when_used_as_spec(spec_pos)]
    #[inline(always)]
    pub fn pos(&self) -> (r: usize)
        ensures
            r == self.pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
    {
        self.pos_impl()
    }

    /// Where the iterator's window ends: the index in the column that
    /// its last value comes before. It can lie past the column's end
    /// ([`set_max`](Self::set_max)), and the iterator then gives what the
    /// column has. Once the iterator has given all it has, its window
    /// ends where it stands.
    #[verifier::when_used_as_spec(spec_end_pos)]
    #[inline(always)]
    pub fn end_pos(&self) -> (r: usize)
        ensures
            r == self.end_pos(),
            self.pos() <= self.end_pos(),
            self.pos() <= self.column().len(),
    {
        self.end_pos_impl()
    }

    /// Make the window end at `end`: the iterator then gives the
    /// column's values from where it stands up to `end`, and none if it
    /// stands at `end` or past it.
    ///
    /// This can widen the window as well as narrow it. With an `end`
    /// past the column's, the iterator gives what the column has.
    #[inline(always)]
    pub fn set_max(&mut self, end: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
    /// next. The iterator moves only forwards: at `target` or past it
    /// already, it stays. With a `target` past what it has left, it
    /// gives all it has, and stands at the end of that with nothing left.
    ///
    /// Few of the values passed over are looked at
    /// ([`nth`](Self::nth)).
    #[inline(always)]
    pub fn advance_to(&mut self, target: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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

    /// Move `amount` values on: past as many as the iterator has left,
    /// if it has fewer.
    #[inline(always)]
    pub fn advance_by(&mut self, amount: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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

    /// Move the window to `range`: the iterator then gives the column's
    /// values there. It moves only forwards: if `range` starts before
    /// the iterator, the window starts where the iterator stands.
    ///
    /// If `range` starts past its own end or the column's, the iterator
    /// has nothing left, and stands where `range` ends or the column
    /// does, whichever comes first, if that is ahead of it.
    #[inline(always)]
    pub fn shift(&mut self, range: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
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
    /// one. `range` is to start at the iterator or after it.
    ///
    /// If the window has no value, the iterator has nothing left, and
    /// stands where [`shift`](Self::shift) leaves it then.
    #[allow(clippy::manual_map)]
    #[inline(always)]
    pub fn shift_next(&mut self, range: Range<usize>) -> (r: Option<T>)
        requires
            range.start >= old(self).pos(),
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let w = window(old(self).column(), range.start as int, range.end as int);
                &&& w.len() > 0 ==> {
                    &&& r == Some(w[0])
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

    /// The next run of the differences the column keeps: how much the
    /// value the iterator gives next differs from the number before it
    /// (`delta`), how many values in a row differ by that much each from
    /// the one before (`count`), and the number before the first of
    /// them (`prefix`). A run of nulls has no difference. The iterator
    /// moves past the whole run.
    ///
    /// The run says which values the iterator moved past
    /// (`delta_run_values`), and they are the type's
    /// (`delta_run_fits`): it is what
    /// [`splice_runs`](DeltaColumn::splice_runs) takes.
    ///
    /// A run is as long as the differences make it, up to the window's
    /// end: how the column lies in memory does not cut it short.
    #[inline(always)]
    pub fn next_run(&mut self) -> (r: Option<DeltaRun>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(run) => {
                    &&& head_run(
                        window(deltas(old(self).column()), old(self).pos() as int, old(self).end_pos() as int),
                        run.delta,
                        run.count as int,
                    )
                    &&& run.prefix == delta_prefix(old(self).column(), old(self).pos() as int)
                    &&& delta_run_values::<T>(run) == old(self).rest().take(run.count as int)
                    &&& delta_run_fits::<T>(run)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
    {
        self.next_run_impl()
    }

    /// An iterator of the runs of differences the iterator has left to
    /// give ([`next_run`](Self::next_run)): what
    /// [`splice_runs`](DeltaColumn::splice_runs) takes.
    #[inline(always)]
    pub fn runs(self) -> (r: DeltaRuns<'a, T, C>)
        ensures
            r.iter() == self,
    {
        self.runs_impl()
    }

    /// Where the iterator stands and where its window ends, to keep
    /// without the borrow of the column:
    /// [`DeltaIterState::try_resume`] gives an iterator from it.
    ///
    /// The state fits the column the iterator reads: resumed on that
    /// column, as it is, it gives an iterator that stands where this one
    /// does and has the same values left.
    #[inline(always)]
    pub fn suspend(&self) -> (s: DeltaIterState)
        ensures
            s.pos() == self.pos(),
            s.end_pos() == self.end_pos(),
            match self.source() {
                Some(c) => s.fits(&c),
                None => true,
            },
    {
        self.suspend_impl()
    }

    /// Move on to the first of the values left that is `target`, and
    /// past it: its index in the column is given. If none of them is
    /// `target`, the iterator gives all it has, as
    /// [`advance_to`](Self::advance_to) leaves it then.
    ///
    /// The values need not be in order. Few of them are looked at where
    /// the column's index can tell that a part of the column holds no
    /// `target`; a search for the null reads the values a run at a
    /// time.
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    #[inline(always)]
    pub fn scan_to_value(&mut self, target: T) -> (r: Option<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            match r {
                Some(at) => {
                    &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                    &&& old(self).rest()[at - old(self).pos()] == target
                    &&& forall|i: int| 0 <= i < at - old(self).pos() ==> #[trigger] old(self).rest()[i] != target
                    &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                    &&& final(self).pos() == at + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& forall|i: int| 0 <= i < old(self).rest().len() ==> #[trigger] old(self).rest()[i] != target
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                    &&& final(self).end_pos() == final(self).pos()
                },
            },
    {
        self.scan_to_value_impl(target)
    }

    /// Move on to the first of the values left that is within the
    /// bounds of `range`, and past it: given are its index in the
    /// column and the value. If none of them is, the iterator gives
    /// all it has, as [`advance_to`](Self::advance_to) leaves it then.
    ///
    /// The bounds are in the order of the type's values
    /// (`VSpec::below`), and a null is within no bounds
    /// (`delta_within`). A bound can be any value of the type, also
    /// one that no delta column holds: a null as a bound comes before
    /// every number, and a number that does not fit comes after every
    /// value a column holds.
    ///
    /// The values need not be in order. Few of them are looked at where
    /// the column's index can tell that a part of the column holds
    /// none within the bounds.
    #[inline(always)]
    pub fn scan_to_range<R: RangeBounds<T>>(&mut self, range: R) -> (r: Option<(usize, T)>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                let (lo, hi) = (range.spec_start_bound(), range.spec_end_bound());
                match r {
                    Some((at, v)) => {
                        &&& old(self).pos() <= at < old(self).pos() + old(self).rest().len()
                        &&& old(self).rest()[at - old(self).pos()] == v
                        &&& delta_within::<T>(lo, hi, v)
                        &&& forall|i: int|
                            0 <= i < at - old(self).pos() ==> !delta_within::<T>(lo, hi, #[trigger] old(self).rest()[i])
                        &&& final(self).rest() == old(self).rest().skip(at - old(self).pos() + 1)
                        &&& final(self).pos() == at + 1
                        &&& final(self).end_pos() == old(self).end_pos()
                    },
                    None => {
                        &&& forall|i: int|
                            0 <= i < old(self).rest().len() ==> !delta_within::<T>(lo, hi, #[trigger] old(self).rest()[i])
                        &&& final(self).rest().len() == 0
                        &&& final(self).pos() == old(self).pos() + old(self).rest().len()
                        &&& final(self).end_pos() == final(self).pos()
                    },
                }
            }),
    {
        self.scan_to_range_impl::<R>(range)
    }

    /// Search the values of the column in `range` for `target`, where
    /// they are in order, and move on to it: given is the range of the
    /// places that hold it, and the iterator gives the first of them
    /// next. If no value there is `target`, the range is the empty one
    /// at the place where it would stand, and the iterator stands
    /// there.
    ///
    /// The search is over `range` as far as that lies ahead of the
    /// iterator and in the column. The window is made to end where
    /// `range` ends ([`set_max`](Self::set_max)), whether that is
    /// before its end or after it, and the iterator moves on to where
    /// `range` starts ([`advance_to`](Self::advance_to)); a `range`
    /// that starts past its end is taken as the empty one at its
    /// start. The bounds are read as `bound_start` and `bound_end` say:
    /// `..` is all the window has ahead of the iterator.
    ///
    /// Few of the values are looked at: the column's index tells the
    /// part of the column to look in. The values searched are taken to
    /// be in order (`sorted`); where they are not, the range is some
    /// range of the places searched. A `target` that does not fit comes
    /// after every value of a column: the iterator then gives all it
    /// has, and its window ends where it stands.
    #[inline(always)]
    pub fn seek_to_value<R: RangeBounds<usize>>(&mut self, target: T, range: R) -> (r: Range<usize>)
        ensures
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
            ({
                // where the range starts and ends
                let (rs, re) = (
                    bound_start(range.spec_start_bound()),
                    bound_end(range.spec_start_bound(), range.spec_end_bound(), old(self).end_pos() as int),
                );
                let (p, c) = (old(self).pos() as int, old(self).column());
                // where the window is made to end
                let e = if rs <= re {
                    re as int
                } else {
                    rs as int
                };
                // the places searched are `s..t` of the column
                let t = if e <= p {
                    p
                } else if e <= c.len() {
                    e
                } else {
                    c.len() as int
                };
                let s = if rs <= p {
                    p
                } else if rs <= t {
                    rs as int
                } else {
                    t
                };
                &&& p <= s <= r.start <= r.end <= t <= c.len()
                &&& final(self).pos() == r.start
                &&& final(self).rest() == c.subrange(r.start as int, t)
                &&& final(self).end_pos() == if rs > t || !target.fits() {
                    r.start as int
                } else if e >= p {
                    e
                } else {
                    p
                }
                &&& sorted::<T>(c.subrange(s, t)) ==> forall|k: int|
                    s <= k < t ==> {
                        &&& (k < r.start) == T::below(#[trigger] c[k], target)
                        &&& (r.start <= k < r.end) == (c[k] == target)
                        &&& (r.end <= k) == T::below(target, c[k])
                    }
            }),
    {
        self.seek_to_value_impl::<R>(target, range)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: DeltaValue<C>, C: Codec> vstd::std_specs::iter::IteratorSpecImpl for DeltaIter<'a, T, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<T> {
        self.rest()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.rest().len())
    }

    open spec fn peek(&self, index: int) -> Option<T> {
        if 0 <= index < self.rest().len() {
            Some(self.rest()[index])
        } else {
            None
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Iterator for DeltaIter<'a, T, C> {
    type Item = T;

    /// The next value, if one is left.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<T>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                    &&& final(self).end_pos() == old(self).end_pos()
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                    &&& final(self).end_pos() == old(self).pos()
                },
            },
            final(self).column() == old(self).column(),
            final(self).source() == old(self).source(),
    {
        self.next_impl()
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a long move asks the
    /// column's index for the slab to go to and for the sum of the
    /// differences passed over, and within a slab a whole run is passed
    /// over at once.
    #[allow(clippy::manual_map)]
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<T>)
        ensures
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).rest()[n as int])
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
    {
        self.nth_impl(n)
    }

    /// How many values are left: exactly.
    #[inline(always)]
    fn size_hint(&self) -> (r: (usize, Option<usize>))
        ensures
            r.0 == self.rest().len(),
            r.1 == Some(r.0),
    {
        self.size_hint_impl()
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: DeltaValue<C>, C: Codec> vstd::std_specs::iter::ExactSizeIteratorSpecImpl for DeltaIter<'a, T, C> {
    open spec fn exact_len(&self) -> usize {
        self.rest().len() as usize
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> ExactSizeIterator for DeltaIter<'a, T, C> {
    /// How many values are left.
    #[inline]
    fn len(&self) -> (len: usize)
        ensures
            len == self.rest().len(),
    {
        self.size_hint().0
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Clone for DeltaIter<'a, T, C> {
    /// An iterator of its own with the same values left.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r.rest() == self.rest(),
            r.column() == self.column(),
            r.source() == self.source(),
            r.pos() == self.pos(),
            r.end_pos() == self.end_pos(),
    {
        self.clone_impl()
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Default for DeltaIter<'a, T, C> {
    /// An iterator with nothing to give.
    #[inline(always)]
    fn default() -> (r: Self)
        ensures
            r.rest().len() == 0,
            r.column().len() == 0,
            r.source() is None,
            r.pos() == 0,
            r.end_pos() == 0,
    {
        Self::default_impl()
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    /// An iterator over the values.
    #[inline(always)]
    pub fn iter(&self) -> (it: DeltaIter<'_, T, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        self.iter_impl()
    }

    /// An iterator over the values of `range`, clamped to the column
    /// (`range_in`): over `start..end`.
    ///
    /// The iterator moves from the column's start to `start` as
    /// [`advance_by`](DeltaIter::advance_by) does: the column's
    /// index gives the sum of the differences before `start`.
    #[inline(always)]
    pub fn iter_range(&self, range: Range<usize>) -> (it: DeltaIter<'_, T, C>)
        ensures
            ({
                let (start, end) = range_in(range, self@.len());
                &&& it.rest() == self@.subrange(start, end)
                &&& it.column() == self@
                &&& it.source() == Some(*self)
                &&& it.pos() == start
                &&& it.end_pos() == end
            }),
    {
        self.iter_range_impl(range)
    }

    /// The range of the places of `range` that hold `value`, where the
    /// values there are in order: the first such place and the place
    /// after the last. If no value there is `value`, the range is the
    /// empty one at the place where it would stand. `range` is clamped to
    /// the column (`bounds_in`): call `start..end` the places it names
    /// there.
    ///
    /// Few of the values are looked at: the column's index tells the
    /// part of the column to look in. The values of `start..end` are
    /// taken to be in order (`sorted`); where they are not, the range is
    /// some range of the places in `start..end`. A `value` that does not
    /// fit comes after every value of a column.
    #[inline(always)]
    pub fn scope_to_value<R: RangeBounds<usize>>(&self, value: T, range: R) -> (r: Range<usize>)
        ensures
            ({
                let (start, end) = bounds_in(range.spec_start_bound(), range.spec_end_bound(), self@.len());
                &&& start <= r.start <= r.end <= end
                &&& sorted::<T>(self@.subrange(start, end)) ==> forall|k: int|
                    start <= k < end ==> {
                        &&& (k < r.start) == T::below(#[trigger] self@[k], value)
                        &&& (r.start <= k < r.end) == (self@[k] == value)
                        &&& (r.end <= k) == T::below(value, self@[k])
                    }
            }),
    {
        self.scope_to_value_impl::<R>(value, range)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> IntoIterator for &'a DeltaColumn<T, C> {
    type Item = T;

    type IntoIter = DeltaIter<'a, T, C>;

    /// An iterator over the values ([`DeltaColumn::iter`]).
    #[inline(always)]
    fn into_iter(self) -> (it: DeltaIter<'a, T, C>)
        ensures
            it.rest() == self@,
            it.column() == self@,
            it.source() == Some(*self),
            it.pos() == 0,
            it.end_pos() == self@.len(),
    {
        self.iter()
    }
}

/// An iterator of the runs of differences of a delta column's values,
/// which [`DeltaIter::runs`] gives: each run is of as many
/// equal differences as lie together in what the iterator had left
/// (`delta_runs_at` of their `runs_of`), with the number before it.
pub struct DeltaRuns<'a, T: DeltaValue<C>, C: Codec = Leb128>(DeltaIter<'a, T, C>);

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    /// The iterator of values whose runs these are, where it stands now.
    pub closed spec fn iter(&self) -> DeltaIter<'a, T, C> {
        self.0
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: DeltaValue<C>, C: Codec> vstd::std_specs::iter::IteratorSpecImpl for DeltaRuns<'a, T, C> {
    /// The iterator keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one run after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<DeltaRun> {
        delta_runs_at(
            self.iter().column(),
            self.iter().pos() as int,
            runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
        )
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.iter().rest().len())
    }

    open spec fn peek(&self, index: int) -> Option<DeltaRun> {
        let rs = delta_runs_at(
            self.iter().column(),
            self.iter().pos() as int,
            runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
        );
        if 0 <= index < rs.len() {
            Some(rs[index])
        } else {
            None
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Iterator for DeltaRuns<'a, T, C> {
    type Item = DeltaRun;

    /// The next run, if one is left ([`DeltaIter::next_run`]).
    #[inline(always)]
    fn next(&mut self) -> (r: Option<DeltaRun>)
        ensures
            final(self).iter().column() == old(self).iter().column(),
            final(self).iter().source() == old(self).iter().source(),
            match r {
                Some(run) => {
                    &&& head_run(
                        window(
                            deltas(old(self).iter().column()),
                            old(self).iter().pos() as int,
                            old(self).iter().end_pos() as int,
                        ),
                        run.delta,
                        run.count as int,
                    )
                    &&& run.prefix == delta_prefix(old(self).iter().column(), old(self).iter().pos() as int)
                    &&& delta_run_values::<T>(run) == old(self).iter().rest().take(run.count as int)
                    &&& delta_run_fits::<T>(run)
                    &&& final(self).iter().rest() == old(self).iter().rest().skip(run.count as int)
                    &&& final(self).iter().pos() == old(self).iter().pos() + run.count
                    &&& final(self).iter().end_pos() == old(self).iter().end_pos()
                },
                None => {
                    &&& old(self).iter().rest().len() == 0
                    &&& final(self).iter().rest().len() == 0
                    &&& final(self).iter().pos() == old(self).iter().pos()
                    &&& final(self).iter().end_pos() == old(self).iter().pos()
                },
            },
    {
        self.next_impl()
    }
}

/// Where a [`DeltaIter`] stood and where its window ended, kept
/// without the borrow of the column: [`DeltaIter::suspend`]
/// gives it, and [`try_resume`](Self::try_resume) an iterator from it.
///
/// It holds the iterator's place in the column's slabs, and how many
/// slabs and edits the column counted then, as a
/// [`IterState`] does. It does not hold the sum of the
/// differences before the place: `try_resume` takes that from the
/// column it resumes on. It is of no value type: a state fits any delta
/// column whose slabs it is a place in.
pub struct DeltaIterState {
    inner: IterState,
}

impl DeltaIterState {
    /// Where the iterator stood: its `pos()`.
    pub closed spec fn pos(&self) -> usize {
        self.inner.pos
    }

    /// Where the iterator's window ended: its `end_pos()`.
    pub closed spec fn end_pos(&self) -> usize {
        (self.inner.pos + self.inner.items_left) as usize
    }

    /// The count of edits of the column the state was taken of
    /// (`DeltaColumn::edits`).
    pub closed spec fn edits(&self) -> usize {
        self.inner.counter
    }

    /// The state is a place in `column` as it is: the column counts the
    /// edits and the slabs that the state does, the state's place is one
    /// in those slabs, and it is in the column or at its end.
    /// [`try_resume`](Self::try_resume) gives an iterator exactly then.
    pub closed spec fn fits<T: DeltaValue<C>, C: Codec>(&self, column: &DeltaColumn<T, C>) -> bool {
        let lens = slab_lens(column.core.slabs_spec());
        &&& self.inner.counter == column.core.count()
        &&& self.inner.num_slabs == lens.len()
        &&& resumes(lens, self.inner.slab_idx, self.inner.slab_consumed, self.inner.pos, self.inner.items_left)
        &&& self.inner.pos <= total(lens)
    }

    /// A state fits only a column that counts the edits it does. A
    /// column that has been edited since counts others
    /// (`lemma_after_edits`), so the state does not fit it, and
    /// [`try_resume`](Self::try_resume) refuses it.
    pub proof fn lemma_fits<T: DeltaValue<C>, C: Codec>(&self, column: &DeltaColumn<T, C>)
        requires
            self.fits(column),
        ensures
            self.edits() == column.edits(),
    {
        self.lemma_fits_impl::<T, C>(column)
    }

    /// An iterator of `column` that stands where the suspended one stood,
    /// with its window, if the state fits the column (`fits`); an error
    /// ([`PackError::InvalidResume`]) if it does not.
    ///
    /// A state fits the column it was taken of, as long as that is not
    /// edited ([`DeltaIter::suspend`]). The iterator given is
    /// one of `column` whatever column the state was taken of: it gives
    /// the values of `column` in the state's window, and the sum it
    /// keeps is of that column's differences. Where the state stands at
    /// the column's end, the iterator's window ends there.
    ///
    /// An edit moves the column's count of edits
    /// (`DeltaColumn::edits`), as each method that edits says,
    /// so a state taken before an edit is refused after it
    /// (`lemma_fits`), and after more edits unless they are
    /// `usize::MAX + 1` or more (`lemma_after_edits`).
    ///
    /// The sum of the differences before the place is found as
    /// [`advance_by`](DeltaIter::advance_by) finds it, with the
    /// column's index.
    #[inline(always)]
    pub fn try_resume<'a, T: DeltaValue<C>, C: Codec>(&self, column: &'a DeltaColumn<T, C>) -> (r: Result<
        DeltaIter<'a, T, C>,
        PackError,
    >)
        ensures
            r is Ok <==> self.fits(column),
            r is Ok ==> self.edits() == column.edits(),
            r matches Ok(it) ==> {
                &&& it.source() == Some(*column)
                &&& it.column() == column@
                &&& it.pos() == self.pos()
                &&& it.rest() == window(column@, self.pos() as int, self.end_pos() as int)
                &&& it.end_pos() == self.end_pos() || (self.pos() == column@.len() && it.end_pos() == self.pos())
            },
    {
        self.try_resume_impl::<T, C>(column)
    }
}

// ── Repositioning (`Shiftable`) ───────────────────────────────────────────

/// The iterator repositioned as any of the verified ones is: its model is
/// its column's values, from where it stands to the end of its window,
/// each value an item.
impl<'a, T: DeltaValue<C>, C: Codec> Shiftable for DeltaIter<'a, T, C> {
    closed spec fn shift_vals(&self) -> Seq<T> {
        self.column()
    }

    closed spec fn shift_at(&self) -> int {
        self.spec_pos() as int
    }

    closed spec fn shift_end(&self) -> int {
        self.spec_end_pos() as int
    }

    closed spec fn shift_left(&self) -> Seq<T> {
        self.rest()
    }

    closed spec fn shift_items(&self) -> Seq<T> {
        self.rest()
    }

    open spec fn shift_spans(x: T) -> Seq<T> {
        seq![x]
    }

    open spec fn shift_unit() -> bool {
        true
    }

    proof fn lemma_shift(&self) {
        self.lemma_shift_impl()
    }

    proof fn lemma_shift_unit(x: T) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<T>) {
        self.nth(0)
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<T>) {
        self.shift_nth_impl(n)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        self.dup_impl()
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        self.pos()
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        self.end_pos()
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        DeltaIter::set_max(self, end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        DeltaIter::advance_to(self, target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        DeltaIter::advance_by(self, amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<T>) {
        self.seek_to_impl(at)
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        DeltaIter::shift(self, range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<T>) {
        DeltaIter::shift_next(self, range)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaIter<'a, T, C> {
    /// The iterator's repositioning model ([`Shiftable`]) in the terms
    /// of its own specification: the values it ranges over are its
    /// column's; it stands at `pos()`; what it has left is `rest()`,
    /// each item a value.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals() == self.column(),
            self.shift_at() == self.pos(),
            self.shift_items() == self.rest(),
            self.shift_left() == self.rest(),
            Self::shift_unit(),
    {
        self.lemma_shift_model_impl()
    }

    /// A lookahead: the iterator wrapped so that the value it gives next
    /// can be looked at first ([`Unshift::peek`]).
    #[inline(always)]
    pub fn unshift(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.column(),
            r.unshift_left() == self.rest(),
    {
        self.unshift_impl()
    }

    /// The values of each of `ranges` of the column, one range after
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
            r.ranges_vals() == self.column(),
            r.ranges_left() == windows(self.column(), ranges@),
    {
        self.ranges_impl(ranges)
    }
}

/// The runs of a delta column, repositioned as its values are: the model
/// is each value as a delta run of one, with the number before it, and
/// an item, a run, stands for as many of those as it counts.
impl<'a, T: DeltaValue<C>, C: Codec> Shiftable for DeltaRuns<'a, T, C> {
    closed spec fn shift_vals(&self) -> Seq<DeltaRun> {
        delta_units(self.0.column())
    }

    closed spec fn shift_at(&self) -> int {
        self.0.spec_pos() as int
    }

    closed spec fn shift_end(&self) -> int {
        self.0.spec_end_pos() as int
    }

    closed spec fn shift_left(&self) -> Seq<DeltaRun> {
        window(delta_units(self.0.column()), self.0.spec_pos() as int, self.0.spec_pos() + self.0.rest().len())
    }

    closed spec fn shift_items(&self) -> Seq<DeltaRun> {
        delta_runs_at(
            self.0.column(),
            self.0.spec_pos() as int,
            runs_of(window(deltas(self.0.column()), self.0.spec_pos() as int, self.0.spec_pos() + self.0.rest().len())),
        )
    }

    open spec fn shift_spans(x: DeltaRun) -> Seq<DeltaRun> {
        delta_run_units(x)
    }

    open spec fn shift_unit() -> bool {
        false
    }

    proof fn lemma_shift(&self) {
        self.lemma_shift_impl()
    }

    proof fn lemma_shift_unit(x: DeltaRun) {
    }

    #[inline(always)]
    fn shift_step(&mut self) -> (r: Option<DeltaRun>) {
        self.take_run()
    }

    #[inline(always)]
    fn shift_nth(&mut self, n: usize) -> (r: Option<DeltaRun>) {
        self.shift_nth_impl(n)
    }

    #[inline(always)]
    fn dup(&self) -> (r: Self) {
        DeltaRuns(Shiftable::dup(&self.0))
    }

    #[inline(always)]
    fn get_pos(&self) -> (r: usize) {
        Shiftable::get_pos(&self.0)
    }

    #[inline(always)]
    fn get_max(&self) -> (r: usize) {
        Shiftable::get_max(&self.0)
    }

    #[inline(always)]
    fn set_max(&mut self, end: usize) {
        Shiftable::set_max(&mut self.0, end)
    }

    #[inline(always)]
    fn advance_to(&mut self, target: usize) {
        Shiftable::advance_to(&mut self.0, target)
    }

    #[inline(always)]
    fn advance_by(&mut self, amount: usize) {
        Shiftable::advance_by(&mut self.0, amount)
    }

    #[inline(always)]
    fn seek_to(&mut self, at: usize) -> (r: Option<DeltaRun>) {
        self.seek_to_impl(at)
    }

    #[inline(always)]
    fn shift(&mut self, range: Range<usize>) {
        Shiftable::shift(&mut self.0, range)
    }

    #[inline(always)]
    fn shift_next(&mut self, range: Range<usize>) -> (r: Option<DeltaRun>) {
        self.shift_next_impl(range)
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> DeltaRuns<'a, T, C> {
    /// The runs' repositioning model ([`Shiftable`]) in the terms of
    /// their own specification: they range over the column's values, each
    /// as a delta run of one; they stand where their iterator of values
    /// does; their items are the runs of the differences that has left,
    /// each with the number before it.
    pub proof fn lemma_shift_model(&self)
        ensures
            self.shift_vals() == delta_units(self.iter().column()),
            self.shift_at() == self.iter().pos(),
            self.shift_items() == delta_runs_at(
                self.iter().column(),
                self.iter().pos() as int,
                runs_of(window(deltas(self.iter().column()), self.iter().pos() as int, self.iter().end_pos() as int)),
            ),
            self.shift_left() == window(
                delta_units(self.iter().column()),
                self.iter().pos() as int,
                self.iter().end_pos() as int,
            ),
            !Self::shift_unit(),
    {
        self.lemma_shift_model_impl()
    }

    /// A lookahead: the runs wrapped so that the run they give next can
    /// be looked at first ([`Unshift::peek`]). (It cannot be moved to a window: its
    /// items are not each one value.)
    #[inline(always)]
    pub fn unshift(self) -> (r: Unshift<Self>)
        ensures
            r.unshift_vals() == self.shift_vals(),
            r.unshift_left() == self.shift_items(),
    {
        self.unshift_impl()
    }

    /// The runs of each of `ranges` of the column, one range after
    /// another: a run
    /// that crosses the edge of a range is cut there, and each says the
    /// number before it. The ranges ascend and do not overlap (they may
    /// touch), and start where the iterator stands or later.
    ///
    /// # Panics
    ///
    /// When it comes to a range that starts before where it stands.
    #[inline(always)]
    pub fn ranges<'r>(self, ranges: &'r [Range<usize>]) -> (r: Ranges<'r, Self>)
        ensures
            r.ranges_vals() == self.shift_vals(),
            r.ranges_left() == windows(self.shift_vals(), ranges@),
    {
        self.ranges_impl(ranges)
    }
}

} // verus!
