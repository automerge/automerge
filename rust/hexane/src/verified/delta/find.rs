//! The finds of a delta column: the places of the values within some
//! bounds, found by the column's index.
//!
//! [`find_by_range`](DeltaColumn::find_by_range) and
//! [`find_by_value`](DeltaColumn::find_by_value) of a
//! [`DeltaColumn`] give a [`FindByRange`]: an iterator
//! over the places of the column whose values are within the bounds,
//! from the first on. [`find_first`](DeltaColumn::find_first)
//! gives the first place of a value. The values need be in no order.
//!
//! The search is [`FindByRange`]'s, the shared one
//! of `delta/dfind.rs` (`d_find_new`, `d_find_next`): the column's index
//! keeps the least and the greatest value of each slab, so a slab that
//! can hold no value within the bounds is passed over whole, and the
//! others are read through.
//!
//! What is proved is that the places a finder gives are those of
//! `delta_finds`, one after another: every place of the column whose
//! value is within the bounds, each once, in order
//! (`lemma_delta_finds`, `lemma_delta_finds_first`).
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`find/imp.rs`), which has
//! its contract word for word and is proved there.

use super::{bounds_of, DKind, DeltaColumn, DeltaValue, DE, DX};
#[cfg(verus_keep_ghost)]
use super::{delta_within, Delta};
use crate::column::Slab;
use crate::cursor::Slabs;
use crate::delta::dfind::DFind;
#[cfg(verus_keep_ghost)]
use crate::delta::dfind::{d_find_new, d_find_next};
use crate::iter_core::Decoders;
use crate::verified::codec::Codec;
use crate::Leb128;
#[cfg(verus_keep_ghost)]
use std::ops::Bound;
use std::ops::RangeBounds;
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::range::RangeBoundsSpec;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// The places of the values `vs`, from `from` on, whose values are
/// within the bounds `lo` and `hi` ([`delta_within`]), in order: what a
/// find over the bounds gives ([`DeltaColumn::find_by_range`]).
/// [`lemma_delta_finds`] says which places they are, and
/// [`lemma_delta_finds_first`] which is the first.
pub open spec fn delta_finds<T: Delta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int) -> Seq<usize>
    decreases vs.len() - from,
{
    if from < 0 || from >= vs.len() {
        Seq::empty()
    } else if delta_within::<T>(lo, hi, vs[from]) {
        seq![from as usize] + delta_finds::<T>(vs, lo, hi, from + 1)
    } else {
        delta_finds::<T>(vs, lo, hi, from + 1)
    }
}

/// The finds of values `vs` from `from` on are places from `from` on
/// whose values are within the bounds; they come in order, so none
/// comes twice; and every such place is one of them.
pub proof fn lemma_delta_finds<T: Delta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int)
    requires
        0 <= from,
        vs.len() <= usize::MAX,
    ensures
        forall|k: int|
            0 <= k < delta_finds::<T>(vs, lo, hi, from).len() ==> {
                let i = #[trigger] delta_finds::<T>(vs, lo, hi, from)[k];
                from <= i < vs.len() && delta_within::<T>(lo, hi, vs[i as int])
            },
        forall|j: int, k: int|
            0 <= j < k < delta_finds::<T>(vs, lo, hi, from).len() ==> #[trigger] delta_finds::<T>(vs, lo, hi, from)[j]
                < #[trigger] delta_finds::<T>(vs, lo, hi, from)[k],
        forall|i: int|
            from <= i < vs.len() && delta_within::<T>(lo, hi, #[trigger] vs[i]) ==> delta_finds::<T>(
                vs,
                lo,
                hi,
                from,
            ).contains(i as usize),
    decreases vs.len() - from,
{
    imp::lemma_delta_finds_impl::<T>(vs, lo, hi, from)
}

/// The first of the finds of values `vs` from `from` on is the first
/// place from `from` on whose value is within the bounds, and the finds
/// after it are those from the next place on; and where there is no
/// find, no value from `from` on is within the bounds.
pub proof fn lemma_delta_finds_first<T: Delta>(vs: Seq<T>, lo: Bound<&T>, hi: Bound<&T>, from: int)
    requires
        0 <= from,
        vs.len() <= usize::MAX,
    ensures
        ({
            let fs = delta_finds::<T>(vs, lo, hi, from);
            if fs.len() > 0 {
                let i = fs[0] as int;
                &&& from <= i < vs.len()
                &&& delta_within::<T>(lo, hi, vs[i])
                &&& forall|j: int| from <= j < i ==> !delta_within::<T>(lo, hi, #[trigger] vs[j])
                &&& fs.drop_first() == delta_finds::<T>(vs, lo, hi, i + 1)
            } else {
                forall|j: int| from <= j < vs.len() ==> !delta_within::<T>(lo, hi, #[trigger] vs[j])
            }
        }),
    decreases vs.len() - from,
{
    imp::lemma_delta_finds_first_impl::<T>(vs, lo, hi, from)
}

/// A value `v` that a delta column can hold is within the bounds that
/// include `t` and nothing else exactly if it is `t`, and no null.
pub proof fn lemma_delta_within_one<T: Delta>(t: T, v: T)
    requires
        v.fits(),
    ensures
        delta_within::<T>(Bound::Included(&t), Bound::Included(&t), v) == (v == t && v.dv() is Some),
{
    imp::lemma_delta_within_one_impl::<T>(t, v)
}

/// The places of a [`DeltaColumn`] whose values are within some
/// bounds, one after another from the first:
/// [`find_by_range`](DeltaColumn::find_by_range) and
/// [`find_by_value`](DeltaColumn::find_by_value) give one.
///
/// It is an iterator, and says what it has left to give (`finds`):
/// `next` gives the first of them and leaves the rest.
pub struct FindByRange<'a, T: DeltaValue<C>, C: Codec = Leb128> {
    slabs: &'a [Slab<<DE<T, C> as Slabs>::Tail>],
    /// The search of the column's index and slabs; none where there is
    /// nothing to find.
    find: Option<DFind<'a, <DX<T, C> as Decoders<'a>>::D>>,
}

impl<'a, T: DeltaValue<C>, C: Codec> FindByRange<'a, T, C> {
    /// The places the finder has left to give, in the order it gives
    /// them.
    pub closed spec fn finds(&self) -> Seq<usize> {
        match self.find {
            Some(fd) => hits_from(self.kept(), fd.lo, fd.hi, fd.from@),
            None => Seq::empty(),
        }
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: DeltaValue<C>, C: Codec> vstd::std_specs::iter::IteratorSpecImpl for FindByRange<'a, T, C> {
    /// The finder keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one place after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<usize> {
        self.finds()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.finds().len())
    }

    open spec fn peek(&self, index: int) -> Option<usize> {
        if 0 <= index < self.finds().len() {
            Some(self.finds()[index])
        } else {
            None
        }
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Iterator for FindByRange<'a, T, C> {
    type Item = usize;

    /// The next place, if one is left: the first of `finds()`, which
    /// leaves the rest of them.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<usize>) {
        self.next_impl()
    }
}

impl<'a, T: DeltaValue<C>, C: Codec> Default for FindByRange<'a, T, C> {
    /// A finder with nothing to give.
    #[inline(always)]
    fn default() -> (r: Self)
        ensures
            r.finds() == Seq::<usize>::empty(),
    {
        Self::default_impl()
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    /// The places of the values within `range`, from the first on.
    ///
    /// The bounds are values of the column's type, and a value is
    /// within them as `delta_within` says: a null is within no
    /// bounds, though a bound may be one. The finder gives the places of
    /// `delta_finds`. The column's index tells it which slabs can
    /// hold such a value, and it reads through those alone.
    #[inline(always)]
    pub fn find_by_range<R: RangeBounds<T>>(&self, range: R) -> (r: FindByRange<'_, T, C>)
        ensures
            r.finds() == delta_finds::<T>(self@, range.spec_start_bound(), range.spec_end_bound(), 0),
    {
        self.find_by_range_impl::<R>(range)
    }

    /// The places of the values that are `target`, from the first on.
    ///
    /// A null is found nowhere, as with
    /// [`DeltaColumn::find_by_value`](crate::DeltaColumn::find_by_value):
    /// the places are those of the values within the bounds that
    /// include `target` and nothing else (`lemma_delta_within_one`).
    #[inline(always)]
    pub fn find_by_value(&self, target: T) -> (r: FindByRange<'_, T, C>)
        ensures
            r.finds() == delta_finds::<T>(self@, Bound::Included(&target), Bound::Included(&target), 0),
    {
        self.find_by_value_impl(target)
    }

    /// The first place of a value that is `target`, if there is one.
    ///
    /// A null is found nowhere: for a `target` that is one the answer
    /// is `None` whatever the column holds.
    #[inline(always)]
    pub fn find_first(&self, target: T) -> (r: Option<usize>)
        ensures
            match r {
                Some(i) => {
                    &&& i < self@.len()
                    &&& self@[i as int] == target
                    &&& target.dv() is Some
                    &&& forall|j: int| 0 <= j < i ==> #[trigger] self@[j] != target
                },
                None => target.dv() is Some ==> forall|j: int| 0 <= j < self@.len() ==> #[trigger] self@[j] != target,
            },
    {
        self.find_first_impl(target)
    }
}

} // verus!
