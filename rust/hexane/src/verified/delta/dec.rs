//! The decoder of a saved delta column ([`VerifiedDeltaDecoder`]),
//! verified with Verus.
//!
//! It reads the differences a saved delta column holds with the slab
//! decoder that [`DeltaDecoder`](crate::delta::DeltaDecoder) reads them
//! with, from the bytes as they are, and adds them up as it goes: no
//! column is built.
//!
//! What it speaks of is the whole saved column (`column()`) and how
//! far into it the decoder stands (`pos()`), so that a run of
//! differences read ([`next_delta_run_max`]) can be said by the
//! differences of the column (`deltas`), as the runs of a column's
//! iterator are.
//!
//! [`next_delta_run_max`]: VerifiedDeltaDecoder::next_delta_run_max
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`dec/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use super::{all_fit, deltas, vals};
use super::{DIt, DKind, VDeltaValue, DE, DX};
#[cfg(verus_keep_ghost)]
use super::{VDelta, VerifiedDeltaColumn};
#[cfg(verus_keep_ghost)]
use crate::cursor::{ReadsAgree, Slabs};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::DeltaWrite;
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::iter_core::past;
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decode;
use crate::iter_core::Decoders;
use crate::verified::codec::VCodec;
#[cfg(verus_keep_ghost)]
use crate::verified::head_run_max;
#[cfg(verus_keep_ghost)]
use crate::verified::model::Model;
use crate::{Leb128, PackError, Run};
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// A decoder of a saved delta column of values of type `T`, its
/// numbers written with the codec `C`: it gives the column's values
/// one after another from the bytes as they are, without a column in
/// between, as [`DeltaDecoder`](crate::delta::DeltaDecoder) does.
///
/// It is a `std::iter::Iterator`. The values of the whole saved column
/// are a specification `column()`, how many of them the decoder has
/// passed is `pos()`, and what it has left to give, `rest()`, are the
/// column's values from there on; Verus's specifications of the
/// standard library's iterator functions speak of `remaining()`, which
/// is `rest()`. A decoder made of bytes has all the values to give that
/// the bytes are the saved column of
/// (`VerifiedDeltaColumn::values`), which are the values of the column
/// or the encoder that saved them.
///
/// It keeps the sum of the differences it has passed, wrapping as an
/// `i64` does; that no value of a saved column is a wrapped one is
/// part of what `valid` says of the bytes.
pub struct VerifiedDeltaDecoder<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    d: <DX<T, C> as Decoders<'a>>::D,
    /// The sum of the differences passed, wrapping as an `i64` does.
    running: i64,
    /// The differences of the whole saved column.
    ds: Ghost<Seq<Option<i64>>>,
    /// How many of them are passed.
    at: Ghost<int>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaDecoder<'a, T, C> {
    /// The values of the whole saved column that the decoder reads.
    pub closed spec fn column(&self) -> Seq<T> {
        vals::<T>(rz(0, self.ds@))
    }

    /// How many values of the column the decoder has passed.
    pub closed spec fn pos(&self) -> nat {
        self.at@ as nat
    }

    /// The values the decoder has left to give.
    pub open spec fn rest(&self) -> Seq<T> {
        self.column().skip(self.pos() as int)
    }

    /// A decoder stands within its column or at its end, and the
    /// column's values fit: they are values a delta column holds.
    pub proof fn lemma_column(tracked &self)
        ensures
            self.pos() <= self.column().len(),
            all_fit(self.column()),
    {
        self.lemma_column_impl()
    }

    /// A decoder of the saved delta column `bytes`, which has all its
    /// values to give.
    ///
    /// The bytes are not looked at first: that they are a saved delta
    /// column is required. A client that is compiled without Verus has
    /// that to keep by itself, and on other bytes the decoder may panic
    /// or give nonsense. For bytes of which it is not known, there is
    /// [`try_new`](Self::try_new).
    #[inline(always)]
    pub fn new(bytes: &'a [u8]) -> (r: Self)
        requires
            VerifiedDeltaColumn::<T, C>::valid(bytes@),
        ensures
            r.column() == VerifiedDeltaColumn::<T, C>::values(bytes@),
            r.pos() == 0,
            r.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@),
    {
        Self::new_impl(bytes)
    }

    /// A decoder of the saved delta column `bytes`, which are read
    /// through first: an error unless they are a saved column of the
    /// column underneath, and an error unless the values are the
    /// type's, by the check the old API's `try_new` makes.
    ///
    /// The check adds the differences up exactly, run by run: a run is
    /// taken if its first and its last value are the type's and
    /// neither sum leaves `i64`. That is enough for every value to be
    /// the type's, which is what is proved: bytes that are taken are a
    /// saved delta column. It is not said that every saved delta column
    /// is taken, as it is not said of
    /// [`load`](crate::verified::VerifiedDeltaColumn::load).
    #[inline(always)]
    pub fn try_new(bytes: &'a [u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(d) ==> {
                &&& VerifiedDeltaColumn::<T, C>::valid(bytes@)
                &&& d.column() == VerifiedDeltaColumn::<T, C>::values(bytes@)
                &&& d.pos() == 0
                &&& d.rest() == VerifiedDeltaColumn::<T, C>::values(bytes@)
            },
    {
        Self::try_new_impl(bytes)
    }

    /// Move `n` values on: past as many as the decoder has left, if it
    /// has fewer.
    #[inline(always)]
    pub fn advance_by(&mut self, n: usize)
        ensures
            final(self).column() == old(self).column(),
            final(self).pos() == if old(self).pos() + n <= old(self).column().len() {
                old(self).pos() + n
            } else {
                old(self).column().len() as int
            },
            final(self).rest() == past(old(self).rest(), n as int),
    {
        self.advance_by_impl(n)
    }

    /// The next run of differences, of no more than `max` values: the
    /// difference between the value the decoder gives next and the
    /// number before it (or none, for a null), and how many times that
    /// difference comes before another does, `max` are reached or the
    /// decoder has no more. The decoder moves past those values. With a
    /// `max` of 0 there is no run to give: `None`, and the decoder does
    /// not move.
    ///
    /// The differences are those of the column, `deltas(column())`,
    /// from `pos()` on; the number before the run is
    /// `delta_prefix(column(), pos())`, and `lemma_delta_run_at` says
    /// what values such a run stands for.
    #[inline(always)]
    pub fn next_delta_run_max(&mut self, max: usize) -> (r: Option<Run<Option<i64>>>)
        ensures
            final(self).column() == old(self).column(),
            match r {
                Some(run) => {
                    &&& head_run_max(
                        deltas(old(self).column()).skip(old(self).pos() as int),
                        run.value,
                        run.count as int,
                        max as int,
                    )
                    &&& final(self).pos() == old(self).pos() + run.count
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                },
                None => {
                    &&& max == 0 || old(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                },
            },
    {
        self.next_delta_run_max_impl(max)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, T: VDeltaValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedDeltaDecoder<'a, T, C> {
    /// The decoder keeps to what Verus asks of an iterator: `next`
    /// gives `remaining()`, one value after another, and then `None`.
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

impl<'a, T: VDeltaValue<C>, C: VCodec> Iterator for VerifiedDeltaDecoder<'a, T, C> {
    type Item = T;

    /// The next value, if one is left.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<T>)
        ensures
            final(self).column() == old(self).column(),
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                    &&& final(self).pos() == old(self).pos() + 1
                },
                None => {
                    &&& old(self).rest().len() == 0
                    &&& final(self).pos() == old(self).pos()
                },
            },
    {
        self.next_impl()
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// The values before it are passed over a run of differences at a
    /// time.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<T>)
        ensures
            final(self).column() == old(self).column(),
            n < old(self).rest().len() ==> {
                &&& r == Some(old(self).rest()[n as int])
                &&& final(self).pos() == old(self).pos() + n + 1
            },
            n >= old(self).rest().len() ==> {
                &&& r is None
                &&& final(self).pos() == old(self).column().len()
            },
    {
        self.nth_impl(n)
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Clone for VerifiedDeltaDecoder<'a, T, C> {
    /// A decoder of its own of the same column, at the same place.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r.column() == self.column(),
            r.pos() == self.pos(),
            r.rest() == self.rest(),
    {
        self.clone_impl()
    }
}

} // verus!
