//! The decoder of the verified public API ([`VerifiedDecoder`]),
//! verified with Verus.
//!
//! It runs the slab decoders that a column's iterator runs (`rle::dec`,
//! `bool_wire`) and that [`RleDecoder`](crate::rle::RleDecoder) and
//! [`BoolDecoder`](crate::bool::BoolDecoder) run, on a saved column's bytes as
//! they are: no column is built, and nothing is assumed that a column
//! does not assume.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`decoder/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::VerifiedColumn;
#[cfg(verus_keep_ghost)]
use super::{past, VGet, VSpec};
use super::{Get, VCodec, VValue};
#[cfg(verus_keep_ghost)]
use crate::iter_core::Decode;
use crate::iter_core::Decoders;
use crate::Leb128;
use crate::PackError;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// The values `o` begin with a run of `n` values `v`, counted up to
/// `max`: the run ends there (`o` has no more, or its next value is
/// another), or it is `max` values and may go on.
pub open spec fn head_run_max<S>(o: Seq<S>, v: S, n: int, max: int) -> bool {
    &&& 1 <= n <= o.len()
    &&& n <= max
    &&& forall|i: int| 0 <= i < n ==> #[trigger] o[i] == v
    &&& n < max && n < o.len() ==> o[n] != v
}

/// Values begin with one run counted up to `max`, and no other.
pub proof fn lemma_head_run_max<S>(o: Seq<S>, v: S, n: int, w: S, m: int, max: int)
    requires
        head_run_max(o, v, n, max),
        head_run_max(o, w, m, max),
    ensures
        v == w,
        n == m,
{
    imp::lemma_head_run_max_impl::<S>(o, v, n, w, m, max)
}

/// How many values the run is of that `o` begins with, counted up to
/// `max`: the `n` of `head_run_max`, where there is one (`o` has a
/// value, and `max` is 1 or more).
pub open spec fn head_len<S>(o: Seq<S>, max: int) -> int {
    choose|n: int| head_run_max(o, o[0], n, max)
}

/// The run that values begin with is as long as `head_len` says.
pub proof fn lemma_head_len<S>(o: Seq<S>, v: S, n: int, max: int)
    requires
        head_run_max(o, v, n, max),
    ensures
        v == o[0],
        head_len(o, max) == n,
{
    imp::lemma_head_len_impl::<S>(o, v, n, max)
}

/// What [`VerifiedDecoder::scan_for`] returns for the values `o`, the
/// target `t` and the bound `max`: that it passed `skipped` values and
/// then found `count` of the target.
///
/// The values passed are all before the target in the order of the
/// values (`VSpec::below`), and no more than `max`. Where `count` is
/// not 0, the values go on with a run of that many of the target,
/// counted up to what is left of `max` ([`head_run_max`]). Where it is
/// 0, they go on with a value that comes after the target, or with
/// none, or `max` values were passed.
///
/// Nothing is asked of the order the values are in. Where they are in
/// order, `skipped` is so the place where the target stands, or would
/// stand.
pub open spec fn scan_of<V: VSpec>(o: Seq<V::S>, t: V::S, max: int, skipped: int, count: int) -> bool {
    &&& 0 <= skipped <= max
    &&& skipped <= o.len()
    &&& forall|i: int| 0 <= i < skipped ==> V::below(#[trigger] o[i], t)
    &&& if count > 0 {
        head_run_max(o.skip(skipped), t, count, max - skipped)
    } else {
        skipped < max && skipped < o.len() ==> V::below(t, o[skipped])
    }
}

/// How many of the values `o` a [`VerifiedDecoder::scan_for`] has moved
/// past that returned `skipped` and `count` for the bound `max`: the
/// values it passed and the run it found, and, where it found a value
/// that comes after the target, that value's run too (counted up to
/// what was left of `max`), which is so not there to be read again.
pub open spec fn scan_taken<S>(o: Seq<S>, max: int, skipped: int, count: int) -> int {
    if count > 0 {
        skipped + count
    } else if skipped < max && skipped < o.len() {
        skipped + head_len(o.skip(skipped), max - skipped)
    } else {
        skipped
    }
}

/// A decoder of a saved column of values of type `V`, its numbers
/// written with the codec `C`: it gives the column's values one after
/// another from the bytes as they are, without a column in between, as
/// [`RleDecoder`](crate::rle::RleDecoder) and
/// [`BoolDecoder`](crate::bool::BoolDecoder) do.
///
/// It is a `std::iter::Iterator`. What it has left to give is in a
/// specification `items()`, and as values `rest()`; Verus's
/// specifications of the standard library's iterator functions speak of
/// `remaining()`, which is `items()`. A decoder made of bytes has all
/// the values to give that the bytes are the saved column of
/// (`VerifiedColumn::values`), which are the values of the column or
/// the encoder that saved them.
///
/// [`new`](Self::new) stands for the old API's
/// [`decoder`](crate::decoder) and [`decoder_in`](crate::decoder_in),
/// and [`try_new`](Self::try_new) for [`try_decoder`](crate::try_decoder)
/// and [`try_decoder_in`](crate::try_decoder_in).
///
/// A value that borrows (a byte string or a string) borrows from the
/// bytes, for `'a`.
pub struct VerifiedDecoder<'a, V: VValue<C>, C: VCodec = Leb128> {
    d: <V::X as Decoders<'a>>::D,
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedDecoder<'a, V, C> {
    /// What the decoder has left to give.
    pub closed spec fn items(&self) -> Seq<Get<'a, V>> {
        self.d.rest()
    }

    /// The values the decoder has left to give.
    pub open spec fn rest(&self) -> Seq<V::S> {
        <Get<'a, V> as VGet>::svs(self.items())
    }

    /// The decoder has as many values left in either form. (They may be
    /// more than a column holds, which is `usize::MAX - 3` values.)
    pub proof fn lemma_items_len(&self)
        ensures
            self.rest().len() == self.items().len(),
    {
        self.lemma_items_len_impl()
    }

    /// A decoder of the saved column `bytes`, which has all its values
    /// to give.
    ///
    /// The bytes are not looked at first: that they are a saved column
    /// is required. A client that is compiled without Verus has that to
    /// keep by itself, and on other bytes the decoder may panic or give
    /// nonsense. For bytes of which it is not known, there is
    /// [`try_new`](Self::try_new).
    #[inline(always)]
    pub fn new(bytes: &'a [u8]) -> (r: Self)
        requires
            VerifiedColumn::<V, C>::valid(bytes@),
        ensures
            r.rest() == VerifiedColumn::<V, C>::values(bytes@),
    {
        Self::new_impl(bytes)
    }

    /// A decoder of the saved column `bytes`, which are read through
    /// first, as a load does: an error unless they are a saved column,
    /// and no error for one of less than 4 GiB.
    #[inline(always)]
    pub fn try_new(bytes: &'a [u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(d) ==> {
                &&& VerifiedColumn::<V, C>::valid(bytes@)
                &&& d.rest() == VerifiedColumn::<V, C>::values(bytes@)
            },
            VerifiedColumn::<V, C>::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Ok,
    {
        Self::try_new_impl(bytes)
    }

    /// Move `n` values on: past as many as the decoder has left, if it
    /// has fewer.
    #[inline(always)]
    pub fn advance_by(&mut self, n: usize)
        ensures
            final(self).items() == past(old(self).items(), n as int),
            final(self).rest() == past(old(self).rest(), n as int),
    {
        self.advance_by_impl(n)
    }

    /// The next run: the value the decoder gives next, and how many
    /// times it comes before another value does or the decoder has no
    /// more. The decoder moves past the whole run.
    ///
    /// The count is a `usize`, and a run of more values than
    /// `usize::MAX` is given `usize::MAX` at a time.
    #[inline(always)]
    pub fn next_run(&mut self) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            match r {
                Some(run) => {
                    &&& head_run_max(old(self).items(), run.value, run.count as int, usize::MAX as int)
                    &&& head_run_max(old(self).rest(), run.value.sv(), run.count as int, usize::MAX as int)
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                },
                None => {
                    &&& old(self).items().len() == 0
                    &&& old(self).rest().len() == 0
                    &&& final(self).items() == old(self).items()
                },
            },
    {
        self.next_run_impl()
    }

    /// The next run, of no more than `max` values: where the run is
    /// longer, `max` of it are given and the decoder moves past those
    /// alone. With a `max` of 0 there is no run to give: `None`, and
    /// the decoder does not move.
    #[inline(always)]
    pub fn next_run_max(&mut self, max: usize) -> (r: Option<Run<Get<'a, V>>>)
        ensures
            match r {
                Some(run) => {
                    &&& head_run_max(old(self).items(), run.value, run.count as int, max as int)
                    &&& head_run_max(old(self).rest(), run.value.sv(), run.count as int, max as int)
                    &&& final(self).items() == old(self).items().skip(run.count as int)
                    &&& final(self).rest() == old(self).rest().skip(run.count as int)
                },
                None => {
                    &&& max == 0 || (old(self).items().len() == 0 && old(self).rest().len() == 0)
                    &&& final(self).items() == old(self).items()
                },
            },
    {
        self.next_run_max_impl(max)
    }

    /// Look for `target` among values in order: pass the runs of values
    /// that come before it, `max` values at most, and give how many
    /// values were passed and how many of the target follow them (0 if
    /// none does). What that is, is `scan_of`'s to say.
    ///
    /// The decoder moves past what it passed and the run it found. If
    /// it found a value that comes after the target, it has moved past
    /// that value's run too (`scan_taken`), as the old API's `scan_for`
    /// has.
    #[inline(always)]
    pub fn scan_for<'b>(&mut self, target: Get<'b, V>, max: usize) -> (r: (usize, usize))
        ensures
            scan_of::<V>(old(self).rest(), target.sv(), max as int, r.0 as int, r.1 as int),
            final(self).items() == old(self).items().skip(
                scan_taken(old(self).rest(), max as int, r.0 as int, r.1 as int),
            ),
            final(self).rest() == old(self).rest().skip(
                scan_taken(old(self).rest(), max as int, r.0 as int, r.1 as int),
            ),
    {
        self.scan_for_impl(target, max)
    }
}

#[cfg(verus_keep_ghost)]
impl<'a, V: VValue<C>, C: VCodec> vstd::std_specs::iter::IteratorSpecImpl for VerifiedDecoder<'a, V, C> {
    /// The decoder keeps to what Verus asks of an iterator: `next` gives
    /// `remaining()`, one value after another, and then `None`.
    open spec fn obeys_prophetic_iter_laws(&self) -> bool {
        true
    }

    open spec fn remaining(&self) -> Seq<Get<'a, V>> {
        self.items()
    }

    open spec fn will_return_none(&self) -> bool {
        true
    }

    open spec fn decrease(&self) -> Option<nat> {
        Some(self.items().len())
    }

    open spec fn peek(&self, index: int) -> Option<Get<'a, V>> {
        if 0 <= index < self.items().len() {
            Some(self.items()[index])
        } else {
            None
        }
    }
}

impl<'a, V: VValue<C>, C: VCodec> Iterator for VerifiedDecoder<'a, V, C> {
    type Item = Get<'a, V>;

    /// The next value, if one is left.
    #[inline(always)]
    fn next(&mut self) -> (r: Option<Get<'a, V>>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v.sv() == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                },
                None => old(self).rest().len() == 0,
            },
    {
        self.next_impl()
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a whole run is
    /// passed over at once.
    #[inline(always)]
    fn nth(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
        ensures
            n < old(self).items().len() ==> {
                &&& r == Some(old(self).items()[n as int])
                &&& final(self).items() == old(self).items().skip(n + 1)
                &&& r matches Some(v) && v.sv() == old(self).rest()[n as int]
                &&& final(self).rest() == old(self).rest().skip(n + 1)
            },
            n >= old(self).items().len() ==> {
                &&& r is None
                &&& final(self).items().len() == 0
                &&& final(self).rest().len() == 0
            },
    {
        self.nth_impl(n)
    }
}

impl<'a, V: VValue<C>, C: VCodec> Clone for VerifiedDecoder<'a, V, C> {
    /// A decoder of its own with the same values left.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
    {
        self.clone_impl()
    }
}

} // verus!
