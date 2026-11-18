//! The decoder of the verified public API ([`VerifiedDecoder`]),
//! verified with Verus.
//!
//! It runs the slab decoders that a column's iterator runs (`rle::dec`,
//! `bool_wire`) and that [`RleDecoder`](crate::rle::RleDecoder) and
//! [`BoolDecoder`](crate::bool::BoolDecoder) run, on a saved column's bytes as
//! they are: no column is built, and nothing is assumed that a column
//! does not assume.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::VerifiedColumn;
#[cfg(verus_keep_ghost)]
use super::{held, lemma_held, lemma_svs_none, lemma_svs_sub, past, VGet, VSpec};
use super::{Get, VCodec, VValue};
use crate::iter_core::{Decode, Decoders};
use crate::Leb128;
use crate::PackError;
use crate::Run;
use std::cmp::Ordering;
#[allow(unused_imports)]
use vstd::prelude::*;

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
    assert(o[0] == v && o[0] == w);
    if n < m {
        assert(o[n] == w);
    } else if m < n {
        assert(o[m] == v);
    }
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
    assert(o[0] == v);
    lemma_head_run_max(o, v, n, o[0], head_len(o, max), max);
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
    /// The decoder stands in a saved column's bytes, at a value or at
    /// the end.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        self.d.inv()
    }

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
        <Get<'a, V> as VGet>::lemma_svs(self.items());
    }

    /// A decoder of the saved column `bytes`, which has all its values
    /// to give.
    ///
    /// The bytes are not looked at first: that they are a saved column
    /// is required. A client that is compiled without Verus has that to
    /// keep by itself, and on other bytes the decoder may panic or give
    /// nonsense. For bytes of which it is not known, there is
    /// [`try_new`](Self::try_new).
    #[inline]
    pub fn new(bytes: &'a [u8]) -> (r: Self)
        requires
            VerifiedColumn::<V, C>::valid(bytes@),
        ensures
            r.rest() == VerifiedColumn::<V, C>::values(bytes@),
    {
        let d = <V::E as Model<V::X>>::decoder(bytes);
        proof {
            let o = d.rest();
            assert(<V::E as Model<V::X>>::values(bytes@) =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
        }
        VerifiedDecoder { d }
    }

    /// A decoder of the saved column `bytes`, which are read through
    /// first, as a load does: an error unless they are a saved column,
    /// and no error for one of less than 4 GiB.
    pub fn try_new(bytes: &'a [u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(d) ==> {
                &&& VerifiedColumn::<V, C>::valid(bytes@)
                &&& d.rest() == VerifiedColumn::<V, C>::values(bytes@)
            },
            VerifiedColumn::<V, C>::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Ok,
    {
        match <V::E as Model<V::X>>::validate(bytes) {
            Ok(_) => Ok(Self::new(bytes)),
            Err(e) => Err(e),
        }
    }

    /// Move `n` values on: past as many as the decoder has left, if it
    /// has fewer.
    pub fn advance_by(&mut self, n: usize)
        ensures
            final(self).items() == past(old(self).items(), n as int),
            final(self).rest() == past(old(self).rest(), n as int),
    {
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.d.rest();
        if n > 0 {
            let _ = self.d.d_nth(n - 1);
        }
        proof {
            let o1 = self.d.rest();
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(o1);
            if n > 0 {
                if n <= o.len() {
                    lemma_svs_sub(o, n as int, o.len() as int);
                } else {
                    assert(o1 =~= Seq::empty());
                    lemma_svs_none::<Get<'a, V>>();
                }
            }
        }
    }

    /// The next run: the value the decoder gives next, and how many
    /// times it comes before another value does or the decoder has no
    /// more. The decoder moves past the whole run.
    ///
    /// The count is a `usize`, and a run of more values than
    /// `usize::MAX` is given `usize::MAX` at a time.
    #[inline]
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
        proof {
            <Get<'a, V> as VGet>::lemma_svs(self.items());
        }
        self.next_run_max(usize::MAX)
    }

    /// The next run, of no more than `max` values: where the run is
    /// longer, `max` of it are given and the decoder moves past those
    /// alone. With a `max` of 0 there is no run to give: `None`, and
    /// the decoder does not move.
    #[inline]
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
        proof {
            use_type_invariant(&*self);
        }
        if max == 0 {
            return None;
        }
        let ghost o = self.d.rest();
        let r = self.d.d_run_max(max);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            if let Some(run) = r {
                let n = run.count as int;
                lemma_svs_sub(o, n, o.len() as int);
                assert forall|i: int| 0 <= i < n implies #[trigger] o[i] == run.value by {
                    assert(o.take(n)[i] == run.value);
                }
                if n < o.len() && o[n].sv() == run.value.sv() {
                    <Get<'a, V> as VGet>::lemma_sv_inj(o[n], run.value);
                }
            }
        }
        r
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
        let ghost o = self.items();
        let ghost s = self.rest();
        let mut skipped: usize = 0;
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            assert(o.skip(0) =~= o);
        }
        while skipped < max
            invariant
                skipped <= max,
                skipped <= o.len(),
                o == old(self).items(),
                s == old(self).rest(),
                s == <Get<'a, V> as VGet>::svs(o),
                s.len() == o.len(),
                self.items() == o.skip(skipped as int),
                forall|i: int| 0 <= i < skipped ==> V::below(#[trigger] s[i], target.sv()),
            decreases max - skipped,
        {
            let ghost k = skipped as int;
            proof {
                lemma_svs_sub(o, k, o.len() as int);
                assert(self.rest() == s.skip(k));
            }
            match self.next_run_max(max - skipped) {
                None => {
                    proof {
                        assert(o.skip(k).len() == 0);
                    }
                    return (skipped, 0);
                },
                Some(run) => {
                    let ghost n = run.count as int;
                    proof {
                        assert(o.skip(k).skip(n) =~= o.skip(k + n));
                        assert(s.skip(k).skip(n) =~= s.skip(k + n));
                        assert(s.skip(k)[0] == run.value.sv());
                        assert(s[k] == run.value.sv());
                    }
                    match V::compare(run.value, target) {
                        Ordering::Equal => {
                            return (skipped, run.count);
                        },
                        Ordering::Greater => {
                            proof {
                                lemma_head_len(s.skip(k), run.value.sv(), n, max - k);
                            }
                            return (skipped, 0);
                        },
                        Ordering::Less => {
                            proof {
                                assert forall|i: int| 0 <= i < k + n implies V::below(
                                    #[trigger] s[i],
                                    target.sv(),
                                ) by {
                                    if i >= k {
                                        assert(s.skip(k)[i - k] == run.value.sv());
                                    }
                                }
                            }
                            skipped = skipped + run.count;
                        },
                    }
                },
            }
        }
        proof {
            assert(s.skip(skipped as int) == self.rest()) by {
                lemma_svs_sub(o, skipped as int, o.len() as int);
            }
        }
        (skipped, 0)
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
    #[inline]
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
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.d.rest();
        let r = self.d.d_next();
        proof {
            let o1 = self.d.rest();
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(o1);
            if r is Some {
                lemma_svs_sub(o, 1, o.len() as int);
                assert(o.skip(1) =~= o.drop_first());
            }
        }
        r
    }

    /// The `n`th of the values left, counted from 0, if there are that
    /// many: it and the values before it are then given. If there are
    /// not, none is left.
    ///
    /// Few of the values passed over are looked at: a whole run is
    /// passed over at once.
    #[inline]
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
        proof {
            use_type_invariant(&*self);
        }
        let ghost o = self.d.rest();
        let r = self.d.d_nth(n);
        proof {
            <Get<'a, V> as VGet>::lemma_svs(o);
            <Get<'a, V> as VGet>::lemma_svs(self.d.rest());
            if n < o.len() {
                lemma_svs_sub(o, n + 1, o.len() as int);
            }
        }
        r
    }
}

impl<'a, V: VValue<C>, C: VCodec> Clone for VerifiedDecoder<'a, V, C> {
    /// A decoder of its own with the same values left.
    fn clone(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
    {
        proof {
            use_type_invariant(self);
        }
        VerifiedDecoder { d: self.d.dup() }
    }
}

} // verus!

impl<V: VValue<C>, C: VCodec> std::fmt::Debug for VerifiedDecoder<'_, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDecoder").finish_non_exhaustive()
    }
}
