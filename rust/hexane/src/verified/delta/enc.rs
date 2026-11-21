//! The delta encoder of the verified API: [`VerifiedDeltaEncoder`] and
//! [`VerifiedDeltaEncoderState`], which write the bytes of a delta
//! column from values given one after another. They mirror
//! [`DeltaEncoder`](crate::DeltaEncoder) and
//! [`DeltaEncoderState`](crate::delta::DeltaEncoderState).
//!
//! A delta column keeps each number as its difference from the number
//! before it, and its saved form is that of the column of the
//! differences. So the encoder here gives the differences to the
//! encoder of that column ([`VerifiedEncoderState`]) and keeps, itself,
//! the last number given. What is proved is said of the bytes: they are
//! the encoding of the values given (`VerifiedDeltaColumn::encoding`),
//! the bytes that a column of those values saves.
//!
//! Differences are taken wrapping and read wrapping, so the bytes are
//! the encoding whatever numbers are given. But a delta column holds no
//! values that span 2^63 or more, and no load takes their bytes: so
//! `finish` panics for such values, as the old encoder does, and what it
//! returns from promises that there are none (`delta_span_ok`).
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`enc/imp.rs`), which has
//! its contract word for word and is proved there.
#[cfg(verus_keep_ghost)]
use super::{all_fit, VerifiedDeltaColumn, DE, DX};
use super::{DKind, VDelta, VDeltaValue};
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::DeltaWrite;
use crate::verified::codec::VCodec;
#[cfg(verus_keep_ghost)]
use crate::verified::model::Model;
use crate::verified::VerifiedEncoderState;
#[cfg(verus_keep_ghost)]
use crate::verified::{only, VGet, VSpec, VerifiedColumn};
use crate::Leb128;
use std::ops::Range;
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// Whether `b` is less than 2^63 above `a`; so it is, too, where one of
/// them is null.
pub open spec fn delta_near<T: VDelta>(a: T, b: T) -> bool {
    match (a.dv(), b.dv()) {
        (Some(x), Some(y)) => y - x <= i64::MAX,
        _ => true,
    }
}

/// Whether no two numbers among `vs` are 2^63 or more apart. A delta
/// column holds no values that are, and an encoder panics for them when
/// it is done ([`finish`](VerifiedDeltaEncoderState::finish)).
pub open spec fn delta_span_ok<T: VDelta>(vs: Seq<T>) -> bool {
    forall|i: int, j: int| 0 <= i < vs.len() && 0 <= j < vs.len() ==> delta_near(#[trigger] vs[i], #[trigger] vs[j])
}

/// Whether the values an iterator has to give are known to fit a delta
/// column: every value of their type does (every type's but `u64`'s,
/// `usize`'s and their `Option`s'), or the iterator keeps the laws Verus
/// has for one and what it has to give fits.
#[verifier::prophetic]
pub open spec fn delta_iter_fits<T: VDelta, I: Iterator<Item = T>>(it: I) -> bool {
    (forall|v: T| #[trigger] v.fits()) || (it.obeys_prophetic_iter_laws() && all_fit(it.remaining()))
}

/// A delta encoder that writes into a buffer of its caller's, which is
/// given to every call that may write: it holds no bytes of its own.
/// [`finish`](Self::finish) writes what is left.
///
/// The buffer must be the same one each time, with what this state wrote
/// to it left as it is: that is `on`, which every method that takes the
/// buffer requires and nothing checks as it runs.
pub struct VerifiedDeltaEncoderState<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    /// The encoder of the differences.
    s: VerifiedEncoderState<'a, <T as DKind<C>>::I, C>,
    /// The last number given, 0 before the first.
    abs: i64,
    /// The least and the greatest number given, kept only for the types
    /// whose numbers can be 2^63 apart.
    low: i64,
    high: i64,
    /// What every value given is kept as, where all are kept as one and
    /// the same.
    uniform: Option<Option<i64>>,
    /// The values given.
    #[allow(dead_code)]
    vals: Ghost<Seq<T>>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> View for VerifiedDeltaEncoderState<'a, T, C> {
    type V = Seq<T>;

    /// The values given so far.
    closed spec fn view(&self) -> Seq<T> {
        self.vals@
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaEncoderState<'a, T, C> {
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
            forall|buf: Seq<u8>| #[trigger] r.on(buf),
            forall|buf: Seq<u8>| #[trigger] r.before(buf) == buf,
    {
        Self::new()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaEncoderState<'a, T, C> {
    /// `buf` is what the buffer this state writes into holds now.
    pub closed spec fn on(&self, buf: Seq<u8>) -> bool {
        self.s.on(buf) && self.ok()
    }

    /// What the buffer held before this state wrote to it, the buffer
    /// holding `buf` now.
    pub closed spec fn before(&self, buf: Seq<u8>) -> Seq<u8> {
        self.s.before(buf)
    }

    /// A state that was given nothing. It may start on any buffer.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
            forall|buf: Seq<u8>| #[trigger] r.on(buf),
            forall|buf: Seq<u8>| #[trigger] r.before(buf) == buf,
    {
        Self::new_impl()
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == self@.len(),
    {
        self.len_impl()
    }

    /// Whether no value was given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    /// Give the state `value`, writing to `buf` what no longer needs to
    /// be held.
    ///
    /// The value must be one a delta column holds (`fits`): the old
    /// encoder panics for one that is not. Like every method that gives
    /// the state values, this panics if there would be more of them than
    /// a column can count (some `usize::MAX`).
    #[inline(always)]
    pub fn append(&mut self, buf: &mut Vec<u8>, value: T)
        requires
            old(self).on(old(buf)@),
            value.fits(),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@.push(value),
    {
        self.append_impl(buf, value)
    }

    /// Give the state `value`, `n` times over.
    #[inline(always)]
    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: T, n: usize)
        requires
            old(self).on(old(buf)@),
            value.fits(),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value),
    {
        self.append_n_impl(buf, value, n)
    }

    /// What the buffer held before this state wrote to it is still at
    /// its start.
    pub proof fn lemma_before(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            self.before(buf).len() <= buf.len(),
            buf.take(self.before(buf).len() as int) == self.before(buf),
    {
        self.lemma_before_impl(buf)
    }

    /// Every value given is one a delta column holds.
    pub proof fn lemma_fits(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            all_fit(self@),
    {
        self.lemma_fits_impl(buf)
    }

    /// Give the state every value of `iter`, in order.
    ///
    /// The values must be known to fit (`delta_iter_fits`): that asks
    /// nothing of an iterator over `u32`s, `i32`s or `i64`s, or over
    /// `Option`s of them.
    #[inline(always)]
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, buf: &mut Vec<u8>, iter: I)
        requires
            old(self).on(old(buf)@),
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + it.remaining()),
    {
        self.extend_impl::<I>(buf, iter)
    }

    /// Whether every value given is `value`: so it is of a state that was
    /// given nothing, whatever `value` is. No value given is one that
    /// does not fit.
    #[inline(always)]
    pub fn is_uniform(&self, value: T) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == only(self@, value),
    {
        self.is_uniform_impl(value)
    }

    /// Write to `buf` what is left: `buf` then holds, after what it held
    /// before the state wrote to it, the encoding of the values given,
    /// which is the bytes a delta column of those values saves.
    ///
    /// This panics if two of the numbers given are 2^63 or more apart,
    /// as the old encoder does: a delta column holds no such values.
    #[inline(always)]
    pub fn finish(self, buf: &mut Vec<u8>)
        requires
            self.on(old(buf)@),
        ensures
            final(buf)@ == self.before(old(buf)@) + VerifiedDeltaColumn::<T, C>::encoding(self@),
            VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@)),
            VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@,
            delta_span_ok(self@),
    {
        self.finish_impl(buf)
    }
}

/// A delta encoder: values go in one after another, and the bytes of a
/// delta column of them come out. It mirrors
/// [`DeltaEncoder`](crate::DeltaEncoder).
///
/// What an encoder has to be to be used is `wf`: every encoder made
/// here is, and stays so through every call that returns.
pub struct VerifiedDeltaEncoder<'a, T: VDeltaValue<C>, C: VCodec = Leb128> {
    data: Vec<u8>,
    state: VerifiedDeltaEncoderState<'a, T, C>,
}

impl<'a, T: VDeltaValue<C>, C: VCodec> View for VerifiedDeltaEncoder<'a, T, C> {
    type V = Seq<T>;

    /// The values given so far.
    closed spec fn view(&self) -> Seq<T> {
        self.state@
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaEncoder<'a, T, C> {
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<T>::empty(),
    {
        Self::new()
    }
}

impl<'a, T: VDeltaValue<C>, C: VCodec> VerifiedDeltaEncoder<'a, T, C> {
    /// The encoder's bytes are those its state has written, and nothing
    /// before them.
    pub closed spec fn wf(&self) -> bool {
        self.state.on(self.data@) && self.state.before(self.data@) == Seq::<u8>::empty()
    }

    /// An encoder that was given nothing.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<T>::empty(),
    {
        Self::new_impl()
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            self.wf(),
        ensures
            r == self@.len(),
    {
        self.len_impl()
    }

    /// Whether no value was given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    /// Every value given is one a delta column holds.
    pub proof fn lemma_fits(&self)
        requires
            self.wf(),
        ensures
            all_fit(self@),
    {
        self.lemma_fits_impl()
    }

    /// Give the encoder `value`, which must be one a delta column holds
    /// (`fits`).
    #[inline(always)]
    pub fn append(&mut self, value: T)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(value),
    {
        self.append_impl(value)
    }

    /// Give the encoder `value`, `n` times over.
    #[inline(always)]
    pub fn append_n(&mut self, value: T, n: usize)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value),
    {
        self.append_n_impl(value, n)
    }

    /// [`append`](Self::append) by another name, as the old encoder has
    /// it.
    #[inline(always)]
    pub fn append_owned(&mut self, value: T)
        requires
            old(self).wf(),
            value.fits(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(value),
    {
        self.append_owned_impl(value)
    }

    /// Give the encoder every value of `iter`, in order. The values must
    /// be known to fit (`delta_iter_fits`).
    #[inline(always)]
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
        requires
            old(self).wf(),
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            final(self).wf(),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + it.remaining()),
    {
        self.extend_impl::<I>(iter)
    }

    /// The bytes of a delta column of the values given: its encoding,
    /// which a column of those values saves and which loads as them.
    ///
    /// Like every method that writes the values out, this panics if two
    /// of the numbers given are 2^63 or more apart, as the old encoder
    /// does: a delta column holds no such values.
    #[inline(always)]
    pub fn save(self) -> (r: Vec<u8>)
        requires
            self.wf(),
        ensures
            VerifiedDeltaColumn::<T, C>::valid(r@),
            VerifiedDeltaColumn::<T, C>::values(r@) == self@,
            r@ == VerifiedDeltaColumn::<T, C>::encoding(self@),
            delta_span_ok(self@),
    {
        self.save_impl()
    }

    /// Save the values given after what `out` holds: `out` is then what
    /// it was, and after that the encoding of the values. The range
    /// returned is where in `out` that is.
    #[inline(always)]
    pub fn save_to(self, out: &mut Vec<u8>) -> (r: Range<usize>)
        requires
            self.wf(),
        ensures
            final(out)@ == old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(self@),
            VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@)),
            VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@,
            delta_span_ok(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_impl(out)
    }

    /// [`save_to`](Self::save_to), unless every value given is `value`:
    /// then nothing is written, and the range is empty. So is it for an
    /// encoder that was given nothing, whatever `value` is.
    #[inline(always)]
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T) -> (r: Range<usize>)
        requires
            self.wf(),
        ensures
            final(out)@ == if only(self@, value) {
                old(out)@
            } else {
                old(out)@ + VerifiedDeltaColumn::<T, C>::encoding(self@)
            },
            !only(self@, value) ==> {
                &&& VerifiedDeltaColumn::<T, C>::valid(VerifiedDeltaColumn::<T, C>::encoding(self@))
                &&& VerifiedDeltaColumn::<T, C>::values(VerifiedDeltaColumn::<T, C>::encoding(self@)) == self@
                &&& delta_span_ok(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_unless_impl(out, value)
    }

    /// The bytes of a delta column of the values of `iter`, which must
    /// be known to fit (`delta_iter_fits`).
    #[inline(always)]
    pub fn encode<I: IntoIterator<Item = T>>(iter: I) -> (r: Vec<u8>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            VerifiedDeltaColumn::<T, C>::valid(r@),
            r@ == VerifiedDeltaColumn::<T, C>::encoding(VerifiedDeltaColumn::<T, C>::values(r@)),
            delta_span_ok(VerifiedDeltaColumn::<T, C>::values(r@)),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> VerifiedDeltaColumn::<
                    T,
                    C,
                >::values(r@) == it.remaining()),
    {
        Self::encode_impl::<I>(iter)
    }

    /// Write the bytes of a delta column of the values of `iter` after
    /// what `out` holds, with no buffer between. The range returned is
    /// where in `out` they are.
    #[inline(always)]
    pub fn encode_to<I: IntoIterator<Item = T>>(out: &mut Vec<u8>, iter: I) -> (r: Range<usize>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            ({
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedDeltaColumn::<T, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedDeltaColumn::<T, C>::valid(d)
                &&& d == VerifiedDeltaColumn::<T, C>::encoding(w)
                &&& delta_span_ok(w)
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> w == it.remaining())
            }),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        Self::encode_to_impl::<I>(out, iter)
    }

    /// [`encode_to`](Self::encode_to), unless every value of `iter` is
    /// `value`: then `out` is left as it was, and the range is empty. So
    /// is it for an `iter` with nothing to give.
    #[inline(always)]
    pub fn encode_to_unless<I: IntoIterator<Item = T>>(out: &mut Vec<u8>, iter: I, value: T) -> (r: Range<usize>)
        requires
            forall|it: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), it) ==> delta_iter_fits(it),
        ensures
            ({
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedDeltaColumn::<T, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedDeltaColumn::<T, C>::valid(d)
                &&& d == VerifiedDeltaColumn::<T, C>::encoding(w)
                &&& delta_span_ok(w)
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> {
                        &&& (final(out)@ == old(out)@) == only(it.remaining(), value)
                        &&& !only(it.remaining(), value) ==> w == it.remaining()
                    })
            }),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        Self::encode_to_unless_impl::<I>(out, iter, value)
    }
}

} // verus!
