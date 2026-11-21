//! The encoder of the verified public API ([`VerifiedEncoder`]),
//! verified with Verus.
//!
//! It runs the cores that a column's `remap` runs (`rle::enc`,
//! `bool_enc`) and that [`RleEncoder`](crate::encoder::RleEncoder) and
//! [`BoolEncoder`](crate::encoder::BoolEncoder) run, through the model's
//! `Streams`, and nothing is assumed that a column does not assume.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`encoder/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::{Core, Flows, Streams};
use super::VerifiedColumn;
#[cfg(verus_keep_ghost)]
use super::{gives, held, mapped, only, Held, VGet, VSpec};
use super::{Get, Given, VCodec, VValue, MAX_SEG};
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::iter_core::each;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::remap::Encodes;
use crate::Leb128;

use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

use crate::trusted::unloadable;
mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {

/// An encoder of values of type `V`: it takes values one after another
/// and gives the saved column of them, without a column in between, as
/// [`RleEncoder`](crate::encoder::RleEncoder) and
/// [`BoolEncoder`](crate::encoder::BoolEncoder) do. Its numbers are
/// written with the codec `C`.
///
/// It is specified by the values it has been given (`enc@`, each as a
/// column has it), and what it saves is specified as a column's save
/// is: the bytes are `VerifiedColumn::encoding` of those values, which
/// is what a [`VerifiedColumn`] of them saves, and what a load reads
/// back.
///
/// A value given by reference (a byte string or a string) is kept by
/// reference until the encoder is saved, so the encoder lives no longer
/// than `'a`.
///
/// An encoder made with a segment budget
/// ([`with_max_segments`](Self::with_max_segments),
/// [`with_segments`](Self::with_segments)) cuts what it is
/// given into a column's slabs as it arrives, and
/// [`into_column`](Self::into_column) then builds the column from them
/// without reading the bytes again. What it saves is the same.
///
/// # What a client has to keep
///
/// Unlike a column, an encoder asks its client for a fact about itself:
/// every method requires `wf()`, and every method that changes the
/// encoder ensures it again. [`new`](Self::new) and
/// [`with_max_segments`](Self::with_max_segments) give it. So
/// a client's loop that appends carries `enc.wf()` as an invariant, and
/// a client's function that takes an encoder requires it. No method
/// loses it, and a client that compiles without Verus has nothing to
/// do for it.
///
/// A column keeps what it needs out of sight (a type invariant). An
/// encoder cannot, as Verus stands: an append writes into the encoder's
/// buffer in place and may panic midway (out of memory, or more values
/// than it can count), and Verus lets a type's hidden invariant be
/// suspended only across code that it knows cannot unwind.
pub struct VerifiedEncoder<'a, V: VValue<C>, C: VCodec = Leb128> {
    e: <V::E as Streams<'a, V::X>>::Enc,
}

impl<'a, V: VValue<C>, C: VCodec> View for VerifiedEncoder<'a, V, C> {
    type V = Seq<V::S>;

    closed spec fn view(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(self.e.e_values())
    }
}

impl<'a, V: VValue<C>, C: VCodec> Default for VerifiedEncoder<'a, V, C> {
    /// An encoder that has been given nothing.
    fn default() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() is None,
    {
        Self::new()
    }
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedEncoder<'a, V, C> {
    /// The encoder is one that [`new`](Self::new) or
    /// [`with_max_segments`](Self::with_max_segments) gave and this
    /// type's methods have changed: what each of them requires, and
    /// keeps.
    pub closed spec fn wf(&self) -> bool {
        &&& self.e.e_inv()
        &&& <V::E as Streams<'a, V::X>>::budget(self.e) is None ==> <V::E as Streams<'a, V::X>>::whole(self.e)
    }

    /// The segment budget the encoder was made with
    /// ([`with_max_segments`](Self::with_max_segments)), if with one. No
    /// method changes it.
    pub closed spec fn max_segments(&self) -> Option<usize> {
        <V::E as Streams<'a, V::X>>::budget(self.e)
    }

    /// An encoder that has been given nothing. It has no segment
    /// budget.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() is None,
    {
        Self::new_impl()
    }

    /// An encoder that has been given nothing, with the segment budget
    /// `max_segments`: it cuts what it is given into slabs as it
    /// arrives (at half the budget, as an edit of a column leaves
    /// them), and [`into_column`](Self::into_column) builds a column of
    /// that budget from them, without reading the bytes again.
    ///
    /// `Column`'s encoder gets a budget from the setter
    /// [`EncoderApi::max_segments`](crate::EncoderApi::max_segments),
    /// which has to be called before anything is appended, or from the
    /// builder [`EncoderApi::with_max_segments`](crate::EncoderApi::with_max_segments):
    /// what `RleEncoder::new().with_max_segments(max)` gives there, this
    /// gives here. It requires a budget of at least 2, as a column's
    /// [`with_max_segments`](VerifiedColumn::with_max_segments) does;
    /// the old ones panic on less.
    #[inline(always)]
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() == Some(max_segments),
    {
        Self::with_max_segments_impl(max_segments)
    }

    /// An encoder that has been given nothing, with the segment budget
    /// of a column that was made without one, which is 64:
    /// [`with_max_segments`](Self::with_max_segments) of that. It is
    /// what [`EncoderApi::with_segments`](crate::EncoderApi::with_segments)
    /// gives of `Column`'s encoder.
    #[inline(always)]
    pub fn with_segments() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() == Some(64usize),
    {
        Self::with_segments_impl()
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

    /// Whether no value has been given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    /// Give the encoder `value`.
    ///
    /// Like every method that gives the encoder values, this panics if
    /// there would be more of them than a column can count (some
    /// `usize::MAX`).
    #[inline(always)]
    pub fn append(&mut self, value: Get<'a, V>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(value.sv()),
            final(self).max_segments() == old(self).max_segments(),
    {
        self.append_impl(value)
    }

    /// Give the encoder `value`, `n` times.
    #[inline(always)]
    pub fn append_n(&mut self, value: Get<'a, V>, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value.sv()),
            final(self).max_segments() == old(self).max_segments(),
    {
        self.append_n_impl(value, n)
    }

    /// Give the encoder `value`, which it then owns: for a byte string
    /// or a string, nothing is borrowed and nothing is copied.
    #[inline(always)]
    pub fn append_owned(&mut self, value: V)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(V::sv_of(value)),
            final(self).max_segments() == old(self).max_segments(),
    {
        self.append_owned_impl(value)
    }

    /// Give the encoder `value`, which it then owns, `n` times.
    #[inline(always)]
    pub fn append_n_owned(&mut self, value: V, n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| V::sv_of(value)),
            final(self).max_segments() == old(self).max_segments(),
    {
        self.append_n_owned_impl(value, n)
    }

    /// Give the encoder the values that `iter` gives, in their order.
    ///
    /// The specification speaks of what the iterator gives (`gives`),
    /// as [`VerifiedColumn::splice_iter`]'s does: of an iterator that
    /// keeps to what Verus asks of every iterator. Of another it says
    /// nothing but that the encoder is still one.
    ///
    /// That this returns is not proved: it does when the iterator ends.
    #[inline(always)]
    pub fn extend<I: IntoIterator<Item = Get<'a, V>>>(&mut self, iter: I)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).max_segments() == old(self).max_segments(),
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
    {
        self.extend_impl::<I>(iter)
    }

    /// The values given, saved: the encoding of them, which is what a
    /// [`VerifiedColumn`] of them saves.
    #[inline(always)]
    pub fn save(self) -> (r: Vec<u8>)
        requires
            self.wf(),
        ensures
            VerifiedColumn::<V, C>::valid(r@),
            VerifiedColumn::<V, C>::values(r@) == self@,
            r@ == VerifiedColumn::<V, C>::encoding(self@),
            r@.len() <= V::size_bound(self@),
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
            final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(self@),
            VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(self@)),
            VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(self@)) == self@,
            VerifiedColumn::<V, C>::encoding(self@).len() <= V::size_bound(self@),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_impl(out)
    }

    /// [`save_to`](Self::save_to), unless every value given is `value`:
    /// then nothing is written, and the range is empty. So is it for an
    /// encoder that was given nothing, whatever `value` is.
    ///
    /// For an encoder without a segment budget that is all of it, as it
    /// is of a column's
    /// [`save_to_unless`](VerifiedColumn::save_to_unless). One with a
    /// budget writes nothing only if every value is `value`, and may
    /// write the values though every one is: it does once it has cut a
    /// slab, as [`EncoderApi::save_to_unless`](crate::EncoderApi::save_to_unless)
    /// does. When it cuts is tested against the old encoder and not
    /// proved: with a budget of 2 or 3, equal values other than `bool`
    /// given in more than one call are cut apart, and are then written;
    /// given in one call, or of `bool`, or with a budget of 4 or more,
    /// they were not cut in what was tried.
    #[inline(always)]
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: Get<'a, V>) -> (r: Range<usize>)
        requires
            self.wf(),
        ensures
            final(out)@ == old(out)@ ==> only(self@, value.sv()),
            self.max_segments() is None && only(self@, value.sv()) ==> final(out)@ == old(out)@,
            final(out)@ != old(out)@ ==> {
                &&& final(out)@ == old(out)@ + VerifiedColumn::<V, C>::encoding(self@)
                &&& VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(self@))
                &&& VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(self@)) == self@
                &&& VerifiedColumn::<V, C>::encoding(self@).len() <= V::size_bound(self@)
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_unless_impl(out, value)
    }

    /// The encoding of the values of `iter`: what a column of them
    /// saves, and what an encoder given them saves.
    #[inline(always)]
    pub fn encode<I: IntoIterator<Item = Get<'a, V>>>(iter: I) -> (r: Vec<u8>)
        ensures
            VerifiedColumn::<V, C>::valid(r@),
            r@ == VerifiedColumn::<V, C>::encoding(VerifiedColumn::<V, C>::values(r@)),
            r@.len() <= V::size_bound(VerifiedColumn::<V, C>::values(r@)),
            gives(iter, |xs: Seq<I::Item>| VerifiedColumn::<V, C>::values(r@) == VGet::svs(xs)),
    {
        Self::encode_impl::<I>(iter)
    }

    /// Write the encoding of the values of `iter` after what `buf`
    /// holds. The range returned is where in `buf` it is.
    ///
    /// Nothing is held but the run in hand: the bytes go to `buf` as
    /// the values come.
    #[inline(always)]
    pub fn encode_to<I: IntoIterator<Item = Get<'a, V>>>(buf: &mut Vec<u8>, iter: I) -> (r: Range<usize>)
        ensures
            ({
                let d = final(buf)@.skip(old(buf)@.len() as int);
                let w = VerifiedColumn::<V, C>::values(d);
                &&& final(buf)@ == old(buf)@ + d
                &&& VerifiedColumn::<V, C>::valid(d)
                &&& d == VerifiedColumn::<V, C>::encoding(w)
                &&& d.len() <= V::size_bound(w)
                &&& gives(iter, |xs: Seq<I::Item>| w == VGet::svs(xs))
            }),
            r.start == old(buf)@.len(),
            r.end == final(buf)@.len(),
    {
        Self::encode_to_impl::<I>(buf, iter)
    }

    /// [`encode_to`](Self::encode_to), unless every value of `iter` is
    /// `unless`: then `buf` is left as it was, and the range is empty.
    /// So it is for an `iter` of no values, whatever `unless` is.
    #[inline(always)]
    pub fn encode_to_unless<I: IntoIterator<Item = Get<'a, V>>>(
        buf: &mut Vec<u8>,
        iter: I,
        unless: Get<'a, V>,
    ) -> (r: Range<usize>)
        ensures
            ({
                let d = final(buf)@.skip(old(buf)@.len() as int);
                let w = VerifiedColumn::<V, C>::values(d);
                &&& final(buf)@ == old(buf)@ + d
                &&& VerifiedColumn::<V, C>::valid(d)
                &&& d == VerifiedColumn::<V, C>::encoding(w)
                &&& d.len() <= V::size_bound(w)
                &&& gives(
                    iter,
                    |xs: Seq<I::Item>| {
                        let vs = VGet::svs(xs);
                        &&& (final(buf)@ == old(buf)@) == only(vs, unless.sv())
                        &&& !only(vs, unless.sv()) ==> w == vs
                    },
                )
            }),
            r.start == old(buf)@.len(),
            r.end == final(buf)@.len(),
    {
        Self::encode_to_unless_impl::<I>(buf, iter, unless)
    }

    /// [`save_to`](Self::save_to), of what `f` gives for the values
    /// given: `out` is then what it was, and after that the encoding of
    /// as many values, each what `f` gave for the value given in its
    /// place (`mapped`). The range returned is where in `out` that is.
    ///
    /// `f` is called once for a run of equal values that lie together,
    /// and what it gives stands for all of them: where `f` can give
    /// more than one value for a value, places apart may get different
    /// ones. `f` has to accept every value given, and need accept no
    /// other. If `f` panics, `out` is as it was.
    ///
    /// It is [`RleEncoder::save_to_and_remap`](crate::encoder::RleEncoder::save_to_and_remap),
    /// and here an encoder of `bool` has it too. The values are read
    /// from what the encoder has written: no column is made of them,
    /// as [`into_column`](Self::into_column) and then a column's
    /// [`remap`](VerifiedColumn::remap) would make two.
    #[inline(always)]
    pub fn save_to_and_remap<F: Fn(V) -> V>(self, out: &mut Vec<u8>, f: F) -> (r: Range<usize>)
        requires
            self.wf(),
            forall|v: V| self@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
        ensures
            ({
                // what was written, and the values it is the encoding of
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedColumn::<V, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedColumn::<V, C>::valid(d)
                &&& d == VerifiedColumn::<V, C>::encoding(w)
                &&& d.len() <= V::size_bound(w)
                &&& w.len() == self@.len()
                &&& forall|i: int| 0 <= i < self@.len() ==> mapped(f, self@[i], #[trigger] w[i])
            }),
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_and_remap_impl::<F>(out, f)
    }

    /// [`save_to_and_remap`](Self::save_to_and_remap), unless every
    /// value `f` gives is `unless`: then nothing is written, and the
    /// range is empty. So is it for an encoder that was given nothing,
    /// whatever `unless` is.
    ///
    /// If nothing was written, `f` gave `unless` for every value given.
    /// And if `f` can give nothing but `unless` for the values given,
    /// nothing is written, whether the encoder has a segment budget or
    /// not: what `f` gives goes to an encoder without one. It is
    /// [`RleEncoder::save_to_unless_and_remap`](crate::encoder::RleEncoder::save_to_unless_and_remap).
    #[inline(always)]
    pub fn save_to_unless_and_remap<F: Fn(V) -> V>(self, out: &mut Vec<u8>, unless: Get<'a, V>, f: F) -> (r: Range<
        usize,
    >)
        requires
            self.wf(),
            forall|v: V| self@.contains(V::sv_of(v)) ==> #[trigger] call_requires(f, (v,)),
        ensures
            final(out)@ == old(out)@ ==> forall|i: int|
                0 <= i < self@.len() ==> mapped(f, #[trigger] self@[i], unless.sv()),
            (forall|i: int, b: V::S| 0 <= i < self@.len() && #[trigger] mapped(f, self@[i], b) ==> b == unless.sv())
                ==> final(out)@ == old(out)@,
            final(out)@ != old(out)@ ==> {
                let d = final(out)@.skip(old(out)@.len() as int);
                let w = VerifiedColumn::<V, C>::values(d);
                &&& final(out)@ == old(out)@ + d
                &&& VerifiedColumn::<V, C>::valid(d)
                &&& d == VerifiedColumn::<V, C>::encoding(w)
                &&& d.len() <= V::size_bound(w)
                &&& w.len() == self@.len()
                &&& forall|i: int| 0 <= i < self@.len() ==> mapped(f, self@[i], #[trigger] w[i])
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_unless_and_remap_impl::<F>(out, unless, f)
    }

    /// A column of the values given.
    ///
    /// With a segment budget
    /// ([`with_max_segments`](Self::with_max_segments)) the column is
    /// built from the slabs the encoder cut, and has that budget.
    /// Without one the values are saved and the bytes loaded, as
    /// [`EncoderApi::into_column`](crate::EncoderApi::into_column) does.
    ///
    /// Panics if the values are more than a column counts (`usize::MAX
    /// - 3`), or, without a budget, if they save to 4 GiB or more: a
    /// load reads no more than that. Without a budget it is proved to
    /// panic for no other values (those whose encoding is of at most
    /// `0xffff_fff5` bytes and that are at most `usize::MAX - 3` become
    /// a column).
    #[inline(always)]
    pub fn into_column(self) -> (r: VerifiedColumn<V, C>)
        requires
            self.wf(),
        ensures
            r@ == self@,
    {
        self.into_column_impl()
    }
}

/// An encoder that writes into a buffer of its caller's, which is given
/// to every call that may write: it holds no bytes of its own, only the
/// run in hand. [`finish`](Self::finish) writes what is left.
///
/// The buffer must be the same one each time, with what this state wrote
/// to it left as it is: that is `on`, which every method that takes the
/// buffer requires and nothing checks as it runs.
pub struct VerifiedEncoderState<'a, V: VValue<C>, C: VCodec = Leb128> {
    s: <V::E as Flows<'a, V::X>>::Flow,
}

impl<'a, V: VValue<C>, C: VCodec> View for VerifiedEncoderState<'a, V, C> {
    type V = Seq<V::S>;

    /// The values given so far.
    closed spec fn view(&self) -> Seq<V::S> {
        <Held<V, C> as VGet>::svs(<V::E as Flows<'a, V::X>>::f_values(self.s))
    }
}

impl<'a, V: VValue<C>, C: VCodec> Default for VerifiedEncoderState<'a, V, C> {
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
            forall|buf: Seq<u8>| #[trigger] r.on(buf),
            forall|buf: Seq<u8>| #[trigger] r.before(buf) == buf,
    {
        Self::new()
    }
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedEncoderState<'a, V, C> {
    /// `buf` is what the buffer this state writes into holds now.
    pub closed spec fn on(&self, buf: Seq<u8>) -> bool {
        <V::E as Flows<'a, V::X>>::f_on(self.s, buf)
    }

    /// What the buffer held before this state wrote to it, the buffer
    /// holding `buf` now.
    pub closed spec fn before(&self, buf: Seq<u8>) -> Seq<u8> {
        <V::E as Flows<'a, V::X>>::f_head(self.s, buf)
    }

    /// A state that was given nothing. It may start on any buffer.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<V::S>::empty(),
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
    /// Like every method that gives the state values, this panics if
    /// there would be more of them than a column can count (some
    /// `usize::MAX`).
    #[inline(always)]
    pub fn append(&mut self, buf: &mut Vec<u8>, value: Get<'a, V>)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@.push(value.sv()),
    {
        self.append_impl(buf, value)
    }

    /// Give the state `value`, `n` times.
    #[inline(always)]
    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: Get<'a, V>, n: usize)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| value.sv()),
    {
        self.append_n_impl(buf, value, n)
    }

    /// [`append`](Self::append), of a value that is owned.
    #[inline(always)]
    pub fn append_owned(&mut self, buf: &mut Vec<u8>, value: V)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@.push(V::sv_of(value)),
    {
        self.append_owned_impl(buf, value)
    }

    /// [`append_n`](Self::append_n), of a value that is owned.
    #[inline(always)]
    pub fn append_n_owned(&mut self, buf: &mut Vec<u8>, value: V, n: usize)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| V::sv_of(value)),
    {
        self.append_n_owned_impl(buf, value, n)
    }

    /// The buffer a state is on still begins with what it held before
    /// the state wrote to it.
    pub proof fn lemma_before(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            self.before(buf).len() <= buf.len(),
            buf.take(self.before(buf).len() as int) == self.before(buf),
    {
        self.lemma_before_impl(buf)
    }

    /// Give the state each value of `iter`, in order.
    #[inline(always)]
    pub fn extend<I: IntoIterator<Item = Get<'a, V>>>(&mut self, buf: &mut Vec<u8>, iter: I)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
    {
        self.extend_impl::<I>(buf, iter)
    }

    /// Whether every value given is `value`. It is for a state that was
    /// given nothing, whatever `value` is.
    #[inline(always)]
    pub fn is_single_run_of(&self, value: Get<'a, V>) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == only(self@, value.sv()),
    {
        self.is_single_run_of_impl(value)
    }

    /// Write what is still held. `buf` is then what it held before this
    /// state and after that the encoding of the values given, which is
    /// what a column of them saves.
    #[inline(always)]
    pub fn finish(self, buf: &mut Vec<u8>)
        requires
            self.on(old(buf)@),
        ensures
            final(buf)@ == self.before(old(buf)@) + VerifiedColumn::<V, C>::encoding(self@),
            VerifiedColumn::<V, C>::valid(VerifiedColumn::<V, C>::encoding(self@)),
            VerifiedColumn::<V, C>::values(VerifiedColumn::<V, C>::encoding(self@)) == self@,
            VerifiedColumn::<V, C>::encoding(self@).len() <= V::size_bound(self@),
    {
        self.finish_impl(buf)
    }
}

} // verus!
