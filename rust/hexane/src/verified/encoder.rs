//! The encoder of the verified public API ([`VerifiedEncoder`]),
//! verified with Verus.
//!
//! It runs the cores that a column's `remap` runs (`rle::enc`,
//! `bool_enc`) and that [`RleEncoder`](crate::encoder::RleEncoder) and
//! [`BoolEncoder`](crate::encoder::BoolEncoder) run, through the model's
//! `Streams`, and nothing is assumed that a column does not assume.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::{Core, Flows, Streams};
use super::VerifiedColumn;
#[cfg(verus_keep_ghost)]
use super::{
    held, lemma_held, lemma_svs_add, lemma_svs_inj, lemma_svs_none, lemma_svs_one, mapped, only,
    Held, VGet, VSpec,
};
use super::{Get, Given, VCodec, VValue, MAX_SEG};
use crate::column::Slab;
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::cursor::{all_ok, lemma_lens_ok, lemma_vals_own};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{each, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
use crate::remap::Encodes;
use crate::Leb128;
use crate::PackError;
use std::ops::Range;
#[allow(unused_imports)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

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
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() is None,
    {
        let e = <V::E as Streams<'a, V::X>>::fresh();
        proof {
            assert(e.e_values() =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        VerifiedEncoder { e }
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
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() == Some(max_segments),
    {
        let e = <V::E as Streams<'a, V::X>>::budgeted(max_segments);
        proof {
            assert(e.e_values() =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        VerifiedEncoder { e }
    }

    /// An encoder that has been given nothing, with the segment budget
    /// of a column that was made without one, which is 64:
    /// [`with_max_segments`](Self::with_max_segments) of that. It is
    /// what [`EncoderApi::with_segments`](crate::EncoderApi::with_segments)
    /// gives of `Column`'s encoder.
    pub fn with_segments() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() == Some(64usize),
    {
        Self::with_max_segments(MAX_SEG)
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            self.wf(),
        ensures
            r == self@.len(),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.e.e_values());
        }
        <V::E as Streams<'a, V::X>>::count(&self.e)
    }

    /// Whether no value has been given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == 0),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(self.e.e_values());
        }
        <V::E as Streams<'a, V::X>>::count(&self.e) == 0
    }

    /// Give the encoder `n` copies of the value `x` stands for, which
    /// is `s` in a specification.
    #[inline(always)]
    fn put(&mut self, x: Given<'a, V, C>, n: usize, Ghost(s): Ghost<V::S>)
        requires
            old(self).wf(),
            <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x).sv() == s,
        ensures
            final(self).wf(),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| s),
            final(self).max_segments() == old(self).max_segments(),
    {
        <V::E as Streams<'a, V::X>>::give(&mut self.e, x, n);
        proof {
            let o = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x);
            let added = Seq::new(n as nat, |_i: int| o);
            lemma_svs_add(old(self).e.e_values(), added);
            <Held<V, C> as VGet>::lemma_svs(added);
            assert(<Held<V, C> as VGet>::svs(added) =~= Seq::new(n as nat, |_i: int| s));
        }
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        self.put(x, 1, Ghost(value.sv()));
        proof {
            assert(self@ =~= old(self)@.push(value.sv()));
        }
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        self.put(x, n, Ghost(value.sv()));
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
        let ghost s = V::sv_of(value);
        let x = V::owned_for(value);
        self.put(x, 1, Ghost(s));
        proof {
            assert(self@ =~= old(self)@.push(s));
        }
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
        let ghost s = V::sv_of(value);
        let x = V::owned_for(value);
        self.put(x, n, Ghost(s));
    }

    /// Give the encoder the values that `iter` gives, in their order.
    ///
    /// The specification speaks of the iterator `iter` becomes (`it`),
    /// and of what Verus says that one will give (`it.remaining()`), as
    /// [`VerifiedColumn::splice_iter`]'s does. It holds for an iterator
    /// that keeps to what Verus asks of every iterator
    /// (`obeys_prophetic_iter_laws`); of another it says nothing but
    /// that the encoder is still one.
    ///
    /// That this returns is not proved: it does when the iterator ends.
    #[verifier::exec_allows_no_decreases_clause]
    pub fn extend<I: IntoIterator<Item = Get<'a, V>>>(&mut self, iter: I)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).max_segments() == old(self).max_segments(),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + VGet::svs(it.remaining())),
    {
        let mut it = iter.into_iter();
        let ghost it0 = it;
        let ghost mut given: Seq<Get<'a, V>> = Seq::empty();
        proof {
            lemma_svs_none::<Get<'a, V>>();
            assert(given + it.remaining() =~= it0.remaining());
            assert(self@ =~= old(self)@ + <Get<'a, V> as VGet>::svs(given));
        }
        // a Verus loop with an invariant: `loop` + `match`
        #[allow(clippy::while_let_loop)]
        loop
            invariant_except_break
                it.obeys_prophetic_iter_laws() ==> given + it.remaining() == it0.remaining(),
            invariant
                self.wf(),
                self.max_segments() == old(self).max_segments(),
                it.obeys_prophetic_iter_laws() == it0.obeys_prophetic_iter_laws(),
                self@ == old(self)@ + <Get<'a, V> as VGet>::svs(given),
            ensures
                it0.obeys_prophetic_iter_laws() ==> given == it0.remaining(),
        {
            let ghost before = given;
            let ghost rem = it.remaining();
            match it.next() {
                Some(v) => {
                    let ghost s0 = self@;
                    self.append(v);
                    proof {
                        given = before.push(v);
                        lemma_svs_add(before, seq![v]);
                        lemma_svs_one(v);
                        assert(before.push(v) =~= before + seq![v]);
                        assert(s0.push(v.sv()) =~= s0 + seq![v.sv()]);
                        assert(self@ =~= old(self)@ + <Get<'a, V> as VGet>::svs(given));
                        if it.obeys_prophetic_iter_laws() {
                            assert(given + it.remaining() =~= before + rem);
                        }
                    }
                },
                None => {
                    proof {
                        if it.obeys_prophetic_iter_laws() {
                            assert(given =~= before + rem);
                        }
                    }
                    break ;
                },
            }
        }
    }

    /// The values given, saved: the encoding of them, which is what a
    /// [`VerifiedColumn`] of them saves.
    pub fn save(self) -> (r: Vec<u8>)
        requires
            self.wf(),
        ensures
            VerifiedColumn::<V, C>::valid(r@),
            VerifiedColumn::<V, C>::values(r@) == self@,
            r@ == VerifiedColumn::<V, C>::encoding(self@),
            r@.len() <= V::size_bound(self@),
    {
        let r = <V::E as Streams<'a, V::X>>::bytes(self.e);
        proof {
            VerifiedColumn::<V, C>::lemma_encoding(r@);
            V::lemma_size(r@);
        }
        r
    }

    /// Save the values given after what `out` holds: `out` is then what
    /// it was, and after that the encoding of the values. The range
    /// returned is where in `out` that is.
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
        let start = out.len();
        <V::E as Streams<'a, V::X>>::write(self.e, out);
        proof {
            let d = out@.skip(start as int);
            VerifiedColumn::<V, C>::lemma_encoding(d);
            V::lemma_size(d);
            assert(out@ =~= old(out)@ + d);
        }
        start..out.len()
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        let ghost o = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x);
        let ghost vs = self.e.e_values();
        let ghost all = Seq::new(vs.len(), |_i: int| o);
        proof {
            <Held<V, C> as VGet>::lemma_svs(vs);
            <Held<V, C> as VGet>::lemma_svs(all);
        }
        if <V::E as Streams<'a, V::X>>::all_are(&self.e, &x) {
            let n = out.len();
            return n..n;
        }
        proof {
            if self.max_segments() is None && only(self@, value.sv()) {
                assert(<Held<V, C> as VGet>::svs(vs) =~= <Held<V, C> as VGet>::svs(all));
                lemma_svs_inj::<Held<V, C>>(vs, all);
            }
        }
        let r = self.save_to(out);
        proof {
            let d = VerifiedColumn::<V, C>::encoding(self@);
            if out@ == old(out)@ {
                assert(d =~= Seq::empty());
                VerifiedColumn::<V, C>::lemma_no_bytes();
            }
        }
        r
    }

    /// The encoding of the values of `iter`: what a column of them
    /// saves, and what an encoder given them saves.
    pub fn encode<I: IntoIterator<Item = Get<'a, V>>>(iter: I) -> (r: Vec<u8>)
        ensures
            VerifiedColumn::<V, C>::valid(r@),
            r@ == VerifiedColumn::<V, C>::encoding(VerifiedColumn::<V, C>::values(r@)),
            r@.len() <= V::size_bound(VerifiedColumn::<V, C>::values(r@)),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws()
                    ==> VerifiedColumn::<V, C>::values(r@) == VGet::svs(it.remaining())),
    {
        let mut buf: Vec<u8> = Vec::new();
        Self::encode_to(&mut buf, iter);
        proof {
            assert(buf@.skip(0) =~= buf@);
        }
        buf
    }

    /// Write the encoding of the values of `iter` after what `buf`
    /// holds. The range returned is where in `buf` it is.
    ///
    /// Nothing is held but the run in hand: the bytes go to `buf` as
    /// the values come.
    pub fn encode_to<I: IntoIterator<Item = Get<'a, V>>>(buf: &mut Vec<u8>, iter: I) -> (r: Range<usize>)
        ensures
            ({
                let d = final(buf)@.skip(old(buf)@.len() as int);
                let w = VerifiedColumn::<V, C>::values(d);
                &&& final(buf)@ == old(buf)@ + d
                &&& VerifiedColumn::<V, C>::valid(d)
                &&& d == VerifiedColumn::<V, C>::encoding(w)
                &&& d.len() <= V::size_bound(w)
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> w == VGet::svs(
                        it.remaining(),
                    ))
            }),
            r.start == old(buf)@.len(),
            r.end == final(buf)@.len(),
    {
        let start = buf.len();
        let mut s = VerifiedEncoderState::<'a, V, C>::new();
        s.extend(buf, iter);
        let ghost vs = s@;
        s.finish(buf);
        proof {
            let d = buf@.skip(start as int);
            assert(d =~= VerifiedColumn::<V, C>::encoding(vs));
            assert(Seq::<V::S>::empty() + vs =~= vs);
        }
        start..buf.len()
    }

    /// [`encode_to`](Self::encode_to), unless every value of `iter` is
    /// `unless`: then `buf` is left as it was, and the range is empty.
    /// So it is for an `iter` of no values, whatever `unless` is.
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
                &&& exists|it: I::IntoIter| #[trigger]
                    (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> {
                        let vs = VGet::svs(it.remaining());
                        &&& (final(buf)@ == old(buf)@) == only(vs, unless.sv())
                        &&& !only(vs, unless.sv()) ==> w == vs
                    })
            }),
            r.start == old(buf)@.len(),
            r.end == final(buf)@.len(),
    {
        let start = buf.len();
        let mut s = VerifiedEncoderState::<'a, V, C>::new();
        s.extend(buf, iter);
        let ghost vs = s@;
        proof {
            assert(Seq::<V::S>::empty() + vs =~= vs);
        }
        if s.is_single_run_of(unless) {
            proof {
                s.lemma_before(buf@);
            }
            buf.truncate(start);
            proof {
                assert(buf@ =~= old(buf)@);
                let d = buf@.skip(start as int);
                assert(d =~= Seq::<u8>::empty());
                VerifiedColumn::<V, C>::lemma_no_bytes();
                VerifiedColumn::<V, C>::lemma_encoding(d);
                V::lemma_size(d);
                assert(buf@ =~= old(buf)@ + d);
            }
            return start..start;
        }
        s.finish(buf);
        proof {
            let d = buf@.skip(start as int);
            assert(d =~= VerifiedColumn::<V, C>::encoding(vs));
            if buf@ == old(buf)@ {
                assert(d =~= Seq::<u8>::empty());
                VerifiedColumn::<V, C>::lemma_no_bytes();
            }
        }
        start..buf.len()
    }

    /// An encoder without a segment budget, given what `f` gives for
    /// the values of the slabs `s`: read slab by slab, `f` called once
    /// for each run of equal values of a slab, and what it gives given
    /// for the run.
    fn walk<'s, F: Fn(V) -> V>(s: &'s [Slab<<V::E as Slabs>::Tail>], f: &F) -> (to: <V::E as Streams<'a, V::X>>::Enc)
        requires
            all_ok::<V::E>(s@),
            forall|v: V|
                <Held<V, C> as VGet>::svs(crate::cursor::vals_of::<V::E>(s@)).contains(V::sv_of(v))
                    ==> #[trigger] call_requires(*f, (v,)),
        ensures
            to.e_inv(),
            <V::E as Streams<'a, V::X>>::budget(to) is None,
            <V::E as Streams<'a, V::X>>::whole(to),
            ({
                let src = <Held<V, C> as VGet>::svs(crate::cursor::vals_of::<V::E>(s@));
                let dst = <Held<V, C> as VGet>::svs(to.e_values());
                &&& dst.len() == src.len()
                &&& forall|i: int| 0 <= i < src.len() ==> mapped(*f, src[i], #[trigger] dst[i])
            }),
    {
        let mut to = <V::E as Streams<'a, V::X>>::fresh();
        let ghost src = <Held<V, C> as VGet>::svs(crate::cursor::vals_of::<V::E>(s@));
        // the values as a reader gives them, and those read so far
        let ghost o: Seq<Get<'s, V>> = vals_of::<V::X>(s@);
        let ghost mut done: Seq<Get<'s, V>> = Seq::empty();
        proof {
            lemma_lens_ok::<V::E, V::X>(s@);
            lemma_vals_own::<V::E, V::X>(s@);
            assert(crate::cursor::vals_of::<V::E>(s@) =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
            <Get<'s, V> as VGet>::lemma_svs(o);
            assert(each::<V::X>(s@.take(0)) =~= Seq::empty());
        }
        let mut k: usize = 0;
        while k < s.len()
            invariant
                k <= s.len(),
                lens_ok::<V::X>(s@),
                o == vals_of::<V::X>(s@),
                src.len() == o.len(),
                forall|i: int| 0 <= i < o.len() ==> #[trigger] src[i] == o[i].sv(),
                forall|v: V| src.contains(V::sv_of(v)) ==> #[trigger] call_requires(*f, (v,)),
                done == vals_of::<V::X>(s@.take(k as int)),
                to.e_inv(),
                <V::E as Streams<'a, V::X>>::budget(to) is None,
                <V::E as Streams<'a, V::X>>::whole(to),
                to.e_values().len() == done.len(),
                forall|i: int|
                    0 <= i < done.len() ==> mapped(*f, done[i].sv(), (#[trigger] to.e_values()[i]).sv()),
            decreases s.len() - k,
        {
            // the values up to the end of this slab
            let ghost pre = vals_of::<V::X>(s@.take(k + 1));
            proof {
                lemma_read_on::<V::X>(s@, k as int);
                lemma_read_first::<V::X>(s@, k + 1);
            }
            let mut dec = <V::X as Decoders<'s>>::open(&s[k]);
            // a Verus loop with an invariant: `loop` + `match`
            #[allow(clippy::while_let_loop)]
            loop
                invariant
                    dec.inv(),
                    done + dec.rest() == pre,
                    pre.len() <= o.len(),
                    forall|i: int| 0 <= i < pre.len() ==> #[trigger] pre[i] == o[i],
                    src.len() == o.len(),
                    forall|i: int| 0 <= i < o.len() ==> #[trigger] src[i] == o[i].sv(),
                    forall|v: V| src.contains(V::sv_of(v)) ==> #[trigger] call_requires(*f, (v,)),
                    to.e_inv(),
                    <V::E as Streams<'a, V::X>>::budget(to) is None,
                    <V::E as Streams<'a, V::X>>::whole(to),
                    to.e_values().len() == done.len(),
                    forall|i: int|
                        0 <= i < done.len() ==> mapped(*f, done[i].sv(), (#[trigger] to.e_values()[i]).sv()),
                ensures
                    done == pre,
                decreases dec.rest().len(),
            {
                let ghost rest0 = dec.rest();
                let ghost done0 = done;
                let ghost vals0 = to.e_values();
                match dec.d_run_max(usize::MAX) {
                    Some(run) => {
                        proof {
                            // the run's value is the first value left, and
                            // one of the encoder's
                            let p = done0.len() as int;
                            assert(rest0.take(run.count as int)[0] == run.value);
                            assert(pre[p] == rest0[0]);
                            assert(src[p] == o[p].sv());
                            assert(src.contains(run.value.sv()));
                        }
                        let v = V::to_owned(run.value);
                        let ghost gv = v;
                        let w = f(v);
                        let ghost gw = w;
                        let x = V::owned_for(w);
                        <V::E as Streams<'a, V::X>>::give(&mut to, x, run.count);
                        proof {
                            let n = run.count as int;
                            let xv = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x);
                            done = done0 + rest0.take(n);
                            assert(done + dec.rest() =~= pre);
                            assert forall|i: int| 0 <= i < done.len() implies mapped(
                                *f,
                                done[i].sv(),
                                (#[trigger] to.e_values()[i]).sv(),
                            ) by {
                                if i >= done0.len() {
                                    assert(done[i] == rest0.take(n)[i - done0.len()]);
                                    assert(to.e_values()[i] == xv);
                                    assert(call_ensures(*f, (gv,), gw));
                                } else {
                                    assert(done[i] == done0[i]);
                                    assert(to.e_values()[i] == vals0[i]);
                                }
                            }
                        }
                    },
                    None => {
                        proof {
                            assert(done =~= pre);
                        }
                        break ;
                    },
                }
            }
            k = k + 1;
        }
        proof {
            assert(s@.take(k as int) =~= s@);
            <Held<V, C> as VGet>::lemma_svs(to.e_values());
        }
        to
    }

    /// An encoder without a segment budget, given what `f` gives for
    /// the values this one was given.
    fn remapped<F: Fn(V) -> V>(self, f: &F) -> (r: Self)
        requires
            self.wf(),
            forall|v: V| self@.contains(V::sv_of(v)) ==> #[trigger] call_requires(*f, (v,)),
        ensures
            r.wf(),
            r.max_segments() is None,
            r@.len() == self@.len(),
            forall|i: int| 0 <= i < self@.len() ==> mapped(*f, self@[i], #[trigger] r@[i]),
    {
        let mut e = self.e;
        let slabs = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::take(&mut e);
        let to = Self::walk(slabs.as_slice(), f);
        VerifiedEncoder { e: to }
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
        let m = self.remapped(&f);
        let r = m.save_to(out);
        proof {
            assert(out@.skip(old(out)@.len() as int) =~= VerifiedColumn::<V, C>::encoding(m@));
        }
        r
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
        let m = self.remapped(&f);
        let r = m.save_to_unless(out, unless);
        proof {
            if out@ != old(out)@ {
                assert(out@.skip(old(out)@.len() as int) =~= VerifiedColumn::<V, C>::encoding(m@));
            } else {
                assert forall|i: int| 0 <= i < self@.len() implies mapped(f, #[trigger] self@[i], unless.sv()) by {
                    assert(mapped(f, self@[i], m@[i]));
                }
            }
            if forall|i: int, b: V::S| 0 <= i < self@.len() && #[trigger] mapped(f, self@[i], b) ==> b == unless.sv() {
                assert forall|i: int| 0 <= i < m@.len() implies m@[i] == unless.sv() by {
                    assert(mapped(f, self@[i], m@[i]));
                }
            }
        }
        r
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
    pub fn into_column(self) -> (r: VerifiedColumn<V, C>)
        requires
            self.wf(),
        ensures
            r@ == self@,
    {
        let mut e = self.e;
        match <V::E as Streams<'a, V::X>>::budget_of(&e) {
            Some(max) => {
                let slabs = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::take(&mut e);
                let core = Core::<V::E>::from_cut(slabs, max);
                VerifiedColumn { core }
            },
            None => {
                let bytes = <V::E as Streams<'a, V::X>>::bytes(e);
                match VerifiedColumn::<V, C>::load(bytes.as_slice()) {
                    Ok(c) => c,
                    Err(err) => {
                        proof {
                            // what was saved is the encoding, and a load
                            // takes every encoding that is `loadable`
                            VerifiedColumn::<V, C>::lemma_encoding(bytes@);
                            assert(!VerifiedColumn::<V, C>::loadable(bytes@));
                        }
                        unloadable(err, Ghost(self@))
                    },
                }
            },
        }
    }
}

/// The values of the slabs up to and with slab `k`, as a reader gives
/// them, are those of the slabs before it and then its own.
proof fn lemma_read_on<'s, X: Decoders<'s>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        0 <= k < s.len(),
    ensures
        vals_of::<X>(s.take(k + 1)) == vals_of::<X>(s.take(k)) + X::vals(s[k]),
{
    let cs = each::<X>(s.take(k + 1));
    assert(cs.drop_last() =~= each::<X>(s.take(k)));
    assert(cs.last() == X::vals(s[k]));
}

/// The values of the first `k` slabs are the first of all the values.
proof fn lemma_read_first<'s, X: Decoders<'s>>(s: Seq<Slab<X::Tail>>, k: int)
    requires
        0 <= k <= s.len(),
    ensures
        vals_of::<X>(s.take(k)).len() <= vals_of::<X>(s).len(),
        forall|i: int|
            0 <= i < vals_of::<X>(s.take(k)).len() ==> (#[trigger] vals_of::<X>(s.take(k))[i]) == vals_of::<X>(s)[i],
    decreases s.len() - k,
{
    if k == s.len() {
        assert(s.take(k) =~= s);
    } else {
        lemma_read_on::<X>(s, k);
        lemma_read_first::<X>(s, k + 1);
        let a = vals_of::<X>(s.take(k));
        let b = vals_of::<X>(s.take(k + 1));
        assert forall|i: int| 0 <= i < a.len() implies (#[trigger] a[i]) == vals_of::<X>(s)[i] by {
            assert(b[i] == a[i]);
        }
    }
}

/// Panics: what an encoder saved is no column that loads. A caller has
/// to show that it is not one a load is sure to take
/// ([`loadable`](VerifiedColumn::loadable)): that the values `vs` save
/// to bytes too long for it, or are more than a column counts. The
/// words are those of the `unwrap` in
/// [`EncoderApi::into_column`](crate::EncoderApi::into_column).
#[verifier::external_body]
#[cold]
#[inline(never)]
fn unloadable<V: VValue<C>, C: VCodec>(e: PackError, Ghost(vs): Ghost<Seq<V::S>>) -> VerifiedColumn<V, C>
    requires
        VerifiedColumn::<V, C>::encoding(vs).len() > 0xffff_fff5 || vs.len() > usize::MAX - 3,
    ensures
        false,
{
    panic!("called `Result::unwrap()` on an `Err` value: {e:?}")
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
        let r = VerifiedEncoderState { s: <V::E as Flows<'a, V::X>>::f_new() };
        proof {
            lemma_svs_none::<Held<V, C>>();
            assert(<V::E as Flows<'a, V::X>>::f_values(r.s) =~= Seq::empty());
        }
        r
    }

    /// The number of values given.
    #[inline(always)]
    pub fn len(&self) -> (r: usize)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == self@.len(),
    {
        proof {
            <Held<V, C> as VGet>::lemma_svs(<V::E as Flows<'a, V::X>>::f_values(self.s));
        }
        <V::E as Flows<'a, V::X>>::f_count(&self.s)
    }

    /// Whether no value was given.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == (self@.len() == 0),
    {
        self.len() == 0
    }

    #[inline(always)]
    fn put(&mut self, buf: &mut Vec<u8>, x: Given<'a, V, C>, n: usize, Ghost(s): Ghost<V::S>)
        requires
            old(self).on(old(buf)@),
            <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x).sv() == s,
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            final(self)@ == old(self)@ + Seq::new(n as nat, |_i: int| s),
    {
        <V::E as Flows<'a, V::X>>::f_give(&mut self.s, buf, x, n);
        proof {
            let o = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x);
            let added = Seq::new(n as nat, |_i: int| o);
            lemma_svs_add(<V::E as Flows<'a, V::X>>::f_values(old(self).s), added);
            <Held<V, C> as VGet>::lemma_svs(added);
            assert(<Held<V, C> as VGet>::svs(added) =~= Seq::new(n as nat, |_i: int| s));
        }
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        self.put(buf, x, 1, Ghost(value.sv()));
        proof {
            assert(self@ =~= old(self)@.push(value.sv()));
        }
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        self.put(buf, x, n, Ghost(value.sv()));
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
        let ghost s = V::sv_of(value);
        let x = V::owned_for(value);
        self.put(buf, x, 1, Ghost(s));
        proof {
            assert(self@ =~= old(self)@.push(s));
        }
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
        let ghost s = V::sv_of(value);
        let x = V::owned_for(value);
        self.put(buf, x, n, Ghost(s));
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
        <V::E as Flows<'a, V::X>>::lemma_f_head(self.s, buf);
    }

    /// Give the state each value of `iter`, in order.
    #[verifier::exec_allows_no_decreases_clause]
    pub fn extend<I: IntoIterator<Item = Get<'a, V>>>(&mut self, buf: &mut Vec<u8>, iter: I)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            exists|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> final(self)@ == old(
                    self,
                )@ + VGet::svs(it.remaining())),
    {
        let mut it = iter.into_iter();
        let ghost it0 = it;
        let ghost mut given: Seq<Get<'a, V>> = Seq::empty();
        proof {
            lemma_svs_none::<Get<'a, V>>();
            assert(given + it.remaining() =~= it0.remaining());
            assert(self@ =~= old(self)@ + <Get<'a, V> as VGet>::svs(given));
        }
        // a Verus loop with an invariant: `loop` + `match`
        #[allow(clippy::while_let_loop)]
        loop
            invariant_except_break
                it.obeys_prophetic_iter_laws() ==> given + it.remaining() == it0.remaining(),
            invariant
                self.on(buf@),
                self.before(buf@) == old(self).before(old(buf)@),
                it.obeys_prophetic_iter_laws() == it0.obeys_prophetic_iter_laws(),
                self@ == old(self)@ + <Get<'a, V> as VGet>::svs(given),
            ensures
                it0.obeys_prophetic_iter_laws() ==> given == it0.remaining(),
        {
            let ghost before = given;
            let ghost rem = it.remaining();
            match it.next() {
                Some(v) => {
                    let ghost s0 = self@;
                    self.append(buf, v);
                    proof {
                        given = before.push(v);
                        lemma_svs_add(before, seq![v]);
                        lemma_svs_one(v);
                        assert(before.push(v) =~= before + seq![v]);
                        assert(s0.push(v.sv()) =~= s0 + seq![v.sv()]);
                        assert(self@ =~= old(self)@ + <Get<'a, V> as VGet>::svs(given));
                        if it.obeys_prophetic_iter_laws() {
                            assert(given + it.remaining() =~= before + rem);
                        }
                    }
                },
                None => {
                    proof {
                        if it.obeys_prophetic_iter_laws() {
                            assert(given =~= before + rem);
                        }
                    }
                    break ;
                },
            }
        }
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
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        let ghost o = <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::val(x);
        let ghost vs = <V::E as Flows<'a, V::X>>::f_values(self.s);
        let ghost all = Seq::new(vs.len(), |_i: int| o);
        proof {
            <Held<V, C> as VGet>::lemma_svs(vs);
            <Held<V, C> as VGet>::lemma_svs(all);
        }
        let r = <V::E as Flows<'a, V::X>>::f_all_are(&self.s, &x);
        proof {
            if only(self@, value.sv()) {
                assert(<Held<V, C> as VGet>::svs(vs) =~= <Held<V, C> as VGet>::svs(all));
                lemma_svs_inj::<Held<V, C>>(vs, all);
            }
        }
        r
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
        let ghost h = self.before(buf@);
        <V::E as Flows<'a, V::X>>::f_finish(self.s, buf);
        proof {
            let d = buf@.skip(h.len() as int);
            VerifiedColumn::<V, C>::lemma_encoding(d);
            V::lemma_size(d);
            assert(buf@ =~= h + d);
        }
    }
}

} // verus!

impl<V: VValue<C>, C: VCodec> std::fmt::Debug for VerifiedEncoder<'_, V, C> {
    /// How many values it has been given. This is not checked: it reads
    /// the count that [`len`](VerifiedEncoder::len) returns.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedEncoder")
            .field("len", &<V::E as Streams<'_, V::X>>::count(&self.e))
            .finish()
    }
}
