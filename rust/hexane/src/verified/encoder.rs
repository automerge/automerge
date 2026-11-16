//! The encoder of the verified public API ([`VerifiedEncoder`]),
//! verified with Verus.
//!
//! It runs the cores that a column's `remap` runs (`rle::enc`,
//! `bool_enc`) and that [`RleEncoder`](crate::encoder::RleEncoder) and
//! [`BoolEncoder`](crate::encoder::BoolEncoder) run, through the model's
//! `Streams`, and nothing is assumed that a column does not assume.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::model::Streams;
// named in specifications, and in these docs
#[allow(unused_imports)]
use super::VerifiedColumn;
#[cfg(verus_keep_ghost)]
use super::{lemma_svs_add, lemma_svs_none, lemma_svs_one, Held, VGet, VSpec};
use super::{Get, Given, VCodec, VValue};
use crate::remap::Encodes;
use crate::Leb128;
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
/// A value given by reference (a byte string) is kept by reference
/// until the encoder is saved, so the encoder lives no longer than
/// `'a`.
///
/// # What a client has to keep
///
/// Unlike a column, an encoder asks its client for a fact about itself:
/// every method requires `wf()`, and every method that changes the
/// encoder ensures it again. [`new`](Self::new) gives it. So
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
    {
        Self::new()
    }
}

impl<'a, V: VValue<C>, C: VCodec> VerifiedEncoder<'a, V, C> {
    /// The encoder is one that [`new`](Self::new) gave and this type's
    /// methods have changed: what each of them requires, and keeps.
    pub closed spec fn wf(&self) -> bool {
        self.e.e_inv()
    }

    /// An encoder that has been given nothing.
    pub fn new() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
    {
        let e = <V::E as Streams<'a, V::X>>::fresh();
        proof {
            assert(e.e_values() =~= Seq::empty());
            lemma_svs_none::<Held<V, C>>();
        }
        VerifiedEncoder { e }
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
    {
        <<V::E as Streams<'a, V::X>>::Enc as Encodes<V::E>>::put(&mut self.e, x, n);
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
    {
        proof {
            V::lemma_own(value);
        }
        let x = <V::E as Streams<'a, V::X>>::given(value);
        self.put(x, n, Ghost(value.sv()));
    }

    /// Give the encoder `value`, which it then owns: for a byte string,
    /// nothing is borrowed and nothing is copied.
    #[inline(always)]
    pub fn append_owned(&mut self, value: V)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.push(V::sv_of(value)),
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
