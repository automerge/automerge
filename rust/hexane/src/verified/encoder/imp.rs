//! The implementation of `verified::encoder` and its proofs: what its
//! specification (`encoder.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{lemma_held, lemma_svs_add, lemma_svs_inj, lemma_svs_none, lemma_svs_one};
use super::*;

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

impl<'a, V: VValue<C>, C: VCodec> VerifiedEncoder<'a, V, C> {
    pub(super) fn new_impl() -> (r: Self)
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

    pub(super) fn with_max_segments_impl(max_segments: usize) -> (r: Self)
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

    pub(super) fn with_segments_impl() -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<V::S>::empty(),
            r.max_segments() == Some(64usize),
    {
        Self::with_max_segments(MAX_SEG)
    }

    #[inline(always)]
    pub(super) fn len_impl(&self) -> (r: usize)
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

    #[inline(always)]
    pub(super) fn is_empty_impl(&self) -> (r: bool)
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
    pub(super) fn put(&mut self, x: Given<'a, V, C>, n: usize, Ghost(s): Ghost<V::S>)
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

    #[inline(always)]
    pub(super) fn append_impl(&mut self, value: Get<'a, V>)
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

    #[inline(always)]
    pub(super) fn append_n_impl(&mut self, value: Get<'a, V>, n: usize)
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

    #[inline(always)]
    pub(super) fn append_owned_impl(&mut self, value: V)
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

    #[inline(always)]
    pub(super) fn append_n_owned_impl(&mut self, value: V, n: usize)
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

    #[verifier::exec_allows_no_decreases_clause]
    pub(super) fn extend_impl<I: IntoIterator<Item = Get<'a, V>>>(&mut self, iter: I)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).max_segments() == old(self).max_segments(),
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
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

    pub(super) fn save_impl(self) -> (r: Vec<u8>)
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

    pub(super) fn save_to_impl(self, out: &mut Vec<u8>) -> (r: Range<usize>)
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

    pub(super) fn save_to_unless_impl(self, out: &mut Vec<u8>, value: Get<'a, V>) -> (r: Range<usize>)
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

    pub(super) fn encode_impl<I: IntoIterator<Item = Get<'a, V>>>(iter: I) -> (r: Vec<u8>)
        ensures
            VerifiedColumn::<V, C>::valid(r@),
            r@ == VerifiedColumn::<V, C>::encoding(VerifiedColumn::<V, C>::values(r@)),
            r@.len() <= V::size_bound(VerifiedColumn::<V, C>::values(r@)),
            gives(iter, |xs: Seq<I::Item>| VerifiedColumn::<V, C>::values(r@) == VGet::svs(xs)),
    {
        let mut buf: Vec<u8> = Vec::new();
        Self::encode_to(&mut buf, iter);
        proof {
            assert(buf@.skip(0) =~= buf@);
        }
        buf
    }

    pub(super) fn encode_to_impl<I: IntoIterator<Item = Get<'a, V>>>(buf: &mut Vec<u8>, iter: I) -> (r: Range<usize>)
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

    pub(super) fn encode_to_unless_impl<I: IntoIterator<Item = Get<'a, V>>>(
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
    pub(super) fn walk<'s, F: Fn(V) -> V>(s: &'s [Slab<<V::E as Slabs>::Tail>], f: &F) -> (to: <V::E as Streams<'a, V::X>>::Enc)
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
    pub(super) fn remapped<F: Fn(V) -> V>(self, f: &F) -> (r: Self)
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

    pub(super) fn save_to_and_remap_impl<F: Fn(V) -> V>(self, out: &mut Vec<u8>, f: F) -> (r: Range<usize>)
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

    pub(super) fn save_to_unless_and_remap_impl<F: Fn(V) -> V>(self, out: &mut Vec<u8>, unless: Get<'a, V>, f: F) -> (r: Range<
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

    pub(super) fn into_column_impl(self) -> (r: VerifiedColumn<V, C>)
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
pub(super) proof fn lemma_read_on<'s, X: Decoders<'s>>(s: Seq<Slab<X::Tail>>, k: int)
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
pub(super) proof fn lemma_read_first<'s, X: Decoders<'s>>(s: Seq<Slab<X::Tail>>, k: int)
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

impl<'a, V: VValue<C>, C: VCodec> VerifiedEncoderState<'a, V, C> {
    #[inline(always)]
    pub(super) fn new_impl() -> (r: Self)
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

    #[inline(always)]
    pub(super) fn len_impl(&self) -> (r: usize)
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

    #[inline(always)]
    pub(super) fn is_empty_impl(&self) -> (r: bool)
        requires
            exists|buf: Seq<u8>| self.on(buf),
        ensures
            r == (self@.len() == 0),
    {
        self.len() == 0
    }

    #[inline(always)]
    pub(super) fn put(&mut self, buf: &mut Vec<u8>, x: Given<'a, V, C>, n: usize, Ghost(s): Ghost<V::S>)
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

    #[inline(always)]
    pub(super) fn append_impl(&mut self, buf: &mut Vec<u8>, value: Get<'a, V>)
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

    #[inline(always)]
    pub(super) fn append_n_impl(&mut self, buf: &mut Vec<u8>, value: Get<'a, V>, n: usize)
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

    #[inline(always)]
    pub(super) fn append_owned_impl(&mut self, buf: &mut Vec<u8>, value: V)
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

    #[inline(always)]
    pub(super) fn append_n_owned_impl(&mut self, buf: &mut Vec<u8>, value: V, n: usize)
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

    pub(super) proof fn lemma_before_impl(&self, buf: Seq<u8>)
        requires
            self.on(buf),
        ensures
            self.before(buf).len() <= buf.len(),
            buf.take(self.before(buf).len() as int) == self.before(buf),
    {
        <V::E as Flows<'a, V::X>>::lemma_f_head(self.s, buf);
    }

    #[verifier::exec_allows_no_decreases_clause]
    pub(super) fn extend_impl<I: IntoIterator<Item = Get<'a, V>>>(&mut self, buf: &mut Vec<u8>, iter: I)
        requires
            old(self).on(old(buf)@),
        ensures
            final(self).on(final(buf)@),
            final(self).before(final(buf)@) == old(self).before(old(buf)@),
            gives(iter, |xs: Seq<I::Item>| final(self)@ == old(self)@ + VGet::svs(xs)),
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

    #[inline(always)]
    pub(super) fn is_single_run_of_impl(&self, value: Get<'a, V>) -> (r: bool)
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

    #[inline(always)]
    pub(super) fn finish_impl(self, buf: &mut Vec<u8>)
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
