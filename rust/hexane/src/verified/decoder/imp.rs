//! The implementation of `verified::decoder` and its proofs: what its
//! specification (`decoder.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

#[cfg(verus_keep_ghost)]
use super::super::{held, lemma_held, lemma_svs_none, lemma_svs_sub};
use super::*;

use crate::iter_core::{Decode, Decoders};
use crate::Leb128;
use crate::PackError;
use crate::Run;
use std::cmp::Ordering;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

pub(super) proof fn lemma_head_run_max_impl<S>(o: Seq<S>, v: S, n: int, w: S, m: int, max: int)
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

pub(super) proof fn lemma_head_len_impl<S>(o: Seq<S>, v: S, n: int, max: int)
    requires
        head_run_max(o, v, n, max),
    ensures
        v == o[0],
        head_len(o, max) == n,
{
    assert(o[0] == v);
    lemma_head_run_max(o, v, n, o[0], head_len(o, max), max);
}

impl<'a, V: ColumnValueRef<C>, C: Codec> Decoder<'a, V, C> {
    /// The decoder stands in a saved column's bytes, at a value or at
    /// the end.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        self.d.inv()
    }

    pub(super) proof fn lemma_items_len_impl(&self)
        ensures
            self.rest().len() == self.items().len(),
    {
        <Get<'a, V> as VGet>::lemma_svs(self.items());
    }

    #[inline]
    pub(super) fn new_impl(bytes: &'a [u8]) -> (r: Self)
        requires
            Column::<V, C>::valid(bytes@),
        ensures
            r.rest() == Column::<V, C>::values(bytes@),
    {
        let d = <V::E as Model<V::X>>::decoder(bytes);
        proof {
            let o = d.rest();
            assert(<V::E as Model<V::X>>::values(bytes@) =~= held::<V, C>(o));
            lemma_held::<V, C>(o);
        }
        Decoder { d }
    }

    pub(super) fn try_new_impl(bytes: &'a [u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(d) ==> {
                &&& Column::<V, C>::valid(bytes@)
                &&& d.rest() == Column::<V, C>::values(bytes@)
            },
            Column::<V, C>::valid(bytes@) && bytes@.len() <= u32::MAX ==> r is Ok,
    {
        match <V::E as Model<V::X>>::validate(bytes) {
            Ok(_) => Ok(Self::new(bytes)),
            Err(e) => Err(e),
        }
    }

    pub(super) fn advance_by_impl(&mut self, n: usize)
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

    #[inline]
    pub(super) fn next_run_impl(&mut self) -> (r: Option<Run<Get<'a, V>>>)
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

    #[inline]
    pub(super) fn next_run_max_impl(&mut self, max: usize) -> (r: Option<Run<Get<'a, V>>>)
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

    pub(super) fn scan_for_impl<'b>(&mut self, target: Get<'b, V>, max: usize) -> (r: (usize, usize))
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

impl<'a, V: ColumnValueRef<C>, C: Codec> Decoder<'a, V, C> {
    #[inline]
    pub(super) fn next_impl(&mut self) -> (r: Option<Get<'a, V>>)
        ensures
            match r {
                Some(v) => {
                    &&& old(self).rest().len() > 0
                    &&& v.sv() == old(self).rest()[0]
                    &&& final(self).rest() == old(self).rest().skip(1)
                },
                None => old(self).rest().len() == 0,
            },
            // what Verus asks of every iterator's `next`
            old(self).items().len() > 0 ==> final(self).items() == old(self).items().drop_first() && r == Some(old(self).items()[0]),
            old(self).items().len() == 0 ==> final(self).items() == old(self).items() && r is None,
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

    #[inline]
    pub(super) fn nth_impl(&mut self, n: usize) -> (r: Option<Get<'a, V>>)
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

impl<'a, V: ColumnValueRef<C>, C: Codec> Decoder<'a, V, C> {
    pub(super) fn clone_impl(&self) -> (r: Self)
        ensures
            r.items() == self.items(),
    {
        proof {
            use_type_invariant(self);
        }
        Decoder { d: self.d.dup() }
    }
}

} // verus!

impl<V: ColumnValueRef<C>, C: Codec> std::fmt::Debug for Decoder<'_, V, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder").finish_non_exhaustive()
    }
}
