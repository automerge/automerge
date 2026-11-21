//! The implementation of `verified::counts` and its proofs: what its
//! specification (`counts.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

#[cfg(verus_keep_ghost)]
use crate::bool_wire::{
    bool_counts, bool_parses, bool_valid, bool_values, counts_bytes, counts_len, lemma_values_len,
};
use crate::bool_wire::{CountOps, CountsOf};
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A `bool` column under codec `C`: the name its slab model and readers
/// take (`BoolModel<VB<C>>`), for the counts `VCounts<C>`.
#[verifier::reject_recursive_types(C)]
pub struct VB<C>(PhantomData<C>);

/// The counts of a `bool` column under codec `C`: unsigned numbers of
/// the codec.
#[verifier::reject_recursive_types(C)]
pub struct VCounts<C>(PhantomData<C>);

impl<C: VCodec> CountsOf for VB<C> {
    type K = VCounts<C>;
}

impl<C: VCodec> CountOps for VCounts<C> {
    open spec fn enc(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(a, b, s, t);
    }

    #[inline(always)]
    fn put_count(n: usize, buf: &mut Vec<u8>) {
        C::put_u(n as u64, buf)
    }

    #[inline(always)]
    fn read_count(data: &[u8]) -> (r: Option<(usize, usize)>) {
        match C::read_u_opt(data) {
            Some((n, x)) => {
                if x <= usize::MAX as u64 {
                    Some((n, x as usize))
                } else {
                    // the number there is no `usize`, and no other starts
                    // the bytes
                    proof {
                        assert forall|y: u64| y <= usize::MAX implies !(#[trigger] Self::enc(y)).is_prefix_of(data@) by {
                            let e = C::enc_u(y);
                            if e.is_prefix_of(data@) {
                                assert(e + data@.skip(e.len() as int) =~= data@);
                                assert(C::enc_u(x) + data@.skip(n as int) =~= data@);
                                C::lemma_u(y, x, data@.skip(e.len() as int), data@.skip(n as int));
                            }
                        }
                    }
                    None
                }
            },
            None => None,
        }
    }
}

// ── Sizes ───────────────────────────────────────────────────────────────────

/// Counts are at most ten bytes each.
pub(super) proof fn lemma_bytes_le<K: CountOps>(cs: Seq<u64>)
    ensures
        counts_bytes::<K>(cs).len() <= 10 * cs.len(),
    decreases cs.len(),
{
    if cs.len() > 0 {
        K::axiom_enc(cs.last(), cs.last(), Seq::empty(), Seq::empty());
        lemma_bytes_le::<K>(cs.drop_last());
    }
}

/// Counts that are none of them zero, but for the first, are no more
/// than the values they stand for and one.
pub(super) proof fn lemma_runs_le(cs: Seq<u64>)
    requires
        forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0,
    ensures
        cs.len() <= counts_len(cs) + 1,
    decreases cs.len(),
{
    if cs.len() > 1 {
        let p = cs.drop_last();
        assert forall|i: int| 1 <= i < p.len() implies #[trigger] p[i] > 0 by {
            assert(p[i] == cs[i]);
        }
        lemma_runs_le(p);
        assert(cs.last() == cs[cs.len() - 1]);
    }
}

/// A saved `bool` column is at most ten bytes a value, and ten more: a
/// run is at least one value, but for the first, which may be none.
pub(crate) proof fn lemma_bool_size<K: CountOps>(d: Seq<u8>)
    requires
        bool_valid::<K>(d),
    ensures
        d.len() <= 10 * bool_values::<K>(d).len() + 10,
{
    let cs = bool_counts::<K>(d);
    assert(bool_parses::<K>(cs, d));
    lemma_bytes_le::<K>(cs);
    lemma_runs_le(cs);
    lemma_values_len(cs);
}

} // verus!
