//! What a delta encoder's values realize, in specifications: the values
//! a run of deltas gives (`realize`), the least and greatest of some
//! values (`vlow`, `vhigh`), and whether they span less than 2^63
//! (`span_ok`), which the verified delta encoder
//! (`crate::verified::delta::enc`) checks when it finishes.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::btree::{max_i, min_i};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{dval, psum};
#[cfg(verus_keep_ghost)]
use crate::rle::model::*;
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::encodes;
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
#[allow(unused_imports)]
use vstd::prelude::*;

pub(crate) use crate::trusted::values_too_wide;
verus! {

/// The values deltas `ds` realize: at each non-null delta, the running
/// sum after it.
pub open spec fn realize(ds: Seq<Option<i64>>) -> Seq<Option<int>>
    decreases ds.len(),
{
    if ds.len() == 0 {
        Seq::empty()
    } else {
        realize(ds.drop_last()).push(match ds.last() {
            Some(_) => Some(psum(ds)),
            None => None,
        })
    }
}

/// The last value of `vs`, or 0 if there is none: where the next delta
/// counts from.
pub open spec fn last_val(vs: Seq<Option<i64>>) -> int
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        match vs.last() {
            Some(x) => x as int,
            None => last_val(vs.drop_last()),
        }
    }
}

/// The least value of `vs` (`i64::MAX` if there is none).
pub open spec fn vlow(vs: Seq<Option<i64>>) -> int
    decreases vs.len(),
{
    if vs.len() == 0 {
        i64::MAX as int
    } else {
        match vs.last() {
            Some(x) => min_i(vlow(vs.drop_last()), x as int),
            None => vlow(vs.drop_last()),
        }
    }
}

/// The greatest value of `vs` (`i64::MIN` if there is none).
pub open spec fn vhigh(vs: Seq<Option<i64>>) -> int
    decreases vs.len(),
{
    if vs.len() == 0 {
        i64::MIN as int
    } else {
        match vs.last() {
            Some(x) => max_i(vhigh(vs.drop_last()), x as int),
            None => vhigh(vs.drop_last()),
        }
    }
}

/// The values span less than 2^63 (or there are none).
pub open spec fn span_ok(vs: Seq<Option<i64>>) -> bool {
    vlow(vs) > vhigh(vs) || vhigh(vs) - vlow(vs) <= i64::MAX
}

/// A run of `n` copies of `v` after `vs`.
pub(crate) proof fn lemma_vals_run(vs: Seq<Option<i64>>, n: nat, v: Option<i64>)
    requires n >= 1,
    ensures ({
        let r = vs + copies(n, v);
        match v {
            Some(x) => vlow(r) == min_i(vlow(vs), x as int) && vhigh(r) == max_i(vhigh(vs), x as int)
                && last_val(r) == x as int,
            None => vlow(r) == vlow(vs) && vhigh(r) == vhigh(vs) && last_val(r) == last_val(vs),
        }
    }),
    decreases n,
{
    let m = (n - 1) as nat;
    let r0 = vs + copies(m, v);
    assert(vs + copies(n, v) =~= r0.push(v));
    assert(r0.push(v).drop_last() =~= r0);
    if m == 0 {
        assert(r0 =~= vs);
    } else {
        lemma_vals_run(vs, m, v);
    }
}

/// The last value is among the values, if there are any.
proof fn lemma_last_in(vs: Seq<Option<i64>>)
    ensures
        vlow(vs) <= vhigh(vs) ==> vlow(vs) <= last_val(vs) <= vhigh(vs),
        vlow(vs) > vhigh(vs) ==> last_val(vs) == 0 && vlow(vs) == i64::MAX && vhigh(vs) == i64::MIN,
    decreases vs.len(),
{
    if vs.len() > 0 {
        lemma_last_in(vs.drop_last());
    }
}

/// Deltas `ds`, then `n` copies of a delta of 0 or of null.
proof fn lemma_realize_run(ds: Seq<Option<i64>>, n: nat, d: Option<i64>)
    requires d == Some(0i64) || d is None,
    ensures
        realize(ds + copies(n, d)) == realize(ds) + copies(n, match d { Some(_) => Some(psum(ds)), None => None::<int> }),
        psum(ds + copies(n, d)) == psum(ds),
    decreases n,
{
    let x = match d { Some(_) => Some(psum(ds)), None => None::<int> };
    if n == 0 {
        assert(ds + copies(0, d) =~= ds);
        assert(realize(ds) + copies(0, x) =~= realize(ds));
    } else {
        let m = (n - 1) as nat;
        lemma_realize_run(ds, m, d);
        let r0 = ds + copies(m, d);
        assert(ds + copies(n, d) =~= r0.push(d));
        assert(r0.push(d).drop_last() =~= r0);
        assert(realize(ds) + copies(n, x) =~= (realize(ds) + copies(m, x)).push(x));
    }
}



/// A domain `lo..=hi` as wide as `i64`'s, so that values in it can span
/// 2^63.
pub open spec fn wide(lo: i64, hi: i64) -> bool {
    hi - lo > i64::MAX
}

} // verus!
