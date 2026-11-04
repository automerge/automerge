//! A delta slab's aggregate (`SlabAgg`), verified with Verus.
//!
//! A delta column stores each value as its difference from the one
//! before (a null holds the running sum where it is). A slab's aggregate
//! records its item count, the sum of its deltas, and the least and
//! greatest running sum after each of its items, relative to the slab's
//! start: what value searches prune by, and what `load` checks the value
//! domain against. [`agg_run`] folds one run into an aggregate: the
//! loader calls it as runs stream past, and `compute_slab_agg` for a
//! whole slab, so the two agree by construction. Each is exact
//! (`agg_of`), or reports that a running sum leaves `i64`.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

use crate::btree::SlabAgg;
#[cfg(verus_keep_ghost)]
use crate::btree::{max_i, min_i, SlabAggregate};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A delta's step to the running sum: a null holds it.
pub open spec fn dval(d: Option<i64>) -> int {
    match d {
        Some(v) => v as int,
        None => 0,
    }
}

/// The running sum after deltas `ds`.
pub open spec fn psum(ds: Seq<Option<i64>>) -> int
    decreases ds.len(),
{
    if ds.len() == 0 { 0 } else { psum(ds.drop_last()) + dval(ds.last()) }
}

/// The least running sum after each of deltas `ds` (0 for none).
pub open spec fn pmin(ds: Seq<Option<i64>>) -> int
    decreases ds.len(),
{
    if ds.len() <= 1 { psum(ds) } else { min_i(pmin(ds.drop_last()), psum(ds)) }
}

/// The greatest running sum after each of deltas `ds` (0 for none).
pub open spec fn pmax(ds: Seq<Option<i64>>) -> int
    decreases ds.len(),
{
    if ds.len() <= 1 { psum(ds) } else { max_i(pmax(ds.drop_last()), psum(ds)) }
}

/// `w` is the aggregate of deltas `ds`.
pub open spec fn agg_of(w: SlabAgg, ds: Seq<Option<i64>>) -> bool {
    &&& w.len == ds.len()
    &&& w.total == psum(ds)
    &&& w.min_offset == pmin(ds)
    &&& w.max_offset == pmax(ds)
}

/// `n` copies of delta `d`: a run.
pub open spec fn reps(n: nat, d: Option<i64>) -> Seq<Option<i64>> {
    Seq::new(n, |_i: int| d)
}

proof fn lemma_push(ds: Seq<Option<i64>>, d: Option<i64>)
    ensures
        psum(ds.push(d)) == psum(ds) + dval(d),
        ds.len() == 0 ==> pmin(ds.push(d)) == dval(d) && pmax(ds.push(d)) == dval(d),
        ds.len() > 0 ==> pmin(ds.push(d)) == min_i(pmin(ds), psum(ds) + dval(d))
            && pmax(ds.push(d)) == max_i(pmax(ds), psum(ds) + dval(d)),
{
    assert(ds.push(d).drop_last() =~= ds);
    if ds.len() == 0 {
        assert(psum(ds) == 0);
    }
}

/// A run of `n` copies of non-null delta `v` after `ds`: the running sums
/// move one way, so its ends bound them.
proof fn lemma_run(ds: Seq<Option<i64>>, n: nat, v: i64)
    requires n >= 1,
    ensures ({
        let r = ds + reps(n, Some(v));
        let p = psum(ds);
        let (first, last) = (p + v, p + n * v);
        &&& psum(r) == last
        &&& pmin(r) == if ds.len() == 0 { min_i(first, last) } else { min_i(pmin(ds), min_i(first, last)) }
        &&& pmax(r) == if ds.len() == 0 { max_i(first, last) } else { max_i(pmax(ds), max_i(first, last)) }
    }),
    decreases n,
{
    let p = psum(ds);
    if n == 1 {
        assert(ds + reps(1, Some(v)) =~= ds.push(Some(v)));
        lemma_push(ds, Some(v));
        assert(n * v == v) by (nonlinear_arith)
            requires n == 1;
    } else {
        let m = (n - 1) as nat;
        lemma_run(ds, m, v);
        let r0 = ds + reps(m, Some(v));
        assert(ds + reps(n, Some(v)) =~= r0.push(Some(v)));
        lemma_push(r0, Some(v));
        assert(p + m * v + v == p + n * v) by (nonlinear_arith)
            requires n == m + 1;
        // the run's sums lie between its ends
        assert(min_i(min_i(p + v, p + m * v), p + n * v) == min_i(p + v, p + n * v)
            && max_i(max_i(p + v, p + m * v), p + n * v) == max_i(p + v, p + n * v)) by (nonlinear_arith)
            requires m >= 1, n == m + 1;
    }
}

/// A run of `n` nulls after `ds`: the running sum holds.
proof fn lemma_nulls(ds: Seq<Option<i64>>, n: nat)
    requires n >= 1,
    ensures ({
        let r = ds + reps(n, None);
        &&& psum(r) == psum(ds)
        &&& pmin(r) == if ds.len() == 0 { 0 } else { min_i(pmin(ds), psum(ds)) }
        &&& pmax(r) == if ds.len() == 0 { 0 } else { max_i(pmax(ds), psum(ds)) }
    }),
    decreases n,
{
    if n == 1 {
        assert(ds + reps(1, None) =~= ds.push(None));
        lemma_push(ds, None);
    } else {
        let m = (n - 1) as nat;
        lemma_nulls(ds, m);
        let r0 = ds + reps(m, None);
        assert(ds + reps(n, None) =~= r0.push(None));
        lemma_push(r0, None);
    }
}

/// Fold a run of `count` copies of delta `v` into `w`, the aggregate of
/// `ds`. False, leaving `w` as it was, if a running sum (or the run's
/// span) leaves `i64`: the delta value domain rules those out.
#[inline]
pub(crate) fn agg_run(w: &mut SlabAgg, count: usize, v: Option<i64>, Ghost(ds): Ghost<Seq<Option<i64>>>) -> (ok: bool)
    requires
        agg_of(*old(w), ds),
        count >= 1,
        ds.len() + count <= usize::MAX,
    ensures
        ok ==> agg_of(*final(w), ds + reps(count as nat, v)),
        !ok ==> *final(w) == *old(w),
{
    match v {
        None => {
            proof {
                lemma_nulls(ds, count as nat);
            }
            // a null run holds the running sum where it is
            if w.len == 0 {
                w.min_offset = 0;
                w.max_offset = 0;
            } else {
                w.min_offset = if w.min_offset <= w.total { w.min_offset } else { w.total };
                w.max_offset = if w.max_offset >= w.total { w.max_offset } else { w.total };
            }
            w.len = w.len + count;
            true
        },
        Some(v) => {
            if count > i64::MAX as usize {
                return false;
            }
            let step = match v.checked_mul(count as i64) {
                Some(s) => s,
                None => return false,
            };
            let first = match w.total.checked_add(v) {
                Some(f) => f,
                None => return false,
            };
            let last = match w.total.checked_add(step) {
                Some(l) => l,
                None => return false,
            };
            proof {
                lemma_run(ds, count as nat, v);
                assert(step as int == (count as nat) * (v as int)) by (nonlinear_arith)
                    requires step as int == (v as int) * ((count as i64) as int), (count as i64) as int == count as int;
            }
            let lo = if first <= last { first } else { last };
            let hi = if first >= last { first } else { last };
            if w.len == 0 {
                w.min_offset = lo;
                w.max_offset = hi;
            } else {
                w.min_offset = if w.min_offset <= lo { w.min_offset } else { lo };
                w.max_offset = if w.max_offset >= hi { w.max_offset } else { hi };
            }
            w.len = w.len + count;
            w.total = last;
            true
        },
    }
}

/// [`agg_run`] after the first run: `w` aggregates some deltas already.
#[inline]
pub(crate) fn agg_more(w: &mut SlabAgg, count: usize, v: Option<i64>, Ghost(ds): Ghost<Seq<Option<i64>>>) -> (ok: bool)
    requires
        agg_of(*old(w), ds),
        ds.len() > 0,
        count >= 1,
        ds.len() + count <= usize::MAX,
    ensures
        ok ==> agg_of(*final(w), ds + reps(count as nat, v)),
        !ok ==> *final(w) == *old(w),
{
    match v {
        None => {
            proof {
                lemma_nulls(ds, count as nat);
            }
            // a null run holds the running sum where it is
            w.min_offset = if w.min_offset <= w.total { w.min_offset } else { w.total };
            w.max_offset = if w.max_offset >= w.total { w.max_offset } else { w.total };
            w.len = w.len + count;
            true
        },
        Some(v) => {
            if count > i64::MAX as usize {
                return false;
            }
            let step = match v.checked_mul(count as i64) {
                Some(s) => s,
                None => return false,
            };
            let first = match w.total.checked_add(v) {
                Some(f) => f,
                None => return false,
            };
            let last = match w.total.checked_add(step) {
                Some(l) => l,
                None => return false,
            };
            proof {
                lemma_run(ds, count as nat, v);
                assert(step as int == (count as nat) * (v as int)) by (nonlinear_arith)
                    requires step as int == (v as int) * ((count as i64) as int), (count as i64) as int == count as int;
            }
            let lo = if first <= last { first } else { last };
            let hi = if first >= last { first } else { last };
            w.min_offset = if w.min_offset <= lo { w.min_offset } else { lo };
            w.max_offset = if w.max_offset >= hi { w.max_offset } else { hi };
            w.len = w.len + count;
            w.total = last;
            true
        },
    }
}

/// Aggregates merge as their deltas concatenate, where every sum fits
/// (`merge_ok`): the B-tree's internal aggregates are those of the deltas
/// beneath them.
pub proof fn lemma_merge(a: SlabAgg, da: Seq<Option<i64>>, b: SlabAgg, db: Seq<Option<i64>>)
    requires
        agg_of(a, da), agg_of(b, db),
        SlabAgg::merge_ok(a, b),
    ensures
        agg_of(SlabAgg::spec_merge(a, b), da + db),
{
    if da.len() == 0 {
        assert(da + db =~= db);
    } else if db.len() == 0 {
        assert(da + db =~= da);
    } else {
        lemma_concat(da, db);
    }
}

/// The running sums of a concatenation: the second part's shifted by the
/// first part's total.
proof fn lemma_concat(da: Seq<Option<i64>>, db: Seq<Option<i64>>)
    requires da.len() > 0, db.len() > 0,
    ensures
        psum(da + db) == psum(da) + psum(db),
        pmin(da + db) == min_i(pmin(da), psum(da) + pmin(db)),
        pmax(da + db) == max_i(pmax(da), psum(da) + pmax(db)),
    decreases db.len(),
{
    let d = db.last();
    let db0 = db.drop_last();
    assert(da + db =~= (da + db0).push(d));
    lemma_push(da + db0, d);
    lemma_push(db0, d);
    assert(db0.push(d) =~= db);
    if db0.len() == 0 {
        assert(da + db0 =~= da);
    } else {
        lemma_concat(da, db0);
    }
}

/// The last running sum lies between the least and the greatest.
proof fn lemma_bounds(ds: Seq<Option<i64>>)
    requires ds.len() > 0,
    ensures pmin(ds) <= psum(ds) <= pmax(ds),
{
}

/// The load's walk over its slabs' aggregates, checking every running sum
/// against the value domain `lo..=hi` (`DeltaValue`): the running sum at
/// the next slab's start, and the least and greatest running sums so far
/// (`low > high` before any).
pub(crate) struct DomainWalk {
    pub(crate) running: i128,
    pub(crate) low: i128,
    pub(crate) high: i128,
    /// the deltas walked
    #[allow(dead_code)]
    pub(crate) ds: Ghost<Seq<Option<i64>>>,
}

impl DomainWalk {
    pub(crate) open spec fn inv(&self, lo: i64, hi: i64) -> bool {
        let ds = self.ds@;
        &&& self.running == psum(ds)
        &&& ds.len() == 0 ==> self.low == i128::MAX && self.high == i128::MIN
        &&& ds.len() > 0 ==> self.low == pmin(ds) && self.high == pmax(ds) && lo <= pmin(ds) && pmax(ds) <= hi
    }

    pub(crate) fn new() -> (r: Self)
        ensures r.ds@.len() == 0, forall|lo: i64, hi: i64| r.inv(lo, hi),
    {
        DomainWalk { running: 0, low: i128::MAX, high: i128::MIN, ds: Ghost(Seq::empty()) }
    }

    /// Walk a slab whose aggregate `w` is that of deltas `d`. False if one
    /// of their running sums is outside the domain.
    #[inline]
    pub(crate) fn slab(&mut self, w: &SlabAgg, lo: i64, hi: i64, Ghost(d): Ghost<Seq<Option<i64>>>) -> (ok: bool)
        requires old(self).inv(lo, hi), agg_of(*w, d),
        ensures
            ok ==> final(self).inv(lo, hi) && final(self).ds@ == old(self).ds@ + d,
            !ok ==> d.len() > 0 && !(lo <= psum(old(self).ds@) + pmin(d) && psum(old(self).ds@) + pmax(d) <= hi),
    {
        let ghost ds = self.ds@;
        if w.len == 0 {
            proof {
                assert(ds + d =~= ds);
            }
            return true;
        }
        proof {
            if ds.len() > 0 {
                lemma_bounds(ds);
                lemma_concat(ds, d);
            } else {
                assert(ds + d =~= d);
            }
        }
        let min = self.running + w.min_offset as i128;
        let max = self.running + w.max_offset as i128;
        if min < lo as i128 || max > hi as i128 {
            return false;
        }
        self.low = if self.low <= min { self.low } else { min };
        self.high = if self.high >= max { self.high } else { max };
        self.running = self.running + w.total as i128;
        proof {
            self.ds = Ghost(ds + d);
        }
        true
    }

    /// The running sums span more than `i64` holds: the values may too.
    #[inline]
    pub(crate) fn too_wide(&self, Ghost(lo): Ghost<i64>, Ghost(hi): Ghost<i64>) -> (r: bool)
        requires self.inv(lo, hi),
        ensures r == (self.ds@.len() > 0 && pmax(self.ds@) - pmin(self.ds@) > i64::MAX),
    {
        self.low <= self.high && self.high - self.low > i64::MAX as i128
    }
}

} // verus!
