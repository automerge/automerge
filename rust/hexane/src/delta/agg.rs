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
use crate::btree::{in_i64, max_i, min_i, SlabAggregate};
use crate::column::Slab;
use crate::iter_core::{Decode, Decoders};
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

pub(crate) proof fn lemma_push(ds: Seq<Option<i64>>, d: Option<i64>)
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

/// A run of `count` copies of delta `v` after deltas `ds` keeps its
/// running sums, and its step `count * v`, in `i64`.
pub open spec fn run_fits(ds: Seq<Option<i64>>, count: nat, v: Option<i64>) -> bool {
    match v {
        Some(v) => {
            &&& count <= i64::MAX
            &&& in_i64(count * v)
            &&& in_i64(psum(ds) + v)
            &&& in_i64(psum(ds) + count * v)
        },
        None => true,
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
        !ok ==> *final(w) == *old(w) && !run_fits(ds, count as nat, v),
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
            if count as u64 > i64::MAX as u64 {
                return false;
            }
            let step = match v.checked_mul(count as i64) {
                Some(s) => s,
                None => {
                    proof {
                        assert(!in_i64((count as nat) * (v as int))) by (nonlinear_arith)
                            requires !in_i64((v as int) * ((count as i64) as int)), (count as i64) as int == count as int;
                    }
                    return false;
                },
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
        !ok ==> *final(w) == *old(w) && !run_fits(ds, count as nat, v),
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
            if count as u64 > i64::MAX as u64 {
                return false;
            }
            let step = match v.checked_mul(count as i64) {
                Some(s) => s,
                None => {
                    proof {
                        assert(!in_i64((count as nat) * (v as int))) by (nonlinear_arith)
                            requires !in_i64((v as int) * ((count as i64) as int)), (count as i64) as int == count as int;
                    }
                    return false;
                },
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

/// The least running sum at a value (a non-null delta) of `ds`; above any
/// sum if there is none.
pub open spec fn vmin(ds: Seq<Option<i64>>) -> int
    decreases ds.len(),
{
    if ds.len() == 0 {
        i128::MAX as int
    } else {
        match ds.last() {
            Some(_) => min_i(vmin(ds.drop_last()), psum(ds)),
            None => vmin(ds.drop_last()),
        }
    }
}

/// The greatest running sum at a value of `ds`; below any if there is none.
pub open spec fn vmax(ds: Seq<Option<i64>>) -> int
    decreases ds.len(),
{
    if ds.len() == 0 {
        i128::MIN as int
    } else {
        match ds.last() {
            Some(_) => max_i(vmax(ds.drop_last()), psum(ds)),
            None => vmax(ds.drop_last()),
        }
    }
}

/// Every running sum of `ds` fits an `i64`.
pub open spec fn sums_fit(ds: Seq<Option<i64>>) -> bool {
    ds.len() == 0 || (i64::MIN <= pmin(ds) && pmax(ds) <= i64::MAX)
}

/// A run after `ds`: its values' sums move one way, so its ends bound
/// them; a null run adds no value.
proof fn lemma_vrun(ds: Seq<Option<i64>>, n: nat, d: Option<i64>)
    requires n >= 1,
    ensures ({
        let r = ds + reps(n, d);
        let p = psum(ds);
        match d {
            Some(v) => {
                let (first, last) = (p + v, p + n * v);
                &&& vmin(r) == min_i(vmin(ds), min_i(first, last))
                &&& vmax(r) == max_i(vmax(ds), max_i(first, last))
            },
            None => vmin(r) == vmin(ds) && vmax(r) == vmax(ds),
        }
    }),
    decreases n,
{
    let r = ds + reps(n, d);
    let m = (n - 1) as nat;
    let r0 = ds + reps(m, d);
    assert(r =~= r0.push(d));
    assert(r.drop_last() =~= r0);
    if m > 0 {
        lemma_vrun(ds, m, d);
        lemma_run_sum(ds, m, d);
    } else {
        assert(r0 =~= ds);
    }
    lemma_push(r0, d);
    if let Some(v) = d {
        let p = psum(ds);
        if m == 0 {
            assert(n * v == v) by (nonlinear_arith)
                requires n == 1;
        } else {
            assert(p + m * v + v == p + n * v) by (nonlinear_arith)
                requires n == m + 1;
            assert(min_i(min_i(p + v, p + m * v), p + n * v) == min_i(p + v, p + n * v)
                && max_i(max_i(p + v, p + m * v), p + n * v) == max_i(p + v, p + n * v)) by (nonlinear_arith)
                requires m >= 1, n == m + 1;
        }
    }
}

/// The sums at values are among all the running sums.
proof fn lemma_vbounds(ds: Seq<Option<i64>>)
    requires sums_fit(ds),
    ensures
        vmin(ds) <= vmax(ds) ==> ds.len() > 0 && pmin(ds) <= vmin(ds) && vmax(ds) <= pmax(ds),
        vmin(ds) > vmax(ds) ==> vmin(ds) == i128::MAX && vmax(ds) == i128::MIN,
    decreases ds.len(),
{
    if ds.len() > 0 {
        let d0 = ds.drop_last();
        let x = ds.last();
        assert(d0.push(x) =~= ds);
        lemma_push(d0, x);
        lemma_bounds(ds);
        if d0.len() > 0 {
            lemma_bounds(d0);
        }
        assert(sums_fit(d0));
        lemma_vbounds(d0);
        if d0.len() == 0 {
            assert(vmin(d0) == i128::MAX && vmax(d0) == i128::MIN);
        }
        match x {
            Some(_) => {
                assert(vmin(ds) == min_i(vmin(d0), psum(ds)));
                assert(vmax(ds) == max_i(vmax(d0), psum(ds)));
            },
            None => {
                assert(vmin(ds) == vmin(d0) && vmax(ds) == vmax(d0));
            },
        }
    }
}

/// The running sum after a run.
pub(crate) proof fn lemma_run_sum(ds: Seq<Option<i64>>, n: nat, d: Option<i64>)
    ensures psum(ds + reps(n, d)) == psum(ds) + n * dval(d),
    decreases n,
{
    if n == 0 {
        assert(ds + reps(0, d) =~= ds);
    } else {
        let m = (n - 1) as nat;
        lemma_run_sum(ds, m, d);
        assert(ds + reps(n, d) =~= (ds + reps(m, d)).push(d));
        lemma_push(ds + reps(m, d), d);
        assert(m * dval(d) + dval(d) == n * dval(d)) by (nonlinear_arith)
            requires n == m + 1;
    }
}


/// Every running sum lies between the least value (or 0) and the
/// greatest (or 0): a null holds the running sum before it, which is a
/// value or, before any, 0.
pub(crate) proof fn lemma_sums_within_values(ds: Seq<Option<i64>>)
    ensures
        min_i(vmin(ds), 0) <= psum(ds) <= max_i(vmax(ds), 0),
        ds.len() > 0 ==> min_i(vmin(ds), 0) <= pmin(ds) && pmax(ds) <= max_i(vmax(ds), 0),
    decreases ds.len(),
{
    if ds.len() > 0 {
        let d0 = ds.drop_last();
        let x = ds.last();
        assert(d0.push(x) =~= ds);
        lemma_push(d0, x);
        lemma_sums_within_values(d0);
    }
}

/// The least value is at most the greatest, unless there are none.
proof fn lemma_vmin_vmax(ds: Seq<Option<i64>>)
    requires ds.len() <= usize::MAX,
    ensures
        vmin(ds) <= vmax(ds) || (vmin(ds) == i128::MAX && vmax(ds) == i128::MIN),
    decreases ds.len(),
{
    if ds.len() > 0 {
        let d0 = ds.drop_last();
        assert(d0.push(ds.last()) =~= ds);
        lemma_vmin_vmax(d0);
        lemma_psum_size(ds);
        assert(0x8000_0000_0000_0000 * ds.len() <= 0x8000_0000_0000_0000 * 0xffff_ffff_ffff_ffffint)
            by (nonlinear_arith) requires ds.len() <= 0xffff_ffff_ffff_ffff;
    }
}

/// A running sum after `ds` is at most `2^63` per delta from 0.
proof fn lemma_psum_size(ds: Seq<Option<i64>>)
    ensures
        -0x8000_0000_0000_0000 * ds.len() <= psum(ds) <= 0x8000_0000_0000_0000 * ds.len(),
    decreases ds.len(),
{
    if ds.len() > 0 {
        let d0 = ds.drop_last();
        assert(d0.push(ds.last()) =~= ds);
        lemma_push(d0, ds.last());
        lemma_psum_size(d0);
        assert(-0x8000_0000_0000_0000 * d0.len() - 0x8000_0000_0000_0000 == -0x8000_0000_0000_0000 * ds.len()
            && 0x8000_0000_0000_0000 * d0.len() + 0x8000_0000_0000_0000 == 0x8000_0000_0000_0000 * ds.len())
            by (nonlinear_arith)
            requires ds.len() == d0.len() + 1;
    }
}

/// The least and greatest running sums at the values of any deltas
/// (`check_span`'s walk over a column after an edit, whose deltas may
/// wrap): in `i128`, which holds any running sum of a column's deltas.
pub(crate) struct AnySpan {
    pub(crate) running: i128,
    pub(crate) low: i128,
    pub(crate) high: i128,
    /// the deltas walked
    #[allow(dead_code)]
    pub(crate) ds: Ghost<Seq<Option<i64>>>,
}

impl AnySpan {
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.running == psum(self.ds@)
        &&& self.low == vmin(self.ds@)
        &&& self.high == vmax(self.ds@)
        &&& self.ds@.len() <= usize::MAX
    }

    pub(crate) fn new() -> (r: Self)
        ensures r.inv(), r.ds@.len() == 0,
    {
        AnySpan { running: 0, low: i128::MAX, high: i128::MIN, ds: Ghost(Seq::empty()) }
    }

    /// Take a run of `count` copies of delta `d`; the deltas walked are a
    /// column's, so there are fewer than `usize::MAX`.
    #[inline]
    pub(crate) fn run(&mut self, count: usize, d: Option<i64>)
        requires
            old(self).inv(), count >= 1,
            old(self).ds@.len() + count <= usize::MAX,
        ensures
            final(self).inv(), final(self).ds@ == old(self).ds@ + reps(count as nat, d),
    {
        let ghost ds = self.ds@;
        proof {
            lemma_vrun(ds, count as nat, d);
            lemma_run_sum(ds, count as nat, d);
            lemma_psum_size(ds);
        }
        if let Some(v) = d {
            proof {
                // |running| <= 2^63·len and |v·count| <= 2^63·count, and
                // len + count < 2^64: the sums fit `i128`
                let b = 0x8000_0000_0000_0000int;
                let n = ds.len() as int;
                let c = count as int;
                assert(-b * c <= v * c <= b * c) by (nonlinear_arith)
                    requires -b <= v < b, c >= 1;
                assert(b * n + b * c <= b * 0xffff_ffff_ffff_ffff) by (nonlinear_arith)
                    requires n + c <= 0xffff_ffff_ffff_ffff, b > 0, n >= 0, c >= 0;
                assert(b * n + b <= b * n + b * c) by (nonlinear_arith)
                    requires c >= 1, b > 0;
                assert(v as int * count as int == count as nat * v as int) by (nonlinear_arith);
            }
            let first = self.running + v as i128;
            let last = self.running + v as i128 * count as i128;
            let lo = if first <= last { first } else { last };
            let hi = if first >= last { first } else { last };
            self.low = if self.low <= lo { self.low } else { lo };
            self.high = if self.high >= hi { self.high } else { hi };
            self.running = last;
        }
        proof {
            self.ds = Ghost(ds + reps(count as nat, d));
        }
    }

    /// Whether the values lie in the domain `min..=max` (within `i64`),
    /// within a range less than 2^63 wide; and then every running sum
    /// fits `i64` too, as the index's sums need.
    #[inline]
    pub(crate) fn fits(&self, min: i64, max: i64) -> (r: bool)
        requires self.inv(),
        ensures
            r == (vmin(self.ds@) > vmax(self.ds@) || (vmax(self.ds@) - vmin(self.ds@) <= i64::MAX
                && min <= vmin(self.ds@) && vmax(self.ds@) <= max)),
            r ==> sums_fit(self.ds@),
    {
        proof {
            lemma_sums_within_values(self.ds@);
            lemma_vmin_vmax(self.ds@);
        }
        // the domain first: the difference of values that lie in it fits
        self.low > self.high
            || (self.low >= min as i128
                && self.high <= max as i128
                && self.high - self.low <= i64::MAX as i128)
    }
}

/// A delta column's values as deltas: the trusted bridge to
/// `DeltaInner::to_opt` (user code: an `i64` is its own delta, an
/// `Option<i64>` too, a null holding the running sum).
pub(crate) trait DeltaOps<V: Copy> {
    spec fn dv(v: V) -> Option<i64>;

    fn to_opt(v: V) -> (r: Option<i64>)
        ensures
            r == Self::dv(v),
    ;
}

/// The deltas of values `vs`.
pub(crate) open spec fn deltas_of<V: Copy, B: DeltaOps<V>>(vs: Seq<V>) -> Seq<Option<i64>> {
    vs.map_values(|v: V| B::dv(v))
}

/// The running sum after deltas `j` less that after deltas `i`.
pub open spec fn pdiff(ds: Seq<Option<i64>>, i: int, j: int) -> int {
    psum(ds.take(j)) - psum(ds.take(i))
}

/// Every running sum of deltas `ds`, and every difference of two (a
/// run's step among them), fits `i64`: true of a slab of a column whose
/// values lie in the domain, within a range less than 2^63 wide.
pub open spec fn steps_fit(ds: Seq<Option<i64>>) -> bool {
    &&& ds.len() <= i64::MAX
    &&& forall|i: int, j: int| 0 <= i <= j <= ds.len() ==> in_i64(#[trigger] pdiff(ds, i, j))
}

/// Where a slab's steps fit, so does each of its runs (`agg_run`'s test).
proof fn lemma_run_in_steps(ds: Seq<Option<i64>>, k: int, c: nat, x: Option<i64>)
    requires
        steps_fit(ds),
        0 <= k,
        k + c <= ds.len(),
        c >= 1,
        ds.take(k + c) == ds.take(k) + reps(c, x),
    ensures
        run_fits(ds.take(k), c, x),
{
    lemma_run_sum(ds.take(k), c, x);
    lemma_run_sum(ds.take(k), 1, x);
    assert(ds.take(k + 1) =~= ds.take(k) + reps(1, x)) by {
        assert(ds.take(k + 1) =~= ds.take(k + c).take(k + 1));
        assert((ds.take(k) + reps(c, x)).take(k + 1) =~= ds.take(k) + reps(1, x));
    }
    assert(ds.take(0) =~= Seq::<Option<i64>>::empty());
    assert(in_i64(pdiff(ds, k, k + c)));
    assert(in_i64(pdiff(ds, 0, k + 1)));
    assert(in_i64(pdiff(ds, 0, k + c)));
    if let Some(v) = x {
        assert(1 * dval(x) == v);
    }
}

/// The item count of a slab whose sums leave `i64` (which only an edit
/// still under way weighs, as the next one weighs it again): its
/// aggregate's other fields then mean nothing, and are taken wrapping.
// a Verus loop with an invariant: `loop` + `match`
#[allow(clippy::while_let_loop)]
#[inline(never)]
#[cold]
fn slab_agg_wrapping<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
) -> (w: SlabAgg)
    requires
        X::ok(*slab),
        X::vals(*slab).len() <= usize::MAX,
    ensures
        w.len == X::vals(*slab).len(),
{
    let ghost v = X::vals(*slab);
    let mut d = X::open(slab);
    let mut w = SlabAgg { len: 0, total: 0, min_offset: 0, max_offset: 0 };
    proof {
        assert(v.skip(0) =~= v);
    }
    loop
        invariant
            v == X::vals(*slab),
            v.len() <= usize::MAX,
            d.inv(),
            w.len <= v.len(),
            d.rest() == v.skip(w.len as int),
        ensures
            w.len == v.len(),
        decreases d.rest().len(),
    {
        let ghost k = w.len as int;
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                let c = run.count as i64;
                if let Some(x) = B::to_opt(run.value) {
                    let first = w.total.wrapping_add(x);
                    let last = w.total.wrapping_add(x.wrapping_mul(c));
                    let lo = if first <= last { first } else { last };
                    let hi = if first >= last { first } else { last };
                    if w.len == 0 {
                        w.min_offset = lo;
                        w.max_offset = hi;
                    } else {
                        w.min_offset = if w.min_offset <= lo { w.min_offset } else { lo };
                        w.max_offset = if w.max_offset >= hi { w.max_offset } else { hi };
                    }
                    w.total = last;
                } else if w.len == 0 {
                    w.min_offset = 0;
                    w.max_offset = 0;
                }
                w.len = w.len + run.count;
                proof {
                    assert(v.skip(k).skip(run.count as int) =~= v.skip(k + run.count));
                }
            },
            None => break,
        }
    }
    w
}

/// Values `v` from `k` on starting with `c` copies of `x`: their deltas
/// through `k + c` are those through `k`, then `c` copies of `x`'s.
pub(crate) proof fn lemma_take_run<V: Copy, B: DeltaOps<V>>(v: Seq<V>, k: int, c: int, x: V)
    requires
        0 <= k,
        0 <= c,
        k + c <= v.len(),
        v.skip(k).take(c) == Seq::new(c as nat, |_i: int| x),
    ensures
        deltas_of::<_, B>(v.take(k + c)) == deltas_of::<_, B>(v.take(k)) + reps(c as nat, B::dv(x)),
        v.skip(k).skip(c) == v.skip(k + c),
{
    assert(v.take(k + c) =~= v.take(k) + Seq::new(c as nat, |_i: int| x)) by {
        assert forall|i: int| 0 <= i < c implies #[trigger] v[k + i] == x by {
            assert(v.skip(k).take(c)[i] == x);
        }
    }
    assert(deltas_of::<_, B>(v.take(k + c)) =~= deltas_of::<_, B>(v.take(k)) + reps(c as nat, B::dv(x)));
    assert(v.skip(k).skip(c) =~= v.skip(k + c));
}

/// A slab's aggregate, its runs folded in order (`agg_run`, then
/// `agg_more`): its weight in the index.
pub(crate) fn slab_agg<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
) -> (w: SlabAgg)
    requires
        X::ok(*slab),
        X::vals(*slab).len() <= usize::MAX,
    ensures
        w.len == X::vals(*slab).len(),
        steps_fit(deltas_of::<_, B>(X::vals(*slab))) ==> agg_of(w, deltas_of::<_, B>(X::vals(*slab))),
{
    let ghost v = X::vals(*slab);
    let ghost all = deltas_of::<_, B>(v);
    let mut d = X::open(slab);
    let mut w = SlabAgg { len: 0, total: 0, min_offset: 0, max_offset: 0 };
    proof {
        assert(deltas_of::<_, B>(v.take(0)) =~= Seq::<Option<i64>>::empty());
        assert(v.skip(0) =~= v);
    }
    let first = match d.d_run_max(usize::MAX) {
        Some(run) => run,
        None => {
            proof {
                assert(v =~= Seq::<<X::D as Decode>::Item>::empty());
            }
            return w;
        },
    };
    let ghost e = Seq::<Option<i64>>::empty();
    let dv = B::to_opt(first.value);
    proof {
        lemma_take_run::<_, B>(v, 0, first.count as int, first.value);
        assert(all.take(first.count as int) =~= deltas_of::<_, B>(v.take(first.count as int)));
        assert(all.take(0) =~= e);
        if steps_fit(all) {
            lemma_run_in_steps(all, 0, first.count as nat, dv);
        }
    }
    if !agg_run(&mut w, first.count, dv, Ghost(e)) {
        // sums that leave `i64`: an edit's slab, for a moment
        return slab_agg_wrapping::<X, B>(slab);
    }
    let ghost mut k: int = first.count as int;
    loop
        invariant
            v == X::vals(*slab),
            v.len() <= usize::MAX,
            d.inv(),
            X::ok(*slab),
            0 < k <= v.len(),
            d.rest() == v.skip(k),
            agg_of(w, deltas_of::<_, B>(v.take(k))),
            all == deltas_of::<_, B>(v),
        ensures
            agg_of(w, deltas_of::<_, B>(v)),
        decreases d.rest().len(),
    {
        let ghost ds = deltas_of::<_, B>(v.take(k));
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                let dv = B::to_opt(run.value);
                proof {
                    lemma_take_run::<_, B>(v, k, run.count as int, run.value);
                    assert(all.take(k + run.count) =~= deltas_of::<_, B>(v.take(k + run.count)));
                    assert(all.take(k) =~= ds);
                    if steps_fit(all) {
                        lemma_run_in_steps(all, k, run.count as nat, dv);
                    }
                }
                if !agg_more(&mut w, run.count, dv, Ghost(ds)) {
                    // sums that leave `i64`: an edit's slab, for a moment
                    return slab_agg_wrapping::<X, B>(slab);
                }
                proof {
                    k = k + run.count;
                }
            },
            None => {
                proof {
                    assert(v.take(k) =~= v);
                }
                break;
            },
        }
    }
    w
}

} // verus!
