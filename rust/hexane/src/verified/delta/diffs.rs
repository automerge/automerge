//! The differences a delta column keeps, as a specification: the runs
//! that a column's iterator gives (`VerifiedDeltaIter::next_run`,
//! `VerifiedDeltaIter::runs`) are said in them.
//!
//! A delta column keeps of each number its difference from the last
//! number before it (`delta_prefix`; nulls are passed over), and of a
//! null nothing: `delta_at` is what it keeps at an index and `deltas`
//! all of it. Equal differences that lie together are kept as one
//! run, and a [`DeltaRun`] is such a run with the number before it,
//! so that it says its values alone (`delta_runs_at`).
//!
//! Two lemmas say that the runs so given hold the column's values:
//! `lemma_delta_run_at` of one run, and `lemma_delta_runs_at` of all the
//! runs of a window.

use super::{
    all_dom, all_fit, delta_run_fits, delta_run_kept, delta_run_values, delta_runs_fit,
    delta_runs_kept, delta_runs_values, dvs, in_dom, lemma_kept, vals, VDelta,
};
use crate::delta::agg::psum;
use crate::delta::dedit::rz;
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_crop_eq};
use crate::iter_core::{lemma_window_past, window};
use crate::verified::{head_run, lemma_runs_first, lemma_runs_of, runs_of};
use crate::{DeltaRun, Run};
use vstd::prelude::*;

verus! {

// ── The differences ────────────────────────────────────────────────────

/// The number that a delta column takes the difference at index `i` of
/// values `vs` from: the last number before `i`, as an `i64`, with the
/// nulls passed over; 0 if there is no number before `i`. It is the
/// `prefix` of a run that begins at `i`.
pub open spec fn delta_prefix<T: VDelta>(vs: Seq<T>, i: int) -> i64
    decreases i,
{
    if i <= 0 || i > vs.len() {
        0
    } else {
        match vs[i - 1].dv() {
            Some(x) => x,
            None => delta_prefix(vs, i - 1),
        }
    }
}

/// What a delta column keeps at index `i` of values `vs`: of a number
/// its difference from the last number before it (`delta_prefix`),
/// wrapping as an `i64` does; of a null, none.
pub open spec fn delta_at<T: VDelta>(vs: Seq<T>, i: int) -> Option<i64> {
    match vs[i].dv() {
        Some(x) => Some(vstd::wrapping::i64_specs::signed_crop(x - delta_prefix(vs, i))),
        None => None,
    }
}

/// The differences a delta column keeps of values `vs`: `delta_at` at
/// each index.
pub open spec fn deltas<T: VDelta>(vs: Seq<T>) -> Seq<Option<i64>> {
    Seq::new(vs.len(), |i: int| delta_at(vs, i))
}

/// Runs `rs` of the differences of values `vs`, the first of them at
/// index `a` and each of the others where the one before it ends, as
/// runs of a delta column: each with the number before it
/// (`delta_prefix`).
pub open spec fn delta_runs_at<T: VDelta>(vs: Seq<T>, a: int, rs: Seq<Run<Option<i64>>>) -> Seq<DeltaRun>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        seq![DeltaRun { prefix: delta_prefix(vs, a), delta: rs[0].value, count: rs[0].count }] + delta_runs_at(
            vs,
            a + rs[0].count,
            rs.drop_first(),
        )
    }
}

// ── Sums that wrap ─────────────────────────────────────────────────────

/// A number `x` that is `d` from `c`, wrapping, where `c` is the sum
/// `t` as an `i64` holds it, is the sum `t + d` as an `i64` holds it.
proof fn lemma_crop_step(x: i64, c: i64, d: i64, t: int)
    requires
        crop(x - c) == d,
        c == crop(t),
    ensures
        x == crop(t + d),
{
    lemma_crop_add(x - c, t);
    lemma_crop_add(t, x - c);
    lemma_crop(x as int);
    assert(c + (x - c) == x);
    assert(x - c + t == t + (x - c));
    assert(d + t == t + d);
}

/// The difference of two sums as an `i64` holds them, wrapping, is what
/// was added from the one to the other.
proof fn lemma_crop_diff(p: int, x: i64)
    ensures
        crop(crop(p + x) - crop(p)) == x,
{
    let r = 0x1_0000_0000_0000_0000int;
    let v = crop(p);
    lemma_crop_add(p + x, -v);
    lemma_crop(p);
    vstd::arithmetic::div_mod::lemma_sub_mod_noop(p + x, v as int, r);
    vstd::arithmetic::div_mod::lemma_sub_mod_noop(p + x, p, r);
    assert(p + x - p == x);
    assert((p + x - v) % r == (x as int) % r);
    lemma_crop_eq(p + x - v, x as int);
    lemma_crop(x as int);
    assert(crop(p + x) - v == crop(p + x) + (-v));
    assert(p + x - v == p + x + (-v));
}

// ── What a column keeps, and its differences ───────────────────────────

/// Of the values that sums of differences `ds` are, the differences
/// are `ds`: the number before index `i` is the sum of the differences
/// before it, and the difference at `i` is `ds[i]`.
pub(crate) proof fn lemma_deltas_kept<T: VDelta>(ds: Seq<Option<i64>>, i: int)
    requires
        all_dom::<T>(rz(0, ds)),
        0 <= i <= ds.len(),
    ensures
        delta_prefix(vals::<T>(rz(0, ds)), i) == crop(psum(ds.take(i))),
        i < ds.len() ==> delta_at(vals::<T>(rz(0, ds)), i) == ds[i],
    decreases i,
{
    let kept = rz(0, ds);
    let vs = vals::<T>(kept);
    if i == 0 {
        assert(ds.take(0) =~= Seq::<Option<i64>>::empty());
        lemma_crop(0);
    } else {
        lemma_deltas_kept::<T>(ds, i - 1);
        T::lemma_delta(vs[i - 1], kept[i - 1]);
        assert(in_dom::<T>(kept[i - 1]));
        assert(ds.take(i).drop_last() =~= ds.take(i - 1));
        assert(ds.take(i).last() == ds[i - 1]);
    }
    if i < ds.len() {
        T::lemma_delta(vs[i], kept[i]);
        assert(in_dom::<T>(kept[i]));
        match ds[i] {
            Some(x) => {
                lemma_crop_diff(psum(ds.take(i)), x);
            },
            None => {},
        }
    }
}

/// What a delta column of `T` can keep are values that fit.
pub(crate) proof fn lemma_vals_fit<T: VDelta>(ds: Seq<Option<i64>>)
    requires
        all_dom::<T>(ds),
    ensures
        all_fit(vals::<T>(ds)),
{
    assert forall|i: int| 0 <= i < vals::<T>(ds).len() implies (#[trigger] vals::<T>(ds)[i]).fits() by {
        assert(in_dom::<T>(ds[i]));
        T::lemma_delta(T::of(None), ds[i]);
    }
}

// ── A run of equal differences ─────────────────────────────────────────

/// Where the differences of `vs` at the `n` indices from `a` on are all
/// `d`, the value `k` places after `a` is the number before `a` and
/// `k + 1` times `d` more, as an `i64` holds it.
proof fn lemma_delta_run_kth<T: VDelta>(vs: Seq<T>, a: int, n: int, d: i64, k: int)
    requires
        0 <= a,
        a + n <= vs.len(),
        forall|j: int| a <= j < a + n ==> delta_at(vs, j) == Some(d),
        0 <= k < n,
    ensures
        vs[a + k].dv() == Some(crop(delta_prefix(vs, a) + (k + 1) * d)),
    decreases k,
{
    let p = delta_prefix(vs, a);
    assert(delta_at(vs, a + k) == Some(d));
    let x = vs[a + k].dv()->0;
    if k == 0 {
        lemma_crop(p as int);
        lemma_crop_step(x, p, d, p as int);
        assert((k + 1) * d == d) by (nonlinear_arith)
            requires
                k == 0,
        ;
    } else {
        lemma_delta_run_kth(vs, a, n, d, k - 1);
        let c = delta_prefix(vs, a + k);
        assert((k - 1 + 1) * d == k * d);
        assert(c == crop(p + k * d));
        assert((k + 1) * d == k * d + d) by (nonlinear_arith);
        lemma_crop_step(x, c, d, p + k * d);
    }
}

/// A run of equal differences of values that fit, with the number
/// before it, is a run of a delta column that holds those values: where
/// the differences of `vs` at the `n` indices from `a` on are all `d`,
/// the [`DeltaRun`] of `d`, `n` times, after `delta_prefix(vs, a)` has
/// the values of `vs` there, and fits.
pub proof fn lemma_delta_run_at<T: VDelta>(vs: Seq<T>, a: int, n: usize, d: Option<i64>)
    requires
        all_fit(vs),
        0 <= a,
        a + n <= vs.len(),
        forall|j: int| a <= j < a + n ==> delta_at(vs, j) == d,
    ensures
        delta_run_values::<T>(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n }) == vs.subrange(
            a,
            a + n,
        ),
        delta_run_fits::<T>(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n }),
{
    let run = DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n };
    let sub = vs.subrange(a, a + n);
    assert forall|k: int| 0 <= k < n implies delta_run_kept(run)[k] == #[trigger] dvs(sub)[k] by {
        assert(delta_at(vs, a + k) == d);
        assert(sub[k] == vs[a + k]);
        match d {
            Some(x) => {
                lemma_delta_run_kth(vs, a, n as int, x, k);
            },
            None => {},
        }
    }
    assert(delta_run_kept(run) =~= dvs(sub));
    assert forall|i: int| 0 <= i < sub.len() implies (#[trigger] sub[i]).fits() by {
        assert(sub[i] == vs[a + i]);
    }
    lemma_kept(sub);
}

// ── The runs of a window ───────────────────────────────────────────────

/// A run before runs: its values come before theirs, and where it and
/// they fit so do all together.
proof fn lemma_delta_runs_front<T: VDelta>(r: DeltaRun, rs: Seq<DeltaRun>)
    ensures
        delta_runs_kept(seq![r] + rs) == delta_run_kept(r) + delta_runs_kept(rs),
        delta_runs_values::<T>(seq![r] + rs) == delta_run_values::<T>(r) + delta_runs_values::<T>(rs),
        delta_run_fits::<T>(r) && delta_runs_fit::<T>(rs) ==> delta_runs_fit::<T>(seq![r] + rs),
    decreases rs.len(),
{
    let all = seq![r] + rs;
    let none = Seq::<DeltaRun>::empty();
    if rs.len() == 0 {
        assert(all.drop_last() =~= none);
        assert(all.last() == r);
        assert(delta_runs_kept(none) =~= Seq::<Option<i64>>::empty());
        assert(delta_runs_kept(rs) =~= Seq::<Option<i64>>::empty());
        assert(delta_runs_kept(all) =~= delta_run_kept(r) + delta_runs_kept(rs));
    } else {
        lemma_delta_runs_front::<T>(r, rs.drop_last());
        assert(all.drop_last() =~= seq![r] + rs.drop_last());
        assert(all.last() == rs.last());
        assert(delta_runs_kept(all) =~= delta_run_kept(r) + delta_runs_kept(rs));
    }
    assert(vals::<T>(delta_run_kept(r) + delta_runs_kept(rs)) =~= vals::<T>(delta_run_kept(r)) + vals::<T>(
        delta_runs_kept(rs),
    ));
    if delta_run_fits::<T>(r) && delta_runs_fit::<T>(rs) {
        assert forall|i: int| 0 <= i < all.len() implies delta_run_fits::<T>(#[trigger] all[i]) by {
            if i > 0 {
                assert(all[i] == rs[i - 1]);
            }
        }
    }
}

/// How many values equal to the first `o` begins with.
spec fn first_len<S>(o: Seq<S>) -> int
    decreases o.len(),
{
    if o.len() <= 1 {
        o.len() as int
    } else if o[1] == o[0] {
        1 + first_len(o.drop_first())
    } else {
        1
    }
}

/// Values begin with a run of their first value, if there are any.
proof fn lemma_first_len<S>(o: Seq<S>)
    requires
        o.len() > 0,
    ensures
        head_run(o, o[0], first_len(o)),
    decreases o.len(),
{
    if o.len() > 1 && o[1] == o[0] {
        let d = o.drop_first();
        lemma_first_len(d);
        let n = first_len(d);
        assert(d[0] == o[1]);
        assert forall|i: int| 0 <= i < n + 1 implies #[trigger] o[i] == o[0] by {
            if i > 0 {
                assert(d[i - 1] == o[i]);
            }
        }
        if n + 1 < o.len() {
            assert(d[n] == o[n + 1]);
        }
    }
}

/// The runs of the differences of values that fit, in a window of
/// them, are runs of a delta column that hold the window's values:
/// taken each with the number before it (`delta_runs_at`), the runs of
/// `deltas(vs)` from index `a` to `b` have the values of `vs` from `a`
/// to `b`, and fit.
pub proof fn lemma_delta_runs_at<T: VDelta>(vs: Seq<T>, a: int, b: int)
    requires
        all_fit(vs),
        0 <= a,
        vs.len() <= usize::MAX,
    ensures
        delta_runs_values::<T>(delta_runs_at(vs, a, runs_of(window(deltas(vs), a, b)))) == window(vs, a, b),
        delta_runs_fit::<T>(delta_runs_at(vs, a, runs_of(window(deltas(vs), a, b)))),
    decreases window(vs, a, b).len(),
{
    let ds = deltas(vs);
    let w = window(ds, a, b);
    let rs = runs_of(w);
    assert(w.len() == window(vs, a, b).len());
    lemma_runs_first(w);
    if w.len() == 0 {
        let none = Seq::<DeltaRun>::empty();
        assert(delta_runs_at(vs, a, rs) =~= none);
        assert(delta_runs_kept(none) =~= Seq::<Option<i64>>::empty());
        assert(delta_runs_values::<T>(none) =~= window(vs, a, b));
    } else {
        lemma_first_len(w);
        let n = first_len(w);
        lemma_runs_of(w, w[0], n as usize);
        lemma_window_past(ds, a, b, n);
        lemma_window_past(vs, a, b, n);
        lemma_delta_runs_at::<T>(vs, a + n, b);
        let r = DeltaRun { prefix: delta_prefix(vs, a), delta: w[0], count: n as usize };
        let rest = delta_runs_at(vs, a + n, runs_of(window(ds, a + n, b)));
        assert(rs.drop_first() =~= runs_of(w.skip(n)));
        assert(delta_runs_at(vs, a, rs) == seq![r] + rest);
        assert forall|j: int| a <= j < a + n implies delta_at(vs, j) == w[0] by {
            assert(w[j - a] == ds[j]);
            assert(w[j - a] == w[0]);
        }
        lemma_delta_run_at::<T>(vs, a, n as usize, w[0]);
        lemma_delta_runs_front::<T>(r, rest);
        assert(vs.subrange(a, a + n) + window(vs, a + n, b) =~= window(vs, a, b));
    }
}

} // verus!
