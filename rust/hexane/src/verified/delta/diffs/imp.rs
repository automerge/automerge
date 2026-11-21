//! The implementation of `verified::delta::diffs` and its proofs: what its
//! specification (`diffs.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;
use crate::cursor::lemma_concat_append;
use crate::delta::agg::psum;
use crate::delta::dedit::rz;
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_crop_eq};
use crate::iter_core::{lemma_window_past, window};
use crate::rle::writer::concat;
use crate::verified::shift::{lemma_concat_one, run_units, unit_runs, units_of_runs};
use crate::verified::{head_run, lemma_runs_first, lemma_runs_of, runs_of};
use crate::{DeltaRun, Run};
use vstd::prelude::*;

verus! {

// ── Sums that wrap ─────────────────────────────────────────────────────

/// A number `x` that is `d` from `c`, wrapping, where `c` is the sum
/// `t` as an `i64` holds it, is the sum `t + d` as an `i64` holds it.
pub(super) proof fn lemma_crop_step(x: i64, c: i64, d: i64, t: int)
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
pub(super) proof fn lemma_crop_diff(p: int, x: i64)
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
pub(super) proof fn lemma_delta_run_kth<T: VDelta>(vs: Seq<T>, a: int, n: int, d: i64, k: int)
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

pub(super) proof fn lemma_delta_run_at_impl<T: VDelta>(vs: Seq<T>, a: int, n: usize, d: Option<i64>)
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
pub(super) proof fn lemma_delta_runs_front<T: VDelta>(r: DeltaRun, rs: Seq<DeltaRun>)
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
pub(super) open spec fn first_len<S>(o: Seq<S>) -> int
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
pub(super) proof fn lemma_first_len<S>(o: Seq<S>)
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

pub(super) proof fn lemma_delta_runs_at_impl<T: VDelta>(vs: Seq<T>, a: int, b: int)
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
        lemma_delta_runs_at_impl::<T>(vs, a + n, b);
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

/// The number before index `a + k`, in a run of equal differences `d`
/// that starts at `a`: the one before the run, moved on `k` times.
pub(super) proof fn lemma_delta_prefix_in_run<T: VDelta>(vs: Seq<T>, a: int, n: int, d: Option<i64>, k: int)
    requires
        0 <= a,
        a + n <= vs.len(),
        forall|j: int| a <= j < a + n ==> delta_at(vs, j) == d,
        0 <= k < n,
    ensures
        delta_prefix(vs, a + k) == delta_run_prefix(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: 0 }, k),
    decreases k,
{
    match d {
        Some(x) => {
            if k > 0 {
                lemma_delta_run_kth(vs, a, n, x, k - 1);
                assert(delta_at(vs, a + k - 1) == d);
            }
        },
        None => {
            if k > 0 {
                lemma_delta_prefix_in_run(vs, a, n, d, k - 1);
                assert(delta_at(vs, a + k - 1) == d);
            }
        },
    }
}

/// A run of equal differences `d` at `a`, with the number before it,
/// stands for the values there.
pub(crate) proof fn lemma_delta_head<T: VDelta>(vs: Seq<T>, a: int, n: usize, d: Option<i64>)
    requires
        0 <= a,
        a + n <= vs.len(),
        forall|j: int| a <= j < a + n ==> delta_at(vs, j) == d,
    ensures
        delta_run_units(DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n }) == delta_units(vs).subrange(
            a,
            a + n,
        ),
{
    let r = DeltaRun { prefix: delta_prefix(vs, a), delta: d, count: n };
    assert forall|k: int| 0 <= k < n implies #[trigger] delta_run_units(r)[k] == delta_units(vs)[a + k] by {
        lemma_delta_prefix_in_run(vs, a, n as int, d, k);
    }
}

/// Runs `rs` of the differences of `vs` from `a` up to `b`, as runs of a
/// delta column, stand for the values there, each as a delta run of one.
pub(crate) proof fn lemma_delta_runs_units<T: VDelta>(vs: Seq<T>, a: int, b: int, rs: Seq<Run<Option<i64>>>)
    requires
        0 <= a <= b <= vs.len(),
        concat(units_of_runs(rs)) == unit_runs(deltas(vs).subrange(a, b)),
    ensures
        concat(delta_units_of_runs(delta_runs_at(vs, a, rs))) == delta_units(vs).subrange(a, b),
        delta_runs_at(vs, a, rs).len() == rs.len(),
        forall|k: int|
            0 <= k < rs.len() ==> (#[trigger] delta_runs_at(vs, a, rs)[k]).count == rs[k].count,
    decreases rs.len(),
{
    if rs.len() == 0 {
        assert(delta_units_of_runs(delta_runs_at(vs, a, rs)) =~= Seq::<Seq<DeltaRun>>::empty());
        assert(delta_units(vs).subrange(a, b) =~= Seq::<DeltaRun>::empty());
    } else {
        let c = rs[0].count as int;
        let rest = units_of_runs(rs.drop_first());
        assert(units_of_runs(rs) =~= seq![run_units(rs[0])] + rest);
        lemma_concat_append(seq![run_units(rs[0])], rest);
        lemma_concat_one(run_units(rs[0]));
        let w = deltas(vs).subrange(a, b);
        assert(unit_runs(w).skip(c) =~= concat(rest));
        assert forall|j: int| a <= j < a + c implies delta_at(vs, j) == rs[0].value by {
            assert(unit_runs(w).take(c)[j - a] == run_units(rs[0])[j - a]);
        }
        assert(unit_runs(deltas(vs).subrange(a + c, b)) =~= unit_runs(w).skip(c));
        lemma_delta_runs_units(vs, a + c, b, rs.drop_first());
        lemma_delta_head(vs, a, rs[0].count, rs[0].value);
        let h = DeltaRun { prefix: delta_prefix(vs, a), delta: rs[0].value, count: rs[0].count };
        let drest = delta_units_of_runs(delta_runs_at(vs, a + c, rs.drop_first()));
        assert(delta_units_of_runs(seq![h] + delta_runs_at(vs, a + c, rs.drop_first())) =~= seq![
            delta_run_units(h),
        ] + drest);
        lemma_concat_append(seq![delta_run_units(h)], drest);
        lemma_concat_one(delta_run_units(h));
        assert forall|k: int| 0 <= k < rs.len() implies (#[trigger] delta_runs_at(vs, a, rs)[k]).count
            == rs[k].count by {}
    }
}

/// The run at the head of the `l` differences of `vs` from `a`, with the
/// number before it: the first of their delta runs, which stands for the
/// first of the values there as delta runs of one; and what is left
/// after it.
pub(crate) proof fn lemma_delta_take<T: VDelta>(vs: Seq<T>, a: int, l: int, x: DeltaRun)
    requires
        0 <= a,
        0 <= l <= usize::MAX,
        a + l <= vs.len(),
        head_run(deltas(vs).subrange(a, a + l), x.delta, x.count as int),
        x.prefix == delta_prefix(vs, a),
    ensures
        delta_runs_at(vs, a, runs_of(window(deltas(vs), a, a + l))) == seq![x] + delta_runs_at(
            vs,
            a + x.count,
            runs_of(window(deltas(vs), a + x.count, a + l)),
        ),
        delta_run_units(x) == window(delta_units(vs), a, a + l).take(x.count as int),
        window(delta_units(vs), a + x.count, a + l) == window(delta_units(vs), a, a + l).skip(x.count as int),
{
    let o = deltas(vs).subrange(a, a + l);
    let n = x.count as int;
    assert forall|j: int| a <= j < a + n implies delta_at(vs, j) == x.delta by {
        assert(o[j - a] == x.delta);
    }
    lemma_runs_of(o, x.delta, x.count);
    lemma_delta_head(vs, a, x.count, x.delta);
    assert(o.skip(n) =~= window(deltas(vs), a + n, a + l));
    assert(runs_of(o).drop_first() =~= runs_of(o.skip(n)));
}

} // verus!
