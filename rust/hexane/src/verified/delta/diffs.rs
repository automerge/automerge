//! The differences a delta column keeps, as a specification: the runs
//! that a column's iterator gives (`DeltaIter::next_run`,
//! `DeltaIter::runs`) are said in them.
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
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`diffs/imp.rs`), which has
//! its contract word for word and is proved there.

use super::{
    all_dom, all_fit, delta_run_fits, delta_run_kept, delta_run_values, delta_runs_fit,
    delta_runs_kept, delta_runs_values, dvs, in_dom, lemma_kept, vals, VDelta,
};
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

mod imp;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{
    lemma_delta_head, lemma_delta_runs_units, lemma_delta_take, lemma_deltas_kept, lemma_vals_fit,
};

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
    imp::lemma_delta_run_at_impl::<T>(vs, a, n, d)
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
    imp::lemma_delta_runs_at_impl::<T>(vs, a, b)
}

// ── The runs of a delta column, value by value ─────────────────────────────

/// Each value of `vs` as a delta run of one: the number before it and
/// its difference.
pub open spec fn delta_units<T: VDelta>(vs: Seq<T>) -> Seq<DeltaRun> {
    Seq::new(vs.len(), |p: int| DeltaRun { prefix: delta_prefix(vs, p), delta: delta_at(vs, p), count: 1 })
}

/// The number before the value at `i` of the delta run `r`: the one
/// before the run, moved on by the difference once for each value of the
/// run before it (wrapping, as an `i64` does); a run of nulls does not
/// move it.
pub open spec fn delta_run_prefix(r: DeltaRun, i: int) -> i64 {
    if i == 0 || r.delta is None {
        r.prefix
    } else {
        vstd::wrapping::i64_specs::signed_crop(r.prefix + i * r.delta->Some_0)
    }
}

/// The values the delta run `r` stands for, each a delta run of one.
pub open spec fn delta_run_units(r: DeltaRun) -> Seq<DeltaRun> {
    Seq::new(r.count as nat, |i: int| DeltaRun { prefix: delta_run_prefix(r, i), delta: r.delta, count: 1 })
}

/// What each of the delta runs `rs` stands for.
pub open spec fn delta_units_of_runs(rs: Seq<DeltaRun>) -> Seq<Seq<DeltaRun>> {
    rs.map_values(|r: DeltaRun| delta_run_units(r))
}

} // verus!
