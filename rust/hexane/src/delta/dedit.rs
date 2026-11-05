//! The deltas a delta column's edits write, verified with Verus.
//!
//! A delta column stores the differences between consecutive non-null
//! values; the value at an item is the running sum of the deltas up to
//! it. An edit replaces some deltas, and the ones it writes have to do
//! two things: realize the values asked for, and leave every value after
//! the edit as it was, though those values are stored relative to what
//! stands before them. [`DeltaColumn::splice`](crate::DeltaColumn::splice)
//! does the second by rewriting the first non-null delta after the edit
//! (the boundary) to reach the value it held; a [`DeltaEdit`](crate::delta::DeltaEdit)
//! cursor keeps a [`Ledger`] instead: the value standing before it in the
//! column being written, and what the next original delta owes, which it
//! pays when it moves past that delta.
//!
//! The model: `rz` is the values deltas realize after a running sum,
//! with `i64` sums wrapping, as the iterators read them (`diter`).
//!
//! Proved: [`delta_to`] and [`anchor_run`] give the deltas that realize a
//! value and a run of values after any running sum; each [`Ledger`] step
//! writes deltas realizing exactly the values asked for (an insert), the
//! original values (an item carried past, or paid), or nothing (a
//! delete), and keeps `running` the running sum of what was written and
//! `running + adjust` that of the original deltas passed; so once the
//! debt is paid, every original value after the cursor is realized as it
//! was (`lemma_rest_unchanged`). For `splice`: [`d_boundaries`] finds,
//! with the verified iterator, the running sum before the edit, the nulls
//! after it and the value of the item after those; and
//! `lemma_splice_keeps_rest`: the boundary delta reaching that value
//! leaves every value after it as it was; and [`SplicePlan`] emits, a run
//! at a time, deltas realizing exactly the items' values, then the nulls
//! after the deleted items and the value after them. Trusted: the glue
//! that drives these (that `splice` feeds the plan its items), that what the
//! cursor reports carrying, deleting and jumping over is what it did, the
//! running sum `prefix_at` reads from the index, and (as for reading,
//! `diter`) that the index holds each slab's aggregate.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::deltas_of;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{dval, lemma_push, lemma_run_sum, psum, reps};
use crate::delta::diter::add_run;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{at_run, dbefore, dindex_ok};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{
    crop, lemma_crop, lemma_crop_add, lemma_psum_cat, lemma_wrapping_add, realize,
};
use crate::delta::diter::{d_next_run, d_nth};
#[cfg(verus_keep_ghost)]
use crate::iter_core::vals_of;
use crate::iter_core::{Decode, Decoders, ICore};
use vstd::prelude::*;

verus! {

/// The values deltas `ds` realize after the running sum `p`: at each
/// non-null delta the running sum through it (wrapping), and nulls as they
/// are.
pub(crate) open spec fn rz(p: int, ds: Seq<Option<i64>>) -> Seq<Option<i64>> {
    Seq::new(ds.len(), |i: int| realize(p + psum(ds.take(i)), ds[i]))
}

/// The values of a run of `n` equal deltas `d` after the running sum `p`:
/// `p + d`, `p + 2d`, ….
pub(crate) open spec fn run_vals(p: int, d: i64, n: nat) -> Seq<Option<i64>> {
    Seq::new(n, |k: int| Some(crop(p + (k + 1) * d)))
}

/// A single delta adds itself.
pub(crate) proof fn lemma_psum_one(x: i64)
    ensures
        psum(seq![Some(x)]) == x,
        seq![Some(x)].take(0) == Seq::<Option<i64>>::empty(),
{
    reveal_with_fuel(psum, 2);
    assert(seq![Some(x)].drop_last() =~= Seq::<Option<i64>>::empty());
    assert(seq![Some(x)].take(0) =~= Seq::<Option<i64>>::empty());
}

/// A single delta realizes the running sum through it.
pub(crate) proof fn lemma_rz_one(p: int, x: i64)
    ensures
        rz(p, seq![Some(x)]) == seq![Some(crop(p + x))],
{
    lemma_psum_one(x);
    reveal_with_fuel(psum, 1);
    assert(rz(p, seq![Some(x)]) =~= seq![Some(crop(p + x))]);
}

/// Paying the debt: the delta `d + adjust` written after the running sum
/// `pw` lands where `d` did after `pr`.
proof fn lemma_pay(pw: int, pr: int, running: i64, adjust: i64, d: i64, nd: i64, run2: i64)
    requires
        running == crop(pw),
        crop(running + adjust) == crop(pr),
        nd == crop(d + adjust),
        run2 == crop(running + nd),
    ensures
        run2 == crop(pw + nd),
        crop(pw + nd) == crop(pr + d),
        crop(run2 + 0) == run2,
{
    lemma_crop_add(pw, nd as int);
    lemma_crop_add(d + adjust, pw);
    lemma_crop_add(pw, adjust + d);
    lemma_crop_add(running + adjust, d as int);
    lemma_crop_add(pr, d as int);
    lemma_crop(run2 + 0);
}

/// Deltas `a` then `b`: `b`'s values follow on from `a`'s running sum.
pub(crate) proof fn lemma_rz_cat(p: int, a: Seq<Option<i64>>, b: Seq<Option<i64>>)
    ensures
        rz(p, a + b) == rz(p, a) + rz(p + psum(a), b),
{
    assert forall|i: int| 0 <= i < a.len() + b.len() implies #[trigger] rz(p, a + b)[i] == (rz(p, a) + rz(
        p + psum(a),
        b,
    ))[i] by {
        if i < a.len() {
            assert((a + b).take(i) =~= a.take(i));
        } else {
            let j = i - a.len();
            assert((a + b).take(i) =~= a + b.take(j));
            lemma_psum_cat(a, b.take(j));
        }
    }
    assert(rz(p, a + b) =~= rz(p, a) + rz(p + psum(a), b));
}

/// Running sums alike modulo 2^64 realize alike.
pub(crate) proof fn lemma_rz_crop(p: int, q: int, ds: Seq<Option<i64>>)
    requires
        crop(p) == crop(q),
    ensures
        rz(p, ds) == rz(q, ds),
{
    assert forall|i: int| 0 <= i < ds.len() implies #[trigger] rz(p, ds)[i] == rz(q, ds)[i] by {
        let x = psum(ds.take(i)) + dval(ds[i]);
        lemma_crop_add(p, x);
        lemma_crop_add(q, x);
    }
    assert(rz(p, ds) =~= rz(q, ds));
}

/// Nulls realize nulls, whatever the running sum.
pub(crate) proof fn lemma_rz_nulls(p: int, n: nat)
    ensures
        rz(p, reps(n, None)) == reps(n, None),
        psum(reps(n, None)) == 0,
{
    lemma_run_sum(Seq::empty(), n, None);
    assert(Seq::<Option<i64>>::empty() + reps(n, None) =~= reps(n, None));
    assert(rz(p, reps(n, None)) =~= reps(n, None));
}

/// A run of `n` deltas `d` realizes `p + d`, `p + 2d`, …, and adds `n·d`.
pub(crate) proof fn lemma_rz_run(p: int, d: i64, n: nat)
    ensures
        rz(p, reps(n, Some(d))) == run_vals(p, d, n),
        psum(reps(n, Some(d))) == n * d,
{
    assert forall|k: int| 0 <= k < n implies #[trigger] rz(p, reps(n, Some(d)))[k] == run_vals(p, d, n)[k] by {
        assert(reps(n, Some(d)).take(k) =~= Seq::<Option<i64>>::empty() + reps(k as nat, Some(d)));
        lemma_run_sum(Seq::empty(), k as nat, Some(d));
        assert(k * d + d == (k + 1) * d) by (nonlinear_arith);
    }
    assert(rz(p, reps(n, Some(d))) =~= run_vals(p, d, n));
    lemma_run_sum(Seq::empty(), n, Some(d));
    assert(Seq::<Option<i64>>::empty() + reps(n, Some(d)) =~= reps(n, Some(d)));
}

/// A run of `n` deltas `d` whose first delta is replaced by one reaching
/// `prefix + d` realizes the run's values after `prefix`, and adds up to
/// `prefix + n·d`.
pub(crate) proof fn lemma_rz_anchored(p: int, prefix: int, first: i64, d: i64, n: nat)
    requires
        n >= 1,
        crop(p + first) == crop(prefix + d),
    ensures
        rz(p, seq![Some(first)] + reps((n - 1) as nat, Some(d))) == run_vals(prefix, d, n),
        crop(p + psum(seq![Some(first)] + reps((n - 1) as nat, Some(d)))) == crop(prefix + n * d),
{
    let one = seq![Some(first)];
    let rest = reps((n - 1) as nat, Some(d));
    lemma_rz_cat(p, one, rest);
    lemma_psum_one(first);
    lemma_rz_run(p + first, d, (n - 1) as nat);
    lemma_psum_cat(one, rest);
    assert forall|k: int| 0 <= k < n implies #[trigger] (rz(p, one) + rz(p + first, rest))[k] == run_vals(
        prefix,
        d,
        n,
    )[k] by {
        if k == 0 {
            assert(crop(p + first) == crop(prefix + (k + 1) * d));
        } else {
            // the run's values follow on from where the anchor put it
            let j = k - 1;
            lemma_crop_add(p + first, (j + 1) * d);
            lemma_crop_add(prefix + d, (j + 1) * d);
            assert(prefix + d + (j + 1) * d == prefix + (k + 1) * d) by (nonlinear_arith)
                requires k == j + 1;
        }
    }
    assert(rz(p, one + rest) =~= run_vals(prefix, d, n));
    lemma_crop_add(p + first, (n - 1) * d);
    lemma_crop_add(prefix + d, (n - 1) * d);
    assert(prefix + d + (n - 1) * d == prefix + n * d) by (nonlinear_arith);
}

/// `wrapping_sub` crops the difference.
pub(crate) proof fn lemma_wrapping_sub(x: i64, y: i64)
    ensures
        vstd::wrapping::i64_specs::wrapping_sub(x, y) == crop(x - y),
{
    let r = 0x1_0000_0000_0000_0000int;
    let s = x - y;
    if s > i64::MAX {
        lemma_crop(s - r);
        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(s - r, r);
    } else if s < i64::MIN {
        lemma_crop(s + r);
        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(s, r);
    } else {
        lemma_crop(s);
    }
}

/// The delta that realizes `v` after the running sum `running`.
#[inline(always)]
pub(crate) fn delta_to(running: i64, v: i64) -> (d: i64)
    ensures
        crop(running + d) == v,
{
    let d = v.wrapping_sub(running);
    proof {
        lemma_wrapping_sub(v, running);
        lemma_crop_add(v - running, running as int);
        lemma_crop(v as int);
    }
    d
}

/// A run of `n` deltas `d` whose values follow the running sum `prefix`,
/// written after the running sum `running`: the delta its first item needs
/// (`d` itself where `prefix` is `running`, so the run copies whole), and
/// the running sum after the run.
#[inline(always)]
pub(crate) fn anchor_run(running: i64, prefix: i64, d: i64, n: usize) -> (r: (i64, i64))
    requires
        n >= 1,
    ensures
        crop(running + r.0) == crop(prefix + d),
        prefix == running ==> r.0 == d,
        r.1 == crop(prefix + n * d),
{
    let first = delta_to(running, prefix.wrapping_add(d));
    let end = add_run(prefix, d, n, Ghost(prefix as int));
    proof {
        lemma_wrapping_add(prefix, d);
        lemma_crop(prefix + d);
        if prefix == running {
            // `first` and `d` both reach `prefix + d` from `running`
            lemma_crop_add(prefix + d, 0);
            lemma_crop(d as int);
            lemma_crop(first as int);
            assert(crop(running + first) == crop(running + d));
            assert(crop(running + first) as int % 0x1_0000_0000_0000_0000 == (running + first) % 0x1_0000_0000_0000_0000)
                by { lemma_crop(running + first); }
            assert(crop(running + d) as int % 0x1_0000_0000_0000_0000 == (running + d) % 0x1_0000_0000_0000_0000)
                by { lemma_crop(running + d); }
            lemma_same_rem(running as int, first as int, d as int);
        }
    }
    (first, end)
}

/// Two `i64`s that land alike modulo 2^64 when added to the same number
/// are equal.
proof fn lemma_same_rem(a: int, x: int, y: int)
    requires
        (a + x) % 0x1_0000_0000_0000_0000 == (a + y) % 0x1_0000_0000_0000_0000,
        i64::MIN <= x <= i64::MAX,
        i64::MIN <= y <= i64::MAX,
    ensures
        x == y,
{
    let r = 0x1_0000_0000_0000_0000int;
    vstd::arithmetic::div_mod::lemma_sub_mod_noop(a + x, a + y, r);
    assert((a + x) - (a + y) == x - y);
    assert((x - y) % r == 0) by {
        vstd::arithmetic::div_mod::lemma_mod_self_0(r);
    }
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(x - y, r);
    let q = (x - y) / r;
    assert(x - y == r * q);
    assert(q == 0) by (nonlinear_arith)
        requires
            x - y == r * q,
            -r < x - y < r,
            r > 0,
    ;
}

/// The arithmetic of a [`DeltaEdit`](crate::delta::DeltaEdit) cursor:
/// the value standing before the cursor in the column being written, and
/// what the next original delta owes.
pub(crate) struct Ledger {
    /// The running sum of the deltas written before the cursor: the value
    /// standing there in the column being written.
    pub(crate) running: i64,
    /// What the next non-null original delta owes: the original running
    /// sum at the cursor less `running`.
    pub(crate) adjust: i64,
    /// The deltas written before the cursor.
    pub(crate) written: Ghost<Seq<Option<i64>>>,
    /// The original deltas the cursor has passed (carried, deleted or
    /// paid).
    pub(crate) read: Ghost<Seq<Option<i64>>>,
    /// The cursor has passed every original delta, and let go of the debt
    /// (nothing is left to owe it).
    pub(crate) closed: Ghost<bool>,
}

impl Ledger {
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.running == crop(psum(self.written@))
        &&& !self.closed@ ==> crop(self.running + self.adjust) == crop(psum(self.read@))
    }

    /// A cursor with `before` behind it, untouched: `running` is their sum.
    #[inline(always)]
    pub(crate) fn new(running: i64, Ghost(before): Ghost<Seq<Option<i64>>>) -> (l: Self)
        requires
            running == crop(psum(before)),
        ensures
            l.inv(),
            l.written@ == before,
            l.read@ == before,
            l.adjust == 0,
            !l.closed@,
    {
        proof {
            lemma_crop_add(psum(before), 0);
        }
        Ledger { running, adjust: 0, written: Ghost(before), read: Ghost(before), closed: Ghost(false) }
    }

    /// The original value of the item at the cursor, whose delta is `d`.
    #[inline(always)]
    pub(crate) fn next_value(&self, d: i64) -> (v: i64)
        requires
            self.inv(),
            !self.closed@,
        ensures
            Some(v) == realize(psum(self.read@), Some(d)),
    {
        let orig = self.running.wrapping_add(self.adjust);
        proof {
            lemma_wrapping_add(self.running, self.adjust);
            lemma_wrapping_add(orig, d);
            lemma_crop_add(psum(self.read@), d as int);
            lemma_crop_add(self.running + self.adjust, d as int);
        }
        orig.wrapping_add(d)
    }

    /// Carry `n` original items of delta `d` past the cursor, unchanged:
    /// they keep their values, which needs no debt outstanding unless they
    /// are nulls.
    #[inline(always)]
    pub(crate) fn carry(&mut self, d: Option<i64>, n: usize)
        requires
            old(self).inv(),
            !old(self).closed@,
            old(self).adjust == 0 || d is None,
        ensures
            final(self).inv(),
            !final(self).closed@,
            final(self).adjust == old(self).adjust,
            final(self).written@ == old(self).written@ + reps(n as nat, d),
            final(self).read@ == old(self).read@ + reps(n as nat, d),
            rz(0, final(self).written@) == rz(0, old(self).written@) + rz(psum(old(self).read@), reps(n as nat, d)),
    {
        let ghost w0 = self.written@;
        let ghost r0 = self.read@;
        let ghost x = reps(n as nat, d);
        proof {
            lemma_rz_cat(0, w0, x);
            lemma_run_sum(w0, n as nat, d);
            lemma_run_sum(r0, n as nat, d);
            if d is None {
                lemma_rz_nulls(psum(w0), n as nat);
                lemma_rz_nulls(psum(r0), n as nat);
            } else {
                // no debt: the two running sums agree, and so do the values
                lemma_crop_add(psum(w0), 0);
                lemma_crop_add(psum(r0), 0);
                lemma_crop(psum(w0));
                lemma_rz_crop(psum(w0), psum(r0), x);
            }
        }
        if let Some(v) = d {
            self.running = add_run(self.running, v, n, Ghost(psum(w0)));
        }
        self.written = Ghost(w0 + x);
        self.read = Ghost(r0 + x);
        proof {
            let s = n * dval(d);
            lemma_crop_add(psum(w0), s);
            lemma_crop_add(self.running + self.adjust, 0);
            if d is None {
                assert(s == 0) by (nonlinear_arith) requires s == n * 0;
            } else {
                lemma_crop_add(psum(r0), s);
                lemma_crop_add(psum(w0) + s, old(self).adjust as int);
                lemma_crop_add(psum(w0) + old(self).adjust, s);
            }
            lemma_crop_add(psum(w0), old(self).adjust + s);
            lemma_crop_add(psum(r0), s);
            lemma_crop_add(old(self).running + old(self).adjust, s);
            lemma_crop_add(psum(w0) + s, old(self).adjust as int);
        }
    }

    /// The cursor jumped over original deltas `x` without reading them,
    /// which the column carries unchanged; `running` is read back from the
    /// column (with no debt outstanding, the running sums agree).
    #[inline(always)]
    pub(crate) fn jump(&mut self, running: i64, Ghost(x): Ghost<Seq<Option<i64>>>)
        requires
            old(self).inv(),
            !old(self).closed@,
            old(self).adjust == 0,
            running == crop(psum(old(self).written@ + x)),
        ensures
            final(self).inv(),
            !final(self).closed@,
            final(self).adjust == 0,
            final(self).written@ == old(self).written@ + x,
            final(self).read@ == old(self).read@ + x,
            rz(0, final(self).written@) == rz(0, old(self).written@) + rz(psum(old(self).read@), x),
    {
        let ghost w0 = self.written@;
        let ghost r0 = self.read@;
        proof {
            lemma_rz_cat(0, w0, x);
            lemma_psum_cat(w0, x);
            lemma_psum_cat(r0, x);
            lemma_crop_add(psum(w0), 0);
            lemma_crop_add(psum(r0), 0);
            lemma_crop(psum(w0));
            lemma_rz_crop(psum(w0), psum(r0), x);
            lemma_crop_add(psum(w0), psum(x));
            lemma_crop_add(psum(r0), psum(x));
            lemma_crop_add(psum(w0 + x), 0);
        }
        self.running = running;
        self.written = Ghost(w0 + x);
        self.read = Ghost(r0 + x);
    }

    /// Insert `n` copies of the value `v` at the cursor: the delta reaching
    /// it (returned, to be written first), then `n - 1` zeroes.
    #[inline(always)]
    #[allow(unused_variables)] // `n` is for the spec: the zeroes the caller writes
    pub(crate) fn insert_value(&mut self, v: i64, n: usize) -> (d: i64)
        requires
            old(self).inv(),
            n >= 1,
        ensures
            final(self).inv(),
            final(self).closed@ == old(self).closed@,
            final(self).read@ == old(self).read@,
            final(self).written@ == old(self).written@ + (seq![Some(d)] + reps((n - 1) as nat, Some(0i64))),
            rz(0, final(self).written@) == rz(0, old(self).written@) + reps(n as nat, Some(v)),
    {
        let r = self.running;
        let d = delta_to(r, v);
        let ghost w0 = self.written@;
        let ghost x = seq![Some(d)] + reps((n - 1) as nat, Some(0i64));
        proof {
            lemma_rz_cat(0, w0, x);
            lemma_crop_add(psum(w0), d as int);
            lemma_crop_add(v as int, 0);
            lemma_crop(v as int);
            lemma_rz_anchored(psum(w0), v as int, d, 0, n as nat);
            assert(run_vals(v as int, 0, n as nat) =~= reps(n as nat, Some(v))) by {
                assert forall|k: int| 0 <= k < n implies #[trigger] run_vals(v as int, 0, n as nat)[k] == Some(v) by {
                    assert((k + 1) * 0 == 0) by (nonlinear_arith);
                }
            }
            lemma_crop_add(psum(w0) + psum(x), 0);
            assert(v + (n as int) * 0 == v) by (nonlinear_arith);
        }
        // the original values after the cursor are still relative to `r`;
        // the difference is now owed
        let owed = r.wrapping_sub(v);
        self.adjust = self.adjust.wrapping_add(owed);
        self.running = v;
        self.written = Ghost(w0 + x);
        proof {
            lemma_wrapping_sub(r, v);
            lemma_wrapping_add(old(self).adjust, owed);
            lemma_psum_cat(w0, x);
            if !self.closed@ {
                lemma_crop_add(old(self).adjust + crop(r - v), v as int);
                lemma_crop_add(r - v, old(self).adjust + v);
                lemma_crop_add(old(self).adjust + (r - v), v as int);
                assert(old(self).adjust + (r - v) + v == r + old(self).adjust);
            }
        }
        d
    }

    /// Insert `n` nulls at the cursor: they hold no delta and leave the
    /// running sum where it is.
    #[inline(always)]
    #[allow(unused_variables)] // `n` is for the spec: the nulls the caller writes
    pub(crate) fn insert_nulls(&mut self, n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).closed@ == old(self).closed@,
            final(self).adjust == old(self).adjust,
            final(self).read@ == old(self).read@,
            final(self).written@ == old(self).written@ + reps(n as nat, None),
            rz(0, final(self).written@) == rz(0, old(self).written@) + reps(n as nat, None),
    {
        let ghost w0 = self.written@;
        proof {
            lemma_rz_cat(0, w0, reps(n as nat, None));
            lemma_rz_nulls(psum(w0), n as nat);
            lemma_psum_cat(w0, reps(n as nat, None));
        }
        self.written = Ghost(w0 + reps(n as nat, None));
    }

    /// Insert a run of `n` deltas `d` whose values follow the running sum
    /// `prefix`: the delta its first item needs (returned; `d` where the
    /// run copies whole), then `n - 1` deltas `d`.
    #[inline(always)]
    pub(crate) fn insert_run(&mut self, prefix: i64, d: i64, n: usize) -> (first: i64)
        requires
            old(self).inv(),
            n >= 1,
        ensures
            final(self).inv(),
            final(self).closed@ == old(self).closed@,
            final(self).read@ == old(self).read@,
            prefix == old(self).running ==> first == d,
            final(self).written@ == old(self).written@ + (seq![Some(first)] + reps((n - 1) as nat, Some(d))),
            rz(0, final(self).written@) == rz(0, old(self).written@) + run_vals(prefix as int, d, n as nat),
    {
        let r = self.running;
        let (first, end) = anchor_run(r, prefix, d, n);
        let ghost w0 = self.written@;
        let ghost x = seq![Some(first)] + reps((n - 1) as nat, Some(d));
        proof {
            lemma_rz_cat(0, w0, x);
            lemma_crop_add(psum(w0), first as int);
            lemma_rz_anchored(psum(w0), prefix as int, first, d, n as nat);
            lemma_psum_cat(w0, x);
            lemma_crop_add(psum(w0) + first, psum(x) - first);
            lemma_crop_add(psum(w0), psum(x));
        }
        let owed = r.wrapping_sub(end);
        self.adjust = self.adjust.wrapping_add(owed);
        self.running = end;
        self.written = Ghost(w0 + x);
        proof {
            lemma_wrapping_sub(r, end);
            lemma_wrapping_add(old(self).adjust, owed);
            if !self.closed@ {
                lemma_crop_add(old(self).adjust + crop(r - end), end as int);
                lemma_crop_add(r - end, old(self).adjust + end);
                lemma_crop_add(old(self).adjust + (r - end), end as int);
                assert(old(self).adjust + (r - end) + end == r + old(self).adjust);
            }
        }
        first
    }

    /// Delete original deltas `ds` at the cursor, whose sum is `sum`: they
    /// are gone from the column being written, but the original values
    /// after them still count them, so their sum is owed.
    #[inline(always)]
    pub(crate) fn delete(&mut self, sum: i64, Ghost(ds): Ghost<Seq<Option<i64>>>)
        requires
            old(self).inv(),
            !old(self).closed@,
            sum == crop(psum(ds)),
        ensures
            final(self).inv(),
            !final(self).closed@,
            final(self).running == old(self).running,
            final(self).written@ == old(self).written@,
            final(self).read@ == old(self).read@ + ds,
    {
        let ghost r0 = self.read@;
        let ghost a0 = self.adjust;
        self.adjust = self.adjust.wrapping_add(sum);
        self.read = Ghost(r0 + ds);
        proof {
            lemma_wrapping_add(a0, sum);
            lemma_psum_cat(r0, ds);
            lemma_crop_add(a0 + sum, self.running as int);
            lemma_crop_add(sum as int, self.running + a0);
            lemma_crop_add(psum(ds), self.running + a0);
            lemma_crop_add(self.running + a0, psum(ds));
            lemma_crop_add(psum(r0), psum(ds));
        }
    }

    /// Pay the debt into the next original delta `d`, taken out of the
    /// column: returns the delta to write in its place, which realizes the
    /// item's original value.
    #[inline(always)]
    pub(crate) fn pay(&mut self, d: i64) -> (nd: i64)
        requires
            old(self).inv(),
            !old(self).closed@,
        ensures
            final(self).inv(),
            !final(self).closed@,
            final(self).adjust == 0,
            final(self).read@ == old(self).read@.push(Some(d)),
            final(self).written@ == old(self).written@.push(Some(nd)),
            rz(0, final(self).written@) == rz(0, old(self).written@) + rz(psum(old(self).read@), seq![Some(d)]),
    {
        let ghost w0 = self.written@;
        let ghost r0 = self.read@;
        let nd = d.wrapping_add(self.adjust);
        let running = self.running.wrapping_add(nd);
        proof {
            lemma_wrapping_add(d, self.adjust);
            lemma_wrapping_add(self.running, nd);
            lemma_pay(psum(w0), psum(r0), self.running, self.adjust, d, nd, running);
            assert(w0.push(Some(nd)) =~= w0 + seq![Some(nd)]);
            assert(r0.push(Some(d)) =~= r0 + seq![Some(d)]);
            lemma_psum_one(nd);
            lemma_psum_one(d);
            lemma_rz_cat(0, w0, seq![Some(nd)]);
            lemma_psum_cat(w0, seq![Some(nd)]);
            lemma_psum_cat(r0, seq![Some(d)]);
            // the written item lands where the original did
            lemma_rz_one(psum(w0), nd);
            lemma_rz_one(psum(r0), d);
        }
        self.running = running;
        self.adjust = 0;
        self.written = Ghost(w0.push(Some(nd)));
        self.read = Ghost(r0.push(Some(d)));
        nd
    }

    /// The cursor has passed every original delta: nothing is left to owe
    /// the debt, so it is dropped.
    #[inline(always)]
    pub(crate) fn close(&mut self)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).closed@,
            final(self).adjust == 0,
            final(self).running == old(self).running,
            final(self).written@ == old(self).written@,
            final(self).read@ == old(self).read@,
    {
        self.adjust = 0;
        self.closed = Ghost(true);
    }
}

/// The run of deltas `ds` adds, wrapping: `n·d` for a run of `n` deltas
/// `d`, nothing for nulls.
#[inline(always)]
pub(crate) fn run_sum(d: Option<i64>, n: usize) -> (s: i64)
    ensures
        s == crop(psum(reps(n as nat, d))),
{
    proof {
        lemma_run_sum(Seq::empty(), n as nat, d);
        assert(Seq::<Option<i64>>::empty() + reps(n as nat, d) =~= reps(n as nat, d));
    }
    match d {
        Some(v) => add_run(0, v, n, Ghost(0)),
        None => {
            proof {
                assert(n * 0 == 0) by (nonlinear_arith);
                lemma_crop(0);
            }
            0
        },
    }
}

/// Once the debt is paid (or nothing follows), the original deltas after
/// the cursor, `rest`, realize after what was written exactly the values
/// they realized in the original column, which was `read` then `rest`.
pub(crate) proof fn lemma_rest_unchanged(l: Ledger, rest: Seq<Option<i64>>)
    requires
        l.inv(),
        l.closed@ ==> rest.len() == 0,
        !l.closed@ ==> l.adjust == 0,
    ensures
        rz(0, l.written@ + rest) == rz(0, l.written@) + rz(0, l.read@ + rest).skip(l.read@.len() as int),
{
    lemma_rz_cat(0, l.written@, rest);
    lemma_rz_cat(0, l.read@, rest);
    assert((rz(0, l.read@) + rz(psum(l.read@), rest)).skip(l.read@.len() as int) =~= rz(psum(l.read@), rest));
    if !l.closed@ {
        lemma_crop_add(psum(l.written@), 0);
        lemma_crop_add(psum(l.read@), 0);
        lemma_crop(psum(l.written@));
        lemma_rz_crop(psum(l.written@), psum(l.read@), rest);
    } else {
        assert(rz(psum(l.written@), rest) =~= rz(psum(l.read@), rest));
    }
}

/// `splice` replaces deltas `x` after `a` with `e`, carrying over the
/// `skip` nulls that follow and rewriting the next delta (`b`, if there is
/// one) to `bd`, which reaches from where `e` leaves off the value `b`
/// realized. Every value after `e` is then what it was.
pub(crate) proof fn lemma_splice_keeps_rest(
    a: Seq<Option<i64>>,
    x: Seq<Option<i64>>,
    e: Seq<Option<i64>>,
    skip: nat,
    b: i64,
    bd: i64,
    rest: Seq<Option<i64>>,
)
    requires
        crop(psum(a) + psum(e) + bd) == crop(psum(a + x) + b),
    ensures
        ({
            let tail = reps(skip, None) + seq![Some(b)] + rest;
            let new_tail = reps(skip, None) + seq![Some(bd)] + rest;
            &&& rz(0, a + e + new_tail) == rz(0, a) + rz(psum(a), e) + rz(0, a + x + tail).skip(
                (a.len() + x.len()) as int,
            )
        }),
{
    let nulls = reps(skip, None);
    let tail = nulls + seq![Some(b)] + rest;
    let new_tail = nulls + seq![Some(bd)] + rest;
    let p = psum(a) + psum(e);
    let q = psum(a + x);
    lemma_rz_cat(0, a, e);
    lemma_psum_cat(a, e);
    lemma_rz_cat(0, a + e, new_tail);
    lemma_rz_cat(0, a + x, tail);
    assert((rz(0, a + x) + rz(q, tail)).skip((a.len() + x.len()) as int) =~= rz(q, tail));
    // both tails: the nulls, then the boundary at the same value, then the
    // rest after it
    lemma_rz_nulls(p, skip);
    lemma_rz_nulls(q, skip);
    lemma_rz_cat(p, nulls, seq![Some(bd)] + rest);
    lemma_rz_cat(q, nulls, seq![Some(b)] + rest);
    assert(nulls + seq![Some(bd)] + rest =~= nulls + (seq![Some(bd)] + rest));
    assert(nulls + seq![Some(b)] + rest =~= nulls + (seq![Some(b)] + rest));
    lemma_rz_cat(p, seq![Some(bd)], rest);
    lemma_rz_cat(q, seq![Some(b)], rest);
    lemma_psum_one(bd);
    lemma_psum_one(b);
    assert(rz(p, seq![Some(bd)]) =~= rz(q, seq![Some(b)]));
    lemma_crop_add(p + bd, 0);
    lemma_crop_add(q + b, 0);
    lemma_rz_crop(p + bd, q + b, rest);
    assert(rz(0, a + e + new_tail) =~= rz(0, a) + rz(psum(a), e) + rz(q, tail));
}

/// Where a splice's emission of deltas stands: the inserted items, then
/// the nulls after the deleted ones, then the rewritten boundary delta.
pub(crate) enum SplicePhase {
    Values,
    NullRun,
    Boundary,
    Done,
}

/// The deltas a splice writes, emitted a run at a time (`splice`'s
/// iterator drives it): for each inserted item the deltas realizing its
/// values; then the nulls that followed the deleted items, carried over;
/// then a boundary delta reaching the value of the item after them again.
pub(crate) struct SplicePlan {
    pub(crate) running: i64,
    /// the rest of a run whose first item was re-anchored
    pub(crate) pending: Option<(i64, usize)>,
    pub(crate) skip: usize,
    pub(crate) next_val: Option<i64>,
    pub(crate) phase: SplicePhase,
    /// the running sum before the splice
    #[allow(dead_code)]
    pub(crate) prev: Ghost<int>,
    /// the deltas emitted so far
    #[allow(dead_code)]
    pub(crate) emitted: Ghost<Seq<Option<i64>>>,
    /// the values of the items taken so far
    #[allow(dead_code)]
    pub(crate) wanted: Ghost<Seq<Option<i64>>>,
}

/// The deltas a plan has yet to emit of a run it re-anchored.
pub(crate) open spec fn pend_seq(pending: Option<(i64, usize)>) -> Seq<Option<i64>> {
    match pending {
        Some((d, n)) => reps(n as nat, Some(d)),
        None => Seq::empty(),
    }
}

/// What a splice's deltas realize once done: the items' values, then (if
/// an item followed the deleted ones) the nulls before it and its value.
pub(crate) open spec fn splice_vals(wanted: Seq<Option<i64>>, skip: nat, next_val: Option<i64>) -> Seq<Option<i64>> {
    match next_val {
        Some(v) => wanted + reps(skip, None) + seq![Some(v)],
        None => wanted,
    }
}

impl SplicePlan {
    pub(crate) open spec fn inv(&self) -> bool {
        let p = self.prev@;
        let e = self.emitted@;
        match self.phase {
            SplicePhase::Values => {
                &&& rz(p, e + pend_seq(self.pending)) == self.wanted@
                &&& self.running == crop(p + psum(e + pend_seq(self.pending)))
            },
            SplicePhase::NullRun => {
                &&& self.pending is None && self.next_val is Some
                &&& rz(p, e) == self.wanted@
                &&& self.running == crop(p + psum(e))
            },
            SplicePhase::Boundary => {
                &&& self.pending is None && self.next_val is Some
                &&& rz(p, e) == self.wanted@ + reps(self.skip as nat, None)
                &&& self.running == crop(p + psum(e))
            },
            SplicePhase::Done => rz(p, e) == splice_vals(self.wanted@, self.skip as nat, self.next_val),
        }
    }

    /// A plan for a splice after the running sum `prev`, followed by `skip`
    /// nulls and then (if any) an item of value `next_val`.
    #[inline(always)]
    pub(crate) fn new(prev: i64, skip: usize, next_val: Option<i64>) -> (r: Self)
        ensures
            r.inv(),
            r.phase is Values,
            r.prev@ == prev as int,
            r.emitted@.len() == 0,
            r.wanted@.len() == 0,
            r.skip == skip,
            r.next_val == next_val,
    {
        proof {
            assert(Seq::<Option<i64>>::empty() + Seq::<Option<i64>>::empty() =~= Seq::<Option<i64>>::empty());
            assert(rz(prev as int, Seq::<Option<i64>>::empty()) =~= Seq::<Option<i64>>::empty());
            lemma_crop(prev as int);
        }
        SplicePlan {
            running: prev,
            pending: None,
            skip,
            next_val,
            phase: SplicePhase::Values,
            prev: Ghost(prev as int),
            emitted: Ghost(Seq::empty()),
            wanted: Ghost(Seq::empty()),
        }
    }

    /// Emit the rest of a re-anchored run, if there is one.
    #[inline(always)]
    pub(crate) fn take_pending(&mut self) -> (r: Option<(Option<i64>, usize)>)
        requires
            old(self).inv(),
            old(self).phase is Values,
        ensures
            final(self).inv(),
            final(self).phase is Values,
            final(self).pending is None,
            final(self).wanted == old(self).wanted,
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
            final(self).emitted@ == old(self).emitted@ + match r {
                Some((x, n)) => reps(n as nat, x),
                None => Seq::empty(),
            },
    {
        match self.pending {
            Some((d, n)) => {
                self.pending = None;
                proof {
                    let e = self.emitted@ + reps(n as nat, Some(d));
                    assert(e + Seq::<Option<i64>>::empty() =~= e);
                    self.emitted = Ghost(e);
                }
                Some((Some(d), n))
            },
            None => {
                proof {
                    assert(self.emitted@ + Seq::<Option<i64>>::empty() =~= self.emitted@);
                }
                None
            },
        }
    }

    /// Emit the delta of an inserted value `v` (or a null).
    #[inline(always)]
    pub(crate) fn value(&mut self, v: Option<i64>) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).phase is Values,
            old(self).pending is None,
        ensures
            final(self).inv(),
            final(self).phase is Values,
            final(self).pending is None,
            final(self).wanted@ == old(self).wanted@.push(v),
            final(self).emitted@ == old(self).emitted@ + reps(r.1 as nat, r.0),
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
    {
        let ghost p = self.prev@;
        let ghost e = self.emitted@;
        let ghost w = self.wanted@;
        proof {
            assert(e + Seq::<Option<i64>>::empty() =~= e);
        }
        match v {
            None => {
                proof {
                    let x = reps(1, None);
                    lemma_rz_cat(p, e, x);
                    lemma_rz_nulls(p + psum(e), 1);
                    lemma_psum_cat(e, x);
                    assert(x =~= seq![None]);
                    let e1 = e + x;
                    assert(e1 + Seq::<Option<i64>>::empty() =~= e1);
                    assert(w.push(v) =~= w + x);
                    self.emitted = Ghost(e1);
                    self.wanted = Ghost(w.push(v));
                }
                (None, 1)
            },
            Some(x) => {
                let d = delta_to(self.running, x);
                proof {
                    let one = seq![Some(d)];
                    lemma_rz_cat(p, e, one);
                    lemma_psum_cat(e, one);
                    lemma_rz_one(p + psum(e), d);
                    lemma_psum_one(d);
                    lemma_crop_add(p + psum(e), d as int);
                    lemma_crop(x as int);
                    assert(reps(1, Some(d)) =~= one);
                    let e1 = e + one;
                    assert(e1 + Seq::<Option<i64>>::empty() =~= e1);
                    assert(w.push(v) =~= w + seq![Some(x)]);
                    self.emitted = Ghost(e1);
                    self.wanted = Ghost(w.push(v));
                }
                self.running = x;
                (Some(d), 1)
            },
        }
    }

    /// Emit a run of `count` deltas `d` whose values follow the running sum
    /// `prefix` (or `count` nulls): verbatim where the run is anchored
    /// already, else its first item re-anchored, with the rest left
    /// pending.
    #[inline(always)]
    pub(crate) fn run(&mut self, prefix: i64, d: Option<i64>, count: usize) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).phase is Values,
            old(self).pending is None,
            count >= 1,
        ensures
            final(self).inv(),
            final(self).phase is Values,
            final(self).wanted@ == old(self).wanted@ + match d {
                Some(x) => run_vals(prefix as int, x, count as nat),
                None => reps(count as nat, None),
            },
            final(self).emitted@ == old(self).emitted@ + reps(r.1 as nat, r.0),
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
    {
        let ghost p = self.prev@;
        let ghost e = self.emitted@;
        proof {
            assert(e + Seq::<Option<i64>>::empty() =~= e);
        }
        match d {
            None => {
                proof {
                    let x = reps(count as nat, None);
                    lemma_rz_cat(p, e, x);
                    lemma_rz_nulls(p + psum(e), count as nat);
                    lemma_psum_cat(e, x);
                    let e1 = e + x;
                    assert(e1 + Seq::<Option<i64>>::empty() =~= e1);
                    self.emitted = Ghost(e1);
                    self.wanted = Ghost(self.wanted@ + x);
                }
                (None, count)
            },
            Some(x) => {
                let (first, end) = anchor_run(self.running, prefix, x, count);
                let ghost whole = seq![Some(first)] + reps((count - 1) as nat, Some(x));
                proof {
                    lemma_rz_cat(p, e, whole);
                    lemma_psum_cat(e, whole);
                    // the run realizes after where the emitted deltas leave off
                    lemma_crop_add(p + psum(e), first as int);
                    lemma_rz_anchored(self.running as int, prefix as int, first, x, count as nat);
                    lemma_crop_add(p + psum(e), 0);
                    lemma_crop(p + psum(e));
                    lemma_rz_crop(p + psum(e), self.running as int, whole);
                    lemma_crop_add(p + psum(e), psum(whole));
                    lemma_crop_add(self.running as int, psum(whole));
                    self.wanted = Ghost(self.wanted@ + run_vals(prefix as int, x, count as nat));
                }
                self.running = end;
                if first == x {
                    proof {
                        assert(whole =~= reps(count as nat, Some(x)));
                        let e1 = e + reps(count as nat, Some(x));
                        assert(e1 + Seq::<Option<i64>>::empty() =~= e1);
                        assert(e + whole =~= e1);
                        self.emitted = Ghost(e1);
                    }
                    (Some(x), count)
                } else {
                    if count > 1 {
                        self.pending = Some((x, count - 1));
                    }
                    proof {
                        let e1 = e + reps(1, Some(first));
                        assert(reps(1, Some(first)) =~= seq![Some(first)]);
                        assert(e1 + pend_seq(self.pending) =~= e + whole);
                        self.emitted = Ghost(e1);
                    }
                    (Some(first), 1)
                }
            },
        }
    }

    /// The inserted items are done: on to the nulls and boundary, if an
    /// item followed the deleted ones, else finished.
    #[inline(always)]
    pub(crate) fn end_values(&mut self)
        requires
            old(self).inv(),
            old(self).phase is Values,
            old(self).pending is None,
        ensures
            final(self).inv(),
            final(self).next_val is Some ==> final(self).phase is NullRun,
            final(self).next_val is None ==> final(self).phase is Done,
            final(self).emitted == old(self).emitted,
            final(self).wanted == old(self).wanted,
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
    {
        proof {
            assert(self.emitted@ + Seq::<Option<i64>>::empty() =~= self.emitted@);
        }
        self.phase = if self.next_val.is_some() { SplicePhase::NullRun } else { SplicePhase::Done };
    }

    /// Emit the nulls that followed the deleted items, if any.
    #[inline(always)]
    pub(crate) fn nulls(&mut self) -> (r: Option<(Option<i64>, usize)>)
        requires
            old(self).inv(),
            old(self).phase is NullRun,
        ensures
            final(self).inv(),
            final(self).phase is Boundary,
            final(self).emitted@ == old(self).emitted@ + match r {
                Some((x, n)) => reps(n as nat, x),
                None => Seq::empty(),
            },
            final(self).wanted == old(self).wanted,
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
    {
        let ghost p = self.prev@;
        let ghost e = self.emitted@;
        let ghost x = reps(self.skip as nat, None);
        proof {
            lemma_rz_cat(p, e, x);
            lemma_rz_nulls(p + psum(e), self.skip as nat);
            lemma_psum_cat(e, x);
        }
        self.phase = SplicePhase::Boundary;
        // a zero-length run of nulls can't be written (a non-nullable
        // column has no null to write)
        if self.skip > 0 {
            proof {
                self.emitted = Ghost(e + x);
            }
            Some((None, self.skip))
        } else {
            proof {
                assert(x =~= Seq::<Option<i64>>::empty());
                assert(self.wanted@ + x =~= self.wanted@);
                assert(e + Seq::<Option<i64>>::empty() =~= e);
            }
            None
        }
    }

    /// Emit the boundary delta: from where the plan has left the running
    /// sum to the value of the item after the deleted ones.
    #[inline(always)]
    pub(crate) fn boundary(&mut self) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).phase is Boundary,
        ensures
            final(self).inv(),
            final(self).phase is Done,
            final(self).emitted@ == old(self).emitted@ + reps(r.1 as nat, r.0),
            final(self).wanted == old(self).wanted,
            final(self).prev == old(self).prev,
            final(self).skip == old(self).skip,
            final(self).next_val == old(self).next_val,
    {
        let ghost p = self.prev@;
        let ghost e = self.emitted@;
        let next = match self.next_val {
            Some(v) => v,
            None => {
                proof {
                    assert(false);
                }
                0
            },
        };
        let bd = delta_to(self.running, next);
        proof {
            lemma_rz_cat(p, e, seq![Some(bd)]);
            lemma_rz_one(p + psum(e), bd);
            lemma_crop_add(p + psum(e), bd as int);
            assert(reps(1, Some(bd)) =~= seq![Some(bd)]);
            self.emitted = Ghost(e + seq![Some(bd)]);
        }
        self.phase = SplicePhase::Done;
        (Some(bd), 1)
    }
}

/// The deltas before the iterator's position `k` items further on: those
/// before it, then those of the first `k` items of its window.
pub(crate) proof fn lemma_dbefore_window<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        c.wf::<X>(s),
        0 <= k <= c.out::<X>(s).len(),
    ensures
        dbefore::<X, B>(s, c.pos + k) == dbefore::<X, B>(s, c.pos as int) + deltas_of::<_, B>(c.out::<X>(s).take(k)),
{
    let vv = vals_of::<X>(s);
    let o = c.out::<X>(s);
    if k > 0 {
        assert(c.pos + k <= vv.len());
        assert(o.take(k) =~= vv.subrange(c.pos as int, c.pos + k));
        assert(vv.take(c.pos + k) =~= vv.take(c.pos as int) + vv.subrange(c.pos as int, c.pos + k));
    } else {
        assert(o.take(k) =~= Seq::<<X::D as Decode>::Item>::empty());
        if c.pos <= vv.len() {
            assert(vv.take(c.pos as int) + Seq::<<X::D as Decode>::Item>::empty() =~= vv.take(c.pos as int));
        } else {
            assert(vv + Seq::<<X::D as Decode>::Item>::empty() =~= vv);
        }
    }
    assert(deltas_of::<_, B>(vv.take(c.pos as int) + o.take(k)) =~= deltas_of::<_, B>(vv.take(c.pos as int))
        + deltas_of::<_, B>(o.take(k)));
    assert(deltas_of::<_, B>(Seq::<<X::D as Decode>::Item>::empty()) =~= Seq::<Option<i64>>::empty());
}

/// Where a splice at window offset `index` that deletes `del` items meets
/// the column, from an iterator at the start of the window: the running
/// sum before `index` (`prev`), the number of nulls straight after the
/// deleted items (`skip`), and the value of the item after those, if
/// there is one: the deltas after it decode against that value, so the
/// splice has to reach it again.
pub(crate) fn d_boundaries<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    running: &mut i64,
    index: usize,
    del: usize,
) -> (r: (i64, usize, Option<i64>))
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
        index + del <= old(core).out::<X>(slabs@).len(),
    ensures
        ({
            let dw = deltas_of::<_, B>(old(core).out::<X>(slabs@));
            let p0 = psum(dbefore::<X, B>(slabs@, old(core).pos as int));
            let e = index + del + r.1;
            &&& r.0 == crop(p0 + psum(dw.take(index as int)))
            &&& e <= dw.len()
            &&& forall|j: int| index + del <= j < e ==> #[trigger] dw[j] is None
            &&& e < dw.len() ==> dw[e] is Some && r.2 == Some(crop(p0 + psum(dw.take(e + 1))))
            &&& e == dw.len() ==> r.2 is None
        }),
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost w = c0.out::<X>(s);
    let ghost dw = deltas_of::<_, B>(w);
    let ghost p0 = psum(dbefore::<X, B>(s, c0.pos as int));
    if index > 0 {
        d_nth::<X, B>(core, slabs, ix, index - 1, running);
    }
    proof {
        if index == 0 {
            assert(w.skip(0) =~= w);
        }
        lemma_dbefore_window::<X, B>(c0, s, index as int);
        assert(deltas_of::<_, B>(w.take(index as int)) =~= dw.take(index as int));
        lemma_psum_cat(dbefore::<X, B>(s, c0.pos as int), dw.take(index as int));
    }
    let prev = *running;
    if del > 0 {
        d_nth::<X, B>(core, slabs, ix, del - 1, running);
    }
    proof {
        if del == 0 {
            assert(w.skip(index as int).skip(0) =~= w.skip(index as int));
        }
        assert(w.skip(index as int).skip(del as int) =~= w.skip(index + del));
    }
    let mut skip: usize = 0;
    loop
        invariant
            s == slabs@,
            c0 == *old(core),
            w == c0.out::<X>(s),
            dw == deltas_of::<_, B>(w),
            p0 == psum(dbefore::<X, B>(s, c0.pos as int)),
            c0.wf::<X>(s),
            at_run::<X, B>(*core, *running, s),
            index + del + skip <= w.len(),
            core.out::<X>(s) == w.skip(index + del + skip),
            core.pos == c0.pos + index + del + skip,
            prev == crop(p0 + psum(dw.take(index as int))),
            forall|j: int| index + del <= j < index + del + skip ==> #[trigger] dw[j] is None,
        decreases w.len() - (index + del + skip),
    {
        let ghost e = index + del + skip;
        proof {
            lemma_window_sum::<X, B>(c0, s, e);
        }
        match d_boundary_run::<X, B>(core, slabs, running) {
            None => {
                proof {
                    assert(w.skip(e).len() == 0);
                    assert(e == w.len());
                    assert(dw.len() == w.len());
                }
                return (prev, skip, None);
            },
            Some((n, None)) => {
                proof {
                    assert(w.skip(e).len() == w.len() - e);
                    lemma_nulls_in_window::<_, B>(w, e, n as int);
                }
                skip = skip + n;
            },
            Some((_, Some(v))) => {
                proof {
                    assert(w.skip(e).len() == w.len() - e);
                    assert(dw.len() == w.len());
                    lemma_value_in_window::<_, B>(w, e, p0, psum(dbefore::<X, B>(s, c0.pos + e)), v);
                }
                return (prev, skip, Some(v));
            },
        }
    }
}

/// The deltas before window offset `e`: those before the window and the
/// window's first `e`.
proof fn lemma_window_sum<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    e: int,
)
    requires
        c.wf::<X>(s),
        0 <= e <= c.out::<X>(s).len(),
    ensures
        psum(dbefore::<X, B>(s, c.pos + e)) == psum(dbefore::<X, B>(s, c.pos as int)) + psum(
            deltas_of::<_, B>(c.out::<X>(s)).take(e),
        ),
{
    let w = c.out::<X>(s);
    lemma_dbefore_window::<X, B>(c, s, e);
    assert(deltas_of::<_, B>(w.take(e)) =~= deltas_of::<_, B>(w).take(e));
    lemma_psum_cat(dbefore::<X, B>(s, c.pos as int), deltas_of::<_, B>(w).take(e));
}

/// A run of `n` nulls at window offset `e`.
proof fn lemma_nulls_in_window<V: Copy, B: DeltaOps<V>>(w: Seq<V>, e: int, n: int)
    requires
        0 <= e,
        1 <= n,
        e + n <= w.len(),
        forall|j: int| 0 <= j < n ==> #[trigger] B::dv(w.skip(e)[j]) is None,
    ensures
        forall|j: int| e <= j < e + n ==> #[trigger] deltas_of::<_, B>(w)[j] is None,
        w.skip(e).skip(n) == w.skip(e + n),
{
    assert forall|j: int| e <= j < e + n implies #[trigger] deltas_of::<_, B>(w)[j] is None by {
        assert(w[j] == w.skip(e)[j - e]);
        assert(B::dv(w.skip(e)[j - e]) is None);
    }
    assert(w.skip(e).skip(n) =~= w.skip(e + n));
}

/// A non-null item at window offset `e`, after the running sum `p0 + q`
/// with `q` the sum of the window's first `e` deltas.
proof fn lemma_value_in_window<V: Copy, B: DeltaOps<V>>(w: Seq<V>, e: int, p0: int, pe: int, v: i64)
    requires
        0 <= e < w.len(),
        pe == p0 + psum(deltas_of::<_, B>(w).take(e)),
        B::dv(w.skip(e)[0]) is Some,
        v == crop(pe + dval(B::dv(w.skip(e)[0]))),
    ensures
        deltas_of::<_, B>(w)[e] is Some,
        v == crop(p0 + psum(deltas_of::<_, B>(w).take(e + 1))),
{
    let dw = deltas_of::<_, B>(w);
    assert(w.skip(e)[0] == w[e]);
    assert(dw.take(e + 1) =~= dw.take(e).push(dw[e]));
    lemma_push(dw.take(e), dw[e]);
}

/// The next run of the window, past a splice's deleted items: `None` at
/// the end of the window; else its length, and, unless it is nulls, the
/// value its first item realizes.
#[inline(always)]
fn d_boundary_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    running: &mut i64,
) -> (r: Option<(usize, Option<i64>)>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let o = old(core).out::<X>(slabs@);
            let p = psum(dbefore::<X, B>(slabs@, old(core).pos as int));
            match r {
                None => o.len() == 0,
                Some((n, x)) => {
                    &&& 1 <= n <= o.len()
                    &&& final(core).out::<X>(slabs@) == o.skip(n as int)
                    &&& final(core).pos == old(core).pos + n
                    &&& x is None ==> forall|j: int| 0 <= j < n ==> #[trigger] B::dv(o[j]) is None
                    &&& x matches Some(v) ==> B::dv(o[0]) is Some && v == crop(p + dval(B::dv(o[0])))
                },
            }
        }),
{
    let ghost o = old(core).out::<X>(slabs@);
    let r0 = *running;
    let run = d_next_run::<X, B>(core, slabs, usize::MAX, running)?;
    proof {
        assert(o.take(run.count as int)[0] == run.value);
        assert forall|j: int| 0 <= j < run.count implies o[j] == run.value by {
            assert(o.take(run.count as int)[j] == run.value);
        }
    }
    match B::to_opt(run.value) {
        None => Some((run.count, None)),
        Some(d) => {
            proof {
                lemma_wrapping_add(r0, d);
                lemma_crop_add(psum(dbefore::<X, B>(slabs@, old(core).pos as int)), d as int);
            }
            Some((run.count, Some(r0.wrapping_add(d))))
        },
    }
}

} // verus!
