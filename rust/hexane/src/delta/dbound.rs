//! A bound on the values a delta column holds after an edit, verified
//! with Verus.
//!
//! An `i64` delta column's values must lie within a range less than 2^63
//! wide, or a delta between two of them wraps and the index's sums leave
//! `i64` (`check_span`). Walking the whole column after every edit to see
//! would cost what the edit didn't, so an edit keeps a bound instead: the
//! least and greatest value the column held, widened by each value it
//! writes. Where that bound is narrow enough, the walk is skipped.
//!
//! Proved: where every value the deltas realize (wrapping, as the
//! iterators read them, `rz`) lies within a bound inside `i64` and less
//! than 2^63 wide, no running sum wraps, so the sums at the values are
//! the values themselves (`lemma_exact`), and the walk's test passes
//! (`lemma_span_ok`). The bound an edit starts from, the least and
//! greatest running sum in the index's root aggregate, holds the
//! column's values ([`root_bound`]). A [`SpanLedger`] is a cursor's
//! [`Ledger`] that also keeps the bound: every value the column holds
//! after the edit lies in the bound it ends with, so where that bound
//! passes the walk's test, so does the column (`SpanLedger::fits`).
//! Likewise a [`SpanPlan`] is a splice's [`SplicePlan`] that keeps the
//! bound, widened by the items it inserts: where it fits, so does the
//! column the splice leaves, given what `find_boundaries` reports around
//! it (`bounds_of`). And [`ToDeltas`] gives `from_values` the deltas
//! realizing its values and a bound on them.
//!
//! A splice's wiring is verified too: `ColumnCore::splice_delta` gives
//! its plan the column's deltas, a bound from the column's invariant and
//! what `d_boundaries` finds, and the cursor writes what the plan emits
//! (`ddomain::DeltaFeed`). Trusted, for an edit cursor ([`DeltaEdit`]'s
//! glue): that the ghost original deltas its [`SpanLedger`] is given are
//! the column's and, as for the [`Ledger`], that what the cursor reports
//! carrying, deleting and jumping over is what it did.
//!
//! [`DeltaEdit`]: crate::delta::DeltaEdit

#[cfg(verus_keep_ghost)]
use crate::btree::{max_i, min_i};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, lemma_push, pmax, pmin, psum, reps, vmax, vmin};
use crate::delta::dedit::{delta_to, Ledger, SplicePhase, SplicePlan};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{
    lemma_rest_unchanged, lemma_rz_cat, lemma_rz_crop, lemma_rz_one, run_vals, rz, splice_vals,
};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, dindex_ok, lemma_crop, lemma_crop_add, lemma_dfold, realize};
#[cfg(verus_keep_ghost)]
use crate::fenwick::total;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
use vstd::prelude::*;

verus! {

/// Every value of `vs` (a null is none) lies in `lo..=hi`.
pub(crate) open spec fn in_bound(vs: Seq<Option<i64>>, lo: int, hi: int) -> bool {
    forall|i: int| 0 <= i < vs.len() && (#[trigger] vs[i]) is Some ==> lo <= vs[i]->0 <= hi
}

/// A bound that still says something: empty (`lo > hi`, no value seen)
/// or within `i64`. Widened past `i64`, it bounds nothing.
pub(crate) open spec fn narrow(lo: int, hi: int) -> bool {
    lo > hi || (i64::MIN <= lo && hi <= i64::MAX)
}

/// The walk's test (`AnySpan::fits`): the values of deltas `ds` lie in
/// `min..=max`, within a range less than 2^63 wide.
pub(crate) open spec fn span_ok(ds: Seq<Option<i64>>, min: int, max: int) -> bool {
    vmin(ds) > vmax(ds) || (vmax(ds) - vmin(ds) <= i64::MAX && min <= vmin(ds) && vmax(ds) <= max)
}

/// Values in a bound stay in a wider one.
pub(crate) proof fn lemma_widen(vs: Seq<Option<i64>>, lo: int, hi: int, lo2: int, hi2: int)
    requires
        in_bound(vs, lo, hi),
        lo2 <= lo,
        hi <= hi2,
    ensures
        in_bound(vs, lo2, hi2),
{
}

/// Both parts of values in a bound are.
pub(crate) proof fn lemma_cat(a: Seq<Option<i64>>, b: Seq<Option<i64>>, lo: int, hi: int)
    ensures
        in_bound(a + b, lo, hi) == (in_bound(a, lo, hi) && in_bound(b, lo, hi)),
{
    if in_bound(a, lo, hi) && in_bound(b, lo, hi) {
        assert forall|i: int| 0 <= i < (a + b).len() && (#[trigger] (a + b)[i]) is Some implies lo <= (a
            + b)[i]->0 <= hi by {
            if i >= a.len() {
                assert((a + b)[i] == b[i - a.len()]);
            }
        }
    }
    if in_bound(a + b, lo, hi) {
        assert forall|i: int| 0 <= i < a.len() && (#[trigger] a[i]) is Some implies lo <= a[i]->0 <= hi by {
            assert((a + b)[i] == a[i]);
        }
        assert forall|i: int| 0 <= i < b.len() && (#[trigger] b[i]) is Some implies lo <= b[i]->0 <= hi by {
            assert((a + b)[i + a.len()] == b[i]);
        }
    }
}

/// The values of a run lie between its first and its last, where those
/// lie in a bound inside `i64`: the sums move one way and don't wrap.
pub(crate) proof fn lemma_run(p: int, d: i64, n: nat, lo: int, hi: int)
    requires
        n >= 1,
        i64::MIN <= lo,
        hi <= i64::MAX,
        lo <= p + d <= hi,
        lo <= p + n * d <= hi,
    ensures
        in_bound(run_vals(p, d, n), lo, hi),
{
    let vs = run_vals(p, d, n);
    assert forall|k: int| 0 <= k < vs.len() && (#[trigger] vs[k]) is Some implies lo <= vs[k]->0 <= hi by {
        let x = p + (k + 1) * d;
        assert(lo <= x <= hi) by (nonlinear_arith)
            requires
                x == p + (k + 1) * d,
                0 <= k < n,
                lo <= p + d <= hi,
                lo <= p + n * d <= hi,
        ;
        lemma_crop(x);
    }
}

/// Where every value deltas `ds` realize lies in `lo..=hi`, a bound
/// inside `i64` less than 2^63 wide, no running sum wraps: the sum at
/// each value is the value, and the running sum after them is the last
/// (or 0 if there is none).
pub(crate) proof fn lemma_exact(ds: Seq<Option<i64>>, lo: int, hi: int)
    requires
        lo > hi || (i64::MIN <= lo && hi <= i64::MAX && hi - lo <= i64::MAX),
        in_bound(rz(0, ds), lo, hi),
    ensures
        (vmin(ds) == i128::MAX && vmax(ds) == i128::MIN && psum(ds) == 0) || (lo <= vmin(ds) && vmax(ds) <= hi
            && lo <= psum(ds) <= hi),
    decreases ds.len(),
{
    if ds.len() > 0 {
        let n = ds.len() - 1;
        let d0 = ds.drop_last();
        assert(ds.take(n) =~= d0);
        assert(in_bound(rz(0, d0), lo, hi)) by {
            assert forall|i: int| 0 <= i < rz(0, d0).len() && (#[trigger] rz(0, d0)[i]) is Some implies lo <= rz(
                0,
                d0,
            )[i]->0 <= hi by {
                assert(ds.take(i) =~= d0.take(i));
                assert(rz(0, ds)[i] == rz(0, d0)[i]);
            }
        }
        lemma_exact(d0, lo, hi);
        if let Some(x) = ds.last() {
            let p = psum(d0);
            let v = rz(0, ds)[n];
            assert(v == Some(crate::delta::diter::crop(p + x)));
            let w = v->0 as int;
            assert(lo <= w <= hi);
            // the sum and the value agree modulo 2^64 and differ by less
            lemma_crop(p + x);
            let r = 0x1_0000_0000_0000_0000int;
            assert((p + x) % r == w % r);
            assert(p + x == w) by {
                assert(-r < (p + x) - w < r);
                vstd::arithmetic::div_mod::lemma_fundamental_div_mod(p + x, r);
                vstd::arithmetic::div_mod::lemma_fundamental_div_mod(w, r);
                let q = (p + x) / r - w / r;
                assert((p + x) - w == q * r);
                assert(q == 0) by (nonlinear_arith)
                    requires
                        (p + x) - w == q * r,
                        -r < (p + x) - w < r,
                        r == 0x1_0000_0000_0000_0000int,
                ;
            }
        }
    }
}

/// The walk's test passes where the values lie in a bound that would pass
/// it too.
pub(crate) proof fn lemma_span_ok(ds: Seq<Option<i64>>, lo: int, hi: int, min: i64, max: i64)
    requires
        lo > hi || (min <= lo && hi <= max && hi - lo <= i64::MAX),
        in_bound(rz(0, ds), lo, hi),
    ensures
        span_ok(ds, min as int, max as int),
{
    lemma_exact(ds, lo, hi);
}

/// The values of a prefix of deltas are a prefix of their values.
proof fn lemma_take(orig: Seq<Option<i64>>, k: int, lo: int, hi: int)
    requires
        0 <= k <= orig.len(),
        in_bound(rz(0, orig), lo, hi),
    ensures
        in_bound(rz(0, orig.take(k)), lo, hi),
        in_bound(rz(0, orig).skip(k), lo, hi),
        rz(0, orig).skip(k) == rz(psum(orig.take(k)), orig.skip(k)),
{
    assert(orig.take(k) + orig.skip(k) =~= orig);
    lemma_rz_cat(0, orig.take(k), orig.skip(k));
    lemma_cat(rz(0, orig.take(k)), rz(psum(orig.take(k)), orig.skip(k)), lo, hi);
    assert(rz(0, orig).skip(k) =~= rz(psum(orig.take(k)), orig.skip(k)));
}

/// The values of the original deltas `x` that follow `read`, a prefix of
/// `orig`: some of `orig`'s.
proof fn lemma_next(orig: Seq<Option<i64>>, read: Seq<Option<i64>>, x: Seq<Option<i64>>, lo: int, hi: int)
    requires
        read.len() + x.len() <= orig.len(),
        read == orig.take(read.len() as int),
        x == orig.subrange(read.len() as int, (read.len() + x.len()) as int),
    ensures
        in_bound(rz(0, orig), lo, hi) ==> in_bound(rz(psum(read), x), lo, hi),
        read + x == orig.take((read.len() + x.len()) as int),
{
    let k = (read.len() + x.len()) as int;
    assert(read + x =~= orig.take(k));
    if in_bound(rz(0, orig), lo, hi) {
        lemma_take(orig, k, lo, hi);
        lemma_rz_cat(0, read, x);
        lemma_cat(rz(0, read), rz(psum(read), x), lo, hi);
    }
}

/// A delta cursor's [`Ledger`], and a bound on the values of the column it
/// writes (`lo..=hi`, `i128` so that a run's last value can't overflow;
/// kept only where the values could span 2^63, `tracking`).
pub(crate) struct SpanLedger {
    pub(crate) l: Ledger,
    pub(crate) lo: i128,
    pub(crate) hi: i128,
    /// the column's original deltas, all of them
    #[allow(dead_code)]
    pub(crate) orig: Ghost<Seq<Option<i64>>>,
    #[allow(dead_code)]
    pub(crate) tracking: Ghost<bool>,
}

impl SpanLedger {
    pub(crate) open spec fn inv(&self) -> bool {
        let (orig, read) = (self.orig@, self.l.read@);
        &&& self.l.inv()
        &&& read.len() <= orig.len() && read == orig.take(read.len() as int)
        &&& self.l.closed@ ==> read.len() == orig.len()
        &&& self.tracking@ && narrow(self.lo as int, self.hi as int) ==> {
            &&& in_bound(rz(0, self.l.written@), self.lo as int, self.hi as int)
            &&& in_bound(rz(0, orig), self.lo as int, self.hi as int)
        }
    }

    /// The original deltas the cursor has yet to pass.
    pub(crate) open spec fn rest(&self) -> Seq<Option<i64>> {
        self.orig@.skip(self.l.read@.len() as int)
    }

    /// A cursor with `before`, the first of the original deltas `orig`,
    /// behind it, untouched (see [`Ledger::new`]); `lo..=hi` holds the
    /// values of `orig` (if `track`).
    #[allow(unused_variables)] // `track` is for the spec
    #[inline(always)]
    pub(crate) fn new(
        running: i64,
        lo: i128,
        hi: i128,
        track: bool,
        Ghost(before): Ghost<Seq<Option<i64>>>,
        Ghost(orig): Ghost<Seq<Option<i64>>>,
    ) -> (s: Self)
        requires
            running == crate::delta::diter::crop(psum(before)),
            before.len() <= orig.len() && before == orig.take(before.len() as int),
            track && narrow(lo as int, hi as int) ==> in_bound(rz(0, orig), lo as int, hi as int),
        ensures
            s.inv(),
            s.tracking@ == track,
            s.l.adjust == 0,
            !s.l.closed@,
            s.orig@ == orig,
            s.l.read@ == before,
    {
        proof {
            if track && narrow(lo as int, hi as int) {
                lemma_take(orig, before.len() as int, lo as int, hi as int);
            }
        }
        SpanLedger { l: Ledger::new(running, Ghost(before)), lo, hi, orig: Ghost(orig), tracking: Ghost(track) }
    }

    /// See [`Ledger::next_value`].
    #[inline(always)]
    pub(crate) fn next_value(&self, d: i64) -> (v: i64)
        requires
            self.inv(),
            !self.l.closed@,
    {
        self.l.next_value(d)
    }

    /// Carry the next `n` original items, of delta `d` (see
    /// [`Ledger::carry`]).
    #[inline(always)]
    pub(crate) fn carry(&mut self, d: Option<i64>, n: usize)
        requires
            old(self).inv(),
            !old(self).l.closed@,
            old(self).l.adjust == 0 || d is None,
            old(self).l.read@.len() + n <= old(self).orig@.len(),
            old(self).orig@.subrange(old(self).l.read@.len() as int, old(self).l.read@.len() + n) == reps(n as nat, d),
        ensures
            final(self).inv(),
            !final(self).l.closed@,
            final(self).l.adjust == old(self).l.adjust,
            final(self).tracking == old(self).tracking,
    {
        let ghost (w0, r0, orig) = (self.l.written@, self.l.read@, self.orig@);
        self.l.carry(d, n);
        proof {
            lemma_next(orig, r0, reps(n as nat, d), self.lo as int, self.hi as int);
            if self.tracking@ && narrow(self.lo as int, self.hi as int) {
                lemma_cat(rz(0, w0), rz(psum(r0), reps(n as nat, d)), self.lo as int, self.hi as int);
            }
        }
    }

    /// The cursor jumped over the next original deltas `x` (see
    /// [`Ledger::jump`]).
    #[inline(always)]
    pub(crate) fn jump(&mut self, running: i64, Ghost(x): Ghost<Seq<Option<i64>>>)
        requires
            old(self).inv(),
            !old(self).l.closed@,
            old(self).l.adjust == 0,
            running == crate::delta::diter::crop(psum(old(self).l.written@ + x)),
            old(self).l.read@.len() + x.len() <= old(self).orig@.len(),
            old(self).orig@.subrange(old(self).l.read@.len() as int, (old(self).l.read@.len() + x.len()) as int) == x,
        ensures
            final(self).inv(),
            !final(self).l.closed@,
            final(self).l.adjust == 0,
            final(self).tracking == old(self).tracking,
    {
        let ghost (w0, r0, orig) = (self.l.written@, self.l.read@, self.orig@);
        self.l.jump(running, Ghost(x));
        proof {
            lemma_next(orig, r0, x, self.lo as int, self.hi as int);
            if self.tracking@ && narrow(self.lo as int, self.hi as int) {
                lemma_cat(rz(0, w0), rz(psum(r0), x), self.lo as int, self.hi as int);
            }
        }
    }

    /// Insert `n` copies of the value `v` (see [`Ledger::insert_value`]);
    /// it widens the bound.
    #[inline(always)]
    pub(crate) fn insert_value(&mut self, v: i64, n: usize, track: bool) -> (d: i64)
        requires
            old(self).inv(),
            n >= 1,
            track == old(self).tracking@,
        ensures
            final(self).inv(),
            final(self).l.closed@ == old(self).l.closed@,
            final(self).tracking == old(self).tracking,
    {
        let ghost (w0, lo0, hi0) = (self.l.written@, self.lo as int, self.hi as int);
        let d = self.l.insert_value(v, n);
        if track {
            if (v as i128) < self.lo {
                self.lo = v as i128;
            }
            if (v as i128) > self.hi {
                self.hi = v as i128;
            }
            proof {
                let (lo, hi) = (self.lo as int, self.hi as int);
                if narrow(lo, hi) {
                    assert(narrow(lo0, hi0));
                    lemma_widen(rz(0, w0), lo0, hi0, lo, hi);
                    lemma_widen(rz(0, self.orig@), lo0, hi0, lo, hi);
                    lemma_cat(rz(0, w0), reps(n as nat, Some(v)), lo, hi);
                }
            }
        }
        d
    }

    /// Insert `n` nulls (see [`Ledger::insert_nulls`]).
    #[inline(always)]
    pub(crate) fn insert_nulls(&mut self, n: usize)
        requires
            old(self).inv(),
        ensures
            final(self).inv(),
            final(self).l.closed@ == old(self).l.closed@,
            final(self).l.adjust == old(self).l.adjust,
            final(self).tracking == old(self).tracking,
    {
        let ghost w0 = self.l.written@;
        self.l.insert_nulls(n);
        proof {
            if self.tracking@ && narrow(self.lo as int, self.hi as int) {
                lemma_cat(rz(0, w0), reps(n as nat, None), self.lo as int, self.hi as int);
            }
        }
    }

    /// Insert a run of `n` deltas `d` after the running sum `prefix` (see
    /// [`Ledger::insert_run`]); its first and last values widen the bound.
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn insert_run(&mut self, prefix: i64, d: i64, n: usize, track: bool) -> (first: i64)
        requires
            old(self).inv(),
            n >= 1,
            track == old(self).tracking@,
        ensures
            final(self).inv(),
            final(self).l.closed@ == old(self).l.closed@,
            final(self).tracking == old(self).tracking,
            prefix == old(self).l.running ==> first == d,
    {
        let ghost (w0, lo0, hi0) = (self.l.written@, self.lo as int, self.hi as int);
        let first = self.l.insert_run(prefix, d, n);
        if track {
            let p = prefix as i128;
            let a = p + d as i128;
            // `d * n` and `p + d * n` fit an `i128`
            proof {
                assert(-0x8000_0000_0000_0000int * 0xffff_ffff_ffff_ffffint <= (d as int) * (n as int)
                    <= 0x7fff_ffff_ffff_ffffint * 0xffff_ffff_ffff_ffffint) by (nonlinear_arith)
                    requires
                        i64::MIN <= d <= i64::MAX,
                        0 <= n <= 0xffff_ffff_ffff_ffffint,
                ;
            }
            let b = p + (d as i128) * (n as i128);
            if a < self.lo {
                self.lo = a;
            }
            if a > self.hi {
                self.hi = a;
            }
            if b < self.lo {
                self.lo = b;
            }
            if b > self.hi {
                self.hi = b;
            }
            proof {
                let (lo, hi) = (self.lo as int, self.hi as int);
                if narrow(lo, hi) {
                    assert(narrow(lo0, hi0));
                    lemma_widen(rz(0, w0), lo0, hi0, lo, hi);
                    lemma_widen(rz(0, self.orig@), lo0, hi0, lo, hi);
                    assert((d as int) * (n as int) == (n as int) * (d as int)) by (nonlinear_arith);
                    assert(b as int == prefix as int + (n as nat) * d);
                    lemma_run(prefix as int, d, n as nat, lo, hi);
                    lemma_cat(rz(0, w0), run_vals(prefix as int, d, n as nat), lo, hi);
                }
            }
        }
        first
    }

    /// Delete the next original deltas `ds`, whose sum is `sum` (see
    /// [`Ledger::delete`]).
    #[inline(always)]
    pub(crate) fn delete(&mut self, sum: i64, Ghost(ds): Ghost<Seq<Option<i64>>>)
        requires
            old(self).inv(),
            !old(self).l.closed@,
            sum == crate::delta::diter::crop(psum(ds)),
            old(self).l.read@.len() + ds.len() <= old(self).orig@.len(),
            old(self).orig@.subrange(old(self).l.read@.len() as int, (old(self).l.read@.len() + ds.len()) as int) == ds,
        ensures
            final(self).inv(),
            !final(self).l.closed@,
            final(self).tracking == old(self).tracking,
    {
        let ghost (r0, orig) = (self.l.read@, self.orig@);
        self.l.delete(sum, Ghost(ds));
        proof {
            let k = (r0.len() + ds.len()) as int;
            assert(r0 + ds =~= orig.take(k));
        }
    }

    /// Pay the debt into the next original delta `d` (see [`Ledger::pay`]).
    #[inline(always)]
    pub(crate) fn pay(&mut self, d: i64) -> (nd: i64)
        requires
            old(self).inv(),
            !old(self).l.closed@,
            old(self).l.read@.len() < old(self).orig@.len(),
            old(self).orig@[old(self).l.read@.len() as int] == Some(d),
        ensures
            final(self).inv(),
            !final(self).l.closed@,
            final(self).l.adjust == 0,
            final(self).tracking == old(self).tracking,
    {
        let ghost (w0, r0, orig) = (self.l.written@, self.l.read@, self.orig@);
        let nd = self.l.pay(d);
        proof {
            let x = seq![Some(d)];
            assert(orig.subrange(r0.len() as int, r0.len() + 1int) =~= x);
            lemma_next(orig, r0, x, self.lo as int, self.hi as int);
            assert(r0.push(Some(d)) =~= r0 + x);
            if self.tracking@ && narrow(self.lo as int, self.hi as int) {
                lemma_cat(rz(0, w0), rz(psum(r0), x), self.lo as int, self.hi as int);
            }
        }
        nd
    }

    /// The cursor has passed every original delta (see [`Ledger::close`]).
    #[inline(always)]
    pub(crate) fn close(&mut self)
        requires
            old(self).inv(),
            old(self).l.read@.len() == old(self).orig@.len(),
        ensures
            final(self).inv(),
            final(self).l.closed@,
            final(self).l.adjust == 0,
            final(self).tracking == old(self).tracking,
    {
        self.l.close();
    }

    /// Whether the bound shows the values of `T`'s domain `min..=max` lie
    /// within a range less than 2^63 wide; where it does, so do those of
    /// the column the cursor leaves once its debt is paid: what it wrote,
    /// then the original deltas it hasn't passed.
    #[inline(always)]
    pub(crate) fn fits(&self, min: i64, max: i64) -> (r: bool)
        requires
            self.inv(),
            self.tracking@,
            self.l.closed@ || self.l.adjust == 0,
        ensures
            r ==> span_ok(self.l.written@ + self.rest(), min as int, max as int),
    {
        let r = self.lo > self.hi || (self.lo >= min as i128 && self.hi <= max as i128 && self.hi - self.lo
            <= i64::MAX as i128);
        proof {
            if r {
                let (orig, read, written) = (self.orig@, self.l.read@, self.l.written@);
                let (lo, hi) = (self.lo as int, self.hi as int);
                let rest = self.rest();
                lemma_rest_unchanged(self.l, rest);
                assert(read + rest =~= orig);
                lemma_take(orig, read.len() as int, lo, hi);
                lemma_cat(rz(0, written), rz(0, read + rest).skip(read.len() as int), lo, hi);
                lemma_span_ok(written + rest, lo, hi, min, max);
            }
        }
        r
    }
}

/// The column a splice leaves: the original deltas before it (`a`), the
/// deltas the plan emitted (`e`), then the original deltas after the
/// deleted ones (`tail`), less the nulls and the item the plan wrote
/// again if one followed them.
pub(crate) open spec fn spliced(
    a: Seq<Option<i64>>,
    e: Seq<Option<i64>>,
    tail: Seq<Option<i64>>,
    skip: nat,
    next_val: Option<i64>,
) -> Seq<Option<i64>> {
    a + e + if next_val is Some { tail.skip(skip + 1int) } else { tail }
}

/// What `find_boundaries` reports of the original deltas `a + x + tail`
/// around a splice deleting `x` after `a`: the running sum before it
/// (`prev`), the nulls after the deleted items (`skip`), and the value of
/// the item after those (`next_val`), or none if only nulls follow.
pub(crate) open spec fn bounds_of(
    a: Seq<Option<i64>>,
    x: Seq<Option<i64>>,
    tail: Seq<Option<i64>>,
    prev: int,
    skip: nat,
    next_val: Option<i64>,
) -> bool {
    &&& crop(psum(a)) == prev
    &&& match next_val {
        Some(v) => {
            &&& tail.len() > skip
            &&& tail.take(skip as int) == reps(skip, None)
            &&& tail[skip as int] is Some
            &&& v == crop(psum(a + x + tail.take(skip + 1int)))
        },
        None => forall|i: int| 0 <= i < tail.len() ==> (#[trigger] tail[i]) is None,
    }
}

/// Nulls realize no values.
proof fn lemma_no_values(p: int, ds: Seq<Option<i64>>, lo: int, hi: int)
    requires
        forall|i: int| 0 <= i < ds.len() ==> (#[trigger] ds[i]) is None,
    ensures
        in_bound(rz(p, ds), lo, hi),
{
    assert forall|i: int| 0 <= i < rz(p, ds).len() && (#[trigger] rz(p, ds)[i]) is Some implies lo <= rz(p, ds)[i]->0
        <= hi by {
        assert(ds[i] is None);
    }
}

/// The deltas a splice ends with land on the value of the item after it.
proof fn lemma_lands(prev: int, e: Seq<Option<i64>>, wanted: Seq<Option<i64>>, skip: nat, v: i64)
    requires
        rz(prev, e) == splice_vals(wanted, skip, Some(v)),
    ensures
        crop(prev + psum(e)) == v,
{
    let n = e.len() - 1;
    let vals = splice_vals(wanted, skip, Some(v));
    assert(vals[vals.len() - 1] == Some(v));
    assert(rz(prev, e)[n] == realize(prev + psum(e.take(n)), e[n]));
    assert(e.take(n) =~= e.drop_last());
    assert(e[n] == e.last());
}

/// Where the original values, and the values a splice inserted, lie in a
/// bound, so do those of the column it leaves.
#[verifier::spinoff_prover]
proof fn lemma_spliced(
    orig: Seq<Option<i64>>,
    a: Seq<Option<i64>>,
    x: Seq<Option<i64>>,
    tail: Seq<Option<i64>>,
    e: Seq<Option<i64>>,
    wanted: Seq<Option<i64>>,
    prev: i64,
    skip: nat,
    next_val: Option<i64>,
    lo: int,
    hi: int,
)
    requires
        orig == a + x + tail,
        bounds_of(a, x, tail, prev as int, skip, next_val),
        rz(prev as int, e) == splice_vals(wanted, skip, next_val),
        in_bound(rz(0, orig), lo, hi),
        in_bound(wanted, lo, hi),
    ensures
        in_bound(rz(0, spliced(a, e, tail, skip, next_val)), lo, hi),
{
    let r = if next_val is Some { tail.skip(skip + 1int) } else { tail };
    assert(spliced(a, e, tail, skip, next_val) =~= a + (e + r));
    // before the splice: the original values
    assert(orig.take(a.len() as int) =~= a);
    lemma_take(orig, a.len() as int, lo, hi);
    lemma_rz_cat(0, a, e + r);
    lemma_rz_cat(psum(a), e, r);
    // what the plan emitted: the items' values, then the nulls and the
    // value after them
    lemma_crop(prev as int);
    lemma_rz_crop(psum(a), prev as int, e);
    lemma_crop_add(psum(a), psum(e));
    match next_val {
        Some(v) => {
            let k = (a.len() + x.len() + skip + 1) as int;
            assert(a + x + tail.take(skip + 1int) =~= orig.take(k));
            // the value after them is an original one
            assert(orig.take(k).drop_last() =~= orig.take(k - 1));
            assert(orig[k - 1] == tail[skip as int]);
            assert(rz(0, orig)[k - 1] == realize(psum(orig.take(k - 1)), orig[k - 1]));
            assert(rz(0, orig)[k - 1] == Some(v));
            assert(in_bound(seq![Some(v)], lo, hi));
            crate::delta::dedit::lemma_rz_nulls(0, skip);
            lemma_no_values(0, reps(skip, None), lo, hi);
            lemma_cat(wanted, reps(skip, None), lo, hi);
            lemma_cat(wanted + reps(skip, None), seq![Some(v)], lo, hi);
            // and after it, the original values, the running sums agreeing
            lemma_lands(prev as int, e, wanted, skip, v);
            assert(tail.skip(skip + 1int) =~= orig.skip(k));
            lemma_take(orig, k, lo, hi);
            lemma_rz_crop(psum(a) + psum(e), psum(orig.take(k)), r);
        },
        None => {
            lemma_no_values(psum(a) + psum(e), r, lo, hi);
        },
    }
    lemma_cat(rz(psum(a), e), rz(psum(a) + psum(e), r), lo, hi);
    lemma_cat(rz(0, a), rz(psum(a), e + r), lo, hi);
}

/// A splice's [`SplicePlan`], and a bound on the values of the column it
/// writes, as for [`SpanLedger`].
pub(crate) struct SpanPlan {
    pub(crate) p: SplicePlan,
    pub(crate) lo: i128,
    pub(crate) hi: i128,
    /// the column's original deltas, all of them
    #[allow(dead_code)]
    pub(crate) orig: Ghost<Seq<Option<i64>>>,
    #[allow(dead_code)]
    pub(crate) tracking: Ghost<bool>,
}

impl SpanPlan {
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.p.inv()
        &&& self.tracking@ && narrow(self.lo as int, self.hi as int) ==> {
            &&& in_bound(self.p.wanted@, self.lo as int, self.hi as int)
            &&& in_bound(rz(0, self.orig@), self.lo as int, self.hi as int)
        }
    }

    /// The same splice of the same column: what the plan was made with.
    pub(crate) open spec fn fixed(&self, o: Self) -> bool {
        &&& self.p.prev == o.p.prev
        &&& self.p.skip == o.p.skip
        &&& self.p.next_val == o.p.next_val
        &&& self.orig == o.orig
        &&& self.tracking == o.tracking
    }

    /// A plan (see [`SplicePlan::new`]) for a column of original deltas
    /// `orig`, whose values `lo..=hi` holds (if `track`).
    #[allow(unused_variables)] // `track` is for the spec
    #[inline(always)]
    pub(crate) fn new(
        prev: i64,
        skip: usize,
        next_val: Option<i64>,
        lo: i128,
        hi: i128,
        track: bool,
        Ghost(orig): Ghost<Seq<Option<i64>>>,
    ) -> (s: Self)
        requires
            track && narrow(lo as int, hi as int) ==> in_bound(rz(0, orig), lo as int, hi as int),
        ensures
            s.inv(),
            s.p.phase is Values,
            s.tracking@ == track,
            s.p.prev@ == prev as int,
            s.p.skip == skip,
            s.p.next_val == next_val,
            s.p.emitted@.len() == 0,
            s.orig@ == orig,
    {
        SpanPlan {
            p: SplicePlan::new(prev, skip, next_val),
            lo,
            hi,
            orig: Ghost(orig),
            tracking: Ghost(track),
        }
    }

    /// See [`SplicePlan::take_pending`].
    #[inline(always)]
    pub(crate) fn take_pending(&mut self) -> (r: Option<(Option<i64>, usize)>)
        requires
            old(self).inv(),
            old(self).p.phase is Values,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.phase is Values,
            final(self).p.pending is None,
            final(self).p.emitted@ == old(self).p.emitted@ + match r {
                Some((x, n)) => reps(n as nat, x),
                None => Seq::empty(),
            },
    {
        self.p.take_pending()
    }

    /// The next item is the value `v` (see [`SplicePlan::value`]); it
    /// widens the bound.
    #[inline(always)]
    pub(crate) fn value(&mut self, v: Option<i64>, track: bool) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).p.phase is Values,
            old(self).p.pending is None,
            track == old(self).tracking@,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.phase is Values,
            final(self).p.emitted@ == old(self).p.emitted@ + reps(r.1 as nat, r.0),
    {
        let ghost (w0, lo0, hi0) = (self.p.wanted@, self.lo as int, self.hi as int);
        let r = self.p.value(v);
        if track {
            if let Some(x) = v {
                if (x as i128) < self.lo {
                    self.lo = x as i128;
                }
                if (x as i128) > self.hi {
                    self.hi = x as i128;
                }
            }
            proof {
                let (lo, hi) = (self.lo as int, self.hi as int);
                if narrow(lo, hi) {
                    assert(narrow(lo0, hi0));
                    lemma_widen(w0, lo0, hi0, lo, hi);
                    lemma_widen(rz(0, self.orig@), lo0, hi0, lo, hi);
                    assert(w0.push(v) =~= w0 + seq![v]);
                    lemma_cat(w0, seq![v], lo, hi);
                }
            }
        }
        r
    }

    /// The next item is a run (see [`SplicePlan::run`]); its first and
    /// last values widen the bound.
    #[verifier::spinoff_prover]
    #[inline(always)]
    pub(crate) fn run(&mut self, prefix: i64, d: Option<i64>, count: usize, track: bool) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).p.phase is Values,
            old(self).p.pending is None,
            count >= 1,
            track == old(self).tracking@,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.phase is Values,
            final(self).p.emitted@ == old(self).p.emitted@ + reps(r.1 as nat, r.0),
    {
        let ghost (w0, lo0, hi0) = (self.p.wanted@, self.lo as int, self.hi as int);
        let r = self.p.run(prefix, d, count);
        if track {
            if let Some(x) = d {
                let p = prefix as i128;
                let a = p + x as i128;
                proof {
                    assert(-0x8000_0000_0000_0000int * 0xffff_ffff_ffff_ffffint <= (x as int) * (count as int)
                        <= 0x7fff_ffff_ffff_ffffint * 0xffff_ffff_ffff_ffffint) by (nonlinear_arith)
                        requires
                            i64::MIN <= x <= i64::MAX,
                            0 <= count <= 0xffff_ffff_ffff_ffffint,
                    ;
                }
                let b = p + (x as i128) * (count as i128);
                if a < self.lo {
                    self.lo = a;
                }
                if a > self.hi {
                    self.hi = a;
                }
                if b < self.lo {
                    self.lo = b;
                }
                if b > self.hi {
                    self.hi = b;
                }
                proof {
                    let (lo, hi) = (self.lo as int, self.hi as int);
                    if narrow(lo, hi) {
                        assert(narrow(lo0, hi0));
                        lemma_widen(w0, lo0, hi0, lo, hi);
                        lemma_widen(rz(0, self.orig@), lo0, hi0, lo, hi);
                        assert((x as int) * (count as int) == (count as int) * (x as int)) by (nonlinear_arith);
                        assert(b as int == prefix as int + (count as nat) * x);
                        lemma_run(prefix as int, x, count as nat, lo, hi);
                        lemma_cat(w0, run_vals(prefix as int, x, count as nat), lo, hi);
                    }
                }
            } else {
                proof {
                    let (lo, hi) = (self.lo as int, self.hi as int);
                    if narrow(lo, hi) {
                        crate::delta::dedit::lemma_rz_nulls(0, count as nat);
                        lemma_no_values(0, reps(count as nat, None), lo, hi);
                        lemma_cat(w0, reps(count as nat, None), lo, hi);
                    }
                }
            }
        }
        r
    }

    /// See [`SplicePlan::end_values`].
    #[inline(always)]
    pub(crate) fn end_values(&mut self)
        requires
            old(self).inv(),
            old(self).p.phase is Values,
            old(self).p.pending is None,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.emitted == old(self).p.emitted,
    {
        self.p.end_values()
    }

    /// See [`SplicePlan::nulls`].
    #[inline(always)]
    pub(crate) fn nulls(&mut self) -> (r: Option<(Option<i64>, usize)>)
        requires
            old(self).inv(),
            old(self).p.phase is NullRun,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.emitted@ == old(self).p.emitted@ + match r {
                Some((x, n)) => reps(n as nat, x),
                None => Seq::empty(),
            },
    {
        self.p.nulls()
    }

    /// See [`SplicePlan::boundary`].
    #[inline(always)]
    pub(crate) fn boundary(&mut self) -> (r: (Option<i64>, usize))
        requires
            old(self).inv(),
            old(self).p.phase is Boundary,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).fixed(*old(self)),
            final(self).p.phase is Done,
            final(self).p.emitted@ == old(self).p.emitted@ + reps(r.1 as nat, r.0),
    {
        self.p.boundary()
    }

    /// Whether the bound shows the values lie in `min..=max`, within a
    /// range less than 2^63 wide, once the plan is done; where it does, so
    /// do those of the column the splice leaves.
    #[inline(always)]
    pub(crate) fn fits(&self, min: i64, max: i64) -> (r: bool)
        requires
            self.inv(),
            self.tracking@,
        ensures
            r ==> forall|a: Seq<Option<i64>>, x: Seq<Option<i64>>, tail: Seq<Option<i64>>|
                #![trigger bounds_of(a, x, tail, self.p.prev@, self.p.skip as nat, self.p.next_val)]
                self.orig@ == a + x + tail && bounds_of(a, x, tail, self.p.prev@, self.p.skip as nat, self.p.next_val)
                    ==> span_ok(
                    spliced(a, self.p.emitted@, tail, self.p.skip as nat, self.p.next_val),
                    min as int,
                    max as int,
                ),
    {
        // a plan cut short has written only some of the deltas
        match self.p.phase {
            SplicePhase::Done => {},
            _ => return false,
        }
        let r = self.lo > self.hi || (self.lo >= min as i128 && self.hi <= max as i128 && self.hi - self.lo
            <= i64::MAX as i128);
        proof {
            if r {
                let (lo, hi) = (self.lo as int, self.hi as int);
                let (e, wanted, skip, next_val) = (self.p.emitted@, self.p.wanted@, self.p.skip as nat, self.p.next_val);
                assert forall|a: Seq<Option<i64>>, x: Seq<Option<i64>>, tail: Seq<Option<i64>>|
                    #![trigger bounds_of(a, x, tail, self.p.prev@, skip, next_val)]
                    self.orig@ == a + x + tail && bounds_of(a, x, tail, self.p.prev@, skip, next_val)
                    implies span_ok(spliced(a, e, tail, skip, next_val), min as int, max as int) by {
                    let prev = crop(self.p.prev@);
                    lemma_crop(self.p.prev@);
                    lemma_spliced(self.orig@, a, x, tail, e, wanted, prev, skip, next_val, lo, hi);
                    lemma_span_ok(spliced(a, e, tail, skip, next_val), lo, hi, min, max);
                }
            }
        }
        r
    }
}

/// Values turned into the deltas that realize them, one at a time
/// (`from_values`), with a bound on the values, as for [`SpanLedger`]
/// (`i64`: it holds only values).
pub(crate) struct ToDeltas {
    pub(crate) running: i64,
    pub(crate) lo: i64,
    pub(crate) hi: i64,
    /// the values taken, and the deltas given for them
    #[allow(dead_code)]
    pub(crate) vals: Ghost<Seq<Option<i64>>>,
    #[allow(dead_code)]
    pub(crate) ds: Ghost<Seq<Option<i64>>>,
    #[allow(dead_code)]
    pub(crate) tracking: Ghost<bool>,
}

impl ToDeltas {
    pub(crate) open spec fn inv(&self) -> bool {
        &&& self.running == crop(psum(self.ds@))
        &&& rz(0, self.ds@) == self.vals@
        &&& self.tracking@ ==> in_bound(self.vals@, self.lo as int, self.hi as int)
    }

    /// No values yet; keep the bound if `track`.
    #[allow(unused_variables)] // `track` is for the spec
    #[inline(always)]
    pub(crate) fn new(track: bool) -> (t: Self)
        ensures
            t.inv(),
            t.tracking@ == track,
            t.vals@.len() == 0,
            t.ds@.len() == 0,
    {
        proof {
            lemma_crop(0);
            assert(rz(0, Seq::<Option<i64>>::empty()) =~= Seq::<Option<i64>>::empty());
        }
        ToDeltas {
            running: 0,
            lo: i64::MAX,
            hi: i64::MIN,
            vals: Ghost(Seq::empty()),
            ds: Ghost(Seq::empty()),
            tracking: Ghost(track),
        }
    }

    /// The delta for the next value, `v` (a null for a null).
    #[inline(always)]
    pub(crate) fn next(&mut self, v: Option<i64>, track: bool) -> (d: Option<i64>)
        requires
            old(self).inv(),
            track == old(self).tracking@,
        ensures
            final(self).inv(),
            final(self).tracking == old(self).tracking,
            final(self).vals@ == old(self).vals@.push(v),
            final(self).ds@ == old(self).ds@.push(d),
    {
        let ghost (ds, vals, lo0, hi0) = (self.ds@, self.vals@, self.lo as int, self.hi as int);
        let d = match v {
            None => None,
            Some(x) => {
                let d = delta_to(self.running, x);
                self.running = x;
                if track {
                    if x < self.lo {
                        self.lo = x;
                    }
                    if x > self.hi {
                        self.hi = x;
                    }
                }
                Some(d)
            },
        };
        proof {
            let one = seq![d];
            assert(ds.push(d) =~= ds + one);
            lemma_rz_cat(0, ds, one);
            lemma_push(ds, d);
            match d {
                Some(x) => {
                    lemma_rz_one(psum(ds), x);
                    lemma_crop_add(psum(ds), x as int);
                },
                None => {
                    crate::delta::dedit::lemma_rz_nulls(psum(ds), 1);
                    assert(one =~= reps(1, None));
                },
            }
            assert(rz(0, ds.push(d)) =~= vals.push(v));
            self.ds = Ghost(ds.push(d));
            self.vals = Ghost(vals.push(v));
            if track {
                let (lo, hi) = (self.lo as int, self.hi as int);
                lemma_widen(vals, lo0, hi0, lo, hi);
                assert(vals.push(v) =~= vals + seq![v]);
                lemma_cat(vals, seq![v], lo, hi);
            }
        }
        d
    }

    /// Whether the bound shows the values lie in `min..=max`, within a
    /// range less than 2^63 wide; where it does, so do those the deltas
    /// given realize.
    #[inline(always)]
    pub(crate) fn fits(&self, min: i64, max: i64) -> (r: bool)
        requires
            self.inv(),
            self.tracking@,
        ensures
            r ==> span_ok(self.ds@, min as int, max as int),
    {
        let r = self.lo > self.hi || (self.lo >= min && self.hi <= max && (self.hi as i128) - (self.lo as i128)
            <= i64::MAX as i128);
        proof {
            if r {
                lemma_span_ok(self.ds@, self.lo as int, self.hi as int, min, max);
            }
        }
        r
    }
}

/// Every running sum through at least one of deltas `ds` lies between
/// their least and greatest.
pub(crate) proof fn lemma_psum_within(ds: Seq<Option<i64>>, k: int)
    requires
        1 <= k <= ds.len(),
    ensures
        pmin(ds) <= psum(ds.take(k)) <= pmax(ds),
    decreases ds.len(),
{
    if k == ds.len() {
        assert(ds.take(k) =~= ds);
        if ds.len() > 1 {
            assert(pmin(ds) == min_i(pmin(ds.drop_last()), psum(ds)));
            assert(pmax(ds) == max_i(pmax(ds.drop_last()), psum(ds)));
        }
    } else {
        let d0 = ds.drop_last();
        assert(d0.take(k) =~= ds.take(k));
        lemma_psum_within(d0, k);
        assert(pmin(ds) == min_i(pmin(d0), psum(ds)));
        assert(pmax(ds) == max_i(pmax(d0), psum(ds)));
    }
}

/// Where the running sums of deltas `ds` fit `i64`, their least and
/// greatest bound the values they realize.
proof fn lemma_sums_bound(ds: Seq<Option<i64>>)
    requires
        ds.len() > 0,
        i64::MIN <= pmin(ds),
        pmax(ds) <= i64::MAX,
    ensures
        in_bound(rz(0, ds), pmin(ds), pmax(ds)),
{
    assert forall|i: int| 0 <= i < rz(0, ds).len() && (#[trigger] rz(0, ds)[i]) is Some implies pmin(ds) <= rz(
        0,
        ds,
    )[i]->0 <= pmax(ds) by {
        let x = ds[i]->0;
        assert(ds.take(i + 1).drop_last() =~= ds.take(i));
        assert(ds.take(i + 1).last() == ds[i]);
        assert(psum(ds.take(i + 1)) == psum(ds.take(i)) + x);
        lemma_psum_within(ds, i + 1);
        lemma_crop(psum(ds.take(i)) + x);
    }
}

/// The bound an edit starts from: the least and greatest running sum of
/// the column's deltas, from the index's root aggregate. It holds the
/// column's values (and the 0 before the first, where nulls come first),
/// wherever it says anything: given the column's invariant (each slab's
/// weight its aggregate, the index valid), as the readers assume.
#[allow(unused_variables)] // `slabs` is for the spec
pub(crate) fn root_bound<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<SlabAgg>,
) -> (r: (i128, i128))
    requires
        dindex_ok::<X, B>(Some(ix), slabs@),
        lens_ok::<X>(slabs@),
        total(slab_lens(slabs@)) <= usize::MAX,
    ensures
        narrow(r.0 as int, r.1 as int) ==> in_bound(rz(0, deltas_of::<_, B>(vals_of::<X>(slabs@))), r.0 as int, r.1 as int),
{
    let agg = ix.root_agg();
    let ghost (s, ws) = (slabs@, ix@);
    let ghost ds = deltas_of::<_, B>(vals_of::<X>(s));
    proof {
        lemma_dfold::<X, B>(ws, s, s.len() as int);
        assert(ws.subrange(0, s.len() as int) =~= ws);
        assert(s.take(s.len() as int) =~= s);
    }
    if agg.len == 0 {
        proof {
            assert(rz(0, ds).len() == 0);
        }
        return (i128::MAX, i128::MIN);
    }
    proof {
        if narrow(agg.min_offset as int, agg.max_offset as int) {
            lemma_sums_bound(ds);
        }
    }
    (agg.min_offset as i128, agg.max_offset as i128)
}

} // verus!
