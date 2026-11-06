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
//! (`lemma_span_ok`). A [`SpanLedger`] is a cursor's [`Ledger`] that also
//! keeps the bound: given that the bound it starts from holds the
//! column's values, every value the column holds after the edit lies in
//! the bound it ends with, so where that bound passes the walk's test, so
//! does the column (`SpanLedger::fits`). Trusted: that the bound it
//! starts from (the index's root aggregate) holds the column's values,
//! and, as for the [`Ledger`], that what the cursor reports carrying,
//! deleting and jumping over is what it did.

#[cfg(verus_keep_ghost)]
use crate::delta::agg::{psum, reps, vmax, vmin};
use crate::delta::dedit::Ledger;
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{lemma_rest_unchanged, lemma_rz_cat, run_vals, rz};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{lemma_crop, realize};
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

} // verus!
