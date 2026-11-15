//! The delta encoder's transformation of values into deltas, verified
//! with Verus.
//!
//! [`DStream`] writes each value as its difference from the value before
//! (nulls as nulls) through the verified RLE stream (`RStream`). Once
//! finished, the deltas written realize exactly the values appended: the
//! running sum at each non-null delta is the value there. Values must lie
//! in the type's domain `lo..=hi` and, together, span less than 2^63 (the
//! `DeltaValue` domain): `finish` checks the span, and panics otherwise.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::btree::{max_i, min_i};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{dval, psum};
use crate::rle::enc::RStream;
use crate::rle::model::*;
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::encodes;
#[cfg(verus_keep_ghost)]
use crate::rle::state::copies;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A value as an integer.
pub open spec fn ival(v: Option<i64>) -> Option<int> {
    match v {
        Some(x) => Some(x as int),
        None => None,
    }
}

/// Values as integers.
pub open spec fn ivals(vs: Seq<Option<i64>>) -> Seq<Option<int>> {
    vs.map_values(|v: Option<i64>| ival(v))
}

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
proof fn lemma_vals_run(vs: Seq<Option<i64>>, n: nat, v: Option<i64>)
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

/// Deltas `ds`, then one more.
proof fn lemma_realize_push(ds: Seq<Option<i64>>, d: Option<i64>)
    ensures
        realize(ds.push(d)) == realize(ds).push(match d { Some(_) => Some(psum(ds) + dval(d)), None => None::<int> }),
        psum(ds.push(d)) == psum(ds) + dval(d),
{
    assert(ds.push(d).drop_last() =~= ds);
}

/// Values spanning less than 2^63 span less with fewer of them.
proof fn lemma_span_prefix(vs: Seq<Option<i64>>, n: nat, v: Option<i64>)
    requires n >= 1, span_ok(vs + copies(n, v)),
    ensures span_ok(vs),
{
    lemma_vals_run(vs, n, v);
    lemma_last_in(vs);
}

/// The integers of values, then `n` copies of `v`.
proof fn lemma_ivals_run(vs: Seq<Option<i64>>, n: nat, v: Option<i64>)
    ensures
        ivals(vs + copies(n, v)) == ivals(vs) + copies(n, ival(v)),
{
    assert(ivals(vs + copies(n, v)) =~= ivals(vs) + copies(n, ival(v)));
}

/// Values `vs`, then no more.
proof fn lemma_no_copies<V>(vs: Seq<V>, v: V)
    ensures
        vs + copies(0, v) == vs,
{
    assert(vs + copies(0, v) =~= vs);
}

/// Values `vs` and then `n` copies of `x`: where deltas `ds` realize
/// `vs`, so do `ds`, the step `d` from the last value to `x` and `n - 1`
/// steps of 0 realize them all (`after`: `ds` and `d`; `all`: the
/// zeros too).
proof fn lemma_append_value(
    vs: Seq<Option<i64>>,
    ds: Seq<Option<i64>>,
    after: Seq<Option<i64>>,
    all: Seq<Option<i64>>,
    abs: i64,
    x: i64,
    d: i64,
    n: nat,
)
    requires
        n >= 1,
        abs == last_val(vs),
        d == vstd::wrapping::i64_specs::wrapping_sub(x, abs),
        span_ok(vs) ==> realize(ds) == ivals(vs) && psum(ds) == last_val(vs),
        after == ds + copies(1, Some(d)),
        all == if n > 1 { after + copies((n - 1) as nat, Some(0i64)) } else { after },
    ensures ({
        let r = vs + copies(n, Some(x));
        &&& vlow(r) == min_i(vlow(vs), x as int)
        &&& vhigh(r) == max_i(vhigh(vs), x as int)
        &&& vlow(vs) > vhigh(vs) ==> vlow(vs) == i64::MAX && vhigh(vs) == i64::MIN
        &&& last_val(r) == x
        &&& span_ok(r) ==> realize(all) == ivals(r) && psum(all) == x
    }),
{
    let r = vs + copies(n, Some(x));
    let m = (n - 1) as nat;
    lemma_vals_run(vs, n, Some(x));
    lemma_last_in(vs);
    if span_ok(r) {
        lemma_span_prefix(vs, n, Some(x));
        assert(d as int == x - last_val(vs));
        assert(after =~= ds.push(Some(d)));
        lemma_realize_push(ds, Some(d));
        lemma_realize_run(after, m, Some(0i64));
        lemma_no_copies(after, Some(0i64));
        assert(all == after + copies(m, Some(0i64)));
        lemma_ivals_run(vs, n, Some(x));
        assert(ivals(vs).push(Some(x as int)) + copies(m, Some(x as int)) =~= ivals(vs) + copies(n, Some(x as int)));
    }
}

/// Values `vs` and then `n` nulls: where deltas `ds` realize `vs`, `ds`
/// and `n` nulls (`all`) realize them all.
proof fn lemma_append_nulls(vs: Seq<Option<i64>>, ds: Seq<Option<i64>>, all: Seq<Option<i64>>, n: nat)
    requires
        n >= 1,
        span_ok(vs) ==> realize(ds) == ivals(vs) && psum(ds) == last_val(vs),
        all == ds + copies(n, None::<i64>),
    ensures ({
        let r = vs + copies(n, None::<i64>);
        &&& vlow(r) == vlow(vs)
        &&& vhigh(r) == vhigh(vs)
        &&& last_val(r) == last_val(vs)
        &&& span_ok(r) ==> realize(all) == ivals(r) && psum(all) == last_val(r)
    }),
{
    lemma_vals_run(vs, n, None);
    lemma_realize_run(ds, n, None);
    lemma_ivals_run(vs, n, None);
}

/// Values spanning 2^63 or more: not in the delta domain.  Trusted only
/// in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn values_too_wide(Ghost(why): Ghost<bool>)
    requires
        why,
    ensures
        false,
{
    panic!("delta values out of domain: they span 2^63 or more")
}

/// The delta encoder's state: the RLE stream of deltas, the last value
/// (where the next delta counts from), and the least and greatest values
/// (tracked only where the type's domain `lo..=hi` is as wide as `i64`'s,
/// as only there can they span 2^63).
pub(crate) struct DStream<'a, L: WriteOps<'a>> {
    pub(crate) s: RStream<'a, L>,
    pub(crate) abs: i64,
    pub(crate) low: i64,
    pub(crate) high: i64,
    /// the values appended
    #[allow(dead_code)]
    pub(crate) vals: Ghost<Seq<Option<i64>>>,
}

/// A domain `lo..=hi` as wide as `i64`'s, so that values in it can span
/// 2^63.
pub open spec fn wide(lo: i64, hi: i64) -> bool {
    hi - lo > i64::MAX
}

impl<'a, L> DStream<'a, L> where L: WriteOps<'a>, L::R: RleOps<'a, V = Option<i64>> {
    pub(crate) open spec fn inv(&self, buf: Seq<u8>, lo: i64, hi: i64) -> bool {
        let vals = self.vals@;
        &&& self.s.on(buf)
        &&& vlow(vals) > vhigh(vals) || (lo <= vlow(vals) && vhigh(vals) <= hi)
        &&& wide(lo, hi) ==> self.low == vlow(vals) && self.high == vhigh(vals)
        &&& self.abs == last_val(vals)
        &&& span_ok(vals) ==> realize(self.s.cur@) == ivals(vals) && psum(self.s.cur@) == last_val(vals)
    }

    pub(crate) fn new() -> (r: Self)
        ensures r.s.fresh(), r.vals@.len() == 0, forall|buf: Seq<u8>, lo: i64, hi: i64| r.inv(buf, lo, hi),
    {
        let r = DStream { s: RStream::new(), abs: 0, low: i64::MAX, high: i64::MIN, vals: Ghost(Seq::empty()) };
        proof {
            assert(ivals(Seq::empty()) =~= Seq::<Option<int>>::empty());
        }
        r
    }

    /// Append `n` copies of `v`, a value in the domain `lo..=hi`.
    #[inline(always)]
    #[verifier::spinoff_prover]
    pub(crate) fn append(&mut self, buf: &mut Vec<u8>, v: Option<i64>, n: usize, lo: i64, hi: i64)
        requires
            old(self).inv(old(buf)@, lo, hi),
            v is Some ==> lo <= v->Some_0 <= hi,
        ensures
            final(self).inv(final(buf)@, lo, hi),
            final(self).vals@ == old(self).vals@ + copies(n as nat, v),
            final(self).s.head_of(final(buf)@) == old(self).s.head_of(old(buf)@),
    {
        let ghost vals0 = self.vals@;
        let ghost cur0 = self.s.cur@;
        let ghost abs0 = self.abs;
        if n == 0 {
            proof {
                lemma_no_copies(vals0, v);
            }
            return;
        }
        match v {
            Some(x) => {
                if hi as i128 - lo as i128 > i64::MAX as i128 {
                    self.low = if self.low <= x { self.low } else { x };
                    self.high = if self.high >= x { self.high } else { x };
                }
                // exact unless the values span 2^63, which `finish` rejects
                let d = x.wrapping_sub(self.abs);
                self.abs = x;
                self.s.append(buf, L::cow_ref(Some(d)), 1);
                let ghost cur1 = self.s.cur@;
                if n > 1 {
                    self.s.append(buf, L::cow_ref(Some(0i64)), n - 1);
                }
                proof {
                    self.vals = Ghost(vals0 + copies(n as nat, v));
                    lemma_append_value(vals0, cur0, cur1, self.s.cur@, abs0, x, d, n as nat);
                }
            },
            None => {
                self.s.append(buf, L::cow_ref(None), n);
                proof {
                    self.vals = Ghost(vals0 + copies(n as nat, v));
                    lemma_append_nulls(vals0, cur0, self.s.cur@, n as nat);
                }
            },
        }
    }

    /// Flush: the bytes after the caller's are the canonical encoding of
    /// deltas that realize exactly the values appended.
    pub(crate) fn finish(&mut self, buf: &mut Vec<u8>, lo: i64, hi: i64)
        requires
            old(self).inv(old(buf)@, lo, hi),
        ensures ({
            let h = old(self).s.head_of(old(buf)@);
            &&& h.len() <= final(buf)@.len()
            &&& final(buf)@.take(h.len() as int) == h
            &&& exists|segs: int| encodes::<L::R>(final(buf)@.skip(h.len() as int), old(self).s.cur@, segs)
            &&& realize(old(self).s.cur@) == ivals(old(self).vals@)
        }),
    {
        if hi as i128 - lo as i128 > i64::MAX as i128 && self.low <= self.high
            && self.high.checked_sub(self.low).is_none()
        {
            values_too_wide(Ghost(true));
        }
        self.s.finish(buf);
    }
}

} // verus!
