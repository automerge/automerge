//! A delta column's value search, verified with Verus.
//!
//! [`DeltaColumn::find_by_range`](crate::DeltaColumn::find_by_range) and
//! `find_by_value` walk the slabs the index says may hold a value in range
//! (the B-tree's value search, verified in `btree`), and scan each one's
//! runs: a run of equal deltas `v` after the running sum `a` realizes
//! `a + v·k` for its `k`th item, so the items in range are found by
//! division, not one by one (`run_hits`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

use crate::btree::SlabAgg;
use crate::btree::ValueSearch;
#[cfg(verus_keep_ghost)]
use crate::btree::{
    fold, in_i64, lemma_fold_one, max_i, may_hold, min_i, pre_at, sum_len, valid, SlabAggregate,
};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, deltas_of, lemma_run_sum, lemma_take_run, pmax, pmin, psum, reps};
use crate::delta::diter::add_run;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{dbefore, dix_ok, lemma_crop, lemma_dfold, lemma_found, lemma_psum_cat};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::before;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// `x - a` for `x >= a`, in a `u64`: the difference of two `i64`s always
/// fits one. (Two's complement makes it `x - a` wrapping; Verus leaves a
/// negative `i64`'s cast unspecified, so the cases are spelled out.)
#[inline(always)]
fn udiff(x: i64, a: i64) -> (r: u64)
    requires
        x >= a,
    ensures
        r == x - a,
{
    if a >= 0 || x < 0 {
        (x - a) as u64
    } else {
        x as u64 + (-(a + 1)) as u64 + 1
    }
}

/// The `k`th value of a run of deltas `v` after the running sum `a`.
pub(crate) open spec fn kth(a: int, v: int, k: int) -> int {
    a + k * v
}

/// The items `k` in `1..=count` of a run whose values are `a + v·k` that
/// lie in `[lo, hi]`: `k_start..=k_end`, empty when `k_start > k_end`.
pub(crate) fn run_hits(a: i64, v: i64, count: usize, lo: i64, hi: i64) -> (r: (usize, usize))
    requires
        count >= 1,
        v != 0,
        in_i64(a + v),
        in_i64(a + count * v),
    ensures
        1 <= r.0,
        r.1 <= count,
        forall|k: int| 1 <= k <= count ==> ((r.0 <= k <= r.1) <==> (lo <= #[trigger] kth(a as int, v as int, k) <= hi)),
{
    let first = a + v;
    let last = add_run(a, v, count, Ghost(a as int));
    proof {
        lemma_crop(a + count * v);
        // the run's values lie between its first and last
        assert forall|k: int| 1 <= k <= count implies (v > 0 ==> first <= #[trigger] kth(a as int, v as int, k) <= last) && (v < 0
            ==> last <= kth(a as int, v as int, k) <= first) by {
            if v > 0 {
                assert(v <= k * v <= count * v) by (nonlinear_arith) requires 1 <= k <= count, v > 0;
            } else {
                assert(count * v <= k * v <= v) by (nonlinear_arith) requires 1 <= k <= count, v < 0;
            }
        }
    }
    if v > 0 {
        proof {
            assert(v <= count * v) by (nonlinear_arith) requires count >= 1, v > 0;
        }
        if hi < first || lo > last {
            return (1, 0);
        }
        // a + v·k >= lo' and <= hi', with the bounds clamped to the run
        let dlo = udiff(if lo > first { lo } else { first }, a);
        let dhi = udiff(if hi < last { hi } else { last }, a);
        let vu = v as u64;
        let k_start = (dlo - 1) / vu + 1;
        let k_end = dhi / vu;
        proof {
            let (l, h, w) = (dlo as int, dhi as int, vu as int);
            assert(last == a + count * v);
            assert(w == v);
            assert(count * w == count * v);
            assert(v <= count * v) by (nonlinear_arith) requires count >= 1, v > 0;
            assert(w <= l <= count * w);
            assert(w <= h <= count * w);
            assert forall|k: int| 1 <= k <= count implies ((k_start <= k <= k_end) <==> (l <= #[trigger] kth(0, w, k) <= h)) by {
                let q1 = (l - 1) / w;
                let q2 = h / w;
                assert(q1 * w <= l - 1 < q1 * w + w) by (nonlinear_arith) requires q1 == (l - 1) / w, w > 0, l >= 1;
                assert(q2 * w <= h < q2 * w + w) by (nonlinear_arith) requires q2 == h / w, w > 0, h >= 0;
                assert((k >= q1 + 1) <==> (k * w >= l)) by (nonlinear_arith)
                    requires q1 * w <= l - 1, l - 1 < q1 * w + w, w > 0;
                assert((k <= q2) <==> (k * w <= h)) by (nonlinear_arith)
                    requires q2 * w <= h, h < q2 * w + w, w > 0;
            }
            assert(k_end <= count) by {
                assert((h / w) <= count) by (nonlinear_arith) requires h <= count * w, w > 0;
            }
            assert(k_start <= count) by {
                assert((l - 1) / w + 1 <= count) by (nonlinear_arith) requires 1 <= l <= count * w, w > 0;
            }
            assert(k_start >= 1);
        }
        proof {
            let w = vu as int;
            assert forall|k: int| 1 <= k <= count implies ((k_start <= k <= k_end) <==> (lo <= #[trigger] kth(
                a as int,
                v as int,
                k,
            ) <= hi)) by {
                assert(kth(0, w, k) == kth(a as int, v as int, k) - a);
                assert(first <= kth(a as int, v as int, k) <= last);
            }
        }
        (k_start as usize, k_end as usize)
    } else {
        proof {
            assert(count * v <= v) by (nonlinear_arith) requires count >= 1, v < 0;
        }
        if lo > first || hi < last {
            return (1, 0);
        }
        // a - |v|·k <= hi' and >= lo', with the bounds clamped to the run
        let dlo = udiff(a, if hi < first { hi } else { first });
        let dhi = udiff(a, if lo > last { lo } else { last });
        let vu = udiff(0, v);
        let k_start = (dlo - 1) / vu + 1;
        let k_end = dhi / vu;
        proof {
            let (l, h, w) = (dlo as int, dhi as int, vu as int);
            assert(w == -v);
            assert(last == a + count * v);
            assert(count * w == -(count * v)) by (nonlinear_arith) requires w == -v;
            assert(count * v <= v) by (nonlinear_arith) requires count >= 1, v < 0;
            assert(w <= l <= count * w);
            assert(w <= h <= count * w);
            assert forall|k: int| 1 <= k <= count implies ((k_start <= k <= k_end) <==> (l <= #[trigger] kth(0, w, k) <= h)) by {
                let q1 = (l - 1) / w;
                let q2 = h / w;
                assert(q1 * w <= l - 1 < q1 * w + w) by (nonlinear_arith) requires q1 == (l - 1) / w, w > 0, l >= 1;
                assert(q2 * w <= h < q2 * w + w) by (nonlinear_arith) requires q2 == h / w, w > 0, h >= 0;
                assert((k >= q1 + 1) <==> (k * w >= l)) by (nonlinear_arith)
                    requires q1 * w <= l - 1, l - 1 < q1 * w + w, w > 0;
                assert((k <= q2) <==> (k * w <= h)) by (nonlinear_arith)
                    requires q2 * w <= h, h < q2 * w + w, w > 0;
                assert(k * w == -(k * v)) by (nonlinear_arith) requires w == -v;
            }
            assert(k_end <= count) by {
                assert((h / w) <= count) by (nonlinear_arith) requires h <= count * w, w > 0;
            }
            assert(k_start <= count) by {
                assert((l - 1) / w + 1 <= count) by (nonlinear_arith) requires 1 <= l <= count * w, w > 0;
            }
        }
        proof {
            let w = vu as int;
            assert forall|k: int| 1 <= k <= count implies ((k_start <= k <= k_end) <==> (lo <= #[trigger] kth(
                a as int,
                v as int,
                k,
            ) <= hi)) by {
                assert(kth(0, w, k) == a - kth(a as int, v as int, k)) by {
                    assert(k * w == -(k * v)) by (nonlinear_arith) requires w == -v;
                }
                assert(last <= kth(a as int, v as int, k) <= first);
            }
        }
        (k_start as usize, k_end as usize)
    }
}


/// Item `j` of values `vs`, after the running sum `base`, realizes a
/// value in `[lo, hi]` (a null realizes none).
pub(crate) open spec fn hit<V: Copy, B: DeltaOps<V>>(vs: Seq<V>, base: int, lo: int, hi: int, j: int) -> bool {
    match B::dv(vs[j]) {
        Some(_) => lo <= base + psum(deltas_of::<V, B>(vs.take(j + 1))) <= hi,
        None => false,
    }
}

/// Every running sum of the deltas of `vs`, after `base`, fits `i64`.
pub(crate) open spec fn sums_in_i64<V: Copy, B: DeltaOps<V>>(vs: Seq<V>, base: int) -> bool {
    forall|m: int| 0 <= m <= vs.len() ==> in_i64(base + #[trigger] psum(deltas_of::<V, B>(vs.take(m))))
}

/// The running sum after `base` and the first `k` of the deltas of `vs`.
pub(crate) open spec fn s1_running<V: Copy, B: DeltaOps<V>>(base: int, vs: Seq<V>, k: int) -> int {
    base + psum(deltas_of::<V, B>(vs.take(k)))
}

/// Item `k + kk - 1` of a run of `c` copies of `x` starting at item `k`:
/// it is `x`, and the running sum after it is `kk` steps on.
proof fn lemma_run_item<V: Copy, B: DeltaOps<V>>(vs: Seq<V>, k: int, c: int, x: V, kk: int)
    requires
        0 <= k,
        k + c <= vs.len(),
        vs.skip(k).take(c) == Seq::new(c as nat, |_i: int| x),
        1 <= kk <= c,
    ensures
        vs[k + kk - 1] == x,
        psum(deltas_of::<V, B>(vs.take(k + kk))) == psum(deltas_of::<V, B>(vs.take(k))) + kk
            * crate::delta::agg::dval(B::dv(x)),
{
    assert(vs.skip(k).take(c)[kk - 1] == x);
    assert(vs.skip(k).take(kk) =~= Seq::new(kk as nat, |_i: int| x)) by {
        assert forall|i: int| 0 <= i < kk implies #[trigger] vs.skip(k).take(kk)[i] == x by {
            assert(vs.skip(k).take(c)[i] == x);
        }
    }
    lemma_take_run::<_, B>(vs, k, kk, x);
    lemma_run_sum(deltas_of::<_, B>(vs.take(k)), kk as nat, B::dv(x));
}

/// A scan of one slab's runs for the items whose values lie in a range.
pub(crate) struct DScan<D> {
    pub(crate) dec: D,
    /// the running sum after the items scanned
    pub(crate) running: i64,
    /// items scanned
    pub(crate) item: usize,
    /// the slab's first item, in the column
    pub(crate) start: usize,
    /// the matches found in the last run, not yet returned
    pub(crate) pend: usize,
    pub(crate) pend_end: usize,
}

/// The scan of a slab of values `vs`, after the running sum `base`, has
/// returned every match before `f` (a column index), and its pending range
/// is exactly the matches from `f` up to what it has scanned.
pub(crate) open spec fn scan_ok<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    sc: DScan<X::D>,
    vs: Seq<<X::D as Decode>::Item>,
    base: int,
    lo: int,
    hi: int,
    f: int,
) -> bool {
    &&& sc.dec.inv()
    &&& sc.item <= vs.len()
    &&& sc.dec.rest() == vs.skip(sc.item as int)
    &&& sc.start + vs.len() <= usize::MAX
    &&& sums_in_i64::<_, B>(vs, base)
    &&& sc.running == base + psum(deltas_of::<_, B>(vs.take(sc.item as int)))
    &&& sc.start <= f <= sc.pend <= sc.pend_end <= sc.start + sc.item
    &&& forall|j: int| f <= sc.start + j < sc.start + sc.item ==> (#[trigger] hit::<_, B>(vs, base, lo, hi, j)
        <==> sc.pend <= sc.start + j < sc.pend_end)
}

/// A scan from the slab's start.
pub(crate) fn d_scan_new<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
    start: usize,
    base: i64,
    Ghost(lo): Ghost<int>,
    Ghost(hi): Ghost<int>,
) -> (sc: DScan<X::D>)
    requires
        X::ok(*slab),
        start + X::vals(*slab).len() <= usize::MAX,
        sums_in_i64::<_, B>(X::vals(*slab), base as int),
    ensures
        scan_ok::<X, B>(sc, X::vals(*slab), base as int, lo, hi, start as int),
        sc.start == start,
{
    let ghost vs = X::vals(*slab);
    let dec = X::open(slab);
    proof {
        assert(vs.skip(0) =~= vs);
        assert(vs.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(deltas_of::<_, B>(vs.take(0)) =~= Seq::<Option<i64>>::empty());
    }
    DScan { dec, running: base, item: 0, start, pend: start, pend_end: start }
}

/// The next match in the slab, at or after `f`: the least index there
/// whose item realizes a value in `[lo, hi]`, or `None` if none does.
pub(crate) fn d_scan_next<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    sc: &mut DScan<X::D>,
    lo: i64,
    hi: i64,
    Ghost(vs): Ghost<Seq<<X::D as Decode>::Item>>,
    Ghost(base): Ghost<int>,
    Ghost(f): Ghost<int>,
) -> (r: Option<usize>)
    requires
        scan_ok::<X, B>(*old(sc), vs, base, lo as int, hi as int, f),
    ensures
        final(sc).start == old(sc).start,
        match r {
            Some(i) => {
                &&& f <= i < old(sc).start + vs.len()
                &&& hit::<_, B>(vs, base, lo as int, hi as int, i - old(sc).start)
                &&& forall|j: int| f <= old(sc).start + j < i ==> !#[trigger] hit::<_, B>(
                    vs,
                    base,
                    lo as int,
                    hi as int,
                    j,
                )
                &&& scan_ok::<X, B>(*final(sc), vs, base, lo as int, hi as int, i + 1)
            },
            None => forall|j: int| f <= old(sc).start + j < old(sc).start + vs.len() ==> !#[trigger] hit::<
                _,
                B,
            >(vs, base, lo as int, hi as int, j),
        },
{
    let ghost s0 = *sc;
    loop
        invariant
            scan_ok::<X, B>(*sc, vs, base, lo as int, hi as int, f),
            sc.start == s0.start,
            // nothing matched between `f` and the pending range's start
            forall|j: int| f <= sc.start + j < sc.pend ==> !#[trigger] hit::<_, B>(
                vs,
                base,
                lo as int,
                hi as int,
                j,
            ),
        decreases sc.dec.rest().len(),
    {
        if sc.pend < sc.pend_end {
            let i = sc.pend;
            sc.pend = sc.pend + 1;
            return Some(i);
        }
        let ghost k = sc.item as int;
        let ghost st = sc.start as int;
        match sc.dec.d_run_max(usize::MAX) {
            None => {
                proof {
                    assert(k == vs.len());
                }
                return None;
            },
            Some(run) => {
                let count = run.count;
                let ghost c = count as int;
                let ghost p0 = sc.pend;
                let ghost e0 = sc.pend_end;
                proof {
                    assert(vs.skip(k).take(c) == Seq::new(count as nat, |_i: int| run.value));
                    lemma_run_item::<_, B>(vs, k, c, run.value, c);
                    lemma_run_item::<_, B>(vs, k, c, run.value, 1);
                }
                if let Some(v) = B::to_opt(run.value) {
                    let a = sc.running;
                    if v == 0 {
                        if a >= lo && a <= hi {
                            sc.pend = sc.start + sc.item;
                            sc.pend_end = sc.start + sc.item + count;
                        }
                    } else {
                        proof {
                            assert(in_i64(base + psum(deltas_of::<_, B>(vs.take(k + 1)))));
                            assert(in_i64(base + psum(deltas_of::<_, B>(vs.take(k + c)))));
                        }
                        let (ks, ke) = run_hits(a, v, count, lo, hi);
                        if ks <= ke {
                            sc.pend = sc.start + sc.item + ks - 1;
                            sc.pend_end = sc.start + sc.item + ke;
                        }
                    }
                    sc.running = add_run(a, v, count, Ghost(a as int));
                    proof {
                        assert(in_i64(base + psum(deltas_of::<_, B>(vs.take(k + c)))));
                        lemma_crop(a + count * v);
                    }
                }
                sc.item = sc.item + count;
                proof {
                    assert(vs.skip(k).skip(c) =~= vs.skip(k + c));
                    // before the run: nothing matched since `f`
                    assert(p0 == e0);
                    assert forall|j: int| f <= st + j < st + k implies !#[trigger] hit::<_, B>(
                        vs,
                        base,
                        lo as int,
                        hi as int,
                        j,
                    ) && !(sc.pend <= st + j < sc.pend_end) by {
                    }
                    // the run's items: which match
                    assert forall|j: int| st + k <= st + j < st + k + c implies (#[trigger] hit::<_, B>(
                        vs,
                        base,
                        lo as int,
                        hi as int,
                        j,
                    ) <==> sc.pend <= st + j < sc.pend_end) by {
                        let kk = j - k + 1;
                        lemma_run_item::<_, B>(vs, k, c, run.value, kk);
                        if let Some(v) = B::dv(run.value) {
                            assert(base + psum(deltas_of::<_, B>(vs.take(j + 1))) == kth(s1_running::<_, B>(base, vs, k), v as int, kk))
                                by {
                                assert(j + 1 == k + kk);
                            }
                        }
                    }
                }
            },
        }
    }
}


/// The running sums after each of the first `m` deltas lie within the
/// least and greatest after any of them.
proof fn lemma_take_in_bounds(ds: Seq<Option<i64>>, m: int)
    requires
        1 <= m <= ds.len(),
    ensures
        pmin(ds) <= psum(ds.take(m)) <= pmax(ds),
    decreases ds.len(),
{
    if m == ds.len() {
        assert(ds.take(m) =~= ds);
        if ds.len() > 1 {
            assert(pmin(ds) == min_i(pmin(ds.drop_last()), psum(ds)));
            assert(pmax(ds) == max_i(pmax(ds.drop_last()), psum(ds)));
        }
    } else {
        lemma_take_in_bounds(ds.drop_last(), m);
        assert(ds.drop_last().take(m) =~= ds.take(m));
    }
}

/// Item `i` of the column (of slabs `s`) realizes a value in `[lo, hi]`.
pub(crate) open spec fn col_hit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    i: int,
) -> bool {
    hit::<_, B>(vals_of::<X>(s), 0, lo, hi, i)
}

/// Slab `k` of the column, scanned after the running sum before it: its
/// running sums fit, its matches are the column's, and a slab the index
/// rules out holds none.
proof fn lemma_slab_hits<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    k: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k < s.len(),
    ensures
        pre_at(ws, k) == psum(dbefore::<X, B>(s, sum(slab_lens(s), 0, k) as int)),
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        sum(slab_lens(s), 0, k) + X::vals(s[k]).len() <= total(slab_lens(s)),
        sums_in_i64::<_, B>(X::vals(s[k]), pre_at(ws, k) as int),
        forall|j: int| 0 <= j < X::vals(s[k]).len() ==> (#[trigger] hit::<_, B>(
            X::vals(s[k]),
            pre_at(ws, k) as int,
            lo,
            hi,
            j,
        ) <==> col_hit::<X, B>(s, lo, hi, sum(slab_lens(s), 0, k) + j)),
        !may_hold(ws, lo, hi, k) ==> forall|j: int| 0 <= j < X::vals(s[k]).len() ==> !#[trigger] hit::<_, B>(
            X::vals(s[k]),
            pre_at(ws, k) as int,
            lo,
            hi,
            j,
        ),
{
    let vv = vals_of::<X>(s);
    let vs = X::vals(s[k]);
    let ds = deltas_of::<_, B>(vs);
    let l = sum(slab_lens(s), 0, k);
    lemma_found::<X, B>(ws, s, k);
    lemma_dfold::<X, B>(ws, s, k);
    lemma_upto::<X>(s, k);
    lemma_from::<X>(s, k, 0);
    let base = pre_at(ws, k) as int;
    let w = ws[k];
    assert(agg_of(w, ds));
    assert(vs.len() == s[k].len);
    // the merge of the slabs before with this one is representable
    lemma_fold_one(ws, k);
    assert(SlabAgg::merge_ok(fold(ws.subrange(0, k)), fold(ws.subrange(k, k + 1))));
    let before_agg = fold(ws.subrange(0, k));
    assert forall|m: int| 0 <= m <= vs.len() implies in_i64(base + #[trigger] psum(deltas_of::<_, B>(vs.take(m)))) by {
        if m == 0 {
            assert(vs.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
            assert(deltas_of::<_, B>(vs.take(0)) =~= Seq::<Option<i64>>::empty());
        } else {
            assert(deltas_of::<_, B>(vs.take(m)) =~= ds.take(m));
            lemma_take_in_bounds(ds, m);
            if before_agg.len == 0 {
                assert(base == 0);
            }
        }
    }
    assert forall|j: int| 0 <= j < vs.len() implies (#[trigger] hit::<_, B>(vs, base, lo, hi, j) <==> col_hit::<
        X,
        B,
    >(s, lo, hi, l + j)) by {
        assert(vv.skip(l as int).take(vs.len() as int) =~= vs);
        assert(vv[l + j] == vs[j]);
        assert(vv.take(l + j + 1) =~= vv.take(l as int) + vs.take(j + 1));
        assert(deltas_of::<_, B>(vv.take(l + j + 1)) =~= deltas_of::<_, B>(vv.take(l as int)) + deltas_of::<_, B>(
            vs.take(j + 1),
        ));
        lemma_psum_cat(deltas_of::<_, B>(vv.take(l as int)), deltas_of::<_, B>(vs.take(j + 1)));
        assert(deltas_of::<_, B>(vv.take(0 + l + j + 1)) == deltas_of::<_, B>(vv.take(l + j + 1)));
    }
    if !may_hold(ws, lo, hi, k) {
        assert forall|j: int| 0 <= j < vs.len() implies !#[trigger] hit::<_, B>(vs, base, lo, hi, j) by {
            assert(deltas_of::<_, B>(vs.take(j + 1)) =~= ds.take(j + 1));
            lemma_take_in_bounds(ds, j + 1);
        }
    }
}

/// The slab holding item `j`: the one whose items start at or before it
/// and end after it.
pub(crate) open spec fn slab_at(lens: Seq<nat>, j: int) -> int {
    choose|k: int| #![trigger sum(lens, 0, k)] 0 <= k < lens.len() && sum(lens, 0, k) <= j < sum(lens, 0, k + 1)
}

proof fn lemma_slab_exists(lens: Seq<nat>, j: int, n: int)
    requires
        0 <= j < sum(lens, 0, n),
        0 <= n <= lens.len(),
    ensures
        exists|k: int| #![trigger sum(lens, 0, k)] 0 <= k < n && sum(lens, 0, k) <= j < sum(lens, 0, k + 1),
    decreases n,
{
    if n == 0 {
        assert(sum(lens, 0, 0) == 0);
    } else {
        lemma_sum_split(lens, 0, n - 1, n);
        lemma_sum_one(lens, n - 1);
        if j < sum(lens, 0, n - 1) {
            lemma_slab_exists(lens, j, n - 1);
        } else {
            assert(sum(lens, 0, n - 1) <= j < sum(lens, 0, n));
        }
    }
}

/// Item `j` of the column is item `j - sum(0, k)` of slab `slab_at(j)`.
proof fn lemma_slab_of<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, j: int)
    requires
        0 <= j < total(slab_lens(s)),
        lens_ok::<X>(s),
    ensures
        0 <= slab_at(slab_lens(s), j) < s.len(),
        sum(slab_lens(s), 0, slab_at(slab_lens(s), j)) <= j,
        j - sum(slab_lens(s), 0, slab_at(slab_lens(s), j)) < X::vals(s[slab_at(slab_lens(s), j)]).len(),
{
    let lens = slab_lens(s);
    lemma_slab_exists(lens, j, s.len() as int);
    let k = slab_at(lens, j);
    lemma_sum_split(lens, 0, k, k + 1);
    lemma_sum_one(lens, k);
    assert(X::vals(s[k]).len() == s[k].len);
}

/// A slab's first item is at or after a slab before it ends.
proof fn lemma_slab_order(lens: Seq<nat>, j: int, p: int, q: int)
    requires
        0 <= p <= lens.len(),
        0 <= q <= lens.len(),
        0 <= slab_at(lens, j) < lens.len(),
        sum(lens, 0, slab_at(lens, j)) <= j < sum(lens, 0, slab_at(lens, j) + 1),
        sum(lens, 0, p) <= j < sum(lens, 0, q),
    ensures
        p <= slab_at(lens, j) < q,
{
    let k = slab_at(lens, j);
    if k < p {
        lemma_sum_le(lens, 0, k + 1, 0, p);
    }
    if k >= q {
        lemma_sum_le(lens, 0, q, 0, k);
    }
}

/// A match the scan of slab `k` returned is the column's next.
proof fn lemma_found_in_slab<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    k: int,
    f0: int,
    f: int,
    i: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k < s.len(),
        f0 <= f <= i,
        forall|j: int| f0 <= j < f ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
        sum(slab_lens(s), 0, k) <= f,
        i < sum(slab_lens(s), 0, k) + X::vals(s[k]).len(),
        hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, i - sum(slab_lens(s), 0, k)),
        forall|j: int| f <= sum(slab_lens(s), 0, k) + j < i ==> !#[trigger] hit::<_, B>(
            X::vals(s[k]),
            pre_at(ws, k) as int,
            lo,
            hi,
            j,
        ),
    ensures
        col_hit::<X, B>(s, lo, hi, i),
        forall|j: int| f0 <= j < i ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
{
    let st = sum(slab_lens(s), 0, k);
    lemma_slab_hits::<X, B>(ws, s, lo, hi, k);
    assert(hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, i - st));
    assert forall|j: int| f0 <= j < i implies !#[trigger] col_hit::<X, B>(s, lo, hi, j) by {
        if j >= f {
            assert(!hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, j - st));
        }
    }
}

/// A scan of slab `k` that found nothing more leaves no match before the
/// slab's end.
proof fn lemma_slab_done<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    k: int,
    f0: int,
    f: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k < s.len(),
        f0 <= f,
        sum(slab_lens(s), 0, k) <= f,
        forall|j: int| f0 <= j < f ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
        forall|j: int| f <= sum(slab_lens(s), 0, k) + j < sum(slab_lens(s), 0, k) + X::vals(s[k]).len()
            ==> !#[trigger] hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, j),
    ensures
        sum(slab_lens(s), 0, k + 1) == sum(slab_lens(s), 0, k) + X::vals(s[k]).len(),
        forall|j: int| f0 <= j < sum(slab_lens(s), 0, k + 1) ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
{
    let st = sum(slab_lens(s), 0, k);
    lemma_slab_hits::<X, B>(ws, s, lo, hi, k);
    lemma_sum_split(slab_lens(s), 0, k, k + 1);
    lemma_sum_one(slab_lens(s), k);
    assert(X::vals(s[k]).len() == s[k].len);
    assert forall|j: int| f0 <= j < sum(slab_lens(s), 0, k + 1) implies !#[trigger] col_hit::<X, B>(s, lo, hi, j) by {
        if j >= f {
            assert(!hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, j - st));
        }
    }
}

/// Slabs `p..q` the index ruled out hold no matches.
proof fn lemma_skipped<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    lo: int,
    hi: int,
    p: int,
    q: int,
    f0: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= p <= q <= s.len(),
        f0 <= sum(slab_lens(s), 0, p),
        forall|j: int| f0 <= j < sum(slab_lens(s), 0, p) ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
        forall|k: int| p <= k < q ==> !#[trigger] may_hold(ws, lo, hi, k),
    ensures
        forall|j: int| f0 <= j < sum(slab_lens(s), 0, q) ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
{
    let lens = slab_lens(s);
    assert forall|j: int| f0 <= j < sum(lens, 0, q) implies !#[trigger] col_hit::<X, B>(s, lo, hi, j) by {
        if j >= sum(lens, 0, p) {
            lemma_sum_le(lens, 0, q, 0, s.len() as int);
            lemma_slab_of::<X>(s, j);
            lemma_slab_exists(lens, j, s.len() as int);
            let k = slab_at(lens, j);
            lemma_slab_order(lens, j, p, q);
            lemma_slab_hits::<X, B>(ws, s, lo, hi, k);
            assert(!may_hold(ws, lo, hi, k));
            assert(!hit::<_, B>(X::vals(s[k]), pre_at(ws, k) as int, lo, hi, j - sum(lens, 0, k)));
        }
    }
}

/// The search for the items of a column whose values lie in `[lo, hi]`:
/// the index's candidate slabs, and a scan of the current one.
pub(crate) struct DFind<'a, D> {
    pub(crate) search: ValueSearch<'a>,
    /// the scan of the slab before `search`'s, while `active`
    pub(crate) cur: DScan<D>,
    pub(crate) active: bool,
    pub(crate) lo: i64,
    pub(crate) hi: i64,
}

/// The search has returned every match before `f`.
pub(crate) open spec fn find_ok<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    fd: DFind<'a, X::D>,
    s: Seq<Slab<X::Tail>>,
    f: int,
) -> bool {
    let ws = fd.search.view_tree();
    let pos = fd.search.pos();
    &&& fd.search.inv()
    &&& fd.search.spec_lo() == fd.lo
    &&& fd.search.spec_hi() == fd.hi
    &&& dix_ok::<X, B>(ws, s)
    &&& lens_ok::<X>(s)
    &&& total(slab_lens(s)) <= usize::MAX
    &&& 0 <= pos <= ws.len()
    &&& !fd.active ==> f == sum(slab_lens(s), 0, pos)
    &&& fd.active ==> {
        &&& pos >= 1
        &&& fd.cur.start == sum(slab_lens(s), 0, pos - 1)
        &&& scan_ok::<X, B>(fd.cur, X::vals(s[pos - 1]), pre_at(ws, pos - 1) as int, fd.lo as int, fd.hi as int, f)
    }
}

/// A search from the column's start.
pub(crate) fn d_find_new<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ix: &'a crate::btree::SlabBTree<SlabAgg>,
    lo: i64,
    hi: i64,
    Ghost(s): Ghost<Seq<Slab<X::Tail>>>,
) -> (fd: DFind<'a, X::D>)
    requires
        ix.wf(),
        dix_ok::<X, B>(ix@, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
    ensures
        find_ok::<X, B>(fd, s, 0),
{
    proof {
        lemma_found::<X, B>(ix@, s, 0);
    }
    let search = ValueSearch::new(ix, lo, hi);
    proof {
        assert(sum(slab_lens(s), 0, 0) == 0);
    }
    let cur = DScan { dec: X::none(), running: 0, item: 0, start: 0, pend: 0, pend_end: 0 };
    DFind { search, cur, active: false, lo, hi }
}

/// The next item of the column, at or after `f`, whose value lies in
/// `[lo, hi]`.
#[verifier::spinoff_prover]
pub(crate) fn d_find_next<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    fd: &mut DFind<'a, X::D>,
    slabs: &'a [Slab<X::Tail>],
    Ghost(f0): Ghost<int>,
) -> (r: Option<usize>)
    requires
        find_ok::<X, B>(*old(fd), slabs@, f0),
    ensures
        match r {
            Some(i) => {
                &&& f0 <= i
                &&& col_hit::<X, B>(slabs@, old(fd).lo as int, old(fd).hi as int, i as int)
                &&& forall|j: int| f0 <= j < i ==> !#[trigger] col_hit::<X, B>(
                    slabs@,
                    old(fd).lo as int,
                    old(fd).hi as int,
                    j,
                )
                &&& find_ok::<X, B>(*final(fd), slabs@, i + 1)
            },
            None => forall|j: int| f0 <= j < total(slab_lens(slabs@)) ==> !#[trigger] col_hit::<X, B>(
                slabs@,
                old(fd).lo as int,
                old(fd).hi as int,
                j,
            ),
        },
{
    let ghost s = slabs@;
    let ghost ws = fd.search.view_tree();
    let ghost lo = fd.lo as int;
    let ghost hi = fd.hi as int;
    let ghost mut f = f0;
    loop
        invariant
            s == slabs@,
            ws == fd.search.view_tree(),
            lo == fd.lo as int,
            hi == fd.hi as int,
            find_ok::<X, B>(*fd, s, f),
            f0 <= f,
            forall|j: int| f0 <= j < f ==> !#[trigger] col_hit::<X, B>(s, lo, hi, j),
        decreases 2 * (ws.len() - fd.search.pos()) + if fd.active { 1int } else { 0int },
    {
        if fd.active {
            let ghost k = fd.search.pos() - 1;
            let ghost vs = X::vals(s[k]);
            let ghost base = pre_at(ws, k) as int;
            let r = d_scan_next::<X, B>(&mut fd.cur, fd.lo, fd.hi, Ghost(vs), Ghost(base), Ghost(f));
            if let Some(i) = r {
                proof {
                    lemma_found_in_slab::<X, B>(ws, s, lo, hi, k, f0, f, i as int);
                }
                return Some(i);
            }
            fd.active = false;
            proof {
                lemma_slab_done::<X, B>(ws, s, lo, hi, k, f0, f);
                f = sum(slab_lens(s), 0, k + 1) as int;
            }
        }
        let ghost pos0 = fd.search.pos();
        match fd.search.next_hit() {
            None => {
                proof {
                    lemma_skipped::<X, B>(ws, s, lo, hi, pos0, s.len() as int, f0);
                }
                return None;
            },
            Some((k, items, prefix)) => {
                proof {
                    lemma_skipped::<X, B>(ws, s, lo, hi, pos0, k as int, f0);
                    lemma_slab_hits::<X, B>(ws, s, lo, hi, k as int);
                    lemma_sum_le(slab_lens(s), 0, pos0, 0, k as int);
                    f = items as int;
                }
                fd.cur = d_scan_new::<X, B>(&slabs[k], items, prefix, Ghost(lo), Ghost(hi));
                fd.active = true;
            },
        }
    }
}

} // verus!
