//! A delta column's sorted search, verified with Verus.
//!
//! [`DeltaIter::seek_to_value`](crate::DeltaIter::seek_to_value) narrows
//! the iterator's window to the items equal to a target, where the
//! window's values are sorted (nulls first). The index's prefix sums jump
//! it to the slab where the running sum first reaches the target, and from
//! there it reads a run at a time: a run of equal deltas `d` after the
//! running sum `a` holds `a + d·j` at its `j`th item, so the first item at
//! or above the target is found by division. The items equal to a value
//! after the first are runs of zero deltas.
//!
//! Proved: where the window is sorted, the result is exactly the range of
//! items equal to the target, or the empty range where it would go
//! (`dfound`); in any case it is a range inside the window, and the
//! iterator ends at its start. Trusted as for reading (`diter`): the value
//! type's `to_opt`, and that the index holds each slab's aggregate.

// Verus proofs read more plainly with `x = x + n`; ghost arguments count
// towards clippy's limit.
#![allow(clippy::assign_op_pattern, clippy::too_many_arguments)]

#[cfg(verus_keep_ghost)]
use crate::btree::{fold, in_i64, passed, pre_at, reached, sum_len, OrdPrefix, PrefixAggregate};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, dval, psum};
use crate::delta::dfind::udiff;
#[cfg(verus_keep_ghost)]
use crate::delta::dfind::{
    lemma_run_item, lemma_slab_exists, lemma_slab_hits, lemma_slab_of, slab_at, sums_in_i64,
};
use crate::delta::diter::{add_run, d_next_run, d_nth};
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{
    at_run, dbefore, dindex_ok, dix_ok, lemma_crop, lemma_found, lemma_psum_cat,
};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders, ICore};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The running sum of the column's first `n` deltas.
pub(crate) open spec fn rsum<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    n: int,
) -> int {
    psum(deltas_of::<_, B>(vals_of::<X>(s).take(n)))
}

/// What item `i` of the column (slabs `s`) realizes: the running sum of
/// the deltas through it, or null.
pub(crate) open spec fn rv<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    i: int,
) -> Option<int> {
    match B::dv(vals_of::<X>(s)[i]) {
        Some(_) => Some(rsum::<X, B>(s, i + 1)),
        None => None,
    }
}

/// `a` sorts before `b`, with a null before every value.
pub(crate) open spec fn below(a: Option<int>, b: Option<int>) -> bool {
    match a {
        None => b is Some,
        Some(x) => match b {
            Some(y) => x < y,
            None => false,
        },
    }
}

/// The column's values in `p..e` are sorted.
pub(crate) open spec fn dsorted<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    p: int,
    e: int,
) -> bool {
    forall|i: int, j: int| p <= i <= j < e ==> !below(#[trigger] rv::<X, B>(s, j), #[trigger] rv::<X, B>(s, i))
}

/// A search of `p..e` for `t` found `a..b`: the values before `a` are
/// below `t`, those from `a` to `b` equal it, and the rest are above it.
pub(crate) open spec fn dfound<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    p: int,
    e: int,
    t: Option<int>,
    a: int,
    b: int,
) -> bool {
    forall|k: int| #![trigger rv::<X, B>(s, k)] p <= k < e ==> {
        &&& (k < a) == below(rv::<X, B>(s, k), t)
        &&& (k < b) == !below(t, rv::<X, B>(s, k))
    }
}

/// Every running sum of the column's deltas fits `i64`, given the index's
/// aggregates: each slab's running sums lie between its least and
/// greatest, after the sum before it, and the index's merges of those are
/// representable.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_sums_fit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    n: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= n <= vals_of::<X>(s).len(),
    ensures
        in_i64(rsum::<X, B>(s, n)),
{
    let vv = vals_of::<X>(s);
    let lens = slab_lens(s);
    if n == 0 {
        assert(vv.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(deltas_of::<_, B>(vv.take(0)) =~= Seq::<Option<i64>>::empty());
    } else {
        lemma_vals_len::<X>(s);
        let j = n - 1;
        lemma_slab_of::<X>(s, j);
        let k = slab_at(lens, j);
        lemma_slab_hits::<X, B>(ws, s, 0, 0, k);
        let st = sum(lens, 0, k);
        let vs = X::vals(s[k]);
        let m = n - st;
        lemma_upto::<X>(s, k);
        lemma_from::<X>(s, k, 0);
        assert(vv.skip(st as int).take(vs.len() as int) =~= vs);
        assert(vv.take(n) =~= vv.take(st as int) + vs.take(m));
        assert(deltas_of::<_, B>(vv.take(n)) =~= deltas_of::<_, B>(vv.take(st as int)) + deltas_of::<_, B>(vs.take(m)));
        lemma_psum_cat(deltas_of::<_, B>(vv.take(st as int)), deltas_of::<_, B>(vs.take(m)));
        assert(dbefore::<X, B>(s, st as int) == deltas_of::<_, B>(vv.take(st as int)));
        assert(sums_in_i64::<_, B>(vs, pre_at(ws, k) as int));
        assert(in_i64(pre_at(ws, k) + psum(deltas_of::<_, B>(vs.take(m)))));
    }
}

/// The column's values number its slabs' lengths.
pub(crate) proof fn lemma_vals_len<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>)
    requires
        lens_ok::<X>(s),
    ensures
        vals_of::<X>(s).len() == total(slab_lens(s)),
{
    if s.len() > 0 {
        lemma_from::<X>(s, 0, 0);
    } else {
        assert(slab_lens(s) =~= Seq::<nat>::empty());
        lemma_upto::<X>(s, 0);
        assert(s.take(0) =~= s);
        assert(sum(slab_lens(s), 0, 0) == 0);
    }
}

/// The items of a run of `c` copies of `x` from item `pos`: each realizes
/// the running sum `kk` steps on, or null.
proof fn lemma_run_rv<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    pos: int,
    c: int,
    x: <X::D as Decode>::Item,
    kk: int,
)
    requires
        0 <= pos,
        pos + c <= vals_of::<X>(s).len(),
        vals_of::<X>(s).skip(pos).take(c) == Seq::new(c as nat, |_i: int| x),
        1 <= kk <= c,
    ensures
        vals_of::<X>(s)[pos + kk - 1] == x,
        rsum::<X, B>(s, pos + kk) == rsum::<X, B>(s, pos) + kk * dval(B::dv(x)),
        rv::<X, B>(s, pos + kk - 1) == match B::dv(x) {
            Some(_) => Some(rsum::<X, B>(s, pos) + kk * dval(B::dv(x))),
            None => None::<int>,
        },
{
    lemma_run_item::<_, B>(vals_of::<X>(s), pos, c, x, kk);
}

/// The window's values come in runs: after `count` of them, the iterator's
/// next run starts `count` further on, with the values the window's next.
proof fn lemma_out_vals<'a, D: Decode, X: Decoders<'a, D = D>>(
    c: ICore<D>,
    s: Seq<Slab<X::Tail>>,
    count: int,
    x: D::Item,
)
    requires
        c.wf::<X>(s),
        1 <= count <= c.out::<X>(s).len(),
        c.out::<X>(s).take(count) == Seq::new(count as nat, |_i: int| x),
    ensures
        c.pos + count <= vals_of::<X>(s).len(),
        vals_of::<X>(s).skip(c.pos as int).take(count) == Seq::new(count as nat, |_i: int| x),
{
    let v = vals_of::<X>(s);
    assert(c.out::<X>(s).take(count) =~= v.skip(c.pos as int).take(count));
}

/// The window from `p` holds `out.len()` items, all of the column's.
proof fn lemma_window<'a, D: Decode, X: Decoders<'a, D = D>>(c: ICore<D>, s: Seq<Slab<X::Tail>>)
    requires
        c.wf::<X>(s),
    ensures
        c.pos + c.out::<X>(s).len() <= c.pos + c.items_left,
        c.out::<X>(s).len() > 0 ==> c.pos + c.out::<X>(s).len() <= vals_of::<X>(s).len(),
        c.out::<X>(s).len() < c.items_left ==> c.pos + c.out::<X>(s).len() >= vals_of::<X>(s).len(),
        forall|k: int| 0 <= k < c.out::<X>(s).len() ==> #[trigger] c.out::<X>(s)[k] == vals_of::<X>(s)[c.pos + k],
{
}

/// With the window's values sorted, none before the slab the index
/// found for `t` reaches `t`: `passed` says every slab end before it that
/// reaches `t` is followed by one that doesn't, but sorted values, once
/// at `t`, never fall back.
proof fn lemma_jump<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    t: i64,
    si: int,
    p: int,
    e: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= si < s.len(),
        passed(ws, t, si),
        0 <= p <= sum(slab_lens(s), 0, si) <= e <= vals_of::<X>(s).len(),
        dsorted::<X, B>(s, p, e),
    ensures
        forall|k: int| p <= k < sum(slab_lens(s), 0, si) ==> below(#[trigger] rv::<X, B>(s, k), Some(t as int)),
{
    let lens = slab_lens(s);
    let at = sum(lens, 0, si);
    lemma_vals_len::<X>(s);
    assert forall|k: int| p <= k < at implies below(#[trigger] rv::<X, B>(s, k), Some(t as int)) by {
        if !below(rv::<X, B>(s, k), Some(t as int)) {
            lemma_slab_of::<X>(s, k);
            lemma_slab_exists(lens, k, s.len() as int);
            let kk = slab_at(lens, k);
            if kk >= si {
                lemma_sum_le(lens, 0, si, 0, kk);
            }
            // every slab end from `kk + 1` to `si` reaches `t`
            assert forall|j: int| kk + 1 <= j <= si implies #[trigger] reached(ws, t, j) by {
                lemma_sum_le(lens, 0, kk + 1, 0, j);
                lemma_sum_le(lens, 0, j, 0, si);
                let q = sum(lens, 0, j) - 1;
                assert(k <= q < at);
                assert(!below(rv::<X, B>(s, q), rv::<X, B>(s, k)));
                lemma_found::<X, B>(ws, s, j);
                assert(dbefore::<X, B>(s, q + 1) == deltas_of::<_, B>(vals_of::<X>(s).take(q + 1)));
            }
            reveal(passed);
            assert(reached(ws, t, kk + 1));
            let j = choose|j: int| kk + 1 < j <= si && !(#[trigger] reached(ws, t, j));
            assert(reached(ws, t, j));
        }
    }
}

/// `seek_to_value` for null: the end of the window's leading nulls, from
/// the iterator's position, which stays put.
pub(crate) fn d_seek_null<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
) -> (r: usize)
    requires
        core.wf::<X>(slabs@),
    ensures
        ({
            let p = core.pos as int;
            let e = p + core.out::<X>(slabs@).len();
            &&& p <= r <= e
            &&& dsorted::<X, B>(slabs@, p, e) ==> dfound::<X, B>(slabs@, p, e, None, p, r as int)
        }),
{
    let ghost s = slabs@;
    let ghost p = core.pos as int;
    let ghost o = core.out::<X>(s);
    let ghost e = p + o.len();
    let mut probe = core.dup();
    let start = core.pos;
    let end = core.pos + core.items_left;
    let mut run_end = start;
    proof {
        lemma_window::<X::D, X>(*core, s);
        assert(o.skip(0) =~= o);
    }
    while run_end < end
        invariant_except_break
            probe.wf::<X>(s),
            probe.pos == run_end,
            probe.out::<X>(s) == o.skip(run_end - p),
        invariant
            s == slabs@,
            p <= run_end <= e,
            e == p + o.len(),
            end == p + core.items_left,
            e <= end,
            forall|k: int| p <= k < run_end ==> #[trigger] rv::<X, B>(s, k) is None,
        ensures
            p <= run_end <= e,
            forall|k: int| p <= k < run_end ==> #[trigger] rv::<X, B>(s, k) is None,
            run_end < e ==> rv::<X, B>(s, run_end as int) is Some,
        decreases e - run_end,
    {
        let ghost c0 = probe;
        let max = probe.items_left;
        let Some(run) = probe.next_run_max::<X>(slabs, max) else {
            proof {
                lemma_window::<X::D, X>(c0, s);
                assert(o.skip(run_end - p).len() == e - run_end);
            }
            break;
        };
        proof {
            lemma_out_vals::<X::D, X>(c0, s, run.count as int, run.value);
            lemma_window::<X::D, X>(c0, s);
            assert(o.skip(run_end - p).skip(run.count as int) =~= o.skip(run_end + run.count - p));
        }
        if B::to_opt(run.value).is_some() {
            proof {
                lemma_run_rv::<X, B>(s, run_end as int, run.count as int, run.value, 1);
            }
            break;
        }
        proof {
            assert forall|k: int| p <= k < run_end + run.count implies #[trigger] rv::<X, B>(s, k) is None by {
                if k >= run_end {
                    lemma_run_rv::<X, B>(s, run_end as int, run.count as int, run.value, k - run_end + 1);
                }
            }
        }
        run_end = run_end + run.count;
    }
    proof {
        if dsorted::<X, B>(s, p, e) {
            assert forall|k: int| #![trigger rv::<X, B>(s, k)] p <= k < e implies {
                &&& (k < p) == below(rv::<X, B>(s, k), None)
                &&& (k < run_end) == !below(None, rv::<X, B>(s, k))
            } by {
                if k >= run_end {
                    assert(!below(rv::<X, B>(s, k), rv::<X, B>(s, run_end as int)));
                }
            }
        }
    }
    if run_end < end {
        run_end
    } else {
        end
    }
}


/// Moving `n` items into the window leaves the rest of it.
proof fn lemma_out_skip<'a, D: Decode, X: Decoders<'a, D = D>>(c0: ICore<D>, c1: ICore<D>, s: Seq<Slab<X::Tail>>, n: int)
    requires
        c0.wf::<X>(s),
        0 <= n <= c0.out::<X>(s).len(),
        c1.pos == c0.pos + n,
        c1.items_left == c0.items_left - n,
    ensures
        c1.out::<X>(s) == c0.out::<X>(s).skip(n),
{
    let v = vals_of::<X>(s);
    if c0.pos >= v.len() {
        assert(c1.out::<X>(s) =~= c0.out::<X>(s).skip(n));
    } else {
        assert(c1.out::<X>(s) =~= c0.out::<X>(s).skip(n));
    }
}

/// The first of `count` items `a + d·kk` (`d > 0`) to reach `t`, which
/// the last does: `(t - a - 1) / d + 1`.
proof fn lemma_steps(a: int, d: int, t: int, count: int, q: int)
    requires
        d > 0,
        t > a,
        count >= 1,
        a + count * d >= t,
        q == (t - a - 1) / d,
    ensures
        0 <= q < count,
        a + (q + 1) * d >= t,
        forall|kk: int| 1 <= kk <= q ==> a + #[trigger] (kk * d) < t,
{
    let x = t - a - 1;
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(x, d);
    vstd::arithmetic::div_mod::lemma_mod_pos_bound(x, d);
    vstd::arithmetic::div_mod::lemma_div_pos_is_pos(x, d);
    let r = x % d;
    assert(x == d * q + r);
    assert(q * d <= x) by (nonlinear_arith)
        requires
            x == d * q + r,
            r >= 0,
    ;
    assert((q + 1) * d > x) by (nonlinear_arith)
        requires
            x == d * q + r,
            r < d,
    ;
    assert(q < count) by (nonlinear_arith)
        requires
            q * d <= x,
            x < count * d,
            d > 0,
    ;
    assert forall|kk: int| 1 <= kk <= q implies a + #[trigger] (kk * d) < t by {
        assert(kk * d <= q * d) by (nonlinear_arith)
            requires
                kk <= q,
                d > 0,
        ;
    }
}

/// Sorted values, below `t` before `a`, equal to it from `a` to `b`, and
/// above it at `b`: the search found `a..b`.
proof fn lemma_dfound<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    p: int,
    e: int,
    t: int,
    a: int,
    b: int,
)
    requires
        dsorted::<X, B>(s, p, e),
        p <= a <= b <= e,
        forall|k: int| p <= k < a ==> below(#[trigger] rv::<X, B>(s, k), Some(t)),
        forall|k: int| a <= k < b ==> #[trigger] rv::<X, B>(s, k) == Some(t),
        b < e ==> below(Some(t), rv::<X, B>(s, b)),
    ensures
        dfound::<X, B>(s, p, e, Some(t), a, b),
{
    assert forall|k: int| #![trigger rv::<X, B>(s, k)] p <= k < e implies {
        &&& (k < a) == below(rv::<X, B>(s, k), Some(t))
        &&& (k < b) == !below(Some(t), rv::<X, B>(s, k))
    } by {
        if k >= b {
            assert(!below(rv::<X, B>(s, k), rv::<X, B>(s, b)));
        }
    }
}

/// The facts the run-by-run search keeps about the probe: it stands at
/// item `q` of the window `o` (from `p`), with its running sum.
pub(crate) open spec fn probe_at<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    probe: ICore<X::D>,
    prun: i64,
    s: Seq<Slab<X::Tail>>,
    o: Seq<<X::D as Decode>::Item>,
    p: int,
    q: int,
) -> bool {
    &&& at_run::<X, B>(probe, prun, s)
    &&& probe.pos == q
    &&& probe.out::<X>(s) == o.skip(q - p)
}

/// Every running sum of the column fits `i64`.
pub(crate) open spec fn all_fit<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(s: Seq<Slab<X::Tail>>) -> bool {
    forall|n: int| 0 <= n <= vals_of::<X>(s).len() ==> in_i64(#[trigger] rsum::<X, B>(s, n))
}

/// Read one run from the probe, at item `q`, keeping its running sum:
/// the run's items are the window's next, `x` repeated.
fn d_step<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    probe: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    prun: &mut i64,
    Ghost(o): Ghost<Seq<<X::D as Decode>::Item>>,
    Ghost(p): Ghost<int>,
    Ghost(q): Ghost<int>,
) -> (r: Option<crate::Run<<X::D as Decode>::Item>>)
    requires
        probe_at::<X, B>(*old(probe), *old(prun), slabs@, o, p, q),
        p <= q <= p + o.len(),
        all_fit::<X, B>(slabs@),
    ensures
        match r {
            Some(run) => {
                let c = run.count as int;
                &&& 1 <= c
                &&& q + c <= p + o.len()
                &&& q + c <= vals_of::<X>(slabs@).len()
                &&& vals_of::<X>(slabs@).skip(q).take(c) == Seq::new(c as nat, |_i: int| run.value)
                &&& probe_at::<X, B>(*final(probe), *final(prun), slabs@, o, p, q + c)
                &&& *old(prun) == rsum::<X, B>(slabs@, q)
                &&& *final(prun) == rsum::<X, B>(slabs@, q + c)
            },
            None => q == p + o.len(),
        },
{
    let ghost s = slabs@;
    let ghost c1 = *probe;
    proof {
        lemma_window::<X::D, X>(c1, s);
    }
    let max = probe.items_left;
    let r = d_next_run::<X, B>(probe, slabs, max, prun);
    proof {
        let vv = vals_of::<X>(s);
        match r {
            Some(run) => {
                let c = run.count as int;
                lemma_out_vals::<X::D, X>(c1, s, c, run.value);
                assert(o.skip(q - p).skip(c) =~= o.skip(q + c - p));
                assert(dbefore::<X, B>(s, q) == deltas_of::<_, B>(vv.take(q)));
                assert(dbefore::<X, B>(s, q + c) == deltas_of::<_, B>(vv.take(q + c)));
                lemma_crop(rsum::<X, B>(s, q));
                lemma_crop(rsum::<X, B>(s, q + c));
            },
            None => {
                assert(o.skip(q - p).len() == p + o.len() - q);
            },
        }
    }
    r
}

/// After a match ending at a run's end (`q`, where the running sum is
/// `t`), the runs of zero deltas that follow repeat it: where the
/// matches end.
fn d_zeros<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    probe: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    prun: &mut i64,
    q: usize,
    end: usize,
    Ghost(t): Ghost<i64>,
    Ghost(o): Ghost<Seq<<X::D as Decode>::Item>>,
    Ghost(p): Ghost<int>,
) -> (r: usize)
    requires
        probe_at::<X, B>(*old(probe), *old(prun), slabs@, o, p, q as int),
        p <= q <= p + o.len() <= end,
        all_fit::<X, B>(slabs@),
        rsum::<X, B>(slabs@, q as int) == t,
    ensures
        q <= r <= p + o.len(),
        forall|k: int| q <= k < r ==> #[trigger] rv::<X, B>(slabs@, k) == Some(t as int),
        r < p + o.len() ==> rv::<X, B>(slabs@, r as int) != Some(t as int),
{
    let ghost s = slabs@;
    let ghost e = p + o.len();
    let mut run_end = q;
    while run_end < end
        invariant_except_break
            probe_at::<X, B>(*probe, *prun, s, o, p, run_end as int),
            rsum::<X, B>(s, run_end as int) == t,
        invariant
            s == slabs@,
            e == p + o.len(),
            e <= end,
            p <= q <= run_end <= e,
            all_fit::<X, B>(s),
            forall|k: int| q <= k < run_end ==> #[trigger] rv::<X, B>(s, k) == Some(t as int),
        ensures
            q <= run_end <= e,
            forall|k: int| q <= k < run_end ==> #[trigger] rv::<X, B>(s, k) == Some(t as int),
            run_end < e ==> rv::<X, B>(s, run_end as int) != Some(t as int),
        decreases e - run_end,
    {
        let Some(next) = d_step::<X, B>(probe, slabs, prun, Ghost(o), Ghost(p), Ghost(run_end as int)) else {
            break;
        };
        let ghost n2 = next.count as int;
        proof {
            lemma_run_rv::<X, B>(s, run_end as int, n2, next.value, 1);
        }
        let zero = match B::to_opt(next.value) {
            Some(x) => x == 0,
            None => false,
        };
        if !zero {
            break;
        }
        proof {
            assert forall|k: int| q <= k < run_end + n2 implies #[trigger] rv::<X, B>(s, k) == Some(t as int) by {
                if k >= run_end {
                    lemma_run_rv::<X, B>(s, run_end as int, n2, next.value, k - run_end + 1);
                }
            }
            lemma_run_rv::<X, B>(s, run_end as int, n2, next.value, n2);
        }
        run_end = run_end + next.count;
    }
    run_end
}

/// The run of `count` copies of `x` (delta `d`) at item `pos`, after the
/// running sum `a`, ends at or above `t`: its first item at or above `t`,
/// and where the items equal to `t` from there end.
#[verifier::spinoff_prover]
fn d_cross<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    probe: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    prun: &mut i64,
    pos: usize,
    count: usize,
    a: i64,
    d: i64,
    t: i64,
    end: usize,
    Ghost(x): Ghost<<X::D as Decode>::Item>,
    Ghost(o): Ghost<Seq<<X::D as Decode>::Item>>,
    Ghost(p): Ghost<int>,
) -> (r: (usize, usize))
    requires
        probe_at::<X, B>(*old(probe), *old(prun), slabs@, o, p, pos + count),
        p <= pos,
        1 <= count,
        pos + count <= p + o.len() <= end,
        pos + count <= vals_of::<X>(slabs@).len(),
        vals_of::<X>(slabs@).skip(pos as int).take(count as int) == Seq::new(count as nat, |_i: int| x),
        B::dv(x) == Some(d),
        all_fit::<X, B>(slabs@),
        a == rsum::<X, B>(slabs@, pos as int),
        *old(prun) == rsum::<X, B>(slabs@, pos + count),
        *old(prun) >= t,
        dsorted::<X, B>(slabs@, p, p + o.len()) ==> forall|k: int| p <= k < pos ==> below(#[trigger] rv::<X, B>(slabs@, k), Some(t as int)),
    ensures
        pos <= r.0 <= r.1 <= p + o.len(),
        r.0 < pos + count,
        dsorted::<X, B>(slabs@, p, p + o.len()) ==> dfound::<X, B>(slabs@, p, p + o.len(), Some(t as int), r.0 as int, r.1 as int),
{
    let ghost s = slabs@;
    let ghost e = p + o.len();
    let ghost c = count as int;
    proof {
        lemma_run_rv::<X, B>(s, pos as int, c, x, c);
    }
    let steps: usize = if d <= 0 || t <= a {
        1
    } else {
        let u = udiff(t, a);
        let q = (u - 1) / (d as u64);
        proof {
            lemma_steps(a as int, d as int, t as int, c, q as int);
        }
        (q + 1) as usize
    };
    let ghost st = steps as int;
    proof {
        lemma_run_rv::<X, B>(s, pos as int, c, x, st);
        if d < 0 {
            assert(c * d <= d) by (nonlinear_arith)
                requires
                    c >= 1,
                    d < 0,
            ;
        }
        assert(rsum::<X, B>(s, pos + st) >= t);
    }
    let run_start = pos + steps - 1;
    let v = add_run(a, d, steps, Ghost(a as int));
    proof {
        lemma_crop(a + st * d);
        assert(v == rsum::<X, B>(s, pos + st));
        assert(rv::<X, B>(s, run_start as int) == Some(v as int));
    }
    let mut run_end = run_start;
    if v == t {
        run_end = run_end + 1;
        if d == 0 {
            run_end = pos + count;
        }
        proof {
            assert forall|k: int| run_start <= k < run_end implies #[trigger] rv::<X, B>(s, k) == Some(t as int) by {
                lemma_run_rv::<X, B>(s, pos as int, c, x, k - pos + 1);
            }
            if run_end < pos + c {
                lemma_run_rv::<X, B>(s, pos as int, c, x, run_end - pos + 1);
                assert((st + 1) * d == st * d + d) by (nonlinear_arith);
            }
        }
        if run_end == pos + count {
            run_end = d_zeros::<X, B>(probe, slabs, prun, run_end, end, Ghost(t), Ghost(o), Ghost(p));
        }
    }
    proof {
        if dsorted::<X, B>(s, p, e) {
            assert forall|k: int| p <= k < run_start implies below(#[trigger] rv::<X, B>(s, k), Some(t as int)) by {
                if k >= pos {
                    lemma_run_rv::<X, B>(s, pos as int, c, x, k - pos + 1);
                }
            }
            if run_end < e {
                assert(!below(rv::<X, B>(s, run_end as int), rv::<X, B>(s, run_start as int)));
            }
            lemma_dfound::<X, B>(s, p, e, t as int, run_start as int, run_end as int);
        }
    }
    (run_start, run_end)
}

/// `seek_to_value` for the value `t`: where the window's values are
/// sorted, the range of the items equal to `t`, or the empty range where
/// it would go; in any case a range in the window. The iterator moves to
/// the range's start.
#[verifier::spinoff_prover]
pub(crate) fn d_seek<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    running: &mut i64,
    t: i64,
) -> (r: (usize, usize))
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let p = old(core).pos as int;
            let o = old(core).out::<X>(slabs@);
            let e = p + o.len();
            &&& p <= r.0 <= r.1 <= e
            &&& final(core).pos == r.0
            &&& final(core).out::<X>(slabs@) == o.skip(r.0 - p)
            &&& dsorted::<X, B>(slabs@, p, e) ==> dfound::<X, B>(slabs@, p, e, Some(t as int), r.0 as int, r.1 as int)
        }),
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost p = core.pos as int;
    let ghost o = core.out::<X>(s);
    let ghost e = p + o.len();
    let ghost vv = vals_of::<X>(s);
    proof {
        lemma_window::<X::D, X>(c0, s);
        lemma_vals_len::<X>(s);
        assert forall|n: int| 0 <= n <= vv.len() implies in_i64(#[trigger] rsum::<X, B>(s, n)) by {
            match ix {
                Some(ix) => lemma_sums_fit::<X, B>(ix@, s, n),
                None => {
                    assert(vv.take(n) =~= Seq::<<X::D as Decode>::Item>::empty());
                    assert(deltas_of::<_, B>(vv.take(n)) =~= Seq::<Option<i64>>::empty());
                },
            }
        }
        assert(o.skip(0) =~= o);
    }
    let end = core.pos + core.items_left;
    let mut probe = core.dup();
    let mut prun = *running;
    // jump to the slab where the running sum first reaches `t`: with the
    // window sorted, the values before it are below `t`
    if let Some(ix) = ix {
        let (si, prefix, at) = ix.find_slab_at_prefix(&t);
        if at > probe.pos && at < end && si < slabs.len() {
            let ghost c1 = probe;
            proof {
                let ws = ix@;
                lemma_found::<X, B>(ws, s, si as int);
                lemma_sum_le(slab_lens(s), 0, si as int, 0, s.len() as int);
                if dsorted::<X, B>(s, p, e) {
                    lemma_jump::<X, B>(ws, s, t, si as int, p, e);
                }
                lemma_crop(prefix as int);
            }
            prun = prefix;
            probe.advance_to_slab::<X>(slabs, si, at);
            proof {
                lemma_out_skip::<X::D, X>(c1, probe, s, at - p);
            }
        }
    }
    // from there, a run at a time: the first item at or above `t`
    let mut pos = probe.pos;
    let mut found: Option<(usize, usize)> = None;
    while pos < end
        invariant_except_break
            probe_at::<X, B>(probe, prun, s, o, p, pos as int),
            found is None,
        invariant
            s == slabs@,
            p <= pos <= e,
            e == p + o.len(),
            end == p + c0.items_left,
            e <= end,
            all_fit::<X, B>(s),
            dsorted::<X, B>(s, p, e) ==> forall|k: int| p <= k < pos ==> below(#[trigger] rv::<X, B>(s, k), Some(t as int)),
        ensures
            match found {
                None => pos == e,
                Some(r) => {
                    &&& p <= r.0 <= r.1 <= e
                    &&& dsorted::<X, B>(s, p, e) ==> dfound::<X, B>(s, p, e, Some(t as int), r.0 as int, r.1 as int)
                },
            },
        decreases e - pos,
    {
        let a = prun;
        let Some(run) = d_step::<X, B>(&mut probe, slabs, &mut prun, Ghost(o), Ghost(p), Ghost(pos as int)) else {
            break;
        };
        let count = run.count;
        let ghost c = count as int;
        proof {
            lemma_run_rv::<X, B>(s, pos as int, c, run.value, c);
        }
        match B::to_opt(run.value) {
            None => {
                proof {
                    if dsorted::<X, B>(s, p, e) {
                        assert forall|k: int| p <= k < pos + c implies below(#[trigger] rv::<X, B>(s, k), Some(t as int)) by {
                            if k >= pos {
                                lemma_run_rv::<X, B>(s, pos as int, c, run.value, k - pos + 1);
                            }
                        }
                    }
                }
                pos = pos + count;
            },
            Some(d) => {
                if prun < t {
                    proof {
                        if dsorted::<X, B>(s, p, e) {
                            assert forall|k: int| p <= k < pos + c implies below(#[trigger] rv::<X, B>(s, k), Some(t as int)) by {
                                if k >= pos {
                                    let kk = k - pos + 1;
                                    lemma_run_rv::<X, B>(s, pos as int, c, run.value, kk);
                                    if d >= 0 {
                                        assert(kk * d <= c * d) by (nonlinear_arith)
                                            requires
                                                kk <= c,
                                                d >= 0,
                                        ;
                                    } else if c >= 2 {
                                        // falling values aren't sorted
                                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, 1);
                                        lemma_run_rv::<X, B>(s, pos as int, c, run.value, 2);
                                        assert(!below(rv::<X, B>(s, pos + 1), rv::<X, B>(s, pos as int)));
                                    }
                                }
                            }
                        }
                    }
                    pos = pos + count;
                } else {
                    found = Some(d_cross::<X, B>(&mut probe, slabs, &mut prun, pos, count, a, d, t, end, Ghost(run.value), Ghost(o), Ghost(p)));
                    break;
                }
            },
        }
    }
    let (run_start, run_end) = match found {
        Some(r) => r,
        None => {
            // above every value: where the window's items end
            proof {
                if dsorted::<X, B>(s, p, e) {
                    lemma_dfound::<X, B>(s, p, e, t as int, e, e);
                }
            }
            (pos, pos)
        },
    };
    let n = run_start - core.pos;
    if n > 0 {
        d_nth::<X, B>(core, slabs, ix, n - 1, running);
    } else {
        proof {
            assert(o.skip(0) =~= o);
        }
    }
    (run_start, run_end)
}

} // verus!
