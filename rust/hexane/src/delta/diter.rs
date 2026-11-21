//! A delta column's values, verified with Verus.
//!
//! A delta column stores each value as its difference from the one
//! before (a null holds the running sum where it is), so reading it means
//! summing: [`DeltaIter`](crate::DeltaIter) walks the column with the
//! verified iterator core ([`ICore`]) and keeps the running sum of the
//! deltas, and it jumps to a far slab by the index, which hands back the
//! sum of the slabs before it (`SlabAgg`'s total). The functions here keep
//! its invariant (`at_run`): the running sum is the sum of the deltas
//! before the iterator, so each value it yields is the column's.
//!
//! The running sum is an `i64` that wraps, as release arithmetic does
//! (the code adds and multiplies with `wrapping_*`, which compiles the
//! same): it equals the deltas' sum modulo 2^64 (`crop`), which is the sum
//! itself whenever that fits `i64`, as a column's values do.
//!
//! The readers require that the index holds each slab's aggregate, with
//! the sums at slab boundaries fitting `i64` (`dix_ok`), which follows
//! from the column's type invariant (`ddomain`). Trusted: the value
//! type's `to_opt` (`DeltaOps`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::btree::{fold, in_i64, sum_len, valid, PrefixAggregate, SlabAggregate};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::deltas_of;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, dval, lemma_merge, lemma_run_sum, lemma_take_run, psum, reps};
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{
    each, first_run, lemma_from, lemma_skip_add, lemma_upto, lemma_window_past, lens_ok, past,
    vals_of, window,
};
use crate::iter_core::{Decode, Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::before;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

use crate::trusted::diter_past_window as past_window;
verus! {

/// `x` as a wrapping `i64` holds it: its remainder modulo 2^64.
pub(crate) open spec fn crop(x: int) -> i64 {
    vstd::wrapping::i64_specs::signed_crop(x)
}

/// `crop` keeps a number's remainder modulo 2^64, and a number that fits
/// `i64` as it is.
pub(crate) proof fn lemma_crop(x: int)
    ensures
        crop(x) as int % 0x1_0000_0000_0000_0000 == x % 0x1_0000_0000_0000_0000,
        in_i64(x) ==> crop(x) == x,
{
    let r = 0x1_0000_0000_0000_0000int;
    let m = x % r;
    assert(0 <= m < r);
    if m > i64::MAX as int {
        assert((m - r) % r == m) by {
            vstd::arithmetic::div_mod::lemma_mod_sub_multiples_vanish(m, r);
            vstd::arithmetic::div_mod::lemma_small_mod(m as nat, r as nat);
        }
    } else {
        vstd::arithmetic::div_mod::lemma_small_mod(m as nat, r as nat);
    }
    if in_i64(x) {
        if x >= 0 {
            vstd::arithmetic::div_mod::lemma_small_mod(x as nat, r as nat);
        } else {
            vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(x, r);
            vstd::arithmetic::div_mod::lemma_small_mod((x + r) as nat, r as nat);
        }
    }
}

/// Numbers alike modulo 2^64 crop alike.
pub(crate) proof fn lemma_crop_eq(x: int, y: int)
    requires
        x % 0x1_0000_0000_0000_0000 == y % 0x1_0000_0000_0000_0000,
    ensures
        crop(x) == crop(y),
{
}

/// Adding to a cropped sum crops the whole sum.
pub(crate) proof fn lemma_crop_add(a: int, b: int)
    ensures
        crop(crop(a) + b) == crop(a + b),
{
    let r = 0x1_0000_0000_0000_0000int;
    lemma_crop(a);
    vstd::arithmetic::div_mod::lemma_add_mod_noop(crop(a) as int, b, r);
    vstd::arithmetic::div_mod::lemma_add_mod_noop(a, b, r);
    lemma_crop_eq(crop(a) + b, a + b);
}

/// `wrapping_add` crops the sum.
pub(crate) proof fn lemma_wrapping_add(x: i64, y: i64)
    ensures
        vstd::wrapping::i64_specs::wrapping_add(x, y) == crop(x + y),
{
    let r = 0x1_0000_0000_0000_0000int;
    let s = x + y;
    if s > i64::MAX {
        lemma_crop(s - r);
        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(s - r, r);
        lemma_crop_eq(s - r, s);
    } else if s < i64::MIN {
        lemma_crop(s + r);
        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(s, r);
        lemma_crop_eq(s + r, s);
    } else {
        lemma_crop(s);
    }
}

/// `n`, as an `i64` with the same remainder modulo 2^64 (what `n as i64`
/// computes; Verus leaves an out-of-range cast unspecified).
#[inline(always)]
fn count_i64(n: usize) -> (r: i64)
    ensures
        r as int % 0x1_0000_0000_0000_0000 == n as int % 0x1_0000_0000_0000_0000,
{
    let c = n as u64;
    if c <= i64::MAX as u64 {
        c as i64
    } else {
        proof {
            vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(
                c as int - 0x1_0000_0000_0000_0000int,
                0x1_0000_0000_0000_0000int,
            );
        }
        (c - 0x8000_0000_0000_0000u64) as i64 + i64::MIN
    }
}

/// The running sum after a run: `running + d * n`, wrapping.
#[inline(always)]
pub(crate) fn add_run(running: i64, d: i64, n: usize, Ghost(p): Ghost<int>) -> (r: i64)
    requires
        running == crop(p),
    ensures
        r == crop(p + n * d),
{
    let c = count_i64(n);
    let m = d.wrapping_mul(c);
    let r = running.wrapping_add(m);
    proof {
        let k = 0x1_0000_0000_0000_0000int;
        // d * c and d * n agree modulo 2^64, as c and n do
        vstd::arithmetic::div_mod::lemma_mul_mod_noop_right(d as int, c as int, k);
        vstd::arithmetic::div_mod::lemma_mul_mod_noop_right(d as int, n as int, k);
        lemma_crop_eq(d * c, d * n);
        lemma_wrapping_add(running, m);
        lemma_crop_add(p, crop(d * n) as int);
        lemma_crop_add(d * n, p);
        assert(d * n == n * d) by (nonlinear_arith);
    }
    r
}

/// The running sum after deltas `a` then `b`.
pub(crate) proof fn lemma_psum_cat(a: Seq<Option<i64>>, b: Seq<Option<i64>>)
    ensures
        psum(a + b) == psum(a) + psum(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        lemma_psum_cat(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        assert((a + b).last() == b.last());
    }
}

/// The deltas of the values before item `pos` of slabs `s` (all of them,
/// at or past the end).
pub(crate) open spec fn dbefore<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    pos: int,
) -> Seq<Option<i64>> {
    deltas_of::<_, B>(before::<X>(s, pos))
}

/// The iterator's running sum is the deltas' before it.
pub(crate) open spec fn at_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: ICore<X::D>,
    running: i64,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    &&& core.wf::<X>(s)
    &&& running == crop(psum(dbefore::<X, B>(s, core.pos as int)))
}

/// What delta `d` realizes after the running sum `r`: the new sum, or
/// null.
pub(crate) open spec fn realize(r: int, d: Option<i64>) -> Option<i64> {
    match d {
        Some(x) => Some(crop(r + x)),
        None => None,
    }
}

/// The value at window offset `n` realizes, with the running sum before
/// the iterator at `p0`.
pub(crate) open spec fn realized_at<V: Copy, B: DeltaOps<V>>(o: Seq<V>, p0: int, n: int) -> Option<i64> {
    realize(p0 + psum(deltas_of::<V, B>(o.take(n))), B::dv(o[n]))
}

/// Each slab's weight in the index is its aggregate, and the sums at
/// every slab boundary fit `i64` (`valid`): the column's invariant.
pub(crate) open spec fn dix_ok<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    &&& ws.len() == s.len()
    &&& valid(ws)
    &&& forall|i: int| 0 <= i < s.len() ==> agg_of(#[trigger] ws[i], deltas_of::<_, B>(X::vals(s[i])))
}

/// The index: the column's, or for a default iterator none, over no
/// slabs.
pub(crate) open spec fn dindex_ok<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ix: Option<&SlabBTree<SlabAgg>>,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    match ix {
        Some(ix) => ix.wf() && dix_ok::<X, B>(ix@, s),
        None => s.len() == 0,
    }
}

/// The index's aggregate of the slabs before slab `k` is that of their
/// deltas.
pub(crate) proof fn lemma_dfold<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k <= s.len(),
    ensures
        agg_of(fold(ws.subrange(0, k)), deltas_of::<_, B>(vals_of::<X>(s.take(k)))),
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        vals_of::<X>(s.take(k)).len() == sum(slab_lens(s), 0, k),
    decreases k,
{
    let w0 = ws.subrange(0, k);
    if k == 0 {
        assert(w0 =~= Seq::<SlabAgg>::empty());
        assert(s.take(0) =~= Seq::<Slab<X::Tail>>::empty());
        assert(each::<X>(s.take(0)) =~= Seq::<Seq<<X::D as Decode>::Item>>::empty());
        assert(deltas_of::<_, B>(vals_of::<X>(s.take(0))) =~= Seq::<Option<i64>>::empty());
    } else {
        lemma_dfold::<X, B>(ws, s, k - 1);
        let w1 = ws.subrange(0, k - 1);
        assert(w0.drop_last() =~= w1);
        assert(w0.last() == ws[k - 1]);
        lemma_sum_split(slab_lens(s), 0, k - 1, k);
        lemma_sum_one(slab_lens(s), k - 1);
        lemma_sum_le(slab_lens(s), 0, k, 0, s.len() as int);
        // the merge is representable: the index is valid
        crate::btree::lemma_fold_one(ws, k - 1);
        assert(SlabAgg::merge_ok(fold(ws.subrange(0, k - 1)), fold(ws.subrange(k - 1, k))));
        assert(each::<X>(s.take(k)) =~= each::<X>(s.take(k - 1)).push(X::vals(s[k - 1])));
        assert(each::<X>(s.take(k)).drop_last() =~= each::<X>(s.take(k - 1)));
        let a = vals_of::<X>(s.take(k - 1));
        let b = X::vals(s[k - 1]);
        assert(deltas_of::<_, B>(a + b) =~= deltas_of::<_, B>(a) + deltas_of::<_, B>(b));
        lemma_merge(fold(w1), deltas_of::<_, B>(a), ws[k - 1], deltas_of::<_, B>(b));
    }
}

/// Taking `k` copies of `v` from the window adds them to the deltas
/// before the iterator.
proof fn lemma_dtake<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    k: nat,
    v: <X::D as Decode>::Item,
)
    requires
        c0.wf::<X>(s),
        k <= c0.out::<X>(s).len(),
        c0.out::<X>(s).take(k as int) =~= Seq::new(k, |_i: int| v),
    ensures
        psum(dbefore::<X, B>(s, c0.pos + k)) == psum(dbefore::<X, B>(s, c0.pos as int)) + k * dval(
            B::dv(v),
        ),
{
    let vv = vals_of::<X>(s);
    let o = c0.out::<X>(s);
    if k == 0 {
        assert(k * dval(B::dv(v)) == 0) by (nonlinear_arith) requires k == 0;
    } else {
        assert(c0.pos + k <= vv.len());
        assert(vv.skip(c0.pos as int).take(k as int) == Seq::new(k, |_i: int| v)) by {
            assert(vv.skip(c0.pos as int).take(k as int) =~= o.take(k as int));
        }
        lemma_take_run::<_, B>(vv, c0.pos as int, k as int, v);
        lemma_run_sum(deltas_of::<_, B>(vv.take(c0.pos as int)), k, B::dv(v));
    }
}

/// `next`, adding the delta to the running sum: the value it realizes.
#[inline(always)]
pub(crate) fn d_next<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        match r {
            Some(x) => {
                let o = old(core).out::<X>(slabs@);
                &&& o.len() > 0
                &&& x == realize(psum(dbefore::<X, B>(slabs@, old(core).pos as int)), B::dv(o[0]))
                &&& final(core).out::<X>(slabs@) == o.skip(1)
                &&& final(core).pos == old(core).pos + 1
                &&& final(core).items_left == old(core).items_left - 1
            },
            None => {
                &&& old(core).out::<X>(slabs@).len() == 0
                &&& final(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == 0
            },
        },
{
    let ghost c0 = *core;
    let ghost p = psum(dbefore::<X, B>(slabs@, c0.pos as int));
    let v = core.next::<X>(slabs)?;
    proof {
        assert(c0.out::<X>(slabs@).take(1) =~= Seq::new(1, |_i: int| v));
        lemma_dtake::<X, B>(c0, slabs@, 1, v);
    }
    match B::to_opt(v) {
        None => Some(None),
        Some(d) => {
            *running = running.wrapping_add(d);
            proof {
                lemma_wrapping_add(*old(running), d);
                lemma_crop_add(p, d as int);
            }
            Some(Some(*running))
        },
    }
}

/// `next_run_max`, adding the run to the running sum.
#[inline(always)]
pub(crate) fn d_next_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    max: usize,
    running: &mut i64,
) -> (r: Option<Run<<X::D as Decode>::Item>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        match r {
            Some(run) => {
                let o = old(core).out::<X>(slabs@);
                &&& 1 <= run.count <= max
                &&& run.count <= o.len()
                &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                &&& final(core).out::<X>(slabs@) == o.skip(run.count as int)
                &&& final(core).pos == old(core).pos + run.count
                &&& final(core).items_left == old(core).items_left - run.count
                &&& run.count < max && run.count < o.len() ==> o[run.count as int] != run.value
            },
            None => {
                &&& max == 0 || old(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == if max == 0 {
                    old(core).items_left
                } else {
                    0
                }
            },
        },
{
    let ghost c0 = *core;
    let ghost p = psum(dbefore::<X, B>(slabs@, c0.pos as int));
    let r = core.next_run_max::<X>(slabs, max);
    if let Some(run) = &r {
        proof {
            lemma_dtake::<X, B>(c0, slabs@, run.count as nat, run.value);
        }
        if let Some(d) = B::to_opt(run.value) {
            *running = add_run(*running, d, run.count, Ghost(p));
        } else {
            proof {
                assert(run.count * dval(B::dv(run.value)) == 0);
            }
        }
    }
    r
}

/// The target is the last of a run of `m + 1` copies of `x` from window
/// offset `k`: the deltas before it are those before the run and `m` of
/// the run's.
proof fn lemma_target_in_run<V: Copy, B: DeltaOps<V>>(o0: Seq<V>, k: int, m: int, x: V)
    requires
        0 <= k,
        0 <= m,
        k + m + 1 <= o0.len(),
        o0.skip(k).take(m + 1) =~= Seq::new((m + 1) as nat, |_i: int| x),
    ensures
        o0[k + m] == x,
        psum(deltas_of::<V, B>(o0.take(k + m))) == psum(deltas_of::<V, B>(o0.take(k))) + m * dval(B::dv(x)),
        (m + 1) * dval(B::dv(x)) == m * dval(B::dv(x)) + dval(B::dv(x)),
{
    assert(o0.skip(k).take(m + 1)[m] == x);
    assert(o0.skip(k).take(m) =~= Seq::new(m as nat, |_i: int| x)) by {
        assert forall|i: int| 0 <= i < m implies #[trigger] o0.skip(k).take(m)[i] == x by {
            assert(o0.skip(k).take(m + 1)[i] == x);
        }
    }
    lemma_take_run::<_, B>(o0, k, m, x);
    lemma_run_sum(deltas_of::<_, B>(o0.take(k)), m as nat, B::dv(x));
    let d = dval(B::dv(x));
    assert((m + 1) * d == m * d + d) by (nonlinear_arith);
}

/// A run of `c` copies of `x` at window offset `k`: the deltas through it
/// sum to those before it and `c` of `x`'s, and past it the window goes on
/// from `k + c`.
proof fn lemma_run_step<V: Copy, B: DeltaOps<V>>(o0: Seq<V>, k: int, c: int, x: V)
    requires
        0 <= k,
        0 <= c,
        k + c <= o0.len(),
        o0.skip(k).take(c) =~= Seq::new(c as nat, |_i: int| x),
    ensures
        psum(deltas_of::<V, B>(o0.take(k + c))) == psum(deltas_of::<V, B>(o0.take(k))) + c * dval(B::dv(x)),
        o0.skip(k).skip(c) == o0.skip(k + c),
{
    lemma_take_run::<_, B>(o0, k, c, x);
    lemma_run_sum(deltas_of::<_, B>(o0.take(k)), c as nat, B::dv(x));
}

/// The window holds no more than the items left.
proof fn lemma_out_len<'a, X: Decoders<'a>>(c: ICore<X::D>, s: Seq<Slab<X::Tail>>)
    ensures
        c.out::<X>(s).len() <= c.items_left,
{
}

/// A run of `c` copies of `x` taken by the iterator `c1`, which stands `k`
/// items into the window `o0` with the running sum `p0` before that: the
/// iterator `c2` after it stands `k + c` items in, its running sum `c` of
/// `x`'s deltas on.
proof fn lemma_take_step<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c1: ICore<X::D>,
    c2: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    o0: Seq<<X::D as Decode>::Item>,
    p0: int,
    k: int,
    c: int,
    x: <X::D as Decode>::Item,
)
    requires
        c1.wf::<X>(s),
        0 <= k <= o0.len(),
        c1.out::<X>(s) == o0.skip(k),
        0 <= c <= c1.out::<X>(s).len(),
        c1.out::<X>(s).take(c) =~= Seq::new(c as nat, |_i: int| x),
        psum(dbefore::<X, B>(s, c1.pos as int)) == p0 + psum(deltas_of::<_, B>(o0.take(k))),
        c2.pos == c1.pos + c,
        c2.out::<X>(s) == c1.out::<X>(s).skip(c),
    ensures
        k + c <= o0.len(),
        c2.out::<X>(s) == o0.skip(k + c),
        psum(dbefore::<X, B>(s, c2.pos as int)) == p0 + psum(deltas_of::<_, B>(o0.take(k + c))),
        psum(dbefore::<X, B>(s, c2.pos as int)) == psum(dbefore::<X, B>(s, c1.pos as int)) + match B::dv(x) {
            Some(d) => c * d,
            None => 0,
        },
{
    hide(psum);
    hide(ICore::wf);
    hide(ICore::out);
    lemma_dtake::<X, B>(c1, s, c as nat, x);
    lemma_run_step::<_, B>(o0, k, c, x);
    let d = dval(B::dv(x));
    match B::dv(x) {
        Some(v) => {
            assert(c * d == c * v);
        },
        None => {
            assert(c * d == 0) by (nonlinear_arith)
                requires
                    d == 0,
            ;
        },
    }
}

/// The target, item `n` of the window `o0`, is the last of a run of `c`
/// copies of `x` from offset `k`, where the running sum is `q`: it
/// realizes the sum after the run, or null.
proof fn lemma_target_realized<V: Copy, B: DeltaOps<V>>(o0: Seq<V>, p0: int, q: int, k: int, c: int, n: int, x: V)
    requires
        0 <= k,
        1 <= c,
        k + c <= o0.len(),
        n == k + c - 1,
        o0.skip(k).take(c) =~= Seq::new(c as nat, |_i: int| x),
        q == p0 + psum(deltas_of::<V, B>(o0.take(k))),
    ensures
        realized_at::<V, B>(o0, p0, n) == match B::dv(x) {
            Some(d) => Some(crop(q + c * d)),
            None => None,
        },
{
    hide(psum);
    hide(crop);
    lemma_target_in_run::<V, B>(o0, k, c - 1, x);
    match B::dv(x) {
        Some(d) => {
            assert(c * d == (c - 1) * d + d) by (nonlinear_arith);
        },
        None => {},
    }
}


/// The value the `n`th left in the window realizes (0-based), consuming
/// it and those before it run by run: for `n` within the slab the
/// iterator stands in, or the one it jumped to.
#[allow(clippy::manual_map)]
#[verifier::spinoff_prover]
fn d_take_nth<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    n: usize,
    running: &mut i64,
) -> (r: Option<i64>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        n < old(core).out::<X>(slabs@).len(),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        r == realized_at::<_, B>(
            old(core).out::<X>(slabs@),
            psum(dbefore::<X, B>(slabs@, old(core).pos as int)),
            n as int,
        ),
        final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(n + 1),
        final(core).pos == old(core).pos + n + 1,
        final(core).items_left == old(core).items_left - (n + 1),
{
    hide(ICore::wf);
    hide(ICore::out);
    hide(psum);
    hide(crop);
    hide(dbefore);
    hide(realized_at);
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost o0 = c0.out::<X>(s);
    let ghost p0 = psum(dbefore::<X, B>(s, c0.pos as int));
    // `m` left to go before the target
    let mut m = n;
    proof {
        assert(o0.skip(0) =~= o0);
        assert(psum(deltas_of::<_, B>(o0.take(0))) == 0) by {
            reveal(psum);
        }
    }
    loop
        invariant
            s == slabs@,
            c0 == *old(core),
            o0 == c0.out::<X>(s),
            p0 == psum(dbefore::<X, B>(s, c0.pos as int)),
            at_run::<X, B>(*core, *running, s),
            n < o0.len(),
            m <= n,
            core.out::<X>(s) == o0.skip(n - m),
            core.pos == c0.pos + (n - m),
            core.items_left == c0.items_left - (n - m),
            psum(dbefore::<X, B>(s, core.pos as int)) == p0 + psum(deltas_of::<_, B>(o0.take(n - m))),
        decreases m,
    {
        let ghost o = core.out::<X>(s);
        let ghost c1 = *core;
        proof {
            lemma_out_len::<X>(c1, s);
        }
        match core.next_run_max::<X>(slabs, m + 1) {
            Some(run) => {
                let ghost k = (n - m) as int;
                let ghost c = run.count as int;
                let ghost q = psum(dbefore::<X, B>(s, c1.pos as int));
                proof {
                    assert(o.take(c) =~= Seq::new(c as nat, |_i: int| run.value));
                    lemma_take_step::<X, B>(c1, *core, s, o0, p0, k, c, run.value);
                }
                let delta = B::to_opt(run.value);
                if let Some(d) = delta {
                    *running = add_run(*running, d, run.count, Ghost(psum(dbefore::<X, B>(s, c1.pos as int))));
                } else {
                    proof {
                        assert(psum(dbefore::<X, B>(s, core.pos as int)) == q);
                    }
                }
                if run.count > m {
                    proof {
                        // the target is the run's last value
                        lemma_target_realized::<_, B>(o0, p0, q, k, c, n as int, run.value);
                    }
                    return match delta {
                        None => None,
                        Some(_) => Some(*running),
                    };
                }
                m -= run.count;
            },
            None => {
                past_window();
            },
        }
    }
}

/// The index's prefix before slab `k` is the running sum there, an `i64`.
pub(crate) proof fn lemma_found<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        dix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k <= s.len(),
    ensures
        forall|i: int| 0 <= i < ws.len() ==> #[trigger] ws[i].prefix_ok(),
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        fold(ws.subrange(0, k)).spec_prefix() == crop(
            psum(dbefore::<X, B>(s, sum(slab_lens(s), 0, k) as int)),
        ),
        fold(ws.subrange(0, k)).spec_prefix() == psum(dbefore::<X, B>(s, sum(slab_lens(s), 0, k) as int)),
{
    assert forall|i: int| 0 <= i < ws.len() implies #[trigger] ws[i].prefix_ok() by {
        assert(agg_of(ws[i], deltas_of::<_, B>(X::vals(s[i]))));
    }
    lemma_dfold::<X, B>(ws, s, k);
    lemma_upto::<X>(s, k);
    let l = sum(slab_lens(s), 0, k);
    lemma_dfold::<X, B>(ws, s, s.len() as int);
    assert(s.take(s.len() as int) =~= s);
    lemma_sum_le(slab_lens(s), 0, k, 0, s.len() as int);
    lemma_crop(fold(ws.subrange(0, k)).spec_prefix() as int);
}

/// The slab the index finds for the item `n` on from the iterator, when
/// that is past the slab the iterator stands in: the iterator can jump to
/// its start `start`, the index's prefix is the running sum there, and the
/// item is in it. Past the last slab, the item is past the window, and
/// `start` is the column's end: where the window of an iterator in the
/// column ends.
proof fn lemma_jump<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    ws: Seq<SlabAgg>,
    s: Seq<Slab<X::Tail>>,
    k: int,
    start: int,
    n: int,
)
    requires
        c0.wf::<X>(s),
        dix_ok::<X, B>(ws, s),
        c0.slab_remaining <= n < c0.items_left,
        0 <= k <= s.len(),
        start == sum_len(ws.subrange(0, k)),
        k < s.len() ==> start <= c0.pos + n < start + ws[k].spec_len(),
        k == s.len() ==> c0.pos + n >= sum_len(ws),
    ensures
        fold(ws.subrange(0, k)).spec_prefix() == crop(psum(dbefore::<X, B>(s, start))),
        k < s.len() ==> {
            &&& start == sum(slab_lens(s), 0, k)
            &&& c0.pos <= start
            &&& start - c0.pos <= c0.items_left
            &&& c0.pos + n - start < s[k].len
        },
        k == s.len() ==> {
            &&& start >= total(slab_lens(s))
            &&& n >= c0.out::<X>(s).len()
            &&& c0.pos <= total(slab_lens(s)) ==> start == c0.pos + c0.out::<X>(s).len()
        },
{
    let lens = slab_lens(s);
    c0.lemma_here::<X>(s);
    c0.lemma_out_len::<X>(s);
    lemma_found::<X, B>(ws, s, k);
    lemma_dfold::<X, B>(ws, s, s.len() as int);
    assert(ws.subrange(0, s.len() as int) =~= ws);
    if k < s.len() {
        assert(agg_of(ws[k], deltas_of::<_, B>(X::vals(s[k]))));
        lemma_from::<X>(s, k, 0);
        // the slab holding the item starts at or after the iterator
        if c0.slab_idx < s.len() {
            let cur = c0.slab_idx as int;
            lemma_sum_split(lens, 0, cur, cur + 1);
            lemma_sum_one(lens, cur);
            if k <= cur {
                lemma_sum_le(lens, 0, k + 1, 0, cur + 1);
                lemma_sum_split(lens, 0, k, k + 1);
                lemma_sum_one(lens, k);
            } else {
                lemma_sum_le(lens, 0, cur + 1, 0, k);
            }
        }
    }
}

/// After a jump to the start of the slab that holds the target, `n` on
/// from where the iterator stood and `j` on from where it stands: the
/// target is in both windows, realizes the same value in both, and leaves
/// the same window after it.
proof fn lemma_landed<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    c1: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    n: int,
    j: int,
)
    requires
        c0.wf::<X>(s),
        c1.wf::<X>(s),
        c1.slab_idx < s.len(),
        c1.slab_remaining == s[c1.slab_idx as int].len,
        c0.pos <= c1.pos,
        c1.items_left == c0.items_left - (c1.pos - c0.pos),
        c1.pos + j == c0.pos + n,
        0 <= j < c1.slab_remaining,
        n < c0.items_left,
    ensures
        j < c1.out::<X>(s).len(),
        n < c0.out::<X>(s).len(),
        realized_at::<_, B>(c1.out::<X>(s), psum(dbefore::<X, B>(s, c1.pos as int)), j) == realized_at::<_, B>(
            c0.out::<X>(s),
            psum(dbefore::<X, B>(s, c0.pos as int)),
            n,
        ),
        c1.out::<X>(s).skip(j + 1) == c0.out::<X>(s).skip(n + 1),
{
    c1.lemma_here::<X>(s);
    c0.lemma_out_window::<X>(s);
    c1.lemma_out_window::<X>(s);
    lemma_realized_window::<X, B>(s, c0.pos as int, c0.pos + c0.items_left, n);
    lemma_realized_window::<X, B>(s, c1.pos as int, c1.pos + c1.items_left, j);
    c0.lemma_moved::<X>(c1, s, c1.pos - c0.pos);
    lemma_skip_add(c0.out::<X>(s), c1.pos - c0.pos, j + 1);
}

/// `nth`, for `n` inside the window, keeping the running sum: a jump to a
/// far slab takes the sum of the slabs before it from the index.
#[verifier::spinoff_prover]
fn d_nth_in<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    n: usize,
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
        n < old(core).items_left,
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        n < old(core).out::<X>(slabs@).len() ==> {
            &&& r == Some(realized_at::<_, B>(
                old(core).out::<X>(slabs@),
                psum(dbefore::<X, B>(slabs@, old(core).pos as int)),
                n as int,
            ))
            &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(n + 1)
            &&& final(core).pos == old(core).pos + n + 1
            &&& final(core).items_left == old(core).items_left - (n + 1)
        },
        n >= old(core).out::<X>(slabs@).len() ==> {
            &&& r is None
            &&& final(core).out::<X>(slabs@).len() == 0
            &&& final(core).items_left == 0
            &&& old(core).pos <= total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos + old(core).out::<X>(
                slabs@,
            ).len()
        },
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    proof {
        core.lemma_here::<X>(s);
    }
    // Fast path: the target is within the current slab.
    if n < core.slab_remaining {
        return Some(d_take_nth::<X, B>(core, slabs, n, running));
    }
    let target = core.pos + n;
    let ix = match ix {
        Some(ix) => ix,
        None => {
            // a default iterator: no column, no slabs
            core.advance_to_slab::<X>(slabs, slabs.len(), 0);
            proof {
                assert(slab_lens(s) =~= Seq::<nat>::empty());
                assert(total(slab_lens(s)) == 0);
            }
            return None;
        },
    };
    proof {
        lemma_found::<X, B>(ix@, s, 0);
    }
    let found = ix.find_slab_at_item(target);
    let ghost k = found.index as int;
    proof {
        lemma_jump::<X, B>(c0, ix@, s, k, found.pos as int, n as int);
    }
    let in_slab = core.advance_to_slab::<X>(slabs, found.index, found.pos);
    // the index hands back the sum before the slab (past the last slab,
    // the column's)
    *running = found.prefix;
    if !in_slab {
        return None;
    }
    let ghost c1 = *core;
    proof {
        lemma_landed::<X, B>(c0, c1, s, n as int, target - found.pos);
    }
    let r = d_take_nth::<X, B>(core, slabs, target - found.pos, running);
    Some(r)
}

/// `nth`, keeping the running sum: the value the `n`th left in the window
/// realizes (0-based), consuming it and those before it; past the window,
/// consume the window.
pub(crate) fn d_nth<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    n: usize,
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        n < old(core).out::<X>(slabs@).len() ==> {
            &&& r == Some(realized_at::<_, B>(
                old(core).out::<X>(slabs@),
                psum(dbefore::<X, B>(slabs@, old(core).pos as int)),
                n as int,
            ))
            &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(n + 1)
            &&& final(core).pos == old(core).pos + n + 1
            &&& final(core).items_left == old(core).items_left - (n + 1)
        },
        n >= old(core).out::<X>(slabs@).len() ==> {
            &&& r is None
            &&& final(core).out::<X>(slabs@).len() == 0
            &&& final(core).items_left == 0
            &&& old(core).pos <= total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos + old(core).out::<X>(
                slabs@,
            ).len()
        },
{
    if n >= core.items_left {
        proof {
            assert(core.out::<X>(slabs@).len() <= core.items_left);
        }
        if core.items_left > 0 {
            let k = core.items_left - 1;
            let ghost c0 = *core;
            d_nth_in::<X, B>(core, slabs, ix, k, running);
            proof {
                if k < c0.out::<X>(slabs@).len() {
                    assert(c0.out::<X>(slabs@).skip(k + 1).len() == 0);
                    assert(c0.out::<X>(slabs@).len() == c0.items_left);
                }
            }
        }
        return None;
    }
    d_nth_in::<X, B>(core, slabs, ix, n, running)
}

/// The value at item `i` of slabs `s`, as a delta column's readers give
/// it: the running sum of the deltas through it, or null.
pub(crate) open spec fn dval_at<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    i: int,
) -> Option<i64> {
    realize(psum(dbefore::<X, B>(s, i)), B::dv(vals_of::<X>(s)[i]))
}

/// Read from the window's start, the value at offset `k` is the one at
/// item `a + k`.
pub(crate) proof fn lemma_realized_window<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    a: int,
    b: int,
    k: int,
)
    requires
        0 <= a,
        0 <= k < window(vals_of::<X>(s), a, b).len(),
    ensures
        realized_at::<_, B>(window(vals_of::<X>(s), a, b), psum(dbefore::<X, B>(s, a)), k)
            == dval_at::<X, B>(s, a + k),
{
    let v = vals_of::<X>(s);
    let w = window(v, a, b);
    assert(w.take(k) =~= v.subrange(a, a + k));
    assert(v.take(a + k) =~= v.take(a) + v.subrange(a, a + k));
    assert(deltas_of::<_, B>(v.take(a + k)) =~= deltas_of::<_, B>(v.take(a)) + deltas_of::<_, B>(
        v.subrange(a, a + k),
    ));
    lemma_psum_cat(deltas_of::<_, B>(v.take(a)), deltas_of::<_, B>(v.subrange(a, a + k)));
    assert(w[k] == v[a + k]);
}

/// `d_nth`, by `d_next` for the next value: what `DeltaIter::nth` does.
pub(crate) fn d_step<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    n: usize,
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        n < old(core).out::<X>(slabs@).len() ==> {
            &&& r == Some(realized_at::<_, B>(
                old(core).out::<X>(slabs@),
                psum(dbefore::<X, B>(slabs@, old(core).pos as int)),
                n as int,
            ))
            &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(n + 1)
            &&& final(core).pos == old(core).pos + n + 1
            &&& final(core).items_left == old(core).items_left - (n + 1)
        },
        n >= old(core).out::<X>(slabs@).len() ==> {
            &&& r is None
            &&& final(core).out::<X>(slabs@).len() == 0
            &&& final(core).items_left == 0
            &&& old(core).pos <= total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos + old(core).out::<X>(
                slabs@,
            ).len()
        },
{
    if n == 0 {
        proof {
            let o = core.out::<X>(slabs@);
            assert(deltas_of::<_, B>(o.take(0)) =~= Seq::<Option<i64>>::empty());
        }
        d_next::<X, B>(core, slabs, running)
    } else {
        d_nth::<X, B>(core, slabs, ix, n, running)
    }
}

/// `advance_to`, keeping the running sum: advance so that the item at
/// `target` comes next; at or past it already, stay.
#[inline(always)]
pub(crate) fn d_advance_to<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    target: usize,
    running: &mut i64,
)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        final(core).out::<X>(slabs@) == past(old(core).out::<X>(slabs@), target - old(core).pos),
        target <= old(core).pos ==> final(core).pos == old(core).pos,
        old(core).pos < target <= old(core).pos + old(core).out::<X>(slabs@).len() ==> final(core).pos
            == target,
{
    if target > core.pos {
        d_step::<X, B>(core, slabs, ix, target - core.pos - 1, running);
    }
}

/// `advance_by`, keeping the running sum: move `amount` items on.
#[inline(always)]
pub(crate) fn d_advance_by<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    amount: usize,
    running: &mut i64,
)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        final(core).out::<X>(slabs@) == past(old(core).out::<X>(slabs@), amount as int),
        amount <= old(core).out::<X>(slabs@).len() ==> final(core).pos == old(core).pos + amount,
{
    if amount > 0 {
        let ghost o = core.out::<X>(slabs@);
        d_step::<X, B>(core, slabs, ix, amount - 1, running);
        proof {
            if amount > o.len() {
                assert(final(core).out::<X>(slabs@) =~= Seq::<<X::D as Decode>::Item>::empty());
            }
        }
    }
}

/// `shift`, keeping the running sum: move the window to `start..end`,
/// only forwards (from a `start` before the iterator, the window starts
/// where it stands).
#[inline(always)]
pub(crate) fn d_shift<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    start: usize,
    end: usize,
    running: &mut i64,
)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        final(core).out::<X>(slabs@) == window(
            vals_of::<X>(slabs@),
            if start <= old(core).pos { old(core).pos as int } else { start as int },
            end as int,
        ),
        start <= old(core).pos ==> final(core).pos == old(core).pos,
        old(core).pos < start <= end && start <= vals_of::<X>(slabs@).len() ==> final(core).pos == start,
{
    let ghost p = core.pos as int;
    core.set_max::<X>(slabs, end);
    proof {
        core.lemma_out_window::<X>(slabs@);
        if end < p {
            assert(window(vals_of::<X>(slabs@), p, p) =~= window(vals_of::<X>(slabs@), p, end as int));
        }
        if start > p {
            lemma_window_past(vals_of::<X>(slabs@), p, end as int, start - p);
        }
    }
    d_advance_to::<X, B>(core, slabs, ix, start, running);
}

/// An iterator at or before item `start` whose window ends at `end`: the
/// window `start..end` has a first value where the iterator's reaches
/// `start`, and there the iterator's realizes the value at item `start`,
/// with the same window after it.
proof fn lemma_shifted<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    c: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    start: int,
    end: int,
)
    requires
        c.pos <= start,
        c.items_left == if end >= c.pos { end - c.pos } else { 0 },
    ensures
        ({
            let (o, w, n) = (c.out::<X>(s), window(vals_of::<X>(s), start, end), start - c.pos);
            &&& w.len() > 0 <==> n < o.len()
            &&& n < o.len() ==> {
                &&& w.skip(1) == o.skip(n + 1)
                &&& realized_at::<_, B>(o, psum(dbefore::<X, B>(s, c.pos as int)), n) == dval_at::<X, B>(s, start)
            }
        }),
{
    let p = c.pos as int;
    let o = c.out::<X>(s);
    let (w, n) = (window(vals_of::<X>(s), start, end), start - p);
    c.lemma_out_window::<X>(s);
    if end < p {
        assert(window(vals_of::<X>(s), p, p) =~= window(vals_of::<X>(s), p, end));
    }
    lemma_window_past(vals_of::<X>(s), p, end, n);
    if n < o.len() {
        assert(w =~= o.skip(n));
        assert(w.skip(1) =~= o.skip(n + 1));
        lemma_realized_window::<X, B>(s, p, end, n);
    }
}

/// `shift_next`, keeping the running sum: move the window to `start..end`
/// and give the value at its start; `start` is at or after the iterator.
#[inline(always)]
pub(crate) fn d_shift_next<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    start: usize,
    end: usize,
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
        start >= old(core).pos,
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let w = window(vals_of::<X>(slabs@), start as int, end as int);
            &&& w.len() > 0 ==> {
                &&& r == Some(dval_at::<X, B>(slabs@, start as int))
                &&& final(core).out::<X>(slabs@) == w.skip(1)
                &&& final(core).pos == start + 1
            }
            &&& w.len() == 0 ==> r is None && final(core).out::<X>(slabs@).len() == 0
        }),
{
    hide(ICore::wf);
    hide(ICore::out);
    hide(dindex_ok);
    hide(psum);
    hide(crop);
    hide(dbefore);
    hide(realized_at);
    hide(dval_at);
    hide(window);
    core.set_max::<X>(slabs, end);
    proof {
        lemma_shifted::<X, B>(*core, slabs@, start as int, end as int);
    }
    d_step::<X, B>(core, slabs, ix, start - core.pos, running)
}

/// `seek_to`, keeping the running sum: the value at item `at`, moving past
/// it; `None`, moving nowhere, if `at` is behind the iterator.
#[inline(always)]
pub(crate) fn d_seek_to<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    at: usize,
    running: &mut i64,
) -> (r: Option<Option<i64>>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        at < old(core).pos ==> r is None && *final(core) == *old(core) && *final(running) == *old(running),
        at >= old(core).pos ==> {
            let (o, k) = (old(core).out::<X>(slabs@), at - old(core).pos);
            &&& k < o.len() ==> r == Some(dval_at::<X, B>(slabs@, at as int)) && final(core).out::<X>(
                slabs@,
            ) == o.skip(k + 1) && final(core).pos == at + 1
            &&& k >= o.len() ==> r is None && final(core).out::<X>(slabs@).len() == 0
        },
{
    if at >= core.pos {
        let ghost c0 = *core;
        proof {
            c0.lemma_out_window::<X>(slabs@);
            if at - c0.pos < c0.out::<X>(slabs@).len() {
                lemma_realized_window::<X, B>(slabs@, c0.pos as int, c0.pos + c0.items_left, at - c0.pos);
            }
        }
        d_step::<X, B>(core, slabs, ix, at - core.pos, running)
    } else {
        None
    }
}

/// The run of a run with its running sum.
pub(crate) open spec fn run_of<V>(r: Option<(i64, Run<V>)>) -> Option<Run<V>> {
    match r {
        Some((_, run)) => Some(run),
        None => None,
    }
}

/// The next run of deltas in the window, as many equal ones as there
/// are up to the window's end, with the running sum before it: what
/// `DeltaIter::next_run` gives.
#[inline(always)]
pub(crate) fn d_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    running: &mut i64,
) -> (r: Option<(i64, Run<<X::D as Decode>::Item>)>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let o = old(core).out::<X>(slabs@);
            &&& first_run(o, run_of(r))
            &&& r matches Some((p, run)) ==> {
                &&& p == *old(running)
                &&& final(core).out::<X>(slabs@) == o.skip(run.count as int)
                &&& final(core).pos == old(core).pos + run.count
                &&& final(core).items_left == old(core).items_left - run.count
            }
            &&& r is None ==> final(core).pos == old(core).pos && final(core).items_left == 0
        }),
{
    let prefix = *running;
    let max = core.items_left;
    proof {
        assert(core.out::<X>(slabs@).len() <= max);
    }
    let run = d_next_run::<X, B>(core, slabs, max, running)?;
    Some((prefix, run))
}

/// `DeltaRuns::shift_next`: move the window to `start..end` (only
/// forwards, as `d_shift`) and give its first run, with the running sum
/// before it.
pub(crate) fn d_shift_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    start: usize,
    end: usize,
    running: &mut i64,
) -> (r: Option<(i64, Run<<X::D as Decode>::Item>)>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        ({
            let a = if start <= old(core).pos { old(core).pos as int } else { start as int };
            let w = window(vals_of::<X>(slabs@), a, end as int);
            &&& first_run(w, run_of(r))
            &&& r matches Some((p, run)) ==> {
                &&& p == crop(psum(dbefore::<X, B>(slabs@, a)))
                &&& final(core).out::<X>(slabs@) == w.skip(run.count as int)
            }
        }),
{
    d_shift::<X, B>(core, slabs, ix, start, end, running);
    d_run::<X, B>(core, slabs, running)
}

/// `DeltaRuns::seek_to`: advance to item `at` and give the run from
/// there, with the running sum before it; `None`, moving nowhere, if `at`
/// is behind the iterator.
pub(crate) fn d_seek_run<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<SlabAgg>>,
    at: usize,
    running: &mut i64,
) -> (r: Option<(i64, Run<<X::D as Decode>::Item>)>)
    requires
        at_run::<X, B>(*old(core), *old(running), slabs@),
        dindex_ok::<X, B>(ix, slabs@),
    ensures
        at_run::<X, B>(*final(core), *final(running), slabs@),
        at < old(core).pos ==> r is None && *final(core) == *old(core) && *final(running) == *old(running),
        at >= old(core).pos ==> {
            let w = past(old(core).out::<X>(slabs@), at - old(core).pos);
            &&& first_run(w, run_of(r))
            &&& r matches Some((p, run)) ==> {
                &&& p == crop(psum(dbefore::<X, B>(slabs@, at as int)))
                &&& final(core).out::<X>(slabs@) == w.skip(run.count as int)
            }
        },
{
    if at < core.pos {
        return None;
    }
    d_advance_to::<X, B>(core, slabs, ix, at, running);
    d_run::<X, B>(core, slabs, running)
}

} // verus!
