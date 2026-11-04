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
//! Trusted: the value type's `to_opt` (`DeltaOps`), and that the index
//! holds each slab's aggregate, with the sums at slab boundaries fitting
//! `i64` (`dix_ok`): the column's invariant, which loads check (edits of
//! delta columns are outside the cursor's proof).

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
use crate::iter_core::{each, lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::before;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

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
fn add_run(running: i64, d: i64, n: usize, Ghost(p): Ghost<int>) -> (r: i64)
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
        c0.out::<X>(s).take(k as int) == Seq::new(k, |_i: int| v),
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
            },
            None => old(core).out::<X>(slabs@).len() == 0 && final(core).out::<X>(slabs@).len() == 0,
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
                &&& run.count < max && run.count < o.len() ==> o[run.count as int] != run.value
            },
            None => max == 0 || old(core).out::<X>(slabs@).len() == 0,
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

/// Panics: `d_take_nth` ran out of values. Never: `n` is within the
/// window.
#[verifier::external_body]
fn past_window()
    requires
        false,
{
    panic!("d_take_nth called with n past the window");
}

/// The value the `n`th left in the window realizes (0-based), consuming
/// it and those before it run by run: for `n` within the slab the
/// iterator stands in, or the one it jumped to.
#[allow(clippy::manual_map)]
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
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost o0 = c0.out::<X>(s);
    let ghost p0 = psum(dbefore::<X, B>(s, c0.pos as int));
    // `m` left to go before the target
    let mut m = n;
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
            psum(dbefore::<X, B>(s, core.pos as int)) == p0 + psum(deltas_of::<_, B>(o0.take(n - m))),
        decreases m,
    {
        let ghost o = core.out::<X>(s);
        let ghost c1 = *core;
        proof {
            assert(o.len() <= core.items_left);
        }
        match core.next_run_max::<X>(slabs, m + 1) {
            Some(run) => {
                let ghost k = (n - m) as int;
                let ghost c = run.count as int;
                proof {
                    lemma_dtake::<X, B>(c1, s, run.count as nat, run.value);
                    assert(o0.skip(k).take(c) == Seq::new(run.count as nat, |_i: int| run.value)) by {
                        assert(o0.skip(k).take(c) =~= o.take(c));
                    }
                    lemma_take_run::<_, B>(o0, k, c, run.value);
                    lemma_run_sum(deltas_of::<_, B>(o0.take(k)), run.count as nat, B::dv(run.value));
                }
                let delta = B::to_opt(run.value);
                if let Some(d) = delta {
                    *running = add_run(*running, d, run.count, Ghost(psum(dbefore::<X, B>(s, c1.pos as int))));
                } else {
                    proof {
                        assert(run.count * dval(B::dv(run.value)) == 0);
                    }
                }
                if run.count > m {
                    proof {
                        // the target is the run's last value
                        assert(c == m + 1);
                        assert(o.take(c)[m as int] == run.value);
                        assert(o[m as int] == o0[n as int]);
                        assert(o.skip(c) =~= o0.skip(n + 1));
                        // the deltas before it: those before the run, and
                        // `m` of the run's
                        assert(o0.skip(k).take(m as int) =~= Seq::new(m as nat, |_i: int| run.value)) by {
                            assert forall|i: int| 0 <= i < m implies #[trigger] o0.skip(k).take(m as int)[i]
                                == run.value by {
                                assert(o0.skip(k).take(c)[i] == run.value);
                            }
                        }
                        lemma_take_run::<_, B>(o0, k, m as int, run.value);
                        lemma_run_sum(deltas_of::<_, B>(o0.take(k)), m as nat, B::dv(run.value));
                        if let Some(d) = B::dv(run.value) {
                            assert((m + 1) * d == m * d + d) by (nonlinear_arith);
                            assert(c * d == (m + 1) * d);
                        }
                    }
                    return match delta {
                        None => None,
                        Some(_) => Some(*running),
                    };
                }
                m -= run.count;
                proof {
                    assert(o.skip(c) =~= o0.skip(n - m));
                }
            },
            None => {
                past_window();
            },
        }
    }
}

/// After a jump, the deltas before the target are the same, counted from
/// the slab the jump lands in or from where the iterator stood.
proof fn lemma_rejoin<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    p0: int,
    o0: Seq<<X::D as Decode>::Item>,
    n: int,
    p1: int,
    o1: Seq<<X::D as Decode>::Item>,
    j: int,
)
    requires
        0 <= p0 <= p0 + n <= vals_of::<X>(s).len(),
        0 <= p1 <= p1 + j,
        p1 + j == p0 + n,
        o0.take(n) == vals_of::<X>(s).subrange(p0, p0 + n),
        o1.take(j) == vals_of::<X>(s).subrange(p1, p1 + j),
    ensures
        psum(dbefore::<X, B>(s, p1)) + psum(deltas_of::<_, B>(o1.take(j)))
            == psum(dbefore::<X, B>(s, p0)) + psum(deltas_of::<_, B>(o0.take(n))),
{
    let vv = vals_of::<X>(s);
    let t = p0 + n;
    assert(vv.take(p1) + o1.take(j) =~= vv.take(t));
    assert(vv.take(p0) + o0.take(n) =~= vv.take(t));
    assert(deltas_of::<_, B>(vv.take(p1) + o1.take(j)) =~= deltas_of::<_, B>(vv.take(p1))
        + deltas_of::<_, B>(o1.take(j)));
    assert(deltas_of::<_, B>(vv.take(p0) + o0.take(n)) =~= deltas_of::<_, B>(vv.take(p0))
        + deltas_of::<_, B>(o0.take(n)));
    lemma_psum_cat(deltas_of::<_, B>(vv.take(p1)), deltas_of::<_, B>(o1.take(j)));
    lemma_psum_cat(deltas_of::<_, B>(vv.take(p0)), deltas_of::<_, B>(o0.take(n)));
}

/// The index's prefix before slab `k` is the running sum there, an `i64`.
proof fn lemma_found<'a, X: Decoders<'a>, B: DeltaOps<<X::D as Decode>::Item>>(
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

/// `nth`, for `n` inside the window, keeping the running sum: a jump to a
/// far slab takes the sum of the slabs before it from the index.
#[verifier::spinoff_prover]
#[verifier::rlimit(80)]
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
        },
        n >= old(core).out::<X>(slabs@).len() ==> r is None && final(core).out::<X>(slabs@).len() == 0,
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost vv = vals_of::<X>(s);
    let ghost lens = slab_lens(s);
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
            return None;
        },
    };
    proof {
        lemma_found::<X, B>(ix@, s, 0);
    }
    let found = ix.find_slab_at_item(target);
    let ghost k = found.index as int;
    proof {
        lemma_found::<X, B>(ix@, s, k);
        lemma_dfold::<X, B>(ix@, s, s.len() as int);
        assert(ix@.subrange(0, s.len() as int) =~= ix@);
        if k < s.len() {
            // the slab holding `target` starts at or after the iterator
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
            } else {
                lemma_from::<X>(s, k, 0);
            }
        }
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
        lemma_from::<X>(s, k, 0);
        assert(c1.out::<X>(s).len() > target - found.pos);
    }
    let r = d_take_nth::<X, B>(core, slabs, target - found.pos, running);
    proof {
        let j = target - found.pos;
        let o0 = c0.out::<X>(s);
        let o1 = c1.out::<X>(s);
        assert(o1[j] == vv[target as int]);
        assert(o0[n as int] == vv[target as int]);
        assert(o1.skip(j + 1) =~= o0.skip(n + 1));
        assert(o1.take(j) =~= vv.subrange(found.pos as int, target as int));
        assert(o0.take(n as int) =~= vv.subrange(c0.pos as int, target as int));
        lemma_rejoin::<X, B>(s, c0.pos as int, o0, n as int, found.pos as int, o1, j);
    }
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
        },
        n >= old(core).out::<X>(slabs@).len() ==> r is None && final(core).out::<X>(slabs@).len() == 0,
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
                }
            }
        }
        return None;
    }
    d_nth_in::<X, B>(core, slabs, ix, n, running)
}

} // verus!
