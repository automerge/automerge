//! A prefix column's running sums, verified with Verus.
//!
//! [`PrefixIter`](crate::prefix::PrefixIter) walks a column with the
//! verified iterator core ([`ICore`]) and keeps a running total of the
//! values' contributions (`T::accumulate`). It can be rebased
//! (`reset_prefix`: totals then count from there, with the offset in
//! `base`), and it jumps to a far slab by the index, which hands back the
//! sum of the slabs before it. The functions here keep its invariant
//! (`at_sum`): `base` plus the running total is the sum of the
//! contributions of the values before the iterator.
//!
//! Trusted: prefix arithmetic's laws (`axiom_prefix_add`: `+` is
//! associative, with `default` its identity; `axiom_prefix_sub`:
//! `b + (a - b) == a`), as for the wrapping integers (in debug builds an
//! operation that would wrap panics rather than return), and that the
//! value type's `accumulate` and `accumulate_run` add its values'
//! contributions (`PrefixOps`: user code). That each slab's weight in the
//! index holds the slab's sum is the column's invariant (`ix_ok`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::btree::{
    axiom_prefix_add, fold, prefix_add, prefix_zero, sum_len, PrefixAggregate, SlabAggregate,
};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{each, lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders, ICore};
use crate::prefix::PrefixSlabWeight;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::Run;
use std::ops::{Add, AddAssign, SubAssign};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// `a - b` on a prefix type.
pub(crate) uninterp spec fn prefix_sub<P>(a: P, b: P) -> P;

/// Adding back what was subtracted restores the sum, as for the integer
/// types (wrapping). Trusted, like `axiom_prefix_add`.
#[verifier::external_body]
pub(crate) proof fn axiom_prefix_sub<P>(a: P, b: P)
    ensures
        prefix_add(b, prefix_sub(a, b)) == a,
{
}

/// `n` copies of `c`, added.
pub(crate) open spec fn reps<P>(n: nat, c: P) -> P
    decreases n,
{
    if n == 0 { prefix_zero() } else { prefix_add(reps((n - 1) as nat, c), c) }
}

/// A prefix column's values' contributions, and its prefix arithmetic:
/// the trusted bridge to `PrefixValue` (user code) and `T::Prefix`.
pub(crate) trait PrefixOps<V: Copy> {
    type P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign + Add<Output = Self::P>
        + for<'x> AddAssign<&'x Self::P>;

    /// What value `v` adds to a running sum.
    spec fn contrib(v: V) -> Self::P;

    /// `T::accumulate`.
    fn acc(t: &mut Self::P, v: V)
        ensures *final(t) == prefix_add(*old(t), Self::contrib(v));

    /// `T::accumulate_run`.
    fn acc_run(t: &mut Self::P, run: &Run<V>)
        ensures *final(t) == prefix_add(*old(t), reps(run.count as nat, Self::contrib(run.value)));

    /// `T::try_accumulate_run`: `acc_run`, unless the sum would not fit.
    fn try_acc_run(t: &mut Self::P, run: &Run<V>) -> (ok: bool)
        ensures ok ==> *final(t) == prefix_add(*old(t), reps(run.count as nat, Self::contrib(run.value)));

    /// `P::default()`.
    fn zero() -> (r: Self::P)
        ensures r == prefix_zero::<Self::P>();

    /// `a - b.clone()`.
    fn sub(a: Self::P, b: &Self::P) -> (r: Self::P)
        ensures r == prefix_sub(a, *b);

    /// `*a += b`.
    fn add_to(a: &mut Self::P, b: &Self::P)
        ensures *final(a) == prefix_add(*old(a), *b);
}

/// The sum of the contributions of values `vs`.
pub(crate) open spec fn vsum<V: Copy, B: PrefixOps<V>>(vs: Seq<V>) -> B::P
    decreases vs.len(),
{
    if vs.len() == 0 { prefix_zero() } else { prefix_add(vsum::<V, B>(vs.drop_last()), B::contrib(vs.last())) }
}

proof fn lemma_vsum_push<V: Copy, B: PrefixOps<V>>(vs: Seq<V>, v: V)
    ensures vsum::<V, B>(vs.push(v)) == prefix_add(vsum::<V, B>(vs), B::contrib(v)),
{
    assert(vs.push(v).drop_last() =~= vs);
}

/// Sums add over concatenation.
pub(crate) proof fn lemma_vsum_cat<V: Copy, B: PrefixOps<V>>(a: Seq<V>, b: Seq<V>)
    ensures vsum::<V, B>(a + b) == prefix_add(vsum::<V, B>(a), vsum::<V, B>(b)),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        axiom_prefix_add(vsum::<V, B>(a), vsum::<V, B>(a), vsum::<V, B>(a));
    } else {
        let b0 = b.drop_last();
        lemma_vsum_cat::<V, B>(a, b0);
        assert(a + b =~= (a + b0).push(b.last()));
        lemma_vsum_push::<V, B>(a + b0, b.last());
        axiom_prefix_add(vsum::<V, B>(a), vsum::<V, B>(b0), B::contrib(b.last()));
    }
}

/// A run's sum.
pub(crate) proof fn lemma_vsum_run<V: Copy, B: PrefixOps<V>>(n: nat, v: V)
    ensures vsum::<V, B>(Seq::new(n, |_i: int| v)) == reps(n, B::contrib(v)),
    decreases n,
{
    if n > 0 {
        lemma_vsum_run::<V, B>((n - 1) as nat, v);
        assert(Seq::new(n, |_i: int| v).drop_last() =~= Seq::new((n - 1) as nat, |_i: int| v));
    }
}

/// The values before position `pos` (all of them, past the end).
pub(crate) open spec fn before<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, pos: int) -> Seq<<X::D as Decode>::Item> {
    let v = vals_of::<X>(s);
    if pos <= v.len() { v.take(pos) } else { v }
}

/// The iterator's running total, with `base`, is the sum of the values
/// before it.
pub(crate) open spec fn at_sum<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: ICore<X::D>,
    total: B::P,
    base: B::P,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    &&& core.wf::<X>(s)
    &&& prefix_add(base, total) == vsum::<_, B>(before::<X>(s, core.pos as int))
}

/// Each slab's weight in the index holds its item count and its sum: the
/// column's invariant.
pub(crate) open spec fn ix_ok<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    ws: Seq<PrefixSlabWeight<B::P>>,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    &&& ws.len() == s.len()
    &&& forall|i: int| 0 <= i < s.len() ==> {
        &&& #[trigger] ws[i].spec_len() == s[i].len
        &&& ws[i].spec_prefix() == vsum::<_, B>(X::vals(s[i]))
    }
}

/// The index's sums before slab `k` are those of the values before it.
pub(crate) proof fn lemma_fold<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    ws: Seq<PrefixSlabWeight<B::P>>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        ix_ok::<X, B>(ws, s), lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k <= s.len(),
    ensures
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        fold(ws.subrange(0, k)).spec_len() == sum(slab_lens(s), 0, k),
        fold(ws.subrange(0, k)).spec_prefix() == vsum::<_, B>(vals_of::<X>(s.take(k))),
        vals_of::<X>(s.take(k)).len() == sum(slab_lens(s), 0, k),
    decreases k,
{
    let w0 = ws.subrange(0, k);
    if k == 0 {
        assert(w0 =~= Seq::<PrefixSlabWeight<B::P>>::empty());
        assert(s.take(0) =~= Seq::<Slab<X::Tail>>::empty());
        assert(each::<X>(s.take(0)) =~= Seq::<Seq<<X::D as Decode>::Item>>::empty());
        <PrefixSlabWeight<B::P> as PrefixAggregate>::lemma_prefix_empty(prefix_zero());
        <PrefixSlabWeight<B::P> as SlabAggregate>::lemma_empty(PrefixSlabWeight::<B::P>::spec_empty());
    } else {
        lemma_fold::<X, B>(ws, s, k - 1);
        let w1 = ws.subrange(0, k - 1);
        assert(w0.drop_last() =~= w1);
        assert(w0.last() == ws[k - 1]);
        lemma_sum_split(slab_lens(s), 0, k - 1, k);
        lemma_sum_one(slab_lens(s), k - 1);
        lemma_sum_le(slab_lens(s), 0, k, 0, s.len() as int);
        let a = fold(w1);
        let b = ws[k - 1];
        <PrefixSlabWeight<B::P> as SlabAggregate>::lemma_plain(a, b);
        <PrefixSlabWeight<B::P> as SlabAggregate>::lemma_merge_len(a, b);
        <PrefixSlabWeight<B::P> as PrefixAggregate>::lemma_prefix_merge(a, b);
        // the slabs' values, one more slab
        assert(each::<X>(s.take(k)) =~= each::<X>(s.take(k - 1)).push(X::vals(s[k - 1])));
        assert(each::<X>(s.take(k)).drop_last() =~= each::<X>(s.take(k - 1)));
        lemma_vsum_cat::<_, B>(vals_of::<X>(s.take(k - 1)), X::vals(s[k - 1]));
    }
}

/// Taking `k` copies of `v` from the window adds them to the sum of the
/// values before the iterator.
proof fn lemma_take<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
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
        vsum::<_, B>(before::<X>(s, c0.pos + k)) == prefix_add(
            vsum::<_, B>(before::<X>(s, c0.pos as int)),
            reps(k, B::contrib(v)),
        ),
{
    let b0 = vsum::<_, B>(before::<X>(s, c0.pos as int));
    if k == 0 {
        axiom_prefix_add(b0, b0, b0);
    } else {
        let vv = vals_of::<X>(s);
        let o = c0.out::<X>(s);
        let run = Seq::new(k, |_i: int| v);
        assert(before::<X>(s, c0.pos + k) =~= vv.take(c0.pos as int) + run) by {
            assert forall|i: int| 0 <= i < k implies #[trigger] vv[c0.pos + i] == v by {
                assert(o.take(k as int)[i] == v);
                assert(o[i] == vv[c0.pos + i]);
            }
        }
        lemma_vsum_cat::<_, B>(vv.take(c0.pos as int), run);
        lemma_vsum_run::<_, B>(k, v);
    }
}

/// The index: the column's, whose weights hold its slabs' sums, or for a
/// default iterator none, over no slabs.
pub(crate) open spec fn index_ok<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    ix: Option<&SlabBTree<PrefixSlabWeight<B::P>>>,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    match ix {
        Some(ix) => ix.wf() && ix_ok::<X, B>(ix@, s),
        None => s.len() == 0,
    }
}

/// `next`, adding the value to the running total.
#[inline(always)]
pub(crate) fn p_next<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    total: &mut B::P,
    Ghost(base): Ghost<B::P>,
) -> (r: Option<<X::D as Decode>::Item>)
    requires
        at_sum::<X, B>(*old(core), *old(total), base, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), base, slabs@),
        match r {
            Some(v) => {
                &&& old(core).out::<X>(slabs@).len() > 0
                &&& v == old(core).out::<X>(slabs@)[0]
                &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(1)
                &&& *final(total) == prefix_add(*old(total), B::contrib(v))
            },
            None => old(core).out::<X>(slabs@).len() == 0 && final(core).out::<X>(slabs@).len() == 0,
        },
{
    let ghost c0 = *core;
    let ghost t0 = *total;
    let r = core.next::<X>(slabs);
    if let Some(v) = r {
        B::acc(total, v);
        proof {
            assert(c0.out::<X>(slabs@).take(1) =~= Seq::new(1, |_i: int| v));
            lemma_take::<X, B>(c0, slabs@, 1, v);
            assert(reps(1, B::contrib(v)) == prefix_add(reps(0, B::contrib(v)), B::contrib(v)));
            assert(reps(0, B::contrib(v)) == prefix_zero::<B::P>());
            axiom_prefix_add(B::contrib(v), B::contrib(v), B::contrib(v));
            axiom_prefix_add(base, t0, B::contrib(v));
        }
    }
    r
}

/// `next_run_max`, adding the run to the running total.
#[inline(always)]
pub(crate) fn p_next_run<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    max: usize,
    total: &mut B::P,
    Ghost(base): Ghost<B::P>,
) -> (r: Option<Run<<X::D as Decode>::Item>>)
    requires
        at_sum::<X, B>(*old(core), *old(total), base, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), base, slabs@),
        match r {
            Some(run) => {
                let o = old(core).out::<X>(slabs@);
                &&& 1 <= run.count <= max
                &&& run.count <= o.len()
                &&& o.take(run.count as int) == Seq::new(run.count as nat, |_i: int| run.value)
                &&& final(core).out::<X>(slabs@) == o.skip(run.count as int)
                &&& final(core).pos == old(core).pos + run.count
                &&& run.count < max && run.count < o.len() ==> o[run.count as int] != run.value
                &&& *final(total) == prefix_add(*old(total), reps(run.count as nat, B::contrib(run.value)))
            },
            None => max == 0 || old(core).out::<X>(slabs@).len() == 0,
        },
{
    let ghost c0 = *core;
    let ghost t0 = *total;
    let r = core.next_run_max::<X>(slabs, max);
    if let Some(run) = &r {
        B::acc_run(total, run);
        proof {
            lemma_take::<X, B>(c0, slabs@, run.count as nat, run.value);
            axiom_prefix_add(base, t0, reps(run.count as nat, B::contrib(run.value)));
        }
    }
    r
}

/// Panics: `take_nth` ran out of values. Never: `n` is within the window.
#[verifier::external_body]
fn past_window()
    requires
        false,
{
    panic!("same_slab_nth called with n > slab len");
}

/// The `n`th value left in the window (0-based), consuming it and those
/// before it run by run, adding them to the running total: for `n`
/// within the slab the iterator stands in, or the one it jumped to.
fn take_nth<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    n: usize,
    total: &mut B::P,
    Ghost(base): Ghost<B::P>,
) -> (r: <X::D as Decode>::Item)
    requires
        at_sum::<X, B>(*old(core), *old(total), base, slabs@),
        n < old(core).out::<X>(slabs@).len(),
    ensures
        at_sum::<X, B>(*final(core), *final(total), base, slabs@),
        r == old(core).out::<X>(slabs@)[n as int],
        final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(n + 1),
        final(core).pos == old(core).pos + n + 1,
{
    let ghost s = slabs@;
    let ghost c0 = *core;
    let ghost o0 = c0.out::<X>(s);
    // `m` left to go before the target
    let mut m = n;
    loop
        invariant
            s == slabs@,
            c0 == *old(core),
            o0 == c0.out::<X>(s),
            at_sum::<X, B>(*core, *total, base, s),
            n < o0.len(),
            m <= n,
            core.out::<X>(s) == o0.skip(n - m),
            core.pos == c0.pos + (n - m),
        decreases m,
    {
        let ghost o = core.out::<X>(s);
        proof {
            assert(o.len() <= core.items_left);
        }
        match p_next_run::<X, B>(core, slabs, m + 1, total, Ghost(base)) {
            Some(run) => {
                if run.count > m {
                    proof {
                        assert(Seq::new(run.count as nat, |_i: int| run.value)[m as int] == run.value);
                        assert(o.take(run.count as int)[m as int] == o[m as int]);
                        assert(o[m as int] == o0[n as int]);
                        assert(o.skip(run.count as int) =~= o0.skip(n + 1));
                    }
                    return run.value;
                }
                m -= run.count;
                proof {
                    assert(o.skip(run.count as int) =~= o0.skip(n - m));
                }
            },
            None => {
                past_window();
            },
        }
    }
}

/// `nth`, for `n` inside the window, keeping the running total: a jump to
/// a far slab takes the sum of the slabs before it from the index.
#[verifier::spinoff_prover]
fn p_nth_in<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<PrefixSlabWeight<B::P>>>,
    n: usize,
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<<X::D as Decode>::Item>)
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
        index_ok::<X, B>(ix, slabs@),
        n < old(core).items_left,
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
        n < old(core).out::<X>(slabs@).len() ==> {
            &&& r == Some(old(core).out::<X>(slabs@)[n as int])
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
        return Some(take_nth::<X, B>(core, slabs, n, total, Ghost(*base)));
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
    // Use the column's index for an O(log S) slab lookup.
    let found = ix.find_slab_at_item(target);
    let ghost k = found.index as int;
    proof {
        lemma_fold::<X, B>(ix@, s, k);
        lemma_fold::<X, B>(ix@, s, s.len() as int);
        assert(ix@.subrange(0, s.len() as int) =~= ix@);
        lemma_upto::<X>(s, k);
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
    // the column's); carry any `reset_prefix` offset across the jump
    *total = B::sub(found.prefix, base);
    proof {
        axiom_prefix_sub(found.prefix, *base);
        assert(s.take(s.len() as int) =~= s);
    }
    if !in_slab {
        return None;
    }
    let ghost c1 = *core;
    proof {
        lemma_from::<X>(s, k, 0);
        assert(c1.out::<X>(s).len() > target - found.pos);
    }
    let r = take_nth::<X, B>(core, slabs, target - found.pos, total, Ghost(*base));
    proof {
        assert(c1.out::<X>(s)[target - found.pos] == vv[target as int]);
        assert(c0.out::<X>(s)[n as int] == vv[target as int]);
        assert(c1.out::<X>(s).skip(target - found.pos + 1) =~= c0.out::<X>(s).skip(n + 1));
    }
    Some(r)
}

/// `nth`, keeping the running total: the `n`th value left in the window
/// (0-based), consuming it and those before it; past the window, consume
/// the window.
pub(crate) fn p_nth<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<PrefixSlabWeight<B::P>>>,
    n: usize,
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<<X::D as Decode>::Item>)
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
        index_ok::<X, B>(ix, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
        n < old(core).out::<X>(slabs@).len() ==> {
            &&& r == Some(old(core).out::<X>(slabs@)[n as int])
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
            p_nth_in::<X, B>(core, slabs, ix, k, total, base);
            proof {
                if k < c0.out::<X>(slabs@).len() {
                    assert(c0.out::<X>(slabs@).skip(k + 1).len() == 0);
                }
            }
        }
        return None;
    }
    p_nth_in::<X, B>(core, slabs, ix, n, total, base)
}

/// `sum_range`: from an iterator over the whole column, at its start with
/// nothing summed, advance to `start`, move the running total into `base`
/// (as `reset_prefix` does), and advance to `end`. Then `base` is the sum
/// before `start` and the running total the sum of `start..end`: what
/// added to the one gives the sum before `end`.
pub(crate) fn p_sum_range<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<PrefixSlabWeight<B::P>>>,
    start: usize,
    end: usize,
    total: &mut B::P,
    base: &mut B::P,
)
    requires
        at_sum::<X, B>(*old(core), *old(total), *old(base), slabs@),
        index_ok::<X, B>(ix, slabs@),
        old(core).pos == 0,
        old(core).items_left == vals_of::<X>(slabs@).len(),
        *old(base) == prefix_zero::<B::P>(),
        start < end,
    ensures
        *final(base) == vsum::<_, B>(before::<X>(slabs@, start as int)),
        prefix_add(*final(base), *final(total)) == vsum::<_, B>(before::<X>(slabs@, end as int)),
{
    let ghost s = slabs@;
    let ghost vv = vals_of::<X>(s);
    let ghost c0 = *core;
    proof {
        core.lemma_here::<X>(s);
        assert(c0.out::<X>(s) =~= vv);
    }
    // the whole column is the window: clamp the targets to its end
    let len = core.items_left;
    let s0 = if start < len { start } else { len };
    let e0 = if end < len { end } else { len };
    if s0 > 0 {
        p_nth::<X, B>(core, slabs, ix, s0 - 1, total, base);
    }
    proof {
        assert(before::<X>(s, s0 as int) == before::<X>(s, start as int));
        axiom_prefix_add(*base, *base, *base);
        if s0 > 0 {
            assert(core.out::<X>(s) == vv.skip(s0 as int));
        } else {
            assert(vv.skip(0) =~= vv);
        }
    }
    let ghost c1 = *core;
    p_reset::<X, B>(Ghost(*core), Ghost(s), total, base);
    proof {
        axiom_prefix_add(*base, *base, *base);
        assert(*base == vsum::<_, B>(before::<X>(s, c1.pos as int)));
        assert(c1.pos == s0);
    }
    if s0 < e0 {
        p_nth::<X, B>(core, slabs, ix, e0 - 1 - s0, total, base);
    }
    proof {
        assert(before::<X>(s, e0 as int) == before::<X>(s, end as int));
    }
}

/// `reset_prefix`: count totals from here on, moving the running total
/// into `base`.
#[inline(always)]
pub(crate) fn p_reset<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    Ghost(core): Ghost<ICore<X::D>>,
    Ghost(s): Ghost<Seq<Slab<X::Tail>>>,
    total: &mut B::P,
    base: &mut B::P,
)
    requires
        at_sum::<X, B>(core, *old(total), *old(base), s),
    ensures
        at_sum::<X, B>(core, *final(total), *final(base), s),
        *final(total) == prefix_zero::<B::P>(),
{
    B::add_to(base, total);
    *total = B::zero();
    proof {
        axiom_prefix_add(*base, *base, *base);
    }
}

/// The sum of the values before item `index` (of all of them, at or past
/// the end): the index's sum for the slabs before the one holding it,
/// then that slab's values before it, run by run.
pub(crate) fn p_prefix<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<PrefixSlabWeight<B::P>>,
    index: usize,
) -> (r: B::P)
    requires
        ix.wf(),
        ix_ok::<X, B>(ix@, slabs@),
        lens_ok::<X>(slabs@),
        total(slab_lens(slabs@)) <= usize::MAX,
        slabs@.len() < usize::MAX,
    ensures
        r == vsum::<_, B>(before::<X>(slabs@, index as int)),
{
    let ghost s = slabs@;
    let found = ix.find_slab_at_item(index);
    let ghost k = found.index as int;
    proof {
        lemma_fold::<X, B>(ix@, s, k);
        lemma_upto::<X>(s, k);
        lemma_fold::<X, B>(ix@, s, s.len() as int);
        assert(ix@.subrange(0, s.len() as int) =~= ix@);
        assert(s.take(s.len() as int) =~= s);
        axiom_prefix_add(found.prefix, found.prefix, found.prefix);
    }
    if found.index >= slabs.len() {
        // `index` is at or past the column's end
        return found.prefix;
    }
    proof {
        lemma_from::<X>(s, k, 0);
    }
    let mut t = found.prefix;
    let n = index - found.pos;
    if n == 0 {
        return t;
    }
    let ghost zero = prefix_zero::<B::P>();
    let mut core = ICore::at::<X>(slabs, found.index, 0, found.pos, n);
    proof {
        assert(core.out::<X>(s).len() == n);
    }
    take_nth::<X, B>(&mut core, slabs, n - 1, &mut t, Ghost(zero));
    proof {
        axiom_prefix_add(t, t, t);
    }
    t
}

/// The sum of a slab's values, run by run: the prefix of its weight in
/// the index.
pub(crate) fn p_slab_sum<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
) -> (r: B::P)
    requires
        X::ok(*slab),
    ensures
        r == vsum::<_, B>(X::vals(*slab)),
{
    let ghost v = X::vals(*slab);
    let mut d = X::open(slab);
    let mut t = B::zero();
    let ghost mut k: int = 0;
    proof {
        assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(v.skip(0) =~= v);
    }
    loop
        invariant
            v == X::vals(*slab),
            d.inv(),
            0 <= k <= v.len(),
            d.rest() == v.skip(k),
            t == vsum::<_, B>(v.take(k)),
        ensures
            t == vsum::<_, B>(v),
        decreases d.rest().len(),
    {
        let ghost t0 = t;
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                B::acc_run(&mut t, &run);
                proof {
                    let c = run.count as int;
                    let r = Seq::new(run.count as nat, |_i: int| run.value);
                    assert(v.take(k + c) =~= v.take(k) + r) by {
                        assert forall|i: int| 0 <= i < c implies #[trigger] v[k + i] == run.value by {
                            assert(v.skip(k).take(c)[i] == run.value);
                        }
                    }
                    lemma_vsum_cat::<_, B>(v.take(k), r);
                    lemma_vsum_run::<_, B>(run.count as nat, run.value);
                    assert(v.skip(k).skip(c) =~= v.skip(k + c));
                    k = k + c;
                }
            },
            None => {
                proof {
                    assert(v.take(k) =~= v);
                }
                break;
            },
        }
    }
    t
}

/// [`p_slab_sum`], unless a sum overflows: then `None`.
#[inline(never)]
pub(crate) fn p_slab_sum_checked<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
) -> (r: Option<B::P>)
    requires
        X::ok(*slab),
    ensures
        r matches Some(t) ==> t == vsum::<_, B>(X::vals(*slab)),
{
    let ghost v = X::vals(*slab);
    let mut d = X::open(slab);
    let mut t = B::zero();
    let ghost mut k: int = 0;
    proof {
        assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(v.skip(0) =~= v);
    }
    loop
        invariant
            v == X::vals(*slab),
            d.inv(),
            0 <= k <= v.len(),
            d.rest() == v.skip(k),
            t == vsum::<_, B>(v.take(k)),
        ensures
            t == vsum::<_, B>(v),
        decreases d.rest().len(),
    {
        let ghost t0 = t;
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                if !B::try_acc_run(&mut t, &run) {
                    return None;
                }
                proof {
                    let c = run.count as int;
                    let r = Seq::new(run.count as nat, |_i: int| run.value);
                    assert(v.take(k + c) =~= v.take(k) + r) by {
                        assert forall|i: int| 0 <= i < c implies #[trigger] v[k + i] == run.value by {
                            assert(v.skip(k).take(c)[i] == run.value);
                        }
                    }
                    lemma_vsum_cat::<_, B>(v.take(k), r);
                    lemma_vsum_run::<_, B>(run.count as nat, run.value);
                    assert(v.skip(k).skip(c) =~= v.skip(k + c));
                    k = k + c;
                }
            },
            None => {
                proof {
                    assert(v.take(k) =~= v);
                }
                break;
            },
        }
    }
    Some(t)
}

} // verus!
