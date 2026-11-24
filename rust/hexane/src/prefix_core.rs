//! A prefix column's running sums, verified with Verus.
//!
//! A prefix iterator ([`PrefixIter`](crate::PrefixIter),
//! [`AccIter`](crate::AccIter)) walks a column with the verified iterator
//! core ([`ICore`]) and keeps a running total of the values'
//! contributions. It can be rebased (`reset_prefix`: totals then count
//! from there, with the offset in `base`), and it jumps to a far slab by
//! the index, which hands back the sum of the slabs before it. The
//! functions here keep its invariant (`at_sum`): `base` plus the running
//! total is the sum of the contributions of the values before the
//! iterator.
//!
//! The sums are a slab weight's prefixes (`PrefixOps::W`), and the
//! arithmetic is that weight's, with the laws the functions here rest on
//! (`PrefixWeight`): `+` is associative, with zero its identity, and
//! `b + (a - b) == a`. Each weight proves them (`SumWeight`,
//! `AccWeight`). That each slab's weight in the index holds the slab's
//! sum is the column's invariant (`ix_ok`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::btree::{fold, sum_len, SlabAggregate};
use crate::btree::{PrefixAggregate, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{
    each, lemma_from, lemma_skip_add, lemma_skip_none, lemma_upto, lemma_window_past, lens_ok,
    past, vals_of, window,
};
use crate::iter_core::{Decode, Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::rle::writer::concat;
use crate::Run;
#[allow(unused_imports)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
use crate::index::lemma_plain_valid;

use crate::trusted::prefix_core_past_window as past_window;
verus! {

/// A slab weight whose prefixes the functions here can sum: its `+` has
/// the laws they rest on, and a `-` that undoes it.
pub(crate) trait PrefixWeight: PrefixAggregate {
    /// `a - b` on prefixes.
    spec fn prefix_minus(a: Self::Prefix, b: Self::Prefix) -> Self::Prefix;

    /// `+` is associative, with `prefix_zero` its identity.
    proof fn lemma_prefix_add(a: Self::Prefix, b: Self::Prefix, c: Self::Prefix)
        ensures
            Self::prefix_plus(Self::prefix_plus(a, b), c) == Self::prefix_plus(a, Self::prefix_plus(b, c)),
            Self::prefix_plus(Self::prefix_zero(), a) == a,
            Self::prefix_plus(a, Self::prefix_zero()) == a;

    /// Adding back what was subtracted restores the sum.
    proof fn lemma_prefix_sub(a: Self::Prefix, b: Self::Prefix)
        ensures Self::prefix_plus(b, Self::prefix_minus(a, b)) == a;

    /// Weights whose lengths fit always merge, and the laws of sums cover
    /// every weight.
    proof fn lemma_prefix_total()
        ensures
            Self::plain(),
            forall|w: Self| #[trigger] w.prefix_ok();
}

/// `n` copies of `c`, added.
pub(crate) open spec fn reps<W: PrefixWeight>(n: nat, c: W::Prefix) -> W::Prefix
    decreases n,
{
    if n == 0 { W::prefix_zero() } else { W::prefix_plus(reps::<W>((n - 1) as nat, c), c) }
}

/// A prefix column's values' contributions, and its prefix arithmetic,
/// which each implementation proves (`SumOf`, `AccOf`).
pub(crate) trait PrefixOps<V: Copy> {
    /// What the sums are kept in. Nothing is asked of the type here:
    /// the readers reach its arithmetic through the functions below
    /// alone.
    type P;

    /// The slab weight whose prefixes the sums are: its arithmetic is
    /// theirs.
    type W: PrefixWeight<Prefix = Self::P>;

    /// What value `v` adds to a running sum.
    spec fn contrib(v: V) -> Self::P;

    /// `T::accumulate`.
    fn acc(t: &mut Self::P, v: V)
        ensures *final(t) == Self::W::prefix_plus(*old(t), Self::contrib(v));

    /// `T::accumulate_run`.
    fn acc_run(t: &mut Self::P, run: &Run<V>)
        ensures *final(t) == Self::W::prefix_plus(*old(t), reps::<Self::W>(run.count as nat, Self::contrib(run.value)));

    /// `P::default()`.
    fn zero() -> (r: Self::P)
        ensures r == Self::W::prefix_zero();

    /// `a - b.clone()`.
    fn sub(a: Self::P, b: &Self::P) -> (r: Self::P)
        ensures r == Self::W::prefix_minus(a, *b);

    /// `a.clone()`.
    fn cp(a: &Self::P) -> (r: Self::P)
        ensures r == *a;

    /// `*a += b`.
    fn add_to(a: &mut Self::P, b: &Self::P)
        ensures *final(a) == Self::W::prefix_plus(*old(a), *b);

}

/// Sums whose addition does not unwind: a prefix column's (`SumOf`), not
/// an accumulator column's, whose addition is its client's. Their
/// readers can move in place (`p_next_nu`).
pub(crate) trait PrefixOpsNu<V: Copy>: PrefixOps<V> {
    /// [`acc`](PrefixOps::acc), which does not unwind.
    fn acc_nu(t: &mut Self::P, v: V)
        ensures *final(t) == Self::W::prefix_plus(*old(t), Self::contrib(v)),
        no_unwind;
}

/// The sum of the contributions of values `vs`.
pub(crate) open spec fn vsum<V: Copy, B: PrefixOps<V>>(vs: Seq<V>) -> B::P
    decreases vs.len(),
{
    if vs.len() == 0 { B::W::prefix_zero() } else { B::W::prefix_plus(vsum::<V, B>(vs.drop_last()), B::contrib(vs.last())) }
}

proof fn lemma_vsum_push<V: Copy, B: PrefixOps<V>>(vs: Seq<V>, v: V)
    ensures vsum::<V, B>(vs.push(v)) == B::W::prefix_plus(vsum::<V, B>(vs), B::contrib(v)),
{
    assert(vs.push(v).drop_last() =~= vs);
}

/// Sums add over concatenation.
pub(crate) proof fn lemma_vsum_cat<V: Copy, B: PrefixOps<V>>(a: Seq<V>, b: Seq<V>)
    ensures vsum::<V, B>(a + b) == B::W::prefix_plus(vsum::<V, B>(a), vsum::<V, B>(b)),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        B::W::lemma_prefix_add(vsum::<V, B>(a), vsum::<V, B>(a), vsum::<V, B>(a));
    } else {
        let b0 = b.drop_last();
        lemma_vsum_cat::<V, B>(a, b0);
        assert(a + b =~= (a + b0).push(b.last()));
        lemma_vsum_push::<V, B>(a + b0, b.last());
        B::W::lemma_prefix_add(vsum::<V, B>(a), vsum::<V, B>(b0), B::contrib(b.last()));
    }
}

/// A run's sum.
pub(crate) proof fn lemma_vsum_run<V: Copy, B: PrefixOps<V>>(n: nat, v: V)
    ensures vsum::<V, B>(Seq::new(n, |_i: int| v)) == reps::<B::W>(n, B::contrib(v)),
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
    &&& B::W::prefix_plus(base, total) == vsum::<_, B>(before::<X>(s, core.pos as int))
}

/// Each slab's weight in the index holds its item count and its sum: the
/// column's invariant.
pub(crate) open spec fn ix_ok<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
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
    ws: Seq<B::W>,
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
    B::W::lemma_prefix_total();
    let w0 = ws.subrange(0, k);
    if k == 0 {
        assert(w0 =~= Seq::<B::W>::empty());
        assert(s.take(0) =~= Seq::<Slab<X::Tail>>::empty());
        assert(each::<X>(s.take(0)) =~= Seq::<Seq<<X::D as Decode>::Item>>::empty());
        <B::W as PrefixAggregate>::lemma_prefix_empty(B::W::prefix_zero());
        <B::W as SlabAggregate>::lemma_empty(B::W::spec_empty());
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
        <B::W as SlabAggregate>::lemma_plain(a, b);
        <B::W as SlabAggregate>::lemma_merge_len(a, b);
        <B::W as PrefixAggregate>::lemma_prefix_merge(a, b);
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
        vsum::<_, B>(before::<X>(s, c0.pos + k)) == B::W::prefix_plus(
            vsum::<_, B>(before::<X>(s, c0.pos as int)),
            reps::<B::W>(k, B::contrib(v)),
        ),
{
    let b0 = vsum::<_, B>(before::<X>(s, c0.pos as int));
    if k == 0 {
        B::W::lemma_prefix_add(b0, b0, b0);
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
    ix: Option<&SlabBTree<B::W>>,
    s: Seq<Slab<X::Tail>>,
) -> bool {
    match ix {
        Some(ix) => ix.wf() && ix_ok::<X, B>(ix@, s),
        None => s.len() == 0,
    }
}

/// [`p_next`] of sums whose addition does not unwind, and which does
/// not unwind either: an iterator whose type has an invariant can give
/// it its reader and its total in place (Verus lets a call take fields
/// of such a value mutably only if the call does not unwind), where
/// `p_next` needs them copied out and written back.
#[inline(always)]
#[allow(unused_variables)] // `base` is for the spec
pub(crate) fn p_next_nu<'a, X: Decoders<'a>, B: PrefixOpsNu<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    total: &mut B::P,
    base: Ghost<B::P>,
) -> (r: Option<<X::D as Decode>::Item>)
    requires
        at_sum::<X, B>(*old(core), *old(total), base@, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), base@, slabs@),
        match r {
            Some(v) => {
                &&& old(core).out::<X>(slabs@).len() > 0
                &&& v == old(core).out::<X>(slabs@)[0]
                &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(1)
                &&& final(core).pos == old(core).pos + 1
                &&& final(core).items_left == old(core).items_left - 1
                &&& *final(total) == B::W::prefix_plus(*old(total), B::contrib(v))
            },
            None => {
                &&& old(core).out::<X>(slabs@).len() == 0
                &&& final(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == 0
                &&& *final(total) == *old(total)
            },
        },
    no_unwind
{
    let ghost c0 = *core;
    let ghost t0 = *total;
    let ghost base = base@;
    let r = core.next::<X>(slabs);
    if let Some(v) = r {
        B::acc_nu(total, v);
        proof {
            assert(c0.out::<X>(slabs@).take(1) =~= Seq::new(1, |_i: int| v));
            lemma_take::<X, B>(c0, slabs@, 1, v);
            assert(reps::<B::W>(1, B::contrib(v)) == B::W::prefix_plus(reps::<B::W>(0, B::contrib(v)), B::contrib(v)));
            assert(reps::<B::W>(0, B::contrib(v)) == B::W::prefix_zero());
            B::W::lemma_prefix_add(B::contrib(v), B::contrib(v), B::contrib(v));
            B::W::lemma_prefix_add(base, t0, B::contrib(v));
        }
    }
    r
}

/// `next`, adding the value to the running total.
#[inline(always)]
#[allow(unused_variables)] // `base` is for the spec
pub(crate) fn p_next<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<<X::D as Decode>::Item>)
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
        match r {
            Some(v) => {
                &&& old(core).out::<X>(slabs@).len() > 0
                &&& v == old(core).out::<X>(slabs@)[0]
                &&& final(core).out::<X>(slabs@) == old(core).out::<X>(slabs@).skip(1)
                &&& final(core).pos == old(core).pos + 1
                &&& final(core).items_left == old(core).items_left - 1
                &&& *final(total) == B::W::prefix_plus(*old(total), B::contrib(v))
            },
            None => {
                &&& old(core).out::<X>(slabs@).len() == 0
                &&& final(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == 0
                &&& *final(total) == *old(total)
            },
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
            assert(reps::<B::W>(1, B::contrib(v)) == B::W::prefix_plus(reps::<B::W>(0, B::contrib(v)), B::contrib(v)));
            assert(reps::<B::W>(0, B::contrib(v)) == B::W::prefix_zero());
            B::W::lemma_prefix_add(B::contrib(v), B::contrib(v), B::contrib(v));
            B::W::lemma_prefix_add(*base, t0, B::contrib(v));
        }
    }
    r
}

/// [`p_next_run`], for the iterator, which holds `base`.
#[inline(always)]
#[allow(unused_variables)] // `base` is for the spec
pub(crate) fn p_next_run_from<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    max: usize,
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<Run<<X::D as Decode>::Item>>)
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
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
                &&& *final(total) == B::W::prefix_plus(*old(total), reps::<B::W>(run.count as nat, B::contrib(run.value)))
            },
            None => {
                &&& max == 0 || old(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == if max == 0 {
                    old(core).items_left
                } else {
                    0
                }
                &&& *final(total) == *old(total)
            },
        },
{
    p_next_run::<X, B>(core, slabs, max, total, Ghost(*base))
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
                &&& final(core).items_left == old(core).items_left - run.count
                &&& run.count < max && run.count < o.len() ==> o[run.count as int] != run.value
                &&& *final(total) == B::W::prefix_plus(*old(total), reps::<B::W>(run.count as nat, B::contrib(run.value)))
            },
            None => {
                &&& max == 0 || old(core).out::<X>(slabs@).len() == 0
                &&& final(core).pos == old(core).pos
                &&& final(core).items_left == if max == 0 {
                    old(core).items_left
                } else {
                    0
                }
                &&& *final(total) == *old(total)
            },
        },
{
    let ghost c0 = *core;
    let ghost t0 = *total;
    let r = core.next_run_max::<X>(slabs, max);
    if let Some(run) = &r {
        B::acc_run(total, run);
        proof {
            lemma_take::<X, B>(c0, slabs@, run.count as nat, run.value);
            B::W::lemma_prefix_add(base, t0, reps::<B::W>(run.count as nat, B::contrib(run.value)));
        }
    }
    r
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
        final(core).pos + final(core).items_left == old(core).pos + old(core).items_left,
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
            core.pos + core.items_left == c0.pos + c0.items_left,
        decreases m,
    {
        let ghost o = core.out::<X>(s);
        proof {
            assert(o.len() <= core.items_left);
        }
        match p_next_run::<X, B>(core, slabs, m + 1, total, Ghost(base)) {
            Some(run) => {
                proof {
                    lemma_skip_add(o0, n - m, run.count as int);
                }
                if run.count > m {
                    proof {
                        assert(Seq::new(run.count as nat, |_i: int| run.value)[m as int] == run.value);
                        assert(o.take(run.count as int)[m as int] == o[m as int]);
                        assert(o[m as int] == o0[n as int]);
                    }
                    return run.value;
                }
                m -= run.count;
            },
            None => {
                past_window();
            },
        }
    }
}

/// The slab the index finds for the item `n` on from the iterator, when
/// that is past the slab the iterator stands in: the iterator can jump to
/// its start `start`, the index's prefix is the sum of the values before
/// it, and the item is in it. Past the last slab, the item is past the
/// window.
proof fn lemma_jump<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    k: int,
    start: int,
    n: int,
)
    requires
        c0.wf::<X>(s),
        ix_ok::<X, B>(ws, s),
        c0.slab_remaining <= n < c0.items_left,
        0 <= k <= s.len(),
        start == sum_len(ws.subrange(0, k)),
        k < s.len() ==> start <= c0.pos + n < start + ws[k].spec_len(),
        k == s.len() ==> c0.pos + n >= sum_len(ws),
    ensures
        fold(ws.subrange(0, k)).spec_prefix() == vsum::<_, B>(before::<X>(s, start)),
        k < s.len() ==> {
            &&& start == sum(slab_lens(s), 0, k)
            &&& c0.pos <= start
            &&& start - c0.pos <= c0.items_left
            &&& c0.pos + n - start < s[k].len
        },
        k == s.len() ==> start == total(slab_lens(s)) && n >= c0.out::<X>(s).len(),
{
    let lens = slab_lens(s);
    c0.lemma_here::<X>(s);
    lemma_fold::<X, B>(ws, s, k);
    lemma_fold::<X, B>(ws, s, s.len() as int);
    assert(ws.subrange(0, s.len() as int) =~= ws);
    lemma_upto::<X>(s, k);
    if k < s.len() {
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
    } else {
        assert(s.take(s.len() as int) =~= s);
    }
}

/// `nth`, for `n` inside the window, keeping the running total: a jump to
/// a far slab takes the sum of the slabs before it from the index.
#[verifier::spinoff_prover]
fn p_nth_in<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<B::W>>,
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
            &&& final(core).pos + final(core).items_left == old(core).pos + old(core).items_left
        },
        n >= old(core).out::<X>(slabs@).len() ==> {
            &&& r is None
            &&& final(core).out::<X>(slabs@).len() == 0
            &&& final(core).items_left == 0
            // past the data (the window can run past it), it stops at the end
            &&& old(core).pos <= crate::fenwick::total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos
                + old(core).out::<X>(slabs@).len()
        },
{
    let ghost s = slabs@;
    let ghost c0 = *core;
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
            proof {
                c0.lemma_out_len::<X>(s);
                assert(crate::fenwick::total(slab_lens(s)) == 0);
            }
            return None;
        },
    };
    // Use the column's index for an O(log S) slab lookup.
    proof {
        ix.lemma_wf_len();
        B::W::lemma_prefix_total();
        lemma_plain_valid(ix@);
    }
    let found = ix.find_slab_at_item(target);
    let ghost k = found.index as int;
    proof {
        lemma_jump::<X, B>(c0, ix@, s, k, found.pos as int, n as int);
    }
    let in_slab = core.advance_to_slab::<X>(slabs, found.index, found.pos);
    // the index hands back the sum before the slab (past the last slab,
    // the column's); carry any `reset_prefix` offset across the jump
    *total = B::sub(found.prefix, base);
    proof {
        B::W::lemma_prefix_sub(found.prefix, *base);
    }
    if !in_slab {
        proof {
            // the window ran past the data, and this is the data's end
            c0.lemma_out_len::<X>(s);
        }
        return None;
    }
    let ghost c1 = *core;
    proof {
        c0.lemma_jumped::<X>(c1, s, n as int, target - found.pos);
    }
    let r = take_nth::<X, B>(core, slabs, target - found.pos, total, Ghost(*base));
    Some(r)
}

/// `nth`, keeping the running total: the `n`th value left in the window
/// (0-based), consuming it and those before it; past the window, consume
/// the window.
pub(crate) fn p_nth<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<B::W>>,
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
            &&& final(core).pos + final(core).items_left == old(core).pos + old(core).items_left
        },
        n >= old(core).out::<X>(slabs@).len() ==> {
            &&& r is None
            &&& final(core).out::<X>(slabs@).len() == 0
            &&& final(core).items_left == 0
            // past the data (the window can run past it), it stops at the end
            &&& old(core).pos <= crate::fenwick::total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos
                + old(core).out::<X>(slabs@).len()
        },
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





/// `delta_nth`: the `n`th value left in the window (0-based), as `p_nth`
/// gives it, and what its prefix added since the iterator's start: the
/// sum of the values before it from there, `d`, which added to the sum
/// before the start gives the sum before the value. Taken as the
/// difference of the running totals before and after the values passed,
/// which the prefix laws assumed (`b + (a - b) == a`) give.
pub(crate) fn p_delta_nth<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<B::W>>,
    n: usize,
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<(<X::D as Decode>::Item, B::P)>)
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
        index_ok::<X, B>(ix, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
        match r {
            Some((v, d)) => {
                let o = old(core).out::<X>(slabs@);
                &&& n < o.len()
                &&& v == o[n as int]
                &&& final(core).out::<X>(slabs@) == o.skip(n + 1)
                &&& final(core).pos == old(core).pos + n + 1
                &&& final(core).pos + final(core).items_left == old(core).pos + old(core).items_left
                &&& B::W::prefix_plus(vsum::<_, B>(before::<X>(slabs@, old(core).pos as int)), d) == vsum::<_, B>(
                    before::<X>(slabs@, old(core).pos + n),
                )
            },
            None => {
                &&& n >= old(core).out::<X>(slabs@).len()
                &&& final(core).out::<X>(slabs@).len() == 0
                &&& final(core).items_left == 0
                // past the data (the window can run past it), it stops at the end
                &&& old(core).pos <= crate::fenwick::total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos
                    + old(core).out::<X>(slabs@).len()
            },
        },
{
    hide(ICore::wf);
    hide(ICore::out);
    hide(index_ok);
    hide(vsum);
    hide(before);
    let ghost c0 = *core;
    let ghost o = core.out::<X>(slabs@);
    let t0 = B::cp(total);
    if n > 0 {
        p_nth::<X, B>(core, slabs, ix, n - 1, total, base)?;
    }
    let ghost t1 = *total;
    let ghost c1 = *core;
    proof {
        // `n` values on
        if n == 0 {
            assert(o.skip(0) =~= o);
        }
        assert(c1.out::<X>(slabs@) == o.skip(n as int));
        assert(c1.pos == c0.pos + n);
        // if no value is left there, the window ends there
        assert(n <= o.len());
        assert(c1.out::<X>(slabs@).len() == o.len() - n);
        assert(c1.pos + c1.items_left == c0.pos + c0.items_left);
    }
    let d = B::sub(B::cp(total), &t0);
    let v = p_next::<X, B>(core, slabs, total, base)?;
    proof {
        // the sum before the start, plus the difference, is the sum before `v`
        B::W::lemma_prefix_add(*base, t0, d);
        B::W::lemma_prefix_sub(t1, t0);
        assert(v == o[n as int]);
        assert(core.out::<X>(slabs@) =~= o.skip(n + 1));
    }
    Some((v, d))
}



/// At or past the end, the values before a position are all of them.
proof fn lemma_before_all<'a, X: Decoders<'a>>(s: Seq<Slab<X::Tail>>, pos: int)
    requires
        pos >= vals_of::<X>(s).len(),
    ensures
        before::<X>(s, pos) == vals_of::<X>(s),
{
    let v = vals_of::<X>(s);
    assert(v.take(v.len() as int) =~= v);
}

/// An iterator at the column's start with as many items left as the
/// column holds has all of its values left.
proof fn lemma_all_out<'a, X: Decoders<'a>>(c: ICore<X::D>, s: Seq<Slab<X::Tail>>)
    requires
        c.pos == 0,
        c.items_left == vals_of::<X>(s).len(),
    ensures
        c.out::<X>(s) == vals_of::<X>(s),
{
    assert(c.out::<X>(s) =~= vals_of::<X>(s));
}

/// `sum_range`: from an iterator over the whole column, at its start with
/// nothing summed, advance to `start`, move the running total into `base`
/// (as `reset_prefix` does), and advance to `end`. Then `base` is the sum
/// before `start` and the running total the sum of `start..end`: what
/// added to the one gives the sum before `end`.
#[verifier::spinoff_prover]
pub(crate) fn p_sum_range<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: Option<&SlabBTree<B::W>>,
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
        *old(base) == B::W::prefix_zero(),
        start < end,
    ensures
        *final(base) == vsum::<_, B>(before::<X>(slabs@, start as int)),
        B::W::prefix_plus(*final(base), *final(total)) == vsum::<_, B>(before::<X>(slabs@, end as int)),
{
    // the positions and the sums come from the steps' contracts alone
    hide(ICore::wf);
    hide(ICore::out);
    hide(before);
    hide(vals_of);
    let ghost s = slabs@;
    let ghost vv = vals_of::<X>(s);
    let ghost c0 = *core;
    proof {
        lemma_all_out::<X>(c0, s);
    }
    // the whole column is the window: clamp the targets to its end
    let len = core.items_left;
    let s0 = if start < len { start } else { len };
    let e0 = if end < len { end } else { len };
    if s0 > 0 {
        p_nth::<X, B>(core, slabs, ix, s0 - 1, total, base);
    }
    let ghost c1 = *core;
    proof {
        if s0 == 0 {
            lemma_skip_none(vv);
        }
        assert(c1.pos == s0);
        assert(c1.out::<X>(s) == vv.skip(s0 as int));
    }
    p_reset::<X, B>(core, slabs, total, base);
    proof {
        B::W::lemma_prefix_add(*base, *base, *base);
        if start >= len {
            lemma_before_all::<X>(s, start as int);
            lemma_before_all::<X>(s, s0 as int);
        }
    }
    if s0 < e0 {
        p_nth::<X, B>(core, slabs, ix, e0 - 1 - s0, total, base);
    }
    proof {
        if end >= len {
            lemma_before_all::<X>(s, end as int);
            lemma_before_all::<X>(s, e0 as int);
        }
    }
}

/// `reset_prefix`: count totals from here on, moving the running total
/// into `base`.
#[inline(always)]
#[allow(unused_variables)] // `core` and `slabs` are for the spec
pub(crate) fn p_reset<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    core: &ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    total: &mut B::P,
    base: &mut B::P,
)
    requires
        at_sum::<X, B>(*core, *old(total), *old(base), slabs@),
    ensures
        at_sum::<X, B>(*core, *final(total), *final(base), slabs@),
        *final(total) == B::W::prefix_zero(),
        *final(base) == B::W::prefix_plus(*old(base), *old(total)),
{
    B::add_to(base, total);
    *total = B::zero();
    proof {
        B::W::lemma_prefix_add(*base, *base, *base);
    }
}

/// The sum of the values before item `index` (of all of them, at or past
/// the end): the index's sum for the slabs before the one holding it,
/// then that slab's values before it, run by run.
pub(crate) fn p_prefix<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<B::W>,
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
    proof {
        ix.lemma_wf_len();
        B::W::lemma_prefix_total();
        lemma_plain_valid(ix@);
    }
    let found = ix.find_slab_at_item(index);
    let ghost k = found.index as int;
    proof {
        lemma_fold::<X, B>(ix@, s, k);
        lemma_upto::<X>(s, k);
        lemma_fold::<X, B>(ix@, s, s.len() as int);
        assert(ix@.subrange(0, s.len() as int) =~= ix@);
        assert(s.take(s.len() as int) =~= s);
        B::W::lemma_prefix_add(found.prefix, found.prefix, found.prefix);
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
    let ghost zero = B::W::prefix_zero();
    let mut core = ICore::at::<X>(slabs, found.index, 0, found.pos, n);
    proof {
        assert(core.out::<X>(s).len() == n);
    }
    take_nth::<X, B>(&mut core, slabs, n - 1, &mut t, Ghost(zero));
    proof {
        B::W::lemma_prefix_add(t, t, t);
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


} // verus!
