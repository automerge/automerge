//! A prefix column's searches by sum, verified with Verus.
//!
//! [`get_index_for_prefix`](crate::PrefixColumn::get_index_for_prefix)
//! finds the first item whose prefix sum (of the values before it)
//! reaches a target: the index finds the slab where the sums cross it, and
//! a walk of that slab's runs the item, dividing within a run. Its result
//! is proved to be exactly that item, or one past the end if no sum
//! reaches the target, whenever the column's sum fits its prefix type,
//! which loads and edits check at runtime (`PrefixValue::MAX_UNCHECKED_LEN`).
//!
//! The searches compare and divide sums, so they need a model of the
//! prefix type as numbers: each prefix value stands for a natural number
//! (`pnum`), sums that fit the type are the numbers' sums, `<=` is the
//! numbers' order, and so on (`PrefixOrd`). For the unverified API that
//! model is trusted: it is what unsigned integers do, and the searches are
//! only offered for prefix types that are (`UnsignedPrefix`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

#[cfg(verus_keep_ghost)]
use crate::btree::{fold, pre_at, reached, stays_reached, sum_len, PrefixAggregate};
use crate::btree::{OrdPrefix, SlabBTree};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders, ICore};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{
    at_sum, before, ix_ok, lemma_fold, lemma_vsum_cat, lemma_vsum_run, reps, vsum, PrefixWeight,
};
use crate::prefix_core::{p_nth, PrefixOps};
#[allow(unused_imports)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
use crate::index::lemma_plain_valid;

verus! {

/// An unsigned prefix type's arithmetic and order, as numbers: for the
/// unverified API, the trusted bridge to `T::Prefix`'s `<=`, `/` and
/// `try_into`.
pub(crate) trait PrefixOrd<V: Copy>: PrefixOps<V> {
    /// The number a prefix value stands for.
    spec fn pnum(p: Self::P) -> nat;

    /// The largest number the prefix type holds.
    spec fn pmax() -> nat;

    /// Sums that fit the type are the numbers' sums, and a difference
    /// that isn't negative is theirs, as for unsigned integers.
    proof fn axiom_pnum(a: Self::P, b: Self::P)
        ensures
            Self::pnum(a) <= Self::pmax(),
            Self::pnum(Self::W::prefix_zero()) == 0,
            Self::pnum(a) + Self::pnum(b) <= Self::pmax() ==> Self::pnum(Self::W::prefix_plus(a, b)) == Self::pnum(a) + Self::pnum(b),
            Self::pnum(b) <= Self::pnum(a) ==> Self::pnum(Self::W::prefix_minus(a, b)) == Self::pnum(a) - Self::pnum(b),
    ;

    /// The weight's `<=` is the numbers' order, as for unsigned integers.
    proof fn axiom_ple(a: Self::P, b: Self::P)
        where Self::W: OrdPrefix
        ensures
            <Self::W as OrdPrefix>::prefix_le(a, b) == (Self::pnum(a) <= Self::pnum(b)),
    ;

    /// `a <= b`.
    fn le(a: &Self::P, b: &Self::P) -> (r: bool)
        ensures
            r == (Self::pnum(*a) <= Self::pnum(*b)),
    ;

    /// `(a - 1) / b + 1`: `a` divided by `b`, rounding up (when both are
    /// positive; otherwise it may panic).
    fn div_ceil(a: &Self::P, b: &Self::P) -> (r: Self::P)
        ensures
            Self::pnum(*a) > 0 && Self::pnum(*b) > 0 ==> Self::pnum(r) == (Self::pnum(*a) - 1) as nat / Self::pnum(*b) + 1,
    ;

    /// `p.try_into()`.
    fn to_usize(p: &Self::P) -> (r: Option<usize>)
        ensures
            Self::pnum(*p) <= usize::MAX ==> r == Some(Self::pnum(*p) as usize),
    ;

    /// `*a + *b`.
    fn add(a: &Self::P, b: &Self::P) -> (r: Self::P)
        ensures
            r == Self::W::prefix_plus(*a, *b),
    ;

    /// `a.checked_add(b)` (`UnsignedPrefix`).
    fn checked_add(a: &Self::P, b: &Self::P) -> (r: Option<Self::P>)
        ensures
            r is Some <==> Self::pnum(*a) + Self::pnum(*b) <= Self::pmax(),
            r matches Some(c) ==> c == Self::W::prefix_plus(*a, *b),
    ;

    /// `1`.
    fn one() -> (r: Self::P)
        ensures
            Self::pnum(r) == 1,
    ;
}

/// The sum of values' contributions, as numbers.
pub(crate) open spec fn nsum<V: Copy, B: PrefixOrd<V>>(vs: Seq<V>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        nsum::<V, B>(vs.drop_last()) + B::pnum(B::contrib(vs.last()))
    }
}

pub(crate) proof fn lemma_nsum_cat<V: Copy, B: PrefixOrd<V>>(a: Seq<V>, b: Seq<V>)
    ensures
        nsum::<V, B>(a + b) == nsum::<V, B>(a) + nsum::<V, B>(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        lemma_nsum_cat::<V, B>(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
    }
}

/// Sums of longer prefixes are no smaller.
pub(crate) proof fn lemma_nsum_mono<V: Copy, B: PrefixOrd<V>>(vs: Seq<V>, i: int, j: int)
    requires
        0 <= i <= j <= vs.len(),
    ensures
        nsum::<V, B>(vs.take(i)) <= nsum::<V, B>(vs.take(j)),
{
    lemma_nsum_cat::<V, B>(vs.take(i), vs.take(j).skip(i));
    assert(vs.take(i) + vs.take(j).skip(i) =~= vs.take(j));
}

/// A sum that fits is its number.
pub(crate) proof fn lemma_nsum_vsum<V: Copy, B: PrefixOrd<V>>(vs: Seq<V>)
    requires
        nsum::<V, B>(vs) <= B::pmax(),
    ensures
        B::pnum(vsum::<V, B>(vs)) == nsum::<V, B>(vs),
    decreases vs.len(),
{
    if vs.len() == 0 {
        B::axiom_pnum(B::W::prefix_zero(), B::W::prefix_zero());
    } else {
        let d = vs.drop_last();
        lemma_nsum_vsum::<V, B>(d);
        B::axiom_pnum(vsum::<V, B>(d), B::contrib(vs.last()));
    }
}

/// A run of `n` copies of `v` adds `n` times `v`'s contribution.
pub(crate) proof fn lemma_nsum_run<V: Copy, B: PrefixOrd<V>>(n: nat, v: V)
    ensures
        nsum::<V, B>(Seq::new(n, |_i: int| v)) == n * B::pnum(B::contrib(v)),
    decreases n,
{
    let c = B::pnum(B::contrib(v));
    if n > 0 {
        lemma_nsum_run::<V, B>((n - 1) as nat, v);
        assert(Seq::new(n, |_i: int| v).drop_last() =~= Seq::new((n - 1) as nat, |_i: int| v));
        assert(Seq::new(n, |_i: int| v).last() == v);
        assert(n * c == (n - 1) * c + c) by (nonlinear_arith);
    } else {
        assert(n * c == 0) by (nonlinear_arith) requires n == 0;
    }
}

/// `r` is the first index whose prefix sum (of the values before it)
/// reaches `target`, or one past the end if none does: whenever the sum of
/// all the values `v` fits the prefix type.
pub(crate) open spec fn found_at<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, target: B::P, r: int) -> bool {
    nsum::<V, B>(v) <= B::pmax() ==> {
        &&& 0 <= r <= v.len() + 1
        &&& forall|i: int| 0 <= i < r && i <= v.len() ==> nsum::<V, B>(#[trigger] v.take(i)) < B::pnum(target)
        &&& r <= v.len() ==> B::pnum(target) <= nsum::<V, B>(v.take(r))
    }
}

/// With a run of `c` copies of `x` at `k` in `v`, the sum through its
/// first `i`.
proof fn lemma_run_sum<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, k: int, c: int, x: V, i: int)
    requires
        0 <= k,
        0 <= i <= c,
        k + c <= v.len(),
        v.skip(k).take(c) =~= Seq::new(c as nat, |_i: int| x),
    ensures
        nsum::<V, B>(v.take(k + i)) == nsum::<V, B>(v.take(k)) + i * B::pnum(B::contrib(x)),
{
    hide(nsum);
    let ri = Seq::new(i as nat, |_i: int| x);
    assert forall|j: int| 0 <= j < k + i implies v.take(k + i)[j] == (v.take(k) + ri)[j] by {
        if j >= k {
            assert(v.skip(k).take(c)[j - k] == x);
        }
    }
    assert(v.take(k + i) =~= v.take(k) + ri);
    lemma_nsum_cat::<V, B>(v.take(k), ri);
    lemma_nsum_run::<V, B>(i as nat, x);
    assert((i as nat) * B::pnum(B::contrib(x)) == i * B::pnum(B::contrib(x)));
}

/// A run of `c` copies of `x` at `k` in `v`: what is left after it, the
/// sum through it, and, where the sum of `v` fits the prefix type, the
/// run's sum as a number.
proof fn lemma_run_step<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, k: int, c: int, x: V)
    requires
        0 <= k,
        0 <= c,
        k + c <= v.len(),
        v.skip(k).take(c) =~= Seq::new(c as nat, |_i: int| x),
    ensures
        v.skip(k).skip(c) == v.skip(k + c),
        nsum::<V, B>(v.take(k + c)) == nsum::<V, B>(v.take(k)) + c * B::pnum(B::contrib(x)),
        nsum::<V, B>(v) <= B::pmax() ==> B::pnum(reps::<B::W>(c as nat, B::contrib(x))) == c * B::pnum(B::contrib(x)),
{
    hide(nsum);
    let rs = Seq::new(c as nat, |_i: int| x);
    lemma_run_sum::<V, B>(v, k, c, x, c);
    assert(v.skip(k).skip(c) =~= v.skip(k + c));
    lemma_nsum_run::<V, B>(c as nat, x);
    lemma_vsum_run::<V, B>(c as nat, x);
    assert((c as nat) * B::pnum(B::contrib(x)) == c * B::pnum(B::contrib(x)));
    if nsum::<V, B>(v) <= B::pmax() {
        lemma_nsum_mono::<V, B>(v, k + c, v.len() as int);
        assert(v.take(v.len() as int) =~= v);
        lemma_nsum_vsum::<V, B>(rs);
    }
}

/// Where a target `l` past the sum before `k` falls in a run of `c`
/// copies of `x` at `k` whose sum reaches it: `m` values in, `m` being
/// `l` over the value's contribution, rounded up.
proof fn lemma_in_run<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, k: int, c: int, x: V, l: int, m: int)
    requires
        0 <= k,
        0 <= c,
        k + c <= v.len(),
        v.skip(k).take(c) =~= Seq::new(c as nat, |_i: int| x),
        0 < l <= c * B::pnum(B::contrib(x)),
        B::pnum(B::contrib(x)) > 0 ==> m == (l - 1) as nat / B::pnum(B::contrib(x)) + 1,
    ensures
        1 <= m <= c,
        forall|j: int| 0 <= j < k + m ==> nsum::<V, B>(#[trigger] v.take(j)) < nsum::<V, B>(v.take(k)) + l,
        l <= nsum::<V, B>(v.take(k + m)) - nsum::<V, B>(v.take(k)),
{
    hide(nsum);
    let pc = B::pnum(B::contrib(x));
    let base = nsum::<V, B>(v.take(k));
    assert(pc > 0) by {
        if pc == 0 {
            assert(c * pc == 0) by (nonlinear_arith)
                requires
                    pc == 0,
            ;
        }
    }
    let q = (l - 1) as nat / pc;
    assert(q * pc <= l - 1) by (nonlinear_arith)
        requires
            q == (l - 1) as nat / pc,
            pc > 0,
            l > 0,
    ;
    assert((q + 1) * pc >= l) by (nonlinear_arith)
        requires
            q == (l - 1) as nat / pc,
            pc > 0,
            l > 0,
    ;
    assert(q + 1 <= c) by (nonlinear_arith)
        requires
            q * pc <= l - 1,
            l <= c * pc,
            pc > 0,
            l > 0,
    ;
    assert forall|j: int| 0 <= j < k + m implies nsum::<V, B>(#[trigger] v.take(j)) < base + l by {
        if j <= k {
            lemma_nsum_mono::<V, B>(v, j, k);
        } else {
            let i = j - k;
            lemma_run_sum::<V, B>(v, k, c, x, i);
            assert(k + i == j);
            assert(i * pc <= q * pc) by (nonlinear_arith)
                requires
                    0 <= i <= q,
                    pc >= 0,
            ;
        }
    }
    lemma_run_sum::<V, B>(v, k, c, x, m);
    assert(m * pc >= l) by (nonlinear_arith)
        requires
            m == q + 1,
            (q + 1) * pc >= l,
    ;
}

/// The first `j` such that the slab's first `j` values sum to `rem` or
/// more, or the slab's length if none do (given its sum fits the prefix
/// type, and `rem` is positive).
pub(crate) fn p_find_in_slab<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    slab: &'a Slab<X::Tail>,
    rem: B::P,
) -> (r: usize)
    requires
        X::ok(*slab),
        X::vals(*slab).len() <= usize::MAX,
    ensures
        r <= X::vals(*slab).len(),
        nsum::<_, B>(X::vals(*slab)) <= B::pmax() && B::pnum(rem) > 0 ==> {
            &&& forall|j: int| 0 <= j < r ==> nsum::<_, B>(#[trigger] X::vals(*slab).take(j)) < B::pnum(rem)
            &&& B::pnum(rem) <= nsum::<_, B>(X::vals(*slab)) ==> B::pnum(rem) <= nsum::<_, B>(
                X::vals(*slab).take(r as int),
            )
        },
{
    let ghost v = X::vals(*slab);
    let ghost fits = nsum::<_, B>(v) <= B::pmax() && B::pnum(rem) > 0;
    let ghost r0 = rem;
    let mut d = X::open(slab);
    // what is left to reach, past the values so far
    let mut left = rem;
    let mut items: usize = 0;
    proof {
        assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        assert(v.skip(0) =~= v);
    }
    loop
        invariant
            v == X::vals(*slab),
            v.len() <= usize::MAX,
            r0 == rem,
            d.inv(),
            0 <= items <= v.len(),
            d.rest() == v.skip(items as int),
            fits == (nsum::<_, B>(v) <= B::pmax() && B::pnum(r0) > 0),
            fits ==> B::pnum(left) + nsum::<_, B>(v.take(items as int)) == B::pnum(r0),
            fits ==> nsum::<_, B>(v.take(items as int)) < B::pnum(r0),
        ensures
            items == v.len(),
            fits ==> nsum::<_, B>(v) < B::pnum(r0),
        decreases d.rest().len(),
    {
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                let ghost k = items as int;
                let ghost c = run.count as int;
                proof {
                    lemma_run_step::<_, B>(v, k, c, run.value);
                }
                // the run's sum
                let mut runsum = B::zero();
                B::acc_run(&mut runsum, &run);
                proof {
                    B::W::lemma_prefix_add(reps::<B::W>(run.count as nat, B::contrib(run.value)), runsum, runsum);
                }
                if B::le(&left, &runsum) {
                    // within this run: divide by the value's contribution,
                    // rounding up
                    let mut p = B::zero();
                    B::acc(&mut p, run.value);
                    proof {
                        B::W::lemma_prefix_add(B::contrib(run.value), p, p);
                    }
                    let needed = B::div_ceil(&left, &p);
                    let mut n = run.count;
                    if let Some(m) = B::to_usize(&needed) {
                        if m <= run.count {
                            n = m;
                        }
                    }
                    proof {
                        if fits {
                            lemma_in_run::<_, B>(v, k, c, run.value, B::pnum(left) as int, B::pnum(needed) as int);
                        }
                    }
                    return items + n;
                }
                proof {
                    if fits {
                        B::axiom_pnum(left, runsum);
                    }
                }
                left = B::sub(left, &runsum);
                items += run.count;
            },
            None => {
                proof {
                    assert(v.take(items as int) =~= v);
                }
                break;
            },
        }
    }
    proof {
        if fits {
            assert forall|j: int| 0 <= j < items implies nsum::<_, B>(#[trigger] v.take(j)) < B::pnum(
                r0,
            ) by {
                lemma_nsum_mono::<_, B>(v, j, v.len() as int);
                assert(v.take(v.len() as int) =~= v);
            }
        }
    }
    items
}

/// The column's sums at slab boundary `k`, where it fits: the index's
/// prefix there is the number of the values before slab `k`, and whether
/// it reaches `target` is whether that number does.
proof fn lemma_at_slab<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    target: B::P,
    k: int,
)
    where B::W: OrdPrefix
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k <= s.len(),
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax(),
    ensures
        vals_of::<X>(s).len() == total(slab_lens(s)),
        sum(slab_lens(s), 0, k) <= vals_of::<X>(s).len(),
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        B::pnum(pre_at(ws, k)) == nsum::<_, B>(vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int)),
        reached(ws, target, k) == (B::pnum(target) <= nsum::<_, B>(
            vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int),
        )),
{
    let v = vals_of::<X>(s);
    let l = sum(slab_lens(s), 0, k);
    lemma_fold::<X, B>(ws, s, k);
    lemma_upto::<X>(s, k);
    lemma_fold::<X, B>(ws, s, s.len() as int);
    assert(s.take(s.len() as int) =~= s);
    lemma_sum_le(slab_lens(s), 0, k, 0, s.len() as int);
    assert(v.len() == total(slab_lens(s)));
    lemma_nsum_mono::<_, B>(v, l as int, v.len() as int);
    assert(v.take(v.len() as int) =~= v);
    lemma_nsum_vsum::<_, B>(v.take(l as int));
    B::axiom_pnum(target, pre_at(ws, k));
    B::axiom_ple(target, pre_at(ws, k));
}

/// Where the column's sum fits its prefix type, a running sum that has
/// reached the target stays there.
proof fn lemma_stays<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    target: B::P,
)
    where B::W: OrdPrefix
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax(),
    ensures
        stays_reached(ws, target),
{
    reveal(stays_reached);
    let v = vals_of::<X>(s);
    assert forall|i: int, j: int|
        0 <= i <= j <= ws.len() && #[trigger] reached(ws, target, i) implies #[trigger] reached(
        ws,
        target,
        j,
    ) by {
        lemma_at_slab::<X, B>(ws, s, target, i);
        lemma_at_slab::<X, B>(ws, s, target, j);
        lemma_sum_le(slab_lens(s), 0, i, 0, j);
        lemma_nsum_mono::<_, B>(v, sum(slab_lens(s), 0, i) as int, sum(slab_lens(s), 0, j) as int);
    }
}

/// Slab `k`: well-formed, holding as many values as its length, which
/// with the items before it fit a `usize`; and the index counts those
/// items.
proof fn lemma_slab_at<'a, X: Decoders<'a>, B: PrefixOps<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k < s.len(),
    ensures
        X::ok(s[k]),
        X::vals(s[k]).len() == s[k].len,
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        sum(slab_lens(s), 0, k) + s[k].len <= usize::MAX,
{
    lemma_fold::<X, B>(ws, s, k);
    lemma_from::<X>(s, k, 0);
    assert(X::ok(s[k]));
}

/// The column's values before item `i` of slab `k` are those before the
/// slab and the slab's first `i`: they sum to the two's sums.
proof fn lemma_nsum_in<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    s: Seq<Slab<X::Tail>>,
    k: int,
    i: int,
)
    requires
        lens_ok::<X>(s),
        0 <= k < s.len(),
        0 <= i <= s[k].len,
    ensures
        sum(slab_lens(s), 0, k) + i <= vals_of::<X>(s).len(),
        nsum::<_, B>(vals_of::<X>(s).take(sum(slab_lens(s), 0, k) + i)) == nsum::<_, B>(
            vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int),
        ) + nsum::<_, B>(X::vals(s[k]).take(i)),
{
    let v = vals_of::<X>(s);
    let l = sum(slab_lens(s), 0, k) as int;
    let sv = X::vals(s[k]);
    lemma_from::<X>(s, k, 0);
    assert(X::ok(s[k]));
    assert(sv.skip(0) =~= sv);
    assert(v.skip(l).take(i) =~= sv.take(i));
    assert(v.take(l + i) =~= v.take(l) + v.skip(l).take(i));
    lemma_nsum_cat::<_, B>(v.take(l), sv.take(i));
}

/// With every slab's end below the target, the first index whose prefix
/// sum reaches it is one past the column's end.
proof fn lemma_found_past<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    target: B::P,
)
    where B::W: OrdPrefix
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        B::pnum(target) > 0,
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax() && s.len() > 0 ==> !reached(ws, target, s.len() as int),
    ensures
        sum_len(ws.subrange(0, s.len() as int)) == vals_of::<X>(s).len(),
        found_at::<_, B>(vals_of::<X>(s), target, vals_of::<X>(s).len() as int + 1),
{
    let v = vals_of::<X>(s);
    let n = s.len() as int;
    lemma_fold::<X, B>(ws, s, n);
    lemma_upto::<X>(s, n);
    assert(s.take(n) =~= s);
    if nsum::<_, B>(v) <= B::pmax() {
        lemma_at_slab::<X, B>(ws, s, target, n);
        assert(v.take(v.len() as int) =~= v);
        assert(nsum::<_, B>(v) < B::pnum(target));
        assert forall|i: int| 0 <= i < v.len() + 1 && i <= v.len() implies nsum::<_, B>(#[trigger] v.take(i))
            < B::pnum(target) by {
            lemma_nsum_mono::<_, B>(v, i, v.len() as int);
        }
    }
}

/// The target is first reached `j` values into slab `k`: where the sums
/// before the slab are below it, the sums through the slab reach it, and
/// `j` is what `p_find_in_slab` gives for what is left of the target past
/// the slabs before.
proof fn lemma_found_in<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    ws: Seq<B::W>,
    s: Seq<Slab<X::Tail>>,
    target: B::P,
    k: int,
    rem: B::P,
    j: int,
)
    where B::W: OrdPrefix
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax(),
        0 <= k < s.len(),
        B::pnum(target) > 0,
        k > 0 ==> !reached(ws, target, k),
        reached(ws, target, k + 1),
        rem == B::W::prefix_minus(target, pre_at(ws, k)),
        0 <= j <= X::vals(s[k]).len(),
        nsum::<_, B>(X::vals(s[k])) <= B::pmax() && B::pnum(rem) > 0 ==> {
            &&& forall|i: int| 0 <= i < j ==> nsum::<_, B>(#[trigger] X::vals(s[k]).take(i)) < B::pnum(rem)
            &&& B::pnum(rem) <= nsum::<_, B>(X::vals(s[k])) ==> B::pnum(rem) <= nsum::<_, B>(X::vals(s[k]).take(j))
        },
    ensures
        found_at::<_, B>(vals_of::<X>(s), target, sum(slab_lens(s), 0, k) + j),
{
    let v = vals_of::<X>(s);
    let sv = X::vals(s[k]);
    let l = sum(slab_lens(s), 0, k) as int;
    let below = nsum::<_, B>(v.take(l));
    lemma_at_slab::<X, B>(ws, s, target, k);
    lemma_at_slab::<X, B>(ws, s, target, k + 1);
    assert(X::ok(s[k]));
    lemma_sum_split(slab_lens(s), 0, k, k + 1);
    lemma_sum_one(slab_lens(s), k);
    // the sums before the slab are below the target
    assert(below < B::pnum(target)) by {
        if k == 0 {
            assert(v.take(0).len() == 0);
        }
    }
    B::axiom_pnum(target, pre_at(ws, k));
    assert(B::pnum(rem) == B::pnum(target) - below);
    // and the slab's sum takes them to it
    lemma_nsum_in::<X, B>(s, k, sv.len() as int);
    assert(sv.take(sv.len() as int) =~= sv);
    lemma_nsum_mono::<_, B>(v, l + sv.len(), v.len() as int);
    assert(v.take(v.len() as int) =~= v);
    assert(B::pnum(rem) <= nsum::<_, B>(sv) <= B::pmax());
    assert forall|i: int| 0 <= i < l + j && i <= v.len() implies nsum::<_, B>(#[trigger] v.take(i)) < B::pnum(
        target,
    ) by {
        if i <= l {
            lemma_nsum_mono::<_, B>(v, i, l);
        } else {
            lemma_nsum_in::<X, B>(s, k, i - l);
            assert(l + (i - l) == i);
            assert(nsum::<_, B>(sv.take(i - l)) < B::pnum(rem));
        }
    }
    lemma_nsum_in::<X, B>(s, k, j);
}

/// Panics: one past the end of a column of `usize::MAX` items has no
/// index.
#[verifier::external_body]
fn past_usize()
    ensures
        false,
{
    panic!("get_index_for_prefix: one past a column of usize::MAX items has no index")
}

/// [`PrefixColumn::get_index_for_prefix`](crate::PrefixColumn::get_index_for_prefix):
/// the first index whose prefix sum reaches `target`, or one past the end.
#[verifier::spinoff_prover]
pub(crate) fn p_index_for_prefix<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<B::W>,
    target: B::P,
) -> (r: usize)
    where B::W: OrdPrefix
    requires
        ix.wf(),
        ix_ok::<X, B>(ix@, slabs@),
        lens_ok::<X>(slabs@),
        total(slab_lens(slabs@)) <= usize::MAX,
    ensures
        found_at::<_, B>(vals_of::<X>(slabs@), target, r as int),
{
    let ghost s = slabs@;
    let ghost v = vals_of::<X>(s);
    let ghost fits = nsum::<_, B>(v) <= B::pmax();
    let zero = B::zero();
    if B::le(&target, &zero) {
        proof {
            B::axiom_pnum(zero, zero);
            assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        }
        return 0;
    }
    proof {
        ix.lemma_wf_len();
        B::W::lemma_prefix_total();
        lemma_plain_valid(ix@);
    }
    let (si, prefix_before, items_before) = ix.find_slab_at_prefix(&target);
    let ghost k = si as int;
    proof {
        B::axiom_pnum(target, zero);
        if fits {
            lemma_stays::<X, B>(ix@, s, target);
        }
    }
    if si >= slabs.len() {
        // no slab's end reaches the target: one past the end
        proof {
            lemma_found_past::<X, B>(ix@, s, target);
        }
        if items_before == usize::MAX {
            past_usize();
        }
        return items_before + 1;
    }
    proof {
        lemma_slab_at::<X, B>(ix@, s, k);
    }
    let rem = B::sub(target, &prefix_before);
    let j = p_find_in_slab::<X, B>(&slabs[si], rem);
    proof {
        if fits {
            lemma_found_in::<X, B>(ix@, s, target, k, rem, j as int);
        }
    }
    items_before + j
}

/// `advance_prefix(n)` from a window of values `o`: it lands on the first
/// whose running sum, from the window's start, exceeds `n` (returning it,
/// and leaving the window after it), or, if none does, runs to the end.
/// Given that the column's sum fits its prefix type.
pub(crate) open spec fn advanced<V: Copy, B: PrefixOrd<V>>(
    o: Seq<V>,
    n: B::P,
    r: Option<V>,
    k: int,
    o2: Seq<V>,
) -> bool {
    match r {
        Some(v) => {
            &&& 0 <= k < o.len()
            &&& v == o[k]
            &&& o2 == o.skip(k + 1)
            &&& B::pnum(n) < nsum::<V, B>(o.take(k + 1))
            &&& forall|j: int| 0 <= j < k ==> nsum::<V, B>(#[trigger] o.take(j + 1)) <= B::pnum(n)
        },
        None => nsum::<V, B>(o) <= B::pnum(n) && o2.len() == 0,
    }
}

/// The window's running sums are the column's, past its start.
proof fn lemma_window<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    core: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    k: int,
)
    requires
        core.wf::<X>(s),
        core.pos <= vals_of::<X>(s).len(),
        0 <= k <= core.out::<X>(s).len(),
    ensures
        before::<X>(s, core.pos + k) =~= before::<X>(s, core.pos as int) + core.out::<X>(s).take(k),
        nsum::<_, B>(before::<X>(s, core.pos + k)) == nsum::<_, B>(before::<X>(s, core.pos as int))
            + nsum::<_, B>(core.out::<X>(s).take(k)),
        core.pos + core.out::<X>(s).len() <= vals_of::<X>(s).len(),
{
    let v = vals_of::<X>(s);
    let o = core.out::<X>(s);
    assert(v.take(core.pos + k) =~= v.take(core.pos as int) + o.take(k));
    lemma_nsum_cat::<_, B>(v.take(core.pos as int), o.take(k));
}

/// `r` is the last index whose running sum (of the values before it) is
/// at most `m`; `None` if every one is.
pub(crate) open spec fn target_ok<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, m: nat, r: Option<usize>) -> bool {
    match r {
        None => nsum::<V, B>(v) <= m,
        Some(tp) => {
            &&& tp <= v.len()
            &&& forall|j: int| 0 <= j <= tp && j <= v.len() ==> nsum::<V, B>(#[trigger] v.take(j)) <= m
            &&& tp < v.len() ==> m < nsum::<V, B>(v.take(tp + 1))
        },
    }
}

/// The running sums of a stretch `o` of `v` from `p0` are those of `v`,
/// less the sum before `p0`.
proof fn lemma_stretch_sum<V: Copy, B: PrefixOrd<V>>(v: Seq<V>, o: Seq<V>, p0: int, k: int)
    requires
        0 <= p0,
        p0 + o.len() <= v.len(),
        o == v.subrange(p0, p0 + o.len()),
        0 <= k <= o.len(),
    ensures
        nsum::<V, B>(v.take(p0 + k)) == nsum::<V, B>(v.take(p0)) + nsum::<V, B>(o.take(k)),
{
    hide(nsum);
    assert(v.take(p0 + k) =~= v.take(p0) + o.take(k));
    lemma_nsum_cat::<V, B>(v.take(p0), o.take(k));
}

/// From the target position, where a search lands in a stretch `o` of
/// `v` from `p0` that is no longer than `left`: on the item there, if
/// the stretch holds it, else past its end.
proof fn lemma_advance_in<V: Copy, B: PrefixOrd<V>>(
    v: Seq<V>,
    o: Seq<V>,
    p0: int,
    left: int,
    pn: nat,
    target: Option<usize>,
)
    requires
        0 <= p0,
        p0 + o.len() <= v.len(),
        o == v.subrange(p0, p0 + o.len()),
        o.len() <= left,
        target_ok::<V, B>(v, nsum::<V, B>(v.take(p0)) + pn, target),
    ensures
        match target {
            Some(tp) if p0 <= tp < p0 + left => {
                let k = tp - p0;
                &&& k < o.len() ==> pn < nsum::<V, B>(o.take(k + 1)) && forall|j: int|
                    0 <= j < k ==> nsum::<V, B>(#[trigger] o.take(j + 1)) <= pn
                &&& k >= o.len() ==> nsum::<V, B>(o) <= pn
            },
            _ => nsum::<V, B>(o) <= pn,
        },
{
    hide(nsum);
    let e = p0 + o.len();
    lemma_stretch_sum::<V, B>(v, o, p0, o.len() as int);
    assert(o.take(o.len() as int) =~= o);
    match target {
        None => {
            lemma_nsum_mono::<V, B>(v, e, v.len() as int);
            assert(v.take(v.len() as int) =~= v);
        },
        Some(tp) => {
            if tp < p0 {
                lemma_nsum_mono::<V, B>(v, tp + 1, p0);
            } else if tp < p0 + left {
                let k = tp - p0;
                if k < o.len() {
                    lemma_stretch_sum::<V, B>(v, o, p0, k + 1);
                    assert forall|j: int| 0 <= j < k implies nsum::<V, B>(#[trigger] o.take(j + 1)) <= pn by {
                        lemma_stretch_sum::<V, B>(v, o, p0, j + 1);
                    }
                }
            }
        },
    }
}

/// From the target position, where `advance_prefix` lands in the window:
/// on the item there, if the window holds it, else past its end.
proof fn lemma_advance<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    s: Seq<Slab<X::Tail>>,
    pn: nat,
    target: Option<usize>,
)
    requires
        c0.wf::<X>(s),
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax(),
        target_ok::<_, B>(vals_of::<X>(s), nsum::<_, B>(before::<X>(s, c0.pos as int)) + pn, target),
    ensures
        ({
            let o = c0.out::<X>(s);
            match target {
                Some(tp) if c0.pos <= tp < c0.pos + c0.items_left => {
                    let k = tp - c0.pos;
                    &&& k < o.len() ==> pn < nsum::<_, B>(o.take(k + 1)) && forall|j: int|
                        0 <= j < k ==> nsum::<_, B>(#[trigger] o.take(j + 1)) <= pn
                    &&& k >= o.len() ==> nsum::<_, B>(o) <= pn
                },
                _ => nsum::<_, B>(o) <= pn,
            }
        }),
{
    hide(ICore::wf);
    let v = vals_of::<X>(s);
    let o = c0.out::<X>(s);
    let p0 = c0.pos as int;
    if p0 > v.len() {
        assert(o =~= Seq::<<X::D as Decode>::Item>::empty());
        return;
    }
    assert(o =~= v.subrange(p0, p0 + o.len()));
    lemma_advance_in::<_, B>(v, o, p0, c0.items_left as int, pn, target);
}

/// Where a search starts from: the window is no longer than the items
/// left, and the running total, when the column's sum fits its prefix
/// type, is the number that the values before the iterator sum to.
proof fn lemma_advance_from<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    c0: ICore<X::D>,
    sum: B::P,
    base: B::P,
    s: Seq<Slab<X::Tail>>,
)
    requires
        at_sum::<X, B>(c0, sum, base, s),
    ensures
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        c0.pos + c0.items_left <= usize::MAX,
        c0.out::<X>(s).len() <= c0.items_left,
        nsum::<_, B>(vals_of::<X>(s)) <= B::pmax() ==> B::pnum(B::W::prefix_plus(base, sum)) == nsum::<_, B>(
            before::<X>(s, c0.pos as int),
        ),
{
    let v = vals_of::<X>(s);
    let p0 = c0.pos as int;
    if nsum::<_, B>(v) <= B::pmax() {
        if p0 <= v.len() {
            lemma_nsum_mono::<_, B>(v, p0, v.len() as int);
            assert(v.take(v.len() as int) =~= v);
        }
        lemma_nsum_vsum::<_, B>(before::<X>(s, p0));
    }
}

/// The position of the item holding unit `n + 1` past the sum `abs`: the
/// last index whose running sum (of the values before it) is at most
/// `abs + n`, or `None` if `abs + n` passes the prefix type's limit (and
/// so every running sum).
fn p_target<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<B::W>,
    abs: &B::P,
    n: &B::P,
) -> (r: Option<usize>)
    where B::W: OrdPrefix
    requires
        ix.wf(),
        ix_ok::<X, B>(ix@, slabs@),
        lens_ok::<X>(slabs@),
        total(slab_lens(slabs@)) <= usize::MAX,
    ensures
        nsum::<_, B>(vals_of::<X>(slabs@)) <= B::pmax() ==> target_ok::<_, B>(
            vals_of::<X>(slabs@),
            B::pnum(*abs) + B::pnum(*n),
            r,
        ),
{
    let ghost v = vals_of::<X>(slabs@);
    let one = B::one();
    let t = B::checked_add(abs, n)?;
    let t1 = match B::checked_add(&t, &one) {
        Some(t1) => t1,
        None => {
            proof {
                B::axiom_pnum(*abs, *n);
            }
            return None;
        },
    };
    let i = p_index_for_prefix::<X, B>(slabs, ix, t1);
    proof {
        B::axiom_pnum(*abs, *n);
        B::axiom_pnum(t, one);
        assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
    }
    if i == 0 {
        Some(0)
    } else {
        Some(i - 1)
    }
}

/// [`PrefixIter::advance_prefix`](crate::prefix::PrefixIter::advance_prefix):
/// past `n` units of the running sum, the item holding the next one.
#[verifier::spinoff_prover]
pub(crate) fn p_advance<'a, X: Decoders<'a>, B: PrefixOrd<<X::D as Decode>::Item>>(
    core: &mut ICore<X::D>,
    slabs: &'a [Slab<X::Tail>],
    ix: &SlabBTree<B::W>,
    n: &B::P,
    total: &mut B::P,
    base: &B::P,
) -> (r: Option<<X::D as Decode>::Item>)
    where B::W: OrdPrefix
    requires
        at_sum::<X, B>(*old(core), *old(total), *base, slabs@),
        ix.wf(),
        ix_ok::<X, B>(ix@, slabs@),
    ensures
        at_sum::<X, B>(*final(core), *final(total), *base, slabs@),
        nsum::<_, B>(vals_of::<X>(slabs@)) <= B::pmax() ==> advanced::<_, B>(
            old(core).out::<X>(slabs@),
            *n,
            r,
            final(core).pos - old(core).pos - 1,
            final(core).out::<X>(slabs@),
        ),
        // a value found leaves the window's end where it was
        r is Some ==> final(core).pos + final(core).items_left == old(core).pos + old(core).items_left,
        r is None ==> {
            &&& final(core).items_left == 0
            // past the data (the window can run past it), it stops at the end
            &&& old(core).pos <= crate::fenwick::total(slab_lens(slabs@)) ==> final(core).pos == old(core).pos
                + old(core).out::<X>(slabs@).len()
        },
{
    hide(ICore::wf);
    hide(ICore::out);
    hide(target_ok);
    let ghost s = slabs@;
    let ghost fits = nsum::<_, B>(vals_of::<X>(s)) <= B::pmax();
    let ghost c0 = *core;
    proof {
        c0.lemma_out_len::<X>(s);
    }
    // the sum of the values before the iterator
    let abs = B::add(base, total);
    proof {
        lemma_advance_from::<X, B>(c0, *total, *base, s);
    }
    let target = p_target::<X, B>(slabs, ix, &abs, n);
    proof {
        if fits {
            lemma_advance::<X, B>(c0, s, B::pnum(*n), target);
        }
    }
    // the item, if the window holds it; else run to the window's end
    let mut landed = false;
    let mut k = 0;
    if let Some(tp) = target {
        if tp >= core.pos && tp < core.pos + core.items_left {
            landed = true;
            k = tp - core.pos;
        }
    }
    if !landed {
        if core.items_left == 0 {
            return None;
        }
        k = core.items_left - 1;
    }
    let r = p_nth::<X, B>(core, slabs, Some(ix), k, total, base);
    if landed {
        r
    } else {
        None
    }
}

} // verus!
