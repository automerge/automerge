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
//! numbers' order, and so on. That model is trusted (`PrefixOrd`): it is
//! what unsigned integers do, and the searches are only offered for prefix
//! types that are (`UnsignedPrefix`).

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::assign_op_pattern)]

use crate::btree::SlabBTree;
#[cfg(verus_keep_ghost)]
use crate::btree::{
    axiom_prefix_add, fold, pre_at, prefix_add, prefix_leq, prefix_zero, reached, stays_reached,
    sum_len, PrefixAggregate,
};
use crate::column::Slab;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
#[cfg(verus_keep_ghost)]
use crate::fenwick::{lemma_sum_le, lemma_sum_one, lemma_sum_split, sum, total};
#[cfg(verus_keep_ghost)]
use crate::iter_core::{lemma_from, lemma_upto, lens_ok, vals_of};
use crate::iter_core::{Decode, Decoders};
use crate::prefix::PrefixSlabWeight;
use crate::prefix_core::PrefixOps;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{
    ix_ok, lemma_fold, lemma_vsum_cat, lemma_vsum_run, prefix_sub, reps, vsum,
};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The number a prefix value stands for.
pub(crate) uninterp spec fn pnum<P>(p: P) -> nat;

/// The largest number prefix type `P` holds.
pub(crate) uninterp spec fn pmax<P>() -> nat;

/// An unsigned prefix type's arithmetic and order, as numbers: the
/// trusted bridge to `T::Prefix`'s `<=`, `/` and `try_into`.
pub(crate) trait PrefixOrd<V: Copy>: PrefixOps<V> {
    /// Sums that fit the type are the numbers' sums, a difference that
    /// isn't negative is theirs, and `<=` is their order, as for unsigned
    /// integers.
    proof fn axiom_pnum(a: Self::P, b: Self::P)
        ensures
            pnum(a) <= pmax::<Self::P>(),
            pnum(prefix_zero::<Self::P>()) == 0,
            pnum(a) + pnum(b) <= pmax::<Self::P>() ==> pnum(prefix_add(a, b)) == pnum(a) + pnum(b),
            pnum(b) <= pnum(a) ==> pnum(prefix_sub(a, b)) == pnum(a) - pnum(b),
            prefix_leq(a, b) == (pnum(a) <= pnum(b)),
    ;

    /// `a <= b`.
    fn le(a: &Self::P, b: &Self::P) -> (r: bool)
        ensures
            r == (pnum(*a) <= pnum(*b)),
    ;

    /// `(a - 1) / b + 1`: `a` divided by `b`, rounding up (when both are
    /// positive; otherwise it may panic).
    fn div_ceil(a: &Self::P, b: &Self::P) -> (r: Self::P)
        ensures
            pnum(*a) > 0 && pnum(*b) > 0 ==> pnum(r) == (pnum(*a) - 1) as nat / pnum(*b) + 1,
    ;

    /// `p.try_into()`.
    fn to_usize(p: &Self::P) -> (r: Option<usize>)
        ensures
            pnum(*p) <= usize::MAX ==> r == Some(pnum(*p) as usize),
    ;
}

/// The sum of values' contributions, as numbers.
pub(crate) open spec fn nsum<V: Copy, B: PrefixOps<V>>(vs: Seq<V>) -> nat
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        nsum::<V, B>(vs.drop_last()) + pnum(B::contrib(vs.last()))
    }
}

pub(crate) proof fn lemma_nsum_cat<V: Copy, B: PrefixOps<V>>(a: Seq<V>, b: Seq<V>)
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
pub(crate) proof fn lemma_nsum_mono<V: Copy, B: PrefixOps<V>>(vs: Seq<V>, i: int, j: int)
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
        nsum::<V, B>(vs) <= pmax::<B::P>(),
    ensures
        pnum(vsum::<V, B>(vs)) == nsum::<V, B>(vs),
    decreases vs.len(),
{
    if vs.len() == 0 {
        B::axiom_pnum(prefix_zero::<B::P>(), prefix_zero::<B::P>());
    } else {
        let d = vs.drop_last();
        lemma_nsum_vsum::<V, B>(d);
        B::axiom_pnum(vsum::<V, B>(d), B::contrib(vs.last()));
    }
}

/// A run of `n` copies of `v` adds `n` times `v`'s contribution.
pub(crate) proof fn lemma_nsum_run<V: Copy, B: PrefixOps<V>>(n: nat, v: V)
    ensures
        nsum::<V, B>(Seq::new(n, |_i: int| v)) == n * pnum(B::contrib(v)),
    decreases n,
{
    let c = pnum(B::contrib(v));
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
pub(crate) open spec fn found_at<V: Copy, B: PrefixOps<V>>(v: Seq<V>, target: B::P, r: int) -> bool {
    nsum::<V, B>(v) <= pmax::<B::P>() ==> {
        &&& 0 <= r <= v.len() + 1
        &&& forall|i: int| 0 <= i < r && i <= v.len() ==> nsum::<V, B>(#[trigger] v.take(i)) < pnum(target)
        &&& r <= v.len() ==> pnum(target) <= nsum::<V, B>(v.take(r))
    }
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
        nsum::<_, B>(X::vals(*slab)) <= pmax::<B::P>() && pnum(rem) > 0 ==> {
            &&& forall|j: int| 0 <= j < r ==> nsum::<_, B>(#[trigger] X::vals(*slab).take(j)) < pnum(rem)
            &&& pnum(rem) <= nsum::<_, B>(X::vals(*slab)) ==> pnum(rem) <= nsum::<_, B>(
                X::vals(*slab).take(r as int),
            )
        },
{
    let ghost v = X::vals(*slab);
    let ghost fits = nsum::<_, B>(v) <= pmax::<B::P>() && pnum(rem) > 0;
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
            fits == (nsum::<_, B>(v) <= pmax::<B::P>() && pnum(r0) > 0),
            fits ==> pnum(left) + nsum::<_, B>(v.take(items as int)) == pnum(r0),
            fits ==> nsum::<_, B>(v.take(items as int)) < pnum(r0),
        ensures
            items == v.len(),
            fits ==> nsum::<_, B>(v) < pnum(r0),
        decreases d.rest().len(),
    {
        match d.d_run_max(usize::MAX) {
            Some(run) => {
                let ghost k = items as int;
                let ghost c = run.count as int;
                let ghost rs = Seq::new(run.count as nat, |_i: int| run.value);
                proof {
                    assert(v.take(k + c) =~= v.take(k) + rs) by {
                        assert forall|i: int| 0 <= i < c implies #[trigger] v[k + i] == run.value by {
                            assert(v.skip(k).take(c)[i] == run.value);
                        }
                    }
                    assert(v.skip(k).skip(c) =~= v.skip(k + c));
                    lemma_nsum_cat::<_, B>(v.take(k), rs);
                    lemma_nsum_run::<_, B>(run.count as nat, run.value);
                    lemma_vsum_run::<_, B>(run.count as nat, run.value);
                    if fits {
                        lemma_nsum_mono::<_, B>(v, k + c, v.len() as int);
                        assert(v.take(v.len() as int) =~= v);
                        lemma_nsum_vsum::<_, B>(rs);
                    }
                }
                // the run's sum
                let mut runsum = B::zero();
                B::acc_run(&mut runsum, &run);
                proof {
                    axiom_prefix_add(reps(run.count as nat, B::contrib(run.value)), runsum, runsum);
                }
                if B::le(&left, &runsum) {
                    // within this run: divide by the value's contribution,
                    // rounding up
                    let mut p = B::zero();
                    B::acc(&mut p, run.value);
                    proof {
                        axiom_prefix_add(B::contrib(run.value), p, p);
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
                            let pc = pnum(B::contrib(run.value));
                            let l = pnum(left);
                            let base = nsum::<_, B>(v.take(k));
                            assert(l + base == pnum(r0));
                            assert(l > 0);
                            assert(pnum(runsum) == c * pc);
                            assert(l <= c * pc);
                            assert(pnum(p) == pc);
                            assert(pc > 0) by {
                                if pc == 0 {
                                    assert(c * pc == 0) by (nonlinear_arith) requires pc == 0;
                                }
                            }
                            let q = (l - 1) as nat / pc;
                            assert(pnum(needed) == q + 1);
                            assert(q * pc <= l - 1) by (nonlinear_arith)
                                requires q == (l - 1) as nat / pc, pc > 0, l > 0;
                            assert((q + 1) * pc >= l) by (nonlinear_arith)
                                requires q == (l - 1) as nat / pc, pc > 0, l > 0;
                            assert(q + 1 <= c) by (nonlinear_arith)
                                requires q * pc <= l - 1, l <= c * pc, pc > 0, l > 0;
                            assert(n == q + 1);
                            let m = n as int;
                            // the values through the run's first `m`
                            assert forall|j: int| 0 <= j < k + m implies nsum::<_, B>(
                                #[trigger] v.take(j),
                            ) < pnum(r0) by {
                                if j <= k {
                                    lemma_nsum_mono::<_, B>(v, j, k);
                                } else {
                                    let i = j - k;
                                    let ri = Seq::new(i as nat, |_x: int| run.value);
                                    assert(v.take(j) =~= v.take(k) + ri) by {
                                        assert forall|x: int| 0 <= x < i implies #[trigger] v[k + x]
                                            == run.value by {
                                            assert(v.skip(k).take(c)[x] == run.value);
                                        }
                                    }
                                    lemma_nsum_cat::<_, B>(v.take(k), ri);
                                    lemma_nsum_run::<_, B>(i as nat, run.value);
                                    let ip = (i as nat) * pc;
                                    assert(ip <= q * pc) by (nonlinear_arith)
                                        requires ip == (i as nat) * pc, i <= q, i >= 0, pc >= 0;
                                    assert(nsum::<_, B>(v.take(j)) == base + ip);
                                }
                            }
                            let rm = Seq::new(m as nat, |_x: int| run.value);
                            assert(v.take(k + m) =~= v.take(k) + rm) by {
                                assert forall|x: int| 0 <= x < m implies #[trigger] v[k + x] == run.value by {
                                    assert(v.skip(k).take(c)[x] == run.value);
                                }
                            }
                            lemma_nsum_cat::<_, B>(v.take(k), rm);
                            lemma_nsum_run::<_, B>(m as nat, run.value);
                            let mp = (m as nat) * pc;
                            assert(mp >= l) by (nonlinear_arith)
                                requires mp == (m as nat) * pc, m == q + 1, (q + 1) * pc >= l;
                            assert(nsum::<_, B>(v.take(k + m)) == base + mp);
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
            assert forall|j: int| 0 <= j < items implies nsum::<_, B>(#[trigger] v.take(j)) < pnum(
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
    ws: Seq<PrefixSlabWeight<B::P>>,
    s: Seq<Slab<X::Tail>>,
    target: B::P,
    k: int,
)
    where B::P: Ord
    requires
        ix_ok::<X, B>(ws, s),
        lens_ok::<X>(s),
        total(slab_lens(s)) <= usize::MAX,
        0 <= k <= s.len(),
        nsum::<_, B>(vals_of::<X>(s)) <= pmax::<B::P>(),
    ensures
        vals_of::<X>(s).len() == total(slab_lens(s)),
        sum(slab_lens(s), 0, k) <= vals_of::<X>(s).len(),
        sum_len(ws.subrange(0, k)) == sum(slab_lens(s), 0, k),
        pnum(pre_at(ws, k)) == nsum::<_, B>(vals_of::<X>(s).take(sum(slab_lens(s), 0, k) as int)),
        reached(ws, target, k) == (pnum(target) <= nsum::<_, B>(
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
    ix: &SlabBTree<PrefixSlabWeight<B::P>>,
    target: B::P,
) -> (r: usize)
    where B::P: Ord
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
    let ghost fits = nsum::<_, B>(v) <= pmax::<B::P>();
    let zero = B::zero();
    if B::le(&target, &zero) {
        proof {
            B::axiom_pnum(zero, zero);
            assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
        }
        return 0;
    }
    let (si, prefix_before, items_before) = ix.find_slab_at_prefix(&target);
    let ghost k = si as int;
    let ghost l = sum(slab_lens(s), 0, k);
    proof {
        B::axiom_pnum(target, zero);
        if fits {
            lemma_at_slab::<X, B>(ix@, s, target, k);
            reveal(stays_reached);
            assert forall|i: int, j: int|
                0 <= i <= j <= ix@.len() && #[trigger] reached(ix@, target, i) implies #[trigger] reached(
                ix@,
                target,
                j,
            ) by {
                lemma_at_slab::<X, B>(ix@, s, target, i);
                lemma_at_slab::<X, B>(ix@, s, target, j);
                lemma_sum_le(slab_lens(s), 0, i, 0, j);
                lemma_nsum_mono::<_, B>(v, sum(slab_lens(s), 0, i) as int, sum(slab_lens(s), 0, j) as int);
            }
            assert(stays_reached(ix@, target));
            // the prefix before the slab is below the target
            if k > 0 {
                assert(!reached(ix@, target, k));
            } else {
                lemma_fold::<X, B>(ix@, s, 0);
                assert(s.take(0) =~= Seq::<Slab<X::Tail>>::empty());
                assert(v.take(0) =~= Seq::<<X::D as Decode>::Item>::empty());
            }
            assert(pnum(prefix_before) < pnum(target));
        }
    }
    if si >= slabs.len() {
        // no slab's end reaches the target: one past the end
        proof {
            lemma_fold::<X, B>(ix@, s, k);
            lemma_upto::<X>(s, k);
            assert(s.take(k) =~= s);
            if fits {
                lemma_at_slab::<X, B>(ix@, s, target, k);
                assert(v.take(v.len() as int) =~= v);
                assert forall|i: int| 0 <= i < items_before + 1 && i <= v.len() implies nsum::<_, B>(
                    #[trigger] v.take(i),
                ) < pnum(target) by {
                    lemma_nsum_mono::<_, B>(v, i, v.len() as int);
                }
            }
        }
        if items_before == usize::MAX {
            past_usize();
        }
        return items_before + 1;
    }
    proof {
        lemma_fold::<X, B>(ix@, s, k);
        lemma_from::<X>(s, k, 0);
        lemma_upto::<X>(s, k);
        assert(X::ok(s[k]));
        assert(X::vals(s[k]).len() == s[k].len);
    }
    let rem = B::sub(target, &prefix_before);
    let j = p_find_in_slab::<X, B>(&slabs[si], rem);
    proof {
        let sv = X::vals(s[k]);
        // the values before item `l + i` are those before the slab and its
        // first `i`
        assert forall|i: int| 0 <= i <= sv.len() implies #[trigger] v.take(l + i) =~= v.take(l as int) + sv.take(i) by {
            assert(v.skip(l as int).take(i) =~= sv.take(i));
            assert(v.take(l + i) =~= v.take(l as int) + v.skip(l as int).take(i));
        }
        if fits {
            lemma_at_slab::<X, B>(ix@, s, target, k + 1);
            lemma_sum_split(slab_lens(s), 0, k, k + 1);
            assert(sum(slab_lens(s), k, k + 1) == sv.len()) by {
                lemma_sum_one(slab_lens(s), k);
            }
            assert(v.take((l + sv.len()) as int) =~= v.take(l as int) + sv.take(sv.len() as int));
            assert(sv.take(sv.len() as int) =~= sv);
            lemma_nsum_cat::<_, B>(v.take(l as int), sv);
            lemma_nsum_mono::<_, B>(v, (l + sv.len()) as int, v.len() as int);
            assert(v.take(v.len() as int) =~= v);
            B::axiom_pnum(target, prefix_before);
            assert(pnum(rem) > 0);
            assert(pnum(rem) <= nsum::<_, B>(sv));
            assert forall|i: int| 0 <= i < l + j && i <= v.len() implies nsum::<_, B>(#[trigger] v.take(i))
                < pnum(target) by {
                if i <= l {
                    lemma_nsum_mono::<_, B>(v, i, l as int);
                } else {
                    assert(v.take(l + (i - l)) =~= v.take(l as int) + sv.take(i - l));
                    lemma_nsum_cat::<_, B>(v.take(l as int), sv.take(i - l));
                    assert(nsum::<_, B>(sv.take(i - l)) < pnum(rem));
                }
            }
            assert(v.take(l + j) =~= v.take(l as int) + sv.take(j as int));
            lemma_nsum_cat::<_, B>(v.take(l as int), sv.take(j as int));
        }
    }
    items_before + j
}

} // verus!
