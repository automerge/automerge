//! The weight a column's index keeps for each slab, verified with Verus.
//!
//! The edit cursor keeps every slab's weight related to the slab by the
//! column's weight model (`cursor::Weigh::wrel`), and each weight
//! function names its model (`WeightFn::Model`), so the relation is
//! concrete, and each model's `compute` is the verified computation:
//!
//! - [`LenModel`], a plain column's: the slab's item count.
//! - [`PrefixModel`], a prefix column's: the item count and the sum of the
//!   slab's values (`vsum`), computed by `prefix_core::p_slab_sum` (or its
//!   checked twin, which panics where the sum would overflow).
//! - [`DeltaModel`], a delta column's: the item count and, where the
//!   slab's running sums fit `i64`, the aggregate of its deltas
//!   (`agg_of`), computed by `delta::agg::slab_agg`.
//!
//! The models read slabs through their encoding's read model, which
//! agrees with the edit model (`cursor::ReadsAgree`). Trusted: the value
//! types' contributions and deltas (`PrefixOps`, `DeltaOps`), and the
//! prefix columns' run-time overflow checks, which specify nothing as they
//! can only panic.

use crate::btree::SlabAgg;
#[cfg(verus_keep_ghost)]
use crate::btree::{lemma_prefix_len, SlabAggregate};
use crate::column::Slab;
use crate::cursor::{ReadsAgree, Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{agg_of, deltas_of, steps_fit};
use crate::delta::agg::{slab_agg, DeltaOps};
use crate::index::IndexOps;
use crate::iter_core::{Decode, Decoders};
use crate::prefix::PrefixSlabWeight;
#[cfg(verus_keep_ghost)]
use crate::prefix_core::vsum;
use crate::prefix_core::{p_slab_sum, p_slab_sum_checked, PrefixOps};
use std::marker::PhantomData;
use std::ops::{AddAssign, SubAssign};
use vstd::prelude::*;

verus! {

/// Panics: a prefix sum that overflows its type.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn sum_overflows()
    ensures
        false,
{
    panic!("prefix sum overflows its type")
}

/// A plain column's weights: each slab's item count.
pub struct LenModel;

impl<E: Slabs> Weigh<E, usize> for LenModel {
    open spec fn wrel(w: usize, s: Slab<E::Tail>) -> bool {
        w == s.len
    }

    #[inline(always)]
    fn compute<I: IndexOps<usize>>(s: &Slab<E::Tail>) -> (w: usize) {
        proof {
            I::lemma_wlen(s.len);
        }
        s.len
    }

    #[inline(always)]
    fn check_set<I: IndexOps<usize>>(_index: &I, _i: usize, _w: &usize, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<usize>>(_index: &I, _start: usize, _end: usize, _new: &Vec<usize>, _len: usize) {
    }
}

/// A prefix column's weights: each slab's item count and the sum of its
/// values, through read model `X` and prefix arithmetic `B`.
pub struct PrefixModel<X, B>(PhantomData<(X, B)>);

impl<E, X, B, P> Weigh<E, PrefixSlabWeight<P>> for PrefixModel<X, B> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> PrefixOps<<<X as Decoders<'a>>::D as Decode>::Item, P = P>,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
 {
    open spec fn wrel(w: PrefixSlabWeight<P>, s: Slab<E::Tail>) -> bool {
        &&& w.len == s.len
        &&& w.prefix == vsum::<_, B>(<X as Decoders<'static>>::vals(s))
    }

    #[inline(always)]
    fn compute<I: IndexOps<PrefixSlabWeight<P>>>(s: &Slab<E::Tail>) -> (w: PrefixSlabWeight<P>) {
        proof {
            <E as ReadsAgree<'static, X>>::lemma_reads(*s);
        }
        let prefix = if s.len <= <B as PrefixOps<<<X as Decoders<'static>>::D as Decode>::Item>>::max_unchecked_len() {
            p_slab_sum::<X, B>(s)
        } else {
            match p_slab_sum_checked::<X, B>(s) {
                Some(p) => p,
                None => {
                    sum_overflows();
                    unreached()
                },
            }
        };
        let w = PrefixSlabWeight { len: s.len, prefix };
        proof {
            I::lemma_wlen(w);
            lemma_prefix_len(w);
        }
        w
    }

    #[verifier::external_body]
    #[inline(always)]
    fn check_set<I: IndexOps<PrefixSlabWeight<P>>>(index: &I, i: usize, w: &PrefixSlabWeight<P>, len: usize) {
        check_total::<I, B, P, <<X as Decoders<'static>>::D as Decode>::Item>(index, i, i + 1, std::slice::from_ref(w), len)
    }

    #[verifier::external_body]
    #[inline(always)]
    fn check_splice<I: IndexOps<PrefixSlabWeight<P>>>(
        index: &I,
        start: usize,
        end: usize,
        new: &Vec<PrefixSlabWeight<P>>,
        len: usize,
    ) {
        check_total::<I, B, P, <<X as Decoders<'static>>::D as Decode>::Item>(index, start, end, new, len)
    }
}

/// A delta column's weights: each slab's item count and, where its
/// running sums fit `i64`, the aggregate of its deltas, through read model
/// `X` and deltas `B`.
pub struct DeltaModel<X, B>(PhantomData<(X, B)>);

impl<E, X, B> Weigh<E, SlabAgg> for DeltaModel<X, B> where
    E: Slabs + for<'a> ReadsAgree<'a, X>,
    X: for<'a> Decoders<'a, Tail = E::Tail>,
    B: for<'a> DeltaOps<<<X as Decoders<'a>>::D as Decode>::Item>,
 {
    open spec fn wrel(w: SlabAgg, s: Slab<E::Tail>) -> bool {
        let ds = deltas_of::<_, B>(<X as Decoders<'static>>::vals(s));
        &&& w.len == s.len
        &&& steps_fit(ds) ==> agg_of(w, ds)
    }

    #[inline(always)]
    fn compute<I: IndexOps<SlabAgg>>(s: &Slab<E::Tail>) -> (w: SlabAgg) {
        proof {
            <E as ReadsAgree<'static, X>>::lemma_reads(*s);
        }
        let w = slab_agg::<X, B>(s);
        proof {
            I::lemma_wlen(w);
        }
        w
    }

    #[inline(always)]
    fn check_set<I: IndexOps<SlabAgg>>(_index: &I, _i: usize, _w: &SlabAgg, _len: usize) {
    }

    #[inline(always)]
    fn check_splice<I: IndexOps<SlabAgg>>(_index: &I, _start: usize, _end: usize, _new: &Vec<SlabAgg>, _len: usize) {
    }
}

/// Never returns: after a call that does not.
#[verifier::external_body]
#[inline(always)]
fn unreached<A>() -> A
    requires
        false,
{
    unreachable!()
}

} // verus!

/// Panic unless the column `index` indexes, with `new` in place of the
/// weights of slabs `start..end`, sums within the prefix type, for a
/// column of `len` items longer than one whose sums cannot overflow. Its
/// values add nonnegative amounts, so the total bounds every prefix sum.
#[inline(always)]
fn check_total<I, B, P, V>(
    index: &I,
    start: usize,
    end: usize,
    new: &[PrefixSlabWeight<P>],
    len: usize,
) where
    I: IndexOps<PrefixSlabWeight<P>>,
    B: PrefixOps<V, P = P>,
    V: Copy,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    if len > B::max_unchecked_len() {
        check_spliced_total::<I, B, P, V>(index, start, end, new);
    }
}

#[cold]
#[inline(never)]
fn check_spliced_total<I, B, P, V>(index: &I, start: usize, end: usize, new: &[PrefixSlabWeight<P>])
where
    I: IndexOps<PrefixSlabWeight<P>>,
    B: PrefixOps<V, P = P>,
    V: Copy,
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    // the other slabs' total: it fits, as the column's did
    let mut total = index.ix_total().prefix;
    for i in start..end {
        total -= index.ix_weight(i).prefix;
    }
    for w in new {
        if !B::try_add(&mut total, &w.prefix) {
            panic!("prefix sum overflows its type");
        }
    }
}
