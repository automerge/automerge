//! The implementation of `verified::delta` and its proofs: what its
//! specification (`delta.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::iter::IteratorSpec;

use crate::attrib::weigh_run;
use crate::btree::{SlabAgg, SlabBTree};
use crate::column::Slab;
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::slab_lens;
use crate::cursor::{splice_feed_bounded, ReadsAgree, Slabs, Visitor, Weigh};
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::{deltas_of, psum, reps};
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::in_bound;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{
    dslabs, item_vals, items_vals, lemma_dslabs_own, lemma_dslabs_unit, rvals,
};
use crate::delta::ddomain::{
    BoundedItems, BoundedValues, DeltaFeed, DeltaWrite, SpliceDeltas, SpliceItem, SpliceItems,
    SpliceValues, ValueItems, ValuesFeed,
};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::{lemma_rz_nulls, lemma_rz_run, rz};
use crate::delta::dedit_core::DCol;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_wrapping_add};
#[cfg(verus_keep_ghost)]
use crate::fenwick::lemma_sum_one;
use crate::index::IndexOps;
use crate::iter_core::{Decode, Decoders};
use crate::rle::dec::RleReads;
use crate::rle::edit::{null_in_column, ReadIns, RleModel};
use crate::rle::lift::Lift;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::verified::{delta_below, dom};
use crate::weights::{DeltaModel, LoadWeigh};
use crate::{DeltaRun, Leb128, LoadOpts, MaybeFill, PackError};
use std::marker::PhantomData;
use std::ops::{Bound, Range};
use vstd::prelude::*;

verus! {

impl seal::DeltaSeal for u32 {

}

impl seal::DeltaSeal for u64 {

}

impl seal::DeltaSeal for i32 {

}

impl seal::DeltaSeal for i64 {

}

impl seal::DeltaSeal for usize {

}

/// The value a column keeps as `d` is ordered against a value `v` as
/// `d` is against what `v` is kept as, and comes before a `v` that does
/// not fit.
pub(crate) proof fn lemma_bound<T: Delta>(v: T, d: Option<i64>)
    requires
        in_dom::<T>(d),
    ensures
        T::of(d).fits(),
        T::of(d).dv() == d,
        v.fits() ==> {
            &&& T::below(T::of(d), v) == delta_below(d, v.dv())
            &&& T::below(v, T::of(d)) == delta_below(v.dv(), d)
            &&& (T::of(d) == v) == (d == v.dv())
        },
        !v.fits() ==> T::below(T::of(d), v) && !T::below(v, T::of(d)) && T::of(d) != v,
{
    let w = T::of(d);
    T::lemma_delta(w, d);
    T::lemma_delta(v, d);
    T::lemma_order(w, v);
    T::lemma_order(v, w);
    T::lemma_below(w, v, w);
}

/// Whether `v` is at the lower bound `lo` or after it.
pub(super) open spec fn above_low<T: Delta>(lo: Bound<&T>, v: T) -> bool {
    match lo {
        Bound::Included(a) => !T::below(v, *a),
        Bound::Excluded(a) => T::below(*a, v),
        Bound::Unbounded => true,
    }
}

/// Whether `v` is at the upper bound `hi` or before it.
pub(super) open spec fn below_high<T: Delta>(hi: Bound<&T>, v: T) -> bool {
    match hi {
        Bound::Included(b) => !T::below(*b, v),
        Bound::Excluded(b) => T::below(v, *b),
        Bound::Unbounded => true,
    }
}

/// The least number the lower bound `lo` lets in, of those a delta
/// column of `T` keeps; none if it lets none of them in.
pub(super) fn low_of<T: Delta>(lo: Bound<&T>) -> (r: Option<i64>)
    ensures
        forall|n: i64|
            #![trigger T::of(Some(n))]
            in_dom::<T>(Some(n)) ==> above_low::<T>(lo, T::of(Some(n))) == match r {
                Some(x) => x <= n,
                None => false,
            },
{
    let r = match lo {
        Bound::Unbounded => Some(T::lo()),
        Bound::Included(a) => match (*a).try_i64() {
            Some(Some(x)) => Some(x),
            // a null: every number comes after it
            Some(None) => Some(T::lo()),
            // too large for the column: no number it keeps reaches it
            None => None,
        },
        Bound::Excluded(a) => match (*a).try_i64() {
            Some(Some(x)) => if x < i64::MAX {
                Some(x + 1)
            } else {
                None
            },
            Some(None) => Some(T::lo()),
            None => None,
        },
    };
    proof {
        assert forall|n: i64| #![trigger T::of(Some(n))] in_dom::<T>(Some(n)) implies above_low::<T>(
            lo,
            T::of(Some(n)),
        ) == match r {
            Some(x) => x <= n,
            None => false,
        } by {
            match lo {
                Bound::Included(a) => lemma_bound::<T>(*a, Some(n)),
                Bound::Excluded(a) => lemma_bound::<T>(*a, Some(n)),
                Bound::Unbounded => {},
            }
        }
    }
    r
}

/// The greatest number the upper bound `hi` lets in, of those a delta
/// column of `T` keeps; none if it lets none of them in.
pub(super) fn high_of<T: Delta>(hi: Bound<&T>) -> (r: Option<i64>)
    ensures
        forall|n: i64|
            #![trigger T::of(Some(n))]
            in_dom::<T>(Some(n)) ==> below_high::<T>(hi, T::of(Some(n))) == match r {
                Some(x) => n <= x,
                None => false,
            },
{
    let r = match hi {
        Bound::Unbounded => Some(T::hi()),
        Bound::Included(b) => match (*b).try_i64() {
            Some(Some(x)) => Some(x),
            // a null: no number comes before it
            Some(None) => None,
            // too large for the column: every number it keeps is below
            None => Some(T::hi()),
        },
        Bound::Excluded(b) => match (*b).try_i64() {
            Some(Some(x)) => if x > i64::MIN {
                Some(x - 1)
            } else {
                None
            },
            Some(None) => None,
            None => Some(T::hi()),
        },
    };
    proof {
        assert forall|n: i64| #![trigger T::of(Some(n))] in_dom::<T>(Some(n)) implies below_high::<T>(
            hi,
            T::of(Some(n)),
        ) == match r {
            Some(x) => n <= x,
            None => false,
        } by {
            match hi {
                Bound::Included(b) => lemma_bound::<T>(*b, Some(n)),
                Bound::Excluded(b) => lemma_bound::<T>(*b, Some(n)),
                Bound::Unbounded => {},
            }
        }
    }
    r
}

/// The bounds `lo` and `hi` as the least and the greatest of the
/// numbers within them that a delta column of `T` keeps; none if it
/// keeps none within them.
pub(crate) fn bounds_of<T: Delta>(lo: Bound<&T>, hi: Bound<&T>) -> (r: Option<(i64, i64)>)
    ensures
        forall|d: Option<i64>|
            #![trigger T::of(d)]
            in_dom::<T>(d) ==> delta_within::<T>(lo, hi, T::of(d)) == match (r, d) {
                (Some((a, b)), Some(n)) => a <= n <= b,
                _ => false,
            },
{
    let r = match (low_of::<T>(lo), high_of::<T>(hi)) {
        (Some(a), Some(b)) => if a <= b {
            Some((a, b))
        } else {
            None
        },
        _ => None,
    };
    proof {
        assert forall|d: Option<i64>| #![trigger T::of(d)] in_dom::<T>(d) implies delta_within::<T>(lo, hi, T::of(d))
            == match (r, d) {
            (Some((a, b)), Some(n)) => a <= n <= b,
            _ => false,
        } by {
            T::lemma_delta(T::of(d), d);
            if let Some(n) = d {
                assert(T::of(d) == T::of(Some(n)));
            }
        }
    }
    r
}

impl<T: DeltaNum> seal::DeltaValSeal for T {

}

impl<T: DeltaNum> seal::DeltaValSeal for Option<T> {

}

// ── The column underneath, as deltas ────────────────────────────────────────

/// The values of the column underneath a delta column as deltas, for
/// the verified cores: an `i64` is its own delta and so is an
/// `Option<i64>`, a null being no delta. `I` is `i64` or `Option<i64>`.
#[doc(hidden)]
#[verifier::reject_recursive_types(I)]
#[verifier::reject_recursive_types(C)]
pub struct Deltas<I, C>(PhantomData<(I, C)>);

impl<C: Codec> DeltaOps<i64> for Deltas<i64, C> {
    open spec fn dv(v: i64) -> Option<i64> {
        Some(v)
    }

    #[inline(always)]
    fn to_opt(v: i64) -> Option<i64> {
        Some(v)
    }
}

impl<C: Codec> DeltaOps<Option<i64>> for Deltas<Option<i64>, C> {
    open spec fn dv(v: Option<i64>) -> Option<i64> {
        v
    }

    #[inline(always)]
    fn to_opt(v: Option<i64>) -> Option<i64> {
        v
    }
}

impl<'a, C: Codec> DeltaWrite<'a, RleModel<Write<i64, C>>, RleReads<Ops<i64, C>>> for Deltas<i64, C> {
    type Ins = ReadIns<'a, Write<i64, C>>;

    open spec fn dvo(o: i64) -> Option<i64> {
        Some(o)
    }

    proof fn axiom_own(v: i64) {
    }

    /// # Panics
    ///
    /// Panics when asked to write a null: the column has none.
    #[inline(always)]
    fn ins(d: Option<i64>) -> ReadIns<'a, Write<i64, C>> {
        match d {
            Some(v) => ReadIns { v },
            None => {
                null_in_column(Ghost(true));
                ReadIns { v: 0 }
            },
        }
    }
}

impl<'a, C: Codec> DeltaWrite<'a, RleModel<OptWrite<i64, C>>, RleReads<OptOps<i64, C>>> for Deltas<
    Option<i64>,
    C,
> {
    type Ins = ReadIns<'a, OptWrite<i64, C>>;

    open spec fn dvo(o: Option<i64>) -> Option<i64> {
        o
    }

    proof fn axiom_own(v: Option<i64>) {
    }

    #[inline(always)]
    fn ins(d: Option<i64>) -> ReadIns<'a, OptWrite<i64, C>> {
        ReadIns { v: d }
    }
}

// ── Sources that say what they give ─────────────────────────────────────────

/// The values of a slice, for a delta column to be built from
/// (`ColumnCore::fill_delta`): each as the column keeps it, in the
/// slice's order.
pub(crate) struct SliceValues<'v, T: Delta> {
    pub(crate) vs: &'v [T],
    pub(crate) at: usize,
}

impl<'v, T: Delta> SliceValues<'v, T> {
    /// The source is within its slice, whose values all fit.
    #[verifier::type_invariant]
    pub(crate) open spec fn inv(&self) -> bool {
        self.at <= self.vs@.len() && all_fit(self.vs@)
    }

    pub(crate) fn new(vs: &'v [T]) -> (r: Self)
        requires
            all_fit(vs@),
        ensures
            r.left() == Some(dvs(vs@)),
    {
        proof {
            assert(vs@.skip(0) =~= vs@);
        }
        SliceValues { vs, at: 0 }
    }
}

impl<'v, T: Delta> ValueItems for SliceValues<'v, T> {
    open spec fn left(&self) -> Option<Seq<Option<i64>>> {
        Some(dvs(self.vs@.skip(self.at as int)))
    }

    #[inline]
    fn next_value(&mut self) -> (r: Option<Option<i64>>) {
        proof {
            use_type_invariant(&*self);
        }
        if self.at < self.vs.len() {
            let v = self.vs[self.at];
            proof {
                let l = self.vs@.skip(self.at as int);
                assert(l[0] == v);
                assert(dvs(l).skip(1) =~= dvs(self.vs@.skip(self.at + 1)));
            }
            self.at = self.at + 1;
            Some(v.to_i64())
        } else {
            None
        }
    }
}

impl<'v, T: Delta> SpliceValues for SliceValues<'v, T> {
    #[inline(always)]
    fn splice_values<E, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, M, B>(
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        at: usize,
        del: usize,
        f: &mut F,
        feed: &mut ValuesFeed<Self, E, M, B>,
    ) -> (ins: Ghost<Seq<E::V>>) where
        Self: Sized,
        E: Slabs + for<'a> ReadsAgree<'a, M>,
        M: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaWrite<'a, E, M>,
     {
        splice_feed_bounded::<E, W, I, G, F, ValuesFeed<Self, E, M, B>>(slabs, index, total_len, max_segments, counter, at, del, f, feed)
    }
}

impl<'v, T: Delta> BoundedValues for SliceValues<'v, T> {
    proof fn lemma_told(&self) {
    }
}

/// Values `ds` as a splice's items, each an item of its own.
pub(crate) open spec fn value_items(ds: Seq<Option<i64>>) -> Seq<SpliceItem> {
    ds.map_values(|d: Option<i64>| SpliceItem::Value(d))
}

/// Items that are each one value stand for those values.
pub(crate) proof fn lemma_value_items(ds: Seq<Option<i64>>)
    ensures
        items_vals(value_items(ds)) == ds,
    decreases ds.len(),
{
    if ds.len() > 0 {
        lemma_value_items(ds.drop_last());
        assert(value_items(ds).drop_last() =~= value_items(ds.drop_last()));
        assert(ds.drop_last() + seq![ds.last()] =~= ds);
    }
}

/// The values of a slice, for a delta column's splice
/// (`ColumnCore::splice_delta`): each an item, as the column keeps it,
/// in the slice's order.
pub(crate) struct SliceItems<'v, T: Delta> {
    pub(crate) vs: &'v [T],
    pub(crate) at: usize,
}

impl<'v, T: Delta> SliceItems<'v, T> {
    /// The source is within its slice, whose values all fit.
    #[verifier::type_invariant]
    pub(crate) open spec fn inv(&self) -> bool {
        self.at <= self.vs@.len() && all_fit(self.vs@)
    }

    pub(crate) fn new(vs: &'v [T]) -> (r: Self)
        requires
            all_fit(vs@),
        ensures
            r.left() == Some(value_items(dvs(vs@))),
    {
        proof {
            assert(vs@.skip(0) =~= vs@);
        }
        SliceItems { vs, at: 0 }
    }
}

impl<'v, T: Delta> SpliceItems for SliceItems<'v, T> {
    open spec fn left(&self) -> Option<Seq<SpliceItem>> {
        Some(value_items(dvs(self.vs@.skip(self.at as int))))
    }

    #[inline]
    fn next_item(&mut self) -> (r: Option<SpliceItem>) {
        proof {
            use_type_invariant(&*self);
        }
        if self.at < self.vs.len() {
            let v = self.vs[self.at];
            proof {
                let l = self.vs@.skip(self.at as int);
                assert(l[0] == v);
                assert(value_items(dvs(l)).skip(1) =~= value_items(dvs(self.vs@.skip(self.at + 1))));
            }
            self.at = self.at + 1;
            Some(SpliceItem::Value(v.to_i64()))
        } else {
            None
        }
    }
}

impl<'v, T: Delta> SpliceDeltas for SliceItems<'v, T> {
    #[inline(always)]
    fn splice_deltas<E, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, M, B>(
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        at: usize,
        del: usize,
        f: &mut F,
        feed: &mut DeltaFeed<Self, E, M, B>,
    ) -> (ins: Ghost<Seq<E::V>>) where
        Self: Sized,
        E: Slabs + for<'a> ReadsAgree<'a, M>,
        M: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaWrite<'a, E, M>,
     {
        splice_feed_bounded::<E, W, I, G, F, DeltaFeed<Self, E, M, B>>(slabs, index, total_len, max_segments, counter, at, del, f, feed)
    }
}

impl<'v, T: Delta> BoundedItems for SliceItems<'v, T> {
    proof fn lemma_told(&self) {
    }
}

pub(super) proof fn lemma_delta_run_exact_impl(r: DeltaRun, k: int)
    requires
        0 <= k < r.count,
        r.delta matches Some(d) && i64::MIN <= r.prefix + d <= i64::MAX && i64::MIN <= r.prefix + d * r.count
            <= i64::MAX,
    ensures
        i64::MIN <= r.prefix + (k + 1) * r.delta->0 <= i64::MAX,
        delta_run_kept(r)[k] == Some((r.prefix + (k + 1) * r.delta->0) as i64),
        r.delta->0 >= 0 ==> r.prefix + r.delta->0 <= r.prefix + (k + 1) * r.delta->0 <= r.prefix + r.delta->0
            * r.count,
        r.delta->0 <= 0 ==> r.prefix + r.delta->0 * r.count <= r.prefix + (k + 1) * r.delta->0 <= r.prefix
            + r.delta->0,
{
    let d = r.delta->0 as int;
    let n = r.count as int;
    let x = r.prefix + (k + 1) * d;
    if d >= 0 {
        assert(d <= (k + 1) * d <= d * n) by (nonlinear_arith)
            requires
                0 <= k < n,
                d >= 0,
        ;
    }
    if d <= 0 {
        assert(d * n <= (k + 1) * d <= d) by (nonlinear_arith)
            requires
                0 <= k < n,
                d <= 0,
        ;
    }
    lemma_crop(x);
    assert(crop(x) == x);
}

pub(super) proof fn lemma_delta_run_fits_impl<T: Delta>(r: DeltaRun)
    requires
        delta_run_in_range::<T>(r),
    ensures
        delta_run_fits::<T>(r),
{
    let ks = delta_run_kept(r);
    assert forall|k: int| 0 <= k < ks.len() implies in_dom::<T>(#[trigger] ks[k]) by {
        match r.delta {
            Some(d) => {
                if !(T::least() == i64::MIN && T::most() == i64::MAX) {
                    lemma_delta_run_exact(r, k);
                }
            },
            None => {},
        }
    }
}

pub(super) proof fn lemma_delta_runs_push_impl<T: Delta>(rs: Seq<DeltaRun>, r: DeltaRun)
    ensures
        delta_runs_kept(rs.push(r)) == delta_runs_kept(rs) + delta_run_kept(r),
        delta_runs_values::<T>(rs.push(r)) == delta_runs_values::<T>(rs) + delta_run_values::<T>(r),
        delta_runs_fit::<T>(rs) && delta_run_fits::<T>(r) ==> delta_runs_fit::<T>(rs.push(r)),
{
    assert(rs.push(r).drop_last() =~= rs);
    assert(vals::<T>(delta_runs_kept(rs) + delta_run_kept(r)) =~= vals::<T>(delta_runs_kept(rs)) + vals::<T>(
        delta_run_kept(r),
    ));
}

pub(super) proof fn lemma_delta_runs_one_impl<T: Delta>(r: DeltaRun)
    ensures
        delta_runs_values::<T>(seq![r]) == delta_run_values::<T>(r),
        delta_runs_values::<T>(Seq::empty()) == Seq::<T>::empty(),
        delta_runs_fit::<T>(seq![r]) == delta_run_fits::<T>(r),
        delta_runs_fit::<T>(Seq::empty()),
{
    let none = Seq::<DeltaRun>::empty();
    assert(seq![r] =~= none.push(r));
    lemma_delta_runs_push::<T>(none, r);
    assert(delta_runs_kept(none) + delta_run_kept(r) =~= delta_run_kept(r));
    assert(vals::<T>(delta_runs_kept(none)) =~= Seq::<T>::empty());
    assert(seq![r][0] == r);
}

/// Runs that each hold only values a delta column of `T` can keep hold
/// only such values together.
pub(crate) proof fn lemma_delta_runs_dom<T: Delta>(rs: Seq<DeltaRun>)
    requires
        delta_runs_fit::<T>(rs),
    ensures
        all_dom::<T>(delta_runs_kept(rs)),
    decreases rs.len(),
{
    if rs.len() > 0 {
        assert forall|i: int| 0 <= i < rs.drop_last().len() implies delta_run_fits::<T>(#[trigger] rs.drop_last()[i]) by {
            assert(rs.drop_last()[i] == rs[i]);
        }
        lemma_delta_runs_dom::<T>(rs.drop_last());
        let a = delta_runs_kept(rs.drop_last());
        let b = delta_run_kept(rs.last());
        assert(delta_run_fits::<T>(rs[rs.len() - 1]));
        assert forall|i: int| 0 <= i < (a + b).len() implies in_dom::<T>(#[trigger] (a + b)[i]) by {
            if i < a.len() {
                assert((a + b)[i] == a[i]);
            } else {
                assert((a + b)[i] == b[i - a.len()]);
            }
        }
    }
}

/// Runs of no values hold none together.
pub(crate) proof fn lemma_delta_runs_none(rs: Seq<DeltaRun>)
    requires
        forall|i: int| 0 <= i < rs.len() ==> (#[trigger] rs[i]).count == 0,
    ensures
        delta_runs_kept(rs) == Seq::<Option<i64>>::empty(),
    decreases rs.len(),
{
    if rs.len() > 0 {
        assert forall|i: int| 0 <= i < rs.drop_last().len() implies (#[trigger] rs.drop_last()[i]).count == 0 by {
            assert(rs.drop_last()[i] == rs[i]);
        }
        lemma_delta_runs_none(rs.drop_last());
        assert(rs[rs.len() - 1].count == 0);
        assert(delta_run_kept(rs.last()) =~= Seq::<Option<i64>>::empty());
        assert(delta_runs_kept(rs) =~= Seq::<Option<i64>>::empty());
    }
}

/// A run as a splice's item: none, where it holds no value.
pub(crate) open spec fn run_item(r: DeltaRun) -> Seq<SpliceItem> {
    if r.count == 0 {
        Seq::empty()
    } else {
        seq![SpliceItem::Run(r.prefix, r.delta, r.count)]
    }
}

/// Runs `rs` as a splice's items: each that holds a value an item, in
/// their order.
pub(crate) open spec fn run_items(rs: Seq<DeltaRun>) -> Seq<SpliceItem>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        run_items(rs.drop_last()) + run_item(rs.last())
    }
}

/// The items of runs are the first run's and then the others'.
pub(crate) proof fn lemma_run_items_front(s: Seq<DeltaRun>)
    requires
        s.len() > 0,
    ensures
        run_items(s) == run_item(s[0]) + run_items(s.skip(1)),
    decreases s.len(),
{
    if s.len() == 1 {
        assert(run_items(s.drop_last()) =~= Seq::<SpliceItem>::empty());
        assert(run_items(s.skip(1)) =~= Seq::<SpliceItem>::empty());
        assert(run_items(s) =~= run_item(s[0]) + run_items(s.skip(1)));
    } else {
        lemma_run_items_front(s.drop_last());
        assert(s.skip(1).drop_last() =~= s.drop_last().skip(1));
        assert(s.skip(1).last() == s.last());
        assert(run_items(s) =~= run_item(s[0]) + run_items(s.skip(1)));
    }
}

/// Items and then more stand for the values of each.
pub(crate) proof fn lemma_items_cat(a: Seq<SpliceItem>, b: Seq<SpliceItem>)
    ensures
        items_vals(a + b) == items_vals(a) + items_vals(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(items_vals(a) + items_vals(b) =~= items_vals(a));
    } else {
        lemma_items_cat(a, b.drop_last());
        assert((a + b).drop_last() =~= a + b.drop_last());
        assert((a + b).last() == b.last());
        assert(items_vals(a + b) =~= items_vals(a) + items_vals(b));
    }
}

/// A run's item stands for what a delta column keeps for the run.
pub(crate) proof fn lemma_run_item(r: DeltaRun)
    ensures
        items_vals(run_item(r)) == delta_run_kept(r),
{
    if r.count == 0 {
        assert(delta_run_kept(r) =~= Seq::<Option<i64>>::empty());
        assert(items_vals(run_item(r)) =~= Seq::<Option<i64>>::empty());
    } else {
        let it = SpliceItem::Run(r.prefix, r.delta, r.count);
        assert(items_vals(seq![it].drop_last()) =~= Seq::<Option<i64>>::empty());
        assert(item_vals(it) =~= delta_run_kept(r));
        assert(items_vals(seq![it]) =~= delta_run_kept(r));
    }
}

/// And so do the items of runs, for the runs.
pub(crate) proof fn lemma_run_items(rs: Seq<DeltaRun>)
    ensures
        items_vals(run_items(rs)) == delta_runs_kept(rs),
    decreases rs.len(),
{
    if rs.len() == 0 {
        assert(items_vals(run_items(rs)) =~= Seq::<Option<i64>>::empty());
    } else {
        lemma_run_items(rs.drop_last());
        lemma_run_item(rs.last());
        lemma_items_cat(run_items(rs.drop_last()), run_item(rs.last()));
    }
}

/// The runs of a slice, for a delta column's splice
/// (`ColumnCore::splice_delta`): each that holds a value an item, in
/// the slice's order.
pub(crate) struct RunItems<'v> {
    pub(crate) rs: &'v [DeltaRun],
    pub(crate) at: usize,
}

impl<'v> RunItems<'v> {
    /// The source is within its slice.
    #[verifier::type_invariant]
    pub(crate) open spec fn inv(&self) -> bool {
        self.at <= self.rs@.len()
    }

    pub(crate) fn new(rs: &'v [DeltaRun]) -> (r: Self)
        ensures
            r.left() == Some(run_items(rs@)),
    {
        proof {
            assert(rs@.skip(0) =~= rs@);
        }
        RunItems { rs, at: 0 }
    }
}

impl<'v> SpliceItems for RunItems<'v> {
    open spec fn left(&self) -> Option<Seq<SpliceItem>> {
        Some(run_items(self.rs@.skip(self.at as int)))
    }

    #[inline]
    fn next_item(&mut self) -> (r: Option<SpliceItem>) {
        proof {
            use_type_invariant(&*self);
        }
        let ghost l0 = run_items(self.rs@.skip(self.at as int));
        // a run of no values is no item
        while self.at < self.rs.len()
            invariant
                self.at <= self.rs@.len(),
                old(self).left() == Some(l0),
                self.left() == Some(l0),
            decreases self.rs@.len() - self.at,
        {
            let run = &self.rs[self.at];
            let (prefix, delta, count) = (run.prefix, run.delta, run.count);
            proof {
                let s = self.rs@.skip(self.at as int);
                lemma_run_items_front(s);
                assert(s[0] == self.rs@[self.at as int]);
                assert(s.skip(1) =~= self.rs@.skip(self.at + 1));
            }
            self.at = self.at + 1;
            if count > 0 {
                let item = SpliceItem::Run(prefix, delta, count);
                proof {
                    assert(l0[0] == item);
                    assert(l0.skip(1) =~= run_items(self.rs@.skip(self.at as int)));
                }
                return Some(item);
            }
            proof {
                assert(l0 =~= run_items(self.rs@.skip(self.at as int)));
            }
        }
        proof {
            assert(self.rs@.skip(self.at as int).len() == 0);
        }
        None
    }
}

impl<'v> SpliceDeltas for RunItems<'v> {
    #[inline(always)]
    fn splice_deltas<E, W, I: IndexOps<W>, G: Weigh<E, W>, F: Visitor<E>, M, B>(
        slabs: &mut Vec<Slab<E::Tail>>,
        index: &mut I,
        total_len: &mut usize,
        max_segments: usize,
        counter: &mut usize,
        at: usize,
        del: usize,
        f: &mut F,
        feed: &mut DeltaFeed<Self, E, M, B>,
    ) -> (ins: Ghost<Seq<E::V>>) where
        Self: Sized,
        E: Slabs + for<'a> ReadsAgree<'a, M>,
        M: for<'a> Decoders<'a, Tail = E::Tail>,
        B: for<'a> DeltaWrite<'a, E, M>,
     {
        splice_feed_bounded::<E, W, I, G, F, DeltaFeed<Self, E, M, B>>(slabs, index, total_len, max_segments, counter, at, del, f, feed)
    }
}

impl<'v> BoundedItems for RunItems<'v> {
    proof fn lemma_told(&self) {
    }
}

// ── The column ──────────────────────────────────────────────────────────────

/// How a delta column of the type is kept: the column underneath, of
/// `i64`s or of `Option<i64>`s, and how its values are deltas.
pub(crate) trait DKind<C: Codec>: Delta {
    /// What the column underneath holds.
    type I: ColumnValueRef<C>;

    /// Its values as deltas.
    type B: for <'a>DeltaWrite<'a, <Self::I as seal::Sealed<C>>::E, <Self::I as seal::Sealed<C>>::X>;

    /// The column underneath by the names its edit cursor's core has
    /// for it: how it writes its values (`L`), how it reads them (`O`)
    /// and its values as deltas (`W`). They name what `I` and `B` do:
    /// `to_edit` and `of_edit` say so.
    type L: for <'t>Lift<'t, W = Self::L> + for <'t>WriteOps<'t, R = Self::O>;

    type O: for <'b>RleOps<'b>;

    type W: for <'a>DeltaWrite<'a, RleModel<Self::L>, RleReads<Self::O>>;

    /// A column's core as its edit cursor's core takes it: the same
    /// core, of the same values.
    fn to_edit(core: DCore<Self, C>) -> (r: ECore<Self, C>)
        ensures
            rvals::<RleModel<Self::L>, RleReads<Self::O>, Self::W>(r@) == rvals::<DE<Self, C>, DX<Self, C>, Self::B>(
                core@,
            ),
    ;

    /// And the core the cursor's gives back, as the column's.
    fn of_edit(core: ECore<Self, C>) -> (r: DCore<Self, C>)
        ensures
            rvals::<DE<Self, C>, DX<Self, C>, Self::B>(r@) == rvals::<RleModel<Self::L>, RleReads<Self::O>, Self::W>(
                core@,
            ),
    ;

    /// Two values of the column underneath are two deltas, and none of
    /// them is a null where the type has none.
    proof fn lemma_kind(a: <DE<Self, C> as Slabs>::V, b: <DE<Self, C> as Slabs>::V)
        ensures
            <Self::B as DeltaWrite<'static, DE<Self, C>, DX<Self, C>>>::dvo(a) == <Self::B as DeltaWrite<
                'static,
                DE<Self, C>,
                DX<Self, C>,
            >>::dvo(b) ==> a == b,
            !Self::nullable() ==> <Self::B as DeltaWrite<'static, DE<Self, C>, DX<Self, C>>>::dvo(a) is Some,
    ;

    /// Two values of the column underneath, as they are read, are two
    /// deltas.
    proof fn lemma_item<'a>(a: DIt<'a, Self, C>, b: DIt<'a, Self, C>)
        ensures
            <Self::B as DeltaOps<DIt<'a, Self, C>>>::dv(a) == <Self::B as DeltaOps<DIt<'a, Self, C>>>::dv(b) ==> a
                == b,
    ;

    /// A fill as a value of the column underneath: the one whose delta
    /// it is.
    fn fill_item(f: <Self as Delta>::Fill) -> (r: DItem<Self, C>)
        ensures
            <Self::B as DeltaOps<DItem<Self, C>>>::dv(r) == Self::fill_delta(f),
    ;

    /// A value of the column underneath, as a specification has it, as
    /// the delta it is.
    spec fn d_of(s: <Self::I as VSpec>::S) -> Option<i64>;

    /// That is the delta of the value as the column holds it.
    proof fn lemma_d_of(o: <DE<Self, C> as Slabs>::V)
        ensures
            <Self::B as DeltaWrite<'static, DE<Self, C>, DX<Self, C>>>::dvo(o) == Self::d_of(
                <<DE<Self, C> as Slabs>::V as VGet>::sv(o),
            ),
    ;

    /// A delta as a value of the column underneath.
    fn item(d: Option<i64>) -> (r: Self::I)
        requires
            !Self::nullable() ==> d is Some,
        ensures
            Self::d_of(<Self::I as VSpec>::sv_of(r)) == d,
    ;
}

impl<T: DeltaNum, C: Codec> DKind<C> for T {
    type I = i64;

    type B = Deltas<i64, C>;

    type L = Write<i64, C>;

    type O = Ops<i64, C>;

    type W = Deltas<i64, C>;

    #[inline(always)]
    fn to_edit(core: DCore<T, C>) -> ECore<T, C> {
        core
    }

    #[inline(always)]
    fn of_edit(core: ECore<T, C>) -> DCore<T, C> {
        core
    }

    proof fn lemma_kind(a: i64, b: i64) {
    }

    proof fn lemma_item<'a>(a: i64, b: i64) {
    }

    #[inline(always)]
    fn fill_item(f: i64) -> i64 {
        f
    }

    open spec fn d_of(s: i64) -> Option<i64> {
        Some(s)
    }

    proof fn lemma_d_of(o: i64) {
    }

    #[inline(always)]
    #[allow(clippy::manual_unwrap_or_default, clippy::manual_unwrap_or)]
    fn item(d: Option<i64>) -> i64 {
        match d {
            Some(n) => n,
            None => 0,
        }
    }
}

impl<T: DeltaNum, C: Codec> DKind<C> for Option<T> {
    type I = Option<i64>;

    type B = Deltas<Option<i64>, C>;

    type L = OptWrite<i64, C>;

    type O = OptOps<i64, C>;

    type W = Deltas<Option<i64>, C>;

    #[inline(always)]
    fn to_edit(core: DCore<Option<T>, C>) -> ECore<Option<T>, C> {
        core
    }

    #[inline(always)]
    fn of_edit(core: ECore<Option<T>, C>) -> DCore<Option<T>, C> {
        core
    }

    proof fn lemma_kind(a: Option<i64>, b: Option<i64>) {
    }

    proof fn lemma_item<'a>(a: Option<i64>, b: Option<i64>) {
    }

    #[inline(always)]
    fn fill_item(f: Option<i64>) -> Option<i64> {
        f
    }

    open spec fn d_of(s: Option<i64>) -> Option<i64> {
        s
    }

    proof fn lemma_d_of(o: Option<i64>) {
    }

    #[inline(always)]
    fn item(d: Option<i64>) -> Option<i64> {
        d
    }
}

/// The slabs of the column underneath.
pub(super) type DE<T, C> = <<T as DKind<C>>::I as seal::Sealed<C>>::E;

/// How the column underneath is read.
pub(super) type DX<T, C> = <<T as DKind<C>>::I as seal::Sealed<C>>::X;

/// A value of the column underneath, as it is read.
pub(super) type DItem<T, C> = <<DX<T, C> as Decoders<'static>>::D as Decode>::Item;

/// A value of the column underneath, as a reader that borrows the
/// column for `'a` gives it.
pub(super) type DIt<'a, T, C> = <<DX<T, C> as Decoders<'a>>::D as Decode>::Item;

/// The verified core of a delta column.
pub(super) type DCore<T, C> = ColumnCore<DE<T, C>, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<DX<T, C>, <T as DKind<C>>::B>>;

/// The same core, as the edit cursor's core takes it (`DKind::to_edit`).
pub(super) type ECore<T, C> = DCol<<T as DKind<C>>::L, <T as DKind<C>>::O, <T as DKind<C>>::W>;

/// Values that fit are kept as what a column can keep, and are the
/// values of what they are kept as.
pub(crate) proof fn lemma_kept<T: Delta>(vs: Seq<T>)
    requires
        all_fit(vs),
    ensures
        all_dom::<T>(dvs(vs)),
        vals::<T>(dvs(vs)) == vs,
{
    assert forall|i: int| 0 <= i < vs.len() implies in_dom::<T>(#[trigger] dvs(vs)[i]) && T::of(dvs(vs)[i]) == vs[i] by {
        T::lemma_delta(vs[i], None);
    }
    assert(vals::<T>(dvs(vs)) =~= vs);
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    pub(super) fn extend_impl<I: IntoIterator<Item = T>>(&mut self, iter: I)
        ensures
            gives(iter, |xs: Seq<T>| all_fit(xs) && final(self)@ == old(self)@ + xs),
            final(self).edits() == after_call(old(self).edits(), final(self)@.len() != old(self)@.len()),
    {
        let len = self.len();
        self.splice(len, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> all_fit(it.remaining())
                    && self@ == old(self)@.take(len as int) + it.remaining() + old(self)@.skip(len + 0));
            assert(old(self)@.take(len as int) + it.remaining() + old(self)@.skip(len + 0) =~= old(self)@ + it.remaining());
        }
    }

    pub(super) fn from_iter_impl<I: IntoIterator<Item = T>>(iter: I) -> (s: Self)
        ensures
            // what Verus asks of every `FromIterator`
            exists|initial: I::IntoIter| #[trigger] (I::into_iter).ensures((iter,), initial) && (initial.obeys_prophetic_iter_laws() ==> all_fit(initial.remaining()) && s@ == initial.remaining()),
    {
        let mut s = Self::new();
        s.splice(0, 0, iter);
        proof {
            let it = choose|it: I::IntoIter| #[trigger]
                (I::into_iter).ensures((iter,), it) && (it.obeys_prophetic_iter_laws() ==> all_fit(it.remaining())
                    && s@ == Seq::<T>::empty().take(0) + it.remaining() + Seq::<T>::empty().skip(0));
            assert(Seq::<T>::empty().take(0) + it.remaining() + Seq::<T>::empty().skip(0) =~= it.remaining());
        }
        s
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    /// Every value kept is one of the type's.
    #[verifier::type_invariant]
    pub(super) open spec fn inv(&self) -> bool {
        all_dom::<T>(self.kept())
    }

    pub(super) fn new_impl() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::with_max_segments(MAX_SEG)
    }

    pub(super) fn with_max_segments_impl(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<T>::empty(),
    {
        let core: DCore<T, C> = ColumnCore::with_budget(max_segments);
        proof {
            assert(rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@) =~= Seq::empty());
            assert(vals::<T>(Seq::empty()) =~= Seq::<T>::empty());
        }
        DeltaColumn { core }
    }

    pub(super) fn from_values_impl(values: &[T]) -> (r: Self)
        requires
            all_fit(values@),
        ensures
            r@ == values@,
    {
        let mut core: DCore<T, C> = ColumnCore::with_budget(MAX_SEG);
        let mut f = <DE<T, C> as Model<DX<T, C>>>::blind();
        let _taken = core.fill_delta(SliceValues::new(values), T::lo(), T::hi(), &mut f);
        proof {
            lemma_kept(values@);
        }
        DeltaColumn { core }
    }

    pub(super) fn len_impl(&self) -> (n: usize)
        ensures
            n == self@.len(),
    {
        self.core.len()
    }

    pub(super) fn is_empty_impl(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.core.len() == 0
    }

    pub(super) proof fn lemma_fits_impl(tracked &self)
        ensures
            all_fit(self@),
    {
        use_type_invariant(self);
        assert forall|i: int| 0 <= i < self@.len() implies (#[trigger] self@[i]).fits() by {
            T::lemma_delta(T::of(None), self.kept()[i]);
        }
    }

    pub(super) fn splice_slice_impl(&mut self, at: usize, del: usize, values: &[T])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            all_fit(values@),
        ensures
            final(self)@ == old(self)@.take(at as int) + values@ + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || values@.len() > 0),
    {
        proof {
            use_type_invariant(&*self);
        }
        if del == 0 && values.len() == 0 {
            // nothing to do: the column is left as it is, as by the old
            // API
            proof {
                assert(self@ =~= self@.take(at as int) + values@ + self@.skip(at as int));
            }
            return;
        }
        let ghost k0 = self.kept();
        // the column's values are kept in bounds while it is whole: a
        // panic part way leaves an empty one
        let mut me = Self::new();
        core::mem::swap(self, &mut me);
        let mut core = me.core;
        let before = core.counter();
        let mut f = <DE<T, C> as Model<DX<T, C>>>::blind();
        let _taken = core.splice_delta(at, del, Some(SliceItems::new(values)), T::lo(), T::hi(), &mut f);
        // the shared cursor moves the count as it writes, by a number
        // that depends on the edit: this makes it one an edit
        core.set_counter(
            if del > 0 || values.len() > 0 {
                before.wrapping_add(1)
            } else {
                before
            },
        );
        proof {
            let ds = dvs(values@);
            lemma_value_items(ds);
            lemma_kept(values@);
            let k1 = rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@);
            assert(k1 == k0.take(at as int) + ds + k0.skip(at + del));
            assert(vals::<T>(k1) =~= vals::<T>(k0).take(at as int) + values@ + vals::<T>(k0).skip(at + del));
        }
        *self = DeltaColumn { core };
    }

    #[verifier::exec_allows_no_decreases_clause]
    pub(super) fn splice_impl<I: IntoIterator<Item = T>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(values, |xs: Seq<T>| all_fit(xs) && final(self)@ == old(self)@.take(at as int) + xs + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        // the values first, each checked, so that a panic leaves the
        // column as it was
        let mut it = values.into_iter();
        let ghost it0 = it;
        let mut vals: Vec<T> = Vec::new();
        // a Verus loop with an invariant: `loop` + `match`
        #[allow(clippy::while_let_loop)]
        loop
            invariant_except_break
                it.obeys_prophetic_iter_laws() ==> vals@ + it.remaining() == it0.remaining(),
            invariant
                it.obeys_prophetic_iter_laws() == it0.obeys_prophetic_iter_laws(),
                all_fit(vals@),
            ensures
                it0.obeys_prophetic_iter_laws() ==> vals@ == it0.remaining(),
        {
            let ghost before = vals@;
            let ghost rem = it.remaining();
            match it.next() {
                Some(v) => {
                    if v.try_i64().is_none() {
                        crate::trusted::fail("a value a delta column cannot keep", Ghost(!v.fits()));
                    }
                    vals.push(v);
                    proof {
                        assert(vals@ == before.push(v));
                        if it.obeys_prophetic_iter_laws() {
                            assert(vals@ + it.remaining() =~= before + rem);
                        }
                    }
                },
                None => {
                    proof {
                        if it.obeys_prophetic_iter_laws() {
                            assert(vals@ =~= before + rem);
                        }
                    }
                    break ;
                },
            }
        }
        self.splice_slice_impl(at, del, vals.as_slice());
    }

    pub(super) fn run_fits_impl(run: &DeltaRun) -> (r: bool)
        ensures
            r == delta_run_in_range::<T>(*run),
            r ==> delta_run_fits::<T>(*run),
    {
        proof {
            if delta_run_in_range::<T>(*run) {
                lemma_delta_run_fits::<T>(*run);
            }
        }
        if run.count == 0 {
            return true;
        }
        match run.delta {
            None => T::has_null(),
            Some(d) => {
                let (lo, hi) = (T::lo(), T::hi());
                if lo == i64::MIN && hi == i64::MAX {
                    return true;
                }
                // the run's ends, as integers: an `i128` holds them
                let n = run.count as i128;
                proof {
                    let m = 0xffff_ffff_ffff_ffffint;
                    assert(-0x8000_0000_0000_0000 * m <= (d as int) * (n as int) <= 0x7fff_ffff_ffff_ffff * m)
                        by (nonlinear_arith)
                        requires
                            -0x8000_0000_0000_0000 <= d as int <= 0x7fff_ffff_ffff_ffff,
                            0 <= n as int <= m,
                            m == 0xffff_ffff_ffff_ffff,
                    ;
                }
                let first = run.prefix as i128 + d as i128;
                let last = run.prefix as i128 + d as i128 * n;
                lo as i128 <= first && first <= hi as i128 && lo as i128 <= last && last <= hi as i128
            },
        }
    }

    pub(super) fn splice_runs_impl(&mut self, at: usize, del: usize, runs: &[DeltaRun])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            delta_runs_fit::<T>(runs@),
        ensures
            final(self)@ == old(self)@.take(at as int) + delta_runs_values::<T>(runs@) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        proof {
            use_type_invariant(&*self);
        }
        if del == 0 {
            // runs of no values, and none deleted: the column is left as
            // it is
            let mut i: usize = 0;
            while i < runs.len() && runs[i].count == 0
                invariant
                    i <= runs@.len(),
                    forall|j: int| 0 <= j < i ==> (#[trigger] runs@[j]).count == 0,
                decreases runs@.len() - i,
            {
                i = i + 1;
            }
            if i == runs.len() {
                proof {
                    lemma_delta_runs_none(runs@);
                    assert(delta_runs_values::<T>(runs@) =~= Seq::<T>::empty());
                    assert(self@ =~= self@.take(at as int) + Seq::<T>::empty() + self@.skip(at as int));
                }
                return;
            }
        }
        let ghost k0 = self.kept();
        let mut me = Self::new();
        core::mem::swap(self, &mut me);
        let mut core = me.core;
        let before = core.counter();
        let len = core.len();
        let mut f = <DE<T, C> as Model<DX<T, C>>>::blind();
        let _taken = core.splice_delta(at, del, Some(RunItems::new(runs)), T::lo(), T::hi(), &mut f);
        core.set_counter(
            if del > 0 || core.len() != len {
                before.wrapping_add(1)
            } else {
                before
            },
        );
        proof {
            let ds = delta_runs_kept(runs@);
            lemma_run_items(runs@);
            lemma_delta_runs_dom::<T>(runs@);
            let k1 = rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@);
            assert(k1 == k0.take(at as int) + ds + k0.skip(at + del));
            assert(vals::<T>(k1) =~= vals::<T>(k0).take(at as int) + vals::<T>(ds) + vals::<T>(k0).skip(at + del));
        }
        *self = DeltaColumn { core };
    }

    pub(super) fn get_impl(&self, index: usize) -> (r: Option<T>)
        ensures
            index < self@.len() ==> r == Some(self@[index as int]),
            index >= self@.len() ==> r is None,
    {
        proof {
            use_type_invariant(self);
        }
        if index >= self.core.len() {
            return None;
        }
        let ghost kept = self.kept();
        let mut src = self.core.dsrc();
        assert(src.src() == kept);
        src.shift(index..index + 1);
        match src.next_run() {
            Some((prefix, d, _n)) => {
                let ghost run = item_vals(SpliceItem::Run(prefix, d, _n));
                proof {
                    assert(_n == 1);
                    assert(kept.subrange(index as int, index + 1) == run);
                    assert(kept.subrange(index as int, index + 1)[0] == kept[index as int]);
                }
                let got = match d {
                    Some(x) => {
                        let sum = prefix.wrapping_add(x);
                        proof {
                            lemma_wrapping_add(prefix, x);
                            assert((0 + 1) * x == x) by (nonlinear_arith);
                            assert(run[0] == Some(crop(prefix + (0 + 1) * x)));
                            assert(run[0] == Some(sum));
                        }
                        Some(sum)
                    },
                    None => {
                        proof {
                            assert(run[0] == None::<i64>);
                        }
                        None
                    },
                };
                proof {
                    assert(got == kept[index as int]);
                    assert(in_dom::<T>(kept[index as int]));
                }
                Some(T::from_i64(got))
            },
            None => None,
        }
    }

    pub(super) fn slab_count_impl(&self) -> usize
    {
        self.core.slabs().len()
    }

    /// Delete `del` values at `at`: a splice that puts nothing in their
    /// place.
    pub(super) fn delete(&mut self, at: usize, del: usize)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0),
    {
        let none: Vec<T> = Vec::new();
        self.splice_slice(at, del, none.as_slice());
        proof {
            let (a, b) = (old(self)@.take(at as int), old(self)@.skip(at + del));
            assert(a + none@ + b =~= a + b);
        }
    }

    pub(super) fn insert_impl(&mut self, at: usize, value: T)
        requires
            at <= old(self)@.len(),
            value.fits(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value),
            final(self).edits() == after_edit(old(self).edits()),
    {
        let one = [value];
        proof {
            assert(one@ =~= seq![value]);
        }
        self.splice_slice(at, 0, one.as_slice());
        proof {
            assert(self@ =~= old(self)@.insert(at as int, value));
        }
    }

    pub(super) fn push_impl(&mut self, value: T)
        requires
            value.fits(),
        ensures
            final(self)@ == old(self)@.push(value),
            final(self).edits() == after_edit(old(self).edits()),
    {
        let at = self.len();
        self.insert(at, value);
        proof {
            assert(self@ =~= old(self)@.push(value));
        }
    }

    pub(super) fn remove_impl(&mut self, at: usize)
        ensures
            final(self)@ == if at < old(self)@.len() {
                old(self)@.remove(at as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), at < old(self)@.len()),
    {
        if at < self.len() {
            self.delete(at, 1);
            proof {
                assert(self@ =~= old(self)@.remove(at as int));
            }
        }
    }

    pub(super) fn remove_n_impl(&mut self, at: usize, n: usize)
        requires
            n > 0 ==> at <= old(self)@.len() && n <= old(self)@.len() - at,
        ensures
            final(self)@ == if n > 0 {
                old(self)@.take(at as int) + old(self)@.skip(at + n)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), n > 0),
    {
        if n > 0 {
            self.delete(at, n);
        }
    }

    pub(super) fn truncate_impl(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), len < old(self)@.len()),
    {
        let n = self.len();
        if len < n {
            self.delete(len, n - len);
            proof {
                assert(self@ =~= old(self)@.take(len as int));
            }
        }
    }

    pub(super) fn clear_impl(&mut self)
        ensures
            final(self)@ == Seq::<T>::empty(),
            final(self).edits() == after_call(old(self).edits(), old(self)@.len() > 0),
    {
        if self.core.len() == 0 {
            proof {
                assert(self@ =~= Seq::<T>::empty());
            }
            return;
        }
        // a new column, and not a delete of every value, as the old
        // API's. The edit counter goes on from where it is, as after any
        // other edit.
        self.core.lemma_column();
        let mut core: DCore<T, C> = ColumnCore::with_budget(self.core.max_segments());
        core.set_counter(self.core.counter().wrapping_add(1));
        proof {
            assert(rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@) =~= Seq::empty());
            assert(vals::<T>(Seq::empty()) =~= Seq::<T>::empty());
        }
        *self = DeltaColumn { core };
    }

    pub(super) fn pop_impl(&mut self) -> (r: Option<T>)
        ensures
            old(self)@.len() == 0 ==> {
                &&& r is None
                &&& final(self)@ == old(self)@
                &&& final(self).edits() == old(self).edits()
            },
            old(self)@.len() > 0 ==> {
                &&& r == Some(old(self)@.last())
                &&& final(self)@ == old(self)@.drop_last()
                &&& final(self).edits() == after_edit(old(self).edits())
            },
    {
        let n = self.len();
        if n == 0 {
            return None;
        }
        let v = self.get(n - 1);
        self.delete(n - 1, 1);
        proof {
            assert(self@ =~= old(self)@.drop_last());
        }
        v
    }

    pub(super) fn first_impl(&self) -> (r: Option<T>)
        ensures
            self@.len() > 0 ==> r == Some(self@.first()),
            self@.len() == 0 ==> r is None,
    {
        self.get(0)
    }

    pub(super) fn last_impl(&self) -> (r: Option<T>)
        ensures
            self@.len() > 0 ==> r == Some(self@.last()),
            self@.len() == 0 ==> r is None,
    {
        let n = self.len();
        if n == 0 {
            None
        } else {
            self.get(n - 1)
        }
    }

    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    pub(super) fn to_vec_impl(&self) -> (r: Vec<T>)
        ensures
            r@ == self@,
    {
        proof {
            use_type_invariant(self);
        }
        let len = self.core.len();
        let ghost kept = self.kept();
        let mut out: Vec<T> = Vec::with_capacity(len);
        let mut src = self.core.dsrc();
        src.shift(0..len);
        proof {
            assert(kept.take(0) =~= Seq::<Option<i64>>::empty());
            assert(vals::<T>(Seq::empty()) =~= Seq::<T>::empty());
        }
        loop
            invariant
                src.inv(),
                src.src() == kept,
                src.end() == len,
                kept.len() == len,
                0 <= src.at() <= len,
                all_dom::<T>(kept),
                out@ == vals::<T>(kept.take(src.at())),
            ensures
                src.at() == len,
            decreases len - src.at(),
        {
            let ghost at0 = src.at();
            match src.next_run() {
                Some((prefix, d, n)) => {
                    // the run's values: each the one before it and the
                    // run's delta, or each a null
                    let ghost run = item_vals(SpliceItem::Run(prefix, d, n));
                    let mut cur = prefix;
                    let mut k: usize = 0;
                    while k < n
                        invariant
                            k <= n,
                            0 <= at0,
                            at0 + n <= len,
                            kept.len() == len,
                            all_dom::<T>(kept),
                            kept.subrange(at0, at0 + n) == run,
                            run == item_vals(SpliceItem::Run(prefix, d, n)),
                            d matches Some(x) ==> cur == crop(prefix + k * x),
                            out@ == vals::<T>(kept.take(at0 + k)),
                            src.inv(),
                            src.src() == kept,
                            src.end() == len,
                            src.at() == at0 + n,
                        decreases n - k,
                    {
                        let ghost here = kept[at0 + k];
                        proof {
                            assert(kept.subrange(at0, at0 + n)[k as int] == here);
                            assert(in_dom::<T>(here));
                        }
                        let got = match d {
                            Some(x) => {
                                let next = cur.wrapping_add(x);
                                proof {
                                    lemma_wrapping_add(cur, x);
                                    lemma_crop_add(prefix + k * x, x as int);
                                    assert(prefix + k * x + x == prefix + (k + 1) * x) by (nonlinear_arith);
                                    assert(run[k as int] == Some(crop(prefix + (k + 1) * x)));
                                    assert(here == Some(next));
                                }
                                cur = next;
                                Some(next)
                            },
                            None => {
                                proof {
                                    assert(run[k as int] == None::<i64>);
                                }
                                None
                            },
                        };
                        out.push(T::from_i64(got));
                        proof {
                            assert(kept.take(at0 + k + 1) =~= kept.take(at0 + k).push(here));
                            assert(vals::<T>(kept.take(at0 + k).push(here)) =~= vals::<T>(kept.take(at0 + k)).push(
                                T::of(here),
                            ));
                        }
                        k = k + 1;
                    }
                },
                None => {
                    break;
                },
            }
        }
        proof {
            assert(kept.take(len as int) =~= kept);
        }
        out
    }
}

// ── The saved form ──────────────────────────────────────────────────────────

/// Two `i64`s that take a running sum to the same wrapped sum are the
/// same.
pub(super) proof fn lemma_crop_same(q: int, u: i64, v: i64)
    requires
        crop(q + u) == crop(q + v),
    ensures
        u == v,
{
    let m = 0x1_0000_0000_0000_0000int;
    lemma_crop(q + u);
    lemma_crop(q + v);
    vstd::arithmetic::div_mod::lemma_mod_equivalence(q + u, q + v, m);
    let d = u - v;
    assert((q + u) - (q + v) == d);
    assert(d % m == 0);
    if d > 0 {
        vstd::arithmetic::div_mod::lemma_small_mod(d as nat, m as nat);
    } else if d < 0 {
        vstd::arithmetic::div_mod::lemma_mod_add_multiples_vanish(d, m);
        vstd::arithmetic::div_mod::lemma_small_mod((m + d) as nat, m as nat);
    }
}

/// Deltas that realize the same values after the same running sum are
/// the same deltas.
pub(super) proof fn lemma_rz_inj(p: int, a: Seq<Option<i64>>, b: Seq<Option<i64>>)
    requires
        rz(p, a) == rz(p, b),
    ensures
        a == b,
    decreases a.len(),
{
    assert(a.len() == rz(p, a).len() && b.len() == rz(p, b).len());
    if a.len() > 0 {
        let n = a.len() - 1;
        let (a0, b0) = (a.drop_last(), b.drop_last());
        assert(rz(p, a0) =~= rz(p, b0)) by {
            assert forall|i: int| 0 <= i < n implies rz(p, a0)[i] == rz(p, b0)[i] by {
                assert(a0.take(i) =~= a.take(i));
                assert(b0.take(i) =~= b.take(i));
                assert(rz(p, a)[i] == rz(p, b)[i]);
            }
        }
        lemma_rz_inj(p, a0, b0);
        // the last ones: after the same sum, to the same value
        assert(a.take(n) =~= a0);
        assert(b.take(n) =~= b0);
        assert(rz(p, a)[n] == rz(p, b)[n]);
        match (a[n], b[n]) {
            (Some(u), Some(v)) => {
                lemma_crop_same(p + psum(a0), u, v);
            },
            _ => {},
        }
        assert(a[n] == b[n]);
        assert(a =~= a0.push(a[n]));
        assert(b =~= b0.push(b[n]));
    } else {
        assert(a =~= b);
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    pub(super) proof fn lemma_save_unique_impl(x: Seq<u8>, y: Seq<u8>)
        requires
            Self::valid(x),
            Self::valid(y),
            Self::values(x) == Self::values(y),
        ensures
            x == y,
    {
        let (kx, ky) = (Self::saved_kept(x), Self::saved_kept(y));
        // they keep the same: each value is kept as one thing only
        assert(kx =~= ky) by {
            assert(kx.len() == vals::<T>(kx).len() && ky.len() == vals::<T>(ky).len());
            assert forall|i: int| 0 <= i < kx.len() implies kx[i] == ky[i] by {
                assert(in_dom::<T>(kx[i]) && in_dom::<T>(ky[i]));
                assert(vals::<T>(kx)[i] == vals::<T>(ky)[i]);
                let v = T::of(kx[i]);
                T::lemma_delta(v, kx[i]);
                T::lemma_delta(v, ky[i]);
            }
        }
        // so the deltas are the same, and what the columns underneath
        // hold
        let vx = <DE<T, C> as Model<DX<T, C>>>::values(x);
        let vy = <DE<T, C> as Model<DX<T, C>>>::values(y);
        let f = |o: <DE<T, C> as Slabs>::V| <<T as DKind<C>>::B as DeltaWrite<'static, DE<T, C>, DX<T, C>>>::dvo(o);
        lemma_rz_inj(0, vx.map_values(f), vy.map_values(f));
        assert(vx =~= vy) by {
            assert(vx.len() == vx.map_values(f).len() && vy.len() == vy.map_values(f).len());
            assert forall|i: int| 0 <= i < vx.len() implies vx[i] == vy[i] by {
                assert(vx.map_values(f)[i] == vy.map_values(f)[i]);
                T::lemma_kind(vx[i], vy[i]);
            }
        }
        <DE<T, C> as Model<DX<T, C>>>::lemma_unique(x, y);
    }

    pub(super) proof fn lemma_encoding_impl(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        let e = Self::encoding(Self::values(d));
        assert(Self::valid(e) && Self::values(e) == Self::values(d));
        Self::lemma_save_unique(e, d);
    }

    pub(super) proof fn lemma_no_bytes_impl()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<T>::empty(),
    {
        <DE<T, C> as Model<DX<T, C>>>::lemma_none();
        let e = Seq::<u8>::empty();
        assert(Self::saved_kept(e) =~= Seq::<Option<i64>>::empty());
        assert(Self::values(e) =~= Seq::<T>::empty());
    }

    pub(super) fn save_impl(&self) -> (r: Vec<u8>)
        ensures
            Self::valid(r@),
            Self::values(r@) == self@,
            r@ == Self::encoding(self@),
    {
        proof {
            use_type_invariant(self);
        }
        self.core.lemma_column();
        let mut out: Vec<u8> = Vec::new();
        <DE<T, C> as Model<DX<T, C>>>::save(self.core.slabs(), self.core.len(), &mut out);
        proof {
            assert(out@.skip(0) =~= out@);
            Self::lemma_encoding(out@);
        }
        out
    }

    pub(super) fn save_to_impl(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + Self::encoding(self@),
            Self::valid(Self::encoding(self@)),
            Self::values(Self::encoding(self@)) == self@,
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        proof {
            use_type_invariant(self);
        }
        self.core.lemma_column();
        let start = out.len();
        <DE<T, C> as Model<DX<T, C>>>::save(self.core.slabs(), self.core.len(), out);
        proof {
            let d = out@.skip(start as int);
            Self::lemma_encoding(d);
            assert(out@ =~= old(out)@ + d);
        }
        start..out.len()
    }

    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    pub(super) fn is_only_impl(&self, value: T) -> (r: bool)
        requires
            value.fits(),
        ensures
            r == only(self@, value),
    {
        proof {
            use_type_invariant(self);
            T::lemma_delta(value, None);
        }
        let want = value.to_i64();
        let len = self.core.len();
        let ghost kept = self.kept();
        let mut src = self.core.dsrc();
        src.shift(0..len);
        loop
            invariant
                src.inv(),
                src.src() == kept,
                src.end() == len,
                kept == self.kept(),
                kept.len() == len,
                0 <= src.at() <= len,
                all_dom::<T>(kept),
                value.fits(),
                want == value.dv(),
                forall|i: int| 0 <= i < src.at() ==> kept[i] == want,
            ensures
                src.at() == len,
            decreases len - src.at(),
        {
            let ghost at0 = src.at();
            match src.next_run() {
                Some((prefix, d, n)) => {
                    let ghost run = item_vals(SpliceItem::Run(prefix, d, n));
                    // a run of one value: of nulls; or of a delta that
                    // is 0, or of one item
                    let same = match (d, want) {
                        (Some(x), Some(w)) => prefix.wrapping_add(x) == w && (n == 1 || x == 0),
                        (None, None) => true,
                        _ => false,
                    };
                    // an item of the run that is not the value, if any
                    let ghost mut other: int = 0;
                    proof {
                        assert forall|k: int| 0 <= k < n implies kept[at0 + k] == #[trigger] run[k] by {
                            assert(kept.subrange(at0 as int, at0 + n)[k] == kept[at0 + k]);
                        }
                        match (d, want) {
                            (Some(x), Some(w)) => {
                                lemma_wrapping_add(prefix, x);
                                let first = crop(prefix + x);
                                assert(prefix + (0 + 1) * x == prefix + x) by (nonlinear_arith);
                                assert(run[0] == Some(first));
                                if x == 0 {
                                    assert forall|k: int| 0 <= k < n implies run[k] == Some(first) by {
                                        assert((k + 1) * x == 0) by (nonlinear_arith)
                                            requires
                                                x == 0,
                                        ;
                                    }
                                } else if n > 1 {
                                    // the second is another than the first
                                    assert(prefix + (1 + 1) * x == prefix + x + x) by (nonlinear_arith);
                                    assert(run[1] == Some(crop(prefix + x + x)));
                                    if run[1] == run[0] {
                                        lemma_crop_same(prefix + x, x, 0);
                                    }
                                    if run[0] == want {
                                        other = 1;
                                    }
                                }
                            },
                            (None, None) => {
                                assert forall|k: int| 0 <= k < n implies run[k] == want by {}
                            },
                            _ => {
                                assert(run[0] != want);
                            },
                        }
                        assert(same ==> forall|k: int| 0 <= k < n ==> run[k] == want);
                        assert(!same ==> 0 <= other < n && run[other] != want);
                    }
                    if !same {
                        proof {
                            // that item is not `value`: `value` is kept
                            // as `want` and as nothing else
                            let j = at0 + other;
                            assert(kept[j] == run[other]);
                            assert(in_dom::<T>(kept[j]));
                            T::lemma_delta(value, kept[j]);
                            assert(self@[j] == T::of(kept[j]));
                            assert(self@[j] != value);
                        }
                        return false;
                    }
                    proof {
                        assert forall|i: int| 0 <= i < at0 + n implies kept[i] == want by {
                            if i >= at0 {
                                assert(kept[at0 + (i - at0)] == run[i - at0]);
                            }
                        }
                    }
                },
                None => {
                    break;
                },
            }
        }
        proof {
            T::lemma_delta(value, None);
            assert forall|i: int| 0 <= i < self@.len() implies self@[i] == value by {
                assert(self@[i] == T::of(kept[i]));
            }
        }
        true
    }

    pub(super) fn save_to_unless_impl(&self, out: &mut Vec<u8>, value: T) -> (r: Range<usize>)
        requires
            value.fits(),
        ensures
            final(out)@ == if only(self@, value) {
                old(out)@
            } else {
                old(out)@ + Self::encoding(self@)
            },
            !only(self@, value) ==> {
                &&& Self::valid(Self::encoding(self@))
                &&& Self::values(Self::encoding(self@)) == self@
            },
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        if self.is_only(value) {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    pub(super) fn load_impl(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(c) ==> Self::valid(bytes@) && c@ == Self::values(bytes@),
    {
        Self::load_in(bytes, MAX_SEG, None)
    }

    pub(super) fn load_with_impl<F: MaybeFill<T::Fill>>(bytes: &[u8], opts: LoadOpts<F>) -> (r: Result<Self, PackError>)
        requires
            opts.max_segments() >= 2,
        ensures
            match fill_of(bytes@, opts.length(), opts.fill().value()) {
                Some((len, d)) => r matches Ok(c) ==> c@ == delta_run_values::<T>(
                    DeltaRun { prefix: 0, delta: T::fill_delta(d), count: len },
                ),
                None => r matches Ok(c) ==> {
                    &&& Self::valid(bytes@)
                    &&& c@ == Self::values(bytes@)
                    &&& opts.length() matches Some(len) ==> c@.len() == len
                },
            },
    {
        let (length, fill, max_segments) = opts.parts();
        if bytes.len() == 0 {
            if let Some(len) = length {
                if len as u64 <= i64::MAX as u64 && len <= usize::MAX - 3 {
                    if let Some(d) = fill.fill_value() {
                        return Self::filled(len, d, max_segments);
                    }
                }
            }
        }
        Self::load_in(bytes, max_segments, length)
    }

    /// The column a load fills: `len` values after 0, each `fill` more
    /// than the one before it, or `len` nulls. Built as
    /// [`DeltaColumn`] builds it: one slab of the
    /// fill, weighed and checked as a loaded slab is.
    pub(super) fn filled(len: usize, fill: T::Fill, max_segments: usize) -> (r: Result<Self, PackError>)
        requires
            len <= i64::MAX,
            len <= usize::MAX - 3,
            max_segments >= 2,
        ensures
            r matches Ok(c) ==> c@ == delta_run_values::<T>(
                DeltaRun { prefix: 0, delta: T::fill_delta(fill), count: len },
            ),
    {
        let ghost run = DeltaRun { prefix: 0, delta: T::fill_delta(fill), count: len };
        if len == 0 {
            let r = Self::with_max_segments(max_segments);
            proof {
                assert(r@ =~= delta_run_values::<T>(run));
            }
            return Ok(r);
        }
        let it = T::fill_item(fill);
        let slab = <DE<T, C> as Model<DX<T, C>>>::fill(len, it);
        let w = match weigh_run::<_, SlabAgg, DeltaModel<DX<T, C>, <T as DKind<C>>::B>>(len, it) {
            Ok(w) => w,
            Err(e) => {
                return Err(e);
            },
        };
        let ghost s = slab;
        let mut slabs = Vec::with_capacity(1);
        slabs.push(slab);
        let mut ws = Vec::with_capacity(1);
        ws.push(w);
        proof {
            assert(slabs@ =~= seq![s]);
            assert(ws@ =~= seq![w]);
            // the slab's values are the fill, and its deltas the fill's
            <DE<T, C> as ReadsAgree<'static, DX<T, C>>>::lemma_reads(s);
            let xs = <DX<T, C> as Decoders<'static>>::vals(s);
            let dl = T::fill_delta(fill);
            <<T as DKind<C>>::B as DeltaWrite<'static, DE<T, C>, DX<T, C>>>::axiom_own(it);
            assert forall|i: int| 0 <= i < xs.len() implies <<T as DKind<C>>::B as DeltaOps<DItem<T, C>>>::dv(
                #[trigger] xs[i],
            ) == dl by {
                <<T as DKind<C>>::B as DeltaWrite<'static, DE<T, C>, DX<T, C>>>::axiom_own(xs[i]);
                assert(<DE<T, C> as Slabs>::vals(s)[i] == <DE<T, C> as ReadsAgree<'static, DX<T, C>>>::own(xs[i]));
                assert(<DE<T, C> as Slabs>::vals(s)[i] == <DE<T, C> as ReadsAgree<'static, DX<T, C>>>::own(it));
            }
            assert(deltas_of::<_, <T as DKind<C>>::B>(xs) =~= reps(len as nat, dl));
            assert(deltas_of::<_, <T as DKind<C>>::B>(Seq::new(len as nat, |_i: int| it)) =~= reps(len as nat, dl));
            lemma_sum_one(slab_lens(slabs@), 0);
        }
        match <DeltaModel<DX<T, C>, <T as DKind<C>>::B> as LoadWeigh<'static, DE<T, C>, DX<T, C>, SlabAgg>>::load_check(
            ws.as_slice(),
            slabs.as_slice(),
            len,
            T::lo(),
            T::hi(),
        ) {
            Ok(()) => {},
            Err(e) => {
                return Err(e);
            },
        }
        let core = DCore::<T, C>::from_cut(slabs, max_segments);
        proof {
            // what the column keeps is the run realized, and the check's
            // bound is on it
            let dl = T::fill_delta(fill);
            lemma_dslabs_own::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(seq![s]);
            lemma_dslabs_unit::<DX<T, C>, <T as DKind<C>>::B>(s);
            let k = rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@);
            assert(k == rz(0, reps(len as nat, dl)));
            match dl {
                Some(d) => {
                    lemma_rz_run(0, d, len as nat);
                },
                None => {
                    lemma_rz_nulls(0, len as nat);
                },
            }
            assert(k =~= delta_run_kept(run));
            assert(in_bound(k, T::least() as int, T::most() as int));
            assert forall|i: int| 0 <= i < k.len() implies in_dom::<T>(#[trigger] k[i]) by {
                T::lemma_kind(core@[i], core@[i]);
            }
        }
        Ok(DeltaColumn { core })
    }

    /// [`load`](Self::load), in slabs of at most `max_segments` runs,
    /// and of a column of `expected` values where a number is given.
    pub(super) fn load_in(bytes: &[u8], max_segments: usize, expected: Option<usize>) -> (r: Result<Self, PackError>)
        requires
            max_segments >= 2,
        ensures
            r matches Ok(c) ==> {
                &&& Self::valid(bytes@)
                &&& c@ == Self::values(bytes@)
                &&& expected matches Some(len) ==> c@.len() == len
            },
    {
        match <DE<T, C> as Model<DX<T, C>>>::load::<SlabAgg, SlabBTree<SlabAgg>, DeltaModel<DX<T, C>, <T as DKind<C>>::B>>(
            bytes,
            max_segments,
            expected,
            T::lo(),
            T::hi(),
        ) {
            Ok(core) => {
                core.lemma_column();
                proof {
                    // the load's bound is on what the column keeps
                    let s = core.slabs_spec();
                    lemma_dslabs_own::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(s);
                    let ds = dslabs::<DX<T, C>, <T as DKind<C>>::B>(s);
                    let k = rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(core@);
                    assert(k == rz(0, ds));
                    assert(in_bound(k, T::least() as int, T::most() as int));
                    assert forall|i: int| 0 <= i < k.len() implies in_dom::<T>(#[trigger] k[i]) by {
                        // and a null is kept only by a kind that has them
                        T::lemma_kind(core@[i], core@[i]);
                    }
                    assert(vals::<T>(k).len() == core@.len());
                }
                Ok(DeltaColumn { core })
            },
            Err(e) => Err(e),
        }
    }
}

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    pub(super) fn clone_impl(&self) -> (r: Self)
        ensures
            r@ == self@,
            r.edits() == self.edits(),
    {
        proof {
            use_type_invariant(self);
        }
        DeltaColumn { core: self.core.duplicate() }
    }
}

#[inline(always)]
pub(super) fn vdeltanum_try_to_i64_u64(this: u64) -> (r: Option<i64>)
    ensures
        r == if <u64 as DeltaNum>::fits(this) {
            Some(<u64 as DeltaNum>::n64(this))
        } else {
            None::<i64>
        },
{
    if this <= 0x7fff_ffff_ffff_ffff {
        Some(this as i64)
    } else {
        None
    }
}

#[inline(always)]
pub(super) fn vdeltanum_try_to_i64_usize(this: usize) -> (r: Option<i64>)
    ensures
        r == if <usize as DeltaNum>::fits(this) {
            Some(<usize as DeltaNum>::n64(this))
        } else {
            None::<i64>
        },
{
    if this as u64 <= 0x7fff_ffff_ffff_ffff {
        Some(this as i64)
    } else {
        None
    }
}

#[inline(always)]
pub(super) fn vdeltanum_hi_usize() -> (r: i64)
    ensures
        r == <usize as DeltaNum>::most(),
{
    if usize::MAX as u64 > 0x7fff_ffff_ffff_ffff {
        0x7fff_ffff_ffff_ffff
    } else {
        usize::MAX as i64
    }
}

pub(super) proof fn vdelta_lemma_delta_t<T: DeltaNum>(v: T, d: Option<i64>)
    ensures
        <T as Delta>::least() <= 0 <= <T as Delta>::most(),
        v.fits() ==> dom(v.dv(), <T as Delta>::least(), <T as Delta>::most(), <T as Delta>::nullable()) && <T as Delta>::of(v.dv()) == v,
        dom(d, <T as Delta>::least(), <T as Delta>::most(), <T as Delta>::nullable()) ==> <T as Delta>::of(d).fits() && <T as Delta>::of(d).dv() == d,
{
    <T as DeltaNum>::lemma_num(v, 0);
    if d is Some {
        <T as DeltaNum>::lemma_num(v, d->0);
    }
}

pub(super) proof fn vdelta_lemma_order_t<T: DeltaNum>(a: T, b: T)
    ensures
        a.fits() && b.fits() ==> <T as VSpec>::below(a, b) == delta_below(a.dv(), b.dv()),
        a.fits() && !b.fits() ==> <T as VSpec>::below(a, b),
{
    T::lemma_fit_order(a);
    T::lemma_fit_order(b);
}

#[inline(always)]
#[allow(clippy::manual_map)]  // a match is what the proof follows
pub(super) fn vdelta_try_i64_t<T: DeltaNum>(this: T) -> (r: Option<Option<i64>>)
    ensures
        r == if <T as Delta>::fits(this) {
            Some(<T as Delta>::dv(this))
        } else {
            None::<Option<i64>>
        },
{
    match <T as DeltaNum>::try_to_i64(this) {
        Some(n) => Some(Some(n)),
        None => None,
    }
}

#[inline(always)]
pub(super) fn vdelta_from_i64_t<T: DeltaNum>(d: Option<i64>) -> (r: T)
    requires
        dom(d, <T as Delta>::least(), <T as Delta>::most(), <T as Delta>::nullable()),
    ensures
        r == <T as Delta>::of(d),
{
    match d {
        Some(n) => <T as DeltaNum>::from_i64(n),
        None => {
            // not reached: the type has no null
            proof {
                <T as DeltaNum>::lemma_num(<T as DeltaNum>::of64(0), 0);
            }
            <T as DeltaNum>::from_i64(0)
        },
    }
}

pub(super) proof fn vdelta_lemma_delta_option_t<T: DeltaNum>(v: Option<T>, d: Option<i64>)
    ensures
        <Option<T> as Delta>::least() <= 0 <= <Option<T> as Delta>::most(),
        v.fits() ==> dom(v.dv(), <Option<T> as Delta>::least(), <Option<T> as Delta>::most(), <Option<T> as Delta>::nullable()) && <Option<T> as Delta>::of(v.dv()) == v,
        dom(d, <Option<T> as Delta>::least(), <Option<T> as Delta>::most(), <Option<T> as Delta>::nullable()) ==> <Option<T> as Delta>::of(d).fits() && <Option<T> as Delta>::of(d).dv() == d,
{
    if v is Some {
        <T as DeltaNum>::lemma_num(v->0, 0);
    }
    if d is Some {
        <T as DeltaNum>::lemma_num(T::of64(0), d->0);
    } else {
        <T as DeltaNum>::lemma_num(T::of64(0), 0);
    }
}

#[inline(always)]
#[allow(clippy::manual_map)]  // a match is what the proof follows
pub(super) fn vdelta_to_i64_option_t<T: DeltaNum>(this: Option<T>) -> (r: Option<i64>)
    requires
        <Option<T> as Delta>::fits(this),
    ensures
        r == <Option<T> as Delta>::dv(this),
{
    match this {
        Some(v) => Some(<T as DeltaNum>::to_i64(v)),
        None => None,
    }
}

pub(super) proof fn vdelta_lemma_order_option_t<T: DeltaNum>(a: Option<T>, b: Option<T>)
    ensures
        a.fits() && b.fits() ==> <Option<T> as VSpec>::below(a, b) == delta_below(a.dv(), b.dv()),
        a.fits() && !b.fits() ==> <Option<T> as VSpec>::below(a, b),
{
    if a is Some {
        T::lemma_fit_order(a->0);
    }
    if b is Some {
        T::lemma_fit_order(b->0);
    }
}

#[inline(always)]
#[allow(clippy::manual_map)]  // a match is what the proof follows
pub(super) fn vdelta_try_i64_option_t<T: DeltaNum>(this: Option<T>) -> (r: Option<Option<i64>>)
    ensures
        r == if <Option<T> as Delta>::fits(this) {
            Some(<Option<T> as Delta>::dv(this))
        } else {
            None::<Option<i64>>
        },
{
    match this {
        Some(v) => match <T as DeltaNum>::try_to_i64(v) {
            Some(n) => Some(Some(n)),
            None => None,
        },
        None => Some(None),
    }
}

#[inline(always)]
#[allow(clippy::manual_map)]  // a match is what the proof follows
pub(super) fn vdelta_from_i64_option_t<T: DeltaNum>(d: Option<i64>) -> (r: Option<T>)
    requires
        dom(d, <Option<T> as Delta>::least(), <Option<T> as Delta>::most(), <Option<T> as Delta>::nullable()),
    ensures
        r == <Option<T> as Delta>::of(d),
{
    match d {
        Some(n) => Some(<T as DeltaNum>::from_i64(n)),
        None => None,
    }
}

} // verus!

impl<T: DeltaValue<C>, C: Codec> std::fmt::Debug for DeltaColumn<T, C> {
    /// How many values and how many slabs, as `DeltaColumn` prints
    /// itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaColumn")
            .field("len", &self.core.len())
            .field("slabs", &self.core.slabs().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Delta, DeltaNum};

    /// The conversions, and the bounds of what fits (`lo..=hi`, which are
    /// those hexane has always had): a value fits as itself and
    /// comes back as itself, and a null is none.
    fn converts<T>(lo: i64, hi: i64, samples: &[T])
    where
        T: DeltaNum + std::fmt::Debug + PartialEq + Copy,
    {
        assert_eq!((<T as DeltaNum>::lo(), <T as DeltaNum>::hi()), (lo, hi));
        assert_eq!((<T as Delta>::lo(), <T as Delta>::hi()), (lo, hi));
        assert_eq!(
            (<Option<T> as Delta>::lo(), <Option<T> as Delta>::hi()),
            (lo, hi)
        );
        assert_eq!(<Option<T> as Delta>::to_i64(None), None);
        assert_eq!(<Option<T> as Delta>::from_i64(None), None);
        for &v in samples {
            let n = <T as DeltaNum>::to_i64(v);
            assert!(lo <= n && n <= hi, "{v:?}");
            assert_eq!(<T as Delta>::to_i64(v), Some(n));
            assert_eq!(<Option<T> as Delta>::to_i64(Some(v)), Some(n));
            assert_eq!(<T as DeltaNum>::from_i64(n), v);
            assert_eq!(<T as Delta>::from_i64(Some(n)), v);
            assert_eq!(<Option<T> as Delta>::from_i64(Some(n)), Some(v));
        }
    }

    #[test]
    fn conversions_and_bounds() {
        converts::<u32>(0, u32::MAX as i64, &[0, 1, 77, u32::MAX - 1, u32::MAX]);
        converts::<u64>(
            0,
            i64::MAX,
            &[0, 1, 1 << 40, i64::MAX as u64 - 1, i64::MAX as u64],
        );
        converts::<i32>(
            i32::MIN as i64,
            i32::MAX as i64,
            &[i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX],
        );
        converts::<i64>(
            i64::MIN,
            i64::MAX,
            &[i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX],
        );
        let top = if usize::BITS >= 64 {
            i64::MAX as usize
        } else {
            usize::MAX
        };
        converts::<usize>(0, top as i64, &[0, 1, 4096, top - 1, top]);
    }
}
