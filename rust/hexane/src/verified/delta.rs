//! Delta columns of the verified public API: the numbers a delta
//! column holds ([`VDelta`]), how the values of the column underneath
//! are deltas (`VDeltas`, in place of the old API's trusted
//! `delta::indexed::DB`), sources of values that say what they give
//! (`SliceValues`, `SliceItems`), so that what the verified cores build
//! from them is known, and the column ([`VerifiedDeltaColumn`]).

#[cfg(verus_keep_ghost)]
use super::after_edit;
use super::codec::VCodec;
use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::only;
use super::seal;
use super::value::{VOps, VOptOps, VOptWrite, VWrite};
use super::{VValue, MAX_SEG};
use crate::btree::{SlabAgg, SlabBTree};
use crate::column_core::ColumnCore;
#[cfg(verus_keep_ghost)]
use crate::cursor::Slabs;
#[cfg(verus_keep_ghost)]
use crate::delta::agg::psum;
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::dbound::in_bound;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{dslabs, item_vals, items_vals, lemma_dslabs_own, rvals};
use crate::delta::ddomain::{DeltaWrite, SpliceItem, SpliceItems, ValueItems};
#[cfg(verus_keep_ghost)]
use crate::delta::dedit::rz;
#[cfg(verus_keep_ghost)]
use crate::delta::diter::{crop, lemma_crop, lemma_crop_add, lemma_wrapping_add};
use crate::rle::dec::RleReads;
use crate::rle::edit::{null_in_column, ReadIns, RleModel};
use crate::weights::DeltaModel;
use crate::{Leb128, PackError};
use std::marker::PhantomData;
use std::ops::Range;
use vstd::prelude::*;

verus! {

// ── The numbers of a delta column ───────────────────────────────────────────

/// A number a delta column can hold: `u32`, `u64`, `i32`, `i64` or
/// `usize`. A delta column keeps its values' running sums as `i64`s, so
/// of the wider types it holds the numbers up to `i64::MAX` only
/// (`fits`).
pub trait VDeltaNum: Copy + seal::DeltaSeal {
    /// Whether a delta column can hold the number: always, but for a
    /// `u64` or a `usize` above `i64::MAX`.
    spec fn fits(self) -> bool;

    /// The number as an `i64`.
    #[doc(hidden)]
    spec fn n64(self) -> i64;

    /// The number of the type that an `i64` is.
    #[doc(hidden)]
    spec fn of64(n: i64) -> Self;

    /// The least number of the type that fits.
    #[doc(hidden)]
    spec fn least() -> i64;

    /// The greatest number of the type that fits.
    #[doc(hidden)]
    spec fn most() -> i64;

    /// The numbers that fit are those from the least to the greatest, 0
    /// among them, each of them one `i64` and no two the same.
    #[doc(hidden)]
    proof fn lemma_num(v: Self, n: i64)
        ensures
            Self::least() <= 0 <= Self::most(),
            v.fits() ==> Self::least() <= v.n64() <= Self::most() && Self::of64(v.n64()) == v,
            Self::least() <= n <= Self::most() ==> Self::of64(n).fits() && Self::of64(n).n64() == n,
    ;

    #[doc(hidden)]
    fn to_i64(self) -> (r: i64)
        requires
            self.fits(),
        ensures
            r == self.n64(),
    ;

    #[doc(hidden)]
    fn from_i64(n: i64) -> (r: Self)
        requires
            Self::least() <= n <= Self::most(),
        ensures
            r == Self::of64(n),
    ;

    #[doc(hidden)]
    fn lo() -> (r: i64)
        ensures
            r == Self::least(),
    ;

    #[doc(hidden)]
    fn hi() -> (r: i64)
        ensures
            r == Self::most(),
    ;
}

impl seal::DeltaSeal for u32 {

}

impl VDeltaNum for u32 {
    open spec fn fits(self) -> bool {
        true
    }

    open spec fn n64(self) -> i64 {
        self as i64
    }

    open spec fn of64(n: i64) -> u32 {
        n as u32
    }

    open spec fn least() -> i64 {
        0
    }

    open spec fn most() -> i64 {
        0xffff_ffff
    }

    proof fn lemma_num(v: u32, n: i64) {
    }

    #[inline(always)]
    fn to_i64(self) -> i64 {
        self as i64
    }

    #[inline(always)]
    fn from_i64(n: i64) -> u32 {
        n as u32
    }

    #[inline(always)]
    fn lo() -> i64 {
        0
    }

    #[inline(always)]
    fn hi() -> i64 {
        0xffff_ffff
    }
}

impl seal::DeltaSeal for u64 {

}

impl VDeltaNum for u64 {
    open spec fn fits(self) -> bool {
        self <= 0x7fff_ffff_ffff_ffff
    }

    open spec fn n64(self) -> i64 {
        self as i64
    }

    open spec fn of64(n: i64) -> u64 {
        n as u64
    }

    open spec fn least() -> i64 {
        0
    }

    open spec fn most() -> i64 {
        0x7fff_ffff_ffff_ffff
    }

    proof fn lemma_num(v: u64, n: i64) {
    }

    #[inline(always)]
    fn to_i64(self) -> i64 {
        self as i64
    }

    #[inline(always)]
    fn from_i64(n: i64) -> u64 {
        n as u64
    }

    #[inline(always)]
    fn lo() -> i64 {
        0
    }

    #[inline(always)]
    fn hi() -> i64 {
        0x7fff_ffff_ffff_ffff
    }
}

impl seal::DeltaSeal for i32 {

}

impl VDeltaNum for i32 {
    open spec fn fits(self) -> bool {
        true
    }

    open spec fn n64(self) -> i64 {
        self as i64
    }

    open spec fn of64(n: i64) -> i32 {
        n as i32
    }

    open spec fn least() -> i64 {
        i32::MIN as i64
    }

    open spec fn most() -> i64 {
        0x7fff_ffff
    }

    proof fn lemma_num(v: i32, n: i64) {
    }

    #[inline(always)]
    fn to_i64(self) -> i64 {
        self as i64
    }

    #[inline(always)]
    fn from_i64(n: i64) -> i32 {
        n as i32
    }

    #[inline(always)]
    fn lo() -> i64 {
        i32::MIN as i64
    }

    #[inline(always)]
    fn hi() -> i64 {
        0x7fff_ffff
    }
}

impl seal::DeltaSeal for i64 {

}

impl VDeltaNum for i64 {
    open spec fn fits(self) -> bool {
        true
    }

    open spec fn n64(self) -> i64 {
        self
    }

    open spec fn of64(n: i64) -> i64 {
        n
    }

    open spec fn least() -> i64 {
        i64::MIN
    }

    open spec fn most() -> i64 {
        i64::MAX
    }

    proof fn lemma_num(v: i64, n: i64) {
    }

    #[inline(always)]
    fn to_i64(self) -> i64 {
        self
    }

    #[inline(always)]
    fn from_i64(n: i64) -> i64 {
        n
    }

    #[inline(always)]
    fn lo() -> i64 {
        i64::MIN
    }

    #[inline(always)]
    fn hi() -> i64 {
        i64::MAX
    }
}

impl seal::DeltaSeal for usize {

}

impl VDeltaNum for usize {
    open spec fn fits(self) -> bool {
        self <= 0x7fff_ffff_ffff_ffff
    }

    open spec fn n64(self) -> i64 {
        self as i64
    }

    open spec fn of64(n: i64) -> usize {
        n as usize
    }

    open spec fn least() -> i64 {
        0
    }

    /// `i64::MAX`, or `usize::MAX` where a `usize` is narrower.
    open spec fn most() -> i64 {
        if usize::MAX > 0x7fff_ffff_ffff_ffff {
            0x7fff_ffff_ffff_ffff
        } else {
            usize::MAX as i64
        }
    }

    proof fn lemma_num(v: usize, n: i64) {
    }

    #[inline(always)]
    fn to_i64(self) -> i64 {
        self as i64
    }

    #[inline(always)]
    fn from_i64(n: i64) -> usize {
        n as usize
    }

    #[inline(always)]
    fn lo() -> i64 {
        0
    }

    #[inline(always)]
    fn hi() -> i64 {
        if usize::MAX as u64 > 0x7fff_ffff_ffff_ffff {
            0x7fff_ffff_ffff_ffff
        } else {
            usize::MAX as i64
        }
    }
}

// ── The values of a delta column ────────────────────────────────────────────

/// What a delta column keeps, of numbers from `least` to `most`: one of
/// those, or a null where there is one.
#[doc(hidden)]
pub open spec fn dom(d: Option<i64>, least: i64, most: i64, nullable: bool) -> bool {
    match d {
        Some(n) => least <= n <= most,
        None => nullable,
    }
}

/// A value type of a delta column: a number ([`VDeltaNum`]) or an
/// `Option` of one, `None` the null.
pub trait VDelta: Copy + seal::DeltaValSeal {
    /// Whether a delta column can hold the value: a null, or a number
    /// that fits ([`VDeltaNum::fits`]).
    spec fn fits(self) -> bool;

    /// Whether the type has a null.
    #[doc(hidden)]
    spec fn nullable() -> bool;

    /// The value as the column keeps it: the number as an `i64`, or
    /// none for a null.
    #[doc(hidden)]
    spec fn dv(self) -> Option<i64>;

    /// The value that the column keeps as `d`.
    #[doc(hidden)]
    spec fn of(d: Option<i64>) -> Self;

    /// The least number of the type that fits.
    #[doc(hidden)]
    spec fn least() -> i64;

    /// The greatest number of the type that fits.
    #[doc(hidden)]
    spec fn most() -> i64;

    /// The values that fit are what the column can keep ([`dom`]),
    /// each kept as itself and no two the same.
    #[doc(hidden)]
    proof fn lemma_delta(v: Self, d: Option<i64>)
        ensures
            Self::least() <= 0 <= Self::most(),
            v.fits() ==> dom(v.dv(), Self::least(), Self::most(), Self::nullable()) && Self::of(v.dv()) == v,
            dom(d, Self::least(), Self::most(), Self::nullable()) ==> Self::of(d).fits() && Self::of(d).dv() == d,
    ;

    #[doc(hidden)]
    fn to_i64(self) -> (r: Option<i64>)
        requires
            self.fits(),
        ensures
            r == self.dv(),
    ;

    #[doc(hidden)]
    fn from_i64(d: Option<i64>) -> (r: Self)
        requires
            dom(d, Self::least(), Self::most(), Self::nullable()),
        ensures
            r == Self::of(d),
    ;

    #[doc(hidden)]
    fn lo() -> (r: i64)
        ensures
            r == Self::least(),
    ;

    #[doc(hidden)]
    fn hi() -> (r: i64)
        ensures
            r == Self::most(),
    ;
}

/// What a delta column of `T` can keep: a number from the least to the
/// greatest that fits, or a null where `T` has one.
#[doc(hidden)]
pub open spec fn in_dom<T: VDelta>(d: Option<i64>) -> bool {
    dom(d, T::least(), T::most(), T::nullable())
}

impl<T: VDeltaNum> seal::DeltaValSeal for T {

}

impl<T: VDeltaNum> VDelta for T {
    open spec fn fits(self) -> bool {
        <T as VDeltaNum>::fits(self)
    }

    open spec fn nullable() -> bool {
        false
    }

    open spec fn dv(self) -> Option<i64> {
        Some(self.n64())
    }

    open spec fn of(d: Option<i64>) -> T {
        match d {
            Some(n) => T::of64(n),
            None => T::of64(0),
        }
    }

    open spec fn least() -> i64 {
        <T as VDeltaNum>::least()
    }

    open spec fn most() -> i64 {
        <T as VDeltaNum>::most()
    }

    proof fn lemma_delta(v: T, d: Option<i64>) {
        T::lemma_num(v, 0);
        if d is Some {
            T::lemma_num(v, d->0);
        }
    }

    #[inline(always)]
    fn to_i64(self) -> Option<i64> {
        Some(<T as VDeltaNum>::to_i64(self))
    }

    #[inline(always)]
    fn from_i64(d: Option<i64>) -> T {
        match d {
            Some(n) => <T as VDeltaNum>::from_i64(n),
            None => {
                // not reached: the type has no null
                proof {
                    T::lemma_num(<T as VDeltaNum>::of64(0), 0);
                }
                <T as VDeltaNum>::from_i64(0)
            },
        }
    }

    #[inline(always)]
    fn lo() -> i64 {
        <T as VDeltaNum>::lo()
    }

    #[inline(always)]
    fn hi() -> i64 {
        <T as VDeltaNum>::hi()
    }
}

impl<T: VDeltaNum> seal::DeltaValSeal for Option<T> {

}

impl<T: VDeltaNum> VDelta for Option<T> {
    open spec fn fits(self) -> bool {
        match self {
            Some(v) => v.fits(),
            None => true,
        }
    }

    open spec fn nullable() -> bool {
        true
    }

    open spec fn dv(self) -> Option<i64> {
        match self {
            Some(v) => Some(v.n64()),
            None => None,
        }
    }

    open spec fn of(d: Option<i64>) -> Option<T> {
        match d {
            Some(n) => Some(T::of64(n)),
            None => None,
        }
    }

    open spec fn least() -> i64 {
        <T as VDeltaNum>::least()
    }

    open spec fn most() -> i64 {
        <T as VDeltaNum>::most()
    }

    proof fn lemma_delta(v: Option<T>, d: Option<i64>) {
        if v is Some {
            T::lemma_num(v->0, 0);
        }
        if d is Some {
            T::lemma_num(T::of64(0), d->0);
        } else {
            T::lemma_num(T::of64(0), 0);
        }
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn to_i64(self) -> Option<i64> {
        match self {
            Some(v) => Some(<T as VDeltaNum>::to_i64(v)),
            None => None,
        }
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn from_i64(d: Option<i64>) -> Option<T> {
        match d {
            Some(n) => Some(<T as VDeltaNum>::from_i64(n)),
            None => None,
        }
    }

    #[inline(always)]
    fn lo() -> i64 {
        <T as VDeltaNum>::lo()
    }

    #[inline(always)]
    fn hi() -> i64 {
        <T as VDeltaNum>::hi()
    }
}

/// Values `vs` as a delta column keeps them.
#[doc(hidden)]
pub open spec fn dvs<T: VDelta>(vs: Seq<T>) -> Seq<Option<i64>> {
    vs.map_values(|v: T| v.dv())
}

/// Every one of values `vs` is one a delta column can hold
/// ([`VDelta::fits`]).
pub open spec fn all_fit<T: VDelta>(vs: Seq<T>) -> bool {
    forall|i: int| 0 <= i < vs.len() ==> (#[trigger] vs[i]).fits()
}

// ── The column underneath, as deltas ────────────────────────────────────────

/// The values of the column underneath a delta column as deltas, for
/// the verified cores: an `i64` is its own delta and so is an
/// `Option<i64>`, a null being no delta. `I` is `i64` or `Option<i64>`.
#[doc(hidden)]
#[verifier::reject_recursive_types(I)]
#[verifier::reject_recursive_types(C)]
pub struct VDeltas<I, C>(PhantomData<(I, C)>);

impl<C: VCodec> DeltaOps<i64> for VDeltas<i64, C> {
    open spec fn dv(v: i64) -> Option<i64> {
        Some(v)
    }

    #[inline(always)]
    fn to_opt(v: i64) -> Option<i64> {
        Some(v)
    }
}

impl<C: VCodec> DeltaOps<Option<i64>> for VDeltas<Option<i64>, C> {
    open spec fn dv(v: Option<i64>) -> Option<i64> {
        v
    }

    #[inline(always)]
    fn to_opt(v: Option<i64>) -> Option<i64> {
        v
    }
}

impl<'a, C: VCodec> DeltaWrite<'a, RleModel<VWrite<i64, C>>, RleReads<VOps<i64, C>>> for VDeltas<i64, C> {
    type Ins = ReadIns<'a, VWrite<i64, C>>;

    open spec fn dvo(o: i64) -> Option<i64> {
        Some(o)
    }

    proof fn axiom_own(v: i64) {
    }

    /// # Panics
    ///
    /// Panics when asked to write a null: the column has none.
    #[inline(always)]
    fn ins(d: Option<i64>) -> ReadIns<'a, VWrite<i64, C>> {
        match d {
            Some(v) => ReadIns { v },
            None => {
                null_in_column(Ghost(true));
                ReadIns { v: 0 }
            },
        }
    }
}

impl<'a, C: VCodec> DeltaWrite<'a, RleModel<VOptWrite<i64, C>>, RleReads<VOptOps<i64, C>>> for VDeltas<
    Option<i64>,
    C,
> {
    type Ins = ReadIns<'a, VOptWrite<i64, C>>;

    open spec fn dvo(o: Option<i64>) -> Option<i64> {
        o
    }

    proof fn axiom_own(v: Option<i64>) {
    }

    #[inline(always)]
    fn ins(d: Option<i64>) -> ReadIns<'a, VOptWrite<i64, C>> {
        ReadIns { v: d }
    }
}

// ── Sources that say what they give ─────────────────────────────────────────

/// The values of a slice, for a delta column to be built from
/// (`ColumnCore::fill_delta`): each as the column keeps it, in the
/// slice's order.
pub(crate) struct SliceValues<'v, T: VDelta> {
    pub(crate) vs: &'v [T],
    pub(crate) at: usize,
}

impl<'v, T: VDelta> SliceValues<'v, T> {
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

impl<'v, T: VDelta> ValueItems for SliceValues<'v, T> {
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
pub(crate) struct SliceItems<'v, T: VDelta> {
    pub(crate) vs: &'v [T],
    pub(crate) at: usize,
}

impl<'v, T: VDelta> SliceItems<'v, T> {
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

impl<'v, T: VDelta> SpliceItems for SliceItems<'v, T> {
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

// ── The column ──────────────────────────────────────────────────────────────

/// How a delta column of the type is kept: the column underneath, of
/// `i64`s or of `Option<i64>`s, and how its values are deltas.
pub(crate) trait DKind<C: VCodec>: VDelta {
    /// What the column underneath holds.
    type I: VValue<C>;

    /// Its values as deltas.
    type B: for <'a>DeltaWrite<'a, <Self::I as seal::Sealed<C>>::E, <Self::I as seal::Sealed<C>>::X>;

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
}

impl<T: VDeltaNum, C: VCodec> DKind<C> for T {
    type I = i64;

    type B = VDeltas<i64, C>;

    proof fn lemma_kind(a: i64, b: i64) {
    }
}

impl<T: VDeltaNum, C: VCodec> DKind<C> for Option<T> {
    type I = Option<i64>;

    type B = VDeltas<Option<i64>, C>;

    proof fn lemma_kind(a: Option<i64>, b: Option<i64>) {
    }
}

/// A value type of a delta column under codec `C`: a number
/// ([`VDeltaNum`]) or an `Option` of one.
#[allow(private_bounds)]
pub trait VDeltaValue<C: VCodec = Leb128>: DKind<C> {

}

impl<T: DKind<C>, C: VCodec> VDeltaValue<C> for T {

}

/// The slabs of the column underneath.
type DE<T, C> = <<T as DKind<C>>::I as seal::Sealed<C>>::E;

/// How the column underneath is read.
type DX<T, C> = <<T as DKind<C>>::I as seal::Sealed<C>>::X;

/// The verified core of a delta column.
type DCore<T, C> = ColumnCore<DE<T, C>, SlabAgg, SlabBTree<SlabAgg>, DeltaModel<DX<T, C>, <T as DKind<C>>::B>>;

/// A delta column of the verified public API: a column of numbers, or
/// of `Option`s of them, kept as each one's difference from the one
/// before it.
pub struct VerifiedDeltaColumn<T: VDeltaValue<C>, C: VCodec = Leb128> {
    core: DCore<T, C>,
}

impl<T: VDeltaValue<C>, C: VCodec> View for VerifiedDeltaColumn<T, C> {
    type V = Seq<T>;

    closed spec fn view(&self) -> Seq<T> {
        vals::<T>(self.kept())
    }
}

/// The values a delta column keeps as `ds`.
#[doc(hidden)]
pub open spec fn vals<T: VDelta>(ds: Seq<Option<i64>>) -> Seq<T> {
    ds.map_values(|d: Option<i64>| T::of(d))
}

/// Each of `ds` is one a delta column of `T` can keep.
#[doc(hidden)]
pub open spec fn all_dom<T: VDelta>(ds: Seq<Option<i64>>) -> bool {
    forall|i: int| 0 <= i < ds.len() ==> in_dom::<T>(#[trigger] ds[i])
}

/// Values that fit are kept as what a column can keep, and are the
/// values of what they are kept as.
pub(crate) proof fn lemma_kept<T: VDelta>(vs: Seq<T>)
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

impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// What the column keeps: each value as an `i64`, or none for a
    /// null.
    #[doc(hidden)]
    pub closed spec fn kept(&self) -> Seq<Option<i64>> {
        rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(self.core@)
    }

    /// Every value kept is one of the type's.
    #[verifier::type_invariant]
    spec fn inv(&self) -> bool {
        all_dom::<T>(self.kept())
    }

    /// The count of the column's edits. Each method that changes the
    /// column says what it is after the call: one more (`after_edit`)
    /// where the call was an edit, and what it was where the call was
    /// none.
    pub closed spec fn edits(&self) -> usize {
        self.core.count()
    }

    /// An empty column.
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::with_max_segments(MAX_SEG)
    }

    /// An empty column whose slabs hold at most `max_segments` runs
    /// each.
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
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
        VerifiedDeltaColumn { core }
    }

    /// A column of the values `values`.
    ///
    /// # Panics
    ///
    /// Panics if two of the values lie 2^63 or more apart: the
    /// difference between them is kept as an `i64`.
    pub fn from_values(values: &[T]) -> (r: Self)
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
        VerifiedDeltaColumn { core }
    }

    /// The number of values.
    pub fn len(&self) -> (n: usize)
        ensures
            n == self@.len(),
    {
        self.core.len()
    }

    /// Whether the column has no values.
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.core.len() == 0
    }

    /// Every value of the column is one a delta column can hold.
    pub proof fn lemma_fits(tracked &self)
        ensures
            all_fit(self@),
    {
        use_type_invariant(self);
        assert forall|i: int| 0 <= i < self@.len() implies (#[trigger] self@[i]).fits() by {
            T::lemma_delta(T::of(None), self.kept()[i]);
        }
    }

    /// Remove `del` values from index `at` on and put `values` in their
    /// place.
    ///
    /// The column counts an edit more (`after_edit` of its `edits()`),
    /// unless the call deletes no value and inserts none.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    pub fn splice(&mut self, at: usize, del: usize, values: &[T])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            all_fit(values@),
        ensures
            final(self)@ == old(self)@.take(at as int) + values@ + old(self)@.skip(at + del),
            final(self).edits() == if del > 0 || values@.len() > 0 {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
    {
        proof {
            use_type_invariant(&*self);
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
        *self = VerifiedDeltaColumn { core };
    }

    /// The value at `index`, or `None` past the end.
    pub fn get(&self, index: usize) -> (r: Option<T>)
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

    /// How many slabs hold the column. That is a fact of how the column
    /// lies in memory and not of its values: nothing is specified of it.
    pub fn slab_count(&self) -> usize {
        self.core.slabs().len()
    }

    /// Delete `del` values at `at`: a splice that puts nothing in their
    /// place.
    fn delete(&mut self, at: usize, del: usize)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            final(self)@ == old(self)@.take(at as int) + old(self)@.skip(at + del),
            final(self).edits() == if del > 0 {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
    {
        let none: Vec<T> = Vec::new();
        self.splice(at, del, none.as_slice());
        proof {
            let (a, b) = (old(self)@.take(at as int), old(self)@.skip(at + del));
            assert(a + none@ + b =~= a + b);
        }
    }

    /// Insert `value` before the value at `at`. The column counts an
    /// edit more.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    pub fn insert(&mut self, at: usize, value: T)
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
        self.splice(at, 0, one.as_slice());
        proof {
            assert(self@ =~= old(self)@.insert(at as int, value));
        }
    }

    /// Add `value` at the end. The column counts an edit more.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    pub fn push(&mut self, value: T)
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

    /// Delete the value at `at`, where there is one: then the column
    /// counts an edit more. Past the end there is nothing to delete, and
    /// the call is no edit, as it is none of
    /// [`DeltaColumn`](crate::DeltaColumn).
    pub fn remove(&mut self, at: usize)
        ensures
            final(self)@ == if at < old(self)@.len() {
                old(self)@.remove(at as int)
            } else {
                old(self)@
            },
            final(self).edits() == if at < old(self)@.len() {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
    {
        if at < self.len() {
            self.delete(at, 1);
            proof {
                assert(self@ =~= old(self)@.remove(at as int));
            }
        }
    }

    /// Delete the `n` values at `at`. The column counts an edit more,
    /// unless `n` is 0: then the call does nothing, wherever `at` is.
    pub fn remove_n(&mut self, at: usize, n: usize)
        requires
            n > 0 ==> at <= old(self)@.len() && n <= old(self)@.len() - at,
        ensures
            final(self)@ == if n > 0 {
                old(self)@.take(at as int) + old(self)@.skip(at + n)
            } else {
                old(self)@
            },
            final(self).edits() == if n > 0 {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
    {
        if n > 0 {
            self.delete(at, n);
        }
    }

    /// Keep the first `len` values, where there are more: then the
    /// column counts an edit more, and where there are no more it counts
    /// what it did.
    pub fn truncate(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
            final(self).edits() == if len < old(self)@.len() {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
    {
        let n = self.len();
        if len < n {
            self.delete(len, n - len);
            proof {
                assert(self@ =~= old(self)@.take(len as int));
            }
        }
    }

    /// Delete every value. The column keeps its segment budget.
    ///
    /// Of a column with no value this is no edit, as it is none of
    /// [`DeltaColumn`](crate::DeltaColumn). Of any other the column
    /// counts an edit more.
    pub fn clear(&mut self)
        ensures
            final(self)@ == Seq::<T>::empty(),
            final(self).edits() == if old(self)@.len() > 0 {
                after_edit(old(self).edits())
            } else {
                old(self).edits()
            },
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
        *self = VerifiedDeltaColumn { core };
    }

    /// Take the last value off the column, if it has one: then the
    /// column counts an edit more.
    pub fn pop(&mut self) -> (r: Option<T>)
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

    /// The first value, or `None` of an empty column.
    pub fn first(&self) -> (r: Option<T>)
        ensures
            self@.len() > 0 ==> r == Some(self@.first()),
            self@.len() == 0 ==> r is None,
    {
        self.get(0)
    }

    /// The last value, or `None` of an empty column.
    pub fn last(&self) -> (r: Option<T>)
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

    /// The values, in a `Vec`.
    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    pub fn to_vec(&self) -> (r: Vec<T>)
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
proof fn lemma_crop_same(q: int, u: i64, v: i64)
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
proof fn lemma_rz_inj(p: int, a: Seq<Option<i64>>, b: Seq<Option<i64>>)
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

// The saved form is the column's underneath, of `i64`s or of
// `Option<i64>`s: the values' deltas, each value one of the type's.
impl<T: VDeltaValue<C>, C: VCodec> VerifiedDeltaColumn<T, C> {
    /// What the saved column `d` keeps: each value as an `i64`, or
    /// none for a null.
    #[doc(hidden)]
    pub closed spec fn saved_kept(d: Seq<u8>) -> Seq<Option<i64>> {
        rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(<DE<T, C> as Model<DX<T, C>>>::values(d))
    }

    /// `d` is a saved delta column of `T`: the canonical encoding of
    /// some values' deltas, each of the values one of the type's.
    pub closed spec fn valid(d: Seq<u8>) -> bool {
        <DE<T, C> as Model<DX<T, C>>>::valid(d) && all_dom::<T>(Self::saved_kept(d))
    }

    /// The values of the saved delta column `d`.
    pub closed spec fn values(d: Seq<u8>) -> Seq<T> {
        vals::<T>(Self::saved_kept(d))
    }

    /// The bytes a delta column of the values `vs` saves to.
    pub closed spec fn encoding(vs: Seq<T>) -> Seq<u8> {
        choose|d: Seq<u8>| Self::valid(d) && Self::values(d) == vs
    }

    /// Two saved delta columns of the same values are the same bytes.
    pub proof fn lemma_save_unique(x: Seq<u8>, y: Seq<u8>)
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

    /// A saved delta column is the encoding of its values.
    pub proof fn lemma_encoding(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        let e = Self::encoding(Self::values(d));
        assert(Self::valid(e) && Self::values(e) == Self::values(d));
        Self::lemma_save_unique(e, d);
    }

    /// No bytes are a saved delta column: the one of no values.
    pub proof fn lemma_no_bytes()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<T>::empty(),
    {
        <DE<T, C> as Model<DX<T, C>>>::lemma_none();
        let e = Seq::<u8>::empty();
        assert(Self::saved_kept(e) =~= Seq::<Option<i64>>::empty());
        assert(Self::values(e) =~= Seq::<T>::empty());
    }

    /// The column, saved: the encoding of its values.
    pub fn save(&self) -> (r: Vec<u8>)
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

    /// Save the column after what `out` holds: `out` is then what it
    /// was, and after that the encoding of the column's values. The
    /// range returned is where in `out` that is.
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
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

    /// Whether every value of the column is `value`. So it is of an
    /// empty column, whatever `value` is. The column's runs are read,
    /// not each of its values.
    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    pub fn is_only(&self, value: T) -> (r: bool)
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

    /// [`save_to`](Self::save_to), unless every value of the column is
    /// `value`: then nothing is written, and the range is empty. So is
    /// it for an empty column, whatever `value` is.
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: T) -> (r: Range<usize>)
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

    /// The delta column saved as `bytes`. An error unless `bytes` is a
    /// saved delta column of `T`: the load checks, as it reads the
    /// runs, that each value they stand for is one of the type's.
    ///
    /// What is not said: that a saved delta column loads. The checks
    /// are made of sums kept as the runs pass, and those are not
    /// proved of the bytes.
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(c) ==> Self::valid(bytes@) && c@ == Self::values(bytes@),
    {
        match <DE<T, C> as Model<DX<T, C>>>::load::<SlabAgg, DeltaModel<DX<T, C>, <T as DKind<C>>::B>>(
            bytes,
            MAX_SEG,
            None,
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
                }
                Ok(VerifiedDeltaColumn { core })
            },
            Err(e) => Err(e),
        }
    }
}

impl<T: VDeltaValue<C>, C: VCodec> Default for VerifiedDeltaColumn<T, C> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::new()
    }
}

impl<T: VDeltaValue<C>, C: VCodec> Clone for VerifiedDeltaColumn<T, C> {
    /// A column of its own with the same values, which counts the edits
    /// this one does.
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
            r.edits() == self.edits(),
    {
        proof {
            use_type_invariant(self);
        }
        VerifiedDeltaColumn { core: self.core.duplicate() }
    }
}

} // verus!

impl<T: VDeltaValue<C>, C: VCodec> std::fmt::Debug for VerifiedDeltaColumn<T, C> {
    /// How many values and how many slabs, as `DeltaColumn` prints
    /// itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDeltaColumn")
            .field("len", &self.core.len())
            .field("slabs", &self.core.slabs().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{VDelta, VDeltaNum};
    use crate::DeltaValue;

    /// The verified conversions are the old API's (`DeltaValue`), and
    /// so are the bounds of what fits.
    fn agrees<T>(samples: &[T])
    where
        T: VDeltaNum + DeltaValue + std::fmt::Debug + PartialEq,
        Option<T>: DeltaValue + std::fmt::Debug + PartialEq,
    {
        assert_eq!(<T as VDeltaNum>::lo(), <T as DeltaValue>::MIN_I64);
        assert_eq!(<T as VDeltaNum>::hi(), <T as DeltaValue>::MAX_I64);
        assert_eq!(<T as VDelta>::lo(), <T as DeltaValue>::MIN_I64);
        assert_eq!(<T as VDelta>::hi(), <T as DeltaValue>::MAX_I64);
        assert_eq!(
            <Option<T> as VDelta>::lo(),
            <Option<T> as DeltaValue>::MIN_I64
        );
        assert_eq!(
            <Option<T> as VDelta>::hi(),
            <Option<T> as DeltaValue>::MAX_I64
        );
        assert_eq!(
            <Option<T> as VDelta>::to_i64(None),
            DeltaValue::to_i64(None::<T>)
        );
        assert_eq!(
            <Option<T> as VDelta>::from_i64(None),
            <Option<T> as DeltaValue>::null_value()
        );
        for &v in samples {
            let n = DeltaValue::try_to_i64(v).expect("a sample that fits");
            assert_eq!(<T as VDeltaNum>::to_i64(v), n);
            assert_eq!(<T as VDelta>::to_i64(v), DeltaValue::to_i64(v));
            assert_eq!(
                <Option<T> as VDelta>::to_i64(Some(v)),
                DeltaValue::to_i64(Some(v))
            );
            assert_eq!(
                <T as VDeltaNum>::from_i64(n),
                <T as DeltaValue>::from_i64(n)
            );
            assert_eq!(<T as VDeltaNum>::from_i64(n), v);
            assert_eq!(<T as VDelta>::from_i64(Some(n)), v);
            assert_eq!(<Option<T> as VDelta>::from_i64(Some(n)), Some(v));
        }
    }

    #[test]
    fn conversions_agree_with_the_old_api() {
        agrees::<u32>(&[0, 1, 77, u32::MAX - 1, u32::MAX]);
        agrees::<u64>(&[0, 1, 1 << 40, i64::MAX as u64 - 1, i64::MAX as u64]);
        agrees::<i32>(&[i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX]);
        agrees::<i64>(&[i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX]);
        let top = if usize::BITS >= 64 {
            i64::MAX as usize
        } else {
            usize::MAX
        };
        agrees::<usize>(&[0, 1, 4096, top - 1, top]);
    }
}
