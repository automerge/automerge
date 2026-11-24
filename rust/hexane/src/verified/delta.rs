//! Delta columns of the verified public API: the numbers a delta
//! column holds ([`VDelta`]), how the values of the column underneath
//! are deltas (`VDeltas`, in place of the old API's trusted
//! `delta::indexed::DB`), sources of values that say what they give
//! (`SliceValues`, `SliceItems`), so that what the verified cores build
//! from them is known, and the column ([`DeltaColumn`]). The
//! column's iterator ([`DeltaIter`]) is in `delta/iter.rs`.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`delta/imp.rs`), which has
//! its contract word for word and is proved there.

mod copy;
mod dec;
#[cfg(verus_keep_ghost)]
mod diffs;
mod edit;
mod enc;
mod find;
mod iter;

pub use dec::DeltaDecoder;
#[cfg(verus_keep_ghost)]
pub use diffs::{
    delta_at, delta_prefix, delta_run_prefix, delta_run_units, delta_runs_at, delta_units,
    delta_units_of_runs, deltas, lemma_delta_run_at, lemma_delta_runs_at,
};
pub use edit::DeltaEdit;
#[cfg(verus_keep_ghost)]
pub use enc::{delta_iter_fits, delta_near, delta_span_ok};
pub use enc::{DeltaEncoder, DeltaEncoderState};
pub use find::FindByRange;
#[cfg(verus_keep_ghost)]
pub use find::{delta_finds, lemma_delta_finds, lemma_delta_finds_first, lemma_delta_within_one};
pub use iter::{DeltaIter, DeltaIterState, DeltaRuns};

use super::codec::Codec;
#[cfg(verus_keep_ghost)]
use super::fill_of;
use super::model::Model;
#[cfg(verus_keep_ghost)]
use super::only;
use super::seal;
use super::value::{RleValue, VOps, VOptOps, VOptWrite, VWrite};
#[cfg(verus_keep_ghost)]
use super::VGet;
#[cfg(verus_keep_ghost)]
use super::{after_call, after_edit, gives};
use super::{ColumnValueRef, VSpec, MAX_SEG};
#[cfg(verus_keep_ghost)]
use crate::cursor::{ReadsAgree, Slabs, Visitor, Weigh};
#[cfg(verus_keep_ghost)]
use crate::delta::agg::DeltaOps;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::rvals;
#[cfg(verus_keep_ghost)]
use crate::delta::ddomain::{
    BoundedItems, BoundedValues, DeltaWrite, SpliceDeltas, SpliceItems, SpliceValues, ValueItems,
};
#[cfg(verus_keep_ghost)]
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::weights::LoadWeigh;
use crate::{DeltaRun, Leb128, LoadOpts, MaybeFill, PackError};
#[cfg(verus_keep_ghost)]
use std::ops::Bound;
use std::ops::Range;
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;
#[allow(unused_imports)]
pub(crate) use imp::{bounds_of, DKind, RunItems, SliceItems, SliceValues, VDeltas};
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{
    lemma_bound, lemma_delta_runs_dom, lemma_delta_runs_none, lemma_items_cat, lemma_kept,
    lemma_run_item, lemma_run_items, lemma_run_items_front, lemma_value_items, run_item, run_items,
    value_items,
};

verus! {

// ── The numbers of a delta column ───────────────────────────────────────────

/// A number a delta column can hold: `u32`, `u64`, `i32`, `i64` or
/// `usize`. A delta column keeps its values' running sums as `i64`s, so
/// of the wider types it holds the numbers up to `i64::MAX` only
/// (`fits`).
pub trait VDeltaNum: RleValue + seal::DeltaSeal {
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

    /// A number that fits is ordered (`RleValue::num`) as the `i64` it is
    /// kept as, and one that does not fit comes after all that do.
    #[doc(hidden)]
    proof fn lemma_fit_order(v: Self)
        ensures
            v.fits() ==> Self::num(v) == v.n64(),
            !v.fits() ==> Self::num(v) > i64::MAX,
    ;

    /// The number as an `i64`, if it fits.
    #[doc(hidden)]
    fn try_to_i64(self) -> (r: Option<i64>)
        ensures
            r == if self.fits() {
                Some(self.n64())
            } else {
                None::<i64>
            },
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

    proof fn lemma_fit_order(v: u32) {
    }

    #[inline(always)]
    fn try_to_i64(self) -> Option<i64> {
        Some(self as i64)
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

    proof fn lemma_fit_order(v: u64) {
    }

    #[inline(always)]
    fn try_to_i64(self) -> Option<i64> {
        imp::vdeltanum_try_to_i64_u64(self)
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

    proof fn lemma_fit_order(v: i32) {
    }

    #[inline(always)]
    fn try_to_i64(self) -> Option<i64> {
        Some(self as i64)
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

    proof fn lemma_fit_order(v: i64) {
    }

    #[inline(always)]
    fn try_to_i64(self) -> Option<i64> {
        Some(self)
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

    proof fn lemma_fit_order(v: usize) {
    }

    #[inline(always)]
    fn try_to_i64(self) -> Option<i64> {
        imp::vdeltanum_try_to_i64_usize(self)
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
        imp::vdeltanum_hi_usize()
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
pub trait VDelta: VSpec<S = Self> + Copy + seal::DeltaValSeal {
    /// What a load fills a delta column of the type with
    /// ([`DeltaColumn::load_with`]): the difference between
    /// each value and the one before it, an `i64`; for a type with a
    /// null an `Option<i64>`, of which none fills with nulls.
    type Fill: Copy;

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

    /// Values that fit are in the order (`VSpec::below`) of what the
    /// column keeps them as ([`delta_below`]), and each comes before
    /// every value that does not fit.
    #[doc(hidden)]
    proof fn lemma_order(a: Self, b: Self)
        ensures
            a.fits() && b.fits() ==> Self::below(a, b) == delta_below(a.dv(), b.dv()),
            a.fits() && !b.fits() ==> Self::below(a, b),
    ;

    /// The value as the column keeps it, if it fits.
    #[doc(hidden)]
    fn try_i64(self) -> (r: Option<Option<i64>>)
        ensures
            r == if self.fits() {
                Some(self.dv())
            } else {
                None::<Option<i64>>
            },
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

    /// A fill as the difference it is, or none for nulls.
    #[doc(hidden)]
    spec fn fill_delta(f: Self::Fill) -> Option<i64>;

    #[doc(hidden)]
    fn has_null() -> (r: bool)
        ensures
            r == Self::nullable(),
    ;
}

/// What a delta column of `T` can keep: a number from the least to the
/// greatest that fits, or a null where `T` has one.
#[doc(hidden)]
pub open spec fn in_dom<T: VDelta>(d: Option<i64>) -> bool {
    dom(d, T::least(), T::most(), T::nullable())
}

/// Whether `a` comes before `b`, of what a delta column keeps: numbers
/// in their order, and a null before every number.
#[doc(hidden)]
pub open spec fn delta_below(a: Option<i64>, b: Option<i64>) -> bool {
    match (a, b) {
        (None, Some(_)) => true,
        (Some(x), Some(y)) => x < y,
        _ => false,
    }
}

/// Whether the value `v` of a delta column is within the bounds `lo`
/// and `hi`: a number, not a null, that comes at `lo` or after it and
/// at `hi` or before it in the order of the type's values
/// (`VSpec::below`); not at a bound that is `Excluded`, and with no
/// limit where one is `Unbounded`. A bound can be any value of the
/// type: a null, which comes before every number, and a number too
/// large for a delta column, which comes after all it keeps.
pub open spec fn delta_within<T: VDelta>(lo: Bound<&T>, hi: Bound<&T>, v: T) -> bool {
    &&& v.dv() is Some
    &&& match lo {
        Bound::Included(a) => !T::below(v, *a),
        Bound::Excluded(a) => T::below(*a, v),
        Bound::Unbounded => true,
    }
    &&& match hi {
        Bound::Included(b) => !T::below(*b, v),
        Bound::Excluded(b) => T::below(v, *b),
        Bound::Unbounded => true,
    }
}

impl<T: VDeltaNum> VDelta for T {
    type Fill = i64;

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
        imp::vdelta_lemma_delta_t::<T>(v, d)
    }

    #[inline(always)]
    fn to_i64(self) -> Option<i64> {
        Some(<T as VDeltaNum>::to_i64(self))
    }

    proof fn lemma_order(a: T, b: T) {
        imp::vdelta_lemma_order_t::<T>(a, b)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn try_i64(self) -> Option<Option<i64>> {
        imp::vdelta_try_i64_t::<T>(self)
    }

    #[inline(always)]
    fn from_i64(d: Option<i64>) -> T {
        imp::vdelta_from_i64_t::<T>(d)
    }

    #[inline(always)]
    fn lo() -> i64 {
        <T as VDeltaNum>::lo()
    }

    #[inline(always)]
    fn hi() -> i64 {
        <T as VDeltaNum>::hi()
    }

    open spec fn fill_delta(f: i64) -> Option<i64> {
        Some(f)
    }

    #[inline(always)]
    fn has_null() -> bool {
        false
    }
}

impl<T: VDeltaNum> VDelta for Option<T> {
    type Fill = Option<i64>;

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
        imp::vdelta_lemma_delta_option_t::<T>(v, d)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn to_i64(self) -> Option<i64> {
        imp::vdelta_to_i64_option_t::<T>(self)
    }

    proof fn lemma_order(a: Option<T>, b: Option<T>) {
        imp::vdelta_lemma_order_option_t::<T>(a, b)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn try_i64(self) -> Option<Option<i64>> {
        imp::vdelta_try_i64_option_t::<T>(self)
    }

    #[inline(always)]
    #[allow(clippy::manual_map)]  // a match is what the proof follows
    fn from_i64(d: Option<i64>) -> Option<T> {
        imp::vdelta_from_i64_option_t::<T>(d)
    }

    #[inline(always)]
    fn lo() -> i64 {
        <T as VDeltaNum>::lo()
    }

    #[inline(always)]
    fn hi() -> i64 {
        <T as VDeltaNum>::hi()
    }

    open spec fn fill_delta(f: Option<i64>) -> Option<i64> {
        f
    }

    #[inline(always)]
    fn has_null() -> bool {
        true
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


/// What a delta column keeps for the run `r`: `r.count` numbers, the
/// first `r.prefix + r.delta` and each of the others `r.delta` more
/// than the one before it (wrapping, as an `i64` does); or `r.count`
/// nulls, where `r.delta` is none.
#[doc(hidden)]
pub open spec fn delta_run_kept(r: DeltaRun) -> Seq<Option<i64>> {
    Seq::new(
        r.count as nat,
        |k: int|
            match r.delta {
                Some(d) => Some(vstd::wrapping::i64_specs::signed_crop(r.prefix + (k + 1) * d)),
                None => None,
            },
    )
}

/// What a delta column keeps for the runs `rs`: one run's values
/// after another's.
#[doc(hidden)]
pub open spec fn delta_runs_kept(rs: Seq<DeltaRun>) -> Seq<Option<i64>>
    decreases rs.len(),
{
    if rs.len() == 0 {
        Seq::empty()
    } else {
        delta_runs_kept(rs.drop_last()) + delta_run_kept(rs.last())
    }
}

/// The values of the run `r` in a delta column of `T`: `r.count` of
/// them, the first `r.prefix + r.delta` and each of the others
/// `r.delta` more than the one before it; or `r.count` nulls, where
/// `r.delta` is none. (`lemma_delta_run_exact` says which numbers they
/// are.)
pub open spec fn delta_run_values<T: VDelta>(r: DeltaRun) -> Seq<T> {
    vals::<T>(delta_run_kept(r))
}

/// The values of the runs `rs` in a delta column of `T`: one run's
/// after another's.
pub open spec fn delta_runs_values<T: VDelta>(rs: Seq<DeltaRun>) -> Seq<T> {
    vals::<T>(delta_runs_kept(rs))
}

/// Every value of the run `r` is one a delta column of `T` can keep.
pub open spec fn delta_run_fits<T: VDelta>(r: DeltaRun) -> bool {
    all_dom::<T>(delta_run_kept(r))
}

/// Every value of each of the runs `rs` is one a delta column of `T`
/// can keep.
pub open spec fn delta_runs_fit<T: VDelta>(rs: Seq<DeltaRun>) -> bool {
    forall|i: int| 0 <= i < rs.len() ==> delta_run_fits::<T>(#[trigger] rs[i])
}

/// The check [`DeltaColumn::splice_runs`](crate::DeltaColumn::splice_runs)
/// makes of a run, and a null run only where `T` has a null: a run of
/// no values passes; a run of numbers passes where `T` holds every
/// `i64`, or where its first value and its last, taken as integers, are
/// numbers of `T`. (The values of a run go one way, so its ends bound
/// them.)
pub open spec fn delta_run_in_range<T: VDelta>(r: DeltaRun) -> bool {
    r.count == 0 || match r.delta {
        Some(d) => (T::least() == i64::MIN && T::most() == i64::MAX) || {
            &&& T::least() <= r.prefix + d <= T::most()
            &&& T::least() <= r.prefix + d * r.count <= T::most()
        },
        None => T::nullable(),
    }
}

/// A run's values, where its first and its last are `i64`s as integers:
/// the `k`-th is `r.prefix` and `k + 1` times `r.delta`, an `i64` too.
pub proof fn lemma_delta_run_exact(r: DeltaRun, k: int)
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
    imp::lemma_delta_run_exact_impl(r, k)
}

/// A run that passes the check holds only values a delta column of `T`
/// can keep.
pub proof fn lemma_delta_run_fits<T: VDelta>(r: DeltaRun)
    requires
        delta_run_in_range::<T>(r),
    ensures
        delta_run_fits::<T>(r),
{
    imp::lemma_delta_run_fits_impl::<T>(r)
}

/// One more run adds its values.
pub proof fn lemma_delta_runs_push<T: VDelta>(rs: Seq<DeltaRun>, r: DeltaRun)
    ensures
        delta_runs_kept(rs.push(r)) == delta_runs_kept(rs) + delta_run_kept(r),
        delta_runs_values::<T>(rs.push(r)) == delta_runs_values::<T>(rs) + delta_run_values::<T>(r),
        delta_runs_fit::<T>(rs) && delta_run_fits::<T>(r) ==> delta_runs_fit::<T>(rs.push(r)),
{
    imp::lemma_delta_runs_push_impl::<T>(rs, r)
}

/// The values of one run alone are its own, and no runs have none.
pub proof fn lemma_delta_runs_one<T: VDelta>(r: DeltaRun)
    ensures
        delta_runs_values::<T>(seq![r]) == delta_run_values::<T>(r),
        delta_runs_values::<T>(Seq::empty()) == Seq::<T>::empty(),
        delta_runs_fit::<T>(seq![r]) == delta_run_fits::<T>(r),
        delta_runs_fit::<T>(Seq::empty()),
{
    imp::lemma_delta_runs_one_impl::<T>(r)
}

/// A value type of a delta column under codec `C`: a number
/// ([`VDeltaNum`]) or an `Option` of one.
#[allow(private_bounds)]
pub trait DeltaValue<C: Codec = Leb128>: DKind<C> {

}

impl<T: DKind<C>, C: Codec> DeltaValue<C> for T {

}

/// A delta column of the verified public API: a column of numbers, or
/// of `Option`s of them, kept as each one's difference from the one
/// before it.
pub struct DeltaColumn<T: DeltaValue<C>, C: Codec = Leb128> {
    core: DCore<T, C>,
}

impl<T: DeltaValue<C>, C: Codec> View for DeltaColumn<T, C> {
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

impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
    /// What the column keeps: each value as an `i64`, or none for a
    /// null.
    #[doc(hidden)]
    pub closed spec fn kept(&self) -> Seq<Option<i64>> {
        rvals::<DE<T, C>, DX<T, C>, <T as DKind<C>>::B>(self.core@)
    }

    /// The count of the column's edits. Each method that changes the
    /// column says what it is after the call: one more (`after_edit`)
    /// where the call was an edit, and what it was where the call was
    /// none.
    pub closed spec fn edits(&self) -> usize {
        self.core.count()
    }

    /// An empty column.
    #[inline(always)]
    pub fn new() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::new_impl()
    }

    /// An empty column whose slabs hold at most `max_segments` runs
    /// each.
    #[inline(always)]
    pub fn with_max_segments(max_segments: usize) -> (r: Self)
        requires
            max_segments >= 2,
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::with_max_segments_impl(max_segments)
    }

    /// A column of the values `values`.
    ///
    /// # Panics
    ///
    /// Panics if two of the values lie 2^63 or more apart: the
    /// difference between them is kept as an `i64`.
    #[inline(always)]
    pub fn from_values(values: &[T]) -> (r: Self)
        requires
            all_fit(values@),
        ensures
            r@ == values@,
    {
        Self::from_values_impl(values)
    }

    /// The number of values.
    #[inline(always)]
    pub fn len(&self) -> (n: usize)
        ensures
            n == self@.len(),
    {
        self.len_impl()
    }

    /// Whether the column has no values.
    #[inline(always)]
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.is_empty_impl()
    }

    /// Every value of the column is one a delta column can hold.
    pub proof fn lemma_fits(tracked &self)
        ensures
            all_fit(self@),
    {
        self.lemma_fits_impl()
    }

    /// Remove `del` values from index `at` on and put `values` in their
    /// place: as [`splice`](Self::splice) does with the values of a
    /// slice, each of which must be one the column can keep.
    ///
    /// The column counts an edit more (`after_edit` of its `edits()`),
    /// unless the call deletes no value and inserts none.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    #[inline(always)]
    pub fn splice_slice(&mut self, at: usize, del: usize, values: &[T])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            all_fit(values@),
        ensures
            final(self)@ == old(self)@.take(at as int) + values@ + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || values@.len() > 0),
    {
        self.splice_slice_impl(at, del, values)
    }

    /// Remove `del` values from index `at` on and put in their place the
    /// values that `values` gives, as an iterator.
    ///
    /// The specification speaks of what the iterator gives (`gives`), as
    /// [`Column::splice`](super::Column::splice)'s does: of the values it
    /// gives, each is one the column can keep (`fits`), and they take the
    /// place of the deleted ones.
    ///
    /// The column counts an edit more unless the call deletes no value
    /// and inserts none.
    ///
    /// # Panics
    ///
    /// Panics if the iterator gives a value the column cannot keep (one
    /// that does not `fit`), before the column is changed, or if two of
    /// the column's values would lie 2^63 or more apart.
    #[inline(always)]
    pub fn splice<I: IntoIterator<Item = T>>(&mut self, at: usize, del: usize, values: I)
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
        ensures
            gives(values, |xs: Seq<T>| all_fit(xs) && final(self)@ == old(self)@.take(at as int) + xs + old(self)@.skip(at + del)),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        self.splice_impl::<I>(at, del, values)
    }

    /// Whether the run `run` passes the check
    /// [`DeltaColumn::splice_runs`](crate::DeltaColumn::splice_runs)
    /// makes of its runs (`delta_run_in_range`), a null run passing
    /// only where `T` has a null. A run that does holds only values a
    /// delta column of `T` can keep, and
    /// [`splice_runs`](Self::splice_runs) takes it.
    #[inline(always)]
    pub fn run_fits(run: &DeltaRun) -> (r: bool)
        ensures
            r == delta_run_in_range::<T>(*run),
            r ==> delta_run_fits::<T>(*run),
    {
        Self::run_fits_impl(run)
    }

    /// Remove `del` values from index `at` on and put the values of
    /// the runs `runs` in their place (`delta_runs_values`): of each
    /// run its `count` values, the first `prefix + delta` and each of
    /// the others `delta` more than the one before it, or `count` nulls
    /// where `delta` is none. A run of no values adds none.
    ///
    /// Every value of the runs must be one the column can keep
    /// (`delta_runs_fit`): [`run_fits`](Self::run_fits) checks a run as
    /// [`DeltaColumn::splice_runs`](crate::DeltaColumn::splice_runs)
    /// does, which panics where this method has a `requires`.
    ///
    /// The column counts an edit more (`after_edit` of its `edits()`),
    /// unless the call deletes no value and its length is what it was.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    #[inline(always)]
    pub fn splice_runs(&mut self, at: usize, del: usize, runs: &[DeltaRun])
        requires
            at <= old(self)@.len(),
            del <= old(self)@.len() - at,
            delta_runs_fit::<T>(runs@),
        ensures
            final(self)@ == old(self)@.take(at as int) + delta_runs_values::<T>(runs@) + old(self)@.skip(at + del),
            final(self).edits() == after_call(old(self).edits(), del > 0 || final(self)@.len() != old(self)@.len()),
    {
        self.splice_runs_impl(at, del, runs)
    }

    /// The value at `index`, or `None` past the end.
    #[inline(always)]
    pub fn get(&self, index: usize) -> (r: Option<T>)
        ensures
            index < self@.len() ==> r == Some(self@[index as int]),
            index >= self@.len() ==> r is None,
    {
        self.get_impl(index)
    }

    /// How many slabs hold the column. That is a fact of how the column
    /// lies in memory and not of its values: nothing is specified of it.
    #[inline(always)]
    pub fn slab_count(&self) -> usize {
        self.slab_count_impl()
    }

    /// Insert `value` before the value at `at`. The column counts an
    /// edit more.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    #[inline(always)]
    pub fn insert(&mut self, at: usize, value: T)
        requires
            at <= old(self)@.len(),
            value.fits(),
        ensures
            final(self)@ == old(self)@.insert(at as int, value),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.insert_impl(at, value)
    }

    /// Add `value` at the end. The column counts an edit more.
    ///
    /// # Panics
    ///
    /// Panics if two of the column's values would lie 2^63 or more
    /// apart.
    #[inline(always)]
    pub fn push(&mut self, value: T)
        requires
            value.fits(),
        ensures
            final(self)@ == old(self)@.push(value),
            final(self).edits() == after_edit(old(self).edits()),
    {
        self.push_impl(value)
    }

    /// Delete the value at `at`, where there is one: then the column
    /// counts an edit more. Past the end there is nothing to delete, and
    /// the call is no edit, as it is none of
    /// [`DeltaColumn`](crate::DeltaColumn).
    #[inline(always)]
    pub fn remove(&mut self, at: usize)
        ensures
            final(self)@ == if at < old(self)@.len() {
                old(self)@.remove(at as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), at < old(self)@.len()),
    {
        self.remove_impl(at)
    }

    /// Delete the `n` values at `at`. The column counts an edit more,
    /// unless `n` is 0: then the call does nothing, wherever `at` is.
    #[inline(always)]
    pub fn remove_n(&mut self, at: usize, n: usize)
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
        self.remove_n_impl(at, n)
    }

    /// Keep the first `len` values, where there are more: then the
    /// column counts an edit more, and where there are no more it counts
    /// what it did.
    #[inline(always)]
    pub fn truncate(&mut self, len: usize)
        ensures
            final(self)@ == if len < old(self)@.len() {
                old(self)@.take(len as int)
            } else {
                old(self)@
            },
            final(self).edits() == after_call(old(self).edits(), len < old(self)@.len()),
    {
        self.truncate_impl(len)
    }

    /// Delete every value. The column keeps its segment budget.
    ///
    /// Of a column with no value this is no edit, as it is none of
    /// [`DeltaColumn`](crate::DeltaColumn). Of any other the column
    /// counts an edit more.
    #[inline(always)]
    pub fn clear(&mut self)
        ensures
            final(self)@ == Seq::<T>::empty(),
            final(self).edits() == after_call(old(self).edits(), old(self)@.len() > 0),
    {
        self.clear_impl()
    }

    /// Take the last value off the column, if it has one: then the
    /// column counts an edit more.
    #[inline(always)]
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
        self.pop_impl()
    }

    /// The first value, or `None` of an empty column.
    #[inline(always)]
    pub fn first(&self) -> (r: Option<T>)
        ensures
            self@.len() > 0 ==> r == Some(self@.first()),
            self@.len() == 0 ==> r is None,
    {
        self.first_impl()
    }

    /// The last value, or `None` of an empty column.
    #[inline(always)]
    pub fn last(&self) -> (r: Option<T>)
        ensures
            self@.len() > 0 ==> r == Some(self@.last()),
            self@.len() == 0 ==> r is None,
    {
        self.last_impl()
    }

    /// The values, in a `Vec`.
    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    #[inline(always)]
    pub fn to_vec(&self) -> (r: Vec<T>)
        ensures
            r@ == self@,
    {
        self.to_vec_impl()
    }
}

// The saved form is the column's underneath, of `i64`s or of
// `Option<i64>`s: the values' deltas, each value one of the type's.
impl<T: DeltaValue<C>, C: Codec> DeltaColumn<T, C> {
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
        Self::lemma_save_unique_impl(x, y)
    }

    /// A saved delta column is the encoding of its values.
    pub proof fn lemma_encoding(d: Seq<u8>)
        requires
            Self::valid(d),
        ensures
            Self::encoding(Self::values(d)) == d,
    {
        Self::lemma_encoding_impl(d)
    }

    /// No bytes are a saved delta column: the one of no values.
    pub proof fn lemma_no_bytes()
        ensures
            Self::valid(Seq::<u8>::empty()),
            Self::values(Seq::<u8>::empty()) == Seq::<T>::empty(),
    {
        Self::lemma_no_bytes_impl()
    }

    /// The column, saved: the encoding of its values.
    #[inline(always)]
    pub fn save(&self) -> (r: Vec<u8>)
        ensures
            Self::valid(r@),
            Self::values(r@) == self@,
            r@ == Self::encoding(self@),
    {
        self.save_impl()
    }

    /// Save the column after what `out` holds: `out` is then what it
    /// was, and after that the encoding of the column's values. The
    /// range returned is where in `out` that is.
    #[inline(always)]
    pub fn save_to(&self, out: &mut Vec<u8>) -> (r: Range<usize>)
        ensures
            final(out)@ == old(out)@ + Self::encoding(self@),
            Self::valid(Self::encoding(self@)),
            Self::values(Self::encoding(self@)) == self@,
            r.start == old(out)@.len(),
            r.end == final(out)@.len(),
    {
        self.save_to_impl(out)
    }

    /// Whether every value of the column is `value`, which is to be a
    /// value of the column's type (`fits`). So it is of an empty column.
    /// The column's runs are read,
    /// not each of its values.
    #[allow(clippy::while_let_loop)]  // a loop with its invariants
    #[inline(always)]
    pub fn is_only(&self, value: T) -> (r: bool)
        requires
            value.fits(),
        ensures
            r == only(self@, value),
    {
        self.is_only_impl(value)
    }

    /// [`save_to`](Self::save_to), unless every value of the column is
    /// `value` (a value of the column's type, `fits`): then nothing is
    /// written, and the range is empty. So is it for an empty column.
    #[inline(always)]
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
        self.save_to_unless_impl(out, value)
    }

    /// The delta column saved as `bytes`. An error unless `bytes` is a
    /// saved delta column of `T`: the load checks, as it reads the
    /// runs, that each value they stand for is one of the type's.
    ///
    /// What is not said: that a saved delta column loads. The checks
    /// are made of sums kept as the runs pass, and those are not
    /// proved of the bytes.
    #[inline(always)]
    pub fn load(bytes: &[u8]) -> (r: Result<Self, PackError>)
        ensures
            r matches Ok(c) ==> Self::valid(bytes@) && c@ == Self::values(bytes@),
    {
        Self::load_impl(bytes)
    }

    /// [`load`](Self::load) with options ([`LoadOpts`]): how many
    /// values the column is to have, what to fill it with, and how many
    /// runs a slab may hold.
    ///
    /// The fill of a delta column is a difference
    /// ([`VDelta::Fill`]: an `i64`, and an `Option` of one for a type
    /// with a null), as it is for
    /// [`DeltaColumn::load_with`](crate::DeltaColumn::load_with). With
    /// a length and a fill, empty `bytes` load as the run of that many
    /// values after 0, each the fill more than the one before it (for
    /// the fill `d` the values `d`, `2d`, `3d`, …; for no number, that
    /// many nulls: `delta_run_values`), if a column can be that long
    /// (`fill_of`). If it cannot, or with no fill, `bytes` load as what
    /// they are; and a column of another length than the one asked for
    /// is an error, as empty `bytes` are for every length but 0.
    ///
    /// What is not said: that the load succeeds. A fill is an error
    /// where its values are not all the type's, or as integers leave
    /// `i64`; and of `bytes` the same is said as [`load`](Self::load)
    /// says.
    #[inline(always)]
    pub fn load_with<F: MaybeFill<T::Fill>>(bytes: &[u8], opts: LoadOpts<F>) -> (r: Result<Self, PackError>)
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
        Self::load_with_impl::<F>(bytes, opts)
    }
}

impl<T: DeltaValue<C>, C: Codec> Default for DeltaColumn<T, C> {
    /// An empty column.
    fn default() -> (r: Self)
        ensures
            r@ == Seq::<T>::empty(),
    {
        Self::new()
    }
}

impl<T: DeltaValue<C>, C: Codec> Clone for DeltaColumn<T, C> {
    /// A column of its own with the same values, which counts the edits
    /// this one does.
    #[inline(always)]
    fn clone(&self) -> (r: Self)
        ensures
            r@ == self@,
            r.edits() == self.edits(),
    {
        self.clone_impl()
    }
}

} // verus!
