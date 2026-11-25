//! The sums of a column's values, for the prefix columns, verified with
//! Verus.
//!
//! A prefix column answers what the values before a position add up to.
//! The sums are of this crate's integer types ([`Sum`]), and the laws
//! the prefix core asks of their arithmetic (`+` is associative, and `-`
//! undoes it) are *proved* of them.
//!
//! What a value adds to a sum is an integer (`PrefixValue::amount`), and
//! the sum of some values is the sum of those integers (`summed`): a
//! mathematical integer, which no arithmetic of a machine type enters.
//! A column keeps its sums in a machine type all the same, and adds
//! them with wrapping arithmetic. The two agree wherever the true sums
//! fit the type (`sums_fit`), and a prefix column keeps its values so
//! that they do. For most value types that asks nothing: no column is
//! long enough for a sum of `u64`s to leave a `u128`. A sum of `u32`s
//! is kept in a `u64`, which a column of more than 2^32 + 1 of them can
//! overflow, and such a column is checked.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`sums/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
use super::seal;
use super::value::RleValue;
#[cfg(verus_keep_ghost)]
use super::Held;
#[cfg(verus_keep_ghost)]
use super::VGet;
use super::{ColumnValueRef, Form, Get, VSpec};
#[cfg(verus_keep_ghost)]
use crate::attrib::Accumulate;
#[cfg(verus_keep_ghost)]
use crate::btree::{OrdPrefix, PrefixAggregate, SlabAggregate};
#[cfg(verus_keep_ghost)]
use crate::cursor::ReadsAgree;
#[cfg(verus_keep_ghost)]
use crate::cursor::{Slabs, Weigh};
#[cfg(verus_keep_ghost)]
use crate::fenwick::sum;
#[cfg(verus_keep_ghost)]
use crate::index::IndexOps;
#[cfg(verus_keep_ghost)]
use crate::iter_core::each;
#[cfg(verus_keep_ghost)]
use crate::iter_core::{Decode, Decoders};
#[cfg(verus_keep_ghost)]
use crate::prefix_core::{PrefixOps, PrefixWeight};
#[cfg(verus_keep_ghost)]
use crate::prefix_search::PrefixOrd;
#[cfg(verus_keep_ghost)]
use crate::weights::{CopyWeigh, LoadWeigh};
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::ops::{Add, AddAssign, SubAssign};
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{lemma_nsum, lemma_sum_diff, lemma_svs_slabs, lemma_vsum};
#[allow(unused_imports)]
pub(crate) use imp::{SumModel, SumOf, SumWeight};

verus! {

// ── The types sums are kept in ──────────────────────────────────────────────

/// A type a prefix column keeps its sums in: `u64`, `u128`, `usize` or
/// `i128`. The types are this crate's: no other can implement this
/// trait.
///
/// A sum is an integer, `as_int`, between `lo` and `hi`. The type's
/// arithmetic wraps: what it gives for an integer outside those bounds
/// is `of_int` of the integer, which is the integer itself wherever it
/// is within them.
pub trait Sum: Copy + Default + Debug + AddAssign + SubAssign + Add<Output = Self> + for <'x>AddAssign<
    &'x Self,
> + seal::SumSeal {
    /// The sum, as an integer.
    spec fn as_int(self) -> int;

    /// The least sum of the type.
    spec fn lo() -> int;

    /// The greatest sum of the type.
    spec fn hi() -> int;

    /// The integer `x` as the type has it: `x` itself where it is
    /// between `lo` and `hi`, and otherwise the integer between them
    /// that differs from `x` by a multiple of the number of the type's
    /// values.
    spec fn of_int(x: int) -> Self;

    /// A sum is between the bounds, and is its integer. Zero is a sum.
    proof fn lemma_as_int(p: Self)
        ensures
            Self::lo() <= p.as_int() <= Self::hi(),
            Self::of_int(p.as_int()) == p,
            Self::lo() <= 0 <= Self::hi(),
    ;

    /// An integer between the bounds is itself as a sum, and adding to
    /// a wrapped integer is adding to the integer.
    proof fn lemma_of_int(x: int, y: int)
        ensures
            Self::lo() <= x <= Self::hi() ==> Self::of_int(x).as_int() == x,
            Self::of_int(Self::of_int(x).as_int() + y) == Self::of_int(x + y),
    ;

    /// Zero.
    #[doc(hidden)]
    fn zero() -> (r: Self)
        ensures
            r == Self::of_int(0),
        no_unwind
    ;

    /// `self + b`, wrapping.
    #[doc(hidden)]
    fn w_add(self, b: Self) -> (r: Self)
        ensures
            r == Self::of_int(self.as_int() + b.as_int()),
        no_unwind
    ;

    /// `self - b`, wrapping.
    #[doc(hidden)]
    fn w_sub(self, b: Self) -> (r: Self)
        ensures
            r == Self::of_int(self.as_int() - b.as_int()),
    ;

    /// `self * n`, wrapping.
    #[doc(hidden)]
    fn w_mul(self, n: usize) -> (r: Self)
        ensures
            r == Self::of_int(self.as_int() * n),
    ;

    /// `self + b`, where that is a sum of the type.
    #[doc(hidden)]
    fn c_add(self, b: Self) -> (r: Option<Self>)
        ensures
            match r {
                Some(x) => x.as_int() == self.as_int() + b.as_int(),
                None => !(Self::lo() <= self.as_int() + b.as_int() <= Self::hi()),
            },
    ;

    /// `self * n`, where that is a sum of the type.
    #[doc(hidden)]
    fn c_mul(self, n: usize) -> (r: Option<Self>)
        ensures
            match r {
                Some(x) => x.as_int() == self.as_int() * n,
                None => !(Self::lo() <= self.as_int() * n <= Self::hi()),
            },
    ;

    /// `self <= b`.
    #[doc(hidden)]
    fn le(self, b: Self) -> (r: bool)
        ensures
            r == (self.as_int() <= b.as_int()),
    ;
}

/// A type of sums that are never negative: `u64`, `u128` or `usize`.
/// The sums of values of such a type do not fall as more are added, so
/// a prefix column of them can be searched by sum
/// ([`get_index_for_prefix`](super::Column::get_index_for_prefix)).
pub trait UnsignedPrefix: Sum {
    /// The least sum of the type is zero.
    proof fn lemma_count()
        ensures
            Self::lo() == 0,
    ;

    /// `self / b`, rounded up, where neither is zero.
    #[doc(hidden)]
    fn div_up(self, b: Self) -> (r: Self)
        ensures
            self.as_int() > 0 && b.as_int() > 0 ==> r.as_int() == (self.as_int() - 1) / b.as_int() + 1,
    ;

    /// The sum as a `usize`, where it is one.
    #[doc(hidden)]
    fn as_len(self) -> (r: Option<usize>)
        ensures
            self.as_int() <= usize::MAX ==> r == Some(self.as_int() as usize),
    ;

    /// One.
    #[doc(hidden)]
    fn one() -> (r: Self)
        ensures
            r.as_int() == 1,
    ;
}

} // verus!
verus! {

/// The integers of an `i128` are those of either sign, and it wraps as
/// two's complement does.
impl Sum for i128 {
    open spec fn as_int(self) -> int {
        self as int
    }

    open spec fn lo() -> int {
        i128::MIN as int
    }

    open spec fn hi() -> int {
        i128::MAX as int
    }

    open spec fn of_int(x: int) -> i128 {
        vstd::wrapping::i128_specs::signed_crop(x)
    }

    proof fn lemma_as_int(p: i128) {
        lemma_wrap(p as int, 0, u128::MAX + 1);
    }

    proof fn lemma_of_int(x: int, y: int) {
        lemma_wrap(x, y, u128::MAX + 1);
    }

    #[inline(always)]
    fn zero() -> i128 {
        imp::vsum_zero_i128()
    }

    #[inline(always)]
    fn w_add(self, b: i128) -> i128 {
        imp::vsum_w_add_i128(self, b)
    }

    #[inline(always)]
    fn w_sub(self, b: i128) -> i128 {
        imp::vsum_w_sub_i128(self, b)
    }

    #[inline(always)]
    fn w_mul(self, n: usize) -> i128 {
        self.wrapping_mul(n as i128)
    }

    #[inline(always)]
    fn c_add(self, b: i128) -> Option<i128> {
        self.checked_add(b)
    }

    #[inline(always)]
    fn c_mul(self, n: usize) -> Option<i128> {
        self.checked_mul(n as i128)
    }

    #[inline(always)]
    fn le(self, b: i128) -> bool {
        self <= b
    }
}

// ── What a value adds to a sum ──────────────────────────────────────────────

/// A number type whose values a prefix column can add up: what a value
/// adds, and the type the sums are kept in.
///
/// It is given here for `u64` (summed in a `u128`), `i64` (in an
/// `i128`), and `u32` and `NonZeroU32` (in a `u64`).
///
/// A value adds an integer, `amount_of`, of size at most `amount_max`.
/// The sums of any `room` values then fit `Sum`, which `lemma_room`
/// asks to be proved. A column of no more values than that is not
/// checked, and a longer one is: so a type whose `room` is every length
/// a column can have (`usize::MAX`) is never checked, and a type summed
/// in a signed type must be one of those. An edit of a longer column
/// reads the values it put in, adds their sum to the column's, and only
/// where that does not fit reads the whole column.
pub trait Amount: RleValue {
    /// The type the sums are kept in.
    type Sum: Sum;

    /// What the value `v` adds to a sum.
    spec fn amount_of(v: Self) -> int;

    /// No value adds more than this, or less than minus this.
    spec fn amount_max() -> nat;

    /// The sums of this many values, or fewer, fit `Sum`.
    spec fn room() -> nat;

    /// What a value adds is within the bound, and is not negative
    /// where the sums are of an unsigned type.
    proof fn lemma_amount_of(v: Self)
        ensures
            -(Self::amount_max() as int) <= Self::amount_of(v) <= Self::amount_max(),
            Self::Sum::lo() == 0 ==> Self::amount_of(v) >= 0,
    ;

    /// `room` values, each as large as can be, add up to a sum of the
    /// type; and only sums of an unsigned type have less room than
    /// every column needs.
    proof fn lemma_room()
        ensures
            Self::room() >= 1,
            Self::room() * Self::amount_max() <= Self::Sum::hi(),
            Self::Sum::lo() == 0 || -(Self::room() * Self::amount_max()) >= Self::Sum::lo(),
            Self::Sum::lo() == 0 || Self::room() >= usize::MAX,
    ;

    /// `room`, as a `usize`.
    fn room_len() -> (n: usize)
        ensures
            n == Self::room(),
    ;

    /// What `v` adds, as a sum. It does not unwind (`no_unwind`): a
    /// prefix column's iterator adds it to its sum as it moves, in place.
    fn part_of(v: Self) -> (r: Self::Sum)
        ensures
            r.as_int() == Self::amount_of(v),
        no_unwind
    ;
}

impl Amount for u64 {
    type Sum = u128;

    open spec fn amount_of(v: u64) -> int {
        v as int
    }

    open spec fn amount_max() -> nat {
        u64::MAX as nat
    }

    open spec fn room() -> nat {
        usize::MAX as nat
    }

    proof fn lemma_amount_of(v: u64) {
    }

    proof fn lemma_room() {
        imp::vamount_lemma_room_u64()
    }

    #[inline(always)]
    fn room_len() -> usize {
        usize::MAX
    }

    #[inline(always)]
    fn part_of(v: u64) -> u128 {
        v as u128
    }
}

impl Amount for i64 {
    type Sum = i128;

    open spec fn amount_of(v: i64) -> int {
        v as int
    }

    open spec fn amount_max() -> nat {
        0x8000_0000_0000_0000
    }

    open spec fn room() -> nat {
        usize::MAX as nat
    }

    proof fn lemma_amount_of(v: i64) {
    }

    proof fn lemma_room() {
        imp::vamount_lemma_room_i64()
    }

    #[inline(always)]
    fn room_len() -> usize {
        usize::MAX
    }

    #[inline(always)]
    fn part_of(v: i64) -> i128 {
        v as i128
    }
}

/// How many `u32`s cannot overflow a `u64`: 2^32 + 1 of them, for
/// (2^32 + 1)(2^32 - 1) is `u64::MAX`; or every length there is, where
/// that is fewer.
pub open spec fn u32_room() -> nat {
    if usize::MAX - u32::MAX >= 2 {
        (u32::MAX + 2) as nat
    } else {
        usize::MAX as nat
    }
}

impl Amount for u32 {
    type Sum = u64;

    open spec fn amount_of(v: u32) -> int {
        v as int
    }

    open spec fn amount_max() -> nat {
        u32::MAX as nat
    }

    open spec fn room() -> nat {
        u32_room()
    }

    proof fn lemma_amount_of(v: u32) {
    }

    proof fn lemma_room() {
        lemma_u32_room();
    }

    #[inline(always)]
    fn room_len() -> usize {
        u32_room_len()
    }

    #[inline(always)]
    fn part_of(v: u32) -> u64 {
        v as u64
    }
}

impl Amount for NonZeroU32 {
    type Sum = u64;

    open spec fn amount_of(v: NonZeroU32) -> int {
        v@ as int
    }

    open spec fn amount_max() -> nat {
        u32::MAX as nat
    }

    open spec fn room() -> nat {
        u32_room()
    }

    proof fn lemma_amount_of(v: NonZeroU32) {
    }

    proof fn lemma_room() {
        lemma_u32_room();
    }

    #[inline(always)]
    fn room_len() -> usize {
        u32_room_len()
    }

    #[inline(always)]
    fn part_of(v: NonZeroU32) -> u64 {
        imp::vamount_part_of_nonzerou32(v)
    }
}

/// A value type a prefix column can hold: a number type that says what
/// its values add to a sum ([`Amount`]), an `Option` of one, whose
/// `None` adds nothing, or `bool`, whose `true` adds one to a sum kept
/// in a `usize`. No other type can implement this trait.
pub trait PrefixValue: VSpec + for <'a>Form<'a> + seal::SummedSeal {
    /// The type the sums are kept in.
    type Sum: Sum;

    /// What the value `v` adds to a sum.
    spec fn amount(v: <Self as VSpec>::S) -> int;

    /// No value adds more than this, or less than minus this.
    spec fn most() -> nat;

    /// The sums of this many values, or fewer, fit `Sum`
    /// ([`lemma_sums_fit`]): a column of no more is not checked.
    spec fn safe() -> nat;

    /// What a value adds is within the bound, and is not negative
    /// where the sums are of an unsigned type.
    proof fn lemma_amount(v: <Self as VSpec>::S)
        ensures
            -(Self::most() as int) <= Self::amount(v) <= Self::most(),
            Self::Sum::lo() == 0 ==> Self::amount(v) >= 0,
    ;

    /// `safe` values, each as large as can be, add up to a sum of the
    /// type; and only sums of an unsigned type have less room than
    /// every column needs.
    proof fn lemma_safe()
        ensures
            Self::safe() >= 1,
            Self::safe() * Self::most() <= Self::Sum::hi(),
            Self::Sum::lo() == 0 || -(Self::safe() * Self::most()) >= Self::Sum::lo(),
            Self::Sum::lo() == 0 || Self::safe() >= usize::MAX,
    ;

    /// `safe`, as a `usize`.
    #[doc(hidden)]
    fn safe_len() -> (n: usize)
        ensures
            n == Self::safe(),
    ;

    /// What `v` adds, as a sum.
    #[doc(hidden)]
    fn to_part<'a>(v: Get<'a, Self>) -> (r: Self::Sum)
        ensures
            r.as_int() == Self::amount(v.sv()),
        no_unwind
    ;
}

impl<T: Amount> PrefixValue for T {
    type Sum = <T as Amount>::Sum;

    open spec fn amount(v: T) -> int {
        T::amount_of(v)
    }

    open spec fn most() -> nat {
        T::amount_max()
    }

    open spec fn safe() -> nat {
        T::room()
    }

    proof fn lemma_amount(v: T) {
        T::lemma_amount_of(v);
    }

    proof fn lemma_safe() {
        T::lemma_room();
    }

    #[inline(always)]
    fn safe_len() -> usize {
        T::room_len()
    }

    #[inline(always)]
    fn to_part<'a>(v: T) -> <T as Amount>::Sum {
        T::part_of(v)
    }
}

impl<T: Amount> PrefixValue for Option<T> {
    type Sum = <T as Amount>::Sum;

    open spec fn amount(v: Option<T>) -> int {
        match v {
            Some(x) => T::amount_of(x),
            None => 0,
        }
    }

    open spec fn most() -> nat {
        T::amount_max()
    }

    open spec fn safe() -> nat {
        T::room()
    }

    proof fn lemma_amount(v: Option<T>) {
        imp::vsummed_lemma_amount_option_t::<T>(v)
    }

    proof fn lemma_safe() {
        T::lemma_room();
    }

    #[inline(always)]
    fn safe_len() -> usize {
        T::room_len()
    }

    #[inline(always)]
    fn to_part<'a>(v: Option<T>) -> <T as Amount>::Sum {
        imp::vsummed_to_part_option_t::<T>(v)
    }
}

impl PrefixValue for bool {
    type Sum = usize;

    open spec fn amount(v: bool) -> int {
        if v {
            1
        } else {
            0
        }
    }

    open spec fn most() -> nat {
        1
    }

    open spec fn safe() -> nat {
        usize::MAX as nat
    }

    proof fn lemma_amount(v: bool) {
    }

    proof fn lemma_safe() {
    }

    #[inline(always)]
    fn safe_len() -> usize {
        usize::MAX
    }

    #[inline(always)]
    fn to_part<'a>(v: bool) -> usize {
        imp::vsummed_to_part_bool(v)
    }
}

// ── The sum of some values ──────────────────────────────────────────────────

/// What the values `vs` add up to: an integer, whatever type a column
/// keeps its sums in.
pub open spec fn summed<V: PrefixValue>(vs: Seq<<V as VSpec>::S>) -> int
    decreases vs.len(),
{
    if vs.len() == 0 {
        0
    } else {
        summed::<V>(vs.drop_last()) + V::amount(vs.last())
    }
}

/// Whether the sum of the first `k` of the values `vs` fits the type
/// their sums are kept in, for every `k`. A prefix column's values are
/// always so.
pub open spec fn sums_fit<V: PrefixValue>(vs: Seq<<V as VSpec>::S>) -> bool {
    forall|k: int| 0 <= k <= vs.len() ==> V::Sum::lo() <= #[trigger] summed::<V>(vs.take(k)) <= V::Sum::hi()
}

/// The sum of two sequences, one after the other, is the sum of their
/// sums.
pub proof fn lemma_sum_cat<V: PrefixValue>(a: Seq<<V as VSpec>::S>, b: Seq<<V as VSpec>::S>)
    ensures
        summed::<V>(a + b) == summed::<V>(a) + summed::<V>(b),
    decreases b.len(),
{
    imp::lemma_sum_cat_impl::<V>(a, b)
}

/// `n` copies of a value add up to `n` times what it adds.
pub proof fn lemma_sum_run<V: PrefixValue>(n: nat, x: <V as VSpec>::S)
    ensures
        summed::<V>(Seq::new(n, |_i: int| x)) == V::amount(x) * n,
    decreases n,
{
    imp::lemma_sum_run_impl::<V>(n, x)
}

/// A sum is no larger than its length times the most a value adds, and
/// is not negative where the sums are of an unsigned type.
pub proof fn lemma_sum_bound<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    ensures
        -(vs.len() * V::most()) <= summed::<V>(vs) <= vs.len() * V::most(),
        V::Sum::lo() == 0 ==> summed::<V>(vs) >= 0,
    decreases vs.len(),
{
    imp::lemma_sum_bound_impl::<V>(vs)
}

/// Where the sums are of an unsigned type, the sum of the first `k`
/// values is no more than the sum of them all.
pub proof fn lemma_sum_mono<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, k: int)
    requires
        V::Sum::lo() == 0,
        0 <= k <= vs.len(),
    ensures
        0 <= summed::<V>(vs.take(k)) <= summed::<V>(vs),
{
    imp::lemma_sum_mono_impl::<V>(vs, k)
}

/// The sums of no more than `safe` values fit.
pub proof fn lemma_sums_fit<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    requires
        vs.len() <= V::safe(),
    ensures
        sums_fit::<V>(vs),
{
    imp::lemma_sums_fit_impl::<V>(vs)
}

/// Where the sums are of an unsigned type, they all fit if the sum of
/// all the values does.
pub proof fn lemma_sums_fit_unsigned<V: PrefixValue>(vs: Seq<<V as VSpec>::S>)
    requires
        V::Sum::lo() == 0,
    ensures
        sums_fit::<V>(vs) <==> summed::<V>(vs) <= V::Sum::hi(),
{
    imp::lemma_sums_fit_unsigned_impl::<V>(vs)
}

/// The sums of the first `k` of values whose sums fit, fit.
pub proof fn lemma_sums_fit_take<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, k: int)
    requires
        sums_fit::<V>(vs),
        0 <= k <= vs.len(),
    ensures
        sums_fit::<V>(vs.take(k)),
        V::Sum::lo() <= summed::<V>(vs.take(k)) <= V::Sum::hi(),
{
    imp::lemma_sums_fit_take_impl::<V>(vs, k)
}

/// What the values at the positions from `i` up to `j` add up to is
/// the sum before `j` less the sum before `i`. Of values whose sums
/// fit, and no more of them than a column holds, it fits.
pub proof fn lemma_sum_range<V: PrefixValue>(vs: Seq<<V as VSpec>::S>, i: int, j: int)
    requires
        0 <= i <= j <= vs.len(),
    ensures
        summed::<V>(vs.subrange(i, j)) == summed::<V>(vs.take(j)) - summed::<V>(vs.take(i)),
        sums_fit::<V>(vs) && vs.len() <= usize::MAX ==> V::Sum::lo() <= summed::<V>(vs.subrange(i, j)) <= V::Sum::hi(),
{
    imp::lemma_sum_range_impl::<V>(vs, i, j)
}

} // verus!
