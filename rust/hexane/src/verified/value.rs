//! The value types of the verified public API, verified with Verus.
//!
//! [`Column`](crate::Column) reaches a value type through the trusted
//! bridges `Ops` and `WOps`, which state what the type's packing code
//! does and leave Verus to take it on trust. Here the same contracts
//! ([`RleOps`](crate::rle::model::RleOps), [`WriteOps`](crate::rle::model::WriteOps),
//! [`Lift`](crate::rle::lift::Lift)) are *proved*: for every type that
//! says how it packs ([`RleValue`]), over every codec ([`Codec`]), and for
//! `Option`s of them. So a column over these types rests on no trusted
//! value code.
//!
//! [`RleValue`] is proved for `u64` and `i64`, which are the codec's own
//! numbers, and for every type that says how it is one of those
//! ([`VNewtype`]): `u32`, `i32`, `usize` and `NonZeroU32` here, and a
//! client's own types.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`value/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
#[cfg(verus_keep_ghost)]
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::{RleOps, WriteOps};
use crate::PackError;
use std::cmp::Ordering;
use std::num::NonZeroU32;
#[allow(unused_imports)]
use vstd::prelude::*;

pub use crate::trusted::refused;
mod imp;
#[allow(unused_imports)]
use imp::*;
#[cfg(verus_keep_ghost)]
#[allow(unused_imports)]
pub(crate) use imp::{lemma_opt_sizes, lemma_plain_sizes};
#[allow(unused_imports)]
pub(crate) use imp::{VOps, VOptOps, VOptWrite, VWrite};

verus! {

// ── Packing ─────────────────────────────────────────────────────────────────

/// A value that packs as one to ten bytes: how, with that specified and
/// proved. The types are this crate's numbers (`u64` and `i64`), those
/// that say which of these they are ([`VNewtype`]), and a client's own
/// with an encoding of its own, as a client of the old API writes
/// [`RleValue`](crate::RleValue)'s `pack` and `try_unpack` for its type.
///
/// The proofs about a column of the type rest on what this trait's
/// functions ensure, its laws. Verus checks them for a client it is run
/// on. A client compiled without Verus writes only the functions, and
/// it is then that client's duty that they keep them: that `pack`
/// writes one to ten bytes, and no value's bytes start another's; that
/// `try_unpack` reads the value whose bytes start `data`, and fails
/// where none do; that `unpack` and `value_len` agree with it; that
/// `same` is `==`; and that `compare` is an order in which a value is
/// equal to itself only.
pub trait RleValue: Copy + Sized {
    /// The bytes of `v`.
    spec fn enc<C: Codec>(v: Self) -> Seq<u8>;

    /// A value is one to ten bytes, and no two values' bytes start the
    /// same string.
    proof fn lemma_enc<C: Codec>(v: Self, w: Self, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc::<C>(v).len() <= 10,
            Self::enc::<C>(v) + s == Self::enc::<C>(w) + t ==> v == w,
    ;

    /// The number `v` is stored as: the values' order is that of their
    /// numbers.
    spec fn num(v: Self) -> int;

    /// No two values are stored as the same number.
    proof fn lemma_num(v: Self, w: Self)
        ensures
            Self::num(v) == Self::num(w) ==> v == w,
    ;

    /// `a == b`.
    fn same(a: &Self, b: &Self) -> (r: bool)
        ensures
            r == (*a == *b),
    ;

    /// How `a` stands to `b` in the order of the values: that of their
    /// numbers.
    fn compare(a: &Self, b: &Self) -> (r: Ordering)
        ensures
            (r is Less) == (Self::num(*a) < Self::num(*b)),
            (r is Greater) == (Self::num(*a) > Self::num(*b)),
    ;

    /// The value at the start of `data` and its length, or an error
    /// where no value's bytes start `data`.
    fn try_unpack<C: Codec>(data: &[u8]) -> (r: Result<(usize, Self), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc::<C>(v),
                Err(_) => forall|v: Self| !(#[trigger] Self::enc::<C>(v)).is_prefix_of(data@),
            },
    ;

    /// The value known to start `data`, and its length.
    fn unpack<C: Codec>(data: &[u8]) -> (r: (usize, Self))
        requires
            exists|v: Self| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@),
        ensures
            r.0 <= data.len(),
            data@.take(r.0 as int) == Self::enc::<C>(r.1),
        no_unwind
    ;

    /// The length of the value that starts `data`, without reading it.
    fn value_len<C: Codec>(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: Self| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@) ==> r == Some(Self::enc::<C>(v).len() as usize),
        no_unwind
    ;

    /// Append the bytes of `v`; their number.
    fn pack<C: Codec>(v: &Self, buf: &mut Vec<u8>) -> (n: usize)
        ensures
            final(buf)@ == old(buf)@ + Self::enc::<C>(*v),
            n == Self::enc::<C>(*v).len(),
    ;
}

impl RleValue for u64 {
    open spec fn enc<C: Codec>(v: u64) -> Seq<u8> {
        C::enc_u(v)
    }

    proof fn lemma_enc<C: Codec>(v: u64, w: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(v, w, s, t);
    }

    open spec fn num(v: u64) -> int {
        v as int
    }

    proof fn lemma_num(v: u64, w: u64) {
    }

    #[inline(always)]
    fn same(a: &u64, b: &u64) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn compare(a: &u64, b: &u64) -> Ordering {
        a.cmp(b)
    }

    #[inline(always)]
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }

    #[inline(always)]
    fn unpack<C: Codec>(data: &[u8]) -> (usize, u64) {
        imp::vpack_unpack_u64::<C>(data)
    }

    #[inline(always)]
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::len_u(data)
    }

    #[inline(always)]
    fn pack<C: Codec>(v: &u64, buf: &mut Vec<u8>) -> usize {
        imp::vpack_pack_u64::<C>(v, buf)
    }
}

impl RleValue for i64 {
    open spec fn enc<C: Codec>(v: i64) -> Seq<u8> {
        C::enc_i(v)
    }

    proof fn lemma_enc<C: Codec>(v: i64, w: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(v, w, s, t);
    }

    open spec fn num(v: i64) -> int {
        v as int
    }

    proof fn lemma_num(v: i64, w: i64) {
    }

    #[inline(always)]
    fn same(a: &i64, b: &i64) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn compare(a: &i64, b: &i64) -> Ordering {
        a.cmp(b)
    }

    #[inline(always)]
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn unpack<C: Codec>(data: &[u8]) -> (usize, i64) {
        imp::vpack_unpack_i64::<C>(data)
    }

    #[inline(always)]
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::len_i(data)
    }

    #[inline(always)]
    fn pack<C: Codec>(v: &i64, buf: &mut Vec<u8>) -> usize {
        imp::vpack_pack_i64::<C>(v, buf)
    }
}


/// A value type that is stored as another's values: each value as its
/// own `Base` value, and not every `Base` value need be one.
///
/// A type that implements this is a [`RleValue`], and so a value type of
/// [`Column`](super::Column), alone and in an `Option`.
/// It is how this crate's `u32`, `i32`, `usize` and `NonZeroU32` are
/// value types, and how a client's own type is one: a column of it holds
/// the bytes a column of `Base` would hold for the values' `Base`
/// values, and loads only bytes whose every value
/// [`from_base`](Self::from_base) takes. And a column has the values in
/// the order of their `Base` values (`VSpec::below`),
/// which is the order a search of it goes by: an `Ord` of the type's
/// own is not asked.
///
/// The proofs about a column of the type rest on what this trait's
/// functions ensure. Verus checks that for a client it is run on. A
/// client compiled without Verus writes only `Base`, `to_base` and
/// `from_base`, and it is then that client's duty that `from_base`
/// gives back the value `to_base` was given and no other (and, if it
/// writes a [`from_valid_base`](Self::from_valid_base) of its own, that
/// it gives what `from_base` does).
pub trait VNewtype: Copy + Sized {
    /// The type it is stored as.
    type Base: RleValue;

    /// The `Base` value of `v`.
    spec fn base(v: Self) -> Self::Base;

    /// No two values have the same `Base` value.
    proof fn lemma_base(v: Self, w: Self)
        ensures
            Self::base(v) == Self::base(w) ==> v == w,
    ;

    /// The `Base` value.
    fn to_base(self) -> (r: Self::Base)
        ensures
            r == Self::base(self),
    ;

    /// The value whose `Base` value is `b`, or an error where there is
    /// none.
    fn from_base(b: Self::Base) -> (r: Result<Self, PackError>)
        ensures
            match r {
                Ok(v) => Self::base(v) == b,
                Err(_) => forall|v: Self| #[trigger] Self::base(v) != b,
            },
        no_unwind when exists|v: Self| #[trigger] Self::base(v) == b
    ;

    /// The value whose `Base` value is `b`, for a `b` that is some
    /// value's: what a read of a column calls, whose bytes hold no other
    /// `b`. As given here it is [`from_base`](Self::from_base). A type
    /// may give its own that leaves out `from_base`'s check, as `u32`
    /// does (a cast): the check and its error then stay out of the loop
    /// that reads value after value.
    #[inline(always)]
    fn from_valid_base(b: Self::Base) -> (r: Self)
        requires
            exists|v: Self| #[trigger] Self::base(v) == b,
        ensures
            Self::base(r) == b,
        no_unwind
    {
        match Self::from_base(b) {
            Ok(v) => v,
            Err(_) => vstd::pervasive::unreached(),
        }
    }
}

impl<T: VNewtype> RleValue for T {
    open spec fn enc<C: Codec>(v: T) -> Seq<u8> {
        T::Base::enc::<C>(T::base(v))
    }

    proof fn lemma_enc<C: Codec>(v: T, w: T, s: Seq<u8>, t: Seq<u8>) {
        imp::vpack_lemma_enc_t::<T, C>(v, w, s, t)
    }

    open spec fn num(v: T) -> int {
        T::Base::num(T::base(v))
    }

    proof fn lemma_num(v: T, w: T) {
        imp::vpack_lemma_num_t::<T>(v, w)
    }

    #[inline(always)]
    fn same(a: &T, b: &T) -> bool {
        imp::vpack_same_t::<T>(a, b)
    }

    #[inline(always)]
    fn compare(a: &T, b: &T) -> Ordering {
        T::Base::compare(&a.to_base(), &b.to_base())
    }

    #[inline(always)]
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, T), PackError> {
        imp::vpack_try_unpack_t::<T, C>(data)
    }

    #[inline(always)]
    fn unpack<C: Codec>(data: &[u8]) -> (usize, T) {
        imp::vpack_unpack_t::<T, C>(data)
    }

    #[inline(always)]
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        T::Base::value_len::<C>(data)
    }

    #[inline(always)]
    fn pack<C: Codec>(v: &T, buf: &mut Vec<u8>) -> usize {
        T::Base::pack::<C>(&v.to_base(), buf)
    }
}

impl VNewtype for u32 {
    type Base = u64;

    open spec fn base(v: u32) -> u64 {
        v as u64
    }

    proof fn lemma_base(v: u32, w: u32) {
    }

    #[inline(always)]
    fn to_base(self) -> u64 {
        self as u64
    }

    #[inline(always)]
    fn from_base(b: u64) -> Result<u32, PackError> {
        imp::vnewtype_from_base_u32(b)
    }

    #[inline(always)]
    fn from_valid_base(b: u64) -> u32 {
        b as u32
    }
}

impl VNewtype for i32 {
    type Base = i64;

    open spec fn base(v: i32) -> i64 {
        v as i64
    }

    proof fn lemma_base(v: i32, w: i32) {
    }

    #[inline(always)]
    fn to_base(self) -> i64 {
        self as i64
    }

    #[inline(always)]
    fn from_base(b: i64) -> Result<i32, PackError> {
        imp::vnewtype_from_base_i32(b)
    }

    #[inline(always)]
    fn from_valid_base(b: i64) -> i32 {
        b as i32
    }
}

impl VNewtype for usize {
    type Base = u64;

    open spec fn base(v: usize) -> u64 {
        v as u64
    }

    proof fn lemma_base(v: usize, w: usize) {
    }

    #[inline(always)]
    fn to_base(self) -> u64 {
        self as u64
    }

    #[inline(always)]
    fn from_base(b: u64) -> Result<usize, PackError> {
        imp::vnewtype_from_base_usize(b)
    }

    #[inline(always)]
    fn from_valid_base(b: u64) -> usize {
        b as usize
    }
}

/// Every `u32` but zero. What a `NonZeroU32` is to Verus comes from
/// vstd's specifications of `NonZero`: its number (`v@`), that no two
/// have the same, and that `NonZeroU32::new` refuses zero alone.
impl VNewtype for NonZeroU32 {
    type Base = u32;

    open spec fn base(v: NonZeroU32) -> u32 {
        v@
    }

    proof fn lemma_base(v: NonZeroU32, w: NonZeroU32) {
        imp::vnewtype_lemma_base_nonzerou32(v, w)
    }

    #[inline(always)]
    fn to_base(self) -> u32 {
        self.get()
    }

    #[inline(always)]
    fn from_base(b: u32) -> Result<NonZeroU32, PackError> {
        imp::vnewtype_from_base_nonzerou32(b)
    }
}

} // verus!
