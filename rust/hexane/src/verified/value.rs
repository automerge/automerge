//! The value types of the verified public API, verified with Verus.
//!
//! [`Column`](crate::Column) reaches a value type through the trusted
//! bridges `Ops` and `WOps`, which state what the type's packing code
//! does and leave Verus to take it on trust. Here the same contracts
//! ([`RleOps`], [`WriteOps`], [`Lift`]) are *proved*: for every type that
//! says how it packs ([`VPack`]), over every codec ([`VCodec`]), and for
//! `Option`s of them. So a column over these types rests on no trusted
//! value code.
//!
//! [`VPack`] is proved for `u64` and `i64`, which are the codec's own
//! numbers, and for every type that says how it is one of those
//! ([`VNewtype`]): `u32`, `i32` and `usize` here, and a client's own
//! types.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::size::heads_bound;
use crate::PackError;
use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── Packing ─────────────────────────────────────────────────────────────────

/// A value that is one number of a column's codec: how it packs, with
/// that specified and proved.
///
/// The types are this crate's (`u64` and `i64`) and those that say which
/// of these they are ([`VNewtype`]): no other can implement this trait.
pub trait VPack: Copy + Sized + super::seal::PackSeal {
    /// The bytes of `v`.
    spec fn enc<C: VCodec>(v: Self) -> Seq<u8>;

    /// A value is one to ten bytes, and no two values' bytes start the
    /// same string.
    proof fn lemma_enc<C: VCodec>(v: Self, w: Self, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc::<C>(v).len() <= 10,
            Self::enc::<C>(v) + s == Self::enc::<C>(w) + t ==> v == w,
    ;

    /// `a == b`.
    fn same(a: &Self, b: &Self) -> (r: bool)
        ensures
            r == (*a == *b),
    ;

    /// The value at the start of `data` and its length, or an error
    /// where no value's bytes start `data`.
    fn try_unpack<C: VCodec>(data: &[u8]) -> (r: Result<(usize, Self), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc::<C>(v),
                Err(_) => forall|v: Self| !(#[trigger] Self::enc::<C>(v)).is_prefix_of(data@),
            },
    ;

    /// The value known to start `data`, and its length.
    fn unpack<C: VCodec>(data: &[u8]) -> (r: (usize, Self))
        requires
            exists|v: Self| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@),
        ensures
            r.0 <= data.len(),
            data@.take(r.0 as int) == Self::enc::<C>(r.1),
    ;

    /// The length of the value that starts `data`, without reading it.
    fn value_len<C: VCodec>(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: Self| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@) ==> r == Some(Self::enc::<C>(v).len() as usize),
    ;

    /// Append the bytes of `v`; their number.
    fn pack<C: VCodec>(v: &Self, buf: &mut Vec<u8>) -> (n: usize)
        ensures
            final(buf)@ == old(buf)@ + Self::enc::<C>(*v),
            n == Self::enc::<C>(*v).len(),
    ;
}

impl super::seal::PackSeal for u64 {

}

impl VPack for u64 {
    open spec fn enc<C: VCodec>(v: u64) -> Seq<u8> {
        C::enc_u(v)
    }

    proof fn lemma_enc<C: VCodec>(v: u64, w: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(v, w, s, t);
    }

    #[inline(always)]
    fn same(a: &u64, b: &u64) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn try_unpack<C: VCodec>(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }

    #[inline(always)]
    fn unpack<C: VCodec>(data: &[u8]) -> (usize, u64) {
        match C::read_u_opt(data) {
            Some(r) => r,
            None => {
                proof {
                    let v = choose|v: u64| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@);
                    assert(C::enc_u(v).is_prefix_of(data@));
                }
                (0, 0)
            },
        }
    }

    #[inline(always)]
    fn value_len<C: VCodec>(data: &[u8]) -> Option<usize> {
        C::len_u(data)
    }

    #[inline(always)]
    fn pack<C: VCodec>(v: &u64, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        C::put_u(*v, buf);
        buf.len() - pos
    }
}

impl super::seal::PackSeal for i64 {

}

impl VPack for i64 {
    open spec fn enc<C: VCodec>(v: i64) -> Seq<u8> {
        C::enc_i(v)
    }

    proof fn lemma_enc<C: VCodec>(v: i64, w: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(v, w, s, t);
    }

    #[inline(always)]
    fn same(a: &i64, b: &i64) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn try_unpack<C: VCodec>(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn unpack<C: VCodec>(data: &[u8]) -> (usize, i64) {
        match C::read_i_opt(data) {
            Some(r) => r,
            None => {
                proof {
                    let v = choose|v: i64| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@);
                    assert(C::enc_i(v).is_prefix_of(data@));
                }
                (0, 0)
            },
        }
    }

    #[inline(always)]
    fn value_len<C: VCodec>(data: &[u8]) -> Option<usize> {
        C::len_i(data)
    }

    #[inline(always)]
    fn pack<C: VCodec>(v: &i64, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        C::put_i(*v, buf);
        buf.len() - pos
    }
}

// ── A type that is another's values ─────────────────────────────────────────

/// The error of a [`VNewtype::from_base`] for a base value its type has
/// no value for. Nothing is assumed of it.
#[verifier::external_body]
#[cold]
pub fn refused(msg: &'static str) -> PackError {
    PackError::InvalidValue(msg.into())
}

/// A value type that is stored as another's values: each value as its
/// own `Base` value, and not every `Base` value need be one.
///
/// A type that implements this is a [`VPack`], and so a value type of
/// [`VerifiedColumn`](super::VerifiedColumn), alone and in an `Option`.
/// It is how this crate's `u32`, `i32` and `usize` are value types, and
/// how a client's own type is one: a column of it holds the bytes a
/// column of `Base` would hold for the values' `Base` values, and loads
/// only bytes whose every value [`from_base`](Self::from_base) takes.
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
    type Base: VPack;

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
    {
        match Self::from_base(b) {
            Ok(v) => v,
            Err(_) => vstd::pervasive::unreached(),
        }
    }
}

impl<T: VNewtype> super::seal::PackSeal for T {

}

impl<T: VNewtype> VPack for T {
    open spec fn enc<C: VCodec>(v: T) -> Seq<u8> {
        T::Base::enc::<C>(T::base(v))
    }

    proof fn lemma_enc<C: VCodec>(v: T, w: T, s: Seq<u8>, t: Seq<u8>) {
        T::Base::lemma_enc::<C>(T::base(v), T::base(w), s, t);
        T::lemma_base(v, w);
    }

    #[inline(always)]
    fn same(a: &T, b: &T) -> bool {
        proof {
            T::lemma_base(*a, *b);
        }
        T::Base::same(&a.to_base(), &b.to_base())
    }

    #[inline(always)]
    fn try_unpack<C: VCodec>(data: &[u8]) -> Result<(usize, T), PackError> {
        match T::Base::try_unpack::<C>(data) {
            Ok((n, b)) => match T::from_base(b) {
                Ok(v) => Ok((n, v)),
                Err(e) => {
                    proof {
                        lemma_base_front::<T, C>(data@, n as int, b);
                    }
                    Err(e)
                },
            },
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    fn unpack<C: VCodec>(data: &[u8]) -> (usize, T) {
        proof {
            let v = choose|v: T| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@);
            assert(T::Base::enc::<C>(T::base(v)).is_prefix_of(data@));
        }
        let (n, b) = T::Base::unpack::<C>(data);
        proof {
            lemma_base_front::<T, C>(data@, n as int, b);
            let v = choose|v: T| (#[trigger] Self::enc::<C>(v)).is_prefix_of(data@);
            assert(T::base(v) == b);
        }
        (n, T::from_valid_base(b))
    }

    #[inline(always)]
    fn value_len<C: VCodec>(data: &[u8]) -> Option<usize> {
        T::Base::value_len::<C>(data)
    }

    #[inline(always)]
    fn pack<C: VCodec>(v: &T, buf: &mut Vec<u8>) -> usize {
        T::Base::pack::<C>(&v.to_base(), buf)
    }
}

/// Where `data` starts with the bytes of the base value `b`, a value
/// whose bytes start `data` has the base value `b`.
proof fn lemma_base_front<T: VNewtype, C: VCodec>(data: Seq<u8>, n: int, b: T::Base)
    requires
        0 <= n <= data.len(),
        data.take(n) == T::Base::enc::<C>(b),
    ensures
        forall|v: T| (#[trigger] T::enc::<C>(v)).is_prefix_of(data) ==> T::base(v) == b,
{
    assert forall|v: T| (#[trigger] T::enc::<C>(v)).is_prefix_of(data) implies T::base(v) == b by {
        let e = T::Base::enc::<C>(T::base(v));
        assert(e + data.skip(e.len() as int) =~= data);
        assert(T::Base::enc::<C>(b) + data.skip(n) =~= data);
        T::Base::lemma_enc::<C>(T::base(v), b, data.skip(e.len() as int), data.skip(n));
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
        if b <= u32::MAX as u64 {
            Ok(b as u32)
        } else {
            Err(refused("u32 overflow"))
        }
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
        if i32::MIN as i64 <= b && b <= i32::MAX as i64 {
            Ok(b as i32)
        } else {
            Err(refused("i32 overflow"))
        }
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
        if b <= usize::MAX as u64 {
            Ok(b as usize)
        } else {
            Err(refused("usize overflow"))
        }
    }

    #[inline(always)]
    fn from_valid_base(b: u64) -> usize {
        b as usize
    }
}

// ── Plain values to the RLE layer ───────────────────────────────────────────

/// Values of `T` under codec `C`, none of them null: what they are on
/// the wire.
#[verifier::reject_recursive_types(T)]
#[verifier::reject_recursive_types(C)]
pub struct VOps<T, C>(PhantomData<(T, C)>);

/// Values of `T` under codec `C`, none of them null: how they are
/// written.
#[verifier::reject_recursive_types(T)]
#[verifier::reject_recursive_types(C)]
pub struct VWrite<T, C>(PhantomData<(T, C)>);

impl<'a, T: VPack, C: VCodec> RleOps<'a> for VOps<T, C> {
    type V = T;

    open spec fn nullable() -> bool {
        false
    }

    open spec fn is_null(v: T) -> bool {
        false
    }

    open spec fn enc(v: T) -> Seq<u8> {
        T::enc::<C>(v)
    }

    open spec fn null() -> T {
        arbitrary()
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: T, w: T, s: Seq<u8>, t: Seq<u8>) {
        T::lemma_enc::<C>(v, w, s, t);
    }

    proof fn axiom_null(v: T) {
    }

    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(a, b, s, t);
    }

    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(a, b, s, t);
    }

    #[inline(always)]
    fn nullable_exec() -> bool {
        false
    }

    #[inline(always)]
    fn same(a: &T, b: &T) -> bool {
        T::same(a, b)
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> Result<(usize, T), PackError> {
        T::try_unpack::<C>(data)
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, T) {
        proof {
            let v = choose|v: T| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            assert(T::enc::<C>(v).is_prefix_of(data@));
        }
        T::unpack::<C>(data)
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> Option<usize> {
        T::value_len::<C>(data)
    }

    #[inline(always)]
    fn get_null() -> T {
        // no column of these has a null
        vstd::pervasive::unreached()
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        C::read_i_opt(data)
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        C::read_u_opt(data)
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }
}

impl<'a, T: VPack, C: VCodec> WriteOps<'a> for VWrite<T, C> {
    type R = VOps<T, C>;

    type Cow = T;

    open spec fn val(c: T) -> T {
        c
    }

    #[inline(always)]
    fn cow_ref(v: T) -> T {
        v
    }

    #[inline(always)]
    fn cow_eq(a: &T, b: &T) -> bool {
        T::same(a, b)
    }

    #[inline(always)]
    #[allow(unused_variables)]  // none is null
    fn cow_is_null(c: &T) -> bool {
        false
    }

    #[inline(always)]
    fn cow_pack(c: &T, buf: &mut Vec<u8>) -> usize {
        T::pack::<C>(c, buf)
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        C::put_i(n, buf)
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        C::put_u(n, buf)
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        C::rewrite_header(buf, pos, total)
    }
}

impl<'s, T: VPack, C: VCodec> Lift<'s> for VWrite<T, C> {
    type W = VWrite<T, C>;

    open spec fn lift(v: T) -> T {
        v
    }

    proof fn axiom_lift(v: T) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: T) {
        assert(<VWrite<T, C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: T) -> T {
        c
    }
}

// ── Values that may be null ─────────────────────────────────────────────────

/// `Option`s of `T` under codec `C`, `None` the null: what they are on
/// the wire.
#[verifier::reject_recursive_types(T)]
#[verifier::reject_recursive_types(C)]
pub struct VOptOps<T, C>(PhantomData<(T, C)>);

/// `Option`s of `T` under codec `C`, `None` the null: how they are
/// written.
#[verifier::reject_recursive_types(T)]
#[verifier::reject_recursive_types(C)]
pub struct VOptWrite<T, C>(PhantomData<(T, C)>);

impl<'a, T: VPack, C: VCodec> RleOps<'a> for VOptOps<T, C> {
    type V = Option<T>;

    open spec fn nullable() -> bool {
        true
    }

    open spec fn is_null(v: Option<T>) -> bool {
        v is None
    }

    /// A null has no bytes of its own: a column counts its nulls. The
    /// one here is to make every value's bytes some bytes.
    open spec fn enc(v: Option<T>) -> Seq<u8> {
        match v {
            Some(x) => T::enc::<C>(x),
            None => seq![0u8],
        }
    }

    open spec fn null() -> Option<T> {
        None
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        C::enc_i(n)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        C::enc_u(n)
    }

    proof fn axiom_enc(v: Option<T>, w: Option<T>, s: Seq<u8>, t: Seq<u8>) {
        match (v, w) {
            (Some(x), Some(y)) => {
                T::lemma_enc::<C>(x, y, s, t);
            },
            (Some(x), None) => {
                T::lemma_enc::<C>(x, x, s, s);
            },
            _ => {},
        }
    }

    proof fn axiom_null(v: Option<T>) {
    }

    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_i(a, b, s, t);
    }

    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        C::lemma_u(a, b, s, t);
    }

    #[inline(always)]
    fn nullable_exec() -> bool {
        true
    }

    #[inline(always)]
    fn same(a: &Option<T>, b: &Option<T>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => T::same(x, y),
            (None, None) => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> Result<(usize, Option<T>), PackError> {
        match T::try_unpack::<C>(data) {
            Ok((n, v)) => Ok((n, Some(v))),
            Err(e) => {
                proof {
                    assert forall|v: Option<T>| !Self::is_null(v) implies !Self::enc(v).is_prefix_of(data@) by {
                        assert(!T::enc::<C>(v->Some_0).is_prefix_of(data@));
                    }
                }
                Err(e)
            },
        }
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, Option<T>) {
        proof {
            let v = choose|v: Option<T>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
            assert(T::enc::<C>(v->Some_0).is_prefix_of(data@));
        }
        let (n, v) = T::unpack::<C>(data);
        (n, Some(v))
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> (r: Option<usize>) {
        let r = T::value_len::<C>(data);
        proof {
            assert forall|v: Option<T>| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@) implies r == Some(
                Self::enc(v).len() as usize,
            ) by {
                assert(T::enc::<C>(v->Some_0).is_prefix_of(data@));
            }
        }
        r
    }

    #[inline(always)]
    fn get_null() -> Option<T> {
        None
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        C::read_i_opt(data)
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        C::read_u_opt(data)
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::read_i(data)
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::read_u(data)
    }
}

impl<'a, T: VPack, C: VCodec> WriteOps<'a> for VOptWrite<T, C> {
    type R = VOptOps<T, C>;

    type Cow = Option<T>;

    open spec fn val(c: Option<T>) -> Option<T> {
        c
    }

    #[inline(always)]
    fn cow_ref(v: Option<T>) -> Option<T> {
        v
    }

    #[inline(always)]
    fn cow_eq(a: &Option<T>, b: &Option<T>) -> bool {
        <VOptOps<T, C> as RleOps<'a>>::same(a, b)
    }

    #[inline(always)]
    fn cow_is_null(c: &Option<T>) -> bool {
        c.is_none()
    }

    #[inline(always)]
    fn cow_pack(c: &Option<T>, buf: &mut Vec<u8>) -> usize {
        match c {
            Some(v) => T::pack::<C>(v, buf),
            // a null is not packed: this is not reached
            None => {
                proof {
                    assert(false);
                }
                0
            },
        }
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        C::put_i(n, buf)
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        C::put_u(n, buf)
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        C::rewrite_header(buf, pos, total)
    }
}

impl<'s, T: VPack, C: VCodec> Lift<'s> for VOptWrite<T, C> {
    type W = VOptWrite<T, C>;

    open spec fn lift(v: Option<T>) -> Option<T> {
        v
    }

    proof fn axiom_lift(v: Option<T>) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: Option<T>) {
        assert(<VOptWrite<T, C> as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: Option<T>) -> Option<T> {
        c
    }
}

// ── Sizes ───────────────────────────────────────────────────────────────────

/// No header and no count of a column of plain values is longer than ten
/// bytes, and its values are at most ten bytes each.
pub(crate) proof fn lemma_plain_sizes<T: VPack, C: VCodec>(vs: Seq<T>)
    ensures
        heads_bound::<VOps<T, C>>(10),
        encs::<VOps<T, C>>(vs).len() <= 10 * vs.len(),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VOps::<T, C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VOps::<T, C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    if vs.len() > 0 {
        lemma_plain_sizes::<T, C>(vs.drop_last());
        T::lemma_enc::<C>(vs.last(), vs.last(), Seq::empty(), Seq::empty());
    }
}

/// The same, of a column of values that may be null (a null counts as
/// one byte here, though it is none in the column).
pub(crate) proof fn lemma_opt_sizes<T: VPack, C: VCodec>(vs: Seq<Option<T>>)
    ensures
        heads_bound::<VOptOps<T, C>>(10),
        encs::<VOptOps<T, C>>(vs).len() <= 10 * vs.len(),
    decreases vs.len(),
{
    assert forall|n: i64| (#[trigger] VOptOps::<T, C>::enc_signed(n)).len() <= 10 by {
        C::lemma_i(n, n, Seq::empty(), Seq::empty());
    }
    assert forall|n: u64| (#[trigger] VOptOps::<T, C>::enc_unsigned(n)).len() <= 10 by {
        C::lemma_u(n, n, Seq::empty(), Seq::empty());
    }
    if vs.len() > 0 {
        lemma_opt_sizes::<T, C>(vs.drop_last());
        if let Some(x) = vs.last() {
            T::lemma_enc::<C>(x, x, Seq::empty(), Seq::empty());
        }
    }
}

} // verus!
