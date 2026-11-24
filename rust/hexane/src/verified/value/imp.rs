//! The implementation of `verified::value` and its proofs: what its
//! specification (`value.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::size::heads_bound;
use crate::PackError;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::num::NonZeroU32;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// Where `data` starts with the bytes of the base value `b`, a value
/// whose bytes start `data` has the base value `b`.
pub(super) proof fn lemma_base_front<T: VNewtype, C: Codec>(data: Seq<u8>, n: int, b: T::Base)
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

impl<'a, T: RleValue, C: Codec> RleOps<'a> for VOps<T, C> {
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

    proof fn axiom_null() {
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

impl<'a, T: RleValue, C: Codec> WriteOps<'a> for VWrite<T, C> {
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

impl<'s, T: RleValue, C: Codec> Lift<'s> for VWrite<T, C> {
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

impl<'a, T: RleValue, C: Codec> RleOps<'a> for VOptOps<T, C> {
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

    proof fn axiom_null() {
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

impl<'a, T: RleValue, C: Codec> WriteOps<'a> for VOptWrite<T, C> {
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

impl<'s, T: RleValue, C: Codec> Lift<'s> for VOptWrite<T, C> {
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
pub(crate) proof fn lemma_plain_sizes<T: RleValue, C: Codec>(vs: Seq<T>)
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
pub(crate) proof fn lemma_opt_sizes<T: RleValue, C: Codec>(vs: Seq<Option<T>>)
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

#[inline(always)]
pub(super) fn vpack_unpack_u64<C: Codec>(data: &[u8]) -> (r: (usize, u64))
    requires
        exists|v: u64| (#[trigger] <u64 as RleValue>::enc::<C>(v)).is_prefix_of(data@),
    ensures
        r.0 <= data.len(),
        data@.take(r.0 as int) == <u64 as RleValue>::enc::<C>(r.1),
    no_unwind
{
    match C::read_u_opt(data) {
        Some(r) => r,
        None => {
            proof {
                let v = choose|v: u64| (#[trigger] <u64 as RleValue>::enc::<C>(v)).is_prefix_of(data@);
                assert(C::enc_u(v).is_prefix_of(data@));
            }
            (0, 0)
        },
    }
}

#[inline(always)]
pub(super) fn vpack_pack_u64<C: Codec>(v: &u64, buf: &mut Vec<u8>) -> (n: usize)
    ensures
        final(buf)@ == old(buf)@ + <u64 as RleValue>::enc::<C>(*v),
        n == <u64 as RleValue>::enc::<C>(*v).len(),
{
    let pos = buf.len();
    C::put_u(*v, buf);
    buf.len() - pos
}

#[inline(always)]
pub(super) fn vpack_unpack_i64<C: Codec>(data: &[u8]) -> (r: (usize, i64))
    requires
        exists|v: i64| (#[trigger] <i64 as RleValue>::enc::<C>(v)).is_prefix_of(data@),
    ensures
        r.0 <= data.len(),
        data@.take(r.0 as int) == <i64 as RleValue>::enc::<C>(r.1),
    no_unwind
{
    match C::read_i_opt(data) {
        Some(r) => r,
        None => {
            proof {
                let v = choose|v: i64| (#[trigger] <i64 as RleValue>::enc::<C>(v)).is_prefix_of(data@);
                assert(C::enc_i(v).is_prefix_of(data@));
            }
            (0, 0)
        },
    }
}

#[inline(always)]
pub(super) fn vpack_pack_i64<C: Codec>(v: &i64, buf: &mut Vec<u8>) -> (n: usize)
    ensures
        final(buf)@ == old(buf)@ + <i64 as RleValue>::enc::<C>(*v),
        n == <i64 as RleValue>::enc::<C>(*v).len(),
{
    let pos = buf.len();
    C::put_i(*v, buf);
    buf.len() - pos
}

pub(super) proof fn vpack_lemma_enc_t<T: VNewtype, C: Codec>(v: T, w: T, s: Seq<u8>, t: Seq<u8>)
    ensures
        0 < <T as RleValue>::enc::<C>(v).len() <= 10,
        <T as RleValue>::enc::<C>(v) + s == <T as RleValue>::enc::<C>(w) + t ==> v == w,
{
    T::Base::lemma_enc::<C>(T::base(v), T::base(w), s, t);
    T::lemma_base(v, w);
}

pub(super) proof fn vpack_lemma_num_t<T: VNewtype>(v: T, w: T)
    ensures
        <T as RleValue>::num(v) == <T as RleValue>::num(w) ==> v == w,
{
    T::Base::lemma_num(T::base(v), T::base(w));
    T::lemma_base(v, w);
}

#[inline(always)]
pub(super) fn vpack_same_t<T: VNewtype>(a: &T, b: &T) -> (r: bool)
    ensures
        r == (*a == *b),
{
    proof {
        T::lemma_base(*a, *b);
    }
    T::Base::same(&a.to_base(), &b.to_base())
}

#[inline(always)]
pub(super) fn vpack_try_unpack_t<T: VNewtype, C: Codec>(data: &[u8]) -> (r: Result<(usize, T), PackError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == <T as RleValue>::enc::<C>(v),
            Err(_) => forall|v: T| !(#[trigger] <T as RleValue>::enc::<C>(v)).is_prefix_of(data@),
        },
{
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
pub(super) fn vpack_unpack_t<T: VNewtype, C: Codec>(data: &[u8]) -> (r: (usize, T))
    requires
        exists|v: T| (#[trigger] <T as RleValue>::enc::<C>(v)).is_prefix_of(data@),
    ensures
        r.0 <= data.len(),
        data@.take(r.0 as int) == <T as RleValue>::enc::<C>(r.1),
    no_unwind
{
    proof {
        let v = choose|v: T| (#[trigger] <T as RleValue>::enc::<C>(v)).is_prefix_of(data@);
        assert(T::Base::enc::<C>(T::base(v)).is_prefix_of(data@));
    }
    let (n, b) = T::Base::unpack::<C>(data);
    proof {
        lemma_base_front::<T, C>(data@, n as int, b);
        let v = choose|v: T| (#[trigger] <T as RleValue>::enc::<C>(v)).is_prefix_of(data@);
        assert(T::base(v) == b);
    }
    (n, T::from_valid_base(b))
}

#[inline(always)]
pub(super) fn vnewtype_from_base_u32(b: u64) -> (r: Result<u32, PackError>)
    ensures
        match r {
            Ok(v) => <u32 as VNewtype>::base(v) == b,
            Err(_) => forall|v: u32| #[trigger] <u32 as VNewtype>::base(v) != b,
        },
    no_unwind when exists|v: u32| #[trigger] <u32 as VNewtype>::base(v) == b
{
    if b <= u32::MAX as u64 {
        Ok(b as u32)
    } else {
        Err(refused("u32 overflow"))
    }
}

#[inline(always)]
pub(super) fn vnewtype_from_base_i32(b: i64) -> (r: Result<i32, PackError>)
    ensures
        match r {
            Ok(v) => <i32 as VNewtype>::base(v) == b,
            Err(_) => forall|v: i32| #[trigger] <i32 as VNewtype>::base(v) != b,
        },
    no_unwind when exists|v: i32| #[trigger] <i32 as VNewtype>::base(v) == b
{
    if i32::MIN as i64 <= b && b <= i32::MAX as i64 {
        Ok(b as i32)
    } else {
        Err(refused("i32 overflow"))
    }
}

#[inline(always)]
pub(super) fn vnewtype_from_base_usize(b: u64) -> (r: Result<usize, PackError>)
    ensures
        match r {
            Ok(v) => <usize as VNewtype>::base(v) == b,
            Err(_) => forall|v: usize| #[trigger] <usize as VNewtype>::base(v) != b,
        },
    no_unwind when exists|v: usize| #[trigger] <usize as VNewtype>::base(v) == b
{
    if b <= usize::MAX as u64 {
        Ok(b as usize)
    } else {
        Err(refused("usize overflow"))
    }
}

pub(super) proof fn vnewtype_lemma_base_nonzerou32(v: NonZeroU32, w: NonZeroU32)
    ensures
        <NonZeroU32 as VNewtype>::base(v) == <NonZeroU32 as VNewtype>::base(w) ==> v == w,
{
    broadcast use vstd::std_specs::nonzero::group_nonzero_axioms;

    assert(vstd::std_specs::nonzero::nonzero_from_primitive(v@) == v);
    assert(vstd::std_specs::nonzero::nonzero_from_primitive(w@) == w);
}

#[inline(always)]
pub(super) fn vnewtype_from_base_nonzerou32(b: u32) -> (r: Result<NonZeroU32, PackError>)
    ensures
        match r {
            Ok(v) => <NonZeroU32 as VNewtype>::base(v) == b,
            Err(_) => forall|v: NonZeroU32| #[trigger] <NonZeroU32 as VNewtype>::base(v) != b,
        },
    no_unwind when exists|v: NonZeroU32| #[trigger] <NonZeroU32 as VNewtype>::base(v) == b
{
    broadcast use vstd::std_specs::nonzero::group_nonzero_axioms;

    match NonZeroU32::new(b) {
        Some(v) => Ok(v),
        None => Err(refused("NonZeroU32 is zero")),
    }
}

} // verus!
