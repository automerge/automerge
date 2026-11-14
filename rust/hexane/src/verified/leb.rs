//! The LEB128 value types of the verified public API, verified with Verus.
//!
//! [`Column`](crate::Column) reaches a value type through the trusted
//! bridges `Ops` and `WOps`, which state what the type's packing code
//! does and leave Verus to take it on trust. Here the same contracts
//! ([`RleOps`], [`WriteOps`], [`Lift`]) are *proved*, from the verified
//! LEB128 codec in `leb128_codec`. So a column over these types rests on
//! no trusted value code.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::leb128_codec::{decode, decode_signed, encode_signed, encoded_len_at, DecodeError};
// spec functions and lemmas exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use crate::leb128_codec::{
    lemma_encode_shape, lemma_encode_signed_shape, lemma_i64_len, lemma_u64_len, lemma_unique,
    lemma_unique_signed, spec_encode, spec_encode_signed,
};
use crate::rle::lift::Lift;
#[cfg(verus_keep_ghost)]
use crate::rle::model::encs;
use crate::rle::model::{RleOps, WriteOps};
#[cfg(verus_keep_ghost)]
use crate::rle::size::heads_bound;
use crate::PackError;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// The error for bytes that do not start with a canonical LEB128 value.
#[verifier::external_body]
#[cold]
fn bad_number(e: DecodeError) -> PackError {
    PackError::InvalidNumber(e)
}

/// `u64` values in LEB128: what they are on the wire.
pub struct VU64Ops;

/// `u64` values in LEB128: how they are written.
pub struct VU64Write;

/// An unsigned encoding at the front of `s` is the one of `v`.
proof fn lemma_front(v: nat, w: nat, s: Seq<u8>, t: Seq<u8>)
    ensures
        spec_encode(v).len() > 0,
        spec_encode(v) + s == spec_encode(w) + t ==> v == w,
{
    lemma_encode_shape(v);
    if spec_encode(v) + s == spec_encode(w) + t {
        let x = spec_encode(v) + s;
        assert(spec_encode(v).is_prefix_of(x));
        assert(spec_encode(w).is_prefix_of(x));
        lemma_unique(v, w, x);
    }
}

/// A signed encoding at the front of `s` is the one of `v`.
proof fn lemma_front_signed(v: int, w: int, s: Seq<u8>, t: Seq<u8>)
    ensures
        spec_encode_signed(v).len() > 0,
        spec_encode_signed(v) + s == spec_encode_signed(w) + t ==> v == w,
{
    lemma_encode_signed_shape(v);
    if spec_encode_signed(v) + s == spec_encode_signed(w) + t {
        let x = spec_encode_signed(v) + s;
        assert(spec_encode_signed(v).is_prefix_of(x));
        assert(spec_encode_signed(w).is_prefix_of(x));
        lemma_unique_signed(v, w, x);
    }
}

/// Read an unsigned value, or say that none starts the bytes.
#[inline(always)]
fn read_u64(data: &[u8]) -> (r: Result<(usize, u64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode(v as nat),
            Err(_) => forall|v: u64| !(#[trigger] spec_encode(v as nat)).is_prefix_of(data@),
        },
{
    match decode(data) {
        Ok((v, n)) => {
            proof {
                assert(data@.take(n as int) =~= data@.subrange(0, n as int));
            }
            Ok((n, v))
        },
        Err(e) => Err(e),
    }
}

/// Read a signed value, or say that none starts the bytes.
#[inline(always)]
fn read_i64(data: &[u8]) -> (r: Result<(usize, i64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode_signed(v as int),
            Err(_) => forall|v: i64| !(#[trigger] spec_encode_signed(v as int)).is_prefix_of(data@),
        },
{
    match decode_signed(data) {
        Ok((v, n)) => {
            proof {
                assert(data@.take(n as int) =~= data@.subrange(0, n as int));
            }
            Ok((n, v))
        },
        Err(e) => Err(e),
    }
}

impl<'a> RleOps<'a> for VU64Ops {
    type V = u64;

    open spec fn nullable() -> bool {
        false
    }

    open spec fn is_null(v: u64) -> bool {
        false
    }

    open spec fn enc(v: u64) -> Seq<u8> {
        spec_encode(v as nat)
    }

    open spec fn null() -> u64 {
        0
    }

    open spec fn enc_signed(n: i64) -> Seq<u8> {
        spec_encode_signed(n as int)
    }

    open spec fn enc_unsigned(n: u64) -> Seq<u8> {
        spec_encode(n as nat)
    }

    proof fn axiom_enc(v: u64, w: u64, s: Seq<u8>, t: Seq<u8>) {
        lemma_front(v as nat, w as nat, s, t);
    }

    proof fn axiom_null(v: u64) {
    }

    proof fn axiom_enc_signed(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        lemma_front_signed(a as int, b as int, s, t);
    }

    proof fn axiom_enc_unsigned(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        lemma_front(a as nat, b as nat, s, t);
    }

    #[inline(always)]
    fn nullable_exec() -> bool {
        false
    }

    #[inline(always)]
    fn same(a: &u64, b: &u64) -> bool {
        *a == *b
    }

    #[inline(always)]
    fn try_unpack(data: &'a [u8]) -> Result<(usize, u64), PackError> {
        match read_u64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_number(e)),
        }
    }

    #[inline(always)]
    fn unpack(data: &'a [u8]) -> (usize, u64) {
        match read_u64(data) {
            Ok(r) => r,
            Err(_) => {
                proof {
                    let v = choose|v: u64| !Self::is_null(v) && Self::enc(v).is_prefix_of(data@);
                    assert(spec_encode(v as nat).is_prefix_of(data@));
                }
                (0, 0)
            },
        }
    }

    #[inline(always)]
    fn value_len(data: &[u8]) -> Option<usize> {
        encoded_len_at(data)
    }

    #[inline(always)]
    fn get_null() -> u64 {
        0
    }

    #[inline(always)]
    fn read_signed_opt(data: &[u8]) -> Option<(usize, i64)> {
        read_i64(data).ok()
    }

    #[inline(always)]
    fn read_unsigned_opt(data: &[u8]) -> Option<(usize, u64)> {
        read_u64(data).ok()
    }

    #[inline(always)]
    fn read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        match read_i64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_number(e)),
        }
    }

    #[inline(always)]
    fn read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        match read_u64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_number(e)),
        }
    }
}

/// Write a signed value. One in `-64..64`, as most headers are, is its
/// own single byte, pushed directly.
#[inline(always)]
#[allow(clippy::manual_range_contains)]
fn put_i64(n: i64, buf: &mut Vec<u8>)
    ensures final(buf)@ == old(buf)@ + spec_encode_signed(n as int),
{
    if n >= -64 && n < 64 {
        assert((n & 0x7f) == n % 128) by (bit_vector);
        buf.push((n & 0x7f) as u8);
        proof {
            assert(buf@ =~= old(buf)@ + spec_encode_signed(n as int));
        }
        return;
    }
    let e = encode_signed(n);
    buf.extend_from_slice(e.as_slice());
}

/// Replace the literal-run header at `pos` with one for `total` values.
#[inline(always)]
fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> (d: i64)
    requires
        pos <= old(buf).len(),
        1 <= total <= i64::MAX,
        exists|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(old(buf)@.skip(pos as int)),
    ensures
        final(buf).len() == old(buf).len() + d,
        forall|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(old(buf)@.skip(pos as int))
            ==> final(buf)@ == old(buf)@.take(pos as int)
                + spec_encode_signed(-(total as int) as i64 as int)
                + old(buf)@.skip(pos + spec_encode_signed(h as int).len()),
{
    let len = buf.len();
    let ghost h0 = choose|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(buf@.skip(pos as int));
    let n = {
        let tail = vstd::slice::slice_subrange(buf.as_slice(), pos, len);
        proof {
            assert(tail@ =~= buf@.skip(pos as int));
        }
        encoded_len_at(tail).unwrap_or(0)
    };
    let e = encode_signed(-(total as i64));
    proof {
        lemma_i64_len(h0 as int);
        lemma_i64_len(-(total as int));
        lemma_encode_signed_shape(h0 as int);
        lemma_encode_signed_shape(-(total as int));
        assert(n == spec_encode_signed(h0 as int).len());
        assert(pos + n <= len);
        assert forall|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(buf@.skip(pos as int))
            implies spec_encode_signed(h as int).len() == n by {
            lemma_unique_signed(h as int, h0 as int, buf@.skip(pos as int));
        }
    }
    let m = e.as_slice().len();
    crate::rle::writer::splice_bytes(buf, pos, n, e.as_slice());
    m as i64 - n as i64
}

impl<'a> WriteOps<'a> for VU64Write {
    type R = VU64Ops;

    type Cow = u64;

    open spec fn val(c: u64) -> u64 {
        c
    }

    #[inline(always)]
    fn cow_ref(v: u64) -> u64 {
        v
    }

    #[inline(always)]
    fn cow_eq(a: &u64, b: &u64) -> bool {
        *a == *b
    }

    #[inline(always)]
    #[allow(unused_variables)]  // no `u64` is null
    fn cow_is_null(c: &u64) -> bool {
        false
    }

    #[inline(always)]
    fn cow_pack(c: &u64, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        crate::leb128_codec::put::encode_to(*c, buf);
        buf.len() - pos
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        put_i64(n, buf)
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        crate::leb128_codec::put::encode_to(n, buf)
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        rewrite_header(buf, pos, total)
    }
}

impl<'s> Lift<'s> for VU64Write {
    type W = VU64Write;

    open spec fn lift(v: u64) -> u64 {
        v
    }

    proof fn axiom_lift(v: u64) {
    }

    proof fn axiom_lift_codec(n: i64, m: u64) {
    }

    proof fn axiom_unlift(w: u64) {
        assert(<VU64Write as Lift<'s>>::lift(w) == w);
    }

    #[inline(always)]
    fn own(c: u64) -> u64 {
        c
    }
}

/// No header and no count of a `u64` column is longer than ten bytes.
pub(crate) proof fn lemma_u64_heads()
    ensures
        heads_bound::<VU64Ops>(10),
{
    assert forall|n: i64| (#[trigger] VU64Ops::enc_signed(n)).len() <= 10 by {
        lemma_i64_len(n as int);
    }
    assert forall|n: u64| (#[trigger] VU64Ops::enc_unsigned(n)).len() <= 10 by {
        lemma_u64_len(n as nat);
    }
}

/// `u64` values are at most ten bytes each.
pub(crate) proof fn lemma_u64_encs(vs: Seq<u64>)
    ensures
        encs::<VU64Ops>(vs).len() <= 10 * vs.len(),
    decreases vs.len(),
{
    if vs.len() > 0 {
        lemma_u64_encs(vs.drop_last());
        lemma_u64_len(vs.last() as nat);
    }
}

} // verus!
