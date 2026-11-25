//! The implementation of `verified::bijou` and its proofs: what its
//! specification (`bijou.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

use crate::bijou64::{
    decode, decode_signed, encoded_bytes, encoded_bytes_signed, encoded_len_at, DecodeError,
};
#[cfg(verus_keep_ghost)]
use crate::bijou64::{
    lemma_encode_len, lemma_roundtrip, spec_decode, spec_encode, spec_encode_signed, spec_zigzag,
};
use crate::{Bijou64, PackError};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A number's bytes are one to nine.
pub(super) proof fn lemma_len(v: nat)
    ensures
        0 < spec_encode(v).len() <= 9,
{
    lemma_encode_len(v);
}

/// Bytes that start with a number's bytes are read as that number, of
/// that many bytes.
pub(super) proof fn lemma_prefix(v: nat, s: Seq<u8>)
    requires
        v <= u64::MAX,
        spec_encode(v).is_prefix_of(s),
    ensures
        spec_decode(s) == Some((v, spec_encode(v).len())),
{
    let e = spec_encode(v);
    assert(s =~= e + s.skip(e.len() as int));
    lemma_roundtrip(v, s.skip(e.len() as int));
}

/// Why no unsigned number is read, in the words of the codec of
/// [`Column`](crate::Column) (`Codec::try_read_unsigned` of [`Bijou64`]).
#[cold]
pub(super) fn bad_u(e: DecodeError) -> PackError {
    match e {
        DecodeError::BufferTooShort => refused("bijou64: buffer too short for bijou64 encoding"),
        DecodeError::Overflow => refused("bijou64: bijou64 tier 8 payload overflows u64"),
    }
}

/// Why no signed number is read, in the words of the codec of
/// [`Column`](crate::Column) (`Codec::try_read_signed` of [`Bijou64`]).
#[cold]
pub(super) fn bad_i(e: DecodeError) -> PackError {
    match e {
        DecodeError::BufferTooShort => refused("bijou64s: buffer too short for bijou64 encoding"),
        DecodeError::Overflow => refused("bijou64s: bijou64 tier 8 payload overflows u64"),
    }
}

/// Read an unsigned value, or say that none starts the bytes.
#[inline(always)]
pub(super) fn read_u64(data: &[u8]) -> (r: Result<(usize, u64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode(v as nat),
            Err(_) => forall|v: u64| !(#[trigger] spec_encode(v as nat)).is_prefix_of(data@),
        },
    no_unwind
{
    match decode(data) {
        Ok((v, n)) => Ok((n, v)),
        Err(e) => {
            proof {
                assert forall|v: u64| !(#[trigger] spec_encode(v as nat)).is_prefix_of(data@) by {
                    if spec_encode(v as nat).is_prefix_of(data@) {
                        lemma_prefix(v as nat, data@);
                    }
                }
            }
            Err(e)
        },
    }
}

/// Read a signed value, or say that none starts the bytes.
#[inline(always)]
pub(super) fn read_i64(data: &[u8]) -> (r: Result<(usize, i64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode_signed(v as int),
            Err(_) => forall|v: i64| !(#[trigger] spec_encode_signed(v as int)).is_prefix_of(data@),
        },
    no_unwind
{
    match decode_signed(data) {
        Ok((v, n)) => Ok((n, v)),
        Err(e) => {
            proof {
                assert forall|v: i64| !(#[trigger] spec_encode_signed(v as int)).is_prefix_of(data@) by {
                    if spec_encode_signed(v as int).is_prefix_of(data@) {
                        lemma_prefix(spec_zigzag(v as int), data@);
                    }
                }
            }
            Err(e)
        },
    }
}

impl Codec for Bijou64 {
    open spec fn enc_u(n: u64) -> Seq<u8> {
        spec_encode(n as nat)
    }

    open spec fn enc_i(n: i64) -> Seq<u8> {
        spec_encode_signed(n as int)
    }

    proof fn lemma_u(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        lemma_len(a as nat);
        if Self::enc_u(a) + s == Self::enc_u(b) + t {
            lemma_roundtrip(a as nat, s);
            lemma_roundtrip(b as nat, t);
        }
    }

    proof fn lemma_i(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        lemma_len(spec_zigzag(a as int));
        if Self::enc_i(a) + s == Self::enc_i(b) + t {
            lemma_roundtrip(spec_zigzag(a as int), s);
            lemma_roundtrip(spec_zigzag(b as int), t);
        }
    }

    #[inline(always)]
    fn put_u(n: u64, buf: &mut Vec<u8>) {
        let e = encoded_bytes(n);
        buf.extend_from_slice(e.as_slice());
    }

    #[inline(always)]
    fn put_i(n: i64, buf: &mut Vec<u8>) {
        let e = encoded_bytes_signed(n);
        buf.extend_from_slice(e.as_slice());
    }

    #[inline(always)]
    fn read_u(data: &[u8]) -> Result<(usize, u64), PackError> {
        match read_u64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_u(e)),
        }
    }

    #[inline(always)]
    fn read_u_opt(data: &[u8]) -> Option<(usize, u64)> {
        read_u64(data).ok()
    }

    #[inline(always)]
    fn read_i(data: &[u8]) -> Result<(usize, i64), PackError> {
        match read_i64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_i(e)),
        }
    }

    #[inline(always)]
    fn read_i_opt(data: &[u8]) -> Option<(usize, i64)> {
        read_i64(data).ok()
    }

    #[inline(always)]
    fn len_u(data: &[u8]) -> Option<usize> {
        proof {
            assert forall|v: u64| (#[trigger] spec_encode(v as nat)).is_prefix_of(data@) implies spec_decode(data@)
                == Some((v as nat, spec_encode(v as nat).len())) by {
                lemma_prefix(v as nat, data@);
            }
        }
        encoded_len_at(data)
    }

    #[inline(always)]
    fn len_i(data: &[u8]) -> Option<usize> {
        proof {
            assert forall|v: i64| (#[trigger] spec_encode_signed(v as int)).is_prefix_of(data@) implies spec_decode(
                data@,
            ) == Some((spec_zigzag(v as int), spec_encode_signed(v as int).len())) by {
                lemma_prefix(spec_zigzag(v as int), data@);
            }
        }
        encoded_len_at(data)
    }

    /// The header's bytes are cut out and the new one's put in, whatever
    /// their lengths: a header of one byte is not written in its place
    /// as it is for [`Leb128`](crate::Leb128).
    #[inline(always)]
    fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> (d: i64) {
        let len = buf.len();
        let ghost rest = buf@.skip(pos as int);
        let ghost h0 = choose|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(rest);
        let n = encoded_len_at(vstd::slice::slice_subrange(buf.as_slice(), pos, len)).unwrap_or(0);
        proof {
            lemma_prefix(spec_zigzag(h0 as int), rest);
            lemma_len(spec_zigzag(h0 as int));
            lemma_len(spec_zigzag(-(total as int)));
            assert forall|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(rest)
                implies spec_encode_signed(h as int).len() == n by {
                lemma_prefix(spec_zigzag(h as int), rest);
            }
        }
        let e = encoded_bytes_signed(-(total as i64));
        let m = e.as_slice().len();
        crate::rle::writer::splice_bytes(buf, pos, n, e.as_slice());
        m as i64 - n as i64
    }
}

} // verus!
