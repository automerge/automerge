//! The second codec of the verified public API, bijou64, verified with
//! Verus; and the column types of the verified API with it as their
//! codec (feature `bijou64`).
//!
//! [`Bijou64`] writes a number below 248 as its own single byte, and any
//! other as a byte that says how many bytes follow and then those, the
//! most significant first: nine bytes at most. A signed number is written
//! as the unsigned one that counts it in the order 0, −1, 1, −2, 2, …
//! [`VCodec`] is *proved* here for [`Bijou64`], from the verified bijou64
//! codec in `bijou64`: what the bytes of a number are, that no two
//! numbers' bytes start the same string, and that the reading and writing
//! functions read and write those bytes. Every column type of the
//! verified API is written for any [`VCodec`], so each is proved for
//! this codec as it is for [`Leb128`](crate::Leb128), with nothing said
//! twice.
//!
//! `use hexane::verified::bijou::VerifiedColumn` gives the API of
//! [`VerifiedColumn`](super::VerifiedColumn) with the [`Bijou64`] wire
//! format, so a consuming crate picks its codec with one import:
//!
//! ```
//! use hexane::verified::bijou::VerifiedColumn;
//!
//! let mut col = VerifiedColumn::<u32>::new();
//! col.push(300u32);
//! let col = VerifiedColumn::<u32>::load(&col.save()).unwrap();
//! assert_eq!(col.get(0), Some(300));
//! ```
//!
//! What such a column saves is what [`hexane::bijou::Column`](crate::bijou)
//! saves of the same values, and each loads what the other saved: the
//! tests hold the two to that, as they do for LEB128.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
use super::value::refused;
use crate::bijou64::{
    decode, decode_signed, encoded_bytes, encoded_bytes_signed, encoded_len_at, DecodeError,
};
// spec functions and lemmas exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use crate::bijou64::{
    lemma_encode_len, lemma_roundtrip, spec_decode, spec_encode, spec_encode_signed, spec_zigzag,
};
use crate::{Bijou64, PackError};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExBijou64(Bijou64);

/// A number's bytes are one to nine.
proof fn lemma_len(v: nat)
    ensures
        0 < spec_encode(v).len() <= 9,
{
    lemma_encode_len(v);
}

/// Bytes that start with a number's bytes are read as that number, of
/// that many bytes.
proof fn lemma_prefix(v: nat, s: Seq<u8>)
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
fn bad_u(e: DecodeError) -> PackError {
    match e {
        DecodeError::BufferTooShort => refused("bijou64: buffer too short for bijou64 encoding"),
        DecodeError::Overflow => refused("bijou64: bijou64 tier 8 payload overflows u64"),
    }
}

/// Why no signed number is read, in the words of the codec of
/// [`Column`](crate::Column) (`Codec::try_read_signed` of [`Bijou64`]).
#[cold]
fn bad_i(e: DecodeError) -> PackError {
    match e {
        DecodeError::BufferTooShort => refused("bijou64s: buffer too short for bijou64 encoding"),
        DecodeError::Overflow => refused("bijou64s: bijou64 tier 8 payload overflows u64"),
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
fn read_i64(data: &[u8]) -> (r: Result<(usize, i64), DecodeError>)
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

impl super::seal::CodecSeal for Bijou64 {

}

impl VCodec for Bijou64 {
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
/// A [`VerifiedColumn`](super::VerifiedColumn) whose numbers are written
/// with [`Bijou64`].
pub type VerifiedColumn<V> = super::VerifiedColumn<V, Bijou64>;
/// A [`VerifiedDeltaColumn`](super::VerifiedDeltaColumn) whose numbers are
/// written with [`Bijou64`].
pub type VerifiedDeltaColumn<T> = super::VerifiedDeltaColumn<T, Bijou64>;
/// A [`VerifiedPrefixColumn`](super::VerifiedPrefixColumn) whose numbers
/// are written with [`Bijou64`].
pub type VerifiedPrefixColumn<V> = super::VerifiedPrefixColumn<V, Bijou64>;
/// A [`VerifiedAccColumn`](super::VerifiedAccColumn) whose numbers are
/// written with [`Bijou64`].
pub type VerifiedAccColumn<V> = super::VerifiedAccColumn<V, Bijou64>;
/// A [`VerifiedEncoder`](super::VerifiedEncoder) that writes numbers with
/// [`Bijou64`].
pub type VerifiedEncoder<'a, V> = super::VerifiedEncoder<'a, V, Bijou64>;
/// A [`VerifiedDecoder`](super::VerifiedDecoder) that reads numbers
/// written with [`Bijou64`].
pub type VerifiedDecoder<'a, V> = super::VerifiedDecoder<'a, V, Bijou64>;
/// A [`VerifiedDeltaEncoder`](super::VerifiedDeltaEncoder) that writes
/// numbers with [`Bijou64`].
pub type VerifiedDeltaEncoder<'a, T> = super::VerifiedDeltaEncoder<'a, T, Bijou64>;
/// A [`VerifiedDeltaDecoder`](super::VerifiedDeltaDecoder) that reads
/// numbers written with [`Bijou64`].
pub type VerifiedDeltaDecoder<'a, T> = super::VerifiedDeltaDecoder<'a, T, Bijou64>;
