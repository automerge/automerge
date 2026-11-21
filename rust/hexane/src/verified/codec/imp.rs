//! The implementation of `verified::codec` and its proofs: what its
//! specification (`codec.rs`) says. Each function named `..._impl` here has
//! the contract of the function there that calls it, word for word; a
//! function named after a trait, a method and a type (`vpack_pack_u64`)
//! is the body of that method in that type's impl of the trait, with the
//! trait's contract said of the type.

#![allow(unused_imports)]

use super::*;

use crate::leb128_codec::{
    decode, decode_signed, encode_signed, encoded_len, encoded_len_at, encoded_len_signed,
    DecodeError,
};
#[cfg(verus_keep_ghost)]
use crate::leb128_codec::{
    lemma_encode_shape, lemma_encode_signed_shape, lemma_i64_len, lemma_u64_len, lemma_unique,
    lemma_unique_signed, spec_encode, spec_encode_signed,
};
use crate::{Leb128, PackError};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// An unsigned encoding at the front of `s` is the one of `v`.
pub(super) proof fn lemma_front(v: nat, w: nat, s: Seq<u8>, t: Seq<u8>)
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
pub(super) proof fn lemma_front_signed(v: int, w: int, s: Seq<u8>, t: Seq<u8>)
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
pub(super) fn read_u64(data: &[u8]) -> (r: Result<(usize, u64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode(v as nat),
            Err(_) => forall|v: u64| !(#[trigger] spec_encode(v as nat)).is_prefix_of(data@),
        },
    no_unwind
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
pub(super) fn read_i64(data: &[u8]) -> (r: Result<(usize, i64), DecodeError>)
    ensures
        match r {
            Ok((n, v)) => n <= data.len() && data@.take(n as int) == spec_encode_signed(v as int),
            Err(_) => forall|v: i64| !(#[trigger] spec_encode_signed(v as int)).is_prefix_of(data@),
        },
    no_unwind
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

impl VCodec for Leb128 {
    open spec fn enc_u(n: u64) -> Seq<u8> {
        spec_encode(n as nat)
    }

    open spec fn enc_i(n: i64) -> Seq<u8> {
        spec_encode_signed(n as int)
    }

    proof fn lemma_u(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>) {
        lemma_front(a as nat, b as nat, s, t);
        lemma_u64_len(a as nat);
    }

    proof fn lemma_i(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>) {
        lemma_front_signed(a as int, b as int, s, t);
        lemma_i64_len(a as int);
    }

    #[inline(always)]
    fn put_u(n: u64, buf: &mut Vec<u8>) {
        crate::leb128_codec::put::encode_to(n, buf)
    }

    /// One in `-64..64`, as most headers are, is its own single byte,
    /// pushed directly: by the push of the unsigned writer, so that a
    /// program has one place that pushes a byte of a number, and not
    /// two for the compiler to tell apart at each.
    #[inline(always)]
    #[allow(clippy::manual_range_contains)]
    fn put_i(n: i64, buf: &mut Vec<u8>) {
        if n >= -64 && n < 64 {
            assert((n & 0x7f) == n % 128 && 0 <= (n & 0x7f) < 128) by (bit_vector);
            crate::leb128_codec::put::encode_to((n & 0x7f) as u64, buf);
            proof {
                assert(spec_encode((n & 0x7f) as nat) =~= spec_encode_signed(n as int));
            }
            return ;
        }
        let e = encode_signed(n);
        buf.extend_from_slice(e.as_slice());
    }

    #[inline(always)]
    fn read_u(data: &[u8]) -> Result<(usize, u64), PackError> {
        match read_u64(data) {
            Ok(r) => Ok(r),
            Err(e) => Err(bad_number(e)),
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
            Err(e) => Err(bad_number(e)),
        }
    }

    #[inline(always)]
    fn read_i_opt(data: &[u8]) -> Option<(usize, i64)> {
        read_i64(data).ok()
    }

    #[inline(always)]
    fn len_u(data: &[u8]) -> Option<usize> {
        encoded_len_at(data)
    }

    #[inline(always)]
    fn len_i(data: &[u8]) -> Option<usize> {
        encoded_len_at(data)
    }

    #[inline(always)]
    fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> (d: i64) {
        let len = buf.len();
        let ghost h0 = choose|h: i64| (#[trigger] spec_encode_signed(h as int)).is_prefix_of(buf@.skip(pos as int));
        proof {
            let h = choose|h: i64| (#[trigger] Self::enc_i(h)).is_prefix_of(buf@.skip(pos as int));
            assert(spec_encode_signed(h as int).is_prefix_of(buf@.skip(pos as int)));
        }
        let n = {
            let tail = vstd::slice::slice_subrange(buf.as_slice(), pos, len);
            proof {
                assert(tail@ =~= buf@.skip(pos as int));
            }
            encoded_len_at(tail).unwrap_or(0)
        };
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
        // one byte in the place of one, as it is for a literal run of up
        // to 64 values whose header was one byte: written where it is
        if n == 1 && total <= 64 {
            let t = -(total as i64);
            assert((t & 0x7f) == t % 128 && 0 <= (t & 0x7f) < 128) by (bit_vector)
                requires
                    -64 <= t < 0,
            ;
            let ghost b0 = buf@;
            buf.set(pos, (t & 0x7f) as u8);
            proof {
                assert(buf@ =~= b0.take(pos as int) + spec_encode_signed(t as int) + b0.skip(pos + 1));
            }
            return 0;
        }
        let e = encode_signed(-(total as i64));
        let m = e.as_slice().len();
        crate::rle::writer::splice_bytes(buf, pos, n, e.as_slice());
        m as i64 - n as i64
    }
}

pub(super) fn ulebsize_impl(n: u64) -> (r: u64)
    ensures
        r == <Leb128 as VCodec>::enc_u(n).len(),
{
    encoded_len(n) as u64
}

pub(super) fn lebsize_impl(n: i64) -> (r: u64)
    ensures
        r == <Leb128 as VCodec>::enc_i(n).len(),
{
    encoded_len_signed(n) as u64
}

} // verus!
