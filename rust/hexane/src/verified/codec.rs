//! The number codecs of the verified public API, verified with Verus.
//!
//! A column writes its numbers (run headers, null counts, and the
//! integers it holds) with a codec. [`Column`](crate::Column) reaches
//! one through the [`Codec`](crate::Codec) trait, of which Verus sees
//! nothing. [`VCodec`] is that trait with specifications: what the bytes
//! of a number are, that no two numbers' bytes start the same string, and
//! that the reading and writing functions read and write those bytes.
//! It is *proved* for [`Leb128`], from the verified LEB128 codec in
//! `leb128_codec`.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::leb128_codec::{decode, decode_signed, encode_signed, encoded_len_at, DecodeError};
// spec functions and lemmas exist only when Verus compiles the crate
#[cfg(verus_keep_ghost)]
use crate::leb128_codec::{
    lemma_encode_shape, lemma_encode_signed_shape, lemma_i64_len, lemma_u64_len, lemma_unique,
    lemma_unique_signed, spec_encode, spec_encode_signed,
};
use crate::{Leb128, PackError};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

#[verifier::external_type_specification]
#[allow(dead_code)]
pub struct ExLeb128(Leb128);

/// A codec for the numbers of a [`VerifiedColumn`](super::VerifiedColumn),
/// with what it does specified and proved. The codecs are this crate's
/// ([`Leb128`](crate::Leb128), and `Bijou64` with the `bijou64`
/// feature) and a client's own, as a client of the old API may write a
/// [`Codec`](crate::Codec).
///
/// The proofs about a column rest on what this trait's functions ensure,
/// its laws. Verus checks them for a client it is run on. A client
/// compiled without Verus writes only the functions, and it is then that
/// client's duty that they keep them: that a number is one to ten bytes,
/// and no number's bytes start another's; that a read reads the number
/// whose bytes start `data`, and fails where none do; that a length is
/// that of the number a read would read; and that `rewrite_header`
/// replaces a header with another as it says.
pub trait VCodec {
    /// The bytes of an unsigned number.
    spec fn enc_u(n: u64) -> Seq<u8>;

    /// The bytes of a signed number.
    spec fn enc_i(n: i64) -> Seq<u8>;

    /// An unsigned number is one to ten bytes, and no two numbers' bytes
    /// start the same string.
    proof fn lemma_u(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc_u(a).len() <= 10,
            Self::enc_u(a) + s == Self::enc_u(b) + t ==> a == b,
    ;

    /// The same, for signed numbers.
    proof fn lemma_i(a: i64, b: i64, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc_i(a).len() <= 10,
            Self::enc_i(a) + s == Self::enc_i(b) + t ==> a == b,
    ;

    /// Append the bytes of `n`.
    fn put_u(n: u64, buf: &mut Vec<u8>)
        ensures
            final(buf)@ == old(buf)@ + Self::enc_u(n),
    ;

    /// Append the bytes of `n`.
    fn put_i(n: i64, buf: &mut Vec<u8>)
        ensures
            final(buf)@ == old(buf)@ + Self::enc_i(n),
    ;

    /// The unsigned number at the start of `data` and its length, or an
    /// error where no number's bytes start `data`.
    fn read_u(data: &[u8]) -> (r: Result<(usize, u64), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_u(v),
                Err(_) => forall|v: u64| !(#[trigger] Self::enc_u(v)).is_prefix_of(data@),
            },
    ;

    /// [`read_u`](Self::read_u), with no word on what is wrong.
    fn read_u_opt(data: &[u8]) -> (r: Option<(usize, u64)>)
        ensures
            match r {
                Some((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_u(v),
                None => forall|v: u64| !(#[trigger] Self::enc_u(v)).is_prefix_of(data@),
            },
        no_unwind
    ;

    /// The signed number at the start of `data` and its length, or an
    /// error where no number's bytes start `data`.
    fn read_i(data: &[u8]) -> (r: Result<(usize, i64), PackError>)
        ensures
            match r {
                Ok((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_i(v),
                Err(_) => forall|v: i64| !(#[trigger] Self::enc_i(v)).is_prefix_of(data@),
            },
    ;

    /// [`read_i`](Self::read_i), with no word on what is wrong.
    fn read_i_opt(data: &[u8]) -> (r: Option<(usize, i64)>)
        ensures
            match r {
                Some((n, v)) => n <= data.len() && data@.take(n as int) == Self::enc_i(v),
                None => forall|v: i64| !(#[trigger] Self::enc_i(v)).is_prefix_of(data@),
            },
        no_unwind
    ;

    /// The length of the unsigned number that starts `data`, without
    /// reading it.
    fn len_u(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: u64| (#[trigger] Self::enc_u(v)).is_prefix_of(data@) ==> r == Some(Self::enc_u(v).len() as usize),
        no_unwind
    ;

    /// The length of the signed number that starts `data`, without
    /// reading it.
    fn len_i(data: &[u8]) -> (r: Option<usize>)
        ensures
            forall|v: i64| (#[trigger] Self::enc_i(v)).is_prefix_of(data@) ==> r == Some(Self::enc_i(v).len() as usize),
        no_unwind
    ;

    /// Replace the signed number at `pos`, a literal run's header, with
    /// the header for `total` values; the change in length.
    #[doc(hidden)]
    fn rewrite_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> (d: i64)
        requires
            pos <= old(buf).len(),
            1 <= total <= i64::MAX,
            exists|h: i64| (#[trigger] Self::enc_i(h)).is_prefix_of(old(buf)@.skip(pos as int)),
        ensures
            final(buf).len() == old(buf).len() + d,
            forall|h: i64| (#[trigger] Self::enc_i(h)).is_prefix_of(old(buf)@.skip(pos as int))
                ==> final(buf)@ == old(buf)@.take(pos as int)
                    + Self::enc_i(-(total as int) as i64)
                    + old(buf)@.skip(pos + Self::enc_i(h).len()),
    ;
}

// ── LEB128 ──────────────────────────────────────────────────────────────────

/// The error for bytes that do not start with a canonical LEB128 number.
#[verifier::external_body]
#[cold]
fn bad_number(e: DecodeError) -> PackError {
    PackError::InvalidNumber(e)
}

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
fn read_i64(data: &[u8]) -> (r: Result<(usize, i64), DecodeError>)
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

} // verus!
