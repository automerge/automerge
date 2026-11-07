//! Pluggable variable-length integer codec.
//!
//! Every varint that hexane reads or writes — RLE run headers, value
//! payloads, bool run counts, string/bytes length prefixes — goes through
//! a [`Codec`] implementation.  The codec is selected at the type level
//! (e.g. `Column<u64, MyCodec>`) and defaults to [`Leb128`] everywhere,
//! so `Column<T>` means the same wire format it always has.
//!
//! Implementations must be *forward-decodable*: the length of a value must
//! be discoverable by reading from its first byte.  Nothing in hexane scans
//! backwards through encoded bytes, so codecs without LEB128's
//! continuation-bit structure (e.g. tag-prefixed encodings) are supported.

use crate::leb128_codec;
use crate::PackError;
use std::ops::Range;

/// Capacity of [`VarBuf`] — the largest single encoded integer any codec
/// may produce.  LEB128's worst case is 10 bytes; the extra headroom
/// accommodates future codecs without a format change.
pub const MAX_VARINT_LEN: usize = 16;

/// Stack-buffered encoded integer (no heap allocation).
#[derive(Clone, Copy)]
pub struct VarBuf {
    buf: [u8; MAX_VARINT_LEN],
    len: u8,
}

impl VarBuf {
    #[inline]
    pub fn new() -> Self {
        VarBuf {
            buf: [0u8; MAX_VARINT_LEN],
            len: 0,
        }
    }

    /// Append one byte.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is full (`MAX_VARINT_LEN` bytes).
    #[inline]
    pub fn push(&mut self, byte: u8) {
        self.buf[self.len as usize] = byte;
        self.len += 1;
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    /// Append a byte slice.
    ///
    /// # Panics
    ///
    /// Panics if the slice does not fit in the remaining capacity.
    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.buf[self.len as usize..self.len as usize + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len() as u8;
    }
}

impl From<leb128_codec::Encoded> for VarBuf {
    #[inline]
    fn from(e: leb128_codec::Encoded) -> Self {
        let (buf, len) = e.into_parts();
        VarBuf { buf, len }
    }
}

impl Default for VarBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for VarBuf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::fmt::Debug for VarBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.as_bytes()).finish()
    }
}

/// Owned byte iterator over a `VarBuf`. No heap allocation.
pub struct VarBufIter {
    buf: [u8; MAX_VARINT_LEN],
    pos: u8,
    len: u8,
}

impl Iterator for VarBufIter {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.pos < self.len {
            let b = self.buf[self.pos as usize];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.pos) as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for VarBufIter {}

impl IntoIterator for VarBuf {
    type Item = u8;
    type IntoIter = VarBufIter;
    #[inline]
    fn into_iter(self) -> VarBufIter {
        VarBufIter {
            buf: self.buf,
            pos: 0,
            len: self.len,
        }
    }
}

// ── Codec trait ─────────────────────────────────────────────────────────────

/// A variable-length integer wire format.
///
/// Implemented by zero-sized marker types ([`Leb128`] is the default
/// everywhere).  All methods are associated functions — the codec carries
/// no state; it is pure wire format.
///
/// The required methods are the four encode/decode primitives plus the
/// fallible reads; everything else has defaults derived from those.
/// Override the defaults where the format admits something faster
/// (e.g. length-from-first-byte instead of a full decode).
pub trait Codec: std::fmt::Debug + 'static {
    /// Encode an unsigned integer into a stack buffer.
    fn encode_unsigned(n: u64) -> VarBuf;

    /// Encode a signed integer into a stack buffer.
    fn encode_signed(n: i64) -> VarBuf;

    /// Decode one unsigned value. Returns `(bytes_read, value)`.
    fn read_unsigned(data: &[u8]) -> Option<(usize, u64)>;

    /// Decode one signed value. Returns `(bytes_read, value)`.
    fn read_signed(data: &[u8]) -> Option<(usize, i64)>;

    /// Decode one unsigned value with a descriptive error on malformed input.
    fn try_read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError>;

    /// Decode one signed value with a descriptive error on malformed input.
    fn try_read_signed(data: &[u8]) -> Result<(usize, i64), PackError>;

    /// Byte length of the encoded unsigned value at the start of `data`,
    /// without materialising it.  Override when the format can answer from
    /// the leading byte(s) alone.
    #[inline]
    fn unsigned_len(data: &[u8]) -> Option<usize> {
        Self::read_unsigned(data).map(|(n, _)| n)
    }

    /// Byte length of the encoded signed value at the start of `data`.
    #[inline]
    fn signed_len(data: &[u8]) -> Option<usize> {
        Self::read_signed(data).map(|(n, _)| n)
    }

    /// The number of bytes required to encode `n` as an unsigned integer.
    #[inline]
    fn unsigned_size(n: u64) -> u64 {
        Self::encode_unsigned(n).len() as u64
    }

    /// The number of bytes required to encode `n` as a signed integer.
    #[inline]
    fn signed_size(n: i64) -> u64 {
        Self::encode_signed(n).len() as u64
    }

    /// Encode `n` as an unsigned count (bool runs, lengths).
    #[inline(always)]
    fn encode_count(n: usize) -> VarBuf {
        Self::encode_unsigned(n as u64)
    }

    /// Decode one unsigned count as `usize`.
    #[inline]
    fn read_count(data: &[u8]) -> Option<(usize, usize)> {
        let (n, v) = Self::read_unsigned(data)?;
        // `None` rather than truncate when `usize` is narrower than 64 bits
        Some((n, usize::try_from(v).ok()?))
    }

    /// Compute the byte range of the signed value at `pos` in `buf`.
    ///
    /// `buf[pos..]` must start with a valid encoded value (these bytes were
    /// written by this codec earlier in the same session).
    #[inline]
    fn signed_bytes(buf: &[u8], pos: usize) -> Range<usize> {
        let n = Self::signed_len(&buf[pos..]).unwrap_or(0);
        pos..pos + n
    }

    /// Rewrite (or remove) a literal-run count header at `header_pos`.
    /// Returns the change in buffer length (can be negative if the header
    /// shrinks).
    fn rewrite_lit_header(buf: &mut Vec<u8>, header_pos: usize, total: usize) -> i64 {
        let len = buf.len();
        let header_bytes = Self::signed_bytes(buf, header_pos);
        if total == 0 {
            buf.splice(header_bytes, []);
        } else {
            buf.splice(header_bytes, Self::encode_signed(-(total as i64)));
        }
        buf.len() as i64 - len as i64
    }
}

// ── LEB128 ──────────────────────────────────────────────────────────────────

/// The default codec: LEB128, hexane's original wire format — a thin
/// [`Codec`] adapter over `crate::leb128_codec`, which is verified with
/// Verus.
///
/// Decoding accepts only canonical encodings, the shortest one for each
/// value: an overlong encoding (a redundant final `0x00`, or `0x7f` after a
/// negative group, as in `0x80 0x00` for 0) is rejected with
/// [`Leb128Error::Overlong`]. So each value has exactly one encoding, and
/// a column that loads re-saves to the same bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Leb128;

/// Why bytes are not a canonical LEB128 value.
pub use crate::leb128_codec::DecodeError as Leb128Error;

impl Codec for Leb128 {
    #[inline(always)]
    fn encode_unsigned(n: u64) -> VarBuf {
        leb128_codec::encode(n).into()
    }

    #[inline(always)]
    fn encode_signed(n: i64) -> VarBuf {
        leb128_codec::encode_signed(n).into()
    }

    #[inline]
    fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
        match leb128_codec::decode(data) {
            Ok((v, n)) => Some((n, v)),
            Err(_) => None,
        }
    }

    #[inline]
    fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
        match leb128_codec::decode_signed(data) {
            Ok((v, n)) => Some((n, v)),
            Err(_) => None,
        }
    }

    #[inline]
    fn try_read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
        let (v, n) = leb128_codec::decode(data)?;
        Ok((n, v))
    }

    #[inline]
    fn try_read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
        let (v, n) = leb128_codec::decode_signed(data)?;
        Ok((n, v))
    }

    /// Fast path: find the first byte without a continuation bit, without
    /// doing LEB128 arithmetic.
    #[inline]
    fn unsigned_len(data: &[u8]) -> Option<usize> {
        leb128_codec::encoded_len_at(data)
    }

    /// Same byte structure as unsigned LEB128.
    #[inline]
    fn signed_len(data: &[u8]) -> Option<usize> {
        leb128_codec::encoded_len_at(data)
    }

    #[inline]
    fn unsigned_size(n: u64) -> u64 {
        leb128_codec::encoded_len(n) as u64
    }

    #[inline]
    fn signed_size(n: i64) -> u64 {
        leb128_codec::encoded_len_signed(n) as u64
    }
}

// ── Bijou64 ─────────────────────────────────────────────────────────────────

/// The [bijou64](https://github.com/inkandswitch/bijou) tag-byte codec
/// (feature `bijou64`) — a thin [`Codec`] adapter over `crate::bijou64`,
/// hexane's Verus-verified copy of the `bijoux` crate's `u64`/`i64`
/// formats.
///
/// Unsigned values 0–247 encode as a single byte equal to the value;
/// larger values use a tag byte `0xF8`–`0xFF` followed by 1–8 big-endian
/// payload bytes with a per-tier offset.  Max 9 bytes for `u64::MAX`.
///
/// Properties that matter here:
/// - **Canonical by construction** — each value has exactly one encoding
///   and vice versa, so there is no overlong-encoding malleability to
///   validate away (LEB128 admits overlong forms, which the [`Leb128`]
///   decoder has to check for and reject).
/// - **Length from the first byte** — skips ([`unsigned_len`]) are a
///   table lookup, no continuation-bit scan.  The flip side is that the
///   bytes cannot be scanned backwards, which [`Codec`] already forbids
///   relying on.
///
/// Signed values use the bijou64s format: zigzag
/// (`(n << 1) ^ (n >> 63)`) composed with the unsigned encoding.  Zigzag
/// is itself a bijection, so canonicality is preserved; the 1-byte
/// signed range is −124..=123 (vs LEB128's −64..=63).
///
/// [`unsigned_len`]: Codec::unsigned_len
#[cfg(feature = "bijou64")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bijou64;

#[cfg(feature = "bijou64")]
mod bijou64_impl {
    use super::{Bijou64, Codec, VarBuf};
    use crate::bijou64;
    use crate::PackError;

    impl Codec for Bijou64 {
        #[inline]
        fn encode_unsigned(n: u64) -> VarBuf {
            let mut out = VarBuf::new();
            out.extend_from_slice(bijou64::encoded_bytes(n).as_slice());
            out
        }

        #[inline]
        fn encode_signed(n: i64) -> VarBuf {
            let mut out = VarBuf::new();
            out.extend_from_slice(bijou64::encoded_bytes_signed(n).as_slice());
            out
        }

        #[inline]
        fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
            match bijou64::decode(data) {
                Ok((v, n)) => Some((n, v)),
                Err(_) => None,
            }
        }

        #[inline]
        fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
            match bijou64::decode_signed(data) {
                Ok((v, n)) => Some((n, v)),
                Err(_) => None,
            }
        }

        fn try_read_unsigned(data: &[u8]) -> Result<(usize, u64), PackError> {
            match bijou64::decode(data) {
                Ok((v, n)) => Ok((n, v)),
                Err(e) => Err(PackError::InvalidValue(format!("bijou64: {e}"))),
            }
        }

        fn try_read_signed(data: &[u8]) -> Result<(usize, i64), PackError> {
            match bijou64::decode_signed(data) {
                Ok((v, n)) => Ok((n, v)),
                Err(e) => Err(PackError::InvalidValue(format!("bijou64s: {e}"))),
            }
        }

        /// Total length from the tag byte alone — no decode, no scan.
        /// See `bijou64::encoded_len_at`: it sits in the loop-carried
        /// dependency chain of every skip (`value_len`/`nth`).
        #[inline(always)]
        fn unsigned_len(data: &[u8]) -> Option<usize> {
            bijou64::encoded_len_at(data)
        }

        #[inline]
        fn signed_len(data: &[u8]) -> Option<usize> {
            Self::unsigned_len(data)
        }

        #[inline]
        fn unsigned_size(n: u64) -> u64 {
            bijou64::encoded_len(n) as u64
        }

        #[inline]
        fn signed_size(n: i64) -> u64 {
            bijou64::encoded_len_signed(n) as u64
        }
    }
}

/// The number of bytes required to encode `val` as a signed LEB128 integer.
pub fn lebsize(val: i64) -> u64 {
    Leb128::signed_size(val)
}

/// The number of bytes required to encode `val` as an unsigned LEB128 integer.
pub fn ulebsize(val: u64) -> u64 {
    Leb128::unsigned_size(val)
}
