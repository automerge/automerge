//! The codecs a column writes its numbers with: [`Leb128`], and with the
//! `bijou64` feature `Bijou64`. What a codec does is [`Codec`](crate::Codec)'s to say
//! (`crate::verified::codec`), where both are proved.

/// The default codec: LEB128, hexane's original wire format — a thin
/// [`Codec`](crate::Codec) adapter over `crate::leb128_codec`, which is verified with
/// Verus.
///
/// Decoding accepts only canonical encodings, the shortest one for each
/// value: an overlong encoding (a redundant final `0x00`, or `0x7f` after a
/// negative group, as in `0x80 0x00` for 0) is rejected with
/// `Leb128Error::Overlong`. So each value has exactly one encoding, and
/// a column that loads re-saves to the same bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Leb128;

/// Why bytes are not a canonical LEB128 value.
pub use crate::leb128_codec::DecodeError as Leb128Error;

/// The [bijou64](https://github.com/inkandswitch/bijou) tag-byte codec
/// (feature `bijou64`) — a thin [`Codec`](crate::Codec) adapter over `crate::bijou64`,
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
/// - **Length from the first byte** — skips (`unsigned_len`) are a
///   table lookup, no continuation-bit scan.  The flip side is that the
///   bytes cannot be scanned backwards, which [`Codec`](crate::Codec) already forbids
///   relying on.
///
/// Signed values use the bijou64s format: zigzag
/// (`(n << 1) ^ (n >> 63)`) composed with the unsigned encoding.  Zigzag
/// is itself a bijection, so canonicality is preserved; the 1-byte
/// signed range is −124..=123 (vs LEB128's −64..=63).
#[cfg(feature = "bijou64")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bijou64;
