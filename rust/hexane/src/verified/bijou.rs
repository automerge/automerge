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
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([`verified`](crate::verified#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`bijou/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::VCodec;
use super::value::refused;

// spec functions and lemmas exist only when Verus compiles the crate

use crate::Bijou64;
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {} // verus!

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
