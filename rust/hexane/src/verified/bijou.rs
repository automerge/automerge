//! The second codec of the verified public API, bijou64, verified with
//! Verus; and the column types of the verified API with it as their
//! codec (feature `bijou64`).
//!
//! [`Bijou64`] writes a number below 248 as its own single byte, and any
//! other as a byte that says how many bytes follow and then those, the
//! most significant first: nine bytes at most. A signed number is written
//! as the unsigned one that counts it in the order 0, −1, 1, −2, 2, …
//! [`Codec`] is *proved* here for [`Bijou64`], from the verified bijou64
//! codec in `bijou64`: what the bytes of a number are, that no two
//! numbers' bytes start the same string, and that the reading and writing
//! functions read and write those bytes. Every column type of the
//! verified API is written for any [`Codec`], so each is proved for
//! this codec as it is for [`Leb128`](crate::Leb128), with nothing said
//! twice.
//!
//! `use hexane::bijou::Column` gives the API of
//! [`Column`](super::Column) with the [`Bijou64`] wire
//! format, so a consuming crate picks its codec with one import:
//!
//! ```
//! use hexane::bijou::Column;
//!
//! let mut col = Column::<u32>::new();
//! col.push(300u32);
//! let col = Column::<u32>::load(&col.save()).unwrap();
//! assert_eq!(col.get(0), Some(300));
//! ```
//!
//! The tests tie what such a column saves to what hexane's earlier
//! bijou64 columns saved, as they do for LEB128.
//!
//! This file is a specification, to be read as the guide to reviewing
//! says ([Reviewing the specification](crate#reviewing-the-specification)): each
//! function's body is a call of one of `imp` (`bijou/imp.rs`), which has
//! its contract word for word and is proved there.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use super::codec::Codec;
use super::value::refused;

// spec functions and lemmas exist only when Verus compiles the crate

use crate::Bijou64;
#[allow(unused_imports)]
use vstd::prelude::*;

mod imp;
#[allow(unused_imports)]
use imp::*;

verus! {} // verus!

/// A [`Column`](super::Column) whose numbers are written
/// with [`Bijou64`].
pub type Column<V> = super::Column<V, Bijou64>;

/// A [`DeltaColumn`](super::DeltaColumn) whose numbers are
/// written with [`Bijou64`].
pub type DeltaColumn<T> = super::DeltaColumn<T, Bijou64>;

/// A [`PrefixColumn`](super::PrefixColumn) whose numbers
/// are written with [`Bijou64`].
pub type PrefixColumn<V> = super::PrefixColumn<V, Bijou64>;

/// A [`AccColumn`](super::AccColumn) whose numbers are
/// written with [`Bijou64`].
pub type AccColumn<V> = super::AccColumn<V, Bijou64>;

/// A [`Encoder`](super::Encoder) that writes numbers with
/// [`Bijou64`].
pub type Encoder<'a, V> = super::Encoder<'a, V, Bijou64>;

/// A [`Decoder`](super::Decoder) that reads numbers
/// written with [`Bijou64`].
pub type Decoder<'a, V> = super::Decoder<'a, V, Bijou64>;

/// A [`DeltaEncoder`](super::DeltaEncoder) that writes
/// numbers with [`Bijou64`].
pub type DeltaEncoder<'a, T> = super::DeltaEncoder<'a, T, Bijou64>;

/// A [`DeltaDecoder`](super::DeltaDecoder) that reads
/// numbers written with [`Bijou64`].
pub type DeltaDecoder<'a, T> = super::DeltaDecoder<'a, T, Bijou64>;
