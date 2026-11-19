//! The column types of the verified API with LEB128 as their codec.
//!
//! `use hexane::verified::leb128::VerifiedColumn` reads the same as
//! `use hexane::verified::VerifiedColumn` ([`Leb128`] is the codec where
//! none is named) but states the wire format at the import, as the
//! `bijou` module beside this one does for bijou64 (behind the `bijou64`
//! feature). Code that mixes codecs can import both modules and qualify
//! at the use site:
//!
//! ```
//! use hexane::verified::leb128::VerifiedColumn;
//!
//! let mut col = VerifiedColumn::<u32>::new();
//! col.push(300u32);
//! let col = hexane::verified::VerifiedColumn::<u32>::load(&col.save()).unwrap();
//! assert_eq!(col.get(0), Some(300));
//! ```

use crate::Leb128;

/// A [`VerifiedColumn`](super::VerifiedColumn) whose numbers are written
/// with [`Leb128`].
pub type VerifiedColumn<V> = super::VerifiedColumn<V, Leb128>;
/// A [`VerifiedDeltaColumn`](super::VerifiedDeltaColumn) whose numbers are
/// written with [`Leb128`].
pub type VerifiedDeltaColumn<T> = super::VerifiedDeltaColumn<T, Leb128>;
/// A [`VerifiedPrefixColumn`](super::VerifiedPrefixColumn) whose numbers
/// are written with [`Leb128`].
pub type VerifiedPrefixColumn<V> = super::VerifiedPrefixColumn<V, Leb128>;
/// A [`VerifiedAccColumn`](super::VerifiedAccColumn) whose numbers are
/// written with [`Leb128`].
pub type VerifiedAccColumn<V> = super::VerifiedAccColumn<V, Leb128>;
/// A [`VerifiedEncoder`](super::VerifiedEncoder) that writes numbers with
/// [`Leb128`].
pub type VerifiedEncoder<'a, V> = super::VerifiedEncoder<'a, V, Leb128>;
/// A [`VerifiedDecoder`](super::VerifiedDecoder) that reads numbers
/// written with [`Leb128`].
pub type VerifiedDecoder<'a, V> = super::VerifiedDecoder<'a, V, Leb128>;
/// A [`VerifiedDeltaEncoder`](super::VerifiedDeltaEncoder) that writes
/// numbers with [`Leb128`].
pub type VerifiedDeltaEncoder<'a, T> = super::VerifiedDeltaEncoder<'a, T, Leb128>;
/// A [`VerifiedDeltaDecoder`](super::VerifiedDeltaDecoder) that reads
/// numbers written with [`Leb128`].
pub type VerifiedDeltaDecoder<'a, T> = super::VerifiedDeltaDecoder<'a, T, Leb128>;
