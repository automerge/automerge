//! The column types of the verified API with LEB128 as their codec.
//!
//! `use hexane::leb128::Column` reads the same as
//! `use hexane::Column` ([`Leb128`] is the codec where
//! none is named) but states the wire format at the import, as the
//! `bijou` module beside this one does for bijou64 (behind the `bijou64`
//! feature). Code that mixes codecs can import both modules and qualify
//! at the use site:
//!
//! ```
//! use hexane::leb128::Column;
//!
//! let mut col = Column::<u32>::new();
//! col.push(300u32);
//! let col = hexane::Column::<u32>::load(&col.save()).unwrap();
//! assert_eq!(col.get(0), Some(300));
//! ```

use crate::Leb128;

/// A [`Column`](super::Column) whose numbers are written
/// with [`Leb128`].
pub type Column<V> = super::Column<V, Leb128>;
/// A [`DeltaColumn`](super::DeltaColumn) whose numbers are
/// written with [`Leb128`].
pub type DeltaColumn<T> = super::DeltaColumn<T, Leb128>;
/// A [`PrefixColumn`](super::PrefixColumn) whose numbers
/// are written with [`Leb128`].
pub type PrefixColumn<V> = super::PrefixColumn<V, Leb128>;
/// A [`AccColumn`](super::AccColumn) whose numbers are
/// written with [`Leb128`].
pub type AccColumn<V> = super::AccColumn<V, Leb128>;
/// A [`Encoder`](super::Encoder) that writes numbers with
/// [`Leb128`].
pub type Encoder<'a, V> = super::Encoder<'a, V, Leb128>;
/// A [`Decoder`](super::Decoder) that reads numbers
/// written with [`Leb128`].
pub type Decoder<'a, V> = super::Decoder<'a, V, Leb128>;
/// A [`DeltaEncoder`](super::DeltaEncoder) that writes
/// numbers with [`Leb128`].
pub type DeltaEncoder<'a, T> = super::DeltaEncoder<'a, T, Leb128>;
/// A [`DeltaDecoder`](super::DeltaDecoder) that reads
/// numbers written with [`Leb128`].
pub type DeltaDecoder<'a, T> = super::DeltaDecoder<'a, T, Leb128>;
