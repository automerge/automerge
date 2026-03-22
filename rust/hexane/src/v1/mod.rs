//! v1 public interface for `Column`.
//!
//! The type parameter is now a *value type* (`u64`, `Option<String>`, …)
//! rather than a cursor type.  Storage is a single `Vec<u8>` using the same
//! RLE + LEB128 wire format as v0; mutations edit bytes in place instead of
//! re-encoding the whole slab.

pub mod bool_encoding;
pub mod column;
pub mod delta_column;
pub mod encoding;
pub mod indexed;
pub mod load_opts;
pub mod mirrored;
pub mod prefix_column;
pub mod rle;
pub use column::{Column, Iter};
pub use encoding::RunDecoder;
pub use delta_column::{DeltaColumn, DeltaValue};
pub use encoding::ColumnEncoding;
pub use indexed::IndexedDeltaColumn;
pub use load_opts::LoadOpts;
pub use mirrored::{
    MirrorIter, MirrorIterState, MirrorPrefixIter, Mirrorable, MirroredColumn, MirroredPrefixColumn,
};
pub use prefix_column::{PrefixColumn, PrefixIter, PrefixValue};

#[cfg(test)]
mod tests;

use crate::PackError;

use bool_encoding::BoolEncoding;
use rle::RleEncoding;

// ── Run ─────────────────────────────────────────────────────────────────────

/// A run of identical values from a column iterator.
///
/// For repeat runs, `count` is the number of remaining items (including the
/// current one) and `value` is the repeated value.  For null runs, `count`
/// is the remaining null count and `value` is the type's null representation.
/// For literal runs, `count` is always 1 since each item is distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run<V> {
    pub count: usize,
    pub value: V,
}

// ── Core trait ────────────────────────────────────────────────────────────────

/// Types that can be stored as column values.
///
/// The type parameter encodes both the element and its nullability:
///
/// | `T`                | `Get<'a>`          | Encoding        | Nullable |
/// |--------------------|--------------------|-----------------|----------|
/// | `u64`              | `u64`              | `RleEncoding`   | no       |
/// | `Option<u64>`      | `Option<u64>`      | `RleEncoding`   | yes      |
/// | `i64`              | `i64`              | `RleEncoding`   | no       |
/// | `Option<i64>`      | `Option<i64>`      | `RleEncoding`   | yes      |
/// | `String`           | `&'a str`          | `RleEncoding`   | no       |
/// | `Option<String>`   | `Option<&'a str>`  | `RleEncoding`   | yes      |
/// | `Vec<u8>`          | `&'a [u8]`         | `RleEncoding`   | no       |
/// | `Option<Vec<u8>>`  | `Option<&'a [u8]>` | `RleEncoding`   | yes      |
/// | `bool`             | `bool`             | `BoolEncoding`  | no       |
pub trait ColumnValue: 'static {
    /// The encoding strategy for this value type.
    type Encoding: ColumnEncoding<Value = Self>;

    /// The optimal return type for `get()`: owned for `Copy` types, borrowed
    /// for ref types (`&str`, `&[u8]`).
    type Get<'a>: Copy + PartialEq + 'a
    where
        Self: 'a;

    /// Convert an owned value to its borrowed `Get` form.
    fn as_get(&self) -> Self::Get<'_>;
}

/// Extension of [`ColumnValue`] with RLE-specific encoding/decoding methods.
///
/// This is implemented by all value types that use [`RleEncoding`] — i.e.,
/// everything except `bool` (which uses [`BoolEncoding`]).
pub trait RleValue: ColumnValue {
    /// Whether this column type allows null entries.
    ///
    /// `true` for `Option<T>` types, `false` for bare `T`.  Used by `load`
    /// to reject null runs in non-nullable columns.
    const NULLABLE: bool;

    /// Return the byte length of one encoded value at the start of `data`,
    /// or `None` if the data is malformed / too short.
    ///
    /// This is the primary "skip over a value" operation used by `scan_to`,
    /// `split_at_item`, `count_segments`, and similar slab-walking functions.
    fn value_len(data: &[u8]) -> Option<usize>;

    /// Decode a non-null value from raw slab bytes with full validation.
    ///
    /// Returns `(bytes_consumed, value)` on success, or a [`PackError`] if the
    /// data is malformed (e.g. truncated LEB128, invalid UTF-8).
    fn try_unpack(data: &[u8]) -> Result<(usize, Self::Get<'_>), PackError>;

    /// Decode a non-null value from raw slab bytes.
    ///
    /// Panics if the data is malformed — use [`try_unpack`](Self::try_unpack)
    /// when error handling is needed.
    fn unpack(data: &[u8]) -> (usize, Self::Get<'_>) {
        Self::try_unpack(data).unwrap()
    }

    /// Construct the `Get` value for a null entry.  Panics for non-nullable types.
    ///
    /// The `slab` parameter is unused but anchors the lifetime so that
    /// the return type `Self::Get<'_>` is well-formed for borrowing types.
    fn get_null(slab: &[u8]) -> Self::Get<'_>;

    /// Encode a value (in borrowed `Get` form) to raw slab bytes.
    /// Returns `true` if a value was written, `false` for null entries.
    fn pack(value: Self::Get<'_>, out: &mut Vec<u8>) -> bool;
}

// ── Base impls ──────────────────────────────────────────────────────────────
//
// The slab wire format only has four value types.  All other ColumnValue
// impls delegate to one of these.

impl ColumnValue for Option<u64> {
    type Encoding = RleEncoding<Option<u64>>;
    type Get<'a> = Option<u64>;
    fn as_get(&self) -> Option<u64> {
        *self
    }
}

impl RleValue for Option<u64> {
    const NULLABLE: bool = true;

    fn value_len(data: &[u8]) -> Option<usize> {
        let mut buf = data;
        let start = buf.len();
        leb128::read::unsigned(&mut buf).ok()?;
        Some(start - buf.len())
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, Option<u64>), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::unsigned(&mut buf)?;
        Ok((start - buf.len(), Some(v)))
    }
    fn get_null(_slab: &[u8]) -> Option<u64> {
        None
    }
    fn pack(value: Option<u64>, out: &mut Vec<u8>) -> bool {
        match value {
            Some(v) => {
                leb128::write::unsigned(out, v).unwrap();
                true
            }
            None => false,
        }
    }
}

impl ColumnValue for Option<i64> {
    type Encoding = RleEncoding<Option<i64>>;
    type Get<'a> = Option<i64>;
    fn as_get(&self) -> Option<i64> {
        *self
    }
}

impl RleValue for Option<i64> {
    const NULLABLE: bool = true;

    fn value_len(data: &[u8]) -> Option<usize> {
        let mut buf = data;
        let start = buf.len();
        leb128::read::signed(&mut buf).ok()?;
        Some(start - buf.len())
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, Option<i64>), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::signed(&mut buf)?;
        Ok((start - buf.len(), Some(v)))
    }
    fn get_null(_slab: &[u8]) -> Option<i64> {
        None
    }
    fn pack(value: Option<i64>, out: &mut Vec<u8>) -> bool {
        match value {
            Some(v) => {
                leb128::write::signed(out, v).unwrap();
                true
            }
            None => false,
        }
    }
}

impl ColumnValue for Option<Vec<u8>> {
    type Encoding = RleEncoding<Option<Vec<u8>>>;
    type Get<'a> = Option<&'a [u8]>;
    fn as_get(&self) -> Option<&[u8]> {
        self.as_deref()
    }
}

impl RleValue for Option<Vec<u8>> {
    const NULLABLE: bool = true;

    fn value_len(data: &[u8]) -> Option<usize> {
        let mut buf = data;
        let start = buf.len();
        let len = leb128::read::unsigned(&mut buf).ok()? as usize;
        let hdr = start - buf.len();
        if buf.len() < len {
            return None;
        }
        Some(hdr + len)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, Option<&[u8]>), PackError> {
        let mut buf = data;
        let start = buf.len();
        let len = leb128::read::unsigned(&mut buf)? as usize;
        let hdr = start - buf.len();
        if buf.len() < len {
            return Err(PackError::BadFormat);
        }
        Ok((hdr + len, Some(&buf[..len])))
    }
    fn get_null(_slab: &[u8]) -> Option<&[u8]> {
        None
    }
    fn pack(value: Option<&[u8]>, out: &mut Vec<u8>) -> bool {
        match value {
            Some(v) => {
                leb128::write::unsigned(out, v.len() as u64).unwrap();
                out.extend_from_slice(v);
                true
            }
            None => false,
        }
    }
}

impl ColumnValue for bool {
    type Encoding = BoolEncoding;
    type Get<'a> = bool;
    fn as_get(&self) -> bool {
        *self
    }
}

// ── IntoColumnValue ─────────────────────────────────────────────────────────

/// Conversion trait for values that can be inserted into a column of type `T`.
///
/// This allows `insert` and `splice` to accept both owned and borrowed forms:
///
/// ```ignore
/// col.insert(0, "hello");          // &str → String
/// col.insert(0, Some("hello"));    // Option<&str> → Option<String>
/// col.insert(0, b"bytes".as_slice()); // &[u8] → Vec<u8>
/// ```
///
/// All `ColumnValue` types implement this as an identity conversion.
/// No conflict with the borrowed impls because `&str`, `Option<&str>`, etc.
/// have lifetimes and cannot be `ColumnValue: 'static`.
pub trait IntoColumnValue<T: ColumnValue> {
    /// Borrow as the column's `Get` type without allocating.
    fn as_column_ref(&self) -> T::Get<'_>;

    /// Convert to the owned column value (allocates for `&str` → `String`, etc.).
    fn into_column_value(self) -> T;
}

impl<T: ColumnValue> IntoColumnValue<T> for T {
    #[inline]
    fn as_column_ref(&self) -> T::Get<'_> {
        self.as_get()
    }
    #[inline]
    fn into_column_value(self) -> T {
        self
    }
}

impl IntoColumnValue<String> for &str {
    #[inline]
    fn as_column_ref(&self) -> &str {
        self
    }
    #[inline]
    fn into_column_value(self) -> String {
        self.to_string()
    }
}

impl IntoColumnValue<Option<String>> for Option<&str> {
    #[inline]
    fn as_column_ref(&self) -> Option<&str> {
        *self
    }
    #[inline]
    fn into_column_value(self) -> Option<String> {
        self.map(str::to_string)
    }
}

impl IntoColumnValue<Option<String>> for &str {
    #[inline]
    fn as_column_ref(&self) -> Option<&str> {
        Some(self)
    }
    #[inline]
    fn into_column_value(self) -> Option<String> {
        Some(self.to_string())
    }
}

impl IntoColumnValue<Vec<u8>> for &[u8] {
    #[inline]
    fn as_column_ref(&self) -> &[u8] {
        self
    }
    #[inline]
    fn into_column_value(self) -> Vec<u8> {
        self.to_vec()
    }
}

impl IntoColumnValue<Option<Vec<u8>>> for Option<&[u8]> {
    #[inline]
    fn as_column_ref(&self) -> Option<&[u8]> {
        *self
    }
    #[inline]
    fn into_column_value(self) -> Option<Vec<u8>> {
        self.map(<[u8]>::to_vec)
    }
}

impl IntoColumnValue<Option<Vec<u8>>> for &[u8] {
    #[inline]
    fn as_column_ref(&self) -> Option<&[u8]> {
        Some(self)
    }
    #[inline]
    fn into_column_value(self) -> Option<Vec<u8>> {
        Some(self.to_vec())
    }
}

// ── ColumnDefault ───────────────────────────────────────────────────────────

/// Trait for column value types that have a meaningful default value.
///
/// For `Option<T>` the default is `None` (null).  For `bool` the default is
/// `false`.  Non-nullable, non-bool types (`u64`, `String`, …) do not
/// implement this trait.
///
/// Enables `Column<T>::is_default()`, `Column<T>::init_default()`, and
/// `Column<T>::save_to_unless_default()`.
pub trait ColumnDefault: ColumnValue {
    /// Returns `true` if the slab contains only default values.
    fn slab_is_default(data: &[u8], len: usize) -> bool;

    /// Build a single slab of `len` default values.
    fn default_slab(len: usize) -> column::Slab;
}

impl ColumnDefault for Option<u64> {
    fn slab_is_default(data: &[u8], _len: usize) -> bool {
        data.first() == Some(&0x00)
    }
    fn default_slab(len: usize) -> column::Slab {
        // Wire: signed_leb128(0) unsigned_leb128(len)
        let mut data = vec![0x00u8];
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        column::Slab {
            data,
            len,
            segments: 1,
        }
    }
}

impl ColumnDefault for Option<i64> {
    fn slab_is_default(data: &[u8], _len: usize) -> bool {
        data.first() == Some(&0x00)
    }
    fn default_slab(len: usize) -> column::Slab {
        let mut data = vec![0x00u8];
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        column::Slab {
            data,
            len,
            segments: 1,
        }
    }
}

impl ColumnDefault for Option<String> {
    fn slab_is_default(data: &[u8], _len: usize) -> bool {
        data.first() == Some(&0x00)
    }
    fn default_slab(len: usize) -> column::Slab {
        let mut data = vec![0x00u8];
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        column::Slab {
            data,
            len,
            segments: 1,
        }
    }
}

impl ColumnDefault for Option<Vec<u8>> {
    fn slab_is_default(data: &[u8], _len: usize) -> bool {
        data.first() == Some(&0x00)
    }
    fn default_slab(len: usize) -> column::Slab {
        let mut data = vec![0x00u8];
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        column::Slab {
            data,
            len,
            segments: 1,
        }
    }
}

impl ColumnDefault for bool {
    fn slab_is_default(data: &[u8], len: usize) -> bool {
        // All-false: a single false-run whose count equals len.
        let mut buf = data;
        matches!(leb128::read::unsigned(&mut buf), Ok(count) if count as usize == len)
    }
    fn default_slab(len: usize) -> column::Slab {
        // Bool wire format: [uleb128(len)] = single false-run.
        let mut data = Vec::new();
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        column::Slab {
            data,
            len,
            segments: 1,
        }
    }
}

// ── Delegating impls ────────────────────────────────────────────────────────

impl ColumnValue for u64 {
    type Encoding = RleEncoding<u64>;
    type Get<'a> = u64;
    fn as_get(&self) -> u64 {
        *self
    }
}

impl RleValue for u64 {
    const NULLABLE: bool = false;

    fn value_len(data: &[u8]) -> Option<usize> {
        Option::<u64>::value_len(data)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, u64), PackError> {
        let (n, v) = Option::<u64>::try_unpack(data)?;
        Ok((n, v.expect("unexpected null in non-nullable u64 column")))
    }
    fn get_null(_slab: &[u8]) -> u64 {
        panic!("unexpected null in non-nullable u64 column")
    }
    fn pack(value: u64, out: &mut Vec<u8>) -> bool {
        Option::<u64>::pack(Some(value), out)
    }
}

impl ColumnValue for i64 {
    type Encoding = RleEncoding<i64>;
    type Get<'a> = i64;
    fn as_get(&self) -> i64 {
        *self
    }
}

impl RleValue for i64 {
    const NULLABLE: bool = false;

    fn value_len(data: &[u8]) -> Option<usize> {
        Option::<i64>::value_len(data)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, i64), PackError> {
        let (n, v) = Option::<i64>::try_unpack(data)?;
        Ok((n, v.expect("unexpected null in non-nullable i64 column")))
    }
    fn get_null(_slab: &[u8]) -> i64 {
        panic!("unexpected null in non-nullable i64 column")
    }
    fn pack(value: i64, out: &mut Vec<u8>) -> bool {
        Option::<i64>::pack(Some(value), out)
    }
}

impl ColumnValue for String {
    type Encoding = RleEncoding<String>;
    type Get<'a> = &'a str;
    fn as_get(&self) -> &str {
        self.as_str()
    }
}

impl RleValue for String {
    const NULLABLE: bool = false;

    fn value_len(data: &[u8]) -> Option<usize> {
        Option::<Vec<u8>>::value_len(data)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, &str), PackError> {
        let mut buf = data;
        let start = buf.len();
        let len = leb128::read::unsigned(&mut buf)? as usize;
        let hdr = start - buf.len();
        if buf.len() < len {
            return Err(PackError::BadFormat);
        }
        let s = std::str::from_utf8(&buf[..len]).map_err(|_| PackError::InvalidUtf8)?;
        Ok((hdr + len, s))
    }
    fn get_null(_slab: &[u8]) -> &str {
        panic!("unexpected null in non-nullable String column")
    }
    fn pack(value: &str, out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value.len() as u64).unwrap();
        out.extend_from_slice(value.as_bytes());
        true
    }
}

impl ColumnValue for Option<String> {
    type Encoding = RleEncoding<Option<String>>;
    type Get<'a> = Option<&'a str>;
    fn as_get(&self) -> Option<&str> {
        self.as_deref()
    }
}

impl RleValue for Option<String> {
    const NULLABLE: bool = true;

    fn value_len(data: &[u8]) -> Option<usize> {
        Option::<Vec<u8>>::value_len(data)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, Option<&str>), PackError> {
        let (n, s) = String::try_unpack(data)?;
        Ok((n, Some(s)))
    }
    fn get_null(_slab: &[u8]) -> Option<&str> {
        None
    }
    fn pack(value: Option<&str>, out: &mut Vec<u8>) -> bool {
        Option::<Vec<u8>>::pack(value.map(str::as_bytes), out)
    }
}

impl ColumnValue for Vec<u8> {
    type Encoding = RleEncoding<Vec<u8>>;
    type Get<'a> = &'a [u8];
    fn as_get(&self) -> &[u8] {
        self.as_slice()
    }
}

impl RleValue for Vec<u8> {
    const NULLABLE: bool = false;

    fn value_len(data: &[u8]) -> Option<usize> {
        Option::<Vec<u8>>::value_len(data)
    }
    fn try_unpack(data: &[u8]) -> Result<(usize, &[u8]), PackError> {
        let (n, v) = Option::<Vec<u8>>::try_unpack(data)?;
        Ok((
            n,
            v.expect("unexpected null in non-nullable Vec<u8> column"),
        ))
    }
    fn get_null(_slab: &[u8]) -> &[u8] {
        panic!("unexpected null in non-nullable Vec<u8> column")
    }
    fn pack(value: &[u8], out: &mut Vec<u8>) -> bool {
        Option::<Vec<u8>>::pack(Some(value), out)
    }
}
