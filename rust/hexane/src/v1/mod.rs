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
pub mod prefix_column;
pub mod rle;
pub(crate) mod rle_state;
pub use column::{Column, Iter};
pub use delta_column::{DeltaColumn, DeltaIter, DeltaValue};
pub use encoding::ColumnEncoding;
pub use encoding::RunDecoder;
pub use indexed::IndexedDeltaColumn;
pub use load_opts::LoadOpts;
pub use prefix_column::{PrefixColumn, PrefixIter, PrefixValue};

#[cfg(test)]
mod tests;

use crate::PackError;

use bool_encoding::BoolEncoding;
use rle::RleEncoding;
use std::fmt::Debug;

// ── Run ─────────────────────────────────────────────────────────────────────

/// A run of identical values from a column iterator.
///
/// For repeat runs, `count` is the number of remaining items (including the
/// current one) and `value` is the repeated value.  For null runs, `count`
/// is the remaining null count and `value` is the type's null representation.
/// For literal runs, `count` is always 1 since each item is distinct.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
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
pub trait ColumnValueRef: 'static + Sized + AsColumnRef<Self> + Debug {
    /// The encoding strategy for this value type.
    type Encoding: ColumnEncoding<Value = Self>;

    /// The optimal return type for `get()`: owned for `Copy` types, borrowed
    /// for ref types (`&str`, `&[u8]`).
    type Get<'a>: Copy + PartialEq + Debug + Default;
}

/// Simplified [`ColumnValueRef`] for `Copy` RLE types where `Get<'a> = Self`.
///
/// Blanket impls provide [`ColumnValueRef`], [`AsColumnRef`], and a nullable
/// `Option<T>` column type automatically.
///
/// ```ignore
/// impl ColumnValue for ValueMeta {
///     type Encoding = RleEncoding<ValueMeta>;
/// }
///
/// impl RleValue for ValueMeta {
///     fn try_unpack(data: &[u8]) -> Result<(usize, ValueMeta), PackError> { ... }
///     fn pack(value: ValueMeta, out: &mut Vec<u8>) -> bool { ... }
/// }
/// // That's it — Column<ValueMeta> and Column<Option<ValueMeta>> now work.
/// ```
pub trait ColumnValue: Copy + PartialEq + Debug + Default + 'static {
    /// The encoding strategy — always `RleEncoding<Self>` for RLE types.
    type Encoding: ColumnEncoding<Value = Self>;
}

impl<T: ColumnValue> ColumnValueRef for T {
    type Encoding = T::Encoding;
    type Get<'a> = T;
}

/// Extension of [`ColumnValueRef`] with RLE-specific encoding/decoding methods.
///
/// This is implemented by all value types that use [`RleEncoding`] — i.e.,
/// everything except `bool` (which uses [`BoolEncoding`]).
pub trait RleValue: ColumnValueRef {
    /// Whether this column type allows null entries.
    ///
    /// `true` for `Option<T>` types, `false` for bare `T`.  Used by `load`
    /// to reject null runs in non-nullable columns.
    const NULLABLE: bool = false;

    /// Check if a value is null. Always returns `false` for non-nullable types.
    fn is_null(_value: Self::Get<'_>) -> bool {
        false
    }

    /// Return the byte length of one encoded value at the start of `data`,
    /// or `None` if the data is malformed / too short.
    ///
    /// This is the primary "skip over a value" operation used by `scan_to`,
    /// `split_at_item`, `count_segments`, and similar slab-walking functions.
    ///
    /// The default delegates to [`try_unpack`](Self::try_unpack) and discards
    /// the decoded value.  Override for types where skipping is cheaper than
    /// decoding (e.g. `String` avoids UTF-8 validation).
    fn value_len(data: &[u8]) -> Option<usize> {
        Self::try_unpack(data).ok().map(|(n, _)| n)
    }

    /// Decode a non-null value from raw slab bytes with full validation.
    ///
    /// Returns `(bytes_consumed, value)` on success, or a [`PackError`] if the
    /// data is malformed (e.g. truncated LEB128, invalid UTF-8).
    fn try_unpack(data: &[u8]) -> Result<(usize, Self::Get<'_>), PackError>;

    /// Decode a non-null value from slab bytes that have already been validated
    /// during load. The default delegates to `try_unpack` and unwraps.
    fn unpack(data: &[u8]) -> (usize, Self::Get<'_>) {
        Self::try_unpack(data).unwrap()
    }

    /// Construct the `Get` value for a null entry.
    ///
    /// The default panics, which is correct for non-nullable types (`NULLABLE = false`).
    /// Nullable types (`Option<T>`) must override this to return `None`.
    ///
    /// The `slab` parameter is unused but anchors the lifetime so that
    /// the return type `Self::Get<'_>` is well-formed for borrowing types.
    fn get_null<'a>() -> Self::Get<'a> {
        panic!("unexpected null in non-nullable column")
    }

    /// Encode a value (in borrowed `Get` form) to raw slab bytes.
    /// Returns `true` if a value was written, `false` for null entries.
    fn pack(value: Self::Get<'_>, out: &mut Vec<u8>) -> bool;
}

// ── Option<T> blanket impls ─────────────────────────────────────────────────
//
// Any `T: RleValue` automatically gets `Option<T>` as a nullable column type.
// The blanket wraps `T`'s encoding/decoding in `Some`/`None`.

impl<T: RleValue> ColumnValueRef for Option<T> {
    type Encoding = RleEncoding<Option<T>>;
    type Get<'a> = Option<T::Get<'a>>;
}

impl<T: RleValue> AsColumnRef<Option<T>> for Option<T> {
    #[inline]
    fn as_column_ref(&self) -> Option<T::Get<'_>> {
        self.as_ref().map(|v| v.as_column_ref())
    }
}

impl<T: RleValue> RleValue for Option<T> {
    const NULLABLE: bool = true;

    fn value_len(data: &[u8]) -> Option<usize> {
        T::value_len(data)
    }

    fn try_unpack(data: &[u8]) -> Result<(usize, Option<T::Get<'_>>), PackError> {
        let (n, v) = T::try_unpack(data)?;
        Ok((n, Some(v)))
    }

    fn unpack(data: &[u8]) -> (usize, Option<T::Get<'_>>) {
        let (n, v) = T::unpack(data);
        (n, Some(v))
    }

    fn is_null(value: Option<T::Get<'_>>) -> bool {
        value.is_none()
    }

    fn get_null<'a>() -> Option<T::Get<'a>> {
        None
    }

    fn pack(value: Option<T::Get<'_>>, out: &mut Vec<u8>) -> bool {
        match value {
            Some(v) => T::pack(v, out),
            None => false,
        }
    }
}

// ── Base impls ──────────────────────────────────────────────────────────────
//
// The slab wire format only has four value types.  All other ColumnValueRef
// impls delegate to one of these.

impl ColumnValueRef for bool {
    type Encoding = BoolEncoding;
    type Get<'a> = bool;
}

impl AsColumnRef<bool> for bool {
    #[inline]
    fn as_column_ref(&self) -> bool {
        *self
    }
}

// ── AsColumnRef ─────────────────────────────────────────────────────────

/// Conversion trait for values that can be inserted into a column of type `T`.
///
/// This allows `insert` and `splice` to accept both owned and borrowed forms:
///
/// ```ignore
/// col.insert(0, "hello");          // &str for Column<String>
/// col.insert(0, Some("hello"));    // Option<&str> for Column<Option<String>>
/// col.insert(0, b"bytes".as_slice()); // &[u8] for Column<Vec<u8>>
/// ```
///
/// All `ColumnValueRef` types implement this as an identity conversion.
/// No conflict with the borrowed impls because `&str`, `Option<&str>`, etc.
/// have lifetimes and cannot be `ColumnValueRef: 'static`.
pub trait AsColumnRef<T: ColumnValueRef>: Debug + Clone + Default {
    /// Borrow as the column's `Get` type without allocating.
    fn as_column_ref(&self) -> T::Get<'_>;
}

impl<T: ColumnValue> AsColumnRef<T> for T {
    #[inline]
    fn as_column_ref(&self) -> T {
        *self
    }
}

impl AsColumnRef<String> for String {
    #[inline]
    fn as_column_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsColumnRef<String> for &str {
    #[inline]
    fn as_column_ref(&self) -> &str {
        self
    }
}

impl AsColumnRef<Option<String>> for Option<&str> {
    #[inline]
    fn as_column_ref(&self) -> Option<&str> {
        *self
    }
}

impl AsColumnRef<Option<String>> for &str {
    #[inline]
    fn as_column_ref(&self) -> Option<&str> {
        Some(self)
    }
}

impl AsColumnRef<Vec<u8>> for Vec<u8> {
    #[inline]
    fn as_column_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsColumnRef<Vec<u8>> for &[u8] {
    #[inline]
    fn as_column_ref(&self) -> &[u8] {
        self
    }
}

impl AsColumnRef<Option<Vec<u8>>> for Option<&[u8]> {
    #[inline]
    fn as_column_ref(&self) -> Option<&[u8]> {
        *self
    }
}

impl AsColumnRef<Option<Vec<u8>>> for &[u8] {
    #[inline]
    fn as_column_ref(&self) -> Option<&[u8]> {
        Some(self)
    }
}

// ── Delegating impls ────────────────────────────────────────────────────────

impl ColumnValue for u64 {
    type Encoding = RleEncoding<u64>;
}

impl RleValue for u64 {
    fn try_unpack(data: &[u8]) -> Result<(usize, u64), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::unsigned(&mut buf)?;
        Ok((start - buf.len(), v))
    }
    fn pack(value: u64, out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value).unwrap();
        true
    }
}

impl ColumnValue for i64 {
    type Encoding = RleEncoding<i64>;
}

impl RleValue for i64 {
    fn try_unpack(data: &[u8]) -> Result<(usize, i64), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::signed(&mut buf)?;
        Ok((start - buf.len(), v))
    }
    fn pack(value: i64, out: &mut Vec<u8>) -> bool {
        leb128::write::signed(out, value).unwrap();
        true
    }
}

impl ColumnValueRef for String {
    type Encoding = RleEncoding<String>;
    type Get<'a> = &'a str;
}

impl RleValue for String {
    fn value_len(data: &[u8]) -> Option<usize> {
        Vec::<u8>::value_len(data)
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
    /// Data was validated during load — UTF-8 was checked by try_unpack.
    /// Data was validated during load, so the unwrap never fires.
    fn unpack(data: &[u8]) -> (usize, &str) {
        let mut cursor = data;
        let start = cursor.len();
        let len = leb128::read::unsigned(&mut cursor).unwrap() as usize;
        let hdr = start - cursor.len();
        let s = std::str::from_utf8(&cursor[..len]).unwrap();
        (hdr + len, s)
    }
    fn pack(value: &str, out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value.len() as u64).unwrap();
        out.extend_from_slice(value.as_bytes());
        true
    }
}

impl ColumnValueRef for Vec<u8> {
    type Encoding = RleEncoding<Vec<u8>>;
    type Get<'a> = &'a [u8];
}

impl RleValue for Vec<u8> {
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
    fn try_unpack(data: &[u8]) -> Result<(usize, &[u8]), PackError> {
        let mut buf = data;
        let start = buf.len();
        let len = leb128::read::unsigned(&mut buf)? as usize;
        let hdr = start - buf.len();
        if buf.len() < len {
            return Err(PackError::BadFormat);
        }
        Ok((hdr + len, &buf[..len]))
    }
    fn pack(value: &[u8], out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value.len() as u64).unwrap();
        out.extend_from_slice(value);
        true
    }
}
