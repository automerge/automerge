#![doc = include_str!("../README.md")]

#[doc(hidden)]
#[macro_export]
macro_rules! log {
     ( $( $t:tt )* ) => {
          {
            use $crate::__log;
            __log!( $( $t )* );
          }
     }
 }

#[cfg(all(feature = "wasm", target_family = "wasm"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
     }
 }

mod error;
pub use error::PackError;

pub use codec::{lebsize, ulebsize, Codec, Leb128, VarBuf};

#[cfg(feature = "bijou64")]
pub use codec::Bijou64;

/// LEB128-pinned aliases for the column types.
///
/// `use hexane::leb128::Column` reads the same as `use hexane::Column`
/// (LEB128 is the crate-wide default) but states the wire format at the
/// import, mirroring the `bijou` module (behind the `bijou64`
/// feature).  Code that mixes codecs can import both
/// modules and qualify at the use site.
pub mod leb128 {
    use crate::Leb128;

    pub type Column<T> = crate::Column<T, Leb128>;
    pub type DeltaColumn<T> = crate::DeltaColumn<T, Leb128>;
    pub type PrefixColumn<T> = crate::PrefixColumn<T, Leb128>;
    pub type Encoder<'a, T> = crate::Encoder<'a, T, Leb128>;
    pub type Decoder<'a, T> = crate::Decoder<'a, T, Leb128>;
    pub type DeltaEncoder<'a, T> = crate::DeltaEncoder<'a, T, Leb128>;
    pub type DeltaDecoder<'a, T> = crate::DeltaDecoder<'a, T, Leb128>;

    /// Construct a streaming [`Decoder`] over raw column bytes.
    pub fn decoder<T: crate::ColumnValueRef>(data: &[u8]) -> Decoder<'_, T> {
        crate::decoder_in::<T, Leb128>(data)
    }
}

/// Bijou64-pinned aliases for the column types (feature `bijou64`).
///
/// `use hexane::bijou::Column` gives the same API as [`crate::Column`]
/// with the [`Bijou64`] wire format, so a consuming crate picks its codec
/// with one import:
///
/// ```
/// # #[cfg(feature = "bijou64")] {
/// use hexane::bijou::Column;
///
/// let mut col = Column::<u32>::new();
/// col.push(300u32);
/// let col = Column::<u32>::load(&col.save()).unwrap();
/// assert_eq!(col.get(0), Some(300));
/// # }
/// ```
#[cfg(feature = "bijou64")]
pub mod bijou {
    use crate::Bijou64;

    pub type Column<T> = crate::Column<T, Bijou64>;
    pub type DeltaColumn<T> = crate::DeltaColumn<T, Bijou64>;
    pub type PrefixColumn<T> = crate::PrefixColumn<T, Bijou64>;
    pub type Encoder<'a, T> = crate::Encoder<'a, T, Bijou64>;
    pub type Decoder<'a, T> = crate::Decoder<'a, T, Bijou64>;
    pub type DeltaEncoder<'a, T> = crate::DeltaEncoder<'a, T, Bijou64>;
    pub type DeltaDecoder<'a, T> = crate::DeltaDecoder<'a, T, Bijou64>;

    /// Construct a streaming [`Decoder`] over raw column bytes.
    pub fn decoder<T: crate::ColumnValueRef>(data: &[u8]) -> Decoder<'_, T> {
        crate::decoder_in::<T, Bijou64>(data)
    }
}

pub mod bool;
pub(crate) mod btree;
pub mod codec;
pub mod column;
pub mod delta;
pub mod encoder;
pub mod encoding;
#[doc(hidden)]
pub mod index;
pub mod load_opts;
pub mod prefix;
pub mod raw;
pub mod rle;
pub mod shift;
pub use column::{Column, ColumnLoadIter, Iter, IterState};
pub use delta::indexed::FindByRange;
pub use delta::{DeltaColumn, DeltaDecoder, DeltaEncoder, DeltaIter, DeltaIterState, DeltaValue};
pub use shift::{Shiftable, Unshift};
/// Streaming encoder for column type `T`, resolved via `T::Encoding<C>`.
///
/// For RLE types (u64, i64, String, etc.) this resolves to `RleEncoder`.
/// For bool this resolves to `BoolEncoder`.  The codec parameter defaults
/// to [`Leb128`].
pub type Encoder<'a, T, C = Leb128> =
    <<T as ColumnValueRef>::Encoding<C> as ColumnEncoding>::Encoder<'a>;

/// Streaming decoder for column type `T`, resolved via `T::Encoding<C>`.
///
/// Symmetric with [`Encoder`]: yields `T::Get<'a>` items from raw bytes
/// without allocating a `Column`.  Construct via [`decoder`].
pub type Decoder<'a, T, C = Leb128> =
    <<T as ColumnValueRef>::Encoding<C> as ColumnEncoding>::Decoder<'a>;

/// Construct a streaming [`Decoder`] over raw column bytes ([`Leb128`]).
///
/// ```no_run
/// # let bytes: Vec<u8> = vec![0];
/// for v in hexane::decoder::<Option<u64>>(&bytes) {
///     // do something
/// }
/// ```
pub fn decoder<'a, T: ColumnValueRef>(data: &'a [u8]) -> Decoder<'a, T> {
    <T::Encoding<Leb128> as ColumnEncoding>::decoder(data)
}

/// Construct a streaming [`Decoder`] over raw column bytes in codec `C`.
pub fn decoder_in<'a, T: ColumnValueRef, C: Codec>(data: &'a [u8]) -> Decoder<'a, T, C> {
    <T::Encoding<C> as ColumnEncoding>::decoder(data)
}
pub use bool::BoolLoadIter;
pub use btree::SlabAggregate;
pub use encoding::ColumnEncoding;
pub use encoding::EncoderApi;
pub use encoding::LoadIterApi;
pub use encoding::RunDecoder;
pub use encoding::RunSrc;
pub use load_opts::{Fill, LoadOpts, MaybeFill, NoFill};
pub use prefix::{
    PrefixColumn, PrefixColumnLoadIter, PrefixIter, PrefixIterState, PrefixValue, PrefixedValue,
};
pub use raw::{RawColumn, RawColumnIter};
pub use rle::RleLoadIter;

/// Sealing for the internal plumbing traits (`WeightFn`, `SlabWeight`,
/// `ColumnIndex`).  They must be `pub` because they appear in `Column`'s
/// generic bounds, but they are implementation detail: sealing keeps
/// external crates from implementing them, so the index/weight machinery
/// can change shape in any release without breakage.
mod sealed {
    pub trait Sealed {}
}

#[cfg(test)]
mod tests;

pub use bool::BoolEncoding;
pub use rle::RleEncoding;
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
    /// The encoding strategy for this value type, parameterized by the
    /// varint codec `C` (see [`Codec`]).  `Column<T>` uses
    /// `T::Encoding<Leb128>`; `Column<T, C>` uses `T::Encoding<C>`.
    type Encoding<C: Codec>: ColumnEncoding<Value = Self, Codec = C>;

    /// The optimal return type for `get()`: owned for `Copy` types, borrowed
    /// for ref types (`&str`, `&[u8]`).
    type Get<'a>: Copy + PartialEq + Debug;

    /// Convert a borrowed `Get` value back to an owned `Self`.
    fn to_owned(val: Self::Get<'_>) -> Self;

    /// Cross-lifetime equality: compare two `Get` values that may have
    /// different borrow lifetimes.
    fn eq(a: Self::Get<'_>, b: Self::Get<'_>) -> bool;

    /// Reborrow a `Get` value at a shorter lifetime.
    ///
    /// `Get<'l>` is covariant in `'l` for every implementation — owned
    /// `Copy` values ignore the lifetime, borrowed forms are plain
    /// references — but Rust can't express that for a generic associated
    /// type, so implementations provide the coercion (always just `*v` or
    /// a `map` over it).  Lets generic code like `RleCow::get` shorten
    /// lifetimes without `unsafe`.
    fn shorten<'s>(v: &'s Self::Get<'_>) -> Self::Get<'s>;
}

/// Marker trait for `Copy` value types, giving them a blanket
/// [`ColumnValueRef`] impl with `Get<'a> = Self`.
///
/// To store a new type in a column, implement this (to pick the encoding)
/// plus [`RleValue`] (to define the wire format):
///
/// ```
/// # use hexane::{Codec, Column, ColumnValue, RleEncoding, RleValue};
/// # use hexane::PackError;
/// #[derive(Clone, Copy, PartialEq, Debug)]
/// enum ValueMeta {
///     Int,
///     Str,
/// }
///
/// impl ColumnValue for ValueMeta {
///     type Encoding<C: Codec> = RleEncoding<ValueMeta, C>;
/// }
///
/// impl RleValue for ValueMeta {
///     // `C` is the column's varint codec — impls that don't store
///     // varints (like this one-byte tag) simply ignore it.
///     fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, ValueMeta), PackError> {
///         match data.first() {
///             Some(0) => Ok((1, ValueMeta::Int)),
///             Some(1) => Ok((1, ValueMeta::Str)),
///             Some(_) => Err(PackError::InvalidValue("bad ValueMeta tag".into())),
///             None => Err(PackError::BadFormat),
///         }
///     }
///     fn pack<C: Codec>(value: ValueMeta, out: &mut Vec<u8>) -> bool {
///         out.push(match value {
///             ValueMeta::Int => 0,
///             ValueMeta::Str => 1,
///         });
///         true
///     }
/// }
///
/// // That's it — Column<ValueMeta> and Column<Option<ValueMeta>> now work.
/// let mut col = Column::<Option<ValueMeta>>::new();
/// col.push(Some(ValueMeta::Int));
/// col.push(None::<ValueMeta>);
/// assert_eq!(col.get(0), Some(Some(ValueMeta::Int)));
/// assert_eq!(col.get(1), Some(None));
/// ```
pub trait ColumnValue: Copy + PartialEq + Debug + 'static {
    /// The encoding strategy — always `RleEncoding<Self, C>` for RLE types.
    type Encoding<C: Codec>: ColumnEncoding<Value = Self, Codec = C>;
}

impl<T: ColumnValue> ColumnValueRef for T {
    type Encoding<C: Codec> = T::Encoding<C>;
    type Get<'a> = T;

    fn to_owned(val: T) -> T {
        val
    }
    fn eq(a: T, b: T) -> bool {
        a == b
    }
    fn shorten(v: &T) -> T {
        *v
    }
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
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        Self::try_unpack::<C>(data).ok().map(|(n, _)| n)
    }

    /// Decode a non-null value from raw slab bytes with full validation.
    ///
    /// `C` is the column's varint codec — every varint the impl reads must
    /// go through it.  Impls that don't store varints ignore it.
    ///
    /// Returns `(bytes_consumed, value)` on success, or a [`PackError`] if the
    /// data is malformed (e.g. truncated varint, invalid UTF-8).
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Self::Get<'_>), PackError>;

    /// Decode a non-null value from slab bytes that have already been validated
    /// during load. The default delegates to `try_unpack` and unwraps.
    fn unpack<C: Codec>(data: &[u8]) -> (usize, Self::Get<'_>) {
        Self::try_unpack::<C>(data).unwrap()
    }

    /// Construct the `Get` value for a null entry.
    ///
    /// # Panics
    ///
    /// The default panics, which is correct for non-nullable types (`NULLABLE = false`).
    /// Nullable types (`Option<T>`) must override this to return `None`.
    fn get_null<'a>() -> Self::Get<'a> {
        panic!("unexpected null in non-nullable column")
    }

    /// Encode a value (in borrowed `Get` form) to raw slab bytes.
    /// Returns `true` if a value was written, `false` for null entries.
    ///
    /// `C` is the column's varint codec — every varint the impl writes must
    /// go through it.  Impls that don't store varints ignore it.
    fn pack<C: Codec>(value: Self::Get<'_>, out: &mut Vec<u8>) -> bool;
}

// ── Option<T> blanket impls ─────────────────────────────────────────────────
//
// Any `T: RleValue` automatically gets `Option<T>` as a nullable column type.
// The blanket wraps `T`'s encoding/decoding in `Some`/`None`.

impl<T: RleValue> ColumnValueRef for Option<T> {
    type Encoding<C: Codec> = RleEncoding<Option<T>, C>;
    type Get<'a> = Option<T::Get<'a>>;

    fn to_owned(val: Option<T::Get<'_>>) -> Option<T> {
        val.map(T::to_owned)
    }
    fn eq(a: Option<T::Get<'_>>, b: Option<T::Get<'_>>) -> bool {
        match (a, b) {
            (Some(a), Some(b)) => T::eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    fn shorten<'s>(v: &'s Option<T::Get<'_>>) -> Option<T::Get<'s>> {
        v.as_ref().map(T::shorten)
    }
}

impl<T: RleValue> AsColumnRef<Option<T>> for Option<T> {
    #[inline]
    fn as_column_ref(&self) -> Option<T::Get<'_>> {
        self.as_ref().map(|v| v.as_column_ref())
    }
}

impl<T: RleValue> RleValue for Option<T> {
    const NULLABLE: bool = true;

    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        T::value_len::<C>(data)
    }

    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, Option<T::Get<'_>>), PackError> {
        let (n, v) = T::try_unpack::<C>(data)?;
        Ok((n, Some(v)))
    }

    fn unpack<C: Codec>(data: &[u8]) -> (usize, Option<T::Get<'_>>) {
        let (n, v) = T::unpack::<C>(data);
        (n, Some(v))
    }

    fn is_null(value: Option<T::Get<'_>>) -> bool {
        value.is_none()
    }

    fn get_null<'a>() -> Option<T::Get<'a>> {
        None
    }

    fn pack<C: Codec>(value: Option<T::Get<'_>>, out: &mut Vec<u8>) -> bool {
        match value {
            Some(v) => T::pack::<C>(v, out),
            None => false,
        }
    }
}

// ── Base impls ──────────────────────────────────────────────────────────────
//
// The slab wire format only has four value types.  All other ColumnValueRef
// impls delegate to one of these.

impl ColumnValueRef for bool {
    type Encoding<C: Codec> = BoolEncoding<C>;
    type Get<'a> = bool;

    fn to_owned(val: bool) -> bool {
        val
    }
    fn eq(a: bool, b: bool) -> bool {
        a == b
    }
    fn shorten(v: &bool) -> bool {
        *v
    }
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
/// ```
/// # use hexane::Column;
/// let mut strings = Column::<String>::new();
/// strings.insert(0, "hello");                // &str
/// strings.insert(1, String::from("world"));  // String
///
/// let mut nullable = Column::<Option<String>>::new();
/// nullable.insert(0, Some("hello"));         // Option<&str>
///
/// let mut bytes = Column::<Vec<u8>>::new();
/// bytes.insert(0, b"bytes".as_slice());      // &[u8]
///
/// assert_eq!(strings.get(0), Some("hello"));
/// assert_eq!(nullable.get(0), Some(Some("hello")));
/// assert_eq!(bytes.get(0), Some(b"bytes".as_slice()));
/// ```
///
/// All `ColumnValueRef` types implement this as an identity conversion.
/// No conflict with the borrowed impls because `&str`, `Option<&str>`, etc.
/// have lifetimes and cannot be `ColumnValueRef: 'static`.
pub trait AsColumnRef<T: ColumnValueRef>: Debug + Clone {
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
    type Encoding<C: Codec> = RleEncoding<u64, C>;
}

impl RleValue for u64 {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, u64), PackError> {
        C::try_read_unsigned(data)
    }
    /// Skip without decoding — codecs answer from the byte structure alone.
    /// Used by `RleDecoder::nth` when skipping past values whose value the
    /// caller will discard.
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::unsigned_len(data)
    }
    fn pack<C: Codec>(value: u64, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value));
        true
    }
}

impl ColumnValue for i64 {
    type Encoding<C: Codec> = RleEncoding<i64, C>;
}

impl RleValue for i64 {
    #[inline(always)]
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, i64), PackError> {
        C::try_read_signed(data)
    }
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::signed_len(data)
    }
    fn pack<C: Codec>(value: i64, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_signed(value));
        true
    }
}

impl ColumnValue for u32 {
    type Encoding<C: Codec> = RleEncoding<u32, C>;
}

impl RleValue for u32 {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, u32), PackError> {
        let (n, v) = C::try_read_unsigned(data)?;
        let v = u32::try_from(v).map_err(|_| PackError::InvalidValue("u32 overflow".into()))?;
        Ok((n, v))
    }
    /// Skipping doesn't need the range check — data was validated at load.
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::unsigned_len(data)
    }
    fn pack<C: Codec>(value: u32, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value as u64));
        true
    }
}

impl ColumnValue for usize {
    type Encoding<C: Codec> = RleEncoding<usize, C>;
}

impl RleValue for usize {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, usize), PackError> {
        let (n, v) = C::try_read_unsigned(data)?;
        let v = usize::try_from(v).map_err(|_| PackError::InvalidValue("usize overflow".into()))?;
        Ok((n, v))
    }
    /// Skipping doesn't need the range check — data was validated at load.
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::unsigned_len(data)
    }
    fn pack<C: Codec>(value: usize, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value as u64));
        true
    }
}

impl ColumnValue for std::num::NonZeroU32 {
    type Encoding<C: Codec> = RleEncoding<std::num::NonZeroU32, C>;
}

impl RleValue for std::num::NonZeroU32 {
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, std::num::NonZeroU32), PackError> {
        let (n, v) = C::try_read_unsigned(data)?;
        let v = u32::try_from(v).map_err(|_| PackError::InvalidValue("u32 overflow".into()))?;
        let v = std::num::NonZeroU32::new(v)
            .ok_or_else(|| PackError::InvalidValue("NonZeroU32 is zero".into()))?;
        Ok((n, v))
    }
    /// Skipping doesn't need the zero/range checks — data was validated at load.
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        C::unsigned_len(data)
    }
    fn pack<C: Codec>(value: std::num::NonZeroU32, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.get() as u64));
        true
    }
}

impl ColumnValueRef for String {
    type Encoding<C: Codec> = RleEncoding<String, C>;
    type Get<'a> = &'a str;

    fn to_owned(val: &str) -> String {
        val.to_string()
    }
    fn eq(a: &str, b: &str) -> bool {
        a == b
    }
    fn shorten<'s>(v: &'s &str) -> &'s str {
        v
    }
}

impl RleValue for String {
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        Vec::<u8>::value_len::<C>(data)
    }
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, &str), PackError> {
        let (hdr, len) = C::try_read_unsigned(data)?;
        let len = len as usize;
        let rest = &data[hdr..];
        if rest.len() < len {
            return Err(PackError::BadFormat);
        }
        let s = std::str::from_utf8(&rest[..len]).map_err(|_| PackError::InvalidUtf8)?;
        Ok((hdr + len, s))
    }
    /// Data was validated during load — UTF-8 was checked by `try_unpack`,
    /// so we skip re-validation on this hot path.  The byte slice was either
    /// produced by `pack` (which only writes valid UTF-8) or validated when
    /// the column was loaded from external bytes.
    fn unpack<C: Codec>(data: &[u8]) -> (usize, &str) {
        let (hdr, len) = C::read_unsigned(data).unwrap();
        let len = len as usize;
        // Safety: hexane only writes valid UTF-8 via `pack` (extends from
        // `&str::as_bytes`), and `Column::load` calls `try_unpack` on every
        // value during validation — see `Column::load`'s validate-on-load
        // path.  Internal column manipulation never produces invalid UTF-8.
        let s = unsafe { std::str::from_utf8_unchecked(&data[hdr..hdr + len]) };
        (hdr + len, s)
    }

    fn pack<C: Codec>(value: &str, out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.len() as u64));
        out.extend_from_slice(value.as_bytes());
        true
    }
}

impl ColumnValueRef for Vec<u8> {
    type Encoding<C: Codec> = RleEncoding<Vec<u8>, C>;
    type Get<'a> = &'a [u8];

    fn to_owned(val: &[u8]) -> Vec<u8> {
        val.to_vec()
    }
    fn eq(a: &[u8], b: &[u8]) -> bool {
        a == b
    }
    fn shorten<'s>(v: &'s &[u8]) -> &'s [u8] {
        v
    }
}

impl RleValue for Vec<u8> {
    fn value_len<C: Codec>(data: &[u8]) -> Option<usize> {
        let (hdr, len) = C::read_unsigned(data)?;
        let len = len as usize;
        if data.len() - hdr < len {
            return None;
        }
        Some(hdr + len)
    }
    fn try_unpack<C: Codec>(data: &[u8]) -> Result<(usize, &[u8]), PackError> {
        let (hdr, len) = C::try_read_unsigned(data)?;
        let len = len as usize;
        let rest = &data[hdr..];
        if rest.len() < len {
            return Err(PackError::BadFormat);
        }
        Ok((hdr + len, &rest[..len]))
    }
    fn pack<C: Codec>(value: &[u8], out: &mut Vec<u8>) -> bool {
        out.extend(C::encode_unsigned(value.len() as u64));
        out.extend_from_slice(value);
        true
    }
}
