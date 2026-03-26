use super::ValidBytes;
use super::ColumnValueRef;
use crate::PackError;

/// Validation function type for [`ColumnEncoding::load_and_verify`].
pub type ValidateFn<V> = for<'a> fn(<V as super::ColumnValueRef>::Get<'a>) -> Option<String>;

/// Trait abstracting the byte-level encoding strategy for a column.
///
/// Implementors provide `get`, bulk encoding/decoding, split/merge, and
/// validation operations on raw `Vec<u8>` slabs.  [`super::Column`] delegates
/// to `T::Encoding`.
///
/// Both [`super::rle::RleEncoding`] and [`super::bool_encoding::BoolEncoding`]
/// are zero-sized types — all state lives in the slab bytes.
pub trait ColumnEncoding: Default {
    /// The column value type this encoding operates on.
    type Value: ColumnValueRef<Encoding = Self>;

    /// Read the value at `index` from `slab`, returning the optimal `Get` type.
    ///
    /// For `Copy` types this returns the value directly.  For ref types
    /// (`str`, `[u8]`) this borrows directly from the slab without allocation.
    /// Returns `None` if `index >= len`.
    fn get<'a>(
        slab: &'a ValidBytes,
        index: usize,
        len: usize,
    ) -> Option<<Self::Value as ColumnValueRef>::Get<'a>>;

    /// Count the total number of segments in `slab`.
    ///
    /// A segment is: one repeat run, one null run, or one value within a
    /// literal run.
    fn count_segments(slab: &[u8]) -> usize;

    /// Split `slab` at logical item `index`, producing two byte arrays:
    /// `[0..index)` and `[index..len)`.
    fn split_at_item(slab: &[u8], index: usize, len: usize) -> (Vec<u8>, Vec<u8>);

    /// Merge two adjacent slab byte arrays into one canonical byte array.
    ///
    /// Only decodes boundary runs (last of `a`, first of `b`); interior bytes
    /// are memcopied.  Returns `(merged_bytes, segment_count)`.
    fn merge_slab_bytes(a: &[u8], b: &[u8]) -> (Vec<u8>, usize);

    /// Validate that `slab` is in canonical encoding form.
    ///
    /// Returns `Ok(())` if the encoding is canonical, or `Err(description)` if
    /// any invariant is violated.  This is intended for testing and debugging.
    fn validate_encoding(slab: &[u8]) -> Result<(), String>;

    /// Encode values into pre-split slabs in a single O(n) pass.
    ///
    /// Returns `(data, item_count, segment_count)` tuples, each respecting
    /// `max_segments`.  Much faster than repeated `insert` calls.
    ///
    /// Accepts any type convertible to the column value via [`super::AsColumnRef`],
    /// so borrowed forms (e.g. `&str` for a `String` column) can be packed
    /// directly without an intermediate owned allocation.
    /// Returns `(slabs, total_item_count)`.
    fn encode_all_slabs<V: super::AsColumnRef<Self::Value>>(
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<(Vec<u8>, usize, usize)>, usize);

    /// Decode and validate raw bytes, splitting into slabs.
    ///
    /// Walks the wire format, validates every encoded value is well-formed,
    /// rejects nulls in non-nullable column types, and splits the data into
    /// slabs respecting `max_segments`.
    ///
    /// If `validate` is `Some`, each decoded value is also passed to the
    /// function during the same pass.  Returns
    /// [`PackError::InvalidValue`] if the function returns `Some(msg)`.
    ///
    /// Returns `(data, item_count, segment_count)` tuples on success.
    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<ValidateFn<Self::Value>>,
    ) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError>;

    /// Serialize multiple slabs into a single canonical byte array in O(n).
    ///
    /// Memcopies slab interiors and only decodes/re-encodes the boundary
    /// runs between adjacent slabs.
    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8>;

    /// Decoder type for iterating over all items in a slab.
    type Decoder<'a>: Iterator<Item = <Self::Value as ColumnValueRef>::Get<'a>> + RunDecoder + Clone;

    /// Create a decoder that yields all items in `slab` in order.
    fn decoder(slab: &ValidBytes) -> Self::Decoder<'_>;
}

/// Trait for decoders that can yield runs of values.
///
/// See [`super::Run`] for the semantics of each run kind.
pub trait RunDecoder: Iterator {
    /// Returns the next run of values without consuming individual items.
    ///
    /// For repeat runs, returns the remaining count and the repeated value.
    /// For null runs, returns the remaining count and the null representation.
    /// For literal runs, returns count=1 and the next value (consuming it).
    ///
    /// Returns `None` when the iterator is exhausted.
    fn next_run(&mut self) -> Option<super::Run<Self::Item>>;
}
