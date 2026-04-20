use std::marker::PhantomData;
use std::ops::Range;

use super::btree::DeltaAggregate;
use super::column::{Column, WeightFn};
use super::encoding::{ColumnEncoding, RunDecoder};
use super::prefix::{PrefixValue, PrefixWeightFn};
use super::{ColumnValueRef, RleValue, TypedLoadOpts};
use crate::PackError;

// ── DeltaValue trait ────────────────────────────────────────────────────────

/// Trait for value types that can be stored in a delta-encoded column.
///
/// All types store deltas internally using `Self::Inner` — `i64` for
/// non-nullable types, `Option<i64>` for nullable types.
///
/// | External type   | `Inner`       |
/// |-----------------|---------------|
/// | `u32`           | `i64`         |
/// | `u64`           | `i64`         |
/// | `i32`           | `i64`         |
/// | `i64`           | `i64`         |
/// | `Option<u32>`   | `Option<i64>` |
/// | `Option<u64>`   | `Option<i64>` |
/// | `Option<i32>`   | `Option<i64>` |
/// | `Option<i64>`   | `Option<i64>` |
pub trait DeltaValue: Copy + PartialEq + Default {
    /// The inner column value type for storing deltas.
    type Inner: PrefixValue + Copy;

    /// Whether this type supports null values.
    const NULLABLE: bool;

    /// Convert to `i64` for delta computation. Returns `None` for null values.
    fn to_i64(self) -> Option<i64>;

    /// Convert from an `i64` realized value back to this type.
    fn from_i64(v: i64) -> Self;

    /// Checked conversion from `i64`.  Returns `Err` if the value is
    /// out of range for `Self` (e.g. negative for unsigned types, or
    /// exceeds the type's max).  Used during `load` to validate data.
    fn try_from_i64(v: i64) -> Result<Self, String> {
        Ok(Self::from_i64(v))
    }

    /// Create a null value. Panics for non-nullable types.
    fn null_value() -> Self;

    /// Create an inner delta value from an `Option<i64>` delta.
    /// For non-nullable types, `None` input panics.
    fn make_inner(delta: Option<i64>) -> Self::Inner;

    /// Extract `Option<i64>` from an inner column get result.
    /// Returns `None` for null deltas, `Some(d)` for non-null.
    fn get_inner(inner: <Self::Inner as ColumnValueRef>::Get<'_>) -> Option<i64>;

    /// Convert the prefix sum type to `i64`.
    fn prefix_to_i64(p: <Self::Inner as PrefixValue>::Prefix) -> i64;
}

// ── Non-nullable impls ──────────────────────────────────────────────────────

impl DeltaValue for u32 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as u32
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        u32::try_from(v).map_err(|_| format!("delta value {} out of u32 range", v))
    }
    fn null_value() -> Self {
        panic!("non-nullable u32")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable u32")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for u64 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as u64
    }
    fn null_value() -> Self {
        panic!("non-nullable u64")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable u64")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for i32 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as i32
    }
    fn null_value() -> Self {
        panic!("non-nullable i32")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable i32")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for i64 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self)
    }
    fn from_i64(v: i64) -> Self {
        v
    }
    fn null_value() -> Self {
        panic!("non-nullable i64")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable i64")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for usize {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as usize
    }
    fn null_value() -> Self {
        panic!("non-nullable usize")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable usize")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

// ── Nullable impls ──────────────────────────────────────────────────────────

impl DeltaValue for Option<u32> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as u32)
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        u32::try_from(v)
            .map(Some)
            .map_err(|_| format!("delta value {} out of u32 range", v))
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<u64> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as u64)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<i32> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as i32)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<i64> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self
    }
    fn from_i64(v: i64) -> Self {
        Some(v)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<usize> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as usize)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

// ── DeltaEncoder ────────────────────────────────────────────────────────────

/// Streaming encoder for delta-encoded columns.
///
/// Mirrors [`RleEncoder`](super::encoder::RleEncoder)'s interface but applies
/// delta encoding on append: each absolute value is transformed into the
/// difference from the previous non-null value before being written to an
/// inner RLE encoder.  The serialized bytes are byte-compatible with both
/// [`DeltaColumn::save`] and v0's `DeltaCursor::encode`.
///
/// Use this when you need to build a delta column incrementally (e.g. while
/// walking change ops) rather than collecting a `Vec` and calling
/// [`DeltaColumn::from_values`].
///
/// ```ignore
/// let mut enc = DeltaEncoder::<i64>::new();
/// enc.append(10);
/// enc.append(20);
/// enc.append(30);
/// let bytes = enc.save(); // [10, 10, 10] deltas, RLE-encoded
/// ```
pub struct DeltaEncoder<'a, T: DeltaValue>
where
    T::Inner: super::RleValue,
{
    inner: super::encoder::RleEncoder<'a, T::Inner>,
    abs: i64,
    /// Tracks whether every appended value has been equal so far.
    ///
    /// - `None` — either nothing has been appended yet, or the appended
    ///   values are not all equal (i.e. the column is "mixed").
    /// - `Some(v)` — every appended value has been equal to `v`.
    ///
    /// Used by [`save_to_unless`](Self::save_to_unless) to match v0's
    /// `encode_unless_empty` semantics for nullable columns (where
    /// `v == null`) and to provide RleEncoder-style single-run-of-value
    /// elision for non-nullable columns.
    uniform: Option<T>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue> Default for DeltaEncoder<'_, T>
where
    T::Inner: super::RleValue,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DeltaValue> std::fmt::Debug for DeltaEncoder<'_, T>
where
    T::Inner: super::RleValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaEncoder")
            .field("len", &self.inner.len())
            .field("abs", &self.abs)
            .finish()
    }
}

impl<'a, T: DeltaValue> DeltaEncoder<'a, T>
where
    T::Inner: super::RleValue,
{
    /// Create a new empty delta encoder.
    pub fn new() -> Self {
        Self {
            inner: super::encoder::RleEncoder::new(),
            abs: 0,
            uniform: None,
            _phantom: PhantomData,
        }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append a single (absolute) value.
    ///
    /// For non-nullable types the value is always stored.  For nullable
    /// types, a `null` value does not advance the running absolute — it's
    /// emitted as a null entry in the inner column.
    pub fn append(&mut self, value: T) {
        self.append_n(value, 1);
    }

    /// Append `n` copies of the same (absolute) `value`.
    ///
    /// The first copy is encoded as `value - prev_abs`; subsequent copies
    /// are encoded as `0` (since the absolute hasn't changed).  For a null
    /// value, `n` null entries are emitted and `abs` is unchanged.
    pub fn append_n(&mut self, value: T, n: usize) {
        if n == 0 {
            return;
        }
        // Update the uniform tracker.
        if self.inner.is_empty() {
            self.uniform = Some(value);
        } else if self.uniform != Some(value) {
            self.uniform = None;
        }
        match value.to_i64() {
            Some(v) => {
                let first_delta = v - self.abs;
                self.abs = v;
                self.inner
                    .append_n_owned(T::make_inner(Some(first_delta)), 1);
                if n > 1 {
                    self.inner.append_n_owned(T::make_inner(Some(0)), n - 1);
                }
            }
            None => {
                self.inner.append_n_owned(T::make_inner(None), n);
            }
        }
    }

    /// Alias for [`append`](Self::append) — provided so call sites that
    /// use [`append_owned`](super::encoder::RleEncoder::append_owned) on
    /// `RleEncoder` can swap encoders without edits.
    pub fn append_owned(&mut self, value: T) {
        self.append(value);
    }

    /// Append all values from an iterator.
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for v in iter {
            self.append(v);
        }
    }

    /// Flush and return the encoded bytes.  Consumes the encoder.
    pub fn save(self) -> Vec<u8> {
        self.inner.save()
    }

    /// Flush and append the encoded bytes to `out`.  Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.inner.save_to(out)
    }

    /// Flush and append the encoded bytes to `out`, returning an empty
    /// range when the encoder is empty or every appended value equals
    /// `value`.
    ///
    /// Mirrors [`RleEncoder::save_to_unless`](super::encoder::RleEncoder::save_to_unless)
    /// on the absolute (realized) values.  For nullable delta columns
    /// pass `None` to get v0's `encode_unless_empty` semantics (elide on
    /// empty or all-null).
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T) -> std::ops::Range<usize> {
        if self.inner.is_empty() || self.uniform == Some(value) {
            return out.len()..out.len();
        }
        self.inner.save_to(out)
    }

    /// Encode values from an iterator and return the raw bytes.
    pub fn encode<I: IntoIterator<Item = T>>(iter: I) -> Vec<u8> {
        let mut enc = Self::new();
        enc.extend(iter);
        enc.save()
    }

    /// Encode values from an iterator and append the bytes to `out`.
    /// Returns the byte range written.
    pub fn encode_to<I: IntoIterator<Item = T>>(
        out: &mut Vec<u8>,
        iter: I,
    ) -> std::ops::Range<usize> {
        let mut enc = Self::new();
        enc.extend(iter);
        enc.save_to(out)
    }

    /// Encode values from an iterator and append the bytes to `out`,
    /// eliding the column if it's empty or every value equals `value`.
    /// See [`save_to_unless`](Self::save_to_unless).
    pub fn encode_to_unless<I: IntoIterator<Item = T>>(
        out: &mut Vec<u8>,
        iter: I,
        value: T,
    ) -> std::ops::Range<usize> {
        let mut enc = Self::new();
        enc.extend(iter);
        enc.save_to_unless(out, value)
    }
}

// ── (old values_to_deltas helpers removed — see pub(crate) copies below) ──

#[doc(hidden)]
#[allow(dead_code)]
fn _unused_values_to_deltas<T: DeltaValue>(values: &[T]) -> Vec<T::Inner> {
    let _ = values;
    let mut deltas: Vec<T::Inner> = Vec::new();
    let mut prev = 0i64;
    for v in values {
        match v.to_i64() {
            None => deltas.push(T::make_inner(None)),
            Some(r) => {
                deltas.push(T::make_inner(Some(r - prev)));
                prev = r;
            }
        }
    }
    deltas
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_col<T: DeltaValue + PartialEq + std::fmt::Debug>(col: &DeltaColumn<T>, expected: &[T])
    where
        T::Inner: super::super::RleValue,
        super::super::prefix::PrefixSlabWeight<<T::Inner as super::super::PrefixValue>::Prefix>:
            super::super::btree::DeltaAggregate,
    {
        assert_eq!(col.len(), expected.len(), "length mismatch");
        for (i, exp) in expected.iter().enumerate() {
            assert_eq!(col.get(i).as_ref(), Some(exp), "mismatch at index {i}");
        }
    }

    // ── Basic ───────────────────────────────────────────────────────────────

    #[test]
    fn empty() {
        let col = DeltaColumn::<u64>::new();
        assert_eq!(col.len(), 0);
        assert_eq!(col.get(0), None);
    }

    #[test]
    fn single() {
        let col = DeltaColumn::<u64>::from_values(vec![42]);
        assert_col(&col, &[42]);
    }

    #[test]
    fn constant_stride() {
        let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40]);
        assert_col(&col, &[10, 20, 30, 40]);
    }

    #[test]
    fn non_monotonic() {
        let col = DeltaColumn::<i64>::from_values(vec![5, 3, 8, 2, 10]);
        assert_col(&col, &[5, 3, 8, 2, 10]);
    }

    #[test]
    fn unsigned_non_monotonic() {
        let col = DeltaColumn::<u64>::from_values(vec![100, 50, 200, 10]);
        assert_col(&col, &[100, 50, 200, 10]);
    }

    // ── Insert ──────────────────────────────────────────────────────────────

    #[test]
    fn insert_at_start() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(0, 5);
        assert_col(&col, &[5, 10, 20, 30]);
    }

    #[test]
    fn insert_at_end() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(3, 40);
        assert_col(&col, &[10, 20, 30, 40]);
    }

    #[test]
    fn insert_in_middle() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(1, 15);
        assert_col(&col, &[10, 15, 20, 30]);
    }

    #[test]
    fn insert_preserves_subsequent() {
        let mut col = DeltaColumn::<i64>::from_values(vec![6, 7, 8, 9]);
        col.insert(2, 10);
        assert_col(&col, &[6, 7, 10, 8, 9]);
    }

    #[test]
    fn sequential_inserts() {
        let mut col = DeltaColumn::<u64>::new();
        for i in 0..20 {
            col.insert(i, (i + 1) as u64 * 100);
        }
        let expected: Vec<u64> = (1..=20).map(|i| i * 100).collect();
        assert_col(&col, &expected);
    }

    // ── Remove ──────────────────────────────────────────────────────────────

    #[test]
    fn remove_first() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(0);
        assert_col(&col, &[20, 30]);
    }

    #[test]
    fn remove_last() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(2);
        assert_col(&col, &[10, 20]);
    }

    #[test]
    fn remove_middle() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(1);
        assert_col(&col, &[10, 30]);
    }

    #[test]
    fn remove_preserves_subsequent() {
        let mut col = DeltaColumn::<i64>::from_values(vec![6, 7, 8, 9]);
        col.remove(1);
        assert_col(&col, &[6, 8, 9]);
    }

    #[test]
    fn remove_all() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(0);
        col.remove(0);
        col.remove(0);
        assert_eq!(col.len(), 0);
    }

    // ── Splice ──────────────────────────────────────────────────────────────

    #[test]
    fn splice_replace() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        col.splice(1, 2, [25, 35]);
        assert_col(&col, &[10, 25, 35, 40, 50]);
    }

    #[test]
    fn splice_insert_only() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.splice(1, 0, [15, 17]);
        assert_col(&col, &[10, 15, 17, 20, 30]);
    }

    #[test]
    fn splice_delete_only() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        col.splice(1, 3, std::iter::empty());
        assert_col(&col, &[10, 50]);
    }

    #[test]
    fn splice_at_end() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.splice(2, 1, [100, 200]);
        assert_col(&col, &[10, 20, 100, 200]);
    }

    // ── Save / Load ─────────────────────────────────────────────────────────

    #[test]
    fn save_load_roundtrip() {
        let col = DeltaColumn::<u64>::from_values(vec![100, 200, 300, 400, 500]);
        let bytes = col.save();
        let loaded = DeltaColumn::<u64>::load(&bytes).unwrap();
        assert_col(&loaded, &[100, 200, 300, 400, 500]);
    }

    #[test]
    fn save_load_negative_deltas() {
        let col = DeltaColumn::<i64>::from_values(vec![10, 5, 15, 3, 20]);
        let bytes = col.save();
        let loaded = DeltaColumn::<i64>::load(&bytes).unwrap();
        assert_col(&loaded, &[10, 5, 15, 3, 20]);
    }

    // ── Multi-slab ──────────────────────────────────────────────────────────

    #[test]
    fn multi_slab_get() {
        let values: Vec<u64> = (0..50)
            .map(|i| if i % 2 == 0 { i * 3 } else { i * 7 })
            .collect();
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        assert!(col.slab_count() > 1, "should have multiple slabs");
        assert_col(&col, &values);
    }

    #[test]
    fn multi_slab_insert_remove() {
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        let mut mirror: Vec<u64> = Vec::new();
        for i in 0..30 {
            let v = (i * 7 + 3) as u64;
            col.insert(i, v);
            mirror.push(v);
        }
        assert_col(&col, &mirror);
        for _ in 0..5 {
            col.remove(10);
            mirror.remove(10);
        }
        assert_col(&col, &mirror);
        for i in 0..5 {
            let v = (1000 + i) as u64;
            col.insert(5, v);
            mirror.insert(5, v);
        }
        assert_col(&col, &mirror);
    }

    #[test]
    fn multi_slab_save_load() {
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        let values: Vec<u64> = (1..=100).collect();
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        let bytes = col.save();
        let loaded = DeltaColumn::<u64>::load(&bytes).unwrap();
        assert_col(&loaded, &values);
    }

    // ── Type variants ───────────────────────────────────────────────────────

    #[test]
    fn u32_type() {
        let col = DeltaColumn::<u32>::from_values(vec![1, 2, 3, 4, 5]);
        assert_col(&col, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn i32_type() {
        let col = DeltaColumn::<i32>::from_values(vec![10, -5, 20, -10, 0]);
        assert_col(&col, &[10, -5, 20, -10, 0]);
    }

    // ── Nullable ────────────────────────────────────────────────────────────

    #[test]
    fn nullable_basic() {
        let col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8), Some(10)]);
        assert_col(&col, &[Some(5), None, Some(8), Some(10)]);
    }

    #[test]
    fn nullable_insert_before_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8)]);
        col.insert(1, Some(6));
        assert_col(&col, &[Some(5), Some(6), None, Some(8)]);
    }

    #[test]
    fn nullable_insert_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), Some(10)]);
        col.insert(1, None);
        assert_col(&col, &[Some(5), None, Some(8), Some(10)]);
    }

    #[test]
    fn nullable_remove_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8)]);
        col.remove(1);
        assert_col(&col, &[Some(5), Some(8)]);
    }

    #[test]
    fn nullable_remove_before_null() {
        let mut col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), None, Some(12)]);
        col.remove(1);
        assert_col(&col, &[Some(5), None, Some(12)]);
    }

    #[test]
    fn nullable_splice() {
        let mut col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), Some(10), Some(15)]);
        col.splice(1, 2, [None, Some(12)]);
        assert_col(&col, &[Some(5), None, Some(12), Some(15)]);
    }

    #[test]
    fn nullable_all_null() {
        let col = DeltaColumn::<Option<u64>>::from_values(vec![None, None, None]);
        assert_col(&col, &[None, None, None]);
    }

    #[test]
    fn nullable_save_load() {
        let col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8), None, Some(12)]);
        let bytes = col.save();
        let loaded = DeltaColumn::<Option<u64>>::load(&bytes).unwrap();
        assert_col(&loaded, &[Some(5), None, Some(8), None, Some(12)]);
    }

    // ── v0 save compatibility ───────────────────────────────────────────────

    /// Build a v0 DeltaCursor column and a v1 DeltaColumn from the same
    /// realized values and assert their serialized bytes are identical.
    fn assert_v0_v1_save_match(values: &[Option<i64>]) {
        use crate::ColumnData as V0ColumnData;
        use crate::DeltaCursor;

        // v0
        let mut v0: V0ColumnData<DeltaCursor> = V0ColumnData::new();
        v0.splice(0, 0, values.to_vec());
        let v0_bytes = v0.save();

        // v1
        let v1 = DeltaColumn::<Option<i64>>::from_values(values.to_vec());
        let v1_bytes = v1.save();

        assert_eq!(
            v0_bytes, v1_bytes,
            "v0 vs v1 save mismatch for values: {:?}",
            values
        );

        // Also verify v1 reads back correctly.
        let loaded = DeltaColumn::<Option<i64>>::load(&v1_bytes).unwrap();
        for (i, exp) in values.iter().enumerate() {
            assert_eq!(loaded.get(i).as_ref(), Some(exp), "reload mismatch at {i}");
        }
    }

    #[test]
    fn v0_v1_save_empty() {
        assert_v0_v1_save_match(&[]);
    }

    #[test]
    fn v0_v1_save_single() {
        assert_v0_v1_save_match(&[Some(42)]);
    }

    #[test]
    fn v0_v1_save_constant_stride() {
        assert_v0_v1_save_match(&[Some(10), Some(20), Some(30), Some(40)]);
    }

    #[test]
    fn v0_v1_save_with_nulls() {
        assert_v0_v1_save_match(&[None, Some(0), Some(2), Some(3)]);
    }

    #[test]
    fn v0_v1_save_mixed() {
        assert_v0_v1_save_match(&[
            Some(1),
            Some(10),
            Some(2),
            Some(11),
            Some(4),
            Some(27),
            Some(19),
            Some(3),
            Some(21),
            Some(14),
            Some(2),
            Some(8),
        ]);
    }

    #[test]
    fn v0_v1_save_runs() {
        assert_v0_v1_save_match(&[
            Some(1),
            Some(2),
            Some(4),
            Some(6),
            Some(9),
            Some(12),
            Some(16),
            Some(20),
            Some(25),
            Some(30),
        ]);
    }

    #[test]
    fn v0_v1_save_nulls_and_values() {
        assert_v0_v1_save_match(&[
            None,
            Some(0),
            Some(2),
            Some(3),
            Some(4),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(7),
            Some(8),
            Some(9),
        ]);
    }

    #[test]
    fn v0_v1_save_fuzz() {
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(12345);

        for _ in 0..200 {
            let len = rng.random_range(0..50);
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            for _ in 0..len {
                let r: u32 = rng.random_range(0..10);
                if r == 0 {
                    values.push(None);
                } else {
                    values.push(Some(rng.random_range(0..100)));
                }
            }
            assert_v0_v1_save_match(&values);
        }
    }

    // ── DeltaEncoder ────────────────────────────────────────────────────────

    /// Encode `values` via `DeltaEncoder` and compare to v0 DeltaCursor
    /// and v1 DeltaColumn::from_values.  All three must produce identical
    /// serialized bytes.
    fn assert_delta_encoder_match(values: &[Option<i64>]) {
        use crate::ColumnData as V0ColumnData;
        use crate::DeltaCursor;

        // v0 reference
        let mut v0: V0ColumnData<DeltaCursor> = V0ColumnData::new();
        v0.splice(0, 0, values.to_vec());
        let v0_bytes = v0.save();

        // v1 from_values reference
        let v1_col = DeltaColumn::<Option<i64>>::from_values(values.to_vec());
        let v1_col_bytes = v1_col.save();

        // v1 DeltaEncoder streaming
        let mut enc = DeltaEncoder::<Option<i64>>::new();
        for v in values {
            enc.append(*v);
        }
        let enc_bytes = enc.save();

        assert_eq!(
            v0_bytes, v1_col_bytes,
            "v0 / v1-from_values mismatch for {:?}",
            values
        );
        assert_eq!(
            enc_bytes, v0_bytes,
            "DeltaEncoder mismatch for {:?}",
            values
        );

        // Sanity: bytes reload into a DeltaColumn with the same values.
        let reloaded = DeltaColumn::<Option<i64>>::load(&enc_bytes).unwrap();
        for (i, exp) in values.iter().enumerate() {
            assert_eq!(
                reloaded.get(i).as_ref(),
                Some(exp),
                "reload mismatch at {i}"
            );
        }
    }

    #[test]
    fn delta_encoder_empty() {
        assert_delta_encoder_match(&[]);
    }

    #[test]
    fn delta_encoder_single() {
        assert_delta_encoder_match(&[Some(42)]);
    }

    #[test]
    fn delta_encoder_constant_stride() {
        assert_delta_encoder_match(&[Some(10), Some(20), Some(30), Some(40)]);
    }

    #[test]
    fn delta_encoder_with_nulls() {
        assert_delta_encoder_match(&[None, Some(0), Some(2), Some(3)]);
    }

    #[test]
    fn delta_encoder_trailing_nulls() {
        assert_delta_encoder_match(&[Some(1), Some(2), None, None, Some(3)]);
    }

    #[test]
    fn delta_encoder_non_monotonic() {
        assert_delta_encoder_match(&[Some(5), Some(3), Some(8), Some(2), Some(10)]);
    }

    #[test]
    fn delta_encoder_repeated_values() {
        // Same value repeated produces delta-0 runs; confirm those encode correctly.
        assert_delta_encoder_match(&[Some(5), Some(5), Some(5), Some(10), Some(10)]);
    }

    #[test]
    fn delta_encoder_append_n() {
        // Verify append_n produces the same bytes as the same value appended
        // one at a time.
        let values: Vec<Option<i64>> = vec![Some(5); 7];
        let single_bytes = {
            let mut enc = DeltaEncoder::<Option<i64>>::new();
            for v in &values {
                enc.append(*v);
            }
            enc.save()
        };
        let append_n_bytes = {
            let mut enc = DeltaEncoder::<Option<i64>>::new();
            enc.append_n(Some(5), 7);
            enc.save()
        };
        assert_eq!(single_bytes, append_n_bytes);
    }

    #[test]
    fn delta_encoder_non_nullable_i64() {
        // DeltaEncoder<i64> (non-nullable) should produce the same bytes as
        // DeltaEncoder<Option<i64>> with all-Some values.
        let values: Vec<i64> = vec![1, 3, 6, 10, 15];
        let non_null = {
            let mut enc = DeltaEncoder::<i64>::new();
            for v in &values {
                enc.append(*v);
            }
            enc.save()
        };
        let nullable = {
            let mut enc = DeltaEncoder::<Option<i64>>::new();
            for v in &values {
                enc.append(Some(*v));
            }
            enc.save()
        };
        assert_eq!(non_null, nullable);
    }

    // ── encode_to_unless / save_to_unless ────────────────────────────────

    #[test]
    fn delta_encoder_unless_empty_elides() {
        // Empty iterator → empty range regardless of `value`.
        let mut out = Vec::new();
        let range =
            DeltaEncoder::<Option<i64>>::encode_to_unless(&mut out, std::iter::empty(), None);
        assert!(range.is_empty());
        assert!(out.is_empty());
    }

    #[test]
    fn delta_encoder_unless_all_null_elides() {
        // Nullable column, all-null sequence, `value = None` → elide.
        let mut out = Vec::new();
        let range =
            DeltaEncoder::<Option<i64>>::encode_to_unless(&mut out, vec![None, None, None], None);
        assert!(range.is_empty());
        assert!(out.is_empty());
    }

    #[test]
    fn delta_encoder_unless_matches_v0_encode_unless_empty_all_null() {
        // Cross-check: v0 `encode_unless_empty` on an all-null sequence
        // produces an empty range; v1 `encode_to_unless(None)` must too.
        use crate::ColumnCursor;
        use crate::DeltaCursor;

        let values: Vec<Option<i64>> = vec![None; 5];

        let mut v0_out = Vec::new();
        let v0_range = DeltaCursor::encode_unless_empty(&mut v0_out, values.iter().copied());

        let mut v1_out = Vec::new();
        let v1_range = DeltaEncoder::<Option<i64>>::encode_to_unless(
            &mut v1_out,
            values.iter().copied(),
            None,
        );

        assert_eq!(&v1_out[v1_range.clone()], &v0_out[v0_range.clone()]);
        assert!(v0_range.is_empty());
        assert!(v1_range.is_empty());
    }

    #[test]
    fn delta_encoder_unless_mixed_nulls_and_values_saves() {
        // Not all-null → must save normally.
        let mut out = Vec::new();
        let range = DeltaEncoder::<Option<i64>>::encode_to_unless(
            &mut out,
            vec![None, Some(5), None, Some(10)],
            None,
        );
        assert!(!range.is_empty());

        // And the bytes should round-trip via DeltaColumn::load.
        let loaded = DeltaColumn::<Option<i64>>::load(&out[range]).unwrap();
        assert_eq!(loaded.get(0), Some(None));
        assert_eq!(loaded.get(1), Some(Some(5)));
        assert_eq!(loaded.get(2), Some(None));
        assert_eq!(loaded.get(3), Some(Some(10)));
    }

    #[test]
    fn delta_encoder_unless_single_run_of_non_null_value_elides() {
        // Non-null column, all values equal to `value` → elide.
        // This is RleEncoder-style single-run elision on absolute values.
        let mut out = Vec::new();
        let range = DeltaEncoder::<i64>::encode_to_unless(&mut out, vec![7, 7, 7], 7);
        assert!(range.is_empty());
        assert!(out.is_empty());
    }

    #[test]
    fn delta_encoder_unless_non_matching_value_saves() {
        // Non-null column, all values equal but `value` doesn't match → save.
        let mut out = Vec::new();
        let range = DeltaEncoder::<i64>::encode_to_unless(&mut out, vec![7, 7, 7], 0);
        assert!(!range.is_empty());
    }

    #[test]
    fn delta_encoder_unless_mixed_values_saves() {
        // Non-null column, values differ → save regardless of `value`.
        let mut out = Vec::new();
        let range = DeltaEncoder::<i64>::encode_to_unless(&mut out, vec![5, 6, 7], 5);
        assert!(!range.is_empty());
    }

    #[test]
    fn delta_encoder_unless_matches_for_all_sequences() {
        // Fuzz: compare encode_to_unless(None) to v0 encode_unless_empty
        // for nullable sequences.  Both should produce identical bytes
        // for every input.
        use crate::ColumnCursor;
        use crate::DeltaCursor;
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(77777);

        for _ in 0..200 {
            let len = rng.random_range(0..50);
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            for _ in 0..len {
                let r: u32 = rng.random_range(0..10);
                if r == 0 {
                    values.push(None);
                } else {
                    values.push(Some(rng.random_range(0..100)));
                }
            }

            let mut v0_out = Vec::new();
            let v0_range = DeltaCursor::encode_unless_empty(&mut v0_out, values.iter().copied());

            let mut v1_out = Vec::new();
            let v1_range = DeltaEncoder::<Option<i64>>::encode_to_unless(
                &mut v1_out,
                values.iter().copied(),
                None,
            );

            assert_eq!(
                &v1_out[v1_range], &v0_out[v0_range],
                "mismatch for {:?}",
                values
            );
        }
    }

    #[test]
    fn delta_encoder_fuzz() {
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(54321);

        for _ in 0..200 {
            let len = rng.random_range(0..50);
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            for _ in 0..len {
                let r: u32 = rng.random_range(0..10);
                if r == 0 {
                    values.push(None);
                } else {
                    values.push(Some(rng.random_range(0..100)));
                }
            }
            assert_delta_encoder_match(&values);
        }
    }

    // ── Stress / fuzz ───────────────────────────────────────────────────────

    #[test]
    fn fuzz_insert_remove() {
        let mut col = DeltaColumn::<i64>::new();
        let mut mirror: Vec<i64> = Vec::new();
        let values: Vec<i64> = vec![
            100, 50, 200, 150, 300, 10, 500, 250, 400, 350, 80, 90, 70, 60, 40, 30, 20, 15, 5, 1,
        ];
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
            mirror.push(v);
            assert_col(&col, &mirror);
        }
        for &idx in &[5, 0, 10, 3, 7, 1, 0, 0, 0, 0] {
            if idx < col.len() {
                col.remove(idx);
                mirror.remove(idx);
                assert_col(&col, &mirror);
            }
        }
        for (i, &v) in [999, 888, 777, 666].iter().enumerate() {
            let pos = i.min(col.len());
            col.insert(pos, v);
            mirror.insert(pos, v);
            assert_col(&col, &mirror);
        }
    }

    #[test]
    fn fuzz_splice() {
        let mut col = DeltaColumn::<u64>::from_values((1..=20).collect());
        let mut mirror: Vec<u64> = (1..=20).collect();
        for iter in 0..10 {
            let len = col.len();
            if len < 4 {
                break;
            }
            let pos = (iter * 3 + 1) % (len - 2);
            let del = 2.min(len - pos);
            let new_vals: Vec<u64> = (0..3).map(|j| (iter * 100 + j + 1) as u64).collect();
            col.splice(pos, del, new_vals.clone());
            mirror.splice(pos..pos + del, new_vals);
            assert_col(&col, &mirror);
        }
    }
}
// ── DeltaColumn (B-tree backed, generic over weight fn) ────────────────────
//
// Default WF = PrefixWeightFn<T::Inner> — `len + prefix` aggregate, no
// value queries.  WF = IndexedDeltaWeightFn<T> (see indexed.rs) uses
// SlabAgg to unlock `find_by_value` / `find_by_range` via min/max pruning.

// ── DeltaColumn ────────────────────────────────────────────────────────────

/// A delta-encoded column.  Generic over the per-slab aggregate: any
/// [`WeightFn`] whose weight satisfies [`DeltaAggregate`] plugs in.
pub struct DeltaColumn<T, WF = PrefixWeightFn<<T as DeltaValue>::Inner>>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    pub(crate) col: Column<T::Inner, WF>,
    _phantom: PhantomData<T>,
}

impl<T, WF> Clone for DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn clone(&self) -> Self {
        Self {
            col: self.col.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, WF> std::fmt::Debug for DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaColumn")
            .field("len", &self.col.len())
            .field("slabs", &self.col.slab_count())
            .finish()
    }
}

impl<T, WF> Default for DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, WF> DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    pub fn new() -> Self {
        Self {
            col: Column::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            col: Column::with_max_segments(max_segments),
            _phantom: PhantomData,
        }
    }

    pub fn from_values(values: Vec<T>) -> Self {
        if values.is_empty() {
            return Self::new();
        }
        let deltas = values_to_deltas::<T>(&values);
        Self {
            col: Column::from_values(deltas),
            _phantom: PhantomData,
        }
    }

    pub fn load_with(data: &[u8], opts: TypedLoadOpts<T::Inner>) -> Result<Self, PackError> {
        if data.is_empty() {
            // Delegate fill/length to Column::load_with for empty data.
            let col = Column::load_with(data, opts)?;
            return Ok(Self {
                col,
                _phantom: PhantomData,
            });
        }
        let validate = |running: i64, count: usize, val: <T::Inner as ColumnValueRef>::Get<'_>| {
            if let Some(f) = opts.validate {
                if let Some(msg) = f(val) {
                    return Err(msg);
                }
            }
            match T::get_inner(val) {
                None => Ok(running),
                Some(d) => {
                    let new_running = running + d * count as i64;
                    T::try_from_i64(new_running)?;
                    Ok(new_running)
                }
            }
        };
        let col = Column::load_verified_fold(data, opts.max_segments, Some(validate))?;
        if let Some(expected) = opts.length {
            if col.len() != expected {
                return Err(PackError::InvalidLength(col.len(), expected));
            }
        }
        Ok(Self {
            col,
            _phantom: PhantomData,
        })
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_with(data, super::LoadOpts::default().into())
    }

    pub fn len(&self) -> usize {
        self.col.len()
    }

    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.col.save_to(out)
    }

    pub fn save_to_unless(
        &self,
        out: &mut Vec<u8>,
        value: <T::Inner as ColumnValueRef>::Get<'_>,
    ) -> std::ops::Range<usize> {
        self.col.save_to_unless(out, value)
    }

    /// Collect realized values into a Vec.
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().collect()
    }

    pub fn get(&self, index: usize) -> Option<T> {
        self.iter().nth(index)
    }

    pub fn iter(&self) -> DeltaIter<'_, T> {
        DeltaIter {
            inner: self.col.iter(),
            running: 0,
            delta_col: Some(&self.col as &dyn DeltaPrefixLookup<T>),
            _phantom: PhantomData,
        }
    }

    pub fn iter_range(&self, range: Range<usize>) -> DeltaIter<'_, T> {
        let running = self.col.delta_prefix_through::<T>(range.start);
        DeltaIter {
            inner: self.col.iter_range(range),
            running,
            delta_col: Some(&self.col as &dyn DeltaPrefixLookup<T>),
            _phantom: PhantomData,
        }
    }

    pub fn insert(&mut self, index: usize, value: T) {
        let len = self.col.len();
        assert!(index <= len, "insert index out of bounds");

        match value.to_i64() {
            None => {
                self.col.insert(index, T::make_inner(None));
            }
            Some(v) => {
                let prev = self.prev_realized(index);
                let new_delta = v - prev;

                if index >= len {
                    self.col.insert(index, T::make_inner(Some(new_delta)));
                    return;
                }

                let current = T::get_inner(self.col.get(index).unwrap());
                match current {
                    Some(d) => {
                        self.col.splice(
                            index,
                            1,
                            [
                                T::make_inner(Some(new_delta)),
                                T::make_inner(Some(d - new_delta)),
                            ],
                        );
                    }
                    None => {
                        self.insert_before_null_run(index, new_delta);
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) {
        let len = self.col.len();
        if index >= len {
            return;
        }
        let delta = T::get_inner(self.col.get(index).unwrap());
        match delta {
            None => {
                self.col.remove(index);
            }
            Some(d) => {
                if index + 1 >= len {
                    self.col.remove(index);
                    return;
                }
                self.remove_and_absorb(index, d);
            }
        }
    }

    pub fn push(&mut self, value: T) {
        let len = self.col.len();
        self.insert(len, value);
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let val = self.get(self.col.len() - 1)?;
        self.remove(self.col.len() - 1);
        Some(val)
    }

    pub fn first(&self) -> Option<T> {
        self.get(0)
    }

    pub fn last(&self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            self.get(self.col.len() - 1)
        }
    }

    pub fn clear(&mut self) {
        self.col.clear();
    }

    pub fn truncate(&mut self, len: usize) {
        let cur = self.col.len();
        if len < cur {
            self.splice(len, cur - len, std::iter::empty::<T>());
        }
    }

    pub fn splice(&mut self, index: usize, del: usize, values: impl IntoIterator<Item = T>) {
        let _ = self.splice_inner(index, del, values);
    }

    pub(crate) fn splice_inner(
        &mut self,
        index: usize,
        del: usize,
        values: impl IntoIterator<Item = T>,
    ) -> std::ops::Range<usize> {
        let len = self.col.len();
        assert!(index + del <= len, "splice range out of bounds");

        let values: Vec<T> = values.into_iter().collect();
        if del == 0 && values.is_empty() {
            return 0..0;
        }

        let prev = self.prev_realized(index);
        let new_deltas = values_to_deltas_from::<T>(&values, prev);

        let new_prefix_end = {
            let mut p = prev;
            for v in &values {
                if let Some(r) = v.to_i64() {
                    p = r;
                }
            }
            p
        };

        let splice_end = index + del;
        if splice_end < len {
            let old_prefix_at_end = self.prefix_sum(splice_end);
            let adjustment = old_prefix_at_end - new_prefix_end;

            let (extra_del, mut boundary_deltas) = self.find_nonnull_from(splice_end, adjustment);

            let mut all_deltas: Vec<T::Inner> = new_deltas;
            all_deltas.append(&mut boundary_deltas);
            self.col.splice_inner(index, del + extra_del, all_deltas)
        } else {
            self.col.splice_inner(index, del, new_deltas)
        }
    }

    // ── Prefix sum (O(log n) via the B-tree + decoder partial walk) ─────────

    pub(crate) fn prefix_sum(&self, count: usize) -> i64 {
        self.col.delta_prefix_through::<T>(count)
    }

    fn prev_realized(&self, index: usize) -> i64 {
        self.prefix_sum(index)
    }

    // ── Null-aware helpers ──────────────────────────────────────────────────

    fn insert_before_null_run(&mut self, index: usize, new_delta: i64) {
        let len = self.col.len();
        let mut j = index;
        while j < len {
            if T::get_inner(self.col.get(j).unwrap()).is_some() {
                break;
            }
            j += 1;
        }
        if j < len {
            let d_j = T::get_inner(self.col.get(j).unwrap()).unwrap();
            let adjusted = d_j - new_delta;
            let null_count = j - index;
            let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 2);
            vals.push(T::make_inner(Some(new_delta)));
            for _ in 0..null_count {
                vals.push(T::make_inner(None));
            }
            vals.push(T::make_inner(Some(adjusted)));
            self.col.splice(index, j - index + 1, vals);
        } else {
            self.col.insert(index, T::make_inner(Some(new_delta)));
        }
    }

    fn remove_and_absorb(&mut self, index: usize, delta: i64) {
        let len = self.col.len();
        debug_assert!(index + 1 < len);

        if !T::NULLABLE {
            let d_next = T::get_inner(self.col.get(index + 1).unwrap()).unwrap();
            self.col
                .splice(index, 2, [T::make_inner(Some(delta + d_next))]);
            return;
        }

        let mut j = index + 1;
        while j < len {
            if let Some(d_j) = T::get_inner(self.col.get(j).unwrap()) {
                let adjusted = delta + d_j;
                let null_count = j - index - 1;
                let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 1);
                for _ in 0..null_count {
                    vals.push(T::make_inner(None));
                }
                vals.push(T::make_inner(Some(adjusted)));
                self.col.splice(index, j - index + 1, vals);
                return;
            }
            j += 1;
        }
        self.col.remove(index);
    }

    fn find_nonnull_from(&self, from: usize, adjustment: i64) -> (usize, Vec<T::Inner>) {
        if adjustment == 0 && !T::NULLABLE {
            let d = T::get_inner(self.col.get(from).unwrap()).unwrap();
            return (1, vec![T::make_inner(Some(d))]);
        }
        if adjustment == 0 {
            return (0, vec![]);
        }
        let len = self.col.len();
        if !T::NULLABLE {
            let d = T::get_inner(self.col.get(from).unwrap()).unwrap();
            return (1, vec![T::make_inner(Some(d + adjustment))]);
        }
        let mut j = from;
        while j < len {
            if let Some(d) = T::get_inner(self.col.get(j).unwrap()) {
                let adjusted = d + adjustment;
                let null_count = j - from;
                let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 1);
                for _ in 0..null_count {
                    vals.push(T::make_inner(None));
                }
                vals.push(T::make_inner(Some(adjusted)));
                return (j - from + 1, vals);
            }
            j += 1;
        }
        (0, vec![])
    }
}

// ── DeltaIter ──────────────────────────────────────────────────────────────

/// Iterator over realized values in a [`DeltaColumn`].
///
/// `next()` walks the underlying `Column` value-by-value and
/// accumulates a running `i64` prefix.  `nth(n)` uses the column's
/// B-tree to jump directly to the target position and reset the
/// running prefix in O(log n), avoiding an O(n) replay.
pub(crate) trait DeltaPrefixLookup<T: DeltaValue> {
    fn prefix_through(&self, idx: usize) -> i64;
}

impl<T, WF> DeltaPrefixLookup<T> for Column<T::Inner, WF>
where
    T: DeltaValue,
    T::Inner: RleValue + super::ColumnValueRef,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn prefix_through(&self, idx: usize) -> i64 {
        self.delta_prefix_through::<T>(idx)
    }
}

pub struct DeltaIter<'a, T: DeltaValue> {
    inner: super::column::Iter<'a, T::Inner>,
    running: i64,
    delta_col: Option<&'a dyn DeltaPrefixLookup<T>>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue> Default for DeltaIter<'_, T>
where
    T::Inner: RleValue,
{
    fn default() -> Self {
        Self {
            inner: Default::default(),
            running: 0,
            delta_col: None,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeltaValue> Iterator for DeltaIter<'_, T>
where
    T::Inner: RleValue,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        let raw = self.inner.next()?;
        match T::get_inner(raw) {
            None => Some(T::null_value()),
            Some(d) => {
                self.running += d;
                Some(T::from_i64(self.running))
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<T> {
        let raw = self.inner.nth(n)?;
        let col = self
            .delta_col
            .expect("DeltaIter::nth requires a column reference");
        self.running = col.prefix_through(self.inner.pos);
        match T::get_inner(raw) {
            None => Some(T::null_value()),
            Some(_) => Some(T::from_i64(self.running)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: DeltaValue> std::fmt::Debug for DeltaIter<'_, T>
where
    T::Inner: RleValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaIter")
            .field("pos", &self.inner.pos())
            .field("running", &self.running)
            .field("items_left", &self.inner.len())
            .finish()
    }
}

impl<T: DeltaValue> ExactSizeIterator for DeltaIter<'_, T> where T::Inner: RleValue {}

impl<'a, T: DeltaValue> DeltaIter<'a, T>
where
    T::Inner: RleValue,
{
    #[inline]
    pub fn pos(&self) -> usize {
        self.inner.pos()
    }

    #[inline]
    pub fn end_pos(&self) -> usize {
        self.inner.end_pos()
    }

    pub fn set_max(&mut self, pos: usize) {
        self.inner.set_max(pos);
    }

    pub fn advance_to(&mut self, target: usize) {
        assert!(
            target >= self.pos(),
            "advance_to: target ({target}) < pos ({})",
            self.pos()
        );
        if target > self.pos() {
            self.nth(target - self.pos() - 1);
        }
    }

    pub fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
    }

    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T> {
        let raw = self.inner.shift_next(range)?;
        let col = self
            .delta_col
            .expect("DeltaIter::shift_next requires a column reference");
        self.running = col.prefix_through(self.inner.pos);
        match T::get_inner(raw) {
            None => Some(T::null_value()),
            Some(_) => Some(T::from_i64(self.running)),
        }
    }

    pub fn suspend(&self) -> DeltaIterState {
        DeltaIterState {
            inner: self.inner.suspend(),
            running: self.running,
        }
    }
}

pub struct DeltaIterState {
    inner: super::column::IterState,
    running: i64,
}

impl DeltaIterState {
    pub fn try_resume<'a, T, WF>(
        &self,
        column: &'a DeltaColumn<T, WF>,
    ) -> Result<DeltaIter<'a, T>, crate::PackError>
    where
        T: DeltaValue,
        T::Inner: RleValue,
        WF: WeightFn<T::Inner>,
        WF::Weight: DeltaAggregate,
    {
        let inner = self.inner.try_resume(&column.col)?;
        Ok(DeltaIter {
            inner,
            running: self.running,
            delta_col: Some(&column.col as &dyn DeltaPrefixLookup<T>),
            _phantom: PhantomData,
        })
    }
}

impl<'a, T: DeltaValue> Clone for DeltaIter<'a, T>
where
    T::Inner: RleValue,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            running: self.running,
            delta_col: self.delta_col,
            _phantom: PhantomData,
        }
    }
}

// ── Column extension: prefix + partial-sum helpers ─────────────────────────

impl<T, WF> Column<T, WF>
where
    T: ColumnValueRef + RleValue,
    WF: WeightFn<T>,
    WF::Weight: DeltaAggregate,
{
    /// Exclusive prefix sum of realized deltas at item `idx` — the
    /// sum of non-null deltas over `0..idx`, as `i64`.
    ///
    /// O(log n) via `find_slab_at_item` on the B-tree + a decoder walk
    /// of the landing slab.
    pub(crate) fn delta_prefix_through<D>(&self, idx: usize) -> i64
    where
        D: DeltaValue<Inner = T>,
    {
        if idx == 0 || self.is_empty() {
            return 0;
        }
        let idx = idx.min(self.len());
        let (si, prefix_before, items_before) = self.index.find_slab_at_item(idx - 1);
        let si = si.min(self.slab_count() - 1);
        let items_in_slab = idx - items_before;
        let partial = partial_sum_in_slab::<D>(&self.slabs[si].data, items_in_slab);
        <WF::Weight as DeltaAggregate>::prefix_to_i64(prefix_before) + partial
    }
}

// ── Free helpers ────────────────────────────────────────────────────────────

/// Running prefix sum of realized deltas over the first `n` items of
/// `data`.  Uses `next_run_max(n - items)` so the decoder stops the
/// instant we've counted `n` items.
pub(crate) fn partial_sum_in_slab<T: DeltaValue>(data: &[u8], n: usize) -> i64
where
    T::Inner: RleValue,
{
    let mut decoder = <T::Inner as ColumnValueRef>::Encoding::decoder(data);
    let mut items = 0usize;
    let mut sum = 0i64;
    while items < n {
        let Some(run) = decoder.next_run_max(n - items) else {
            break;
        };
        if let Some(v) = T::get_inner(run.value) {
            sum += v * run.count as i64;
        }
        items += run.count;
    }
    sum
}

pub(crate) fn values_to_deltas<T: DeltaValue>(values: &[T]) -> Vec<T::Inner> {
    values_to_deltas_from::<T>(values, 0)
}

pub(crate) fn values_to_deltas_from<T: DeltaValue>(
    values: &[T],
    prev_realized: i64,
) -> Vec<T::Inner> {
    let mut deltas = Vec::with_capacity(values.len());
    let mut prev = prev_realized;
    for v in values {
        match v.to_i64() {
            None => deltas.push(T::make_inner(None)),
            Some(r) => {
                deltas.push(T::make_inner(Some(r - prev)));
                prev = r;
            }
        }
    }
    deltas
}

// ── FromIterator / Extend / IntoIterator ────────────────────────────────────

impl<T, WF> FromIterator<T> for DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<T, WF> Extend<T> for DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let vals: Vec<T> = iter.into_iter().collect();
        if !vals.is_empty() {
            let len = self.len();
            self.splice(len, 0, vals);
        }
    }
}

impl<'a, T, WF> IntoIterator for &'a DeltaColumn<T, WF>
where
    T: DeltaValue,
    T::Inner: RleValue,
    WF: WeightFn<T::Inner>,
    WF::Weight: DeltaAggregate,
{
    type Item = T;
    type IntoIter = DeltaIter<'a, T>;

    fn into_iter(self) -> DeltaIter<'a, T> {
        self.iter()
    }
}

// ── Overflow safety tests ───────────────────────────────────────────────────

#[cfg(test)]
mod overflow_tests {
    use super::*;

    #[test]
    fn delta_u32_rejects_i64_overflow() {
        let col = DeltaColumn::<i64>::from_values(vec![u32::MAX as i64 + 1]);
        let bytes = col.save();
        let result = DeltaColumn::<u32>::load(&bytes);
        assert!(
            result.is_err(),
            "DeltaColumn<u32> should reject realized value > u32::MAX"
        );
    }

    #[test]
    fn delta_u32_rejects_negative() {
        let col = DeltaColumn::<i64>::from_values(vec![10, -5]);
        let bytes = col.save();
        let result = DeltaColumn::<u32>::load(&bytes);
        assert!(
            result.is_err(),
            "DeltaColumn<u32> should reject negative delta"
        );
    }

    #[test]
    fn delta_u32_accepts_max_u32() {
        let col = DeltaColumn::<i64>::from_values(vec![u32::MAX as i64]);
        let bytes = col.save();
        let loaded = DeltaColumn::<u32>::load(&bytes);
        assert!(loaded.is_ok(), "DeltaColumn<u32> should accept u32::MAX");
        assert_eq!(loaded.unwrap().get(0), Some(u32::MAX));
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────
