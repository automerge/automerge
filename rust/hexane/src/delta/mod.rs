mod decoder;
pub mod indexed;
pub use decoder::DeltaDecoder;

use crate::column::{normalize_range_max, Iter, IterState};
use crate::encoder::RleEncoderState;
use crate::{Codec, Leb128};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

use crate::column::Column;
use crate::LoadOpts;
use crate::PackError;
pub use indexed::IndexedDeltaWeightFn;

// ── DeltaValue trait ────────────────────────────────────────────────────────

/// Sealed inner storage type of a [`DeltaColumn`]: `i64` when `T` is
/// non-nullable, `Option<i64>` when nullable. The wire format is
/// identical either way (nullable RLE of `i64` deltas — a non-nullable
/// column simply never contains null runs, and `Column<i64>` *rejects*
/// them at decode). Delta logic is written against `to_opt`/`from_opt`;
/// for `i64` the null branches are statically dead and compile away,
/// which removes the `Option` widening tax (16-byte values, per-run
/// discriminant compares) from non-nullable columns.
pub trait DeltaInner:
    crate::RleValue
    + crate::ColumnValueRef<Encoding<Leb128> = crate::RleEncoding<Self, Leb128>>
    + crate::AsColumnRef<Self>
    + crate::sealed::Sealed
    + Copy
    + Debug
    + Sized
    + 'static
{
    /// View a decoded value as an optional delta.
    fn to_opt(v: Self::Get<'_>) -> Option<i64>;
    /// Build a storable value from an optional delta. `None` means null —
    /// unreachable for `i64`, whose columns cannot contain nulls.
    fn from_opt(v: Option<i64>) -> Self;
}

impl crate::sealed::Sealed for i64 {}
impl crate::sealed::Sealed for Option<i64> {}

impl DeltaInner for i64 {
    #[inline]
    fn to_opt(v: i64) -> Option<i64> {
        Some(v)
    }
    #[inline]
    fn from_opt(v: Option<i64>) -> i64 {
        v.expect("null delta in a non-nullable delta column")
    }
}

impl DeltaInner for Option<i64> {
    #[inline]
    fn to_opt(v: Option<i64>) -> Option<i64> {
        v
    }
    #[inline]
    fn from_opt(v: Option<i64>) -> Option<i64> {
        v
    }
}

/// Trait for value types that can be stored in a delta-encoded column.
///
/// Deltas are stored internally as [`Self::Inner`] (`i64` for
/// non-nullable types, `Option<i64>` for nullable ones); this trait maps
/// `T` to and from that inner representation.
///
/// # Value domain
///
/// All realized values in a column must lie within a 2^63-wide range —
/// for unsigned types that means `< 2^63`.  This is not an accumulator
/// limitation but the wire format's closure boundary: deltas are stored
/// as `i64`, and deleting an element makes its two neighbours adjacent,
/// so *any* pair of values in the column may end up needing their
/// difference encoded as a single delta.  If values could span 2^63 or
/// more, a deletion could produce a delta that doesn't fit the wire
/// format.
///
/// Unsigned [`to_i64`](Self::to_i64) implementations **panic** outside
/// this domain (a precondition violation on the writer's own data);
/// [`load`](DeltaColumn::load) **rejects** out-of-domain data with an
/// error (untrusted bytes must never panic).
pub trait DeltaValue: Copy + PartialEq + Default + Debug {
    /// Whether this type supports null values.
    const NULLABLE: bool;

    /// The inner column storage type: `i64` for non-nullable `Self`,
    /// `Option<i64>` for nullable — see [`DeltaInner`].
    type Inner: DeltaInner;

    /// Inclusive bounds of the realized-value domain, as `i64`. Must
    /// accept exactly the values [`try_from_i64`](Self::try_from_i64)
    /// accepts; [`load`](DeltaColumn::load) validates each slab's
    /// realized min/max aggregate against them.
    const MIN_I64: i64;
    const MAX_I64: i64;

    /// Convert to `i64` for delta computation. Returns `None` for null values.
    ///
    /// Write paths funnel through this, so implementations for types whose
    /// domain exceeds `i64` (`u64`, `usize`) **panic** on out-of-domain
    /// values — see the trait-level domain contract.
    fn to_i64(self) -> Option<i64>;

    /// Non-panicking variant of [`to_i64`](Self::to_i64) for query paths.
    ///
    /// Returns `None` for nulls *and* for values outside the domain —
    /// such values can never be stored, so a query for one is simply
    /// "not found".  The default delegates to `to_i64`; implementations
    /// whose `to_i64` panics (unsigned types) must override this.
    fn try_to_i64(self) -> Option<i64> {
        self.to_i64()
    }

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
}

// ── Non-nullable impls ──────────────────────────────────────────────────────

impl DeltaValue for u32 {
    const NULLABLE: bool = false;
    type Inner = i64;
    const MIN_I64: i64 = 0;
    const MAX_I64: i64 = u32::MAX as i64;
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
}

impl DeltaValue for u64 {
    const NULLABLE: bool = false;
    type Inner = i64;
    const MIN_I64: i64 = 0;
    const MAX_I64: i64 = i64::MAX;
    /// # Panics
    ///
    /// Panics for values `> i64::MAX` — see the [`DeltaValue`] domain
    /// contract (2^63 is the wire format's closure boundary under
    /// deletion).
    fn to_i64(self) -> Option<i64> {
        assert!(
            self <= i64::MAX as u64,
            "DeltaColumn<u64> values must fit in i64 (wire deltas are i64): {self}"
        );
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as u64
    }
    fn try_to_i64(self) -> Option<i64> {
        i64::try_from(self).ok()
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        u64::try_from(v).map_err(|_| format!("delta value {} out of u64 range", v))
    }
    fn null_value() -> Self {
        panic!("non-nullable u64")
    }
}

impl DeltaValue for i32 {
    const NULLABLE: bool = false;
    type Inner = i64;
    const MIN_I64: i64 = i32::MIN as i64;
    const MAX_I64: i64 = i32::MAX as i64;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as i32
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        i32::try_from(v).map_err(|_| format!("delta value {} out of i32 range", v))
    }
    fn null_value() -> Self {
        panic!("non-nullable i32")
    }
}

impl DeltaValue for i64 {
    const NULLABLE: bool = false;
    type Inner = i64;
    const MIN_I64: i64 = i64::MIN;
    const MAX_I64: i64 = i64::MAX;
    fn to_i64(self) -> Option<i64> {
        Some(self)
    }
    fn from_i64(v: i64) -> Self {
        v
    }
    fn null_value() -> Self {
        panic!("non-nullable i64")
    }
}

impl DeltaValue for usize {
    const NULLABLE: bool = false;
    type Inner = i64;
    const MIN_I64: i64 = 0;
    // 32-bit targets (wasm) have a smaller usize domain
    const MAX_I64: i64 = if usize::BITS >= 64 {
        i64::MAX
    } else {
        usize::MAX as i64
    };
    /// # Panics
    ///
    /// Panics for values `> i64::MAX` — see the [`DeltaValue`] domain
    /// contract.
    fn to_i64(self) -> Option<i64> {
        assert!(
            self as u64 <= i64::MAX as u64,
            "DeltaColumn<usize> values must fit in i64 (wire deltas are i64): {self}"
        );
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as usize
    }
    fn try_to_i64(self) -> Option<i64> {
        i64::try_from(self).ok()
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        usize::try_from(v).map_err(|_| format!("delta value {} out of usize range", v))
    }
    fn null_value() -> Self {
        panic!("non-nullable usize")
    }
}

// ── Nullable impls ──────────────────────────────────────────────────────────

impl DeltaValue for Option<u32> {
    const NULLABLE: bool = true;
    type Inner = Option<i64>;
    const MIN_I64: i64 = u32::MIN_I64;
    const MAX_I64: i64 = u32::MAX_I64;
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
}

impl DeltaValue for Option<u64> {
    const NULLABLE: bool = true;
    type Inner = Option<i64>;
    const MIN_I64: i64 = u64::MIN_I64;
    const MAX_I64: i64 = u64::MAX_I64;
    /// # Panics
    ///
    /// Panics for `Some(v)` with `v > i64::MAX` — see [`DeltaValue`].
    fn to_i64(self) -> Option<i64> {
        self.and_then(DeltaValue::to_i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as u64)
    }
    fn try_to_i64(self) -> Option<i64> {
        self.and_then(DeltaValue::try_to_i64)
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        u64::try_from_i64(v).map(Some)
    }
    fn null_value() -> Self {
        None
    }
}

impl DeltaValue for Option<i32> {
    const NULLABLE: bool = true;
    type Inner = Option<i64>;
    const MIN_I64: i64 = i32::MIN_I64;
    const MAX_I64: i64 = i32::MAX_I64;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as i32)
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        i32::try_from_i64(v).map(Some)
    }
    fn null_value() -> Self {
        None
    }
}

impl DeltaValue for Option<i64> {
    const NULLABLE: bool = true;
    type Inner = Option<i64>;
    const MIN_I64: i64 = i64::MIN;
    const MAX_I64: i64 = i64::MAX;
    fn to_i64(self) -> Option<i64> {
        self
    }
    fn from_i64(v: i64) -> Self {
        Some(v)
    }
    fn null_value() -> Self {
        None
    }
}

impl DeltaValue for Option<usize> {
    const NULLABLE: bool = true;
    type Inner = Option<i64>;
    const MIN_I64: i64 = usize::MIN_I64;
    const MAX_I64: i64 = usize::MAX_I64;
    /// # Panics
    ///
    /// Panics for `Some(v)` with `v > i64::MAX` — see [`DeltaValue`].
    fn to_i64(self) -> Option<i64> {
        self.and_then(DeltaValue::to_i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as usize)
    }
    fn try_to_i64(self) -> Option<i64> {
        self.and_then(DeltaValue::try_to_i64)
    }
    fn try_from_i64(v: i64) -> Result<Self, String> {
        usize::try_from_i64(v).map(Some)
    }
    fn null_value() -> Self {
        None
    }
}

// ── DeltaEncoder ────────────────────────────────────────────────────────────

/// State half of [`DeltaEncoder`] — owns delta tracking and inner-RLE
/// state but **not** the output buffer.  Every mutating method takes a
/// `&mut Vec<u8>` so the caller decides where bytes land.  Used by the
/// fast-path `encode_to_unless` static path to avoid a per-call heap
/// allocation in the change-encoding loop.
pub struct DeltaEncoderState<'a, T: DeltaValue, C: Codec = Leb128> {
    inner: RleEncoderState<'a, Option<i64>, C>,
    abs: i64,
    /// Tracks whether every appended value has been equal so far.
    ///
    /// - `None` — either nothing has been appended yet, or the appended
    ///   values are not all equal (i.e. the column is "mixed").
    /// - `Some(v)` — every appended value has been equal to `v`.
    ///
    /// Used by [`save_to_unless`](DeltaEncoder::save_to_unless) to match v0's
    /// `encode_unless_empty` semantics for nullable columns (where
    /// `v == null`) and to provide RleEncoder-style single-run-of-value
    /// elision for non-nullable columns.
    uniform: Option<T>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue, C: Codec> Default for DeltaEncoderState<'_, T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T: DeltaValue, C: Codec> DeltaEncoderState<'a, T, C> {
    pub fn new() -> Self {
        Self {
            inner: RleEncoderState::new(),
            abs: 0,
            uniform: None,
            _phantom: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn append(&mut self, buf: &mut Vec<u8>, value: T) {
        self.append_n(buf, value, 1);
    }

    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: T, n: usize) {
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
                self.inner.append_n_owned(buf, Some(first_delta), 1);
                if n > 1 {
                    self.inner.append_n_owned(buf, Some(0), n - 1);
                }
            }
            None => {
                self.inner.append_n_owned(buf, None, n);
            }
        }
    }

    pub fn extend<I: IntoIterator<Item = T>>(&mut self, buf: &mut Vec<u8>, iter: I) {
        for v in iter {
            self.append(buf, v);
        }
    }

    /// Flush any pending run into `buf`.
    pub fn finish(&mut self, buf: &mut Vec<u8>) {
        self.inner.finish(buf);
    }

    /// True if every appended value equals `value` (or no values have been
    /// appended at all).  Caller is responsible for not having flushed yet
    /// when this is used to drive elision.
    pub fn is_uniform(&self, value: T) -> bool {
        self.inner.is_empty() || self.uniform == Some(value)
    }
}

/// Streaming encoder for delta-encoded columns.
///
/// Mirrors [`RleEncoder`](crate::encoder::RleEncoder)'s interface but applies
/// delta encoding on append: each absolute value is transformed into the
/// difference from the previous non-null value before being written to an
/// inner RLE encoder.  The serialized bytes are byte-compatible with both
/// [`DeltaColumn::save`] and v0's `DeltaCursor::encode`.
///
/// Use this when you need to build a delta column incrementally (e.g. while
/// walking change ops) rather than collecting a `Vec` and calling
/// [`DeltaColumn::from_values`].
///
/// ```no_run
/// # use hexane::delta::DeltaEncoder;
/// let mut enc = DeltaEncoder::<i64>::new();
/// enc.append(10);
/// enc.append(20);
/// enc.append(30);
/// let bytes = enc.save(); // [10, 10, 10] deltas, RLE-encoded
/// ```
pub struct DeltaEncoder<'a, T: DeltaValue, C: Codec = Leb128> {
    data: Vec<u8>,
    state: DeltaEncoderState<'a, T, C>,
}

impl<T: DeltaValue, C: Codec> Default for DeltaEncoder<'_, T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DeltaValue, C: Codec> Debug for DeltaEncoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaEncoder")
            .field("len", &self.state.inner.len())
            .field("abs", &self.state.abs)
            .finish()
    }
}

impl<'a, T: DeltaValue, C: Codec> DeltaEncoder<'a, T, C> {
    /// Create a new empty delta encoder.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            state: DeltaEncoderState::new(),
        }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Append a single (absolute) value.
    ///
    /// For non-nullable types the value is always stored.  For nullable
    /// types, a `null` value does not advance the running absolute — it's
    /// emitted as a null entry in the inner column.
    pub fn append(&mut self, value: T) {
        self.state.append(&mut self.data, value);
    }

    /// Append `n` copies of the same (absolute) `value`.
    ///
    /// The first copy is encoded as `value - prev_abs`; subsequent copies
    /// are encoded as `0` (since the absolute hasn't changed).  For a null
    /// value, `n` null entries are emitted and `abs` is unchanged.
    pub fn append_n(&mut self, value: T, n: usize) {
        self.state.append_n(&mut self.data, value, n);
    }

    /// Alias for [`append`](Self::append) — provided so call sites that
    /// use [`append_owned`](crate::encoder::RleEncoder::append_owned) on
    /// `RleEncoder` can swap encoders without edits.
    pub fn append_owned(&mut self, value: T) {
        self.append(value);
    }

    /// Append all values from an iterator.
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.state.extend(&mut self.data, iter);
    }

    fn finish(&mut self) {
        self.state.finish(&mut self.data);
    }

    /// Flush and return the encoded bytes.  Consumes the encoder.
    pub fn save(mut self) -> Vec<u8> {
        self.finish();
        self.data
    }

    /// Flush and append the encoded bytes to `out`.  Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(mut self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.finish();
        let start = out.len();
        out.extend_from_slice(&self.data);
        start..out.len()
    }

    /// Flush and append the encoded bytes to `out`, returning an empty
    /// range when the encoder is empty or every appended value equals
    /// `value`.
    ///
    /// Mirrors [`RleEncoder::save_to_unless`](crate::encoder::RleEncoder::save_to_unless)
    /// on the absolute (realized) values.  For nullable delta columns
    /// pass `None` to get v0's `encode_unless_empty` semantics (elide on
    /// empty or all-null).
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T) -> std::ops::Range<usize> {
        if self.state.is_uniform(value) {
            return out.len()..out.len();
        }
        self.save_to(out)
    }

    /// Encode values from an iterator and return the raw bytes.
    pub fn encode<I: IntoIterator<Item = T>>(iter: I) -> Vec<u8> {
        let mut enc = Self::new();
        enc.extend(iter);
        enc.save()
    }

    /// Encode values from an iterator and append the bytes to `out`.
    /// Returns the byte range written.
    ///
    /// Fast-path: writes through to `out` via [`DeltaEncoderState`] without
    /// allocating an inner `Vec<u8>`.
    pub fn encode_to<I: IntoIterator<Item = T>>(
        out: &mut Vec<u8>,
        iter: I,
    ) -> std::ops::Range<usize> {
        let start = out.len();
        let mut state = DeltaEncoderState::<'a, T>::new();
        state.extend(out, iter);
        state.finish(out);
        start..out.len()
    }

    /// Encode values from an iterator and append the bytes to `out`,
    /// eliding the column if it's empty or every value equals `value`.
    /// See [`save_to_unless`](Self::save_to_unless).
    ///
    /// Fast-path: writes through to `out` via [`DeltaEncoderState`].  When
    /// the uniform check passes we truncate `out` back to `start` to undo
    /// any in-progress writes from `extend`.
    pub fn encode_to_unless<I: IntoIterator<Item = T>>(
        out: &mut Vec<u8>,
        iter: I,
        value: T,
    ) -> std::ops::Range<usize> {
        let start = out.len();
        let mut state = DeltaEncoderState::<'a, T>::new();
        state.extend(out, iter);
        if state.is_uniform(value) {
            out.truncate(start);
            return start..start;
        }
        state.finish(out);
        start..out.len()
    }
}

// ── DeltaColumn ────────────────────────────────────────────────────────────
//
// Deltas are stored as `Column<T::Inner, C, IndexedDeltaWeightFn>` — `i64`
// for non-nullable `T`, `Option<i64>` for nullable (see [`DeltaInner`]);
// the wire bytes are identical either way.  `T` only affects how realized
// values are materialized on read (and validated on load).  The `SlabAgg` aggregate (len + total +
// min/max offsets) unlocks `find_by_value` / `find_by_range` via min/max
// pruning.

/// A delta-encoded column.
///
/// Values must respect the [`DeltaValue`] domain contract: all realized
/// values lie within a 2^63-wide range (for unsigned types, `< 2^63`).
/// Writes outside the domain panic; [`load`](DeltaColumn::load) rejects
/// out-of-domain data with an error.
pub struct DeltaColumn<T: DeltaValue, C: Codec = Leb128> {
    pub(crate) col: Column<T::Inner, C, IndexedDeltaWeightFn>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue, C: Codec> Clone for DeltaColumn<T, C> {
    fn clone(&self) -> Self {
        Self {
            col: self.col.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: DeltaValue, C: Codec> Debug for DeltaColumn<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaColumn")
            .field("len", &self.col.len())
            .field("slabs", &self.col.slab_count())
            .finish()
    }
}

impl<T: DeltaValue, C: Codec> Default for DeltaColumn<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DeltaValue, C: Codec> DeltaColumn<T, C> {
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
        let inner: Vec<Option<i64>> = values.into_iter().map(|t| t.to_i64()).collect();
        let mut col = Column::new();
        col.splice(0, 0, deltas_from::<T::Inner>(&inner, 0));
        Self {
            col,
            _phantom: PhantomData,
        }
    }

    pub fn load_with<'a, F>(data: &'a [u8], opts: LoadOpts<F>) -> Result<Self, PackError>
    where
        F: crate::MaybeFill<<T::Inner as crate::ColumnValueRef>::Get<'a>>,
    {
        // Non-nullable T stores `Column<i64>`, whose decoder rejects null
        // runs at the segment level — no run-level null scan needed.
        let iter = InnerColumn::<T, C>::load_iter(data, opts);
        // Domain validation per slab: the aggregates accumulated during
        // the load (exact — `accumulate_run` errors on any within-slab
        // overflow) hold each slab's realized min/max as offsets from its
        // start. Walking them with a widened running sum catches both
        // out-of-domain values and cross-slab overflow, before any
        // aggregate reaches the index's merge arithmetic. On valid data
        // `running` never leaves the domain; i128 keeps the comparisons
        // exact when a hostile slab would wrap them.
        let (lo, hi) = (T::MIN_I64 as i128, T::MAX_I64 as i128);
        let mut running: i128 = 0;
        let col = iter.finalize_with(|w| {
            if w.len == 0 {
                return Ok(());
            }
            if running + (w.min_offset as i128) < lo || running + (w.max_offset as i128) > hi {
                return Err(PackError::InvalidValue("delta value out of domain".into()));
            }
            running += w.total as i128;
            Ok(())
        })?;
        Ok(Self {
            col,
            _phantom: PhantomData,
        })
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_with(data, crate::LoadOpts::default())
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

    /// Serialize, unless every **realized** value equals `value` — in
    /// which case write nothing and return an empty range.
    ///
    /// The sentinel is expressed in `T`, like the rest of the API, and is
    /// compared against realized values (matching
    /// [`DeltaEncoder::save_to_unless`]): a uniform column `[7, 7, 7]`
    /// elides on sentinel `7` even though its stored deltas are
    /// `[7, 0, 0]`.
    ///
    /// # Panics
    ///
    /// Inherits [`DeltaValue::to_i64`]'s domain check for unsigned
    /// sentinels (an unstorable sentinel is a caller bug).
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: T) -> std::ops::Range<usize> {
        match value.to_i64() {
            // Null sentinel: elide iff every entry is null.
            None => {
                let null = T::Inner::from_opt(None);
                self.col
                    .save_to_unless(out, crate::AsColumnRef::as_column_ref(&null))
            }
            // Realized sentinel v: uniform iff the first delta realizes v
            // and every later delta is zero.
            Some(v) => {
                let uniform = self.col.is_empty() || {
                    let mut it = self.col.iter();
                    let mut ok = it.next().map(T::Inner::to_opt) == Some(Some(v));
                    while ok {
                        match it.next_run() {
                            Some(run) if T::Inner::to_opt(run.value) != Some(0) => ok = false,
                            Some(_) => {}
                            None => break,
                        }
                    }
                    ok
                };
                if uniform {
                    out.len()..out.len()
                } else {
                    self.col.save_to(out)
                }
            }
        }
    }

    /// Collect realized values into a Vec.
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().collect()
    }

    pub fn get(&self, index: usize) -> Option<T> {
        self.iter().nth(index)
    }

    pub fn iter(&self) -> DeltaIter<'_, T, C> {
        DeltaIter {
            inner: self.col.iter(),
            running: 0,
            col: Some(&self.col),
            _phantom: PhantomData,
        }
    }

    pub fn iter_range(&self, range: Range<usize>) -> DeltaIter<'_, T, C> {
        let start = range.start.min(self.col.len());
        let end = range.end.min(self.col.len());
        let mut iter = self.iter();
        iter.set_max(end);
        iter.advance_by(start);
        iter
    }

    /// Narrow a range to the contiguous run of items matching `value`.
    ///
    /// Assumes realized values within `range` are sorted.  If they
    /// aren't, the result is unspecified — a wrong or empty range may
    /// be returned — but never a panic, memory unsafety, or column
    /// corruption.
    ///
    /// Returns the sub-range of `range` where every item equals
    /// `value`, or an empty range at the appropriate insertion point if
    /// `value` is not present.
    ///
    /// Same contract as [`Column::scope_to_value`](crate::Column::scope_to_value),
    /// backed by [`DeltaIter::seek_to_value`]: an O(log S) slab-prefix
    /// descent plus a single slab decode, rather than a scan.
    pub fn scope_to_value(&self, value: T, range: impl std::ops::RangeBounds<usize>) -> Range<usize>
    where
        T: Ord,
    {
        self.iter().seek_to_value(value, range)
    }

    pub fn insert(&mut self, index: usize, value: T) {
        self.splice(index, 0, [value]);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.col.total_len {
            self.splice(index, 1, std::iter::empty());
        }
    }

    /// Remove `n` items starting at `index` — same arguments as
    /// [`splice`](Self::splice)`(index, n, [])`.  Panics if `index + n`
    /// exceeds the column length.
    pub fn remove_n(&mut self, index: usize, n: usize) {
        if n > 0 {
            self.splice(index, n, std::iter::empty());
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

        let mut values = values.into_iter().map(|t| t.to_i64()).peekable();
        if del == 0 && values.peek().is_none() {
            return 0..0;
        }

        let (prev, skip, next_val) = self.find_boundaries(index, del);
        // When a boundary non-null follows the splice, replace `del` original
        // items + `skip` preserved nulls + 1 boundary, and inline the bulk
        // null run + a recomputed boundary delta to keep the realized value
        // of `next_val` intact.
        let total_del = del + if next_val.is_some() { skip + 1 } else { 0 };
        let iter = SpliceDeltaIter::<_, T::Inner>::new(values, prev, skip, next_val);
        self.col.splice_inner(index, total_del, iter)
    }

    /// Find the boundary data needed to fix up a splice at `[index, index + del)`.
    ///
    /// Returns `(prev, skip, next_val)` where:
    /// - `prev` is the realized prefix at `index` (running before position `index`).
    /// - `skip` is the number of null entries between `index + del` and the
    ///   next non-null (or the end of the column).
    /// - `next_val` is the realized value of that next non-null (or `None` if
    ///   there isn't one).  Subsequent deltas decode against this value, so
    ///   fix-ups must re-establish it to preserve the column's realized values.
    fn find_boundaries(&self, index: usize, del: usize) -> (i64, usize, Option<i64>) {
        let mut iter = self.iter();
        iter.advance_by(index);
        let prev = iter.running;
        iter.advance_by(del);

        // RLE merges consecutive null runs, so at most one null run sits
        // between `index + del` and the next non-null — no loop needed.
        match iter.inner.next_run() {
            None => (prev, 0, None),
            Some(run) if T::Inner::to_opt(run.value).is_none() => {
                (prev, run.count, iter.next().map(|_| iter.running))
            }
            Some(run) => (
                prev,
                0,
                Some(iter.running + T::Inner::to_opt(run.value).unwrap()),
            ),
        }
    }
}

// ── DeltaIter ──────────────────────────────────────────────────────────────

type InnerColumn<T, C = Leb128> = Column<<T as DeltaValue>::Inner, C, IndexedDeltaWeightFn>;

/// Iterator over realized values in a [`DeltaColumn`].
///
/// `next()` walks the underlying `Column` value-by-value and
/// accumulates a running `i64` prefix.  `nth(n)` uses the column's
/// B-tree to jump directly to the target position and reset the
/// running prefix in O(log n), avoiding an O(n) replay.
pub struct DeltaIter<'a, T: DeltaValue, C: Codec = Leb128> {
    inner: Iter<'a, T::Inner, C>,
    running: i64,
    col: Option<&'a InnerColumn<T, C>>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue, C: Codec> Default for DeltaIter<'_, T, C> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            running: 0,
            col: None,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeltaValue, C: Codec> Iterator for DeltaIter<'_, T, C> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match T::Inner::to_opt(self.inner.next()?) {
            None => Some(T::null_value()),
            Some(d) => {
                self.running += d;
                Some(T::from_i64(self.running))
            }
        }
    }

    fn nth(&mut self, mut n: usize) -> Option<T> {
        if n == 0 {
            return self.next();
        }
        if n >= self.inner.items_left {
            if self.inner.items_left > 0 {
                self.nth(self.inner.items_left - 1);
            }
            return None;
        }

        if self.inner.slab_remaining <= n {
            let pos = self.pos();
            let col = self.col?;
            let found = col.index.find_slab_at_item(pos + n);
            self.running = found.prefix;
            if !self.inner.advance_to_slab(found.index, found.pos) {
                return None;
            }
            n -= self.pos() - pos;
        }
        while let Some(run) = self.inner.next_run_max(n + 1) {
            let delta = T::Inner::to_opt(run.value);
            if let Some(d) = delta {
                self.running += d * run.count as i64;
            }
            if run.count > n {
                return Some(match delta {
                    None => T::null_value(),
                    Some(_) => T::from_i64(self.running),
                });
            }
            n -= run.count
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: DeltaValue, C: Codec> Debug for DeltaIter<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaIter")
            .field("pos", &self.inner.pos())
            .field("running", &self.running)
            .field("items_left", &self.inner.len())
            .finish()
    }
}

impl<T: DeltaValue, C: Codec> ExactSizeIterator for DeltaIter<'_, T, C> {}

impl<'a, T: DeltaValue, C: Codec> DeltaIter<'a, T, C> {
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

    /// Reposition the iterator window to `range`.
    ///
    /// After this call the iterator yields the items in `range` and then
    /// returns `None`. Equivalent to `set_max(range.end)` followed by
    /// `advance_to(range.start)`.
    pub fn shift(&mut self, range: Range<usize>) {
        self.set_max(range.end);
        self.advance_to(range.start);
    }

    pub fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
    }

    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T> {
        assert!(range.start >= self.inner.pos);
        self.inner.set_max(range.end);
        self.nth(range.start - self.inner.pos)
    }

    pub fn suspend(&self) -> DeltaIterState {
        DeltaIterState {
            inner: self.inner.suspend(),
            running: self.running,
        }
    }

    /// Narrow the iterator window to the contiguous run of `target`
    /// within a sorted range, returning that range.
    ///
    /// If the value is not found, returns an empty range at the
    /// insertion point and the iterator is positioned there.
    ///
    /// Assumes values within `range` are sorted by `T`'s `Ord` (for
    /// nullable columns that puts null rows first). If they aren't,
    /// the result is unspecified — a wrong or empty range may be
    /// returned — but never a panic, memory unsafety, or column
    /// corruption.
    ///
    /// The encoding stores deltas, so unlike
    /// [`Iter::seek_to_value`](crate::Iter::seek_to_value) there are no
    /// slab-head values to compare against. But the B-tree knows the
    /// running prefix at the start of every slab, and for sorted data
    /// the prefix *is* the realized value — so one descent
    /// ([`find_slab_at_prefix`](crate::btree::SlabBTree)) lands on the
    /// slab where the running sum first reaches the target, without
    /// decoding anything. Only that slab is decoded: a run walk finds
    /// the exact position (repeats of a value are runs of zero deltas,
    /// which is also how the run of the target is extended).
    pub fn seek_to_value(
        &mut self,
        target: T,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Range<usize>
    where
        T: Ord,
    {
        let (start, end) = normalize_range_max(range, self.end_pos());
        self.set_max(end);
        if start > self.pos() {
            self.advance_to(start);
        }
        let start = self.pos();
        if start >= end {
            return start..start;
        }

        // null targets sort first: the run, if any, is the window's
        // leading null rows
        if T::NULLABLE && target == T::null_value() {
            let mut probe = self.clone();
            let mut run_end = start;
            while run_end < end {
                let Some(run) = probe.inner.next_run() else {
                    break;
                };
                if T::Inner::to_opt(run.value).is_some() {
                    break;
                }
                run_end += run.count;
            }
            return start..run_end.min(end);
        }

        let Some(t) = target.try_to_i64() else {
            // outside the storable domain: greater than anything here
            self.advance_to(end);
            return end..end;
        };

        // jump the probe to the slab where the running prefix first
        // reaches the target. With garbage outside the (sorted) window
        // the descent can only land early, never late, so clamping to
        // the window start keeps this correct — just slower.
        let mut probe = self.clone();
        if let Some(col) = probe.col {
            let (si, prefix, pos) = col.index.find_slab_at_prefix(t);
            if pos > probe.pos() && pos < end {
                probe.running = prefix;
                if !probe.inner.advance_to_slab(si, pos) {
                    probe = self.clone();
                }
            }
        }

        // decode from the slab start: find the first value >= target
        let mut pos = probe.pos();
        let mut running = probe.running;
        let mut run_start = end;
        let mut run_end = end;
        while pos < end {
            let Some(run) = probe.inner.next_run() else {
                break;
            };
            let count = run.count.min(end - pos);
            match T::Inner::to_opt(run.value) {
                // null rows sort before every value
                None => pos += count,
                Some(d) => {
                    let run_final = running + d * count as i64;
                    if run_final < t {
                        running = run_final;
                        pos += count;
                        continue;
                    }
                    // the crossing is inside this run: the j-th item
                    // (1-based) realizes running + j*d
                    // d <= 0 happens at the window boundary (the run
                    // bridges pre-window data) or on all-equal runs: the
                    // first item is the only candidate either way
                    let steps = if d <= 0 {
                        1
                    } else {
                        ((t - running + d - 1).div_euclid(d)).max(1)
                    } as usize;
                    run_start = pos + steps - 1;
                    let found = running + d * steps as i64 == t;
                    run_end = run_start;
                    if found {
                        run_end += 1;
                        if d == 0 {
                            // the rest of this run repeats the target
                            run_end = pos + count;
                        }
                        if run_end == pos + count {
                            // ended at a run boundary: adjacent runs of
                            // zero deltas keep repeating the target
                            while run_end < end {
                                let Some(run) = probe.inner.next_run() else {
                                    break;
                                };
                                if T::Inner::to_opt(run.value) != Some(0) {
                                    break;
                                }
                                run_end += run.count;
                            }
                            run_end = run_end.min(end);
                        }
                    }
                    break;
                }
            }
        }
        if run_start == end {
            run_end = end;
        }

        self.advance_to(run_start);
        run_start..run_end
    }

    /// Scan forward for the next occurrence of `target`, letting the
    /// column's value aggregates skip slabs that cannot contain it.
    ///
    /// Unlike [`Self::seek_to_value`] the values need not be sorted —
    /// this is a find. On a hit, returns the position with the value
    /// consumed: iteration continues with the item after it. On a
    /// miss, returns `None` with the iterator at the end of its
    /// window.
    pub fn scan_to_value(&mut self, target: T) -> Option<usize> {
        if target.try_to_i64().is_none() && T::NULLABLE && target == T::null_value() {
            // nulls are invisible to the value aggregates — plain run scan
            let end = self.end_pos();
            return self.scan_to_null(end);
        }
        self.scan_to_range(target..=target).map(|(pos, _)| pos)
    }

    /// Scan forward for the next value inside `range`, letting the
    /// column's value aggregates skip slabs whose value span cannot
    /// overlap it.
    ///
    /// The values need not be sorted. Within a slab the scan is
    /// run-by-run (a run's values form an arithmetic progression, so
    /// interval membership is O(1)); when a slab is exhausted the
    /// index supplies the next candidate slab and everything in
    /// between is skipped without decoding. Null rows never match.
    ///
    /// On a hit, returns `Some((pos, value))` with the value consumed:
    /// iteration continues with the item after it. On a miss, returns
    /// `None` with the iterator at the end of its window.
    pub fn scan_to_range(&mut self, range: impl std::ops::RangeBounds<T>) -> Option<(usize, T)> {
        use std::ops::Bound;
        let end = self.end_pos();
        if self.pos() >= end {
            return None;
        }

        // resolve the bounds to an inclusive i64 interval, clamped to
        // the storable domain
        let lo = match range.start_bound() {
            Bound::Unbounded => T::MIN_I64,
            Bound::Included(v) => v.try_to_i64()?,
            Bound::Excluded(v) => v.try_to_i64()?.checked_add(1)?,
        };
        let hi = match range.end_bound() {
            Bound::Unbounded => T::MAX_I64,
            Bound::Included(v) => v.try_to_i64()?,
            Bound::Excluded(v) => v.try_to_i64()?.checked_sub(1)?,
        };
        if lo > hi {
            return None;
        }

        // the first item of a delta run hitting `[lo, hi]`, if any:
        // values are `running + d*j` for `j` in `1..=count`
        fn run_hit(running: i64, d: i64, count: usize, lo: i64, hi: i64) -> Option<usize> {
            let j = if d == 0 {
                1
            } else if d > 0 {
                // increasing: first j at or above lo
                ((lo - running) + d - 1).div_euclid(d).max(1)
            } else {
                // decreasing: first j at or below hi
                let d = -d;
                ((running - hi) + d - 1).div_euclid(d).max(1)
            };
            if j as usize > count {
                return None;
            }
            let v = running + d * j;
            (lo <= v && v <= hi).then_some(j as usize)
        }

        let mut candidates = self.col.map(|c| c.find_by_value_range(lo, hi));
        loop {
            // scan the rest of the current slab; on a hit, rewind to
            // the mark and step precisely onto it
            let mark = self.clone();
            let mut pos = self.pos();
            let mut running = self.running;
            let mut hit = None;
            while self.inner.slab_remaining > 0 && pos < end {
                let Some(run) = self.inner.next_run() else {
                    break;
                };
                let count = run.count.min(end - pos);
                match T::Inner::to_opt(run.value) {
                    None => pos += count,
                    Some(d) => {
                        if let Some(j) = run_hit(running, d, count, lo, hi) {
                            hit = Some(pos + j - 1);
                            break;
                        }
                        running += d * count as i64;
                        pos += count;
                    }
                }
            }
            if let Some(h) = hit {
                *self = mark;
                let v = self.nth(h - self.pos())?;
                return Some((h, v));
            }

            // no hit in this slab — jump to the next candidate
            let cands = candidates.as_mut()?;
            loop {
                let Some((si, items_before, prefix)) = cands.next() else {
                    self.advance_to(end);
                    return None;
                };
                if items_before >= end {
                    self.advance_to(end);
                    return None;
                }
                // candidates arrive in slab order; skip the ones behind us
                if items_before < self.pos() {
                    continue;
                }
                self.running = prefix;
                if !self.inner.advance_to_slab(si, items_before) {
                    self.advance_to(end);
                    return None;
                }
                break;
            }
        }
    }

    fn scan_to_null(&mut self, end: usize) -> Option<usize> {
        let mark = self.clone();
        let mut pos = self.pos();
        let mut hit = None;
        while pos < end {
            let Some(run) = self.inner.next_run() else {
                break;
            };
            if T::Inner::to_opt(run.value).is_none() {
                hit = Some(pos);
                break;
            }
            pos += run.count;
        }
        match hit {
            Some(h) if h < end => {
                *self = mark;
                self.nth(h - self.pos())?;
                Some(h)
            }
            _ => {
                self.advance_to(end);
                None
            }
        }
    }
}

pub struct DeltaIterState {
    inner: IterState,
    running: i64,
}

impl DeltaIterState {
    pub fn try_resume<'a, T: DeltaValue, C: Codec>(
        &self,
        column: &'a DeltaColumn<T, C>,
    ) -> Result<DeltaIter<'a, T, C>, crate::PackError> {
        let inner = self.inner.try_resume(&column.col)?;
        Ok(DeltaIter {
            inner,
            running: self.running,
            col: Some(&column.col),
            _phantom: PhantomData,
        })
    }
}

impl<'a, T: DeltaValue, C: Codec> Clone for DeltaIter<'a, T, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            running: self.running,
            col: self.col,
            _phantom: PhantomData,
        }
    }
}

// ── Free helpers ────────────────────────────────────────────────────────────

/// Iterator that maps realized values to their delta encoding relative to
/// a running `prev_realized`.  Null values pass through as `None` and leave
/// `prev_realized` untouched.  No allocation.
pub(crate) fn deltas_from<'a, I: DeltaInner>(
    values: &'a [Option<i64>],
    mut prev_realized: i64,
) -> impl Iterator<Item = I> + 'a {
    values.iter().map(move |v| match v {
        None => I::from_opt(None),
        Some(r) => {
            let d = *r - prev_realized;
            prev_realized = *r;
            I::from_opt(Some(d))
        }
    })
}

/// Streaming iterator that turns realized `Option<i64>` values + the result
/// of `find_boundaries` into the `(delta, count)` pairs that
/// `Column::splice_inner` consumes.  Avoids allocating the values into a
/// `Vec`: emits each delta in lockstep with the input iterator, then —
/// when a boundary non-null follows the splice — emits a bulk
/// `(None, skip)` and a recomputed boundary delta against the post-splice
/// running.
struct SpliceDeltaIter<It, Out> {
    iter: It,
    running: i64,
    skip: usize,
    next_val: Option<i64>,
    phase: SplicePhase,
    _out: PhantomData<Out>,
}

enum SplicePhase {
    Values,
    NullRun,
    Boundary,
    Done,
}

impl<It: Iterator<Item = Option<i64>>, Out: DeltaInner> SpliceDeltaIter<It, Out> {
    fn new(iter: It, prev: i64, skip: usize, next_val: Option<i64>) -> Self {
        Self {
            iter,
            running: prev,
            skip,
            next_val,
            phase: SplicePhase::Values,
            _out: PhantomData,
        }
    }
}

impl<It: Iterator<Item = Option<i64>>, Out: DeltaInner> Iterator for SpliceDeltaIter<It, Out> {
    type Item = (Out, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.phase {
                SplicePhase::Values => match self.iter.next() {
                    Some(None) => return Some((Out::from_opt(None), 1)),
                    Some(Some(r)) => {
                        let d = r - self.running;
                        self.running = r;
                        return Some((Out::from_opt(Some(d)), 1));
                    }
                    None => {
                        self.phase = if self.next_val.is_some() {
                            SplicePhase::NullRun
                        } else {
                            SplicePhase::Done
                        };
                    }
                },
                SplicePhase::NullRun => {
                    self.phase = SplicePhase::Boundary;
                    // don't construct a null for a zero-length run — the
                    // non-nullable inner type can't represent one
                    if self.skip > 0 {
                        return Some((Out::from_opt(None), self.skip));
                    }
                }
                SplicePhase::Boundary => {
                    self.phase = SplicePhase::Done;
                    let next = self.next_val.expect("boundary phase requires next_val");
                    return Some((Out::from_opt(Some(next - self.running)), 1));
                }
                SplicePhase::Done => return None,
            }
        }
    }
}

// ── FromIterator / Extend / IntoIterator ────────────────────────────────────

impl<T: DeltaValue, C: Codec> FromIterator<T> for DeltaColumn<T, C> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<T: DeltaValue, C: Codec> Extend<T> for DeltaColumn<T, C> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let vals: Vec<T> = iter.into_iter().collect();
        if !vals.is_empty() {
            let len = self.len();
            self.splice(len, 0, vals);
        }
    }
}

impl<'a, T: DeltaValue, C: Codec> IntoIterator for &'a DeltaColumn<T, C> {
    type Item = T;
    type IntoIter = DeltaIter<'a, T, C>;

    fn into_iter(self) -> DeltaIter<'a, T, C> {
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

    // ── u64 / usize / i32 domain validation ────────────────────────────

    #[test]
    #[should_panic(expected = "must fit in i64")]
    fn delta_u64_write_panics_above_2_63() {
        let _ = DeltaColumn::<u64>::from_values(vec![i64::MAX as u64 + 1]);
    }

    #[test]
    #[should_panic(expected = "must fit in i64")]
    fn delta_opt_u64_write_panics_above_2_63() {
        let mut col = DeltaColumn::<Option<u64>>::new();
        col.push(Some(u64::MAX));
    }

    #[test]
    #[should_panic(expected = "must fit in i64")]
    fn delta_usize_write_panics_above_2_63() {
        let _ = DeltaColumn::<usize>::from_values(vec![i64::MAX as usize + 1]);
    }

    #[test]
    fn delta_save_to_unless_realized_semantics() {
        // Uniform realized values elide (deltas are [7, 0, 0] internally).
        let col = DeltaColumn::<u64>::from_values(vec![7, 7, 7]);
        let mut out = Vec::new();
        assert!(col.save_to_unless(&mut out, 7).is_empty());
        assert!(out.is_empty());
        // Non-uniform saves, and the bytes match save().
        let col = DeltaColumn::<u64>::from_values(vec![7, 7, 8]);
        let range = col.save_to_unless(&mut out, 7);
        assert_eq!(&out[range], col.save().as_slice());
        // Nullable: all-null elides on None sentinel.
        let col = DeltaColumn::<Option<u32>>::from_values(vec![None, None]);
        let mut out = Vec::new();
        assert!(col.save_to_unless(&mut out, None).is_empty());
        // Mixed does not.
        let col = DeltaColumn::<Option<u32>>::from_values(vec![None, Some(2)]);
        assert!(!col.save_to_unless(&mut out, None).is_empty());
        // Empty column always elides.
        let col = DeltaColumn::<u64>::new();
        let mut out = Vec::new();
        assert!(col.save_to_unless(&mut out, 0).is_empty());
    }

    #[test]
    fn delta_u64_write_accepts_domain_max() {
        let col = DeltaColumn::<u64>::from_values(vec![0, i64::MAX as u64]);
        assert_eq!(col.get(1), Some(i64::MAX as u64));
        let bytes = col.save();
        let loaded = DeltaColumn::<u64>::load(&bytes).unwrap();
        assert_eq!(loaded.get(1), Some(i64::MAX as u64));
    }

    #[test]
    fn delta_u64_load_rejects_negative() {
        let col = DeltaColumn::<i64>::from_values(vec![10, -5]);
        let bytes = col.save();
        assert!(
            DeltaColumn::<u64>::load(&bytes).is_err(),
            "DeltaColumn<u64> should reject negative realized values"
        );
    }

    #[test]
    fn delta_opt_u64_load_rejects_negative() {
        let col = DeltaColumn::<Option<i64>>::from_values(vec![Some(3), Some(-1)]);
        let bytes = col.save();
        assert!(DeltaColumn::<Option<u64>>::load(&bytes).is_err());
    }

    #[test]
    fn delta_usize_load_rejects_negative() {
        let col = DeltaColumn::<i64>::from_values(vec![0, -1]);
        let bytes = col.save();
        assert!(DeltaColumn::<usize>::load(&bytes).is_err());
    }

    #[test]
    fn delta_i32_load_rejects_out_of_range() {
        let col = DeltaColumn::<i64>::from_values(vec![i32::MAX as i64 + 1]);
        let bytes = col.save();
        assert!(
            DeltaColumn::<i32>::load(&bytes).is_err(),
            "DeltaColumn<i32> should reject values above i32::MAX"
        );
        let col = DeltaColumn::<i64>::from_values(vec![i32::MIN as i64 - 1]);
        let bytes = col.save();
        assert!(DeltaColumn::<i32>::load(&bytes).is_err());
    }

    #[test]
    fn delta_i32_load_accepts_bounds() {
        let col = DeltaColumn::<i64>::from_values(vec![i32::MIN as i64, i32::MAX as i64]);
        let bytes = col.save();
        let loaded = DeltaColumn::<i32>::load(&bytes).unwrap();
        assert_eq!(loaded.get(0), Some(i32::MIN));
        assert_eq!(loaded.get(1), Some(i32::MAX));
    }

    #[test]
    fn delta_load_rejects_running_sum_overflow() {
        // Hostile bytes: two i64::MAX deltas overflow the running sum.
        // Must be a clean Err in every build profile, never a panic.
        use crate::encoding::EncoderApi;
        let bytes = crate::Encoder::<Option<i64>>::encode([Some(i64::MAX), Some(i64::MAX)]);
        assert!(
            DeltaColumn::<i64>::load(&bytes).is_err(),
            "overflowing running sum should be a load error"
        );
    }

    #[test]
    fn delta_load_rejects_wrapped_within_slab_overflow() {
        // A single run {delta: 2^62, count: 8}: true partials climb to
        // 2^65 (overflow), but *wrapping* math lands back on 0, making
        // the slab aggregate read as a plausible {min: 0, max: 2^62}.
        // Checked accumulation must reject it instead.
        use crate::encoding::EncoderApi;
        let bytes = crate::Encoder::<Option<i64>>::encode(std::iter::repeat_n(Some(1i64 << 62), 8));
        assert!(
            DeltaColumn::<u64>::load(&bytes).is_err(),
            "wrapped within-slab overflow must be a load error"
        );
    }

    #[test]
    fn delta_load_rejects_cross_slab_overflow() {
        // Each slab's partials fit i64, but the running sum across slabs
        // exceeds the domain — only the per-slab aggregate walk sees it.
        use crate::encoding::EncoderApi;
        let bytes = crate::Encoder::<Option<i64>>::encode([
            Some((1i64 << 62) - 1),
            Some(1),
            Some((1i64 << 62) - 1),
            Some(1),
        ]);
        let opts = crate::LoadOpts::new().with_max_segments(2);
        assert!(
            DeltaColumn::<u64>::load_with(&bytes, opts).is_err(),
            "cross-slab running-sum overflow must be a load error"
        );
    }

    #[test]
    fn delta_u64_find_by_value_out_of_domain_is_empty() {
        // Queries for unstorable values are "not found", not a panic —
        // consistent with find_by_range's TryInto guard.
        let col = DeltaColumn::<u64>::from_values(vec![1, 2, 3]);
        assert_eq!(col.find_by_value(u64::MAX).count(), 0);
        assert_eq!(col.find_first(i64::MAX as u64 + 1), None);
        // In-domain queries still work.
        assert_eq!(col.find_first(2), Some(1));
    }

    #[test]
    fn delta_opt_u64_find_by_value_out_of_domain_is_empty() {
        let col = DeltaColumn::<Option<u64>>::from_values(vec![Some(1), None, Some(3)]);
        assert_eq!(col.find_by_value(Some(u64::MAX)).count(), 0);
        assert_eq!(col.find_first(Some(3)), Some(2));
    }

    #[test]
    fn delta_u64_find_by_range_out_of_domain_is_empty() {
        // find_by_range guards via TryInto and returns empty for
        // unrepresentable targets (find_by_value panics instead — the
        // loud variant of the same domain violation).
        let col = DeltaColumn::<u64>::from_values(vec![1, 2, 3]);
        assert_eq!(col.find_by_range(u64::MAX - 1..u64::MAX).count(), 0);
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── seek_to_value ───────────────────────────────────────────────────────

    /// linear reference: the run of `target` within `window`, or the
    /// empty range at the insertion point
    fn seek_reference<T: DeltaValue + Ord>(
        values: &[T],
        target: T,
        window: Range<usize>,
    ) -> Range<usize> {
        let vals = &values[window.clone()];
        let start = window.start + vals.iter().take_while(|v| **v < target).count();
        let count = values[start..window.end]
            .iter()
            .take_while(|v| **v == target)
            .count();
        start..start + count
    }

    #[test]
    fn seek_to_value_basic() {
        let values = vec![1u64, 3, 3, 3, 7, 9];
        let col = DeltaColumn::<u64>::from_values(values.clone());

        let mut iter = col.iter();
        assert_eq!(iter.seek_to_value(3, ..), 1..4);
        // positioned at the run start
        assert_eq!(iter.next(), Some(3));

        // miss: empty range at the insertion point
        let mut iter = col.iter();
        assert_eq!(iter.seek_to_value(5, ..), 4..4);
        assert_eq!(iter.next(), Some(7));

        // before everything / after everything
        assert_eq!(col.iter().seek_to_value(0, ..), 0..0);
        assert_eq!(col.iter().seek_to_value(10, ..), 6..6);
    }

    #[test]
    fn seek_to_value_windowed() {
        let values = vec![1u64, 3, 3, 3, 7, 9];
        let col = DeltaColumn::<u64>::from_values(values.clone());

        // the run is clipped to the window
        assert_eq!(col.iter().seek_to_value(3, 2..5), 2..4);
        // target exists in the column but not in the window
        assert_eq!(col.iter().seek_to_value(3, 4..6), 4..4);
        // sequential seeks over one iterator
        let mut iter = col.iter();
        assert_eq!(iter.seek_to_value(1, ..), 0..1);
        assert_eq!(iter.seek_to_value(7, ..), 4..5);
    }

    #[test]
    fn seek_to_value_nullable() {
        // nulls sort first for Option's Ord
        let values = vec![None, None, Some(2u32), Some(2), Some(5)];
        let col = DeltaColumn::<Option<u32>>::from_values(values.clone());

        assert_eq!(col.iter().seek_to_value(None, ..), 0..2);
        assert_eq!(col.iter().seek_to_value(Some(2), ..), 2..4);
        assert_eq!(col.iter().seek_to_value(Some(3), ..), 4..4);
        assert_eq!(col.iter().seek_to_value(Some(9), ..), 5..5);
    }

    #[test]
    fn scan_to_value_basic() {
        // unsorted: scan finds occurrences in order, consuming each hit
        let values = vec![5u64, 100, 3, 42, 7, 42];
        let col = DeltaColumn::<u64>::from_values(values.clone());

        let mut iter = col.iter();
        assert_eq!(iter.scan_to_value(42), Some(3));
        // the hit is consumed: iteration continues after it
        assert_eq!(iter.pos(), 4);
        assert_eq!(iter.next(), Some(7));
        assert_eq!(iter.scan_to_value(42), Some(5));
        // miss: iterator parks at the end of the window
        assert_eq!(iter.scan_to_value(42), None);
        assert_eq!(iter.next(), None);

        // window-scoped
        let mut iter = col.iter();
        iter.shift(1..4);
        assert_eq!(iter.scan_to_value(42), Some(3));
        let mut iter = col.iter();
        iter.shift(1..3);
        assert_eq!(iter.scan_to_value(42), None);
        assert_eq!(iter.pos(), 3);
    }

    #[test]
    fn scan_to_value_nullable() {
        let values = vec![Some(9u32), None, Some(2), None, Some(9)];
        let col = DeltaColumn::<Option<u32>>::from_values(values.clone());

        let mut iter = col.iter();
        assert_eq!(iter.scan_to_value(None), Some(1));
        assert_eq!(iter.scan_to_value(None), Some(3));
        assert_eq!(iter.scan_to_value(None), None);

        let mut iter = col.iter();
        assert_eq!(iter.scan_to_value(Some(9)), Some(0));
        assert_eq!(iter.scan_to_value(Some(9)), Some(4));
    }

    #[test]
    fn scan_to_value_prunes_to_deep_slab() {
        // a single 42 buried in a large constant column: the scan must
        // hop straight to the one candidate slab
        let mut values = vec![1u64; 20_000];
        values[17_777] = 42;
        let col = DeltaColumn::<u64>::from_values(values.clone());
        let mut iter = col.iter();
        assert_eq!(iter.scan_to_value(42), Some(17_777));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.scan_to_value(42), None);
    }

    #[test]
    fn scan_to_value_fuzz() {
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(2718);
        for _ in 0..32 {
            let len = rng.random_range(1..4000usize);
            let values: Vec<u64> = (0..len).map(|_| rng.random_range(0..200u64)).collect();
            let col = DeltaColumn::<u64>::from_values(values.clone());

            for _ in 0..16 {
                let target = rng.random_range(0..210u64);
                let want: Vec<usize> = values
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| **v == target)
                    .map(|(i, _)| i)
                    .collect();
                let mut got = vec![];
                let mut iter = col.iter();
                while let Some(pos) = iter.scan_to_value(target) {
                    got.push(pos);
                }
                assert_eq!(got, want, "target {target} len {len}");
                assert_eq!(iter.next(), None, "iterator parks at the end");
            }
        }
    }

    #[test]
    fn scan_to_range_basic() {
        let values = vec![50u64, 3, 90, 12, 7, 60, 11];
        let col = DeltaColumn::<u64>::from_values(values.clone());

        // first value inside [10, 20)
        let mut iter = col.iter();
        assert_eq!(iter.scan_to_range(10..20), Some((3, 12)));
        assert_eq!(iter.scan_to_range(10..20), Some((6, 11)));
        assert_eq!(iter.scan_to_range(10..20), None);
        assert_eq!(iter.next(), None);

        // inclusive / unbounded flavors
        assert_eq!(col.iter().scan_to_range(..=3u64), Some((1, 3)));
        assert_eq!(col.iter().scan_to_range(60u64..), Some((2, 90)));
        assert_eq!(col.iter().scan_to_range(..), Some((0, 50)));
        // empty interval
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert_eq!(col.iter().scan_to_range(20u64..10), None);
        }
    }

    #[test]
    fn scan_to_range_fuzz() {
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(1618);
        for _ in 0..32 {
            let len = rng.random_range(1..3000usize);
            let values: Vec<u64> = (0..len).map(|_| rng.random_range(0..300u64)).collect();
            let col = DeltaColumn::<u64>::from_values(values.clone());

            for _ in 0..16 {
                let a = rng.random_range(0..310u64);
                let b = rng.random_range(0..310u64);
                let (lo, hi) = (a.min(b), a.max(b));
                let want: Vec<(usize, u64)> = values
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| (lo..hi).contains(*v))
                    .map(|(i, v)| (i, *v))
                    .collect();
                let mut got = vec![];
                let mut iter = col.iter();
                while let Some(hit) = iter.scan_to_range(lo..hi) {
                    got.push(hit);
                }
                assert_eq!(got, want, "range {lo}..{hi} len {len}");
            }
        }
    }

    #[test]
    fn seek_to_value_insertion_point() {
        // a miss returns the empty range exactly between the two
        // values the target would be inserted between
        let values = vec![10u64, 20, 30, 40];
        let col = DeltaColumn::<u64>::from_values(values.clone());
        for (target, at) in [(5, 0), (15, 1), (25, 2), (35, 3), (45, 4)] {
            let mut iter = col.iter();
            let got = iter.seek_to_value(target, ..);
            assert_eq!(got, at..at, "insertion point for {target}");
            // positioned between the neighbors: next() is the first
            // value greater than the target
            assert_eq!(iter.pos(), at);
            assert_eq!(iter.next(), values.get(at).copied(), "value after {target}");
        }
    }

    #[test]
    fn seek_to_value_respects_max() {
        // a long run of the target straddles the window end: the
        // returned range must clip at the window, and the iterator
        // must yield nothing past it
        let mut values = vec![1u64, 2];
        values.extend(std::iter::repeat_n(5u64, 50));
        values.push(9);
        let col = DeltaColumn::<u64>::from_values(values.clone());

        let mut iter = col.iter();
        let got = iter.seek_to_value(5, 0..10);
        assert_eq!(got, 2..10, "run clipped at the window end");
        // positioned at the run start...
        assert_eq!(iter.pos(), 2);
        // ...and iteration stops at the window end, not the run end
        assert_eq!(iter.by_ref().take(20).count(), 8);
        assert_eq!(iter.next(), None);

        // a miss positions the iterator at the insertion point and
        // next() yields the first value greater than the target
        let mut iter = col.iter();
        let got = iter.seek_to_value(3, 0..10);
        assert_eq!(got, 2..2);
        assert_eq!(iter.pos(), 2);
        assert_eq!(iter.next(), Some(5));

        // a miss past everything in the window yields None
        let mut iter = col.iter();
        let got = iter.seek_to_value(9, 0..10);
        assert_eq!(got, 10..10);
        assert_eq!(iter.pos(), 10);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn seek_to_value_window_sorted_only() {
        // the column is not globally sorted — only the window is. The
        // slab-prefix descent may land early (pre-window prefixes cross
        // the target) but never late, so clamped results stay exact.
        let values = vec![500i64, 400, 1, 2, 2, 3, 7, 9, 600, 0];
        let col = DeltaColumn::<i64>::from_values(values.clone());
        let window = 2..8;

        for target in [0i64, 1, 2, 3, 5, 7, 9, 10] {
            let want = seek_reference(&values, target, window.clone());
            let mut iter = col.iter();
            let got = iter.seek_to_value(target, window.clone());
            assert_eq!(got, want, "target {target}");
            assert_eq!(iter.pos(), want.start);
        }
    }

    #[test]
    fn seek_to_value_fuzz() {
        use rand::RngExt;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(31415);
        for _ in 0..64 {
            // sorted values with heavy duplication, big enough for
            // several slabs
            let len = rng.random_range(1..4000usize);
            let mut values: Vec<u64> = (0..len).map(|_| rng.random_range(0..500u64)).collect();
            values.sort_unstable();
            let col = DeltaColumn::<u64>::from_values(values.clone());

            for _ in 0..32 {
                let a = rng.random_range(0..=len);
                let b = rng.random_range(0..=len);
                let window = a.min(b)..a.max(b);
                let target = rng.random_range(0..510u64);

                let want = seek_reference(&values, target, window.clone());
                let mut iter = col.iter();
                let got = iter.seek_to_value(target, window.clone());
                assert_eq!(got, want, "target {target} in {window:?} of len {len}");
                assert!(
                    got.start >= window.start && got.end <= window.end,
                    "range escapes the window"
                );
                assert_eq!(iter.pos(), want.start, "iterator position");
                if !want.is_empty() {
                    assert_eq!(iter.next(), Some(target), "value at run start");
                }
                // iteration after the seek never leaves the window
                assert!(iter.count() <= window.end.saturating_sub(want.start));
            }

            // same column with unsorted noise around it: only query the
            // still-sorted middle
            let mut noisy: Vec<u64> = (0..rng.random_range(1..50usize))
                .map(|_| rng.random_range(0..1000u64))
                .collect();
            let offset = noisy.len();
            noisy.extend(values.iter().copied());
            noisy.extend((0..rng.random_range(1..50usize)).map(|_| rng.random_range(0..1000u64)));
            let noisy_col = DeltaColumn::<u64>::from_values(noisy.clone());
            for _ in 0..8 {
                let a = offset + rng.random_range(0..=len);
                let b = offset + rng.random_range(0..=len);
                let window = a.min(b)..a.max(b);
                let target = rng.random_range(0..510u64);
                let want = seek_reference(&noisy, target, window.clone());
                let got = noisy_col.iter().seek_to_value(target, window.clone());
                assert_eq!(got, want, "noisy: target {target} in {window:?}");
            }
        }
    }

    fn assert_col<T: DeltaValue + PartialEq + Debug>(col: &DeltaColumn<T>, expected: &[T]) {
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

    // ── DeltaEncoder ────────────────────────────────────────────────────────

    /// Encode `values` via the streaming `DeltaEncoder` and via
    /// `DeltaColumn::from_values` — both must produce identical bytes.
    /// (Byte-format stability itself is frozen by the golden fixtures in
    /// `v1::tests`.)
    fn assert_delta_encoder_match(values: &[Option<i64>]) {
        let v1_col = DeltaColumn::<Option<i64>>::from_values(values.to_vec());
        let v1_col_bytes = v1_col.save();

        let mut enc = DeltaEncoder::<Option<i64>>::new();
        for v in values {
            enc.append(*v);
        }
        let enc_bytes = enc.save();

        assert_eq!(
            enc_bytes, v1_col_bytes,
            "DeltaEncoder vs from_values mismatch for {:?}",
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
    fn delta_encoder_unless_agrees_with_column_for_all_sequences() {
        // Fuzz: DeltaEncoder::encode_to_unless(None) and
        // DeltaColumn::save_to_unless(None) must agree — same elision
        // decision, same bytes — for every nullable input.
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

            let mut enc_out = Vec::new();
            let enc_range = DeltaEncoder::<Option<i64>>::encode_to_unless(
                &mut enc_out,
                values.iter().copied(),
                None,
            );

            let col = DeltaColumn::<Option<i64>>::from_values(values.clone());
            let mut col_out = Vec::new();
            let col_range = col.save_to_unless(&mut col_out, None);

            assert_eq!(
                &enc_out[enc_range], &col_out[col_range],
                "encoder vs column elision mismatch for {:?}",
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
