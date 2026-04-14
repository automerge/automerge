use std::iter::Sum;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Div, Mul, Range, Sub, SubAssign};

use super::column::{find_slab_bit, Column, Iter, Slab, SlabWeight, TailOf, WeightFn};
use super::encoding::{ColumnEncoding, RunDecoder};
use super::{ColumnValueRef, TypedLoadOpts};

// ── UnsignedPrefix marker ────────────────────────────────────────────────────

/// Marker trait for unsigned prefix types.
///
/// `get_index_for_prefix`, `get_index_for_total`, `find_prefix_in_slab`,
/// and `advance_prefix` rely on monotonically increasing prefix sums.
/// Signed prefix types (e.g. `i128`) can decrease, making these operations
/// incorrect. This trait gates those methods at compile time.
pub trait UnsignedPrefix {}
impl UnsignedPrefix for u32 {}
impl UnsignedPrefix for u64 {}
impl UnsignedPrefix for usize {}
impl UnsignedPrefix for u128 {}

// ── PrefixValue trait ────────────────────────────────────────────────────────

/// Trait for column value types that support prefix-sum queries.
///
/// The `Prefix` type is typically one size larger than the value type to
/// avoid overflow when summing many values:
///
/// | Value type       | `Prefix` |
/// |------------------|----------|
/// | `u64`            | `u128`   |
/// | `i64`            | `i128`   |
/// | `bool`           | `u32`    |
/// | `Option<u64>`    | `u128`   |
/// | `Option<i64>`    | `i128`   |
pub trait PrefixValue: ColumnValueRef {
    /// The accumulator type for prefix sums.
    type Prefix: Copy
        + Default
        + Ord
        + std::fmt::Debug
        + Add<Output = Self::Prefix>
        + Sub<Output = Self::Prefix>
        + Mul<Output = Self::Prefix>
        + Div<Output = Self::Prefix>
        + AddAssign
        + SubAssign
        + Sum
        + TryInto<usize>
        + TryFrom<usize>;

    /// Convert one column value to its prefix contribution.
    fn to_prefix(val: Self::Get<'_>) -> Self::Prefix;

    /// Prefix contribution of an entire run.
    #[inline]
    fn run_prefix(run: &super::Run<Self::Get<'_>>) -> Self::Prefix {
        Self::to_prefix(run.value) * Self::Prefix::try_from(run.count).unwrap_or_default()
    }

    /// Sum all values in a slab.  Walks the encoded runs directly for
    /// efficiency — O(segments) rather than O(items).
    fn slab_sum(slab: &Slab<TailOf<Self>>) -> Self::Prefix {
        let mut decoder = Self::Encoding::decoder(&slab.data);
        let mut acc = Self::Prefix::default();
        while let Some(run) = decoder.next_run() {
            acc += Self::run_prefix(&run);
        }
        acc
    }

    /// Compute the partial prefix sum of the first `count` items in a slab,
    /// returning `(prefix_sum, items_consumed)`.
    fn partial_sum(slab: &Slab<TailOf<Self>>, count: usize) -> Self::Prefix {
        let mut decoder = Self::Encoding::decoder(&slab.data);
        let mut acc = Self::Prefix::default();
        let mut items = 0;
        while let Some(mut run) = decoder.next_run() {
            run.count = run.count.min(count - items);
            acc += Self::run_prefix(&run);
            items += run.count;
            if items >= count {
                break;
            }
        }
        acc
    }

    /// Find the first index within a slab where the running sum reaches or
    /// exceeds `target`.  Returns items consumed.
    ///
    /// Only correct for unsigned prefix types where sums are monotonically
    /// increasing.  Callers are gated by `T::Prefix: UnsignedPrefix`.
    fn find_prefix_in_slab(slab: &Slab<TailOf<Self>>, target: Self::Prefix) -> usize {
        let zero = Self::Prefix::default();
        let one_p = Self::Prefix::try_from(1).unwrap_or_default();
        let mut decoder = Self::Encoding::decoder(&slab.data);
        let mut acc = zero;
        let mut items = 0;
        while let Some(run) = decoder.next_run() {
            let run_total = Self::run_prefix(&run);
            if acc + run_total >= target {
                // Target is within this run — ceiling division.
                let p = Self::to_prefix(run.value);
                let remaining = target - acc;
                let needed = (remaining + p - one_p) / p;
                let needed_usize: usize = needed.try_into().unwrap_or(run.count);
                assert!(needed_usize <= run.count);
                items += needed_usize;
                break;
            }
            acc += run_total;
            items += run.count;
        }
        items
    }
}

// ── Compound weight ──────────────────────────────────────────────────────────

/// A BIT node value that carries both item count and prefix sum.
///
/// This allows a single Fenwick tree to support both O(log S) position
/// queries (via the `len` component) and O(log S) prefix-sum queries
/// (via the `prefix` component).
#[derive(Copy, Clone, Default, Debug)]
pub struct PrefixSlabWeight<P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign> {
    pub(crate) len: usize,
    pub(crate) prefix: P,
}

impl<P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign> AddAssign
    for PrefixSlabWeight<P>
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.len += rhs.len;
        self.prefix += rhs.prefix;
    }
}

impl<P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign> SubAssign
    for PrefixSlabWeight<P>
{
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.len -= rhs.len;
        self.prefix -= rhs.prefix;
    }
}

impl<P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign> SlabWeight
    for PrefixSlabWeight<P>
{
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}

/// Weight function for prefix-sum tracking.
///
/// Computes compound weights `(length, prefix_sum)` so that a single BIT
/// supports both position queries and prefix-sum queries.
#[derive(Clone)]
pub struct PrefixWeightFn<T>(PhantomData<fn() -> T>);

impl<T: PrefixValue> WeightFn<T> for PrefixWeightFn<T> {
    type Weight = PrefixSlabWeight<T::Prefix>;

    #[inline]
    fn compute(slab: &Slab<TailOf<T>>) -> PrefixSlabWeight<T::Prefix> {
        PrefixSlabWeight {
            len: slab.len,
            prefix: T::slab_sum(slab),
        }
    }
}

// ── PrefixColumn ─────────────────────────────────────────────────────────────

/// A column with O(log S) prefix-sum queries backed by a compound Fenwick tree.
///
/// Wraps a `Column<T, PrefixWeightFn<T>>` whose single BIT stores both
/// item counts and prefix sums, eliminating the need for a separate prefix BIT.
#[derive(Clone)]
pub struct PrefixColumn<T: PrefixValue> {
    col: Column<T, PrefixWeightFn<T>>,
}

impl<T: PrefixValue> std::fmt::Debug for PrefixColumn<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixColumn")
            .field("len", &self.col.len())
            .field("slabs", &self.col.slab_count())
            .finish()
    }
}

impl<T: PrefixValue> Default for PrefixColumn<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PrefixValue> PrefixColumn<T> {
    /// Create an empty prefix column with the default segment budget.
    pub fn new() -> Self {
        Self { col: Column::new() }
    }

    /// Create an empty prefix column with a custom segment budget per slab.
    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            col: Column::with_max_segments(max_segments),
        }
    }

    /// Bulk-construct from a Vec of values.
    pub fn from_values(values: Vec<T>) -> Self {
        Self {
            col: Column::from_values(values),
        }
    }

    /// Deserialize a column from bytes produced by [`save`](PrefixColumn::save).
    pub fn load(data: &[u8]) -> Result<Self, crate::PackError> {
        let col = Column::<T>::load(data)?;
        Ok(Self::from_column(col))
    }

    /// Wrap an existing [`Column`] with prefix-sum tracking.
    ///
    /// Converts the plain length BIT into a compound (length, prefix) BIT.
    pub fn from_column(col: Column<T>) -> Self {
        Self {
            col: Column::from_slabs(col.slabs, col.total_len, col.max_segments),
        }
    }

    // ── Delegated read methods ───────────────────────────────────────────

    /// Total number of items in the column.
    pub fn len(&self) -> usize {
        self.col.len()
    }

    /// Returns `true` if the column contains no items.
    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    /// Returns `(prefix, value)` at `index`, where `prefix` is the inclusive
    /// sum of items `0..=index`.
    pub fn get(&self, index: usize) -> Option<(T::Prefix, T::Get<'_>)> {
        self.iter().nth(index)
    }

    /// Returns just the value at `index` (no prefix sum).
    pub fn get_value(&self, index: usize) -> Option<T::Get<'_>> {
        self.col.get(index)
    }

    /// Serialize the column into a byte array.
    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    /// Serialize the column by appending bytes to `out`.
    ///
    /// Returns the byte range written (`out[range]` is the serialized data).
    pub fn save_to(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.col.save_to(out)
    }

    /// Number of slabs in the column.
    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    /// Returns `(len, segments)` for each slab (for debugging/testing).
    pub fn slab_info(&self) -> Vec<(usize, usize)> {
        self.col.slab_info()
    }

    /// Validate that the canonical encoding is well-formed.
    ///
    /// Returns `Ok(())` if the encoding is valid, or a [`PackError`](crate::PackError)
    /// describing the violation.
    pub fn validate_encoding(&self) -> Result<(), crate::PackError> {
        self.col.validate_encoding()
    }

    // ── Mutations (compound BIT maintained automatically) ────────────────

    /// Inserts `value` at `index`, shifting subsequent elements right.
    ///
    /// # Panics
    ///
    /// Panics if `index > self.len()`.
    pub fn insert(&mut self, index: usize, value: impl super::AsColumnRef<T>) {
        self.col.insert(index, value);
    }

    /// Removes the value at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len()`.
    pub fn remove(&mut self, index: usize) {
        self.col.remove(index);
    }

    /// Appends `value` to the end of the column.
    pub fn push(&mut self, value: impl super::AsColumnRef<T>) {
        self.col.push(value);
    }

    /// Removes and returns the last element, or `None` if empty.
    pub fn pop(&mut self) -> Option<T> {
        self.col.pop()
    }

    /// Returns the first value, or `None` if empty.
    pub fn first_value(&self) -> Option<T::Get<'_>> {
        self.col.first()
    }

    /// Returns the last value, or `None` if empty.
    pub fn last_value(&self) -> Option<T::Get<'_>> {
        self.col.last()
    }

    /// Removes all elements from the column.
    pub fn clear(&mut self) {
        self.col.clear();
    }

    /// Shortens the column to `len` elements.
    ///
    /// If `len >= self.len()`, this is a no-op.
    pub fn truncate(&mut self, len: usize) {
        self.col.truncate(len);
    }

    /// Removes `del` elements starting at `index` and inserts `values` in their place.
    ///
    /// # Panics
    ///
    /// Panics if `index + del > self.len()`.
    pub fn splice<V: super::AsColumnRef<T>>(
        &mut self,
        index: usize,
        del: usize,
        values: impl IntoIterator<Item = V>,
    ) {
        self.col.splice(index, del, values);
    }

    // ── Prefix-sum queries ───────────────────────────────────────────────

    /// Returns the inclusive sum of values at indices `0..=index` (through
    /// `index`).
    ///
    /// `get_total(0)` returns the value at index 0.
    /// If `index >= len`, returns the sum of all values.
    pub fn get_total(&self, index: usize) -> T::Prefix {
        self.get_prefix(index + 1)
    }

    /// Returns the exclusive sum of values at indices `0..index` (before `index`).
    ///
    /// `get_prefix(0)` returns `Default::default()` (zero).
    /// `get_prefix(len)` returns the sum of all values.
    pub fn get_prefix(&self, index: usize) -> T::Prefix {
        // TODO : optimize with slab_prefix_remaining to avoid a `find_slab_bit` when possible
        if index == 0 || self.col.is_empty() {
            return T::Prefix::default();
        }
        let index = index.min(self.col.len());

        // Use the compound BIT to find which slab contains item (index-1).
        let (si, off) = find_slab_bit(&self.col.bit, index - 1, self.col.slabs.len());
        let si = si.min(self.col.slabs.len() - 1);

        // `off` is the offset of item (index-1) within slab `si`.
        // We need the sum of the first (off+1) items in this slab.
        let items_in_slab = off + 1;

        let prefix_before = if si > 0 {
            self.prefix_query(si - 1)
        } else {
            T::Prefix::default()
        };
        let partial = T::partial_sum(&self.col.slabs[si], items_in_slab);
        prefix_before + partial
    }

    /// Compute the prefix sum delta across `range.start..range.end`.
    ///
    /// Equivalent to `get_prefix(range.end) - get_prefix(range.start)`, but
    /// when both endpoints fall in the same slab the slab is decoded only once.
    pub fn prefix_delta(&self, range: std::ops::Range<usize>) -> T::Prefix {
        if range.start >= range.end || self.col.is_empty() {
            return T::Prefix::default();
        }
        if range.start == 0 {
            return self.get_prefix(range.end);
        }

        let end = range.end.min(self.col.len());
        let start = range.start.min(end);
        if start == end {
            return T::Prefix::default();
        }

        let (si_start, off_start, prefix_before_start) = self.find_slab_with_prefix(start - 1);
        let (si_end, off_end, prefix_before_end) = self.find_slab_with_prefix(end - 1);

        if si_start == si_end {
            // Same slab — decode once, compute difference of partial sums.
            let slab = &self.col.slabs[si_start];
            let sum_end = T::partial_sum(slab, off_end + 1);
            let sum_start = T::partial_sum(slab, off_start + 1);
            sum_end - sum_start
        } else {
            // Different slabs — fall back to two independent queries.
            let p_start =
                prefix_before_start + T::partial_sum(&self.col.slabs[si_start], off_start + 1);
            let p_end = prefix_before_end + T::partial_sum(&self.col.slabs[si_end], off_end + 1);
            p_end - p_start
        }
    }

    // ── Internal BIT queries ─────────────────────────────────────────────

    /// Combined find_slab + prefix accumulation in one BIT traversal.
    /// Returns `(slab_index, offset_within_slab, prefix_sum_of_slabs_before_this_one)`.
    fn find_slab_with_prefix(&self, index: usize) -> (usize, usize, T::Prefix) {
        let n = self.col.slabs.len();
        if n == 0 {
            return (0, 0, T::Prefix::default());
        }
        let mut pos = 0usize;
        let mut prefix = T::Prefix::default();
        let mut idx = 0usize;
        let mut bit_k = 1;
        while bit_k <= n {
            bit_k <<= 1;
        }
        bit_k >>= 1;
        while bit_k > 0 {
            let next = idx + bit_k;
            if next <= n && pos + self.col.bit[next].len <= index {
                pos += self.col.bit[next].len;
                prefix += self.col.bit[next].prefix;
                idx = next;
            }
            bit_k >>= 1;
        }
        (idx, index - pos, prefix)
    }

    /// Query prefix sum of slabs 0..=i (0-indexed slab index). O(log S).
    fn prefix_query(&self, mut i: usize) -> T::Prefix {
        let mut sum = T::Prefix::default();
        i += 1; // to 1-indexed
        while i > 0 {
            sum += self.col.bit[i].prefix;
            i -= i & i.wrapping_neg();
        }
        sum
    }

    /// Query total item count of slabs 0..=i (0-indexed slab index). O(log S).
    fn len_query(&self, mut i: usize) -> usize {
        let mut sum = 0usize;
        i += 1;
        while i > 0 {
            sum += self.col.bit[i].len;
            i -= i & i.wrapping_neg();
        }
        sum
    }

    /// Access the inner `Column` for value-only iteration.
    ///
    /// Use `prefix_col.value_iter()` when you don't need prefix sums.
    pub fn values(&self) -> &Column<T, PrefixWeightFn<T>> {
        &self.col
    }

    /// Returns a value-only iterator (no prefix sums).
    pub fn value_iter(&self) -> Iter<'_, T> {
        self.col.iter()
    }

    /// Returns a value-only iterator over the given range (no prefix sums).
    pub fn value_iter_range(&self, range: Range<usize>) -> Iter<'_, T> {
        self.col.iter_range(range)
    }

    /// Returns an iterator that yields `(total, value)` for each item.
    ///
    /// `total` is the inclusive cumulative sum *through* the current item
    /// (i.e. `sum(0..=index)`).  Uses the Fenwick tree for the initial
    /// slab prefix, then accumulates within each slab in O(1) per item.
    pub fn iter(&self) -> PrefixIter<'_, T> {
        PrefixIter {
            col: Some(self),
            inner: self.col.iter(),
            total: T::Prefix::default(),
            base: T::Prefix::default(),
        }
    }

    /// Returns an iterator over `range` that yields `(total, value)`.
    ///
    /// `total` is the inclusive sum through the current item.  For the first
    /// item at `range.start`, this equals `get_prefix(range.start) + value`.
    pub fn iter_range(&self, range: Range<usize>) -> PrefixIter<'_, T> {
        let start = range.start.min(self.col.total_len);
        let end = range.end.min(self.col.total_len);
        let prefix_before = self.get_prefix(start);
        PrefixIter {
            col: Some(self),
            inner: self.col.iter_range(start..end),
            total: prefix_before,
            base: prefix_before,
        }
    }

    /// Collect all values into a Vec (without prefix sums).
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.col.to_vec()
    }
}

impl<T: PrefixValue> PrefixColumn<T>
where
    T::Prefix: UnsignedPrefix,
{
    /// Seek forward from `start`, advancing past `n` prefix units.
    ///
    /// Shorthand for `self.iter_range(start..).advance_prefix(n)`.
    pub fn seek(&self, start: usize, n: T::Prefix) -> Option<PrefixSeek<T::Prefix, T::Get<'_>>> {
        self.iter_range(start..self.len()).advance_prefix(n)
    }

    /// Get the value and prefix delta at `pos` relative to `start`.
    ///
    /// Shorthand for `self.iter_range(start..).advance_to(pos)`.
    pub fn get_delta(&self, start: usize, pos: usize) -> Option<PrefixSeek<T::Prefix, T::Get<'_>>> {
        self.iter_range(start..self.len()).advance_to(pos)
    }
}

// ── Unsigned-prefix-only methods ─────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T>
where
    T::Prefix: UnsignedPrefix,
{
    /// Find the first index where the inclusive total (sum through that item)
    /// reaches or exceeds `target`.
    ///
    /// Returns `self.len()` if the total sum is less than `target`.
    pub fn get_index_for_total(&self, target: T::Prefix) -> usize {
        let idx = self.get_index_for_prefix(target);
        // get_index_for_prefix finds where the *exclusive* prefix reaches target,
        // which is one past the item whose inclusive total first reaches it.
        if idx > 0 {
            idx - 1
        } else {
            0
        }
    }

    /// Find the first index where the prefix sum reaches or exceeds `target`.
    ///
    /// Returns `self.len() + 1` if the total sum is less than `target`.
    pub fn get_index_for_prefix(&self, target: T::Prefix) -> usize {
        if target <= T::Prefix::default() {
            return 0;
        }
        if self.col.is_empty() {
            return 0;
        }

        // Binary lifting on the prefix component of the compound BIT.
        let (si, prefix_before) = self.find_slab_by_prefix(target);

        if si >= self.col.slabs.len() {
            return self.col.len() + 1;
        }

        let remaining = target - prefix_before;
        let slab = &self.col.slabs[si];
        let idx_in_slab = T::find_prefix_in_slab(slab, remaining);

        // Use the compound BIT to count items before this slab in O(log S).
        let items_before = if si > 0 { self.len_query(si - 1) } else { 0 };
        items_before + idx_in_slab
    }

    /// Binary lifting on the prefix component of the compound BIT.
    /// Returns `(slab_index, prefix_before_that_slab)`.
    fn find_slab_by_prefix(&self, target: T::Prefix) -> (usize, T::Prefix) {
        let n = self.col.slabs.len();
        if n == 0 {
            return (0, T::Prefix::default());
        }
        let mut pos = T::Prefix::default();
        let mut idx = 0usize;
        let mut bit_k = 1;
        while bit_k <= n {
            bit_k <<= 1;
        }
        bit_k >>= 1;
        while bit_k > 0 {
            let next = idx + bit_k;
            if next <= n && pos + self.col.bit[next].prefix < target {
                pos += self.col.bit[next].prefix;
                idx = next;
            }
            bit_k >>= 1;
        }
        (idx, pos)
    }
}

// ── PrefixValue impls using decoders ─────────────────────────────────────────

impl PrefixValue for u64 {
    type Prefix = u128;
    fn to_prefix(val: u64) -> u128 {
        val as u128
    }
}

impl PrefixValue for Option<u64> {
    type Prefix = u128;
    fn to_prefix(val: Option<u64>) -> u128 {
        val.unwrap_or(0) as u128
    }
}

impl PrefixValue for i64 {
    type Prefix = i128;
    fn to_prefix(val: i64) -> i128 {
        val as i128
    }
}

impl PrefixValue for Option<i64> {
    type Prefix = i128;
    fn to_prefix(val: Option<i64>) -> i128 {
        val.unwrap_or(0) as i128
    }
}

impl PrefixValue for u32 {
    type Prefix = u64;
    fn to_prefix(val: u32) -> u64 {
        val as u64
    }
}

impl PrefixValue for Option<u32> {
    type Prefix = u64;
    fn to_prefix(val: Option<u32>) -> u64 {
        val.unwrap_or(0) as u64
    }
}

impl PrefixValue for std::num::NonZeroU32 {
    type Prefix = u64;
    fn to_prefix(val: std::num::NonZeroU32) -> u64 {
        val.get() as u64
    }
}

impl PrefixValue for Option<std::num::NonZeroU32> {
    type Prefix = u64;
    fn to_prefix(val: Option<std::num::NonZeroU32>) -> u64 {
        val.map_or(0, |v| v.get() as u64)
    }
}

// ── bool impl ────────────────────────────────────────────────────────────────

impl PrefixValue for bool {
    type Prefix = usize;

    fn to_prefix(val: bool) -> usize {
        val as usize
    }
}

// ── Load / save with options ────────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T> {
    /// Deserialize with options. See [`LoadOpts`](super::LoadOpts).
    pub fn load_with(data: &[u8], opts: TypedLoadOpts<T>) -> Result<Self, crate::PackError> {
        let col = Column::<T, PrefixWeightFn<T>>::load_with(data, opts)?;
        Ok(Self { col })
    }

    /// Serialize unless all values equal `value`.
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: T::Get<'_>) -> std::ops::Range<usize> {
        self.col.save_to_unless(out, value)
    }
}

// ── PrefixSeek ──────────────────────────────────────────────────────────────

/// Result of a seek operation on a [`PrefixIter`].
///
/// Returned by [`PrefixIter::advance_prefix`] and [`PrefixIter::advance_to`].
/// After the call the iterator is positioned at `pos + 1`, ready to yield
/// subsequent items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefixSeek<P, V> {
    /// Position of the item.
    pub pos: usize,
    /// Inclusive prefix sum through this item (absolute).
    pub prefix: P,
    /// Prefix sum consumed since the iterator's range start.
    pub prefix_delta: P,
    /// The value at this position.
    pub value: V,
}

// ── PrefixIter ───────────────────────────────────────────────────────────────

/// Forward iterator over a [`PrefixColumn`] that yields `(prefix_sum, value)`.
///
/// Created by [`PrefixColumn::iter`] or [`PrefixColumn::iter_range`].
///
/// - `next()` is O(1): accumulates the total from the yielded value.
/// - `nth(n)` is O(log S + runs): skips slabs via the inner iterator, then
///   recomputes the total from the Fenwick tree.
/// - [`advance_prefix`](PrefixIter::advance_prefix) advances by prefix-sum
///   value instead of item count, using O(log S) BIT binary lifting.
/// - [`advance_to`](PrefixIter::advance_to) jumps to a specific position
///   and returns the value and prefix delta.
///
/// Each yielded item is `(total, value)` where `total` is the inclusive
/// sum of all values through the current item (`0..=pos`).
pub struct PrefixIter<'a, T: PrefixValue> {
    col: Option<&'a PrefixColumn<T>>,
    inner: Iter<'a, T>,
    total: T::Prefix,
    base: T::Prefix,
}

impl<T: PrefixValue> Default for PrefixIter<'_, T> {
    fn default() -> Self {
        Self {
            col: None,
            inner: Iter::default(),
            total: T::Prefix::default(),
            base: T::Prefix::default(),
        }
    }
}

impl<T: PrefixValue> Clone for PrefixIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            col: self.col,
            inner: self.inner.clone(),
            total: self.total,
            base: self.base,
        }
    }
}

impl<T: PrefixValue> std::fmt::Debug for PrefixIter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixIter")
            .field("total", &self.total)
            .field("pos", &self.inner.pos)
            .finish()
    }
}

impl<'a, T: PrefixValue> Iterator for PrefixIter<'a, T> {
    type Item = (T::Prefix, T::Get<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let val = self.inner.next()?;
        self.total += T::to_prefix(val);
        Some((self.total, val))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.inner.items_left {
            self.inner.pos += self.inner.items_left;
            self.inner.items_left = 0;
            return None;
        }

        // Fast path: target is within the current slab.
        if n < self.inner.slab_remaining {
            let saved_items_left = self.inner.items_left;
            self.inner.items_left = n + 1;

            let mut val = None;
            while let Some(run) = self.inner.next_run() {
                self.total += T::run_prefix(&run);
                val = Some(run.value);
            }

            self.inner.items_left = saved_items_left - (n + 1);
            return val.map(|v| (self.total, v));
        }

        // Combined BIT traversal: find slab + accumulate prefix in one pass.
        let col = self.col.expect("nth on default PrefixIter");
        let target_pos = self.inner.pos + n;
        let (si, offset, prefix_before) = col.find_slab_with_prefix(target_pos);
        if si >= self.inner.slabs.len() {
            self.inner.pos += self.inner.items_left;
            self.inner.items_left = 0;
            return None;
        }

        let slab = &self.inner.slabs[si];
        let mut decoder = T::Encoding::decoder(&slab.data);
        let val = decoder.nth(offset)?;
        let partial = T::partial_sum(slab, offset + 1);

        let skipped = n + 1;
        self.inner.slab_idx = si;
        self.inner.decoder = decoder;
        self.inner.items_left -= skipped;
        self.inner.slab_remaining = slab.len - offset - 1;
        self.inner.pos = target_pos + 1;
        self.total = prefix_before + partial;

        Some((self.total, val))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn last(mut self) -> Option<Self::Item> {
        let n = self.inner.items_left;
        if n == 0 {
            return None;
        }
        self.nth(n - 1)
    }

    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        while let Some(run) = self.next_run() {
            for _ in 0..run.count {
                acc = f(acc, run.value);
            }
        }
        acc
    }
}

impl<T: PrefixValue> ExactSizeIterator for PrefixIter<'_, T> {}

impl<'a, T: PrefixValue> PrefixIter<'a, T> {
    /// Index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.inner.pos
    }

    /// Number of items remaining.
    #[inline]
    pub fn items_left(&self) -> usize {
        self.inner.items_left
    }

    /// One past the last item this iterator will yield.
    #[inline]
    pub fn end(&self) -> usize {
        self.inner.pos + self.inner.items_left
    }

    /// Set the upper bound so that this iterator yields items up to `pos`.
    pub fn set_max(&mut self, pos: usize) {
        self.inner.set_max(pos);
    }

    /// Returns the next run of identical values, along with the inclusive
    /// total at the *end* of the run.
    ///
    /// See [`super::Run`] for run semantics.
    pub fn next_run(&mut self) -> Option<super::Run<(T::Prefix, T::Get<'a>)>> {
        let run = self.inner.next_run()?;
        self.total += T::run_prefix(&run);
        Some(super::Run {
            count: run.count,
            value: (self.total, run.value),
        })
    }

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// After this call the iterator will yield items from `range.start + 1`
    /// through `range.end - 1`.
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: std::ops::Range<usize>) -> Option<(T::Prefix, T::Get<'a>)> {
        if range.is_empty() {
            return None;
        }
        let col = self.col.expect("shift_next on default PrefixIter");
        let pos = self.inner.pos;
        assert!(
            range.start >= pos,
            "shift_next: range.start ({}) < pos ({})",
            range.start,
            pos,
        );
        // Reset the prefix total to the exclusive sum before range.start.
        let prefix_before = col.get_prefix(range.start);
        self.total = prefix_before;
        self.base = prefix_before;
        // Create a fresh inner iterator for the range.
        self.inner = col.values().iter_range(range.clone());
        // Consume the first item.
        let val = self.inner.next()?;
        self.total += T::to_prefix(val);
        Some((self.total, val))
    }
}

impl<'a, T: PrefixValue> PrefixIter<'a, T> {
    /// Advance the iterator to a specific position and return the item there.
    ///
    /// Returns a [`PrefixSeek`] with the value, absolute prefix, and the
    /// prefix delta since the iterator's range start.  After this call the
    /// iterator is positioned at `pos + 1`, ready for further iteration.
    ///
    /// Returns `None` if `pos` is before the current position or past the end.
    pub fn advance_to(&mut self, pos: usize) -> Option<PrefixSeek<T::Prefix, T::Get<'a>>> {
        if pos < self.inner.pos || self.inner.items_left == 0 {
            return None;
        }
        let skip = pos - self.inner.pos;
        if skip >= self.inner.items_left {
            return None;
        }
        let (total, value) = if skip == 0 {
            self.next()?
        } else {
            self.nth(skip)?
        };
        // total is inclusive (includes this item's value).
        // prefix_delta is the exclusive sum from base to just before this item.
        let exclusive = total - T::to_prefix(value);
        Some(PrefixSeek {
            pos,
            prefix: total,
            prefix_delta: exclusive - self.base,
            value,
        })
    }
}

impl<'a, T: PrefixValue> PrefixIter<'a, T>
where
    T::Prefix: UnsignedPrefix,
{
    /// Advance the iterator past `n` prefix units and return the item landed on.
    ///
    /// Skips items whose cumulative value sums to `n` (from the current
    /// position), then returns the next item.  If a single item's value
    /// exceeds the remaining budget, that item is returned (not skipped).
    ///
    /// Returns a [`PrefixSeek`] with the value, absolute prefix, and the
    /// prefix delta since the iterator's range start.  After this call the
    /// iterator is positioned at the returned item's `pos + 1`.
    ///
    /// Returns `None` (and exhausts the iterator) if the remaining items
    /// cannot produce enough sum.
    pub fn advance_prefix(&mut self, n: T::Prefix) -> Option<PrefixSeek<T::Prefix, T::Get<'a>>> {
        let col = self.col.expect("advance_prefix on default PrefixIter");
        let target = self.total + n;

        // The +1 makes us advance PAST items summing to n:
        // get_index_for_total(target) returns the item whose inclusive total
        // first reaches target. Adding 1 shifts to the next item — the first
        // one not consumed by the budget.  When a single item overshoots
        // (its value > remaining budget), the +1 still lands on that item
        // because no prior item reached target+1.
        let one = T::Prefix::try_from(1).unwrap_or_default();
        let seek_target = target + one;
        let target_pos = col.get_index_for_total(seek_target);

        if target_pos < self.inner.pos || target_pos >= self.inner.pos + self.inner.items_left {
            // Target unreachable — exhaust the iterator.
            let remaining = self.inner.len();
            if remaining > 0 {
                let _ = self.inner.nth(remaining - 1);
            }
            self.total = col.get_prefix(self.inner.pos);
            return None;
        }

        self.advance_to(target_pos)
    }
}

// ── FromIterator ────────────────────────────────────────────────────────────

impl<T: PrefixValue> FromIterator<T> for PrefixColumn<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<T: PrefixValue> Extend<T> for PrefixColumn<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.col.extend(iter);
    }
}

impl<'a, T: PrefixValue> IntoIterator for &'a PrefixColumn<T> {
    type Item = (T::Prefix, T::Get<'a>);
    type IntoIter = PrefixIter<'a, T>;

    fn into_iter(self) -> PrefixIter<'a, T> {
        self.iter()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_empty() {
        let col = PrefixColumn::<u64>::new();
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_index_for_prefix(0), 0);
        assert_eq!(col.get_index_for_prefix(1), 0);
    }

    #[test]
    fn prefix_single() {
        let mut col = PrefixColumn::<u64>::new();
        col.insert(0, 5);
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 5);
        assert_eq!(col.get_index_for_prefix(5), 1);
        assert_eq!(col.get_index_for_prefix(3), 1);
        assert_eq!(col.get_index_for_prefix(6), 2);
    }

    #[test]
    fn prefix_sequential() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // prefix(0) = 0
        // prefix(1) = 1
        // prefix(2) = 3
        // prefix(3) = 6
        // prefix(4) = 10
        // prefix(5) = 15
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 3);
        assert_eq!(col.get_prefix(3), 6);
        assert_eq!(col.get_prefix(4), 10);
        assert_eq!(col.get_prefix(5), 15);
    }

    #[test]
    fn prefix_with_repeats() {
        let col = PrefixColumn::<u64>::from_values(vec![3, 3, 3, 3]);
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 3);
        assert_eq!(col.get_prefix(2), 6);
        assert_eq!(col.get_prefix(3), 9);
        assert_eq!(col.get_prefix(4), 12);
    }

    #[test]
    fn index_for_prefix_sequential() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // Values: [1, 2, 3, 4, 5]
        // Prefix: [0, 1, 3, 6, 10, 15]
        assert_eq!(col.get_index_for_prefix(0), 0);
        assert_eq!(col.get_index_for_prefix(1), 1); // prefix reaches 1 after index 0
        assert_eq!(col.get_index_for_prefix(2), 2); // prefix reaches 3 >= 2 after index 1
        assert_eq!(col.get_index_for_prefix(3), 2); // prefix reaches 3 after index 1
        assert_eq!(col.get_index_for_prefix(6), 3); // prefix reaches 6 after index 2
        assert_eq!(col.get_index_for_prefix(15), 5); // prefix reaches 15 after index 4
        assert_eq!(col.get_index_for_prefix(16), 6); // beyond total
    }

    #[test]
    fn prefix_after_insert() {
        let mut col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        col.insert(1, 10);
        // Values: [1, 10, 2, 3]
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 11);
        assert_eq!(col.get_prefix(3), 13);
        assert_eq!(col.get_prefix(4), 16);
    }

    #[test]
    fn prefix_after_remove() {
        let mut col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4]);
        col.remove(1);
        // Values: [1, 3, 4]
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 4);
        assert_eq!(col.get_prefix(3), 8);
    }

    #[test]
    fn prefix_bool() {
        let col = PrefixColumn::<bool>::from_values(vec![true, false, true, true, false]);
        // prefix(0) = 0
        // prefix(1) = 1  (true)
        // prefix(2) = 1  (false)
        // prefix(3) = 2  (true)
        // prefix(4) = 3  (true)
        // prefix(5) = 3  (false)
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 1);
        assert_eq!(col.get_prefix(3), 2);
        assert_eq!(col.get_prefix(4), 3);
        assert_eq!(col.get_prefix(5), 3);
    }

    #[test]
    fn index_for_prefix_bool() {
        let col = PrefixColumn::<bool>::from_values(vec![true, false, true, false, true]);
        // We want: first index where count_of_trues >= target
        assert_eq!(col.get_index_for_prefix(0), 0);
        assert_eq!(col.get_index_for_prefix(1), 1); // 1 true after index 0
        assert_eq!(col.get_index_for_prefix(2), 3); // 2 trues after index 2
        assert_eq!(col.get_index_for_prefix(3), 5); // 3 trues after index 3
        assert_eq!(col.get_index_for_prefix(4), 6); // only 3 trues total
    }

    #[test]
    fn prefix_nullable() {
        let col = PrefixColumn::<Option<u64>>::from_values(vec![Some(1), None, Some(3), Some(4)]);
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 1); // None contributes 0
        assert_eq!(col.get_prefix(3), 4);
        assert_eq!(col.get_prefix(4), 8);
    }

    #[test]
    fn prefix_i64() {
        let col = PrefixColumn::<i64>::from_values(vec![1, -2, 3, -4, 5]);
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), -1);
        assert_eq!(col.get_prefix(3), 2);
        assert_eq!(col.get_prefix(4), -2);
        assert_eq!(col.get_prefix(5), 3);
    }

    #[test]
    fn prefix_multi_slab() {
        // Force multiple slabs with a small segment budget.
        let mut col = PrefixColumn::<u64>::with_max_segments(4);
        for i in 0..20 {
            col.insert(i, (i + 1) as u64);
        }
        assert!(col.slab_count() > 1, "should have multiple slabs");
        // Check all prefixes.
        let mut expected_prefix = 0u128;
        for i in 0..=20 {
            assert_eq!(col.get_prefix(i), expected_prefix, "prefix mismatch at {i}");
            if i < 20 {
                expected_prefix += (i + 1) as u128;
            }
        }
    }

    #[test]
    fn index_for_prefix_multi_slab() {
        let mut col = PrefixColumn::<u64>::with_max_segments(4);
        let values: Vec<u64> = (1..=20).collect();
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        // Prefix sums: 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55, ...
        // The index for prefix 10 should be 4 (sum of [1,2,3,4] = 10)
        assert_eq!(col.get_index_for_prefix(10), 4);
        assert_eq!(col.get_index_for_prefix(15), 5);
        assert_eq!(col.get_index_for_prefix(11), 5); // 15 >= 11 at index 5
    }

    #[test]
    fn prefix_splice() {
        let mut col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        col.splice(1, 2, [10, 20]);
        // Values: [1, 10, 20, 4, 5]
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 11);
        assert_eq!(col.get_prefix(3), 31);
        assert_eq!(col.get_prefix(4), 35);
        assert_eq!(col.get_prefix(5), 40);
    }

    // ── PrefixIter tests ────────────────────────────────────────────────

    #[test]
    fn prefix_iter_empty() {
        let col = PrefixColumn::<u64>::new();
        let items: Vec<_> = col.iter().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn prefix_iter_basic() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let items: Vec<_> = col.iter().collect();
        // prefix(through item) = cumulative sum including this item
        assert_eq!(items, vec![(1, 1), (3, 2), (6, 3), (10, 4), (15, 5),]);
    }

    #[test]
    fn prefix_iter_bool() {
        let col = PrefixColumn::<bool>::from_values(vec![true, false, true, true, false]);
        let items: Vec<_> = col.iter().collect();
        assert_eq!(
            items,
            vec![(1, true), (1, false), (2, true), (3, true), (3, false),]
        );
    }

    #[test]
    fn prefix_iter_nullable() {
        let col = PrefixColumn::<Option<u64>>::from_values(vec![Some(1), None, Some(3), Some(4)]);
        let items: Vec<_> = col.iter().collect();
        assert_eq!(
            items,
            vec![(1, Some(1)), (1, None), (4, Some(3)), (8, Some(4)),]
        );
    }

    #[test]
    fn prefix_iter_range() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // iter_range(2..4) yields items at indices 2,3 with prefix starting at get_prefix(2)=3
        let items: Vec<_> = col.iter_range(2..4).collect();
        assert_eq!(
            items,
            vec![
                (6, 3),  // prefix through index 2
                (10, 4), // prefix through index 3
            ]
        );
    }

    #[test]
    fn prefix_iter_exact_size() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let iter = col.iter();
        assert_eq!(iter.len(), 5);
        let iter = col.iter_range(1..3);
        assert_eq!(iter.len(), 2);
    }

    #[test]
    fn prefix_iter_inner_access() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // value_iter() gives value-only iteration
        let values: Vec<_> = col.value_iter().collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn prefix_iter_nth() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let mut iter = col.iter();
        assert_eq!(iter.next(), Some((1, 1)));
        // nth(1) = skip 1, return next = skip index 1, return index 2
        assert_eq!(iter.nth(1), Some((6, 3)));
        assert_eq!(iter.next(), Some((10, 4)));
        assert_eq!(iter.next(), Some((15, 5)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn prefix_iter_nth_multi_slab() {
        let mut col = PrefixColumn::<u64>::with_max_segments(4);
        for i in 0..20 {
            col.insert(i, (i + 1) as u64);
        }
        assert!(col.slab_count() > 1);
        let mut iter = col.iter();
        // Skip to index 10 (value 11)
        let (prefix, val) = iter.nth(10).unwrap();
        assert_eq!(val, 11);
        // prefix through index 10 = sum(1..=11) = 66
        assert_eq!(prefix, 66);
        // Skip to index 19 (value 20)
        let (prefix, val) = iter.nth(8).unwrap();
        assert_eq!(val, 20);
        // prefix through index 19 = sum(1..=20) = 210
        assert_eq!(prefix, 210);
    }

    #[test]
    fn prefix_iter_nth_past_end() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        let mut iter = col.iter();
        assert_eq!(iter.nth(5), None);
        assert_eq!(iter.len(), 0);
    }

    // ── advance_to tests ─────────────────────────────────────────────

    #[test]
    fn advance_to_single_slab() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let mut iter = col.iter();
        let tx = iter.advance_to(2).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 3);
        assert_eq!(tx.prefix, 6); // inclusive: 1+2+3
        assert_eq!(tx.prefix_delta, 3); // exclusive before pos 2: 1+2
                                        // iterator continues from pos 3
        assert_eq!(iter.next(), Some((10, 4)));
    }

    #[test]
    fn advance_to_first_item() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30]);
        let mut iter = col.iter();
        let tx = iter.advance_to(0).unwrap();
        assert_eq!(tx.pos, 0);
        assert_eq!(tx.value, 10);
        assert_eq!(tx.prefix, 10);
        assert_eq!(tx.prefix_delta, 0); // nothing before first item
    }

    #[test]
    fn advance_to_last_item() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        let mut iter = col.iter();
        let tx = iter.advance_to(2).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 3);
        assert_eq!(tx.prefix_delta, 3); // 1+2
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn advance_to_out_of_range() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        let mut iter = col.iter();
        assert!(iter.advance_to(3).is_none());
        assert!(iter.advance_to(100).is_none());
    }

    #[test]
    fn advance_to_multi_slab() {
        // Force multiple slabs with small max_segments
        let vals: Vec<u64> = (1..=20).collect();
        let col = PrefixColumn::from_column(Column::<u64>::from_values_with_max_segments(
            vals.clone(),
            3,
        ));
        assert!(col.values().slab_count() > 1);

        // advance_to a position in a later slab
        let mut iter = col.iter();
        let tx = iter.advance_to(15).unwrap();
        assert_eq!(tx.pos, 15);
        assert_eq!(tx.value, 16);
        let expected_delta: u64 = (1..=15).sum(); // sum of items 0..15
        assert_eq!(tx.prefix_delta, expected_delta as u128);
        // can continue iterating
        let next = iter.next().unwrap();
        assert_eq!(next.1, 17);
    }

    #[test]
    fn advance_to_with_range() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        // iter_range(2..5) covers values [30, 40, 50]
        let mut iter = col.iter_range(2..5);
        let tx = iter.advance_to(3).unwrap();
        assert_eq!(tx.pos, 3);
        assert_eq!(tx.value, 40);
        // prefix_delta is relative to range start (pos 2)
        // exclusive prefix before pos 3 from pos 2 = sum of item at pos 2 = 30
        assert_eq!(tx.prefix_delta, 30);
    }

    #[test]
    fn advance_to_before_current_pos() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let mut iter = col.iter();
        iter.next(); // consume pos 0
        iter.next(); // consume pos 1
                     // pos 0 is before current position
        assert!(iter.advance_to(0).is_none());
    }

    // ── advance_prefix tests ─────────────────────────────────────────

    #[test]
    fn advance_prefix_single_slab_unit_values() {
        // All 1s: advance_prefix(3) should land on pos 3 (past items 0,1,2)
        let col = PrefixColumn::<u64>::from_values(vec![1, 1, 1, 1, 1]);
        let mut iter = col.iter();
        let tx = iter.advance_prefix(3).unwrap();
        assert_eq!(tx.pos, 3);
        assert_eq!(tx.value, 1);
        assert_eq!(tx.prefix_delta, 3); // consumed 3 units before this item
        assert_eq!(iter.next(), Some((5, 1)));
    }

    #[test]
    fn advance_prefix_single_slab_multi_values() {
        // [1, 1, 3, 1]: advance_prefix(3) lands on pos 2 (item with value 3
        // overshoots remaining budget of 1)
        let col = PrefixColumn::<u64>::from_values(vec![1, 1, 3, 1]);
        let mut iter = col.iter();
        let tx = iter.advance_prefix(3).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 3);
        assert_eq!(tx.prefix_delta, 2); // 1+1 consumed before this item
    }

    #[test]
    fn advance_prefix_exact_boundary() {
        // [2, 3, 5]: advance_prefix(5) should land PAST the boundary
        // items 0+1 sum to 5, so we land on pos 2
        let col = PrefixColumn::<u64>::from_values(vec![2, 3, 5, 1]);
        let mut iter = col.iter();
        let tx = iter.advance_prefix(5).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 5);
        assert_eq!(tx.prefix_delta, 5); // 2+3 consumed before pos 2
    }

    #[test]
    fn advance_prefix_zero() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30]);
        let mut iter = col.iter();
        // advance_prefix(0) should return the first item
        let tx = iter.advance_prefix(0).unwrap();
        assert_eq!(tx.pos, 0);
        assert_eq!(tx.value, 10);
        assert_eq!(tx.prefix_delta, 0);
    }

    #[test]
    fn advance_prefix_unreachable() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        let mut iter = col.iter();
        // total is 6, asking for 100
        assert!(iter.advance_prefix(100).is_none());
        assert_eq!(iter.items_left(), 0);
    }

    #[test]
    fn advance_prefix_multi_slab() {
        let vals: Vec<u64> = (1..=20).collect();
        let col = PrefixColumn::from_column(Column::<u64>::from_values_with_max_segments(
            vals.clone(),
            3,
        ));
        assert!(col.values().slab_count() > 1);

        let mut iter = col.iter();
        // advance past 100 prefix units: 1+2+...+13=91, 1+2+...+14=105
        // so we land on item 13 (value=14)
        let tx = iter.advance_prefix(100).unwrap();
        assert_eq!(tx.value, 14);
        assert_eq!(tx.pos, 13);
        let expected_delta: u64 = (1..=13).sum(); // 91
        assert_eq!(tx.prefix_delta, expected_delta as u128);
        // can continue
        assert_eq!(iter.next().unwrap().1, 15);
    }

    #[test]
    fn advance_prefix_mid_stream() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let mut iter = col.iter();
        iter.next(); // consume pos 0 (value 1)
                     // now advance past 4 more units: items 1(2)+2(3)=5 >= 4
                     // lands on pos 2 (value 3, overshoots remaining 2)
        let tx = iter.advance_prefix(4).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 3);
        // prefix_delta from base(0): exclusive prefix before pos 2 = 1+2 = 3
        assert_eq!(tx.prefix_delta, 3);
    }

    #[test]
    fn advance_prefix_bool() {
        let col =
            PrefixColumn::<bool>::from_values(vec![false, true, false, true, true, false, true]);
        let mut iter = col.iter();
        // advance past 2 trues: pos 0(f), 1(t), 2(f), 3(t) — 2 trues consumed
        // land on pos 4 (next item after 2 trues)
        let tx = iter.advance_prefix(2).unwrap();
        assert_eq!(tx.pos, 4);
        assert!(tx.value);
        assert_eq!(tx.prefix_delta, 2);
    }

    #[test]
    fn advance_prefix_with_range() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        // iter_range(1..4) covers values [20, 30, 40]
        let mut iter = col.iter_range(1..4);
        // advance past 30 units: item at pos 1 (20) consumed, then pos 2 (30)
        // overshoots remaining 10 → land on pos 2
        let tx = iter.advance_prefix(30).unwrap();
        assert_eq!(tx.pos, 2);
        assert_eq!(tx.value, 30);
        assert_eq!(tx.prefix_delta, 20); // relative to range start: only item at pos 1 consumed
    }

    // ── seek / get_delta convenience tests ───────────────────────────

    #[test]
    fn seek_convenience() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 1, 1, 1, 1]);
        let tx = col.seek(0, 3).unwrap();
        assert_eq!(tx.pos, 3);
        assert_eq!(tx.prefix_delta, 3);

        // seek from middle
        let tx = col.seek(2, 2).unwrap();
        assert_eq!(tx.pos, 4);
        assert_eq!(tx.prefix_delta, 2);
    }

    #[test]
    fn get_delta_convenience() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40]);
        let tx = col.get_delta(1, 3).unwrap();
        assert_eq!(tx.pos, 3);
        assert_eq!(tx.value, 40);
        assert_eq!(tx.prefix_delta, 50); // 20+30 between pos 1 and pos 3
    }
}
