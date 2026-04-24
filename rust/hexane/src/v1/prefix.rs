use std::iter::Sum;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Div, Mul, Sub, SubAssign};

use super::column::{Column, Iter, Slab, SlabWeight, TailOf, WeightFn};
use super::encoding::{ColumnEncoding, RunDecoder};
use super::{ColumnValueRef, Run, TypedLoadOpts};
use crate::PackError;

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
    fn run_prefix(run: &Run<Self::Get<'_>>) -> Self::Prefix {
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
#[derive(Clone, Default, Debug)]
pub struct PrefixSlabWeight<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> {
    pub(crate) len: usize,
    pub(crate) prefix: P,
}

impl<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> Copy for PrefixSlabWeight<P> where
    P: Copy
{
}

impl<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> AddAssign
    for PrefixSlabWeight<P>
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.len += rhs.len;
        self.prefix += rhs.prefix;
    }
}

impl<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> SubAssign
    for PrefixSlabWeight<P>
{
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.len -= rhs.len;
        self.prefix -= rhs.prefix;
    }
}

impl<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> SlabWeight
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
    pub total: P,
    /// Prefix sum consumed since the iterator's range start.
    pub delta: P,
    /// The value at this position.
    pub value: V,
}
// ── PrefixColumn (B-tree backed — one aggregate for len+prefix queries) ────
//
// Wraps a `Column<T, PrefixWeightFn<T>>`; the inner B-tree carries
// `PrefixSlabWeight<T::Prefix>` per slab, answering both positional and
// prefix-sum queries without a sidecar.

pub struct PrefixColumn<T: PrefixValue> {
    col: Column<T, PrefixWeightFn<T>>,
}

impl<T: PrefixValue> Clone for PrefixColumn<T> {
    fn clone(&self) -> Self {
        Self {
            col: self.col.clone(),
        }
    }
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
    pub fn new() -> Self {
        Self { col: Column::new() }
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            col: Column::with_max_segments(max_segments),
        }
    }

    pub fn from_values(values: Vec<T>) -> Self {
        Self {
            col: Column::from_values(values),
        }
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Ok(Self {
            col: Column::load(data)?,
        })
    }

    // ── Delegated read methods ───────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.col.len()
    }

    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    pub fn get_value(&self, index: usize) -> Option<T::Get<'_>> {
        self.col.get(index)
    }

    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    // ── Mutations ───────────────────────────────────────────────────────

    pub fn insert(&mut self, index: usize, value: impl super::AsColumnRef<T>) {
        self.col.insert(index, value);
    }

    pub fn remove(&mut self, index: usize) {
        self.col.remove(index);
    }

    pub fn push(&mut self, value: impl super::AsColumnRef<T>) {
        self.col.push(value);
    }

    pub fn clear(&mut self) {
        self.col.clear();
    }

    pub fn truncate(&mut self, len: usize) {
        self.col.truncate(len);
    }

    pub fn splice<V, I>(&mut self, index: usize, del: usize, values: I)
    where
        V: super::AsColumnRef<T>,
        I: IntoIterator<Item = V>,
    {
        self.col.splice(index, del, values);
    }

    /// Iterator over raw values (no prefix accumulation).
    pub fn value_iter(&self) -> super::column::Iter<'_, T> {
        self.col.iter()
    }

    // ── Prefix-sum queries — via Column's B-tree ───────────────────────

    /// Exclusive prefix sum at `index` — sum of values at indices
    /// `0..index`.  Matches [`PrefixColumn::get_prefix`] semantics.
    pub fn get_prefix(&self, index: usize) -> T::Prefix {
        self.iter_range(index..self.len()).total
    }

    pub fn get_total(&self, index: usize) -> T::Prefix {
        self.get_prefix(index + 1)
    }

    pub fn delta(&self, from: usize, to: usize) -> Option<PrefixSeek<T::Prefix, T::Get<'_>>> {
        assert!(to >= from);
        let mut iter = self.iter();
        iter.advance_to(from);
        iter.delta_nth(to - from)
    }

    pub fn prefix_delta(&self, range: std::ops::Range<usize>) -> T::Prefix {
        if range.start >= range.end || self.col.is_empty() {
            T::Prefix::default()
        } else {
            let mut iter = self.iter();
            iter.advance_to(range.start);
            let base = iter.total;
            iter.advance_to(range.end);
            iter.total - base
        }
    }
}

// ── Unsigned-prefix-only methods ────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T>
where
    T::Prefix: UnsignedPrefix,
{
    pub fn get_index_for_total(&self, target: T::Prefix) -> usize {
        self.get_index_for_prefix(target).saturating_sub(1)
    }

    pub fn get_index_for_prefix(&self, target: T::Prefix) -> usize {
        if target <= T::Prefix::default() {
            return 0;
        }
        if self.col.is_empty() {
            return 0;
        }

        let (si, prefix_before, items_before) = self.col.index.find_slab_at_prefix(target);

        if si >= self.col.slab_count() {
            return self.col.len() + 1;
        }

        let remaining = target - prefix_before;
        let slab = &self.col.slabs[si];
        let idx_in_slab = T::find_prefix_in_slab(slab, remaining);
        items_before + idx_in_slab
    }
}

// ── More PrefixColumn methods (port from old BIT-backed variant) ───────────

impl<T: PrefixValue> PrefixColumn<T> {
    pub fn save_to(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.col.save_to(out)
    }

    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.col.to_vec()
    }

    /// Reference to the inner `Column` — for code that needs Column-level
    /// operations (iter, get, etc.) without going through the prefix APIs.
    pub fn values(&self) -> &Column<T, PrefixWeightFn<T>> {
        &self.col
    }

    /// Wrap an existing [`Column`] that already tracks `PrefixWeightFn<T>`.
    pub fn from_column(col: Column<T, PrefixWeightFn<T>>) -> Self {
        Self { col }
    }

    /// Returns `(prefix, value)` at `index` via the iterator.
    pub fn get(&self, index: usize) -> Option<(T::Prefix, T::Get<'_>)> {
        self.iter().nth(index)
    }

    /// Iterator that yields `(inclusive_prefix, value)` per item.
    pub fn iter(&self) -> PrefixIter<'_, T> {
        PrefixIter {
            col: Some(self),
            inner: self.col.iter(),
            total: T::Prefix::default(),
            base: T::Prefix::default(),
        }
    }

    /// Iterator over `range` yielding `(inclusive_prefix, value)`.
    pub fn iter_range(&self, range: std::ops::Range<usize>) -> PrefixIter<'_, T> {
        let start = range.start.min(self.col.len());
        let end = range.end.min(self.col.len());
        let mut iter = self.iter();
        iter.set_max(end);
        iter.advance_by(start);
        iter
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
        // FIXME
        self.iter_range(start..self.col.len()).advance_prefix(n)
    }

    pub fn get_delta(&self, start: usize, pos: usize) -> Option<PrefixSeek<T::Prefix, T::Get<'_>>> {
        if pos >= start {
            self.iter_range(start..self.col.len())
                .delta_nth(pos - start)
        } else {
            None
        }
    }
}

// ── PrefixIter ──────────────────────────────────────────────────────────────

/// Forward iterator over a [`PrefixColumn`] that yields `(prefix_sum, value)`.
///
/// `next()` is O(1): accumulates the total from the yielded value.
/// `nth(n)` is O(log S) via the B-tree.
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
            if self.inner.items_left > 0 {
                self.nth(self.inner.items_left - 1);
            }
            return None;
        }

        // Fast path: within current slab — accumulate via next_run.
        if n < self.inner.slab_remaining {
            return Some(self.same_slab_nth(n));
        }

        let target_pos = self.inner.pos + n;

        let found = self.col.unwrap().col.index.find_slab_at_item(target_pos);

        if !self.inner.advance_to_slab(found.index, found.pos) {
            return None;
        }

        self.total = found.prefix;

        Some(self.same_slab_nth(target_pos - found.pos))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: PrefixValue> ExactSizeIterator for PrefixIter<'_, T> {}

impl<'a, T: PrefixValue> PrefixIter<'a, T> {
    fn same_slab_nth(&mut self, mut n: usize) -> (T::Prefix, T::Get<'a>) {
        while let Some(run) = self.inner.next_run_max(n + 1) {
            let run_prefix = T::run_prefix(&run);
            self.total += run_prefix;
            if run.count > n {
                return (self.total, run.value);
            }
            n -= run.count;
        }
        panic!("same_slab_nth called with n > slab len");
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.inner.pos
    }

    #[inline]
    pub fn items_left(&self) -> usize {
        self.inner.items_left
    }

    #[inline]
    pub fn end_pos(&self) -> usize {
        self.inner.pos + self.inner.items_left
    }

    pub fn set_max(&mut self, pos: usize) {
        self.inner.set_max(pos);
    }

    pub fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
    }

    pub fn suspend(&self) -> PrefixIterState<T> {
        PrefixIterState {
            inner: self.inner.suspend(),
            total: self.total,
            base: self.base,
        }
    }

    /// Move the iterator window to `range` and return the item at `range.start`.
    ///
    /// After this call the iterator yields items from `range.start + 1`
    /// through `range.end - 1`.
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: std::ops::Range<usize>) -> Option<(T::Prefix, T::Get<'a>)> {
        assert!(range.start >= self.pos());
        self.set_max(range.end);
        self.nth(range.start - self.pos())
    }

    /// Next run of identical values, paired with the inclusive total at
    /// the *end* of the run.
    pub fn next_run(&mut self) -> Option<Run<(T::Prefix, T::Get<'a>)>> {
        let run = self.inner.next_run()?;
        self.total += T::run_prefix(&run);
        Some(Run {
            count: run.count,
            value: (self.total, run.value),
        })
    }

    /// Advance to position `pos` (must be ≥ current pos and < end) and
    /// return the item there with absolute prefix + delta since base.
    pub fn advance_to(&mut self, target: usize) {
        if target > self.pos() {
            self.nth(target - self.pos() - 1);
        }
    }

    pub fn delta_nth(&mut self, n: usize) -> Option<PrefixSeek<T::Prefix, T::Get<'a>>> {
        let base = self.total;
        let (total, value) = self.nth(n)?;
        let pos = self.pos() - 1;
        let delta = total - base - T::to_prefix(value); // prefix change : exclusive
        Some(PrefixSeek {
            pos,
            total,
            delta,
            value,
        })
    }
}

impl<'a, T: PrefixValue> PrefixIter<'a, T>
where
    T::Prefix: UnsignedPrefix,
{
    /// Advance past `n` prefix units (cumulative sum) and return the
    /// item landed on.
    pub fn advance_prefix(&mut self, n: T::Prefix) -> Option<PrefixSeek<T::Prefix, T::Get<'a>>> {
        // this does an O(slabs) lookup even if the destination is on the current slab
        // if we stored slab_prefix on the iterator we could avoid that
        // but doing so would require an O(slab) lookup on each slab change
        // unless we had an Iterator<Item=(slab,prefix)> instead of doing a slab_index +=1
        // currently this isnt a bottleneck anywhere so not a big deal
        let col = self.col.expect("advance_prefix on default PrefixIter");
        let target = self.total + n;
        let one = T::Prefix::try_from(1).unwrap_or_default();
        let seek_target = target + one;
        let target_pos = col.get_index_for_total(seek_target);

        if target_pos < self.inner.pos || target_pos >= self.inner.pos + self.inner.items_left {
            if self.inner.items_left > 0 {
                self.nth(self.inner.items_left - 1);
            }
            return None;
        }

        self.delta_nth(target_pos - self.inner.pos)
    }
}

// ── PrefixIterState ────────────────────────────────────────────────────────

pub struct PrefixIterState<T: PrefixValue> {
    inner: super::column::IterState,
    total: T::Prefix,
    base: T::Prefix,
}

impl<T: PrefixValue> PrefixIterState<T> {
    pub fn try_resume<'a>(
        &self,
        column: &'a PrefixColumn<T>,
    ) -> Result<PrefixIter<'a, T>, crate::PackError> {
        let inner = self.inner.try_resume(column.values())?;
        Ok(PrefixIter {
            col: Some(column),
            inner,
            total: self.total,
            base: self.base,
        })
    }
}

// ── Trait impls ─────────────────────────────────────────────────────────────

impl<T: PrefixValue> FromIterator<T> for PrefixColumn<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<V, T: PrefixValue> Extend<V> for PrefixColumn<T>
where
    V: super::AsColumnRef<T>,
{
    fn extend<I: IntoIterator<Item = V>>(&mut self, iter: I) {
        let len = self.col.len();
        self.col.splice(len, 0, iter);
    }
}

impl<'a, T: PrefixValue> IntoIterator for &'a PrefixColumn<T> {
    type Item = (T::Prefix, T::Get<'a>);
    type IntoIter = PrefixIter<'a, T>;

    fn into_iter(self) -> PrefixIter<'a, T> {
        self.iter()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::v1::prefix::PrefixColumn;

    fn parity_check(values: Vec<u64>) {
        let col = PrefixColumn::<u64>::from_values(values.clone());
        let tree = PrefixColumn::<u64>::from_values(values.clone());
        let n = values.len();

        assert_eq!(tree.len(), col.len());
        assert_eq!(tree.save(), col.save());

        for i in 0..=n {
            assert_eq!(
                tree.get_prefix(i),
                col.get_prefix(i),
                "get_prefix({i}) mismatch",
            );
        }

        for lo in 0..=n {
            for hi in lo..=n {
                assert_eq!(
                    tree.prefix_delta(lo..hi),
                    col.prefix_delta(lo..hi),
                    "prefix_delta({lo}..{hi}) mismatch",
                );
            }
        }

        let total: u128 = values.iter().map(|&v| v as u128).sum();
        for target in 0..=total + 2 {
            assert_eq!(
                tree.get_index_for_prefix(target),
                col.get_index_for_prefix(target),
                "get_index_for_prefix({target}) mismatch",
            );
        }
    }

    #[test]
    fn empty_parity() {
        parity_check(vec![]);
    }

    #[test]
    fn single_parity() {
        parity_check(vec![42]);
    }

    #[test]
    fn small_sequential_parity() {
        parity_check((1..=20).collect());
    }

    #[test]
    fn duplicates_parity() {
        parity_check(vec![5; 50]);
    }

    #[test]
    fn mixed_parity() {
        parity_check(vec![1, 100, 2, 200, 3, 300, 4, 400, 5, 500]);
    }

    #[test]
    fn many_slabs_parity() {
        let values: Vec<u64> = (0..5_000).map(|i| (i * 3) as u64 + 1).collect();
        let col = PrefixColumn::<u64>::from_values(values.clone());
        let tree = PrefixColumn::<u64>::from_values(values);
        assert_eq!(tree.len(), col.len());
        for i in [0usize, 1, 100, 500, 2500, 4999, 5000] {
            assert_eq!(tree.get_prefix(i), col.get_prefix(i));
        }
        let grand_total: u128 = col.get_prefix(col.len());
        for t in [
            1u128,
            100,
            10_000,
            grand_total / 2,
            grand_total,
            grand_total + 1,
        ] {
            assert_eq!(
                tree.get_index_for_prefix(t),
                col.get_index_for_prefix(t),
                "target={t}",
            );
        }
    }

    #[test]
    fn mutations_parity() {
        let mut col = PrefixColumn::<u64>::from_values((1..=20).collect());
        let mut tree = PrefixColumn::<u64>::from_values((1..=20).collect());

        col.insert(5, 100u64);
        tree.insert(5, 100u64);
        for i in 0..=col.len() {
            assert_eq!(tree.get_prefix(i), col.get_prefix(i));
        }

        col.remove(10);
        tree.remove(10);
        for i in 0..=col.len() {
            assert_eq!(tree.get_prefix(i), col.get_prefix(i));
        }

        col.splice(3, 4, vec![50u64, 60, 70]);
        tree.splice(3, 4, vec![50u64, 60, 70]);
        assert_eq!(tree.len(), col.len());
        for i in 0..=col.len() {
            assert_eq!(tree.get_prefix(i), col.get_prefix(i), "i={i}");
        }

        let n = col.len();
        assert_eq!(tree.get_prefix(n), col.get_prefix(n));
    }

    #[test]
    fn fuzz_mutation_parity() {
        struct Rng(u64);
        impl Rng {
            fn new(seed: u64) -> Self {
                Self(seed.max(1))
            }
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }
        }

        let mut rng = Rng::new(0xCAFE);
        let init: Vec<u64> = (0..50).map(|_| (rng.next() % 100) + 1).collect();
        let mut col = PrefixColumn::<u64>::from_values(init.clone());
        let mut tree = PrefixColumn::<u64>::from_values(init);

        for _ in 0..500 {
            let op = rng.next() % 3;
            let len = col.len();
            match op {
                0 => {
                    let at = (rng.next() as usize) % (len + 1);
                    let v = (rng.next() % 100) + 1;
                    col.insert(at, v);
                    tree.insert(at, v);
                }
                1 if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    col.remove(at);
                    tree.remove(at);
                }
                _ if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    let del = (rng.next() as usize) % (len - at).min(4) + 1;
                    let count = (rng.next() as usize) % 4 + 1;
                    let new: Vec<u64> = (0..count).map(|_| (rng.next() % 100) + 1).collect();
                    col.splice(at, del, new.clone());
                    tree.splice(at, del, new);
                }
                _ => {}
            }
            assert_eq!(tree.len(), col.len());
            for probe in [0, tree.len() / 2, tree.len()] {
                assert_eq!(tree.get_prefix(probe), col.get_prefix(probe));
            }
        }
    }
}
