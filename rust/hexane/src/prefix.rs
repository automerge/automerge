use crate::column::IterState;
use crate::sealed::Sealed;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Div, Sub, SubAssign};

use crate::column::{Column, Iter, Slab, SlabWeight, TailOf, WeightFn};
use crate::encoding::{ColumnEncoding, RunDecoder};
use crate::PackError;
use crate::{ColumnValueRef, RleValue, Run, TypedLoadOpts};

// ── UnsignedPrefix marker ────────────────────────────────────────────────────

/// Marker trait for unsigned prefix types.
///
/// `get_index_for_prefix`, `get_index_for_total`, `find_prefix_in_slab`,
/// and `advance_prefix` rely on monotonically increasing prefix sums.
/// Signed prefix types (e.g. `i128`) can decrease, making these operations
/// incorrect. This trait gates those methods at compile time.
///
/// `Copy` is a supertrait — the inverse-prefix search uses scalar
/// arithmetic (`Add`/`Sub`/`Div`) by-value many times per iteration, and
/// every concrete `UnsignedPrefix` type (u32/u64/u128/usize) is `Copy`.
pub trait UnsignedPrefix: Copy {}
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
    ///
    /// The bounds here cover only what's needed for forward queries
    /// (accumulating left-to-right, computing deltas).  Inverse queries —
    /// "find the row where the running sum reaches `target`" — additionally
    /// require `Ord + Div + TryFrom<usize> + TryInto<usize>` and are gated
    /// behind `T::Prefix: UnsignedPrefix` on the relevant impl blocks.
    ///
    /// Bounded on `Clone` (not `Copy`) so non-scalar prefix types — e.g.
    /// a `HashMap`-backed mark set — can serve as the accumulator.  For
    /// scalar `Copy` types, `Clone` inlines to a memcpy, so there is no
    /// runtime cost.
    type Prefix: Default
        + Clone
        + std::fmt::Debug
        + Add<Output = Self::Prefix>
        + Sub<Output = Self::Prefix>
        + AddAssign
        + SubAssign
        + for<'a> AddAssign<&'a Self::Prefix>;

    /// Accumulate one value into `target` in place.  Required.
    ///
    /// In-place is the hot path for non-scalar prefixes (HashMap-backed
    /// `MarkAcc`, etc.) — implementations mutate `target` directly rather
    /// than allocating a fresh `Prefix` per value and merging.  For Copy
    /// scalars this is a single `*target += val.into()`.
    fn accumulate(target: &mut Self::Prefix, val: Self::Get<'_>);

    /// Accumulate an entire run into `target` in place.  Required.
    ///
    /// The run-shaped form lets RLE-friendly impls scale by `run.count`
    /// in a single op (`*target += val * count`) instead of looping;
    /// non-scalar impls typically just do the same single-entry update
    /// scaled by `count` (Start/End OpIds are unique, so runs of length
    /// > 1 are rare in practice but still cheap).
    fn accumulate_run(target: &mut Self::Prefix, run: &Run<Self::Get<'_>>);

    /// Sum all values in a slab.  Walks the encoded runs directly for
    /// efficiency — O(segments) rather than O(items).
    fn slab_sum(slab: &Slab<TailOf<Self>>) -> Self::Prefix {
        let mut decoder = Self::Encoding::decoder(&slab.data);
        let mut acc = Self::Prefix::default();
        while let Some(run) = decoder.next_run() {
            Self::accumulate_run(&mut acc, &run);
        }
        acc
    }

    /// Compute the partial prefix sum of the first `count` items in a slab.
    fn partial_sum(slab: &Slab<TailOf<Self>>, count: usize) -> Self::Prefix {
        let mut decoder = Self::Encoding::decoder(&slab.data);
        let mut acc = Self::Prefix::default();
        let mut items = 0;
        while let Some(mut run) = decoder.next_run() {
            run.count = run.count.min(count - items);
            Self::accumulate_run(&mut acc, &run);
            items += run.count;
            if items >= count {
                break;
            }
        }
        acc
    }
}

/// Find the first index within a slab where the running sum reaches or
/// exceeds `target`.  Returns items consumed.
///
/// Only correct for unsigned prefix types where sums are monotonically
/// increasing.  Callers are gated by `T::Prefix: UnsignedPrefix`.
fn find_prefix_in_slab<T: PrefixValue>(slab: &Slab<TailOf<T>>, target: T::Prefix) -> usize
where
    T::Prefix: UnsignedPrefix + Div<Output = T::Prefix> + TryInto<usize> + TryFrom<usize> + Ord,
{
    let zero = T::Prefix::default();
    let one_p = T::Prefix::try_from(1).unwrap_or_default();
    let mut decoder = T::Encoding::decoder(&slab.data);
    let mut acc = zero;
    let mut items = 0;
    while let Some(run) = decoder.next_run() {
        // Peek at the run's contribution without losing `acc`.  This
        // function is bounded on `UnsignedPrefix` which implies `Copy`,
        // so the assignment is a memcpy rather than a clone.
        let mut peek = acc;
        T::accumulate_run(&mut peek, &run);
        if peek >= target {
            // Target is within this run — ceiling division by the
            // single-value contribution `p`.
            let mut p = T::Prefix::default();
            T::accumulate(&mut p, run.value);
            let remaining = target - acc;
            let needed: T::Prefix = (remaining + p - one_p) / p;
            let needed_usize: usize = needed.try_into().unwrap_or(run.count);
            assert!(needed_usize <= run.count);
            items += needed_usize;
            break;
        }
        acc = peek;
        items += run.count;
    }
    items
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

impl<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign> Sealed for PrefixSlabWeight<P> {}

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

impl<T> Sealed for PrefixWeightFn<T> {}

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
    fn accumulate(target: &mut u128, val: u64) {
        *target += val as u128;
    }
    fn accumulate_run(target: &mut u128, run: &Run<u64>) {
        *target += run.value as u128 * run.count as u128;
    }
}

impl PrefixValue for i64 {
    type Prefix = i128;
    fn accumulate(target: &mut i128, val: i64) {
        *target += val as i128;
    }
    fn accumulate_run(target: &mut i128, run: &Run<i64>) {
        *target += run.value as i128 * run.count as i128;
    }
}

impl PrefixValue for u32 {
    type Prefix = u64;
    fn accumulate(target: &mut u64, val: u32) {
        *target += val as u64;
    }
    fn accumulate_run(target: &mut u64, run: &Run<u32>) {
        *target += run.value as u64 * run.count as u64;
    }
}

impl PrefixValue for std::num::NonZeroU32 {
    type Prefix = u64;
    fn accumulate(target: &mut u64, val: std::num::NonZeroU32) {
        *target += val.get() as u64;
    }
    fn accumulate_run(target: &mut u64, run: &Run<std::num::NonZeroU32>) {
        *target += run.value.get() as u64 * run.count as u64;
    }
}

// ── Blanket Option<T> impl ──────────────────────────────────────────────────
//
// Any RLE-encoded `T: PrefixValue` automatically yields a nullable
// `Option<T>: PrefixValue`, where `None` contributes the prefix identity
// and `Some(v)` delegates to `T`.  This is also the workaround for the
// orphan rule: downstream crates that define a local `T: PrefixValue +
// RleValue` get `Option<T>: PrefixValue` for free.

impl<T> PrefixValue for Option<T>
where
    T: PrefixValue + RleValue,
{
    type Prefix = T::Prefix;

    #[inline]
    fn accumulate(target: &mut T::Prefix, val: Option<T::Get<'_>>) {
        if let Some(v) = val {
            T::accumulate(target, v);
        }
    }

    #[inline]
    fn accumulate_run(target: &mut T::Prefix, run: &Run<Option<T::Get<'_>>>) {
        if let Some(v) = run.value {
            T::accumulate_run(
                target,
                &Run {
                    count: run.count,
                    value: v,
                },
            );
        }
    }
}

// ── bool impl ────────────────────────────────────────────────────────────────

impl PrefixValue for bool {
    type Prefix = usize;

    fn accumulate(target: &mut usize, val: bool) {
        *target += val as usize;
    }
    fn accumulate_run(target: &mut usize, run: &Run<bool>) {
        *target += run.value as usize * run.count;
    }
}

// ── Load / save with options ────────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T> {
    /// Deserialize with options. See [`LoadOpts`](crate::LoadOpts).
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
/// Returned by [`PrefixIter::advance_prefix`], [`PrefixIter::delta_nth`],
/// and [`PrefixColumn::delta`].  After the call the iterator is positioned
/// at `pos + 1`, ready to yield subsequent items.
pub struct PrefixSeek<'a, T: PrefixValue> {
    /// Position (index) of the item landed on.
    pub pos: usize,
    /// Prefix consumed between the iterator's running total when the seek
    /// began and this item, **exclusive** of the item itself.  For a seek
    /// spanning `from..to`, this is the sum over `[from, to)`.
    pub delta: T::Prefix,
    /// The item itself, with its absolute running sums
    /// ([`prefix()`](PrefixedValue::prefix) / [`total()`](PrefixedValue::total)).
    pub pv: PrefixedValue<'a, T>,
}

impl<'a, T: PrefixValue> Clone for PrefixSeek<'a, T> {
    fn clone(&self) -> Self {
        Self {
            pos: self.pos,
            delta: self.delta.clone(),
            pv: self.pv.clone(),
        }
    }
}

impl<'a, T: PrefixValue> Copy for PrefixSeek<'a, T> where T::Prefix: Copy {}

impl<'a, T: PrefixValue> std::fmt::Debug for PrefixSeek<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixSeek")
            .field("pos", &self.pos)
            .field("delta", &self.delta)
            .field("pv", &self.pv)
            .finish()
    }
}

impl<'a, T: PrefixValue> PartialEq for PrefixSeek<'a, T>
where
    T::Prefix: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.pos == other.pos && self.delta == other.delta && self.pv == other.pv
    }
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

    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    // ── Mutations ───────────────────────────────────────────────────────

    pub fn insert(&mut self, index: usize, value: impl crate::AsColumnRef<T>) {
        self.col.insert(index, value);
    }

    pub fn remove(&mut self, index: usize) {
        self.col.remove(index);
    }

    /// Remove `n` items starting at `index` — same arguments as
    /// [`splice`](Self::splice)`(index, n, [])`, without the typed
    /// empty-iterator dance.  Panics if `index + n` exceeds the length.
    pub fn remove_n(&mut self, index: usize, n: usize) {
        self.col.remove_n(index, n);
    }

    pub fn push(&mut self, value: impl crate::AsColumnRef<T>) {
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
        V: crate::AsColumnRef<T>,
        I: IntoIterator<Item = V>,
    {
        self.col.splice(index, del, values);
    }

    // ── Prefix-sum queries — via Column's B-tree ───────────────────────

    /// **Exclusive** prefix sum at `index` — the sum of values at
    /// indices `0..index`: what [`PrefixedValue::prefix`] shows for the
    /// item at `index`.
    pub fn get_prefix(&self, index: usize) -> T::Prefix {
        self.iter_range(index..self.len()).total
    }

    /// **Inclusive** prefix sum through `index`: what
    /// [`PrefixedValue::total`] shows for the item at `index`.  Equals
    /// `get_prefix(index + 1)`.
    pub fn get_total(&self, index: usize) -> T::Prefix {
        self.get_prefix(index + 1)
    }

    /// Seek to position `to`, returning the item there together with the
    /// prefix consumed over `[from, to)` in [`PrefixSeek::delta`].
    ///
    /// # Panics
    ///
    /// Panics if `to < from` (inverted range is a caller error, matching
    /// [`PrefixIter::shift_next`]).
    pub fn delta(&self, from: usize, to: usize) -> Option<PrefixSeek<'_, T>> {
        assert!(to >= from);
        let mut iter = self.iter();
        iter.advance_to(from);
        iter.delta_nth(to - from)
    }

    /// Sum of the values in `range` — `get_prefix(range.end)` minus
    /// `get_prefix(range.start)`, computed in one pass.
    pub fn sum_range(&self, range: std::ops::Range<usize>) -> T::Prefix {
        if range.start >= range.end || self.col.is_empty() {
            T::Prefix::default()
        } else {
            let mut iter = self.iter();
            iter.advance_to(range.start);
            let base = iter.total.clone();
            iter.advance_to(range.end);
            iter.total - base
        }
    }
}

// ── Unsigned-prefix-only methods ────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T>
where
    T::Prefix: UnsignedPrefix + Div<Output = T::Prefix> + TryInto<usize> + TryFrom<usize> + Ord,
{
    /// Index of the item whose **inclusive** total
    /// ([`PrefixedValue::total`]) first reaches `target` — the inverse of
    /// [`get_total`](Self::get_total).  Equals
    /// `get_index_for_prefix(target).saturating_sub(1)`.
    pub fn get_index_for_total(&self, target: T::Prefix) -> usize {
        self.get_index_for_prefix(target).saturating_sub(1)
    }

    /// First index `i` where the **exclusive** prefix
    /// ([`get_prefix`](Self::get_prefix)`(i)`, i.e. what
    /// [`PrefixedValue::prefix`] shows at `i`) reaches `target`.
    /// Returns `0` when `target` is zero (or non-positive) and
    /// `len() + 1` when `target` exceeds the grand total.
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
        let idx_in_slab = find_prefix_in_slab::<T>(slab, remaining);
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

    /// Reference to the inner `Column` — the gateway for value-level
    /// reads that don't need prefix sums:
    ///
    /// * `col.values().get(i)` — value at `i`
    /// * `col.values().iter()` / `.iter_range(a..b)` — plain value iteration
    ///
    /// (`PrefixColumn::get` / `iter` return [`PrefixedValue`]s.)
    pub fn values(&self) -> &Column<T, PrefixWeightFn<T>> {
        &self.col
    }

    /// Wrap an existing [`Column`] that already tracks `PrefixWeightFn<T>`.
    pub fn from_column(col: Column<T, PrefixWeightFn<T>>) -> Self {
        Self { col }
    }

    /// Returns the [`PrefixedValue`] at `index` via the iterator.
    pub fn get(&self, index: usize) -> Option<PrefixedValue<'_, T>> {
        self.iter().nth(index)
    }

    /// Iterator yielding a [`PrefixedValue`] per item.
    pub fn iter(&self) -> PrefixIter<'_, T> {
        PrefixIter {
            col: Some(self),
            inner: self.col.iter(),
            total: T::Prefix::default(),
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

// ── PrefixIter ──────────────────────────────────────────────────────────────

/// A value paired with its running prefix sum — the item type of
/// [`PrefixIter`] and return type of [`PrefixColumn::get`].
///
/// Both views of the accumulator are available as methods, named to make
/// the inclusive/exclusive distinction impossible to miss:
///
/// * [`prefix()`](Self::prefix) — running sum **before** this item
///   (exclusive).
/// * [`total()`](Self::total) — running sum **through** this item
///   (inclusive).
///
/// With unit widths `[1, 1, 1]`, item 1 has `prefix() == 1` and
/// `total() == 2`.  A byte-offset column reads naturally as
/// `pv.prefix()..pv.total()` — the item's byte range.
///
/// For run-shaped access ([`PrefixIter::next_run`]) the `PrefixedValue`
/// describes the run's **final** item: `total()` is the sum through the
/// end of the run, `prefix()` the sum before that last item.
pub struct PrefixedValue<'a, T: PrefixValue> {
    /// The item's value.
    pub value: T::Get<'a>,
    total: T::Prefix,
}

impl<'a, T: PrefixValue> PrefixedValue<'a, T> {
    /// Running sum **through** this item (inclusive).
    #[inline]
    pub fn total(&self) -> T::Prefix {
        self.total.clone()
    }

    /// Running sum of the items **before** this one (exclusive) —
    /// `total()` minus this value's own contribution.
    #[inline]
    pub fn prefix(&self) -> T::Prefix {
        let mut single = T::Prefix::default();
        T::accumulate(&mut single, self.value);
        self.total.clone() - single
    }
}

impl<'a, T: PrefixValue> Clone for PrefixedValue<'a, T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            total: self.total.clone(),
        }
    }
}

impl<'a, T: PrefixValue> Copy for PrefixedValue<'a, T> where T::Prefix: Copy {}

impl<'a, T: PrefixValue> std::fmt::Debug for PrefixedValue<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixedValue")
            .field("value", &self.value)
            .field("total", &self.total)
            .finish()
    }
}

impl<'a, T: PrefixValue> PartialEq for PrefixedValue<'a, T>
where
    T::Prefix: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        T::eq(self.value, other.value) && self.total == other.total
    }
}

/// Forward iterator over a [`PrefixColumn`], yielding a [`PrefixedValue`]
/// per item.
///
/// `next()` is O(1) — it accumulates the running total from each value.
/// `nth(n)` is O(log S) via the B-tree.
pub struct PrefixIter<'a, T: PrefixValue> {
    col: Option<&'a PrefixColumn<T>>,
    inner: Iter<'a, T>,
    total: T::Prefix,
}

impl<T: PrefixValue> Default for PrefixIter<'_, T> {
    fn default() -> Self {
        Self {
            col: None,
            inner: Iter::default(),
            total: T::Prefix::default(),
        }
    }
}

impl<T: PrefixValue> Clone for PrefixIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            col: self.col,
            inner: self.inner.clone(),
            total: self.total.clone(),
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
    type Item = PrefixedValue<'a, T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.inner.next()?;
        T::accumulate(&mut self.total, value);
        Some(PrefixedValue {
            value,
            total: self.total.clone(),
        })
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
    fn same_slab_nth(&mut self, mut n: usize) -> PrefixedValue<'a, T> {
        while let Some(run) = self.inner.next_run_max(n + 1) {
            T::accumulate_run(&mut self.total, &run);
            if run.count > n {
                return PrefixedValue {
                    value: run.value,
                    total: self.total.clone(),
                };
            }
            n -= run.count;
        }
        panic!("same_slab_nth called with n > slab len");
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.inner.pos
    }

    /// The running sum of everything consumed so far — equivalently, the
    /// **exclusive** prefix of the next item to be yielded. Immediately
    /// after [`PrefixColumn::iter_range`] this is the prefix at the
    /// range's start, already paid for by the construction's tree descent.
    #[inline]
    pub fn total(&self) -> T::Prefix {
        self.total.clone()
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
            total: self.total.clone(),
        }
    }

    /// Move the iterator window to `range` and return the item at `range.start`.
    ///
    /// After this call the iterator yields items from `range.start + 1`
    /// through `range.end - 1`.
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: std::ops::Range<usize>) -> Option<PrefixedValue<'a, T>> {
        assert!(range.start >= self.pos());
        self.set_max(range.end);
        self.nth(range.start - self.pos())
    }

    /// Next run of identical values as a [`PrefixedValue`] describing the
    /// run's final item: `total()` is the sum through the end of the run.
    pub fn next_run(&mut self) -> Option<Run<PrefixedValue<'a, T>>> {
        let run = self.inner.next_run()?;
        T::accumulate_run(&mut self.total, &run);
        Some(Run {
            count: run.count,
            value: PrefixedValue {
                value: run.value,
                total: self.total.clone(),
            },
        })
    }

    /// Advance to position `pos` (must be ≥ current pos and < end) and
    /// return the item there with absolute prefix + delta since base.
    pub fn advance_to(&mut self, target: usize) {
        if target > self.pos() {
            self.nth(target - self.pos() - 1);
        }
    }

    pub fn delta_nth(&mut self, n: usize) -> Option<PrefixSeek<'a, T>> {
        let base = self.total.clone();
        let pv = self.nth(n)?;
        let pos = self.pos() - 1;
        Some(PrefixSeek {
            pos,
            delta: pv.prefix() - base, // prefix consumed before the item
            pv,
        })
    }
}

impl<'a, T: PrefixValue> PrefixIter<'a, T>
where
    T::Prefix: UnsignedPrefix + Div<Output = T::Prefix> + TryInto<usize> + TryFrom<usize> + Ord,
{
    /// Advance **past** `n` prefix units (cumulative sum) and return the
    /// item landed on — i.e. the item containing unit `n + 1`.
    ///
    /// Boundary contract (automerge's text indexing depends on this): with
    /// unit widths `[1, 1, 1]`, `advance_prefix(0)` lands on item 0 and
    /// `advance_prefix(1)` lands on item **1** — passing item 0's single
    /// unit lands *past* it.  An item whose cumulative sum exactly equals
    /// `n` is skipped, not returned.
    pub fn advance_prefix(&mut self, n: T::Prefix) -> Option<PrefixSeek<'a, T>> {
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
    inner: IterState,
    total: T::Prefix,
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
            total: self.total.clone(),
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
    V: crate::AsColumnRef<T>,
{
    fn extend<I: IntoIterator<Item = V>>(&mut self, iter: I) {
        let len = self.col.len();
        self.col.splice(len, 0, iter);
    }
}

impl<'a, T: PrefixValue> IntoIterator for &'a PrefixColumn<T> {
    type Item = PrefixedValue<'a, T>;
    type IntoIter = PrefixIter<'a, T>;

    fn into_iter(self) -> PrefixIter<'a, T> {
        self.iter()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::prefix::PrefixColumn;

    #[test]
    fn iter_total_is_prefix_at_range_start() {
        let values: Vec<u64> = (0..500).map(|i| i % 7).collect();
        let col = PrefixColumn::<u64>::from_values(values);
        for i in 0..=col.len() {
            assert_eq!(
                col.iter_range(i..col.len()).total(),
                col.get_prefix(i),
                "iter_range({i}..).total() != get_prefix({i})",
            );
        }
    }

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
                    tree.sum_range(lo..hi),
                    col.sum_range(lo..hi),
                    "sum_range({lo}..{hi}) mismatch",
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
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xCAFE);
        let init: Vec<u64> = (0..50).map(|_| rng.random_range(1..=100)).collect();
        let mut col = PrefixColumn::<u64>::from_values(init.clone());
        let mut tree = PrefixColumn::<u64>::from_values(init);

        for _ in 0..500 {
            let op = rng.random_range(0..3);
            let len = col.len();
            match op {
                0 => {
                    let at = rng.random_range(0..=len);
                    let v = rng.random_range(1..=100);
                    col.insert(at, v);
                    tree.insert(at, v);
                }
                1 if len > 0 => {
                    let at = rng.random_range(0..len);
                    col.remove(at);
                    tree.remove(at);
                }
                _ if len > 0 => {
                    let at = rng.random_range(0..len);
                    let del = rng.random_range(1..=(len - at).min(4));
                    let count = rng.random_range(1..=4);
                    let new: Vec<u64> = (0..count).map(|_| rng.random_range(1..=100)).collect();
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
