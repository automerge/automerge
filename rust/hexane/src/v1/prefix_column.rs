use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Mul, Range, Sub, SubAssign};

use super::column::{find_slab_bit, Column, Iter, Slab, SlabWeight, WeightFn};
use super::ColumnValueRef;

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
        + AddAssign
        + SubAssign
        + TryFrom<usize>;

    /// Convert one column value to its prefix contribution.
    fn to_prefix(val: Self::Get<'_>) -> Self::Prefix;

    /// Sum all values in a slab.  Walks the encoded runs directly for
    /// efficiency — O(segments) rather than O(items).
    fn slab_sum(data: &[u8], len: usize) -> Self::Prefix;

    /// Compute the partial prefix sum of the first `count` items in a slab,
    /// returning `(prefix_sum, items_consumed)`.
    fn partial_sum(data: &[u8], count: usize) -> Self::Prefix;

    /// Find the first index within a slab where the running sum reaches or
    /// exceeds `target`.  Returns `(index_within_slab, remaining_prefix)`.
    /// If the entire slab's sum is less than `target`, returns `(len, target - slab_sum)`.
    fn find_prefix_in_slab(data: &[u8], len: usize, target: Self::Prefix) -> (usize, Self::Prefix);
}

// ── Compound weight ──────────────────────────────────────────────────────────

/// A BIT node value that carries both item count and prefix sum.
///
/// This allows a single Fenwick tree to support both O(log S) position
/// queries (via the `len` component) and O(log S) prefix-sum queries
/// (via the `prefix` component).
#[derive(Copy, Clone, Default, Debug)]
pub(crate) struct PrefixSlabWeight<P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign> {
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
pub(crate) struct PrefixWeightFn<T>(PhantomData<fn() -> T>);

impl<T: PrefixValue> WeightFn<T> for PrefixWeightFn<T> {
    type Weight = PrefixSlabWeight<T::Prefix>;

    #[inline]
    fn compute(slab: &Slab) -> PrefixSlabWeight<T::Prefix> {
        PrefixSlabWeight {
            len: slab.len,
            prefix: T::slab_sum(&slab.data, slab.len),
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

    pub fn len(&self) -> usize {
        self.col.len()
    }

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

    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    /// Serialize the column by appending bytes to `out`.
    ///
    /// Returns the byte range written (`out[range]` is the serialized data).
    pub fn save_to(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.col.save_to(out)
    }

    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    pub(crate) fn slab_data(&self) -> Vec<Vec<u8>> {
        self.col.slab_data()
    }

    pub fn validate_encoding(&self) {
        self.col.validate_encoding()
    }

    // ── Mutations (compound BIT maintained automatically) ────────────────

    pub fn insert(&mut self, index: usize, value: impl super::AsColumnRef<T>) {
        self.col.insert(index, value);
    }

    pub fn remove(&mut self, index: usize) {
        self.col.remove(index);
    }

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
    /// Panics if `index >= len`.
    pub fn get_total(&self, index: usize) -> T::Prefix {
        self.get_prefix(index + 1)
    }

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

    /// Returns the exclusive sum of values at indices `0..index` (before `index`).
    ///
    /// `get_prefix(0)` returns `Default::default()` (zero).
    /// `get_prefix(len)` returns the sum of all values.
    pub fn get_prefix(&self, index: usize) -> T::Prefix {
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
        let partial = T::partial_sum(&self.col.slabs[si].data, items_in_slab);
        prefix_before + partial
    }

    /// Find the first index where the prefix sum reaches or exceeds `target`.
    ///
    /// Returns `self.len()` if the total sum is less than `target`.
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
            return self.col.len();
        }

        let remaining = target - prefix_before;
        let slab = &self.col.slabs[si];
        let (idx_in_slab, _) = T::find_prefix_in_slab(&slab.data, slab.len, remaining);

        // Use the compound BIT to count items before this slab in O(log S).
        let items_before = if si > 0 { self.len_query(si - 1) } else { 0 };
        items_before + idx_in_slab
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
    pub(crate) fn inner(&self) -> &Column<T, PrefixWeightFn<T>> {
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
            col: self,
            inner: self.col.iter(),
            total: T::Prefix::default(),
            pos: 0,
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
            col: self,
            inner: self.col.iter_range(start..end),
            total: prefix_before,
            pos: start,
        }
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

    /// Collect all values into a Vec (without prefix sums).
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.col.to_vec()
    }
}

// ── RLE slab walking helpers ─────────────────────────────────────────────────

/// Decode one signed LEB128 from `data`. Returns `(bytes_read, value)`.
fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::signed(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

/// Decode one unsigned LEB128 from `data`. Returns `(bytes_read, value)`.
fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

// ── PrefixValue impls for RLE types ──────────────────────────────────────────

/// Walk an RLE slab, calling `f(value_or_none, count)` for each run.
/// Returns the accumulated result.
fn walk_rle_runs<T, P, F>(
    data: &[u8],
    value_len_fn: fn(&[u8]) -> Option<usize>,
    unpack_fn: fn(&[u8]) -> Option<(usize, T)>,
    mut f: F,
) -> P
where
    P: Copy + Default + AddAssign,
    F: FnMut(Option<T>, usize) -> P,
{
    let mut byte_pos = 0;
    let mut acc = P::default();

    while byte_pos < data.len() {
        let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                // Repeat run
                let count = n as usize;
                let vs = byte_pos + cb;
                let vl = value_len_fn(&data[vs..]).unwrap_or(0);
                let val = unpack_fn(&data[vs..]).map(|(_, v)| v);
                acc += f(val, count);
                byte_pos = vs + vl;
            }
            n if n < 0 => {
                // Literal run
                let total = (-n) as usize;
                let mut scan = byte_pos + cb;
                for _ in 0..total {
                    let (vl, val) = match unpack_fn(&data[scan..]) {
                        Some(v) => v,
                        None => break,
                    };
                    acc += f(Some(val), 1);
                    scan += vl;
                }
                byte_pos = scan;
            }
            _ => {
                // Null run
                let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                    Some(v) => v,
                    None => break,
                };
                acc += f(None, null_count as usize);
                byte_pos += cb + ncb;
            }
        }
    }
    acc
}

/// Walk an RLE slab, accumulating prefix sum for the first `limit` items.
fn walk_rle_partial<T, P, F>(
    data: &[u8],
    limit: usize,
    value_len_fn: fn(&[u8]) -> Option<usize>,
    unpack_fn: fn(&[u8]) -> Option<(usize, T)>,
    mut val_to_prefix: F,
) -> P
where
    P: Copy + Default + AddAssign,
    F: FnMut(Option<T>) -> P,
{
    let mut byte_pos = 0;
    let mut items = 0usize;
    let mut acc = P::default();

    while byte_pos < data.len() && items < limit {
        let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let vs = byte_pos + cb;
                let vl = value_len_fn(&data[vs..]).unwrap_or(0);
                let val = unpack_fn(&data[vs..]).map(|(_, v)| v);
                let take = count.min(limit - items);
                let one = val_to_prefix(val);
                for _ in 0..take {
                    acc += one;
                }
                items += take;
                byte_pos = vs + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut scan = byte_pos + cb;
                for _ in 0..total {
                    if items >= limit {
                        break;
                    }
                    let (vl, val) = match unpack_fn(&data[scan..]) {
                        Some(v) => v,
                        None => break,
                    };
                    acc += val_to_prefix(Some(val));
                    items += 1;
                    scan += vl;
                }
                byte_pos = scan;
            }
            _ => {
                let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                    Some(v) => v,
                    None => break,
                };
                let take = (null_count as usize).min(limit - items);
                // Nulls contribute default (zero).
                items += take;
                byte_pos += cb + ncb;
            }
        }
    }
    acc
}

/// Find the first index in an RLE slab where the running u128 sum >= target.
///
/// Uses O(1) ceiling division for repeat runs instead of per-item iteration.
fn find_prefix_rle_u128(
    data: &[u8],
    len: usize,
    target: u128,
    value_len_fn: fn(&[u8]) -> Option<usize>,
    unpack_fn: fn(&[u8]) -> Option<(usize, u64)>,
) -> (usize, u128) {
    let mut byte_pos = 0;
    let mut items = 0usize;
    let mut acc = 0u128;

    while byte_pos < data.len() && items < len {
        let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let vs = byte_pos + cb;
                let vl = value_len_fn(&data[vs..]).unwrap_or(0);
                let one = unpack_fn(&data[vs..]).map(|(_, v)| v as u128).unwrap_or(0);
                let run_total = one * count as u128;
                if one == 0 || acc + run_total < target {
                    acc += run_total;
                    items += count;
                } else {
                    let remaining = target - acc;
                    let needed = remaining.div_ceil(one) as usize;
                    let needed = needed.min(count);
                    items += needed;
                    acc += one * needed as u128;
                    return (items, acc - target);
                }
                byte_pos = vs + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut scan = byte_pos + cb;
                for _ in 0..total {
                    let (vl, val) = match unpack_fn(&data[scan..]) {
                        Some(v) => v,
                        None => break,
                    };
                    acc += val as u128;
                    items += 1;
                    scan += vl;
                    if acc >= target {
                        return (items, acc - target);
                    }
                }
                byte_pos = scan;
            }
            _ => {
                let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                    Some(v) => v,
                    None => break,
                };
                items += null_count as usize;
                byte_pos += cb + ncb;
            }
        }
    }
    (items, target.saturating_sub(acc))
}

/// Find the first index in an RLE slab where the running i128 sum >= target.
fn find_prefix_rle_i128(
    data: &[u8],
    len: usize,
    target: i128,
    value_len_fn: fn(&[u8]) -> Option<usize>,
    unpack_fn: fn(&[u8]) -> Option<(usize, i64)>,
) -> (usize, i128) {
    let mut byte_pos = 0;
    let mut items = 0usize;
    let mut acc = 0i128;

    while byte_pos < data.len() && items < len {
        let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let vs = byte_pos + cb;
                let vl = value_len_fn(&data[vs..]).unwrap_or(0);
                let one = unpack_fn(&data[vs..]).map(|(_, v)| v as i128).unwrap_or(0);
                let run_total = one * count as i128;
                if one == 0 || acc + run_total < target {
                    acc += run_total;
                    items += count;
                } else if one > 0 {
                    let remaining = target - acc;
                    let needed = ((remaining + one - 1) / one) as usize;
                    let needed = needed.min(count);
                    items += needed;
                    acc += one * needed as i128;
                    return (items, acc - target);
                } else {
                    // Negative values: walk item by item (rare in practice)
                    for _ in 0..count {
                        acc += one;
                        items += 1;
                        if acc >= target {
                            return (items, acc - target);
                        }
                    }
                }
                byte_pos = vs + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut scan = byte_pos + cb;
                for _ in 0..total {
                    let (vl, val) = match unpack_fn(&data[scan..]) {
                        Some(v) => v,
                        None => break,
                    };
                    acc += val as i128;
                    items += 1;
                    scan += vl;
                    if acc >= target {
                        return (items, acc - target);
                    }
                }
                byte_pos = scan;
            }
            _ => {
                let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                    Some(v) => v,
                    None => break,
                };
                items += null_count as usize;
                byte_pos += cb + ncb;
            }
        }
    }
    (items, if target > acc { target - acc } else { 0 })
}

// ── u64 impl ─────────────────────────────────────────────────────────────────

fn u64_value_len(data: &[u8]) -> Option<usize> {
    <Option<u64> as super::RleValue>::value_len(data)
}

fn u64_unpack(data: &[u8]) -> Option<(usize, u64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

impl PrefixValue for u64 {
    type Prefix = u128;

    fn to_prefix(val: u64) -> u128 {
        val as u128
    }

    fn slab_sum(data: &[u8], _len: usize) -> u128 {
        walk_rle_runs(data, u64_value_len, u64_unpack, |val, count| {
            val.unwrap_or(0) as u128 * count as u128
        })
    }

    fn partial_sum(data: &[u8], count: usize) -> u128 {
        walk_rle_partial(data, count, u64_value_len, u64_unpack, |val| {
            val.unwrap_or(0) as u128
        })
    }

    fn find_prefix_in_slab(data: &[u8], len: usize, target: u128) -> (usize, u128) {
        find_prefix_rle_u128(data, len, target, u64_value_len, u64_unpack)
    }
}

// ── Option<u64> impl ─────────────────────────────────────────────────────────

impl PrefixValue for Option<u64> {
    type Prefix = u128;

    fn to_prefix(val: Option<u64>) -> u128 {
        val.unwrap_or(0) as u128
    }

    fn slab_sum(data: &[u8], _len: usize) -> u128 {
        walk_rle_runs(data, u64_value_len, u64_unpack, |val, count| {
            val.unwrap_or(0) as u128 * count as u128
        })
    }

    fn partial_sum(data: &[u8], count: usize) -> u128 {
        walk_rle_partial(data, count, u64_value_len, u64_unpack, |val| {
            val.unwrap_or(0) as u128
        })
    }

    fn find_prefix_in_slab(data: &[u8], len: usize, target: u128) -> (usize, u128) {
        find_prefix_rle_u128(data, len, target, u64_value_len, u64_unpack)
    }
}

// ── i64 impl ─────────────────────────────────────────────────────────────────

fn i64_value_len(data: &[u8]) -> Option<usize> {
    <Option<i64> as super::RleValue>::value_len(data)
}

fn i64_unpack(data: &[u8]) -> Option<(usize, i64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::signed(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

impl PrefixValue for i64 {
    type Prefix = i128;

    fn to_prefix(val: i64) -> i128 {
        val as i128
    }

    fn slab_sum(data: &[u8], _len: usize) -> i128 {
        walk_rle_runs(data, i64_value_len, i64_unpack, |val, count| {
            val.unwrap_or(0) as i128 * count as i128
        })
    }

    fn partial_sum(data: &[u8], count: usize) -> i128 {
        walk_rle_partial(data, count, i64_value_len, i64_unpack, |val| {
            val.unwrap_or(0) as i128
        })
    }

    fn find_prefix_in_slab(data: &[u8], len: usize, target: i128) -> (usize, i128) {
        find_prefix_rle_i128(data, len, target, i64_value_len, i64_unpack)
    }
}

// ── Option<i64> impl ─────────────────────────────────────────────────────────

impl PrefixValue for Option<i64> {
    type Prefix = i128;

    fn to_prefix(val: Option<i64>) -> i128 {
        val.unwrap_or(0) as i128
    }

    fn slab_sum(data: &[u8], _len: usize) -> i128 {
        walk_rle_runs(data, i64_value_len, i64_unpack, |val, count| {
            val.unwrap_or(0) as i128 * count as i128
        })
    }

    fn partial_sum(data: &[u8], count: usize) -> i128 {
        walk_rle_partial(data, count, i64_value_len, i64_unpack, |val| {
            val.unwrap_or(0) as i128
        })
    }

    fn find_prefix_in_slab(data: &[u8], len: usize, target: i128) -> (usize, i128) {
        find_prefix_rle_i128(data, len, target, i64_value_len, i64_unpack)
    }
}

// ── bool impl ────────────────────────────────────────────────────────────────

/// Read one unsigned LEB128 count from boolean slab data.
fn bool_read_count(data: &[u8]) -> Option<(usize, usize)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v as usize))
}

impl PrefixValue for bool {
    type Prefix = u32;

    fn to_prefix(val: bool) -> u32 {
        val as u32
    }

    fn slab_sum(data: &[u8], _len: usize) -> u32 {
        // Walk alternating runs starting with false. Sum the true run counts.
        let mut byte_pos = 0;
        let mut value = false;
        let mut sum = 0u32;
        while byte_pos < data.len() {
            let (cb, count) = match bool_read_count(&data[byte_pos..]) {
                Some(v) => v,
                None => break,
            };
            if value {
                sum += count as u32;
            }
            byte_pos += cb;
            value = !value;
        }
        sum
    }

    fn partial_sum(data: &[u8], count: usize) -> u32 {
        let mut byte_pos = 0;
        let mut value = false;
        let mut items = 0usize;
        let mut sum = 0u32;
        while byte_pos < data.len() && items < count {
            let (cb, run_count) = match bool_read_count(&data[byte_pos..]) {
                Some(v) => v,
                None => break,
            };
            let take = run_count.min(count - items);
            if value {
                sum += take as u32;
            }
            items += take;
            byte_pos += cb;
            value = !value;
        }
        sum
    }

    fn find_prefix_in_slab(data: &[u8], len: usize, target: u32) -> (usize, u32) {
        // We're counting trues. Walk runs; only true runs contribute.
        let mut byte_pos = 0;
        let mut value = false;
        let mut items = 0usize;
        let mut acc = 0u32;
        while byte_pos < data.len() && items < len {
            let (cb, run_count) = match bool_read_count(&data[byte_pos..]) {
                Some(v) => v,
                None => break,
            };
            if value {
                // Each item adds 1. Use O(1) arithmetic.
                let remaining = target - acc;
                let needed = (remaining as usize).min(run_count);
                acc += needed as u32;
                items += needed;
                if acc >= target {
                    return (items, acc - target);
                }
                // Full run consumed but target not reached.
                items += run_count - needed;
            } else {
                items += run_count;
            }
            byte_pos += cb;
            value = !value;
        }
        (items, target.saturating_sub(acc))
    }
}

// ── Default-valued PrefixColumn ──────────────────────────────────────────────

impl<T: PrefixValue> PrefixColumn<T> {
    /// Deserialize with options. See [`LoadOpts`](super::LoadOpts).
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T>) -> Result<Self, crate::PackError> {
        let col = Column::<T, PrefixWeightFn<T>>::load_with(data, opts)?;
        Ok(Self { col })
    }

    /// Returns `true` if every item has the default value.
    pub fn is_default(&self) -> bool {
        self.col.is_default()
    }

    /// Create a column of `len` default values.
    pub fn init_default(len: usize) -> Self {
        Self {
            col: Column::fill_inner(len, T::Get::default()),
        }
    }

    /// Serialize unless all values are the default.
    pub fn save_to_unless_default(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.col.save_to_unless_default(out)
    }
}

// ── PrefixIter ───────────────────────────────────────────────────────────────

/// Forward iterator over a [`PrefixColumn`] that yields `(prefix_sum, value)`.
///
/// Created by [`PrefixColumn::iter`] or [`PrefixColumn::iter_range`].
///
/// - `next()` is O(1): accumulates the total from the yielded value.
/// - `nth(n)` is O(log S + runs): skips slabs via the inner iterator, then
///   recomputes the total from the Fenwick tree.
/// - [`advance_total`](PrefixIter::advance_total) advances by prefix-sum
///   value instead of item count, using O(log S) BIT binary lifting.
///
/// Each yielded item is `(total, value)` where `total` is the inclusive
/// sum of all values through the current item (`0..=pos`).
pub struct PrefixIter<'a, T: PrefixValue> {
    col: &'a PrefixColumn<T>,
    inner: Iter<'a, T>,
    total: T::Prefix,
    pos: usize,
}

impl<T: PrefixValue> Clone for PrefixIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            col: self.col,
            inner: self.inner.clone(),
            total: self.total,
            pos: self.pos,
        }
    }
}

impl<T: PrefixValue> std::fmt::Debug for PrefixIter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefixIter")
            .field("total", &self.total)
            .field("pos", &self.pos)
            .finish()
    }
}

impl<'a, T: PrefixValue> Iterator for PrefixIter<'a, T> {
    type Item = (T::Prefix, T::Get<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let val = self.inner.next()?;
        self.total += T::to_prefix(val);
        self.pos += 1;
        Some((self.total, val))
    }

    /// O(log S + runs) — single BIT traversal combining slab lookup and
    /// prefix accumulation via `find_slab_with_prefix`.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        use super::encoding::ColumnEncoding;

        if n >= self.inner.items_left {
            self.inner.pos += self.inner.items_left;
            self.inner.items_left = 0;
            return None;
        }

        // Fast path: target is within the current slab.
        if n < self.inner.slab_remaining {
            let val = self.inner.decoder.nth(n)?;
            self.inner.items_left -= n + 1;
            self.inner.slab_remaining -= n + 1;
            self.inner.pos += n + 1;
            self.pos += n + 1;
            self.total = self.col.get_prefix(self.pos);
            return Some((self.total, val));
        }

        // Combined BIT traversal: find slab + accumulate prefix in one pass.
        let target_pos = self.inner.pos + n;
        let (si, offset, prefix_before) = self.col.find_slab_with_prefix(target_pos);
        if si >= self.inner.slabs.len() {
            self.inner.pos += self.inner.items_left;
            self.inner.items_left = 0;
            return None;
        }

        let slab = &self.inner.slabs[si];
        let mut decoder = T::Encoding::decoder(&slab.data);
        let val = decoder.nth(offset)?;
        let partial = T::partial_sum(&slab.data, offset + 1);

        let skipped = n + 1;
        self.inner.slab_idx = si;
        self.inner.decoder = decoder;
        self.inner.items_left -= skipped;
        self.inner.slab_remaining = slab.len - offset - 1;
        self.inner.pos = target_pos + 1;
        self.pos = target_pos + 1;
        self.total = prefix_before + partial;

        Some((self.total, val))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: PrefixValue> ExactSizeIterator for PrefixIter<'_, T> {}

impl<'a, T: PrefixValue> PrefixIter<'a, T> {
    /// Returns the index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Returns the next run of identical values, along with the inclusive
    /// total at the *end* of the run.
    ///
    /// See [`super::Run`] for run semantics.
    pub fn next_run(&mut self) -> Option<super::Run<(T::Prefix, T::Get<'a>)>> {
        let run = self.inner.next_run()?;
        let count = T::Prefix::try_from(run.count).ok().unwrap();
        self.total += T::to_prefix(run.value) * count;
        self.pos += run.count;
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
        assert!(
            range.start >= self.pos,
            "shift_next: range.start ({}) < pos ({})",
            range.start,
            self.pos,
        );
        self.inner.items_left = range.end.saturating_sub(self.pos);
        self.nth(range.start - self.pos)
    }

    /// Advance the iterator until the inclusive total has increased by at
    /// least `val`, and return that item.
    ///
    /// This is like `nth()` but counts in prefix-sum units instead of items.
    /// Uses O(log S) BIT binary lifting to find the target slab, then
    /// O(log S) BIT seek to position the iterator at the target.
    ///
    /// Returns `None` (and exhausts the iterator) if the remaining items
    /// cannot produce enough sum to reach the target.
    pub fn advance_total(&mut self, val: T::Prefix) -> Option<(T::Prefix, T::Get<'a>)> {
        if val <= T::Prefix::default() {
            return self.next();
        }
        let target = self.total + val;
        let target_pos = self.col.get_index_for_prefix(target);

        if target_pos <= self.pos {
            return self.next();
        }

        // Check if target is reachable.
        let total_len = self.col.len();
        if target_pos >= total_len {
            let col_total = self.col.get_prefix(total_len);
            if col_total < target {
                // Target unreachable — exhaust the iterator.
                let remaining = self.inner.len();
                let _ = self.inner.nth(remaining);
                self.pos = total_len;
                self.total = col_total;
                return None;
            }
        }

        // target_pos is 1-indexed (count of items consumed to reach target).
        // The item at index target_pos-1 caused the crossover.
        let item_idx = target_pos - 1;
        let remaining_end = self.pos + self.inner.len();

        // Use iter_range for O(log S) BIT seek.
        self.inner = self.col.inner().iter_range(item_idx..remaining_end);
        let val = self.inner.next()?;
        self.pos = item_idx + 1;
        self.total = self.col.get_prefix(self.pos);
        Some((self.total, val))
    }
}

// ── FromIterator ────────────────────────────────────────────────────────────

impl<T: PrefixValue> FromIterator<T> for PrefixColumn<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
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
        assert_eq!(col.get_index_for_prefix(6), 1);
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
        assert_eq!(col.get_index_for_prefix(16), 5); // beyond total
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
        let col = PrefixColumn::<bool>::from_values(vec![true, false, true, true, false]);
        // We want: first index where count_of_trues >= target
        assert_eq!(col.get_index_for_prefix(0), 0);
        assert_eq!(col.get_index_for_prefix(1), 1); // 1 true after index 0
        assert_eq!(col.get_index_for_prefix(2), 3); // 2 trues after index 2
        assert_eq!(col.get_index_for_prefix(3), 4); // 3 trues after index 3
        assert_eq!(col.get_index_for_prefix(4), 5); // only 3 trues total
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

    // ── advance_total tests ────────────────────────────────────────────

    #[test]
    fn advance_total_basic() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // Prefix sums: [0, 1, 3, 6, 10, 15]
        let mut iter = col.iter();
        // advance_total(6): find first item where cumulative >= 6
        // Item at index 2 (value 3) has prefix 6
        let result = iter.advance_total(6);
        assert_eq!(result, Some((6, 3)));
    }

    #[test]
    fn advance_total_mid_stream() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        let mut iter = col.iter();
        // Consume first item: prefix=1
        assert_eq!(iter.next(), Some((1, 1)));
        // advance_total(5): target = 1 + 5 = 6
        // Item at index 2 (value 3) has prefix 6
        let result = iter.advance_total(5);
        assert_eq!(result, Some((6, 3)));
        // Next should be index 3
        assert_eq!(iter.next(), Some((10, 4)));
    }

    #[test]
    fn advance_total_exact_match() {
        let col = PrefixColumn::<u64>::from_values(vec![5, 5, 5, 5]);
        let mut iter = col.iter();
        // advance_total(10): target = 10
        // prefix(0)=5, prefix(1)=10, so item at index 1 has prefix 10
        let result = iter.advance_total(10);
        assert_eq!(result, Some((10, 5)));
    }

    #[test]
    fn advance_total_unreachable() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        // Total prefix = 6
        let mut iter = col.iter();
        let result = iter.advance_total(100);
        assert_eq!(result, None);
        assert_eq!(iter.len(), 0); // exhausted
    }

    #[test]
    fn advance_total_zero() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
        let mut iter = col.iter();
        // advance_total(0) should behave like next()
        let result = iter.advance_total(0);
        assert_eq!(result, Some((1, 1)));
    }

    #[test]
    fn advance_total_with_zeros() {
        let col = PrefixColumn::<u64>::from_values(vec![0, 0, 5, 0, 3]);
        // Prefix: [0, 0, 0, 5, 5, 8]
        let mut iter = col.iter();
        // advance_total(3): target = 3
        // Item at index 2 (value 5) has prefix 5 >= 3
        let result = iter.advance_total(3);
        assert_eq!(result, Some((5, 5)));
        // Next = index 3
        assert_eq!(iter.next(), Some((5, 0)));
    }

    #[test]
    fn advance_total_bool() {
        let col = PrefixColumn::<bool>::from_values(vec![false, false, true, false, true, true]);
        // Prefix (count of trues): [0, 0, 0, 1, 1, 2, 3]
        let mut iter = col.iter();
        // advance_total(2): find first where true_count >= 2
        // Index 4 (true) has prefix 2
        let result = iter.advance_total(2);
        assert_eq!(result, Some((2, true)));
        // Next = index 5
        assert_eq!(iter.next(), Some((3, true)));
    }

    #[test]
    fn advance_total_multi_slab() {
        let mut col = PrefixColumn::<u64>::with_max_segments(4);
        for i in 0..20 {
            col.insert(i, (i + 1) as u64);
        }
        assert!(col.slab_count() > 1);
        let mut iter = col.iter();
        // advance_total(55): sum(1..=10) = 55
        // Item at index 9 (value 10) has prefix 55
        let result = iter.advance_total(55);
        assert_eq!(result, Some((55, 10)));
    }

    #[test]
    fn advance_total_sequential() {
        let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        // Prefix: [0, 10, 30, 60, 100, 150]
        let mut iter = col.iter();

        // advance_total(25): target = 25, crosses at index 1 (prefix 30)
        assert_eq!(iter.advance_total(25), Some((30, 20)));

        // advance_total(50): target = 30 + 50 = 80, crosses at index 3 (prefix 100)
        assert_eq!(iter.advance_total(50), Some((100, 40)));

        // advance_total(100): target = 100 + 100 = 200, unreachable (total = 150)
        assert_eq!(iter.advance_total(100), None);
    }

    #[test]
    fn advance_total_last_item() {
        let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
        // Total prefix = 15
        let mut iter = col.iter();
        // advance_total(15): target = 15, exactly at the last item
        let result = iter.advance_total(15);
        assert_eq!(result, Some((15, 5)));
        assert_eq!(iter.next(), None);
    }
}
