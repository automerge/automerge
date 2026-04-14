use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{AddAssign, Range, SubAssign};

use super::encoding::{ColumnEncoding, RunDecoder};
use super::{AsColumnRef, ColumnValueRef, TypedLoadOpts};
use crate::PackError;

/// Type alias for the slab tail metadata of a column value type.
pub type TailOf<T> = <<T as ColumnValueRef>::Encoding as ColumnEncoding>::Tail;

/// Default maximum number of RLE/bool segments per slab.
///
/// Slabs are loaded at half capacity (`max / 2`) to leave room for inserts
/// without overflowing. A splice that exceeds `max` triggers an overflow split.
pub const DEFAULT_MAX_SEG: usize = 32;

// ── Slab ─────────────────────────────────────────────────────────────────────

#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Slab<Tail: Copy + Clone + std::fmt::Debug + Default = ()> {
    pub(crate) data: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) tail: Tail,
}

impl<Tail: Copy + Clone + std::fmt::Debug + Default> Slab<Tail> {
    pub(crate) fn new(data: Vec<u8>, len: usize, segments: usize) -> Self {
        Self {
            data,
            len,
            segments,
            tail: Tail::default(),
        }
    }
}

// ── SlabWeight ───────────────────────────────────────────────────────────────

/// A value stored in a Fenwick tree (BIT) node.
///
/// For plain `Column`, this is `usize` (just the slab item count).
/// For columns that track prefix sums, this is a compound value carrying
/// both the item count and the slab's contribution to the prefix sum.
#[doc(hidden)]
pub trait SlabWeight: Copy + Default + std::fmt::Debug + AddAssign + SubAssign {
    /// The length (item count) component of this weight.
    fn len(&self) -> usize;

    /// Whether this weight represents zero items.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SlabWeight for usize {
    #[inline]
    fn len(&self) -> usize {
        *self
    }
}

// ── WeightFn ─────────────────────────────────────────────────────────────────

/// Strategy for computing a [`SlabWeight`] from a slab's data.
///
/// This is a zero-sized type parameter on [`Column`] that controls what
/// the Fenwick tree stores.  The default [`LenWeight`] records only item
/// counts.  Prefix-aware columns use a compound weight that also tracks
/// prefix sums in the same BIT.
#[doc(hidden)]
pub trait WeightFn<T: ColumnValueRef> {
    type Weight: SlabWeight;
    fn compute(slab: &Slab<TailOf<T>>) -> Self::Weight;
}

/// Default weight strategy: BIT stores only slab lengths.
#[doc(hidden)]
#[derive(Clone)]
pub struct LenWeight;

impl<T: ColumnValueRef> WeightFn<T> for LenWeight {
    type Weight = usize;
    #[inline]
    fn compute(slab: &Slab<TailOf<T>>) -> usize {
        slab.len
    }
}

// ── Fenwick tree helpers ─────────────────────────────────────────────────────

/// Rebuild BIT from scratch. O(S).
/// The BIT is 1-indexed: `bit\[0\]` is unused, `bit\[1..=n\]` holds the tree.
///
/// Generic over the slab type and weight: callers pass a closure that extracts
/// the per-slab weight. `Column` passes `WF::compute`; other column types
/// (e.g. the raw-byte arena) pass their own slab-length extractor.
pub(crate) fn rebuild_bit<S, W: SlabWeight>(slabs: &[S], weight: impl Fn(&S) -> W) -> Vec<W> {
    let n = slabs.len();
    let mut bit = vec![W::default(); n + 1];
    for i in 0..n {
        bit[i + 1] = weight(&slabs[i]);
    }
    // Standard O(n) BIT construction: propagate to parent.
    for i in 1..=n {
        let parent = i + (i & i.wrapping_neg());
        if parent <= n {
            let child = bit[i];
            bit[parent] += child;
        }
    }
    bit
}

/// Update the BIT at slab index `si` after a weight change. O(log S).
///
/// Subtracts the old weight and adds the new weight at every ancestor node.
#[inline]
pub(crate) fn bit_point_update<W: SlabWeight>(bit: &mut [W], si: usize, old: W, new: W) {
    let mut i = si + 1; // BIT is 1-indexed
    while i < bit.len() {
        bit[i] -= old;
        bit[i] += new;
        i += i & i.wrapping_neg();
    }
}

/// Walk the four slab boundaries around a recently-modified range and attempt
/// to merge undersized neighbours.  The decision of *whether* two adjacent
/// slabs can merge is column-specific (encoding-aware for RLE, byte-count-only
/// for a raw arena); the caller passes `try_merge_pair(a, b)` which returns
/// `true` if it actually merged slabs `a` and `b` (removing slab `b`).
///
/// Returns the adjusted range accounting for any merges that happened — the
/// end shrinks by one for every merge that collapses two in-range slabs.
pub(crate) fn try_merge_range_skeleton(
    range: Range<usize>,
    mut try_merge_pair: impl FnMut(usize, usize) -> bool,
) -> Range<usize> {
    let mut start = range.start;
    let mut end = range.end;

    if !range.is_empty() {
        // external right
        // [ . [. . . A] B .] -> [. [. . . AB] .]
        //   0  1 2 3 4  5 6      0  1 2 3 4   5
        try_merge_pair(end - 1, end);
        // internal left
        // [ . [. . B A] . .] -> [. [. . BA] .]
        //   0  1 2 3 4  5 6      0  1 2 3   4
        if range.len() > 1 && end > 2 && try_merge_pair(end - 2, end - 1) {
            end -= 1;
        }
        // internal right
        // [ . [A B . .] . .] -> [ . [AB . .] . .]
        //   0  1 2 3 4  5 6       0  1  2 3  4 5
        if (start..end).len() > 1 && try_merge_pair(start, start + 1) {
            end -= 1;
        }
        // external left
        // [ B [A . . .] . .] -> [ [BA . . .] . .]
        //   0  1 2 3 4  5 6        0  1 2 3  4 5
        if start > 1 && try_merge_pair(start - 1, start) {
            start -= 1;
            end -= 1;
        }
    }

    start..end
}

/// Trait for types that can locate the slab containing a logical item index.
///
/// This abstracts the index structure (Fenwick tree, B-tree, etc.) behind the
/// Column so that iterators don't depend on a specific implementation.
pub(crate) trait SlabFind {
    /// Find the slab containing logical `index`.
    /// Returns `(slab_index, offset_within_slab)`.
    fn find_slab(&self, index: usize) -> (usize, usize);
}

impl SlabFind for () {
    fn find_slab(&self, _index: usize) -> (usize, usize) {
        (0, 0)
    }
}

impl<T: ColumnValueRef, WF: WeightFn<T>> SlabFind for Column<T, WF> {
    #[inline]
    fn find_slab(&self, index: usize) -> (usize, usize) {
        find_slab_bit(&self.bit, index, self.slabs.len())
    }
}

/// Find slab containing logical index. Returns (slab_index, offset_within_slab). O(log S).
/// Uses binary lifting on the BIT.
#[inline]
pub(crate) fn find_slab_bit<W: SlabWeight>(bit: &[W], index: usize, n: usize) -> (usize, usize) {
    if n == 0 {
        return (0, 0);
    }
    let mut pos = 0usize;
    let mut idx = 0usize;
    let mut bit_k = 1;
    while bit_k <= n {
        bit_k <<= 1;
    }
    bit_k >>= 1;
    while bit_k > 0 {
        let next = idx + bit_k;
        if next <= n && pos + bit[next].len() <= index {
            pos += bit[next].len();
            idx = next;
        }
        bit_k >>= 1;
    }
    (idx, index - pos)
}

// ── Column ──────────────────────────────────────────────────────────────

impl<T: ColumnValueRef, WF: WeightFn<T>> std::fmt::Debug for Column<T, WF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("len", &self.total_len)
            .field("slabs", &self.slabs.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct Column<T: ColumnValueRef, WF: WeightFn<T> = LenWeight> {
    pub(crate) slabs: Vec<Slab<TailOf<T>>>,
    pub(crate) bit: Vec<WF::Weight>,
    pub(crate) total_len: usize,
    pub(crate) max_segments: usize,
    pub(crate) counter: usize,
    _phantom: PhantomData<fn() -> (T, WF)>,
}

impl<T: ColumnValueRef, WF: WeightFn<T>> Default for Column<T, WF> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ColumnValueRef, WF: WeightFn<T>> Column<T, WF> {
    /// Create an empty column with the default segment budget.
    pub fn new() -> Self {
        Self::with_max_segments(DEFAULT_MAX_SEG)
    }

    /// Create an empty column with a custom segment budget per slab.
    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            slabs: Vec::new(),
            bit: vec![WF::Weight::default()],
            total_len: 0,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        }
    }

    /// Build from pre-existing slabs, computing the BIT via `WF`.
    pub(crate) fn from_slabs(
        slabs: Vec<Slab<TailOf<T>>>,
        total_len: usize,
        max_segments: usize,
    ) -> Self {
        let bit = rebuild_bit(&slabs, WF::compute);
        Self {
            slabs,
            bit,
            total_len,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        }
    }

    /// Create a column of `len` copies of `value`. O(1) — writes a single
    /// run regardless of `len`.
    pub fn fill(len: usize, value: impl AsColumnRef<T>) -> Self {
        if len == 0 {
            return Self::new();
        }
        Self::fill_inner(len, value.as_column_ref())
    }

    pub(crate) fn fill_inner(len: usize, value: T::Get<'_>) -> Self {
        let slab = T::Encoding::fill(len, value);
        let bit = rebuild_bit(std::slice::from_ref(&slab), WF::compute);
        Self {
            slabs: vec![slab],
            bit,
            total_len: len,
            max_segments: DEFAULT_MAX_SEG,
            counter: 0,
            _phantom: PhantomData,
        }
    }

    /// Bulk-construct a column from a Vec of values.
    ///
    /// Much faster than repeated `insert` calls — encodes all values in a
    /// single O(n) pass and builds the slab tree in one shot.
    pub fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_max_segments(values, DEFAULT_MAX_SEG)
    }

    /// Bulk-construct with a custom segment budget per slab.
    pub fn from_values_with_max_segments(values: Vec<T>, max_segments: usize) -> Self {
        let mut col = Self::with_max_segments(max_segments);
        col.splice(0, 0, values);
        col
    }

    /// Deserialize a column from bytes produced by [`save`](Column::save).
    ///
    /// Validates the wire encoding: malformed LEB128, truncated data, and
    /// values that don't decode cleanly all return [`PackError`].  For
    /// non-nullable column types (`Column<u64>`, `Column<String>`, …),
    /// null runs are rejected with [`PackError::InvalidValue`].
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_verified(data, DEFAULT_MAX_SEG, None)
    }

    /// Build a column from the output of `load_and_verify`.
    fn load_verified(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(T::Get<'a>) -> Option<String>>,
    ) -> Result<Self, PackError> {
        let slabs = T::Encoding::load_and_verify(data, max_segments, validate)?;
        let total_len: usize = slabs.iter().map(|s| s.len).sum();
        let bit = rebuild_bit(&slabs, WF::compute);
        Ok(Self {
            slabs,
            bit,
            total_len,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        })
    }

    /// Total number of items in the column.
    pub fn len(&self) -> usize {
        self.total_len
    }

    /// Returns `true` if the column contains no items.
    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Number of slabs in the column.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }

    /// Iterator over raw slab data (for debugging/testing).
    pub fn slab_data(&self) -> Vec<Vec<u8>> {
        self.slabs.iter().map(|s| s.data.to_vec()).collect()
    }

    /// Returns `(len, segments)` for each slab (for debugging/testing).
    pub fn slab_info(&self) -> Vec<(usize, usize)> {
        self.slabs.iter().map(|s| (s.len, s.segments)).collect()
    }

    /// Validate that the canonical encoding (`save()`) is well-formed.
    ///
    /// Returns `Ok(())` if the encoding is valid, or a [`PackError`] describing
    /// the violation.
    pub fn validate_encoding(&self) -> Result<(), PackError> {
        let bytes = self.save();
        T::Encoding::validate_encoding(&bytes)?;
        Ok(())
    }

    /// Validate the canonical encoding and return its slab info.
    ///
    /// Returns the slab metadata on success, or a [`PackError`] if invalid.
    pub fn validate_encoding_info(
        &self,
    ) -> Result<super::encoding::SlabInfo<TailOf<T>>, PackError> {
        let bytes = self.save();
        T::Encoding::validate_encoding(&bytes)
    }

    /// Serialize all slabs into a single canonical byte array.
    ///
    /// Uses O(n) streaming serialization: memcopies slab interiors and only
    /// decodes/re-encodes boundary runs between adjacent slabs.
    pub fn save(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.save_to(&mut out);
        out
    }

    /// Serialize the column by appending bytes to `out`.
    ///
    /// Returns the byte range written (`out[range]` is the serialized data).
    /// Merges slabs directly into `out` with no intermediate allocation.
    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        if let Some(first) = self.slabs.first() {
            out.extend_from_slice(&first.data);
            let mut tail = first.tail;
            let mut segments = first.segments;
            let mut buf = Vec::new();
            for s in &self.slabs[1..] {
                let (new_seg, new_tail) = T::Encoding::do_merge(out, tail, segments, s, &mut buf);
                segments = new_seg;
                tail = new_tail;
                buf.clear();
            }
        }
        start..out.len()
    }

    /// Returns the value at `index`, or `None` if out of bounds.
    ///
    /// The return type is `T::Get<'_>`:
    /// - For `Copy` types (`u64`, `i64`, `bool`, `Option<u64>`, …): returns the value directly.
    /// - For ref types (`String`, `Vec<u8>`, …): borrows from the slab (`&str`, `&[u8]`, …).
    ///
    /// For nullable columns (`Column<Option<T>>`), an in-bounds null entry
    /// returns `Some(None)`.
    pub fn get(&self, index: usize) -> Option<T::Get<'_>> {
        self.iter().nth(index)
    }

    /// Inserts `value` at `index`, shifting subsequent elements right.
    /// Panics if `index > self.len()`.
    ///
    /// Accepts both owned and borrowed forms via [`AsColumnRef`]:
    /// ```ignore
    /// col.insert(0, "hello");       // &str for Column<String>
    /// col.insert(0, Some("hello")); // Option<&str> for Column<Option<String>>
    /// ```
    pub fn insert(&mut self, index: usize, value: impl AsColumnRef<T>) {
        self.splice(index, 0, [value]);
    }

    /// Removes the value at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len()`.
    pub fn remove(&mut self, index: usize) {
        self.splice(index, 1, std::iter::empty::<T>());
    }

    /// Appends `value` to the end of the column.
    pub fn push(&mut self, value: impl AsColumnRef<T>) {
        let len = self.total_len;
        self.insert(len, value);
    }

    /// Removes and returns the last element, or `None` if empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.total_len == 0 {
            return None;
        }
        let val = T::to_owned(self.get(self.total_len - 1)?);
        self.remove(self.total_len - 1);
        Some(val)
    }

    /// Returns the first element, or `None` if empty.
    pub fn first(&self) -> Option<T::Get<'_>> {
        self.get(0)
    }

    /// Returns the last element, or `None` if empty.
    pub fn last(&self) -> Option<T::Get<'_>> {
        if self.total_len == 0 {
            None
        } else {
            self.get(self.total_len - 1)
        }
    }

    /// Absolute position of the first item in slab `si`. O(log S) via BIT.
    fn slab_start(&self, si: usize) -> usize {
        if si == 0 {
            return 0;
        }
        // Sum lengths of slabs 0..si using the BIT.
        let mut sum = 0;
        let mut i = si; // BIT is 1-indexed, query prefix sum of slabs 0..si = BIT query(si)
        while i > 0 {
            sum += self.bit[i].len();
            i -= i & i.wrapping_neg();
        }
        sum
    }

    /// Removes all elements from the column.
    pub fn clear(&mut self) {
        self.slabs.clear();
        self.bit = vec![WF::Weight::default()];
        self.total_len = 0;
    }

    /// Shortens the column to `len` elements.
    ///
    /// If `len >= self.len()`, this is a no-op.
    pub fn truncate(&mut self, len: usize) {
        if len < self.total_len {
            self.splice(len, self.total_len - len, std::iter::empty::<T>());
        }
    }

    /// Removes `del` elements starting at `index` and inserts `values` in their place.
    /// Panics if `index + del > self.len()`.
    ///
    /// Accepts both owned and borrowed forms via [`AsColumnRef`]:
    /// ```ignore
    /// col.splice(0, 2, ["hello", "world"]); // &str items for Column<String>
    /// ```
    pub fn splice<V: AsColumnRef<T>>(
        &mut self,
        index: usize,
        del: usize,
        values: impl IntoIterator<Item = V>,
    ) {
        self.counter += 1;
        assert!(index + del <= self.total_len, "splice range out of bounds");

        let mut iter = values.into_iter().peekable();

        if del == 0 && iter.peek().is_none() {
            return;
        }

        if self.slabs.is_empty() {
            self.slabs.push(T::Encoding::empty_slab());
            self.bit = rebuild_bit(&self.slabs, WF::compute);
        }

        T::Encoding::splice(self, index, del, iter);

        #[cfg(debug_assertions)]
        self.validate_encoding().unwrap();
    }

    // ── Merge ────────────────────────────────────────────────────────────────

    pub(crate) fn try_merge(&mut self, index_a: usize, index_b: usize) -> bool {
        let max = self.max_segments;
        let min = self.max_segments / 4;
        if let Some(a) = self.slabs.get(index_a).map(|s| s.segments) {
            if let Some(b) = self.slabs.get(index_b).map(|s| s.segments) {
                if (a < min || b < min) && a + b <= max {
                    let slab_b = self.slabs.remove(index_b);
                    T::Encoding::merge_slabs(&mut self.slabs[index_a], slab_b);
                    return true;
                }
            }
        }
        false
    }

    /// Try merging at both boundaries of a slab range without rebuilding the BIT.
    /// Returns the adjusted range accounting for any merges that happened.
    pub(crate) fn try_merge_range(&mut self, range: Range<usize>) -> Range<usize> {
        try_merge_range_skeleton(range, |a, b| self.try_merge(a, b))
    }

    /// Returns a forward iterator over all items in the column.
    ///
    /// `nth()` is O(log S) — uses the column's index for slab lookup.
    pub fn iter(&self) -> Iter<'_, T> {
        if self.slabs.is_empty() {
            return Iter {
                slabs: &self.slabs,
                col: self,
                slab_idx: 0,
                decoder: T::Encoding::decoder(&[]),
                items_left: 0,
                slab_remaining: 0,
                pos: 0,
                counter: self.counter,
            };
        }
        Iter {
            slabs: &self.slabs,
            col: self,
            slab_idx: 0,
            decoder: T::Encoding::decoder(&self.slabs[0].data),
            items_left: self.total_len,
            slab_remaining: self.slabs[0].len,
            pos: 0,
            counter: self.counter,
        }
    }

    /// Returns a forward iterator over items in `range`, clamped to the column's length.
    ///
    /// Uses the index for O(log S) initial seek, then O(1) per item.
    pub fn iter_range(&self, range: Range<usize>) -> Iter<'_, T> {
        let start = range.start.min(self.total_len);
        let end = range.end.min(self.total_len);
        if start >= end || self.slabs.is_empty() {
            return Iter {
                slabs: &self.slabs,
                col: self,
                slab_idx: self.slabs.len(),
                decoder: T::Encoding::decoder(&[]),
                items_left: 0,
                slab_remaining: 0,
                pos: start,
                counter: self.counter,
            };
        }
        let (si, offset) = find_slab_bit(&self.bit, start, self.slabs.len());
        let mut decoder = T::Encoding::decoder(&self.slabs[si].data);
        if offset > 0 {
            decoder.nth(offset - 1);
        }
        Iter {
            slabs: &self.slabs,
            col: self,
            slab_idx: si,
            decoder,
            items_left: end - start,
            slab_remaining: self.slabs[si].len - offset,
            pos: start,
            counter: self.counter,
        }
    }

    /// Collect all values into a Vec (without prefix sums).
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.iter().collect()
    }
}

// ── Load / save with options ────────────────────────────────────────────────

impl<T: ColumnValueRef, WF: WeightFn<T>> Column<T, WF> {
    /// Deserialize with options: length validation, fill-on-empty, and
    /// value validation.
    ///
    /// See [`LoadOpts`](super::LoadOpts) for available options.
    pub fn load_with(data: &[u8], opts: TypedLoadOpts<T>) -> Result<Self, PackError> {
        if data.is_empty() {
            return match (opts.length, opts.fill) {
                (Some(0) | None, _) => Ok(Self::new()),
                (Some(len), Some(value)) => Ok(Self::fill_inner(len, value)),
                (Some(len), None) => Err(PackError::InvalidLength(0, len)),
            };
        }
        let col = Self::load_verified(data, opts.max_segments, opts.validate)?;
        if let Some(expected) = opts.length {
            if col.len() != expected {
                return Err(PackError::InvalidLength(col.len(), expected));
            }
        }
        Ok(col)
    }

    /// Returns `true` if the column is empty or every item equals `value`.
    pub fn is_only(&self, value: T::Get<'_>) -> bool {
        self.total_len == 0
            || self.slabs.iter().all(|s| {
                if s.len == 0 {
                    return true;
                }
                let mut dec = T::Encoding::decoder(&s.data);
                match dec.next_run() {
                    Some(run) => {
                        run.count == s.len && T::eq(run.value, value) && dec.next_run().is_none()
                    }
                    None => true,
                }
            })
    }

    /// Serialize the column by appending bytes to `out`, unless all values
    /// equal `value`.
    ///
    /// Returns the byte range written (empty range if all-equal or empty).
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: T::Get<'_>) -> Range<usize> {
        if self.is_only(value) {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    /// Narrow a range to the contiguous run of items matching `value`.
    ///
    /// Assumes values within `range` are sorted.  Behaviour is undefined
    /// if this precondition is violated.
    ///
    /// Returns the sub-range of `range` where every item equals `value`,
    /// or an empty range at the appropriate insertion point if `value` is
    /// not present.
    pub fn scope_to_value<V: AsColumnRef<T>>(
        &self,
        value: V,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord,
    {
        let (start, end) = crate::columndata::normalize_range_max(range, self.total_len);

        let target = value.as_column_ref();

        let mut iter = self.iter_range(start..end);
        let si_start = iter.get_slab();
        let first_run = match iter.next_run() {
            Some(r) => r,
            None => return start..start,
        };

        match first_run.value.cmp(&target) {
            Ordering::Equal => {
                assert!(start + first_run.count <= end);
                start..start + first_run.count
            }
            Ordering::Greater => start..start,
            Ordering::Less => {
                let (si_end, _) = self.find_slab(end - 1);

                // Binary search slabs si_start+1..=si_end by first element.
                let mut lo = si_start + 1;
                let mut hi = si_end + 1;
                let mut candidate = None;

                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let mut dec = T::Encoding::decoder(&self.slabs[mid].data);
                    let head = dec.next_run().unwrap();
                    match head.value.cmp(&target) {
                        Ordering::Less => {
                            candidate = Some(mid);
                            lo = mid + 1;
                        }
                        Ordering::Greater => hi = mid,
                        Ordering::Equal => {
                            // Target starts at (or before) this slab.
                            // Check if the prior slab's last run also matches.
                            let base = self.slab_start(mid);
                            assert!(base < end);
                            assert!(base >= start);
                            assert!(mid > 0);
                            let mut match_start = base;
                            if let Some(tail) = T::Encoding::last_run(&self.slabs[mid - 1]) {
                                if tail.value == target {
                                    match_start = base.saturating_sub(tail.count).max(start);
                                }
                            }
                            let match_end = (base + head.count).min(end);
                            return match_start..match_end;
                        }
                    }
                }

                // `candidate` is the last slab whose first element < target.
                // The target (if present) is at the end of this slab.
                let (mut decoder, pos) = match candidate {
                    Some(i) => {
                        let pos = self.slab_start(i);
                        let dec = T::Encoding::decoder(&self.slabs[i].data);
                        (dec, pos)
                    }
                    None => {
                        let pos = start + first_run.count;
                        (iter.unwrap_decoder(), pos)
                    }
                };
                let (skipped, count) = decoder.scan_for(target, end - pos);
                let begin = pos + skipped;
                begin..begin + count
            }
        }
    }
}

// ── Remap ────────────────────────────────────────────────────────────────────

impl<T: ColumnValueRef, WF: WeightFn<T>> Column<T, WF> {
    /// Replace this column with a re-encoded version where every item has been
    /// transformed by `f`.
    ///
    /// Walks runs (not items) so a single `f` call covers a whole repeating
    /// run.  For nullable columns (`Column<Option<T>>`), the function sees
    /// `None` for null entries.
    pub fn remap<F>(&mut self, f: F)
    where
        F: Fn(T) -> T,
    {
        *self = T::Encoding::remap(self.iter(), self.max_segments, f);
    }
}

// ── Iter ─────────────────────────────────────────────────────────────────────

// ── Iter ─────────────────────────────────────────────────────────────────────

/// Forward iterator over column items.
///
/// Created by [`Column::iter`] or [`Column::iter_range`].
///
/// `nth()` is O(log S + runs_skipped) — uses the column's index structure
/// to skip directly to the target slab.
pub struct Iter<'a, T: ColumnValueRef> {
    pub(crate) slabs: &'a [Slab<TailOf<T>>],
    pub(crate) col: &'a dyn SlabFind,
    pub(crate) slab_idx: usize,
    pub(crate) decoder: <T::Encoding as ColumnEncoding>::Decoder<'a>,
    pub(crate) items_left: usize,
    pub(crate) slab_remaining: usize,
    pub(crate) pos: usize,
    pub(crate) counter: usize,
}

impl<T: ColumnValueRef> Default for Iter<'_, T> {
    fn default() -> Self {
        Self {
            slabs: &[],
            col: &(),
            slab_idx: 0,
            decoder: T::Encoding::decoder(&[]),
            items_left: 0,
            slab_remaining: 0,
            pos: 0,
            counter: 0,
        }
    }
}

impl<'a, T: ColumnValueRef> Iterator for Iter<'a, T> {
    type Item = T::Get<'a>;

    #[inline]
    fn next(&mut self) -> Option<T::Get<'a>> {
        if self.items_left == 0 {
            return None;
        }
        loop {
            if let Some(v) = self.decoder.next() {
                self.items_left -= 1;
                self.slab_remaining -= 1;
                self.pos += 1;
                return Some(v);
            }
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                self.items_left = 0;
                return None;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
        }
    }

    /// O(log S + runs_skipped) — uses the column's index for slab lookup.
    fn nth(&mut self, n: usize) -> Option<T::Get<'a>> {
        if n >= self.items_left {
            self.pos += self.items_left;
            self.items_left = 0;
            return None;
        }

        // Fast path: target is within the current slab.
        if n < self.slab_remaining {
            self.items_left -= n + 1;
            self.slab_remaining -= n + 1;
            self.pos += n + 1;
            return self.decoder.nth(n);
        }

        // Use the column's index for O(log S) slab lookup.
        let target_pos = self.pos + n;
        let (si, offset) = self.col.find_slab(target_pos);
        if si >= self.slabs.len() {
            self.pos += self.items_left;
            self.items_left = 0;
            return None;
        }
        let skipped = n + 1;
        self.slab_idx = si;
        self.slab_remaining = self.slabs[si].len;
        self.decoder = T::Encoding::decoder(&self.slabs[si].data);
        self.items_left -= skipped;
        self.slab_remaining -= offset + 1;
        self.pos = target_pos + 1;
        self.decoder.nth(offset)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.items_left, Some(self.items_left))
    }

    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        while self.items_left > 0 {
            if let Some(run) = self.decoder.next_run() {
                let count = run.count.min(self.items_left);
                for _ in 0..count {
                    acc = f(acc, run.value);
                }
                self.items_left -= count;
                self.slab_remaining = self.slab_remaining.saturating_sub(count);
            } else {
                self.slab_idx += 1;
                if self.slab_idx >= self.slabs.len() {
                    break;
                }
                self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
                self.slab_remaining = self.slabs[self.slab_idx].len;
            }
        }
        acc
    }
}

impl<T: ColumnValueRef> ExactSizeIterator for Iter<'_, T> {}

impl<T: ColumnValueRef> std::fmt::Debug for Iter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("pos", &self.pos)
            .field("items_left", &self.items_left)
            .finish()
    }
}

impl<T: ColumnValueRef> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            slabs: self.slabs,
            col: self.col,
            slab_idx: self.slab_idx,
            decoder: self.decoder.clone(),
            items_left: self.items_left,
            slab_remaining: self.slab_remaining,
            pos: self.pos,
            counter: self.counter,
        }
    }
}

impl<'a, T: ColumnValueRef> Iter<'a, T> {
    /// Set the iterator to yield items up to (but not past) `pos`.
    ///
    /// Can both shorten and extend the iterator window. If `pos` is
    /// beyond the column length, the iterator will cleanly return `None`
    /// when it reaches the end of the data.
    pub fn set_max(&mut self, pos: usize) {
        if pos <= self.pos {
            self.items_left = 0;
            self.slab_remaining = 0;
        } else {
            self.items_left = pos - self.pos;
            // slab_remaining may now be less than items_left — that's fine,
            // the iterator will advance to the next slab when it runs out.
        }
    }

    /// Returns the index of the current slab.
    #[inline]
    pub fn get_slab(&self) -> usize {
        self.slab_idx
    }

    /// Returns the index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Returns the next run of identical values, merging across slab boundaries.
    ///
    /// For repeat runs, returns the full count. For literal runs, returns
    /// count=1 per value. Null runs return the null value with the full count.
    pub fn next_run(&mut self) -> Option<super::Run<T::Get<'a>>> {
        if self.items_left == 0 {
            return None;
        }
        let max = self.items_left.min(self.slab_remaining);
        let run = loop {
            if let Some(run) = self.decoder.next_run_max(max) {
                break run;
            }
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                self.items_left = 0;
                return None;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
        };

        let value = run.value;
        let count = run.count;
        self.items_left -= count;
        self.slab_remaining -= count;
        self.pos += count;
        let mut total_count = count;

        while self.slab_remaining == 0 && self.items_left > 0 {
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                break;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);

            let max = self.items_left.min(self.slab_remaining);
            if let Some(next_run) = self.decoder.next_run_max(max) {
                if next_run.value == value {
                    let c = next_run.count;
                    total_count += c;
                    self.items_left -= c;
                    self.slab_remaining -= c;
                    self.pos += c;
                } else {
                    // Value doesn't match — reset decoder so the consumed
                    // run can be re-read by subsequent calls.
                    self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
                    break;
                }
            } else {
                break;
            }
        }

        Some(super::Run {
            count: total_count,
            value,
        })
    }

    fn unwrap_decoder(self) -> <T::Encoding as ColumnEncoding>::Decoder<'a> {
        self.decoder
    }

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// # Panics
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        assert!(
            range.start >= self.pos,
            "shift_next: range.start ({}) < pos ({})",
            range.start,
            self.pos,
        );
        self.items_left = range.end.saturating_sub(self.pos);
        self.nth(range.start - self.pos)
    }
}

// ── IterState (suspend / resume) ────────────────────────────────────────────

/// Serializable snapshot of an [`Iter`] position.
///
/// Created by [`Iter::suspend`] and restored by [`IterState::try_resume`].
pub struct IterState {
    counter: usize,
    num_slabs: usize,
    slab_idx: usize,
    pos: usize,
    items_left: usize,
    /// How many items from the start of the current slab have been consumed.
    slab_consumed: usize,
}

impl IterState {
    /// Restores the iterator position in any column.
    ///
    /// Returns [`PackError::InvalidResume`] if `column` was mutated since
    /// [`Iter::suspend`].
    pub fn try_resume<'a, T: ColumnValueRef, WF: WeightFn<T>>(
        &self,
        column: &'a Column<T, WF>,
    ) -> Result<Iter<'a, T>, PackError> {
        if self.counter != column.counter {
            return Err(PackError::InvalidResume);
        }
        if self.num_slabs != column.slabs.len() {
            return Err(PackError::InvalidResume);
        }
        let slabs = &column.slabs[..];
        if self.slab_idx >= slabs.len() {
            return Ok(Iter {
                slabs,
                col: column,
                slab_idx: slabs.len(),
                decoder: T::Encoding::decoder(&[]),
                items_left: 0,
                slab_remaining: 0,
                pos: self.pos,
                counter: self.counter,
            });
        }
        let slab = &slabs[self.slab_idx];
        let mut decoder = T::Encoding::decoder(&slab.data);
        if self.slab_consumed > 0 {
            decoder.nth(self.slab_consumed - 1);
        }
        let slab_remaining = slab.len - self.slab_consumed;
        Ok(Iter {
            slabs,
            col: column,
            slab_idx: self.slab_idx,
            decoder,
            items_left: self.items_left,
            slab_remaining,
            pos: self.pos,
            counter: self.counter,
        })
    }
}

impl<'a, T: ColumnValueRef> Iter<'a, T> {
    /// Captures the current iterator position so it can be restored later.
    pub fn suspend(&self) -> IterState {
        let slab_len = if self.slab_idx < self.slabs.len() {
            self.slabs[self.slab_idx].len
        } else {
            0
        };
        IterState {
            counter: self.counter,
            num_slabs: self.slabs.len(),
            slab_idx: self.slab_idx,
            pos: self.pos,
            items_left: self.items_left,
            slab_consumed: slab_len - self.slab_remaining,
        }
    }
}

// ── FromIterator ────────────────────────────────────────────────────────────

impl<T: ColumnValueRef> FromIterator<T> for Column<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

// ── Extend ──────────────────────────────────────────────────────────────────

impl<V: AsColumnRef<T>, T: ColumnValueRef, WF: WeightFn<T>> Extend<V> for Column<T, WF> {
    fn extend<I: IntoIterator<Item = V>>(&mut self, iter: I) {
        let len = self.total_len;
        self.splice(len, 0, iter);
    }
}

// ── IntoIterator ────────────────────────────────────────────────────────────

impl<'a, T: ColumnValueRef, WF: WeightFn<T>> IntoIterator for &'a Column<T, WF> {
    type Item = T::Get<'a>;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}
