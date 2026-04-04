use std::marker::PhantomData;
use std::ops::{AddAssign, Range, SubAssign};

use super::encoding::ColumnEncoding;
use super::AsColumnRef;
use super::ColumnValueRef;
use crate::PackError;

/// Type alias for the slab tail metadata of a column value type.
pub type TailOf<T> = <<T as ColumnValueRef>::Encoding as ColumnEncoding>::Tail;

const DEFAULT_MAX_SEG: usize = 32;

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

impl<Tail: Copy + Clone + std::fmt::Debug + Default> Slab<Tail> {
    /// Returns `true` if the slab contains only `default` values.
    /// O(1) — checks segment count then decodes only the first value.
    pub(crate) fn is_default<T: ColumnValueRef>(&self) -> bool
    where
        for<'a> T::Get<'a>: PartialEq + Default,
    {
        let default = T::Get::default();
        self.len == 0
            || (self.segments == 1
                && T::Encoding::decoder(&self.data)
                    .next()
                    .map_or(true, |v| v == default))
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
pub(crate) fn rebuild_bit<T: ColumnValueRef, WF: WeightFn<T>>(
    slabs: &[Slab<TailOf<T>>],
) -> Vec<WF::Weight> {
    let n = slabs.len();
    let mut bit = vec![WF::Weight::default(); n + 1];
    for i in 0..n {
        bit[i + 1] = WF::compute(&slabs[i]);
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

/// Trait for types that can locate the slab containing a logical item index.
///
/// This abstracts the index structure (Fenwick tree, B-tree, etc.) behind the
/// Column so that iterators don't depend on a specific implementation.
pub(crate) trait SlabFind {
    /// Find the slab containing logical `index`.
    /// Returns `(slab_index, offset_within_slab)`.
    fn find_slab(&self, index: usize) -> (usize, usize);
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
    pub fn new() -> Self {
        Self::with_max_segments(DEFAULT_MAX_SEG)
    }

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
        let bit = rebuild_bit::<T, WF>(&slabs);
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
        let bit = rebuild_bit::<T, WF>(std::slice::from_ref(&slab));
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
        let bit = rebuild_bit::<T, WF>(&slabs);
        Ok(Self {
            slabs,
            bit,
            total_len,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.total_len
    }

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
    /// Panics with a descriptive message if any encoding invariant is violated.
    pub fn validate_encoding(&self) {
        let bytes = self.save();
        if let Err(e) = T::Encoding::validate_encoding(&bytes) {
            panic!("invalid encoding: {e}");
        }
    }

    /// Validate the canonical encoding and return its slab info.
    pub fn validate_encoding_info(&self) -> super::encoding::SlabInfo<TailOf<T>> {
        let bytes = self.save();
        match T::Encoding::validate_encoding(&bytes) {
            Ok(info) => info,
            Err(e) => panic!("invalid encoding: {e}"),
        }
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
    /// Panics if `index >= self.len()`.
    pub fn remove(&mut self, index: usize) {
        self.splice(index, 1, std::iter::empty::<T>());
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
            self.bit = rebuild_bit::<T, WF>(&self.slabs);
        }

        T::Encoding::splice(self, index, del, iter);

        #[cfg(debug_assertions)]
        self.validate_encoding();
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
        let mut start = range.start;
        let mut end = range.end;

        if !range.is_empty() {
            // external right
            // [ . [. . . A] B .] -> [. [. . . AB] .]
            //   0  1 2 3 4  5 6      0  1 2 3 4   5
            self.try_merge(end - 1, end);
            // internal left
            // [ . [. . B A] . .] -> [. [. . BA] .]
            //   0  1 2 3 4  5 6      0  1 2 3   4
            if range.len() > 1 && end > 2 && self.try_merge(end - 2, end - 1) {
                end -= 1;
            }
            // internal right
            // [ . [A B . .] . .] -> [ . [AB . .] . .]
            //   0  1 2 3 4  5 6       0  1  2 3  4 5
            if (start..end).len() > 1 && self.try_merge(start, start + 1) {
                end -= 1;
            }
            // external left
            // [ B [A . . .] . .] -> [ [BA . . .] . .]
            //   0  1 2 3 4  5 6        0  1 2 3  4 5
            if start > 1 && self.try_merge(start - 1, start) {
                start -= 1;
                end -= 1;
            }
        }

        start..end
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

// ── Default-valued columns ──────────────────────────────────────────────────

impl<T: ColumnValueRef, WF: WeightFn<T>> Column<T, WF>
where
    for<'a> T::Get<'a>: Default,
{
    /// Deserialize with options: length validation, value validation, and
    /// default-on-empty behavior.
    ///
    /// See [`LoadOpts`](super::LoadOpts) for available options.
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T>) -> Result<Self, PackError> {
        if data.is_empty() {
            return match opts.length {
                Some(0) | None => Ok(Self::new()),
                Some(len) => Ok(Self::fill_inner(len, T::Get::default())),
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

    /// Returns `true` if every item in the column has the default value.
    ///
    /// O(slabs) — checks each slab's single-segment first value against default.
    /// An empty column is considered default.
    pub fn is_default(&self) -> bool {
        self.total_len == 0 || self.slabs.iter().all(|s| s.is_default::<T>())
    }

    /// Serialize the column by appending bytes to `out`, unless all values
    /// are the default.
    ///
    /// Returns the byte range written (empty range if all-default or empty).
    pub fn save_to_unless_default(&self, out: &mut Vec<u8>) -> Range<usize> {
        if self.is_default() {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    /// Create a column of `len` default values.
    pub fn init_default(len: usize) -> Self {
        Self::fill_inner(len, T::Get::default())
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
}

impl<T: ColumnValueRef> ExactSizeIterator for Iter<'_, T> {}

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
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn next_run(&mut self) -> Option<super::Run<T::Get<'a>>> {
        use super::encoding::RunDecoder;
        if self.items_left == 0 {
            return None;
        }
        let run = loop {
            if let Some(run) = self.decoder.next_run() {
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
        let count = run.count.min(self.items_left).min(self.slab_remaining);
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

            if let Some(next_run) = self.decoder.next_run() {
                if next_run.value == value {
                    let c = next_run.count.min(self.items_left).min(self.slab_remaining);
                    total_count += c;
                    self.items_left -= c;
                    self.slab_remaining -= c;
                    self.pos += c;
                } else {
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
