use std::marker::PhantomData;
use std::ops::{AddAssign, Range, SubAssign};

use super::encoding::ColumnEncoding;
use super::ColumnDefault;
use super::ColumnValueRef;
use super::AsColumnRef;
use super::{ValidBuf, ValidBytes};
use crate::PackError;

// ── Slab ─────────────────────────────────────────────────────────────────────

#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct Slab {
    pub(crate) data: ValidBuf,
    pub(crate) len: usize,
    pub(crate) segments: usize,
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
    fn compute(slab: &Slab) -> Self::Weight;
}

/// Default weight strategy: BIT stores only slab lengths.
#[doc(hidden)]
#[derive(Clone)]
pub struct LenWeight;

impl<T: ColumnValueRef> WeightFn<T> for LenWeight {
    type Weight = usize;
    #[inline]
    fn compute(slab: &Slab) -> usize {
        slab.len
    }
}

// ── Fenwick tree helpers ─────────────────────────────────────────────────────

/// Rebuild BIT from scratch. O(S).
/// The BIT is 1-indexed: bit[0] is unused, bit[1..=n] holds the tree.
pub(crate) fn rebuild_bit<T: ColumnValueRef, WF: WeightFn<T>>(slabs: &[Slab]) -> Vec<WF::Weight> {
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

/// Find slab containing logical index. Returns (slab_index, offset_within_slab). O(log S).
/// Uses binary lifting on the BIT.
#[inline]
pub(crate) fn find_slab<W: SlabWeight>(bit: &[W], index: usize, n: usize) -> (usize, usize) {
    if n == 0 {
        return (0, 0);
    }
    let mut pos = 0usize;
    let mut idx = 0usize;
    // Find the highest bit.
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
    pub(crate) slabs: Vec<Slab>,
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
        Self::with_max_segments(16)
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
    pub(crate) fn from_slabs(slabs: Vec<Slab>, total_len: usize, max_segments: usize) -> Self {
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

    /// Bulk-construct a column from a Vec of values.
    ///
    /// Much faster than repeated `insert` calls — encodes all values in a
    /// single O(n) pass and builds the slab tree in one shot.
    pub fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_max_segments(values, 16)
    }

    /// Bulk-construct with a custom segment budget per slab.
    pub fn from_values_with_max_segments(values: Vec<T>, max_segments: usize) -> Self {
        let total_len = values.len();
        let slabs: Vec<Slab> = T::Encoding::encode_all_slabs(values, max_segments)
            .into_iter()
            .map(|(data, len, segments)| Slab {
                data: ValidBuf::new(data),
                len,
                segments,
            })
            .collect();
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

    /// Deserialize a column from bytes produced by [`save`](Column::save).
    ///
    /// Validates the wire encoding: malformed LEB128, truncated data, and
    /// values that don't decode cleanly all return [`PackError`].  For
    /// non-nullable column types (`Column<u64>`, `Column<String>`, …),
    /// null runs are rejected with [`PackError::InvalidValue`].
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_verified(data, 16, None)
    }

    /// Build a column from the output of `load_and_verify`.
    fn load_verified(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(T::Get<'a>) -> Option<String>>,
    ) -> Result<Self, PackError> {
        let verified = T::Encoding::load_and_verify(data, max_segments, validate)?;
        let total_len: usize = verified.iter().map(|(_, len, _)| *len).sum();
        let slabs: Vec<Slab> = verified
            .into_iter()
            .map(|(data, len, segments)| Slab {
                data: ValidBuf::new(data),
                len,
                segments,
            })
            .collect();
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
    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        let slab_refs: Vec<&[u8]> = self.slabs.iter().map(|s| s.data.as_bytes()).collect();
        let bytes = T::Encoding::streaming_save(&slab_refs);
        out.extend_from_slice(&bytes);
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
        if index >= self.total_len {
            return None;
        }
        let (si, offset) = find_slab(&self.bit, index, self.slabs.len());
        let slab = &self.slabs[si];
        T::Encoding::get(&slab.data, offset, slab.len)
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

        // Collect into the caller's type to avoid premature owned conversion.
        // For the small path, values go straight to insert() via as_column_ref()
        // with no intermediate allocation (e.g. &str stays &str, never becomes String).
        let values: Vec<V> = values.into_iter().collect();

        let new_len = values.len();

        if del == 0 && new_len == 0 {
            return;
        }

        if self.slabs.is_empty() {
            let encoded_slabs = T::Encoding::encode_all_slabs(values, self.max_segments);
            self.slabs = encoded_slabs
                .into_iter()
                .map(|(data, len, segments)| Slab {
                    data: ValidBuf::new(data),
                    len,
                    segments,
                })
                .collect();
            self.bit = rebuild_bit::<T, WF>(&self.slabs);
            self.total_len = new_len;
            return;
        }

        let (si_start, offset_start) = self.locate_index(index);

        let (si_end, del_end_offset) = if del == 0 {
            (si_start, offset_start)
        } else {
            let (si, off) = self.locate_index(index + del - 1);
            (si, off + 1)
        };

        // Encode all new values into pre-split slabs in one O(n) pass.
        let encoded_slabs = T::Encoding::encode_all_slabs(values, self.max_segments);
        let new_slabs: Vec<Slab> = encoded_slabs
            .into_iter()
            .map(|(data, len, segments)| Slab {
                data: ValidBuf::new(data),
                len,
                segments,
            })
            .collect();

        // 1. Left prefix: items before the splice point in the first slab.
        let first_slab = &self.slabs[si_start];
        let first_slab_len = first_slab.len;
        let left_data = if offset_start == 0 {
            vec![]
        } else {
            let (l, _) = T::Encoding::split_at_item(&first_slab.data, offset_start, first_slab_len);
            l
        };
        let left_items = offset_start;

        // 2. Right suffix: items after the splice range in the last slab.
        let last_slab = &self.slabs[si_end];
        let last_slab_len = last_slab.len;
        let right_data = if del_end_offset >= last_slab_len {
            vec![]
        } else {
            let (_, r) = T::Encoding::split_at_item(&last_slab.data, del_end_offset, last_slab_len);
            r
        };
        let right_items = last_slab_len - del_end_offset;

        // 3. Rebuild the slab vec.
        let slab_count = self.slabs.len();
        let suffix_count = slab_count - si_end - 1;
        let capacity = si_start
            + (if left_items > 0 { 1 } else { 0 })
            + new_slabs.len()
            + (if right_items > 0 { 1 } else { 0 })
            + suffix_count;

        let suffix_slabs: Vec<Slab> = if suffix_count > 0 {
            self.slabs.drain((si_end + 1)..).collect()
        } else {
            Vec::new()
        };
        let prefix_slabs: Vec<Slab> = if si_start > 0 {
            self.slabs.drain(0..si_start).collect()
        } else {
            Vec::new()
        };

        let mut result_slabs: Vec<Slab> = Vec::with_capacity(capacity);
        result_slabs.extend(prefix_slabs);

        if left_items > 0 {
            result_slabs.push(Slab {
                segments: T::Encoding::count_segments(&left_data),
                data: ValidBuf::new(left_data),
                len: left_items,
            });
        }

        result_slabs.extend(new_slabs);

        if right_items > 0 {
            result_slabs.push(Slab {
                segments: T::Encoding::count_segments(&right_data),
                data: ValidBuf::new(right_data),
                len: right_items,
            });
        }

        result_slabs.extend(suffix_slabs);

        self.slabs = result_slabs;
        self.total_len = self.total_len - del + new_len;

        let merge_pos = si_start + (if left_items > 0 { 1 } else { 0 });
        if merge_pos < self.slabs.len() {
            self.try_merge_no_rebuild(merge_pos);
        }

        self.bit = rebuild_bit::<T, WF>(&self.slabs);
    }

    fn locate_index(&self, index: usize) -> (usize, usize) {
        if self.slabs.is_empty() {
            return (0, 0);
        }
        let (si, off) = find_slab(&self.bit, index, self.slabs.len());
        // Clamp: if binary lifting walked past the last slab (happens when
        // index == total_len, i.e. appending), return last slab + its length.
        if si >= self.slabs.len() {
            let last = self.slabs.len() - 1;
            (last, self.slabs[last].len)
        } else {
            (si, off)
        }
    }

    // ── Merge ────────────────────────────────────────────────────────────────

    /// Merge two adjacent slabs without rebuilding the BIT.
    fn merge_slabs_no_rebuild(&mut self, a: usize, b: usize) {
        let slab_b = self.slabs.remove(b);
        let slab_a = &mut self.slabs[a];
        let (merged, segments) = T::Encoding::merge_slab_bytes(&slab_a.data, &slab_b.data);
        slab_a.len += slab_b.len;
        slab_a.segments = segments;
        slab_a.data = ValidBuf::new(merged);
    }

    /// Like try_merge_around but does NOT rebuild the BIT.
    /// Used by splice which rebuilds once at the end.
    fn try_merge_no_rebuild(&mut self, si: usize) {
        let half = self.max_segments / 2;

        if si + 1 < self.slabs.len()
            && self.slabs[si].segments + self.slabs[si + 1].segments <= half
        {
            self.merge_slabs_no_rebuild(si, si + 1);
            if si > 0 && self.slabs[si - 1].segments + self.slabs[si].segments <= half {
                self.merge_slabs_no_rebuild(si - 1, si);
            }
            return;
        }

        if si > 0 && self.slabs[si - 1].segments + self.slabs[si].segments <= half {
            self.merge_slabs_no_rebuild(si - 1, si);
        }
    }

    /// Returns a forward iterator over all items in the column.
    ///
    /// Decodes one slab at a time with amortized O(1) per-item cost.
    /// Repeat runs yield a cached value; literal runs decode per item.
    pub fn iter(&self) -> Iter<'_, T> {
        if self.slabs.is_empty() {
            return Iter {
                slabs: &self.slabs,
                slab_idx: 0,
                decoder: T::Encoding::decoder(ValidBytes::from_bytes(&[])),
                items_left: 0,
                slab_remaining: 0,
                pos: 0,
                counter: self.counter,
            };
        }
        let slab_len = self.slabs[0].len;
        Iter {
            slabs: &self.slabs,
            slab_idx: 0,
            decoder: T::Encoding::decoder(&self.slabs[0].data),
            items_left: self.total_len,
            slab_remaining: slab_len,
            pos: 0,
            counter: self.counter,
        }
    }

    /// Returns a forward iterator over items in `range`, clamped to the column's length.
    ///
    /// Uses the Fenwick tree for O(log S) initial seek, then O(1) per item.
    pub fn iter_range(&self, range: Range<usize>) -> Iter<'_, T> {
        let start = range.start.min(self.total_len);
        let end = range.end.min(self.total_len);
        if start >= end || self.slabs.is_empty() {
            return Iter {
                slabs: &self.slabs,
                slab_idx: self.slabs.len(),
                decoder: T::Encoding::decoder(ValidBytes::from_bytes(&[])),
                items_left: 0,
                slab_remaining: 0,
                pos: start,
                counter: self.counter,
            };
        }
        let (si, offset) = find_slab(&self.bit, start, self.slabs.len());
        let mut decoder = T::Encoding::decoder(&self.slabs[si].data);
        if offset > 0 {
            decoder.nth(offset - 1);
        }
        Iter {
            slabs: &self.slabs,
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

impl<T: ColumnDefault, WF: WeightFn<T>> Column<T, WF> {
    /// Deserialize with options: length validation, value validation, and
    /// default-on-empty behavior.
    ///
    /// See [`LoadOpts`](super::LoadOpts) for available options.
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T>) -> Result<Self, PackError> {
        if data.is_empty() {
            return match opts.length {
                Some(0) | None => Ok(Self::new()),
                Some(len) => {
                    let slab = T::default_slab(len);
                    Ok(Self::from_slabs(vec![slab], len, opts.max_segments))
                }
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
    /// For `Option<T>` the default is `None`.  For `bool` the default is `false`.
    /// An empty column (len == 0) is considered default.
    pub fn is_default(&self) -> bool {
        if self.total_len == 0 {
            return true;
        }
        self.slabs
            .iter()
            .all(|slab| slab.segments == 1 && T::slab_is_default(&slab.data, slab.len))
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
    ///
    /// For `Option<T>` this is all-null.  For `bool` this is all-false.
    pub fn init_default(len: usize) -> Self {
        if len == 0 {
            return Self::new();
        }
        let slab = T::default_slab(len);
        Self::from_slabs(vec![slab], len, 16)
    }
}

// ── Iter ─────────────────────────────────────────────────────────────────────

/// Forward iterator over column items.
///
/// Created by [`Column::iter`] or [`Column::iter_range`].
///
/// `nth()` is O(slabs_skipped + runs_skipped) — repeat and null runs are
/// skipped in O(1), whole slabs are skipped without creating a decoder.
pub struct Iter<'a, T: ColumnValueRef> {
    pub(crate) slabs: &'a [Slab],
    pub(crate) slab_idx: usize,
    pub(crate) decoder: <T::Encoding as ColumnEncoding>::Decoder<'a>,
    pub(crate) items_left: usize,
    /// Items remaining in the current slab's decoder (for `nth` slab skipping).
    pub(crate) slab_remaining: usize,
    /// Absolute index of the next item to be yielded.
    pub(crate) pos: usize,
    /// Mutation counter at time of construction, for suspend/resume validation.
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

    /// O(slabs_skipped + runs_skipped) — skips whole slabs without decoding,
    /// and within a slab delegates to the decoder's run-aware `nth()`.
    fn nth(&mut self, mut n: usize) -> Option<T::Get<'a>> {
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

        // Skip past the rest of the current slab.
        n -= self.slab_remaining;
        self.pos += self.slab_remaining;
        self.items_left -= self.slab_remaining;
        self.slab_idx += 1;

        // Skip whole slabs without creating decoders.
        while self.slab_idx < self.slabs.len() {
            let slab_len = self.slabs[self.slab_idx].len;
            if n < slab_len {
                self.slab_remaining = slab_len;
                self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
                self.items_left -= n + 1;
                self.slab_remaining -= n + 1;
                self.pos += n + 1;
                return self.decoder.nth(n);
            }
            n -= slab_len;
            self.pos += slab_len;
            self.items_left -= slab_len;
            self.slab_idx += 1;
        }

        self.items_left = 0;
        None
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
    /// Returns the index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Returns the next run of identical values.
    ///
    /// For repeat runs, returns the remaining count and repeated value.
    /// For null runs, returns the remaining null count and the null value.
    /// For literal runs, returns count=1 and the next distinct value.
    ///
    /// Runs are merged across slab boundaries: if the current slab ends
    /// with value `v` and the next slab starts with a run of the same `v`,
    /// they are combined into a single `Run`.
    pub fn next_run(&mut self) -> Option<super::Run<T::Get<'a>>> {
        use super::encoding::RunDecoder;
        if self.items_left == 0 {
            return None;
        }
        // Get the initial run from the current (or next non-empty) slab.
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

        // Merge across slab boundaries while the next slab starts with
        // the same value.
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
                    // Continue loop — the next slab might also match.
                } else {
                    // Different value — recreate the decoder so the next
                    // call to next_run()/next() sees this run.
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

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// After this call the iterator will yield items from `range.start + 1`
    /// through `range.end - 1` (i.e. `range.end` becomes the new upper bound).
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        assert!(
            range.start >= self.pos,
            "shift_next: range.start ({}) < pos ({})",
            range.start,
            self.pos,
        );
        // Set items_left to reflect the new end bound before calling nth,
        // so that nth respects the new range.
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
    /// Restores the iterator position in `column`.
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
            // Iterator was exhausted — return an empty iterator at end position.
            return Ok(Iter {
                slabs,
                slab_idx: slabs.len(),
                decoder: T::Encoding::decoder(ValidBytes::from_bytes(&[])),
                items_left: 0,
                slab_remaining: 0,
                pos: self.pos,
                counter: self.counter,
            });
        }
        let slab = &slabs[self.slab_idx];
        let mut decoder = T::Encoding::decoder(&slab.data);
        // Skip past the already-consumed items within this slab.
        if self.slab_consumed > 0 {
            decoder.nth(self.slab_consumed - 1);
        }
        let slab_remaining = slab.len - self.slab_consumed;
        Ok(Iter {
            slabs,
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
