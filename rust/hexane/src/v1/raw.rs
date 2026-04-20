//! Raw-byte arena column.
//!
//! A [`RawColumn`] is conceptually a giant `Vec<u8>` that supports O(log S)
//! random splices at any byte offset — where S is the number of slabs, kept
//! bounded by [`RawColumn::with_max_segments`].  Unlike [`Column`](super::Column)
//! it stores no per-item encoding: the bytes you splice in are the bytes that
//! come out.
//!
//! The column does **not** know where logical values begin and end.  That
//! responsibility belongs to the caller (typically a paired `ValueMeta`
//! column whose prefix sum gives byte offsets into the raw arena).  What the
//! column *does* guarantee is that **every splice point becomes a potential
//! slab boundary, and splits only happen at splice points**.  As long as
//! callers splice at value boundaries, no value ever crosses a slab — which
//! is what makes the zero-copy [`RawColumn::get`] API safe.

use super::column::{bit_point_update, find_slab_bit, rebuild_bit, try_merge_range_skeleton};
use crate::PackError;
use std::ops::Range;

/// Default per-slab byte budget.  Chosen empirically via the `raw_compare`
/// bench (`benches/raw_compare.rs`): on a random-insert workload, 4 KiB sits
/// at the bottom of a shallow plateau that runs from ~3 KiB to ~5 KiB — ~5×
/// faster than 64 KiB and ~2× faster than 1 KiB.  Tune per-column via
/// [`RawColumn::with_max_segments`].
const DEFAULT_MAX_SEGMENTS: usize = 4_096;

#[derive(Debug, Clone)]
struct RawSlab {
    data: Vec<u8>,
}

/// A byte arena with O(log S) random splices.
///
/// See the module docs for the invariant around splice points and slab
/// boundaries.
#[derive(Debug, Clone)]
pub struct RawColumn {
    slabs: Vec<RawSlab>,
    /// Fenwick tree of slab byte lengths, 1-indexed.  `bit[0]` is unused,
    /// `bit[1..=slabs.len()]` holds the tree.
    bit: Vec<usize>,
    total_len: usize,
    /// Target per-slab byte budget (not a hard cap — single blobs larger
    /// than this live in their own oversize slab).
    max_segments: usize,
}

impl Default for RawColumn {
    fn default() -> Self {
        Self::new()
    }
}

impl RawColumn {
    pub fn new() -> Self {
        Self::with_max_segments(DEFAULT_MAX_SEGMENTS)
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        assert!(max_segments > 0, "max_segments must be non-zero");
        Self {
            slabs: Vec::new(),
            bit: vec![0],
            total_len: 0,
            max_segments,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.total_len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    // ── Splice ──────────────────────────────────────────────────────────────

    /// Insert / delete / replace bytes at `index`, taking a stream of byte
    /// slices for the inserted content.  Panics if `index + del` exceeds
    /// the column length.
    ///
    /// Use this when the caller already has bytes broken into per-item
    /// chunks (e.g. one `raw_value()` per op).  The chunks are concatenated
    /// internally before handing off to [`splice_slice`](Self::splice_slice).
    /// Zero- and single-chunk inputs skip the concatenation buffer.
    pub fn splice<I, B>(&mut self, index: usize, del: usize, chunks: I)
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.try_splice(index, del, chunks)
            .expect("splice range out of bounds")
    }

    /// Non-panicking variant of [`splice`](Self::splice).
    pub fn try_splice<I, B>(&mut self, index: usize, del: usize, chunks: I) -> Result<(), PackError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut iter = chunks.into_iter();
        let first = iter.next();
        match first {
            None => self.try_splice_slice(index, del, &[]),
            Some(first) => match iter.next() {
                None => {
                    // Single chunk — forward without allocating a concat buffer.
                    self.try_splice_slice(index, del, first.as_ref())
                }
                Some(second) => {
                    let first = first.as_ref();
                    let second = second.as_ref();
                    let mut buf = Vec::with_capacity(first.len() + second.len());
                    buf.extend_from_slice(first);
                    buf.extend_from_slice(second);
                    for chunk in iter {
                        buf.extend_from_slice(chunk.as_ref());
                    }
                    self.try_splice_slice(index, del, &buf)
                }
            },
        }
    }

    /// Insert / delete / replace bytes at `index` with a single byte slice.
    /// Panics if `index + del` exceeds the column length.
    pub fn splice_slice(&mut self, index: usize, del: usize, bytes: &[u8]) {
        self.try_splice_slice(index, del, bytes)
            .expect("splice range out of bounds")
    }

    /// Non-panicking variant of [`splice_slice`](Self::splice_slice).
    ///
    /// Flow mirrors `Column`'s `Encoding::splice`: locate the target slab,
    /// delegate to [`splice_slab`] which handles one slab plus any overflow,
    /// then clean up cross-slab deletes, merge undersized neighbours, and
    /// update the BIT.
    pub fn try_splice_slice(
        &mut self,
        index: usize,
        del: usize,
        bytes: &[u8],
    ) -> Result<(), PackError> {
        if index
            .checked_add(del)
            .map(|end| end > self.total_len)
            .unwrap_or(true)
        {
            return Err(PackError::InvalidValue(format!(
                "raw splice out of bounds: index={index} del={del} len={}",
                self.total_len
            )));
        }
        if del == 0 && bytes.is_empty() {
            return Ok(());
        }

        // Ensure we have at least one slab to land in.
        if self.slabs.is_empty() {
            self.slabs.push(RawSlab { data: Vec::new() });
            self.bit = rebuild_bit(&self.slabs, |s| s.data.len());
        }

        let (si, offset) = self.find_slab(index);

        let mut range = si..(si + 1);
        let mut old_slab_len = self.slabs[si].data.len();
        let old_weight = old_slab_len;
        let old_slab_count = self.slabs.len();

        let (overflow, overflow_del) =
            splice_slab(&mut self.slabs[si], offset, del, bytes, self.max_segments);

        // Insert overflow slabs.
        if !overflow.is_empty() {
            let pos = range.end;
            range.end += overflow.len();
            self.slabs.splice(pos..pos, overflow);
        }

        // Apply remaining deletes to subsequent slabs.
        let mut remaining = overflow_del;
        let drain_start = range.end;
        while remaining > 0 && range.end < self.slabs.len() {
            let slab_len = self.slabs[range.end].data.len();
            if remaining >= slab_len {
                old_slab_len += slab_len;
                remaining -= slab_len;
                range.end += 1;
            } else {
                old_slab_len += slab_len;
                let (partial_overflow, _) = splice_slab(
                    &mut self.slabs[range.end],
                    0,
                    remaining,
                    &[],
                    self.max_segments,
                );
                range.end += 1; // include the partially deleted slab
                if !partial_overflow.is_empty() {
                    let pos = range.end;
                    range.end += partial_overflow.len();
                    self.slabs.splice(pos..pos, partial_overflow);
                }
                break;
            }
        }
        // Bulk-remove fully consumed slabs in one shift.
        if drain_start < range.end {
            let partial = if remaining == 0 {
                range.end - drain_start
            } else {
                range.end - drain_start - 1
            };
            if partial > 0 {
                self.slabs.drain(drain_start..drain_start + partial);
                range.end -= partial;
            }
        }

        // Update total_len from the affected range.
        self.total_len += self.slabs[range.clone()]
            .iter()
            .map(|s| s.data.len())
            .sum::<usize>();
        self.total_len -= old_slab_len;
        debug_assert_eq!(
            self.total_len,
            self.slabs.iter().map(|s| s.data.len()).sum::<usize>()
        );

        // Merge undersized neighbours at the boundaries.
        let range = try_merge_range_skeleton(range, |a, b| self.try_merge_pair(a, b));

        // Update BIT: point update if exactly one slab's weight changed,
        // full rebuild otherwise.
        if self.slabs.len() == old_slab_count && range.len() == 1 {
            let new_weight = self.slabs[range.start].data.len();
            bit_point_update(&mut self.bit, range.start, old_weight, new_weight);
        } else {
            self.bit = rebuild_bit(&self.slabs, |s| s.data.len());
        }

        Ok(())
    }

    fn find_slab(&self, index: usize) -> (usize, usize) {
        if index == self.total_len {
            // Appending at the very end: land in the last slab.
            let last = self.slabs.len() - 1;
            (last, self.slabs[last].data.len())
        } else {
            find_slab_bit(&self.bit, index, self.slabs.len())
        }
    }

    /// Merge two adjacent undersized slabs.  Returns true if slab `b` was
    /// merged into slab `a` (and removed from the vec).
    fn try_merge_pair(&mut self, a: usize, b: usize) -> bool {
        let max = self.max_segments;
        let min = self.max_segments / 4;
        let Some(a_len) = self.slabs.get(a).map(|s| s.data.len()) else {
            return false;
        };
        let Some(b_len) = self.slabs.get(b).map(|s| s.data.len()) else {
            return false;
        };
        if (a_len < min || b_len < min) && a_len + b_len <= max {
            let slab_b = self.slabs.remove(b);
            self.slabs[a].data.extend_from_slice(&slab_b.data);
            true
        } else {
            false
        }
    }

    // ── Range read ──────────────────────────────────────────────────────────

    /// Return a contiguous slice for `range`.
    /// Panics if `range` is out of bounds or crosses a slab boundary.
    pub fn get(&self, range: Range<usize>) -> &[u8] {
        self.try_get(range)
            .expect("raw get: OOB or cross-slab range")
    }

    /// Non-panicking variant of [`get`](Self::get).
    pub fn try_get(&self, range: Range<usize>) -> Result<&[u8], PackError> {
        if range.end > self.total_len || range.start > range.end {
            return Err(PackError::InvalidValue(format!(
                "raw get out of bounds: range={:?} len={}",
                range, self.total_len
            )));
        }
        if range.is_empty() {
            return Ok(&[]);
        }
        let (slab_idx, offset) = find_slab_bit(&self.bit, range.start, self.slabs.len());
        let span = range.end - range.start;
        let slab = &self.slabs[slab_idx];
        if offset + span > slab.data.len() {
            return Err(PackError::InvalidValue(format!(
                "raw get spans slab boundary: range={:?} slab={} offset={} slab_len={}",
                range,
                slab_idx,
                offset,
                slab.data.len(),
            )));
        }
        Ok(&slab.data[offset..offset + span])
    }

    // ── Iter ────────────────────────────────────────────────────────────────

    /// Sequential reader starting at byte 0.
    pub fn iter(&self) -> RawColumnIter<'_> {
        RawColumnIter {
            slabs: &self.slabs,
            slab_idx: 0,
            offset_in_slab: 0,
            pos: 0,
        }
    }

    /// Sequential reader positioned at byte `pos`.  O(log S) seek.
    /// Panics if `pos > len()`.
    pub fn iter_at(&self, pos: usize) -> RawColumnIter<'_> {
        assert!(pos <= self.total_len, "iter_at out of bounds");
        if pos == 0 || self.slabs.is_empty() {
            return self.iter();
        }
        if pos == self.total_len {
            // Positioned one past the end of the last slab.
            return RawColumnIter {
                slabs: &self.slabs,
                slab_idx: self.slabs.len(),
                offset_in_slab: 0,
                pos,
            };
        }
        let (slab_idx, offset) = find_slab_bit(&self.bit, pos, self.slabs.len());
        RawColumnIter {
            slabs: &self.slabs,
            slab_idx,
            offset_in_slab: offset,
            pos,
        }
    }

    // ── Wire format ─────────────────────────────────────────────────────────

    pub fn save(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.total_len);
        self.save_to(&mut out);
        out
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        for slab in &self.slabs {
            out.extend_from_slice(&slab.data);
        }
        start..out.len()
    }

    /// Deserialize from `data`.  Everything lands in a single slab — the
    /// caller knows where value boundaries are and can splice further if
    /// they want splits.
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_with_max_segments(data, DEFAULT_MAX_SEGMENTS)
    }

    /// Deserialize with a custom `max_segments` budget.
    pub fn load_with_max_segments(data: &[u8], max_segments: usize) -> Result<Self, PackError> {
        assert!(max_segments > 0, "max_segments must be non-zero");
        if data.is_empty() {
            return Ok(Self::with_max_segments(max_segments));
        }
        let slab = RawSlab {
            data: data.to_vec(),
        };
        let slabs = vec![slab];
        let bit = rebuild_bit(&slabs, |s| s.data.len());
        Ok(Self {
            slabs,
            bit,
            total_len: data.len(),
            max_segments,
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Byte-level `Vec::splice` without the per-element iterator dance.
///
/// `Vec::splice(range, iter)` in stdlib walks the iterator and does a
/// `ptr::write` per element in its drop handler — no memcpy fast path for
/// `Copy` types with a known size.  This replacement uses three safe
/// stdlib calls that all lower to SIMD memcpy / memmove:
///
///   * [`Vec::resize`] (memset-for-`u8`) to grow,
///   * [`<[u8]>::copy_within`] to shift the suffix,
///   * [`<[u8]>::copy_from_slice`] to drop the new bytes in.
///
/// Compared to `Vec::splice` the inserted bytes go in as one memcpy instead
/// of a byte-at-a-time `ptr::write` loop.  Compared to a hand-rolled
/// `unsafe` version this leaves one wasted pass over the grown tail
/// (the memset-to-zero before we overwrite it), which is negligible:
/// `bytes.len()` is typically tiny next to the suffix shift.
#[inline]
fn splice_bytes(vec: &mut Vec<u8>, index: usize, del: usize, bytes: &[u8]) {
    let old_len = vec.len();
    debug_assert!(index + del <= old_len);
    let insert = bytes.len();
    let new_len = old_len - del + insert;

    use std::cmp::Ordering;
    match insert.cmp(&del) {
        Ordering::Greater => {
            vec.resize(new_len, 0);
            vec.copy_within(index + del..old_len, index + insert);
        }
        Ordering::Less => {
            vec.copy_within(index + del..old_len, index + insert);
            vec.truncate(new_len);
        }
        Ordering::Equal => {}
    }
    vec[index..index + insert].copy_from_slice(bytes);
}

/// Splice a single slab: delete up to `del` bytes at `index`, insert
/// `values`.  Mirrors the shape of [`ColumnEncoding::splice_slab`] used by
/// [`Column`] — `del` may exceed the slab's length, in which case the
/// excess is returned as `overflow_del` for the caller to apply to the
/// following slabs.
///
/// If the insert pushes the slab past `max_segments`, we split at the
/// splice point (before or after the inserted bytes, whichever is more
/// balanced) and return the right half as an overflow slab.  At most one
/// overflow slab is ever produced — the column's invariant only permits
/// splits at splice points, so there's nowhere else to cut.
///
/// Returns `(overflow_slabs, overflow_del)`.
///
/// [`ColumnEncoding::splice_slab`]: super::encoding::ColumnEncoding::splice_slab
fn splice_slab(
    slab: &mut RawSlab,
    index: usize,
    del: usize,
    values: &[u8],
    max_segments: usize,
) -> (Vec<RawSlab>, usize) {
    let slab_len = slab.data.len();
    let local_del = del.min(slab_len - index);
    let overflow_del = del - local_del;

    // Predict post-splice size without committing the work yet — if the slab
    // will overflow we want to split *before* splicing so we don't shift the
    // suffix in place only to immediately move half of it into a new slab.
    let new_len = slab_len - local_del + values.len();
    if new_len <= max_segments {
        // Fast path: in-place splice, no overflow.
        splice_bytes(&mut slab.data, index, local_del, values);
        return (Vec::new(), overflow_del);
    }

    let split_at = pick_split(index, values.len(), new_len);
    if split_at == 0 || split_at == new_len {
        // Edge split wouldn't actually reduce size — happens when a single
        // inserted blob is itself larger than `max_segments`.  Leave the
        // slab oversize (invariant still holds: the blob lives as one unit).
        splice_bytes(&mut slab.data, index, local_del, values);
        return (Vec::new(), overflow_del);
    }

    // Split first: peel the suffix (everything past the deleted region) off
    // as its own Vec, truncate the original down to just the prefix, then
    // distribute `values` to whichever side the balanced split picked.  This
    // saves the in-place suffix memmove that a post-splice split would do.
    let suffix = slab.data.split_off(index + local_del);
    slab.data.truncate(index); // drop the deleted bytes

    let right_data = if split_at == index {
        // Split *before* the inserted bytes: left = prefix, right = values + suffix.
        let mut right = Vec::with_capacity(values.len() + suffix.len());
        right.extend_from_slice(values);
        right.extend_from_slice(&suffix);
        right
    } else {
        // Split *after* the inserted bytes: left = prefix + values, right = suffix.
        slab.data.extend_from_slice(values);
        suffix
    };

    (vec![RawSlab { data: right_data }], overflow_del)
}

/// Pick the byte offset at which to split a slab that has just overflowed.
///
/// Two candidate splits are considered — both at splice-point boundaries,
/// which preserves the "values never cross slabs" invariant:
///
/// * **before the inserted bytes**: `split = offset_before_insert`
///   (left = prefix, right = inserted + suffix)
/// * **after the inserted bytes**: `split = offset_before_insert + inserted_len`
///   (left = prefix + inserted, right = suffix)
///
/// Returns whichever produces the more balanced result.  Ties prefer the
/// "before" split.
fn pick_split(offset_before_insert: usize, inserted_len: usize, total_len: usize) -> usize {
    let split_a = offset_before_insert;
    let imbalance_a = split_a.abs_diff(total_len - split_a);
    let split_b = offset_before_insert + inserted_len;
    let imbalance_b = split_b.abs_diff(total_len - split_b);
    if imbalance_a <= imbalance_b {
        split_a
    } else {
        split_b
    }
}

// ── Iter ───────────────────────────────────────────────────────────────────

/// Sequential byte reader over a [`RawColumn`].
///
/// Not a `std::iter::Iterator` — the natural unit of reading here is a
/// variable-sized blob, so instead of `next()` this exposes [`take`] and
/// [`skip`].  State is maintained across calls so consecutive reads don't
/// re-lookup the slab.
///
/// [`take`]: RawColumnIter::take
/// [`skip`]: RawColumnIter::skip
#[derive(Debug, Clone, Default)]
pub struct RawColumnIter<'a> {
    slabs: &'a [RawSlab],
    slab_idx: usize,
    offset_in_slab: usize,
    /// Absolute byte position — `slab_idx` and `offset_in_slab` are the
    /// physical derivation; this mirrors them for `pos()` and `seek_to`.
    pos: usize,
}

impl<'a> RawColumnIter<'a> {
    /// Current absolute byte position.  Useful for suspending / later
    /// resuming via [`RawColumn::iter_at`].
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Read the next `n` bytes and advance.  Panics if the read would cross
    /// a slab boundary — by the column's invariant this only happens if the
    /// caller passed a length that doesn't correspond to a single value
    /// boundary.
    pub fn take(&mut self, n: usize) -> &'a [u8] {
        if n == 0 {
            return &[];
        }
        let slab = self
            .slabs
            .get(self.slab_idx)
            .expect("RawColumnIter::take past end");
        let available = slab.data.len() - self.offset_in_slab;
        assert!(
            n <= available,
            "RawColumnIter::take would cross slab boundary: want {n}, have {available}",
        );
        let out = &slab.data[self.offset_in_slab..self.offset_in_slab + n];
        self.offset_in_slab += n;
        self.pos += n;
        if self.offset_in_slab >= slab.data.len() && self.slab_idx + 1 < self.slabs.len() {
            self.slab_idx += 1;
            self.offset_in_slab = 0;
        }
        out
    }

    /// Advance the cursor by `n` bytes, crossing slab boundaries as needed.
    /// Panics if `n` walks past the end of the column.
    pub fn skip(&mut self, n: usize) {
        self.pos += n;
        let mut remaining = n;
        while remaining > 0 {
            let slab = self
                .slabs
                .get(self.slab_idx)
                .expect("RawColumnIter::skip past end");
            let available = slab.data.len() - self.offset_in_slab;
            if remaining < available {
                self.offset_in_slab += remaining;
                return;
            }
            remaining -= available;
            self.slab_idx += 1;
            self.offset_in_slab = 0;
        }
    }

    /// Forward-only absolute seek.  `target` must be `>= pos()`; use
    /// [`RawColumn::iter_at`] for arbitrary seeks.  Equivalent to
    /// `self.skip(target - self.pos())` with a bounds check.
    pub fn seek_to(&mut self, target: usize) {
        debug_assert!(
            target >= self.pos,
            "RawColumnIter::seek_to is forward-only (at {} want {target})",
            self.pos,
        );
        self.skip(target - self.pos);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(col: &RawColumn, expected: &[u8]) {
        assert_eq!(col.len(), expected.len(), "len mismatch");
        assert_eq!(col.save(), expected, "save bytes mismatch");
    }

    #[test]
    fn empty_column() {
        let col = RawColumn::new();
        assert!(col.is_empty());
        assert_eq!(col.len(), 0);
        assert_eq!(col.save(), Vec::<u8>::new());
    }

    #[test]
    fn single_slab_append() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello");
        col.splice_slice(5, 0, b", world");
        drive(&col, b"hello, world");
    }

    #[test]
    fn single_slab_insert_middle() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"helloworld");
        col.splice_slice(5, 0, b", ");
        drive(&col, b"hello, world");
    }

    #[test]
    fn single_slab_delete() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        col.splice_slice(5, 2, b""); // delete ", "
        drive(&col, b"helloworld");
    }

    #[test]
    fn single_slab_replace() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        col.splice_slice(7, 5, b"hexane");
        drive(&col, b"hello, hexane");
    }

    #[test]
    fn splice_overflow_splits() {
        let mut col = RawColumn::with_max_segments(8);
        col.splice_slice(0, 0, b"abcdef"); // 6 bytes — fits
        col.splice_slice(3, 0, b"XYZ"); // becomes "abcXYZdef" = 9 bytes — overflow
        drive(&col, b"abcXYZdef");
        // Should have produced 2 slabs.
        assert_eq!(col.slabs.len(), 2, "expected split into 2 slabs");
    }

    #[test]
    fn balanced_split_picks_better_side() {
        // After insert the slab is [PPPP | X | SSSSSSSS] (prefix 4, insert 1, suffix 8).
        // Option A: split before insert → (4 | 9). Imbalance 5.
        // Option B: split after insert → (5 | 8). Imbalance 3.
        // Should pick B.
        let mut col = RawColumn::with_max_segments(10);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // 12 bytes, forces split at construction
                                                 // Clear and rebuild cleanly with no pre-split.
        let mut col = RawColumn::with_max_segments(15);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // 12 bytes, fits
        col.splice_slice(4, 0, b"X"); // 13 bytes — still fits (< 15), no overflow
        drive(&col, b"PPPPXSSSSSSSS");

        // Now force the overflow.
        let mut col = RawColumn::with_max_segments(12);
        col.splice_slice(0, 0, b"PPPPSSSSSSSS"); // exactly 12 bytes
        col.splice_slice(4, 0, b"X"); // 13 bytes — overflow by 1
        drive(&col, b"PPPPXSSSSSSSS");
        assert_eq!(col.slabs.len(), 2);
        // Better split is "after insert": left=5 bytes, right=8.
        assert_eq!(col.slabs[0].data, b"PPPPX");
        assert_eq!(col.slabs[1].data, b"SSSSSSSS");
    }

    #[test]
    fn balanced_split_picks_before_when_ties_or_better() {
        // Insert near the end: PPPPPPPPP | X | S (9 + 1 + 1 = 11).
        // Option A: before insert → (9 | 2), imbalance 7.
        // Option B: after insert  → (10 | 1), imbalance 9.
        // Should pick A.
        let mut col = RawColumn::with_max_segments(10);
        col.splice_slice(0, 0, b"PPPPPPPPPS");
        col.splice_slice(9, 0, b"X");
        drive(&col, b"PPPPPPPPPXS");
        assert_eq!(col.slabs.len(), 2);
        assert_eq!(col.slabs[0].data, b"PPPPPPPPP");
        assert_eq!(col.slabs[1].data, b"XS");
    }

    #[test]
    fn oversize_blob_gets_own_slab() {
        // Blob larger than max_segments — cannot split (would cross value
        // boundaries), so it lives in one oversize slab.
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"xxxxxxxxxxxxxxxx"); // 16 bytes > max 4
        drive(&col, b"xxxxxxxxxxxxxxxx");
        // Either one oversize slab or (if split at edge skipped) something,
        // but `get(0..16)` must return the whole thing intact.
        assert_eq!(col.get(0..16), b"xxxxxxxxxxxxxxxx");
    }

    #[test]
    fn get_within_slab() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        assert_eq!(col.get(0..5), b"hello");
        assert_eq!(col.get(7..12), b"world");
        assert_eq!(col.get(0..12), b"hello, world");
        assert_eq!(col.get(5..5), b"");
    }

    #[test]
    #[should_panic(expected = "cross-slab")]
    fn get_cross_slab_panics() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd"); // slab 0 full
        col.splice_slice(4, 0, b"efgh"); // slab 1
                                         // col is now split into two slabs; get a range spanning both.
        let _ = col.get(2..6);
    }

    #[test]
    fn try_get_cross_slab_errors() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        assert!(col.try_get(2..6).is_err(), "expected cross-slab error");
    }

    #[test]
    fn try_get_oob_errors() {
        let mut col = RawColumn::new();
        col.splice_slice(0, 0, b"abc");
        assert!(col.try_get(0..10).is_err());
        assert!(col.try_get(5..6).is_err());
    }

    #[test]
    fn try_splice_oob_errors() {
        let mut col = RawColumn::new();
        col.splice_slice(0, 0, b"abc");
        assert!(col.try_splice_slice(5, 0, b"x").is_err());
        assert!(col.try_splice_slice(0, 10, b"").is_err());
    }

    #[test]
    fn multi_slab_splice_across_boundary() {
        let mut col = RawColumn::with_max_segments(4);
        // Build up three slabs of content.
        col.splice_slice(0, 0, b"aaaa"); // slab 0 (4 bytes)
        col.splice_slice(4, 0, b"bbbb"); // pushes into slab 1 via split
        col.splice_slice(8, 0, b"cccc"); // pushes into slab 2
        drive(&col, b"aaaabbbbcccc");

        // Delete 6 bytes starting at position 3 (crosses into slab 2).
        col.splice_slice(3, 6, b"");
        drive(&col, b"aaaccc");
    }

    #[test]
    fn merge_after_tiny_splices() {
        // Drive many tiny splices into a column with small max_segments;
        // then delete enough to trigger merges.  Sanity-check that slab
        // count stays bounded.
        let mut col = RawColumn::with_max_segments(16);
        for i in 0..100 {
            let byte = b'a' + (i % 26) as u8;
            col.splice_slice(col.len(), 0, &[byte]);
        }
        let len_before = col.len();
        let slabs_before = col.slabs.len();
        assert!(slabs_before > 1, "should have multiple slabs");

        // Delete most bytes — merges should happen.
        col.splice_slice(0, len_before - 10, b"");
        assert_eq!(col.len(), 10);
        // Slab count should have shrunk (not necessarily to 1, but <= ceil(10/16) ish).
        assert!(
            col.slabs.len() <= slabs_before,
            "slabs didn't shrink: before={} after={}",
            slabs_before,
            col.slabs.len(),
        );
    }

    #[test]
    fn iter_take_and_skip() {
        let mut col = RawColumn::with_max_segments(32);
        col.splice_slice(0, 0, b"hello, world");
        let mut it = col.iter();
        assert_eq!(it.take(5), b"hello");
        it.skip(2);
        assert_eq!(it.take(5), b"world");
    }

    #[test]
    fn iter_across_slabs_with_skip() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        col.splice_slice(8, 0, b"ijkl");
        let mut it = col.iter();
        // Take bytes from within each slab; skip across boundaries.
        assert_eq!(it.take(4), b"abcd");
        it.skip(1); // skip 'e'
        assert_eq!(it.take(3), b"fgh");
        assert_eq!(it.take(4), b"ijkl");
    }

    #[test]
    fn iter_at_seeks() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut it = col.iter_at(4);
        assert_eq!(it.take(4), b"efgh");
    }

    #[test]
    #[should_panic(expected = "cross slab boundary")]
    fn iter_take_cross_slab_panics() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut it = col.iter();
        // take(8) would cross — must panic.
        let _ = it.take(8);
    }

    #[test]
    fn load_save_roundtrip() {
        let data = b"The quick brown fox jumps over the lazy dog".to_vec();
        let col = RawColumn::load(&data).unwrap();
        assert_eq!(col.len(), data.len());
        assert_eq!(col.save(), data);
    }

    #[test]
    fn load_empty() {
        let col = RawColumn::load(&[]).unwrap();
        assert!(col.is_empty());
        assert_eq!(col.save(), Vec::<u8>::new());
    }

    #[test]
    fn save_to_range() {
        let mut col = RawColumn::with_max_segments(4);
        col.splice_slice(0, 0, b"abcd");
        col.splice_slice(4, 0, b"efgh");
        let mut out = b"prefix:".to_vec();
        let range = col.save_to(&mut out);
        assert_eq!(&out[range.clone()], b"abcdefgh");
        assert_eq!(&out[..range.start], b"prefix:");
    }

    // ── Randomised fuzz ─────────────────────────────────────────────────────

    /// Simple deterministic PRNG so tests stay reproducible.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }
        fn next(&mut self) -> u64 {
            // xorshift64*
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn range(&mut self, hi: usize) -> usize {
            if hi == 0 {
                0
            } else {
                (self.next() as usize) % hi
            }
        }
    }

    #[test]
    fn fuzz_splice_vs_vec() {
        let mut col = RawColumn::with_max_segments(16);
        let mut reference: Vec<u8> = Vec::new();
        let mut rng = Rng::new(0x1234_5678);
        for _ in 0..5_000 {
            let op = rng.range(3);
            let len = reference.len();
            match op {
                // insert
                0 => {
                    let at = rng.range(len + 1);
                    let n = rng.range(8) + 1;
                    let bytes: Vec<u8> = (0..n).map(|_| (rng.next() & 0xff) as u8).collect();
                    col.splice_slice(at, 0, &bytes);
                    reference.splice(at..at, bytes);
                }
                // delete
                1 if len > 0 => {
                    let at = rng.range(len);
                    let del = rng.range((len - at).min(6)) + 1;
                    col.splice_slice(at, del, b"");
                    reference.drain(at..at + del);
                }
                // replace
                _ if len > 0 => {
                    let at = rng.range(len);
                    let del = rng.range((len - at).min(4)) + 1;
                    let n = rng.range(6) + 1;
                    let bytes: Vec<u8> = (0..n).map(|_| (rng.next() & 0xff) as u8).collect();
                    col.splice_slice(at, del, &bytes);
                    reference.splice(at..at + del, bytes);
                }
                _ => {}
            }
            assert_eq!(col.len(), reference.len());
            assert_eq!(col.save(), reference);
        }
    }
}
