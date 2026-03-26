use crate::PackError;

use super::encoding::ColumnEncoding;

// ── Wire format ──────────────────────────────────────────────────────────────
//
// Alternating run-length counts, starting with `false`:
//
//   run0: uleb128  (false count)
//   run1: uleb128  (true count)
//   run2: uleb128  (false count)
//   …
//
// No boolean value is stored — the value is implicit from the run's position
// (even-indexed runs are `false`, odd-indexed are `true`).
//
// An empty slab means an empty column.  An all-true column encodes as `[0, N]`
// (zero falses, then N trues).

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Decode one unsigned LEB128 from `data`.  Returns `(bytes_read, value)`.
fn read_count(data: &[u8]) -> Option<(usize, usize)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v as usize))
}

/// Stack-buffered unsigned LEB128 encoding (max 10 bytes, no heap allocation).
#[derive(Clone, Copy)]
struct BoolLeb128Buf {
    buf: [u8; 10],
    len: u8,
}

impl std::ops::Deref for BoolLeb128Buf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

struct BoolLeb128Iter {
    buf: [u8; 10],
    pos: u8,
    len: u8,
}

impl Iterator for BoolLeb128Iter {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.pos < self.len {
            let b = self.buf[self.pos as usize];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.pos) as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for BoolLeb128Iter {}

impl IntoIterator for BoolLeb128Buf {
    type Item = u8;
    type IntoIter = BoolLeb128Iter;
    #[inline]
    fn into_iter(self) -> BoolLeb128Iter {
        BoolLeb128Iter {
            buf: self.buf,
            pos: 0,
            len: self.len,
        }
    }
}

/// Encode `n` as unsigned LEB128 into a stack buffer.
#[inline]
fn encode_count(n: usize) -> BoolLeb128Buf {
    let mut out = BoolLeb128Buf {
        buf: [0u8; 10],
        len: 0,
    };
    let mut val = n as u64;
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        out.buf[out.len as usize] = byte;
        out.len += 1;
        if val == 0 {
            break;
        }
    }
    out
}

// ── Scan ─────────────────────────────────────────────────────────────────────

struct BoolScanResult {
    /// Byte offset of this run's LEB128 count.
    run_start: usize,
    /// Number of bytes in the LEB128 count.
    count_bytes: usize,
    /// Number of items in this run.
    count: usize,
    /// The boolean value of items in this run.
    value: bool,
    /// How many items before the target within this run.
    offset_in_run: usize,
    /// Byte offset and value of the previous run (for merging).
    prev: Option<(usize, usize, bool)>, // (byte_pos, count_bytes, value)
}

fn scan_to(slab: &[u8], target: usize) -> Option<BoolScanResult> {
    let (mut byte_pos, mut item_pos, mut value) = (0, 0, false);

    let mut prev: Option<(usize, usize, bool)> = None;

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..])?;

        if target < item_pos + count {
            return Some(BoolScanResult {
                run_start: byte_pos,
                count_bytes: cb,
                count,
                value,
                offset_in_run: target - item_pos,
                prev,
            });
        }

        prev = Some((byte_pos, cb, value));
        item_pos += count;
        byte_pos += cb;
        value = !value;
    }

    None
}

// ── BoolDecoder ──────────────────────────────────────────────────────────────

/// Forward iterator over all items in a single boolean-encoded slab.
///
/// Created by [`BoolEncoding::decoder`].  Each run yields the same value
/// in O(1) per item; advancing between runs reads one LEB128 count.
#[derive(Clone)]
pub struct BoolDecoder<'a> {
    data: &'a [u8],
    byte_pos: usize,
    remaining: usize,
    /// Current run's boolean value.  Initialized to `true` so the first
    /// `advance_run` flip produces `false` (matching the wire format's
    /// "first run is always false" invariant).
    value: bool,
}

impl<'a> BoolDecoder<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        let mut dec = BoolDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            value: true,
        };
        dec.advance_run();
        dec
    }

    fn advance_run(&mut self) {
        if self.byte_pos >= self.data.len() {
            self.remaining = 0;
            return;
        }
        if let Some((cb, count)) = read_count(&self.data[self.byte_pos..]) {
            self.byte_pos += cb;
            self.value = !self.value;
            self.remaining = count;
        } else {
            self.remaining = 0;
        }
    }
}

impl<'a> Iterator for BoolDecoder<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<bool> {
        loop {
            if self.remaining > 0 {
                self.remaining -= 1;
                return Some(self.value);
            }
            if self.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run();
        }
    }

    /// O(runs_skipped) — each run is skipped in O(1) by decrementing the count.
    fn nth(&mut self, mut n: usize) -> Option<bool> {
        loop {
            if self.remaining > 0 {
                if n < self.remaining {
                    self.remaining -= n;
                    return self.next();
                }
                n -= self.remaining;
                self.remaining = 0;
            }
            if self.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run();
        }
    }
}

impl<'a> super::encoding::RunDecoder for BoolDecoder<'a> {
    fn next_run(&mut self) -> Option<super::Run<bool>> {
        loop {
            if self.remaining > 0 {
                let count = self.remaining;
                let value = self.value;
                self.remaining = 0;
                return Some(super::Run { count, value });
            }
            if self.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run();
        }
    }
}

// ── BoolEncoding ─────────────────────────────────────────────────────────────

/// Boolean encoding strategy — alternating run-length encoding.
///
/// Zero-sized type; all state lives in the slab bytes.
pub struct BoolEncoding;

impl Default for BoolEncoding {
    fn default() -> Self {
        Self
    }
}

impl ColumnEncoding for BoolEncoding {
    type Value = bool;
    #[allow(clippy::needless_lifetimes)]
    fn get<'a>(slab: &'a [u8], index: usize, len: usize) -> Option<bool> {
        if index >= len {
            return None;
        }
        let scan = scan_to(slab, index)?;
        Some(scan.value)
    }

    fn insert<'v>(slab: &mut Vec<u8>, index: usize, len: usize, value: bool) -> i32 {
        #[cfg(debug_assertions)]
        let seg_before = bool_count_segments(slab);

        let delta = if slab.is_empty() || index == len {
            // Append case.
            append_bool(slab, len, value)
        } else {
            let scan = scan_to(slab, index).expect("index < len so scan must succeed");

            if scan.value == value {
                // Same value as current run — increment count.
                let new_count = encode_count(scan.count + 1);
                slab.splice(scan.run_start..scan.run_start + scan.count_bytes, new_count);
                0 // count bump, no segment change
            } else {
                // Different value — need to split the run.
                let k = scan.offset_in_run;

                if k == 0 {
                    if let Some((prev_start, prev_cb, _prev_val)) = scan.prev {
                        // Extend previous run (same value due to alternation).
                        let (_, prev_count) = read_count(&slab[prev_start..]).unwrap();
                        let new_prev = encode_count(prev_count + 1);
                        slab.splice(prev_start..prev_start + prev_cb, new_prev);
                        // If prev was 0-count (structural padding), it just became a
                        // real segment; otherwise no change.
                        if prev_count == 0 {
                            1
                        } else {
                            0
                        }
                    } else {
                        // No prev: insert_split at 0 creates [0][1][N] = +1 seg
                        // (the 0-count run is structural padding, not a segment)
                        insert_split(slab, &scan, 0);
                        1
                    }
                } else if k == scan.count {
                    let next_start = scan.run_start + scan.count_bytes;
                    if next_start < slab.len() {
                        // Extend next run.
                        let (next_cb, next_count) = read_count(&slab[next_start..]).unwrap();
                        let new_next = encode_count(next_count + 1);
                        slab.splice(next_start..next_start + next_cb, new_next);
                        0 // extended next run
                    } else {
                        // Append new run at end.
                        slab.extend(encode_count(1));
                        1 // new non-zero run
                    }
                } else {
                    // Mid-run split: [N] → [k][1][N-k] = +2 segs
                    insert_split(slab, &scan, k);
                    2
                }
            }
        };

        #[cfg(debug_assertions)]
        debug_assert_eq!(
            delta,
            bool_count_segments(slab) as i32 - seg_before as i32,
            "segment delta mismatch in Bool insert"
        );
        delta
    }

    fn remove(slab: &mut Vec<u8>, index: usize, _len: usize) -> i32 {
        #[cfg(debug_assertions)]
        let seg_before = bool_count_segments(slab);

        let scan = scan_to(slab, index).expect("index < len so scan must succeed");

        let delta = if scan.count > 1 {
            // Decrement count — non-structural.
            let new_count = encode_count(scan.count - 1);
            slab.splice(scan.run_start..scan.run_start + scan.count_bytes, new_count);
            0
        } else {
            // count == 1: remove this run and merge neighbors.
            remove_and_merge(slab, &scan)
        };

        #[cfg(debug_assertions)]
        debug_assert_eq!(
            delta,
            bool_count_segments(slab) as i32 - seg_before as i32,
            "segment delta mismatch in Bool remove"
        );
        delta
    }

    fn count_segments(slab: &[u8]) -> usize {
        bool_count_segments(slab)
    }

    fn split_at_item(slab: &[u8], index: usize, len: usize) -> (Vec<u8>, Vec<u8>) {
        bool_split_at_item(slab, index, len)
    }

    fn merge_slab_bytes(a: &[u8], b: &[u8]) -> (Vec<u8>, usize) {
        bool_merge_slab_bytes(a, b)
    }

    fn validate_encoding(slab: &[u8]) -> Result<(), String> {
        bool_validate_encoding(slab)
    }

    fn encode_all_slabs<V: super::AsColumnRef<bool>>(values: Vec<V>, max_segments: usize) -> Vec<(Vec<u8>, usize, usize)> {
        let bools: Vec<bool> = values.iter().map(|v| v.as_column_ref()).collect();
        bool_encode_all_slabs(&bools, max_segments)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(bool) -> Option<String>>,
    ) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError> {
        bool_load_and_verify(data, max_segments, validate)
    }

    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8> {
        bool_streaming_save(slabs)
    }

    type Decoder<'a> = BoolDecoder<'a>;

    fn decoder(slab: &[u8]) -> BoolDecoder<'_> {
        BoolDecoder::new(slab)
    }
}

// ── Append ───────────────────────────────────────────────────────────────────

/// Append a boolean value to the end of the slab. Returns segment delta.
fn append_bool(slab: &mut Vec<u8>, len: usize, value: bool) -> i32 {
    if slab.is_empty() {
        // Empty column — create initial runs.
        if value {
            // [0 false, 1 true] — 0-count false is structural padding, 1 segment
            slab.extend(encode_count(0));
            slab.extend(encode_count(1));
        } else {
            // [1 false] — 1 segment
            slab.extend(encode_count(1));
        }
        return 1;
    }

    // Walk to the last run to determine its value and extend it.
    let mut byte_pos = 0;
    let mut run_value = false;
    let mut last_start = 0;
    let mut last_cb = 0;
    let mut _item_pos = 0;

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..]).unwrap();
        last_start = byte_pos;
        last_cb = cb;
        _item_pos += count;
        byte_pos += cb;
        if byte_pos < slab.len() {
            run_value = !run_value;
        }
    }

    debug_assert_eq!(_item_pos, len, "slab item count mismatch");

    if run_value == value {
        // Extend the last run.
        let (_, last_count) = read_count(&slab[last_start..]).unwrap();
        let new_count = encode_count(last_count + 1);
        slab.splice(last_start..last_start + last_cb, new_count);
        0 // extended existing run
    } else {
        // Append a new 1-count run.
        slab.extend(encode_count(1));
        1 // new non-zero run
    }
}

// ── Mid-run split ────────────────────────────────────────────────────────────

/// Split a run at offset `k`, inserting a 1-count run of the opposite value.
/// Turns `[N same]` into `[k same][1 opposite][N-k same]`.
fn insert_split(slab: &mut Vec<u8>, scan: &BoolScanResult, k: usize) {
    let before = k;
    let after = scan.count - k;

    let mut new_bytes = vec![];
    new_bytes.extend(encode_count(before));
    new_bytes.extend(encode_count(1)); // the inserted opposite-value item
    new_bytes.extend(encode_count(after));

    slab.splice(scan.run_start..scan.run_start + scan.count_bytes, new_bytes);
}

// ── Remove + merge ───────────────────────────────────────────────────────────

/// Remove a run with count==1 and merge the now-adjacent same-value neighbors.
/// Returns segment delta.
fn remove_and_merge(slab: &mut Vec<u8>, scan: &BoolScanResult) -> i32 {
    let run_end = scan.run_start + scan.count_bytes;

    // Check if there's a next run.
    let next = if run_end < slab.len() {
        let (cb, count) = read_count(&slab[run_end..]).unwrap();
        Some((run_end, cb, count))
    } else {
        None
    };

    match (scan.prev, next) {
        (Some((prev_start, _prev_cb, _)), Some((next_start, next_cb, next_count))) => {
            // Both neighbors exist and have the same value (they bookended
            // the removed run).  Merge them.
            let (_, prev_count) = read_count(&slab[prev_start..]).unwrap();
            let merged_count = prev_count + next_count;
            let merge_end = next_start + next_cb;

            // Replace [prev_count][removed_count][next_count] with [merged_count].
            let new_bytes = encode_count(merged_count);
            slab.splice(prev_start..merge_end, new_bytes);
            // Segments removed: 1 (the removed run) + 1 (next, always non-zero)
            // + prev_seg (1 if prev_count > 0, else 0).
            // Segments added: 1 (if merged > 0, which it must be since next > 0).
            let prev_seg = if prev_count > 0 { 1i32 } else { 0 };
            1 - (prev_seg + 1 + 1)
        }
        (None, Some((next_start, next_cb, next_count))) => {
            // No previous run — the removed run was the first run.
            let next_value = !scan.value;
            if !next_value {
                // Next run is false — it becomes the new first run.  Remove
                // the old first run.
                slab.drain(scan.run_start..next_start);
            } else {
                // Next run is true — we need a 0-count false run before it.
                // Replace [removed] with [0], keeping the next run as-is.
                let zero = encode_count(0);
                slab.splice(scan.run_start..next_start, zero);
            }
            let _ = next_cb;
            let _ = next_count;
            // Removed the run = -1 seg
            -1
        }
        (Some((prev_start, prev_cb, _)), None) => {
            // No next run — the removed run was the last run.
            // Just remove it.  But also strip any trailing zero-count runs.
            slab.truncate(scan.run_start);
            strip_trailing_zeros(slab);
            let _ = prev_start;
            let _ = prev_cb;
            // Removed the run = -1 seg
            -1
        }
        (None, None) => {
            // Only run in the slab — clear everything.
            slab.clear();
            // Removed the only run = -1 seg
            -1
        }
    }
}

/// Strip trailing zero-count runs from the slab.
fn strip_trailing_zeros(slab: &mut Vec<u8>) {
    loop {
        if slab.is_empty() {
            break;
        }
        // Walk to find the last run.
        let mut byte_pos = 0;
        let mut last_start = 0;
        while byte_pos < slab.len() {
            last_start = byte_pos;
            let (cb, _) = read_count(&slab[byte_pos..]).unwrap();
            byte_pos += cb;
        }
        let (_, count) = read_count(&slab[last_start..]).unwrap();
        if count == 0 {
            slab.truncate(last_start);
        } else {
            break;
        }
    }
}

// ── count_segments ───────────────────────────────────────────────────────────

/// Count non-zero runs in a boolean slab. Zero-count runs are structural
/// padding (e.g. a leading 0-count false run for an all-true column) and
/// are not counted as segments.
fn bool_count_segments(slab: &[u8]) -> usize {
    let mut byte_pos = 0;
    let mut segments = 0;

    while byte_pos < slab.len() {
        let (cb, count) = match read_count(&slab[byte_pos..]) {
            Some(v) => v,
            None => break,
        };
        if count > 0 {
            segments += 1;
        }
        byte_pos += cb;
    }

    segments
}

// ── validate_encoding ────────────────────────────────────────────────────────

/// Validate that a boolean slab is in canonical form.
///
/// Invariants checked:
/// 1. Runs alternate false/true/false/... (first run is always false)
/// 2. Only the first run (false) may have count 0
/// 3. No trailing zero-count run
/// 4. No two adjacent runs can be merged (implied by alternating, but
///    a zero-count interior run would effectively merge its neighbors)
fn bool_validate_encoding(slab: &[u8]) -> Result<(), String> {
    if slab.is_empty() {
        return Ok(());
    }

    let mut byte_pos = 0;
    let mut run_index = 0;
    let mut value = false; // first run is always false

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..])
            .ok_or_else(|| format!("truncated count at byte {byte_pos}"))?;

        if count == 0 && run_index > 0 {
            return Err(format!(
                "run {run_index} (value={value}): zero count in non-first run"
            ));
        }

        // Check for trailing zero: if this is the last run and count is 0
        let next_pos = byte_pos + cb;
        if next_pos >= slab.len() && count == 0 {
            return Err(format!(
                "run {run_index} (value={value}): trailing zero-count run"
            ));
        }

        byte_pos = next_pos;
        value = !value;
        run_index += 1;
    }

    Ok(())
}

// ── split_at_item ────────────────────────────────────────────────────────────

/// Split a boolean slab at logical item `index`.
fn bool_split_at_item(slab: &[u8], index: usize, len: usize) -> (Vec<u8>, Vec<u8>) {
    if index == 0 {
        return (vec![], slab.to_vec());
    }
    if index >= len {
        return (slab.to_vec(), vec![]);
    }

    let mut byte_pos = 0;
    let mut item_pos = 0;
    let mut value = false;

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..]).unwrap();

        if index <= item_pos + count {
            let k = index - item_pos;
            if k == 0 {
                // Split at run boundary.
                let left = slab[..byte_pos].to_vec();
                let mut right = vec![];
                // Right half must start with false. If current run is true,
                // prepend a 0-count false run.
                if value {
                    right.extend(encode_count(0));
                }
                right.extend_from_slice(&slab[byte_pos..]);
                return (left, right);
            }
            if k == count {
                // Split at end of this run.
                let left = slab[..byte_pos + cb].to_vec();
                let rest_start = byte_pos + cb;
                let mut right = vec![];
                let next_value = !value;
                if next_value {
                    right.extend(encode_count(0));
                }
                right.extend_from_slice(&slab[rest_start..]);
                return (left, right);
            }
            // Mid-run split.
            let mut left = slab[..byte_pos].to_vec();
            left.extend(encode_count(k));

            let remaining = count - k;
            let mut right = vec![];
            // The right half starts with the same value as the current run.
            // If that value is true, prepend a 0-count false.
            if value {
                right.extend(encode_count(0));
            }
            right.extend(encode_count(remaining));
            right.extend_from_slice(&slab[byte_pos + cb..]);
            return (left, right);
        }

        item_pos += count;
        byte_pos += cb;
        value = !value;
    }

    (slab.to_vec(), vec![])
}

// ── merge_slab_bytes ─────────────────────────────────────────────────────────

/// Merge two boolean slabs. Decodes only boundary runs and memcopies
/// interiors.
fn bool_merge_slab_bytes(a: &[u8], b: &[u8]) -> (Vec<u8>, usize) {
    if a.is_empty() {
        return (b.to_vec(), bool_count_segments(b));
    }
    if b.is_empty() {
        return (a.to_vec(), bool_count_segments(a));
    }

    // Find the last run of `a`: walk to end.
    let (a_last_start, _a_last_cb, a_last_count, a_last_value) = {
        let mut byte_pos = 0;
        let mut last_start = 0;
        let mut last_cb = 0;
        let mut last_count = 0;
        let mut value = false;

        while byte_pos < a.len() {
            let (cb, count) = read_count(&a[byte_pos..]).unwrap();
            last_start = byte_pos;
            last_cb = cb;
            last_count = count;
            byte_pos += cb;
            if byte_pos < a.len() {
                value = !value;
            }
        }
        (last_start, last_cb, last_count, value)
    };

    // Find the first run of `b`.
    let (b_first_cb, b_first_count, _b_first_value) = {
        let (cb, count) = read_count(b).unwrap();
        (cb, count, false) // bool slabs always start with false
    };

    let a_interior = &a[..a_last_start];
    let b_rest = &b[b_first_cb..];

    // `b` always starts with a false run.  If `b_first_count == 0`, that run
    // is structural padding and the first *real* run is the next one (true).
    //
    // Merge cases:
    //   a ends false, b starts false (>0) → merge counts
    //   a ends false, b starts false (=0) → skip padding, concat a + b_rest
    //   a ends true,  b starts false (>0) → concat a + b (correct alternation)
    //   a ends true,  b starts false (=0) → skip padding, merge a_last(true) + b_second(true)

    if !a_last_value {
        // a ends with false.
        if b_first_count > 0 {
            // Both false → merge into one run.
            let merged = a_last_count + b_first_count;
            let mut result = Vec::with_capacity(a.len() + b.len());
            result.extend_from_slice(a_interior);
            result.extend(encode_count(merged));
            result.extend_from_slice(b_rest);
            let segs = bool_count_segments(&result);
            (result, segs)
        } else {
            // b starts with 0-count false padding → skip it.
            // a ends false, b's next run is true → proper alternation.
            let mut result = Vec::with_capacity(a.len() + b.len());
            result.extend_from_slice(a);
            result.extend_from_slice(b_rest);
            let segs = bool_count_segments(&result);
            (result, segs)
        }
    } else {
        // a ends with true.
        if b_first_count > 0 {
            // a ends true, b starts false (>0) → proper alternation, just concat.
            let mut result = Vec::with_capacity(a.len() + b.len());
            result.extend_from_slice(a);
            result.extend_from_slice(b);
            let segs = bool_count_segments(&result);
            (result, segs)
        } else {
            // a ends true, b starts 0-count false padding.
            // b's second run is true — same value as a's last run → merge them.
            if b_rest.is_empty() {
                // b has no actual items.
                let segs = bool_count_segments(a);
                (a.to_vec(), segs)
            } else {
                let (cb2, count2) = read_count(b_rest).unwrap();
                let merged = a_last_count + count2;
                let mut result = Vec::with_capacity(a.len() + b.len());
                result.extend_from_slice(a_interior);
                result.extend(encode_count(merged));
                result.extend_from_slice(&b_rest[cb2..]);
                let segs = bool_count_segments(&result);
                (result, segs)
            }
        }
    }
}

// ── Streaming encoder ────────────────────────────────────────────────────────

/// Encode a sequence of booleans into pre-split slabs in a single O(n) pass.
///
/// Each returned tuple is `(data, item_count, segment_count)`.
fn bool_encode_all_slabs(values: &[bool], max_segments: usize) -> Vec<(Vec<u8>, usize, usize)> {
    if values.is_empty() {
        return vec![];
    }

    let mut slabs: Vec<(Vec<u8>, usize, usize)> = Vec::new();
    let mut out = Vec::new();
    let mut out_items: usize = 0;
    let mut out_segs: usize = 0;

    let mut current_value = false; // first run is always false
    let mut current_count: usize = 0;

    for &v in values {
        if v == current_value {
            current_count += 1;
        } else {
            // Flush the finished run.
            out.extend(encode_count(current_count));
            if current_count > 0 {
                out_segs += 1;
            }
            out_items += current_count;

            // Check if we should cut the slab before starting the next run.
            // The next run will be at least 1 segment.
            if out_segs > 0 && out_segs + 1 > max_segments {
                slabs.push((std::mem::take(&mut out), out_items, out_segs));
                out_items = 0;
                out_segs = 0;
                // New slab always starts with false. If the new value is true,
                // emit a 0-count false run as structural padding.
                if v {
                    out.extend(encode_count(0));
                }
                current_value = v;
                current_count = 1;
                continue;
            }

            current_value = !current_value;
            current_count = 1;
        }
    }

    // Flush final run.
    if current_count > 0 {
        out.extend(encode_count(current_count));
        out_segs += 1;
        out_items += current_count;
    }

    if out_items > 0 {
        slabs.push((out, out_items, out_segs));
    }

    slabs
}

// ── Load & verify ─────────────────────────────────────────────────────────

/// Validate boolean-encoded bytes and split into slabs via direct memcpy.
///
/// Because runs alternate false/true starting at position 0, cutting after
/// an even number of runs guarantees the next slab starts on a false run —
/// exactly what the wire format expects.  No intermediate representation
/// or re-encoding is needed: we just validate and byte-copy.
///
/// If `max_segments` is odd it is rounded down to even (17 → 16).
fn bool_load_and_verify(
    data: &[u8],
    max_segments: usize,
    validate: Option<fn(bool) -> Option<String>>,
) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    // Round down to even (min 2) so every cut lands on an even run
    // boundary and the next slab naturally starts with a false run.
    let runs_per_slab = (max_segments & !1).max(2);

    let mut slabs: Vec<(Vec<u8>, usize, usize)> = Vec::new();
    let mut pos: usize = 0;
    let mut slab_start: usize = 0;
    let mut slab_items: usize = 0;
    let mut slab_segs: usize = 0;
    let mut slab_runs: usize = 0;
    let mut run_index: usize = 0; // global, for validation

    while pos < data.len() {
        let (cb, count) = read_count(&data[pos..]).ok_or(PackError::BadFormat)?;

        // Only the very first run may have count 0 (structural padding).
        if count == 0 && run_index > 0 {
            return Err(PackError::BadFormat);
        }

        // A trailing zero-count run is invalid.
        let next_pos = pos + cb;
        if next_pos >= data.len() && count == 0 {
            return Err(PackError::BadFormat);
        }

        slab_items += count;
        slab_runs += 1;
        if count > 0 {
            slab_segs += 1;
            if let Some(validate) = validate {
                // run_index is 0-based; even runs are false, odd are true.
                let value = run_index % 2 != 0;
                if let Some(msg) = validate(value) {
                    return Err(PackError::InvalidValue(msg));
                }
            }
        }

        pos = next_pos;
        run_index += 1;

        // Cut after `runs_per_slab` runs — always even, so the next slab
        // starts on a false run and can be memcpy'd as-is.
        if slab_runs >= runs_per_slab && slab_segs > 0 {
            slabs.push((data[slab_start..pos].to_vec(), slab_items, slab_segs));
            slab_start = pos;
            slab_items = 0;
            slab_segs = 0;
            slab_runs = 0;
        }
    }

    if slab_items > 0 {
        slabs.push((data[slab_start..pos].to_vec(), slab_items, slab_segs));
    }

    Ok(slabs)
}

// ── streaming_save ────────────────────────────────────────────────────────

/// Find the byte offset and run-count of the last run in a bool slab.
/// Returns `(byte_offset, count_bytes, count, value)`.
fn bool_last_run(slab: &[u8]) -> Option<(usize, usize, usize, bool)> {
    if slab.is_empty() {
        return None;
    }
    let mut byte_pos = 0;
    let mut value = false;
    let mut last_start = 0;
    let mut last_cb = 0;
    let mut last_count = 0;

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..])?;
        last_start = byte_pos;
        last_cb = cb;
        last_count = count;
        byte_pos += cb;
        if byte_pos < slab.len() {
            value = !value;
        }
    }
    Some((last_start, last_cb, last_count, value))
}

/// O(n) serialization of multiple boolean slabs into a single byte array.
///
/// Memcopies slab interiors and only decodes/re-encodes boundary runs
/// between adjacent slabs.
fn bool_streaming_save(slabs: &[&[u8]]) -> Vec<u8> {
    if slabs.is_empty() {
        return vec![];
    }
    if slabs.len() == 1 {
        return slabs[0].to_vec();
    }

    let total_bytes: usize = slabs.iter().map(|s| s.len()).sum();
    let mut out = Vec::with_capacity(total_bytes);

    // Copy first non-empty slab entirely.
    let mut start_idx = 0;
    for (i, &slab) in slabs.iter().enumerate() {
        if !slab.is_empty() {
            out.extend_from_slice(slab);
            start_idx = i + 1;
            break;
        }
    }
    if out.is_empty() {
        return vec![];
    }

    // Track the last run in `out` so we don't re-scan the whole buffer.
    let (mut out_last_start, _, mut out_last_count, mut out_last_value) =
        bool_last_run(&out).unwrap();

    for &slab in &slabs[start_idx..] {
        if slab.is_empty() {
            continue;
        }

        // Parse the first run of the incoming slab (always starts as `false`).
        let (b_first_cb, b_first_count) = read_count(slab).unwrap();
        let b_rest = &slab[b_first_cb..];

        // Cases mirror bool_merge_slab_bytes:
        //   out ends false + b starts false(>0)   → merge counts
        //   out ends false + b starts false(=0)   → skip padding, concat b_rest
        //   out ends true  + b starts false(>0)   → proper alternation, concat whole slab
        //   out ends true  + b starts false(=0)   → skip padding, merge true runs

        // The "tail" is whatever bytes from this slab follow the boundary merge.
        // We'll compute `tail` (the bytes to memcopy) and whether we need to
        // modify the last run of `out`.

        let tail: &[u8];

        if !out_last_value {
            if b_first_count > 0 {
                // Merge false runs.
                let merged = out_last_count + b_first_count;
                out.truncate(out_last_start);
                out_last_start = out.len();
                let enc = encode_count(merged);
                out.extend(enc.into_iter());

                out_last_count = merged;
                // out_last_value stays false
                tail = b_rest;
            } else {
                // Skip 0-count padding; b_rest starts with true, proper alternation.
                tail = b_rest;
            }
        } else if b_first_count > 0 {
            // Proper alternation: out ends true, b starts false(>0). Concat whole slab.
            tail = slab;
        } else {
            // b starts with 0-count padding; b's second run is true → merge.
            if b_rest.is_empty() {
                continue;
            }
            let (b2_cb, b2_count) = read_count(b_rest).unwrap();
            let merged = out_last_count + b2_count;
            out.truncate(out_last_start);
            out_last_start = out.len();
            let enc = encode_count(merged);
            out.extend(enc.into_iter());

            out_last_count = merged;
            // out_last_value stays true
            tail = &b_rest[b2_cb..];
        }

        // Append the remaining bytes and update the last-run tracker.
        if !tail.is_empty() {
            // We know what value the first run in `tail` represents:
            // it's the opposite of the current out_last_value (since we handled
            // the merge above and `tail` is the continuation).
            //
            // Walk `tail` to find its last run, counting parity from the known
            // starting value. This is O(tail) not O(out), so overall O(n).
            let tail_first_value = !out_last_value;
            let tail_offset = out.len();
            out.extend_from_slice(tail);

            // Find last run in tail by walking it.
            let mut pos = 0;
            let mut value = tail_first_value;
            let mut last_start = 0;
            let mut last_count = 0;
            while pos < tail.len() {
                let (cb, count) = read_count(&tail[pos..]).unwrap();
                last_start = pos;
                last_count = count;
                pos += cb;
                if pos < tail.len() {
                    value = !value;
                }
            }
            out_last_start = tail_offset + last_start;
            out_last_count = last_count;
            out_last_value = value;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::super::Column;

    fn build_bool(values: &[bool]) -> Column<bool> {
        let mut col = Column::<bool>::new();
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        col
    }

    fn assert_bool(col: &Column<bool>, expected: &[bool]) {
        assert_eq!(col.len(), expected.len(), "length mismatch");
        for (i, &v) in expected.iter().enumerate() {
            assert_eq!(col.get(i), Some(v), "mismatch at {i}");
        }
        col.validate_encoding();
    }

    #[test]
    fn bool_build_all_false() {
        let col = build_bool(&[false; 5]);
        assert_bool(&col, &[false; 5]);
        assert_eq!(col.save(), &[5]); // Wire: [5]
    }

    #[test]
    fn bool_build_all_true() {
        let col = build_bool(&[true; 5]);
        assert_bool(&col, &[true; 5]);
        assert_eq!(col.save(), &[0, 5]); // Wire: [0 false, 5 true]
    }

    #[test]
    fn bool_build_alternating() {
        let col = build_bool(&[true, true, false, true]);
        assert_bool(&col, &[true, true, false, true]);
        assert_eq!(col.save(), &[0, 2, 1, 1]); // 0 false, 2 true, 1 false, 1 true
    }

    #[test]
    fn bool_delete_middle() {
        let mut col = build_bool(&[true, true, false, true]);
        assert_eq!(col.get(2), Some(false));
        col.remove(2);
        assert_bool(&col, &[true, true, true]);
    }

    #[test]
    fn bool_delete_merges_neighbors() {
        let mut col = build_bool(&[false, false, true, false, false]);
        assert_eq!(col.get(2), Some(true));
        col.remove(2);
        assert_bool(&col, &[false, false, false, false]);
        assert_eq!(col.save(), &[4]);
    }

    #[test]
    fn bool_insert_split_run() {
        let mut col = build_bool(&[true, true, true, true]);
        col.insert(2, false); // [true, true, false, true, true]
        assert_bool(&col, &[true, true, false, true, true]);
        assert_eq!(col.save(), &[0, 2, 1, 2]);
    }

    #[test]
    fn bool_insert_at_boundary_extends_prev() {
        let mut col = build_bool(&[false, false, true, true]);
        col.insert(2, false); // extends false run
        assert_bool(&col, &[false, false, false, true, true]);
        assert_eq!(col.save(), &[3, 2]);
    }

    #[test]
    fn bool_delete_last_in_run() {
        let mut col = build_bool(&[true]);
        assert_eq!(col.get(0), Some(true));
        col.remove(0);
        assert_eq!(col.len(), 0);
        assert!(col.save().is_empty());
        col.validate_encoding();
    }

    #[test]
    fn bool_delete_first_run_next_is_true() {
        let mut col = build_bool(&[false, true, true]);
        assert_eq!(col.get(0), Some(false));
        col.remove(0);
        assert_bool(&col, &[true, true]);
        assert_eq!(col.save(), &[0, 2]);
    }

    #[test]
    fn bool_fuzz_sequential_insert_delete() {
        let mut col = build_bool(&[
            true, false, true, true, false, false, true, false, true, true,
        ]);
        let mut mirror = vec![
            true, false, true, true, false, false, true, false, true, true,
        ];
        assert_bool(&col, &mirror);

        // Delete from front
        for _ in 0..3 {
            let expected = mirror.remove(0);
            assert_eq!(col.get(0), Some(expected));
            col.remove(0);
        }
        assert_bool(&col, &mirror);

        // Insert in middle
        col.insert(2, false);
        mirror.insert(2, false);
        col.insert(4, true);
        mirror.insert(4, true);
        assert_bool(&col, &mirror);
    }

    /// Regression: bool_merge_slab_bytes incorrectly handled boundary runs
    /// when merging slabs with different last/first values.
    #[test]
    fn bool_repeated_splice_replace_5() {
        let mut col = Column::<bool>::new();
        let mut mirror: Vec<bool> = Vec::new();
        for i in 0..100 {
            let v = i % 3 == 0;
            col.insert(i, v);
            mirror.insert(i, v);
        }
        assert_bool(&col, &mirror);

        for iter in 0..200 {
            let len = col.len();
            if len < 6 {
                break;
            }
            let pos = (iter * 7 + 13) % (len - 5);
            col.splice(pos, 5, (0..5).map(|j| (iter + j) % 2 == 0));
            mirror.splice(pos..pos + 5, (0..5).map(|j| (iter + j) % 2 == 0));
            assert_bool(&col, &mirror);
        }
    }
}
