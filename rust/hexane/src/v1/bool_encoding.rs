use crate::PackError;

use super::column::Slab;
use super::encoding::{ColumnEncoding, RunDecoder};
use super::{AsColumnRef, Run, ValidBuf, ValidBytes};

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

// ── Partition ───────────────────────────────────────────────────────────────

/// One side of a partition split within a boolean slab.
///
/// Describes a partial (or complete) run at the boundary between the
/// unmodified prefix/suffix bytes and the splice region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoolPartition {
    /// The boolean value of the run at this boundary.
    pub value: bool,
    /// Number of items from this run on the "outside" of the splice.
    /// For the start cursor: items in the prefix.
    /// For the end cursor: items in the suffix.
    pub count: usize,
    /// Byte position in the original slab.
    /// For the start cursor: raw prefix = `data[..pos]`.
    /// For the end cursor: raw suffix = `data[pos..]`.
    pub pos: usize,
    /// Number of non-zero-count segments in `data[..pos]` (start cursor)
    /// or `data[pos..]` (end cursor).
    pub segments: usize,
}

/// Find the partition boundaries for a splice at `[start_index, end_index)`.
///
/// Returns `(prefix_cursor, suffix_cursor)` such that the slab can be
/// reconstructed as:
///
/// ```text
/// data[..prefix.offset]           // raw prefix bytes (complete runs)
/// + encode(prefix.count, prefix.value)  // partial run ending the prefix
/// + [NEW DATA]
/// + encode(suffix.count, suffix.value)  // partial run starting the suffix
/// + data[suffix.offset..]         // raw suffix bytes (complete runs)
/// ```
///
/// Runs with `count == 0` are omitted during reconstruction.
pub(crate) fn find_partition(
    data: &[u8],
    start_index: usize,
    end_index: usize,
) -> Option<(BoolPartition, BoolPartition)> {
    debug_assert!(start_index <= end_index);

    let mut byte_pos = 0;
    let mut item_pos: usize = 0;
    let mut value = false;
    let mut segments: usize = 0;
    let mut prefix = None;
    let mut suffix = None;

    while byte_pos < data.len() {
        let (cb, count) = read_count(&data[byte_pos..])?;
        let run_end_item = item_pos + count;
        let run_end_byte = byte_pos + cb;

        // Start cursor: first run where start_index <= run_end_item
        if prefix.is_none() && start_index <= run_end_item {
            prefix = Some(BoolPartition {
                value,
                count: start_index - item_pos,
                pos: byte_pos,
                segments,
            });
        }

        if count > 0 {
            segments += 1;
        }

        // End cursor: once prefix is set, find where end_index falls
        #[allow(clippy::collapsible_if)]
        if prefix.is_some() && suffix.is_none() {
            if end_index < run_end_item {
                // end_index falls strictly within this run.
                // Suffix segments = total segments from run_end_byte onward.
                let suffix_segs = bool_count_segments(&data[run_end_byte..]);
                // This run contributes 1 segment to the suffix (the partial run).
                suffix = Some(BoolPartition {
                    value,
                    count: run_end_item - end_index,
                    pos: run_end_byte,
                    segments: suffix_segs,
                });
                break;
            }
            // end_index == run_end_item: falls at boundary, continue to next run
        }

        item_pos = run_end_item;
        byte_pos = run_end_byte;
        value = !value;
    }

    // end_index at or past the last item
    if prefix.is_some() && suffix.is_none() {
        suffix = Some(BoolPartition {
            value,
            count: 0,
            pos: byte_pos,
            segments: 0,
        });
    }

    Some((prefix?, suffix?))
}

// ── Fast splice ─────────────────────────────────────────────────────────────

/// Fast in-place boolean splice using partition cursors.
///
/// Builds a replacement buffer for the affected byte range and splices
/// it directly into `slab_data`, avoiding a full slab copy.
///
/// Returns `Some(new_segment_count)` on success.
pub(crate) fn splice_slab(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = bool>,
    max_segments: usize,
) -> Vec<super::column::Slab> {
    let end_index = index + del;
    assert!(end_index <= slab.len, "del extends beyond slab");

    let slab_data = slab.data.as_mut_vec();
    let (prefix, suffix) = if slab_data.is_empty() {
        let p = BoolPartition {
            value: false,
            count: 0,
            pos: 0,
            segments: 0,
        };
        (p, p)
    } else {
        find_partition(slab_data, index, end_index).expect("find_partition failed")
    };

    // Save raw suffix before we modify slab_data.
    let raw_suffix = slab_data[suffix.pos..].to_vec();
    // Items in raw suffix bytes (data[suffix.pos..]), NOT including suffix.count.
    let raw_suffix_item_count = slab.len - end_index - suffix.count;
    let prefix_item_count = index - prefix.count; // items in data[..prefix.pos]

    let mut buf = Vec::new();
    let mut segments = prefix.segments;
    let mut len: usize = 0;
    let mut overflow: Vec<super::column::Slab> = Vec::new();
    let mut overflowed = false;
    let mut items_inserted: usize = 0;

    let mut cur_value = prefix.value;
    let mut cur_count = prefix.count;

    for val in values {
        items_inserted += 1;
        if val == cur_value {
            cur_count += 1;
        } else {
            // Flush current run.
            buf.extend(encode_count(cur_count));
            len += cur_count;
            if cur_count > 0 {
                segments += 1;
            }
            cur_value = !cur_value;
            cur_count = 1;

            // Check if we've hit max segments.
            if segments >= max_segments {
                if !overflowed {
                    slab_data.truncate(prefix.pos);
                    slab_data.extend_from_slice(&buf);
                    let new_len = prefix_item_count + len;
                    slab.len = new_len;
                    slab.segments = segments;
                    overflowed = true;
                } else {
                    overflow.push(Slab {
                        data: ValidBuf::new(buf),
                        len,
                        segments,
                    });
                }
                buf = Vec::new();
                segments = 0;
                len = 0;
                if cur_value {
                    buf.extend(encode_count(0));
                }
            }
        }
    }

    // Merge suffix into the current run.
    if suffix.count > 0 {
        if suffix.value == cur_value {
            cur_count += suffix.count;
        } else {
            // Flush, then start the suffix run.
            buf.extend(encode_count(cur_count));
            len += cur_count;
            if cur_count > 0 {
                segments += 1;
            }
            cur_count = suffix.count;
        }
    }

    // Flush final run.
    if cur_count > 0 {
        buf.extend(encode_count(cur_count));
        len += cur_count;
        segments += 1;
    }

    if !overflowed {
        // Common case: everything fits in the original slab.
        slab_data.splice(prefix.pos..suffix.pos, buf);
        slab.len = slab.len - del + items_inserted;
        slab.segments = segments + suffix.segments;
    } else {
        // Append raw suffix to the last buf.
        buf.extend_from_slice(&raw_suffix);
        len += raw_suffix_item_count;
        segments += suffix.segments;

        overflow.push(Slab {
            data: ValidBuf::new(buf),
            len,
            segments,
        });
    }

    overflow
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

impl<'a> RunDecoder for BoolDecoder<'a> {
    fn next_run(&mut self) -> Option<Run<bool>> {
        loop {
            if self.remaining > 0 {
                let count = self.remaining;
                let value = self.value;
                self.remaining = 0;
                return Some(Run { count, value });
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

    fn merge_slabs(a: &mut Slab, b: &Slab) {
        bool_merge_slabs(a, b);
    }

    fn validate_encoding(slab: &[u8]) -> Result<(), String> {
        bool_validate_encoding(slab)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(bool) -> Option<String>>,
    ) -> Result<Vec<super::column::Slab>, PackError> {
        bool_load_and_verify(data, max_segments, validate)
    }

    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8> {
        bool_streaming_save(slabs)
    }

    fn splice_slab<V: AsColumnRef<bool>>(
        slab: &mut Slab,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<Slab>, usize) {
        let slab_del = del.min(slab.len - index);
        let overflow_del = del - slab_del;
        let bools = values.map(|v| v.as_column_ref());
        (
            splice_slab(slab, index, slab_del, bools, max_segments),
            overflow_del,
        )
    }

    type Decoder<'a> = BoolDecoder<'a>;

    fn decoder(slab: &ValidBytes) -> BoolDecoder<'_> {
        BoolDecoder::new(slab.as_bytes())
    }
}

// ── count_segments ───────────────────────────────────────────────────────────

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

// ── merge_slab_bytes ─────────────────────────────────────────────────────────

/// Merge two boolean slabs. Decodes only boundary runs and memcopies
/// interiors.
/// In-place merge of bool slab `b` into `a`. No extra allocation beyond
/// extending `a`'s buffer. Both slabs must be non-empty.
fn bool_merge_slabs(a: &mut super::column::Slab, b: &super::column::Slab) {
    debug_assert!(a.len > 0 && b.len > 0);

    let b_data: &[u8] = &b.data;

    // a's last run.
    let (a_last_start, _a_last_cb, a_last_count, a_last_value) = bool_last_run(&a.data).unwrap();

    // b's first run (always starts with false).
    let (b_first_cb, b_first_count) = read_count(b_data).unwrap();
    let b_rest = &b_data[b_first_cb..];

    let a_buf = a.data.as_mut_vec();

    // Bool slabs alternate false/true. b always starts with false.
    // Four cases based on a's last value and b's first count:
    if !a_last_value {
        // a ends false, b starts false.
        if b_first_count > 0 {
            // Same value — merge counts: truncate a's last run, write combined, append rest.
            a_buf.truncate(a_last_start);
            a_buf.extend(encode_count(a_last_count + b_first_count));
            a_buf.extend_from_slice(b_rest);
        } else {
            // b starts with 0-count padding → skip it, proper alternation.
            a_buf.extend_from_slice(b_rest);
        }
    } else {
        // a ends true.
        if b_first_count > 0 {
            // a ends true, b starts false (>0) → proper alternation, just append.
            a_buf.extend_from_slice(b_data);
        } else {
            // a ends true, b starts 0-count false padding.
            // b's second run is true — merge with a's last true run.
            if b_rest.is_empty() {
                // b has no actual items — nothing to append.
            } else {
                let (cb2, count2) = read_count(b_rest).unwrap();
                a_buf.truncate(a_last_start);
                a_buf.extend(encode_count(a_last_count + count2));
                a_buf.extend_from_slice(&b_rest[cb2..]);
            }
        }
    }

    a.len += b.len;
    a.segments = bool_count_segments(a_buf);
}

#[allow(dead_code)]
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
) -> Result<Vec<super::column::Slab>, PackError> {
    use super::column::Slab;
    use super::ValidBuf;

    if data.is_empty() {
        return Ok(vec![]);
    }

    let runs_per_slab = (max_segments & !1).max(2);

    let mut slabs: Vec<Slab> = Vec::new();
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
            slabs.push(Slab {
                data: ValidBuf::new(data[slab_start..pos].to_vec()),
                len: slab_items,
                segments: slab_segs,
            });
            slab_start = pos;
            slab_items = 0;
            slab_segs = 0;
            slab_runs = 0;
        }
    }

    if slab_items > 0 {
        slabs.push(Slab {
            data: ValidBuf::new(data[slab_start..pos].to_vec()),
            len: slab_items,
            segments: slab_segs,
        });
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
                out.extend(enc);

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
            out.extend(enc);

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
    use super::{encode_count, find_partition, read_count, BoolPartition};

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

    // ── find_partition tests ─────────────────────────────────────────────

    fn decode_bool_slab(data: &[u8]) -> Vec<bool> {
        let mut result = Vec::new();
        let mut byte_pos = 0;
        let mut value = false;
        while byte_pos < data.len() {
            let (cb, count) = read_count(&data[byte_pos..]).unwrap();
            for _ in 0..count {
                result.push(value);
            }
            byte_pos += cb;
            value = !value;
        }
        result
    }

    /// Encode a bool slab from alternating run counts.
    fn encode_runs(counts: &[usize]) -> Vec<u8> {
        let mut out = Vec::new();
        for &c in counts {
            out.extend(encode_count(c));
        }
        out
    }

    #[test]
    fn partition_mid_run() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 150, 160).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: true,
                count: 50,
                pos: 1,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: true,
                count: 40,
                pos: 2,
                segments: s.segments
            }
        );
    }

    #[test]
    fn partition_on_boundary() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 200, 200).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: true,
                count: 100,
                pos: 1,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: false,
                count: 100,
                pos: 3,
                segments: s.segments
            }
        );
    }

    #[test]
    fn partition_at_start() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 0, 10).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: false,
                count: 0,
                pos: 0,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: false,
                count: 90,
                pos: 1,
                segments: s.segments
            }
        );
    }

    #[test]
    fn partition_at_end() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 290, 300).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: false,
                count: 90,
                pos: 2,
                segments: p.segments
            }
        );
        // end_index == total items → no suffix run, offset at end of data
        assert_eq!(s.count, 0);
        assert_eq!(s.pos, 3);
    }

    #[test]
    fn partition_entire_slab() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 0, 300).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: false,
                count: 0,
                pos: 0,
                segments: p.segments
            }
        );
        assert_eq!(s.count, 0);
        assert_eq!(s.pos, 3);
    }

    #[test]
    fn partition_span_runs() {
        // [100f, 100t, 100f]  — delete items 50..250 (spans all three runs)
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 50, 250).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: false,
                count: 50,
                pos: 0,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: false,
                count: 50,
                pos: 3,
                segments: s.segments
            }
        );
    }

    #[test]
    fn partition_single_insert_point() {
        // [100f, 100t, 100f] — insert at position 150 (no delete)
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 150, 150).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: true,
                count: 50,
                pos: 1,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: true,
                count: 50,
                pos: 2,
                segments: s.segments
            }
        );
    }

    #[test]
    fn partition_at_run_start() {
        // [100f, 100t, 100f] — insert at position 100 (start of true run)
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&data, 100, 100).unwrap();
        assert_eq!(
            p,
            BoolPartition {
                value: false,
                count: 100,
                pos: 0,
                segments: p.segments
            }
        );
        assert_eq!(
            s,
            BoolPartition {
                value: true,
                count: 100,
                pos: 2,
                segments: s.segments
            }
        );
    }

    /// Reconstruct a bool slab from partition cursors and (empty) new data.
    /// Handles the bool alternation convention properly.
    /// Decode a bool slab suffix where the first run has value `start_value`.
    fn decode_bool_slab_with_start(data: &[u8], start_value: bool) -> Vec<bool> {
        let mut result = Vec::new();
        let mut byte_pos = 0;
        let mut value = start_value;
        while byte_pos < data.len() {
            let (cb, count) = read_count(&data[byte_pos..]).unwrap();
            for _ in 0..count {
                result.push(value);
            }
            byte_pos += cb;
            value = !value;
        }
        result
    }

    fn reconstruct(data: &[u8], p: &BoolPartition, s: &BoolPartition) -> Vec<bool> {
        let mut items = Vec::new();
        // Decode raw prefix (always starts with false)
        items.extend(decode_bool_slab(&data[..p.pos]));
        // Add prefix cursor's partial run
        for _ in 0..p.count {
            items.push(p.value);
        }
        // (new data would go here)
        // Add suffix cursor's partial run
        for _ in 0..s.count {
            items.push(s.value);
        }
        // Decode raw suffix — starts with the opposite of the suffix cursor's value
        items.extend(decode_bool_slab_with_start(&data[s.pos..], !s.value));
        items
    }

    #[test]
    fn partition_reconstruct_identity() {
        // Verify that reconstructing from partition yields the original
        // when there's no deletion and no insertion.
        let data = encode_runs(&[100, 100, 100]);
        let orig = decode_bool_slab(&data);
        for idx in [0, 50, 100, 150, 200, 250, 300] {
            let (p, s) = find_partition(&data, idx, idx).unwrap();
            let recon = reconstruct(&data, &p, &s);
            assert_eq!(orig, recon, "identity failed at idx={idx}");
        }
    }
}
