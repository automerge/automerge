use crate::PackError;

type Slab = super::column::Slab<u8>;
use super::encoding::{ColumnEncoding, RunDecoder, SlabInfo};
use super::leb::{encode_count, read_count};
use super::{AsColumnRef, Run};

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

/// Validate a bool slab's len, segments, and tail. Panics on mismatch.
#[cfg(debug_assertions)]
fn validate_slab(slab: &Slab) {
    let info = bool_validate_encoding(&slab.data).expect("invalid bool encoding");
    assert_eq!(slab.len, info.len, "bool slab len mismatch");
    assert_eq!(slab.segments, info.segments, "bool slab segments mismatch");
    assert_eq!(slab.tail, info.tail, "bool slab tail mismatch");
}

// ── Partition ───────────────────────────────────────────────────────────────

/// One side of a partition split within a boolean slab.
///
/// Describes a partial (or complete) run at the boundary between the
/// unmodified prefix/suffix bytes and the splice region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

impl BoolPartition {
    fn padding(&self, cur_count: usize, cur_value: bool) -> usize {
        (cur_count > 0) as usize + (self.count > 0 && self.value != cur_value) as usize
    }
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
    slab: &Slab,
    start_index: usize,
    end_index: usize,
) -> Option<(BoolPartition, BoolPartition)> {
    let data: &[u8] = &slab.data;
    debug_assert!(start_index <= end_index);

    let mut byte_pos = 0;
    let mut item_pos: usize = 0;
    let mut value = false;
    let mut segments: usize = 0;
    let mut prefix = None;
    let mut suffix = None;

    while byte_pos < data.len() {
        let (cb, count) = read_count(&data[byte_pos..]).unwrap();
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

        segments += 1;

        // End cursor: once prefix is set, find where end_index falls
        if prefix.is_some() && suffix.is_none() && end_index < run_end_item {
            // end_index falls strictly within this run.
            // Suffix segments = total segments from run_end_byte onward.
            let suffix_segs = slab.segments - segments;
            debug_assert_eq!(suffix_segs, bool_count_segments(&data[run_end_byte..]));
            // This run contributes 1 segment to the suffix (the partial run).
            suffix = Some(BoolPartition {
                value,
                count: run_end_item - end_index,
                pos: run_end_byte,
                segments: suffix_segs,
            });
            break;
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
/// Returns overflow slabs `Vec<Slab>` on success.
pub(crate) fn splice_slab(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = (bool, usize)>,
    max_segments: usize,
) -> Vec<Slab> {
    let end_index = index + del;
    assert!(end_index <= slab.len, "del extends beyond slab");

    let (prefix, suffix) = if slab.data.is_empty() {
        (BoolPartition::default(), BoolPartition::default())
    } else {
        find_partition(slab, index, end_index).expect("find_partition failed")
    };

    let slab_data = &mut slab.data;

    // Save raw suffix before we modify slab_data.
    let mut raw_suffix = vec![];
    // Items in raw suffix bytes (data[suffix.pos..]), NOT including suffix.count.
    let raw_suffix_item_count = slab.len - end_index - suffix.count;
    let prefix_item_count = index - prefix.count; // items in data[..prefix.pos]

    let old_tail = slab.tail;
    let mut buf = Vec::new();
    let mut segments = prefix.segments;
    let mut len: usize = 0;
    let mut overflow: Vec<Slab> = Vec::new();
    let mut overflowed = false;
    let mut items_inserted: usize = 0;
    let mut target_segments = max_segments;

    let mut cur_value = prefix.value;
    let mut cur_count = prefix.count;
    let mut tail = prefix.pos as u8;

    for (val, count) in values {
        if count == 0 {
            continue;
        }
        items_inserted += count;
        if val == cur_value {
            cur_count += count;
        } else {
            // Flush current run.
            let c = encode_count(cur_count);
            tail = c.len() as u8;
            buf.extend(c);
            len += cur_count;
            segments += 1;
            cur_value = !cur_value;
            cur_count = count;

            // Check if we've hit the segment budget.
            if segments >= target_segments {
                if !overflowed {
                    overflowed = true;
                    target_segments = max_segments / 2;
                    raw_suffix = slab_data[suffix.pos..].to_vec(); // save suffix
                    slab_data.truncate(prefix.pos);
                    slab_data.extend_from_slice(&buf);
                    let new_len = prefix_item_count + len;
                    slab.len = new_len;
                    slab.segments = segments;
                    slab.tail = tail;
                    buf.clear();
                } else {
                    overflow.push(Slab {
                        data: buf,
                        len,
                        segments,
                        tail,
                    });
                    buf = Vec::new();
                }
                segments = 0;
                len = 0;
                tail = 0;
                if cur_value {
                    // Zero-count false padding — counts as a segment.
                    buf.extend(encode_count(0));
                    segments = 1;
                    tail = 1;
                }
            }
        }
    }

    // Check if suffix would push us over max_segments before merging it.
    // Estimate: current segments + 1 (flush cur_count) + possible suffix boundary + suffix.segments
    let suffix_extra = suffix.padding(cur_count, cur_value) + suffix.segments;

    if !overflowed && segments + suffix_extra > max_segments {
        // Flush cur_count, commit buf to main slab, put suffix in overflow.
        if cur_count > 0 {
            let c = encode_count(cur_count);
            tail = c.len() as u8;
            buf.extend(c);
            len += cur_count;
            segments += 1;
        }

        raw_suffix = slab_data[suffix.pos..].to_vec(); // save suffix
        slab_data.splice(prefix.pos.., buf);
        slab.len = prefix_item_count + len;
        slab.segments = segments;
        slab.tail = tail;

        // Build suffix slab.
        let mut suffix_buf = Vec::new();
        let mut suffix_segs = 0;
        let mut suffix_len = 0;
        let mut suffix_tail = 0u8;
        // Bool slabs must start on a false run.
        if suffix.value && suffix.count > 0 {
            suffix_buf.extend(encode_count(0));
            suffix_segs += 1;
        }
        if suffix.count > 0 {
            let c = encode_count(suffix.count);
            suffix_tail = c.len() as u8;
            suffix_buf.extend(c);
            suffix_len += suffix.count;
            suffix_segs += 1;
        }
        if suffix.segments > 0 {
            suffix_tail = old_tail;
        }
        suffix_buf.extend_from_slice(&raw_suffix);
        suffix_len += raw_suffix_item_count;
        suffix_segs += suffix.segments;
        if suffix_len > 0 {
            overflow.push(Slab {
                data: suffix_buf,
                len: suffix_len,
                segments: suffix_segs,
                tail: suffix_tail,
            });
        }

        #[cfg(debug_assertions)]
        validate_slab(slab);
        #[cfg(debug_assertions)]
        for s in &overflow {
            validate_slab(s);
        }
        return overflow;
    }

    // Merge suffix into the current run.
    if suffix.count > 0 {
        if suffix.value == cur_value {
            cur_count += suffix.count;
        } else {
            // Flush, then start the suffix run.
            let c = encode_count(cur_count);
            tail = c.len() as u8;
            buf.extend(c);
            len += cur_count;
            segments += 1;
            cur_count = suffix.count;
        }
    }

    // Flush final run.
    if cur_count > 0 {
        let c = encode_count(cur_count);
        tail = c.len() as u8;
        buf.extend(c);
        len += cur_count;
        segments += 1;
    }

    if !overflowed {
        // Common case: everything fits in the original slab.
        if suffix.segments == 0 {
            slab.tail = tail;
        }
        slab_data.splice(prefix.pos..suffix.pos, buf);
        slab.len = slab.len - del + items_inserted;
        slab.segments = segments + suffix.segments;
        #[cfg(debug_assertions)]
        validate_slab(slab);
    } else {
        // Overflowed — attach suffix to the last overflow slab.
        let suffix_total_segs = segments + suffix.segments;

        if suffix_total_segs <= max_segments {
            // Suffix fits on the current overflow buf.
            if suffix.segments > 0 {
                tail = old_tail;
            }
            buf.extend_from_slice(&raw_suffix);
            len += raw_suffix_item_count;

            overflow.push(Slab {
                data: buf,
                len,
                segments: suffix_total_segs,
                tail,
            });
        } else {
            // Suffix would exceed max_segments — flush current buf,
            // then put suffix in its own slab.
            if segments > 0 || !buf.is_empty() {
                overflow.push(Slab {
                    data: buf,
                    len,
                    segments,
                    tail,
                });
            }

            // Build suffix slab: partial run + raw suffix bytes.
            let mut suffix_buf = Vec::new();
            let mut suffix_segs = 0;
            let mut suffix_len = 0;
            let mut suffix_tail = 0u8;

            if suffix.count > 0 {
                let c = encode_count(suffix.count);
                suffix_tail = c.len() as u8;
                suffix_buf.extend(c);
                suffix_len += suffix.count;
                suffix_segs += 1;
            }
            if suffix.segments > 0 {
                suffix_tail = old_tail;
            }
            suffix_buf.extend_from_slice(&raw_suffix);
            suffix_len += raw_suffix_item_count;
            suffix_segs += suffix.segments;

            if suffix_len > 0 {
                // Ensure slab starts on a false run.
                if suffix.value && suffix.count > 0 {
                    let mut padded = Vec::new();
                    padded.extend(encode_count(0)); // zero-count false
                    padded.extend_from_slice(&suffix_buf);
                    suffix_segs += 1;
                    if suffix_segs == 1 {
                        suffix_tail = 1; // just the padding byte
                    }
                    suffix_buf = padded;
                }

                overflow.push(Slab {
                    data: suffix_buf,
                    len: suffix_len,
                    segments: suffix_segs,
                    tail: suffix_tail,
                });
            }
        }

        #[cfg(debug_assertions)]
        validate_slab(slab);
        #[cfg(debug_assertions)]
        for s in &overflow {
            validate_slab(s);
        }
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
        BoolDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            value: true,
        }
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
        self.next_run_max(usize::MAX)
    }

    fn next_run_max(&mut self, max: usize) -> Option<Run<bool>> {
        loop {
            if self.remaining > 0 {
                let count = self.remaining.min(max);
                let value = self.value;
                self.remaining -= count;
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
    type Tail = u8;

    fn fill(len: usize, value: bool) -> Slab {
        let mut data = Vec::new();
        let segments = if value {
            leb128::write::unsigned(&mut data, 0).unwrap();
            2 // zero-count false + true
        } else {
            1
        };
        let t = data.len();
        leb128::write::unsigned(&mut data, len as u64).unwrap();
        let tail = (data.len() - t) as u8;
        Slab {
            data,
            len,
            segments,
            tail,
        }
    }

    fn merge_slabs(a: &mut Slab, b: Slab) {
        if a.len == 0 {
            *a = b;
        } else if b.len > 0 {
            let (new_segs, new_tail) = bool_merge_slabs(&mut a.data, a.tail, a.segments, &b);
            a.len += b.len;
            a.segments = new_segs;
            a.tail = new_tail;
        }
        #[cfg(debug_assertions)]
        validate_slab(a);
    }

    fn last_run(slab: &Slab) -> Option<Run<bool>> {
        use super::encoding::RunDecoder;
        if slab.len == 0 {
            return None;
        }
        let mut dec = Self::decoder(&slab.data);
        let mut last_val = None;
        let mut last_count = 0;
        while let Some(run) = dec.next_run() {
            last_val = Some(run.value);
            last_count = run.count;
        }
        Some(Run {
            count: last_count,
            value: last_val?,
        })
    }

    fn validate_encoding(slab: &[u8]) -> Result<SlabInfo<u8>, PackError> {
        bool_validate_encoding(slab)
    }

    fn load_and_verify_fold<'a, F, P: Default + Copy>(
        data: &'a [u8],
        max_segments: usize,
        validate: Option<F>,
    ) -> Result<Vec<Slab>, PackError>
    where
        F: Fn(
            P,
            usize,
            <<BoolEncoding as ColumnEncoding>::Value as super::ColumnValueRef>::Get<'a>,
        ) -> Result<P, String>,
    {
        bool_load_and_verify(data, max_segments, validate.as_ref())
    }

    fn do_merge(
        acc: &mut Vec<u8>,
        a_tail: u8,
        a_segments: usize,
        b: &Slab,
        _buf: &mut Vec<u8>,
    ) -> (usize, u8) {
        if b.len == 0 || b.data.is_empty() {
            (a_segments, a_tail)
        } else {
            bool_merge_slabs(acc, a_tail, a_segments, b)
        }
    }

    fn splice_slab<V: AsColumnRef<bool>>(
        slab: &mut Slab,
        index: usize,
        del: usize,
        values: impl Iterator<Item = (V, usize)>,
        max_segments: usize,
    ) -> (Vec<Slab>, usize) {
        let slab_del = del.min(slab.len - index);
        let overflow_del = del - slab_del;
        let bools = values.map(|(v, count)| (v.as_column_ref(), count));
        let overflow_slabs = splice_slab(slab, index, slab_del, bools, max_segments);
        (overflow_slabs, overflow_del)
    }

    type Decoder<'a> = BoolDecoder<'a>;

    fn decoder(slab: &[u8]) -> BoolDecoder<'_> {
        BoolDecoder::new(slab)
    }

    type Encoder<'a> = super::encoder::BoolEncoder;

    fn encoder<'a>() -> Self::Encoder<'a> {
        super::encoder::BoolEncoder::new()
    }
}

// ── count_segments ───────────────────────────────────────────────────────────

fn bool_count_segments(slab: &[u8]) -> usize {
    let mut byte_pos = 0;
    let mut segments = 0;

    while byte_pos < slab.len() {
        let (cb, _count) = match read_count(&slab[byte_pos..]) {
            Some(v) => v,
            None => break,
        };
        segments += 1;
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
fn bool_validate_encoding(slab: &[u8]) -> Result<SlabInfo<u8>, PackError> {
    if slab.is_empty() {
        return Ok(SlabInfo {
            segments: 0,
            len: 0,
            tail: 0,
        });
    }

    let mut byte_pos = 0;
    let mut run_index = 0;
    let mut value = false;
    let mut segments = 0;
    let mut len = 0;
    let mut last_cb: u8 = 0;

    while byte_pos < slab.len() {
        let (cb, count) = read_count(&slab[byte_pos..]).ok_or(PackError::BadFormat)?;

        if count == 0 && run_index > 0 {
            return Err(PackError::InvalidValue(format!(
                "run {run_index} (value={value}): zero count in non-first run"
            )));
        }

        let next_pos = byte_pos + cb;
        if next_pos >= slab.len() && count == 0 {
            return Err(PackError::InvalidValue(format!(
                "run {run_index} (value={value}): trailing zero-count run"
            )));
        }

        segments += 1;
        len += count;
        last_cb = cb as u8;

        byte_pos = next_pos;
        value = !value;
        run_index += 1;
    }

    Ok(SlabInfo {
        segments,
        len,
        tail: last_cb,
    })
}

// ── merge_slab_bytes ─────────────────────────────────────────────────────────

/// Merge two boolean slabs. Decodes only boundary runs and memcopies
/// interiors.
/// In-place merge of bool slab `b` into `a`. No extra allocation beyond
/// extending `a`'s buffer. Both slabs must be non-empty.
fn bool_merge_slabs(a_data: &mut Vec<u8>, a_tail: u8, a_segments: usize, b: &Slab) -> (usize, u8) {
    // a's last run — derived from tail (no scan).
    if b.len == 0 {
        return (a_segments, a_tail);
    }

    let a_last_start = a_data.len() - a_tail as usize;
    let (a_last_cb, a_last_count) = read_count(&a_data[a_last_start..]).unwrap();
    debug_assert_eq!(a_last_cb, a_tail as usize);
    // Runs alternate false/true: seg 1=false, 2=true, 3=false...
    // Even segments → last is true, odd → last is false.
    let a_last_value = a_segments % 2 == 0;

    // b's first run (always starts with false).
    let b_data: &[u8] = &b.data;
    let (b_first_cb, b_first_count) = read_count(b_data).unwrap();
    let b_rest = &b_data[b_first_cb..];

    let b_segments = b.segments;

    // Bool slabs alternate false/true. b always starts with false.
    // Track whether we merged boundary runs.
    let mut merged_boundary = 0;
    let mut merge_bytes = 0;
    let mut b_empty = b_rest.is_empty();

    if !a_last_value {
        // a ends false, b starts false.
        if b_first_count > 0 {
            // Same value — merge counts. Removes 1 segment (the two false runs become one).
            a_data.truncate(a_last_start);
            let count = encode_count(a_last_count + b_first_count);
            merge_bytes = count.len() as u8;
            a_data.extend(count);
            a_data.extend_from_slice(b_rest);
            merged_boundary = 1;
        } else {
            // b starts with 0-count padding → skip it.
            // Dropping b's zero-count first segment.
            a_data.extend_from_slice(b_rest);
            merged_boundary = 1;
        }
    } else {
        // a ends true.
        if b_first_count > 0 {
            // Proper alternation, just append all of b.
            a_data.extend_from_slice(b_data);
            b_empty = false;
        } else {
            // b starts with 0-count false padding.
            // b's second run is true — merge with a's last true run.
            // Removes 2 segments: b's zero-count false + b's first true merged into a's last true.
            if !b_rest.is_empty() {
                let (cb2, count2) = read_count(b_rest).unwrap();
                a_data.truncate(a_last_start);
                let count = encode_count(a_last_count + count2);
                merge_bytes = count.len() as u8;
                a_data.extend(count);
                a_data.extend_from_slice(&b_rest[cb2..]);
                b_empty = b_rest[cb2..].is_empty();
                merged_boundary = 2;
            } else {
                // b is just a zero-count false padding with nothing after — drop it.
                merged_boundary = 1;
            }
        }
    }

    let new_segments = a_segments + b_segments - merged_boundary;

    let new_tail = if !b_empty {
        b.tail
    } else if merged_boundary == 0 {
        a_tail
    } else {
        merge_bytes
    };

    (new_segments, new_tail)
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
fn bool_load_and_verify<F, P: Default + Copy>(
    data: &[u8],
    max_segments: usize,
    validate: Option<&F>,
) -> Result<Vec<Slab>, PackError>
where
    F: for<'a> Fn(P, usize, bool) -> Result<P, String>,
{
    if data.is_empty() {
        return Ok(vec![]);
    }

    let mut p = Default::default();

    // Target half-full slabs, rounded to even so each slab starts on a false run.
    let target_segments = ((max_segments / 2) & !1).max(2);

    let mut slabs: Vec<Slab> = Vec::new();
    let mut pos: usize = 0;
    let mut slab_start: usize = 0;
    let mut slab_items: usize = 0;
    let mut slab_segs: usize = 0;
    let mut run_index: usize = 0; // global, for validation
    let mut tail: u8 = 0;

    while pos < data.len() {
        let (cb, count) = read_count(&data[pos..]).ok_or(PackError::BadFormat)?;
        tail = cb as u8;

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
        slab_segs += 1;

        if count > 0 {
            if let Some(validate) = validate {
                let value = run_index % 2 != 0;
                match validate(p, count, value) {
                    Ok(new_p) => p = new_p,
                    Err(msg) => return Err(PackError::InvalidValue(msg)),
                }
            }
        }

        pos = next_pos;
        run_index += 1;

        // Cut after target_segments — always even, so the next slab
        // starts on a false run and can be memcpy'd as-is.
        if slab_segs >= target_segments {
            slabs.push(Slab {
                data: data[slab_start..pos].to_vec(),
                len: slab_items,
                segments: slab_segs,
                tail,
            });
            slab_start = pos;
            slab_items = 0;
            slab_segs = 0;
        }
    }

    if slab_segs > 0 {
        slabs.push(Slab {
            data: data[slab_start..pos].to_vec(),
            len: slab_items,
            segments: slab_segs,
            tail,
        });
    }

    Ok(slabs)
}

#[cfg(test)]
mod tests {
    use super::super::Column;
    use super::Slab;
    use super::{bool_count_segments, find_partition, BoolPartition};
    use crate::v1::leb::{encode_count, read_count};

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
        col.validate_encoding().unwrap();
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
        col.validate_encoding().unwrap();
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

    fn make_slab(data: Vec<u8>) -> Slab {
        let _segments = bool_count_segments(&data);
        let mut tail = 0;
        let mut len = 0;
        let mut pos = 0;
        let mut segments = 0;
        while pos < data.len() {
            let (cb, count) = read_count(&data[pos..]).unwrap();
            if count > 0 {
                segments += 1
            }
            len += count;
            pos += cb;
            tail = cb as u8;
        }
        assert_eq!(segments, _segments);
        Slab {
            data,
            len,
            segments,
            tail,
        }
    }

    #[test]
    fn partition_mid_run() {
        // [100f, 100t, 100f]
        let data = encode_runs(&[100, 100, 100]);
        let (p, s) = find_partition(&make_slab(data.clone()), 150, 160).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 200, 200).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 0, 10).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 290, 300).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 0, 300).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 50, 250).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 150, 150).unwrap();
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
        let (p, s) = find_partition(&make_slab(data.clone()), 100, 100).unwrap();
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
            let (p, s) = find_partition(&make_slab(data.clone()), idx, idx).unwrap();
            let recon = reconstruct(&data, &p, &s);
            assert_eq!(orig, recon, "identity failed at idx={idx}");
        }
    }
}
