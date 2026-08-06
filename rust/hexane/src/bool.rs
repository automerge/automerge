use crate::column::splice_bytes;
use crate::encoder::BoolEncoder;
use crate::PackError;

type Slab = crate::column::Slab<u8>;
use crate::encoding::{ColumnEncoding, RunDecoder, SlabInfo};

use crate::{AsColumnRef, Codec, Leb128, Run};
use std::marker::PhantomData;
use std::ops::Range;

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
fn validate_slab<C: Codec>(slab: &Slab) {
    let info = bool_validate_encoding::<C>(&slab.data).expect("invalid bool encoding");
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

/// Build an overflow slab from a suffix partition: an optional zero-count
/// false pad (so the slab starts on a false run), the partial run at the
/// splice boundary, then the raw suffix bytes.  Returns `None` if the
/// suffix holds no items.
///
/// Shared by the two overflow paths in [`splice_slab`].
fn make_suffix_slab<C: Codec>(
    suffix: &BoolPartition,
    raw_suffix: &[u8],
    raw_suffix_item_count: usize,
    old_tail: u8,
) -> Option<Slab> {
    let mut buf = Vec::new();
    let mut segments = 0;
    let mut len = 0;
    let mut tail = 0u8;
    // Bool slabs must start on a false run.
    if suffix.value && suffix.count > 0 {
        buf.extend(C::encode_count(0));
        segments += 1;
    }
    if suffix.count > 0 {
        let c = C::encode_count(suffix.count);
        tail = c.len() as u8;
        buf.extend(c);
        len += suffix.count;
        segments += 1;
    }
    if suffix.segments > 0 {
        tail = old_tail;
    }
    buf.extend_from_slice(raw_suffix);
    len += raw_suffix_item_count;
    segments += suffix.segments;
    (len > 0).then_some(Slab {
        data: buf,
        len,
        segments,
        tail,
    })
}

/// Build an overflow slab from the raw suffix bytes alone, for the case
/// where the boundary run has already been written into an earlier slab.
///
/// The counterpart to [`make_suffix_slab`]: that one owns the partial run
/// *and* the raw bytes, this one only the raw bytes. They resume on a
/// `!suffix.value` run, so the zero-count `false` pad a bool slab needs to
/// start on is required exactly when `suffix.value` is `false`.
///
/// `raw_suffix` is empty whenever `suffix.count` is zero (the partition's
/// no-partial-run case puts `pos` at the end of the data), so there is no
/// case where the leading run's value is `suffix.value` itself.
fn make_raw_suffix_slab<C: Codec>(
    suffix: &BoolPartition,
    raw_suffix: &[u8],
    len: usize,
    tail: u8,
) -> Option<Slab> {
    if len == 0 {
        return None;
    }
    // Only reachable via the partition's partial-run case, which is what
    // makes the leading run `!suffix.value`.
    debug_assert!(suffix.count > 0, "raw suffix without a partial run");
    let mut data = Vec::new();
    let mut segments = suffix.segments;
    if !suffix.value {
        data.extend(C::encode_count(0));
        segments += 1;
    }
    data.extend_from_slice(raw_suffix);
    Some(Slab {
        data,
        len,
        segments,
        tail,
    })
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
pub(crate) fn find_partition<C: Codec>(
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
        let (cb, count) = C::read_count(&data[byte_pos..]).unwrap();
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
            debug_assert_eq!(suffix_segs, bool_count_segments::<C>(&data[run_end_byte..]));
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
pub(crate) fn splice_slab<C: Codec>(
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
        find_partition::<C>(slab, index, end_index).expect("find_partition failed")
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
            let c = C::encode_count(cur_count);
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
                    buf.extend(C::encode_count(0));
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
            let c = C::encode_count(cur_count);
            tail = c.len() as u8;
            buf.extend(c);
            len += cur_count;
            segments += 1;
        }

        raw_suffix = slab_data[suffix.pos..].to_vec(); // save suffix
        let del_bytes = slab_data.len() - prefix.pos;
        splice_bytes(slab_data, prefix.pos, del_bytes, &buf);
        slab.len = prefix_item_count + len;
        slab.segments = segments;
        slab.tail = tail;

        overflow.extend(make_suffix_slab::<C>(
            &suffix,
            &raw_suffix,
            raw_suffix_item_count,
            old_tail,
        ));

        #[cfg(debug_assertions)]
        validate_slab::<C>(slab);
        #[cfg(debug_assertions)]
        for s in &overflow {
            validate_slab::<C>(s);
        }
        return overflow;
    }

    // Merge suffix into the current run.
    if suffix.count > 0 {
        if suffix.value == cur_value {
            cur_count += suffix.count;
        } else {
            // Flush, then start the suffix run.
            let c = C::encode_count(cur_count);
            tail = c.len() as u8;
            buf.extend(c);
            len += cur_count;
            segments += 1;
            cur_count = suffix.count;
        }
    }

    // Flush final run.
    if cur_count > 0 {
        let c = C::encode_count(cur_count);
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
        splice_bytes(slab_data, prefix.pos, suffix.pos - prefix.pos, &buf);
        slab.len = slab.len - del + items_inserted;
        slab.segments = segments + suffix.segments;
        #[cfg(debug_assertions)]
        validate_slab::<C>(slab);
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
            // then put the suffix in its own slab.
            if segments > 0 || !buf.is_empty() {
                overflow.push(Slab {
                    data: buf,
                    len,
                    segments,
                    tail,
                });
            }

            // Only the raw bytes are left: `suffix.count` was merged into
            // the run flushed above. Re-emitting it here (as the other
            // overflow exit does, where the merge has not happened yet)
            // would duplicate those items.
            overflow.extend(make_raw_suffix_slab::<C>(
                &suffix,
                &raw_suffix,
                raw_suffix_item_count,
                old_tail,
            ));
        }

        #[cfg(debug_assertions)]
        validate_slab::<C>(slab);
        #[cfg(debug_assertions)]
        for s in &overflow {
            validate_slab::<C>(s);
        }
    }

    overflow
}

// ── BoolDecoder ──────────────────────────────────────────────────────────────

/// Forward iterator over all items in a single boolean-encoded slab.
///
/// Created by [`BoolEncoding::decoder`].  Each run yields the same value
/// in O(1) per item; advancing between runs reads one LEB128 count.
pub struct BoolDecoder<'a, C: Codec = Leb128> {
    data: &'a [u8],
    byte_pos: usize,
    remaining: usize,
    _codec: PhantomData<fn() -> C>,
    /// Current run's boolean value.  Initialized to `true` so the first
    /// `advance_run` flip produces `false` (matching the wire format's
    /// "first run is always false" invariant).
    value: bool,
}

impl<C: Codec> Clone for BoolDecoder<'_, C> {
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

/// A [`BoolDecoder`]'s position, detached from the slab it was reading.
///
/// Public only because [`ColumnEncoding::State`](crate::ColumnEncoding::State)
/// names it; nothing outside the crate can do anything with one.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoolDecoderState {
    byte_pos: usize,
    remaining: usize,
    /// starts `true` so the first `advance_run` flip yields `false`, as
    /// the wire format requires
    value: bool,
}

impl BoolDecoderState {
    /// The state of a decoder standing at the start of a slab.
    pub(crate) fn start() -> Self {
        Self::default()
    }
}

// hand-written rather than derived: a slab-start reader carries `value:
// true`, so that the first run's flip yields the `false` the wire format
// begins with. A derived `Default` would give `false` and read every run
// at the wrong parity.
impl Default for BoolDecoderState {
    fn default() -> Self {
        BoolDecoderState {
            byte_pos: 0,
            remaining: 0,
            value: true,
        }
    }
}

impl<'a, C: Codec> BoolDecoder<'a, C> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        BoolDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            value: true,
            _codec: PhantomData,
        }
    }

    /// Advance past `n` items in O(runs skipped). Stops at the end of
    /// the column.
    // inherent, so a call on a concrete decoder is never ambiguous with
    // the unstable `Iterator::advance_by`
    pub fn advance_by(&mut self, n: usize) {
        if n > 0 {
            self.nth(n - 1);
        }
    }

    /// Capture the position so it can be restored against the same bytes
    /// later, without holding a borrow of them.
    pub(crate) fn suspend(&self) -> BoolDecoderState {
        BoolDecoderState {
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            value: self.value,
        }
    }

    /// Restore a suspended position against `data`, which must be the
    /// bytes it was suspended from.
    pub(crate) fn resume(data: &'a [u8], state: &BoolDecoderState) -> Self {
        BoolDecoder {
            data,
            byte_pos: state.byte_pos,
            remaining: state.remaining,
            value: state.value,
            _codec: PhantomData,
        }
    }

    fn advance_run(&mut self) {
        if self.byte_pos >= self.data.len() {
            self.remaining = 0;
            return;
        }
        if let Some((cb, count)) = C::read_count(&self.data[self.byte_pos..]) {
            self.byte_pos += cb;
            self.value = !self.value;
            self.remaining = count;
        } else {
            self.remaining = 0;
        }
    }
}

impl<'a, C: Codec> Iterator for BoolDecoder<'a, C> {
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

impl<'a, C: Codec> RunDecoder for BoolDecoder<'a, C> {
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
/// `C` is the varint codec used for run counts.
pub struct BoolEncoding<C: Codec = Leb128>(PhantomData<fn() -> C>);

impl<C: Codec> Default for BoolEncoding<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<C: Codec> ColumnEncoding for BoolEncoding<C> {
    type Codec = C;
    type Value = bool;
    type Tail = u8;
    type State = BoolDecoderState;

    fn state_at(slab: &Slab, at: usize) -> Self::State {
        let mut d = BoolDecoder::<C>::new(&slab.data);
        d.advance_by(at);
        d.suspend()
    }

    fn state_peek(slab: &Slab, state: &Self::State) -> Option<bool> {
        BoolDecoder::<C>::resume(&slab.data, state).next()
    }

    fn state_advance(slab: &Slab, state: &mut Self::State, n: usize) {
        let mut d = BoolDecoder::<C>::resume(&slab.data, state);
        d.advance_by(n);
        *state = d.suspend();
    }

    fn fill(len: usize, value: bool) -> Slab {
        let mut data = Vec::new();
        let segments = if value {
            data.extend(C::encode_count(0));
            2 // zero-count false + true
        } else {
            1
        };
        let t = data.len();
        data.extend(C::encode_count(len));
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
            let (new_segs, new_tail) = bool_merge_slabs::<C>(&mut a.data, a.tail, a.segments, &b);
            a.len += b.len;
            a.segments = new_segs;
            a.tail = new_tail;
        }
        #[cfg(debug_assertions)]
        validate_slab::<C>(a);
    }

    fn last_run(slab: &Slab) -> Option<Run<bool>> {
        use crate::encoding::RunDecoder;
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
        bool_validate_encoding::<C>(slab)
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
            bool_merge_slabs::<C>(acc, a_tail, a_segments, b)
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
        let overflow_slabs = splice_slab::<C>(slab, index, slab_del, bools, max_segments);
        (overflow_slabs, overflow_del)
    }

    type Decoder<'a> = BoolDecoder<'a, C>;

    fn decoder(slab: &[u8]) -> BoolDecoder<'_, C> {
        BoolDecoder::new(slab)
    }

    type Encoder<'a> = BoolEncoder<C>;

    fn encoder<'a>() -> Self::Encoder<'a> {
        BoolEncoder::new()
    }

    type LoadIter<'a> = BoolLoadIter<'a, C>;

    fn load_iter(data: &[u8], max_segments: usize) -> BoolLoadIter<'_, C> {
        BoolLoadIter::new(data, max_segments)
    }
}

// ── count_segments ───────────────────────────────────────────────────────────

fn bool_count_segments<C: Codec>(slab: &[u8]) -> usize {
    let mut byte_pos = 0;
    let mut segments = 0;

    while byte_pos < slab.len() {
        let (cb, _count) = match C::read_count(&slab[byte_pos..]) {
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
fn bool_validate_encoding<C: Codec>(slab: &[u8]) -> Result<SlabInfo<u8>, PackError> {
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
    let mut len: usize = 0;
    let mut last_cb: u8 = 0;

    while byte_pos < slab.len() {
        let (cb, count) = C::read_count(&slab[byte_pos..]).ok_or(PackError::BadFormat)?;

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
        // untrusted count
        len = len
            .checked_add(count)
            .ok_or(PackError::InvalidValue("length overflows usize".into()))?;
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
fn bool_merge_slabs<C: Codec>(
    a_data: &mut Vec<u8>,
    a_tail: u8,
    a_segments: usize,
    b: &Slab,
) -> (usize, u8) {
    // a's last run — derived from tail (no scan).
    if b.len == 0 {
        return (a_segments, a_tail);
    }

    let a_last_start = a_data.len() - a_tail as usize;
    let (a_last_cb, a_last_count) = C::read_count(&a_data[a_last_start..]).unwrap();
    debug_assert_eq!(a_last_cb, a_tail as usize);
    // Runs alternate false/true: seg 1=false, 2=true, 3=false...
    // Even segments → last is true, odd → last is false.
    let a_last_value = a_segments.is_multiple_of(2);

    // b's first run (always starts with false).
    let b_data: &[u8] = &b.data;
    let (b_first_cb, b_first_count) = C::read_count(b_data).unwrap();
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
            let count = C::encode_count(a_last_count + b_first_count);
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
                let (cb2, count2) = C::read_count(b_rest).unwrap();
                a_data.truncate(a_last_start);
                let count = C::encode_count(a_last_count + count2);
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
/// Streaming decode + validate over saved bool-column bytes.
///
/// The wire format is a sequence of unsigned counts, alternating values
/// starting with `false`; only the very first count may be zero (padding
/// so the data can start with `true`). Yields the non-empty runs.
pub struct BoolLoadIter<'a, C: Codec = Leb128> {
    data: &'a [u8],
    pos: usize,
    run_index: usize,
    _codec: PhantomData<fn() -> C>,
    // ── slab-cutting state, identical to the block loader ──
    slabs: Vec<Slab>,
    slab_start: usize,
    slab_items: usize,
    slab_segs: usize,
    tail: u8,
    target_segments: usize,
}

impl<'a, C: Codec> BoolLoadIter<'a, C> {
    pub fn new(data: &'a [u8], max_segments: usize) -> Self {
        Self {
            data,
            pos: 0,
            run_index: 0,
            _codec: PhantomData,
            slabs: Vec::new(),
            slab_start: 0,
            slab_items: 0,
            slab_segs: 0,
            tail: 0,
            // Target half-full slabs, rounded to even so each slab starts
            // on a false run.
            target_segments: ((max_segments / 2) & !1).max(2),
        }
    }

    fn cut_slab(&mut self) {
        self.slabs.push(Slab {
            data: self.data[self.slab_start..self.pos].to_vec(),
            len: self.slab_items,
            segments: self.slab_segs,
            tail: self.tail,
        });
        self.slab_start = self.pos;
        self.slab_items = 0;
        self.slab_segs = 0;
    }

    /// The next run, or `None` at end of input.
    #[inline]
    pub fn try_next_run(&mut self) -> Result<Option<Run<bool>>, PackError> {
        loop {
            if self.pos >= self.data.len() {
                return Ok(None);
            }
            let (cb, count) = C::read_count(&self.data[self.pos..]).ok_or(PackError::BadFormat)?;
            self.tail = cb as u8;
            // Only the very first run may have count 0 (structural padding).
            if count == 0 && self.run_index > 0 {
                return Err(PackError::BadFormat);
            }
            let next_pos = self.pos + cb;
            // A trailing zero-count run is invalid.
            if next_pos >= self.data.len() && count == 0 {
                return Err(PackError::BadFormat);
            }
            let value = !self.run_index.is_multiple_of(2);
            self.pos = next_pos;
            self.run_index += 1;
            // untrusted count
            self.slab_items = self
                .slab_items
                .checked_add(count)
                .ok_or(PackError::BadFormat)?;
            self.slab_segs += 1;
            // Cut after target_segments — always even, so the next slab
            // starts on a false run and can be memcpy'd as-is.
            if self.slab_segs >= self.target_segments {
                self.cut_slab();
            }
            if count == 0 {
                continue;
            }
            return Ok(Some(Run { count, value }));
        }
    }

    /// Drain and validate whatever the consumer did not pull, flush the
    /// final slab, and return the finished slabs.
    pub fn finalize(mut self) -> Result<Vec<Slab>, PackError> {
        let data = self.data;
        let mut slabs = std::mem::take(&mut self.slabs);
        let mut pos = self.pos;
        let mut run_index = self.run_index;
        let mut slab_start = self.slab_start;
        let mut slab_items = self.slab_items;
        let mut slab_segs = self.slab_segs;
        let mut tail = self.tail;
        let target_segments = self.target_segments;

        while pos < data.len() {
            let (cb, count) = C::read_count(&data[pos..]).ok_or(PackError::BadFormat)?;
            tail = cb as u8;
            if count == 0 && run_index > 0 {
                return Err(PackError::BadFormat);
            }
            let next_pos = pos + cb;
            if next_pos >= data.len() && count == 0 {
                return Err(PackError::BadFormat);
            }
            pos = next_pos;
            run_index += 1;
            slab_items += count;
            slab_segs += 1;
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
}

impl<'a, C: Codec> crate::encoding::LoadIterApi<'a, bool> for BoolLoadIter<'a, C> {
    type Tail = u8;

    fn try_next_run(&mut self) -> Result<Option<Run<bool>>, PackError> {
        BoolLoadIter::try_next_run(self)
    }

    fn slabs_completed(&self) -> usize {
        self.slabs.len()
    }

    fn completed_slab_len(&self, i: usize) -> usize {
        self.slabs[i].len
    }

    fn finalize(self) -> Result<Vec<Slab>, PackError> {
        BoolLoadIter::finalize(self)
    }
}

// ── Cursor ──────────────────────────────────────────────────────────────────

/// The slab's tail at the read cursor: the unread part of the run the
/// cursor stands in, which has to be re-encoded because its count
/// changed, and the bytes after that run, which are carried untouched.
struct BoolTail {
    value: bool,
    partial: usize,
    /// where the carried bytes begin
    start: usize,
    /// items in the carried bytes, not counting `partial`
    items: usize,
    segments: usize,
}

impl BoolTail {
    /// The tail as a slab of its own: a false pad if it would start true,
    /// the partial run, then the carried bytes.
    fn into_slab<C: Codec>(self, data: &[u8], old_tail: u8) -> Option<Slab> {
        let mut buf = Vec::new();
        let mut segments = 0;
        let mut tail = 0u8;
        if self.value && self.partial > 0 {
            buf.extend(C::encode_count(0));
            segments += 1;
            tail = 1;
        }
        if self.partial > 0 {
            let c = C::encode_count(self.partial);
            tail = c.len() as u8;
            buf.extend(c);
            segments += 1;
        }
        if self.segments > 0 {
            tail = old_tail;
        }
        buf.extend_from_slice(&data[self.start..]);
        let len = self.partial + self.items;
        (len > 0).then_some(Slab {
            data: buf,
            len,
            segments: segments + self.segments,
            tail,
        })
    }
}

/// What a finished rebuild hands back: the bytes replacing `range` in the
/// slab, the slab's new metadata, and any slabs it spilled into.
#[derive(Debug, Default)]
struct BoolRebuilt {
    bytes: Vec<u8>,
    range: Range<usize>,
    len: usize,
    segments: usize,
    tail: u8,
    overflow: Vec<Slab>,
}

/// A rebuild in progress within one boolean slab.
///
/// Same contract as the RLE cursor: the head before the edit and the tail
/// after it are carried as raw bytes and never re-encoded, no byte is
/// decoded twice, and N edits landing in one slab cost one partition, one
/// byte splice and one index update.
///
/// What the boolean encoding makes simpler is the state. A run is a
/// count, parity alternates, and any header is resumable — so the encoder
/// state is one `(value, count)` pair, there is no postfix type, no
/// literal-group header to rewrite, and a `peek` is one count read.
pub struct BoolEdit<C: Codec = Leb128> {
    /// the run being written: parity, and items in it so far
    cur_value: bool,
    cur_count: usize,
    buf: Vec<u8>,
    /// segments written into the chunk in hand
    segments: usize,
    /// segments of the slab before `byte_start`, which stay put
    prefix_segments: usize,
    /// items of the slab before `byte_start`, which stay put
    head_len: usize,
    /// where the rebuilt bytes replace the original
    byte_start: usize,
    /// items flushed into `buf` — the pending run is not counted
    emitted: usize,
    /// byte length of the last count written into `buf`
    tail_byte: u8,
    max_segments: usize,
    /// budget for the chunk in hand — halves after the first cut, so a
    /// spill leaves room for later growth rather than filling to the brim
    target: usize,
    overflowed: bool,
    out: BoolRebuilt,
    /// Where reading stopped — the same reader the cursor carries.
    dec: BoolDecoderState,
    /// Complete runs behind the reader; the writer needs it to describe
    /// the tail it carries, and a decoder has no use for it.
    read_segments: usize,
    read: usize,
    _phantom: PhantomData<fn() -> C>,
}

impl<C: Codec> Default for BoolEdit<C> {
    fn default() -> Self {
        BoolEdit {
            cur_value: false,
            cur_count: 0,
            buf: Vec::new(),
            segments: 0,
            prefix_segments: 0,
            head_len: 0,
            byte_start: 0,
            emitted: 0,
            tail_byte: 0,
            max_segments: 0,
            target: 0,
            overflowed: false,
            out: BoolRebuilt::default(),
            dec: BoolDecoderState::start(),
            read_segments: 0,
            read: 0,
            _phantom: PhantomData,
        }
    }
}

/// The scan's view of where `at` lands: the run's header, how many of
/// its items are behind `at`, its parity, and the runs before it. Local
/// to [`BoolEdit::open_at`], which turns it into the reader the rebuild
/// keeps and the region the writer opens.
struct BoolResumeLocal {
    byte: usize,
    off: usize,
    value: bool,
    segments: usize,
}

impl<C: Codec> BoolEdit<C> {
    /// Point the rebuild at item `at`, reading the head once.
    fn open_at(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        let data: &[u8] = &slab.data;
        let mut byte = 0usize;
        let mut item = 0usize;
        let mut value = false;
        let mut segments = 0usize;
        // the run before the one `at` falls in, for the backing-up below
        let mut prev: Option<(usize, usize, usize)> = None; // (byte, count, segments)

        // `(header, consumed, count_bytes)` of the run `at` falls in —
        // the scan's vocabulary, which the writer's half below is written
        // in; the reader is stored in the decoder's.
        let (header, off, cbytes, count) = loop {
            if byte >= data.len() {
                break (byte, 0, 0, 0);
            }
            let (cb, count) = C::read_count(&data[byte..]).expect("a valid bool slab");
            if item + count > at {
                break (byte, at - item, cb, count);
            }
            if count > 0 {
                prev = Some((byte, count, segments));
            }
            item += count;
            byte += cb;
            value = !value;
            segments += 1;
        };
        let resume = BoolResumeLocal {
            byte: header,
            off,
            value,
            segments,
        };

        // The pending run must not be empty, or a push of the opposite
        // parity would have to merge into a run already encoded in the
        // head. When the cursor lands on a run boundary, back up one run
        // and re-encode it instead — the same reason the RLE cursor opens
        // its region at a run header rather than an item boundary.
        let (byte_start, prefix_segments, head_len, cur_value, cur_count) = if resume.off > 0 {
            (
                resume.byte,
                resume.segments,
                at - resume.off,
                resume.value,
                resume.off,
            )
        } else if let Some((pbyte, pcount, psegs)) = prev {
            (pbyte, psegs, at - pcount, !resume.value, pcount)
        } else {
            (0, 0, 0, false, 0)
        };

        self.cur_value = cur_value;
        self.cur_count = cur_count;
        self.buf.clear();
        self.segments = 0;
        self.prefix_segments = prefix_segments;
        self.head_len = head_len;
        self.byte_start = byte_start;
        self.emitted = 0;
        self.tail_byte = 0;
        self.max_segments = max_segments;
        self.target = max_segments;
        self.overflowed = false;
        self.out = BoolRebuilt::default();
        self.dec = BoolDecoderState {
            byte_pos: resume.byte + cbytes,
            remaining: count - resume.off,
            value: resume.value,
        };
        self.read_segments = resume.segments;
        self.read = at;
    }

    /// Segments standing before the chunk in hand: the untouched head for
    /// the first chunk, nothing for a spilled one.
    fn standing(&self) -> usize {
        if self.overflowed {
            0
        } else {
            self.prefix_segments
        }
    }

    /// Write the pending run out. Nothing is written for an empty one —
    /// a zero count is only ever legal as the pad below.
    fn flush_run(&mut self) {
        if self.cur_count > 0 {
            let c = C::encode_count(self.cur_count);
            self.tail_byte = c.len() as u8;
            self.buf.extend(c);
            self.emitted += self.cur_count;
            self.segments += 1;
        }
    }

    /// Append `n` items of `value`.
    fn push(&mut self, value: bool, n: usize) {
        if n == 0 {
            return;
        }
        if value == self.cur_value {
            self.cur_count += n;
            return;
        }
        if self.cur_count == 0 && self.segments == 0 && !self.overflowed {
            // The rebuild starts the slab and its first run is true, so
            // the slab needs a zero-count false pad to start on. (An
            // empty pending run only ever happens here: everywhere else
            // it is opened with a non-zero count.)
            debug_assert!(!self.cur_value && self.byte_start == 0);
            self.buf.extend(C::encode_count(0));
            self.tail_byte = 1;
            self.segments += 1;
        } else {
            self.flush_run();
        }
        self.cur_value = value;
        self.cur_count = n;
        if self.standing() + self.segments >= self.target {
            self.cut();
        }
    }

    /// Close the chunk in hand and start a new slab.
    fn cut(&mut self) {
        if !self.overflowed {
            self.overflowed = true;
            self.target = (self.max_segments / 2).max(1);
            self.out.bytes = std::mem::take(&mut self.buf);
            self.out.len = self.head_len + self.emitted;
            self.out.segments = self.prefix_segments + self.segments;
            self.out.tail = self.tail_byte;
        } else {
            self.out.overflow.push(Slab {
                data: std::mem::take(&mut self.buf),
                len: self.emitted,
                segments: self.segments,
                tail: self.tail_byte,
            });
        }
        self.segments = 0;
        self.emitted = 0;
        self.tail_byte = 0;
        // a fresh slab starts on a false run
        if self.cur_value {
            self.buf.extend(C::encode_count(0));
            self.tail_byte = 1;
            self.segments = 1;
        }
    }

    /// Move the read cursor `n` items forward, keeping them when `emit`.
    /// Reading resumes where it stopped, so no run is decoded twice
    /// except the one the cursor is standing in.
    fn walk<F>(&mut self, slab: &Slab, n: usize, emit: bool, f: &mut F) -> usize
    where
        F: FnMut(bool, usize),
    {
        let mut dec = BoolDecoder::<C>::resume(&slab.data, &self.dec);
        let mut want = n;
        while want > 0 {
            // bounded by what is still wanted, so the decoder keeps the
            // rest of a run it only partly gave up
            let Some(run) = dec.next_run_max(want) else {
                break;
            };
            f(run.value, run.count);
            if emit {
                self.push(run.value, run.count);
            }
            self.read += run.count;
            want -= run.count;
            if dec.remaining == 0 {
                self.read_segments += 1;
            }
        }
        self.dec = dec.suspend();
        want
    }

    /// Write the rebuild back, carrying the slab's remaining bytes
    /// through untouched.
    /// The tail at the read cursor. Reads one count — the run the cursor
    /// stands in; everything past it is bytes.
    fn tail(&self, slab: &Slab) -> BoolTail {
        let data_len = slab.data.len();
        // the run in hand: what the reader left of the one it is inside,
        // or — standing exactly on a boundary — the whole of the next
        let (value, partial, start) = if self.dec.remaining > 0 {
            (self.dec.value, self.dec.remaining, self.dec.byte_pos)
        } else if self.dec.byte_pos < data_len {
            let (cb, count) =
                C::read_count(&slab.data[self.dec.byte_pos..]).expect("a valid bool slab");
            (!self.dec.value, count, self.dec.byte_pos + cb)
        } else {
            return BoolTail {
                value: self.dec.value,
                partial: 0,
                start: data_len,
                items: 0,
                segments: 0,
            };
        };
        BoolTail {
            value,
            partial,
            start,
            items: slab.len - self.read - partial,
            // the run in hand is carried by the encoder, not by the bytes
            segments: slab.segments - self.read_segments - 1,
        }
    }

    /// Write the rebuild back, carrying the slab's remaining bytes
    /// through untouched.
    fn write_back(&mut self, slab: &mut Slab) -> Vec<Slab> {
        let t = self.tail(slab);
        // the partial run costs a segment unless it merges into the
        // pending one
        let joins = t.partial > 0 && t.value == self.cur_value;
        let need =
            usize::from(self.cur_count > 0) + t.segments + usize::from(t.partial > 0 && !joins);
        if self.standing() + self.segments + need > self.max_segments {
            // the tail does not fit: it becomes a slab of its own
            self.flush_run();
            self.cur_count = 0;
            self.seal(slab, slab.data.len(), 0, 0);
            let spill = t.into_slab::<C>(&slab.data, slab.tail);
            self.out.overflow.extend(spill);
            return self.apply(slab);
        }
        // it fits: the partial run goes through the encoder like any
        // other, which merges it with the pending run when the parities
        // agree, and pads or cuts when they do not
        self.push(t.value, t.partial);
        self.flush_run();
        self.cur_count = 0;
        self.seal(slab, t.start, t.items, t.segments);
        self.apply(slab)
    }

    /// Finish the chunk in hand: the edited slab's replacement when
    /// nothing has spilled, another new slab when something has — and in
    /// that case the carried tail moves onto it, so everything from
    /// `byte_start` to the end of the original is replaced.
    fn seal(
        &mut self,
        slab: &Slab,
        tail_start: usize,
        raw_tail_items: usize,
        tail_segments: usize,
    ) {
        let data_len = slab.data.len();
        // the slab's last count is unchanged when any tail bytes remain
        let tail = if tail_start < data_len {
            slab.tail
        } else {
            self.tail_byte
        };
        if !self.overflowed {
            self.out.bytes = std::mem::take(&mut self.buf);
            self.out.range = self.byte_start..tail_start;
            self.out.len = self.head_len + self.emitted + raw_tail_items;
            self.out.segments = self.prefix_segments + self.segments + tail_segments;
            self.out.tail = tail;
        } else {
            let mut data = std::mem::take(&mut self.buf);
            data.extend_from_slice(&slab.data[tail_start..]);
            self.out.overflow.push(Slab {
                data,
                len: self.emitted + raw_tail_items,
                segments: self.segments + tail_segments,
                tail,
            });
            self.out.range = self.byte_start..data_len;
        }
    }

    /// Splice the rebuilt bytes in place of the region they replace.
    fn apply(&mut self, slab: &mut Slab) -> Vec<Slab> {
        let out = std::mem::take(&mut self.out);
        let range = out.range;
        crate::column::splice_bytes(&mut slab.data, range.start, range.len(), &out.bytes);
        slab.len = out.len;
        slab.segments = out.segments;
        slab.tail = out.tail;
        // hand the buffer back for the next slab this cursor edits
        self.buf = out.bytes;
        self.buf.clear();
        #[cfg(debug_assertions)]
        {
            validate_slab::<C>(slab);
            for s in &out.overflow {
                validate_slab::<C>(s);
            }
        }
        out.overflow
    }
}

impl<C: Codec> crate::edit::SlabEdit for BoolEncoding<C> {
    type Edit = BoolEdit<C>;
}

impl<C: Codec> crate::edit::EditSlab for BoolEdit<C> {
    type Tail = u8;
    type Value = bool;
    type State = BoolDecoderState;

    fn reset(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        self.open_at(slab, at, max_segments)
    }

    fn pass<F>(&mut self, slab: &Slab, n: usize, _runs: usize, f: &mut F) -> usize
    where
        F: FnMut(bool, usize),
    {
        self.walk(slab, n, true, f)
    }

    fn insert<V: AsColumnRef<bool>>(&mut self, value: V, n: usize) {
        self.push(value.as_column_ref(), n)
    }

    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(bool, usize),
    {
        debug_assert!(self.read + n <= slab.len, "delete past the end of the slab");
        self.walk(slab, n, false, f);
    }

    fn read_state(&self) -> &BoolDecoderState {
        &self.dec
    }

    fn close(&mut self, slab: &mut Slab) -> Vec<Slab> {
        self.write_back(slab)
    }
}

#[cfg(test)]
mod suspend_tests {
    use super::*;

    fn check(values: Vec<bool>) {
        let bytes = crate::Column::<bool>::from_values(values.clone()).save();
        let want: Vec<bool> = crate::decoder::<bool>(&bytes).collect();
        for cut in 0..=want.len() {
            let mut dec = BoolDecoder::<Leb128>::new(&bytes);
            for _ in 0..cut {
                dec.next().expect("value before the cut");
            }
            let state = dec.suspend();
            let mut resumed = BoolDecoder::<Leb128>::resume(&bytes, &state);
            let got: Vec<bool> = std::iter::from_fn(|| resumed.next()).collect();
            assert_eq!(got, want[cut..], "resume at {cut}");
        }
    }

    #[test]
    fn resume_runs() {
        check((0..200).map(|i| (i / 7) % 2 == 0).collect());
    }

    #[test]
    fn resume_alternating() {
        check((0..200).map(|i| i % 2 == 0).collect());
    }

    #[test]
    fn resume_all_true() {
        check(vec![true; 64]);
    }

    /// A fresh decoder and one resumed from the start state must agree —
    /// the first run is `false`, which the `value` field has to encode.
    #[test]
    fn start_state_matches_fresh() {
        let bytes = crate::Column::<bool>::from_values(vec![true; 8]).save();
        let fresh: Vec<bool> = BoolDecoder::<Leb128>::new(&bytes).collect();
        let resumed: Vec<bool> =
            BoolDecoder::<Leb128>::resume(&bytes, &BoolDecoderState::start()).collect();
        assert_eq!(fresh, resumed);
    }
}

#[cfg(test)]
mod tests {
    use super::Slab;
    use super::{bool_count_segments, find_partition, BoolPartition};
    use crate::codec::{Codec as _, Leb128};

    use crate::Column;

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
            let (cb, count) = Leb128::read_count(&data[byte_pos..]).unwrap();
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
            out.extend(Leb128::encode_count(c));
        }
        out
    }

    fn make_slab(data: Vec<u8>) -> Slab {
        let _segments = bool_count_segments::<Leb128>(&data);
        let mut tail = 0;
        let mut len = 0;
        let mut pos = 0;
        let mut segments = 0;
        while pos < data.len() {
            let (cb, count) = Leb128::read_count(&data[pos..]).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 150, 160).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 200, 200).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 0, 10).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 290, 300).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 0, 300).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 50, 250).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 150, 150).unwrap();
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
        let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), 100, 100).unwrap();
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
            let (cb, count) = Leb128::read_count(&data[byte_pos..]).unwrap();
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
            let (p, s) = find_partition::<Leb128>(&make_slab(data.clone()), idx, idx).unwrap();
            let recon = reconstruct(&data, &p, &s);
            assert_eq!(orig, recon, "identity failed at idx={idx}");
        }
    }
}
