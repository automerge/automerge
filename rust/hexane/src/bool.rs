use crate::column::splice_bytes;
use crate::encoder::BoolEncoder;
use crate::PackError;

type Slab = crate::column::Slab<u8>;
use crate::encoding::{ColumnEncoding, RunDecoder, SlabInfo};

use crate::{AsColumnRef, Codec, Leb128, Run};
use std::marker::PhantomData;

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
///
/// The decoding is verified with Verus ([`BDec`](crate::bool_wire::BDec)):
/// on a valid slab it yields the slab's values, a run at a time or one
/// at a time.
pub struct BoolDecoder<'a, C: Codec = Leb128> {
    d: crate::bool_wire::BDec<'a>,
    _codec: PhantomData<fn() -> C>,
}

impl<C: Codec> Clone for BoolDecoder<'_, C> {
    fn clone(&self) -> Self {
        Self {
            d: self.d,
            _codec: PhantomData,
        }
    }
}

/// A [`BoolDecoder`]'s position, detached from the slab it was reading.
///
/// Public only because [`ColumnEncoding::State`](crate::ColumnEncoding::State)
/// names it; nothing outside the crate can do anything with one.
#[doc(hidden)]
pub use crate::bool_wire::BoolDecoderState;

// hand-written rather than derived: a slab-start reader carries `value:
// true`, so that the first run's flip yields the `false` the wire format
// begins with. A derived `Default` would give `false` and read every run
// at the wrong parity.
impl Default for BoolDecoderState {
    fn default() -> Self {
        BoolDecoderState::start()
    }
}

impl<'a, C: Codec> BoolDecoder<'a, C> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        BoolDecoder {
            d: crate::bool_wire::BDec::new::<Counts<C>>(data),
            _codec: PhantomData,
        }
    }

    /// Advance past `n` items in O(runs skipped). Stops at the end of
    /// the column.
    // inherent, so a call on a concrete decoder is never ambiguous with
    // the unstable `Iterator::advance_by`
    pub fn advance_by(&mut self, n: usize) {
        if n > 0 {
            self.d.nth::<Counts<C>>(n - 1);
        }
    }

    /// Capture the position so it can be restored against the same bytes
    /// later, without holding a borrow of them.
    pub(crate) fn suspend(&self) -> BoolDecoderState {
        self.d.suspend()
    }

    /// Restore a suspended position against `data`, which must be the
    /// bytes it was suspended from.
    pub(crate) fn resume(data: &'a [u8], state: &BoolDecoderState) -> Self {
        BoolDecoder {
            d: crate::bool_wire::BDec::resume::<Counts<C>>(data, state),
            _codec: PhantomData,
        }
    }
}

impl<'a, C: Codec> Iterator for BoolDecoder<'a, C> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<bool> {
        self.d.next::<Counts<C>>()
    }

    /// O(runs_skipped) — each run is skipped in O(1) by decrementing the count.
    fn nth(&mut self, n: usize) -> Option<bool> {
        self.d.nth::<Counts<C>>(n)
    }
}

impl<'a, C: Codec> RunDecoder for BoolDecoder<'a, C> {
    fn next_run(&mut self) -> Option<Run<bool>> {
        self.d.next_run_max::<Counts<C>>(usize::MAX)
    }

    fn next_run_max(&mut self, max: usize) -> Option<Run<bool>> {
        self.d.next_run_max::<Counts<C>>(max)
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
            let (new_segs, new_tail) = bool_merge_slabs::<C>(&mut a.data, 0, a.tail, a.segments, &b);
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
        start: usize,
        a_tail: u8,
        a_segments: usize,
        b: &Slab,
        _buf: &mut Vec<u8>,
    ) -> (usize, u8) {
        if b.len == 0 || b.data.is_empty() {
            (a_segments, a_tail)
        } else {
            bool_merge_slabs::<C>(acc, start, a_tail, a_segments, b)
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
    // verified with Verus: Ok exactly for the canonical encodings
    let info = crate::bool_wire::validate::<crate::bool_wire::Counts<C>>(slab)?;
    Ok(SlabInfo {
        segments: info.segments,
        len: info.len,
        tail: info.tail,
    })
}

// ── merge_slab_bytes ─────────────────────────────────────────────────────────

/// Merge bool slab `b` onto the end of the slab in `a_data` from `start`
/// (the bytes before are others'): only the boundary runs are decoded,
/// the rest is copied. Verified with Verus
/// ([`merge`](crate::bool_wire::merge)): the result is the canonical
/// encoding of one slab's values then the other's, with the run count and
/// tail record returned.
fn bool_merge_slabs<C: Codec>(
    a_data: &mut Vec<u8>,
    start: usize,
    a_tail: u8,
    a_segments: usize,
    b: &Slab,
) -> (usize, u8) {
    crate::bool_wire::merge::<Counts<C>>(a_data, start, a_tail, a_segments, &b.data, b.segments, b.tail)
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
    /// the reading, checking and cutting, verified with Verus: a finished
    /// load's slabs are canonical, with their recorded metadata, and
    /// together hold the input's values
    load: crate::bool_wire::BoolLoad<'a>,
    _codec: PhantomData<fn() -> C>,
}

type Counts<C> = crate::bool_wire::Counts<C>;

impl<'a, C: Codec> BoolLoadIter<'a, C> {
    pub fn new(data: &'a [u8], max_segments: usize) -> Self {
        Self {
            // half-full slabs, an even number of runs (at least 2) so each
            // slab starts on a false run
            load: crate::bool_wire::BoolLoad::new::<Counts<C>>(data, max_segments),
            _codec: PhantomData,
        }
    }

    /// The next run, or `None` at end of input.
    #[inline]
    pub fn try_next_run(&mut self) -> Result<Option<Run<bool>>, PackError> {
        self.load.try_next_run::<Counts<C>>()
    }

    /// Drain and validate whatever the consumer did not pull, flush the
    /// final slab, and return the finished slabs.
    pub fn finalize(mut self) -> Result<Vec<Slab>, PackError> {
        self.load.finalize::<Counts<C>>()
    }
}

impl<'a, C: Codec> crate::encoding::LoadIterApi<'a, bool> for BoolLoadIter<'a, C> {
    type Tail = u8;

    fn try_next_run(&mut self) -> Result<Option<Run<bool>>, PackError> {
        BoolLoadIter::try_next_run(self)
    }

    fn slabs_completed(&self) -> usize {
        self.load.slabs_completed()
    }

    fn completed_slab_len(&self, i: usize) -> usize {
        self.load.completed_slab_len(i)
    }

    fn finalize(self) -> Result<Vec<Slab>, PackError> {
        BoolLoadIter::finalize(self)
    }
}

// ── Cursor ──────────────────────────────────────────────────────────────────

/// A rebuild in progress within one boolean slab.
///
/// Same contract as the RLE cursor: the head before the edit and the tail
/// after it are carried as raw bytes and never re-encoded, no byte is
/// decoded twice, and N edits landing in one slab cost one partition, one
/// byte splice and one index update.
///
/// The rebuild is verified with Verus ([`BEdit`](crate::bool_edit::BEdit)):
/// opening, walking, inserting and closing leave the slab, and any it
/// spills into, the canonical encoding of the values written then the
/// slab's from where reading stopped, with their metadata.
pub struct BoolEdit<C: Codec = Leb128> {
    e: crate::bool_edit::BEdit,
    _phantom: PhantomData<fn() -> C>,
}

impl<C: Codec> Default for BoolEdit<C> {
    fn default() -> Self {
        BoolEdit {
            e: crate::bool_edit::BEdit::idle(),
            _phantom: PhantomData,
        }
    }
}

impl<C: Codec> BoolEdit<C> {
    /// Write the rebuild back into `slab`, returning the slabs it spilled
    /// into.
    fn write_back(&mut self, slab: &mut Slab) -> Vec<Slab> {
        let r = self
            .e
            .close::<Counts<C>>(&slab.data, slab.len, slab.segments, slab.tail);
        r.apply_bytes(&mut slab.data);
        slab.len = r.len;
        slab.segments = r.segments;
        slab.tail = r.tail;
        #[cfg(debug_assertions)]
        {
            validate_slab::<C>(slab);
            for s in &r.overflow {
                validate_slab::<C>(s);
            }
        }
        // hand the buffer back for the next slab this cursor edits
        self.e.w.buf = r.bytes;
        r.overflow
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
        let buf = std::mem::take(&mut self.e.w.buf);
        self.e = crate::bool_edit::BEdit::open::<Counts<C>>(&slab.data, at, max_segments, buf);
    }

    fn pass<F>(&mut self, slab: &Slab, n: usize, _runs: usize, f: &mut F) -> usize
    where
        F: FnMut(bool, usize),
    {
        self.e.walk::<Counts<C>, F>(&slab.data, n, true, f)
    }

    fn insert<V: AsColumnRef<bool>>(&mut self, value: V, n: usize) {
        self.e.insert::<Counts<C>>(value.as_column_ref(), n)
    }

    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(bool, usize),
    {
        debug_assert!(self.e.read + n <= slab.len, "delete past the end of the slab");
        self.e.walk::<Counts<C>, F>(&slab.data, n, false, f);
    }

    fn read_state(&self) -> &BoolDecoderState {
        &self.e.dec
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
