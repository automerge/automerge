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

// ── Splice ──────────────────────────────────────────────────────────────────

/// Splice a bool slab: delete `del` items at `index`, insert `values`.
/// Returns the slabs it spilled into (to go after this one).
///
/// The verified edit rebuild ([`BEdit`](crate::bool_edit::BEdit)) does the
/// work: open at `index`, drop `del` items, push the values, close. The
/// slab's head and tail are carried as bytes.
pub(crate) fn splice_slab<C: Codec>(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = (bool, usize)>,
    max_segments: usize,
) -> Vec<Slab> {
    let end_index = index + del;
    assert!(end_index <= slab.len, "del extends beyond slab");
    let mut e =
        crate::bool_edit::BEdit::open::<Counts<C>>(&slab.data, index, max_segments, Vec::new());
    e.walk::<Counts<C>, _>(&slab.data, del, false, &mut |_: bool, _: usize| {});
    for (value, count) in values {
        e.insert::<Counts<C>>(value, count);
    }
    let r = e.close::<Counts<C>>(&slab.data, slab.len, slab.segments, slab.tail);
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
    r.overflow
}

// ── BoolDecoder ──────────────────────────────────────────────────────────────

pub use crate::bool_wire::BoolDecoder;

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
            let (new_segs, new_tail) =
                bool_merge_slabs::<C>(&mut a.data, 0, a.tail, a.segments, &b);
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
    type ReadModel = crate::bool_wire::BoolReads<C>;

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
    crate::bool_wire::merge::<Counts<C>>(
        a_data, start, a_tail, a_segments, &b.data, b.segments, b.tail,
    )
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

pub use crate::bool_edit::BoolEdit;

impl<C> Default for BoolEdit<C> {
    fn default() -> Self {
        BoolEdit {
            e: crate::bool_edit::BEdit::idle(),
            _phantom: PhantomData,
        }
    }
}

impl<C: Codec> crate::edit::SlabEdit for BoolEncoding<C> {
    type Edit = BoolEdit<C>;
    type Model = crate::bool_edit::BoolModel<C>;
}

// Forwards to the verified bool slab model; the column cursor calls the
// model directly, and reaches `pass`, `insert` and `delete` through here.
impl<C: Codec> crate::edit::EditSlab for BoolEdit<C> {
    type Tail = u8;
    type Value = bool;
    type State = BoolDecoderState;

    fn reset(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        <crate::bool_edit::BoolModel<C> as crate::cursor::Slabs>::reset(
            self,
            slab,
            at,
            max_segments,
        )
    }

    fn pass<F>(&mut self, slab: &Slab, n: usize, _runs: usize, f: &mut F) -> usize
    where
        F: FnMut(bool, usize),
    {
        crate::bool_edit::pass::<C, F>(self, slab, n, f)
    }

    fn insert<V: AsColumnRef<bool>>(&mut self, value: V, n: usize) {
        self.e.insert::<Counts<C>>(value.as_column_ref(), n)
    }

    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(bool, usize),
    {
        debug_assert!(
            self.e.read + n <= slab.len,
            "delete past the end of the slab"
        );
        crate::bool_edit::discard::<C, F>(self, slab, n, f)
    }

    fn read_state(&self) -> &BoolDecoderState {
        &self.e.dec
    }

    fn close(&mut self, slab: &mut Slab) -> Vec<Slab> {
        <crate::bool_edit::BoolModel<C> as crate::cursor::Slabs>::close(self, slab)
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
}
