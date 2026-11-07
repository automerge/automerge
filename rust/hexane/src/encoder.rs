//! Streaming encoder for building column data from a sequence of values.
//!
//! Unlike [`Column`] which supports random-access splice,
//! `Encoder<T>` is append-only and produces a single contiguous byte buffer.
//! This is used for building change data where values arrive in order.
//!
//! ```no_run
//! # use hexane::encoder::RleEncoder;
//! let mut enc = RleEncoder::<u64>::default();
//! enc.append(1);
//! enc.append(1);
//! enc.append(2);
//! let bytes = enc.save();
//! ```

use crate::btree::SlabAggregate;
use crate::column::WeightFn;
use crate::column::{Column, Slab, DEFAULT_MAX_SEG};
use crate::encoding::EncoderApi;
use crate::index::ColumnIndex;
use crate::rle::enc::{REncoder, RStream};
use crate::rle::state::{RleCow, WOps};
use crate::rle::RleEncoding;
use crate::rle::RleTail;
use crate::RleValue;
use crate::{Codec, Leb128};

use std::ops::Range;

// ── RLE Encoder ─────────────────────────────────────────────────────────────

/// State half of [`RleEncoder`] — owns the run/flush bookkeeping but **not**
/// the output buffer.  Every mutating method takes a `&mut Vec<u8>` so the
/// caller decides where bytes land.
///
/// Use [`RleEncoder`] when you want a self-contained encoder that owns its
/// buffer.  Use this directly (via the `encode_to_unless` static path on
/// [`crate::encoding::EncoderApi`]) when you want to write through to a
/// caller-owned buffer with no per-call heap allocation.
///
/// Verified with Verus (`RStream`): appending always to the same buffer,
/// with the bytes this state wrote left as they are in between, then
/// finishing, leaves the buffer's earlier bytes as they were and follows
/// them with the canonical encoding of the values appended. Nothing
/// checks that the buffer is the same: given another, the bytes are
/// unspecified, and it may panic.
pub struct RleEncoderState<'a, T: RleValue, C: Codec = Leb128> {
    s: RStream<'a, WOps<T, T, C>>,
}

impl<T: RleValue, C: Codec> Default for RleEncoderState<'_, T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T: RleValue, C: Codec> RleEncoderState<'a, T, C> {
    pub fn new() -> Self {
        Self { s: RStream::new() }
    }

    pub fn len(&self) -> usize {
        self.s.len
    }

    pub fn is_empty(&self) -> bool {
        self.s.len == 0
    }

    pub fn append(&mut self, buf: &mut Vec<u8>, value: T::Get<'a>) {
        self.s.append(buf, RleCow::Ref(value), 1);
    }

    pub fn append_owned(&mut self, buf: &mut Vec<u8>, value: T) {
        self.s.append(buf, RleCow::Owned(value), 1);
    }

    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: T::Get<'a>, n: usize) {
        self.s.append(buf, RleCow::Ref(value), n);
    }

    pub fn append_n_owned(&mut self, buf: &mut Vec<u8>, value: T, n: usize) {
        self.s.append(buf, RleCow::Owned(value), n);
    }

    pub fn extend(&mut self, buf: &mut Vec<u8>, iter: impl IntoIterator<Item = T::Get<'a>>) {
        for value in iter {
            self.append(buf, value);
        }
    }

    /// Flush any pending run into `buf`.
    pub fn finish(&mut self, buf: &mut Vec<u8>) {
        self.s.finish(buf);
    }

    /// True if the entire encoded sequence so far represents a single run
    /// of `value` (or is empty).  Both clauses must hold:
    ///   1. no run has been flushed to `buf` yet (only the in-progress one
    ///      is pending);
    ///   2. the in-progress state is empty or a single run of `value`.
    ///      Call before `finish`; once you flush, the in-progress run is
    ///      flushed too and this check stops being useful.
    ///
    /// Verified with Verus: true only if every value appended is `value`.
    pub fn is_single_run_of(&self, value: T::Get<'a>) -> bool {
        self.s.single_run_of(&RleCow::Ref(value))
    }
}

/// Streaming encoder for RLE-encoded types (`u64`, `i64`, `String`, `Option<u64>`, etc.).
///
/// Accepts values via [`append`](RleEncoder::append) and
/// [`append_n`](RleEncoder::append_n), then produces the encoded bytes
/// with [`save`](RleEncoder::save) or [`save_to`](RleEncoder::save_to).
/// Both output methods consume the encoder.
///
/// When a slab segment budget is set via
/// [`EncoderApi::max_segments`]
/// **before appending**, the encoder rolls completed output into [`Slab`]s
/// as it goes, and [`into_column`](crate::encoding::EncoderApi::into_column)
/// hands them to the column directly — no decode/validate pass.
///
/// The lifetime `'a` ties borrowed values (e.g. `&'a str` for String columns)
/// to the encoder. For `Copy` types like `u64`, `'a` is typically `'static`.
///
/// The encoding and the slab rollover are verified with Verus
/// (`REncoder`): every slab is a well-formed slab of its share of the
/// values, with its metadata, and together they hold the values
/// appended; without rollover the saved bytes are their canonical
/// encoding.
pub struct RleEncoder<'a, T: RleValue, C: Codec = Leb128> {
    e: REncoder<'a, WOps<T, T, C>>,
}

impl<T: RleValue, C: Codec> Default for RleEncoder<'_, T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: RleValue, C: Codec> std::fmt::Debug for RleEncoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RleEncoder")
            .field("len", &self.e.len)
            .field("buf_len", &self.e.data.len())
            .finish()
    }
}

impl<'a, T: RleValue, C: Codec> RleEncoder<'a, T, C> {
    /// Create a new empty encoder.
    pub fn new() -> Self {
        Self { e: REncoder::new() }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.e.len
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.e.len == 0
    }

    /// Flush pending state and return all slabs (rollover slabs + final).
    fn take_slabs(&mut self) -> Vec<Slab<RleTail>> {
        self.e.take_slabs()
    }

    /// Append a single value.
    pub fn append(&mut self, value: T::Get<'a>) {
        self.e.append(RleCow::Ref(value), 1);
    }

    pub fn append_owned(&mut self, value: T) {
        self.e.append(RleCow::Owned(value), 1);
    }

    /// Append `n` copies of `value`.
    pub fn append_n(&mut self, value: T::Get<'a>, n: usize) {
        self.e.append(RleCow::Ref(value), n);
    }

    /// Append `n` copies of `value`.
    pub fn append_n_owned(&mut self, value: T, n: usize) {
        self.e.append(RleCow::Owned(value), n);
    }

    /// Append all values from an iterator.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = T::Get<'a>>) {
        for value in iter {
            self.append(value);
        }
    }

    /// Flush and return the encoded bytes. Consumes the encoder.
    pub fn save(mut self) -> Vec<u8> {
        if !self.e.slabs.is_empty() {
            let mut out = Vec::new();
            self.save_to(&mut out);
            return out;
        }
        self.e.take_bytes()
    }

    /// Flush and append the encoded bytes to `out`. Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(mut self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        if self.e.slabs.is_empty() {
            out.extend_from_slice(&self.e.take_bytes());
            return start..out.len();
        }
        // Slab rollover happened — fold the slabs back into canonical
        // bytes, merging boundary runs (as `Column::save_to` does; verified
        // with Verus).
        let total = self.e.len;
        crate::rle::edit::save_slabs::<WOps<T, T, C>>(&self.take_slabs(), total, out);
        start..out.len()
    }

    /// Like [`save_to`](Self::save_to) but returns an empty range if the
    /// encoded data is empty or consists entirely of a single run of `value`.
    ///
    /// Note: once slab rollover has cut (only possible with `max_segments`
    /// set and multi-run data — or a degenerate budget ≤ 3), elision is
    /// skipped; uniform data never cuts under a sane budget, so this only
    /// forgoes elision where it wouldn't apply anyway.
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T::Get<'a>) -> Range<usize> {
        // verified with Verus: true only if every value is `value`
        if self.e.single_run_of(&RleCow::Ref(value)) {
            return out.len()..out.len();
        }
        self.save_to(out)
    }

    /// Like [`save_to`](Self::save_to) but applies `f` to every value before
    /// re-encoding.  The encoder's accumulated runs are walked directly with
    /// [`RleDecoder`](crate::rle::RleDecoder), so this avoids the round-trip
    /// through [`Column`] that
    /// [`Column::remap`] would require.  Always writes
    /// — no elision.
    pub fn save_to_and_remap<F>(self, out: &mut Vec<u8>, f: F) -> Range<usize>
    where
        F: Fn(T) -> T,
    {
        let mut new_enc = RleEncoder::<'a, T, C>::new();
        self.walk_runs(&mut new_enc, f);
        new_enc.save_to(out)
    }

    /// Like [`save_to_unless`](Self::save_to_unless) but applies `f` to every
    /// value before re-encoding.  See [`save_to_and_remap`](Self::save_to_and_remap)
    /// for the non-eliding variant.
    pub fn save_to_unless_and_remap<F>(
        self,
        out: &mut Vec<u8>,
        unless: T::Get<'a>,
        f: F,
    ) -> Range<usize>
    where
        F: Fn(T) -> T,
    {
        let mut new_enc = RleEncoder::<'a, T, C>::new();
        self.walk_runs(&mut new_enc, f);
        new_enc.save_to_unless(out, unless)
    }

    /// Flush `self` and walk every run through `f`, re-emitting into `dst`.
    /// Shared implementation for the `*_and_remap` methods.
    ///
    /// Walks any rollover slabs first, then the pending buffer; `dst`'s
    /// state machine re-merges runs that were split at slab cuts.
    fn walk_runs<F>(mut self, dst: &mut RleEncoder<'a, T, C>, f: F)
    where
        F: Fn(T) -> T,
    {
        use crate::encoding::RunDecoder;
        use crate::rle::RleDecoder;
        let data = self.e.take_bytes();
        for slab in self.e.slabs.iter().map(|s| &s.data[..]).chain([&data[..]]) {
            let mut dec = RleDecoder::<'_, T, C>::new(slab);
            while let Some(run) = dec.next_run() {
                let value = T::to_owned(run.value);
                dst.append_n_owned(f(value), run.count);
            }
        }
    }
}

impl<'a, T, C> EncoderApi<'a, T, C> for RleEncoder<'a, T, C>
where
    C: Codec,
    T: RleValue + crate::ColumnValueRef<Encoding<C> = RleEncoding<T, C>>,
{
    type Tail = RleTail;
    fn append(&mut self, value: T::Get<'a>) {
        self.append(value);
    }
    fn append_owned(&mut self, value: T) {
        self.append_owned(value);
    }
    fn append_n(&mut self, value: T::Get<'a>, n: usize) {
        self.append_n(value, n);
    }
    fn append_n_owned(&mut self, value: T, n: usize) {
        self.append_n_owned(value, n);
    }
    fn extend(&mut self, iter: impl IntoIterator<Item = T::Get<'a>>) {
        self.extend(iter);
    }
    fn len(&self) -> usize {
        self.e.len
    }
    fn save(self) -> Vec<u8> {
        self.save()
    }
    fn save_to(self, out: &mut Vec<u8>) -> Range<usize> {
        self.save_to(out)
    }
    fn save_to_unless(self, out: &mut Vec<u8>, value: T::Get<'a>) -> Range<usize> {
        self.save_to_unless(out, value)
    }

    /// Enable slab rollover: completed output is cut into [`Slab`]s at
    /// `max / 2` segments as values arrive, so
    /// [`into_column`](crate::encoding::EncoderApi::into_column) can build
    /// the column directly.  Must be called before appending.
    fn max_segments(&mut self, max: usize) {
        assert!(max >= 2, "max_segments must be at least 2");
        debug_assert!(self.e.len == 0, "max_segments must be set before appending");
        self.e.max_segments = Some(max);
    }

    /// Direct path: hand the rollover slabs to the column, skipping the
    /// decode/validate pass that the save-then-load default performs.
    /// Falls back to that default when `max_segments` was never set
    /// (no cut points were tracked, so the slab split is unknown).
    fn into_column<WF, Idx>(mut self) -> Column<T, C, WF, Idx>
    where
        WF: WeightFn<T, C>,
        WF::Weight: SlabAggregate,
        Idx: ColumnIndex<WF::Weight>,
    {
        match self.e.max_segments {
            Some(max) => Column::from_slabs(self.take_slabs(), max),
            None => Column::load(&self.save()).unwrap(),
        }
    }

    fn into_slab(mut self) -> Slab<Self::Tail> {
        // after a cut, the slab would hold only the values since
        assert!(
            self.e.slabs.is_empty(),
            "into_slab is single-slab only — don't combine with max_segments"
        );
        // verified with Verus: a well-formed slab of the values, with its
        // metadata
        self.e.take_slab()
    }

    /// Fast-path: skip the wrapper `Vec<u8>` allocation **and** the full
    /// `FlushState` accounting that [`RleEncoderState`] does.
    /// We only need raw byte output, so just drive RleState directly.
    fn encode_to(buf: &mut Vec<u8>, iter: impl IntoIterator<Item = T::Get<'a>>) -> Range<usize> {
        let start = buf.len();
        // verified with Verus: the bytes after `start` are the canonical
        // encoding of the values
        let mut s = RStream::<'a, WOps<T, T, C>>::new();
        for v in iter {
            s.append(buf, RleCow::Ref(v), 1);
        }
        s.finish(buf);
        start..buf.len()
    }

    /// Fast-path: drive the writer directly into `buf`, tracking only
    /// what the elision check needs.  When the entire
    /// input is a single run of `value` we truncate `buf` back to `start`
    /// to undo any in-progress writes.
    fn encode_to_unless(
        buf: &mut Vec<u8>,
        iter: impl IntoIterator<Item = T::Get<'a>>,
        value: T::Get<'a>,
    ) -> Range<usize> {
        let start = buf.len();
        let mut s = RStream::<'a, WOps<T, T, C>>::new();
        for v in iter {
            s.append(buf, RleCow::Ref(v), 1);
        }
        // verified with Verus: true only if every value is `value`
        if s.single_run_of(&RleCow::Ref(value)) {
            buf.truncate(start);
            return start..start;
        }
        s.finish(buf);
        start..buf.len()
    }
}

// ── Bool Encoder ────────────────────────────────────────────────────────────

type Counts<C> = crate::bool_wire::Counts<C>;

/// State half of [`BoolEncoder`] — owns the run bookkeeping but **not**
/// the output buffer.  Every mutating method takes a `&mut Vec<u8>`.
///
/// Verified with Verus (`BEnc`): the counts it
/// writes stand for the values appended, and finished without a
/// `finish` in the middle they are a canonical bool slab.
pub struct BoolEncoderState<C: Codec = Leb128> {
    s: crate::bool_enc::BEnc,
    _codec: std::marker::PhantomData<fn() -> C>,
}

impl<C: Codec> Default for BoolEncoderState<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Codec> BoolEncoderState<C> {
    pub fn new() -> Self {
        Self {
            s: crate::bool_enc::BEnc::new(),
            _codec: std::marker::PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.s.len
    }

    pub fn is_empty(&self) -> bool {
        self.s.len == 0
    }

    #[inline(always)]
    pub fn append(&mut self, buf: &mut Vec<u8>, value: bool) {
        self.s.append::<Counts<C>>(buf, value, 1);
    }

    #[inline(always)]
    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: bool, n: usize) {
        self.s.append::<Counts<C>>(buf, value, n);
    }

    pub fn extend(&mut self, buf: &mut Vec<u8>, iter: impl IntoIterator<Item = bool>) {
        for v in iter {
            self.append(buf, v);
        }
    }

    /// Flush any pending run into `buf`. Appending afterwards goes on
    /// from the run after it, which has the other value.
    pub fn finish(&mut self, buf: &mut Vec<u8>) {
        self.s.finish::<Counts<C>>(buf);
    }

    /// True if every appended value equals `value` (or nothing has been
    /// appended).  Used to decide elision before flushing — no decoder
    /// round-trip and no trailing count byte to write+truncate.
    ///
    /// `cur_count == len` holds iff no run transition has occurred, which
    /// means every appended value was equal to `cur_value`.  Note the leading
    /// 0-count false run that `flush_run` emits when transitioning into the
    /// first true is *encoding-level* padding — semantically the data is
    /// still a single run of `cur_value`.
    pub fn all_equal_pre_finish(&self, value: bool) -> bool {
        self.s.all_equal_pre_finish(value)
    }
}

/// Streaming encoder for boolean columns.
///
/// Uses the alternating run-length format: `[false_count, true_count, false_count, ...]`.
///
/// Like [`RleEncoder`], setting a segment budget via
/// [`EncoderApi::max_segments`]
/// before appending enables slab rollover for a direct
/// [`into_column`](crate::encoding::EncoderApi::into_column) path.
///
/// The encoding and the slab rollover are verified with Verus
/// (`BEncoder`): every slab is a canonical
/// bool slab with its metadata, and together they hold the values
/// appended.
pub struct BoolEncoder<C: Codec = Leb128> {
    e: crate::bool_enc::BEncoder,
    _codec: std::marker::PhantomData<fn() -> C>,
}

impl<C: Codec> Default for BoolEncoder<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Codec> std::fmt::Debug for BoolEncoder<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoolEncoder")
            .field("len", &self.e.state.len)
            .field("buf_len", &self.e.data.len())
            .finish()
    }
}

impl<C: Codec> BoolEncoder<C> {
    /// Create a new empty encoder.
    pub fn new() -> Self {
        Self {
            e: crate::bool_enc::BEncoder::new::<Counts<C>>(None),
            _codec: std::marker::PhantomData,
        }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.e.state.len
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.e.state.len == 0
    }

    /// Flush pending state and return all slabs (rollover slabs + final).
    fn take_slabs(&mut self) -> Vec<Slab<u8>> {
        self.e.take_slabs::<Counts<C>>()
    }

    /// Append a single boolean value.
    #[inline]
    pub fn append(&mut self, value: bool) {
        self.e.append::<Counts<C>>(value, 1);
    }

    /// Append `n` copies of `value`.
    pub fn append_n(&mut self, value: bool, n: usize) {
        self.e.append::<Counts<C>>(value, n);
    }

    fn finish(&mut self) {
        self.e.state.finish::<Counts<C>>(&mut self.e.data);
    }

    /// Flush and return the encoded bytes. Consumes the encoder.
    pub fn save(mut self) -> Vec<u8> {
        if !self.e.slabs.is_empty() {
            let mut out = Vec::new();
            self.save_to(&mut out);
            return out;
        }
        self.finish();
        self.e.data
    }

    /// Flush and append the encoded bytes to `out`. Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(mut self, out: &mut Vec<u8>) -> Range<usize> {
        if !self.e.slabs.is_empty() {
            // Slab rollover happened — merge boundary runs back into
            // canonical bytes via the column save path.
            let max = self.e.max_segments.unwrap_or(DEFAULT_MAX_SEG);
            let col: Column<bool, C> = Column::from_slabs(self.take_slabs(), max);
            return col.save_to(out);
        }
        self.finish();
        let start = out.len();
        out.extend_from_slice(&self.e.data);
        start..out.len()
    }

    /// Like [`save_to`](Self::save_to) but returns an empty range if the
    /// encoded data is empty or consists entirely of a single run of `value`.
    ///
    /// Uses the pre-finish check (`all_equal_pre_finish`) so we never write
    /// the trailing count byte we'd just throw away on elision, and we
    /// handle `[true]` + sentinel=true correctly (the leading 0-count
    /// false run is part of the encoding, not part of the logical content).
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: bool) -> Range<usize> {
        if self.e.slabs.is_empty() && self.e.state.all_equal_pre_finish(value) {
            return out.len()..out.len();
        }
        self.save_to(out)
    }

    /// Append all values from an iterator.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = bool>) {
        for value in iter {
            self.append(value);
        }
    }
}

impl<'a, C: Codec> EncoderApi<'a, bool, C> for BoolEncoder<C> {
    type Tail = u8;
    fn append(&mut self, value: bool) {
        self.append(value);
    }

    fn append_owned(&mut self, value: bool) {
        self.append(value);
    }
    fn append_n(&mut self, value: bool, n: usize) {
        self.append_n(value, n);
    }
    fn append_n_owned(&mut self, value: bool, n: usize) {
        self.append_n(value, n);
    }
    fn extend(&mut self, iter: impl IntoIterator<Item = bool>) {
        self.extend(iter);
    }
    fn len(&self) -> usize {
        self.e.state.len
    }
    fn save(self) -> Vec<u8> {
        self.save()
    }
    fn save_to(self, out: &mut Vec<u8>) -> Range<usize> {
        self.save_to(out)
    }
    fn save_to_unless(self, out: &mut Vec<u8>, value: bool) -> Range<usize> {
        self.save_to_unless(out, value)
    }

    /// Enable slab rollover — see [`RleEncoder`]'s
    /// [`max_segments`](crate::encoding::EncoderApi::max_segments).
    /// Must be called before appending.
    fn max_segments(&mut self, max: usize) {
        assert!(max >= 2, "max_segments must be at least 2");
        debug_assert!(
            self.e.state.len == 0,
            "max_segments must be set before appending"
        );
        self.e.max_segments = Some(max);
    }

    /// Direct path: hand the rollover slabs to the column, skipping the
    /// decode pass.  Falls back to save-then-load when `max_segments`
    /// was never set.
    fn into_column<WF, Idx>(mut self) -> Column<bool, C, WF, Idx>
    where
        WF: WeightFn<bool, C>,
        WF::Weight: SlabAggregate,
        Idx: ColumnIndex<WF::Weight>,
    {
        match self.e.max_segments {
            Some(max) => Column::from_slabs(self.take_slabs(), max),
            None => Column::load(&self.save()).unwrap(),
        }
    }

    fn into_slab(mut self) -> Slab<Self::Tail> {
        // the verified `take_slab` requires that nothing was cut
        assert!(
            self.e.slabs.is_empty(),
            "into_slab is single-slab only — don't combine with max_segments"
        );
        // verified with Verus: a canonical slab of the values, with its
        // metadata (the tail is the last count's width, measured as
        // `finish` writes it)
        let (data, len, segments, tail) = self.e.take_slab::<Counts<C>>();
        Slab {
            data,
            len,
            segments,
            tail,
        }
    }

    /// Fast-path: write through to `buf` via [`BoolEncoderState`] without
    /// allocating an inner `Vec<u8>`.
    fn encode_to(buf: &mut Vec<u8>, iter: impl IntoIterator<Item = bool>) -> Range<usize> {
        let start = buf.len();
        let mut state = BoolEncoderState::<C>::new();
        state.extend(buf, iter);
        state.finish(buf);
        start..buf.len()
    }

    /// Fast-path: write through to `buf` via [`BoolEncoderState`].  Decide
    /// elision *before* flushing — `all_equal_pre_finish` only consults the
    /// state's `cur_value` / `cur_count` / `len`, no decoder round-trip and
    /// no extra count byte to truncate.
    fn encode_to_unless(
        buf: &mut Vec<u8>,
        iter: impl IntoIterator<Item = bool>,
        value: bool,
    ) -> Range<usize> {
        let start = buf.len();
        let mut state = BoolEncoderState::<C>::new();
        state.extend(buf, iter);
        if state.all_equal_pre_finish(value) {
            buf.truncate(start);
            return start..start;
        }
        state.finish(buf);
        start..buf.len()
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::bool::{BoolDecoder, BoolEncoding};
    use crate::encoding::{ColumnEncoding, EncoderApi};
    use crate::rle::{RleDecoder, RleEncoding};
    use crate::Leb128;
    use crate::{Column, ColumnValueRef, Encoder};

    /// Create an encoder for type T via the encoding trait.
    fn encoder<'a, T: ColumnValueRef>() -> Encoder<'a, T> {
        T::Encoding::encoder()
    }

    #[test]
    fn rle_encoder_u64() {
        let mut enc = encoder::<u64>();
        enc.append(1u64);
        enc.append(1u64);
        enc.append(1u64);
        enc.append(2u64);
        enc.append(3u64);
        assert_eq!(enc.len(), 5);

        let bytes = enc.save();
        let info = RleEncoding::<u64>::validate_encoding(&bytes).unwrap();
        assert_eq!(info.len, 5);

        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals, vec![1, 1, 1, 2, 3]);
    }

    #[test]
    fn rle_encoder_append_n() {
        let mut enc = encoder::<u64>();
        enc.append_n(7u64, 100);
        enc.append(8u64);
        assert_eq!(enc.len(), 101);

        let bytes = enc.save();
        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals.len(), 101);
        assert!(vals[..100].iter().all(|&v| v == 7));
        assert_eq!(vals[100], 8);
    }

    #[test]
    fn rle_encoder_string() {
        let mut enc = encoder::<String>();
        enc.append("hello");
        enc.append("hello");
        enc.append("world");
        assert_eq!(enc.len(), 3);

        let bytes = enc.save();
        let vals: Vec<&str> = RleDecoder::<String>::new(&bytes).collect();
        assert_eq!(vals, vec!["hello", "hello", "world"]);
    }

    #[test]
    fn rle_encoder_nullable() {
        let mut enc = encoder::<Option<u64>>();
        enc.append(Some(1u64));
        enc.append(None);
        enc.append(None);
        enc.append(Some(2u64));
        assert_eq!(enc.len(), 4);

        let bytes = enc.save();
        let vals: Vec<Option<u64>> = RleDecoder::<Option<u64>>::new(&bytes).collect();
        assert_eq!(vals, vec![Some(1), None, None, Some(2)]);
    }

    #[test]
    fn rle_encoder_save_to() {
        let mut enc = encoder::<u64>();
        enc.append(42u64);
        enc.append(42u64);

        let mut out = vec![0xFF]; // prefix byte
        let range = enc.save_to(&mut out);
        assert_eq!(range.start, 1);
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_empty() {
        let enc = encoder::<u64>();
        assert!(enc.is_empty());
        let bytes = enc.save();
        assert!(bytes.is_empty());
    }

    #[test]
    fn bool_encoder_basic() {
        let mut enc = encoder::<bool>();
        enc.append(false);
        enc.append(false);
        enc.append(true);
        enc.append(true);
        enc.append(true);
        enc.append(false);
        assert_eq!(enc.len(), 6);

        let bytes = enc.save();
        let vals: Vec<bool> = BoolDecoder::<Leb128>::new(&bytes).collect();
        assert_eq!(vals, vec![false, false, true, true, true, false]);
    }

    #[test]
    fn bool_encoder_starts_true() {
        let mut enc = encoder::<bool>();
        enc.append(true);
        enc.append(true);
        enc.append(false);
        assert_eq!(enc.len(), 3);

        let bytes = enc.save();
        // Wire format: [0 false, 2 true, 1 false]
        let vals: Vec<bool> = BoolDecoder::<Leb128>::new(&bytes).collect();
        assert_eq!(vals, vec![true, true, false]);
    }

    #[test]
    fn bool_encoder_append_n() {
        let mut enc = encoder::<bool>();
        enc.append_n(true, 100);
        enc.append_n(false, 50);
        assert_eq!(enc.len(), 150);

        let bytes = enc.save();
        let vals: Vec<bool> = BoolDecoder::<Leb128>::new(&bytes).collect();
        assert_eq!(vals.len(), 150);
        assert!(vals[..100].iter().all(|&v| v));
        assert!(vals[100..].iter().all(|&v| !v));
    }

    #[test]
    fn bool_encoder_empty() {
        let enc = encoder::<bool>();
        assert!(enc.is_empty());
        let bytes = enc.save();
        assert!(bytes.is_empty());
    }

    #[test]
    fn bool_encoder_save_to_unless() {
        // Empty encoder — skipped regardless of value
        let enc = encoder::<bool>();
        let mut out = vec![];
        let range = enc.save_to_unless(&mut out, false);
        assert!(range.is_empty());

        // Single run of false — skipped when value=false
        let mut enc = encoder::<bool>();
        enc.append_n(false, 10);
        let range = enc.save_to_unless(&mut out, false);
        assert!(range.is_empty());

        // Single run of true — NOT skipped when value=false
        let mut enc = encoder::<bool>();
        enc.append_n(true, 10);
        let range = enc.save_to_unless(&mut out, false);
        assert!(!range.is_empty());

        // Mixed — never skipped
        out.clear();
        let mut enc = encoder::<bool>();
        enc.append(true);
        enc.append(false);
        let range = enc.save_to_unless(&mut out, false);
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_save_to_unless() {
        let mut out = vec![];

        // Empty — skipped
        let enc = encoder::<u64>();
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(range.is_empty());

        // Single run of 0 — skipped when value=0
        let mut enc = encoder::<u64>();
        enc.append_n(0u64, 100);
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(range.is_empty());

        // Single run of 0 — NOT skipped when value=1
        let mut enc = encoder::<u64>();
        enc.append_n(0u64, 100);
        let range = enc.save_to_unless(&mut out, 1u64);
        assert!(!range.is_empty());

        // Multiple values — never skipped
        out.clear();
        let mut enc = encoder::<u64>();
        enc.append(1u64);
        enc.append(2u64);
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(!range.is_empty());
    }

    #[test]
    fn nullable_encoder_save_to_unless() {
        let mut out = vec![];

        // All nulls — skipped when value=None
        let mut enc = encoder::<Option<u64>>();
        enc.append_n(None, 50);
        let range = enc.save_to_unless(&mut out, None);
        assert!(range.is_empty());

        // All nulls — NOT skipped when value=Some(0)
        let mut enc = encoder::<Option<u64>>();
        enc.append_n(None, 50);
        let range = enc.save_to_unless(&mut out, Some(0u64));
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_roundtrip_with_column() {
        let values = [1u64, 2, 3, 3, 3, 4, 5, 5, 6];
        let bytes = Encoder::<u64>::encode(values);
        let col = Column::<u64>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }

    #[test]
    fn bool_encoder_roundtrip_with_column() {
        let values = [true, false, true, true, false, false, true];
        let bytes = Encoder::<bool>::encode(values);
        let col = Column::<bool>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }

    #[test]
    fn rle_encode_slab() {
        let slab = Encoder::<u64>::encode_slab([1u64, 1, 1, 2, 3]);
        assert_eq!(slab.len, 5);
        assert!(slab.segments > 0);
        let info = RleEncoding::<u64>::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);
    }

    #[test]
    fn bool_encode_slab() {
        let slab = Encoder::<bool>::encode_slab([false, true, true, false]);
        assert_eq!(slab.len, 4);
        assert_eq!(slab.segments, 3);
        let info = BoolEncoding::<Leb128>::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);
    }

    #[test]
    fn encoder_extend() {
        let bytes = Encoder::<u64>::encode([1u64, 2, 3, 3, 3]);
        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals, vec![1, 2, 3, 3, 3]);
    }

    #[test]
    fn bool_encoder_extend() {
        let bytes = Encoder::<bool>::encode([true, true, false, true]);
        let vals: Vec<bool> = BoolDecoder::<Leb128>::new(&bytes).collect();
        assert_eq!(vals, vec![true, true, false, true]);
    }

    #[test]
    fn encoder_100_unique_strings() {
        let values: Vec<String> = (0..100).map(|i| format!("item_{i:04}")).collect();
        let mut enc = encoder::<String>();
        for s in &values {
            enc.append(s.as_str());
        }
        let bytes = enc.save();
        let col = Column::<String>::load(&bytes).unwrap();
        let loaded: Vec<&str> = col.iter().collect();
        let expected: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        assert_eq!(loaded, expected);
    }

    #[test]
    fn encoder_nullable_with_runs_and_nulls() {
        use rand::{RngExt, SeedableRng};
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let choices: [Option<u64>; 4] = [None, Some(1), Some(2), Some(3)];
        let values: Vec<Option<u64>> = (0..100).map(|_| choices[rng.random_range(0..4)]).collect();

        let mut enc = encoder::<Option<u64>>();
        for &v in &values {
            enc.append(v);
        }
        assert_eq!(enc.len(), 100);

        // Validate encode_slab metadata
        let slab = Encoder::<Option<u64>>::encode_slab(values.iter().copied());
        let info = RleEncoding::<Option<u64>>::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);

        // Validate save → load roundtrip
        let bytes = enc.save();
        let col = Column::<Option<u64>>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }
}
