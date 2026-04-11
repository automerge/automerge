use super::column;
use super::column::{Column, Slab, WeightFn};
use super::{AsColumnRef, ColumnValueRef, Run};
use crate::PackError;

/// Validation function type for [`ColumnEncoding::load_and_verify`].
pub type ValidateFn<V> = for<'a> fn(<V as ColumnValueRef>::Get<'a>) -> Option<String>;

/// Trait abstracting the byte-level encoding strategy for a column.
///
/// Implementors provide `get`, bulk encoding/decoding, split/merge, and
/// validation operations on raw `Vec<u8>` slabs.  [`Column`] delegates
/// to `T::Encoding`.
///
/// Both [`super::rle::RleEncoding`] and [`super::bool::BoolEncoding`]
/// are zero-sized types — all state lives in the slab bytes.
pub trait ColumnEncoding: Default {
    /// The column value type this encoding operates on.
    type Value: ColumnValueRef<Encoding = Self>;

    /// Per-slab metadata stored alongside data/len/segments.
    type Tail: Copy + Clone + std::fmt::Debug + Default;

    /// Create an empty slab with no items.
    fn empty_slab() -> Slab<Self::Tail> {
        Slab::new(vec![], 0, 0)
    }

    /// Create a slab of `len` copies of `value`. O(1).
    fn fill(len: usize, value: <Self::Value as ColumnValueRef>::Get<'_>) -> Slab<Self::Tail>;

    /// Merge slab `b` into `a` in place. Both slabs must be non-empty.
    fn merge_slabs(a: &mut Slab<Self::Tail>, b: Slab<Self::Tail>);

    /// Validate that `slab` is in canonical encoding form.
    ///
    /// Returns `Ok(())` if the encoding is canonical, or `Err(description)` if
    /// any invariant is violated.  This is intended for testing and debugging.
    fn validate_encoding(slab: &[u8]) -> Result<SlabInfo<Self::Tail>, PackError>;

    /// Decode and validate raw bytes, splitting into slabs.
    ///
    /// Walks the wire format, validates every encoded value is well-formed,
    /// rejects nulls in non-nullable column types, and splits the data into
    /// slabs respecting `max_segments`.
    ///
    /// If `validate` is `Some`, each decoded value is also passed to the
    /// function during the same pass.  Returns
    /// [`PackError::InvalidValue`] if the function returns `Some(msg)`.
    ///
    /// Returns a Vec of Slabs on success.
    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<ValidateFn<Self::Value>>,
    ) -> Result<Vec<Slab<Self::Tail>>, PackError>;

    /// Merge slab `b`'s data into accumulator `acc`.
    /// `a_tail` and `a_segments` describe the current state of `acc`.
    /// Returns `(segment_delta, new_tail)`, or `None` if `b` is empty.
    fn do_merge(
        acc: &mut Vec<u8>,
        a_tail: Self::Tail,
        a_segments: usize,
        b: &Slab<Self::Tail>,
        buf: &mut Vec<u8>,
    ) -> (usize, Self::Tail);

    /// Splice a single slab: delete `del` items at `index`, insert values.
    /// `del` may exceed the slab length — the excess is returned as `overflow_del`.
    /// Returns `(overflow_slabs, overflow_del)`.
    fn splice_slab<V: AsColumnRef<Self::Value>>(
        slab: &mut Slab<Self::Tail>,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<Slab<Self::Tail>>, usize);

    fn remap<'a, F, WF>(
        mut iter: column::Iter<'a, Self::Value>,
        max: usize,
        f: F,
    ) -> Column<Self::Value, WF>
    where
        WF: WeightFn<Self::Value>,
        F: Fn(Self::Value) -> Self::Value,
    {
        let mut encoder = Self::encoder();
        encoder.max_segments(max);
        while let Some(run) = iter.next_run() {
            // an extra copy for remapping strings b/c of the borrow checker
            // zero cost for copy types
            // remap strings never used in automerge so good enough for now
            let value = <Self::Value as ColumnValueRef>::to_owned(run.value);
            let value = f(value);
            encoder.append_n_owned(value, run.count);
        }
        encoder.into_column()
    }

    /// Splice a Column: locate the target slab, splice it, handle overflow
    /// and cross-slab deletes, merge small neighbours, update the BIT.
    fn splice<WF: WeightFn<Self::Value>, V: AsColumnRef<Self::Value>>(
        col: &mut Column<Self::Value, WF>,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
    ) {
        use super::column::{bit_point_update, find_slab_bit, rebuild_bit};

        assert!(!col.slabs.is_empty());

        // find_slab returns si == slabs.len() when index == total_len (append at end).
        // Clamp to last slab with offset = slab.len.
        let (mut si, mut offset) = find_slab_bit(&col.bit, index, col.slabs.len());
        if si >= col.slabs.len() {
            si = col.slabs.len() - 1;
            offset = col.slabs[si].len;
        }

        let mut range = si..(si + 1);
        let mut old_slab_len = col.slabs[si].len;
        let old_weight = WF::compute(&col.slabs[si]);
        let old_slab_count = col.slabs.len();

        let (overflow, overflow_del) =
            Self::splice_slab(&mut col.slabs[si], offset, del, values, col.max_segments);

        // Insert overflow slabs.
        if !overflow.is_empty() {
            let pos = range.end;
            range.end += overflow.len();
            col.slabs.splice(pos..pos, overflow);
        }

        // Apply remaining deletes to subsequent slabs.
        let mut remaining = overflow_del;
        let drain_start = range.end;
        while remaining > 0 && range.end < col.slabs.len() {
            let slab_len = col.slabs[range.end].len;
            if remaining >= slab_len {
                old_slab_len += slab_len;
                remaining -= slab_len;
                range.end += 1;
            } else {
                old_slab_len += slab_len;
                let (partial_overflow, _) = Self::splice_slab(
                    &mut col.slabs[range.end],
                    0,
                    remaining,
                    std::iter::empty::<V>(),
                    col.max_segments,
                );
                range.end += 1; // include the partially deleted slab
                if !partial_overflow.is_empty() {
                    let pos = range.end;
                    range.end += partial_overflow.len();
                    col.slabs.splice(pos..pos, partial_overflow);
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
                col.slabs.drain(drain_start..drain_start + partial);
                range.end -= partial;
            }
        }

        // add new total for affected slabs
        col.total_len += col.slabs[range.clone()]
            .iter()
            .map(|s| s.len)
            .sum::<usize>();
        col.total_len -= old_slab_len; // and subtract the old total
        debug_assert_eq!(
            col.total_len,
            col.slabs.iter().map(|s| s.len).sum::<usize>()
        );

        // Try merging small neighbours at the boundaries.
        let range = col.try_merge_range(range);

        // Update BIT.
        if col.slabs.len() == old_slab_count && range.len() == 1 {
            let new_weight = WF::compute(&col.slabs[range.start]);
            bit_point_update(&mut col.bit, range.start, old_weight, new_weight);
        } else {
            col.bit = rebuild_bit::<Self::Value, WF>(&col.slabs);
        }
    }

    /// Decode the last run of a slab using tail metadata.
    ///
    /// Returns the value and count of the final run without decoding
    /// the entire slab.  Returns `None` for empty slabs.
    fn last_run(slab: &Slab<Self::Tail>) -> Option<Run<<Self::Value as ColumnValueRef>::Get<'_>>>;

    /// Decoder type for iterating over all items in a slab.
    type Decoder<'a>: Iterator<Item = <Self::Value as ColumnValueRef>::Get<'a>> + RunDecoder + Clone;

    /// Create a decoder that yields all items in `slab` in order.
    fn decoder(slab: &[u8]) -> Self::Decoder<'_>;

    /// Streaming encoder for building encoded bytes from a sequence of values.
    type Encoder<'a>: EncoderApi<'a, Self::Value>;

    /// Create a new empty encoder.
    fn encoder<'a>() -> Self::Encoder<'a>;

    fn encode<V: AsColumnRef<Self::Value>>(values: impl Iterator<Item = V>) -> Slab<Self::Tail> {
        let mut slab = Self::empty_slab();
        Self::splice_slab(&mut slab, 0, 0, values, usize::MAX);
        slab
    }
}

/// Trait for streaming encoders that build encoded bytes from a sequence of values.
pub trait EncoderApi<'a, T: ColumnValueRef>: Sized {
    /// Append a single value.
    fn append(&mut self, value: T::Get<'a>);
    fn append_owned(&mut self, value: T);
    /// Append `n` copies of `value`.
    fn append_n(&mut self, value: T::Get<'a>, n: usize);
    fn append_n_owned(&mut self, value: T, n: usize);

    /// Append all values from an iterator.
    fn extend(&mut self, iter: impl IntoIterator<Item = T::Get<'a>>) {
        for value in iter {
            self.append(value);
        }
    }

    /// Number of items appended so far.
    fn len(&self) -> usize;

    /// Returns `true` if no items have been appended.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // TODO - actually store the segments
    // make into_column not do an extra copy
    fn max_segments(&mut self, _max: usize) {}

    fn into_column<WF: WeightFn<T>>(self) -> Column<T, WF> {
        Column::load(&self.save()).unwrap()
    }

    /// Flush and return the encoded bytes. Consumes the encoder.
    fn save(self) -> Vec<u8>;
    /// Flush and append the encoded bytes to `out`. Returns the byte range written.
    fn save_to(self, out: &mut Vec<u8>) -> std::ops::Range<usize>;
    /// Like `save_to` but returns an empty range if the encoded data is empty
    /// or consists entirely of a single run of `value`.
    fn save_to_unless(self, out: &mut Vec<u8>, value: T::Get<'a>) -> std::ops::Range<usize>;
    /// Flush and return a single [`Slab`] with correct len, segments, and tail.
    fn into_slab(self) -> Slab<Self::Tail>;
    /// The tail metadata type for this encoding.
    type Tail: Copy + Clone + std::fmt::Debug + Default;

    /// Encode values from an iterator and return the raw bytes.
    fn encode(iter: impl IntoIterator<Item = T::Get<'a>>) -> Vec<u8>
    where
        Self: Default,
    {
        let mut buf = vec![];
        Self::encode_to(&mut buf, iter);
        buf
    }

    fn encode_to(
        buf: &mut Vec<u8>,
        iter: impl IntoIterator<Item = T::Get<'a>>,
    ) -> std::ops::Range<usize>
    where
        Self: Default,
    {
        let mut enc = Self::default();
        enc.extend(iter);
        enc.save_to(buf)
    }

    fn encode_to_unless(
        buf: &mut Vec<u8>,
        iter: impl IntoIterator<Item = T::Get<'a>>,
        value: T::Get<'a>,
    ) -> std::ops::Range<usize>
    where
        Self: Default,
    {
        let mut enc = Self::default();
        enc.extend(iter);
        enc.save_to_unless(buf, value)
    }

    /// Encode values from an iterator and return a [`Slab`] with correct metadata.
    fn encode_slab(iter: impl IntoIterator<Item = T::Get<'a>>) -> Slab<Self::Tail>
    where
        Self: Default,
    {
        let mut enc = Self::default();
        enc.extend(iter);
        enc.into_slab()
    }
}

/// Trait for decoders that can yield runs of values.
///
/// See [`Run`] for the semantics of each run kind.
pub trait RunDecoder: Iterator {
    /// Returns the next run of values without consuming individual items.
    ///
    /// For repeat runs, returns the remaining count and the repeated value.
    /// For null runs, returns the remaining count and the null representation.
    /// For literal runs, returns count=1 and the next value (consuming it).
    ///
    /// Returns `None` when the iterator is exhausted.
    fn next_run(&mut self) -> Option<Run<Self::Item>>;

    /// Like [`next_run`](Self::next_run) but consumes at most `max` items
    /// from repeat/null runs.  The remaining items stay in the decoder
    /// for subsequent calls.
    fn next_run_max(&mut self, max: usize) -> Option<Run<Self::Item>>;

    /// Scan forward for `target`, assuming runs are sorted ascending.
    ///
    /// Walks runs (consuming at most `max` items total) until either the
    /// target is found, a run greater than the target is encountered, or
    /// the decoder is exhausted.
    ///
    /// Returns `(skipped, count)` where `skipped` is items consumed before
    /// the match/insertion point and `count` is the length of the matching
    /// run (or `0` if not found).
    fn scan_for(&mut self, target: Self::Item, max: usize) -> (usize, usize)
    where
        Self::Item: Ord,
    {
        use std::cmp::Ordering;
        let mut skipped = 0;
        while let Some(run) = self.next_run_max(max - skipped) {
            match run.value.cmp(&target) {
                Ordering::Equal => return (skipped, run.count),
                Ordering::Greater => return (skipped, 0),
                Ordering::Less => skipped += run.count,
            }
        }
        (skipped, 0)
    }
}

/// Metadata extracted from a validated slab encoding.
///
/// Returned by [`ColumnEncoding::validate_encoding`] and
/// [`Column::validate_encoding_info`](super::Column::validate_encoding_info).
pub struct SlabInfo<T> {
    /// Number of RLE/bool segments in the slab.
    pub segments: usize,
    /// Number of logical items in the slab.
    pub len: usize,
    /// Per-encoding tail metadata (e.g. [`RleTail`](super::rle::RleTail) for RLE columns).
    pub tail: T,
}
