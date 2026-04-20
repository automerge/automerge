use super::column;
use super::column::{Column, Slab, WeightFn};
use super::{AsColumnRef, ColumnValueRef, Run};
use crate::PackError;

/// Fold-style validation function for [`ColumnEncoding::load_and_verify`].
///
/// Receives `(accumulator, run_count, value)` for each run during the
/// decoding pass.  Returns the updated accumulator on success or an
/// error message.  The accumulator starts at `P::default()`.
pub type ValidateFn<P, V> =
    for<'a> fn(P, usize, <V as ColumnValueRef>::Get<'a>) -> Result<P, String>;

/// Simple per-value validation function (no accumulator).
pub type SimpleValidateFn<V> = for<'a> fn(<V as ColumnValueRef>::Get<'a>) -> Option<String>;

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
    fn load_and_verify_fold<'a, F, P: Default + Copy>(
        data: &'a [u8],
        max_segments: usize,
        validate: Option<F>,
    ) -> Result<Vec<Slab<Self::Tail>>, PackError>
    where
        F: Fn(P, usize, <Self::Value as ColumnValueRef>::Get<'a>) -> Result<P, String>;

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<SimpleValidateFn<Self::Value>>,
    ) -> Result<Vec<Slab<Self::Tail>>, PackError> {
        Self::load_and_verify_fold(
            data,
            max_segments,
            validate.map(|f| {
                move |_, _c, v| match f(v) {
                    Some(s) => Err(s),
                    _ => Ok(()),
                }
            }),
        )
    }

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

    fn remap<'a, F, WF, Idx>(
        mut iter: column::Iter<'a, Self::Value>,
        max: usize,
        f: F,
    ) -> Column<Self::Value, WF, Idx>
    where
        WF: WeightFn<Self::Value>,
        WF::Weight: super::btree::SlabAggregate,
        Idx: super::index::ColumnIndex<WF::Weight>,
        F: Fn(Self::Value) -> Self::Value,
    {
        let mut encoder = Self::encoder();
        encoder.max_segments(max);
        while let Some(run) = iter.next_run() {
            let value = <Self::Value as ColumnValueRef>::to_owned(run.value);
            let value = f(value);
            encoder.append_n_owned(value, run.count);
        }
        encoder.into_column()
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

    fn into_column<WF, Idx>(self) -> Column<T, WF, Idx>
    where
        WF: WeightFn<T>,
        WF::Weight: super::btree::SlabAggregate,
        Idx: super::index::ColumnIndex<WF::Weight>,
    {
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
