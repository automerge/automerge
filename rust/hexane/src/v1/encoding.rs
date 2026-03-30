use super::column::{Column, Slab, WeightFn};
use super::{AsColumnRef, ColumnValueRef, Run, ValidBuf, ValidBytes};
use crate::PackError;

/// Validation function type for [`ColumnEncoding::load_and_verify`].
pub type ValidateFn<V> = for<'a> fn(<V as ColumnValueRef>::Get<'a>) -> Option<String>;

/// Trait abstracting the byte-level encoding strategy for a column.
///
/// Implementors provide `get`, bulk encoding/decoding, split/merge, and
/// validation operations on raw `Vec<u8>` slabs.  [`Column`] delegates
/// to `T::Encoding`.
///
/// Both [`super::rle::RleEncoding`] and [`super::bool_encoding::BoolEncoding`]
/// are zero-sized types — all state lives in the slab bytes.
pub trait ColumnEncoding: Default {
    /// The column value type this encoding operates on.
    type Value: ColumnValueRef<Encoding = Self>;

    /// Create an empty slab with no items.
    fn empty_slab() -> Slab {
        Slab {
            data: ValidBuf::new(vec![]),
            len: 0,
            segments: 0,
        }
    }

    /// Read the value at `index` from `slab`, returning the optimal `Get` type.
    ///
    /// For `Copy` types this returns the value directly.  For ref types
    /// (`str`, `[u8]`) this borrows directly from the slab without allocation.
    /// Returns `None` if `index >= len`.
    fn get<'a>(
        slab: &'a ValidBytes,
        index: usize,
        len: usize,
    ) -> Option<<Self::Value as ColumnValueRef>::Get<'a>>;

    /// Merge slab `b` into `a` in place. Both slabs must be non-empty.
    /// Handles boundary run merging. Updates `a.len` and `a.segments`.
    fn merge_slabs(a: &mut Slab, b: &Slab);

    /// Validate that `slab` is in canonical encoding form.
    ///
    /// Returns `Ok(())` if the encoding is canonical, or `Err(description)` if
    /// any invariant is violated.  This is intended for testing and debugging.
    fn validate_encoding(slab: &[u8]) -> Result<(), String>;

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
    ) -> Result<Vec<Slab>, PackError>;

    /// Serialize multiple slabs into a single canonical byte array in O(n).
    ///
    /// Memcopies slab interiors and only decodes/re-encodes the boundary
    /// runs between adjacent slabs.
    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8>;

    /// Splice a single slab: delete `del` items at `index`, insert values.
    /// `del` may exceed the slab length — the excess is returned as `overflow_del`.
    /// Returns `(overflow_slabs, overflow_del)`.
    fn splice_slab<V: AsColumnRef<Self::Value>>(
        slab: &mut Slab,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<Slab>, usize);

    /// Splice a Column: locate the target slab, splice it, handle overflow
    /// and cross-slab deletes, merge small neighbours, update the BIT.
    fn splice<WF: WeightFn<Self::Value>, V: AsColumnRef<Self::Value>>(
        col: &mut Column<Self::Value, WF>,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
    ) {
        use super::column::{bit_point_update, find_slab, rebuild_bit};

        assert!(!col.slabs.is_empty());

        // find_slab returns si == slabs.len() when index == total_len (append at end).
        // Clamp to last slab with offset = slab.len.
        let (mut si, mut offset) = find_slab(&col.bit, index, col.slabs.len());
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
        while remaining > 0 && range.end < col.slabs.len() {
            let slab_len = col.slabs[range.end].len;
            if remaining >= slab_len {
                col.slabs.remove(range.end);
                old_slab_len += slab_len;
                remaining -= slab_len;
            } else {
                old_slab_len += slab_len;
                Self::splice_slab(
                    &mut col.slabs[range.end],
                    0,
                    remaining,
                    std::iter::empty::<V>(),
                    col.max_segments,
                );
                range.end += 1; // include the partially deleted slab
                break;
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
        let range = col.try_merge(range);

        // Update BIT.
        if col.slabs.len() == old_slab_count && range.len() == 1 {
            let new_weight = WF::compute(&col.slabs[range.start]);
            bit_point_update(&mut col.bit, range.start, old_weight, new_weight);
        } else {
            col.bit = rebuild_bit::<Self::Value, WF>(&col.slabs);
        }
    }

    /// Decoder type for iterating over all items in a slab.
    type Decoder<'a>: Iterator<Item = <Self::Value as ColumnValueRef>::Get<'a>> + RunDecoder + Clone;

    /// Create a decoder that yields all items in `slab` in order.
    fn decoder(slab: &ValidBytes) -> Self::Decoder<'_>;
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
}
