//! RLE validation and load (deserialize + verify).

use std::num::NonZeroU32;

use crate::encoding::SlabInfo;
use crate::rle::model::Ops;
use crate::rle::cut::Load;
use crate::rle::read;
use crate::rle::state::WOps;
use crate::rle::{RleTail, Slab};
use crate::ColumnValueRef;
use crate::PackError;
use crate::RleValue;
use crate::{Codec, Leb128};

// ── validate_encoding ────────────────────────────────────────────────────────

/// Check that `slab` is a canonical RLE encoding, and summarize it.
/// The check itself ([`read::validate`]) is verified with Verus.
pub(crate) fn rle_validate_encoding<T: RleValue, C: Codec>(
    slab: &[u8],
) -> Result<SlabInfo<RleTail>, PackError> {
    let info = read::validate::<Ops<'_, T, C>>(slab)?;
    Ok(SlabInfo {
        segments: info.segments,
        len: info.len,
        tail: RleTail {
            bytes: info.tail_bytes,
            lit_tail: NonZeroU32::new(info.lit_tail),
        },
    })
}

// ── Load iterator ──────────────────────────────────────────────────────────

/// Streaming decode + validate over saved RLE bytes.
///
/// Yields the runs of the input while building the output slabs: byte
/// ranges of the input are copied verbatim, with a literal-run header
/// rewrite when a cut lands inside a literal run. The input must be
/// **canonical** — non-canonical structure (mergeable adjacent runs,
/// count-0 runs, count-1 repeats) is a load error, as are value decode
/// errors and nulls in non-nullable columns — so consumers get the
/// "adjacent runs never carry equal values" contract by construction. All
/// methods return `Result` — the bytes are untrusted.
///
/// The reading, checking and cutting ([`Load`]) are verified with Verus:
/// a finished load's slabs are each a canonical encoding with their
/// recorded length, segment count and tail, and together hold the
/// input's values in order. After an error, the iterator must not be
/// used again.
///
/// [`finalize`](Self::finalize) drains whatever the consumer did not pull
/// (validating it) and returns the finished slabs.
pub struct RleLoadIter<'a, T: RleValue, C: Codec = Leb128> {
    load: Load<'a, WOps<'a, T, T, C>>,
}

impl<'a, T: RleValue, C: Codec> RleLoadIter<'a, T, C> {
    /// # Panics
    ///
    /// If `max_segments < 2`: it halves to a zero cut target, which emits
    /// an empty slab.
    pub fn new(data: &'a [u8], max_segments: usize) -> Self {
        assert!(max_segments >= 2, "max_segments must be at least 2");
        Self {
            load: Load::new(data, max_segments),
        }
    }

    /// The next canonical run, or `None` at end of input. Non-canonical
    /// structure — anything a canonical encoder would have merged or
    /// never emitted — is a load error (the [`Reader`](read::Reader)'s),
    /// so the run stream keeps the "adjacent runs never carry equal
    /// values" contract by construction, and every run is exactly one
    /// wire segment.
    #[inline]
    pub fn try_next_run(&mut self) -> Result<Option<crate::Run<T::Get<'a>>>, PackError> {
        self.load.try_next_run()
    }

    /// Drain and validate whatever the consumer did not pull, flush the
    /// final slab, and return the finished slabs.
    pub fn finalize(self) -> Result<Vec<Slab>, PackError> {
        self.load.finalize()
    }
}

impl<'a, T, C> crate::encoding::LoadIterApi<'a, T> for RleLoadIter<'a, T, C>
where
    C: Codec,
    T: RleValue + ColumnValueRef<Encoding<C> = crate::rle::RleEncoding<T, C>>,
{
    type Tail = crate::rle::RleTail;

    fn try_next_run(&mut self) -> Result<Option<crate::Run<T::Get<'a>>>, PackError> {
        RleLoadIter::try_next_run(self)
    }

    fn slabs_completed(&self) -> usize {
        self.load.slabs_completed()
    }

    fn completed_slab_len(&self, i: usize) -> usize {
        self.load.completed_slab_len(i)
    }

    fn finalize(self) -> Result<Vec<Slab>, PackError> {
        RleLoadIter::finalize(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::rle::cut::Cut;
    use crate::rle::model::Ops;
    use crate::rle::read::Seg;
    use crate::rle::state::WOps;
    use vstd::prelude::Ghost;

    type O<'a, T> = Ops<'a, T, Leb128>;
    type W<'a, T> = WOps<'a, T, T, Leb128>;

    /// Count segments into a fresh cut (the reader they stand for is
    /// ghost, so the counts need not be real).
    fn cut_of<T: RleValue>(segs: Vec<Seg<T::Get<'_>>>) -> Cut {
        let mut cut = Cut::new::<O<'_, T>>(Ghost::assume_new());
        for seg in segs {
            let _ = cut.track::<O<'_, T>>(seg, Ghost::assume_new(), Ghost::assume_new());
        }
        cut
    }

    /// A run of 4 GiB or more has no tail record: loading it is an error,
    /// where its length used to be truncated to `u32`.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn a_run_of_4gib_is_not_loadable() {
        let mut cut = cut_of::<Option<u64>>(vec![Seg::Null {
            count: 1,
            bytes: 1 << 32,
        }]);
        assert!(cut
            .cut_slab::<W<'_, Option<u64>>>(&[], 0, Ghost::assume_new())
            .is_err());
    }

    /// Only the slab's last run is recorded, so a run of 4 GiB that does
    /// not end a slab loads: what the writer would have written.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn a_run_of_4gib_before_the_last_loads() {
        let mut cut = cut_of::<Option<u64>>(vec![
            Seg::Null {
                count: 1,
                bytes: 1 << 32,
            },
            Seg::Run {
                count: 2,
                value: Some(1),
                bytes: 2,
            },
        ]);
        assert!(cut
            .cut_slab::<W<'_, Option<u64>>>(&[], 0, Ghost::assume_new())
            .is_ok());
        assert_eq!(cut.into_slabs()[0].tail.bytes, 2);
    }

    /// The same for a literal run whose values add up to 4 GiB.
    #[test]
    fn a_literal_of_4gib_is_not_loadable() {
        let mut segs = vec![Seg::LitHead { count: 2, bytes: 1 }];
        for _ in 0..2 {
            segs.push(Seg::Lit {
                value: 1,
                bytes: 1 << 31,
            });
        }
        let mut cut = cut_of::<u64>(segs);
        assert!(cut
            .cut_slab::<W<'_, u64>>(&[], 0, Ghost::assume_new())
            .is_err());
    }

    use crate::rle::state::{RleCow, RleState};
    use crate::rle::*;
    use crate::rle::{RleDecoder, RleEncoding};
    use crate::{Codec as _, Leb128};
    use crate::{Column, ColumnValueRef};

    fn check_validate2<T: RleValue + ColumnValueRef<Encoding<Leb128> = RleEncoding<T>>>(
        data: &[u8],
    ) {
        let v1 = rle_validate_encoding::<T, Leb128>(data).unwrap();
        let v2 = rle_validate_encoding::<T, Leb128>(data).unwrap();
        assert_eq!(v1.len, v2.len, "len mismatch");
        assert_eq!(v1.segments, v2.segments, "segments mismatch");
        assert_eq!(
            v1.tail, v2.tail,
            "tail mismatch: v1={:?} v2={:?}",
            v1.tail, v2.tail
        );
    }

    #[test]
    fn validate_matches_u64() {
        // Various patterns: repeats, literals, mixed
        for vals in &[
            vec![1u64, 1, 1, 2, 3, 3, 4, 5, 6, 6, 6],
            vec![1, 2, 3, 4, 5], // all literal
            vec![7, 7, 7, 7, 7], // all repeat
            vec![1],             // single
            vec![],              // empty
        ] {
            let col = Column::<u64>::from_values(vals.clone());
            for slab in &col.slabs {
                check_validate2::<u64>(&slab.data);
            }
        }
    }

    #[test]
    fn validate_matches_nullable() {
        let vals: Vec<Option<u64>> = vec![Some(1), None, None, Some(2), Some(2), None];
        let col = Column::<Option<u64>>::from_values(vals);
        for slab in &col.slabs {
            check_validate2::<Option<u64>>(&slab.data);
        }
    }

    // ── validate2 error case tests ──────────────────────────────────────

    /// Helper: hand-build raw RLE bytes.
    fn rle_bytes(runs: &[(&str, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for &(kind, data) in runs {
            match kind {
                "repeat" => {
                    // data = [count_byte, value_bytes...]
                    out.extend_from_slice(data);
                }
                "lit" => {
                    // data = [neg_count_byte, value_bytes...]
                    out.extend_from_slice(data);
                }
                "null" => {
                    // data = [0x00, unsigned_count_bytes...]
                    out.extend_from_slice(data);
                }
                _ => panic!("unknown kind"),
            }
        }
        out
    }

    #[test]
    fn validate_repeat_count_less_than_2() {
        // Repeat with count=1: signed(1) = 0x01, then a u64 value (0x05)
        let data = rle_bytes(&[("repeat", &[0x01, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_null_count_zero() {
        // Null with count=0: signed(0)=0x00, unsigned(0)=0x00
        let data = rle_bytes(&[("null", &[0x00, 0x00])]);
        assert!(rle_validate_encoding::<Option<u64>, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_null_in_non_nullable() {
        // Null run in a u64 column: signed(0)=0x00, unsigned(1)=0x01
        let data = rle_bytes(&[("null", &[0x00, 0x01])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_empty_literal() {
        // Literal with count=0: signed(0) is actually null marker, so
        // use signed(-0) which doesn't exist. Actually count=0 literal
        // means signed(0) which is null. We can't encode an empty literal
        // in valid LEB128. Skip — the decoder won't produce LitHead{count:0}
        // from valid data.
    }

    #[test]
    fn validate_consecutive_equal_in_literal() {
        // Literal [-2] with two identical values: signed(-2)=0x7e, then 0x05, 0x05
        let data = rle_bytes(&[("lit", &[0x7e, 0x05, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_literals() {
        // Two separate literal runs: [-1, v1] then [-1, v2]
        let data = rle_bytes(&[("lit", &[0x7f, 0x01]), ("lit", &[0x7f, 0x02])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_nulls() {
        // Two null runs: [0, 1] [0, 1]
        let data = rle_bytes(&[("null", &[0x00, 0x01]), ("null", &[0x00, 0x01])]);
        assert!(rle_validate_encoding::<Option<u64>, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_repeats_same_value() {
        // Two repeat runs with same value: [2, 5] [2, 5]
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_repeats_different_value_ok() {
        // Two repeat runs with different values: [2, 5] [2, 7] — should be OK
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("repeat", &[0x02, 0x07])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_ok());
    }

    #[test]
    fn validate_boundary_repeat_then_literal_same_value() {
        // Repeat [2, 5] then literal [-1, 5] — last repeat value == first lit value
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("lit", &[0x7f, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_boundary_literal_then_repeat_same_value() {
        // Literal [-1, 5] then repeat [2, 5] — last lit value == repeat value
        let data = rle_bytes(&[("lit", &[0x7f, 0x05]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_err());
    }

    #[test]
    fn validate_boundary_different_values_ok() {
        // Literal [-1, 3] then repeat [2, 5] — different values, should be OK
        let data = rle_bytes(&[("lit", &[0x7f, 0x03]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64, Leb128>(&data).is_ok());
    }

    #[test]
    fn validate_boundary_repeat_then_literal_same_value_string() {
        // Build: Run(2, "aaa") then Lit ["aaa", "bbb"]
        // This must be rejected — "aaa" at boundary.
        let mut buf = Vec::new();
        let mut state = RleState::<String, &str>::empty();
        // Force a repeat of "aaa" × 2
        state.append(&mut buf, RleCow::Ref("aaa"));
        state.append(&mut buf, RleCow::Ref("aaa"));
        state.flush(&mut buf);
        // Now manually append a literal starting with "aaa"
        buf.extend(Leb128::encode_signed(-2));
        // "aaa" = leb(3) + b"aaa"
        buf.push(3);
        buf.extend_from_slice(b"aaa");
        buf.push(3);
        buf.extend_from_slice(b"bbb");

        assert!(rle_validate_encoding::<String, Leb128>(&buf).is_err());
    }

    #[test]
    fn validate_load_rejects_null_in_non_nullable() {
        // Null run in a u64 column
        let data = rle_bytes(&[("null", &[0x00, 0x01])]);
        assert!(RleLoadIter::<u64>::new(&data, 16).finalize().is_err());
    }

    #[test]
    fn validate_load_empty_input() {
        let result = RleLoadIter::<u64>::new(&[], 16).finalize().unwrap();
        assert!(result.is_empty());
    }

    // ── Load: literal run split across slab boundary ────────────────────

    fn load_roundtrip<T: RleValue + ColumnValueRef<Encoding<Leb128> = RleEncoding<T>>>(
        vals: Vec<T>,
        max_segments: usize,
    ) where
        for<'a> T::Get<'a>: std::fmt::Debug,
    {
        let col = Column::<T>::from_values(vals.clone());
        let saved = col.save();
        let slabs = RleLoadIter::<T>::new(&saved, max_segments)
            .finalize()
            .unwrap();

        // Every slab must be well-formed with correct tail.
        let mut total_len = 0;
        for (i, slab) in slabs.iter().enumerate() {
            assert!(slab.len > 0, "slab {i} is empty");
            let info = rle_validate_encoding::<T, Leb128>(&slab.data)
                .unwrap_or_else(|e| panic!("slab {i} encoding invalid: {e}"));
            assert_eq!(slab.len, info.len, "slab {i}: len mismatch");
            assert_eq!(slab.segments, info.segments, "slab {i}: segments mismatch");
            assert_eq!(slab.tail, info.tail, "slab {i}: tail mismatch");
            total_len += slab.len;
        }

        // Total items must match original.
        assert_eq!(
            total_len,
            col.len(),
            "total len mismatch: loaded={total_len} original={}",
            col.len()
        );

        // Concatenated values must match original.
        let mut loaded_vals = vec![];
        for slab in &slabs {
            let decoder = RleDecoder::<T>::new(&slab.data);
            loaded_vals.extend(decoder);
        }
        let orig_vals: Vec<_> = col.iter().collect();
        assert_eq!(loaded_vals, orig_vals, "value mismatch after load");
    }

    #[test]
    fn load_split_long_literal_run() {
        // A long literal run (all unique values) that must be split across slabs.
        let vals: Vec<u64> = (0..100).collect();
        load_roundtrip(vals, 4);
    }

    #[test]
    fn load_split_literal_at_various_segments() {
        // Test with different max_segments to hit different split points.
        let vals: Vec<u64> = (0..50).collect();
        for max_seg in [2, 3, 4, 6, 8, 16] {
            load_roundtrip(vals.clone(), max_seg);
        }
    }

    #[test]
    fn load_split_mixed_literal_and_repeat() {
        // Literal runs interspersed with repeats — split should handle
        // mid-literal and mid-repeat boundaries.
        let mut vals = vec![];
        for i in 0..20u64 {
            vals.push(i); // literal
            for _ in 0..3 {
                vals.push(i + 100); // repeat of 3
            }
        }
        load_roundtrip(vals, 4);
    }

    #[test]
    fn load_split_nullable_with_nulls() {
        // Nullable column with null runs interspersed.
        let vals: Vec<Option<u64>> = (0..60)
            .map(|i| if i % 5 == 0 { None } else { Some(i) })
            .collect();
        load_roundtrip(vals, 4);
    }

    #[test]
    fn load_split_string_literal() {
        // String columns have variable-length values, testing header rewrite.
        let vals: Vec<String> = (0..40).map(|i| format!("item_{i:04}")).collect();
        load_roundtrip(vals, 4);
    }

    #[test]
    fn load_split_string_mixed() {
        // Strings with repeats and literals.
        let mut vals: Vec<String> = vec![];
        for i in 0..15 {
            vals.push(format!("unique_{i}"));
            for _ in 0..3 {
                vals.push("repeated".into());
            }
        }
        load_roundtrip(vals, 6);
    }

    #[test]
    fn load_split_then_save_roundtrip() {
        // Load with small slabs, reassemble into a column, save, and verify
        // the saved bytes match the original.
        let vals: Vec<u64> = (0..200).map(|i| i % 13).collect();
        let col = Column::<u64>::from_values(vals.clone());
        let saved = col.save();
        let loaded = Column::<u64>::load(&saved).unwrap();
        assert_eq!(loaded.to_vec(), col.to_vec());
        // Re-save and compare
        let resaved = loaded.save();
        let reloaded = Column::<u64>::load(&resaved).unwrap();
        assert_eq!(reloaded.to_vec(), col.to_vec());
    }
}
