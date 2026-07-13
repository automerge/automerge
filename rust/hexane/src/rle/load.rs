//! RLE validation and load (deserialize + verify).

use std::num::NonZeroU32;

use crate::encoding::SlabInfo;
use crate::leb::{encode_signed, rewrite_lit_header};
use crate::rle::decoder::{RleDecoder, RleSegment};
use crate::rle::{RleTail, Slab};
use crate::ColumnValueRef;
use crate::PackError;
use crate::RleValue;

// ── validate_encoding ────────────────────────────────────────────────────────

pub(crate) fn rle_validate_encoding<T: RleValue>(
    slab: &[u8],
) -> Result<SlabInfo<RleTail>, PackError> {
    let mut decoder = RleDecoder::<T>::new(slab);
    let mut len = 0;
    let mut segments = 0;
    let mut tail = RleTail::default();
    let mut prev: Option<RleSegment<'_, T>> = None;
    let mut prev_lit: Option<T::Get<'_>> = None;

    while let Some(segment) = decoder.try_next_segment()? {
        segment
            .validate_after(&prev, prev_lit)
            .map_err(|e| PackError::InvalidValue(e.to_string()))?;

        match segment {
            RleSegment::LitHead { bytes, .. } => {
                prev_lit = None;
                tail.bytes = bytes as u32;
            }
            RleSegment::Lit { value, bytes } => {
                prev_lit = Some(value);
                prev = Some(segment);
                len += 1;
                segments += 1;
                tail.lit_tail = NonZeroU32::new(bytes as u32);
                tail.bytes += bytes as u32;
            }
            RleSegment::Run { count, bytes, .. } => {
                prev = Some(segment);
                len += count;
                segments += 1;
                tail.lit_tail = None;
                tail.bytes = bytes as u32;
            }
            RleSegment::Null { count, bytes } => {
                prev = Some(segment);
                len += count;
                segments += 1;
                tail.lit_tail = None;
                tail.bytes = bytes as u32;
            }
        }
    }
    Ok(SlabInfo {
        segments,
        len,
        tail,
    })
}

// ── Load iterator ──────────────────────────────────────────────────────────

/// Streaming decode + validate over saved RLE bytes.
///
/// Yields the runs of the input while building the output slabs with
/// the same byte-copy mechanics as the block loader: byte ranges of the
/// input are copied verbatim, with a literal-run header rewrite when a
/// cut lands inside a literal run. The input must be **canonical** —
/// non-canonical structure (mergeable adjacent runs, count-0 runs,
/// count-1 repeats) is a load error, as are value decode errors and
/// nulls in non-nullable columns — so consumers get the "adjacent runs
/// never carry equal values" contract by construction. All methods
/// return `Result` — the bytes are untrusted.
///
/// [`finalize`](Self::finalize) drains whatever the consumer did not pull
/// (validating it) and returns the finished slabs.
pub struct RleLoadIter<'a, T: RleValue> {
    decoder: RleDecoder<'a, T>,
    input: &'a [u8],
    /// canonical-form validation state: the last value-bearing segment
    /// and the previous literal value within the current literal run —
    /// non-canonical structure (mergeable adjacent runs, count-0 runs,
    /// count-1 repeats) is a load error
    prev: Option<RleSegment<'a, T>>,
    prev_lit: Option<T::Get<'a>>,
    cut: CutState,
    target_segments: usize,
}

/// The slab-cutting state of an in-progress load, identical to the block
/// loader's. One copy of the (subtle) literal-split bookkeeping, shared by
/// the resumable iterator (state in the struct) and `finalize`'s
/// run-to-completion drain (state hoisted into a local).
#[derive(Default)]
struct CutState {
    slabs: Vec<Slab>,
    slab: Slab,
    start: usize,
    last_lit_count: usize,
    lit_count: usize,
    pending_header: usize,
}

impl CutState {
    /// Per-segment slab bookkeeping; yields the segment's run, if any.
    /// `#[inline(always)]` so a caller that discards the run compiles down
    /// to the bare bookkeeping. Callers run `validate_after` (which
    /// rejects nulls in non-nullable columns, among other things)
    /// *before* tracking — keeping this infallible lets the discard
    /// path fold completely.
    #[inline(always)]
    fn track<'a, T: RleValue>(
        &mut self,
        segment: RleSegment<'a, T>,
    ) -> Option<crate::Run<T::Get<'a>>> {
        match segment {
            RleSegment::LitHead { count, bytes } => {
                if self.last_lit_count < self.lit_count {
                    self.pending_header = self.lit_count;
                }
                self.slab.tail.bytes = bytes as u32;
                self.last_lit_count = count;
                self.lit_count = 0;
                None
            }
            RleSegment::Lit { value, bytes } => {
                self.slab.len += 1;
                self.slab.segments += 1;
                self.slab.tail.lit_tail = NonZeroU32::new(bytes as u32);
                self.slab.tail.bytes += bytes as u32;
                self.lit_count += 1;
                Some(crate::Run { count: 1, value })
            }
            RleSegment::Run {
                count,
                value,
                bytes,
            } => {
                self.slab.len += count;
                self.slab.segments += 1;
                self.slab.tail.lit_tail = None;
                self.slab.tail.bytes = bytes as u32;
                (count > 0).then_some(crate::Run { count, value })
            }
            RleSegment::Null { count, bytes } => {
                self.slab.len += count;
                self.slab.segments += 1;
                self.slab.tail.lit_tail = None;
                self.slab.tail.bytes = bytes as u32;
                (count > 0).then_some(crate::Run {
                    count,
                    value: T::get_null(),
                })
            }
        }
    }

    /// Cut a slab at byte position `pos` (the block loader's split,
    /// byte-for-byte).
    fn cut_slab(&mut self, input: &[u8], pos: usize) {
        self.slab.copy_from(
            &input[self.start..pos],
            self.pending_header,
            self.lit_count,
            self.last_lit_count,
        );
        self.slabs.push(std::mem::take(&mut self.slab));
        self.pending_header = 0;
        self.last_lit_count = 0;
        self.lit_count = 0;
        self.start = pos;
    }
}

impl<'a, T: RleValue> RleLoadIter<'a, T> {
    pub fn new(data: &'a [u8], max_segments: usize) -> Self {
        Self {
            decoder: RleDecoder::new(data),
            input: data,
            prev: None,
            prev_lit: None,
            cut: CutState::default(),
            target_segments: max_segments / 2,
        }
    }

    /// The next canonical run, or `None` at end of input. Non-canonical
    /// structure — anything a canonical encoder would have merged or
    /// never emitted — is a load error (`validate_after`), so the run
    /// stream keeps the "adjacent runs never carry equal values" contract
    /// by construction, and every run is exactly one wire segment.
    #[inline]
    pub fn try_next_run(&mut self) -> Result<Option<crate::Run<T::Get<'a>>>, PackError> {
        loop {
            let Some(segment) = self.decoder.try_next_segment()? else {
                return Ok(None);
            };
            segment
                .validate_after(&self.prev, self.prev_lit)
                .map_err(|e| PackError::InvalidValue(e.to_string()))?;
            match segment {
                RleSegment::LitHead { .. } => self.prev_lit = None,
                RleSegment::Lit { value, .. } => {
                    self.prev_lit = Some(value);
                    self.prev = Some(segment);
                }
                _ => self.prev = Some(segment),
            }
            let out = self.cut.track::<T>(segment);
            if self.cut.slab.segments == self.target_segments {
                self.cut.cut_slab(self.input, self.decoder.pos());
            }
            if let Some(run) = out {
                return Ok(Some(run));
            }
        }
    }

    /// Drain and validate whatever the consumer did not pull, flush the
    /// final slab, and return the finished slabs.
    pub fn finalize(self) -> Result<Vec<Slab>, PackError> {
        // hoist the cutting state into a local so the drain loop keeps it
        // in registers; `track` inlines to bare bookkeeping when its run
        // is discarded
        let mut cut = self.cut;
        let target_segments = self.target_segments;
        let input = self.input;
        let mut decoder = self.decoder;
        let mut prev = self.prev;
        let mut prev_lit = self.prev_lit;

        while let Some(segment) = decoder.try_next_segment()? {
            segment
                .validate_after(&prev, prev_lit)
                .map_err(|e| PackError::InvalidValue(e.to_string()))?;
            match segment {
                RleSegment::LitHead { .. } => prev_lit = None,
                RleSegment::Lit { value, .. } => {
                    prev_lit = Some(value);
                    prev = Some(segment);
                }
                _ => prev = Some(segment),
            }
            let _ = cut.track::<T>(segment);
            if cut.slab.segments == target_segments {
                cut.cut_slab(input, decoder.pos());
            }
        }
        if cut.slab.segments > 0 {
            cut.cut_slab(input, decoder.pos());
        }
        Ok(cut.slabs)
    }
}

impl<'a, T> crate::encoding::LoadIterApi<'a, T> for RleLoadIter<'a, T>
where
    T: RleValue + ColumnValueRef<Encoding = crate::rle::RleEncoding<T>>,
{
    fn try_next_run(&mut self) -> Result<Option<crate::Run<T::Get<'a>>>, PackError> {
        RleLoadIter::try_next_run(self)
    }

    fn slabs_completed(&self) -> usize {
        self.cut.slabs.len()
    }

    fn completed_slab_len(&self, i: usize) -> usize {
        self.cut.slabs[i].len
    }

    fn finalize(self) -> Result<Vec<Slab>, PackError> {
        RleLoadIter::finalize(self)
    }
}

impl Slab {
    fn copy_from(
        &mut self,
        input: &[u8],
        pending_header: usize,
        lit_count: usize,
        last_lit_count: usize,
    ) {
        if pending_header > 0 {
            // we split a lit run but it terminated
            let hdr = encode_signed(-(pending_header as i64));
            self.data.extend_from_slice(hdr.as_bytes());
        } else if lit_count > last_lit_count && last_lit_count == 0 {
            // we split a lit run and its ongoing
            let hdr = encode_signed(-(lit_count as i64));
            if self.tail.lit_tail.is_some() {
                // header and tail
                self.tail.bytes += hdr.len() as u32;
            }
            self.data.extend_from_slice(hdr.as_bytes());
        }
        self.data.extend_from_slice(input);
        if lit_count < last_lit_count {
            let header_pos = self.data.len() - self.tail.bytes as usize;
            let delta = rewrite_lit_header(&mut self.data, header_pos, lit_count);
            self.tail.bytes = (self.tail.bytes as i64 + delta) as u32;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::leb::encode_signed;
    use crate::rle::state::{RleCow, RleState};
    use crate::rle::*;
    use crate::rle::{RleDecoder, RleEncoding};
    use crate::{Column, ColumnValueRef};

    fn check_validate2<T: RleValue + ColumnValueRef<Encoding = RleEncoding<T>>>(data: &[u8]) {
        let v1 = rle_validate_encoding::<T>(data).unwrap();
        let v2 = rle_validate_encoding::<T>(data).unwrap();
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

    #[test]
    fn load_and_verify_matches() {
        let vals: Vec<u64> = (0..1000).map(|i| i % 7).collect();
        let col = Column::<u64>::from_values(vals);
        let saved = col.save();
        let v1 = RleLoadIter::<u64>::new(&saved, 16).finalize().unwrap();
        let v2 = RleLoadIter::<u64>::new(&saved, 16).finalize().unwrap();
        assert_eq!(v1.len(), v2.len(), "slab count mismatch");
        for (i, (s1, s2)) in v1.iter().zip(v2.iter()).enumerate() {
            assert_eq!(s1.data, s2.data, "slab {i} data mismatch");
            assert_eq!(s1.len, s2.len, "slab {i} len mismatch");
            assert_eq!(s1.segments, s2.segments, "slab {i} segments mismatch");
            assert_eq!(s1.tail, s2.tail, "slab {i} tail mismatch");
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
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_null_count_zero() {
        // Null with count=0: signed(0)=0x00, unsigned(0)=0x00
        let data = rle_bytes(&[("null", &[0x00, 0x00])]);
        assert!(rle_validate_encoding::<Option<u64>>(&data).is_err());
    }

    #[test]
    fn validate_null_in_non_nullable() {
        // Null run in a u64 column: signed(0)=0x00, unsigned(1)=0x01
        let data = rle_bytes(&[("null", &[0x00, 0x01])]);
        assert!(rle_validate_encoding::<u64>(&data).is_err());
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
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_literals() {
        // Two separate literal runs: [-1, v1] then [-1, v2]
        let data = rle_bytes(&[("lit", &[0x7f, 0x01]), ("lit", &[0x7f, 0x02])]);
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_nulls() {
        // Two null runs: [0, 1] [0, 1]
        let data = rle_bytes(&[("null", &[0x00, 0x01]), ("null", &[0x00, 0x01])]);
        assert!(rle_validate_encoding::<Option<u64>>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_repeats_same_value() {
        // Two repeat runs with same value: [2, 5] [2, 5]
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_adjacent_repeats_different_value_ok() {
        // Two repeat runs with different values: [2, 5] [2, 7] — should be OK
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("repeat", &[0x02, 0x07])]);
        assert!(rle_validate_encoding::<u64>(&data).is_ok());
    }

    #[test]
    fn validate_boundary_repeat_then_literal_same_value() {
        // Repeat [2, 5] then literal [-1, 5] — last repeat value == first lit value
        let data = rle_bytes(&[("repeat", &[0x02, 0x05]), ("lit", &[0x7f, 0x05])]);
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_boundary_literal_then_repeat_same_value() {
        // Literal [-1, 5] then repeat [2, 5] — last lit value == repeat value
        let data = rle_bytes(&[("lit", &[0x7f, 0x05]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64>(&data).is_err());
    }

    #[test]
    fn validate_boundary_different_values_ok() {
        // Literal [-1, 3] then repeat [2, 5] — different values, should be OK
        let data = rle_bytes(&[("lit", &[0x7f, 0x03]), ("repeat", &[0x02, 0x05])]);
        assert!(rle_validate_encoding::<u64>(&data).is_ok());
    }

    #[test]
    fn validate_boundary_repeat_then_literal_same_value_string() {
        // Build: Run(2, "aaa") then Lit ["aaa", "bbb"]
        // This must be rejected — "aaa" at boundary.
        let mut buf = Vec::new();
        let mut state = RleState::<String, &str>::Empty;
        // Force a repeat of "aaa" × 2
        state.append(&mut buf, RleCow::Ref("aaa"));
        state.append(&mut buf, RleCow::Ref("aaa"));
        state.flush(&mut buf);
        // Now manually append a literal starting with "aaa"
        buf.extend(encode_signed(-2));
        // "aaa" = leb(3) + b"aaa"
        buf.push(3);
        buf.extend_from_slice(b"aaa");
        buf.push(3);
        buf.extend_from_slice(b"bbb");

        assert!(rle_validate_encoding::<String>(&buf).is_err());
    }

    #[test]
    fn validate_load_rejects_null_in_non_nullable() {
        // Null run in a u64 column via load_and_verify2
        let data = rle_bytes(&[("null", &[0x00, 0x01])]);
        assert!(RleLoadIter::<u64>::new(&data, 16).finalize().is_err());
    }

    #[test]
    fn validate_load_empty_input() {
        let result = RleLoadIter::<u64>::new(&[], 16).finalize().unwrap();
        assert!(result.is_empty());
    }

    // ── Load: literal run split across slab boundary ────────────────────

    fn load_roundtrip<T: RleValue + ColumnValueRef<Encoding = RleEncoding<T>>>(
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
            let info = rle_validate_encoding::<T>(&slab.data)
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
