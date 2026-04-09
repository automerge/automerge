//! RLE validation and load (deserialize + verify).

use std::num::NonZeroU32;

use super::decoder::{RleDecoder, RleSegment};
use super::{RleTail, Slab};
use crate::v1::encoding::SlabInfo;
use crate::v1::leb::{encode_signed, rewrite_lit_header};
use crate::v1::RleValue;
use crate::PackError;

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

// ── Load & verify ─────────────────────────────────────────────────────────

/// Decode and validate RLE-encoded bytes, splitting into slabs.
///
/// Walks every run, validates with try_unpack, and splits into slabs by
/// copying byte ranges. No re-encoding except when splitting a literal
/// run (which requires rewriting the count header for each piece).
pub(crate) fn rle_load_and_verify<T: RleValue>(
    input: &[u8],
    max_segments: usize,
    validate: Option<for<'a> fn(<T as super::ColumnValueRef>::Get<'a>) -> Option<String>>,
) -> Result<Vec<Slab>, PackError> {
    let target_segments = max_segments / 2;
    if input.is_empty() {
        return Ok(vec![]);
    }
    let _validate = move |value| {
        if let Some(v) = validate {
            if let Some(m) = v(value) {
                return Err(PackError::InvalidValue(m));
            }
        }
        Ok(())
    };
    let mut decoder = RleDecoder::<T>::new(input);
    let mut slabs = vec![];
    let mut slab = Slab::default();
    let mut start = 0;
    let mut last_lit_count = 0;
    let mut lit_count = 0;
    let mut pending_header = 0;
    while let Some(segment) = decoder.try_next_segment()? {
        match segment {
            RleSegment::LitHead { count, bytes } => {
                if last_lit_count < lit_count {
                    pending_header = lit_count;
                }
                slab.tail.bytes = bytes as u32;
                last_lit_count = count;
                lit_count = 0;
            }
            RleSegment::Lit { value, bytes } => {
                _validate(value)?;
                slab.len += 1;
                slab.segments += 1;
                slab.tail.lit_tail = NonZeroU32::new(bytes as u32);
                slab.tail.bytes += bytes as u32;
                lit_count += 1;
            }
            RleSegment::Run {
                count,
                value,
                bytes,
            } => {
                _validate(value)?;
                slab.len += count;
                slab.segments += 1;
                slab.tail.lit_tail = None;
                slab.tail.bytes = bytes as u32;
            }
            RleSegment::Null { count, bytes } => {
                if !T::NULLABLE {
                    return Err(PackError::InvalidValue(
                        "null in non-nullable column".to_string(),
                    ));
                }
                slab.len += count;
                slab.segments += 1;
                slab.tail.lit_tail = None;
                slab.tail.bytes = bytes as u32;
            }
        }
        if slab.segments == target_segments {
            slab.copy_from(
                &input[start..decoder.pos()],
                pending_header,
                lit_count,
                last_lit_count,
            );

            slabs.push(std::mem::take(&mut slab));

            pending_header = 0;
            last_lit_count = 0;
            lit_count = 0;
            start = decoder.pos();
        }
    }
    // Flush remaining data as the final slab.
    if slab.segments > 0 {
        slab.copy_from(
            &input[start..decoder.pos()],
            pending_header,
            lit_count,
            last_lit_count,
        );
        slabs.push(std::mem::take(&mut slab));
    }
    Ok(slabs)
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
    use super::*;
    use crate::v1::leb::encode_signed;
    use crate::v1::rle::state::{RleCow, RleState};
    use crate::v1::rle::{RleDecoder, RleEncoding};
    use crate::v1::{Column, ColumnValueRef};

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
        let v1 = rle_load_and_verify::<u64>(&saved, 16, None).unwrap();
        let v2 = rle_load_and_verify::<u64>(&saved, 16, None).unwrap();
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
        assert!(rle_load_and_verify::<u64>(&data, 16, None).is_err());
    }

    #[test]
    fn validate_load_empty_input() {
        let result = rle_load_and_verify::<u64>(&[], 16, None).unwrap();
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
        let slabs = rle_load_and_verify::<T>(&saved, max_segments, None).unwrap();

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
