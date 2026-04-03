//! RLE validation and load (deserialize + verify).

use std::num::NonZeroU32;

use super::{make_rle_slab, RleTail, Slab};
use crate::v1::encoding::SlabInfo;
use crate::v1::leb::{encode_signed, read_signed, read_unsigned, Leb128Buf};
use crate::v1::RleValue;
use crate::PackError;

// ── validate_encoding ────────────────────────────────────────────────────────

/// Validate that an RLE slab is in canonical form.
///
/// Invariants checked:
/// 1. No adjacent literal runs (should be merged into one)
/// 2. No adjacent repeat runs with the same value (should be merged)
/// 3. No adjacent null runs (should be merged)
/// 4. Repeat count >= 2 (count 1 belongs in a literal)
/// 5. Null count >= 1
/// 6. Literal count >= 1
/// 7. First value of a literal differs from previous run's last value
/// 8. Last value of a literal differs from next run's first value
/// 9. No two consecutive equal values within a literal (would form a repeat)
pub(crate) fn rle_validate_encoding<T: RleValue>(slab: &[u8]) -> Result<SlabInfo<RleTail>, String> {
    if slab.is_empty() {
        return Ok(SlabInfo {
            segments: 0,
            len: 0,
            tail: RleTail::default(),
        });
    }

    // Parse all runs and their value bytes for comparison.
    enum Run {
        Repeat { count: usize, value: Vec<u8> },
        Literal { values: Vec<Vec<u8>> },
        Null { count: usize },
    }

    let mut runs: Vec<Run> = vec![];
    let mut pos = 0;
    let mut last_start = 0;
    let mut lit_tail: Option<NonZeroU32> = None;
    while pos < slab.len() {
        last_start = pos;
        let (cb, raw) = read_signed(&slab[pos..])
            .ok_or_else(|| format!("truncated count header at byte {pos}"))?;
        match raw {
            n if n > 0 => {
                let count = n as usize;
                let vs = pos + cb;
                let vl =
                    T::value_len(&slab[vs..]).ok_or_else(|| format!("bad value at byte {vs}"))?;
                runs.push(Run::Repeat {
                    count,
                    value: slab[vs..vs + vl].to_vec(),
                });
                lit_tail = None;
                pos = vs + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut values = Vec::with_capacity(total);
                let mut sb = pos + cb;
                for j in 0..total {
                    let vl = T::value_len(&slab[sb..])
                        .ok_or_else(|| format!("bad literal value {j} at byte {sb}"))?;
                    values.push(slab[sb..sb + vl].to_vec());
                    sb += vl;
                    lit_tail = NonZeroU32::new(vl as u32);
                }
                runs.push(Run::Literal { values });
                pos = sb;
            }
            _ => {
                let (ncb, nc) = read_unsigned(&slab[pos + cb..])
                    .ok_or_else(|| format!("truncated null count at byte {}", pos + cb))?;
                runs.push(Run::Null { count: nc as usize });
                lit_tail = None;
                pos += cb + ncb;
            }
        }
    }

    // Now validate the invariants across adjacent runs.
    for (i, run) in runs.iter().enumerate() {
        match run {
            Run::Repeat { count, .. } => {
                if *count < 2 {
                    return Err(format!("run {i}: repeat with count {count} (must be >= 2)"));
                }
            }
            Run::Null { count } => {
                if *count < 1 {
                    return Err(format!("run {i}: null with count 0"));
                }
            }
            Run::Literal { values } => {
                if values.is_empty() {
                    return Err(format!("run {i}: empty literal"));
                }
                // Check no two consecutive equal values within the literal.
                for j in 1..values.len() {
                    if values[j] == values[j - 1] {
                        return Err(format!(
                            "run {i}: literal has consecutive equal values at positions {}/{}",
                            j - 1,
                            j
                        ));
                    }
                }
            }
        }

        if i == 0 {
            continue;
        }

        let prev = &runs[i - 1];

        // No adjacent literals.
        if matches!(prev, Run::Literal { .. }) && matches!(run, Run::Literal { .. }) {
            return Err(format!(
                "runs {}/{i}: adjacent literal runs (should be merged)",
                i - 1
            ));
        }

        // No adjacent nulls.
        if matches!(prev, Run::Null { .. }) && matches!(run, Run::Null { .. }) {
            return Err(format!(
                "runs {}/{i}: adjacent null runs (should be merged)",
                i - 1
            ));
        }

        // No adjacent repeats with same value.
        if let (Run::Repeat { value: va, .. }, Run::Repeat { value: vb, .. }) = (prev, run) {
            if va == vb {
                return Err(format!(
                    "runs {}/{i}: adjacent repeat runs with same value",
                    i - 1
                ));
            }
        }

        // Boundary value checks between prev and current.
        let prev_last_value: Option<&[u8]> = match prev {
            Run::Repeat { value, .. } => Some(value),
            Run::Literal { values } => values.last().map(|v| v.as_slice()),
            Run::Null { .. } => None,
        };
        let cur_first_value: Option<&[u8]> = match run {
            Run::Repeat { value, .. } => Some(value),
            Run::Literal { values } => values.first().map(|v| v.as_slice()),
            Run::Null { .. } => None,
        };

        if let (Some(pv), Some(cv)) = (prev_last_value, cur_first_value) {
            if pv == cv {
                let prev_kind = match prev {
                    Run::Repeat { .. } => "repeat",
                    Run::Literal { .. } => "literal",
                    Run::Null { .. } => unreachable!(),
                };
                let cur_kind = match run {
                    Run::Repeat { .. } => "repeat",
                    Run::Literal { .. } => "literal",
                    Run::Null { .. } => unreachable!(),
                };
                return Err(format!(
                    "runs {}/{i}: {prev_kind} ends with same value as {cur_kind} starts with \
                     (should be merged into a repeat or absorbed)",
                    i - 1
                ));
            }
        }
    }

    let mut segments = 0;
    let mut len = 0;
    for run in &runs {
        match run {
            Run::Repeat { count, .. } => {
                segments += 1;
                len += count;
            }
            Run::Literal { values } => {
                segments += values.len();
                len += values.len();
            }
            Run::Null { count } => {
                segments += 1;
                len += count;
            }
        }
    }
    let tail = RleTail {
        bytes: (slab.len() - last_start) as u32,
        lit_tail,
    };
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
    data: &[u8],
    max_segments: usize,
    validate: Option<for<'a> fn(<T as super::ColumnValueRef>::Get<'a>) -> Option<String>>,
) -> Result<Vec<Slab>, PackError> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    let mut slabs: Vec<Slab> = Vec::new();
    let mut slab_start: usize = 0;
    let mut slab_items: usize = 0;
    let mut slab_segs: usize = 0;
    let mut pending_hdr: Option<Leb128Buf> = None;

    /// Flush accumulated bytes into a slab.
    #[inline]
    fn flush<T: RleValue>(
        slabs: &mut Vec<Slab>,
        data: &[u8],
        slab_start: &mut usize,
        slab_items: &mut usize,
        slab_segs: &mut usize,
        pending_hdr: &mut Option<Leb128Buf>,
        end: usize,
    ) {
        if *slab_items == 0 {
            return;
        }
        let d = if let Some(hdr) = pending_hdr.take() {
            let mut v = Vec::with_capacity(hdr.len as usize + (end - *slab_start));
            v.extend_from_slice(hdr.as_bytes());
            v.extend_from_slice(&data[*slab_start..end]);
            v
        } else {
            data[*slab_start..end].to_vec()
        };
        slabs.push(make_rle_slab::<T>(d, *slab_items, *slab_segs));
        *slab_start = end;
        *slab_items = 0;
        *slab_segs = 0;
    }

    let mut pos = 0;
    while pos < data.len() {
        let run_start = pos;
        let (cb, raw) = read_signed(&data[pos..]).ok_or(PackError::BadFormat)?;

        if raw > 0 {
            // Repeat run.
            let count = raw as usize;
            let vs = pos + cb;
            let (vlen, value) = T::try_unpack(&data[vs..])?;
            if let Some(v) = validate {
                if let Some(m) = v(value) {
                    return Err(PackError::InvalidValue(m));
                }
            }
            let run_end = vs + vlen;
            if slab_segs > 0 && slab_segs + 1 > max_segments {
                flush::<T>(
                    &mut slabs,
                    data,
                    &mut slab_start,
                    &mut slab_items,
                    &mut slab_segs,
                    &mut pending_hdr,
                    run_start,
                );
            }
            slab_items += count;
            slab_segs += 1;
            pos = run_end;
        } else if raw < 0 {
            // Literal run.
            let total = (-raw) as usize;
            let scan = pos + cb;

            // Validate all values and compute byte offsets in a single pass.
            let mut offsets = Vec::with_capacity(total + 1);
            {
                let mut check = scan;
                for _ in 0..total {
                    if check >= data.len() {
                        return Err(PackError::BadFormat);
                    }
                    offsets.push(check);
                    let (vlen, value) = T::try_unpack(&data[check..])?;
                    if let Some(v) = validate {
                        if let Some(m) = v(value) {
                            return Err(PackError::InvalidValue(m));
                        }
                    }
                    check += vlen;
                }
                offsets.push(check);
            }
            let run_end = *offsets.last().unwrap();

            // How many fit in current slab?
            let room = if slab_segs > 0 {
                max_segments.saturating_sub(slab_segs)
            } else {
                max_segments
            };

            if total <= room {
                // Whole literal fits — no splitting needed.
                slab_items += total;
                slab_segs += total;
                pos = run_end;
            } else {
                let mut consumed = 0;

                // Fill remaining room in current slab.
                if slab_segs > 0 && room > 0 {
                    let chunk_end = offsets[consumed + room];
                    let chunk_hdr = encode_signed(-(room as i64));
                    let mut d = if let Some(hdr) = pending_hdr.take() {
                        let mut v = Vec::with_capacity(
                            hdr.len as usize
                                + (run_start - slab_start)
                                + chunk_hdr.len as usize
                                + (chunk_end - offsets[0]),
                        );
                        v.extend_from_slice(hdr.as_bytes());
                        v.extend_from_slice(&data[slab_start..run_start]);
                        v
                    } else {
                        data[slab_start..run_start].to_vec()
                    };
                    d.extend_from_slice(chunk_hdr.as_bytes());
                    d.extend_from_slice(&data[offsets[consumed]..chunk_end]);
                    slabs.push(make_rle_slab::<T>(d, slab_items + room, slab_segs + room));
                    consumed += room;
                } else if slab_segs > 0 {
                    flush::<T>(
                        &mut slabs,
                        data,
                        &mut slab_start,
                        &mut slab_items,
                        &mut slab_segs,
                        &mut pending_hdr,
                        run_start,
                    );
                }

                // Full chunks.
                while total - consumed >= max_segments {
                    let hdr = encode_signed(-(max_segments as i64));
                    let cs = offsets[consumed];
                    let ce = offsets[consumed + max_segments];
                    let mut d = Vec::with_capacity(hdr.len as usize + (ce - cs));
                    d.extend_from_slice(hdr.as_bytes());
                    d.extend_from_slice(&data[cs..ce]);
                    slabs.push(make_rle_slab::<T>(d, max_segments, max_segments));
                    consumed += max_segments;
                }

                // Remainder.
                let rem = total - consumed;
                if rem > 0 {
                    pending_hdr = Some(encode_signed(-(rem as i64)));
                    slab_start = offsets[consumed];
                    slab_items = rem;
                    slab_segs = rem;
                } else {
                    slab_start = run_end;
                    slab_items = 0;
                    slab_segs = 0;
                }
                pos = run_end;
            }
        } else {
            // Null run.
            if !T::NULLABLE {
                return Err(PackError::InvalidValue(
                    "null run in non-nullable column".into(),
                ));
            }
            if let Some(v) = validate {
                if let Some(m) = v(T::get_null()) {
                    return Err(PackError::InvalidValue(m));
                }
            }
            let nd = &data[pos + cb..];
            if nd.is_empty() {
                return Err(PackError::BadFormat);
            }
            let (ncb, nc) = read_unsigned(nd).ok_or(PackError::BadFormat)?;
            if nc == 0 {
                return Err(PackError::BadFormat);
            }
            let run_end = pos + cb + ncb;
            if slab_segs > 0 && slab_segs + 1 > max_segments {
                flush::<T>(
                    &mut slabs,
                    data,
                    &mut slab_start,
                    &mut slab_items,
                    &mut slab_segs,
                    &mut pending_hdr,
                    run_start,
                );
            }
            slab_items += nc as usize;
            slab_segs += 1;
            pos = run_end;
        }
    }

    flush::<T>(
        &mut slabs,
        data,
        &mut slab_start,
        &mut slab_items,
        &mut slab_segs,
        &mut pending_hdr,
        pos,
    );
    Ok(slabs)
}
// ── count_segments ───────────────────────────────────────────────────────────

/// Count segments in an RLE slab. A repeat run = 1 segment, a null run = 1
/// segment, a literal of N = N segments.
#[allow(dead_code)]
fn rle_count_segments<T: RleValue>(slab: &[u8]) -> usize {
    let mut byte_pos = 0;
    let mut segments = 0;

    while byte_pos < slab.len() {
        let (count_bytes, count_raw) = match read_signed(&slab[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                // Repeat run: 1 segment.
                segments += 1;
                let value_start = byte_pos + count_bytes;
                let value_len = match T::value_len(&slab[value_start..]) {
                    Some(v) => v,
                    None => break,
                };
                byte_pos = value_start + value_len;
            }
            n if n < 0 => {
                // Literal run of N: N segments.
                let total = (-n) as usize;
                segments += total;
                let mut scan_byte = byte_pos + count_bytes;
                for _ in 0..total {
                    let vlen = match T::value_len(&slab[scan_byte..]) {
                        Some(v) => v,
                        None => return segments,
                    };
                    scan_byte += vlen;
                }
                byte_pos = scan_byte;
            }
            _ => {
                // Null run: 1 segment.
                segments += 1;
                let (ncb, _) = match read_unsigned(&slab[byte_pos + count_bytes..]) {
                    Some(v) => v,
                    None => break,
                };
                byte_pos += count_bytes + ncb;
            }
        }
    }

    segments
}
