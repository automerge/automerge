//! RLE splice — in-place insert/delete/replace within a slab.

use crate::column::splice_bytes;
use std::ops::Range;

use crate::encoding::RunDecoder;

use crate::rle::state::{FlushState, RewriteHeader, RleCow, RleState, WPos};
use crate::rle::{RleDecoder, RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};

#[cfg(debug_assertions)]
use crate::rle::validate_rle_slab;

// ── RLE fast splice ─────────────────────────────────────────────────────────

///// Postfix: what comes after the deleted range in the same/adjacent run(s).
/// `segments` = segment count from outer.end to the end of the slab.
#[derive(Debug)]
pub(crate) enum Postfix<'a, T: RleValue> {
    /// Repeat or null run with count ≥ 1. No lit boundary concern.
    Run {
        count: usize,
        value: T::Get<'a>,
        segments: usize,
    },
    /// Literal item with `lit` more literal items following in the slab.
    /// Use flush_with_lit(lit) to write a header that covers them.
    Lit {
        value: T::Get<'a>,
        lit: usize,
        segments: usize,
    },
    /// Split repeat leaving 1 item, followed immediately by a literal run.
    /// Feed lone + value into state, then flush_with_lit(lit).
    LonePlusLit {
        lone: T::Get<'a>,
        value: T::Get<'a>,
        lit: usize,
        segments: usize,
    },
}

impl<T: RleValue> Postfix<'_, T> {
    fn pending(&self) -> usize {
        match self {
            Self::Run { .. } => 1,
            Self::Lit { .. } => 1,
            Self::LonePlusLit { .. } => 2,
        }
    }
    fn segments(&self) -> usize {
        match self {
            Self::Run { segments, .. } => *segments,
            Self::Lit { segments, .. } => *segments,
            Self::LonePlusLit { segments, .. } => *segments,
        }
    }
}

#[derive(Debug)]
struct Prefix<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> {
    state: RleState<'a, T, V, C>,
    segments: usize,
    bytes: usize,
}

impl<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> Prefix<'a, T, V, C> {
    fn new() -> Self {
        Prefix {
            state: RleState::empty(),
            segments: 0,
            bytes: 0,
        }
    }
}

#[derive(Debug)]
struct RlePartition<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> {
    outer: Range<usize>,
    prefix: Prefix<'a, T, V, C>,
    postfix: Option<Postfix<'a, T>>,
    /// complete runs behind `range.start` — see [`PartitionAt::read_segments`]
    read_segments: usize,
    /// a reader standing at `range.start`
    read_state: crate::rle::RleDecoderState,
}

fn find_partition<'a, T: RleValue, V: AsColumnRef<T>, C: Codec>(
    slab: &'a Slab,
    range: Range<usize>,
) -> RlePartition<'a, T, V, C> {
    find_partition_inner(slab, range, true)
}

/// `want_postfix` builds the tail description, which costs a run decode
/// and sometimes a peek past it. A splice needs it; a cursor opening a
/// rebuild does not — it describes the tail from wherever it stops
/// reading, which is not where the rebuild opened.
fn find_partition_inner<'a, T: RleValue, V: AsColumnRef<T>, C: Codec>(
    slab: &'a Slab,
    range: Range<usize>,
    want_postfix: bool,
) -> RlePartition<'a, T, V, C> {
    let mut decoder = RleDecoder::<T, C>::new(&slab.data);
    let base = 0;
    let mut byte_before = base + decoder.byte_pos;
    let mut read_segments = 0usize;
    let mut read_state = crate::rle::RleDecoderState::start();
    // the reader as of the run boundary in hand, kept only until the cut
    // is found; field copies, no decode
    let mut boundary = decoder.suspend();
    let mut item_pos: usize = 0;
    let mut segments: usize = 0;

    let mut outer = 0..slab.data.len();
    let mut prefix = Prefix::new();
    let mut prefix_done = range.start == 0;
    let mut postfix: Option<Postfix<'a, T>> = None;

    // Literal run tracking.
    let mut header_pos: usize = 0;
    let mut lit_start_item: usize = 0;
    let mut lit_segments_before: usize = 0; // segments of complete runs before lit run

    let mut was_lit = false;

    while let Some(run) = decoder.next_run() {
        let is_lit = decoder.is_literal() && run.count == 1;
        let is_null = T::is_null(run.value);
        let new_run = is_lit && !was_lit;

        if new_run {
            header_pos = byte_before;
            lit_start_item = item_pos;
            lit_segments_before = segments;
        }
        let run_end_item = item_pos + run.count;

        // ── Prefix ──────────────────────────────────────────────────────
        if !prefix_done && range.start <= run_end_item {
            let k = range.start - item_pos;
            // A cut landing on a run's end is *past* that run for a
            // reader, though the writer's region still opens at its
            // header — which is why the two counts differ by one there.
            read_segments = segments + usize::from(k == run.count);
            // the reader at the cut: the boundary before this run, plus
            // the k items of it that are behind the cut. O(1) — the same
            // run the writer's state below describes.
            let mut d = RleDecoder::<T, C>::resume(&slab.data, &boundary);
            d.advance_by(k);
            read_state = d.suspend();
            outer.start = byte_before;
            prefix.segments = segments;
            prefix.bytes = byte_before;
            prefix_done = true;

            if is_lit {
                let count = item_pos - lit_start_item;
                prefix.state = RleState::lit(count, RleCow::Ref(run.value), header_pos);
            } else if is_null {
                prefix.state = RleState::Null(k);
            } else if k == 1 && !is_lit && was_lit {
                let count = segments - lit_segments_before;
                prefix.state = RleState::lit(count, RleCow::Ref(run.value), header_pos);
            } else {
                prefix.state = RleState::make_run(k, RleCow::Ref(run.value));
            }
        }

        // ── Postfix ─────────────────────────────────────────────────────
        if prefix_done && range.end < run_end_item {
            if !want_postfix {
                break;
            }
            let (p, end) = postfix_from_run(
                slab,
                &mut decoder,
                base,
                run,
                is_lit,
                run_end_item - range.end,
                segments,
            );
            outer.end = end;
            postfix = Some(p);
            break;
        }

        segments += 1;
        item_pos = run_end_item;
        byte_before = base + decoder.byte_pos;
        if !prefix_done {
            // only the run the cut lands in needs a boundary behind it;
            // the walk carries on past it to describe the tail
            boundary = decoder.suspend();
        }
        was_lit = is_lit;
    }

    RlePartition {
        outer,
        prefix,
        postfix,
        read_segments,
        read_state,
    }
}

/// Build the postfix at a split point.
///
/// `run` is the run the split falls inside (or that begins at it), `count`
/// how many of its items lie after the split, `segments_before` the slab's
/// segment count before `run`, and `dec` a decoder positioned just past
/// `run` — the lone-plus-literal case consumes one more run from it.
/// Returns the postfix and the byte offset where the slab's carried-through
/// bytes resume.
///
/// Both callers stop reading somewhere in a slab and need to describe the
/// tail: [`find_partition`] at the end of a splice range, and
/// [`RleEdit`](crate::rle::edit::RleEdit)'s walk at wherever its cursor
/// stands. Keeping it in one place is what lets the cursor close without
/// re-reading anything: it captures the postfix as it passes, where a
/// second walk would have to decode a whole literal group again to reach
/// the same point.
#[allow(clippy::too_many_arguments)]
pub(crate) fn postfix_from_run<'a, T: RleValue, C: Codec>(
    slab: &'a Slab,
    dec: &mut RleDecoder<'a, T, C>,
    base: usize,
    run: crate::Run<T::Get<'a>>,
    is_lit: bool,
    count: usize,
    segments_before: usize,
) -> (Postfix<'a, T>, usize) {
    let value = run.value;
    let is_null = T::is_null(value);
    let consumed = segments_before + 1; // runs before this one, plus it
    let mut byte_end = base + dec.byte_pos;
    let p = if is_lit {
        Postfix::Lit {
            value,
            lit: dec.remaining,
            segments: slab.segments - consumed,
        }
    } else {
        // A repeat run cannot be left holding a single item — that is not
        // canonical — so when the split leaves one behind and a literal
        // group follows, the two are flushed together.
        let lone_plus_lit = if count == 1 && !is_null {
            dec.next_run().and_then(|post_run| {
                (dec.is_literal() && post_run.count == 1).then(|| {
                    byte_end = base + dec.byte_pos; // past the first lit value
                    Postfix::LonePlusLit {
                        lone: value,
                        value: post_run.value,
                        lit: dec.remaining,
                        segments: slab.segments - consumed - 1, // the peeked lit value
                    }
                })
            })
        } else {
            None
        };
        lone_plus_lit.unwrap_or(Postfix::Run {
            count,
            value,
            segments: slab.segments - consumed,
        })
    };
    (p, byte_end)
}

/// [`find_partition`] at a single item position, repackaged for
/// [`RleEdit`](crate::rle::edit::RleEdit): the encoder state and byte
/// offset *at* `at`, plus the postfix describing everything after it.
pub(crate) struct PartitionAt<'a, T: RleValue, C: Codec> {
    pub(crate) state: RleState<'a, T, T, C>,
    /// segments of the slab before `byte_start`
    pub(crate) segments: usize,
    pub(crate) byte_start: usize,
    /// Complete runs behind this position, counted for a *reader*: a
    /// position at a run's end is past it, where [`segments`](Self::segments)
    /// — the writer's count, of runs before the region it opens — is not.
    pub(crate) read_segments: usize,
    /// A reader standing at this position, built from the same walk —
    /// so opening a rebuild reads the slab once, not once for the writer
    /// and again for the reader.
    pub(crate) read_state: crate::rle::RleDecoderState,
}

pub(crate) fn find_partition_at<T: RleValue, C: Codec>(
    slab: &Slab,
    at: usize,
) -> PartitionAt<'_, T, C> {
    let p = find_partition_inner::<T, T, C>(slab, at..at, false);
    PartitionAt {
        state: p.prefix.state,
        segments: p.prefix.segments,
        byte_start: p.outer.start,
        read_segments: p.read_segments,
        read_state: p.read_state,
    }
}

impl<'a, T: RleValue> Postfix<'a, T> {
    /// Detach from the slab, cloning the one or two values held.
    pub(crate) fn into_owned(self) -> crate::rle::state::OwnedPostfix<T> {
        use crate::rle::state::OwnedPostfix as O;
        match self {
            Postfix::Run {
                count,
                value,
                segments,
            } => O::Run {
                count,
                value: T::to_owned(value),
                segments,
            },
            Postfix::Lit {
                value,
                lit,
                segments,
            } => O::Lit {
                value: T::to_owned(value),
                lit,
                segments,
            },
            Postfix::LonePlusLit {
                lone,
                value,
                lit,
                segments,
            } => O::LonePlusLit {
                lone: T::to_owned(lone),
                value: T::to_owned(value),
                lit,
                segments,
            },
        }
    }
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::codec::Leb128;
    use crate::encoding::EncoderApi;
    use crate::rle::state::RleState;
    use crate::Encoder;

    fn state_item_count<T: RleValue, V: AsColumnRef<T>, C: Codec>(
        state: &RleState<'_, T, V, C>,
    ) -> usize {
        match state {
            RleState::Empty(_) => 0,
            RleState::Lone(_) => 1,
            RleState::Run(n, _) => *n,
            RleState::Lit { count, .. } => count + 1,
            RleState::Null(n) => *n,
        }
    }

    fn encode_u64_slab(vals: &[u64]) -> Slab {
        Encoder::<u64>::encode_slab(vals.iter().copied())
    }

    fn encode_opt_slab(vals: &[Option<u64>]) -> Slab {
        Encoder::<Option<u64>>::encode_slab(vals.iter().copied())
    }

    #[test]
    fn mid_repeat() {
        let slab = encode_u64_slab(&[7, 7, 7, 7, 7]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 2..3);
        match &p.prefix.state {
            RleState::Run(2, v) => assert_eq!(v.get(), 7),
            s => panic!("expected Run(2, 7), got {:?}", state_item_count(s)),
        }
        assert_eq!(p.prefix.segments, 0);
        match p.postfix.unwrap() {
            Postfix::Run {
                count: 2, value: 7, ..
            } => {}
            _ => panic!("expected Run(2, 7)"),
        }
    }

    #[test]
    fn mid_literal() {
        let slab = encode_u64_slab(&[1, 2, 3, 4, 5]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 2..3);
        assert_eq!(state_item_count(&p.prefix.state), 2);
        match p.postfix.unwrap() {
            Postfix::Lit {
                value: 4, lit: 1, ..
            } => {}
            _ => panic!("expected Lit(4, lit=1)"),
        }
    }

    #[test]
    fn mid_null() {
        let slab = encode_opt_slab(&[Some(1), None, None, None, Some(2)]);
        let p = find_partition::<Option<u64>, Option<u64>, Leb128>(&slab, 2..3);
        match &p.postfix {
            Some(Postfix::Run {
                count: 1,
                value: None,
                ..
            }) => {}
            _ => panic!("expected Run(1, None)"),
        }
    }

    #[test]
    fn exact_boundary() {
        let slab = encode_u64_slab(&[1, 1, 1, 2, 2, 2]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 3..3);
        match &p.prefix.state {
            RleState::Run(3, v) => assert_eq!(v.get(), 1),
            _ => panic!("expected Run(3, 1)"),
        }
        match p.postfix.unwrap() {
            Postfix::Run {
                count: 3, value: 2, ..
            } => {}
            _ => panic!("expected Run(3, 2)"),
        }
    }

    #[test]
    fn at_start() {
        let slab = encode_u64_slab(&[5, 5, 5]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 0..1);
        assert_eq!(state_item_count(&p.prefix.state), 0);
        match p.postfix.unwrap() {
            Postfix::Run {
                count: 2, value: 5, ..
            } => {}
            _ => panic!("expected Run(2, 5)"),
        }
    }

    #[test]
    fn at_end() {
        let slab = encode_u64_slab(&[1, 2, 3]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 3..3);
        assert_eq!(state_item_count(&p.prefix.state), 3);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn delete_all() {
        let slab = encode_u64_slab(&[1, 2, 3]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 0..3);
        assert_eq!(state_item_count(&p.prefix.state), 0);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn insert_mid_repeat() {
        let slab = encode_u64_slab(&[7, 7, 7, 7]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 2..2);
        match &p.prefix.state {
            RleState::Run(2, v) => assert_eq!(v.get(), 7),
            _ => panic!("expected Run(2, 7)"),
        }
        match p.postfix.unwrap() {
            Postfix::Run {
                count: 2, value: 7, ..
            } => {}
            _ => panic!("expected Run(2, 7)"),
        }
    }

    /// Use build_splice_buf to splice vals[start..end] back in and verify roundtrip.
    fn roundtrip_check(vals: &[u64], start: usize, end: usize) {
        let slab = encode_u64_slab(vals);
        let data: &[u8] = &slab.data;

        let result = build_splice_buf::<u64, u64, Leb128>(
            &slab,
            start,
            end - start,
            vals[start..end].iter().copied().map(|v| (v, 1)),
            usize::MAX,
        );

        let mut reconstructed_bytes = data.to_vec();
        reconstructed_bytes.splice(result.range.clone(), result.bytes);
        if let Some(rw) = result.rewrite {
            Leb128::rewrite_lit_header(&mut reconstructed_bytes, rw.pos, rw.count);
        }

        let original = decode_u64_bytes(data);
        let reconstructed = match std::panic::catch_unwind(|| decode_u64_bytes(&reconstructed_bytes)) {
            Ok(v) => v,
            Err(_) => panic!(
                "decode failed for vals={vals:?}, range={start}..{end}\n  orig bytes={data:?}\n  recon bytes={reconstructed_bytes:?}\n  range={:?} rewrite={:?}",
                result.range, result.rewrite,
            ),
        };
        assert_eq!(
            original, reconstructed,
            "roundtrip failed for vals={vals:?}, range={start}..{end}\n  orig bytes={data:?}\n  recon bytes={reconstructed_bytes:?}"
        );
    }

    fn decode_u64_bytes(data: &[u8]) -> Vec<u64> {
        let mut result = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let (cb, raw) = Leb128::read_signed(&data[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    let (vl, val) = u64::try_unpack::<Leb128>(&data[pos + cb..]).unwrap();
                    for _ in 0..n as usize {
                        result.push(val);
                    }
                    pos += cb + vl;
                }
                n if n < 0 => {
                    let mut scan = pos + cb;
                    for _ in 0..(-n) as usize {
                        let (vl, val) = u64::try_unpack::<Leb128>(&data[scan..]).unwrap();
                        result.push(val);
                        scan += vl;
                    }
                    pos = scan;
                }
                _ => {
                    let (ncb, _nc) = Leb128::read_unsigned(&data[pos + cb..]).unwrap();
                    pos += cb + ncb;
                }
            }
        }
        result
    }

    #[test]
    fn roundtrip_identity_no_delete() {
        // Partition at every point with no deletion — reconstruction must match.
        let vals = vec![1u64, 2, 3, 3, 3, 4, 5, 5, 6, 7, 7, 7, 7, 8];
        for i in 0..=vals.len() {
            roundtrip_check(&vals, i, i);
        }
    }

    #[test]
    fn roundtrip_delete_one() {
        let vals = vec![1u64, 2, 3, 3, 3, 4, 5, 5, 6, 7, 7, 7, 7, 8];
        for i in 0..vals.len() {
            roundtrip_check(&vals, i, i + 1);
        }
    }

    #[test]
    fn roundtrip_delete_range() {
        let vals = vec![1u64, 2, 3, 3, 3, 4, 5, 5, 6, 7, 7, 7, 7, 8];
        for i in 0..vals.len() {
            for j in i..=vals.len() {
                roundtrip_check(&vals, i, j);
            }
        }
    }

    #[test]
    fn roundtrip_fuzz() {
        use rand::{rng, RngExt};
        let mut r = rng();
        for _ in 0..200 {
            let len = r.random_range(0u32..30) as usize + 3;
            let vals: Vec<u64> = (0..len).map(|_| r.random_range(0u64..5)).collect();
            let start = r.random_range(0..len);
            let end = start + r.random_range(0..len - start + 1);
            roundtrip_check(&vals, start, end.min(len));
        }
    }

    #[test]
    fn roundtrip_regression_delete_end() {
        let vals = vec![
            3u64, 4, 3, 0, 2, 1, 3, 3, 4, 1, 1, 3, 2, 2, 4, 0, 1, 2, 4, 2, 0, 1, 1, 2, 3, 3, 0, 1,
            3,
        ];
        roundtrip_check(&vals, 23, 27);
    }

    // ── String splice fuzz (variable-length values) ─────────────────────

    fn decode_string_bytes(data: &[u8]) -> Vec<String> {
        RleDecoder::<String>::new(data)
            .map(str::to_string)
            .collect()
    }

    /// String flavour of `roundtrip_check` — variable-length values stress
    /// the `lit_tail` metadata and header rewrites that fixed-width u64
    /// values can't.
    fn roundtrip_check_str(vals: &[String], start: usize, end: usize) {
        let slab = Encoder::<String>::encode_slab(vals.iter().map(|s| s.as_str()));
        let result = build_splice_buf::<String, &str, Leb128>(
            &slab,
            start,
            end - start,
            vals[start..end].iter().map(|v| (v.as_str(), 1)),
            usize::MAX,
        );
        let mut recon = slab.data.to_vec();
        recon.splice(result.range.clone(), result.bytes);
        if let Some(rw) = result.rewrite {
            Leb128::rewrite_lit_header(&mut recon, rw.pos, rw.count);
        }
        crate::rle::rle_validate_encoding::<String, Leb128>(&recon)
            .unwrap_or_else(|e| panic!("invalid encoding for {vals:?}, range={start}..{end}: {e}"));
        assert_eq!(
            decode_string_bytes(&recon),
            vals,
            "roundtrip failed for {vals:?}, range={start}..{end}"
        );
    }

    #[test]
    fn roundtrip_fuzz_strings() {
        use rand::{rng, RngExt};
        let mut r = rng();
        // Mixed lengths so literal-run values have different byte widths.
        let pool = ["a", "bb", "ccc", "dddd", "ee"];
        for _ in 0..300 {
            let len = r.random_range(3u32..25) as usize;
            let vals: Vec<String> = (0..len)
                .map(|_| pool[r.random_range(0..pool.len())].to_string())
                .collect();
            let start = r.random_range(0..len);
            let end = (start + r.random_range(0..len - start + 1)).min(len);
            roundtrip_check_str(&vals, start, end);
        }
    }

    // ── Overflow tests ──────────────────────────────────────────────────

    /// Verify that build_splice_buf with overflow produces correct slabs
    /// that decode to the expected values.
    fn overflow_insert_check(initial: &[u64], index: usize, new_vals: &[u64], max_seg: usize) {
        let slab = encode_u64_slab(initial);
        let result = build_splice_buf::<u64, u64, Leb128>(
            &slab,
            index,
            0,
            new_vals.iter().copied().map(|v| (v, 1)),
            max_seg,
        );

        // Decode all slabs: first slab (after splice) + overflow slabs.
        let mut first = slab.data.to_vec();
        first.splice(result.range.clone(), result.bytes);
        if let Some(rw) = result.rewrite {
            Leb128::rewrite_lit_header(&mut first, rw.pos, rw.count);
        }
        let mut all_vals = decode_u64_bytes(&first);
        for s in &result.overflow {
            let d: &[u8] = &s.data;
            all_vals.extend(decode_u64_bytes(d));
        }

        // Build expected: initial[..index] + new_vals + initial[index..]
        let mut expected = initial[..index].to_vec();
        expected.extend_from_slice(new_vals);
        expected.extend_from_slice(&initial[index..]);
        assert_eq!(
            all_vals, expected,
            "overflow insert mismatch: index={index} max_seg={max_seg}"
        );
    }

    #[test]
    fn overflow_insert_many_at_start() {
        // Insert enough values to trigger overflow with max_segments=4.
        overflow_insert_check(&[1, 2, 3], 0, &[10, 20, 30, 40, 50, 60], 4);
    }

    #[test]
    fn overflow_insert_many_at_mid() {
        overflow_insert_check(&[1, 2, 3, 4, 5], 2, &[10, 20, 30, 40, 50], 5);
    }

    #[test]
    fn overflow_insert_many_at_end() {
        overflow_insert_check(&[1, 2, 3], 3, &[10, 20, 30, 40, 50], 3);
    }

    #[test]
    fn overflow_insert_repeats() {
        // Repeats compress well — may not overflow even with many values.
        overflow_insert_check(&[7, 7, 7], 1, &[7, 7, 7, 7, 7, 7, 7, 7], 4);
    }

    #[test]
    fn overflow_fuzz() {
        use rand::{rng, RngExt};
        let mut r = rng();
        for _ in 0..100 {
            let initial_len = r.random_range(0u32..10) as usize + 1;
            let initial: Vec<u64> = (0..initial_len).map(|_| r.random_range(0u64..5)).collect();
            let insert_len = r.random_range(0u32..20) as usize + 1;
            let new_vals: Vec<u64> = (0..insert_len).map(|_| r.random_range(0u64..5)).collect();
            let index = r.random_range(0..initial_len + 1);
            let max_seg = initial_len + r.random_range(0u32..8) as usize;
            overflow_insert_check(&initial, index, &new_vals, max_seg);
        }
    }

    // ── Partition edge case: k=1 into repeat after literal ──────────────

    #[test]
    fn partition_k1_repeat_after_literal() {
        // [1, 2, 3, 3, 3] — literal [1, 2] then repeat [3, 3, 3]
        // Delete at index 3 (k=1 into the repeat, was_lit=true for value 2)
        let slab = encode_u64_slab(&[1, 2, 3, 3, 3]);
        let p = find_partition::<u64, u64, Leb128>(&slab, 3..4);
        // Prefix should capture [1, 2, 3] — the literal + 1 item from repeat
        assert_eq!(state_item_count(&p.prefix.state), 3);
        // Postfix should be Run { count: 1, value: 3 }
        match p.postfix.unwrap() {
            Postfix::Run {
                count: 1, value: 3, ..
            } => {}
            other => panic!("expected Run(1, 3), got {:?}", other),
        }
        roundtrip_check(&[1, 2, 3, 3, 3], 3, 4);
    }

    #[test]
    fn partition_k1_repeat_after_literal_identity() {
        // Identity splice (no delete, no insert) at every position in [1, 2, 3, 3, 3]
        let vals = [1u64, 2, 3, 3, 3];
        for i in 0..=vals.len() {
            roundtrip_check(&vals, i, i);
        }
    }

    #[test]
    fn partition_lone_plus_lit_postfix() {
        // [7, 7, 7, 1, 2, 3] — delete index 1, postfix has count=1 of repeat
        // followed by a literal run. Depending on the peek, this is either
        // LonePlusLit or Run(1, 7).
        // The roundtrip is what matters — both variants produce correct output.
        roundtrip_check(&[7, 7, 7, 1, 2, 3], 1, 2);
    }

    #[test]
    fn partition_lone_plus_lit_delete_two_from_repeat() {
        // [7, 7, 7, 1, 2, 3] — delete indices 0..2, leaving [7, 1, 2, 3]
        roundtrip_check(&[7, 7, 7, 1, 2, 3], 0, 2);
    }

    #[test]
    fn roundtrip_all_positions_repeat_then_literal() {
        // Comprehensive: every (start, end) for a repeat-then-literal pattern
        let vals = vec![5u64, 5, 5, 1, 2, 3, 4];
        for i in 0..vals.len() {
            for j in i..=vals.len() {
                roundtrip_check(&vals, i, j);
            }
        }
    }

    #[test]
    fn roundtrip_all_positions_literal_repeat_literal() {
        // Pattern: literal, repeat, literal
        let vals = vec![1u64, 2, 3, 3, 3, 4, 5, 6];
        for i in 0..vals.len() {
            for j in i..=vals.len() {
                roundtrip_check(&vals, i, j);
            }
        }
    }

    // ── Overflow with postfix in literal ─────────────────────────────────

    #[test]
    fn overflow_postfix_in_literal() {
        // [1, 2, 3, 4, 5, 6, 7, 8] all unique — insert enough to overflow
        // at a point where the postfix falls inside the literal run.
        let initial = [1u64, 2, 3, 4, 5, 6, 7, 8];
        let new_vals = [10u64, 20, 30, 40, 50];
        for index in 0..=initial.len() {
            overflow_insert_check(&initial, index, &new_vals, initial.len());
        }
    }

    #[test]
    fn overflow_postfix_at_literal_repeat_junction() {
        // Postfix lands at the literal→repeat boundary during overflow.
        let initial = [1u64, 2, 3, 3, 3, 4, 5];
        let new_vals = [10u64, 20, 30, 40, 50, 60, 70];
        // Insert at every position
        for index in 0..=initial.len() {
            overflow_insert_check(&initial, index, &new_vals, initial.len());
        }
    }
}

#[derive(Default)]
struct SpliceBuf {
    bytes: Vec<u8>,
    range: Range<usize>,
    len: usize,
    segments: usize,
    rewrite: Option<RewriteHeader>,
    overflow: Vec<Slab>,
    wpos: WPos,
}

/// Build the splice buffer. Borrows slab immutably; returns owned output.
/// After this, caller does: `slab.data.splice(result.range, result.bytes)`,
/// applies rewrite, sets slab.len and slab.segments.
fn build_splice_buf<T: RleValue, V: AsColumnRef<T>, C: Codec>(
    slab: &Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = (V, usize)>,
    max_segments: usize,
) -> SpliceBuf {
    assert!(slab.segments <= max_segments);
    let p = find_partition::<T, V, C>(slab, index..index + del);
    let mut target_segments = max_segments;

    let mut result = SpliceBuf {
        range: p.outer,
        ..Default::default()
    };

    let mut buf = Vec::new();
    let mut state = p.prefix.state;
    let mut f = FlushState::default();
    let mut overflowed = false;
    let mut inserted = 0;
    let mut starting_segments = p.prefix.segments;
    let postfix_bytes = &slab.data[result.range.end..];

    // 1. Feed new values.  Each item is `(value, count)` — count > 1 inserts
    //    a run of identical values in bulk via `append_n`.  Count == 0 is a
    //    no-op (handled by `append_n`).
    for (val, count) in values {
        if starting_segments + f.segments + state.pending_segments() >= target_segments {
            f += state.flush(&mut buf);
            if !overflowed {
                overflowed = true;
                target_segments = max_segments / 2;
                result.wpos = f.wpos;
                result.bytes = std::mem::take(&mut buf);
                result.len = index + inserted;
                result.segments = p.prefix.segments + f.segments;
                result.rewrite = f.rewrite;
            } else {
                let tail = f.wpos.as_tail(0, buf.len());
                let data = std::mem::take(&mut buf);
                let len = inserted;
                let segments = f.segments;
                result.overflow.push(Slab {
                    data,
                    len,
                    segments,
                    tail,
                });
            }
            state = RleState::empty();
            f = FlushState::default();
            inserted = 0;
            starting_segments = 0;
        }
        inserted += count;
        f += state.append_n(&mut buf, val, count);
    }

    // 2. Feed postfix + flush.
    let postfix_segments = p.postfix.as_ref().map(|p| p.segments()).unwrap_or(0);
    let postfix_pending = p.postfix.as_ref().map(|p| p.pending()).unwrap_or(0);
    if !overflowed {
        let pending = state.pending_segments() + postfix_pending;
        if p.prefix.segments + pending + f.segments + postfix_segments <= max_segments {
            f += state.flush_postfix(&mut buf, p.postfix).0;
            result.bytes = buf;
            result.wpos = f.wpos;
            result.rewrite = f.rewrite;
            result.len = slab.len - del + inserted;
            result.segments = p.prefix.segments + f.segments + postfix_segments;
            return result;
        } else {
            // overflow now!

            f += state.flush(&mut buf);
            result.bytes = std::mem::take(&mut buf);
            result.wpos = f.wpos;
            result.rewrite = f.rewrite;
            result.len = index + inserted;
            result.segments = p.prefix.segments + f.segments;

            f = FlushState::default();
            inserted = 0;
        }
    }

    // if the postfix wont fit - flush the last overflow slab
    let pending = state.pending_segments() + postfix_pending;
    if pending + f.segments + postfix_segments > max_segments {
        f += state.flush(&mut buf);
        let tail = f.wpos.as_tail(0, buf.len());
        let data = std::mem::take(&mut buf);
        let len = inserted;
        let segments = f.segments;
        result.overflow.push(Slab {
            data,
            len,
            segments,
            tail,
        });
        state = RleState::empty();
        f = FlushState::default();
        inserted = 0;
    }

    f += state.flush_postfix(&mut buf, p.postfix).0;

    // the postfix goes on the final slab
    result.range.end = slab.data.len();

    let postfix_count = slab.len - index - del;
    let len = inserted + postfix_count;
    let segments = f.segments + postfix_segments;
    let tail = f.wpos.merge(0, buf.len(), postfix_bytes.len(), slab.tail);
    buf.extend_from_slice(postfix_bytes);
    let data = std::mem::take(&mut buf);
    result.overflow.push(Slab {
        data,
        len,
        segments,
        tail,
    });

    result
}

pub(crate) fn splice_slab<T: RleValue, V: AsColumnRef<T>, C: Codec>(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = (V, usize)>,
    max_segments: usize,
) -> Vec<Slab> {
    assert!(index + del <= slab.len, "del extends beyond slab");
    assert!(slab.segments <= max_segments);

    let result = build_splice_buf::<T, V, C>(slab, index, del, values, max_segments);
    let wpos = result.wpos;
    let range = result.range;

    let mut prefix = range.start as i64;
    let middle = result.bytes.len();
    let postfix = slab.data.len() - range.end;

    // we have to splice before rewrite header so range will be correct
    // (splice_bytes = memcpy-based Vec::splice — see column::splice_bytes)
    splice_bytes(&mut slab.data, range.start, range.len(), &result.bytes);

    if let Some(rw) = result.rewrite {
        prefix += C::rewrite_lit_header(&mut slab.data, rw.pos, rw.count);
    }

    // we have to gen the tail after rewrite so tail will be correct
    slab.tail = wpos.merge(prefix as usize, middle, postfix, slab.tail);
    slab.len = result.len;
    slab.segments = result.segments;

    #[cfg(debug_assertions)]
    validate_rle_slab::<T, C>(slab);
    #[cfg(debug_assertions)]
    for s in &result.overflow {
        validate_rle_slab::<T, C>(s);
    }

    result.overflow
}

fn head<T: RleValue, C: Codec>(slab: &Slab) -> (Postfix<'_, T>, usize) {
    debug_assert!(slab.segments > 0, "head() on empty slab");
    let segments = slab.segments - 1;
    match C::read_signed(&slab.data).unwrap() {
        (tb, count) if count > 0 => {
            let (vb, value) = T::unpack::<C>(&slab.data[tb..]);
            let count = count as usize;
            (
                Postfix::Run {
                    count,
                    value,
                    segments,
                },
                tb + vb,
            )
        }
        (tb, 0) => {
            let (vb, nulls) = C::read_unsigned(&slab.data[tb..]).unwrap();
            let count = nulls as usize;
            let value = T::get_null();
            (
                Postfix::Run {
                    count,
                    value,
                    segments,
                },
                tb + vb,
            )
        }
        (tb, count) => {
            let (vb, value) = T::unpack::<C>(&slab.data[tb..]);
            let count = -count as usize;
            let lit = count - 1;
            (
                Postfix::Lit {
                    lit,
                    value,
                    segments,
                },
                tb + vb,
            )
        }
    }
}

pub(crate) fn tail<T: RleValue, C: Codec>(
    data: &[u8],
    tail: RleTail,
) -> (RleState<'_, T, T, C>, usize, usize) {
    let len = data.len();
    let bytes = tail.bytes as usize;
    let header_pos = len - bytes;
    match C::read_signed(&data[header_pos..]) {
        None => (RleState::empty(), 0, 0),
        Some((tb, count)) if count > 0 => {
            let (_, value) = T::unpack::<C>(&data[header_pos + tb..]);
            (
                RleState::make_run(count as usize, RleCow::Ref(value)),
                header_pos,
                1,
            )
        }
        Some((tb, 0)) => {
            let (_, nulls) = C::read_unsigned(&data[header_pos + tb..]).unwrap();
            (RleState::Null(nulls as usize), header_pos, 1)
        }
        Some((tb, -1)) => {
            let (_, value) = T::unpack::<C>(&data[header_pos + tb..]);
            (RleState::Lone(RleCow::Ref(value)), header_pos, 1)
        }
        Some((_tb, count)) => {
            let bytes = tail.lit_tail.unwrap().get() as usize;
            let value_pos = len - bytes;
            let (_, value) = T::unpack::<C>(&data[value_pos..]);
            let current = RleCow::Ref(value);
            let count = -count as usize - 1;
            let state = RleState::Lit {
                count,
                local: 0,
                // `bytes` above located `current` in the slab; nothing has
                // been written locally, so there is no width to record
                bytes: None,
                current,
                header_pos,
            };
            (state, value_pos, 1)
        }
    }
}

pub(crate) fn rle_merge<T: RleValue, C: Codec>(a: &mut Slab, b: &Slab) {
    let mut buf = vec![];
    let (seg, tail) = do_merge::<T, C>(&mut a.data, a.tail, a.segments, b, &mut buf);
    a.segments = seg;
    a.tail = tail;
    a.len += b.len;
}

pub(crate) fn do_merge<T: RleValue, C: Codec>(
    a: &mut Vec<u8>,
    a_tail: RleTail,
    a_segs: usize,
    b: &Slab,
    buf: &mut Vec<u8>,
) -> (usize, RleTail) {
    if b.len == 0 {
        return (a_segs, a_tail);
    }
    let (tail_pos, b_bytes, seg, f) = {
        let (mut a_state, tail_pos, delta_seg) = tail::<T, C>(a, a_tail);
        let (b_head, b_bytes) = head::<T, C>(b);
        let (f, b_segments) = a_state.flush_postfix(buf, Some(b_head));
        (
            tail_pos,
            b_bytes,
            a_segs + f.segments + b_segments - delta_seg,
            f,
        )
    };
    a.truncate(tail_pos);
    if let Some(rw) = f.rewrite {
        C::rewrite_lit_header(a, rw.pos, rw.count); // a.len() could change here
    }
    let a_len = a.len();
    a.extend_from_slice(buf);
    a.extend_from_slice(&b.data[b_bytes..]);
    let tail = f
        .wpos
        .merge(a_len, buf.len(), b.data.len() - b_bytes, b.tail);
    (seg, tail)
}
