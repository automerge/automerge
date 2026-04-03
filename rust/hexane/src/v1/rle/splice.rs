//! RLE splice — in-place insert/delete/replace within a slab.

use std::ops::Range;

use super::state::{FlushState, RewriteHeader, RleCow, RleState, WPos};
use super::{RleDecoder, RleTail, Slab};
use crate::v1::encoding::RunDecoder;
use crate::v1::leb::{read_signed, read_unsigned, rewrite_lit_header};
use crate::v1::{AsColumnRef, RleValue};

#[cfg(debug_assertions)]
use super::validate_rle_slab;

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

#[derive(Debug)]
struct Prefix<'a, T: RleValue, V: AsColumnRef<T>> {
    state: RleState<'a, T, V>,
    segments: usize,
    bytes: usize,
}

impl<'a, T: RleValue, V: AsColumnRef<T>> Prefix<'a, T, V> {
    fn new() -> Self {
        Prefix {
            state: RleState::Empty,
            segments: 0,
            bytes: 0,
        }
    }
}

#[derive(Debug)]
struct RlePartition<'a, T: RleValue, V: AsColumnRef<T>> {
    outer: Range<usize>,
    prefix: Prefix<'a, T, V>,
    postfix: Option<Postfix<'a, T>>,
}

fn find_partition<'a, T: RleValue, V: AsColumnRef<T>>(
    slab: &'a Slab,
    range: Range<usize>,
) -> RlePartition<'a, T, V> {
    let mut decoder = RleDecoder::<T>::new(&slab.data);
    let mut byte_before = decoder.byte_pos;
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
            outer.start = byte_before;
            prefix.segments = segments;
            prefix.bytes = byte_before;
            prefix_done = true;

            if is_lit {
                let count = item_pos - lit_start_item;
                let bytes = decoder.byte_pos - byte_before;
                prefix.state = RleState::lit(count, RleCow::Ref(run.value), header_pos, bytes);
            } else if is_null {
                prefix.state = RleState::Null(k);
            } else if k == 1 && !is_lit && was_lit {
                let count = segments - lit_segments_before;
                let bytes = decoder.byte_pos - byte_before;
                prefix.state = RleState::lit(count, RleCow::Ref(run.value), header_pos, bytes);
            } else {
                prefix.state = RleState::make_run(k, RleCow::Ref(run.value));
            }
        }

        // ── Postfix ─────────────────────────────────────────────────────
        if prefix_done && range.end < run_end_item {
            let count = run_end_item - range.end;
            let value = run.value;
            let consumed = segments + 1; // loop segments + this run
            outer.end = decoder.byte_pos;
            let p = if is_lit {
                let lit = decoder.remaining;
                Postfix::Lit {
                    value,
                    lit,
                    segments: slab.segments - consumed,
                }
            } else {
                (|| {
                    if count == 1 && !is_null {
                        if let Some(post_run) = decoder.next_run() {
                            if decoder.is_literal() && post_run.count == 1 {
                                let lone = value;
                                let value = post_run.value;
                                let lit = decoder.remaining;
                                outer.end = decoder.byte_pos; // past the first lit value
                                return Some(Postfix::LonePlusLit {
                                    lone,
                                    value,
                                    lit,
                                    segments: slab.segments - consumed - 1, // -1 for the peeked lit value
                                });
                            }
                        }
                    }
                    None
                })()
                .unwrap_or_else(|| Postfix::Run {
                    count,
                    value,
                    segments: slab.segments - consumed,
                })
            };
            postfix = Some(p);
            break;
        }

        segments += 1;
        item_pos = run_end_item;
        byte_before = decoder.byte_pos;
        was_lit = is_lit;
    }

    RlePartition {
        outer,
        prefix,
        postfix,
    }
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::v1::encoding::ColumnEncoding;
    use crate::v1::rle::{state::RleState, RleEncoding};

    fn state_item_count<T: RleValue, V: AsColumnRef<T>>(state: &RleState<'_, T, V>) -> usize {
        match state {
            RleState::Empty => 0,
            RleState::Lone(_) => 1,
            RleState::Run(n, _) => *n,
            RleState::Lit { count, .. } => count + 1,
            RleState::Null(n) => *n,
        }
    }

    fn encode_u64_slab(vals: &[u64]) -> Slab {
        RleEncoding::<u64>::encode(vals.iter().copied())
    }

    fn encode_opt_slab(vals: &[Option<u64>]) -> Slab {
        RleEncoding::<Option<u64>>::encode(vals.iter().copied())
    }

    #[test]
    fn mid_repeat() {
        let slab = encode_u64_slab(&[7, 7, 7, 7, 7]);
        let p = find_partition::<u64, u64>(&slab, 2..3);
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
        let p = find_partition::<u64, u64>(&slab, 2..3);
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
        let p = find_partition::<Option<u64>, Option<u64>>(&slab, 2..3);
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
        let p = find_partition::<u64, u64>(&slab, 3..3);
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
        let p = find_partition::<u64, u64>(&slab, 0..1);
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
        let p = find_partition::<u64, u64>(&slab, 3..3);
        assert_eq!(state_item_count(&p.prefix.state), 3);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn delete_all() {
        let slab = encode_u64_slab(&[1, 2, 3]);
        let p = find_partition::<u64, u64>(&slab, 0..3);
        assert_eq!(state_item_count(&p.prefix.state), 0);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn insert_mid_repeat() {
        let slab = encode_u64_slab(&[7, 7, 7, 7]);
        let p = find_partition::<u64, u64>(&slab, 2..2);
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

        let result = build_splice_buf::<u64, u64>(
            &slab,
            start,
            end - start,
            &mut vals[start..end].iter().copied(),
            usize::MAX,
        );

        let mut reconstructed_bytes = data.to_vec();
        reconstructed_bytes.splice(result.range.clone(), result.bytes);
        if let Some(rw) = result.rewrite {
            rewrite_lit_header(&mut reconstructed_bytes, rw.pos, rw.count);
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
            let (cb, raw) = read_signed(&data[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    let (vl, val) = u64::try_unpack(&data[pos + cb..]).unwrap();
                    for _ in 0..n as usize {
                        result.push(val);
                    }
                    pos += cb + vl;
                }
                n if n < 0 => {
                    let mut scan = pos + cb;
                    for _ in 0..(-n) as usize {
                        let (vl, val) = u64::try_unpack(&data[scan..]).unwrap();
                        result.push(val);
                        scan += vl;
                    }
                    pos = scan;
                }
                _ => {
                    let (ncb, _nc) = read_unsigned(&data[pos + cb..]).unwrap();
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
        use rand::{rng, RngCore};
        let mut r = rng();
        for _ in 0..200 {
            let len = (r.next_u32() % 30 + 3) as usize;
            let vals: Vec<u64> = (0..len).map(|_| r.next_u64() % 5).collect();
            let start = r.next_u32() as usize % len;
            let end = start + (r.next_u32() as usize % (len - start + 1));
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

    // ── Overflow tests ──────────────────────────────────────────────────

    /// Verify that build_splice_buf with overflow produces correct slabs
    /// that decode to the expected values.
    fn overflow_insert_check(initial: &[u64], index: usize, new_vals: &[u64], max_seg: usize) {
        let slab = encode_u64_slab(initial);
        let result =
            build_splice_buf::<u64, u64>(&slab, index, 0, new_vals.iter().copied(), max_seg);

        // Decode all slabs: first slab (after splice) + overflow slabs.
        let mut first = slab.data.to_vec();
        first.splice(result.range.clone(), result.bytes);
        if let Some(rw) = result.rewrite {
            rewrite_lit_header(&mut first, rw.pos, rw.count);
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
        overflow_insert_check(&[1, 2, 3, 4, 5], 2, &[10, 20, 30, 40, 50], 3);
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
        use rand::{rng, RngCore};
        let mut r = rng();
        for _ in 0..100 {
            let initial_len = (r.next_u32() % 10 + 1) as usize;
            let initial: Vec<u64> = (0..initial_len).map(|_| r.next_u64() % 5).collect();
            let insert_len = (r.next_u32() % 20 + 1) as usize;
            let new_vals: Vec<u64> = (0..insert_len).map(|_| r.next_u64() % 5).collect();
            let index = r.next_u32() as usize % (initial_len + 1);
            let max_seg = (r.next_u32() % 8 + 2) as usize;
            overflow_insert_check(&initial, index, &new_vals, max_seg);
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
    //tail: RleTail,
    wpos: WPos,
}

/// Build the splice buffer. Borrows slab immutably; returns owned output.
/// After this, caller does: `slab.data.splice(result.range, result.bytes)`,
/// applies rewrite, sets slab.len and slab.segments.
fn build_splice_buf<T: RleValue, V: AsColumnRef<T>>(
    slab: &Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = V>,
    max_segments: usize,
) -> SpliceBuf {
    let p = find_partition::<T, V>(slab, index..index + del);

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

    // 1. Feed new values.
    for v in values {
        if starting_segments + f.segments + state.pending_segments() >= max_segments {
            f += state.flush(&mut buf);
            if !overflowed {
                overflowed = true;
                //result.tail = f.wpos.as_tail(p.prefix.bytes, buf.len());
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
            state = RleState::Empty;
            f = FlushState::default();
            inserted = 0;
            starting_segments = 0;
        }
        inserted += 1;
        f += state.append(&mut buf, v);
    }

    // 2. Feed postfix + flush.
    let (pf, postfix_segments) = state.flush_postfix(&mut buf, p.postfix);
    f += pf;

    if !overflowed {
        result.bytes = buf;
        result.len = slab.len - del + inserted;
        result.segments = p.prefix.segments + f.segments + postfix_segments;
        result.rewrite = f.rewrite;
        result.wpos = f.wpos;
    /*
            result.tail = f.wpos.merge(
                p.prefix.bytes,
                result.bytes.len(),
                postfix_bytes.len(),
                slab.tail,
            );
    */
    } else {
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
    }

    #[cfg(debug_assertions)]
    for s in &result.overflow {
        s.validate::<T>();
    }

    result
}

pub(crate) fn splice_slab<T: RleValue, V: AsColumnRef<T>>(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = V>,
    max_segments: usize,
) -> Vec<Slab> {
    assert!(index + del <= slab.len, "del extends beyond slab");

    let result = build_splice_buf::<T, V>(slab, index, del, values, max_segments);
    let wpos = result.wpos;
    let range = result.range;

    let mut prefix = range.start as i64;
    let middle = result.bytes.len();
    let postfix = slab.data.len() - range.end;

    // we have to splice before rewrite header so range will be correct
    slab.data.splice(range, result.bytes);

    if let Some(rw) = result.rewrite {
        prefix += rewrite_lit_header(&mut slab.data, rw.pos, rw.count);
    }

    // we have to gen the tail after rewrite so tail will be correct
    slab.tail = wpos.merge(prefix as usize, middle, postfix, slab.tail);
    slab.len = result.len;
    slab.segments = result.segments;

    #[cfg(debug_assertions)]
    validate_rle_slab::<T>(slab);
    #[cfg(debug_assertions)]
    for s in &result.overflow {
        validate_rle_slab::<T>(s);
    }

    result.overflow
}

fn head<T: RleValue>(slab: &Slab) -> (Postfix<'_, T>, usize) {
    let segments = slab.segments - 1;
    match read_signed(&slab.data).unwrap() {
        (tb, count) if count > 0 => {
            let (vb, value) = T::unpack(&slab.data[tb..]);
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
            let (vb, nulls) = read_unsigned(&slab.data[tb..]).unwrap();
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
            let (vb, value) = T::unpack(&slab.data[tb..]);
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

fn tail<T: RleValue>(data: &[u8], tail: RleTail) -> (RleState<'_, T, T>, usize, usize) {
    let len = data.len();
    let bytes = tail.bytes as usize;
    let header_pos = len - bytes;
    match read_signed(&data[header_pos..]) {
        None => (RleState::Empty, 0, 0),
        Some((tb, count)) if count > 0 => {
            let (_, value) = T::unpack(&data[header_pos + tb..]);
            (
                RleState::make_run(count as usize, RleCow::Ref(value)),
                header_pos,
                1,
            )
        }
        Some((tb, 0)) => {
            let (_, nulls) = read_unsigned(&data[header_pos + tb..]).unwrap();
            (RleState::Null(nulls as usize), header_pos, 1)
        }
        Some((tb, -1)) => {
            let (_, value) = T::unpack(&data[header_pos + tb..]);
            (RleState::Lone(RleCow::Ref(value)), header_pos, 1)
        }
        Some((_tb, count)) => {
            let bytes = tail.lit_tail.unwrap().get() as usize;
            let value_pos = len - bytes;
            let (_, value) = T::unpack(&data[value_pos..]);
            let current = RleCow::Ref(value);
            let count = -count as usize - 1;
            let state = RleState::Lit {
                count,
                local: 0,
                current,
                header_pos,
                bytes,
            };
            (state, value_pos, 1)
        }
    }
}

pub(crate) fn rle_merge<T: RleValue>(a: &mut Slab, b: &Slab) {
    let mut buf = vec![];
    let (seg, tail) = do_merge::<T>(&mut a.data, a.tail, a.segments, b, &mut buf);
    a.segments = seg;
    a.tail = tail;
    a.len += b.len;
}

pub(crate) fn do_merge<T: RleValue>(
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
        let (mut a_state, tail_pos, delta_seg) = tail::<T>(a, a_tail);
        let (b_head, b_bytes) = head::<T>(b);
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
        rewrite_lit_header(a, rw.pos, rw.count); // a.len() could change here
    }
    let a_len = a.len();
    a.extend_from_slice(buf);
    a.extend_from_slice(&b.data[b_bytes..]);
    let tail = f
        .wpos
        .merge(a_len, buf.len(), b.data.len() - b_bytes, b.tail);
    (seg, tail)
}
