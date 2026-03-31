use std::marker::PhantomData;
use std::ops::Range;

use crate::PackError;

use super::column::Slab;
use super::encoding::{ColumnEncoding, RunDecoder};
use super::rle_state::RewriteHeader;
use super::{AsColumnRef, ColumnValueRef, RleValue, Run, ValidBuf, ValidBytes};

// ── Wire-format helpers ───────────────────────────────────────────────────────
//
// The encoding (shared with v0) is a sequence of runs:
//
//   Repeat run : signed_leb128( count > 0 )  packed_value
//   Literal run: signed_leb128( -n      )    v0 v1 … v(n-1)
//   Null run   : signed_leb128( 0       )    unsigned_leb128( count )

/// Stack-buffered LEB128 encoding (max 10 bytes, no heap allocation).
#[derive(Clone, Copy)]
pub(crate) struct Leb128Buf {
    pub(crate) buf: [u8; 10],
    pub(crate) len: u8,
}

impl Leb128Buf {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

impl std::ops::Deref for Leb128Buf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Owned byte iterator over a `Leb128Buf`. No heap allocation.
pub(crate) struct Leb128Iter {
    buf: [u8; 10],
    pos: u8,
    len: u8,
}

impl Iterator for Leb128Iter {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.pos < self.len {
            let b = self.buf[self.pos as usize];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.pos) as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for Leb128Iter {}

impl IntoIterator for Leb128Buf {
    type Item = u8;
    type IntoIter = Leb128Iter;
    #[inline]
    fn into_iter(self) -> Leb128Iter {
        Leb128Iter {
            buf: self.buf,
            pos: 0,
            len: self.len,
        }
    }
}

#[inline]
pub(crate) fn encode_signed(n: i64) -> Leb128Buf {
    let mut out = Leb128Buf {
        buf: [0u8; 10],
        len: 0,
    };
    let mut val = n;
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        let more = !((val == 0 && byte & 0x40 == 0) || (val == -1 && byte & 0x40 != 0));
        if more {
            byte |= 0x80;
        }
        out.buf[out.len as usize] = byte;
        out.len += 1;
        if !more {
            break;
        }
    }
    out
}

#[inline]
pub(crate) fn encode_unsigned(n: u64) -> Leb128Buf {
    let mut out = Leb128Buf {
        buf: [0u8; 10],
        len: 0,
    };
    let mut val = n;
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        out.buf[out.len as usize] = byte;
        out.len += 1;
        if val == 0 {
            break;
        }
    }
    out
}

/// Decode one signed LEB128 count from `data`.  Returns (bytes_read, value).
pub(crate) fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::signed(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

/// Decode one unsigned LEB128 count from `data`.  Returns (bytes_read, value).
pub(crate) fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Encode a value-run count header.  A single item is stored as a literal
/// run (`-1`) rather than a repeat run (`+1`) so that every repeat run has
/// count >= 2.
#[allow(dead_code)]
fn value_run_header(count: usize) -> Leb128Buf {
    if count == 1 {
        encode_signed(-1)
    } else {
        encode_signed(count as i64)
    }
}

/// Stack-buffered null run: marker (0) + unsigned count. Max 20 bytes.
#[allow(dead_code)]
struct NullRunBuf {
    buf: [u8; 20],
    len: u8,
}

impl std::ops::Deref for NullRunBuf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

struct NullRunIter {
    buf: [u8; 20],
    pos: u8,
    len: u8,
}

impl Iterator for NullRunIter {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.pos < self.len {
            let b = self.buf[self.pos as usize];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.pos) as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for NullRunIter {}

impl IntoIterator for NullRunBuf {
    type Item = u8;
    type IntoIter = NullRunIter;
    #[inline]
    fn into_iter(self) -> NullRunIter {
        NullRunIter {
            buf: self.buf,
            pos: 0,
            len: self.len,
        }
    }
}

#[allow(dead_code)]
fn null_run_bytes(count: usize) -> NullRunBuf {
    let marker = encode_signed(0);
    let cnt = encode_unsigned(count as u64);
    let mut out = NullRunBuf {
        buf: [0u8; 20],
        len: 0,
    };
    out.buf[..marker.len as usize].copy_from_slice(marker.as_bytes());
    out.len = marker.len;
    out.buf[out.len as usize..out.len as usize + cnt.len as usize].copy_from_slice(cnt.as_bytes());
    out.len += cnt.len;
    out
}

// ── RleDecoder ───────────────────────────────────────────────────────────────

/// Forward iterator over all items in a single RLE-encoded slab.
///
/// Created by [`RleEncoding::decoder`].  Repeat runs yield the cached value
/// in O(1) per item.  Literal runs decode each value.  Null runs yield
/// the type's null value.
pub struct RleDecoder<'a, T: RleValue> {
    data: &'a ValidBytes,
    pub(crate) byte_pos: usize,
    pub(crate) remaining: usize,
    state: RleDecoderState<'a, T>,
}

impl<T: RleValue> Clone for RleDecoder<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            state: self.state.clone(),
        }
    }
}

enum RleDecoderState<'a, T: RleValue> {
    /// Repeat run: yield the same cached value.
    Repeat(<T as ColumnValueRef>::Get<'a>),
    /// Literal run: decode each value from `byte_pos`.
    Literal,
    /// Null run: yield the type's null value.
    Null,
    /// Between runs or exhausted.
    Idle,
}

impl<T: RleValue> Clone for RleDecoderState<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Repeat(v) => Self::Repeat(*v),
            Self::Literal => Self::Literal,
            Self::Null => Self::Null,
            Self::Idle => Self::Idle,
        }
    }
}

impl<'a, T: RleValue> RleDecoder<'a, T> {
    pub(crate) fn new(data: &'a ValidBytes) -> Self {
        RleDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            state: RleDecoderState::Idle,
        }
    }

    pub(crate) fn is_literal(&self) -> bool {
        matches!(self.state, RleDecoderState::Literal)
    }

    fn advance_run(&mut self) {
        if self.byte_pos >= self.data.len() {
            self.state = RleDecoderState::Idle;
            self.remaining = 0;
            return;
        }
        let (count_bytes, count_raw) = match read_signed(&self.data[self.byte_pos..]) {
            Some(v) => v,
            None => {
                self.state = RleDecoderState::Idle;
                self.remaining = 0;
                return;
            }
        };

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let value_start = self.byte_pos + count_bytes;
                let (vlen, value) = T::unpack(&self.data[value_start..]);
                self.byte_pos = value_start + vlen;
                self.remaining = count;
                self.state = RleDecoderState::Repeat(value);
            }
            n if n < 0 => {
                let total = (-n) as usize;
                self.byte_pos += count_bytes;
                self.remaining = total;
                self.state = RleDecoderState::Literal;
            }
            _ => {
                // Null run (count_raw == 0)
                let (ncb, null_count) =
                    read_unsigned(&self.data[self.byte_pos + count_bytes..]).unwrap();
                self.byte_pos += count_bytes + ncb;
                self.remaining = null_count as usize;
                self.state = RleDecoderState::Null;
            }
        }
    }
}

impl<'a, T: RleValue> RleDecoder<'a, T> {
    /// Skip `n` literal values by advancing `byte_pos` without decoding.
    #[inline]
    fn skip_literals(&mut self, n: usize) {
        for _ in 0..n {
            let vlen = T::value_len(&self.data[self.byte_pos..]).unwrap();
            self.byte_pos += vlen;
        }
    }
}

impl<'a, T: RleValue> Iterator for RleDecoder<'a, T> {
    type Item = <T as ColumnValueRef>::Get<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining > 0 {
                self.remaining -= 1;
                return match &self.state {
                    RleDecoderState::Repeat(v) => Some(*v),
                    RleDecoderState::Literal => {
                        let (vlen, value) = T::unpack(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        Some(value)
                    }
                    RleDecoderState::Null => Some(T::get_null(self.data)),
                    RleDecoderState::Idle => None,
                };
            }
            self.advance_run();
            if self.remaining == 0 {
                return None;
            }
        }
    }

    /// O(runs_skipped) skip — repeat and null runs are skipped in O(1) each,
    /// literal runs advance `byte_pos` via `value_len` without full decoding.
    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        loop {
            if self.remaining == 0 {
                self.advance_run();
                if self.remaining == 0 {
                    return None;
                }
            }

            if n < self.remaining {
                // Target is within this run — skip n items, return the next.
                if let RleDecoderState::Literal = self.state {
                    self.skip_literals(n);
                }
                self.remaining -= n;
                return self.next();
            }

            // Skip past the entire run.
            if let RleDecoderState::Literal = self.state {
                self.skip_literals(self.remaining);
            }
            n -= self.remaining;
            self.remaining = 0;
        }
    }
}

impl<'a, T: RleValue> RunDecoder for RleDecoder<'a, T> {
    fn next_run(&mut self) -> Option<Run<Self::Item>> {
        loop {
            if self.remaining > 0 {
                let count = self.remaining;
                return match &self.state {
                    RleDecoderState::Repeat(v) => {
                        let value = *v;
                        self.remaining = 0;
                        // byte_pos already past the value data for repeat runs
                        Some(Run { count, value })
                    }
                    RleDecoderState::Literal => {
                        // Literal: each item is distinct, yield one at a time
                        self.remaining -= 1;
                        let (vlen, value) = T::unpack(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        Some(Run { count: 1, value })
                    }
                    RleDecoderState::Null => {
                        let value = T::get_null(self.data);
                        self.remaining = 0;
                        Some(Run { count, value })
                    }
                    RleDecoderState::Idle => None,
                };
            }
            self.advance_run();
            if self.remaining == 0 {
                return None;
            }
        }
    }
}

// ── RleEncoding ──────────────────────────────────────────────────────────────

/// RLE encoding strategy — used for all non-boolean column value types.
///
/// This is a zero-sized type; all state lives in the slab bytes.
pub struct RleEncoding<T: RleValue>(PhantomData<fn() -> T>);

impl<T: RleValue> Default for RleEncoding<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: RleValue + ColumnValueRef<Encoding = RleEncoding<T>>> ColumnEncoding for RleEncoding<T> {
    type Value = T;

    fn merge_slabs(a: &mut Slab, b: &Slab) {
        rle_merge_slabs::<T>(a, b)
    }

    fn validate_encoding(slab: &[u8]) -> Result<(), String> {
        rle_validate_encoding::<T>(slab)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(<T as ColumnValueRef>::Get<'a>) -> Option<String>>,
    ) -> Result<Vec<Slab>, PackError> {
        rle_load_and_verify::<T>(data, max_segments, validate)
    }

    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8> {
        rle_streaming_save::<T>(slabs)
    }

    fn splice_slab<V: AsColumnRef<T>>(
        slab: &mut Slab,
        index: usize,
        del: usize,
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<Slab>, usize) {
        let slab_del = del.min(slab.len - index);
        let overflow_del = del - slab_del;
        (
            splice_slab::<T, V>(slab, index, slab_del, values, max_segments),
            overflow_del,
        )
    }

    type Decoder<'a> = RleDecoder<'a, T>;

    fn decoder(slab: &ValidBytes) -> RleDecoder<'_, T> {
        RleDecoder::new(slab)
    }
}

// ── RLE fast splice ─────────────────────────────────────────────────────────

use super::rle_state::{FlushState, RleCow, RleState};

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
struct Prefix<'a, T: RleValue, V: super::AsColumnRef<T>> {
    state: RleState<'a, T, V>,
    segments: usize,
}

impl<'a, T: RleValue, V: super::AsColumnRef<T>> Prefix<'a, T, V> {
    fn new() -> Self {
        Prefix {
            state: RleState::Empty,
            segments: 0,
        }
    }
}

#[derive(Debug)]
struct RlePartition<'a, T: RleValue, V: super::AsColumnRef<T>> {
    outer: Range<usize>,
    prefix: Prefix<'a, T, V>,
    postfix: Option<Postfix<'a, T>>,
}

fn find_partition<'a, T: RleValue, V: super::AsColumnRef<T>>(
    slab: &'a Slab,
    range: Range<usize>,
) -> RlePartition<'a, T, V> {
    use RunDecoder;

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

    //let prefix = Prefix { state: prefix_state, segments: prefix_segments };
    RlePartition {
        outer,
        prefix,
        postfix,
    }
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::v1::rle_state::RleState;
    use crate::v1::ValidBuf;

    fn make_slab(data: Vec<u8>, len: usize) -> Slab {
        let segments = rle_count_segments::<u64>(&data);
        Slab {
            data: ValidBuf::new(data),
            len,
            segments,
        }
    }

    fn encode_u64_slab(vals: &[u64]) -> Slab {
        let mut buf = Vec::new();
        crate::v1::rle_state::rle_encode_state::<u64>(vals.iter().copied(), &mut buf);
        let len = vals.len();
        make_slab(buf, len)
    }

    fn encode_opt_slab(vals: &[Option<u64>]) -> Slab {
        let mut buf = Vec::new();
        crate::v1::rle_state::rle_encode_state::<Option<u64>>(vals.iter().copied(), &mut buf);
        let len = vals.len();
        make_slab(buf, len)
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
            crate::v1::rle_state::rewrite_lit_header(&mut reconstructed_bytes, rw.pos, rw.count);
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
            crate::v1::rle_state::rewrite_lit_header(&mut first, rw.pos, rw.count);
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

#[cfg(test)]
fn state_item_count<T: RleValue, V: super::AsColumnRef<T>>(state: &RleState<'_, T, V>) -> usize {
    match state {
        RleState::Empty => 0,
        RleState::Lone(_) => 1,
        RleState::Run(n, _) => *n,
        RleState::Lit { count, .. } => count + 1,
        RleState::Null(n) => *n,
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
}

/// Build the splice buffer. Borrows slab immutably; returns owned output.
/// After this, caller does: `slab.data.splice(result.range, result.bytes)`,
/// applies rewrite, sets slab.len and slab.segments.
fn build_splice_buf<T: RleValue, V: super::AsColumnRef<T>>(
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

    // 1. Feed new values.
    for v in values {
        if starting_segments + f.segments + state.pending_segments() >= max_segments {
            f += state.flush(&mut buf);
            if !overflowed {
                overflowed = true;
                result.bytes = std::mem::take(&mut buf);
                result.len = index + inserted;
                result.segments = p.prefix.segments + f.segments;
                result.rewrite = f.rewrite;
            } else {
                result.overflow.push(Slab {
                    data: ValidBuf::new(std::mem::take(&mut buf)),
                    len: inserted,
                    segments: f.segments,
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
    } else {
        // the postfix goes on the final slab
        buf.extend_from_slice(&slab.data[result.range.end..]);
        result.range.end = slab.data.len();

        let postfix_count = slab.len - index - del;

        result.overflow.push(Slab {
            data: ValidBuf::new(std::mem::take(&mut buf)),
            len: inserted + postfix_count,
            segments: f.segments + postfix_segments,
        });
    }

    #[cfg(debug_assertions)]
    for s in &result.overflow {
        s.validate::<T>();
    }

    result
}

pub(crate) fn splice_slab<T: RleValue, V: super::AsColumnRef<T>>(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = V>,
    max_segments: usize,
) -> Vec<Slab> {
    assert!(index + del <= slab.len, "del extends beyond slab");

    let result = build_splice_buf::<T, V>(slab, index, del, values, max_segments);

    let slab_data = slab.data.as_mut_vec();
    slab_data.splice(result.range, result.bytes);

    if let Some(rw) = result.rewrite {
        super::rle_state::rewrite_lit_header(slab_data, rw.pos, rw.count);
    }

    slab.len = result.len;
    slab.segments = result.segments;

    #[cfg(debug_assertions)]
    slab.validate::<T>();

    result.overflow
}

// ── streaming_save ───────────────────────────────────────────────────────────

/// Find the byte offset of the last run in `slab`.  Returns `None` only if
/// `slab` is empty.
fn last_run_start<T: RleValue>(slab: &[u8]) -> Option<usize> {
    let mut pos = 0;
    let mut last = 0;
    while pos < slab.len() {
        last = pos;
        let (cb, raw) = read_signed(&slab[pos..])?;
        match raw {
            n if n > 0 => {
                let vl = T::value_len(&slab[pos + cb..])?;
                pos += cb + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut sb = pos + cb;
                for _ in 0..total {
                    let vl = T::value_len(&slab[sb..])?;
                    sb += vl;
                }
                pos = sb;
            }
            _ => {
                let (ncb, _) = read_unsigned(&slab[pos + cb..])?;
                pos += cb + ncb;
            }
        }
    }
    Some(last)
}

/// End byte offset of the first run in `slab`.
fn first_run_end<T: RleValue>(slab: &[u8]) -> usize {
    if slab.is_empty() {
        return 0;
    }
    let (cb, raw) = read_signed(slab).unwrap();
    match raw {
        n if n > 0 => {
            let vl = T::value_len(&slab[cb..]).unwrap();
            cb + vl
        }
        n if n < 0 => {
            let total = (-n) as usize;
            let mut sb = cb;
            for _ in 0..total {
                let vl = T::value_len(&slab[sb..]).unwrap();
                sb += vl;
            }
            sb
        }
        _ => {
            let (ncb, _) = read_unsigned(&slab[cb..]).unwrap();
            cb + ncb
        }
    }
}

/// Fast in-place merge of slab `b` into slab `a`. Only examines the
/// boundary runs (last of `a`, first of `b`). Interior bytes are memcopied.
///
/// Both slabs must be non-empty.
fn rle_merge_slabs<T: RleValue>(a: &mut Slab, b: &Slab) {
    debug_assert!(a.len > 0 && b.len > 0);

    // Use find_partition to get:
    //   - a's last boundary value as a prefix state
    //   - b's first run as a postfix
    // Then let the state machine handle all boundary merging.

    let (buf, a_outer_start, a_prefix_segs, b_outer_end, postfix_segs, rewrite) = {
        let pa = find_partition::<T, T>(a, a.len..a.len);
        let pb = find_partition::<T, T>(b, 0..0);

        let mut buf = Vec::new();
        let mut state = pa.prefix.state;
        let (f, postfix_segs) = state.flush_postfix(&mut buf, pb.postfix);

        (
            buf,
            pa.outer.start,
            pa.prefix.segments,
            pb.outer.end,
            postfix_segs,
            f,
        )
    };
    // All borrows from a/b are now dropped.

    let a_buf = a.data.as_mut_vec();
    a_buf.truncate(a_outer_start);
    a_buf.extend_from_slice(&buf);
    a_buf.extend_from_slice(&b.data[b_outer_end..]);

    if let Some(rw) = rewrite.rewrite {
        super::rle_state::rewrite_lit_header(a_buf, rw.pos, rw.count);
    }

    a.segments = a_prefix_segs + rewrite.segments + postfix_segs;
    a.len += b.len;
}

#[allow(dead_code)]
enum ParsedRun {
    Repeat { count: usize, value: Vec<u8> },
    Literal { values: Vec<Vec<u8>> },
    Null { count: usize },
}

#[allow(dead_code)]
fn rle_merge_slab_bytes<T: RleValue>(a: &[u8], b: &[u8]) -> (Vec<u8>, usize) {
    if a.is_empty() {
        let segs = rle_count_segments::<T>(b);
        return (b.to_vec(), segs);
    }
    if b.is_empty() {
        let segs = rle_count_segments::<T>(a);
        return (a.to_vec(), segs);
    }

    // Locate the last run in `a` and the first run in `b`.
    let a_last = last_run_start::<T>(a).unwrap();
    let b_first_end = first_run_end::<T>(b);

    let a_interior = &a[..a_last];
    let b_rest = &b[b_first_end..];
    let a_last_bytes = &a[a_last..];
    let b_first_bytes = &b[..b_first_end];

    // Count segments for interior portions.
    let a_interior_segs = rle_count_segments::<T>(a_interior);
    let b_rest_segs = rle_count_segments::<T>(b_rest);

    // Parse only the two boundary runs.
    let a_run = parse_one_run::<T>(a_last_bytes);
    let b_run = parse_one_run::<T>(b_first_bytes);

    // Try to merge them.
    let merged = merge_two_runs(a_run, b_run);

    // Count segments in the merged boundary.
    let boundary_segs: usize = merged.iter().map(run_segments).sum();

    let mut result = Vec::with_capacity(a.len() + b.len());
    result.extend_from_slice(a_interior);
    for run in &merged {
        encode_one_run(run, &mut result);
    }
    result.extend_from_slice(b_rest);
    (result, a_interior_segs + boundary_segs + b_rest_segs)
}

/// Count segments in a single parsed run.
#[allow(dead_code)]
fn run_segments(run: &ParsedRun) -> usize {
    match run {
        ParsedRun::Repeat { .. } => 1,
        ParsedRun::Literal { values } => values.len(),
        ParsedRun::Null { .. } => 1,
    }
}

/// Parse a single run from the start of `data`.
#[allow(dead_code)]
fn parse_one_run<T: RleValue>(data: &[u8]) -> ParsedRun {
    let (cb, raw) = read_signed(data).unwrap();
    match raw {
        n if n > 0 => {
            let count = n as usize;
            let vs = cb;
            let vl = T::value_len(&data[vs..]).unwrap();
            ParsedRun::Repeat {
                count,
                value: data[vs..vs + vl].to_vec(),
            }
        }
        n if n < 0 => {
            let total = (-n) as usize;
            let mut values = Vec::with_capacity(total);
            let mut sb = cb;
            for _ in 0..total {
                let vl = T::value_len(&data[sb..]).unwrap();
                values.push(data[sb..sb + vl].to_vec());
                sb += vl;
            }
            ParsedRun::Literal { values }
        }
        _ => {
            let (_, nc) = read_unsigned(&data[cb..]).unwrap();
            ParsedRun::Null { count: nc as usize }
        }
    }
}

/// Encode a single parsed run into `out`.
#[allow(dead_code)]
fn encode_one_run(run: &ParsedRun, out: &mut Vec<u8>) {
    match run {
        ParsedRun::Repeat { count, value } => {
            out.extend(value_run_header(*count));
            out.extend_from_slice(value);
        }
        ParsedRun::Literal { values } => {
            out.extend(encode_signed(-(values.len() as i64)));
            for v in values {
                out.extend_from_slice(v);
            }
        }
        ParsedRun::Null { count } => {
            out.extend(null_run_bytes(*count));
        }
    }
}

/// Merge two adjacent parsed runs into 1–3 canonical runs.
#[allow(dead_code)]
fn merge_two_runs(a: ParsedRun, b: ParsedRun) -> Vec<ParsedRun> {
    match (a, b) {
        // Null + Null → merge
        (ParsedRun::Null { count: c1 }, ParsedRun::Null { count: c2 }) => {
            vec![ParsedRun::Null { count: c1 + c2 }]
        }

        // Repeat + Repeat, same value → merge
        (
            ParsedRun::Repeat {
                count: c1,
                value: v1,
            },
            ParsedRun::Repeat {
                count: c2,
                value: v2,
            },
        ) if v1 == v2 => {
            vec![ParsedRun::Repeat {
                count: c1 + c2,
                value: v1,
            }]
        }

        // Repeat + Literal starting with same value → absorb first literal item
        (ParsedRun::Repeat { count, value }, ParsedRun::Literal { mut values })
            if !values.is_empty() && values[0] == value =>
        {
            values.remove(0);
            let mut result = vec![ParsedRun::Repeat {
                count: count + 1,
                value,
            }];
            if !values.is_empty() {
                result.push(ParsedRun::Literal { values });
            }
            result
        }

        // Literal ending with same value as Repeat → absorb last literal item
        (ParsedRun::Literal { mut values }, ParsedRun::Repeat { count, value })
            if !values.is_empty() && *values.last().unwrap() == value =>
        {
            values.pop();
            let mut result = vec![];
            if !values.is_empty() {
                result.push(ParsedRun::Literal { values });
            }
            result.push(ParsedRun::Repeat {
                count: count + 1,
                value,
            });
            result
        }

        // Literal + Literal → merge, then canonicalize
        (ParsedRun::Literal { values: mut v1 }, ParsedRun::Literal { values: v2 }) => {
            v1.extend(v2);
            canonicalize_literal(v1)
        }

        // Repeat(count=1) is actually a literal — handle boundary with lit
        // This shouldn't happen with our encoding (count=1 → literal), but be safe.

        // No merge possible — emit both unchanged.
        (a, b) => vec![a, b],
    }
}

/// Canonicalize a merged literal run: extract leading/trailing/internal repeats.
#[allow(dead_code)]
fn canonicalize_literal(values: Vec<Vec<u8>>) -> Vec<ParsedRun> {
    if values.is_empty() {
        return vec![];
    }
    let mut result: Vec<ParsedRun> = vec![];
    let mut i = 0;
    while i < values.len() {
        // Count consecutive equal values.
        let mut count = 1;
        while i + count < values.len() && values[i + count] == values[i] {
            count += 1;
        }
        if count >= 2 {
            result.push(ParsedRun::Repeat {
                count,
                value: values[i].clone(),
            });
            i += count;
        } else {
            // Collect a literal run of distinct values.
            let start = i;
            i += 1;
            while i < values.len() {
                if i + 1 < values.len() && values[i] == values[i + 1] {
                    break;
                }
                i += 1;
            }
            let lit_values: Vec<Vec<u8>> = values[start..i].to_vec();
            // Try to merge with a preceding literal.
            if let Some(ParsedRun::Literal { values: prev_vals }) = result.last_mut() {
                prev_vals.extend(lit_values);
            } else {
                result.push(ParsedRun::Literal { values: lit_values });
            }
        }
    }
    result
}

// ── streaming_save ───────────────────────────────────────────────────────────

/// Serialize multiple RLE slabs into one canonical byte array in O(n).
///
/// Processes runs from all slabs sequentially, maintaining a pending tail
/// run that accumulates adjacent compatible runs.  Each value byte is
/// visited at most twice (once to parse, once to write), giving O(n) total.
fn rle_streaming_save<T: RleValue>(slabs: &[&[u8]]) -> Vec<u8> {
    if slabs.is_empty() {
        return vec![];
    }
    if slabs.len() == 1 {
        return slabs[0].to_vec();
    }

    let total_bytes: usize = slabs.iter().map(|s| s.len()).sum();
    let mut out = Vec::with_capacity(total_bytes);

    // Pending tail state.  For a literal, value bytes accumulate in `p_lit_buf`
    // (without header) and `p_value` holds the last value.  For a repeat,
    // `p_value` holds the repeated value.  For null, `p_value` is unused.
    #[derive(PartialEq)]
    enum PK {
        None,
        Repeat,
        Literal,
        Null,
    }
    let mut p_kind = PK::None;
    let mut p_count: usize = 0;
    let mut p_value: Vec<u8> = Vec::new();
    let mut p_lit_buf: Vec<u8> = Vec::new();

    macro_rules! flush {
        () => {{
            match p_kind {
                PK::None => {}
                PK::Repeat => {
                    out.extend(encode_signed(p_count as i64));
                    out.extend_from_slice(&p_value);
                }
                PK::Literal => {
                    if p_count > 0 {
                        out.extend(encode_signed(-(p_count as i64)));
                        out.extend_from_slice(&p_lit_buf);
                    }
                }
                PK::Null => {
                    out.extend(encode_signed(0));
                    out.extend(encode_unsigned(p_count as u64));
                }
            }
            #[allow(unused_assignments)]
            {
                p_kind = PK::None;
            }
            #[allow(unused_assignments)]
            {
                p_count = 0;
            }
            p_value.clear();
            p_lit_buf.clear();
        }};
    }

    for &slab in slabs {
        let mut pos = 0;
        while pos < slab.len() {
            let (cb, raw) = read_signed(&slab[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    // ── Repeat run ────────────────────────────────
                    let count = n as usize;
                    let vs = pos + cb;
                    let vl = T::value_len(&slab[vs..]).unwrap();
                    let value = &slab[vs..vs + vl];
                    pos = vs + vl;

                    if p_kind == PK::Repeat && p_value == value {
                        p_count += count;
                    } else if p_kind == PK::Literal && p_count > 0 && p_value == value {
                        // Last literal value == repeat value: pop, flush, repeat
                        p_lit_buf.truncate(p_lit_buf.len() - p_value.len());
                        p_count -= 1;
                        if p_count > 0 {
                            let save_val = std::mem::take(&mut p_value);
                            flush!();
                            p_value = save_val;
                        }
                        p_kind = PK::Repeat;
                        p_count = count + 1;
                        // p_value already holds the right value
                    } else {
                        flush!();
                        p_kind = PK::Repeat;
                        p_count = count;
                        p_value.clear();
                        p_value.extend_from_slice(value);
                    }
                }
                n if n < 0 => {
                    // ── Literal run ───────────────────────────────
                    let total = (-n) as usize;
                    let lit_start = pos + cb;

                    // Parse the first value to check boundary.
                    let first_vl = T::value_len(&slab[lit_start..]).unwrap();
                    let first_value = &slab[lit_start..lit_start + first_vl];

                    let absorbed_first = if p_kind == PK::Repeat && p_value == first_value {
                        p_count += 1;
                        true
                    } else if p_kind == PK::Literal && p_count > 0 && p_value == first_value {
                        // Pop last literal value, flush, start repeat(2, v)
                        p_lit_buf.truncate(p_lit_buf.len() - p_value.len());
                        p_count -= 1;
                        if p_count > 0 {
                            let save_val = std::mem::take(&mut p_value);
                            flush!();
                            p_value = save_val;
                        }
                        p_kind = PK::Repeat;
                        p_count = 2;
                        // p_value already set
                        true
                    } else {
                        false
                    };

                    let (vals_start, vals_count) = if absorbed_first {
                        (lit_start + first_vl, total - 1)
                    } else {
                        (lit_start, total)
                    };

                    if vals_count > 0 {
                        // Walk to find the last value's start and the total byte span.
                        let mut walk = vals_start;
                        for _ in 0..vals_count - 1 {
                            walk += T::value_len(&slab[walk..]).unwrap();
                        }
                        let last_vs = walk;
                        let last_vl = T::value_len(&slab[walk..]).unwrap();
                        walk += last_vl;
                        let vals_end = walk;

                        if p_kind == PK::Literal {
                            // Extend existing literal.
                            p_lit_buf.extend_from_slice(&slab[vals_start..vals_end]);
                            p_count += vals_count;
                            p_value.clear();
                            p_value.extend_from_slice(&slab[last_vs..last_vs + last_vl]);
                        } else {
                            // Flush pending (repeat/null/none), start new literal.
                            flush!();
                            p_kind = PK::Literal;
                            p_count = vals_count;
                            p_lit_buf.extend_from_slice(&slab[vals_start..vals_end]);
                            p_value.extend_from_slice(&slab[last_vs..last_vs + last_vl]);
                        }
                        pos = vals_end;
                    } else {
                        // All values absorbed (literal of 1 that matched pending).
                        pos = lit_start + first_vl;
                    }
                }
                _ => {
                    // ── Null run ──────────────────────────────────
                    let (ncb, nc) = read_unsigned(&slab[pos + cb..]).unwrap();
                    let count = nc as usize;
                    pos += cb + ncb;

                    if p_kind == PK::Null {
                        p_count += count;
                    } else {
                        flush!();
                        p_kind = PK::Null;
                        p_count = count;
                    }
                }
            }
        }
    }
    flush!();
    out
}

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
pub(crate) fn rle_validate_encoding<T: RleValue>(slab: &[u8]) -> Result<(), String> {
    if slab.is_empty() {
        return Ok(());
    }

    // Parse all runs and their value bytes for comparison.
    enum Run {
        Repeat { count: usize, value: Vec<u8> },
        Literal { values: Vec<Vec<u8>> },
        Null { count: usize },
    }

    let mut runs: Vec<Run> = vec![];
    let mut pos = 0;
    while pos < slab.len() {
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
                }
                runs.push(Run::Literal { values });
                pos = sb;
            }
            _ => {
                let (ncb, nc) = read_unsigned(&slab[pos + cb..])
                    .ok_or_else(|| format!("truncated null count at byte {}", pos + cb))?;
                runs.push(Run::Null { count: nc as usize });
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

    Ok(())
}

// ── Load & verify ─────────────────────────────────────────────────────────

/// Decode and validate RLE-encoded bytes, splitting into slabs.
///
/// Walks every run, validates with try_unpack, and splits into slabs by
/// copying byte ranges. No re-encoding except when splitting a literal
/// run (which requires rewriting the count header for each piece).
fn rle_load_and_verify<T: RleValue>(
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
    fn flush(
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
        slabs.push(Slab {
            data: ValidBuf::new(d),
            len: *slab_items,
            segments: *slab_segs,
        });
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
                flush(
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
                    slabs.push(Slab {
                        data: ValidBuf::new(d),
                        len: slab_items + room,
                        segments: slab_segs + room,
                    });
                    consumed += room;
                } else if slab_segs > 0 {
                    flush(
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
                    slabs.push(Slab {
                        data: ValidBuf::new(d),
                        len: max_segments,
                        segments: max_segments,
                    });
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
                if let Some(m) = v(T::get_null(ValidBytes::from_bytes(data))) {
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
                flush(
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

    flush(
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

#[cfg(test)]
mod load_verify_tests {
    use super::*;
    use crate::v1::rle_state::rle_encode_state;

    #[test]
    fn load_verify_roundtrip() {
        let mut buf = Vec::new();
        rle_encode_state::<u64>((0..1000u64).map(|i| i % 7), &mut buf);
        let slabs = rle_load_and_verify::<u64>(&buf, 16, None).unwrap();
        let total: usize = slabs.iter().map(|s| s.len).sum();
        assert_eq!(total, 1000);
        let vals: Vec<u64> = slabs
            .iter()
            .flat_map(|s| RleDecoder::<u64>::new(&s.data))
            .collect();
        let expected: Vec<u64> = (0..1000u64).map(|i| i % 7).collect();
        assert_eq!(vals, expected);
        for (i, s) in slabs.iter().enumerate() {
            assert!(
                rle_validate_encoding::<u64>(&s.data).is_ok(),
                "slab {i} invalid"
            );
            assert!(s.segments <= 16, "slab {i} exceeds max_segments");
        }
    }

    #[test]
    fn load_verify_rejects_null_in_non_nullable() {
        // Encode a null run (0, count=1) into raw bytes.
        let mut buf = Vec::new();
        buf.extend(encode_signed(0));
        buf.extend(encode_unsigned(1));
        assert!(rle_load_and_verify::<u64>(&buf, 16, None).is_err());
    }

    #[test]
    fn load_verify_with_validate_fn() {
        let mut buf = Vec::new();
        rle_encode_state::<u64>([1u64, 2, 3, 999].into_iter(), &mut buf);
        let result = rle_load_and_verify::<u64>(
            &buf,
            16,
            Some(|v| {
                if v > 100 {
                    Some(format!("too large: {v}"))
                } else {
                    None
                }
            }),
        );
        assert!(result.is_err());
    }
}
