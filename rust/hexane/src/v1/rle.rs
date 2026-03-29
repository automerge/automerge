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

/// Walk `slab` linearly until the run containing logical item `target` is
/// found.  Returns `None` if out of bounds, `Some(None)` for a null item,
/// or `Some(Some(byte_offset))` for a non-null item.
fn scan_to<T: RleValue>(slab: &[u8], target: usize) -> Option<Option<usize>> {
    let mut byte_pos = 0;
    let mut item_pos = 0;

    while byte_pos < slab.len() {
        let (count_bytes, count_raw) = read_signed(&slab[byte_pos..])?;

        match count_raw {
            // ── Repeat run ────────────────────────────────────────────────
            n if n > 0 => {
                let count = n as usize;
                let value_start = byte_pos + count_bytes;
                let value_len = T::value_len(&slab[value_start..])?;

                if target < item_pos + count {
                    return Some(Some(value_start));
                }

                item_pos += count;
                byte_pos = value_start + value_len;
            }

            // ── Literal run ───────────────────────────────────────────────
            n if n < 0 => {
                let total = (-n) as usize;
                let mut scan_byte = byte_pos + count_bytes;

                for i in 0..total {
                    let vstart = scan_byte;
                    let vlen = T::value_len(&slab[scan_byte..])?;
                    if item_pos + i == target {
                        return Some(Some(vstart));
                    }
                    scan_byte += vlen;
                }

                item_pos += total;
                byte_pos = scan_byte;
            }

            // ── Null run ──────────────────────────────────────────────────
            _ => {
                let (null_count_bytes, null_count) =
                    read_unsigned(&slab[byte_pos + count_bytes..])?;
                let null_count = null_count as usize;

                if target < item_pos + null_count {
                    return Some(None);
                }

                item_pos += null_count;
                byte_pos += count_bytes + null_count_bytes;
            }
        }
    }

    None // target >= len
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Encode a value-run count header.  A single item is stored as a literal
/// run (`-1`) rather than a repeat run (`+1`) so that every repeat run has
/// count >= 2.
fn value_run_header(count: usize) -> Leb128Buf {
    if count == 1 {
        encode_signed(-1)
    } else {
        encode_signed(count as i64)
    }
}

/// Stack-buffered null run: marker (0) + unsigned count. Max 20 bytes.
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

/// Append a single value as `lit-1` to `left`, merging into the trailing
/// literal if `prev_is_literal` is `Some((run_start, count))`.
fn merge_lit1_into_left(left: &mut Vec<u8>, value: &[u8], prev_literal: Option<(usize, usize)>) {
    if let Some((run_start, old_count)) = prev_literal {
        // Trailing literal — merge by bumping count and appending value.
        let old_hdr = encode_signed(-(old_count as i64));
        let new_hdr = encode_signed(-((old_count + 1) as i64));
        left.splice(run_start..run_start + old_hdr.len(), new_hdr);
        left.extend_from_slice(value);
    } else {
        // No trailing literal — emit a new lit-1.
        left.extend(encode_signed(-1));
        left.extend_from_slice(value);
    }
}

/// Build the start of `right` with a `lit-1` for `value`, merging with the
/// following literal in `rest` if present.
fn merge_lit1_into_right<T: RleValue>(right: &mut Vec<u8>, value: &[u8], rest: &[u8]) {
    if !rest.is_empty() {
        if let Some((next_hl, next_hv)) = read_signed(rest) {
            if next_hv < 0 {
                // Next run is a literal — merge.
                let next_n = (-next_hv) as usize;
                let merged_count = 1 + next_n;
                right.extend(encode_signed(-(merged_count as i64)));
                right.extend_from_slice(value);
                // Skip next literal's header, copy its values and the rest.
                let mut next_vals_end = next_hl;
                for _ in 0..next_n {
                    let vl = T::value_len(&rest[next_vals_end..]).unwrap();
                    next_vals_end += vl;
                }
                right.extend_from_slice(&rest[next_hl..next_vals_end]);
                right.extend_from_slice(&rest[next_vals_end..]);
                return;
            }
        }
    }
    // No following literal — emit lit-1 + rest.
    right.extend(encode_signed(-1));
    right.extend_from_slice(value);
    right.extend_from_slice(rest);
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
    /// Uses `value_len` (reads only the length header) rather than `unpack`.
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

    fn get<'a>(slab: &'a ValidBytes, index: usize, len: usize) -> Option<T::Get<'a>> {
        if index >= len {
            return None;
        }
        match scan_to::<T>(slab, index)? {
            None => Some(T::get_null(slab)),
            Some(offset) => {
                let (_, v) = T::unpack(&slab[offset..]);
                Some(v)
            }
        }
    }

    fn count_segments(slab: &[u8]) -> usize {
        rle_count_segments::<T>(slab)
    }

    fn split_at_item(slab: &[u8], index: usize, len: usize) -> (Vec<u8>, Vec<u8>) {
        rle_split_at_item::<T>(slab, index, len)
    }

    fn merge_slab_bytes(a: &[u8], b: &[u8]) -> (Vec<u8>, usize) {
        rle_merge_slab_bytes::<T>(a, b)
    }

    fn validate_encoding(slab: &[u8]) -> Result<(), String> {
        rle_validate_encoding::<T>(slab)
    }

    fn encode_all_slabs<V: AsColumnRef<T>>(
        values: impl Iterator<Item = V>,
        max_segments: usize,
    ) -> (Vec<(Vec<u8>, usize, usize)>, usize) {
        rle_encode_all_slabs::<T, V>(values, max_segments)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(<T as ColumnValueRef>::Get<'a>) -> Option<String>>,
    ) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError> {
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

use super::rle_state::{FlushState, RleState};

///// Postfix: what comes after the deleted range in the same/adjacent run(s).
/// `segments` = segment count from outer.end to the end of the slab.
#[derive(Debug)]
enum Postfix<'a, T: RleValue> {
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
struct Prefix<'a, T: RleValue> {
    state: RleState<'a, T>,
    segments: usize, // segments of data[..outer.start]
}

impl<'a, T: RleValue> Prefix<'a, T> {
    fn new() -> Self {
        Prefix {
            state: RleState::Empty,
            segments: 0,
        }
    }
}

#[derive(Debug)]
struct RlePartition<'a, T: RleValue> {
    outer: Range<usize>, // byte range to splice: data[outer] gets replaced by buf
    prefix: Prefix<'a, T>,
    postfix: Option<Postfix<'a, T>>,
}

fn find_partition<'a, T: RleValue>(slab: &'a Slab, range: Range<usize>) -> RlePartition<'a, T> {
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
                prefix.state = RleState::lit(count, run.value, header_pos);
            } else if is_null {
                prefix.state = RleState::Null(k);
            } else if k == 1 && !is_lit && was_lit {
                let count = segments - lit_segments_before;
                prefix.state = RleState::lit(count, run.value, header_pos);
            } else {
                prefix.state = RleState::run(k, run.value);
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
        let p = find_partition::<u64>(&slab, 2..3);
        match &p.prefix.state {
            RleState::Run(2, v) => assert_eq!(*v, 7),
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
        let p = find_partition::<u64>(&slab, 2..3);
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
        let p = find_partition::<Option<u64>>(&slab, 2..3);
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
        let p = find_partition::<u64>(&slab, 3..3);
        match &p.prefix.state {
            RleState::Run(3, v) => assert_eq!(*v, 1),
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
        let p = find_partition::<u64>(&slab, 0..1);
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
        let p = find_partition::<u64>(&slab, 3..3);
        assert_eq!(state_item_count(&p.prefix.state), 3);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn delete_all() {
        let slab = encode_u64_slab(&[1, 2, 3]);
        let p = find_partition::<u64>(&slab, 0..3);
        assert_eq!(state_item_count(&p.prefix.state), 0);
        assert!(p.postfix.is_none());
    }

    #[test]
    fn insert_mid_repeat() {
        let slab = encode_u64_slab(&[7, 7, 7, 7]);
        let p = find_partition::<u64>(&slab, 2..2);
        match &p.prefix.state {
            RleState::Run(2, v) => assert_eq!(*v, 7),
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
fn state_item_count<T: RleValue>(state: &RleState<'_, T>) -> usize {
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
    // Collect values so they outlive the state machine (needed for &str lifetimes).
    let collected: Vec<V> = values.collect();

    let p = find_partition::<T>(slab, index..index + del);

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
    for v in &collected {
        if starting_segments + f.segments >= max_segments {
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
        f += state.append(&mut buf, v.as_column_ref());
    }

    // 2. Feed postfix + flush.
    let postfix_segments = match p.postfix {
        None => {
            f += state.flush(&mut buf);
            0
        }
        Some(Postfix::Run {
            count,
            value,
            segments,
        }) => {
            f += state.append_n(&mut buf, value, count);
            f += state.flush(&mut buf);
            segments
        }
        Some(Postfix::Lit {
            value,
            lit,
            segments,
        }) => {
            f += state.append(&mut buf, value);
            f += state.flush_with_lit(&mut buf, lit);
            segments
        }
        Some(Postfix::LonePlusLit {
            lone,
            value,
            lit,
            segments,
        }) => {
            f += state.append(&mut buf, lone);
            f += state.append(&mut buf, value);
            f += state.flush_with_lit(&mut buf, lit);
            segments
        }
    };

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

// ── count_segments ───────────────────────────────────────────────────────────

/// Count segments in an RLE slab. A repeat run = 1 segment, a null run = 1
/// segment, a literal of N = N segments.
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

// ── split_at_item ────────────────────────────────────────────────────────────

/// Split an RLE slab at logical item `index` into two byte arrays.
fn rle_split_at_item<T: RleValue>(slab: &[u8], index: usize, len: usize) -> (Vec<u8>, Vec<u8>) {
    if index == 0 {
        return (vec![], slab.to_vec());
    }
    if index >= len {
        return (slab.to_vec(), vec![]);
    }

    // Walk to find the run containing `index`.
    // Track whether the previous run was a literal, for merging when a
    // repeat split produces a lit-1 adjacent to it.
    let mut byte_pos = 0;
    let mut item_pos = 0;
    // (run_start_in_left, item_count) of the last literal run, if the
    // immediately preceding run is a literal.
    let mut prev_literal: Option<(usize, usize)> = None;

    while byte_pos < slab.len() {
        let (count_bytes, count_raw) = read_signed(&slab[byte_pos..]).unwrap();

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let value_start = byte_pos + count_bytes;
                let value_len = T::value_len(&slab[value_start..]).unwrap();
                let run_end = value_start + value_len;

                if index < item_pos + count {
                    let k = index - item_pos;
                    if k == 0 {
                        // Split at run boundary (before this run).
                        return (slab[..byte_pos].to_vec(), slab[byte_pos..].to_vec());
                    }
                    // Mid-run split.
                    let value_bytes = &slab[value_start..value_start + value_len];
                    let mut left = slab[..byte_pos].to_vec();
                    // When k == 1, this produces lit-1 which might be
                    // adjacent to a preceding literal — merge if needed.
                    if k == 1 {
                        merge_lit1_into_left(&mut left, value_bytes, prev_literal);
                    } else {
                        left.extend(value_run_header(k));
                        left.extend_from_slice(value_bytes);
                    }

                    let remaining = count - k;
                    let mut right = vec![];
                    // When remaining == 1, this produces lit-1 which might
                    // be adjacent to a following literal — merge if needed.
                    if remaining == 1 {
                        merge_lit1_into_right::<T>(&mut right, value_bytes, &slab[run_end..]);
                    } else {
                        right.extend(value_run_header(remaining));
                        right.extend_from_slice(value_bytes);
                        right.extend_from_slice(&slab[run_end..]);
                    }

                    return (left, right);
                }
                if index == item_pos + count {
                    // Split at run boundary (after this run).
                    return (slab[..run_end].to_vec(), slab[run_end..].to_vec());
                }

                item_pos += count;
                byte_pos = run_end;
                prev_literal = None;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let first_val_start = byte_pos + count_bytes;

                if index < item_pos + total {
                    let k = index - item_pos;
                    if k == 0 {
                        return (slab[..byte_pos].to_vec(), slab[byte_pos..].to_vec());
                    }
                    // Scan to position k to find the split byte offset.
                    let mut scan_byte = first_val_start;
                    for _ in 0..k {
                        let vlen = T::value_len(&slab[scan_byte..]).unwrap();
                        scan_byte += vlen;
                    }
                    let split_byte = scan_byte; // byte offset of first right value

                    // Scan remaining values to find run_end.
                    for _ in k..total {
                        let vlen = T::value_len(&slab[scan_byte..]).unwrap();
                        scan_byte += vlen;
                    }
                    let run_end = scan_byte;

                    // Split literal at position k.
                    let mut left = slab[..byte_pos].to_vec();
                    let left_hdr = encode_signed(-(k as i64));
                    left.extend(left_hdr);
                    left.extend_from_slice(&slab[first_val_start..split_byte]);

                    let remaining = total - k;
                    let mut right = vec![];
                    let right_hdr = encode_signed(-(remaining as i64));
                    right.extend(right_hdr);
                    right.extend_from_slice(&slab[split_byte..run_end]);
                    right.extend_from_slice(&slab[run_end..]);

                    return (left, right);
                }

                // Not in this run — skip past it.
                let mut scan_byte = first_val_start;
                for _ in 0..total {
                    let vlen = T::value_len(&slab[scan_byte..]).unwrap();
                    scan_byte += vlen;
                }

                item_pos += total;
                prev_literal = Some((byte_pos, total));
                byte_pos = scan_byte;
            }
            _ => {
                // Null run.
                let (ncb, null_count) = read_unsigned(&slab[byte_pos + count_bytes..]).unwrap();
                let null_count = null_count as usize;
                let run_end = byte_pos + count_bytes + ncb;

                if index < item_pos + null_count {
                    let k = index - item_pos;
                    if k == 0 {
                        return (slab[..byte_pos].to_vec(), slab[byte_pos..].to_vec());
                    }
                    let mut left = slab[..byte_pos].to_vec();
                    left.extend(null_run_bytes(k));

                    let remaining = null_count - k;
                    let nrb = null_run_bytes(remaining);
                    let mut right = Vec::with_capacity(nrb.len() + slab.len() - run_end);
                    right.extend_from_slice(&nrb);
                    right.extend_from_slice(&slab[run_end..]);

                    return (left, right);
                }

                item_pos += null_count;
                byte_pos = run_end;
                prev_literal = None;
            }
        }
    }

    // Shouldn't reach here if index < len, but fallback.
    (slab.to_vec(), vec![])
}

// ── split canonicalization ────────────────────────────────────────────────────

/// Parsed RLE run for canonicalization.
enum ParsedRun {
    Repeat { count: usize, value: Vec<u8> },
    Literal { values: Vec<Vec<u8>> },
    Null { count: usize },
}

// ── merge_slab_bytes ─────────────────────────────────────────────────────────

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

/// Merge two RLE slabs, decoding only the boundary runs and memcopying
/// interiors.
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
fn run_segments(run: &ParsedRun) -> usize {
    match run {
        ParsedRun::Repeat { .. } => 1,
        ParsedRun::Literal { values } => values.len(),
        ParsedRun::Null { .. } => 1,
    }
}

/// Parse a single run from the start of `data`.
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

// ── Streaming encoder ────────────────────────────────────────────────────────

/// Pack entry: (offset_in_pack_buf, byte_length). Length 0 with !is_value = null.
struct PackEntry {
    offset: u32,
    len: u16,
    is_value: bool,
}

/// Encode values into pre-split slabs in a single O(n) pass.
///
/// Phase 1: Pack all values into a flat buffer (one allocation).
/// Phase 2: Greedy scan emitting runs directly, cutting slabs when the
///           segment budget is reached.
fn rle_encode_all_slabs<T: RleValue, V: super::AsColumnRef<T>>(
    values: impl Iterator<Item = V>,
    max_segments: usize,
) -> (Vec<(Vec<u8>, usize, usize)>, usize) {
    // Phase 1: Pack into a flat buffer.
    let mut pack_buf = Vec::new();
    let mut entries = Vec::new();
    for value in values {
        let start = pack_buf.len();
        let is_value = T::pack(value.as_column_ref(), &mut pack_buf);
        let len = pack_buf.len() - start;
        entries.push(PackEntry {
            offset: start as u32,
            len: len as u16,
            is_value,
        });
    }

    let n = entries.len();
    if n == 0 {
        return (vec![], 0);
    }

    let val_bytes = |idx: usize| -> &[u8] {
        let e = &entries[idx];
        &pack_buf[e.offset as usize..e.offset as usize + e.len as usize]
    };

    // Phase 2: Greedy scan, emit runs, cut into slabs.
    let mut slabs: Vec<(Vec<u8>, usize, usize)> = Vec::new();
    let mut cur = Vec::new();
    let mut cur_items: usize = 0;
    let mut cur_segs: usize = 0;

    // Cut the current slab if adding `run_segs` would exceed the budget.
    let maybe_cut = |slabs: &mut Vec<(Vec<u8>, usize, usize)>,
                     cur: &mut Vec<u8>,
                     cur_items: &mut usize,
                     cur_segs: &mut usize,
                     run_segs: usize| {
        if *cur_segs > 0 && *cur_segs + run_segs > max_segments {
            slabs.push((std::mem::take(cur), *cur_items, *cur_segs));
            *cur_items = 0;
            *cur_segs = 0;
        }
    };

    let mut i = 0;
    while i < n {
        if !entries[i].is_value {
            // Null run.
            let mut count = 1;
            while i + count < n && !entries[i + count].is_value {
                count += 1;
            }
            maybe_cut(&mut slabs, &mut cur, &mut cur_items, &mut cur_segs, 1);
            cur.extend(encode_signed(0));
            cur.extend(encode_unsigned(count as u64));
            cur_items += count;
            cur_segs += 1;
            i += count;
        } else {
            let vb = val_bytes(i);
            // Count consecutive identical values.
            let mut rcount = 1;
            while i + rcount < n && entries[i + rcount].is_value && val_bytes(i + rcount) == vb {
                rcount += 1;
            }

            if rcount >= 2 {
                // Repeat run: 1 segment.
                maybe_cut(&mut slabs, &mut cur, &mut cur_items, &mut cur_segs, 1);
                cur.extend(encode_signed(rcount as i64));
                cur.extend_from_slice(vb);
                cur_items += rcount;
                cur_segs += 1;
                i += rcount;
            } else {
                // Literal run: collect distinct values, capped at remaining
                // segment budget to avoid oversized runs.
                // Cut BEFORE collecting so seg_room is computed after the cut,
                // avoiding singleton literals followed by adjacent literals.
                maybe_cut(&mut slabs, &mut cur, &mut cur_items, &mut cur_segs, 1);
                let start = i;
                i += 1;
                let seg_room = max_segments.saturating_sub(cur_segs).max(1);
                while i < n && entries[i].is_value && (i - start) < seg_room {
                    // Stop before a repeat.
                    if i + 1 < n && entries[i + 1].is_value && val_bytes(i) == val_bytes(i + 1) {
                        break;
                    }
                    i += 1;
                }
                let lit_count = i - start;
                cur.extend(encode_signed(-(lit_count as i64)));
                for j in start..i {
                    cur.extend_from_slice(val_bytes(j));
                }
                cur_items += lit_count;
                cur_segs += lit_count;
            }
        }
    }

    if cur_items > 0 {
        slabs.push((cur, cur_items, cur_segs));
    }

    (slabs, n)
}

// ── Load & verify ─────────────────────────────────────────────────────────

/// Decode and validate RLE-encoded bytes, splitting into slabs.
///
/// Walks every run, validates that all packed values decode correctly,
/// rejects null runs when `T::NULLABLE` is false, and splits at slab
/// boundaries when the segment budget is reached.
///
/// Uses direct memcpy from the input buffer — no intermediate
/// representations or re-encoding except when splitting a literal run
/// (which requires rewriting the count header for each piece).
fn rle_load_and_verify<T: RleValue>(
    data: &[u8],
    max_segments: usize,
    validate: Option<for<'a> fn(<T as super::ColumnValueRef>::Get<'a>) -> Option<String>>,
) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    let mut slabs: Vec<(Vec<u8>, usize, usize)> = Vec::new();

    // Current slab accumulator — bytes in data[slab_start..pos] plus an
    // optional `pending_hdr` that replaces the original literal header when
    // a literal was split across slabs.
    let mut slab_start: usize = 0;
    let mut slab_items: usize = 0;
    let mut slab_segs: usize = 0;
    let mut pending_hdr: Option<Leb128Buf> = None;

    /// Flush the current slab to `slabs`, incorporating `pending_hdr` if set.
    #[inline]
    fn flush_slab(
        slabs: &mut Vec<(Vec<u8>, usize, usize)>,
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
        let slab_data = if let Some(hdr) = pending_hdr.take() {
            let mut v = Vec::with_capacity(hdr.len as usize + (end - *slab_start));
            v.extend_from_slice(hdr.as_bytes());
            v.extend_from_slice(&data[*slab_start..end]);
            v
        } else {
            data[*slab_start..end].to_vec()
        };
        slabs.push((slab_data, *slab_items, *slab_segs));
        *slab_start = end;
        *slab_items = 0;
        *slab_segs = 0;
    }

    let mut pos = 0;
    while pos < data.len() {
        let run_start = pos;

        let (count_bytes, count_raw) = read_signed(&data[pos..]).ok_or(PackError::BadFormat)?;

        match count_raw {
            // ── Repeat run: count > 0 ────────────────────────────────────
            n if n > 0 => {
                let count = n as usize;
                let value_start = pos + count_bytes;
                if value_start > data.len() {
                    return Err(PackError::BadFormat);
                }
                let (value_len, value) = T::try_unpack(&data[value_start..])?;
                if let Some(validate) = validate {
                    if let Some(msg) = validate(value) {
                        return Err(PackError::InvalidValue(msg));
                    }
                }
                let run_end = value_start + value_len;

                // 1 segment — cut before if it would exceed budget.
                if slab_segs > 0 && slab_segs + 1 > max_segments {
                    flush_slab(
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
            }
            // ── Literal run: count < 0 ───────────────────────────────────
            n if n < 0 => {
                let total = (-n) as usize;
                let values_start = pos + count_bytes;

                // Validate all values and record each value's byte offset
                // so we can split at arbitrary value boundaries.
                let mut val_offsets: Vec<usize> = Vec::with_capacity(total + 1);
                let mut scan = values_start;
                for _ in 0..total {
                    if scan >= data.len() {
                        return Err(PackError::BadFormat);
                    }
                    val_offsets.push(scan);
                    let (vlen, value) = T::try_unpack(&data[scan..])?;
                    if let Some(validate) = validate {
                        if let Some(msg) = validate(value) {
                            return Err(PackError::InvalidValue(msg));
                        }
                    }
                    scan += vlen;
                }
                val_offsets.push(scan); // end sentinel
                let run_end = scan;

                // How many values can we still fit in the current slab?
                let room = if slab_segs > 0 {
                    max_segments.saturating_sub(slab_segs)
                } else {
                    max_segments
                };

                if total <= room {
                    // Entire literal fits — just accumulate, no splitting.
                    slab_items += total;
                    slab_segs += total;
                    pos = run_end;
                } else {
                    // Need to split this literal.
                    let mut consumed = 0;

                    // Step 1: flush current slab, optionally filling
                    // remaining room with values from this literal.
                    if slab_segs > 0 {
                        if room > 0 {
                            // Emit accumulated bytes + a new literal header
                            // + the first `room` values from this literal.
                            let chunk_hdr = encode_signed(-(room as i64));
                            let chunk_vals_end = val_offsets[consumed + room];
                            let mut slab_data = if let Some(hdr) = pending_hdr.take() {
                                let mut v = Vec::with_capacity(
                                    hdr.len as usize
                                        + (run_start - slab_start)
                                        + chunk_hdr.len as usize
                                        + (chunk_vals_end - values_start),
                                );
                                v.extend_from_slice(hdr.as_bytes());
                                v.extend_from_slice(&data[slab_start..run_start]);
                                v
                            } else {
                                data[slab_start..run_start].to_vec()
                            };
                            slab_data.extend_from_slice(chunk_hdr.as_bytes());
                            slab_data
                                .extend_from_slice(&data[val_offsets[consumed]..chunk_vals_end]);
                            slabs.push((slab_data, slab_items + room, slab_segs + room));
                            consumed += room;
                        } else {
                            // Room is 0 — flush accumulated slab as-is.
                            flush_slab(
                                &mut slabs,
                                data,
                                &mut slab_start,
                                &mut slab_items,
                                &mut slab_segs,
                                &mut pending_hdr,
                                run_start,
                            );
                        }
                        slab_items = 0;
                        slab_segs = 0;
                    }

                    // Step 2: emit full max_segments-sized chunks.
                    while total - consumed >= max_segments {
                        let chunk_hdr = encode_signed(-(max_segments as i64));
                        let chunk_start = val_offsets[consumed];
                        let chunk_end = val_offsets[consumed + max_segments];
                        let mut slab_data =
                            Vec::with_capacity(chunk_hdr.len as usize + (chunk_end - chunk_start));
                        slab_data.extend_from_slice(chunk_hdr.as_bytes());
                        slab_data.extend_from_slice(&data[chunk_start..chunk_end]);
                        slabs.push((slab_data, max_segments, max_segments));
                        consumed += max_segments;
                    }

                    // Step 3: remainder becomes start of next slab.
                    let remaining = total - consumed;
                    if remaining > 0 {
                        pending_hdr = Some(encode_signed(-(remaining as i64)));
                        slab_start = val_offsets[consumed];
                        slab_items = remaining;
                        slab_segs = remaining;
                    } else {
                        slab_start = run_end;
                        // slab_items and slab_segs already 0
                    }
                    pos = run_end;
                }
            }
            // ── Null run: count == 0 ─────────────────────────────────────
            _ => {
                if !T::NULLABLE {
                    return Err(PackError::InvalidValue(
                        "null run in non-nullable column".into(),
                    ));
                }
                if let Some(validate) = validate {
                    if let Some(msg) = validate(T::get_null(ValidBytes::from_bytes(data))) {
                        return Err(PackError::InvalidValue(msg));
                    }
                }
                let null_data = &data[pos + count_bytes..];
                if null_data.is_empty() {
                    return Err(PackError::BadFormat);
                }
                let (ncb, nc) = read_unsigned(null_data).ok_or(PackError::BadFormat)?;
                if nc == 0 {
                    return Err(PackError::BadFormat);
                }
                let run_end = pos + count_bytes + ncb;

                // 1 segment — cut before if it would exceed budget.
                if slab_segs > 0 && slab_segs + 1 > max_segments {
                    flush_slab(
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
    }

    // Flush remaining.
    flush_slab(
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
