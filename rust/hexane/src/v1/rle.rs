use std::marker::PhantomData;

use crate::PackError;

use super::encoding::ColumnEncoding;
use super::RleValue;

// ── Wire-format helpers ───────────────────────────────────────────────────────
//
// The encoding (shared with v0) is a sequence of runs:
//
//   Repeat run : signed_leb128( count > 0 )  packed_value
//   Literal run: signed_leb128( -n      )    v0 v1 … v(n-1)
//   Null run   : signed_leb128( 0       )    unsigned_leb128( count )

/// Stack-buffered LEB128 encoding (max 10 bytes, no heap allocation).
#[derive(Clone, Copy)]
struct Leb128Buf {
    buf: [u8; 10],
    len: u8,
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
struct Leb128Iter {
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
fn encode_signed(n: i64) -> Leb128Buf {
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
fn encode_unsigned(n: u64) -> Leb128Buf {
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
fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::signed(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

/// Decode one unsigned LEB128 count from `data`.  Returns (bytes_read, value).
fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

// ── Internal scan result ──────────────────────────────────────────────────────

struct ScanResult {
    /// Byte offset of this run's first byte (the count header).
    run_start: usize,
    /// Byte offset past this run's last byte.
    run_end: usize,
    /// Number of logical items in this run that precede `target`.
    offset_in_run: usize,
    kind: ScannedRun,
    /// Byte offset of the previous run's header, if any.
    prev_run_start: Option<usize>,
    /// Byte offset of the last encoded value in the previous run
    /// (`None` if prev is a null run or there is no prev).
    prev_last_value_start: Option<usize>,
}

enum ScannedRun {
    Repeat {
        count: usize,
        count_bytes: usize,
        /// Byte offset (in slab) of the encoded value.
        value_start: usize,
        value_len: usize,
    },
    Literal {
        total: usize,
        count_bytes: usize,
        /// Byte offset (in slab) of the target item's encoded bytes.
        target_start: usize,
        target_len: usize,
    },
    Null {
        count: usize,
        count_bytes: usize,
        null_count_bytes: usize,
    },
}

/// Walk `slab` linearly until the run containing logical item `target` is
/// found.  Returns `None` if `target` is out of bounds.
fn scan_to<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &[u8],
    target: usize,
) -> Option<ScanResult> {
    let (mut byte_pos, mut item_pos, mut prev_start, mut prev_last_val) = (0, 0, None, None);

    while byte_pos < slab.len() {
        let (count_bytes, count_raw) = read_signed(&slab[byte_pos..])?;

        match count_raw {
            // ── Repeat run ────────────────────────────────────────────────
            n if n > 0 => {
                let count = n as usize;
                let value_start = byte_pos + count_bytes;
                let value_len = T::value_len(&slab[value_start..])?;

                if target < item_pos + count {
                    return Some(ScanResult {
                        run_start: byte_pos,
                        run_end: value_start + value_len,
                        offset_in_run: target - item_pos,
                        kind: ScannedRun::Repeat {
                            count,
                            count_bytes,
                            value_start,
                            value_len,
                        },
                        prev_run_start: prev_start,
                        prev_last_value_start: prev_last_val,
                    });
                }

                prev_start = Some(byte_pos);
                prev_last_val = Some(value_start);
                item_pos += count;
                byte_pos = value_start + value_len;
            }

            // ── Literal run ───────────────────────────────────────────────
            n if n < 0 => {
                let total = (-n) as usize;
                let count_bytes_here = count_bytes;
                let mut scan_byte = byte_pos + count_bytes_here;
                let mut target_info: Option<(usize, usize, usize)> = None; // (offset_in_run, byte_start, byte_len)
                let mut last_val_start = scan_byte;

                for i in 0..total {
                    last_val_start = scan_byte;
                    let vlen = T::value_len(&slab[scan_byte..])?;
                    if item_pos + i == target {
                        target_info = Some((i, scan_byte, vlen));
                    }
                    scan_byte += vlen;
                }
                // scan_byte is now the byte after the last item = run_end

                if let Some((offset, tgt_start, tgt_len)) = target_info {
                    return Some(ScanResult {
                        run_start: byte_pos,
                        run_end: scan_byte,
                        offset_in_run: offset,
                        kind: ScannedRun::Literal {
                            total,
                            count_bytes: count_bytes_here,
                            target_start: tgt_start,
                            target_len: tgt_len,
                        },
                        prev_run_start: prev_start,
                        prev_last_value_start: prev_last_val,
                    });
                }

                prev_start = Some(byte_pos);
                prev_last_val = Some(last_val_start);
                item_pos += total;
                byte_pos = scan_byte;
            }

            // ── Null run ──────────────────────────────────────────────────
            _ => {
                let (null_count_bytes, null_count) =
                    read_unsigned(&slab[byte_pos + count_bytes..])?;
                let null_count = null_count as usize;

                if target < item_pos + null_count {
                    return Some(ScanResult {
                        run_start: byte_pos,
                        run_end: byte_pos + count_bytes + null_count_bytes,
                        offset_in_run: target - item_pos,
                        kind: ScannedRun::Null {
                            count: null_count,
                            count_bytes,
                            null_count_bytes,
                        },
                        prev_run_start: prev_start,
                        prev_last_value_start: prev_last_val,
                    });
                }

                prev_start = Some(byte_pos);
                prev_last_val = None;
                item_pos += null_count;
                byte_pos += count_bytes + null_count_bytes;
            }
        }
    }

    None // target >= len
}

// ── Last-run scan (for appending) ─────────────────────────────────────────────

struct LastRunResult {
    run_start: usize,
    kind: LastRun,
}

enum LastRun {
    Repeat {
        count: usize,
        count_bytes: usize,
        value_start: usize,
        value_len: usize,
    },
    Literal {
        last_value_start: usize,
        last_value_len: usize,
    },
    Null,
}

/// Walk the slab to find the last run.  Returns `None` only if the slab is empty.
fn scan_last_run<T: RleValue<Encoding = RleEncoding<T>>>(slab: &[u8]) -> Option<LastRunResult> {
    let mut byte_pos = 0;
    let mut result: Option<LastRunResult> = None;

    while byte_pos < slab.len() {
        let (count_bytes, count_raw) = read_signed(&slab[byte_pos..])?;

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let value_start = byte_pos + count_bytes;
                let value_len = T::value_len(&slab[value_start..])?;
                result = Some(LastRunResult {
                    run_start: byte_pos,
                    kind: LastRun::Repeat {
                        count,
                        count_bytes,
                        value_start,
                        value_len,
                    },
                });
                byte_pos = value_start + value_len;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut scan_byte = byte_pos + count_bytes;
                let mut last_vs = scan_byte;
                let mut last_vl = 0;
                for _ in 0..total {
                    last_vs = scan_byte;
                    let vlen = T::value_len(&slab[scan_byte..])?;
                    last_vl = vlen;
                    scan_byte += vlen;
                }
                result = Some(LastRunResult {
                    run_start: byte_pos,
                    kind: LastRun::Literal {
                        last_value_start: last_vs,
                        last_value_len: last_vl,
                    },
                });
                byte_pos = scan_byte;
            }
            _ => {
                let (ncb, _) = read_unsigned(&slab[byte_pos + count_bytes..])?;
                result = Some(LastRunResult {
                    run_start: byte_pos,
                    kind: LastRun::Null,
                });
                byte_pos += count_bytes + ncb;
            }
        }
    }

    result
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
fn merge_lit1_into_right<T: RleValue<Encoding = RleEncoding<T>>>(
    right: &mut Vec<u8>,
    value: &[u8],
    rest: &[u8],
) {
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
    data: &'a [u8],
    byte_pos: usize,
    remaining: usize,
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
    Repeat(<T as super::ColumnValue>::Get<'a>),
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

impl<'a, T: RleValue<Encoding = RleEncoding<T>>> RleDecoder<'a, T> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        let mut dec = RleDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            state: RleDecoderState::Idle,
        };
        dec.advance_run();
        dec
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

impl<'a, T: RleValue<Encoding = RleEncoding<T>>> RleDecoder<'a, T> {
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

impl<'a, T: RleValue<Encoding = RleEncoding<T>>> Iterator for RleDecoder<'a, T> {
    type Item = <T as super::ColumnValue>::Get<'a>;

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

impl<'a, T: RleValue<Encoding = RleEncoding<T>>> super::encoding::RunDecoder for RleDecoder<'a, T> {
    fn next_run(&mut self) -> Option<super::Run<Self::Item>> {
        loop {
            if self.remaining > 0 {
                let count = self.remaining;
                return match &self.state {
                    RleDecoderState::Repeat(v) => {
                        let value = *v;
                        self.remaining = 0;
                        // byte_pos already past the value data for repeat runs
                        Some(super::Run { count, value })
                    }
                    RleDecoderState::Literal => {
                        // Literal: each item is distinct, yield one at a time
                        self.remaining -= 1;
                        let (vlen, value) = T::unpack(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        Some(super::Run { count: 1, value })
                    }
                    RleDecoderState::Null => {
                        let value = T::get_null(self.data);
                        self.remaining = 0;
                        Some(super::Run { count, value })
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

impl<T: RleValue<Encoding = RleEncoding<T>>> ColumnEncoding for RleEncoding<T> {
    type Value = T;

    fn get<'a>(slab: &'a [u8], index: usize, len: usize) -> Option<T::Get<'a>> {
        if index >= len {
            return None;
        }
        let scan = scan_to::<T>(slab, index)?;
        match scan.kind {
            ScannedRun::Null { .. } => Some(T::get_null(slab)),
            ScannedRun::Repeat { value_start, .. } => {
                let (_, v) = T::unpack(&slab[value_start..]);
                Some(v)
            }
            ScannedRun::Literal { target_start, .. } => {
                let (_, v) = T::unpack(&slab[target_start..]);
                Some(v)
            }
        }
    }

    fn insert<'v>(slab: &mut Vec<u8>, index: usize, len: usize, value: T::Get<'v>) -> i32 {
        #[cfg(debug_assertions)]
        let seg_before = rle_count_segments::<T>(slab);

        let mut vbytes = Vec::new();
        let delta = if T::pack(value, &mut vbytes) {
            insert_value::<T>(slab, index, len, &vbytes)
        } else {
            insert_null::<T>(slab, index, len)
        };

        #[cfg(debug_assertions)]
        debug_assert_eq!(
            delta,
            rle_count_segments::<T>(slab) as i32 - seg_before as i32,
            "segment delta mismatch in RLE insert"
        );
        delta
    }

    fn remove(slab: &mut Vec<u8>, index: usize, len: usize) -> i32 {
        let _ = len;
        #[cfg(debug_assertions)]
        let seg_before = rle_count_segments::<T>(slab);

        let scan = scan_to::<T>(slab, index).expect("index < len so scan must succeed");
        let delta = delete_mutate::<T>(slab, &scan, index);

        #[cfg(debug_assertions)]
        debug_assert_eq!(
            delta,
            rle_count_segments::<T>(slab) as i32 - seg_before as i32,
            "segment delta mismatch in RLE remove"
        );
        delta
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

    fn encode_all_slabs(values: Vec<T>, max_segments: usize) -> Vec<(Vec<u8>, usize, usize)> {
        rle_encode_all_slabs::<T>(values, max_segments)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(<T as super::ColumnValue>::Get<'a>) -> Option<String>>,
    ) -> Result<Vec<(Vec<u8>, usize, usize)>, PackError> {
        rle_load_and_verify::<T>(data, max_segments, validate)
    }

    fn streaming_save(slabs: &[&[u8]]) -> Vec<u8> {
        rle_streaming_save::<T>(slabs)
    }

    type Decoder<'a> = RleDecoder<'a, T>;

    fn decoder(slab: &[u8]) -> RleDecoder<'_, T> {
        RleDecoder::new(slab)
    }
}

// ── count_segments ───────────────────────────────────────────────────────────

/// Count segments in an RLE slab. A repeat run = 1 segment, a null run = 1
/// segment, a literal of N = N segments.
fn rle_count_segments<T: RleValue<Encoding = RleEncoding<T>>>(slab: &[u8]) -> usize {
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
fn rle_split_at_item<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &[u8],
    index: usize,
    len: usize,
) -> (Vec<u8>, Vec<u8>) {
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
fn last_run_start<T: RleValue<Encoding = RleEncoding<T>>>(slab: &[u8]) -> Option<usize> {
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
fn first_run_end<T: RleValue<Encoding = RleEncoding<T>>>(slab: &[u8]) -> usize {
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
fn rle_merge_slab_bytes<T: RleValue<Encoding = RleEncoding<T>>>(
    a: &[u8],
    b: &[u8],
) -> (Vec<u8>, usize) {
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
fn parse_one_run<T: RleValue<Encoding = RleEncoding<T>>>(data: &[u8]) -> ParsedRun {
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
fn rle_streaming_save<T: RleValue<Encoding = RleEncoding<T>>>(slabs: &[&[u8]]) -> Vec<u8> {
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
fn rle_validate_encoding<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &[u8],
) -> Result<(), String> {
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

// ── insert_null ──────────────────────────────────────────────────────────────

fn insert_null<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    index: usize,
    len: usize,
) -> i32 {
    // Appending past the end, or the slab is empty
    if index == len || slab.is_empty() {
        // Try to extend an existing trailing null run.
        if !slab.is_empty() {
            if let Some(last) = scan_last_run::<T>(slab) {
                if let LastRun::Null = last.kind {
                    let (cb, _) = read_signed(&slab[last.run_start..]).unwrap();
                    let null_start = last.run_start + cb;
                    let (ncb, nc) = read_unsigned(&slab[null_start..]).unwrap();
                    let new_null = encode_unsigned(nc + 1);
                    slab.splice(null_start..null_start + ncb, new_null);
                    return 0; // extended existing null run
                }
            }
        }
        append_null_run(slab, 1);
        return 1; // new null run
    }

    let scan = scan_to::<T>(slab, index).expect("index < len so scan must succeed");

    match scan.kind {
        // Inserting null into an existing null run: just bump the count.
        ScannedRun::Null {
            count,
            count_bytes,
            null_count_bytes,
        } => {
            let null_start = scan.run_start + count_bytes;
            let new_null = encode_unsigned((count + 1) as u64);
            slab.splice(null_start..null_start + null_count_bytes, new_null);
            0 // count bump, no segment change
        }
        // Null landing at the very start of any other run.
        _ if scan.offset_in_run == 0 => {
            // If the previous run is a null run, extend it.
            if let Some(ps) = scan.prev_run_start {
                if let Some((prev_cb, 0)) = read_signed(&slab[ps..]) {
                    let null_start = ps + prev_cb;
                    let (ncb, nc) = read_unsigned(&slab[null_start..]).unwrap();
                    let new_null = encode_unsigned(nc + 1);
                    slab.splice(null_start..null_start + ncb, new_null);
                    return 0; // extended existing null run
                }
            }
            let bytes = null_run_bytes(1);
            slab.splice(scan.run_start..scan.run_start, bytes);
            1 // new null run inserted
        }
        // Null landing in the middle of a repeat run: split it.
        ScannedRun::Repeat {
            count,
            count_bytes: _,
            value_start,
            value_len,
        } => {
            let k = scan.offset_in_run;
            let existing = slab[value_start..value_start + value_len].to_vec();
            let pre_hdr = value_run_header(k);
            let pre_piece_len = pre_hdr.len() + existing.len();
            let null_piece = null_run_bytes(1);
            let null_piece_len = null_piece.len();
            let post_hdr = value_run_header(count - k);
            let mut new_bytes = Vec::with_capacity(
                pre_hdr.len() + existing.len() + null_piece_len + post_hdr.len() + existing.len(),
            );
            new_bytes.extend_from_slice(&pre_hdr);
            new_bytes.extend_from_slice(&existing);
            new_bytes.extend_from_slice(&null_piece);
            new_bytes.extend_from_slice(&post_hdr);
            new_bytes.extend_from_slice(&existing);
            slab.splice(scan.run_start..scan.run_end, new_bytes);

            // Split repeat(N) into pre + null + post = +2 new segs, -1 old = +2 base
            let mut delta: i32 = 2;

            // Merge edge lit-1 pieces with neighbors.
            if count - k == 1 {
                let post_pos = scan.run_start + pre_piece_len + null_piece_len;
                let (absorbed, absorb_delta) = absorb_lit1_into_repeat::<T>(slab, post_pos, None);
                if absorbed {
                    delta += absorb_delta;
                } else {
                    delta += merge_adjacent_lits::<T>(slab, post_pos, None);
                }
            }
            if k == 1 {
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, scan.run_start, scan.prev_run_start);
                if !absorbed {
                    delta += merge_adjacent_lits::<T>(slab, scan.run_start, scan.prev_run_start);
                } else {
                    delta += absorb_delta;
                }
            }
            delta
        }
        // Null inside a literal run: we need to split the literal.
        ScannedRun::Literal {
            total,
            count_bytes,
            target_start,
            ..
        } => {
            let before_count = scan.offset_in_run;
            let after_count = total - before_count;
            let mut new_bytes = vec![];
            let mut before_piece_len = 0;
            if before_count > 0 {
                let hdr = encode_signed(-(before_count as i64));
                before_piece_len = hdr.len() + (target_start - (scan.run_start + count_bytes));
                new_bytes.extend(hdr);
                new_bytes.extend_from_slice(&slab[scan.run_start + count_bytes..target_start]);
            }
            let null_piece = null_run_bytes(1);
            let null_piece_len = null_piece.len();
            new_bytes.extend(null_piece);
            if after_count > 0 {
                new_bytes.extend(encode_signed(-(after_count as i64)));
                new_bytes.extend_from_slice(&slab[target_start..scan.run_end]);
            }
            slab.splice(scan.run_start..scan.run_end, new_bytes);

            // Original lit(N) = N segs. New: lit(before) + null(1) + lit(after)
            // = before + 1 + after = N + 1. Delta = +1.
            let mut delta: i32 = 1;

            if after_count == 1 {
                let post_pos = scan.run_start + before_piece_len + null_piece_len;
                let (absorbed, absorb_delta) = absorb_lit1_into_repeat::<T>(slab, post_pos, None);
                if absorbed {
                    delta += absorb_delta;
                } else {
                    delta += merge_adjacent_lits::<T>(slab, post_pos, None);
                }
            }
            if before_count == 1 {
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, scan.run_start, scan.prev_run_start);
                if !absorbed {
                    delta += merge_adjacent_lits::<T>(slab, scan.run_start, scan.prev_run_start);
                } else {
                    delta += absorb_delta;
                }
            }
            delta
        }
    }
}

// ── insert_value ─────────────────────────────────────────────────────────────

fn insert_value<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    index: usize,
    len: usize,
    vbytes: &[u8],
) -> i32 {
    if index == len || slab.is_empty() {
        return append_value_run::<T>(slab, vbytes);
    }

    let scan = scan_to::<T>(slab, index).expect("index < len so scan must succeed");

    match scan.kind {
        // ── Repeat run ────────────────────────────────────────────────
        ScannedRun::Repeat {
            count,
            count_bytes,
            value_start,
            value_len,
        } => {
            let existing = &slab[value_start..value_start + value_len];
            if existing == vbytes {
                // Same value: just increment the run count — non-structural.
                let new_count = encode_signed((count + 1) as i64);
                slab.splice(scan.run_start..scan.run_start + count_bytes, new_count);
                0 // count bump, no segment change
            } else {
                let k = scan.offset_in_run;
                let existing = existing.to_vec();
                let mut new_bytes = vec![];

                if k == 0 {
                    if try_absorb_into_prev::<T>(slab, &scan, vbytes) {
                        // Value absorbed into previous run — no new segment.
                        0
                    } else {
                        new_bytes.extend(encode_signed(-1));
                        new_bytes.extend_from_slice(vbytes);
                        slab.splice(scan.run_start..scan.run_start, new_bytes);
                        // New lit-1 = +1 segment, then merge may adjust
                        1 + merge_adjacent_lits::<T>(slab, scan.run_start, scan.prev_run_start)
                    }
                } else if k == count {
                    if try_absorb_into_next::<T>(slab, &scan, vbytes) {
                        // Value absorbed into next run — no new segment.
                        0
                    } else {
                        new_bytes.extend(encode_signed(-1));
                        new_bytes.extend_from_slice(vbytes);
                        slab.splice(scan.run_end..scan.run_end, new_bytes);
                        // New lit-1 = +1 segment, then merge may adjust
                        1 + merge_adjacent_lits::<T>(slab, scan.run_end, None)
                    }
                } else {
                    // Split: [pre][new][post]
                    // repeat(N) [1 seg] → pre + lit-1 + post [3 segs min] = +2 base
                    let pre_hdr = value_run_header(k);
                    let pre_hdr_len = pre_hdr.len();
                    new_bytes.extend(pre_hdr);
                    new_bytes.extend_from_slice(&existing);
                    new_bytes.extend(encode_signed(-1));
                    new_bytes.extend_from_slice(vbytes);
                    new_bytes.extend(value_run_header(count - k));
                    new_bytes.extend_from_slice(&existing);
                    slab.splice(scan.run_start..scan.run_end, new_bytes);

                    let merge_delta = if k == 1 {
                        merge_adjacent_lits::<T>(slab, scan.run_start, scan.prev_run_start)
                    } else {
                        let lit_x_pos = scan.run_start + pre_hdr_len + existing.len();
                        merge_adjacent_lits::<T>(slab, lit_x_pos, None)
                    };
                    2 + merge_delta
                }
            }
        }

        // ── Literal run (HOT PATH) ────────────────────────────────────
        ScannedRun::Literal {
            total,
            count_bytes,
            target_start,
            ..
        } => {
            // Combine header update + value insertion into a single splice.
            // Replace [old_header][vals_before_target] with [new_header][vals_before_target][new_value].
            let new_count = encode_signed(-((total + 1) as i64));
            let vals_before_start = scan.run_start + count_bytes;
            let vals_before_len = target_start - vals_before_start;
            let mut replacement =
                Vec::with_capacity(new_count.len() + vals_before_len + vbytes.len());
            replacement.extend_from_slice(&new_count);
            replacement.extend_from_slice(&slab[vals_before_start..target_start]);
            replacement.extend_from_slice(vbytes);
            slab.splice(scan.run_start..target_start, replacement);

            // Fix adjacent duplicates.
            // lit(N) → lit(N+1) = +1 seg, then normalize may adjust
            let norm_delta = normalize_literal_at::<T>(slab, scan.run_start, scan.prev_run_start);
            1 + norm_delta
        }

        // ── Null run ──────────────────────────────────────────────────
        ScannedRun::Null {
            count,
            count_bytes: _,
            null_count_bytes: _,
        } => {
            let k = scan.offset_in_run;
            let mut new_bytes = vec![];

            let null_before = k;
            let null_after = count - k;
            if null_before > 0 {
                new_bytes.extend(null_run_bytes(null_before));
            }
            let lit_hdr = encode_signed(-1);
            let lit_offset = new_bytes.len();
            new_bytes.extend_from_slice(&lit_hdr);
            new_bytes.extend_from_slice(vbytes);
            if null_after > 0 {
                new_bytes.extend(null_run_bytes(null_after));
            }

            slab.splice(scan.run_start..scan.run_end, new_bytes);

            // Original: null(N) = 1 seg.
            // New: null(before)? + lit-1 + null(after)?
            // Pieces: (before>0 ? 1 : 0) + 1 + (after>0 ? 1 : 0)
            // Delta = pieces - 1
            let mut delta: i32 =
                (if null_before > 0 { 1 } else { 0 }) + 1 + (if null_after > 0 { 1 } else { 0 })
                    - 1;

            // Merge the new [lit-1] with adjacent runs.
            let lit_pos = scan.run_start + lit_offset;
            let prev_for_lit = if null_before == 0 {
                scan.prev_run_start
            } else {
                None
            };
            let (absorbed, absorb_delta) =
                absorb_lit1_into_repeat::<T>(slab, lit_pos, prev_for_lit);
            if absorbed {
                delta += absorb_delta;
            } else {
                delta += merge_adjacent_lits::<T>(slab, lit_pos, prev_for_lit);
            }
            delta
        }
    }
}

// ── append helpers ───────────────────────────────────────────────────────────

fn append_null_run(slab: &mut Vec<u8>, count: usize) {
    slab.extend(null_run_bytes(count));
}

fn append_value_run<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    vbytes: &[u8],
) -> i32 {
    // Try to extend the last run if the value matches.
    if !slab.is_empty() {
        if let Some(last) = scan_last_run::<T>(slab) {
            match last.kind {
                LastRun::Repeat {
                    count,
                    count_bytes,
                    value_start,
                    value_len,
                } => {
                    if slab[value_start..value_start + value_len] == *vbytes {
                        // Same value — bump the repeat count.
                        let new_hdr = encode_signed((count + 1) as i64);
                        slab.splice(last.run_start..last.run_start + count_bytes, new_hdr);
                        return 0; // extended repeat, no new segment
                    }
                }
                LastRun::Literal {
                    last_value_start,
                    last_value_len,
                    ..
                } => {
                    if slab[last_value_start..last_value_start + last_value_len] == *vbytes {
                        // Matches last value of literal — peel + repeat-2.
                        // lit(N) = N segs → lit(N-1) + repeat(2) = (N-1) + 1 = N segs
                        let (hl, hv) = read_signed(&slab[last.run_start..]).unwrap();
                        let lit_count = (-hv) as usize;
                        let mut replacement = vec![];
                        if lit_count > 1 {
                            replacement.extend(encode_signed(-((lit_count - 1) as i64)));
                            replacement
                                .extend_from_slice(&slab[last.run_start + hl..last_value_start]);
                        }
                        replacement.extend(encode_signed(2));
                        replacement.extend_from_slice(vbytes);
                        slab.splice(last.run_start.., replacement);
                        return 0; // same total segments
                    }
                    // Different value — extend the literal.
                    let (hl, hv) = read_signed(&slab[last.run_start..]).unwrap();
                    let lit_count = (-hv) as usize;
                    let new_hdr = encode_signed(-((lit_count + 1) as i64));
                    slab.splice(last.run_start..last.run_start + hl, new_hdr);
                    slab.extend_from_slice(vbytes);
                    return 1; // one more item in literal = one more segment
                }
                LastRun::Null => {}
            }
        }
    }
    // Fallback: start a new literal-1 run.
    slab.extend(encode_signed(-1));
    slab.extend_from_slice(vbytes);
    1 // new lit-1 = 1 segment
}

// ── delete ───────────────────────────────────────────────────────────────────

/// Perform the slab mutation for deleting the item at `index`, without
/// decoding the value.  Shared by both `delete_impl` and `remove_impl`.
fn delete_mutate<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    scan: &ScanResult,
    _index: usize,
) -> i32 {
    match &scan.kind {
        // ── Repeat run ────────────────────────────────────────────────
        ScannedRun::Repeat {
            count, count_bytes, ..
        } => {
            let count = *count;
            let count_bytes = *count_bytes;
            if count == 1 {
                // Remove the entire run — structural. -1 for removed run.
                slab.drain(scan.run_start..scan.run_end);
                let canon_delta = if scan.run_start < slab.len() {
                    canonicalize_neighbors::<T>(slab, scan.run_start, scan.prev_run_start)
                } else {
                    0
                };
                -1 + canon_delta
            } else if count == 2 {
                // Drops to literal-1, may merge — structural.
                // repeat(2) [1 seg] → lit-1 [1 seg] = 0 base delta
                let new_count = value_run_header(count - 1);
                slab.splice(scan.run_start..scan.run_start + count_bytes, new_count);
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, scan.run_start, scan.prev_run_start);
                if absorbed {
                    absorb_delta
                } else {
                    merge_adjacent_lits::<T>(slab, scan.run_start, scan.prev_run_start)
                }
            } else {
                // Decrement count (count > 2) — non-structural.
                let new_count = value_run_header(count - 1);
                slab.splice(scan.run_start..scan.run_start + count_bytes, new_count);
                0
            }
        }

        // ── Literal run ───────────────────────────────────────────────
        ScannedRun::Literal {
            total,
            count_bytes,
            target_start,
            target_len,
        } => {
            let total = *total;
            let count_bytes = *count_bytes;
            let target_start = *target_start;
            let target_len = *target_len;

            if total == 1 {
                // Remove the entire run (header + value) in one drain.
                slab.drain(scan.run_start..target_start + target_len);
                let canon_delta = if scan.run_start < slab.len() {
                    canonicalize_neighbors::<T>(slab, scan.run_start, scan.prev_run_start)
                } else {
                    0
                };
                -1 + canon_delta
            } else {
                // Combine header update + value removal into a single splice.
                // Replace [old_header][vals_before_target][target] with [new_header][vals_before_target].
                let new_count = encode_signed(-((total - 1) as i64));
                let vals_before_start = scan.run_start + count_bytes;
                let vals_before_len = target_start - vals_before_start;
                let mut replacement = Vec::with_capacity(new_count.len() + vals_before_len);
                replacement.extend_from_slice(&new_count);
                replacement.extend_from_slice(&slab[vals_before_start..target_start]);
                slab.splice(scan.run_start..target_start + target_len, replacement);

                let norm_delta =
                    normalize_literal_at::<T>(slab, scan.run_start, scan.prev_run_start);
                -1 + norm_delta
            }
        }

        // ── Null run ──────────────────────────────────────────────────
        ScannedRun::Null {
            count,
            count_bytes,
            null_count_bytes,
        } => {
            let count = *count;
            let count_bytes = *count_bytes;
            let null_count_bytes = *null_count_bytes;
            if count == 1 {
                // Remove entire null run — -1 seg.
                slab.drain(scan.run_start..scan.run_end);
                let canon_delta = if scan.run_start < slab.len() {
                    canonicalize_neighbors::<T>(slab, scan.run_start, scan.prev_run_start)
                } else {
                    0
                };
                -1 + canon_delta
            } else {
                // Decrement null count — non-structural.
                let null_start = scan.run_start + count_bytes;
                let new_null = encode_unsigned((count - 1) as u64);
                slab.splice(null_start..null_start + null_count_bytes, new_null);
                0
            }
        }
    }
}

// ── Targeted literal merging ─────────────────────────────────────────────────

fn normalize_literal_at<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    run_start: usize,
    prev_run_start: Option<usize>,
) -> i32 {
    let (hdr_len, hdr_val) = match read_signed(&slab[run_start..]) {
        Some(x) => x,
        None => return 0,
    };
    if hdr_val >= 0 {
        return 0; // not a literal
    }
    let item_count = (-hdr_val) as usize;
    let values_start = run_start + hdr_len;

    let first_vlen = T::value_len(&slab[values_start..]).expect("valid slab");
    let mut prev_pos = values_start;
    let mut prev_vlen = first_vlen;
    let mut pos = prev_pos + prev_vlen;

    for i in 1..item_count {
        let cur_vlen = T::value_len(&slab[pos..]).expect("valid slab");

        if prev_vlen == cur_vlen
            && slab[prev_pos..prev_pos + prev_vlen] == slab[pos..pos + cur_vlen]
        {
            // Adjacent duplicate at logical positions (i-1, i).
            // Original lit(N) = N segs. Replacing with:
            //   lit(before) [before segs] + repeat(2) [1 seg] + lit(after) [after segs]
            // Total new = before + 1 + after = (N - 2) + 1 = N - 1. Delta = -1.
            let before_count = i - 1;
            let after_count = item_count - i - 1;
            let mut delta: i32 = -1;

            let mut run_end = pos + cur_vlen;
            for _ in (i + 1)..item_count {
                let vl = T::value_len(&slab[run_end..]).expect("valid slab");
                run_end += vl;
            }

            let dup_value = slab[prev_pos..prev_pos + prev_vlen].to_vec();
            let before_bytes = slab[values_start..prev_pos].to_vec();
            let after_bytes = slab[pos + cur_vlen..run_end].to_vec();

            let mut new_bytes = vec![];
            let mut before_piece_len = 0;
            if before_count > 0 {
                let hdr = encode_signed(-(before_count as i64));
                before_piece_len = hdr.len() + before_bytes.len();
                new_bytes.extend(hdr);
                new_bytes.extend_from_slice(&before_bytes);
            }
            let repeat_hdr = encode_signed(2);
            let repeat_piece_len = repeat_hdr.len() + dup_value.len();
            new_bytes.extend(repeat_hdr);
            new_bytes.extend_from_slice(&dup_value);
            if after_count > 0 {
                new_bytes.extend(encode_signed(-(after_count as i64)));
                new_bytes.extend_from_slice(&after_bytes);
            }

            slab.splice(run_start..run_end, new_bytes);

            if after_count == 1 {
                let after_lit_pos = run_start + before_piece_len + repeat_piece_len;
                let repeat_pos = Some(run_start + before_piece_len);
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, after_lit_pos, repeat_pos);
                if absorbed {
                    delta += absorb_delta;
                } else {
                    delta += merge_adjacent_lits::<T>(slab, after_lit_pos, None);
                }
            }
            if before_count == 1 {
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, run_start, prev_run_start);
                if absorbed {
                    delta += absorb_delta;
                } else {
                    delta += merge_adjacent_lits::<T>(slab, run_start, prev_run_start);
                }
            }

            return delta;
        }

        prev_pos = pos;
        prev_vlen = cur_vlen;
        pos += cur_vlen;
    }

    // No internal adjacent dups.  Check boundary: does the last value
    // of this literal match a following repeat?
    let run_end = pos;
    if run_end < slab.len() {
        let (next_hl, next_hv) = match read_signed(&slab[run_end..]) {
            Some(x) => x,
            None => return 0,
        };
        if next_hv > 0 {
            let next_vs = run_end + next_hl;
            let next_vl = T::value_len(&slab[next_vs..]).expect("valid slab");
            if prev_vlen == next_vl
                && slab[prev_pos..prev_pos + prev_vlen] == slab[next_vs..next_vs + next_vl]
            {
                // Last literal value absorbed into next repeat: lit shrinks by 1 seg
                let next_count = next_hv as usize;
                let new_repeat_hdr = encode_signed((next_count + 1) as i64);
                slab.splice(run_end..run_end + next_hl, new_repeat_hdr);
                slab.drain(prev_pos..run_end);
                let new_lit_count = item_count - 1;
                let repeat_pos = if new_lit_count == 0 {
                    slab.drain(run_start..run_start + hdr_len);
                    run_start
                } else {
                    let new_hdr = encode_signed(-(new_lit_count as i64));
                    let new_hdr_len = new_hdr.len();
                    let remaining_len = prev_pos - values_start;
                    slab.splice(run_start..run_start + hdr_len, new_hdr);
                    run_start + new_hdr_len + remaining_len
                };
                let merge_delta = merge_adjacent_repeats::<T>(slab, repeat_pos, prev_run_start);
                return -1 + merge_delta;
            }
        }
    }

    // Check if the first value of this literal matches a preceding repeat.
    if let Some(ps) = prev_run_start {
        let (prev_hl, prev_hv) = match read_signed(&slab[ps..]) {
            Some(x) => x,
            None => return 0,
        };
        if prev_hv > 0 {
            let prev_vs = ps + prev_hl;
            let prev_vl = T::value_len(&slab[prev_vs..]).expect("valid slab");
            let first_vlen = T::value_len(&slab[values_start..]).expect("valid slab");
            if first_vlen == prev_vl
                && slab[values_start..values_start + first_vlen] == slab[prev_vs..prev_vs + prev_vl]
            {
                // First literal value absorbed into prev repeat: lit shrinks by 1 seg
                let prev_count = prev_hv as usize;
                let new_repeat_hdr = encode_signed((prev_count + 1) as i64);
                let new_lit_count = item_count - 1;
                if new_lit_count == 0 {
                    slab.drain(run_start..values_start + first_vlen);
                } else {
                    let new_hdr = encode_signed(-(new_lit_count as i64));
                    let mut replacement =
                        Vec::with_capacity(new_hdr.len() + (run_end - values_start - first_vlen));
                    replacement.extend_from_slice(&new_hdr);
                    replacement.extend_from_slice(&slab[values_start + first_vlen..run_end]);
                    slab.splice(run_start..run_end, replacement);
                }
                let new_repeat_hdr_len = new_repeat_hdr.len();
                slab.splice(ps..ps + prev_hl, new_repeat_hdr);
                let repeat_end = ps + new_repeat_hdr_len + prev_vl;
                let mut delta: i32 = -1;
                if repeat_end < slab.len() {
                    delta += merge_adjacent_repeats::<T>(slab, repeat_end, Some(ps));
                }
                return delta;
            }
        }
    }

    0
}

fn merge_adjacent_lits<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    from: usize,
    prev_run_start: Option<usize>,
) -> i32 {
    let mut merge_start = from;
    let mut prev_count = 0usize;
    let mut prev_hdr_len = 0;

    if let Some(ps) = prev_run_start {
        if let Some((hl, hv)) = read_signed(&slab[ps..]) {
            if hv < 0 {
                merge_start = ps;
                prev_count = (-hv) as usize;
                prev_hdr_len = hl;
            }
        }
    }

    let mut forward_count = 0usize;
    let mut forward_ranges: Vec<(usize, usize)> = vec![];
    let mut chain_end = from;

    while chain_end < slab.len() {
        match read_signed(&slab[chain_end..]) {
            Some((hl, hv)) if hv < 0 => {
                let n = (-hv) as usize;
                forward_count += n;
                let values_start = chain_end + hl;
                let mut values_end = values_start;
                for _ in 0..n {
                    let vl = T::value_len(&slab[values_end..]).expect("valid slab");
                    values_end += vl;
                }
                forward_ranges.push((values_start, values_end));
                chain_end = values_end;
            }
            _ => break,
        }
    }

    let total_runs = if prev_count > 0 { 1 } else { 0 } + forward_ranges.len();
    if total_runs <= 1 {
        return 0;
    }

    let total_items = prev_count + forward_count;
    let hdr = encode_signed(-(total_items as i64));
    let mut merged = Vec::with_capacity(hdr.len() + (chain_end - merge_start));
    merged.extend_from_slice(&hdr);

    if prev_count > 0 {
        merged.extend_from_slice(&slab[merge_start + prev_hdr_len..from]);
    }

    for &(vs, ve) in &forward_ranges {
        merged.extend_from_slice(&slab[vs..ve]);
    }

    slab.splice(merge_start..chain_end, merged);

    // Merging N lit runs into 1 doesn't change segment count (total items preserved).
    // But normalize_literal_at may change it (e.g. by extracting duplicates).
    normalize_literal_at::<T>(slab, merge_start, None)
}

fn merge_adjacent_nulls(slab: &mut Vec<u8>, pos: usize, prev_run_start: Option<usize>) -> i32 {
    let ps = match prev_run_start {
        Some(ps) if pos <= slab.len() => ps,
        _ => return 0,
    };

    let (prev_cb, prev_hv) = match read_signed(&slab[ps..]) {
        Some(x) => x,
        None => return 0,
    };
    if prev_hv != 0 {
        return 0;
    }
    let (prev_ncb, prev_nc) = match read_unsigned(&slab[ps + prev_cb..]) {
        Some(x) => x,
        None => return 0,
    };
    let prev_end = ps + prev_cb + prev_ncb;

    if pos != prev_end {
        return 0;
    }

    if pos >= slab.len() {
        return 0;
    }
    let (cur_cb, cur_hv) = match read_signed(&slab[pos..]) {
        Some(x) => x,
        None => return 0,
    };
    if cur_hv != 0 {
        return 0;
    }
    let (cur_ncb, cur_nc) = match read_unsigned(&slab[pos + cur_cb..]) {
        Some(x) => x,
        None => return 0,
    };
    let cur_end = pos + cur_cb + cur_ncb;

    let merged = null_run_bytes((prev_nc + cur_nc) as usize);
    slab.splice(ps..cur_end, merged);
    -1 // merged two null runs into one
}

fn canonicalize_neighbors<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    pos: usize,
    prev_run_start: Option<usize>,
) -> i32 {
    if pos >= slab.len() {
        return 0;
    }
    let (_, hv) = match read_signed(&slab[pos..]) {
        Some(x) => x,
        None => return 0,
    };
    match hv {
        n if n > 0 => {
            if let Some(ps) = prev_run_start {
                let prev_hv = read_signed(&slab[ps..]).map(|(_, v)| v);
                match prev_hv {
                    Some(pv) if pv > 0 => {
                        return merge_adjacent_repeats::<T>(slab, pos, prev_run_start);
                    }
                    Some(pv) if pv < 0 => {
                        return normalize_literal_at::<T>(slab, ps, None);
                    }
                    _ => {}
                }
            }
            0
        }
        n if n < 0 => {
            if n == -1 {
                let (absorbed, absorb_delta) =
                    absorb_lit1_into_repeat::<T>(slab, pos, prev_run_start);
                if absorbed {
                    return absorb_delta;
                }
            }
            let merge_delta = merge_adjacent_lits::<T>(slab, pos, prev_run_start);
            if merge_delta != 0 {
                merge_delta
            } else {
                normalize_literal_at::<T>(slab, pos, prev_run_start)
            }
        }
        _ => merge_adjacent_nulls(slab, pos, prev_run_start),
    }
}

fn merge_adjacent_repeats<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    pos: usize,
    prev_run_start: Option<usize>,
) -> i32 {
    let ps = match prev_run_start {
        Some(ps) if pos < slab.len() => ps,
        _ => return 0,
    };

    let (prev_hl, prev_hv) = match read_signed(&slab[ps..]) {
        Some(x) => x,
        None => return 0,
    };
    if prev_hv <= 0 {
        return 0;
    }
    let prev_vs = ps + prev_hl;
    let prev_vl = match T::value_len(&slab[prev_vs..]) {
        Some(v) => v,
        None => return 0,
    };
    let prev_end = prev_vs + prev_vl;
    if pos != prev_end {
        return 0;
    }

    let (cur_hl, cur_hv) = match read_signed(&slab[pos..]) {
        Some(x) => x,
        None => return 0,
    };
    if cur_hv <= 0 {
        return 0;
    }
    let cur_vs = pos + cur_hl;
    let cur_vl = match T::value_len(&slab[cur_vs..]) {
        Some(v) => v,
        None => return 0,
    };
    if prev_vl != cur_vl || slab[prev_vs..prev_vs + prev_vl] != slab[cur_vs..cur_vs + cur_vl] {
        return 0;
    }

    let merged_count = (prev_hv + cur_hv) as usize;
    let new_hdr = encode_signed(merged_count as i64);
    let cur_end = cur_vs + cur_vl;
    slab.drain(pos..cur_end);
    slab.splice(ps..ps + prev_hl, new_hdr);
    -1 // merged two repeat runs into one
}

fn absorb_lit1_into_repeat<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    pos: usize,
    prev_run_start: Option<usize>,
) -> (bool, i32) {
    if pos >= slab.len() {
        return (false, 0);
    }
    let (hl, hv) = match read_signed(&slab[pos..]) {
        Some(x) => x,
        None => return (false, 0),
    };
    if hv != -1 {
        return (false, 0);
    }
    let val_start = pos + hl;
    let val_len = match T::value_len(&slab[val_start..]) {
        Some(v) => v,
        None => return (false, 0),
    };
    let lit_end = val_start + val_len;

    // Check next run.
    if lit_end < slab.len() {
        let (next_hl, next_hv) = match read_signed(&slab[lit_end..]) {
            Some(x) => x,
            None => return (false, 0),
        };
        if next_hv > 0 {
            let next_vs = lit_end + next_hl;
            let next_vl = T::value_len(&slab[next_vs..]).expect("valid slab");
            if val_len == next_vl
                && slab[val_start..val_start + val_len] == slab[next_vs..next_vs + next_vl]
            {
                let new_count = next_hv as usize + 1;
                let new_hdr = encode_signed(new_count as i64);
                slab.splice(pos..lit_end + next_hl, new_hdr);
                let merge_delta = merge_adjacent_repeats::<T>(slab, pos, prev_run_start);
                // lit-1 removed (-1), absorbed into repeat (no new seg)
                return (true, -1 + merge_delta);
            }
        }
    }

    // Check prev run.
    if let Some(ps) = prev_run_start {
        let (prev_hl, prev_hv) = match read_signed(&slab[ps..]) {
            Some(x) => x,
            None => return (false, 0),
        };
        if prev_hv > 0 {
            let prev_vs = ps + prev_hl;
            let prev_vl = T::value_len(&slab[prev_vs..]).expect("valid slab");
            if val_len == prev_vl
                && slab[val_start..val_start + val_len] == slab[prev_vs..prev_vs + prev_vl]
            {
                let new_count = prev_hv as usize + 1;
                let new_hdr = encode_signed(new_count as i64);
                let new_hdr_len = new_hdr.len();
                slab.drain(pos..lit_end);
                slab.splice(ps..ps + prev_hl, new_hdr);
                let repeat_end = ps + new_hdr_len + prev_vl;
                let merge_delta = merge_adjacent_repeats::<T>(slab, repeat_end, Some(ps));
                // lit-1 removed (-1), absorbed into repeat (no new seg)
                return (true, -1 + merge_delta);
            }
        }
    }

    (false, 0)
}

fn try_absorb_into_prev<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    scan: &ScanResult,
    vbytes: &[u8],
) -> bool {
    let (ps, plv) = match (scan.prev_run_start, scan.prev_last_value_start) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    let (prev_hl, prev_hv) = match read_signed(&slab[ps..]) {
        Some(x) => x,
        None => return false,
    };
    let last_val_len = T::value_len(&slab[plv..]).expect("valid slab");
    if slab[plv..plv + last_val_len] != *vbytes {
        return false;
    }

    if prev_hv > 0 {
        let prev_count = prev_hv as usize;
        let new_hdr = encode_signed((prev_count + 1) as i64);
        slab.splice(ps..ps + prev_hl, new_hdr);
    } else if prev_hv < 0 {
        let prev_count = (-prev_hv) as usize;
        let mut replacement = vec![];
        if prev_count > 1 {
            replacement.extend(encode_signed(-((prev_count - 1) as i64)));
            replacement.extend_from_slice(&slab[ps + prev_hl..plv]);
        }
        replacement.extend(encode_signed(2));
        replacement.extend_from_slice(vbytes);
        slab.splice(ps..scan.run_start, replacement);
    } else {
        return false;
    }
    true
}

fn try_absorb_into_next<T: RleValue<Encoding = RleEncoding<T>>>(
    slab: &mut Vec<u8>,
    scan: &ScanResult,
    vbytes: &[u8],
) -> bool {
    if scan.run_end >= slab.len() {
        return false;
    }
    let (next_hl, next_hv) = match read_signed(&slab[scan.run_end..]) {
        Some(x) => x,
        None => return false,
    };

    if next_hv > 0 {
        let next_vs = scan.run_end + next_hl;
        let next_vl = T::value_len(&slab[next_vs..]).expect("valid slab");
        if slab[next_vs..next_vs + next_vl] != *vbytes {
            return false;
        }
        let next_count = next_hv as usize;
        let new_hdr = encode_signed((next_count + 1) as i64);
        slab.splice(scan.run_end..scan.run_end + next_hl, new_hdr);
    } else if next_hv < 0 {
        let next_first_val = scan.run_end + next_hl;
        let first_vl = T::value_len(&slab[next_first_val..]).expect("valid slab");
        if slab[next_first_val..next_first_val + first_vl] != *vbytes {
            return false;
        }
        let next_count = (-next_hv) as usize;
        let next_end = {
            let mut pos = next_first_val;
            for _ in 0..next_count {
                let vl = T::value_len(&slab[pos..]).expect("valid slab");
                pos += vl;
            }
            pos
        };
        let mut replacement = vec![];
        replacement.extend(encode_signed(2));
        replacement.extend_from_slice(vbytes);
        if next_count > 1 {
            replacement.extend(encode_signed(-((next_count - 1) as i64)));
            replacement.extend_from_slice(&slab[next_first_val + first_vl..next_end]);
        }
        slab.splice(scan.run_end..next_end, replacement);
    } else {
        return false;
    }
    true
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
fn rle_encode_all_slabs<T: RleValue<Encoding = RleEncoding<T>>>(
    values: Vec<T>,
    max_segments: usize,
) -> Vec<(Vec<u8>, usize, usize)> {
    if values.is_empty() {
        return vec![];
    }

    // Phase 1: Pack into a flat buffer.
    let n = values.len();
    let mut pack_buf = Vec::with_capacity(n * 2);
    let mut entries = Vec::with_capacity(n);
    for value in &values {
        let start = pack_buf.len();
        let is_value = T::pack(value.as_get(), &mut pack_buf);
        let len = pack_buf.len() - start;
        entries.push(PackEntry {
            offset: start as u32,
            len: len as u16,
            is_value,
        });
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

    slabs
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
fn rle_load_and_verify<T: RleValue<Encoding = RleEncoding<T>>>(
    data: &[u8],
    max_segments: usize,
    validate: Option<for<'a> fn(<T as super::ColumnValue>::Get<'a>) -> Option<String>>,
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
                    if let Some(msg) = validate(T::get_null(data)) {
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
