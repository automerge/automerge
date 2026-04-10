//! Streaming encoder for building column data from a sequence of values.
//!
//! Unlike [`Column`](super::Column) which supports random-access splice,
//! `Encoder<T>` is append-only and produces a single contiguous byte buffer.
//! This is used for building change data where values arrive in order.
//!
//! ```ignore
//! let mut enc = Encoder::<u64>::new();
//! enc.append(1);
//! enc.append(1);
//! enc.append(2);
//! let bytes = enc.save();
//! ```

use super::leb::encode_count;
use super::rle::state::{FlushState, RleCow, RleState};
use super::RleValue;

use std::ops::Range;

// ── RLE Encoder ─────────────────────────────────────────────────────────────

/// Streaming encoder for RLE-encoded types (`u64`, `i64`, `String`, `Option<u64>`, etc.).
///
/// Accepts values via [`append`](RleEncoder::append) and
/// [`append_n`](RleEncoder::append_n), then produces the encoded bytes
/// with [`save`](RleEncoder::save) or [`save_to`](RleEncoder::save_to).
/// Both output methods consume the encoder.
///
/// The lifetime `'a` ties borrowed values (e.g. `&'a str` for `String` columns)
/// to the encoder. For `Copy` types like `u64`, `'a` is typically `'static`.
pub struct RleEncoder<'a, T: RleValue> {
    buf: Vec<u8>,
    state: RleState<'a, T, T>,
    flush: FlushState,
    len: usize,
}

impl<T: RleValue> Default for RleEncoder<'_, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: RleValue> std::fmt::Debug for RleEncoder<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RleEncoder")
            .field("len", &self.len)
            .field("buf_len", &self.buf.len())
            .finish()
    }
}

impl<'a, T: RleValue> RleEncoder<'a, T> {
    /// Create a new empty encoder.
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            state: RleState::Empty,
            flush: FlushState::default(),
            len: 0,
        }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append a single value.
    pub fn append(&mut self, value: T::Get<'a>) {
        self.flush += self.state.append(&mut self.buf, RleCow::Ref(value));
        self.len += 1;
    }

    /// Append `n` copies of `value`.
    pub fn append_n(&mut self, value: T::Get<'a>, n: usize) {
        self.flush += self.state.append_n(&mut self.buf, RleCow::Ref(value), n);
        self.len += n;
    }

    /// Append all values from an iterator.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = T::Get<'a>>) {
        for value in iter {
            self.append(value);
        }
    }

    fn finish(&mut self) {
        self.flush += self.state.flush(&mut self.buf);
    }

    /// Flush and return the encoded bytes. Consumes the encoder.
    pub fn save(mut self) -> Vec<u8> {
        self.finish();
        self.buf
    }

    /// Flush and append the encoded bytes to `out`. Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(mut self, out: &mut Vec<u8>) -> Range<usize> {
        self.finish();
        let start = out.len();
        out.extend_from_slice(&self.buf);
        start..out.len()
    }

    /// Like [`save_to`](Self::save_to) but returns an empty range if the
    /// encoded data is empty or consists entirely of a single run of `value`.
    /// Like [`save_to`](Self::save_to) but returns an empty range if the
    /// encoded data is empty or consists entirely of a single run of `value`.
    pub fn save_to_unless(self, out: &mut Vec<u8>, value: T::Get<'a>) -> Range<usize> {
        if self.flush.segments == 0 && self.state.is_single_run_of(RleCow::Ref(value)) {
            return out.len()..out.len();
        }
        self.save_to(out)
    }
}

impl<'a, T: RleValue> super::encoding::EncoderApi<'a, T> for RleEncoder<'a, T> {
    type Tail = super::rle::RleTail;
    fn append(&mut self, value: T::Get<'a>) {
        self.append(value);
    }
    fn append_n(&mut self, value: T::Get<'a>, n: usize) {
        self.append_n(value, n);
    }
    fn extend(&mut self, iter: impl IntoIterator<Item = T::Get<'a>>) {
        self.extend(iter);
    }
    fn len(&self) -> usize {
        self.len
    }
    fn save(self) -> Vec<u8> {
        self.save()
    }
    fn save_to(self, out: &mut Vec<u8>) -> Range<usize> {
        self.save_to(out)
    }
    fn save_to_unless(self, out: &mut Vec<u8>, value: T::Get<'a>) -> Range<usize> {
        self.save_to_unless(out, value)
    }
    fn into_slab(mut self) -> super::column::Slab<Self::Tail> {
        self.finish();
        let tail = self.flush.wpos.as_tail(0, self.buf.len());
        super::column::Slab {
            data: self.buf,
            len: self.len,
            segments: self.flush.segments,
            tail,
        }
    }
}

// ── Bool Encoder ────────────────────────────────────────────────────────────

/// Streaming encoder for boolean columns.
///
/// Uses the alternating run-length format: `[false_count, true_count, false_count, ...]`.
pub struct BoolEncoder {
    buf: Vec<u8>,
    cur_value: bool,
    cur_count: usize,
    segments: usize,
    len: usize,
}

impl Default for BoolEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BoolEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoolEncoder")
            .field("len", &self.len)
            .field("buf_len", &self.buf.len())
            .finish()
    }
}

impl BoolEncoder {
    /// Create a new empty encoder.
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            cur_value: false,
            cur_count: 0,
            segments: 0,
            len: 0,
        }
    }

    /// Number of items appended so far.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no items have been appended.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append a single boolean value.
    pub fn append(&mut self, value: bool) {
        if value == self.cur_value {
            self.cur_count += 1;
        } else {
            self.flush_run();
            self.cur_value = value;
            self.cur_count = 1;
        }
        self.len += 1;
    }

    /// Append `n` copies of `value`.
    pub fn append_n(&mut self, value: bool, n: usize) {
        if n == 0 {
            return;
        }
        if value == self.cur_value {
            self.cur_count += n;
        } else {
            self.flush_run();
            self.cur_value = value;
            self.cur_count = n;
        }
        self.len += n;
    }

    fn flush_run(&mut self) {
        if self.cur_count > 0 || self.buf.is_empty() {
            self.buf.extend(encode_count(self.cur_count));
            self.segments += 1;
            self.cur_count = 0;
            self.cur_value = !self.cur_value;
        }
    }

    /// Flush and return the encoded bytes. Consumes the encoder.
    pub fn save(mut self) -> Vec<u8> {
        self.flush_final();
        self.buf
    }

    /// Flush and append the encoded bytes to `out`. Consumes the encoder.
    /// Returns the byte range written.
    pub fn save_to(mut self, out: &mut Vec<u8>) -> Range<usize> {
        self.flush_final();
        let start = out.len();
        out.extend_from_slice(&self.buf);
        start..out.len()
    }

    /// Like [`save_to`](Self::save_to) but returns an empty range if the
    /// encoded data is empty or consists entirely of a single run of `value`.
    pub fn save_to_unless(mut self, out: &mut Vec<u8>, value: bool) -> Range<usize> {
        use super::encoding::RunDecoder;
        self.flush_final();
        if self.segments <= 1 {
            if let Some(run) = super::bool::BoolDecoder::new(&self.buf).next_run() {
                if run.value == value {
                    return out.len()..out.len();
                }
            } else {
                return out.len()..out.len();
            }
        }
        let start = out.len();
        out.extend_from_slice(&self.buf);
        start..out.len()
    }

    fn flush_final(&mut self) {
        if self.cur_count > 0 {
            self.buf.extend(encode_count(self.cur_count));
            self.segments += 1;
        }
    }

    /// Append all values from an iterator.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = bool>) {
        for value in iter {
            self.append(value);
        }
    }
}

impl<'a> super::encoding::EncoderApi<'a, bool> for BoolEncoder {
    type Tail = u8;
    fn append(&mut self, value: bool) {
        self.append(value);
    }
    fn append_n(&mut self, value: bool, n: usize) {
        self.append_n(value, n);
    }
    fn extend(&mut self, iter: impl IntoIterator<Item = bool>) {
        self.extend(iter);
    }
    fn len(&self) -> usize {
        self.len
    }
    fn save(self) -> Vec<u8> {
        self.save()
    }
    fn save_to(self, out: &mut Vec<u8>) -> Range<usize> {
        self.save_to(out)
    }
    fn save_to_unless(self, out: &mut Vec<u8>, value: bool) -> Range<usize> {
        self.save_to_unless(out, value)
    }
    fn into_slab(mut self) -> super::column::Slab<Self::Tail> {
        self.flush_final();
        let tail = if self.segments > 0 {
            let mut pos = self.buf.len();
            while pos > 0 && self.buf[pos - 1] & 0x80 != 0 {
                pos -= 1;
            }
            pos = pos.saturating_sub(1);
            (self.buf.len() - pos) as u8
        } else {
            0
        };
        super::column::Slab {
            data: self.buf,
            len: self.len,
            segments: self.segments,
            tail,
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::v1::bool::{BoolDecoder, BoolEncoding};
    use crate::v1::encoding::{ColumnEncoding, EncoderApi};
    use crate::v1::rle::{RleDecoder, RleEncoding};
    use crate::v1::{Column, ColumnValueRef, Encoder};

    /// Create an encoder for type T via the encoding trait.
    fn encoder<'a, T: ColumnValueRef>() -> Encoder<'a, T> {
        T::Encoding::encoder()
    }

    #[test]
    fn rle_encoder_u64() {
        let mut enc = encoder::<u64>();
        enc.append(1u64);
        enc.append(1u64);
        enc.append(1u64);
        enc.append(2u64);
        enc.append(3u64);
        assert_eq!(enc.len(), 5);

        let bytes = enc.save();
        let info = RleEncoding::<u64>::validate_encoding(&bytes).unwrap();
        assert_eq!(info.len, 5);

        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals, vec![1, 1, 1, 2, 3]);
    }

    #[test]
    fn rle_encoder_append_n() {
        let mut enc = encoder::<u64>();
        enc.append_n(7u64, 100);
        enc.append(8u64);
        assert_eq!(enc.len(), 101);

        let bytes = enc.save();
        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals.len(), 101);
        assert!(vals[..100].iter().all(|&v| v == 7));
        assert_eq!(vals[100], 8);
    }

    #[test]
    fn rle_encoder_string() {
        let mut enc = encoder::<String>();
        enc.append("hello");
        enc.append("hello");
        enc.append("world");
        assert_eq!(enc.len(), 3);

        let bytes = enc.save();
        let vals: Vec<&str> = RleDecoder::<String>::new(&bytes).collect();
        assert_eq!(vals, vec!["hello", "hello", "world"]);
    }

    #[test]
    fn rle_encoder_nullable() {
        let mut enc = encoder::<Option<u64>>();
        enc.append(Some(1u64));
        enc.append(None);
        enc.append(None);
        enc.append(Some(2u64));
        assert_eq!(enc.len(), 4);

        let bytes = enc.save();
        let vals: Vec<Option<u64>> = RleDecoder::<Option<u64>>::new(&bytes).collect();
        assert_eq!(vals, vec![Some(1), None, None, Some(2)]);
    }

    #[test]
    fn rle_encoder_save_to() {
        let mut enc = encoder::<u64>();
        enc.append(42u64);
        enc.append(42u64);

        let mut out = vec![0xFF]; // prefix byte
        let range = enc.save_to(&mut out);
        assert_eq!(range.start, 1);
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_empty() {
        let enc = encoder::<u64>();
        assert!(enc.is_empty());
        let bytes = enc.save();
        assert!(bytes.is_empty());
    }

    #[test]
    fn bool_encoder_basic() {
        let mut enc = encoder::<bool>();
        enc.append(false);
        enc.append(false);
        enc.append(true);
        enc.append(true);
        enc.append(true);
        enc.append(false);
        assert_eq!(enc.len(), 6);

        let bytes = enc.save();
        let vals: Vec<bool> = BoolDecoder::new(&bytes).collect();
        assert_eq!(vals, vec![false, false, true, true, true, false]);
    }

    #[test]
    fn bool_encoder_starts_true() {
        let mut enc = encoder::<bool>();
        enc.append(true);
        enc.append(true);
        enc.append(false);
        assert_eq!(enc.len(), 3);

        let bytes = enc.save();
        // Wire format: [0 false, 2 true, 1 false]
        let vals: Vec<bool> = BoolDecoder::new(&bytes).collect();
        assert_eq!(vals, vec![true, true, false]);
    }

    #[test]
    fn bool_encoder_append_n() {
        let mut enc = encoder::<bool>();
        enc.append_n(true, 100);
        enc.append_n(false, 50);
        assert_eq!(enc.len(), 150);

        let bytes = enc.save();
        let vals: Vec<bool> = BoolDecoder::new(&bytes).collect();
        assert_eq!(vals.len(), 150);
        assert!(vals[..100].iter().all(|&v| v));
        assert!(vals[100..].iter().all(|&v| !v));
    }

    #[test]
    fn bool_encoder_empty() {
        let enc = encoder::<bool>();
        assert!(enc.is_empty());
        let bytes = enc.save();
        assert!(bytes.is_empty());
    }

    #[test]
    fn bool_encoder_save_to_unless() {
        // Empty encoder — skipped regardless of value
        let enc = encoder::<bool>();
        let mut out = vec![];
        let range = enc.save_to_unless(&mut out, false);
        assert!(range.is_empty());

        // Single run of false — skipped when value=false
        let mut enc = encoder::<bool>();
        enc.append_n(false, 10);
        let range = enc.save_to_unless(&mut out, false);
        assert!(range.is_empty());

        // Single run of true — NOT skipped when value=false
        let mut enc = encoder::<bool>();
        enc.append_n(true, 10);
        let range = enc.save_to_unless(&mut out, false);
        assert!(!range.is_empty());

        // Mixed — never skipped
        out.clear();
        let mut enc = encoder::<bool>();
        enc.append(true);
        enc.append(false);
        let range = enc.save_to_unless(&mut out, false);
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_save_to_unless() {
        let mut out = vec![];

        // Empty — skipped
        let enc = encoder::<u64>();
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(range.is_empty());

        // Single run of 0 — skipped when value=0
        let mut enc = encoder::<u64>();
        enc.append_n(0u64, 100);
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(range.is_empty());

        // Single run of 0 — NOT skipped when value=1
        let mut enc = encoder::<u64>();
        enc.append_n(0u64, 100);
        let range = enc.save_to_unless(&mut out, 1u64);
        assert!(!range.is_empty());

        // Multiple values — never skipped
        out.clear();
        let mut enc = encoder::<u64>();
        enc.append(1u64);
        enc.append(2u64);
        let range = enc.save_to_unless(&mut out, 0u64);
        assert!(!range.is_empty());
    }

    #[test]
    fn nullable_encoder_save_to_unless() {
        let mut out = vec![];

        // All nulls — skipped when value=None
        let mut enc = encoder::<Option<u64>>();
        enc.append_n(None, 50);
        let range = enc.save_to_unless(&mut out, None);
        assert!(range.is_empty());

        // All nulls — NOT skipped when value=Some(0)
        let mut enc = encoder::<Option<u64>>();
        enc.append_n(None, 50);
        let range = enc.save_to_unless(&mut out, Some(0u64));
        assert!(!range.is_empty());
    }

    #[test]
    fn rle_encoder_roundtrip_with_column() {
        let values = [1u64, 2, 3, 3, 3, 4, 5, 5, 6];
        let bytes = Encoder::<u64>::encode(values);
        let col = Column::<u64>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }

    #[test]
    fn bool_encoder_roundtrip_with_column() {
        let values = [true, false, true, true, false, false, true];
        let bytes = Encoder::<bool>::encode(values);
        let col = Column::<bool>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }

    #[test]
    fn rle_encode_slab() {
        let slab = Encoder::<u64>::encode_slab([1u64, 1, 1, 2, 3]);
        assert_eq!(slab.len, 5);
        assert!(slab.segments > 0);
        let info = RleEncoding::<u64>::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);
    }

    #[test]
    fn bool_encode_slab() {
        let slab = Encoder::<bool>::encode_slab([false, true, true, false]);
        assert_eq!(slab.len, 4);
        assert_eq!(slab.segments, 3);
        let info = BoolEncoding::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);
    }

    #[test]
    fn encoder_extend() {
        let bytes = Encoder::<u64>::encode([1u64, 2, 3, 3, 3]);
        let vals: Vec<u64> = RleDecoder::<u64>::new(&bytes).collect();
        assert_eq!(vals, vec![1, 2, 3, 3, 3]);
    }

    #[test]
    fn bool_encoder_extend() {
        let bytes = Encoder::<bool>::encode([true, true, false, true]);
        let vals: Vec<bool> = BoolDecoder::new(&bytes).collect();
        assert_eq!(vals, vec![true, true, false, true]);
    }

    #[test]
    fn encoder_100_unique_strings() {
        let values: Vec<String> = (0..100).map(|i| format!("item_{i:04}")).collect();
        let mut enc = encoder::<String>();
        for s in &values {
            enc.append(s.as_str());
        }
        let bytes = enc.save();
        let col = Column::<String>::load(&bytes).unwrap();
        let loaded: Vec<&str> = col.iter().collect();
        let expected: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        assert_eq!(loaded, expected);
    }

    #[test]
    fn encoder_nullable_with_runs_and_nulls() {
        use rand::{RngExt, SeedableRng};
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let choices: [Option<u64>; 4] = [None, Some(1), Some(2), Some(3)];
        let values: Vec<Option<u64>> = (0..100).map(|_| choices[rng.random_range(0..4)]).collect();

        let mut enc = encoder::<Option<u64>>();
        for &v in &values {
            enc.append(v);
        }
        assert_eq!(enc.len(), 100);

        // Validate encode_slab metadata
        let slab = Encoder::<Option<u64>>::encode_slab(values.iter().copied());
        let info = RleEncoding::<Option<u64>>::validate_encoding(&slab.data).unwrap();
        assert_eq!(info.len, slab.len);
        assert_eq!(info.segments, slab.segments);
        assert_eq!(info.tail, slab.tail);

        // Validate save → load roundtrip
        let bytes = enc.save();
        let col = Column::<Option<u64>>::load(&bytes).unwrap();
        assert_eq!(col.to_vec(), values);
    }
}
