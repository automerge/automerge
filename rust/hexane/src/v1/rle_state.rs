//! RLE encoding state machine for cursor-aware splice.

use super::rle::{encode_signed, encode_unsigned};
use super::RleValue;


/// Returned when flushing a Lit whose header is not in our buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RewriteHeader {
    pub count: usize,
    pub pos: usize,
}

impl RewriteHeader {
    fn new(count: usize, pos: usize) -> Self {
        Self { count, pos }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FlushState {
    pub segments: usize,
    pub rewrite: Option<RewriteHeader>,
}

impl FlushState {
    fn new(segments: usize) -> Self {
        Self {
            segments,
            rewrite: None,
        }
    }
    fn rewrite(segments: usize, r: RewriteHeader) -> Self {
        Self {
            segments,
            rewrite: Some(r),
        }
    }
    #[allow(dead_code)]
    fn with_segments(mut self, segments: usize) -> Self {
        self.segments += segments;
        self
    }
}

/// Encoding state for RLE splice operations.
///
/// `'a` is the lifetime of the slab being read. The state borrows values
/// (e.g. `&'a str` for String columns) from the slab. All borrowed data
/// must be written to the output buffer before `RleState` is dropped and
/// the slab is mutated.
#[derive(Debug)]
pub(crate) enum RleState<'a, T: RleValue> {
    Empty,
    Lone(T::Get<'a>),
    Run(usize, T::Get<'a>),
    /// `count`: items already written to buf (after header at `header_pos`).
    /// `local`: items written by THIS state machine (== count when header is ours).
    /// `current`: latest value, not yet written.
    Lit {
        count: usize,
        local: usize,
        header_pos: usize,
        current: T::Get<'a>,
    },
    Null(usize),
}

impl<'a, T: RleValue> RleState<'a, T> {
    pub fn new() -> Self {
        RleState::Empty
    }

    /// Feed one value. Returns segments flushed to buf.
    pub fn append(&mut self, buf: &mut Vec<u8>, value: T::Get<'a>) -> FlushState {
        self.append_n(buf, value, 1)
    }

    /// Feed `count` copies of `value`. Returns segments flushed.
    pub fn append_n(&mut self, buf: &mut Vec<u8>, value: T::Get<'a>, n: usize) -> FlushState {
        if n == 0 {
            return FlushState::default();
        }

        if T::is_null(value) {
            return self.append_null_n(buf, n);
        }
        let mut flushed = FlushState::default();
        let old = std::mem::replace(self, RleState::Empty);
        *self = match old {
            RleState::Empty if n == 1 => RleState::Lone(value),
            RleState::Empty => RleState::Run(n, value),
            RleState::Lone(prev) if value == prev => RleState::Run(n + 1, value),
            RleState::Lone(prev) if n == 1 => {
                let header_pos = emit_lit::<T>(buf, prev);
                RleState::Lit {
                    count: 1,
                    local: 1,
                    header_pos,
                    current: value,
                }
            }
            RleState::Lone(prev) => {
                emit_lit::<T>(buf, prev);
                flushed.segments = 1;
                RleState::Run(n, value)
            }
            RleState::Run(count, prev) if value == prev => RleState::Run(count + n, value),
            RleState::Run(count, prev) => {
                emit_run::<T>(buf, count, prev);
                flushed.segments = 1;
                RleState::run(n, value)
            }
            // Lit with count=0, local=0: no header written yet. Behaves like Lone.
            RleState::Lit {
                count: 0,
                local: 0,
                header_pos,
                current,
            } if value == current => {
                flushed.rewrite = Some(RewriteHeader::new(0, header_pos));
                RleState::Run(n + 1, value)
            }
            RleState::Lit {
                count: 0,
                local: 0,
                header_pos,
                current,
            } if n == 1 => {
                let hp = emit_lit::<T>(buf, current);
                flushed.rewrite = Some(RewriteHeader::new(0, header_pos));
                RleState::Lit {
                    count: 1,
                    local: 1,
                    header_pos: hp,
                    current: value,
                }
            }
            RleState::Lit {
                count: 0,
                local: 0,
                header_pos,
                current,
            } => {
                emit_lit::<T>(buf, current);
                flushed.rewrite = Some(RewriteHeader::new(0, header_pos));
                flushed.segments = 1;
                RleState::Run(n, value)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } if value == current => {
                if local == count {
                    rewrite_lit_header(buf, header_pos, count);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(count, header_pos));
                }
                flushed.segments = local;
                RleState::Run(n + 1, value)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } if n == 1 => {
                T::pack(current, buf);
                Self::next_lit(count, local, header_pos, value)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } => {
                T::pack(current, buf);
                if local == count {
                    rewrite_lit_header(buf, header_pos, count + 1);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(count + 1, header_pos));
                }
                flushed.segments = local + 1;
                RleState::Run(n, value)
            }
            RleState::Null(count) => {
                emit_null(buf, count);
                flushed.segments = 1;
                RleState::run(n, value)
            }
        };
        flushed
    }

    fn next_lit(count: usize, local: usize, header_pos: usize, current: T::Get<'a>) -> Self {
        RleState::Lit {
            count: count + 1,
            local: local + 1,
            header_pos,
            current,
        }
    }

    pub fn lit(count: usize, current: T::Get<'a>, header_pos: usize) -> RleState<'a, T> {
        if count == 0 {
            Self::Lone(current)
        } else {
            Self::Lit {
                count,
                local: 0,
                current,
                header_pos,
            }
        }
    }

    pub fn run(n: usize, value: T::Get<'a>) -> RleState<'a, T> {
        match n {
            0 => Self::Empty,
            1 => Self::Lone(value),
            n => Self::Run(n, value),
        }
    }


    /// Feed `count` null values. Returns segments flushed.
    pub fn append_null_n(&mut self, buf: &mut Vec<u8>, count: usize) -> FlushState {
        let mut flushed = FlushState::default();
        let old = std::mem::replace(self, RleState::Empty);
        *self = match old {
            RleState::Empty => RleState::Null(count),
            RleState::Null(n) => RleState::Null(n + count),
            other => {
                flushed = Self::do_flush(other, buf);
                RleState::Null(count)
            }
        };
        flushed
    }

    /// Final flush with possible trailing lit count for the lit header
    /// consumes the state
    pub fn flush_with_lit(&mut self, buf: &mut Vec<u8>, lit: usize) -> FlushState {
        let old = std::mem::replace(self, RleState::Empty);
        if lit == 0 {
            return Self::do_flush(old, buf);
        }

        match old {
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } => {
                T::pack(current, buf);
                let total = count + 1 + lit;
                if local == count {
                    rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(local + 1)
                } else {
                    let rewrite = RewriteHeader::new(total, header_pos);
                    FlushState::rewrite(local + 1, rewrite)
                }
            }
            RleState::Lone(value) => {
                let total = 1 + lit;
                buf.extend(encode_signed(-(total as i64)));
                T::pack(value, buf);
                FlushState::new(1)
            }
            other => {
                let flushed = Self::do_flush(other, buf);
                buf.extend(encode_signed(-(lit as i64)));
                flushed
            }
        }
    }

    /// Append raw suffix bytes. See rle_state module docs.
    #[allow(dead_code)]
    pub fn append_raw(
        &mut self,
        buf: &mut Vec<u8>,
        raw: &[u8],
        segments: usize,
        lit: usize,
    ) -> FlushState {
        let old = std::mem::replace(self, RleState::Empty);

        if lit == 0 {
            let flushed = Self::do_flush(old, buf);
            buf.extend_from_slice(raw);
            return flushed.with_segments(segments);
        }

        match old {
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } => {
                T::pack(current, buf);
                buf.extend_from_slice(raw);
                let total = count + 1 + lit;
                if local == count {
                    rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(local + 1 + segments)
                } else {
                    let rewrite = RewriteHeader::new(total, header_pos);
                    FlushState::rewrite(local + 1 + segments, rewrite)
                }
            }
            RleState::Lone(value) => {
                let total = 1 + lit;
                buf.extend(encode_signed(-(total as i64)));
                T::pack(value, buf);
                buf.extend_from_slice(raw);
                FlushState::new(1 + segments)
            }
            other => {
                let flushed = Self::do_flush(other, buf);
                buf.extend(encode_signed(-(lit as i64)));
                buf.extend_from_slice(raw);
                flushed.with_segments(lit + segments)
            }
        }
    }

    /// Flush pending state. Returns (segments, optional header rewrite).
    pub fn flush(&mut self, buf: &mut Vec<u8>) -> FlushState {
        let old = std::mem::replace(self, RleState::Empty);
        Self::do_flush(old, buf)
    }

    pub fn do_flush(state: Self, buf: &mut Vec<u8>) -> FlushState {
        match state {
            RleState::Empty => FlushState::default(),
            RleState::Lone(value) => {
                buf.extend(encode_signed(-1));
                T::pack(value, buf);
                FlushState::new(1)
            }
            RleState::Run(count, value) => {
                emit_run::<T>(buf, count, value);
                FlushState::new(1)
            }
            // Lit with count=0, local=0: no header yet, flush like Lone.
            // Emit RewriteHeader(0) to signal the old slab header should be removed.
            RleState::Lit {
                count: 0,
                local: 0,
                header_pos,
                current,
            } => {
                buf.extend(encode_signed(-1));
                T::pack(current, buf);
                FlushState::rewrite(1, RewriteHeader::new(0, header_pos))
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
            } => {
                T::pack(current, buf);
                let total = count + 1;
                if local == count {
                    rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(total)
                } else {
                    // only return locally flushed segments
                    // but rewrite the header with the total
                    FlushState::rewrite(local + 1, RewriteHeader::new(total, header_pos))
                }
            }
            RleState::Null(count) => {
                emit_null(buf, count);
                FlushState::new(1)
            }
        }
    }
}

fn emit_lit<T: RleValue>(buf: &mut Vec<u8>, value: T::Get<'_>) -> usize {
    let header_pos = buf.len();
    buf.extend(encode_signed(-1));
    T::pack(value, buf);
    header_pos
}

fn emit_run<T: RleValue>(buf: &mut Vec<u8>, count: usize, value: T::Get<'_>) {
    if count == 1 {
        buf.extend(encode_signed(-1));
    } else {
        buf.extend(encode_signed(count as i64));
    }
    T::pack(value, buf);
}

fn emit_null(buf: &mut Vec<u8>, count: usize) {
    buf.extend(encode_signed(0));
    buf.extend(encode_unsigned(count as u64));
}

pub(crate) fn rewrite_lit_header(buf: &mut Vec<u8>, header_pos: usize, total: usize) {
    if total == 0 {
        // Remove the header entirely — the lit run is gone.
        let old_header_len = {
            let mut tmp = &buf[header_pos..];
            let start = tmp.len();
            let _ = leb128::read::signed(&mut tmp);
            start - tmp.len()
        };
        buf.splice(header_pos..header_pos + old_header_len, std::iter::empty());
        return;
    }
    let new_header = encode_signed(-(total as i64));
    let old_header_len = {
        let mut tmp = &buf[header_pos..];
        let start = tmp.len();
        let _ = leb128::read::signed(&mut tmp);
        start - tmp.len()
    };
    let new_len = new_header.len as usize;
    if new_len == old_header_len {
        buf[header_pos..header_pos + new_len].copy_from_slice(&new_header.buf[..new_len]);
    } else {
        buf.splice(
            header_pos..header_pos + old_header_len,
            new_header,
        );
    }
}

/// Encode values into canonical RLE bytes. Returns (items, segments).
///
/// Values come from an iterator (owned, not borrowed from a slab),
/// so the state uses `'static` lifetime.
#[allow(dead_code)]
pub(crate) fn rle_encode_state<T: RleValue>(
    values: impl Iterator<Item = T::Get<'static>>,
    buf: &mut Vec<u8>,
) -> (usize, usize)
where
    T::Get<'static>: 'static,
{
    let mut state = RleState::<'static, T>::new();
    let mut segments = 0;
    let mut items = 0;
    for v in values {
        items += 1;
        segments += state.append(buf, v).segments;
    }
    let f = state.flush(buf);
    segments += f.segments;
    (items, segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::rle::rle_validate_encoding;

    fn encode_vals(vals: &[u64]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut state = RleState::<u64>::new();
        for &v in vals {
            state.append(&mut buf, v);
        }
        state.flush(&mut buf);
        buf
    }

    fn decode(buf: &[u8]) -> Vec<u64> {
        use crate::v1::rle::read_signed;
        let mut result = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (cb, raw) = read_signed(&buf[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    let (vl, val) = u64::try_unpack(&buf[pos + cb..]).unwrap();
                    for _ in 0..n as usize {
                        result.push(val);
                    }
                    pos += cb + vl;
                }
                n if n < 0 => {
                    let mut scan = pos + cb;
                    for _ in 0..(-n) as usize {
                        let (vl, val) = u64::try_unpack(&buf[scan..]).unwrap();
                        result.push(val);
                        scan += vl;
                    }
                    pos = scan;
                }
                _ => {
                    let (ncb, nc) = crate::v1::rle::read_unsigned(&buf[pos + cb..]).unwrap();
                    for _ in 0..nc as usize {
                        result.push(0);
                    }
                    pos += cb + ncb;
                }
            }
        }
        result
    }

    fn check(vals: &[u64]) {
        let buf = encode_vals(vals);
        if let Err(e) = rle_validate_encoding::<u64>(&buf) {
            panic!("invalid encoding for {vals:?}: {e}\n  bytes: {buf:?}");
        }
        assert_eq!(decode(&buf), vals, "roundtrip failed for {vals:?}");
    }

    #[test]
    fn empty() {
        check(&[]);
    }
    #[test]
    fn single() {
        check(&[42]);
    }
    #[test]
    fn repeat() {
        check(&[7, 7, 7, 7, 7]);
    }
    #[test]
    fn literal() {
        check(&[1, 2, 3, 4]);
    }
    #[test]
    fn lit_then_repeat() {
        check(&[1, 2, 3, 3, 3]);
    }
    #[test]
    fn repeat_then_lit() {
        check(&[5, 5, 5, 1, 2, 3]);
    }
    #[test]
    fn lit_repeat_lit() {
        check(&[1, 2, 3, 3, 3, 4, 5]);
    }
    #[test]
    fn adjacent_repeats() {
        check(&[1, 1, 1, 2, 2, 2]);
    }
    #[test]
    fn lone_between_repeats() {
        check(&[1, 1, 2, 3, 3]);
    }
    #[test]
    fn two_same() {
        check(&[9, 9]);
    }
    #[test]
    fn two_different() {
        check(&[1, 2]);
    }
    #[test]
    fn long_literal() {
        check(&(0..100).collect::<Vec<_>>());
    }
    #[test]
    fn alternating() {
        check(
            &(0..20)
                .map(|i| if i % 2 == 0 { 1 } else { 2 })
                .collect::<Vec<_>>(),
        );
    }
    #[test]
    fn roundtrip_complex() {
        check(&[1, 2, 3, 3, 3, 4, 5, 5, 6, 7, 7, 7, 7, 8]);
    }
    #[test]
    fn single_then_repeat() {
        check(&[1, 2, 2]);
    }
    #[test]
    fn repeat_then_single() {
        check(&[2, 2, 1]);
    }

    #[test]
    fn nullable_with_nulls() {
        let vals: Vec<Option<u64>> = vec![Some(1), None, None, Some(2), Some(2)];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>>::new();
        for &v in &vals {
            state.append(&mut buf, v);
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }
    #[test]
    fn nullable_null_value_null() {
        let vals: Vec<Option<u64>> = vec![None, Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>>::new();
        for &v in &vals {
            state.append(&mut buf, v);
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }
    #[test]
    fn nullable_value_then_null() {
        let vals: Vec<Option<u64>> = vec![Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>>::new();
        for &v in &vals {
            state.append(&mut buf, v);
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }

    #[test]
    fn append_raw_after_run() {
        // Bug: append_raw with Run state + lit > 0 wrote lit header before flushing Run.
        let mut buf = Vec::new();
        let mut state = RleState::<u64>::new();
        // Build a Run(3, 5)
        state.append_n(&mut buf, 5, 3);
        // Now append_raw with lit=2, raw bytes = packed values [1, 2]
        let mut raw = Vec::new();
        u64::pack(1, &mut raw);
        u64::pack(2, &mut raw);
        state.append_raw(&mut buf, &raw, 0, 2);
        // Result should be: repeat(3, 5) + lit(-2, 1, 2)
        rle_validate_encoding::<u64>(&buf).unwrap();
        // Decode and verify
        let decoded: Vec<u64> = {
            use crate::v1::rle::RleDecoder;
            use crate::v1::ValidBuf;
            let vb = ValidBuf::new(buf);
            RleDecoder::<u64>::new(&vb).collect()
        };
        assert_eq!(decoded, vec![5, 5, 5, 1, 2]);
    }

    // #[test]
    fn string_mixed_lifetimes() {
        let slab_bytes = b"\x05hello"; // a valid RLE-encoded string slab
        let mut buf = Vec::new();
        let mut state = RleState::<'_, String>::new();

        // Feed a value decoded from the slab — borrows slab_bytes.
        let (_, slab_val) = String::try_unpack(slab_bytes).unwrap();
        state.append(&mut buf, slab_val);

        // Feed an owned value — its &str has a DIFFERENT lifetime.
        let owned = String::from("world");
        state.append(&mut buf, owned.as_str());
        state.flush(&mut buf);
    }
}
