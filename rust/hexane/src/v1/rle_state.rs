//! RLE encoding state machine for cursor-aware splice.

use super::rle::{encode_signed, encode_unsigned, RleTail};
use super::{AsColumnRef, RleValue};

use std::num::NonZeroU32;
use std::ops::Range;

/// A Cow-like type for RLE values. Holds either an owned value from the
/// splice iterator (`V`) or a borrowed value decoded from the slab (`T::Get<'a>`).
///
/// Derefs to `T::Get<'_>` via `as_ref()`, and implements `AsColumnRef<T>`
/// so it can be used directly with `T::pack()`.
#[derive(Debug)]
pub(crate) enum RleCow<'a, T: RleValue, V: AsColumnRef<T>> {
    Owned(V),
    Ref(T::Get<'a>),
}

impl<'a, T: RleValue, V: AsColumnRef<T> + Copy> Copy for RleCow<'a, T, V> {}

impl<'a, T: RleValue, V: AsColumnRef<T>> Clone for RleCow<'a, T, V> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(v) => Self::Owned(v.clone()),
            Self::Ref(g) => Self::Ref(*g),
        }
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> RleCow<'a, T, V> {
    /// Get as T::Get<'_>. Uses transmute_copy for the Ref arm to shorten
    /// lifetime 'a → '_ — sound because T::Get is Copy and covariant.
    #[inline]
    pub fn get(&self) -> T::Get<'_> {
        match self {
            Self::Owned(v) => v.as_column_ref(),
            Self::Ref(g) => unsafe { std::mem::transmute_copy(g) },
        }
    }

    #[inline]
    pub fn pack(&self, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        match self {
            Self::Owned(v) => T::pack(v.as_column_ref(), buf),
            Self::Ref(g) => T::pack(*g, buf),
        };
        buf.len() - pos
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        match self {
            Self::Owned(v) => T::is_null(v.as_column_ref()),
            Self::Ref(g) => T::is_null(*g),
        }
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> PartialEq for RleCow<'a, T, V> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> From<V> for RleCow<'a, T, V> {
    fn from(v: V) -> Self {
        Self::Owned(v)
    }
}

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
    pub wpos: WPos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WPos {
    Run {
        pos: usize,
        local: bool,
    },
    Lit {
        pos: usize,
        bytes: usize,
        local: bool,
    },
}

impl Default for WPos {
    fn default() -> Self {
        WPos::Run {
            pos: 0,
            local: true,
        }
    }
}

impl WPos {
    fn lit(pos: usize, bytes: usize, local: bool) -> Self {
        Self::Lit { pos, bytes, local }
    }

    // truncated lit run
    fn trunc(count: usize, pos: usize, bytes: usize, local: bool) -> Self {
        if count == 0 {
            Self::Run { pos, local }
        } else {
            Self::Lit { pos, bytes, local }
        }
    }

    fn run(pos: usize, local: bool) -> Self {
        Self::Run { pos, local }
    }

    pub(crate) fn merge(
        &self,
        prefix: usize,
        middle: usize,
        postfix: usize,
        tail: RleTail,
    ) -> RleTail {
        match postfix {
            0 => self.as_tail(prefix, middle),
            len if tail.bytes <= len as u32 => tail,
            len => self
                .as_tail(prefix, middle + len)
                .with_lit_tail(tail.lit_tail),
        }
    }

    pub(crate) fn as_tail(&self, prefix: usize, local: usize) -> RleTail {
        let len = prefix + local;
        match self {
            WPos::Lit {
                pos,
                bytes,
                local: true,
            } => RleTail {
                bytes: (local - *pos) as u32,
                lit_tail: NonZeroU32::new(*bytes as u32),
            },
            WPos::Lit { pos, bytes, .. } => RleTail {
                bytes: (len - *pos) as u32,
                lit_tail: NonZeroU32::new(*bytes as u32),
            },
            WPos::Run { pos, local: true } => RleTail {
                bytes: (local - *pos) as u32,
                lit_tail: None,
            },
            WPos::Run { pos, .. } => RleTail {
                bytes: (len - *pos) as u32,
                lit_tail: None,
            },
        }
    }
}

impl FlushState {
    fn new(segments: usize, wpos: WPos) -> Self {
        Self {
            segments,
            rewrite: None,
            wpos,
        }
    }
    fn rewrite(segments: usize, r: RewriteHeader, wpos: WPos) -> Self {
        Self {
            segments,
            rewrite: Some(r),
            wpos,
        }
    }
}

impl std::ops::AddAssign for FlushState {
    fn add_assign(&mut self, rhs: Self) {
        self.segments += rhs.segments;
        self.wpos = rhs.wpos;
        if rhs.rewrite.is_some() {
            self.rewrite = rhs.rewrite;
        }
    }
}

impl std::ops::AddAssign<Option<FlushState>> for FlushState {
    fn add_assign(&mut self, rhs: Option<FlushState>) {
        if let Some(rhs) = rhs {
            *self += rhs;
        }
    }
}

/// Encoding state for RLE splice operations.
///
/// `'a` is the lifetime of the slab being read. The state borrows values
/// (e.g. `&'a str` for String columns) from the slab. All borrowed data
/// must be written to the output buffer before `RleState` is dropped and
/// the slab is mutated.
#[derive(Debug)]
pub(crate) enum RleState<'a, T: RleValue, V: AsColumnRef<T>> {
    Empty,
    Lone(RleCow<'a, T, V>),
    Run(usize, RleCow<'a, T, V>),
    /// `count`: items already written to buf (after header at `header_pos`).
    /// `local`: items written by THIS state machine (== count when header is ours).
    /// `current`: latest value, not yet written.
    Lit {
        count: usize,
        local: usize,
        header_pos: usize,
        bytes: usize,
        current: RleCow<'a, T, V>,
    },
    Null(usize),
}

type Cow<'a, T, V> = RleCow<'a, T, V>;

impl<'a, T: RleValue, V: AsColumnRef<T>> RleState<'a, T, V> {
    /// Segments that will be produced when this state is flushed.
    pub fn pending_segments(&self) -> usize {
        match self {
            RleState::Empty => 0,
            RleState::Lone(_) => 1,
            RleState::Run(_, _) => 1,
            RleState::Lit { local, .. } => local + 1,
            RleState::Null(_) => 1,
        }
    }

    pub fn append(&mut self, buf: &mut Vec<u8>, value: impl Into<Cow<'a, T, V>>) -> FlushState {
        self.append_n(buf, value, 1)
    }

    pub fn append_n(
        &mut self,
        buf: &mut Vec<u8>,
        value: impl Into<Cow<'a, T, V>>,
        n: usize,
    ) -> FlushState {
        let value = value.into();
        if n == 0 {
            return FlushState::default();
        }
        if value.is_null() {
            return self.append_null_n(buf, n);
        }

        let mut flushed = FlushState::default();
        let old = std::mem::replace(self, RleState::Empty);
        *self = match old {
            RleState::Empty if n == 1 => RleState::Lone(value),
            RleState::Empty => RleState::Run(n, value),
            RleState::Lone(prev) if value == prev => RleState::Run(n + 1, value),
            RleState::Lone(prev) if n == 1 => {
                let (header_pos, bytes) = emit_lit(buf, &prev);
                RleState::Lit {
                    count: 1,
                    local: 1,
                    header_pos,
                    bytes,
                    current: value,
                }
            }
            RleState::Lone(prev) => {
                let (pos, bytes) = emit_lit(buf, &prev);
                flushed.wpos = WPos::lit(pos, bytes, true);
                flushed.segments = 1;
                RleState::Run(n, value)
            }
            RleState::Run(count, prev) if value == prev => RleState::Run(count + n, value),
            RleState::Run(count, prev) => {
                flushed.wpos = emit_run(buf, count, &prev);
                flushed.segments = 1;
                Self::make_run(n, value)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                bytes,
            } if value == current => {
                if local == count {
                    rewrite_lit_header(buf, header_pos, count);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(count, header_pos));
                }
                flushed.wpos = WPos::trunc(count, header_pos, bytes, local == count);
                flushed.segments = local;
                RleState::Run(n + 1, value)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                ..
            } if n == 1 => {
                let bytes = current.pack(buf);
                RleState::Lit {
                    count: count + 1,
                    local: local + 1,
                    header_pos,
                    current: value,
                    bytes,
                }
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                ..
            } => {
                let bytes = current.pack(buf);
                flushed.wpos = WPos::lit(header_pos, bytes, count == local);
                if local == count {
                    rewrite_lit_header(buf, header_pos, count + 1);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(count + 1, header_pos));
                }
                flushed.segments = local + 1;
                RleState::Run(n, value)
            }
            RleState::Null(count) => {
                flushed.wpos = emit_null(buf, count);
                flushed.segments = 1;
                Self::make_run(n, value)
            }
        };
        flushed
    }

    pub fn lit(count: usize, current: Cow<'a, T, V>, header_pos: usize, bytes: usize) -> Self {
        if count == 0 {
            Self::Lone(current)
        } else {
            Self::Lit {
                count,
                local: 0,
                current,
                header_pos,
                bytes,
            }
        }
    }

    pub fn make_run(n: usize, value: Cow<'a, T, V>) -> Self {
        match n {
            0 => Self::Empty,
            1 => Self::Lone(value),
            n => Self::Run(n, value),
        }
    }

    /// Feed an optional postfix and flush. Returns (flush_state, postfix_segments).
    /// The postfix values are wrapped in RleCow::Ref since they come from slab data.
    pub fn flush_postfix(
        &mut self,
        buf: &mut Vec<u8>,
        postfix: Option<super::rle::Postfix<'a, T>>,
    ) -> (FlushState, usize) {
        use super::rle::Postfix;
        let mut f = FlushState::default();
        let postfix_segs = match postfix {
            None => {
                f += self.flush(buf);
                0
            }
            Some(Postfix::Run {
                count,
                value,
                segments,
            }) => {
                f += self.append_n(buf, RleCow::Ref(value), count);
                f += self.flush(buf);
                segments
            }
            Some(Postfix::Lit {
                value,
                lit,
                segments,
            }) => {
                f += self.append(buf, RleCow::Ref(value));
                f += self.flush_with_lit(buf, lit);
                segments
            }
            Some(Postfix::LonePlusLit {
                lone,
                value,
                lit,
                segments,
            }) => {
                f += self.append(buf, RleCow::Ref(lone));
                f += self.append(buf, RleCow::Ref(value));
                f += self.flush_with_lit(buf, lit);
                segments
            }
        };
        (f, postfix_segs)
    }

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

    fn flush_with_lit(&mut self, buf: &mut Vec<u8>, lit: usize) -> FlushState {
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
                ..
            } => {
                let bytes = current.pack(buf);
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1 + lit;
                if local == count {
                    rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(local + 1, wpos)
                } else {
                    FlushState::rewrite(local + 1, RewriteHeader::new(total, header_pos), wpos)
                }
            }
            RleState::Lone(value) => {
                let total = 1 + lit;
                let pos = buf.len();
                buf.extend(encode_signed(-(total as i64)));
                let bytes = value.pack(buf);
                FlushState::new(1, WPos::lit(pos, bytes, true))
            }
            other => {
                let mut flushed = Self::do_flush(other, buf);
                let pos = buf.len();
                buf.extend(encode_signed(-(lit as i64)));
                flushed.wpos = WPos::lit(pos, 0, true); // fill in bytes later
                flushed
            }
        }
    }

    pub fn flush(&mut self, buf: &mut Vec<u8>) -> FlushState {
        let old = std::mem::replace(self, RleState::Empty);
        Self::do_flush(old, buf)
    }

    pub fn do_flush(state: Self, buf: &mut Vec<u8>) -> FlushState {
        match state {
            RleState::Empty => FlushState::default(),
            RleState::Lone(value) => {
                let wpos = emit_run(buf, 1, &value);
                FlushState::new(1, wpos)
            }
            RleState::Run(count, value) => {
                let wpos = emit_run(buf, count, &value);
                FlushState::new(1, wpos)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                ..
            } => {
                let bytes = current.pack(buf);
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1;
                if local == count {
                    rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(total, wpos)
                } else {
                    let rewrite = RewriteHeader::new(total, header_pos);
                    FlushState::rewrite(local + 1, rewrite, wpos)
                }
            }
            RleState::Null(count) => {
                let wpos = emit_null(buf, count);
                FlushState::new(1, wpos)
            }
        }
    }
}

fn emit_lit<'a, T: RleValue, V: AsColumnRef<T>>(
    buf: &mut Vec<u8>,
    value: &RleCow<'a, T, V>,
) -> (usize, usize) {
    let pos = buf.len();
    buf.extend(encode_signed(-1));
    let bytes = value.pack(buf);
    (pos, bytes)
}

fn emit_run<'a, T: RleValue, V: AsColumnRef<T>>(
    buf: &mut Vec<u8>,
    count: usize,
    value: &RleCow<'a, T, V>,
) -> WPos {
    let pos = buf.len();
    if count == 1 {
        buf.extend(encode_signed(-1));
        let bytes = value.pack(buf);
        WPos::lit(pos, bytes, true)
    } else {
        buf.extend(encode_signed(count as i64));
        value.pack(buf);
        WPos::run(pos, true)
    }
}

fn emit_null(buf: &mut Vec<u8>, count: usize) -> WPos {
    let pos = buf.len();
    buf.extend(encode_signed(0));
    buf.extend(encode_unsigned(count as u64));
    WPos::run(pos, true)
}

fn leb_signed_bytes(buf: &[u8], pos: usize) -> Range<usize> {
    let mut tmp = &buf[pos..];
    let start = tmp.len();
    let _ = leb128::read::signed(&mut tmp);
    let n = start - tmp.len();
    pos..pos + n
}

pub(crate) fn rewrite_lit_header(buf: &mut Vec<u8>, header_pos: usize, total: usize) -> i64 {
    let len = buf.len();
    let header_bytes = leb_signed_bytes(buf, header_pos);
    if total == 0 {
        // remove header
        buf.splice(header_bytes, []);
    } else {
        // rewrite header
        buf.splice(header_bytes, encode_signed(-(total as i64)));
    }
    buf.len() as i64 - len as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::rle::rle_validate_encoding;

    fn encode_vals(vals: &[u64]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut state = RleState::<u64, u64>::Empty;
        for &v in vals {
            state.append(&mut buf, RleCow::Ref(v));
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
                    result.resize(result.len() + nc as usize, 0);
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
        let mut state = RleState::<Option<u64>, Option<u64>>::Empty;
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }
    #[test]
    fn nullable_null_value_null() {
        let vals: Vec<Option<u64>> = vec![None, Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::Empty;
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }
    #[test]
    fn nullable_value_then_null() {
        let vals: Vec<Option<u64>> = vec![Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::Empty;
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>>(&buf).unwrap();
    }
}
