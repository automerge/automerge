//! RLE encoding state machine for cursor-aware splice.

use crate::rle::splice::Postfix;
use crate::rle::RleTail;
use crate::{AsColumnRef, Codec, Leb128, RleValue};

use std::marker::PhantomData;
use std::num::NonZeroU32;

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
    /// Get as `T::Get<'_>` — the Ref arm reborrows at the shorter
    /// lifetime via [`ColumnValueRef::shorten`], no `unsafe` needed.
    ///
    /// [`ColumnValueRef::shorten`]: crate::ColumnValueRef::shorten
    #[inline]
    pub fn get(&self) -> T::Get<'_> {
        match self {
            Self::Owned(v) => v.as_column_ref(),
            Self::Ref(g) => T::shorten(g),
        }
    }

    #[inline]
    pub fn pack<C: Codec>(&self, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        match self {
            Self::Owned(v) => T::pack::<C>(v.as_column_ref(), buf),
            Self::Ref(g) => T::pack::<C>(*g, buf),
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
///
/// `C` is the varint codec used for run headers and packed values.  It is
/// a type parameter (rather than per-method) so one state machine can
/// never mix codecs across appends.  Construct via [`RleState::empty`].
#[derive(Debug)]
pub(crate) enum RleState<'a, T: RleValue, V: AsColumnRef<T>, C: Codec = Leb128> {
    Empty(PhantomData<fn() -> C>),
    Lone(RleCow<'a, T, V>),
    Run(usize, RleCow<'a, T, V>),
    /// `count`: items already written to buf (after header at `header_pos`).
    /// `local`: items written by THIS state machine (== count when header is ours).
    /// `current`: latest value, not yet written.
    Lit {
        count: usize,
        local: usize,
        header_pos: usize,
        /// Byte width of the last value written into the local buffer —
        /// `None` until one has been, which is exactly `local == 0`. The
        /// value a run was imported with is not it: that one is `current`,
        /// still unwritten.
        bytes: Option<std::num::NonZeroUsize>,
        current: RleCow<'a, T, V>,
    },
    Null(usize),
}

type Cow<'a, T, V> = RleCow<'a, T, V>;

/// A postfix detached from the slab it was read from.
///
/// [`Postfix`] borrows the slab, which is
/// what a one-shot splice wants; a cursor closes its rebuild while
/// holding `&mut Column`, so it needs the owned form.
#[derive(Debug)]
pub(crate) enum OwnedPostfix<T> {
    Run {
        count: usize,
        value: T,
        segments: usize,
    },
    Lit {
        value: T,
        lit: usize,
        segments: usize,
    },
    LonePlusLit {
        lone: T,
        value: T,
        lit: usize,
        segments: usize,
    },
}

impl<T> OwnedPostfix<T> {
    /// Segments from the postfix's own end to the end of the slab.
    pub(crate) fn segments(&self) -> usize {
        match self {
            Self::Run { segments, .. } | Self::Lit { segments, .. } => *segments,
            Self::LonePlusLit { segments, .. } => *segments,
        }
    }

    /// Segments the postfix itself will contribute when flushed.
    pub(crate) fn pending(&self) -> usize {
        match self {
            Self::Run { .. } | Self::Lit { .. } => 1,
            Self::LonePlusLit { .. } => 2,
        }
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> RleState<'a, T, V, C> {
    /// Detach from the slab the state was opened against, cloning the one
    /// or two values it is holding.
    ///
    /// A one-shot splice keeps its state on the stack for the length of a
    /// borrow of the slab, so it can hold `Get` values by reference. A
    /// [`RleEdit`](crate::rle::edit::RleEdit) keeps its state across calls,
    /// alongside a `&mut Column`, so it cannot — this is what lets the
    /// cursor own its state.
    pub(crate) fn into_owned(self) -> RleState<'static, T, T, C> {
        fn own<T: RleValue, V: AsColumnRef<T>>(c: RleCow<'_, T, V>) -> RleCow<'static, T, T> {
            RleCow::Owned(match c {
                RleCow::Owned(v) => T::to_owned(v.as_column_ref()),
                RleCow::Ref(g) => T::to_owned(g),
            })
        }
        match self {
            RleState::Empty(_) => RleState::Empty(PhantomData),
            RleState::Lone(v) => RleState::Lone(own(v)),
            RleState::Run(n, v) => RleState::Run(n, own(v)),
            RleState::Lit {
                count,
                local,
                header_pos,
                bytes,
                current,
            } => RleState::Lit {
                count,
                local,
                header_pos,
                bytes,
                current: own(current),
            },
            RleState::Null(n) => RleState::Null(n),
        }
    }

    /// The initial (empty) state.
    pub const fn empty() -> Self {
        RleState::Empty(PhantomData)
    }

    /// Segments that will be produced when this state is flushed.
    pub fn pending_segments(&self) -> usize {
        match self {
            RleState::Empty(_) => 0,
            RleState::Lone(_) => 1,
            RleState::Run(_, _) => 1,
            RleState::Lit { local, .. } => local + 1,
            RleState::Null(_) => 1,
        }
    }

    pub(crate) fn is_single_run_of(&self, value: impl Into<Cow<'a, T, V>>) -> bool {
        let value = value.into();
        match self {
            RleState::Empty(_) => true,
            RleState::Lone(v) => v == &value,
            RleState::Run(_, v) => v == &value,
            RleState::Null(_) => value.is_null(),
            _ => false,
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
        let next_state = match self {
            RleState::Run(count, prev) if value == *prev => {
                *count += n;
                None
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                bytes,
            } if &value == current => {
                if *local == *count {
                    C::rewrite_lit_header(buf, *header_pos, *count);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(*count, *header_pos));
                }
                flushed.wpos = WPos::trunc(
                    *count,
                    *header_pos,
                    bytes.map_or(0, |b| b.get()),
                    *local == *count,
                );
                flushed.segments = *local;
                Some(RleState::Run(n + 1, value))
            }
            RleState::Lit {
                count,
                local,
                current,
                bytes,
                ..
            } if n == 1 => {
                *bytes = std::num::NonZeroUsize::new(current.pack::<C>(buf));
                *count += 1;
                *local += 1;
                *current = value;
                None
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                ..
            } => {
                let bytes = current.pack::<C>(buf);
                flushed.wpos = WPos::lit(*header_pos, bytes, *count == *local);
                if *local == *count {
                    C::rewrite_lit_header(buf, *header_pos, *count + 1);
                } else {
                    flushed.rewrite = Some(RewriteHeader::new(*count + 1, *header_pos));
                }
                flushed.segments = *local + 1;
                Some(RleState::Run(n, value))
            }
            RleState::Empty(_) if n == 1 => Some(RleState::Lone(value)),
            RleState::Empty(_) => Some(RleState::Run(n, value)),
            RleState::Lone(prev) if &value == prev => Some(RleState::Run(n + 1, value)),
            RleState::Lone(prev) if n == 1 => {
                let (header_pos, bytes) = Self::emit_lit(buf, prev);
                Some(RleState::Lit {
                    count: 1,
                    local: 1,
                    header_pos,
                    // written just above, so the width is known
                    bytes: std::num::NonZeroUsize::new(bytes),
                    current: value,
                })
            }
            RleState::Lone(prev) => {
                let (pos, bytes) = Self::emit_lit(buf, prev);
                flushed.wpos = WPos::lit(pos, bytes, true);
                flushed.segments = 1;
                Some(RleState::Run(n, value))
            }
            RleState::Run(count, prev) => {
                flushed.wpos = Self::emit_run(buf, *count, prev);
                flushed.segments = 1;
                Some(Self::make_run(n, value))
            }
            RleState::Null(count) => {
                flushed.wpos = Self::emit_null(buf, *count);
                flushed.segments = 1;
                Some(Self::make_run(n, value))
            }
        };
        if let Some(s) = next_state {
            *self = s;
        }
        flushed
    }

    /// A literal run imported from a slab. Nothing has been written
    /// locally yet, so there is no last-written width to record.
    pub fn lit(count: usize, current: Cow<'a, T, V>, header_pos: usize) -> Self {
        if count == 0 {
            Self::Lone(current)
        } else {
            Self::Lit {
                count,
                local: 0,
                current,
                header_pos,
                bytes: None,
            }
        }
    }

    pub fn make_run(n: usize, value: Cow<'a, T, V>) -> Self {
        match n {
            0 => Self::empty(),
            1 => Self::Lone(value),
            n => Self::Run(n, value),
        }
    }

    /// Feed an optional postfix and flush. Returns (flush_state, postfix_segments).
    /// The postfix values are wrapped in RleCow::Ref since they come from slab data.
    pub fn flush_postfix(
        &mut self,
        buf: &mut Vec<u8>,
        postfix: Option<Postfix<'a, T>>,
    ) -> (FlushState, usize) {
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

    /// [`Self::flush_postfix`] for a postfix that has been detached from
    /// the slab — the form a cursor closes with.
    pub fn flush_postfix_owned(
        &mut self,
        buf: &mut Vec<u8>,
        postfix: Option<OwnedPostfix<V>>,
    ) -> (FlushState, usize) {
        let mut f = FlushState::default();
        let postfix_segs = match postfix {
            None => {
                f += self.flush(buf);
                0
            }
            Some(OwnedPostfix::Run {
                count,
                value,
                segments,
            }) => {
                f += self.append_n(buf, RleCow::Owned(value), count);
                f += self.flush(buf);
                segments
            }
            Some(OwnedPostfix::Lit {
                value,
                lit,
                segments,
            }) => {
                f += self.append(buf, RleCow::Owned(value));
                f += self.flush_with_lit(buf, lit);
                segments
            }
            Some(OwnedPostfix::LonePlusLit {
                lone,
                value,
                lit,
                segments,
            }) => {
                f += self.append(buf, RleCow::Owned(lone));
                f += self.append(buf, RleCow::Owned(value));
                f += self.flush_with_lit(buf, lit);
                segments
            }
        };
        (f, postfix_segs)
    }

    pub fn append_null_n(&mut self, buf: &mut Vec<u8>, count: usize) -> FlushState {
        let mut flushed = FlushState::default();
        let old = std::mem::replace(self, Self::empty());
        *self = match old {
            RleState::Empty(_) => RleState::Null(count),
            RleState::Null(n) => RleState::Null(n + count),
            other => {
                flushed = Self::do_flush(other, buf);
                RleState::Null(count)
            }
        };
        flushed
    }

    fn flush_with_lit(&mut self, buf: &mut Vec<u8>, lit: usize) -> FlushState {
        let old = std::mem::replace(self, Self::empty());
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
                let bytes = current.pack::<C>(buf);
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1 + lit;
                if local == count {
                    C::rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(local + 1, wpos)
                } else {
                    FlushState::rewrite(local + 1, RewriteHeader::new(total, header_pos), wpos)
                }
            }
            RleState::Lone(value) => {
                let total = 1 + lit;
                let pos = buf.len();
                buf.extend(C::encode_signed(-(total as i64)));
                let bytes = value.pack::<C>(buf);
                FlushState::new(1, WPos::lit(pos, bytes, true))
            }
            other => {
                let mut flushed = Self::do_flush(other, buf);
                let pos = buf.len();
                buf.extend(C::encode_signed(-(lit as i64)));
                // fill in bytes later in merge() -> with_lit_tail()
                flushed.wpos = WPos::lit(pos, 0, true);
                flushed
            }
        }
    }

    pub fn flush(&mut self, buf: &mut Vec<u8>) -> FlushState {
        let old = std::mem::replace(self, Self::empty());
        Self::do_flush(old, buf)
    }

    pub fn do_flush(state: Self, buf: &mut Vec<u8>) -> FlushState {
        match state {
            RleState::Empty(_) => FlushState::default(),
            RleState::Lone(value) => {
                let wpos = Self::emit_run(buf, 1, &value);
                FlushState::new(1, wpos)
            }
            RleState::Run(count, value) => {
                let wpos = Self::emit_run(buf, count, &value);
                FlushState::new(1, wpos)
            }
            RleState::Lit {
                count,
                local,
                header_pos,
                current,
                ..
            } => {
                let bytes = current.pack::<C>(buf);
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1;
                if local == count {
                    C::rewrite_lit_header(buf, header_pos, total);
                    FlushState::new(total, wpos)
                } else {
                    let rewrite = RewriteHeader::new(total, header_pos);
                    FlushState::rewrite(local + 1, rewrite, wpos)
                }
            }
            RleState::Null(count) => {
                let wpos = Self::emit_null(buf, count);
                FlushState::new(1, wpos)
            }
        }
    }

    fn emit_lit(buf: &mut Vec<u8>, value: &RleCow<'a, T, V>) -> (usize, usize) {
        let pos = buf.len();
        buf.extend(C::encode_signed(-1));
        let bytes = value.pack::<C>(buf);
        (pos, bytes)
    }

    fn emit_run(buf: &mut Vec<u8>, count: usize, value: &RleCow<'a, T, V>) -> WPos {
        let pos = buf.len();
        if count == 1 {
            buf.extend(C::encode_signed(-1));
            let bytes = value.pack::<C>(buf);
            WPos::lit(pos, bytes, true)
        } else {
            buf.extend(C::encode_signed(count as i64));
            value.pack::<C>(buf);
            WPos::run(pos, true)
        }
    }

    fn emit_null(buf: &mut Vec<u8>, count: usize) -> WPos {
        let pos = buf.len();
        buf.extend(C::encode_signed(0));
        buf.extend(C::encode_unsigned(count as u64));
        WPos::run(pos, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rle::rle_validate_encoding;

    fn encode_vals(vals: &[u64]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut state = RleState::<u64, u64>::empty();
        for &v in vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        buf
    }

    fn decode(buf: &[u8]) -> Vec<u64> {
        let mut result = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (cb, raw) = Leb128::read_signed(&buf[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    let (vl, val) = u64::try_unpack::<Leb128>(&buf[pos + cb..]).unwrap();
                    for _ in 0..n as usize {
                        result.push(val);
                    }
                    pos += cb + vl;
                }
                n if n < 0 => {
                    let mut scan = pos + cb;
                    for _ in 0..(-n) as usize {
                        let (vl, val) = u64::try_unpack::<Leb128>(&buf[scan..]).unwrap();
                        result.push(val);
                        scan += vl;
                    }
                    pos = scan;
                }
                _ => {
                    let (ncb, nc) = Leb128::read_unsigned(&buf[pos + cb..]).unwrap();
                    result.resize(result.len() + nc as usize, 0);
                    pos += cb + ncb;
                }
            }
        }
        result
    }

    fn check(vals: &[u64]) {
        let buf = encode_vals(vals);
        if let Err(e) = rle_validate_encoding::<u64, Leb128>(&buf) {
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
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
    #[test]
    fn nullable_null_value_null() {
        let vals: Vec<Option<u64>> = vec![None, Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
    #[test]
    fn nullable_value_then_null() {
        let vals: Vec<Option<u64>> = vec![Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
}
