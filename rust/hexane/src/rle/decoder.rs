//! RLE decoder — forward iterator over items in a single RLE-encoded slab.

use crate::encoding::RunDecoder;

use crate::PackError;
use crate::{Codec, ColumnValueRef, Leb128, RleValue, Run};
use std::marker::PhantomData;

/// Forward iterator over all items in a single RLE-encoded slab.
///
/// Created by the `ColumnEncoding::decoder` method.  Repeat runs yield the cached value
/// in O(1) per item.  Literal runs decode each value.  Null runs yield
/// the type's null value.
pub struct RleDecoder<'a, T: RleValue, C: Codec = Leb128> {
    data: &'a [u8],
    pub(crate) byte_pos: usize,
    pub(crate) remaining: usize,
    state: RunState<'a, T>,
    /// Where the current repeat run's value starts, which a suspend
    /// keeps so a resume can read it again.
    ///
    /// A separate field rather than a payload on
    /// [`RunState::Repeat`]: widening that variant costs the decode loop
    /// 9-15% on value-heavy columns (strings, long runs), because every
    /// `next` then loads it alongside the value it actually wants.
    run_at: usize,
    _codec: PhantomData<fn() -> C>,
}

impl<T: RleValue, C: Codec> Clone for RleDecoder<'_, T, C> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            state: self.state.clone(),
            run_at: self.run_at,
            _codec: PhantomData,
        }
    }
}

impl<T: RleValue, C: Codec> std::fmt::Debug for RleDecoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RleDecoder")
            .field("byte_pos", &self.byte_pos)
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// The run a decoder is inside, as it reads: the live counterpart of
/// [`SuspendedRun`], which is the same four cases with the value's
/// position in place of the value.
enum RunState<'a, T: RleValue> {
    /// Repeat run: yield the cached value. Where the value's bytes
    /// start is [`RleDecoder::run_at`], kept out of here on purpose.
    Repeat(<T as ColumnValueRef>::Get<'a>),
    /// Literal run: decode each value from `byte_pos`.
    Literal,
    /// Null run: yield the type's null value.
    Null,
    /// Between runs or exhausted.
    Idle,
}

impl<T: RleValue> Clone for RunState<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Repeat(v) => Self::Repeat(*v),
            Self::Literal => Self::Literal,
            Self::Null => Self::Null,
            Self::Idle => Self::Idle,
        }
    }
}

/// A decoder's position, detached from the slab it was reading.
///
/// Holds no borrow, so it can be kept beside a `&mut` to the column the
/// slab belongs to — which is what lets an edit cursor carry its reader
/// across writes. [`resume`](RleDecoder::resume) restores a decoder from
/// one in O(1).
///
/// Public only because [`ColumnEncoding::State`](crate::ColumnEncoding::State)
/// names it; it is not exported, so nothing outside the crate can name
/// one.
///
/// Position only, never a value: a repeat run records where its value is
/// rather than what it is, so the state is `Copy` and costs a `String`
/// column nothing to take. Holding the value instead would mean either
/// owning it — an allocation per suspend, and a cursor suspends on every
/// move — or holding `Get<'static>`, which for a borrowed type can only
/// come from transmuting away the slab's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RleDecoderState {
    byte_pos: usize,
    remaining: usize,
    run: SuspendedRun,
}

/// The run a suspended reader is inside: [`RunState`] with the value's
/// position in place of the value, which is what drops the lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SuspendedRun {
    /// A repeat run, and where its value's bytes start — what a resume
    /// reads to hand the value back out borrowed from the slab.
    Repeat {
        at: usize,
    },
    Literal,
    Null,
    #[default]
    Idle,
}

impl RleDecoderState {
    /// The state of a decoder standing at the start of a slab.
    pub(crate) fn start() -> Self {
        Self::default()
    }
}

impl<'a, T: RleValue, C: Codec> RleDecoder<'a, T, C> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        RleDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            state: RunState::Idle,
            run_at: 0,
            _codec: PhantomData,
        }
    }

    /// Capture the position so it can be restored against the same bytes
    /// later, without holding a borrow of them. Field copies only.
    pub(crate) fn suspend(&self) -> RleDecoderState {
        RleDecoderState {
            byte_pos: self.byte_pos,
            remaining: self.remaining,
            run: match &self.state {
                RunState::Repeat(_) => SuspendedRun::Repeat { at: self.run_at },
                RunState::Literal => SuspendedRun::Literal,
                RunState::Null => SuspendedRun::Null,
                RunState::Idle => SuspendedRun::Idle,
            },
        }
    }

    /// Restore a suspended position against `data`.
    ///
    /// O(1): only a repeat run reads anything, and only its own value.
    /// `data` must be the bytes the state was suspended from — a state
    /// resumed against a different slab decodes garbage, the same way a
    /// byte offset into the wrong buffer would.
    pub(crate) fn resume(data: &'a [u8], state: &RleDecoderState) -> Self {
        let (run, run_at) = match &state.run {
            SuspendedRun::Repeat { at } => (RunState::Repeat(T::unpack::<C>(&data[*at..]).1), *at),
            SuspendedRun::Literal => (RunState::Literal, 0),
            SuspendedRun::Null => (RunState::Null, 0),
            SuspendedRun::Idle => (RunState::Idle, 0),
        };
        RleDecoder {
            data,
            byte_pos: state.byte_pos,
            remaining: state.remaining,
            state: run,
            run_at,
            _codec: PhantomData,
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.byte_pos
    }

    pub(crate) fn is_literal(&self) -> bool {
        matches!(self.state, RunState::Literal)
    }

    fn advance_run(&mut self) {
        if self.byte_pos >= self.data.len() {
            self.state = RunState::Idle;
            self.remaining = 0;
            return;
        }
        let (count_bytes, count_raw) = match C::read_signed(&self.data[self.byte_pos..]) {
            Some(v) => v,
            None => {
                self.state = RunState::Idle;
                self.remaining = 0;
                return;
            }
        };

        match count_raw {
            n if n > 0 => {
                let count = n as usize;
                let value_start = self.byte_pos + count_bytes;
                let (vlen, value) = T::unpack::<C>(&self.data[value_start..]);
                self.byte_pos = value_start + vlen;
                self.remaining = count;
                self.run_at = value_start;
                self.state = RunState::Repeat(value);
            }
            n if n < 0 => {
                let total = (-n) as usize;
                self.byte_pos += count_bytes;
                self.remaining = total;
                self.state = RunState::Literal;
            }
            _ => {
                // Null run (count_raw == 0)
                let (ncb, null_count) =
                    C::read_unsigned(&self.data[self.byte_pos + count_bytes..]).unwrap();
                self.byte_pos += count_bytes + ncb;
                self.remaining = null_count as usize;
                self.state = RunState::Null;
            }
        }
    }

    // multiple monomorphized callers (pull path + drain path) — without
    // the hint LLVM stops inlining this into the per-segment hot loops
    #[inline(always)]
    pub(crate) fn try_next_segment(&mut self) -> Result<Option<RleSegment<'a, T>>, PackError> {
        if self.remaining > 0 && matches!(self.state, RunState::Literal) {
            self.remaining -= 1;
            let (bytes, value) = T::try_unpack::<C>(&self.data[self.byte_pos..])?;
            self.byte_pos += bytes;
            Ok(Some(RleSegment::Lit { value, bytes }))
        } else if self.data[self.byte_pos..].is_empty() {
            Ok(None)
        } else {
            match C::try_read_signed(&self.data[self.byte_pos..])? {
                (count_bytes, n) if n > 0 => {
                    let count = n as usize;
                    let value_start = self.byte_pos + count_bytes;
                    let (vlen, value) = T::try_unpack::<C>(&self.data[value_start..])?;
                    let bytes = count_bytes + vlen;
                    self.byte_pos += bytes;
                    self.state = RunState::Idle;
                    self.remaining = 0;
                    Ok(Some(RleSegment::Run {
                        count,
                        value,
                        bytes,
                    }))
                }
                (bytes, n) if n < 0 => {
                    let count = (-n) as usize;
                    self.byte_pos += bytes;
                    self.remaining = count;
                    self.state = RunState::Literal;
                    Ok(Some(RleSegment::LitHead { count, bytes }))
                }
                (count_bytes, _) => {
                    let (ncb, count) =
                        C::try_read_unsigned(&self.data[self.byte_pos + count_bytes..])?;
                    let count = count as usize;
                    let bytes = count_bytes + ncb;
                    self.byte_pos += bytes;
                    self.state = RunState::Idle;
                    self.remaining = 0;
                    Ok(Some(RleSegment::Null { count, bytes }))
                }
            }
        }
    }
}

impl<'a, T: RleValue, C: Codec> RleDecoder<'a, T, C> {
    /// Advance past `n` items in O(runs skipped), decoding no values.
    /// Stops at the end of the column.
    // inherent, so a call on a concrete decoder is never ambiguous with
    // the unstable `Iterator::advance_by`
    pub fn advance_by(&mut self, n: usize) {
        if n > 0 {
            self.nth(n - 1);
        }
    }

    /// Skip `n` literal values by advancing `byte_pos` without decoding.
    fn skip_literals(&mut self, n: usize) {
        for _ in 0..n {
            let vlen = T::value_len::<C>(&self.data[self.byte_pos..]).unwrap();
            self.byte_pos += vlen;
        }
    }
}

impl<'a, T: RleValue, C: Codec> Iterator for RleDecoder<'a, T, C> {
    type Item = <T as ColumnValueRef>::Get<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining > 0 {
                self.remaining -= 1;
                return match &self.state {
                    RunState::Repeat(v) => Some(*v),
                    RunState::Literal => {
                        let (vlen, value) = T::unpack::<C>(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        Some(value)
                    }
                    RunState::Null => Some(T::get_null()),
                    RunState::Idle => None,
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
        // Consume the current run (if any) before walking byte-by-byte.
        if self.remaining > 0 {
            if n < self.remaining {
                if let RunState::Literal = self.state {
                    self.skip_literals(n);
                }
                self.remaining -= n;
                return self.next();
            }
            if let RunState::Literal = self.state {
                self.skip_literals(self.remaining);
            }
            n -= self.remaining;
            self.remaining = 0;
        }

        // Inline `advance_run` for skipped runs, avoiding self.state writes
        // and using `value_len` instead of full `unpack` until we land on
        // the target run.  This is the hot path for `nth(N)` on slabs with
        // many short runs.
        loop {
            if self.byte_pos >= self.data.len() {
                self.state = RunState::Idle;
                return None;
            }
            let (count_bytes, count_raw) =
                C::read_signed(&self.data[self.byte_pos..]).expect("validated slab");
            match count_raw {
                c if c > 0 => {
                    let count = c as usize;
                    let value_start = self.byte_pos + count_bytes;
                    if n < count {
                        // Land on this run — full unpack for the value.
                        let (vlen, value) = T::unpack::<C>(&self.data[value_start..]);
                        self.byte_pos = value_start + vlen;
                        self.remaining = count - n - 1;
                        self.run_at = value_start;
                        self.state = RunState::Repeat(value);
                        return Some(value);
                        //return self.next();
                    }
                    // Skip past — only need byte length of the value.
                    let vlen =
                        T::value_len::<C>(&self.data[value_start..]).expect("validated slab");
                    self.byte_pos = value_start + vlen;
                    n -= count;
                }
                c if c < 0 => {
                    let total = (-c) as usize;
                    self.byte_pos += count_bytes;
                    if n < total {
                        // Land within a literal run.
                        self.remaining = total - n - 1;
                        self.state = RunState::Literal;
                        self.skip_literals(n);
                        let (vlen, value) = T::unpack::<C>(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        return Some(value);
                    }
                    // Skip whole literal run — must walk `value_len` per
                    // literal since they're packed back-to-back.
                    self.skip_literals(total);
                    n -= total;
                }
                _ => {
                    // Null run.
                    let (ncb, null_count) =
                        C::read_unsigned(&self.data[self.byte_pos + count_bytes..])
                            .expect("validated slab");
                    let total = null_count as usize;
                    self.byte_pos += count_bytes + ncb;
                    if n < total {
                        self.remaining = total - n - 1;
                        self.state = RunState::Null;
                        return Some(T::get_null());
                    }
                    n -= total;
                }
            }
        }
    }
}

pub(crate) enum RleSegment<'a, T: RleValue> {
    Run {
        count: usize,
        value: T::Get<'a>,
        bytes: usize,
    },
    Null {
        count: usize,
        bytes: usize,
    },
    LitHead {
        count: usize,
        bytes: usize,
    },
    Lit {
        value: T::Get<'a>,
        bytes: usize,
    },
}

// Hand-written (not derived) so the impls don't demand `T: Copy` —
// `T::Get<'a>` is `Copy` by trait contract even when `T` (e.g. `String`)
// is not.
impl<T: RleValue> Copy for RleSegment<'_, T> {}
impl<T: RleValue> Clone for RleSegment<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: RleValue> RleSegment<'a, T> {
    /// Check if this segment is valid after `prev` (the last value-bearing segment)
    /// and `prev_lit` (the previous literal value within the current literal run).
    /// Returns `Ok(())` or an error message.
    pub(crate) fn validate_after(
        &self,
        prev: &Option<Self>,
        prev_lit: Option<T::Get<'a>>,
    ) -> Result<(), &'static str> {
        match self {
            Self::LitHead { count, .. } => {
                if *count == 0 {
                    return Err("empty literal run");
                }
                if matches!(prev, Some(Self::Lit { .. })) {
                    return Err("adjacent literal runs");
                }
            }
            Self::Lit { value, .. } => {
                if prev_lit == Some(*value) {
                    return Err("literal has consecutive equal values");
                }
                // Boundary check: first lit value in this run vs prev segment's value.
                if prev_lit.is_none() {
                    match prev {
                        Some(Self::Run { value: v, .. }) | Some(Self::Lit { value: v, .. })
                            if *v == *value =>
                        {
                            return Err("boundary values match (should be merged)");
                        }
                        _ => {}
                    }
                }
            }
            Self::Run { count, value, .. } => {
                if *count < 2 {
                    return Err("repeat run with count < 2");
                }
                match prev {
                    Some(Self::Run { value: v, .. }) if *v == *value => {
                        return Err("adjacent repeat runs with same value");
                    }
                    Some(Self::Lit { value: v, .. }) if *v == *value => {
                        return Err("boundary values match (should be merged)");
                    }
                    _ => {}
                }
            }
            Self::Null { count, .. } => {
                if *count == 0 {
                    return Err("null run with count 0");
                }
                if matches!(prev, Some(Self::Null { .. })) {
                    return Err("adjacent null runs");
                }
                if !T::NULLABLE {
                    return Err("null in non-nullable column");
                }
            }
        }
        Ok(())
    }
}

impl<'a, T: RleValue, C: Codec> RunDecoder for RleDecoder<'a, T, C> {
    fn next_run(&mut self) -> Option<Run<Self::Item>> {
        self.next_run_max(usize::MAX)
    }

    fn next_run_max(&mut self, max: usize) -> Option<Run<Self::Item>> {
        if max == 0 {
            return None;
        }
        loop {
            if self.remaining > 0 {
                return match &self.state {
                    RunState::Repeat(v) => {
                        let value = *v;
                        let count = self.remaining.min(max);
                        self.remaining -= count;
                        Some(Run { count, value })
                    }
                    RunState::Literal => {
                        // Literal: each item is distinct, yield one at a time
                        self.remaining -= 1;
                        let (vlen, value) = T::unpack::<C>(&self.data[self.byte_pos..]);
                        self.byte_pos += vlen;
                        Some(Run { count: 1, value })
                    }
                    RunState::Null => {
                        let value = T::get_null();
                        let count = self.remaining.min(max);
                        self.remaining -= count;
                        Some(Run { count, value })
                    }
                    RunState::Idle => None,
                };
            }
            self.advance_run();
            if self.remaining == 0 {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod suspend_tests {
    use super::*;
    use crate::codec::Leb128;

    /// Suspending at every position and resuming must yield exactly what
    /// an uninterrupted decoder would.
    fn check<T>(values: Vec<T>)
    where
        T: RleValue + PartialEq + std::fmt::Debug + Clone,
        for<'a> T::Get<'a>: PartialEq + std::fmt::Debug,
    {
        let bytes = crate::Column::<T>::from_values(values.clone()).save();
        let want: Vec<T> = crate::decoder::<T>(&bytes).map(T::to_owned).collect();

        for cut in 0..=want.len() {
            let mut dec = RleDecoder::<T, Leb128>::new(&bytes);
            for _ in 0..cut {
                dec.next().expect("value before the cut");
            }
            let state = dec.suspend();
            let mut resumed = RleDecoder::<T, Leb128>::resume(&bytes, &state);
            let got: Vec<T> = std::iter::from_fn(|| resumed.next())
                .map(T::to_owned)
                .collect();
            assert_eq!(got, want[cut..], "resume at {cut}");
        }
    }

    #[test]
    fn resume_scalar_runs() {
        check::<u64>((0..200).map(|i| i / 8).collect());
    }

    #[test]
    fn resume_literals() {
        check::<u64>((0..200).map(|i| i * 7 % 97).collect());
    }

    #[test]
    fn resume_nullable() {
        check::<Option<u64>>(
            (0..200)
                .map(|i| if (i / 5) % 3 == 0 { None } else { Some(i / 5) })
                .collect(),
        );
    }

    #[test]
    fn resume_strings() {
        check::<String>((0..200).map(|i| format!("v{}", i / 6)).collect());
    }

    /// A suspend mid-run must survive the decoder it came from being
    /// dropped — the point of owning the run's value.
    #[test]
    fn state_outlives_decoder() {
        let bytes =
            crate::Column::<String>::from_values((0..40).map(|i| format!("s{}", i / 8)).collect())
                .save();
        let state = {
            let mut dec = RleDecoder::<String, Leb128>::new(&bytes);
            dec.nth(9);
            dec.suspend()
        };
        let mut resumed = RleDecoder::<String, Leb128>::resume(&bytes, &state);
        assert_eq!(resumed.next(), Some("s1"));
    }
}
