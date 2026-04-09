//! RLE decoder — forward iterator over items in a single RLE-encoded slab.

use crate::v1::encoding::RunDecoder;
use crate::v1::leb::{read_signed, read_unsigned, try_read_signed, try_read_unsigned};
use crate::v1::{ColumnValueRef, RleValue, Run};
use crate::PackError;

/// Forward iterator over all items in a single RLE-encoded slab.
///
/// Created by the `ColumnEncoding::decoder` method.  Repeat runs yield the cached value
/// in O(1) per item.  Literal runs decode each value.  Null runs yield
/// the type's null value.
pub struct RleDecoder<'a, T: RleValue> {
    data: &'a [u8],
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
    pub(crate) fn new(data: &'a [u8]) -> Self {
        RleDecoder {
            data,
            byte_pos: 0,
            remaining: 0,
            state: RleDecoderState::Idle,
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.byte_pos
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

    pub(crate) fn try_next_segment(&mut self) -> Result<Option<RleSegment<'a, T>>, PackError> {
        if self.remaining > 0 && matches!(self.state, RleDecoderState::Literal) {
            self.remaining -= 1;
            let (bytes, value) = T::try_unpack(&self.data[self.byte_pos..])?;
            self.byte_pos += bytes;
            Ok(Some(RleSegment::Lit { value, bytes }))
        } else if self.data[self.byte_pos..].is_empty() {
            Ok(None)
        } else {
            match try_read_signed(&self.data[self.byte_pos..])? {
                (count_bytes, n) if n > 0 => {
                    let count = n as usize;
                    let value_start = self.byte_pos + count_bytes;
                    let (vlen, value) = T::try_unpack(&self.data[value_start..])?;
                    let bytes = count_bytes + vlen;
                    self.byte_pos += bytes;
                    self.state = RleDecoderState::Idle;
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
                    self.state = RleDecoderState::Literal;
                    Ok(Some(RleSegment::LitHead { count, bytes }))
                }
                (count_bytes, _) => {
                    let (ncb, count) =
                        try_read_unsigned(&self.data[self.byte_pos + count_bytes..])?;
                    let count = count as usize;
                    let bytes = count_bytes + ncb;
                    self.byte_pos += bytes;
                    self.state = RleDecoderState::Idle;
                    self.remaining = 0;
                    Ok(Some(RleSegment::Null { count, bytes }))
                }
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
                    RleDecoderState::Null => Some(T::get_null()),
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

impl<'a, T: RleValue> RunDecoder for RleDecoder<'a, T> {
    fn next_run(&mut self) -> Option<Run<Self::Item>> {
        self.next_run_max(usize::MAX)
    }

    fn next_run_max(&mut self, max: usize) -> Option<Run<Self::Item>> {
        loop {
            if self.remaining > 0 {
                return match &self.state {
                    RleDecoderState::Repeat(v) => {
                        let value = *v;
                        let count = self.remaining.min(max);
                        self.remaining -= count;
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
                        let value = T::get_null();
                        let count = self.remaining.min(max);
                        self.remaining -= count;
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
