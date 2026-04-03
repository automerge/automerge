//! RLE decoder — forward iterator over items in a single RLE-encoded slab.

use crate::v1::encoding::RunDecoder;
use crate::v1::leb::{read_signed, read_unsigned};
use crate::v1::{ColumnValueRef, RleValue, Run};

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
                        let value = T::get_null();
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
