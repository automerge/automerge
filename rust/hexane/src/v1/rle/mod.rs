mod decoder;
mod load;
pub(crate) mod splice;
pub(crate) mod state;
#[cfg(test)]
mod tests;

pub use decoder::RleDecoder;
use load::rle_load_and_verify;
pub(crate) use load::rle_validate_encoding;
use splice::{do_merge, rle_merge, splice_slab};

use std::marker::PhantomData;
use std::num::NonZeroU32;

use crate::PackError;

type Slab = super::column::Slab<RleTail>;
use super::encoding::{ColumnEncoding, SlabInfo};
use super::{AsColumnRef, ColumnValueRef, RleValue};

// ── Wire-format helpers ───────────────────────────────────────────────────────
//
// The encoding (shared with v0) is a sequence of runs:
//
//   Repeat run : signed_leb128( count > 0 )  packed_value
//   Literal run: signed_leb128( -n      )    v0 v1 … v(n-1)
//   Null run   : signed_leb128( 0       )    unsigned_leb128( count )

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

/// Compute the RleTail for a slab by scanning to the last segment.
#[cfg(test)]
pub(crate) fn compute_rle_tail<T: RleValue>(data: &[u8]) -> RleTail {
    use super::leb::{read_signed, read_unsigned};
    if data.is_empty() {
        return RleTail::default();
    }
    let mut pos = 0;
    let mut last_start = 0;
    let mut lit_tail: Option<NonZeroU32> = None;
    while pos < data.len() {
        last_start = pos;
        let (cb, raw) = read_signed(&data[pos..]).unwrap();
        match raw {
            n if n > 0 => {
                let vl = T::value_len(&data[pos + cb..]).unwrap();
                lit_tail = None;
                pos += cb + vl;
            }
            n if n < 0 => {
                let total = (-n) as usize;
                let mut sb = pos + cb;
                let mut last_vl = 0;
                for _ in 0..total {
                    last_vl = T::value_len(&data[sb..]).unwrap();
                    sb += last_vl;
                }
                lit_tail = NonZeroU32::new(last_vl as u32);
                pos = sb;
            }
            _ => {
                let (ncb, _) = read_unsigned(&data[pos + cb..]).unwrap();
                lit_tail = None;
                pos += cb + ncb;
            }
        }
    }
    RleTail {
        bytes: (data.len() - last_start) as u32,
        lit_tail,
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RleTail {
    /// Total byte length of the last segment (header + values).
    /// Same meaning for repeat, literal, and null runs.
    pub(crate) bytes: u32,
    /// For literal runs: byte length of the last value only.
    /// `None` for repeat/null runs.
    pub(crate) lit_tail: Option<NonZeroU32>,
}

impl RleTail {
    pub(crate) fn with_lit_tail(mut self, lit_tail: Option<NonZeroU32>) -> Self {
        self.lit_tail = lit_tail;
        self
    }
}

/// Validate an RLE slab's len, segments, and tail. Panics on mismatch.
#[cfg(debug_assertions)]
fn validate_rle_slab<T: RleValue>(slab: &Slab) {
    let info = rle_validate_encoding::<T>(&slab.data)
        .unwrap_or_else(|e| panic!("rle slab encoding invalid: {e}"));
    assert_eq!(slab.len, info.len, "rle slab len mismatch");
    assert_eq!(slab.segments, info.segments, "rle slab segments mismatch");
    assert_eq!(slab.tail, info.tail, "rle slab tail mismatch");
}

impl<T: RleValue + ColumnValueRef<Encoding = RleEncoding<T>>> ColumnEncoding for RleEncoding<T> {
    type Value = T;
    type Tail = RleTail;

    fn fill(len: usize, value: T::Get<'_>) -> Slab {
        use state::{RleCow, RleState};
        let mut buf = Vec::new();
        let mut state = RleState::<T, T>::Empty;
        let mut f = state.append_n(&mut buf, RleCow::Ref(value), len);
        f += state.flush(&mut buf);
        let tail = f.wpos.as_tail(0, buf.len());
        Slab {
            data: buf,
            len,
            segments: f.segments,
            tail,
        }
    }

    fn merge_slabs(a: &mut Slab, b: Slab) {
        if a.len == 0 {
            *a = b;
        } else if b.len > 0 {
            rle_merge::<T>(a, &b);
        }
    }

    fn validate_encoding(slab: &[u8]) -> Result<SlabInfo<RleTail>, PackError> {
        rle_validate_encoding::<T>(slab)
    }

    fn load_and_verify(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(<T as ColumnValueRef>::Get<'a>) -> Option<String>>,
    ) -> Result<Vec<Slab>, PackError> {
        rle_load_and_verify::<T>(data, max_segments, validate)
    }

    fn do_merge(
        acc: &mut Vec<u8>,
        a_tail: RleTail,
        a_segments: usize,
        b: &Slab,
        buf: &mut Vec<u8>,
    ) -> (usize, RleTail) {
        do_merge::<T>(acc, a_tail, a_segments, b, buf)
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

    fn decoder(slab: &[u8]) -> RleDecoder<'_, T> {
        RleDecoder::new(slab)
    }
}
