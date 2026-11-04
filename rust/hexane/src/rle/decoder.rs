//! RLE decoder — forward iterator over items in a single RLE-encoded slab.
//!
//! A thin wrapper around [`Dec`], which is verified with Verus: on a
//! valid slab it yields exactly the slab's values, and never panics.

use crate::encoding::RunDecoder;
pub use crate::rle::dec::RleDecoderState;
use crate::rle::dec::{Dec, RDecoder};
use crate::rle::model::Ops;

use crate::{Codec, ColumnValueRef, Leb128, RleValue, Run};

/// Forward iterator over all items in a single RLE-encoded slab.
///
/// Created by the `ColumnEncoding::decoder` method.  Repeat runs yield the cached value
/// in O(1) per item.  Literal runs decode each value.  Null runs yield
/// the type's null value.
///
/// The decoding is verified with Verus (`Dec`), and so is what the column
/// iterator does with it (`RDecoder`, `RleReads`).
pub type RleDecoder<'a, T, C = Leb128> = RDecoder<'a, Ops<T, C>>;

impl<T: RleValue, C: Codec> Clone for RleDecoder<'_, T, C> {
    fn clone(&self) -> Self {
        RDecoder {
            inner: self.inner.clone(),
        }
    }
}

impl<T: RleValue, C: Codec> std::fmt::Debug for RleDecoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RleDecoder")
            .field("byte_pos", &self.inner.byte_pos)
            .field("remaining", &self.inner.remaining)
            .finish()
    }
}

impl<'a, T: RleValue, C: Codec> RleDecoder<'a, T, C> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            inner: Dec::new(data),
        }
    }

    /// Capture the position so it can be restored against the same bytes
    /// later, without holding a borrow of them. Field copies only.
    pub(crate) fn suspend(&self) -> RleDecoderState {
        self.inner.suspend()
    }

    /// Restore a suspended position against `data`.
    ///
    /// O(1): only a repeat run reads anything, and only its own value.
    /// `data` must be the bytes the state was suspended from — a state
    /// resumed against a different slab decodes garbage, the same way a
    /// byte offset into the wrong buffer would.
    pub(crate) fn resume(data: &'a [u8], state: &RleDecoderState) -> Self {
        Self {
            inner: Dec::resume(data, state),
        }
    }

    /// Advance past `n` items in O(runs skipped), decoding no values.
    /// Stops at the end of the column.
    // inherent, so a call on a concrete decoder is never ambiguous with
    // the unstable `Iterator::advance_by`
    pub fn advance_by(&mut self, n: usize) {
        self.inner.advance_by(n)
    }
}

impl<'a, T: RleValue, C: Codec> Iterator for RleDecoder<'a, T, C> {
    type Item = <T as ColumnValueRef>::Get<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    /// O(runs_skipped) skip — repeat and null runs are skipped in O(1) each,
    /// literal runs advance `byte_pos` via `value_len` without full decoding.
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.inner.nth(n)
    }
}

impl<'a, T: RleValue, C: Codec> RunDecoder for RleDecoder<'a, T, C> {
    fn next_run(&mut self) -> Option<Run<Self::Item>> {
        self.next_run_max(usize::MAX)
    }

    fn next_run_max(&mut self, max: usize) -> Option<Run<Self::Item>> {
        self.inner.next_run_max(max)
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
