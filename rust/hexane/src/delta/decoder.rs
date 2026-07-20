//! Streaming decoder for delta-encoded column bytes.
//!
//! Counterpart to [`DeltaEncoder`](crate::delta::DeltaEncoder).  Reads raw
//! `Option<i64>` deltas from a byte slice and accumulates them into
//! realised `T` values, without allocating a [`DeltaColumn`](crate::delta::DeltaColumn) or its
//! slab/B-tree index.  Use this when a single linear pass is all you
//! need — `DeltaColumn::load` + [`DeltaIter`](crate::delta::DeltaIter) is the
//! heavier alternative for when you also want random access.

use std::fmt::Debug;
use std::marker::PhantomData;

use crate::delta::DeltaValue;
use crate::{Codec, Leb128};

/// Streaming decoder over delta-encoded column bytes.  See module docs.
///
/// `next()` is O(1) per item.  There is no `nth` fast path because this
/// decoder doesn't have an index to consult — for skipping, build a
/// [`DeltaColumn`](crate::delta::DeltaColumn) instead.
#[derive(Clone)]
pub struct DeltaDecoder<'a, T: DeltaValue, C: Codec = Leb128> {
    inner: crate::Decoder<'a, T::Inner, C>,
    running: i64,
    _phantom: PhantomData<fn() -> T>,
}

impl<'a, T: DeltaValue, C: Codec> DeltaDecoder<'a, T, C> {
    /// Construct a decoder over delta-encoded column `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            inner: crate::decoder_in::<T::Inner, C>(data),
            running: 0,
            _phantom: PhantomData,
        }
    }

    /// Consume the next run of *raw deltas* (at most `max` items),
    /// folding them into the running sum. `value` is the shared delta:
    /// `Some(0)` means the realized value repeats for the whole run,
    /// `None` is a null run, and literal (varying) deltas come out one
    /// item at a time.
    pub fn next_delta_run_max(&mut self, max: usize) -> Option<crate::Run<Option<i64>>> {
        use crate::delta::DeltaInner;
        use crate::encoding::RunDecoder;
        let run = self.inner.next_run_max(max)?;
        let value = T::Inner::to_opt(run.value);
        if let Some(d) = value {
            self.running += d * run.count as i64;
        }
        Some(crate::Run {
            count: run.count,
            value,
        })
    }

    /// Advance past `n` items in O(runs), keeping the running sum
    /// correct. Panics if fewer than `n` items remain.
    pub fn advance_by(&mut self, mut n: usize) {
        while n > 0 {
            let run = self.next_delta_run_max(n).expect("advance past column end");
            n -= run.count;
        }
    }
}

impl<T: DeltaValue, C: Codec> Iterator for DeltaDecoder<'_, T, C> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        use crate::delta::DeltaInner;
        match T::Inner::to_opt(self.inner.next()?) {
            None => Some(T::null_value()),
            Some(d) => {
                self.running += d;
                Some(T::from_i64(self.running))
            }
        }
    }
}

impl<T: DeltaValue, C: Codec> Debug for DeltaDecoder<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaDecoder")
            .field("running", &self.running)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::delta::DeltaDecoder;
    use crate::delta::DeltaValue;
    use crate::DeltaColumn;

    /// Round-trip parity: bytes saved by `DeltaColumn::save` should
    /// decode to the same sequence via `DeltaDecoder` as via
    /// `DeltaColumn::load(...).iter()`.
    fn parity<T>(values: Vec<T>)
    where
        T: DeltaValue + PartialEq + std::fmt::Debug,
    {
        let col = DeltaColumn::<T>::from_values(values.clone());
        let bytes = col.save();
        let via_iter: Vec<T> = DeltaColumn::<T>::load(&bytes).unwrap().iter().collect();
        let via_decoder: Vec<T> = DeltaDecoder::<T>::new(&bytes).collect();
        assert_eq!(via_decoder, via_iter, "decoder vs DeltaIter mismatch");
        assert_eq!(via_decoder, values, "decoder vs source mismatch");
    }

    #[test]
    fn empty() {
        parity::<u64>(vec![]);
    }

    #[test]
    fn monotonic() {
        parity::<u64>(vec![10, 20, 30, 40]);
    }

    #[test]
    fn non_monotonic_signed() {
        parity::<i64>(vec![5, 3, 8, 2, 10, -100, 0]);
    }

    #[test]
    fn unsigned_non_monotonic() {
        parity::<u64>(vec![100, 50, 200, 10]);
    }

    #[test]
    fn many_runs() {
        let mut v: Vec<u64> = Vec::with_capacity(10_000);
        let mut x = 0u64;
        for i in 0..10_000 {
            // Mix of constant strides + jumps to exercise multi-slab.
            x = x.wrapping_add(if i % 17 == 0 { 1000 } else { 1 });
            v.push(x);
        }
        parity::<u64>(v);
    }
}
