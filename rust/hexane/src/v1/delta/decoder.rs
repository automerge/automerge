//! Streaming decoder for delta-encoded column bytes.
//!
//! Counterpart to [`DeltaEncoder`](super::DeltaEncoder).  Reads raw
//! `Option<i64>` deltas from a byte slice and accumulates them into
//! realised `T` values, without allocating a [`DeltaColumn`] or its
//! slab/B-tree index.  Use this when a single linear pass is all you
//! need — `DeltaColumn::load` + [`DeltaIter`](super::DeltaIter) is the
//! heavier alternative for when you also want random access.

use std::fmt::Debug;
use std::marker::PhantomData;

use super::DeltaValue;

/// Streaming decoder over delta-encoded column bytes.  See module docs.
///
/// `next()` is O(1) per item.  There is no `nth` fast path because this
/// decoder doesn't have an index to consult — for skipping, build a
/// [`DeltaColumn`](super::DeltaColumn) instead.
#[derive(Clone)]
pub struct DeltaDecoder<'a, T: DeltaValue> {
    inner: crate::v1::Decoder<'a, Option<i64>>,
    running: i64,
    _phantom: PhantomData<fn() -> T>,
}

impl<'a, T: DeltaValue> DeltaDecoder<'a, T> {
    /// Construct a decoder over delta-encoded column `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            inner: crate::v1::decoder::<Option<i64>>(data),
            running: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeltaValue> Iterator for DeltaDecoder<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.inner.next()? {
            None => Some(T::null_value()),
            Some(d) => {
                self.running += d;
                Some(T::from_i64(self.running))
            }
        }
    }
}

impl<T: DeltaValue> Debug for DeltaDecoder<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaDecoder")
            .field("running", &self.running)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::DeltaDecoder;
    use super::super::DeltaColumn;

    /// Round-trip parity: bytes saved by `DeltaColumn::save` should
    /// decode to the same sequence via `DeltaDecoder` as via
    /// `DeltaColumn::load(...).iter()`.
    fn parity<T>(values: Vec<T>)
    where
        T: super::DeltaValue + PartialEq + std::fmt::Debug,
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
