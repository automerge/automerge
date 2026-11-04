//! RLE splice — insert/delete within a slab, and slab merges.

use crate::edit::EditSlab;
use crate::rle::edit::RleEdit;
use crate::rle::merge::{merge_apply, merge_plan};
use crate::rle::state::WOps;
use crate::rle::{RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};

#[cfg(debug_assertions)]
use crate::rle::validate_rle_slab;

/// Splice an RLE slab: delete `del` items at `index`, insert `values`
/// (each a value and a count). Returns the slabs it spilled into (to go
/// after this one) when the rebuild passes `max_segments`.
///
/// The verified edit rebuild ([`RleEdit`]) does the work, as it does for
/// the column cursor: open at `index`, drop `del` items, push the values,
/// close.
pub(crate) fn splice_slab<T: RleValue, V: AsColumnRef<T>, C: Codec>(
    slab: &mut Slab,
    index: usize,
    del: usize,
    values: impl Iterator<Item = (V, usize)>,
    max_segments: usize,
) -> Vec<Slab> {
    assert!(index + del <= slab.len, "del extends beyond slab");
    assert!(slab.segments <= max_segments);
    let mut e = RleEdit::<T, C>::default();
    e.reset(slab, index, max_segments);
    e.delete(slab, del, &mut |_, _| {});
    for (value, count) in values {
        e.insert(value, count);
    }
    let overflow = e.close(slab);
    #[cfg(debug_assertions)]
    {
        validate_rle_slab::<T, C>(slab);
        for s in &overflow {
            validate_rle_slab::<T, C>(s);
        }
    }
    overflow
}

pub(crate) fn rle_merge<T: RleValue, C: Codec>(a: &mut Slab, b: &Slab) {
    let mut buf = vec![];
    let (seg, tail) = do_merge::<T, C>(&mut a.data, 0, a.tail, a.segments, b, &mut buf);
    a.segments = seg;
    a.tail = tail;
    a.len += b.len;
}

pub(crate) fn do_merge<T: RleValue, C: Codec>(
    a: &mut Vec<u8>,
    start: usize,
    a_tail: RleTail,
    a_segs: usize,
    b: &Slab,
    buf: &mut Vec<u8>,
) -> (usize, RleTail) {
    if b.len == 0 {
        return (a_segs, a_tail);
    }
    // verified with Verus: the plan (both slabs borrowed), then writing it
    let p = merge_plan::<WOps<T, T, C>>(
        &a[start..],
        a_segs,
        &a_tail,
        &b.data,
        b.segments,
        &b.tail,
        buf,
    );
    let tail = merge_apply::<WOps<T, T, C>>(a, start, &p, buf, &b.data, &b.tail);
    (p.segments, tail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Leb128;
    use crate::encoding::EncoderApi;
    use crate::rle::RleDecoder;
    use crate::Encoder;

    fn slab_of(vals: &[u64]) -> Slab {
        Encoder::<u64>::encode_slab(vals.iter().copied())
    }

    fn values(slab: &Slab) -> Vec<u64> {
        RleDecoder::<u64, Leb128>::new(&slab.data).collect()
    }

    /// `splice_slab` against a `Vec` splice: the values, and (when nothing
    /// spills) the canonical bytes of them.
    fn check(input: &[u64], at: usize, del: usize, ins: &[u64], max_segments: usize) {
        let mut slab = slab_of(input);
        let overflow = splice_slab::<u64, u64, Leb128>(
            &mut slab,
            at,
            del,
            ins.iter().map(|v| (*v, 1)),
            max_segments,
        );
        let mut want = input.to_vec();
        want.splice(at..at + del, ins.iter().copied());
        let mut got = values(&slab);
        for s in &overflow {
            assert!(s.segments <= max_segments);
            got.extend(values(s));
        }
        assert_eq!(
            got, want,
            "at={at} del={del} ins={ins:?} max={max_segments}"
        );
        if overflow.is_empty() {
            let canon = slab_of(&want);
            assert_eq!(slab.data, canon.data, "bytes at={at} del={del} ins={ins:?}");
            assert_eq!((slab.len, slab.segments), (canon.len, canon.segments));
        }
    }

    #[test]
    fn every_small_splice() {
        let input: Vec<u64> = vec![1, 1, 1, 2, 3, 3, 4, 5, 5, 5, 6, 7, 8, 8];
        for at in 0..=input.len() {
            for del in 0..=(input.len() - at).min(4) {
                for ins in [vec![], vec![9u64], vec![9, 9], vec![3, 3, 3], vec![5, 6, 7]] {
                    check(&input, at, del, &ins, usize::MAX);
                }
            }
        }
    }

    #[test]
    fn splices_that_spill() {
        check(&[1, 2, 3], 0, 0, &[10, 20, 30, 40, 50, 60], 4);
        check(&[1, 2, 3], 1, 0, &[10, 20, 30, 40, 50, 60], 4);
        check(&[1, 2, 3], 3, 0, &[10, 20, 30, 40, 50, 60], 4);
        check(&[1, 1, 1, 2, 2], 3, 1, &[7, 7, 8, 9, 9, 10, 11], 3);
    }

    #[test]
    fn nulls_and_strings() {
        let input: Vec<Option<u64>> = vec![None, None, Some(1), Some(1), None, Some(2)];
        for at in 0..=input.len() {
            for del in 0..=(input.len() - at) {
                let mut slab = Encoder::<Option<u64>>::encode_slab(input.iter().copied());
                let ins = [Some(1), None];
                splice_slab::<Option<u64>, Option<u64>, Leb128>(
                    &mut slab,
                    at,
                    del,
                    ins.iter().map(|v| (*v, 1)),
                    usize::MAX,
                );
                let mut want = input.clone();
                want.splice(at..at + del, ins);
                let got: Vec<Option<u64>> =
                    RleDecoder::<Option<u64>, Leb128>::new(&slab.data).collect();
                assert_eq!(got, want);
                let canon = Encoder::<Option<u64>>::encode_slab(want.iter().copied());
                assert_eq!(slab.data, canon.data);
            }
        }
        let words: Vec<String> = ["a", "b", "b", "c"].iter().map(|s| s.to_string()).collect();
        let mut slab = Encoder::<String>::encode_slab(words.iter().map(|s| s.as_str()));
        splice_slab::<String, &str, Leb128>(
            &mut slab,
            1,
            2,
            ["x", "x"].into_iter().map(|v| (v, 1)),
            64,
        );
        let got: Vec<String> = RleDecoder::<String, Leb128>::new(&slab.data)
            .map(|s| s.to_string())
            .collect();
        assert_eq!(got, vec!["a", "x", "x", "c"]);
    }
}
