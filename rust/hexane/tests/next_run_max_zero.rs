//! `Decoder::next_run_max(max)` (the old `RunDecoder::next_run_max`)
//! consumes at most `max` items. With `max == 0` there is no run to give:
//! the RLE decoder returns `None` and consumes nothing, and so must the
//! bool decoder, which returned a run of no items. (A caller looping
//! `while let Some(run) = d.next_run_max(k)` with `k == 0` never
//! finished.)

use hexane::Column;

#[test]
fn a_bool_decoder_gives_no_run_of_no_items() {
    let bytes = Column::<bool>::from_values(&[true, true, false]).save();
    let mut d = hexane::decoder::<bool>(&bytes);
    assert_eq!(d.next_run_max(0), None);
    // nothing was consumed
    assert_eq!(d.collect::<Vec<_>>(), vec![true, true, false]);
}

#[test]
fn neither_does_an_rle_decoder() {
    let bytes = Column::<u64>::from_values(&[5, 5, 6]).save();
    let mut d = hexane::decoder::<u64>(&bytes);
    assert_eq!(d.next_run_max(0), None);
    assert_eq!(d.collect::<Vec<_>>(), vec![5, 5, 6]);
}
