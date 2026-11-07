//! `EncoderApi::into_slab` returns one slab of every value appended. With
//! `max_segments` set, the encoder cuts its output into slabs as values
//! arrive, and one slab can no longer hold them: `into_slab` panics then,
//! in release builds too, rather than return only the values since the
//! last cut.

use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::EncoderApi;

#[test]
#[should_panic(expected = "into_slab is single-slab only")]
fn an_rle_encoder_that_cut_has_no_single_slab() {
    let mut e = RleEncoder::<u64>::default();
    e.max_segments(2);
    for v in 0..100u64 {
        e.append(v);
    }
    e.into_slab();
}

#[test]
#[should_panic(expected = "into_slab is single-slab only")]
fn a_bool_encoder_that_cut_has_no_single_slab() {
    let mut e = BoolEncoder::<hexane::Leb128>::default();
    e.max_segments(2);
    for v in 0..100u64 {
        e.append(v % 2 == 0);
    }
    e.into_slab();
}

#[test]
fn without_a_cut_into_slab_succeeds() {
    let mut e = RleEncoder::<u64>::default();
    e.max_segments(1000);
    for v in 0..100u64 {
        e.append(v);
    }
    e.into_slab();
    let mut e = BoolEncoder::<hexane::Leb128>::default();
    for v in 0..100u64 {
        e.append(v % 3 == 0);
    }
    e.into_slab();
}
