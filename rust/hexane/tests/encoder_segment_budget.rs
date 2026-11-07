//! A column's slabs need a budget of at least two segments, and
//! `Column::with_max_segments` panics on less ("max_segments must be at
//! least 2"). The encoders' `with_max_segments` took 0 or 1, and the
//! encoder then cut slabs the column can't hold: `into_column` failed an
//! internal assertion (`s.segments <= max_segments`) after all the values
//! were in. Now the encoders reject the budget when it's set, with the
//! column's message.

use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::encoding::EncoderApi;
use hexane::Leb128;

#[test]
#[should_panic(expected = "max_segments must be at least 2")]
fn rle_encoder_rejects_zero() {
    let _ = RleEncoder::<u64>::default().with_max_segments(0);
}

#[test]
#[should_panic(expected = "max_segments must be at least 2")]
fn rle_encoder_rejects_one() {
    let _ = RleEncoder::<Option<String>>::default().with_max_segments(1);
}

#[test]
#[should_panic(expected = "max_segments must be at least 2")]
fn bool_encoder_rejects_one() {
    let _ = BoolEncoder::<Leb128>::default().with_max_segments(1);
}

#[test]
fn two_segments_still_work() {
    let mut e = RleEncoder::<u64>::default().with_max_segments(2);
    for i in 0..20u64 {
        e.append(i % 3);
    }
    let col: hexane::Column<u64> = e.into_column();
    assert_eq!(col.to_vec(), (0..20u64).map(|i| i % 3).collect::<Vec<_>>());
    let mut e = BoolEncoder::<Leb128>::default().with_max_segments(2);
    for i in 0..20u64 {
        e.append(i % 3 == 0);
    }
    let col: hexane::Column<bool> = e.into_column();
    assert_eq!(col.len(), 20);
}
