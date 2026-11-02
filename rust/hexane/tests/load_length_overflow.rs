//! Untrusted column data whose run counts add up to more values than a
//! `usize` can count is a load error, never a panic or a wrapped length.

use hexane::{Codec, Column, DeltaColumn, Leb128};

fn signed(n: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(Leb128::encode_signed(n).as_bytes());
}

fn unsigned(n: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(Leb128::encode_unsigned(n).as_bytes());
}

/// Three repeat runs of `i64::MAX` values: more than a `usize` holds.
fn rle_runs() -> Vec<u8> {
    let mut b = vec![];
    for v in [1u8, 2, 3] {
        signed(i64::MAX, &mut b);
        b.push(v);
    }
    b
}

#[test]
fn rle_length_over_usize_is_an_error() {
    let b = rle_runs();
    assert!(Column::<u64>::load(&b).is_err());
    assert!(Column::<Option<u64>>::load(&b).is_err());
    assert!(DeltaColumn::<u64>::load(&b).is_err());
    // a null run that tips the length over
    let mut b = vec![];
    signed(i64::MAX, &mut b);
    b.push(7);
    signed(0, &mut b);
    unsigned(u64::MAX, &mut b);
    assert!(Column::<Option<u64>>::load(&b).is_err());
}

#[test]
fn bool_length_over_usize_is_an_error() {
    let mut b = vec![];
    for _ in 0..3 {
        unsigned(i64::MAX as u64, &mut b);
    }
    assert!(Column::<bool>::load(&b).is_err());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn lengths_that_fit_still_load() {
    let mut b = rle_runs();
    b.truncate(b.len() / 3 * 2);
    assert_eq!(
        Column::<u64>::load(&b).unwrap().len(),
        2 * i64::MAX as usize
    );
    let mut b = vec![];
    for _ in 0..2 {
        unsigned(i64::MAX as u64, &mut b);
    }
    assert_eq!(
        Column::<bool>::load(&b).unwrap().len(),
        2 * i64::MAX as usize
    );
}
