//! `DeltaEncoder::<T, C>::encode_to` and `encode_to_unless` built their
//! encoder state with the default codec, so they wrote LEB128 whatever
//! `C` was. With `Bijou64`, `encode_to` of `[100, 200, 300]` wrote
//! `[3, 228, 0]` where `encode` writes `[6, 200]`, and loading those
//! bytes as a `Bijou64` delta column gave `[114, 114]`.
#![cfg(feature = "bijou64")]

use hexane::{Bijou64, DeltaColumn, DeltaEncoder};

#[test]
fn encode_to_writes_the_codec_asked_for() {
    let vals = vec![100i64, 200, 300];
    let mut out = vec![9];
    let range = DeltaEncoder::<i64, Bijou64>::encode_to(&mut out, vals.clone());
    assert_eq!(
        &out[range],
        &DeltaEncoder::<i64, Bijou64>::encode(vals.clone())[..]
    );
    assert_eq!(out[0], 9);
    let col = DeltaColumn::<i64, Bijou64>::load(&out[1..]).unwrap();
    assert_eq!(col.to_vec(), vals);
}

#[test]
fn encode_to_unless_writes_the_codec_asked_for() {
    let vals = vec![100i64, 200, 300];
    let mut out = vec![];
    let range = DeltaEncoder::<i64, Bijou64>::encode_to_unless(&mut out, vals.clone(), 0);
    assert_eq!(
        &out[range],
        &DeltaEncoder::<i64, Bijou64>::encode(vals.clone())[..]
    );
    let col = DeltaColumn::<i64, Bijou64>::load(&out).unwrap();
    assert_eq!(col.to_vec(), vals);
}

#[test]
fn encode_to_unless_still_elides_uniform_values() {
    let mut out = vec![];
    let range = DeltaEncoder::<i64, Bijou64>::encode_to_unless(&mut out, vec![7i64; 5], 7);
    assert!(range.is_empty());
    assert!(out.is_empty());
}
