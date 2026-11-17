//! A delta column's values must lie within a 2^63-wide range, so that a
//! delete can always store the difference of the two values it makes
//! neighbours. `load` takes untrusted bytes and must reject data outside
//! that range, rather than load it and panic on a later edit.

use hexane::{Codec, DeltaColumn, Leb128, PackError};

/// A literal run of the deltas `ds`.
fn literal(ds: &[i64]) -> Vec<u8> {
    let mut b = Leb128::encode_signed(-(ds.len() as i64))
        .as_bytes()
        .to_vec();
    for &d in ds {
        b.extend_from_slice(Leb128::encode_signed(d).as_bytes());
    }
    b
}

fn rejected<T: std::fmt::Debug>(r: Result<T, PackError>) {
    match r {
        Err(PackError::InvalidValue(m)) if m.contains("domain") => {}
        r => panic!("expected an out-of-domain error, got {r:?}"),
    }
}

/// The values `i64::MAX`, `0`, `i64::MIN + 2`: each fits an `i64`, and so
/// does each delta, but deleting the `0` would need a delta of nearly
/// -2^64.
#[test]
fn i64_values_spanning_2_63_are_rejected() {
    let b = literal(&[i64::MAX, -i64::MAX, i64::MIN + 2]);
    rejected(DeltaColumn::<i64>::load(&b));
    rejected(DeltaColumn::<Option<i64>>::load(&b));
}

/// The same span across slabs: a budget of two segments cuts the
/// literal into a slab per value.
#[test]
fn a_span_across_slabs_is_rejected() {
    let b = literal(&[i64::MAX, -i64::MAX, i64::MIN + 2]);
    let opts = || hexane::LoadOpts::new().with_max_segments(2);
    rejected(DeltaColumn::<i64>::load_with(&b, opts()));
    // the slabs are separate: the same layout loads within the range
    let b = literal(&[i64::MAX, -i64::MAX, 5]);
    let col = DeltaColumn::<i64>::load_with(&b, opts()).unwrap();
    assert!(col.slab_count() > 1);
}

/// The widest span the contract allows still loads, and edits.
#[test]
fn a_span_of_2_63_minus_1_loads() {
    let b = literal(&[i64::MAX, -i64::MAX, 5]);
    let mut col = DeltaColumn::<i64>::load(&b).unwrap();
    assert_eq!(col.to_vec(), vec![i64::MAX, 0, 5]);
    col.remove(1);
    assert_eq!(col.to_vec(), vec![i64::MAX, 5]);

    let b = literal(&[i64::MIN, i64::MAX, -1]);
    let mut col = DeltaColumn::<i64>::load(&b).unwrap();
    assert_eq!(col.to_vec(), vec![i64::MIN, -1, -2]);
    col.remove(1);
    assert_eq!(col.to_vec(), vec![i64::MIN, -2]);
}

/// Nulls are not values: a leading null does not pull the range to the 0
/// the running sum starts from.
#[test]
fn a_leading_null_does_not_widen_the_span() {
    let values = vec![None, Some(i64::MIN), Some(-1)];
    let col = DeltaColumn::<Option<i64>>::from_values(values.clone());
    let loaded = DeltaColumn::<Option<i64>>::load(&col.save()).unwrap();
    assert_eq!(loaded.to_vec(), values);
}
