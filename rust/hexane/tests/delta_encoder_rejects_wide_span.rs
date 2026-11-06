//! A delta column's values must lie within a range narrower than 2^63
//! (see `DeltaValue`): a delete can make any two of them neighbours, and
//! their difference must fit in a delta. `DeltaColumn` panics on writes
//! outside that domain and `load` rejects bytes holding them, but
//! `DeltaEncoder` wrote them, so its bytes then failed to load (in debug
//! builds, it sometimes panicked on the arithmetic instead).

use hexane::delta::DeltaEncoder;
use hexane::DeltaColumn;

fn encode(vals: &[i64]) -> Vec<u8> {
    let mut e = DeltaEncoder::<i64>::new();
    for &v in vals {
        e.append(v);
    }
    e.save()
}

#[test]
#[should_panic(expected = "delta values out of domain")]
fn neighbours_differing_by_2_63_or_more_panic() {
    encode(&[i64::MIN, i64::MAX]);
}

#[test]
#[should_panic(expected = "delta values out of domain")]
fn values_spanning_2_63_or_more_panic_though_neighbours_are_close() {
    encode(&[i64::MIN, -1, i64::MAX - 1]);
}

#[test]
#[should_panic(expected = "delta values out of domain")]
fn encode_to_panics_too() {
    let mut out = Vec::new();
    DeltaEncoder::<Option<i64>>::encode_to(
        &mut out,
        [Some(i64::MIN), None, Some(-1), Some(i64::MAX - 1)],
    );
}

#[test]
fn values_spanning_less_than_2_63_round_trip() {
    for vals in [
        vec![i64::MIN, -1, i64::MIN + 5],
        vec![0, i64::MAX, 7],
        vec![-1, i64::MAX - 1],
    ] {
        let bytes = encode(&vals);
        assert_eq!(DeltaColumn::<i64>::load(&bytes).unwrap().to_vec(), vals);
    }
}
