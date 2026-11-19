//! `DeltaIter::seek_to_value` (behind `DeltaColumn::scope_to_value`) finds
//! the item within a run of equal deltas by a ceiling division,
//! `(t - running + d - 1).div_euclid(d)`, whose sum overflowed `i64` for
//! deltas near 2^62 — values a delta column holds (they span less than
//! 2^63). Debug builds panicked, and release builds could return the wrong
//! range.

use hexane::DeltaColumn;

fn check(vals: &[i64]) {
    let col = DeltaColumn::<i64>::from_values(vals.to_vec());
    for &t in vals.iter().chain([0, 1, -1].iter()) {
        let lo = vals.iter().position(|&v| v >= t).unwrap_or(vals.len());
        let hi = vals.iter().position(|&v| v > t).unwrap_or(vals.len());
        assert_eq!(col.scope_to_value(t, ..), lo..hi, "target {t} in {vals:?}");
    }
}

#[test]
fn a_run_of_large_deltas() {
    // -2^62, then two deltas of 2^62 - 1: the values span 2^63 - 2
    let d = (1i64 << 62) - 1;
    let v0 = -(1i64 << 62);
    check(&[v0, v0 + d, v0 + 2 * d]);
}

#[test]
fn one_large_delta() {
    check(&[-(1i64 << 62) + 1, (1i64 << 62) - 1]);
}

#[test]
fn small_deltas_are_unchanged() {
    check(&[-5, -5, -2, 0, 0, 0, 3, 7, 7, 100]);
    check(&[1, 2, 3, 4, 5, 6, 7, 8]);
}
