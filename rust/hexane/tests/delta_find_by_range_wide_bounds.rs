//! `DeltaColumn::find_by_range` gave up on a bound that doesn't fit an
//! `i64`: `find_by_range(5u64..u64::MAX)` found nothing, though every
//! value from 5 up lies in it. It couldn't tell whether such a bound was
//! above or below the values. Now a bound that doesn't fit lies above
//! every value the column holds.
//!
//! (The bounds were once of any integer type, and so could lie below
//! `i64::MIN` too. They are now of the column's own type, whose values
//! that don't fit an `i64` are all above it: those of `u64` and `usize`
//! past `i64::MAX`.)

use hexane::DeltaColumn;

/// A linear scan.
fn scan(vals: &[u64], lo: u64, hi: u64) -> Vec<usize> {
    (0..vals.len())
        .filter(|&i| lo <= vals[i] && vals[i] < hi)
        .collect()
}

#[test]
fn unsigned_bounds_above_i64() {
    let col = DeltaColumn::<u64>::from_values(&[1, 5, 9, i64::MAX as u64]);
    let found = |r: std::ops::Range<u64>| col.find_by_range(r).collect::<Vec<_>>();
    assert_eq!(found(5..u64::MAX), vec![1, 2, 3]);
    assert_eq!(found(0..u64::MAX), vec![0, 1, 2, 3]);
    assert_eq!(found(i64::MAX as u64..u64::MAX), vec![3]);
    assert_eq!(found(u64::MAX - 1..u64::MAX), Vec::<usize>::new());
    let reversed = u64::MAX..std::hint::black_box(5);
    assert_eq!(found(reversed), Vec::<usize>::new());
}

#[test]
fn usize_bounds_on_a_usize_column() {
    // (on 64-bit targets, `usize::MAX` doesn't fit an `i64`)
    let col = DeltaColumn::<usize>::from_values(&[0, 2, 4, 100]);
    assert_eq!(
        col.find_by_range(2..usize::MAX).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        col.find_by_range(usize::MAX..).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
}

#[test]
fn wide_bounds_match_a_scan() {
    let m = i64::MAX as u64;
    let vals = vec![0, 3, m - 1, m];
    let bounds = [0, 3, 4, m - 1, m, m + 1, u64::MAX - 1, u64::MAX];
    let col = DeltaColumn::<u64>::from_values(&vals);
    for &lo in &bounds {
        for &hi in &bounds {
            assert_eq!(
                col.find_by_range(lo..hi).collect::<Vec<_>>(),
                scan(&vals, lo, hi),
                "{lo}..{hi} in {vals:?}"
            );
        }
    }
}
