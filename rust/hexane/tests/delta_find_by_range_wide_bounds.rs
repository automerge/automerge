//! `DeltaColumn::find_by_range` takes bounds of any integer type, and
//! gave up on a bound that doesn't fit an `i64`: `find_by_range(5u64..
//! u64::MAX)` found nothing, though every value from 5 up lies in it. It
//! couldn't tell whether such a bound was above or below the values. Now
//! a bound that doesn't fit lies above every value if positive, below
//! every value if negative.

use hexane::DeltaColumn;

/// A linear scan, comparing as `i128`s.
fn scan(vals: &[i64], lo: i128, hi: i128) -> Vec<usize> {
    (0..vals.len())
        .filter(|&i| lo <= vals[i] as i128 && (vals[i] as i128) < hi)
        .collect()
}

#[test]
fn unsigned_bounds_above_i64() {
    let col = DeltaColumn::<u64>::from_values(vec![1, 5, 9, i64::MAX as u64]);
    let found = |r: std::ops::Range<u64>| col.find_by_range(r).collect::<Vec<_>>();
    assert_eq!(found(5..u64::MAX), vec![1, 2, 3]);
    assert_eq!(found(0..u64::MAX), vec![0, 1, 2, 3]);
    assert_eq!(found(i64::MAX as u64..u64::MAX), vec![3]);
    assert_eq!(found(u64::MAX - 1..u64::MAX), Vec::<usize>::new());
    let reversed = u64::MAX..std::hint::black_box(5);
    assert_eq!(found(reversed), Vec::<usize>::new());
}

#[test]
fn usize_bounds_on_a_u32_column() {
    // (on 64-bit targets, `usize::MAX` doesn't fit an `i64`)
    let col = DeltaColumn::<u32>::from_values(vec![0, 2, 4, 100]);
    assert_eq!(
        col.find_by_range(2usize..usize::MAX).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn wide_bounds_match_a_scan() {
    // (a delta column's values must lie within `i64::MAX` of each other)
    let top = vec![0, 3, i64::MAX - 1, i64::MAX];
    let bottom = vec![i64::MIN, i64::MIN + 1, -7, -1];
    let big = 1i128 << 70;
    let bounds = [
        -big,
        i64::MIN as i128 - 1,
        i64::MIN as i128,
        -7,
        0,
        4,
        i64::MAX as i128,
        i64::MAX as i128 + 1,
        big,
    ];
    for vals in [top, bottom] {
        let col = DeltaColumn::<i64>::from_values(vals.clone());
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
}
