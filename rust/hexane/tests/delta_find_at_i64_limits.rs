//! `DeltaColumn::find_by_value` and `find_by_range` scan a slab's runs of
//! equal deltas with i64 arithmetic that overflowed at the ends of `i64`:
//! `find_by_value(v)` searched `v..v + 1`, which overflows for `i64::MAX`
//! (and a `u64` column's largest value), and the scan subtracted each run's
//! starting value from the range's bounds, which overflows for a bound far
//! from the column's values. Debug builds panicked, and release builds
//! missed matches.

use hexane::DeltaColumn;

fn scan(vals: &[i64], lo: i64, hi: i64) -> Vec<usize> {
    (0..vals.len())
        .filter(|&i| lo <= vals[i] && vals[i] < hi)
        .collect()
}

fn check(vals: &[i64]) {
    let col = DeltaColumn::<i64>::from_values(vals.to_vec());
    let mut bounds = vec![i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for &v in vals {
        bounds.extend([v.saturating_sub(1), v, v.saturating_add(1)]);
    }
    for &lo in &bounds {
        for &hi in &bounds {
            let got: Vec<usize> = col.find_by_range(lo..hi).collect();
            assert_eq!(
                got,
                scan(vals, lo, hi),
                "find_by_range({lo}..{hi}) in {vals:?}"
            );
        }
    }
    for &v in vals {
        let got: Vec<usize> = col.find_by_value(v).collect();
        let want: Vec<usize> = (0..vals.len()).filter(|&i| vals[i] == v).collect();
        assert_eq!(got, want, "find_by_value({v}) in {vals:?}");
    }
}

#[test]
fn values_at_the_top_of_i64() {
    check(&[1, i64::MAX - 1, i64::MAX, i64::MAX]);
}

#[test]
fn ranges_far_from_the_values() {
    check(&[1 << 62, (1 << 62) + 5, (1 << 62) + 10]);
    check(&[-(1 << 62), -(1 << 62) + 3, -(1 << 62) + 6]);
}

#[test]
fn falling_runs() {
    check(&[(1 << 62), (1 << 62) - 7, (1 << 62) - 14, 0, -(1 << 61)]);
}

#[test]
fn small_values_with_any_bounds() {
    check(&[3, 3, 4, 7, 7, 7, 2, -5, 10, 10]);
}

#[test]
fn the_largest_u64_a_column_holds() {
    let max = i64::MAX as u64;
    let col = DeltaColumn::<u64>::from_values(vec![5, max, max]);
    assert_eq!(col.find_by_value(max).collect::<Vec<_>>(), vec![1, 2]);
}
