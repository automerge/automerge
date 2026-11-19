//! `DeltaIter::scan_to_range` resolves its bounds to an `i64` interval
//! clamped to the values the column can hold. But a bound the column
//! can't hold made it give up: on a `u64` column, `scan_to_range(0..
//! u64::MAX)` found nothing, and so did a null lower bound on a nullable
//! column. Such a bound is now clamped: a null sorts below every value,
//! and a value past the domain above every value.
//!
//! These misses, and an empty range, also returned without moving the
//! iterator, though a miss leaves it at the end of its window.

use hexane::DeltaColumn;
use std::ops::Bound;

fn unsigned() -> DeltaColumn<u64> {
    DeltaColumn::from_values(vec![5, 7, 9])
}

#[test]
fn an_upper_bound_past_the_domain_takes_every_value() {
    let col = unsigned();
    assert_eq!(col.iter().scan_to_range(0..u64::MAX), Some((0, 5)));
    assert_eq!(col.iter().scan_to_range(6..=u64::MAX), Some((1, 7)));
    let mut it = col.iter();
    it.next();
    it.next();
    assert_eq!(it.scan_to_range(..u64::MAX), Some((2, 9)));
}

#[test]
fn a_null_lower_bound_is_below_every_value() {
    let col = DeltaColumn::<Option<i64>>::from_values(vec![None, Some(3), Some(8)]);
    assert_eq!(col.iter().scan_to_range(None..Some(5)), Some((1, Some(3))));
    let after_null = (Bound::Excluded(None), Bound::Included(Some(8)));
    assert_eq!(col.iter().scan_to_range(after_null), Some((1, Some(3))));
}

#[test]
fn a_miss_leaves_the_iterator_at_the_end() {
    let col = unsigned();
    let mut it = col.iter();
    assert_eq!(it.scan_to_range(8..8), None);
    assert_eq!(it.next(), None);

    let mut it = col.iter();
    assert_eq!(it.scan_to_range(u64::MAX..), None);
    assert_eq!(it.next(), None);

    let mut it = col.iter();
    assert_eq!(it.scan_to_value(u64::MAX), None);
    assert_eq!(it.next(), None);

    let col = DeltaColumn::<i64>::from_values(vec![5, 7, 9]);
    let mut it = col.iter();
    let past_max = (Bound::Excluded(i64::MAX), Bound::Unbounded);
    assert_eq!(it.scan_to_range(past_max), None);
    assert_eq!(it.next(), None);

    let col = DeltaColumn::<Option<i64>>::from_values(vec![None, Some(3)]);
    let mut it = col.iter();
    assert_eq!(it.scan_to_range(..=None), None);
    assert_eq!(it.next(), None);
}

#[test]
fn a_miss_in_a_window_stops_at_its_end() {
    let col = unsigned();
    let mut it = col.iter_range(0..2);
    assert_eq!(it.scan_to_range(9..9), None);
    assert_eq!(it.pos(), 2);
}

#[test]
fn bounds_inside_the_domain_are_unchanged() {
    let col = unsigned();
    assert_eq!(col.iter().scan_to_range(6..9), Some((1, 7)));
    assert_eq!(col.iter().scan_to_range(8..=9), Some((2, 9)));
    let mut it = col.iter();
    assert_eq!(it.scan_to_range(10..20), None);
    assert_eq!(it.next(), None);
}
