//! The value searches that take a range of positions (`scope_to_value`,
//! `seek_to_value`) turned it into a window with a helper carried over
//! from v0. It read an excluded start `Excluded(n)` as `n - 1` rather
//! than `n + 1`, so the window began two items early (and `Excluded(0)`
//! underflowed), and an inclusive end `..=usize::MAX` overflowed.

use hexane::{Column, DeltaColumn};
use std::ops::Bound::{Excluded, Unbounded};

fn column() -> Column<u64> {
    Column::from_values((0..10).collect())
}

fn deltas() -> DeltaColumn<u64> {
    DeltaColumn::from_values((0..10).collect())
}

#[test]
fn an_excluded_start_begins_after_it() {
    assert_eq!(
        column().scope_to_value(3u64, (Excluded(3), Unbounded)),
        4..4
    );
    assert_eq!(
        column().scope_to_value(4u64, (Excluded(3), Unbounded)),
        4..5
    );
    assert_eq!(
        deltas().scope_to_value(3u64, (Excluded(3), Unbounded)),
        4..4
    );
    assert_eq!(
        deltas().scope_to_value(4u64, (Excluded(3), Unbounded)),
        4..5
    );
    let col = column();
    assert_eq!(
        col.iter().seek_to_value(3u64, (Excluded(3), Unbounded)),
        4..4
    );
    let col = deltas();
    assert_eq!(
        col.iter().seek_to_value(3u64, (Excluded(3), Unbounded)),
        4..4
    );
}

#[test]
fn an_excluded_zero_start_begins_at_one() {
    assert_eq!(
        column().scope_to_value(0u64, (Excluded(0), Unbounded)),
        1..1
    );
    assert_eq!(
        deltas().scope_to_value(0u64, (Excluded(0), Unbounded)),
        1..1
    );
}

#[test]
fn an_inclusive_end_at_usize_max_takes_everything() {
    assert_eq!(column().scope_to_value(5u64, ..=usize::MAX), 5..6);
    assert_eq!(deltas().scope_to_value(5u64, ..=usize::MAX), 5..6);
    // (`Iter::seek_to_value` over a window past the column's end is
    // fix/iter-seek-to-value-past-end's case)
    let col = deltas();
    assert_eq!(col.iter().seek_to_value(5u64, ..=usize::MAX), 5..6);
}

#[test]
fn other_ranges_are_unchanged() {
    assert_eq!(column().scope_to_value(5u64, 2..8), 5..6);
    assert_eq!(column().scope_to_value(5u64, 2..=4), 5..5);
    assert_eq!(column().scope_to_value(5u64, ..), 5..6);
    assert_eq!(deltas().scope_to_value(5u64, 6..), 6..6);
}
