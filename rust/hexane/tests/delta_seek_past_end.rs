//! `DeltaIter::seek_to_value` returns an empty range at the insertion
//! point when the target is missing, with the iterator positioned there.
//! But past every value in a window that runs past the column's end, it
//! returned the window's end (`100..100` on a 3-item column), while the
//! iterator stopped at the column's end, 3. The insertion point is
//! where the items end, as `Iter::seek_to_value` answers.

use hexane::DeltaColumn;

fn column() -> DeltaColumn<u64> {
    DeltaColumn::from_values(vec![1, 2, 3])
}

#[test]
fn a_value_above_the_rest_goes_at_the_columns_end() {
    let col = column();
    let mut it = col.iter();
    assert_eq!(it.seek_to_value(10, 0..100), 3..3);
    assert_eq!(it.pos(), 3);

    let mut it = col.iter();
    assert_eq!(it.seek_to_value(10, 1..), 3..3);
    assert_eq!(it.pos(), 3);
}

#[test]
fn a_value_past_the_domain_goes_at_the_columns_end() {
    let col = column();
    let mut it = col.iter();
    assert_eq!(it.seek_to_value(u64::MAX, 0..100), 3..3);
    assert_eq!(it.pos(), 3);
}

#[test]
fn a_window_starting_past_the_end_is_empty_there() {
    let col = column();
    let mut it = col.iter();
    let r = it.seek_to_value(10, 50..100);
    assert_eq!(r.start, r.end);
    assert_eq!(r.start, it.pos());
}

#[test]
fn other_searches_are_unchanged() {
    let col = column();
    assert_eq!(col.iter().seek_to_value(2, 0..100), 1..2);
    assert_eq!(col.iter().seek_to_value(10, 0..2), 2..2);
    assert_eq!(col.iter().seek_to_value(0, 0..100), 0..0);
}
