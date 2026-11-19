//! `PrefixColumn::get_total(index)` is `get_prefix(index + 1)`, and
//! `get_prefix` takes any index at or past the end as the column's total.
//! But `index + 1` overflowed at `usize::MAX`: a debug build panicked,
//! and a release build answered `get_prefix(0)`, nothing summed.

use hexane::PrefixColumn;

#[test]
fn the_total_through_usize_max_is_everything() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
    assert_eq!(col.get_total(usize::MAX), 6);
}

#[test]
fn other_totals_are_unchanged() {
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3]);
    assert_eq!(col.get_total(0), 1);
    assert_eq!(col.get_total(2), 6);
    assert_eq!(col.get_total(10), 6);
    assert_eq!(col.get_total(usize::MAX - 1), 6);
}
