//! `iter_range` over a reversed range (`4..2`) yields nothing. A plain
//! column's iterator is then positioned at the range's start (its docs
//! say even an empty range leaves the iterator at `start`), so extending
//! the window with `set_max` goes on from there. The delta and prefix
//! columns' `iter_range` positioned it at the range's end instead, and
//! extending the window then yielded items before `start`.

use hexane::{Column, DeltaColumn, PrefixColumn};

/// `4..2`
fn reversed() -> std::ops::Range<usize> {
    std::hint::black_box(4)..2
}

#[test]
fn a_plain_column_sits_at_the_start() {
    let col = Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut it = col.iter_range(reversed());
    assert_eq!((it.pos(), it.end_pos(), it.next()), (4, 4, None));
    it.set_max(5);
    assert_eq!(it.next(), Some(50));
}

#[test]
fn so_does_a_delta_column() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut it = col.iter_range(reversed());
    assert_eq!((it.pos(), it.end_pos(), it.next()), (4, 4, None));
    it.set_max(5);
    assert_eq!(it.next(), Some(50));
}

#[test]
fn and_a_prefix_column() {
    let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    let mut it = col.iter_range(reversed());
    assert_eq!((it.pos(), it.end_pos()), (4, 4));
    assert!(it.next().is_none());
    it.set_max(5);
    let item = it.next().unwrap();
    assert_eq!((item.total(), item.value), (150, 50));
}

#[test]
fn ranges_past_the_end_are_unchanged() {
    let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
    let it = col.iter_range(9..12);
    assert_eq!((it.pos(), it.end_pos()), (3, 3));
    let it = col.iter_range(1..12);
    assert_eq!(it.collect::<Vec<_>>(), vec![20, 30]);
    let col = PrefixColumn::<u64>::from_values(vec![10, 20, 30]);
    let it = col.iter_range(9..12);
    assert_eq!((it.pos(), it.end_pos()), (3, 3));
}
