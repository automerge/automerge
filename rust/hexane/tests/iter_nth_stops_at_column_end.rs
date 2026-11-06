//! A column iterator's window can run past the column's end (`set_max`).
//! `nth` to an item past the data returned `None`, but left `pos()` at
//! that item: on a five-item column, after `set_max(100)` and `nth(50)`,
//! `pos()` was 50. The delta and prefix iterators stop at the column's
//! end (`pos()` 5), as `next` does. Now the plain iterator does too.

use hexane::{Column, DeltaColumn, PrefixColumn};

#[test]
fn nth_past_the_data_stops_at_the_end() {
    let col = Column::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut it = col.iter();
    it.set_max(100);
    assert_eq!(it.nth(50), None);
    assert_eq!((it.pos(), it.end_pos()), (5, 5));
    it.set_max(100);
    assert_eq!(it.next(), None);
    assert_eq!(it.pos(), 5);

    let col = Column::<bool>::from_values(vec![true, false, true]);
    let mut it = col.iter();
    it.set_max(100);
    assert_eq!(it.nth(50), None);
    assert_eq!(it.pos(), 3);
}

#[test]
fn across_slabs_too() {
    let vals: Vec<u64> = (0..200).map(|i| i * 7 % 13).collect();
    let col = Column::<u64>::from_values_with_max_segments(vals.clone(), 4);
    assert!(col.slab_count() > 4);
    for start in [0, 1, 57, 199] {
        let mut it = col.iter();
        it.set_max(1000);
        it.advance_to(start);
        assert_eq!(it.nth(500), None);
        assert_eq!(it.pos(), 200, "from {start}");
    }
}

#[test]
fn as_the_delta_and_prefix_iterators_do() {
    let col = DeltaColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut it = col.iter();
    it.set_max(100);
    assert_eq!(it.nth(50), None);
    assert_eq!(it.pos(), 5);
    let col = PrefixColumn::<u64>::from_values(vec![1, 2, 3, 4, 5]);
    let mut it = col.iter();
    it.set_max(100);
    assert!(it.nth(50).is_none());
    assert_eq!(it.pos(), 5);
}
