//! `Shiftable::seek_to(pos)` returns the item at `pos`, and `None` when
//! `pos` is behind the current position (the docs say so, and the item
//! iterators do so). The run iterators (`Iter::runs`, `DeltaIter::runs`)
//! position by item and override `seek_to`, but didn't check: a
//! backwards `seek_to` returned the run at the current position.

use hexane::{Column, DeltaColumn, Shiftable};

#[test]
fn runs_seek_to_backwards_is_none() {
    let col = Column::<u64>::from_values(vec![1, 1, 2, 2, 3]);
    let mut runs = col.iter().runs();
    let run = runs.seek_to(2).unwrap();
    assert_eq!((run.count, run.value), (2, 2));
    assert_eq!(runs.get_pos(), 4);
    assert!(runs.seek_to(1).is_none());
    assert_eq!(runs.get_pos(), 4);
    let run = runs.seek_to(4).unwrap();
    assert_eq!((run.count, run.value), (1, 3));
}

#[test]
fn delta_runs_seek_to_backwards_is_none() {
    let col = DeltaColumn::<u64>::from_values(vec![1, 2, 3, 10, 20]);
    let mut runs = col.iter().runs();
    assert!(runs.seek_to(3).is_some());
    let pos = runs.get_pos();
    assert!(runs.seek_to(1).is_none());
    assert_eq!(runs.get_pos(), pos);
}

#[test]
fn item_iterators_agree() {
    let col = Column::<u64>::from_values(vec![1, 1, 2, 2, 3]);
    let mut it = col.iter();
    assert_eq!(it.seek_to(3), Some(2));
    assert_eq!(it.seek_to(1), None);
}
