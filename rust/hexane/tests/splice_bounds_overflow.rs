//! Splices check `index + del <= len` and panic ("splice range out of
//! bounds") past the end. The sum could overflow: with `del` near
//! `usize::MAX` it wrapped in release builds, so the check passed and the
//! splice deleted the rest of the column instead of panicking (debug
//! builds panicked on the addition). Now the check can't overflow.

use hexane::{Column, DeltaColumn, PrefixColumn, Splice};

#[test]
#[should_panic(expected = "splice range out of bounds")]
fn column_splice() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    col.splice(5, usize::MAX, std::iter::empty::<u64>());
}

#[test]
#[should_panic(expected = "splice range out of bounds")]
fn column_remove_n() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    col.remove_n(5, usize::MAX - 2);
}

#[test]
#[should_panic(expected = "splice range out of bounds")]
fn delta_splice() {
    let mut col = DeltaColumn::<i64>::from_values((0..10).collect());
    col.splice(5, usize::MAX, std::iter::empty());
}

#[test]
#[should_panic(expected = "splice range out of bounds")]
fn prefix_splice() {
    let mut col = PrefixColumn::<u64>::from_values((0..10).collect());
    col.splice(5, usize::MAX, std::iter::empty::<u64>());
}

#[test]
#[should_panic(expected = "splice out of bounds")]
fn copy_ranges() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    let src = Column::<u64>::from_values(vec![7, 8]);
    let splice = Splice {
        pos: 5,
        delete: usize::MAX,
        range: 0..2,
    };
    col.copy_ranges(src, [splice]);
}

#[test]
fn splices_in_bounds_are_unchanged() {
    let mut col = Column::<u64>::from_values((0..10).collect());
    col.splice(5, 5, [1u64, 2]);
    assert_eq!(col.to_vec(), vec![0, 1, 2, 3, 4, 1, 2]);
    col.remove_n(0, 7);
    assert!(col.is_empty());
}
