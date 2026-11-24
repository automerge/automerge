//! Splices check `index + del <= len` and panic ("splice range out of
//! bounds") past the end. The sum could overflow: with `del` near
//! `usize::MAX` it wrapped in release builds, so the check passed and the
//! splice deleted the rest of the column instead of panicking (debug
//! builds panicked on the addition). Now the check can't overflow.
//!
//! A splice of a column, a delta column or a prefix column, and
//! `remove_n`, now require their range to be in bounds, and promise
//! nothing of others. `copy_ranges` requires it too, and still says that
//! it panics on splices out of bounds: that is checked here, with the
//! sum that overflowed, as is that splices in bounds are unchanged.

use hexane::{Column, Splice};

#[test]
#[should_panic(expected = "splice out of bounds")]
fn copy_ranges() {
    let mut col = Column::<u64>::from_values(&(0..10).collect::<Vec<_>>());
    let src = Column::<u64>::from_values(&[7, 8]);
    let splice = Splice {
        pos: 5,
        delete: usize::MAX,
        range: 0..2,
    };
    col.copy_ranges(src, &[splice]);
}

#[test]
fn splices_in_bounds_are_unchanged() {
    let mut col = Column::<u64>::from_values(&(0..10).collect::<Vec<_>>());
    col.splice(5, 5, [1u64, 2]);
    assert_eq!(col.to_vec(), vec![0, 1, 2, 3, 4, 1, 2]);
    col.remove_n(0, 7);
    assert!(col.is_empty());
    let mut col = Column::<u64>::from_values(&(0..10).collect::<Vec<_>>());
    col.copy_ranges(
        Column::<u64>::from_values(&[7, 8]),
        &[Splice {
            pos: 5,
            delete: 5,
            range: 0..2,
        }],
    );
    assert_eq!(col.to_vec(), vec![0, 1, 2, 3, 4, 7, 8]);
}
