//! A column's streaming load (`load_iter`) checks each run's weight as it
//! goes. But the loader had already taken the run, so a caller that went
//! on past the error got the runs after it, and `finalize` built a column
//! whose index left the bad run out. An error now ends the load: later
//! calls and `finalize` fail too.
//!
//! (The bug was met with a delta column's inner column, whose runs' sums
//! must fit an `i64`, and which is no longer public. The weight checked
//! here is a plain column's: its length, which must fit a `usize`.)

use hexane::{Codec, Column, Leb128, LoadOpts, PackError};

/// Four repeat runs of `i64::MAX` values: more than a `usize` counts.
fn bytes() -> Vec<u8> {
    let mut b = vec![];
    for v in [1u8, 2, 3, 4] {
        Leb128::put_i(i64::MAX, &mut b);
        b.push(v);
    }
    b
}

/// Take runs until one is an error, and say whether one was.
fn until_an_error(it: &mut hexane::ColumnLoadIter<'_, u64>) -> bool {
    for _ in 0..4 {
        match it.try_next_run() {
            Err(_) => return true,
            Ok(None) => return false,
            Ok(Some(_)) => {}
        }
    }
    false
}

#[test]
fn a_load_goes_no_further_after_an_error() {
    let bytes = bytes();
    let mut it = Column::<u64>::load_iter(&bytes, LoadOpts::new());
    assert!(until_an_error(&mut it));
    assert!(matches!(it.try_next_run(), Err(PackError::BadFormat)));
    assert!(matches!(it.try_next_run(), Err(PackError::BadFormat)));
    assert!(it.finalize().is_err());
}

#[test]
fn finalize_after_an_error_fails() {
    let bytes = bytes();
    let mut it = Column::<u64>::load_iter(&bytes, LoadOpts::new());
    assert!(until_an_error(&mut it));
    assert!(it.finalize().is_err());
}

#[test]
fn a_load_without_errors_is_unchanged() {
    let bytes = Column::<u64>::from_values(&[5, 5, 3]).save();
    let mut it = Column::<u64>::load_iter(&bytes, LoadOpts::new());
    while it.try_next_run().unwrap().is_some() {}
    assert_eq!(it.finalize().unwrap().to_vec(), vec![5, 5, 3]);
}

/// Two runs of the same value side by side are not canonical. Going on
/// past that error used to give a column whose bytes held both runs but
/// whose length counted only the first.
#[test]
fn a_rejected_run_does_not_reach_the_column() {
    let one = Column::<u64>::from_values(&[5, 5]).save();
    let bytes = [one.as_slice(), one.as_slice()].concat();
    let mut it = Column::<u64>::load_iter(&bytes, LoadOpts::new());
    assert!(it.try_next_run().unwrap().is_some());
    assert!(it.try_next_run().is_err());
    assert!(matches!(it.try_next_run(), Err(PackError::BadFormat)));
    assert!(it.finalize().is_err());
}
