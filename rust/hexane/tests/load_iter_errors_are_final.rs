//! A column's streaming load (`load_iter`) checks each run's weight as it
//! goes: a delta run whose sum leaves `i64` is an error. But the loader
//! had already taken the run, so a caller that went on past the error got
//! the runs after it, and `finalize` built a column whose index left the
//! bad run out. An error now ends the load: later calls and `finalize`
//! fail too.

use hexane::delta::indexed::IndexedDeltaWeightFn;
use hexane::{Column, Leb128, LoadOpts, PackError};

type DeltaInner = Column<i64, Leb128, IndexedDeltaWeightFn>;

/// The deltas `i64::MAX` twice (a run whose sum overflows), then 3.
fn bytes() -> Vec<u8> {
    Column::<i64>::from_values(vec![i64::MAX, i64::MAX, 3]).save()
}

#[test]
fn a_load_goes_no_further_after_an_error() {
    let bytes = bytes();
    let mut it = DeltaInner::load_iter(&bytes, LoadOpts::new());
    assert!(it.try_next_run().is_err());
    assert!(matches!(it.try_next_run(), Err(PackError::BadFormat)));
    assert!(it.finalize().is_err());
}

#[test]
fn finalize_after_an_error_fails() {
    let bytes = bytes();
    let mut it = DeltaInner::load_iter(&bytes, LoadOpts::new());
    assert!(it.try_next_run().is_err());
    assert!(it.finalize().is_err());
}

#[test]
fn a_load_without_errors_is_unchanged() {
    let bytes = Column::<i64>::from_values(vec![5, 5, 3]).save();
    let mut it = DeltaInner::load_iter(&bytes, LoadOpts::new());
    while it.try_next_run().unwrap().is_some() {}
    assert_eq!(it.finalize().unwrap().to_vec(), vec![5, 5, 3]);
}

/// Two runs of the same value side by side are not canonical. Going on
/// past that error used to give a column whose bytes held both runs but
/// whose length counted only the first.
#[test]
fn a_rejected_run_does_not_reach_the_column() {
    let one = Column::<u64>::from_values(vec![5, 5]).save();
    let bytes = [one.as_slice(), one.as_slice()].concat();
    let mut it = Column::<u64>::load_iter(&bytes, LoadOpts::new());
    assert!(it.try_next_run().unwrap().is_some());
    assert!(it.try_next_run().is_err());
    assert!(matches!(it.try_next_run(), Err(PackError::BadFormat)));
    assert!(it.finalize().is_err());
}
