//! `IterState::try_resume` rejects a state for a column that was edited
//! since (its edit counter), but took the rest on trust: a state from
//! another column with the same counter and slab count resumed at a
//! position that didn't fit it (debug builds panicked; release builds gave
//! an iterator past the column's end), and a resumed prefix or delta
//! iterator kept the other column's running sum. Now a position that
//! doesn't fit is an error, and the sums are the column's own.

use hexane::{Column, DeltaColumn, PackError, PrefixColumn};

fn loaded<T: hexane::ColumnValue>(vals: Vec<T>) -> Column<T> {
    // loaded columns start at the same edit counter
    Column::<T>::load(&Column::<T>::from_values(vals).save()).unwrap()
}

#[test]
fn a_position_past_the_column_is_rejected() {
    let a = loaded((0..10u64).collect());
    let b = loaded(vec![100u64, 200, 300]);
    let mut it = a.iter();
    it.nth(7);
    let st = it.suspend();
    assert!(matches!(st.try_resume(&b), Err(PackError::InvalidResume)));
}

#[test]
fn a_position_that_fits_resumes() {
    let a = loaded((0..10u64).collect());
    let b = loaded((100..110u64).collect());
    let mut it = a.iter();
    it.nth(7);
    let mut it = it.suspend().try_resume(&b).unwrap();
    assert_eq!(it.pos(), 8);
    assert_eq!(it.next(), Some(108));
}

#[test]
fn a_resumed_prefix_iterator_sums_its_own_column() {
    let a = PrefixColumn::<u64>::load(&Column::<u64>::from_values(vec![1; 10]).save()).unwrap();
    let b = PrefixColumn::<u64>::load(&Column::<u64>::from_values(vec![5; 10]).save()).unwrap();
    let mut it = a.iter();
    it.nth(2);
    let mut it = it.suspend().try_resume(&b).unwrap();
    assert_eq!(it.total(), 15);
    assert_eq!(it.next().map(|v| v.total()), Some(20));
    // and on the same column, as before
    let mut it = a.iter();
    it.nth(2);
    let mut it = it.suspend().try_resume(&a).unwrap();
    assert_eq!(it.next().map(|v| v.total()), Some(4));
}

/// A prefix iterator reset before it was suspended counts its running
/// total, resumed on another column, from the same position there.
#[test]
fn a_reset_prefix_iterator_resumes_on_another_column() {
    let a = PrefixColumn::<u64>::load(&Column::<u64>::from_values(vec![5; 10]).save()).unwrap();
    let b = PrefixColumn::<u64>::load(&Column::<u64>::from_values(vec![1; 10]).save()).unwrap();
    let mut it = a.iter();
    it.nth(2);
    it.reset_prefix();
    it.nth(1);
    assert_eq!(it.total(), 10);
    let mut it = it.suspend().try_resume(&b).unwrap();
    assert_eq!(it.total(), 2);
    assert_eq!(it.next().map(|v| v.total()), Some(3));
    // and on the same column, as before
    let mut it = a.iter();
    it.nth(2);
    it.reset_prefix();
    it.nth(1);
    let mut it = it.suspend().try_resume(&a).unwrap();
    assert_eq!(it.total(), 10);
    assert_eq!(it.next().map(|v| v.total()), Some(15));
}

#[test]
fn a_resumed_delta_iterator_reads_its_own_column() {
    let save = |vals: Vec<i64>| DeltaColumn::<i64>::from_values(vals).save();
    let a = DeltaColumn::<i64>::load(&save((1..=10).collect())).unwrap();
    let b = DeltaColumn::<i64>::load(&save((100..110).collect())).unwrap();
    let mut it = a.iter();
    it.nth(2);
    let mut it = it.suspend().try_resume(&b).unwrap();
    assert_eq!(it.next(), Some(103));
    let mut it = a.iter();
    it.nth(2);
    let mut it = it.suspend().try_resume(&a).unwrap();
    assert_eq!(it.next(), Some(4));
}
