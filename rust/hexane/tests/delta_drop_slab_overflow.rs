//! A delete that drops a whole slab of a delta column makes its
//! neighbours adjacent in the index before the delete writes the delta
//! that joins them. For that moment the index merges the two slabs' sums
//! without the dropped one between them, which can exceed `i64` although
//! every value in the column is in range. A debug build panicked there.

use hexane::{DeltaColumn, LoadOpts};

/// `vals` with a slab per value, then `vals` without item `idx`.
fn remove_with_a_slab_each(vals: Vec<i64>, idx: usize) {
    let bytes = DeltaColumn::<i64>::from_values(vals.clone()).save();
    let mut col =
        DeltaColumn::<i64>::load_with(&bytes, LoadOpts::new().with_max_segments(2)).unwrap();
    assert_eq!(col.slab_count(), vals.len());
    col.remove(idx);
    let mut want = vals;
    want.remove(idx);
    assert_eq!(col.to_vec(), want);
}

/// The values span exactly `i64::MAX`, the widest the domain allows.
#[test]
fn removing_a_slab_between_two_rises_of_i64_max() {
    let m = i64::MAX;
    remove_with_a_slab_each(vec![0, m, 0, m], 2);
    remove_with_a_slab_each(vec![0, m, 0, m, 0, m], 2);
    remove_with_a_slab_each(vec![0, m, 0, m, 0, m, 0, m], 4);
}

/// Falls as well as rises.
#[test]
fn removing_a_slab_between_two_falls() {
    let m = i64::MAX;
    remove_with_a_slab_each(vec![m, 0, m, 0], 2);
    remove_with_a_slab_each(vec![-1, i64::MIN, -1, i64::MIN], 2);
}
