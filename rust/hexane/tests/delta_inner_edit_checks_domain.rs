//! A column with the delta weight function holds deltas whose running
//! sums must lie within a range narrower than 2^63, which its index and
//! the delta readers rely on. Its splices checked that, but the edit
//! cursor handed its parts back unchecked, so an edit could leave sums
//! that wrap. Ending an edit checks it now, as a splice does.

use hexane::delta::indexed::IndexedDeltaWeightFn;
use hexane::{Column, Leb128};

type DeltaInner = Column<i64, Leb128, IndexedDeltaWeightFn>;

#[test]
#[should_panic(expected = "delta values out of domain")]
fn an_edit_leaving_sums_out_of_domain_panics() {
    let mut col = DeltaInner::new();
    let mut e = col.edit();
    e.insert(i64::MAX).insert(i64::MAX);
}

#[test]
#[should_panic(expected = "delta values out of domain")]
fn a_splice_does_too() {
    let mut col = DeltaInner::new();
    col.splice(0, 0, [i64::MAX, i64::MAX]);
}

#[test]
fn an_edit_within_the_domain_ends_normally() {
    let mut col = DeltaInner::new();
    {
        let mut e = col.edit();
        e.insert(i64::MAX).insert(i64::MIN + 1);
    }
    assert_eq!(col.iter().collect::<Vec<_>>(), [i64::MAX, i64::MIN + 1]);
}
