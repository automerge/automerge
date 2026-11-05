//! A delta column's values must lie within a 2^63-wide range (the
//! `DeltaValue` domain contract): `load` rejects data that doesn't, and a
//! delete between values further apart can't store their difference. But
//! edits only checked each value against the type's own bounds, which for
//! `i64` are all of `i64`, so inserts could build a column `load` then
//! rejected, and whose later deletes panicked ("delta value overflows
//! i64"). Writes outside the domain now panic, as the contract says.

use hexane::{DeltaColumn, DeltaRun, LoadOpts};

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn inserts_that_widen_the_span() {
    let mut col = DeltaColumn::<i64>::from_values(vec![0, 1, 2]);
    col.insert(3, i64::MAX);
    col.insert(0, i64::MIN + 1);
}

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn from_values_that_span_too_much() {
    DeltaColumn::<i64>::from_values(vec![i64::MIN + 1, 0, 1, i64::MAX]);
}

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn a_push_one_past_the_widest_span() {
    let mut col = DeltaColumn::<i64>::from_values(vec![0, i64::MAX]);
    col.push(-1);
}

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn a_cursor_insert() {
    let mut col = DeltaColumn::<Option<i64>>::from_values(vec![Some(5), None, Some(i64::MAX)]);
    let mut e = col.edit();
    e.insert(Some(-2));
    e.finish();
}

#[test]
#[should_panic(expected = "span 2^63 or more")]
fn a_run_that_widens_it() {
    // -5, then 2^62 - 5 and 2^63 - 4: every delta and sum fits, the span
    // doesn't (two equal deltas would make a run whose sum the slab's
    // aggregate already refuses)
    let mut col = DeltaColumn::<i64>::from_values(vec![-5]);
    let runs = [
        DeltaRun {
            prefix: -5,
            delta: Some(1 << 62),
            count: 1,
        },
        DeltaRun {
            prefix: (1 << 62) - 5,
            delta: Some((1 << 62) + 1),
            count: 1,
        },
    ];
    col.splice_runs(1, 0, runs);
}

#[test]
fn the_widest_span_is_still_allowed() {
    let mut col = DeltaColumn::<i64>::from_values(vec![0, 7, i64::MAX]);
    col.insert(1, 3);
    col.remove(2);
    assert_eq!(col.to_vec(), vec![0, 3, i64::MAX]);
    let bytes = col.save();
    assert!(DeltaColumn::<i64>::load(&bytes).is_ok());
    // a leading null holds a 0 the column doesn't: it doesn't widen it
    let mut col = DeltaColumn::<Option<i64>>::from_values(vec![None, Some(i64::MIN), Some(-1)]);
    col.insert(3, Some(-5));
    let bytes = col.save();
    assert!(DeltaColumn::<Option<i64>>::load_with(&bytes, LoadOpts::new()).is_ok());
}

// The index merges its slabs' aggregates with wrapping arithmetic, so
// after an edit its root can't bound the values. Here the slabs hold
// `MAX - 5` and `MAX - 5, MIN + 4`: the step between them wraps to +10,
// and the root's least and greatest running sums both come out as
// `MAX - 5`. (A debug build panics before that, on the subtraction.)

#[test]
#[should_panic]
fn a_span_the_index_wraps_around() {
    let mut col = DeltaColumn::<i64>::with_max_segments(2);
    col.splice(0, 0, [i64::MAX - 5]);
    col.splice(1, 0, [i64::MAX - 5, i64::MIN + 4]);
}

#[test]
#[should_panic]
fn a_cursor_span_the_index_wraps_around() {
    let mut col = DeltaColumn::<i64>::with_max_segments(2);
    col.splice(0, 0, [i64::MAX - 5]);
    let mut e = col.edit();
    e.seek(1);
    e.insert(i64::MAX - 5);
    e.insert(i64::MIN + 4);
    e.finish();
}

#[test]
fn values_near_both_ends_of_i64_that_span_less_than_2_63() {
    let mut col = DeltaColumn::<i64>::with_max_segments(2);
    col.splice(0, 0, [i64::MAX - 5]);
    col.splice(1, 0, [i64::MAX - 5, 1]);
    {
        let mut e = col.edit();
        e.seek(3);
        e.insert(2);
        e.finish();
    }
    assert_eq!(col.to_vec(), vec![i64::MAX - 5, i64::MAX - 5, 1, 2]);
    assert!(DeltaColumn::<i64>::load(&col.save()).is_ok());
}
