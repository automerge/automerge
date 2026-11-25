//! A column's slabs need a budget of at least two segments, and
//! `Column::with_max_segments` panicked on less ("max_segments must be at
//! least 2"). The encoders' `with_max_segments` took 0 or 1, and the
//! encoder then cut slabs the column can't hold: `into_column` failed an
//! internal assertion (`s.segments <= max_segments`) after all the values
//! were in. The fix had the encoders reject the budget when it was set.
//!
//! `Encoder::with_max_segments` now requires a budget of at least 2, as
//! `Column::with_max_segments` does, and promises nothing of less.
//! Checked: the least budget it takes still builds a column.

use hexane::{Column, Encoder};

#[test]
fn two_segments_still_work() {
    let mut e = Encoder::<u64>::with_max_segments(2);
    for i in 0..20u64 {
        e.append(i % 3);
    }
    let col: Column<u64> = e.into_column();
    assert_eq!(col.to_vec(), (0..20u64).map(|i| i % 3).collect::<Vec<_>>());
    let mut e = Encoder::<bool>::with_max_segments(2);
    for i in 0..20u64 {
        e.append(i % 3 == 0);
    }
    let col: Column<bool> = e.into_column();
    assert_eq!(col.len(), 20);
    let mut e = Encoder::<Option<String>>::with_max_segments(2);
    for i in 0..20u64 {
        e.append_owned((i % 3 != 0).then(|| i.to_string()));
    }
    let col: Column<Option<String>> = e.into_column();
    assert_eq!(col.get(4), Some(Some("4")));
}
