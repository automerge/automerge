//! `DeltaColumn::scope_to_value` with a range running past the end of
//! the column.
//!
//! It promises `Column::scope_to_value`'s contract, which clamps the
//! range to the column first, so the insertion point for a value not
//! present is never past the last row. The delta version passed the
//! range through unclamped and answered with positions past the end —
//! `400..400` on a 200-row column — which no insert accepts.

use hexane::{Column, DeltaColumn};

fn columns() -> (Column<u64>, DeltaColumn<i64>) {
    (
        Column::<u64>::from_values((0..200).collect()),
        DeltaColumn::<i64>::from_values((0..200).collect()),
    )
}

#[test]
fn above_everything() {
    let (col, delta) = columns();
    assert_eq!(col.scope_to_value(1000u64, 150..400), 200..200);
    assert_eq!(delta.scope_to_value(1000, 150..400), 200..200);
}

#[test]
fn range_wholly_past_the_end() {
    let (col, delta) = columns();
    assert_eq!(col.scope_to_value(0u64, 250..300), 200..200);
    assert_eq!(delta.scope_to_value(0, 250..300), 200..200);
}

#[test]
fn present_value() {
    let (col, delta) = columns();
    assert_eq!(col.scope_to_value(150u64, 0..300), 150..151);
    assert_eq!(delta.scope_to_value(150, 0..300), 150..151);
}

/// Inserting at the answer keeps the column sorted.
#[test]
fn insertion_point_is_usable() {
    let (_, mut delta) = columns();
    let at = delta.scope_to_value(1000, 0..300);
    delta.insert(at.start, 1000);
    assert_eq!(delta.get(200), Some(1000));
}
