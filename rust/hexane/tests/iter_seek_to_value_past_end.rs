//! `Iter::seek_to_value` with a window that runs past the end of the
//! column, and a target greater than every value in it.
//!
//! The scan binary-searches slabs by their first value, up to the slab
//! holding the window's last row. Past the end, the index answers with
//! the slab count — one past the last slab — and the search read that
//! slab, panicking with an index out of bounds.

use hexane::Column;

/// A sorted column over many slabs.
fn sorted() -> Column<u64> {
    Column::<u64>::from_values_with_max_segments((0..200).collect(), 4)
}

#[test]
fn range_past_the_end() {
    let col = sorted();
    assert!(col.slab_count() > 1);
    let mut it = col.iter();
    // the insertion point for a value above everything is the end
    assert_eq!(it.seek_to_value(1000u64, 0..300), 200..200);
    // the same as `Column::scope_to_value`, which clamps the range first
    assert_eq!(col.scope_to_value(1000u64, 0..300), 200..200);
}

#[test]
fn window_extended_past_the_end() {
    let col = sorted();
    let mut it = col.iter();
    it.set_max(500);
    assert_eq!(it.seek_to_value(1000u64, ..), 200..200);
}

#[test]
fn present_value_with_range_past_the_end() {
    let col = sorted();
    let mut it = col.iter();
    assert_eq!(it.seek_to_value(150u64, 0..300), 150..151);
}
