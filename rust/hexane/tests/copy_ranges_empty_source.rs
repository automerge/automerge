//! `copy_ranges` takes splices that delete and copy a source range in.
//! A splice whose source range is empty (or clamped to empty, past the
//! source's end) only deletes, and the normaliser keeps such splices. But
//! the cost estimate that picks a copy strategy took a slab for the
//! range's last item, `range.end - 1`, which an empty range doesn't have:
//! with a source of 8 or more slabs it panicked (subtracting with
//! overflow, or indexing past the slabs).

use hexane::{Column, DeltaColumn, Splice};

/// A source of 64 items in 32 slabs: enough slabs for the estimate to run.
fn source() -> Column<u64> {
    Column::from_values_with_max_segments((0..64).collect(), 2)
}

fn dest() -> Column<u64> {
    Column::from_values_with_max_segments((100..110).collect(), 2)
}

#[test]
fn an_empty_range_at_the_start_only_deletes() {
    let mut dst = dest();
    dst.copy_ranges(
        source(),
        [Splice {
            pos: 0,
            delete: 1,
            range: 0..0,
        }],
    );
    assert_eq!(dst.to_vec(), (101..110).collect::<Vec<u64>>());
}

#[test]
fn a_range_past_the_source_only_deletes() {
    let mut dst = dest();
    dst.copy_ranges(
        source(),
        [Splice {
            pos: 2,
            delete: 3,
            range: 100..200,
        }],
    );
    assert_eq!(dst.to_vec(), vec![100, 101, 105, 106, 107, 108, 109]);
}

#[test]
fn an_empty_range_at_a_slab_boundary_only_deletes() {
    let mut dst = dest();
    dst.copy_ranges(
        source(),
        [Splice {
            pos: 3,
            delete: 2,
            range: 10..10,
        }],
    );
    assert_eq!(dst.to_vec(), vec![100, 101, 102, 105, 106, 107, 108, 109]);
}

#[test]
fn empty_ranges_beside_copies() {
    let mut dst = dest();
    dst.copy_ranges(
        source(),
        [
            Splice {
                pos: 1,
                delete: 1,
                range: 5..5,
            },
            Splice {
                pos: 4,
                delete: 0,
                range: 20..23,
            },
        ],
    );
    assert_eq!(
        dst.to_vec(),
        vec![100, 102, 103, 20, 21, 22, 104, 105, 106, 107, 108, 109]
    );
}

#[test]
fn a_delta_column_too() {
    let src = DeltaColumn::<u64>::from_values((0..64).collect());
    let mut src2 = DeltaColumn::<u64>::with_max_segments(2);
    src2.splice(0, 0, src.iter());
    let mut dst = DeltaColumn::<u64>::with_max_segments(2);
    dst.splice(0, 0, 100..110);
    dst.copy_ranges(
        src2,
        [Splice {
            pos: 0,
            delete: 1,
            range: 0..0,
        }],
    );
    assert_eq!(dst.to_vec(), (101..110).collect::<Vec<u64>>());
}
