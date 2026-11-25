//! A load of empty data with a fill takes its length from the caller. A
//! column holds at most `usize::MAX - 3` items, so a longer fill is a load
//! error, never a panic.
//!
//! Where a `usize` has 64 bits, a run's limit of `i64::MAX` items comes
//! first (`load_fill_length_over_i64_max.rs`). Where it has 32, the column's
//! limit is the only one, and the three lengths past it used to panic when
//! the column was made of the fill's slab.
//!
//! No `catch_unwind` here: wasm32-wasip1 aborts on a panic, so a load that
//! panics there ends the test binary.

use hexane::{Column, DeltaColumn, LoadOpts, PackError, PrefixColumn};

/// The longest column.
const LONGEST: usize = usize::MAX - 3;

fn rejects(name: &str, n: usize, r: Result<usize, PackError>) {
    match r {
        Ok(len) => panic!("{name}: load accepted a column of {len} items"),
        Err(PackError::InvalidLength(0, m)) => assert_eq!(m, n, "{name}"),
        Err(e) => panic!("{name}: unexpected error {e}"),
    }
}

#[test]
fn a_fill_longer_than_a_column_is_an_error() {
    for n in [LONGEST + 1, LONGEST + 2, usize::MAX] {
        let opts = || LoadOpts::new().with_length(n);
        rejects(
            "Column<Option<u64>>",
            n,
            Column::<Option<u64>>::load_with(&[], opts().with_fill(None)).map(|c| c.len()),
        );
        rejects(
            "Column<u64>",
            n,
            Column::<u64>::load_with(&[], opts().with_fill(7u64)).map(|c| c.len()),
        );
        rejects(
            "Column<bool>",
            n,
            Column::<bool>::load_with(&[], opts().with_fill(true)).map(|c| c.len()),
        );
        rejects(
            "PrefixColumn<bool>",
            n,
            PrefixColumn::<bool>::load_with(&[], opts().with_fill(false)).map(|c| c.len()),
        );
        rejects(
            "DeltaColumn<i64>",
            n,
            DeltaColumn::<i64>::load_with(&[], opts().with_fill(0i64)).map(|c| c.len()),
        );
    }
}

/// The longest column still loads as a fill, where one run can hold it.
#[cfg(target_pointer_width = "32")]
#[test]
fn a_fill_of_the_longest_column_loads() {
    let opts = || LoadOpts::new().with_length(LONGEST);

    let col = Column::<Option<u64>>::load_with(&[], opts().with_fill(None)).unwrap();
    assert_eq!(col.len(), LONGEST);
    assert_eq!(col.get(LONGEST - 1), Some(None));
    assert_eq!(col.get(LONGEST), None);

    let col = Column::<u64>::load_with(&[], opts().with_fill(7u64)).unwrap();
    assert_eq!(col.len(), LONGEST);
    assert_eq!(col.get(LONGEST - 1), Some(7));

    let col = Column::<bool>::load_with(&[], opts().with_fill(true)).unwrap();
    assert_eq!(col.len(), LONGEST);
    assert_eq!(col.get(LONGEST - 1), Some(true));

    let col = PrefixColumn::<bool>::load_with(&[], opts().with_fill(false)).unwrap();
    assert_eq!(col.len(), LONGEST);

    let col = DeltaColumn::<i64>::load_with(&[], opts().with_fill(0i64)).unwrap();
    assert_eq!(col.len(), LONGEST);
}
