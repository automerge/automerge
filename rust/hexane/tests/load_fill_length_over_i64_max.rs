//! A load of empty data with a fill takes its length from the caller, and
//! callers pass lengths read from untrusted data: automerge loads its op
//! columns with the length of the id column it loaded first. A length no
//! fill can hold is a load error, never a panic.
#![cfg(target_pointer_width = "64")]

use hexane::{Codec, Column, DeltaColumn, Leb128, LoadOpts, PackError, PrefixColumn};

/// More items than one run holds.
const TOO_LONG: usize = i64::MAX as usize + 1;

fn rejects<C: std::fmt::Debug>(name: &str, load: impl FnOnce() -> Result<C, PackError>) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(load)) {
        Err(_) => panic!("{name}: load panicked"),
        Ok(Ok(c)) => panic!("{name}: load accepted {c:?}"),
        Ok(Err(PackError::InvalidLength(0, n))) => assert_eq!(n, TOO_LONG, "{name}"),
        Ok(Err(e)) => panic!("{name}: unexpected error {e}"),
    }
}

#[test]
fn a_fill_too_long_for_one_run_is_an_error() {
    let opts = || LoadOpts::new().with_length(TOO_LONG);
    rejects("Column<Option<u64>>", || {
        Column::<Option<u64>>::load_with(&[], opts().with_fill(None)).map(|c| c.len())
    });
    rejects("Column<u64>", || {
        Column::<u64>::load_with(&[], opts().with_fill(7u64)).map(|c| c.len())
    });
    rejects("Column<bool>", || {
        Column::<bool>::load_with(&[], opts().with_fill(false)).map(|c| c.len())
    });
    rejects("PrefixColumn<bool>", || {
        PrefixColumn::<bool>::load_with(&[], opts().with_fill(false)).map(|c| c.len())
    });
    rejects("DeltaColumn<i64>", || {
        DeltaColumn::<i64>::load_with(&[], opts().with_fill(0i64)).map(|c| c.len())
    });
}

/// The longest fill still loads.
#[test]
fn a_fill_of_i64_max_items_loads() {
    let len = i64::MAX as usize;
    let col =
        Column::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(len).with_fill(None))
            .unwrap();
    assert_eq!(col.len(), len);
    assert_eq!(col.get(len - 1), Some(None));
}

/// The way automerge meets it: a 22-byte column whose length is past
/// `i64::MAX` (here the longest a column loads, `usize::MAX - 3`), then a
/// filled load of a missing column with that length.
#[test]
fn a_filled_load_with_a_loaded_length() {
    let mut b = vec![];
    for (n, v) in [(i64::MAX, 1u8), (i64::MAX - 2, 2)] {
        b.extend_from_slice(Leb128::encode_signed(n).as_bytes());
        b.push(v);
    }
    let len = Column::<u64>::load(&b).unwrap().len();
    assert!(len > i64::MAX as usize);
    let r = std::panic::catch_unwind(|| {
        Column::<Option<u64>>::load_with(&[], LoadOpts::new().with_length(len).with_fill(None))
            .map(|c| c.len())
    });
    assert!(
        matches!(r, Ok(Err(PackError::InvalidLength(0, n))) if n == len),
        "{r:?}"
    );
}
