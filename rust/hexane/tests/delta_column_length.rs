//! A delta column holds at most `i64::MAX` items: the steps between its
//! running sums, counts included, must fit `i64` (its readers count a
//! slab's items in `i64`). Loads reject longer data, and edits that would
//! make one panic.

// a column of more than i64::MAX items needs a 64-bit `usize`
#![cfg(target_pointer_width = "64")]

use hexane::{Column, DeltaColumn, DeltaRun, PackError};

/// The bytes of a delta column of three runs of 2^62 zeros (deltas), with
/// a 1 and a -1 between them: values 0 and 1, more than `i64::MAX` items.
fn long_bytes() -> Vec<u8> {
    let mut col = Column::<i64>::new();
    {
        let mut e = col.edit();
        e.insert_run(0i64, 1 << 62);
        e.insert(1i64);
        e.insert_run(0i64, 1 << 62);
        e.insert(-1i64);
        e.insert_run(0i64, 1 << 62);
    }
    col.save()
}

#[test]
fn load_rejects_more_than_i64_max_items() {
    let bytes = long_bytes();
    match DeltaColumn::<i64>::load(&bytes) {
        Err(PackError::InvalidLength(len, max)) => {
            assert_eq!(len, 3 * (1 << 62) + 2);
            assert_eq!(max, i64::MAX as usize);
        }
        Err(e) => panic!("wrong error: {e:?}"),
        Ok(c) => panic!("loaded a delta column of {} items", c.len()),
    }
}

#[test]
fn load_accepts_i64_max_items() {
    let mut col = Column::<i64>::new();
    col.edit().insert_run(0i64, i64::MAX as usize);
    let c = DeltaColumn::<i64>::load(&col.save()).unwrap();
    assert_eq!(c.len(), i64::MAX as usize);
}

#[test]
#[should_panic(expected = "more than i64::MAX items")]
fn splice_past_i64_max_items_panics() {
    let mut c = DeltaColumn::<i64>::new();
    let run = |prefix, delta, count| DeltaRun {
        prefix,
        delta: Some(delta),
        count,
    };
    c.splice_runs(0, 0, [run(0, 0, 1 << 62)]);
    // a 1, then 2^62 more of it: values 0 and 1, 2^63 + 1 items
    c.splice_runs(1 << 62, 0, [run(0, 1, 1), run(1, 0, 1 << 62)]);
}
