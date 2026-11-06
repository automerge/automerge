//! A column's edits and `save` count its items with room for three more,
//! so a column holds at most `usize::MAX - 3` of them. A few bytes of runs
//! can claim more; loading them must fail, rather than give a column that
//! `save` and every edit panic on.

#![cfg(target_pointer_width = "64")]

use hexane::{Codec, Column, Leb128, PackError, PrefixColumn};

fn sleb(mut v: i64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if (v == 0 && b & 0x40 == 0) || (v == -1 && b & 0x40 != 0) {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// A literal 0, then `a` copies of -2 and `b` copies of 1: `1 + a + b`
/// items.
fn column_bytes(a: i64, b: i64) -> Vec<u8> {
    let mut out = vec![0x7f, 0x00];
    sleb(a, &mut out);
    sleb(-2, &mut out);
    sleb(b, &mut out);
    sleb(1, &mut out);
    out
}

#[test]
fn a_column_too_long_to_save_fails_to_load() {
    // `usize::MAX - 2` items, and `usize::MAX - 1`
    for b in [i64::MAX - 1, i64::MAX] {
        let data = column_bytes(i64::MAX - 1, b);
        assert!(matches!(
            Column::<i64>::load(&data),
            Err(PackError::InvalidLength(_, _))
        ));
        assert!(PrefixColumn::<i64>::load(&data).is_err());
    }
    // and a bool column's runs, `usize::MAX - 2` items
    let mut data = vec![];
    for n in [i64::MAX as u64, i64::MAX as u64 - 1] {
        data.extend_from_slice(Leb128::encode_unsigned(n).as_bytes());
    }
    assert!(Column::<bool>::load(&data).is_err());
}

#[test]
fn a_column_at_the_limit_loads_and_saves() {
    let data = column_bytes(i64::MAX - 1, i64::MAX - 2);
    let col = Column::<i64>::load(&data).unwrap();
    assert_eq!(col.len(), usize::MAX - 3);
    assert_eq!(col.save(), data);
    let col = PrefixColumn::<i64>::load(&data).unwrap();
    assert_eq!(col.save(), data);
}
