//! `get_index_for_prefix` finds the item within a run by a ceiling
//! division, `(remaining + p - 1) / p`, whose sum overflowed when the
//! column's sums came near the prefix type's limit: in a `u32` column
//! summing to `u64::MAX`, release builds returned index 0 and debug builds
//! panicked.
//!
//! (The run here is longer than a 32-bit `usize` counts.)
#![cfg(target_pointer_width = "64")]

use hexane::{PrefixColumn, Run};

/// 2^32 + 1 copies of `u32::MAX`, which sum to exactly `u64::MAX`.
fn to_the_limit() -> PrefixColumn<u32> {
    let mut col = PrefixColumn::<u32>::new();
    let run = Run {
        count: (1 << 32) + 1,
        value: u32::MAX,
    };
    col.splice_runs(0, 0, [run]);
    col
}

#[test]
fn a_search_near_the_limit() {
    let col = to_the_limit();
    let n = (1 << 32) + 1;
    assert_eq!(col.get_index_for_prefix(u64::MAX), n);
    assert_eq!(col.get_index_for_prefix(u64::MAX - 5), n);
    assert_eq!(col.get_index_for_total(u64::MAX), n - 1);
}

#[test]
fn searches_further_from_it_are_unchanged() {
    let col = to_the_limit();
    let max = u32::MAX as u64;
    assert_eq!(col.get_index_for_prefix(1), 1);
    assert_eq!(col.get_index_for_prefix(7 * max), 7);
    assert_eq!(col.get_index_for_prefix(7 * max + 1), 8);
    let col = PrefixColumn::<u32>::from_values(vec![3, 3, 3]);
    assert_eq!(col.get_index_for_prefix(4), 2);
    assert_eq!(col.get_index_for_prefix(9), 3);
    assert_eq!(col.get_index_for_prefix(10), 4);
}
