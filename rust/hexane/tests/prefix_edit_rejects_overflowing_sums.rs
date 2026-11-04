//! A prefix column's sums are kept in `T::Prefix`, which for `u32` (and
//! `NonZeroU32`, and their `Option`s) is a `u64`. Loading rejects data
//! whose sums overflow it; edits and builds made such columns unchecked:
//! debug builds panicked partway through with an arithmetic overflow, and
//! release builds gave a column whose prefix sums had wrapped (and whose
//! saved bytes `load` rejects). They now panic, saying why.
//!
//! (The runs here are longer than a 32-bit `usize` counts.)
#![cfg(target_pointer_width = "64")]

use hexane::encoder::RleEncoder;
use hexane::prefix::PrefixWeightFn;
use hexane::{Column, EncoderApi, Leb128, PrefixColumn, Run};
use std::num::NonZeroU32;

fn run<T>(value: T, count: usize) -> Run<T> {
    Run { count, value }
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_run_overflowing_the_prefix() {
    let mut col = PrefixColumn::<u32>::new();
    col.splice_runs(0, 0, [run(u32::MAX, 1 << 33)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn runs_overflowing_one_slab_together() {
    let mut col = PrefixColumn::<u32>::new();
    col.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
    col.splice_runs(0, 0, [run(u32::MAX - 1, 1 << 32)]);
}

// Slabs of at most two runs, so that `[big, 0, big]` takes two slabs,
// whose sums each fit while their total doesn't.

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn slabs_overflowing_the_prefix_together() {
    let mut col = PrefixColumn::<u32>::with_max_segments(2);
    col.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
    col.splice_runs(1 << 32, 0, [run(0, 1), run(u32::MAX - 1, 1 << 32)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn slabs_overflowing_the_prefix_in_one_edit() {
    let mut col = PrefixColumn::<u32>::with_max_segments(2);
    let big = run(u32::MAX, 1 << 32);
    col.splice_runs(0, 0, [big, run(0, 1), run(u32::MAX - 1, 1 << 32)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_slab_growing_in_place_past_the_rest() {
    let mut col = PrefixColumn::<u32>::with_max_segments(2);
    let big = run(u32::MAX, 1 << 32);
    col.splice_runs(0, 0, [big, run(0, 1), run(u32::MAX - 1, 1)]);
    // lengthens the last run, which keeps its slab
    col.splice_runs(col.len(), 0, [run(u32::MAX - 1, 1 << 32)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_cursor_insert_overflowing_the_prefix() {
    let mut col = PrefixColumn::<u32>::with_max_segments(2);
    col.splice_runs(0, 0, [run(u32::MAX, 1 << 32), run(0, 1)]);
    let mut e = col.edit();
    e.seek((1 << 32) + 1);
    e.insert_run(u32::MAX - 1, 1 << 32);
    e.finish();
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_nullable_column_too() {
    let mut col = PrefixColumn::<Option<u32>>::with_max_segments(2);
    col.splice_runs(0, 0, [run(Some(u32::MAX), 1 << 32)]);
    col.splice_runs(0, 0, [run(Some(u32::MAX - 1), 1 << 32), run(None, 5)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_nonzero_column_too() {
    let mut col = PrefixColumn::<NonZeroU32>::new();
    col.splice_runs(0, 0, [run(NonZeroU32::MAX, 1 << 33)]);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_filled_column() {
    Column::<u32, Leb128, PrefixWeightFn<u32>>::fill(1 << 33, u32::MAX);
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn an_encoded_column() {
    let mut e = RleEncoder::<u32>::new().with_max_segments(2);
    e.append_n(u32::MAX, 1 << 32);
    e.append(0);
    e.append_n(u32::MAX - 1, 1 << 32);
    let _: Column<u32, Leb128, PrefixWeightFn<u32>> = e.into_column();
}

#[test]
fn sums_that_fit_still_edit() {
    // u32::MAX * (2^32 + 1) == u64::MAX
    let mut col = PrefixColumn::<u32>::with_max_segments(2);
    col.splice_runs(0, 0, [run(u32::MAX, 1 << 32)]);
    col.splice_runs(0, 0, [run(0, 1 << 40)]);
    // three runs: two slabs
    col.insert(0, u32::MAX);
    assert_eq!(col.get_total(col.len() - 1), u64::MAX);
    // and room made by a deletion can be filled again
    col.remove(0);
    col.push(u32::MAX);
    assert_eq!(col.get_total(col.len() - 1), u64::MAX);
    let saved = PrefixColumn::<u32>::load(&col.save()).unwrap();
    assert_eq!(saved.get_total(saved.len() - 1), u64::MAX);
}
