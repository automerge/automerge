//! `Column::remap` builds a new column from the function's values, and a
//! column with a prefix weight function checks that its sums fit its
//! prefix type, as a load or an edit does: a remap that would overflow
//! them panics rather than leave wrapped sums.
//!
//! (The column is longer than a 32-bit `usize` counts.)
#![cfg(target_pointer_width = "64")]

use hexane::prefix::PrefixWeightFn;
use hexane::{Column, Leb128, Run};

type Col = Column<u32, Leb128, PrefixWeightFn<u32>>;

fn run(value: u32, count: usize) -> Run<u32> {
    Run { count, value }
}

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_remap_overflowing_the_prefix_in_one_slab() {
    // 2^33 ones sum to 2^33; as u32::MAX they would sum past u64::MAX
    let mut col = Col::fill(1 << 33, 1);
    col.remap(|_| u32::MAX);
}

// A segment budget of 2 cuts the remapped column into a slab per run, so
// each slab's sum fits while their total doesn't.

#[test]
#[should_panic(expected = "prefix sum overflows")]
fn a_remap_overflowing_the_prefix_across_slabs() {
    let mut col = Col::with_max_segments(2);
    col.splice_runs(0, 0, [run(1, 1 << 32), run(0, 1), run(1, 1 << 32)]);
    col.remap(|x| if x == 0 { 0 } else { u32::MAX });
}

#[test]
fn a_remap_whose_sums_fit() {
    let mut col = Col::fill(1 << 33, 1);
    col.remap(|x| x + 1);
    assert_eq!(col.len(), 1 << 33);
    assert_eq!(col.iter().next(), Some(2));
}
