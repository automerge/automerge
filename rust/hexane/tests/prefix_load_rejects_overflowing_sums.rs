//! A prefix column's sums are kept in `T::Prefix`, which for `u32` (and
//! `NonZeroU32`, and their `Option`s) is a `u64`. A run is cheap to
//! encode, so a few bytes can hold values whose sum overflows it: loading
//! them panicked in debug builds, and in release builds gave a column
//! whose prefix sums had wrapped. `load` now rejects them, as it does
//! other data a column can't hold.
//!
//! (The runs here are longer than a 32-bit `usize` counts.)
#![cfg(target_pointer_width = "64")]

use hexane::encoder::RleEncoder;
use hexane::prefix::PrefixWeightFn;
use hexane::{Column, Leb128, LoadOpts, PrefixColumn};
use std::num::NonZeroU32;

/// One run whose sum overflows `u64`: `u32::MAX` 2^40 times.
fn one_long_run() -> Vec<u8> {
    Column::<u32>::fill(1 << 40, u32::MAX).save()
}

/// Two runs whose sums each fit, but not together.
fn two_runs() -> Vec<u8> {
    let mut e = RleEncoder::<u32>::new();
    e.append_n(u32::MAX, 1 << 32);
    e.append_n(u32::MAX - 1, 1 << 32);
    e.save()
}

#[test]
fn a_run_overflowing_the_prefix_is_rejected() {
    assert!(PrefixColumn::<u32>::load(&one_long_run()).is_err());
}

#[test]
fn slabs_overflowing_the_prefix_together_are_rejected() {
    let bytes = two_runs();
    assert!(PrefixColumn::<u32>::load(&bytes).is_err());
    // one run per slab: each slab's sum fits, their total doesn't
    let opts = LoadOpts::new().with_max_segments(2);
    assert!(PrefixColumn::<u32>::load_with(&bytes, opts).is_err());
}

/// 128 runs of 2^26 values near `u32::MAX`: a default load cuts them
/// into two slabs of 64 runs, whose sums each fit (just under 2^64) but
/// not together.
fn many_runs() -> Vec<u8> {
    let mut e = RleEncoder::<u32>::new();
    for i in 0..128 {
        e.append_n(u32::MAX - (i % 2), 1 << 26);
    }
    e.save()
}

#[test]
fn every_load_checks_the_total() {
    let bytes = many_runs();
    assert!(PrefixColumn::<u32>::load(&bytes).is_err());
    assert!(PrefixColumn::<u32>::load_with(&bytes, LoadOpts::new()).is_err());
    assert!(Column::<u32, Leb128, PrefixWeightFn<u32>>::load(&bytes).is_err());
    let iter = PrefixColumn::<u32>::load_iter(&bytes, LoadOpts::new());
    assert!(iter.finalize().is_err());
}

#[test]
fn nullable_and_nonzero_columns_reject_them_too() {
    let mut e = RleEncoder::<Option<u32>>::new();
    e.append_n(Some(u32::MAX), 1 << 40);
    assert!(PrefixColumn::<Option<u32>>::load(&e.save()).is_err());
    let bytes = Column::<NonZeroU32>::fill(1 << 40, NonZeroU32::MAX).save();
    assert!(PrefixColumn::<NonZeroU32>::load(&bytes).is_err());
}

#[test]
fn sums_that_fit_still_load() {
    // u32::MAX * (2^32 + 1) == u64::MAX
    let mut e = RleEncoder::<u32>::new();
    e.append_n(u32::MAX, 1 << 32);
    e.append(u32::MAX);
    let col = PrefixColumn::<u32>::load(&e.save()).unwrap();
    assert_eq!(col.get_total(col.len() - 1), u64::MAX);
}
