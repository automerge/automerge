//! A load of empty data with a length and a fill reads as `length` copies
//! of the fill. It skipped the checks a load makes of a slab's weight, so
//! a delta column filled with deltas whose sums leave `i64` panicked, or
//! held values outside its type's domain, and a prefix column filled with
//! values whose sum overflows its prefix wrapped it. Such fills are now
//! errors, as the same values in loaded data are.

use hexane::{DeltaColumn, LoadOpts, PrefixColumn};

#[test]
fn a_fill_whose_sums_leave_i64_is_rejected() {
    // realizes `i64::MAX`, then twice that
    let opts = LoadOpts::new().with_length(2).with_fill(i64::MAX);
    assert!(DeltaColumn::<i64>::load_with(&[], opts).is_err());
}

#[test]
fn a_fill_outside_the_domain_is_rejected() {
    // realizes -1, -2, -3: not `u64`s
    let opts = LoadOpts::new().with_length(3).with_fill(-1);
    assert!(DeltaColumn::<u64>::load_with(&[], opts).is_err());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn a_fill_overflowing_the_prefix_is_rejected() {
    let opts = LoadOpts::new().with_length(1 << 40).with_fill(u32::MAX);
    assert!(PrefixColumn::<u32>::load_with(&[], opts).is_err());
}

#[test]
fn fills_that_fit_still_load() {
    let opts = LoadOpts::new().with_length(3).with_fill(2);
    let col = DeltaColumn::<u64>::load_with(&[], opts).unwrap();
    assert_eq!(col.to_vec(), vec![2, 4, 6]);
    let opts = LoadOpts::new().with_length(3).with_fill(u32::MAX);
    let col = PrefixColumn::<u32>::load_with(&[], opts).unwrap();
    assert_eq!(col.get_total(2), 3 * u32::MAX as u64);
}
