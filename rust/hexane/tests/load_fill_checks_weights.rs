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

#[test]
fn fills_that_fit_still_load() {
    let opts = LoadOpts::new().with_length(3).with_fill(2);
    let col = DeltaColumn::<u64>::load_with(&[], opts).unwrap();
    assert_eq!(col.to_vec(), vec![2, 4, 6]);
    let opts = LoadOpts::new().with_length(3).with_fill(u32::MAX);
    let col = PrefixColumn::<u32>::load_with(&[], opts).unwrap();
    assert_eq!(col.get_total(2), 3 * u32::MAX as u64);
}

/// A fill's length comes from the caller, who may have read it from
/// untrusted data: a prefix column filled with values whose sums would
/// not fit the type it keeps them in is an error, not a panic, whether
/// it is loaded whole or run by run.
#[cfg(target_pointer_width = "64")]
#[test]
fn a_fill_overflowing_the_prefix_is_rejected() {
    let opts = || LoadOpts::new().with_length(1 << 40).with_fill(u32::MAX);
    let r =
        std::panic::catch_unwind(|| PrefixColumn::<u32>::load_with(&[], opts()).map(|c| c.len()));
    match r {
        Err(_) => panic!("load panicked on a fill whose sums overflow"),
        Ok(Ok(len)) => panic!("load accepted a fill whose sums overflow: len = {len}"),
        Ok(Err(_)) => {}
    }
    let r = std::panic::catch_unwind(|| {
        PrefixColumn::<u32>::load_iter(&[], opts())
            .finalize()
            .map(|c| c.len())
    });
    assert!(
        matches!(r, Ok(Err(_))),
        "a load run by run takes the fill as a load does"
    );
    // the same values saved (one run, a few bytes) are refused with the
    // same error
    let bytes = hexane::Column::<u32>::fill(1 << 40, u32::MAX).save();
    assert_eq!(
        format!("{:?}", PrefixColumn::<u32>::load(&bytes).unwrap_err()),
        format!(
            "{:?}",
            PrefixColumn::<u32>::load_with(&[], opts()).unwrap_err()
        )
    );
}

#[test]
fn a_fill_that_just_fits_the_prefix_loads() {
    // 2^32 + 1 `u32::MAX`es is the most whose sums fit a `u64`
    #[cfg(target_pointer_width = "64")]
    {
        let n = (1usize << 32) + 1;
        let col =
            PrefixColumn::<u32>::load_with(&[], LoadOpts::new().with_length(n).with_fill(u32::MAX))
                .unwrap();
        assert_eq!(col.len(), n);
        assert_eq!(col.get_total(n - 1), u64::MAX);
        let over = PrefixColumn::<u32>::load_with(
            &[],
            LoadOpts::new().with_length(n + 1).with_fill(u32::MAX),
        );
        assert!(over.is_err());
    }
}
