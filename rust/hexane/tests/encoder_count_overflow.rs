//! An encoder counts the values appended in a `usize`, and runs make
//! appending many cheap. Past `usize::MAX` values the count wrapped (in
//! release builds): `len()` was wrong, and so was the length of each slab
//! cut. On a 32-bit target a run's own count wrapped as well, writing a
//! run of the wrong length. No column can hold that many values, so an
//! encoder now panics instead.

use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::Leb128;

/// Appends that bring the count to `usize::MAX - 1`; each run fits an
/// `i64` on any target.
const HALF: usize = usize::MAX / 2;

#[test]
#[should_panic(expected = "encoder too long to append to")]
fn an_rle_encoder_past_usize_max_values_panics() {
    let mut e = RleEncoder::<u64>::new();
    e.append_n(1, HALF);
    e.append_n(2, HALF);
    e.append_n(3, 2);
}

#[test]
#[should_panic(expected = "encoder too long to append to")]
fn an_rle_encoder_past_usize_max_values_panics_one_at_a_time() {
    let mut e = RleEncoder::<u64>::new();
    e.append_n(1, HALF);
    e.append_n(2, HALF);
    e.append(3);
    e.append(4);
}

#[test]
#[should_panic(expected = "encoder too long to append to")]
fn a_bool_encoder_past_usize_max_values_panics() {
    let mut e = BoolEncoder::<Leb128>::new();
    e.append_n(true, HALF);
    e.append_n(false, HALF);
    e.append_n(true, 2);
}

/// Nearly `usize::MAX` values are still counted.
#[test]
fn an_encoder_of_nearly_usize_max_values_counts_them() {
    let mut e = RleEncoder::<u64>::new();
    e.append_n(1, HALF - 1);
    e.append_n(2, HALF - 1);
    assert_eq!(e.len(), usize::MAX - 3);
    let mut b = BoolEncoder::<Leb128>::new();
    b.append_n(true, HALF - 1);
    b.append_n(false, HALF - 1);
    assert_eq!(b.len(), usize::MAX - 3);
}

/// On a 32-bit target, a run of more than `usize::MAX` equal values.
#[cfg(target_pointer_width = "32")]
#[test]
#[should_panic(expected = "encoder too long to append to")]
fn a_run_past_usize_max_values_panics() {
    let mut e = RleEncoder::<u64>::new();
    e.append_n(5, 3_000_000_000);
    e.append_n(5, 3_000_000_000);
}
