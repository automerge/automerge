//! Run counts in untrusted bytes that do not fit a `usize` are load
//! errors, never panics or truncated counts.

use hexane::{Codec, Column, Leb128};

fn signed(n: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(Leb128::encode_signed(n).as_bytes());
}

fn unsigned(n: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(Leb128::encode_unsigned(n).as_bytes());
}

#[test]
fn literal_header_of_i64_min_is_an_error() {
    // `-n` overflowed for n = i64::MIN (a panic in debug builds)
    let mut b = vec![];
    signed(i64::MIN, &mut b);
    b.push(1);
    assert!(Column::<u64>::load(&b).is_err());
    assert!(Column::<Option<u64>>::load(&b).is_err());
}

// On 32-bit targets (wasm) `as usize` kept only the low bits of a
// count, so different bytes loaded as the same column.
#[cfg(target_pointer_width = "32")]
#[test]
fn counts_over_usize_are_errors_on_32_bit_targets() {
    let big = (1u64 << 32) + 2;
    let mut b = vec![];
    signed(big as i64, &mut b);
    b.push(1);
    assert!(Column::<u64>::load(&b).is_err());

    let mut b = vec![];
    signed(0, &mut b);
    unsigned(big, &mut b);
    assert!(Column::<Option<u64>>::load(&b).is_err());

    let mut b = vec![];
    unsigned(big, &mut b);
    assert!(Column::<bool>::load(&b).is_err());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn counts_that_fit_still_load() {
    let big = (1u64 << 32) + 2;
    let mut b = vec![];
    signed(big as i64, &mut b);
    b.push(1);
    assert_eq!(Column::<u64>::load(&b).unwrap().len(), big as usize);

    let mut b = vec![];
    signed(0, &mut b);
    unsigned(big, &mut b);
    assert_eq!(Column::<Option<u64>>::load(&b).unwrap().len(), big as usize);

    let mut b = vec![];
    unsigned(big, &mut b);
    assert_eq!(Column::<bool>::load(&b).unwrap().len(), big as usize);
}
