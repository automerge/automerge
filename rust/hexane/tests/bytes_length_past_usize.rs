//! A string's or byte string's length is a `u64` on the wire. Where `usize`
//! is narrower (wasm32), a length past `usize::MAX` can't be the length of
//! anything in the data, so loading must reject it, as it rejects any length
//! past the data's end, rather than truncate it: truncated, a second
//! encoding of a value loads as that value, and the encoding is no longer
//! canonical.

use hexane::{Column, RleValue};

/// A literal run of one value (header -1), whose length header is
/// 2^32 + 3 and which is followed by three bytes.
fn long_header() -> Vec<u8> {
    vec![0x7f, 0x83, 0x80, 0x80, 0x80, 0x10, b'a', b'b', b'c']
}

fn rejects<T: RleValue>() {
    let data = long_header();
    assert!(Column::<T>::load(&data).is_err());
    assert!(Column::<Option<T>>::load(&data).is_err());
}

#[test]
fn a_string_length_past_usize_is_rejected() {
    rejects::<String>();
}

#[test]
fn a_bytes_length_past_usize_is_rejected() {
    rejects::<Vec<u8>>();
}

/// The three bytes alone, with the length 3, still load.
#[test]
fn the_canonical_encoding_loads() {
    let data = vec![0x7f, 0x03, b'a', b'b', b'c'];
    let col = Column::<String>::load(&data).unwrap();
    assert_eq!(col.iter().collect::<Vec<_>>(), vec!["abc"]);
}
