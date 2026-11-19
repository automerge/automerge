//! A streaming decoder over a string column hands out `&str`s, which must
//! be UTF-8. The decoder doesn't check each string as it reads it (a
//! column's own bytes were checked when loaded or written), so bytes from
//! outside are checked up front: a decoder over a string column that isn't
//! valid panics, rather than hand out a `&str` that isn't UTF-8.

use hexane::{Codec, Column, Leb128};

/// A literal run of one string of one byte, `0xff`, which isn't UTF-8.
const NOT_UTF8: [u8; 3] = [0x7f, 0x01, 0xff];

#[test]
#[should_panic(expected = "not a valid column")]
fn a_string_decoder_rejects_bytes_that_are_not_utf8() {
    let _ = hexane::decoder::<String>(&NOT_UTF8);
}

#[test]
#[should_panic(expected = "not a valid column")]
fn an_optional_string_decoder_rejects_them_too() {
    let _ = hexane::decoder::<Option<String>>(&NOT_UTF8);
}

#[test]
#[should_panic(expected = "not a valid column")]
fn so_does_one_in_a_given_codec() {
    let _ = hexane::decoder_in::<String, Leb128>(&NOT_UTF8);
}

#[test]
fn a_valid_string_column_decodes() {
    let vals = vec!["a", "héllo", "", "héllo", "z"];
    let bytes = Column::<String>::from_values(vals.iter().map(|s| s.to_string()).collect()).save();
    assert_eq!(hexane::decoder::<String>(&bytes).collect::<Vec<_>>(), vals);
    // and other columns' bytes are not checked: a run of 3 copies of 0x7f
    let mut b = vec![0x03];
    b.extend_from_slice(Leb128::encode_unsigned(0x7f).as_bytes());
    assert_eq!(
        hexane::decoder::<u64>(&b).collect::<Vec<_>>(),
        vec![0x7f; 3]
    );
}
