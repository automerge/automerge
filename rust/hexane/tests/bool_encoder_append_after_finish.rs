//! `BoolEncoderState::finish` flushes the pending run into the caller's
//! buffer; the state stays usable. Appending after it must keep the
//! values: the bool format has no stored values, only alternating run
//! counts, so a run flushed early still decides the parity of every run
//! after it.

use hexane::bool::BoolDecoder;
use hexane::encoder::BoolEncoderState;
use hexane::{Column, ColumnEncoding, Leb128};

fn decode(bytes: &[u8]) -> Vec<bool> {
    let d: BoolDecoder<'_, Leb128> = <hexane::BoolEncoding<Leb128> as ColumnEncoding>::decoder(bytes);
    d.collect()
}

fn encode_in_two(first: &[bool], second: &[bool]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut st = BoolEncoderState::<Leb128>::new();
    for &v in first {
        st.append(&mut buf, v);
    }
    st.finish(&mut buf);
    for &v in second {
        st.append(&mut buf, v);
    }
    st.finish(&mut buf);
    buf
}

/// The same value after a finish used to be counted as the other value:
/// `[false, false] | [false]` decoded as `[false, false, true]`.
#[test]
fn appending_the_same_value_after_finish_keeps_it() {
    let bytes = encode_in_two(&[false, false], &[false]);
    assert_eq!(decode(&bytes), [false, false, false]);
    let bytes = encode_in_two(&[true], &[true, true]);
    assert_eq!(decode(&bytes), [true, true, true]);
}

/// Every split of a mixed sequence decodes to the sequence.
#[test]
fn any_split_keeps_the_values() {
    let vals = [true, true, false, true, false, false, false, true];
    for cut in 0..=vals.len() {
        let bytes = encode_in_two(&vals[..cut], &vals[cut..]);
        assert_eq!(decode(&bytes), vals, "split at {cut}");
    }
}

/// A different value after a finish was and stays canonical: it loads.
#[test]
fn appending_the_other_value_after_finish_still_loads() {
    let bytes = encode_in_two(&[true], &[false, false]);
    assert_eq!(Column::<bool>::load(&bytes).unwrap().to_vec(), [true, false, false]);
}
