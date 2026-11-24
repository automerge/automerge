//! A bool column's bytes are counts of alternating runs. When a count
//! failed to decode (here, one of ten continuation bytes that overflows
//! a `usize`), the streaming decoder set the run empty but stayed where
//! it was, and then read the same bytes again, forever: `next` never
//! returned. automerge runs streaming decoders over document bytes.
//!
//! `decoder` now requires that its bytes be a saved column, and promises
//! nothing of others; bytes from outside go through `try_decoder`, which
//! reads them through first, as a load does. Checked: `try_decoder`
//! refuses these bytes, and returns, and valid bytes decode as before.
//!
//! (The tests watch for a hang from a second thread, which wasm32-wasip1
//! doesn't have.)
#![cfg(not(target_arch = "wasm32"))]

use hexane::{Column, Leb128};
use std::sync::mpsc;
use std::time::Duration;

/// Bytes whose third count runs ten bytes without ending.
fn bad() -> Vec<u8> {
    let mut b = vec![125, 0, 3, 254];
    b.extend([255; 8]);
    b.extend([0, 124, 2, 16, 254]);
    b.extend([255; 8]);
    b.extend([0, 4]);
    b
}

/// Run `f` on a thread, failing if it doesn't finish in ten seconds.
fn finishes<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("the decoder didn't finish")
}

#[test]
fn a_checked_decoder_refuses_a_count_it_cannot_read() {
    let refused = finishes(|| hexane::try_decoder::<bool>(&bad()).is_err());
    assert!(refused);
}

#[test]
fn so_does_one_in_a_given_codec() {
    let refused = finishes(|| hexane::try_decoder_in::<bool, Leb128>(&bad()).is_err());
    assert!(refused);
}

#[test]
fn valid_bytes_decode_as_before() {
    let vals = [false, true, true, false, false, false];
    let bytes = Column::<bool>::from_values(&vals).save();
    assert_eq!(hexane::decoder::<bool>(&bytes).collect::<Vec<_>>(), vals);
    let got: Vec<bool> = hexane::try_decoder::<bool>(&bytes).unwrap().collect();
    assert_eq!(got, vals);
}
