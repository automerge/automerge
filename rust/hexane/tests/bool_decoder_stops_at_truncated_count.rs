//! A bool column's bytes are counts of alternating runs. When a count
//! failed to decode (here, one cut off by the end of the bytes), the
//! streaming decoder set the run empty but stayed where it was, and then
//! read the same bytes again, forever: `next` never returned. automerge
//! runs streaming decoders over the bytes of documents it loads.
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

/// Two falses, then a count cut off by the end.
const BAD: &[u8] = &[0x02, 0xff];

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
    assert!(finishes(|| hexane::try_decoder::<bool>(BAD).is_err()));
}

#[test]
fn so_does_one_in_a_given_codec() {
    assert!(finishes(
        || hexane::try_decoder_in::<bool, Leb128>(BAD).is_err()
    ));
}

#[test]
fn valid_bytes_decode_as_before() {
    let vals = [false, true, true, false, false, false];
    let bytes = Column::<bool>::from_values(&vals).save();
    assert_eq!(hexane::decoder::<bool>(&bytes).collect::<Vec<_>>(), vals);
    let mut d = hexane::try_decoder::<bool>(&bytes).unwrap();
    let mut runs = vec![];
    while let Some(run) = d.next_run() {
        runs.push((run.count, run.value));
    }
    assert_eq!(runs, vec![(1, false), (2, true), (3, false)]);
}
