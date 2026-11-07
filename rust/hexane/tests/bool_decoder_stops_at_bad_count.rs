//! A bool column's bytes are counts of alternating runs. When a count
//! failed to decode (here, one of ten continuation bytes that overflows
//! a `usize`), the streaming decoder set the run empty but stayed where
//! it was, and then read the same bytes again, forever: `next` never
//! returned. Nothing in `decoder` says its input must be valid, and
//! automerge runs streaming decoders over document bytes. The decoder
//! now stops at a count it can't read.
//!
//! (The tests watch for a hang from a second thread, which wasm32-wasip1
//! doesn't have.)
#![cfg(not(target_arch = "wasm32"))]

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
fn next_stops_at_a_count_it_cannot_read() {
    let got = finishes(|| {
        let b = bad();
        hexane::decoder::<bool>(&b).take(1000).count()
    });
    assert!(got <= 1000);
}

#[test]
fn nth_stops_too() {
    finishes(|| {
        let b = bad();
        hexane::decoder::<bool>(&b).nth(500)
    });
}

#[test]
fn runs_stop_too() {
    use hexane::RunDecoder;
    finishes(|| {
        let b = bad();
        let mut d = hexane::decoder::<bool>(&b);
        let mut n = 0;
        while d.next_run().is_some() && n < 1000 {
            n += 1;
        }
        n
    });
}

#[test]
fn valid_bytes_decode_as_before() {
    let col = hexane::Column::<bool>::from_values(vec![false, true, true, false, false, false]);
    let got: Vec<bool> = hexane::decoder::<bool>(&col.save()).collect();
    assert_eq!(got, vec![false, true, true, false, false, false]);
}
