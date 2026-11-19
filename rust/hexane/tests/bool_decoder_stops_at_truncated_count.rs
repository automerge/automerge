//! A bool column's bytes are counts of alternating runs. When a count
//! failed to decode (here, one cut off by the end of the bytes), the
//! streaming decoder set the run empty but stayed where it was, and then
//! read the same bytes again, forever: `next` never returned. automerge
//! runs streaming decoders over the bytes of documents it loads. The
//! decoder now stops at a count it can't read.
//!
//! (The tests watch for a hang from a second thread, which wasm32-wasip1
//! doesn't have.)
#![cfg(not(target_arch = "wasm32"))]

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
fn next_stops_at_a_count_it_cannot_read() {
    let got = finishes(|| hexane::decoder::<bool>(BAD).collect::<Vec<_>>());
    assert_eq!(got, vec![false, false]);
}

#[test]
fn nth_stops_too() {
    assert_eq!(finishes(|| hexane::decoder::<bool>(BAD).nth(5)), None);
}

#[test]
fn runs_stop_too() {
    use hexane::RunDecoder;
    let got = finishes(|| {
        let mut d = hexane::decoder::<bool>(BAD);
        let mut runs = vec![];
        while let Some(run) = d.next_run() {
            runs.push((run.count, run.value));
        }
        runs
    });
    assert_eq!(got, vec![(2, false)]);
}

#[test]
fn valid_bytes_decode_as_before() {
    let col = hexane::Column::<bool>::from_values(vec![false, true, true, false, false, false]);
    let got: Vec<bool> = hexane::decoder::<bool>(&col.save()).collect();
    assert_eq!(got, vec![false, true, true, false, false, false]);
}
