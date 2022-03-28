#![no_main]
use automerge::Automerge;
use libfuzzer_sys::fuzz_target;

// Fuzz the load operation on an Automerge document.
fuzz_target!(|data: &[u8]| {
    let _ = Automerge::load(data);
});
