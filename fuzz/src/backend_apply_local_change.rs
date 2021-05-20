#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|change: automerge_protocol::UncompressedChange| {
    let mut b = automerge_backend::Backend::new();
    let _ = b.apply_local_change(change);
});
