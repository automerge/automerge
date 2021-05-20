#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|changes: Vec<automerge_backend::Change>| {
    let _ =
        automerge_backend::Backend::apply_changes(&mut automerge_backend::Backend::new(), changes);
});
