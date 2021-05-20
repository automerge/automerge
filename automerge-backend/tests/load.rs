use automerge_backend::Backend;

#[test]
fn test_load_index_out_of_bounds() {
    // these are just random bytes
    let bytes = vec![133, 111, 74, 131, 0, 46, 128, 0];
    let _ = Backend::load(bytes);
}

#[test]
fn test_load_index_out_of_bounds_2() {
    // these are just random bytes
    let bytes = vec![
        133, 111, 74, 131, 171, 99, 102, 54, 2, 16, 42, 0, 18, 255, 255, 61, 57, 57, 57, 29, 48,
        48, 48, 116, 0, 0, 0, 46, 46,
    ];
    let _ = Backend::load(bytes);
}

#[test]
fn test_load_leb_failed_to_read_whole_buffer() {
    // these are just random bytes
    let bytes = vec![133, 111, 74, 131, 46, 46, 46, 46, 46];
    let _ = Backend::load(bytes);
}
