//! A slab records the byte length of its last run in a `u32`, so a run
//! of 4 GiB or more (one huge value, say) is not encodable: making one is
//! a panic, as for a run of more than `i64::MAX` values, rather than a
//! slab whose tail record is silently truncated. Large blobs belong in a
//! `RawColumn`.

#[test]
#[ignore = "needs well over 8 GiB of memory"]
#[should_panic(expected = "must stay under 4 GiB")]
fn a_value_of_4gib_panics() {
    let big = "x".repeat(u32::MAX as usize + 1);
    let _ = hexane::Column::<String>::from_values(vec![big]);
}
