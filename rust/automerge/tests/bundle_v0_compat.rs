//! The frozen 3.3.x bundle reader (`storage::bundle_v0`, chunk id 3).
//!
//! The fixture is a real bundle emitted by automerge 3.3.2 — built by
//! checking that tag out and calling `doc.bundle(hashes)` — not by any
//! code in this tree. Change sets in that format are in circulation, so this
//! test is the contract that they keep loading.
//!
//! If this fails, `storage::bundle_v0` has been "improved". Don't fix the
//! test.
use automerge::{Automerge, ObjType, ReadDoc, ROOT};

/// automerge 3.3.2, 329 bytes, chunk type 3, 5 changes.
const BUNDLE_3_3_2: &[u8] = include_bytes!("fixtures/bundle_v0_automerge_3_3_2.bin");

fn load_fixture() -> Automerge {
    let mut doc = Automerge::new();
    doc.load_incremental(BUNDLE_3_3_2)
        .expect("a 3.3.2 bundle must still load");
    doc
}

#[test]
fn v0_bundle_is_chunk_type_three() {
    // byte 8 is the chunk type, right after the 4 magic bytes and the
    // 4-byte checksum — if this ever changes, the fixture was regenerated
    // with the wrong writer
    assert_eq!(BUNDLE_3_3_2[8], 3, "fixture must be a BundleV0 chunk");
}

#[test]
fn v0_bundle_loads_with_expected_content() {
    let doc = load_fixture();

    let (name, _) = doc.get(ROOT, "name").unwrap().unwrap();
    assert_eq!(name.into_string().unwrap(), "fixture");

    let (_, list) = doc.get(ROOT, "list").unwrap().unwrap();
    assert_eq!(doc.length(&list), 3);
    assert_eq!(doc.get(&list, 0).unwrap().unwrap().0.to_i64(), Some(1));
    assert_eq!(doc.get(&list, 1).unwrap().unwrap().0.to_i64(), Some(2));
    assert_eq!(
        doc.get(&list, 2).unwrap().unwrap().0.into_string().unwrap(),
        "three"
    );

    // a splice replacing the middle of the text: exercises the pred
    // column, which is where V0 records deletes (the live format elides
    // them into a succ column this reader has never seen)
    let (_, text) = doc.get(ROOT, "text").unwrap().unwrap();
    assert_eq!(doc.object_type(&text).unwrap(), ObjType::Text);
    assert_eq!(doc.text(&text).unwrap(), "hello there");

    // deleted key stays deleted
    assert!(doc.get(ROOT, "n").unwrap().is_none());

    // counter with an increment applied on top
    let (counter, _) = doc.get(ROOT, "counter").unwrap().unwrap();
    assert_eq!(counter.to_i64(), Some(12));
}

#[test]
fn v0_bundle_reproduces_its_heads() {
    let doc = load_fixture();
    let heads = doc.get_head_hashes();
    assert_eq!(heads.len(), 1);
    assert_eq!(
        heads[0].to_string(),
        "9bfe6212a03b1a679680f7ceae96dc4b2a2dcda76ef729f9e4359f2f4d697138",
        "the hashes recomputed from the decoded ops must match what 3.3.2 \
         computed when it wrote the bundle"
    );
}

#[test]
fn v0_bundle_carries_all_five_changes() {
    let doc = load_fixture();
    let mut doc = doc;
    doc.enable_audit_mode().unwrap();
    assert_eq!(doc.get_changes(&[]).unwrap().len(), 5);
}
