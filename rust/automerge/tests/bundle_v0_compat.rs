//! Bundles written by automerge 3.3.2 (chunk type 3) are in circulation and
//! must keep loading. If this fails, fix the reader, not the test.
use automerge::next::{Automerge, ReadDoc};
use automerge::{ObjType, ROOT};

const BUNDLE_3_3_2: &[u8] = include_bytes!("fixtures/bundle_v0_automerge_3_3_2.bin");

fn load_fixture() -> Automerge {
    let mut doc = Automerge::new();
    doc.load_incremental(BUNDLE_3_3_2)
        .expect("a 3.3.2 bundle must still load");
    doc
}

#[test]
fn v0_bundle_is_chunk_type_three() {
    const CHUNK_TYPE_OFFSET: usize = 8;
    const BUNDLE_V0_CHUNK: u8 = 3;
    assert_eq!(
        BUNDLE_3_3_2[CHUNK_TYPE_OFFSET], BUNDLE_V0_CHUNK,
        "fixture must be a BundleV0 chunk"
    );
}

#[test]
fn v0_bundle_loads_with_expected_content() {
    let doc = load_fixture();

    let (name, _) = doc.get(ROOT, "name").unwrap().unwrap();
    assert_eq!(name.into_string().unwrap(), "fixture");

    let (_, list) = doc.get(ROOT, "list").unwrap().unwrap();
    assert_eq!(doc.length(&list), 3);
    assert_eq!(doc.get(&list, 0).unwrap().unwrap().0.as_i64(), Some(1));
    assert_eq!(doc.get(&list, 1).unwrap().unwrap().0.as_i64(), Some(2));
    assert_eq!(
        doc.get(&list, 2).unwrap().unwrap().0.into_string().unwrap(),
        "three"
    );

    // the text splice exercises the pred column, where V0 records deletes
    let (_, text) = doc.get(ROOT, "text").unwrap().unwrap();
    assert_eq!(doc.object_type(&text).unwrap(), ObjType::Text);
    assert_eq!(doc.text(&text).unwrap(), "hello there");

    assert!(doc.get(ROOT, "n").unwrap().is_none());

    let (counter, _) = doc.get(ROOT, "counter").unwrap().unwrap();
    assert_eq!(counter.as_i64(), Some(12));
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
    let doc = doc.enable_audit_mode().unwrap();
    assert_eq!(doc.get_changes(&[]).unwrap().len(), 5);
}
