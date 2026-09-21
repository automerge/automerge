//! Regression tests for <https://github.com/automerge/automerge/issues/697>.
//!
//! The fixture `issue_697_old_js_doc.automerge` was written by JS automerge
//! 1.0.1-preview.7 (taken from the reproduction gist attached to the issue).
//! Its actor table is stored in first-seen order rather than lexicographic
//! order, which exposed two independent problems in the loader:
//!
//! 1. Change hashes are recomputed at load time by reconstructing each
//!    change's canonical chunk encoding, which lists a change's "other
//!    actors" in lexicographic order. Reconstructing them in actor-table
//!    order instead gave different bytes, different hashes, and
//!    "mismatching heads".
//! 2. `OpId` ordering and actor lookup both treat actor *index* as a stand-in
//!    for actor id, which only holds for a sorted table. Adopting this
//!    document's columns as-is yielded a mis-ordered op set: a debug
//!    assertion on the next change application, silent mis-ordering and
//!    duplicate actor entries in release.
//!
//! These tests load the document end to end, so they cover both.

use automerge::{transaction::Transactable, ActorId, Automerge, ChangeHash, ReadDoc, ROOT};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read("./tests/fixtures/".to_owned() + name).unwrap()
}

/// The head hash stored in the fixture, as computed by the JS automerge
/// which wrote it.
const FIXTURE_HEAD: &str = "fcbf81279eb9fda54156e161f2e17a63ebab81febb7ea2e8878da1194df3c029";

#[test]
fn load_old_js_document_with_unsorted_actor_table() {
    let doc = Automerge::load(&fixture("issue_697_old_js_doc.automerge")).unwrap();

    let expected: ChangeHash = FIXTURE_HEAD.parse().unwrap();
    assert_eq!(doc.get_heads(), vec![expected]);
    assert_eq!(doc.get_changes(&[]).len(), 124);
    assert_eq!(doc.keys(ROOT).count(), 8);
}

/// Loading a document with an unsorted actor table must produce exactly the
/// state that canonical ingestion (applying its changes to an empty document)
/// produces: same heads, same content, and byte-identical `save()` output.
/// The last point implies the actor table was rebuilt in sorted order and
/// the ops were laid out in canonical order.
#[test]
fn unsorted_actor_table_document_loads_as_canonical_ingestion_would() {
    let loaded = Automerge::load(&fixture("issue_697_old_js_doc.automerge")).unwrap();

    let mut ingested = Automerge::new();
    ingested.apply_changes(loaded.get_changes(&[])).unwrap();

    assert_eq!(loaded.get_heads(), ingested.get_heads());
    assert_eq!(
        loaded.hydrate(None),
        ingested.hydrate(None),
        "document content differs"
    );
    assert_eq!(loaded.save(), ingested.save(), "saved bytes differ");
}

#[test]
fn old_js_document_round_trips_through_save_and_load() {
    let doc = Automerge::load(&fixture("issue_697_old_js_doc.automerge")).unwrap();
    let saved = doc.save();
    let reloaded = Automerge::load(&saved).unwrap();
    assert_eq!(reloaded.get_heads(), doc.get_heads());
    assert_eq!(reloaded.keys(ROOT).count(), doc.keys(ROOT).count());
}

#[test]
fn old_js_document_can_be_edited_by_its_existing_actors() {
    // The fixture's actor table is unsorted, so actor lookups (which
    // normally binary-search a sorted table) must not silently miss existing
    // actors - that would insert a duplicate table entry for the same actor.
    let mut doc = Automerge::load(&fixture("issue_697_old_js_doc.automerge")).unwrap();

    let mut actors: Vec<ActorId> = doc
        .get_changes(&[])
        .iter()
        .map(|c| c.actor_id().clone())
        .collect();
    actors.sort();
    actors.dedup();
    assert_eq!(actors.len(), 26);

    for (i, actor) in actors.into_iter().enumerate() {
        doc.set_actor(actor);
        let mut tx = doc.transaction();
        tx.put(ROOT, format!("edit-{}", i), i as i64).unwrap();
        tx.commit();
    }

    let saved = doc.save();
    let reloaded = Automerge::load(&saved).unwrap();
    assert_eq!(reloaded.get_heads(), doc.get_heads());
    assert_eq!(reloaded.get(ROOT, "edit-25").unwrap().unwrap().0, 25.into());
}

#[test]
fn old_js_document_can_be_edited_after_load() {
    let mut doc = Automerge::load(&fixture("issue_697_old_js_doc.automerge")).unwrap();
    // Use an actor which sorts before every actor already in the document so
    // that actor lookups have to cope with the unusual table ordering.
    doc.set_actor(ActorId::from(&[0u8; 16][..]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "issue-697", "resolved").unwrap();
    tx.commit();

    let saved = doc.save();
    let reloaded = Automerge::load(&saved).unwrap();
    assert_eq!(reloaded.get_heads(), doc.get_heads());
    assert_eq!(
        reloaded.get(ROOT, "issue-697").unwrap().unwrap().0,
        "resolved".into()
    );
}
