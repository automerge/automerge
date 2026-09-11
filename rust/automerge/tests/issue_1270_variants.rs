//! Evidence-gathering tests for the class of bug behind issue #1270.
//!
//! Hypothesis: the root problem is not specific to `fork()` + `isolate()`
//! (which PR #1295 patches at the `patch_to` call site). The underlying
//! invariant is that a `PatchLog` may not buffer events without recording the
//! actor table those events were indexed against. Any path which logs events
//! into a *persistent* patch log whose `actors` baseline is empty exhibits the
//! same corruption when a lexicographically-smaller actor is later inserted.
//!
//! This test exercises a different public entry point:
//! `Automerge::load_incremental_log_patches` on an empty document, which calls
//! `log_current_state` into the caller's patch log (seeding events but not the
//! baseline), followed by `transaction_log_patches` with a smaller actor.

use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, ObjType, PatchAction, PatchLog,
    ReadDoc, ROOT,
};

/// Apply SpliceText/DeleteSeq patches for the "text" object to a string,
/// mimicking how a materialized view (e.g. the JS wrapper) consumes patches.
fn apply_text_patches(initial: &str, patches: &[automerge::Patch]) -> String {
    let mut text = String::from(initial);
    for patch in patches {
        match &patch.action {
            PatchAction::SpliceText { index, value, .. } => {
                let s = value.make_string();
                let byte_idx = text
                    .char_indices()
                    .nth(*index)
                    .map(|(i, _)| i)
                    .unwrap_or(text.len());
                text.insert_str(byte_idx, &s);
            }
            PatchAction::DeleteSeq { index, length } => {
                let start = text.char_indices().nth(*index).map(|(i, _)| i).unwrap();
                let end = text
                    .char_indices()
                    .nth(index + length)
                    .map(|(i, _)| i)
                    .unwrap_or(text.len());
                text.replace_range(start..end, "");
            }
            // Object creation (PutMap for "text") etc. — irrelevant to the text content.
            _ => {}
        }
    }
    text
}

#[test]
fn load_incremental_then_transact_with_smaller_actor() {
    // A saved document authored by actor "2222" containing "def".
    let mut doc1 = AutoCommit::new().with_actor(ActorId::from(&b"2222"[..]));
    let txt = doc1.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&txt, 0, 0, "def").unwrap();
    let saved = doc1.save();

    // An empty document with a persistent, caller-held patch log — the
    // documented usage pattern for PatchLog.
    let mut doc2 = Automerge::new().with_actor(ActorId::from(&b"1111"[..]));
    let mut log = PatchLog::active();

    // Empty-doc fast path: log_current_state fills `log` with events indexed
    // against the loaded actor table ["2222"], while log.actors stays [].
    doc2.load_incremental_log_patches(&saved, &mut log).unwrap();

    // A transaction by actor "1111" (sorts before "2222"): the actor table
    // becomes ["1111", "2222"], shifting "2222" to index 1. begin_transaction
    // -> migrate_actors sees an empty baseline and adopts without migrating
    // the already-buffered events.
    let mut tx = doc2.transaction_log_patches(log).unwrap();
    let txt2 = tx.get(&ROOT, "text").unwrap().unwrap().1;
    tx.splice_text(&txt2, 0, 0, "abc").unwrap();
    let (_, mut log) = tx.commit();

    // The document itself is correct...
    assert_eq!(doc2.text(&txt).unwrap(), "abcdef", "document state wrong");

    // ...but are the patches?
    let patches = doc2.make_patches(&mut log);
    let text = apply_text_patches("", &patches);
    assert_eq!(
        text, "abcdef",
        "patches produced wrong result; patches were: {:#?}",
        patches
    );
}
