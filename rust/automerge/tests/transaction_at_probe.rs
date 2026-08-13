//! Known defect (found by the `tests/patch_log/properties/caller_held.rs`):
//! [`Automerge::transaction_at`] with a caller-held patch log produces
//! incorrect patches.
//!
//! The patch log has a second, undocumented reference-frame invariant
//! besides the actor baseline: all buffered events must be relative to the
//! same document view. Crossing views requires finalizing the current view
//! first (see [`PatchLog::finish_current_view`], whose doc comment describes
//! exactly this hazard).
//!
//! [`AutoCommit::isolate`]/[`AutoCommit::integrate`] honor this via `patch_to`
//! (finalize + rewind/replay diffs); [`Autocommit::transaction_at`] does not:
//! its isolation-relative events are sorted and coalesced together with
//! current-view-relative events, producing a self-consistent but wrong view.
//!
//! All actors here are strictly ascending (0x10, 0x20, 0x30), so the actor
//! table never shifts and migrate_actors is a no-op.

use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, ObjType, PatchAction, PatchLog,
    ReadDoc, ROOT,
};

#[test]
#[ignore = "known defect: transaction_at mixes isolation-relative and \
            current-view-relative events in a shared patch log; see module \
            docs and docs/plans/patch-log-actor-interning.md finding #4"]
fn transaction_at_probe_ascending_actors() {
    let mut origin = AutoCommit::new().with_actor(ActorId::from(&[0x10][..]));
    let txt = origin.put_object(&ROOT, "text", ObjType::Text).unwrap();
    origin.splice_text(&txt, 0, 0, "seed").unwrap();
    let saved = origin.save();

    let mut doc = Automerge::new();
    let mut log = PatchLog::active();
    doc.load_incremental_log_patches(&saved, &mut log).unwrap();
    let heads0 = doc.get_heads();

    // Regular transaction: prepend "aa" (actor 0x20, sorts after 0x10).
    doc.set_actor(ActorId::from(&[0x20][..]));
    let mut tx = doc.transaction_log_patches(log).unwrap();
    tx.splice_text(&txt, 0, 0, "aa").unwrap();
    let (_, l) = tx.commit();
    log = l;

    // Isolated transaction at the pre-"aa" heads: insert "X" at index 1 of
    // "seed" (actor 0x30, sorts after everything).
    doc.set_actor(ActorId::from(&[0x30][..]));
    let mut tx = doc.transaction_at(log, &heads0).unwrap();
    tx.splice_text(&txt, 1, 0, "X").unwrap();
    let (_, l) = tx.commit();
    log = l;

    let actual = doc.text(&txt).unwrap();

    let patches = doc.make_patches(&mut log);
    let mut model = String::new();
    for patch in &patches {
        match &patch.action {
            PatchAction::PutMap { key, .. } if key == "text" => model.clear(),
            PatchAction::SpliceText { index, value, .. } => {
                let at = model
                    .char_indices()
                    .nth(*index)
                    .map(|(i, _)| i)
                    .unwrap_or(model.len());
                model.insert_str(at, &value.make_string());
            }
            PatchAction::DeleteSeq { index, length } => {
                let start = model.char_indices().nth(*index).map(|(i, _)| i).unwrap();
                let end = model
                    .char_indices()
                    .nth(index + length)
                    .map(|(i, _)| i)
                    .unwrap_or(model.len());
                model.replace_range(start..end, "");
            }
            _ => {}
        }
    }

    assert_eq!(
        model, actual,
        "patch-built view diverged from document; patches: {:#?}",
        patches
    );
}
