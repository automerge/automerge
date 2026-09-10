use super::View;
use crate::{
    hydrate, transaction::Transactable, ActorId, Author, Automerge, ObjType, PatchLog,
    PatchLogMismatch, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

fn author(name: &str) -> Author<'static> {
    Author::try_from(name).unwrap().into_owned()
}

fn actor(byte: u8) -> ActorId {
    ActorId::from(vec![byte])
}

// Exercise exactly the same internal view transition and materialization path
// as patch logs, without exposing historical revocations through a public API.
fn materialize(doc: &Automerge, view: &View) -> hydrate::Value {
    let mut log = PatchLog::active();
    log.set_view(doc.view_at(&[]));
    log.transition_to(doc, view.clone()).unwrap();
    let mut value = doc.hydrate(Some(&[]));
    value
        .apply_patches(ENCODING, doc.make_patches(&mut log))
        .unwrap();
    value
}

#[test]
fn saved_views_survive_revocation_changes_actor_insertions_and_removals() {
    let revoked = author("aaaa");
    let mut doc = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(revoked.clone()))
        .with_actor(actor(0x80));
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    // Use multiple operations per change so seq clocks cannot masquerade as
    // operation clocks in the saved masks.
    tx.put(ROOT, "x", 1).unwrap();
    tx.put(ROOT, "y", 2).unwrap();
    tx.commit();
    let visible = doc.current_view();
    let expected_visible = doc.hydrate(None);

    doc.revoke(revoked.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let hidden = doc.current_view();
    let expected_hidden = doc.hydrate(None);
    assert_ne!(visible.revocations, hidden.revocations);
    assert_eq!(visible.heads, hidden.heads);
    // Repeating the same effective mask does not add another state.
    doc.revoke(revoked, &[], &mut PatchLog::inactive()).unwrap();
    assert_eq!(hidden.revocations, doc.current_view().revocations);

    // Clearing policy appends an unrestricted state, not a reset of history.
    doc.set_revocations(Default::default());
    let restored = doc.current_view();
    assert_ne!(hidden.revocations, restored.revocations);
    assert_eq!(doc.clock_for_view(&visible), doc.clock_for_view(&restored));

    doc.set_author(Some(author("bbbb")));
    doc.set_actor(actor(0x10));
    let mut tx = doc.transaction();
    tx.put(ROOT, "future", true).unwrap();
    tx.commit();
    assert_eq!(doc.ops().actors[0], actor(0x10));
    assert_eq!(doc.ops().actors[1], actor(0x80));
    // Ordinary allowed imports/edits do not publish a new revocation state.
    assert_eq!(restored.revocations, doc.current_view().revocations);

    // Insert and then remove a speculative actor ahead of both saved actors.
    doc.set_actor(actor(0x05));
    let (hash, _) = doc
        .transaction_log_patches(PatchLog::active())
        .unwrap()
        .commit();
    assert!(hash.is_none());
    assert!(!doc.ops().actors.contains(&actor(0x05)));

    assert_eq!(materialize(&doc, &visible), expected_visible);
    assert_eq!(materialize(&doc, &hidden), expected_hidden);
    assert_eq!(materialize(&doc, &restored), expected_visible);
}

#[test]
fn resolving_a_pending_boundary_does_not_rewrite_a_saved_mask() {
    let revoked = author("aaaa");
    let mut source = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(revoked.clone()))
        .with_actor(actor(0x80));
    let mut tx = source.transaction();
    tx.put(ROOT, "x", 1).unwrap();
    tx.commit();
    let heads = source.get_heads();
    let original = source.get_changes(&[]);
    source.set_author(Some(author("bbbb")));
    source.set_actor(actor(0x10));
    let mut tx = source.transaction();
    tx.put(ROOT, "ack", true).unwrap();
    tx.commit();
    let boundary_heads = source.get_heads();
    let boundary = source.get_changes(&heads);

    let mut doc = Automerge::new_with_encoding(ENCODING);
    doc.apply_changes(original).unwrap();
    doc.revoke(revoked, &boundary_heads, &mut PatchLog::inactive())
        .unwrap();
    let old = doc.view_at(&heads);
    let expected_old = doc.hydrate(Some(&heads));
    doc.apply_changes(boundary).unwrap();
    let new = doc.view_at(&heads);
    assert_ne!(old.revocations, new.revocations);
    assert_eq!(doc.ops().actors[0], actor(0x10));

    assert_eq!(materialize(&doc, &old), expected_old);
    assert_eq!(materialize(&doc, &new), source.hydrate(Some(&heads)));
    // Public historical diffs still use today's revocations on both sides.
    assert!(doc.diff(&heads, &heads).is_empty());
    let mut historical = doc.hydrate(Some(&[]));
    historical
        .apply_patches(ENCODING, doc.diff(&[], &heads))
        .unwrap();
    assert_eq!(historical, materialize(&doc, &new));
}

#[test]
fn pending_patch_paths_use_their_saved_view_after_unlogged_mutations() {
    let revoked = author("aaaa");
    let allowed = author("bbbb");
    let mut doc = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(allowed.clone()))
        .with_actor(actor(0xe0));
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.commit();
    doc.set_author(Some(revoked.clone()));
    doc.set_actor(actor(0x80));
    let mut tx = doc.transaction();
    let text = tx.insert_object(&list, 0, ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    tx.commit();
    let mut rendered = doc.hydrate(None);

    doc.set_author(Some(allowed.clone()));
    doc.set_actor(actor(0x40));
    let mut tx = doc.transaction_log_patches(PatchLog::active()).unwrap();
    tx.splice_text(&text, 3, 0, "X").unwrap();
    let (_, mut pending) = tx.commit();
    let expected = doc.hydrate(None);

    // This log is not passed to either mutation. Its pending text patch must
    // still be materialized with the old path, under the old revocation mask.
    doc.revoke(revoked, &[], &mut PatchLog::inactive()).unwrap();
    doc.set_actor(actor(0x10));
    let mut tx = doc.transaction();
    tx.insert(&list, 0, "prefix").unwrap();
    tx.commit();
    rendered
        .apply_patches(ENCODING, doc.make_patches(&mut pending))
        .unwrap();
    assert_eq!(rendered, expected);
    assert_ne!(rendered, doc.hydrate(None));
}

#[test]
fn diverging_clones_cannot_confuse_equal_history_offsets() {
    let revoked = author("aaaa");
    let mut base = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(revoked.clone()))
        .with_actor(actor(0x80));
    let mut tx = base.transaction();
    tx.put(ROOT, "x", 1).unwrap();
    tx.commit();
    let original = base.current_view();
    let mut left = base.clone();
    let mut right = base.clone();
    let mut log = PatchLog::active();
    left.revoke(revoked.clone(), &[], &mut log).unwrap();
    right
        .revoke(revoked.clone(), &original.heads, &mut PatchLog::inactive())
        .unwrap();
    let right_before = right.current_view();
    assert_ne!(left.current_view().revocations, right_before.revocations);
    let patches_before = left.make_patches(&mut log);

    assert_eq!(right.unrevoke(&revoked, &mut log), Err(PatchLogMismatch));
    assert_eq!(right.current_view(), right_before);
    assert_eq!(left.make_patches(&mut log), patches_before);
    // Shared pre-divergence entries are still valid on either branch.
    assert_eq!(materialize(&left, &original), base.hydrate(None));
    assert_eq!(materialize(&right, &original), base.hydrate(None));
}

#[test]
fn loading_does_not_preserve_process_local_view_identity() {
    let mut doc = Automerge::new_with_encoding(ENCODING);
    let mut tx = doc.transaction();
    tx.put(ROOT, "x", 1).unwrap();
    tx.commit();
    let old = doc.current_view();
    let loaded = Automerge::load(&doc.save()).unwrap();
    assert_eq!(loaded.hydrate(None), doc.hydrate(None));
    assert_eq!(loaded.validate_view(&old), Err(PatchLogMismatch));
}
