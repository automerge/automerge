use automerge::{
    sync::{State, SyncDoc},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Change, ChangeHash, ObjType, Patch, PatchAction, ReadDoc,
    TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

// The distinct import implementations: the batch path (which apply_changes
// and merge delegate to), change-chunk and document-chunk loads, and the
// sync protocol's out-of-order queueing.
#[derive(Clone, Copy)]
enum Import {
    Batch,
    Load,
    LoadDocument,
    Sync,
}

struct PendingMask {
    doc: AutoCommit,
    source: AutoCommit,
    original_heads: Vec<ChangeHash>,
    boundary: Change,
}

impl PendingMask {
    fn new() -> Self {
        Self::with_later_changes(false)
    }

    fn with_later_changes(later: bool) -> Self {
        let author = Author::try_from("aaaa").unwrap();
        let mut source = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(author.clone()))
            .with_actor(ActorId::from(vec![0x80]));
        source.put(ROOT, "x", 1).unwrap();
        let original_heads = source.get_heads();
        if later {
            source.put(ROOT, "x", 2).unwrap();
            source.put(ROOT, "future", true).unwrap();
            source.commit();
        }
        let original_changes = source.get_changes(&[]);

        source.set_author(Some(Author::try_from("bbbb").unwrap()));
        source.set_actor(ActorId::from(vec![0x10]));
        source.put(ROOT, "ack", true).unwrap();
        let boundary_heads = source.get_heads();
        let boundary = source.get_last_local_change().unwrap();

        let mut doc = AutoCommit::new_with_encoding(ENCODING);
        doc.apply_changes(original_changes).unwrap();
        // The boundary is unknown here, so x is initially hidden. Witnessing
        // the acknowledgement later should reveal that x predates the boundary.
        doc.mask_author(author, &boundary_heads);
        assert!(doc.get_at(ROOT, "x", &original_heads).unwrap().is_none());
        Self {
            doc,
            source,
            original_heads,
            boundary,
        }
    }

    fn import_boundary(&mut self, mode: Import) {
        match mode {
            Import::Batch => self
                .doc
                .apply_changes_batch([self.boundary.clone()])
                .unwrap(),
            Import::Load => {
                let bytes = self.source.save_after(&self.original_heads);
                self.doc.load_incremental(&bytes).unwrap();
            }
            Import::LoadDocument => {
                let bytes = self.source.save();
                self.doc.load_incremental(&bytes).unwrap();
            }
            Import::Sync => sync_docs(&mut self.source, &mut self.doc),
        }
        assert_eq!(
            self.doc
                .get_at(ROOT, "x", &self.original_heads)
                .unwrap()
                .unwrap()
                .0,
            1.into(),
        );
    }
}

fn sync_docs(source: &mut AutoCommit, target: &mut AutoCommit) {
    let mut source_state = State::new();
    let mut target_state = State::new();
    for _ in 0..10 {
        let mut exchanged = false;
        if let Some(message) = source.sync().generate_sync_message(&mut source_state) {
            target
                .sync()
                .receive_sync_message(&mut target_state, message)
                .unwrap();
            exchanged = true;
        }
        if let Some(message) = target.sync().generate_sync_message(&mut target_state) {
            source
                .sync()
                .receive_sync_message(&mut source_state, message)
                .unwrap();
            exchanged = true;
        }
        if !exchanged {
            return;
        }
    }
    panic!("sync did not settle");
}

fn puts_x(patches: &[Patch]) -> bool {
    patches.iter().any(|patch| {
        matches!(&patch.action, PatchAction::PutMap { key, value, .. }
            if patch.obj == ROOT && key == "x" && value.0 == 1.into())
    })
}

fn restores_existing_values(mode: Import) {
    let mut case = PendingMask::new();
    assert!(case.doc.diff(&[], &case.original_heads).is_empty());
    let mut view = case.doc.hydrate(ROOT, None).unwrap();
    case.doc.update_diff_cursor();
    case.import_boundary(mode);

    // Patches must include restoration of previously imported operations, not
    // just the operations contained in the newly arriving change.
    let patches = case.doc.diff_incremental();
    assert!(puts_x(&patches), "missing restoration: {patches:?}");
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(view, case.doc.hydrate(ROOT, None).unwrap());
    assert!(case.doc.diff_incremental().is_empty());

    // Both arguments of the historical diff still identify the same heads,
    // but their visible state has changed with the resolution. Repeating the
    // diff must reflect that change.
    let patches = case.doc.diff(&[], &case.original_heads);
    assert!(puts_x(&patches), "historical view must now include x");
    let mut view = case.doc.hydrate(ROOT, Some(&[])).unwrap();
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(
        view,
        case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap()
    );
}

fn isolated_restoration_composes_with_integration(mode: Import) {
    for drain_before_integrating in [false, true] {
        // The underlying document has x=2, but the pinned view must restore
        // x=1 and must not expose either `future` or the boundary's `ack`.
        let mut case = PendingMask::with_later_changes(true);
        case.doc.isolate(&case.original_heads);
        let mut view = case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap();
        case.doc.update_diff_cursor();
        case.import_boundary(mode);
        assert_eq!(case.doc.get_heads(), case.original_heads);
        assert_eq!(case.doc.get(ROOT, "x").unwrap().unwrap().0, 1.into());
        assert!(case.doc.get(ROOT, "future").unwrap().is_none());
        assert!(case.doc.get(ROOT, "ack").unwrap().is_none());
        // A historical comparison uses the resolved write-frontier state on both
        // sides, and must neither return nor consume the recorded restoration.
        assert!(case
            .doc
            .diff(&case.original_heads, &case.original_heads)
            .is_empty());
        assert!(case
            .doc
            .diff_obj(&ROOT, &case.original_heads, &case.original_heads, true)
            .unwrap()
            .is_empty());

        if drain_before_integrating {
            let patches = case.doc.diff_incremental();
            assert!(puts_x(&patches), "missing pinned restoration: {patches:?}");
            view.apply_patches(ENCODING, patches).unwrap();
            assert_eq!(
                view,
                case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap()
            );
            assert!(case.doc.diff_incremental().is_empty());
        }
        case.doc.integrate();
        view.apply_patches(ENCODING, case.doc.diff_incremental())
            .unwrap();
        assert_eq!(view, case.doc.hydrate(ROOT, None).unwrap());
        assert_eq!(case.doc.get(ROOT, "x").unwrap().unwrap().0, 2.into());
        assert!(case.doc.diff_incremental().is_empty());
    }
}

fn resolution_outside_isolation_still_updates_indexes(mode: Import) {
    for tracking in [false, true] {
        let mut case = PendingMask::with_later_changes(true);
        case.doc.isolate(&[]);
        if tracking {
            case.doc.update_diff_cursor();
        }
        case.import_boundary(mode);
        assert!(case.doc.diff_incremental().is_empty());

        // The pinned clock did not change. The full document's indexes must
        // nevertheless reflect the resolution, even with tracking disabled.
        let heads = case.doc.document().get_heads();
        let patches = case.doc.diff(&[], &heads);
        let mut full_view = case.doc.hydrate(ROOT, Some(&[])).unwrap();
        full_view.apply_patches(ENCODING, patches).unwrap();
        assert_eq!(full_view, case.doc.hydrate(ROOT, Some(&heads)).unwrap());
        assert_eq!(
            case.doc.get_at(ROOT, "x", &heads).unwrap().unwrap().0,
            2.into()
        );

        let mut isolated_view = case.doc.hydrate(ROOT, Some(&[])).unwrap();
        case.doc.integrate();
        isolated_view
            .apply_patches(ENCODING, case.doc.diff_incremental())
            .unwrap();
        assert_eq!(isolated_view, full_view);
    }
}

// Keep each entry point and invariant independently runnable, so an early
// failure cannot hide missing handling in the other import implementations.
macro_rules! pending_import_tests {
    ($module:ident, $mode:expr) => {
        mod $module {
            use super::*;

            #[test]
            fn restores_existing_values() {
                super::restores_existing_values($mode);
            }

            #[test]
            fn isolated_restoration_composes_with_integration() {
                super::isolated_restoration_composes_with_integration($mode);
            }

            #[test]
            fn resolution_outside_isolation_still_updates_indexes() {
                super::resolution_outside_isolation_still_updates_indexes($mode);
            }
        }
    };
}

pending_import_tests!(apply_changes_batch, Import::Batch);
pending_import_tests!(load_incremental, Import::Load);
pending_import_tests!(load_document, Import::LoadDocument);
pending_import_tests!(sync, Import::Sync);

#[test]
fn pending_resolution_updates_current_state_indexes() {
    let mut case = PendingMask::new();
    case.import_boundary(Import::Batch);
    case.doc.reset_diff_cursor();
    let heads = case.doc.get_heads();

    // This uses the current-state fast path without pending patch events.
    // Its view must agree with the writer-frontier-aware reads.
    let patches = case.doc.diff(&[], &heads);
    assert!(puts_x(&patches), "stale current-state indexes: {patches:?}");
}

#[test]
fn incoming_boundary_change_is_not_logged_as_masked() {
    let author = Author::try_from("aaaa").unwrap();
    let mut source = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(Author::try_from("bbbb").unwrap()))
        .with_actor(ActorId::from(vec![0x80]));
    source.put(ROOT, "base", true).unwrap();
    source.commit();
    let mut target = source.clone();

    source.set_author(Some(author.clone()));
    source.set_actor(ActorId::from(vec![0x10]));
    source.put(ROOT, "x", 1).unwrap();
    let boundary_heads = source.get_heads();
    let boundary = source.get_last_local_change().unwrap();
    target.mask_author(author, &boundary_heads);
    let mut view = target.hydrate(ROOT, None).unwrap();
    target.update_diff_cursor();

    // Unlike the other pending tests, no existing operations need restoring.
    // The incoming change itself is allowed by the boundary it resolves, so
    // its ops must not retain a masked flag computed before that resolution.
    target.apply_changes([boundary]).unwrap();
    assert_eq!(
        target
            .get_at(ROOT, "x", &boundary_heads)
            .unwrap()
            .unwrap()
            .0,
        1.into()
    );
    let patches = target.diff_incremental();
    assert!(puts_x(&patches), "boundary change was hidden: {patches:?}");
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(view, target.hydrate(ROOT, None).unwrap());
}

#[test]
fn locally_reconstructed_pending_boundary_resolves_at_commit() {
    let author = Author::try_from("aaaa").unwrap();
    let base = AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
    let mut source = base.clone();
    source.put(ROOT, "boundary", 42).unwrap();
    source.commit();
    let heads = source.get_heads();

    let mut receiver = base;
    receiver.mask_author(author, &heads);
    receiver.update_diff_cursor();
    receiver.put(ROOT, "boundary", 42).unwrap();
    receiver.commit();
    assert_eq!(
        receiver.get_heads(),
        heads,
        "probe must reproduce the exact boundary hash"
    );
    assert!(
        receiver.get(ROOT, "boundary").unwrap().is_some(),
        "inclusive boundary must become visible in the committing call"
    );
    let patches = receiver.diff_incremental();
    assert!(
        patches
            .iter()
            .any(|p| matches!(&p.action, PatchAction::PutMap { key, .. } if key == "boundary")),
        "committing must log the boundary's ops becoming visible: {patches:?}"
    );
}

// ==================== boundary edits of restored objects ====================

fn pending_subtree_restoration(isolated: bool) {
    let author = Author::try_from("aaaa").unwrap();
    let mut source = AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
    let text = source.put_object(ROOT, "text", ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "abc").unwrap();
    source.commit();
    source.set_author(Some(Author::try_from("bbbb").unwrap()));
    source.splice_text(&text, 3, 0, "X").unwrap();
    let observed_heads = source.get_heads();
    let original_changes = source.get_changes(&[]);
    // The boundary also edits the restored object. Full exposure must replace
    // the batch's child delta, not duplicate it; isolation must exclude it.
    source.splice_text(&text, 4, 0, "Y").unwrap();
    source.put(ROOT, "ack", true).unwrap();
    let boundary_heads = source.get_heads();
    let boundary = source.get_last_local_change().unwrap();

    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    doc.apply_changes(original_changes).unwrap();
    if isolated {
        doc.isolate(&observed_heads);
    }
    doc.mask_author(author, &boundary_heads);
    doc.update_diff_cursor();
    let mut view = doc.hydrate(ROOT, Some(&observed_heads)).unwrap();
    doc.apply_changes([boundary]).unwrap();
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    let heads = doc.get_heads();
    assert_eq!(view, doc.hydrate(ROOT, Some(&heads)).unwrap());
    assert_eq!(
        doc.text(&text).unwrap(),
        if isolated { "abcX" } else { "abcXY" }
    );
    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn pending_resolution_restores_other_author_children() {
    pending_subtree_restoration(false);
}

#[test]
fn isolated_pending_resolution_restores_other_author_children() {
    pending_subtree_restoration(true);
}

#[test]
fn isolated_reveal_restores_only_the_pinned_subtree_contents() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "abc").unwrap();
    doc.commit();
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.splice_text(&text, 3, 0, "X").unwrap();
    let heads = doc.get_heads();
    doc.splice_text(&text, 4, 0, "future").unwrap();
    doc.commit();
    doc.isolate(&heads);
    doc.mask_author(author.clone(), &[]);
    let mut view = doc.hydrate(ROOT, Some(&heads)).unwrap();
    doc.update_diff_cursor();
    doc.reveal_author(&author);
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(doc.text(&text).unwrap(), "abcX");
    assert_eq!(view, doc.hydrate(ROOT, Some(&heads)).unwrap());
    assert!(doc.diff_incremental().is_empty());
}

// ======================= isolated imports =======================

#[test]
fn queued_boundary_restores_only_the_isolated_view_when_dependency_arrives() {
    let author = Author::try_from("aaaa").unwrap();
    let mut source = AutoCommit::new_with_encoding(ENCODING)
        .with_actor(ActorId::from(vec![0x80]))
        .with_author(Some(author.clone()));
    source.put(ROOT, "x", 1).unwrap();
    let pinned = source.get_heads();
    let original = source.get_last_local_change().unwrap();
    source.put(ROOT, "x", 2).unwrap();
    source.commit();
    let dependency = source.get_last_local_change().unwrap();
    source.set_actor(ActorId::from(vec![0x10]));
    source.set_author(Some(Author::try_from("bbbb").unwrap()));
    source.put(ROOT, "ack", true).unwrap();
    let boundary_heads = source.get_heads();
    let boundary = source.get_last_local_change().unwrap();

    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    doc.apply_changes([original]).unwrap();
    doc.isolate(&pinned);
    doc.mask_author(author, &boundary_heads);
    let mut view = doc.hydrate(ROOT, Some(&pinned)).unwrap();
    doc.update_diff_cursor();

    doc.apply_changes([boundary.clone()]).unwrap();
    assert!(doc.diff_incremental().is_empty());
    assert!(doc.get(ROOT, "x").unwrap().is_none());

    doc.apply_changes_batch([dependency]).unwrap();
    assert_eq!(doc.get_heads(), pinned);
    assert_eq!(doc.get(ROOT, "x").unwrap().unwrap().0, 1.into());
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(ROOT, Some(&pinned)).unwrap());

    // Re-importing an already resolved boundary is not another transition.
    doc.apply_changes([boundary]).unwrap();
    assert!(doc.diff_incremental().is_empty());
    doc.integrate();
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(ROOT, None).unwrap());
}

#[test]
fn restored_subtree_paths_survive_pending_and_subsequent_isolated_edits() {
    let masked = Author::try_from("aaaa").unwrap();
    let allowed = Author::try_from("bbbb").unwrap();
    let mut source = AutoCommit::new_with_encoding(ENCODING)
        .with_actor(ActorId::from(vec![0xe0]))
        .with_author(Some(allowed.clone()));
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "base").unwrap();
    source.commit();

    source.set_actor(ActorId::from(vec![0x80]));
    source.set_author(Some(masked.clone()));
    let text = source.insert_object(&list, 1, ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "abc").unwrap();
    source.commit();

    source.set_actor(ActorId::from(vec![0x40]));
    source.set_author(Some(allowed.clone()));
    source.splice_text(&text, 3, 0, "X").unwrap();
    let pinned = source.get_heads();

    // These known operations are later than the pinned view. Restoring them
    // would give the text both the wrong contents and the wrong list path.
    source.set_actor(ActorId::from(vec![0x80]));
    source.set_author(Some(masked.clone()));
    source.insert(&list, 0, "future").unwrap();
    source.splice_text(&text, 4, 0, "future").unwrap();
    source.commit();
    let original = source.get_changes(&[]);

    // Inserting this actor reorders the actor table during resolution.
    source.set_actor(ActorId::from(vec![0x10]));
    source.set_author(Some(allowed.clone()));
    source.splice_text(&text, 10, 0, "Y").unwrap();
    let boundary_heads = source.get_heads();
    let boundary = source.get_last_local_change().unwrap();

    let mut doc = AutoCommit::new_with_encoding(ENCODING)
        .with_actor(ActorId::from(vec![0xc0]))
        .with_author(Some(allowed));
    doc.apply_changes(original).unwrap();
    doc.isolate(&pinned);
    doc.mask_author(masked, &boundary_heads);
    let mut view = doc.hydrate(ROOT, Some(&pinned)).unwrap();
    doc.update_diff_cursor();

    // Leave a pending patch, and advance the isolation heads with a local edit.
    doc.insert(&list, 0, "prefix").unwrap();
    let local_heads = doc.get_heads();
    doc.apply_changes([boundary]).unwrap();
    assert_eq!(doc.get_heads(), local_heads);
    assert_eq!(doc.text(&text).unwrap(), "abcX");

    // Restoration must already have finalized its paths and exposed contents
    // before these later events are added, even though no patches were drained.
    doc.insert(&list, 0, "later").unwrap();
    doc.splice_text(&text, 4, 0, "local").unwrap();
    let heads = doc.get_heads();
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(ROOT, Some(&heads)).unwrap());
    assert_eq!(doc.text(&text).unwrap(), "abcXlocal");

    doc.integrate();
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(ROOT, None).unwrap());
    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn loading_into_an_empty_isolated_document_does_not_log_the_loaded_state() {
    for full_document in [false, true] {
        for tracking in [false, true] {
            let author = Author::try_from("aaaa").unwrap();
            let mut source =
                AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
            source.put(ROOT, "x", 1).unwrap();
            let boundary = source.get_heads();
            let bytes = if full_document {
                source.save()
            } else {
                source.save_after(&[])
            };
            let mut doc = AutoCommit::new_with_encoding(ENCODING);
            doc.isolate(&[]);
            doc.mask_author(author, &boundary);
            let mut view = doc.hydrate(ROOT, Some(&[])).unwrap();
            if tracking {
                doc.update_diff_cursor();
            }

            assert!(doc.load_incremental(&bytes).unwrap() > 0);
            assert!(doc.get_heads().is_empty());
            assert!(doc.get(ROOT, "x").unwrap().is_none());
            assert!(doc.diff_incremental().is_empty());

            doc.integrate();
            view.apply_patches(ENCODING, doc.diff_incremental())
                .unwrap();
            assert_eq!(view, source.hydrate(ROOT, None).unwrap());
        }
    }
}

#[test]
fn isolation_preserves_empty_document_load_validation() {
    let mut source = AutoCommit::new();
    source.put(ROOT, "x", 1).unwrap();
    let mut bytes = source.save();
    bytes[0] ^= 0xff;

    let mut current = AutoCommit::new();
    let mut isolated = AutoCommit::new();
    isolated.isolate(&[]);
    isolated.update_diff_cursor();
    let expected = current.load_incremental(&bytes);
    assert!(expected.is_err());
    assert_eq!(isolated.load_incremental(&bytes), expected);
    assert!(isolated.diff_incremental().is_empty());
}

/// A missing head that adds no history for the masked author must not
/// disturb contributions already visible through a known head.
#[test]
fn partially_unknown_multi_head_boundary_preserves_known_history() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let h1 = a.get_heads()[0];

    // Bob's acknowledgement, not delivered to the observer yet.
    let mut b = a.fork().with_author(Some(bob));
    b.put(ROOT, "z", 9).unwrap();
    b.commit();
    let h2 = b.get_heads()[0];

    let mut obs = a.fork();
    obs.mask_author(alice, &[h1, h2]);
    assert_eq!(obs.get(ROOT, "x").unwrap().unwrap().0, 1.into());
    let mut view = obs.hydrate(ROOT, None).unwrap();
    obs.update_diff_cursor();

    // Delivering H2 completes the boundary without changing Alice's bound.
    obs.merge(&mut b).unwrap();
    assert_eq!(obs.get(ROOT, "x").unwrap().unwrap().0, 1.into());
    let patches = obs.diff_incremental();
    assert!(!puts_x(&patches), "x was already visible: {patches:?}");
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(view, obs.hydrate(ROOT, None).unwrap());
}

/// Each actor is bounded by the known heads independently. Resolving one
/// missing head can reveal an existing actor's history even while another
/// head remains pending, but never reveals post-boundary changes.
#[test]
fn partially_known_boundary_expands_per_actor_on_import() {
    for mode in [
        Import::Batch,
        Import::Load,
        Import::LoadDocument,
        Import::Sync,
    ] {
        let alice = Author::try_from("aaaa").unwrap();
        let bob = Author::try_from("bbbb").unwrap();
        let mut a = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(alice.clone()))
            .with_actor(ActorId::from(vec![0x80]));
        a.put(ROOT, "x", 1).unwrap();
        let h1 = a.get_heads()[0];

        // A second actor for Alice, concurrent with the first actor's later op.
        let mut b = a.fork().with_actor(ActorId::from(vec![0x90]));
        b.put(ROOT, "y", 2).unwrap();
        let before_boundary = b.get_heads();
        let original = b.get_changes(&[]);
        a.put(ROOT, "x_later", true).unwrap();
        a.commit();

        // Bob's acknowledgement bounds the second actor, but is initially absent.
        b.set_author(Some(bob));
        b.set_actor(ActorId::from(vec![0x10]));
        b.put(ROOT, "ack", true).unwrap();
        let h2 = b.get_heads()[0];
        b.set_author(Some(alice.clone()));
        b.set_actor(ActorId::from(vec![0x90]));
        b.put(ROOT, "y_later", true).unwrap();
        b.commit();

        let mut obs = a.fork();
        obs.apply_changes(original).unwrap();
        // A third head stays missing even after H2 arrives.
        obs.mask_author(alice, &[h1, h2, ChangeHash([7; 32])]);
        assert_eq!(obs.get(ROOT, "x").unwrap().unwrap().0, 1.into());
        for key in ["y", "x_later", "y_later"] {
            assert!(obs.get(ROOT, key).unwrap().is_none(), "{key}");
        }
        let mut view = obs.hydrate(ROOT, None).unwrap();
        obs.update_diff_cursor();

        match mode {
            Import::Batch => obs
                .apply_changes_batch(b.get_changes(&before_boundary))
                .unwrap(),
            Import::Load => {
                obs.load_incremental(&b.save_after(&before_boundary))
                    .unwrap();
            }
            Import::LoadDocument => {
                obs.load_incremental(&b.save()).unwrap();
            }
            Import::Sync => sync_docs(&mut b, &mut obs),
        }

        assert_eq!(obs.get(ROOT, "x").unwrap().unwrap().0, 1.into());
        assert_eq!(obs.get(ROOT, "y").unwrap().unwrap().0, 2.into());
        for key in ["x_later", "y_later"] {
            assert!(obs.get(ROOT, key).unwrap().is_none(), "{key}");
        }
        let patches = obs.diff_incremental();
        assert!(
            patches.iter().any(|patch| {
                matches!(&patch.action, PatchAction::PutMap { key, value, .. }
                    if patch.obj == ROOT && key == "y" && value.0 == 2.into())
            }),
            "missing restoration: {patches:?}"
        );
        view.apply_patches(ENCODING, patches).unwrap();
        assert_eq!(view, obs.hydrate(ROOT, None).unwrap());
        assert!(obs.diff_incremental().is_empty());
    }
}

/// The pending set is read-only between derivations: witnessing the
/// boundary resolves it in the same `apply_changes` call (nothing pops it
/// early), and a later duplicate delivery of the boundary change is not
/// another visibility transition (the installer's unchanged-mask guard
/// makes any re-run free).
#[test]
fn pending_set_is_read_only_between_derivations() {
    let mut case = PendingMask::new();
    case.doc.update_diff_cursor();

    case.doc.apply_changes([case.boundary.clone()]).unwrap();
    assert_eq!(
        case.doc
            .get_at(ROOT, "x", &case.original_heads)
            .unwrap()
            .unwrap()
            .0,
        1.into(),
        "the first delivery must resolve the boundary"
    );
    let patches = case.doc.diff_incremental();
    assert!(puts_x(&patches), "missing restoration: {patches:?}");

    case.doc.apply_changes([case.boundary.clone()]).unwrap();
    assert!(
        case.doc.diff_incremental().is_empty(),
        "a duplicate delivery of a resolved boundary must not log a transition"
    );
}

/// `reveal_author` clears the author's heads from the pending set: importing the
/// former boundary afterwards must not log a spurious visibility transition
/// (only the imported content itself appears in the incremental diff).
#[test]
fn reveal_clears_pending_heads() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    a.put(ROOT, "x", 1).unwrap();
    a.commit();

    // Bob's change is the (not yet delivered) write-frontier boundary.
    let mut b = a.fork().with_author(Some(bob));
    b.put(ROOT, "z", 9).unwrap();
    b.commit();
    let h2 = b.get_heads()[0];

    let mut obs = a.fork();
    obs.mask_author(alice.clone(), &[h2]);
    assert!(obs.get(ROOT, "x").unwrap().is_none());
    obs.reveal_author(&alice);
    assert_eq!(
        obs.get(ROOT, "x").unwrap().and_then(|v| v.0.as_i64()),
        Some(1)
    );

    obs.update_diff_cursor();
    obs.merge(&mut b).unwrap();
    let patches = obs.diff_incremental();
    // Only the imported content; no transition re-publishing x.
    assert!(
        patches.iter().all(|patch| {
            matches!(&patch.action, PatchAction::PutMap { key, .. }
                if patch.obj == ROOT && key == "z")
        }),
        "importing a stale boundary must not log a visibility transition: {patches:?}"
    );
}
