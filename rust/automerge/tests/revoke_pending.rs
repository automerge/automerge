use automerge::{
    sync::{State, SyncDoc},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Change, ChangeHash, Patch, PatchAction, ReadDoc, TextEncoding,
    ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

#[derive(Clone, Copy)]
enum Import {
    Apply,
    Batch,
    Merge,
    Load,
    LoadDocument,
    Sync,
}

struct PendingRevocation {
    doc: AutoCommit,
    source: AutoCommit,
    original_heads: Vec<ChangeHash>,
    boundary: Change,
}

impl PendingRevocation {
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
        doc.revoke(author, &boundary_heads);
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
            Import::Apply => self.doc.apply_changes([self.boundary.clone()]).unwrap(),
            Import::Batch => self
                .doc
                .apply_changes_batch([self.boundary.clone()])
                .unwrap(),
            Import::Merge => {
                self.doc.merge(&mut self.source).unwrap();
            }
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
    let mut case = PendingRevocation::new();
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
}

fn historical_diff_reflects_resolved_revocation(mode: Import) {
    let mut case = PendingRevocation::new();
    assert!(case.doc.diff(&[], &case.original_heads).is_empty());
    case.import_boundary(mode);

    // Both arguments still identify the same historical heads, but their
    // visible state has changed. Repeating the diff must reflect that change.
    let patches = case.doc.diff(&[], &case.original_heads);
    assert!(puts_x(&patches), "historical view must now include x");
    let mut view = case.doc.hydrate(ROOT, Some(&[])).unwrap();
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(
        view,
        case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap()
    );
}

fn records_isolated_visibility_changes(mode: Import) {
    let mut case = PendingRevocation::new();
    case.doc.isolate(&case.original_heads);
    let mut view = case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap();
    case.doc.update_diff_cursor();
    case.import_boundary(mode);

    // Importing changes normally leaves an isolated view alone. Resolving a
    // revocation boundary is different: visibility at those same heads changes.
    assert_eq!(case.doc.get_heads(), case.original_heads);
    assert_eq!(case.doc.get(ROOT, "x").unwrap().unwrap().0, 1.into());
    // A historical comparison uses the resolved revocation state on both
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
    let patches = case.doc.diff_incremental();
    assert!(
        puts_x(&patches),
        "missing isolated restoration: {patches:?}"
    );
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(
        view,
        case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap()
    );
    assert!(case.doc.diff_incremental().is_empty());
}

fn isolated_restoration_composes_with_integration(mode: Import) {
    for drain_before_integrating in [false, true] {
        // The underlying document has x=2, but the pinned view must restore
        // x=1 and must not expose either `future` or the boundary's `ack`.
        let mut case = PendingRevocation::with_later_changes(true);
        case.doc.isolate(&case.original_heads);
        let mut view = case.doc.hydrate(ROOT, Some(&case.original_heads)).unwrap();
        case.doc.update_diff_cursor();
        case.import_boundary(mode);
        assert_eq!(case.doc.get_heads(), case.original_heads);
        assert_eq!(case.doc.get(ROOT, "x").unwrap().unwrap().0, 1.into());
        assert!(case.doc.get(ROOT, "future").unwrap().is_none());
        assert!(case.doc.get(ROOT, "ack").unwrap().is_none());

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
        view.apply_patches(ENCODING, case.doc.diff_incremental()).unwrap();
        assert_eq!(view, case.doc.hydrate(ROOT, None).unwrap());
        assert_eq!(case.doc.get(ROOT, "x").unwrap().unwrap().0, 2.into());
        assert!(case.doc.diff_incremental().is_empty());
    }
}

fn resolution_outside_isolation_still_updates_indexes(mode: Import) {
    for tracking in [false, true] {
        let mut case = PendingRevocation::with_later_changes(true);
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
        isolated_view.apply_patches(ENCODING, case.doc.diff_incremental()).unwrap();
        assert_eq!(isolated_view, full_view);
    }
}

// Keep each entry point and invariant independently runnable, so an early
// failure cannot hide missing handling in the other import wrappers.
macro_rules! pending_import_tests {
    ($module:ident, $mode:expr) => {
        mod $module {
            use super::*;

            #[test]
            fn restores_existing_values() {
                super::restores_existing_values($mode);
            }

            #[test]
            fn historical_diff_reflects_resolved_revocation() {
                super::historical_diff_reflects_resolved_revocation($mode);
            }

            #[test]
            fn records_isolated_visibility_changes() {
                super::records_isolated_visibility_changes($mode);
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

pending_import_tests!(apply_changes, Import::Apply);
pending_import_tests!(apply_changes_batch, Import::Batch);
pending_import_tests!(merge, Import::Merge);
pending_import_tests!(load_incremental, Import::Load);
pending_import_tests!(load_document, Import::LoadDocument);
pending_import_tests!(sync, Import::Sync);

#[test]
fn pending_resolution_updates_current_state_indexes() {
    let mut case = PendingRevocation::new();
    case.import_boundary(Import::Apply);
    case.doc.reset_diff_cursor();
    let heads = case.doc.get_heads();

    // This uses the current-state fast path without pending patch events.
    // Its view must agree with the revocation-aware reads.
    let patches = case.doc.diff(&[], &heads);
    assert!(puts_x(&patches), "stale current-state indexes: {patches:?}");
}

#[test]
fn incoming_boundary_change_is_not_logged_as_revoked() {
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
    target.revoke(author, &boundary_heads);
    let mut view = target.hydrate(ROOT, None).unwrap();
    target.update_diff_cursor();

    // Unlike the other pending tests, no existing operations need restoring.
    // The incoming change itself is allowed by the boundary it resolves, so
    // its ops must not retain a revoked flag computed before that resolution.
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
