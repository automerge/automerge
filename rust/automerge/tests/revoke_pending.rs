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
        let author = Author::try_from("aaaa").unwrap();
        let mut source = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(author.clone()))
            .with_actor(ActorId::from(vec![0x80]));
        source.put(ROOT, "x", 1).unwrap();
        let original_heads = source.get_heads();
        let original = source.get_last_local_change().unwrap();

        source.set_author(Some(Author::try_from("bbbb").unwrap()));
        source.set_actor(ActorId::from(vec![0x10]));
        source.put(ROOT, "ack", true).unwrap();
        let boundary_heads = source.get_heads();
        let boundary = source.get_last_local_change().unwrap();

        let mut doc = AutoCommit::new_with_encoding(ENCODING);
        doc.apply_changes([original]).unwrap();
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

fn invalidates_historical_diff_cache(mode: Import) {
    let mut case = PendingRevocation::new();
    assert!(case.doc.diff(&[], &case.original_heads).is_empty());
    case.import_boundary(mode);

    // Both arguments still identify the same historical heads, but their
    // visible state has changed. Compare the cached result with a fresh diff.
    let cached = case.doc.diff(&[], &case.original_heads);
    case.doc.reset_diff_cursor();
    let fresh = case.doc.diff(&[], &case.original_heads);
    assert!(puts_x(&fresh), "historical view must now include x");
    assert_eq!(cached, fresh);
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
            fn invalidates_historical_diff_cache() {
                super::invalidates_historical_diff_cache($mode);
            }

            #[test]
            fn records_isolated_visibility_changes() {
                super::records_isolated_visibility_changes($mode);
            }
        }
    };
}

pending_import_tests!(apply_changes, Import::Apply);
pending_import_tests!(apply_changes_batch, Import::Batch);
pending_import_tests!(merge, Import::Merge);
pending_import_tests!(load_incremental, Import::Load);
pending_import_tests!(sync, Import::Sync);

#[test]
fn pending_resolution_updates_current_state_indexes() {
    let mut case = PendingRevocation::new();
    case.import_boundary(Import::Apply);
    case.doc.reset_diff_cursor();
    let heads = case.doc.get_heads();

    // This uses the current-state fast path, with neither a cached diff nor
    // pending patch events. Its view must agree with the revocation-aware reads.
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
