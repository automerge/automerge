use automerge::{
    sync::{State, SyncDoc},
    transaction::Transactable,
    ActorId, Author, AutoCommit, ObjType, ReadDoc, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

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
    doc.revoke(author, &boundary_heads);
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
    let revoked = Author::try_from("aaaa").unwrap();
    let allowed = Author::try_from("bbbb").unwrap();
    let mut source = AutoCommit::new_with_encoding(ENCODING)
        .with_actor(ActorId::from(vec![0xe0]))
        .with_author(Some(allowed.clone()));
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "base").unwrap();
    source.commit();

    source.set_actor(ActorId::from(vec![0x80]));
    source.set_author(Some(revoked.clone()));
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
    source.set_author(Some(revoked.clone()));
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
    doc.revoke(revoked, &boundary_heads);
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
            doc.revoke(author, &boundary);
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

#[test]
fn syncing_into_an_empty_isolated_document_does_not_log_the_loaded_state() {
    let mut source = AutoCommit::new_with_encoding(ENCODING);
    source.put(ROOT, "x", 1).unwrap();
    source.commit();
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    doc.isolate(&[]);
    let mut view = doc.hydrate(ROOT, Some(&[])).unwrap();
    doc.update_diff_cursor();
    let mut source_state = State::new();
    let mut target_state = State::new();
    for _ in 0..10 {
        let mut exchanged = false;
        if let Some(message) = source.sync().generate_sync_message(&mut source_state) {
            doc.sync()
                .receive_sync_message(&mut target_state, message)
                .unwrap();
            exchanged = true;
        }
        if let Some(message) = doc.sync().generate_sync_message(&mut target_state) {
            source
                .sync()
                .receive_sync_message(&mut source_state, message)
                .unwrap();
            exchanged = true;
        }
        assert!(doc.diff_incremental().is_empty());
        if !exchanged {
            break;
        }
    }
    assert!(doc.get_heads().is_empty());
    doc.integrate();
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, source.hydrate(ROOT, None).unwrap());
}
