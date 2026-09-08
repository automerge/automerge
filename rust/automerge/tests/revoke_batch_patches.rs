use automerge::{
    hydrate,
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, ObjType, Patch, PatchAction, ReadDoc, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

fn good_author() -> Author<'static> {
    Author::from(vec![0xbb])
}

fn revoked_author() -> Author<'static> {
    Author::from(vec![0xaa])
}

fn source_doc() -> AutoCommit {
    // Setting an author generates a new actor, so set the deterministic actor last.
    AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x10]))
}

fn assert_patches_reproduce_doc(mut view: hydrate::Value, doc: &AutoCommit, patches: &[Patch]) {
    view.apply_patches(ENCODING, patches.iter().cloned())
        .unwrap();
    assert_eq!(
        view,
        doc.hydrate(&ROOT, None).unwrap(),
        "incremental patches must reproduce the visible document: {patches:#?}"
    );
}

#[test]
fn revoked_incoming_list_insert_does_not_advance_patch_index() {
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap(); // R
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(revoked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.insert(&list, 0, "X").unwrap(); // XR
    source.commit();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    // Y is anchored after X, but X must occupy no space in the receiver's view.
    source.insert(&list, 1, "Y").unwrap(); // XYR
    source.commit();

    // The boundary is already imported: this is not pending revocation resolution.
    target.revoke(revoked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn revoked_existing_list_insert_does_not_advance_patch_index() {
    // Sibling of `revoked_incoming_list_insert_does_not_advance_patch_index`,
    // but the revoked "X" is *already imported* as a doc op before the batch
    // arrives. There is no incoming ChangeOp carrying a `revoked` flag for X;
    // its invisibility can only come from the active revocation clock while the
    // untangler walks the pre-existing doc ops. This is the case that a naive
    // fix (treating every doc op as unrevoked) would silently get wrong while
    // the incoming-op test still passes.
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap(); // R  (good author)
    let base = source.get_heads();

    // The revoked author inserts X ahead of R.
    source = source
        .with_author(Some(revoked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.insert(&list, 0, "X").unwrap(); // XR
    source.commit();
    let with_x = source.get_heads();

    // Clone *after* X is committed: the target holds X as a doc op, then hides
    // it by revoking the author at the boundary just before X.
    let mut target = source.clone();
    target.revoke(revoked_author(), &base);
    assert_eq!(target.length(&list), 1);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "R".into());

    // A later good-authored insert, anchored after the (hidden) X, arrives in
    // the batch on its own. X occupies no space in the receiver's view.
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.insert(&list, 1, "Y").unwrap(); // XYR
    source.commit();

    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&with_x))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn revoked_incoming_text_insert_does_not_advance_patch_index() {
    let mut source = source_doc();
    let text = source.put_object(ROOT, "text", ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "R").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(revoked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.splice_text(&text, 0, 0, "X").unwrap();
    source.commit();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.splice_text(&text, 1, 0, "Y").unwrap();
    source.commit();

    target.revoke(revoked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.text(&text).unwrap(), "YR");
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn revoked_incoming_replacement_does_not_displace_visible_insert_patch() {
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source.insert(&list, 0, "Y").unwrap();
    source.commit();
    source = source
        .with_author(Some(revoked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.put(&list, 0, "X").unwrap();
    source.commit();

    target.revoke(revoked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    // Both Y's insertion and its revoked overwrite arrive in the same batch.
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get_all(&list, 0).unwrap().len(), 1);
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

fn assert_revoked_marks_not_patched(import_marks_first: bool) {
    let mut source = source_doc();
    let text = source.put_object(ROOT, "text", ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "ab").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(revoked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source
        .mark(
            &text,
            Mark::new("bold".into(), true, 0, 2),
            ExpandMark::Both,
        )
        .unwrap();
    source.commit();
    let mark_change = source.get_last_local_change().unwrap();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.splice_text(&text, 1, 0, "X").unwrap();
    source.commit();
    let insert_change = source.get_last_local_change().unwrap();

    target.revoke(revoked_author(), &base);
    if import_marks_first {
        target.apply_changes_batch([mark_change.clone()]).unwrap();
        assert_eq!(target.text(&text).unwrap(), "ab");
        assert!(target.marks(&text).unwrap().is_empty());
    }
    target.update_diff_cursor();
    if import_marks_first {
        target.apply_changes_batch([insert_change]).unwrap();
    } else {
        target
            .apply_changes_batch([mark_change, insert_change])
            .unwrap();
    }

    assert_eq!(target.text(&text).unwrap(), "aXb");
    assert!(target.marks(&text).unwrap().is_empty());
    let patches = target.diff_incremental();
    // Hydration does not implement Mark patches; inspect both ways patches can
    // carry marks instead of applying these patches to a hydrated document.
    assert!(
        patches.iter().any(|p| p.obj == text
            && matches!(
                &p.action,
                PatchAction::SpliceText { index: 1, value, .. } if value.make_string() == "X"
            )),
        "missing allowed insertion: {patches:#?}"
    );
    assert!(
        patches.iter().all(|p| match &p.action {
            PatchAction::SpliceText { marks: Some(marks), .. } => marks.is_empty(),
            PatchAction::Mark { marks } => marks.is_empty(),
            _ => true,
        }),
        "revoked marks must not appear in patches (import_marks_first={import_marks_first}): {patches:#?}"
    );
}

#[test]
fn revoked_incoming_marks_do_not_leak_into_batch_patches() {
    assert_revoked_marks_not_patched(false);
}

#[test]
fn already_imported_revoked_marks_do_not_leak_into_allowed_insertion_patches() {
    assert_revoked_marks_not_patched(true);
}
