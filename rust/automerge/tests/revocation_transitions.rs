use automerge::{
    transaction::Transactable, ActorId, Author, AutoCommit, Automerge, ObjType, PatchLog,
    PatchLogMismatch, ReadDoc, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

#[test]
fn mismatched_revocation_logs_leave_visibility_and_log_unchanged() {
    // Exercise missing actors on either side of the target's actor, as well as
    // an empty target. A missing trailing actor must not be silently accepted.
    for foreign_actor in [0x00, 0xff] {
        for empty_target in [false, true] {
            let mut source = Automerge::new().with_actor(ActorId::from(vec![foreign_actor]));
            let mut tx = source.transaction_log_patches(PatchLog::active()).unwrap();
            tx.put(ROOT, "foreign", true).unwrap();
            let (_, mut log) = tx.commit();
            let expected_patches = source.make_patches(&mut log);

            let author = Author::try_from("aaaa").unwrap();
            let mut target = Automerge::new()
                .with_author(Some(author.clone()))
                .with_actor(ActorId::from(vec![0x80]));
            if !empty_target {
                let mut tx = target.transaction();
                tx.put(ROOT, "x", 1).unwrap();
                tx.commit();
            }
            let heads = target.get_heads();
            let visible = target.hydrate(None);
            assert_eq!(
                target.revoke(author.clone(), &[], &mut log),
                Err(PatchLogMismatch)
            );
            assert!(target.get_revocations().is_empty());
            assert_eq!(target.hydrate(None), visible);
            assert_eq!(target.get_heads(), heads);
            assert_eq!(source.make_patches(&mut log), expected_patches);

            target
                .revoke(author.clone(), &[], &mut PatchLog::inactive())
                .unwrap();
            let revocations = target.get_revocations();
            let hidden = target.hydrate(None);
            assert_eq!(target.unrevoke(&author, &mut log), Err(PatchLogMismatch));
            assert_eq!(target.get_revocations(), revocations);
            assert_eq!(target.hydrate(None), hidden);
            assert_eq!(target.get_heads(), heads);
            assert_eq!(source.make_patches(&mut log), expected_patches);
        }
    }
}

#[test]
fn restored_paths_are_finalized_before_subsequent_list_edits() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new_with_encoding(ENCODING);
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.commit();
    doc.set_author(Some(author.clone()));
    let mut tx = doc.transaction();
    let text = tx.insert_object(&list, 0, ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    tx.commit();
    doc.revoke(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);

    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();
    let mut tx = doc.transaction_log_patches(log).unwrap();
    // Move the restored object's path and edit it before draining any patches.
    tx.insert(&list, 0, "prefix").unwrap();
    tx.splice_text(&text, 3, 0, "X").unwrap();
    let (_, mut log) = tx.commit();
    view.apply_patches(ENCODING, doc.make_patches(&mut log))
        .unwrap();
    assert_eq!(view, doc.hydrate(None));
}

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
    doc.revoke(author, &boundary_heads);
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
fn isolated_unrevoke_restores_only_the_pinned_subtree_contents() {
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
    doc.revoke(author.clone(), &[]);
    let mut view = doc.hydrate(ROOT, Some(&heads)).unwrap();
    doc.update_diff_cursor();
    doc.unrevoke(&author);
    view.apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(doc.text(&text).unwrap(), "abcX");
    assert_eq!(view, doc.hydrate(ROOT, Some(&heads)).unwrap());
    assert!(doc.diff_incremental().is_empty());
}
