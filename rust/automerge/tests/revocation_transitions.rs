use automerge::{
    transaction::Transactable, ActorId, Author, Automerge, ObjType, PatchLog, PatchLogMismatch,
    TextEncoding, ROOT,
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
