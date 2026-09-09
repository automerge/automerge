use automerge::{
    hydrate, transaction::Transactable, ActorId, Author, Automerge, ObjId, ObjType, PatchLog,
    ReadDoc, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

fn authored_text() -> (Automerge, Author<'static>, ObjId) {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from(vec![0x80]));
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    tx.commit();
    (doc, author, text)
}

fn apply_log(doc: &Automerge, log: &mut PatchLog, view: &mut hydrate::Value) {
    view.apply_patches(ENCODING, doc.make_patches(log)).unwrap();
    assert_eq!(*view, doc.hydrate(None));
}

#[test]
fn caller_owned_log_preserves_repeated_revocation_transitions() {
    let (mut doc, author, text) = authored_text();
    doc.revoke(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();

    // All three transitions belong to one patch interval. Sorting their raw
    // events together must not duplicate the restored text.
    doc.unrevoke(&author, &mut log).unwrap();
    doc.revoke(author.clone(), &[], &mut log).unwrap();
    doc.unrevoke(&author, &mut log).unwrap();
    assert_eq!(doc.text(&text).unwrap(), "abc");
    apply_log(&doc, &mut log, &mut view);
}

#[test]
fn caller_owned_log_preserves_local_edits_before_revocation() {
    let (mut doc, author, text) = authored_text();
    let mut view = doc.hydrate(None);
    let mut tx = doc.transaction_log_patches(PatchLog::active()).unwrap();
    tx.splice_text(&text, 3, 0, "X").unwrap();
    let (_, mut log) = tx.commit();

    // The pending append and the restoration of that same append must not both
    // be applied to the newly restored text object.
    doc.revoke(author.clone(), &[], &mut log).unwrap();
    doc.unrevoke(&author, &mut log).unwrap();
    assert_eq!(doc.text(&text).unwrap(), "abcX");
    apply_log(&doc, &mut log, &mut view);
}

#[test]
fn caller_owned_revocation_log_tracks_actor_reordering() {
    let (mut doc, author, text) = authored_text();
    doc.revoke(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();

    // The fresh revocation log already references actor 0x80. A subsequent
    // transaction inserts actor 0x00 before it in the actor table. These fixed
    // identities make the required reindexing deterministic.
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.set_actor(ActorId::from(vec![0x00]));
    let mut tx = doc.transaction_log_patches(log).unwrap();
    tx.put(ROOT, "other", 1).unwrap();
    let (_, mut log) = tx.commit();

    assert_eq!(doc.text(&text).unwrap(), "abc");
    assert_eq!(doc.get(ROOT, "other").unwrap().unwrap().0, 1.into());
    apply_log(&doc, &mut log, &mut view);
}

#[test]
fn unrevoke_restores_unchanged_text_from_other_authors() {
    let (mut doc, author, text) = authored_text();
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.set_actor(ActorId::from(vec![0x10]));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 3, 0, "X").unwrap();
    tx.commit();
    let mut view = doc.hydrate(None);

    let mut log = PatchLog::active();
    doc.revoke(author.clone(), &[], &mut log).unwrap();
    assert!(doc.get(ROOT, "text").unwrap().is_none());
    apply_log(&doc, &mut log, &mut view);

    // Start a new interval: this tests full object exposure, independently of
    // finalization of any earlier log. The other author's X has not changed
    // visibility itself, but must be included when its parent is restored.
    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();
    assert_eq!(doc.text(&text).unwrap(), "abcX");
    apply_log(&doc, &mut log, &mut view);
}

#[test]
fn unrevoke_restores_unchanged_map_entries_from_other_authors() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from(vec![0x80]));
    let mut tx = doc.transaction();
    let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
    tx.put(&map, "original", true).unwrap();
    tx.commit();
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.set_actor(ActorId::from(vec![0x10]));
    let mut tx = doc.transaction();
    tx.put(&map, "other_author", true).unwrap();
    tx.commit();
    let mut view = doc.hydrate(None);

    let mut log = PatchLog::active();
    doc.revoke(author.clone(), &[], &mut log).unwrap();
    assert!(doc.get(ROOT, "map").unwrap().is_none());
    apply_log(&doc, &mut log, &mut view);

    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();
    assert_eq!(doc.get(&map, "original").unwrap().unwrap().0, true.into());
    assert_eq!(
        doc.get(&map, "other_author").unwrap().unwrap().0,
        true.into()
    );
    apply_log(&doc, &mut log, &mut view);
}
