use automerge::{transaction::Transactable, ActorId, AutoCommit, AutomergeError, ChangeId, ROOT};

fn doc_with_changes(actor: &[u8], n: u64) -> AutoCommit {
    let mut doc = AutoCommit::new();
    doc.set_actor(ActorId::from(actor)).unwrap();
    for i in 0..n {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    doc
}

#[test]
fn hash_id_round_trip() {
    let mut doc = doc_with_changes(b"aaaa", 3);
    let ids = doc.get_heads();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].seq(), 3);
    assert_eq!(ids[0].actor(), &ActorId::from(&b"aaaa"[..]));

    let hashes = doc.get_hashes_for_change_ids(&ids).unwrap();
    assert_eq!(hashes.len(), 1);

    let back = doc.get_change_ids_for_hashes(&hashes).unwrap();
    assert_eq!(back, ids);

    // singles agree with the batch APIs
    assert_eq!(
        doc.get_change_id_for_hash(&hashes[0]).unwrap(),
        Some(ids[0].clone())
    );
    assert_eq!(
        doc.get_hash_for_change_id(&ids[0]).unwrap(),
        Some(hashes[0])
    );
}

#[test]
fn change_id_matches_change() {
    let mut doc = doc_with_changes(b"aaaa", 2);
    let change = doc.get_last_local_change().unwrap().unwrap();
    let id = change.id();
    assert_eq!(id.seq(), 2);
    assert_eq!(id.actor(), change.actor_id());
    // Change::id() (hint 0) resolves to the change's hash
    assert_eq!(
        doc.get_hash_for_change_id(&id).unwrap(),
        Some(change.hash())
    );
    assert_eq!(
        doc.get_change_id_for_hash(&change.hash()).unwrap(),
        Some(id.clone())
    );
    // and the document's head is this change
    assert_eq!(doc.get_heads(), vec![id]);
}

#[test]
fn foreign_ids_error() {
    let mut doc1 = doc_with_changes(b"aaaa", 2);
    let doc2 = doc_with_changes(b"bbbb", 2);

    let ids1 = doc1.get_heads();
    // doc2 has never seen doc1's changes: the single lookup reports the
    // absence, the batch converter refuses outright
    assert_eq!(doc2.get_hash_for_change_id(&ids1[0]).unwrap(), None);
    assert!(matches!(
        doc2.get_hashes_for_change_ids(&ids1),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

#[test]
fn stale_actor_index_hint_resolves() {
    // Build two docs with different actor tables, merge both ways: the
    // actor-index hint from one doc is wrong in the other, but ids must
    // still resolve via the actor lookup fallback.
    let mut doc1 = doc_with_changes(b"aaaa", 2);
    let mut doc2 = doc_with_changes(b"bbbb", 2);
    doc1.merge(&mut doc2).unwrap();
    doc2.merge(&mut doc1).unwrap();

    let ids1 = doc1.get_heads();
    assert_eq!(ids1.len(), 2);
    let expected = doc1.get_hashes_for_change_ids(&ids1).unwrap();

    // resolve doc1-minted ids in doc2, where actor indices differ
    let mut hashes_via_doc2 = doc2.get_hashes_for_change_ids(&ids1).unwrap();
    let mut expected = expected;
    hashes_via_doc2.sort();
    expected.sort();
    assert_eq!(hashes_via_doc2, expected);
}

#[test]
fn parse_and_display() {
    let mut doc = doc_with_changes(b"aaaa", 3);
    let id = &doc.get_heads()[0];
    let text = id.to_string();
    assert_eq!(text, "3@61616161"); // b"aaaa" in hex

    // a parsed id (hint 0) is equal to and resolves like the original
    let parsed: ChangeId = text.parse().unwrap();
    assert_eq!(&parsed, id);
    assert_eq!(
        doc.get_hash_for_change_id(&parsed).unwrap(),
        doc.get_hash_for_change_id(id).unwrap()
    );
}

#[test]
fn heads_work_as_at_inputs() {
    use automerge::ReadDoc;
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let heads = doc.get_heads();
    doc.put(ROOT, "k", 2).unwrap();
    doc.commit();

    let (old, _) = doc.get_at(ROOT, "k", &heads).unwrap().unwrap();
    assert_eq!(old.to_i64(), Some(1));

    // foreign heads are a solid error, never a silent skip
    let mut foreign = doc_with_changes(b"cccc", 1);
    let foreign_heads = foreign.get_heads();
    assert!(matches!(
        doc.get_at(ROOT, "k", &foreign_heads),
        Err(AutomergeError::InvalidChangeId(_))
    ));
    assert!(matches!(
        doc.length_at(ROOT, &foreign_heads),
        Err(AutomergeError::InvalidChangeId(_))
    ));
    assert!(matches!(
        doc.keys_at(ROOT, &foreign_heads),
        Err(AutomergeError::InvalidChangeId(_))
    ));
    // ...and an id whose seq is beyond the actor's history is also missing
    let too_far: ChangeId = "9@61616161".parse().unwrap();
    assert!(matches!(
        doc.get_at(ROOT, "k", &[too_far]),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}
