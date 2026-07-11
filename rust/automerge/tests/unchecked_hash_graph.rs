use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, AutomergeError, ChangeHash,
    LoadOptions, ReadDoc, ROOT,
};

fn unchecked_opts() -> LoadOptions<'static> {
    LoadOptions::new().skip_hash_graph(true)
}

/// A doc with 3 sequential changes by one actor
fn saved_doc() -> (Vec<u8>, AutoCommit) {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    for i in 0..3 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();
    (bytes, doc)
}

/// The hash of the checked doc's first (pre-load, non-head) change
fn early_hash(orig: &mut AutoCommit) -> ChangeHash {
    let mut hashes: Vec<_> = orig
        .get_changes(&[])
        .unwrap()
        .into_iter()
        .map(|c| c.hash())
        .collect();
    let head = orig.get_heads()[0];
    hashes.retain(|h| *h != head);
    hashes[0]
}

/// A doc with two concurrent branches, saved with two heads
fn saved_multi_head_doc() -> (Vec<u8>, AutoCommit) {
    let mut doc1 = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc1.put(ROOT, "base", 0).unwrap();
    doc1.commit();
    let mut doc2 = doc1.fork().with_actor(ActorId::from(&b"bbbb"[..])).unwrap();
    doc1.put(ROOT, "left", 1).unwrap();
    doc1.commit();
    doc2.put(ROOT, "right", 2).unwrap();
    doc2.commit();
    doc1.merge(&mut doc2).unwrap();
    assert_eq!(doc1.get_heads().len(), 2);
    let bytes = doc1.save();
    (bytes, doc1)
}

#[test]
fn unchecked_load_reads_work() {
    let (bytes, mut orig) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert!(!doc.hash_graph_is_checked());

    // current state reads
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));

    // the heads are known (paired via the document's head index suffix)
    // and match the checked doc
    let mut heads = doc.get_heads();
    let mut orig_heads = orig.get_heads();
    heads.sort();
    orig_heads.sort();
    assert_eq!(heads, orig_heads);

    // historical reads at the load heads work
    let (v, _) = doc.get_at(ROOT, "k", &heads).unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));

    // hashes this document has never seen are silently skipped by the
    // `*_at` methods, exactly like on a checked document
    assert!(doc
        .get_at(ROOT, "k", &[ChangeHash([7; 32])])
        .unwrap()
        .is_none());
}

#[test]
fn unchecked_load_transactions_work() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    doc.put(ROOT, "k", 100).unwrap();
    let hash = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![hash]);

    // a second commit chains on the first (same fresh actor)
    doc.put(ROOT, "k", 101).unwrap();
    doc.commit().unwrap();
}

#[test]
fn unchecked_transaction_at_load_heads_works() {
    let (bytes, _) = saved_doc();
    let mut doc = Automerge::load_with_options(&bytes, unchecked_opts()).unwrap();

    // isolating at the load heads works: their hashes come from the
    // document's head index suffix
    let load_heads = doc.get_heads();
    let tx = doc.transaction_at(automerge::PatchLog::active(), &load_heads);
    assert!(tx.is_ok());
    drop(tx);

    // make a post-load change, then isolate at it
    let mut tx = doc.transaction();
    tx.put(ROOT, "k", 50).unwrap();
    let (hash, _) = tx.commit();
    let hash = hash.unwrap();
    let tx = doc.transaction_at(automerge::PatchLog::active(), &[hash]);
    assert!(tx.is_ok());
    drop(tx);
}

#[test]
fn unchecked_save_incremental_is_infallible() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    // immediately after load there is nothing new to save
    assert!(doc.save_incremental().is_empty());

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let incr = doc.save_incremental();
    assert!(!incr.is_empty());

    // the incremental bytes apply cleanly onto a checked copy
    let mut checked = AutoCommit::load(&bytes).unwrap();
    checked.load_incremental(&incr).unwrap();
    let (v, _) = checked.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(100));
}

#[test]
fn unchecked_save_after_narrow_failure() {
    let (bytes, mut orig) = saved_doc();
    let early = early_hash(&mut orig);
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let load_heads = doc.get_heads();

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    // everything since the load heads is exportable
    assert!(doc.save_after(&load_heads).is_ok());
    // exporting pre-load history is not: the early hash is unknown, so the
    // pre-load changes must be emitted, and their hashes are unavailable
    assert!(matches!(
        doc.save_after(std::slice::from_ref(&early)),
        Err(AutomergeError::UncheckedHashGraph)
    ));

    // same for get_changes
    assert!(doc.get_changes(&load_heads).is_ok());
    assert!(matches!(
        doc.get_changes(&[early]),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    // all changes needs all hashes
    assert!(matches!(
        doc.get_changes(&[]),
        Err(AutomergeError::UncheckedHashGraph)
    ));
}

#[test]
fn unchecked_sync_errors() {
    use automerge::sync::SyncDoc;
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let mut state = automerge::sync::State::new();
    assert!(matches!(
        doc.sync().generate_sync_message(&mut state),
        Err(AutomergeError::UncheckedHashGraph)
    ));
}

#[test]
fn unchecked_set_actor_guard() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    // actor "aaaa" made the last change, which IS the current (single) head,
    // so resurrecting it is fine
    assert!(doc.set_actor(ActorId::from(&b"aaaa"[..])).is_ok());
    doc.put(ROOT, "k", 100).unwrap();
    assert!(doc.commit().is_some());

    // a fresh actor is always fine
    assert!(doc.set_actor(ActorId::random()).is_ok());
}

#[test]
fn unchecked_set_actor_errors_for_non_head_tip() {
    // actor aaaa's last change is buried under actor bbbb's changes
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc.put(ROOT, "k", 0).unwrap();
    doc.commit();
    doc.set_actor(ActorId::from(&b"bbbb"[..])).unwrap();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let bytes = doc.save();

    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    // aaaa's tip is pre-load and not a head: resurrecting it would need its hash
    assert!(matches!(
        doc.set_actor(ActorId::from(&b"aaaa"[..])),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    // bbbb's tip is the head: fine
    assert!(doc.set_actor(ActorId::from(&b"bbbb"[..])).is_ok());
    doc.put(ROOT, "k", 2).unwrap();
    assert!(doc.commit().is_some());
}

#[test]
fn unchecked_hash_lookups() {
    let (bytes, mut orig) = saved_doc();
    let early = early_hash(&mut orig);
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    // the load heads are known hashes
    let head = orig.get_heads()[0];
    assert_eq!(doc.get_heads(), vec![head]);

    // the current op belongs to the head change, whose hash is known
    let opid = doc.get(ROOT, "k").unwrap().unwrap().1;
    assert_eq!(doc.hash_for_opid(&opid).unwrap(), Some(head));

    // an op from a pre-load, non-head change errors rather than guessing
    let mut with_obj = AutoCommit::new()
        .with_actor(ActorId::from(&b"cccc"[..]))
        .unwrap();
    let list = with_obj
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    with_obj.commit();
    with_obj.insert(&list, 0, 1).unwrap();
    with_obj.commit();
    let bytes = with_obj.save();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let (_, list) = doc.get(ROOT, "list").unwrap().unwrap();
    assert!(matches!(
        doc.hash_for_opid(&list),
        Err(AutomergeError::UncheckedHashGraph)
    ));

    // pre-load, non-head hashes exist but can't be resolved: fallible
    // methods that would need to enumerate them refuse
    assert!(matches!(
        doc.get_changes(&[early]),
        Err(AutomergeError::UncheckedHashGraph)
    ));
}

#[test]
fn rebuild_hash_graph_unlocks_everything() {
    let (bytes, mut orig) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    // make some post-load changes first
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    assert!(!doc.hash_graph_is_checked());
    doc.rebuild_hash_graph().unwrap();
    assert!(doc.hash_graph_is_checked());

    // pre-load hashes now resolve: exporting everything works
    let all = doc.get_changes(&[]).unwrap();
    assert_eq!(all.len(), 4);
    let orig_hashes: Vec<_> = orig
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| c.hash())
        .collect();
    for h in &orig_hashes {
        assert!(all.iter().any(|c| c.hash() == *h));
    }

    // and the doc round-trips
    let reloaded = AutoCommit::load(&doc.save()).unwrap();
    drop(reloaded);
}

#[test]
fn unchecked_multi_head_commit_and_roundtrip() {
    let (bytes, mut orig) = saved_multi_head_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert_eq!(doc.get_heads().len(), 2);

    // committing merges both pre-load heads as deps (their hashes come
    // from the head index pairing)
    doc.put(ROOT, "merged", true).unwrap();
    let hash = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![hash]);

    // the incremental bytes (whose deps embed the pre-load head hashes)
    // apply cleanly onto a checked copy: dep hashes must be exactly right
    let incr = doc.save_incremental();
    let mut checked = AutoCommit::load(&bytes).unwrap();
    checked.load_incremental(&incr).unwrap();
    let mut checked_heads = checked.get_heads();
    let mut heads = doc.get_heads();
    checked_heads.sort();
    heads.sort();
    assert_eq!(checked_heads, heads);

    // full save of the unchecked doc round-trips through a verified load
    let saved = doc.save();
    let reloaded = AutoCommit::load(&saved).unwrap();
    drop(reloaded);

    // and rebuilding validates the whole graph: the original heads resolve
    doc.rebuild_hash_graph().unwrap();
    let mut orig_heads = orig.get_heads();
    orig_heads.sort();
    let mut rebuilt_pre_heads: Vec<_> = doc
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| c.hash())
        .filter(|h| orig_heads.contains(h))
        .collect();
    rebuilt_pre_heads.sort();
    assert_eq!(rebuilt_pre_heads, orig_heads);
}

#[test]
fn unchecked_diff_works() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    let before = doc.get_heads();
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let after = doc.get_heads();

    let patches = doc.diff(&before, &after);
    assert!(!patches.is_empty());

    // unknown hashes are silently skipped, so this diffs from the empty
    // document — same semantics as a checked doc given a foreign hash
    let patches = doc.diff(&[ChangeHash([7; 32])], &after);
    assert!(!patches.is_empty());
}
