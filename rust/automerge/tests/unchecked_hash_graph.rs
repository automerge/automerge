use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, AutomergeError, LoadOptions,
    ReadDoc, ROOT,
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

    // heads are available as ids and match the checked doc
    let mut heads = doc.get_heads();
    let mut orig_heads = orig.get_heads();
    heads.sort();
    orig_heads.sort();
    assert_eq!(heads, orig_heads);

    // historical reads via pre-load ChangeIds work without hashes
    let early_id = doc.change_id_for_opid(&doc.get(ROOT, "k").unwrap().unwrap().1);
    assert!(early_id.is_some());
    let ids: Vec<_> = (1..=3)
        .map(|seq| {
            // build ids by parsing seq@actor
            format!("{}@61616161", seq).parse().unwrap()
        })
        .collect::<Vec<automerge::ChangeId>>();
    let (v1, _) = doc.get_at(ROOT, "k", &ids[0..1]).unwrap().unwrap();
    assert_eq!(v1.to_i64(), Some(0));
    let (v2, _) = doc.get_at(ROOT, "k", &ids[1..2]).unwrap().unwrap();
    assert_eq!(v2.to_i64(), Some(1));
}

#[test]
fn unchecked_load_transactions_work() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    doc.put(ROOT, "k", 100).unwrap();
    let id = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![id.clone()]);

    // the new change's hash is known
    assert!(doc.change_id_to_hash(&id).unwrap().is_some());

    // a second commit chains on the first (same fresh actor)
    doc.put(ROOT, "k", 101).unwrap();
    doc.commit().unwrap();
}

#[test]
fn unchecked_transaction_at_post_load_works_pre_load_errors() {
    let (bytes, _) = saved_doc();
    let mut doc = Automerge::load_with_options(&bytes, unchecked_opts()).unwrap();

    // isolate at a pre-load change errors
    let early: automerge::ChangeId = "1@61616161".parse().unwrap();
    let err = doc
        .transaction_at(automerge::PatchLog::active(), &[early])
        .err()
        .unwrap();
    assert!(matches!(err, AutomergeError::UncheckedHashGraph));

    // isolating at the load heads works: their hashes come from the
    // document's head index suffix
    let load_heads = doc.get_heads();
    let tx = doc.transaction_at(automerge::PatchLog::active(), &load_heads);
    assert!(tx.is_ok());
    drop(tx);

    // make a post-load change, then isolate at it
    let mut tx = doc.transaction();
    tx.put(ROOT, "k", 50).unwrap();
    let (id, _) = tx.commit();
    let id = id.unwrap();
    let tx = doc.transaction_at(automerge::PatchLog::active(), &[id]);
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
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let load_heads = doc.get_heads();

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    // everything since the load heads is exportable
    assert!(doc.save_after(&load_heads).is_ok());
    // exporting pre-load history is not
    let early: automerge::ChangeId = "1@61616161".parse().unwrap();
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
fn unchecked_converters() {
    let (bytes, mut orig) = saved_doc();
    let doc = Automerge::load_with_options(&bytes, unchecked_opts()).unwrap();

    let head_id = &orig.get_heads()[0];
    let head_hash = orig.change_id_to_hash(head_id).unwrap().unwrap();

    // the load heads are paired with their nodes via the document's head
    // index suffix, so they convert both ways
    assert_eq!(doc.change_id_to_hash(head_id).unwrap(), Some(head_hash));
    assert_eq!(
        doc.hash_to_change_id(&head_hash).unwrap().as_ref(),
        Some(head_id)
    );

    // anything strictly before the load heads cannot be converted
    let early: automerge::ChangeId = "1@61616161".parse().unwrap();
    assert!(matches!(
        doc.change_id_to_hash(&early),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    // a hash that's definitely garbage is still ambiguous on an unchecked doc
    assert!(doc
        .hash_to_change_id(&automerge::ChangeHash([7; 32]))
        .is_err());
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

    // conversions now work and agree with the checked doc
    let orig_heads = orig.get_heads();
    let orig_hashes = orig.change_ids_to_hashes(&orig_heads).unwrap();
    let via_rebuilt = doc.change_ids_to_hashes(&orig_heads).unwrap();
    assert_eq!(orig_hashes, via_rebuilt);

    // export works
    assert!(doc.get_changes(&[]).is_ok());
    // and the doc round-trips
    let reloaded = AutoCommit::load(&doc.save()).unwrap();
    drop(reloaded);
}

#[test]
fn unchecked_multi_head_commit_and_roundtrip() {
    let (bytes, mut orig) = saved_multi_head_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert_eq!(doc.get_heads().len(), 2);

    // committing merges both pre-load heads as deps (exercises the
    // canonical pre-head pairing in dep serialization)
    doc.put(ROOT, "merged", true).unwrap();
    let id = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![id]);

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

    // and rebuilding validates the whole graph
    doc.rebuild_hash_graph().unwrap();
    let orig_heads_hashes = {
        let h = orig.get_heads();
        orig.change_ids_to_hashes(&h).unwrap()
    };
    // pre-load heads now resolve to their true hashes
    let mut resolved = Vec::new();
    for h in &orig_heads_hashes {
        resolved.push(doc.hash_to_change_id(h).unwrap().unwrap());
    }
    assert_eq!(resolved.len(), 2);
}

#[test]
fn unchecked_diff_works_hash_free() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    let before = doc.get_heads();
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let after = doc.get_heads();

    let patches = doc.diff(&before, &after);
    assert!(!patches.is_empty());

    // diff spanning pre-load history also works (ids resolve via seq_index)
    let early: automerge::ChangeId = "1@61616161".parse().unwrap();
    let patches = doc.diff(&[early], &after);
    assert!(!patches.is_empty());
}
