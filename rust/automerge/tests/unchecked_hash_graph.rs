use automerge::{
    transaction::Transactable, ActorId, AutoCommit, Automerge, AutomergeError, ChangeHash,
    ChangeId, HashGraphRebuild, LoadOptions, ReadDoc, ROOT,
};

/// A change id no real document contains (actor "beefbeef")
fn foreign_id() -> ChangeId {
    "1@beefbeef".parse().unwrap()
}

fn unchecked_opts() -> LoadOptions<'static> {
    LoadOptions::new().hash_graph(HashGraphRebuild::None)
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
    let head = orig.get_head_hashes()[0];
    hashes.retain(|h| *h != head);
    hashes[0]
}

/// A large linear doc plus one of its interior hashes that is *not*
/// carried by the saved hash columns (covered by a cached fragment,
/// level 0, not an anchor) — i.e. genuinely unknown after an unchecked
/// load. Small docs no longer produce such hashes: their whole history
/// is loose commits, which the hash columns persist.
fn saved_big_doc_with_unknown_hash() -> (Vec<u8>, AutoCommit, ChangeHash) {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    for i in 0..4000 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();
    let probe = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let unknown = doc
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| c.hash())
        .find(|h| {
            matches!(
                probe.get_change_by_hash(h),
                Err(AutomergeError::UncheckedHashGraph)
            )
        })
        .expect("a 4000-change doc has covered interior hashes the columns don't store");
    (bytes, doc, unknown)
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

    // ids this document has never seen are an error, exactly like on a
    // checked document
    assert!(matches!(
        doc.get_at(ROOT, "k", &[foreign_id()]),
        Err(AutomergeError::InvalidChangeId(_))
    ));
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
    let (bytes, _orig, early) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let load_heads = doc.get_heads();

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    // everything since the load heads is exportable
    assert!(doc.save_after(&load_heads).is_ok());
    // pre-load history is unreachable: the early change's hash cannot be
    // converted to an id without the hash graph
    assert!(matches!(
        doc.get_change_id_for_hash(&early),
        Err(AutomergeError::UncheckedHashGraph)
    ));

    // get_changes works from the load heads
    assert!(doc.get_changes(&load_heads).is_ok());
    // but all changes needs the pre-load deps' hashes
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
    // actor aaaa's last change is buried deep under actor bbbb's changes,
    // covered by cached fragments and so not carried by the hash columns
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    for i in 0..2000 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let aaaa_tip = doc.get_head_hashes()[0];
    doc.set_actor(ActorId::from(&b"bbbb"[..])).unwrap();
    for i in 0..2000 {
        doc.put(ROOT, "k", 10_000 + i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();

    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    if matches!(
        doc.get_change_by_hash(&aaaa_tip),
        Err(AutomergeError::UncheckedHashGraph)
    ) {
        // aaaa's tip hash is unknown: resurrecting the actor would need it
        assert!(matches!(
            doc.set_actor(ActorId::from(&b"aaaa"[..])),
            Err(AutomergeError::UncheckedHashGraph)
        ));
    } else {
        // (vanishingly unlikely: the tip happened to be stored as a
        // fragment hash or anchor — then resurrecting is legal)
        assert!(doc.set_actor(ActorId::from(&b"aaaa"[..])).is_ok());
    }
    // bbbb's tip is the head: fine
    assert!(doc.set_actor(ActorId::from(&b"bbbb"[..])).is_ok());
    doc.put(ROOT, "k", 2).unwrap();
    assert!(doc.commit().is_some());
}

#[test]
fn unchecked_converters() {
    let (bytes, mut orig) = saved_doc();
    let early = early_hash(&mut orig);
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();

    // the load heads convert both ways (paired via the head index suffix)
    let head_id = orig.get_heads()[0].clone();
    let head_hash = orig.get_head_hashes()[0];
    assert_eq!(doc.get_heads(), vec![head_id.clone()]);
    assert_eq!(doc.get_head_hashes(), vec![head_hash]);
    assert_eq!(
        doc.get_hash_for_change_id(&head_id).unwrap(),
        Some(head_hash)
    );
    assert_eq!(
        doc.get_change_id_for_hash(&head_hash).unwrap(),
        Some(head_id.clone())
    );

    // the current op belongs to the head change
    let opid = doc.get(ROOT, "k").unwrap().unwrap().1;
    assert_eq!(doc.change_id_for_opid(&opid), Some(head_id));

    // small docs' interior hashes are all carried by the hash columns
    // now, so lookups of them succeed (the fragment-hashes state)
    assert!(doc.get_change_by_hash(&early).unwrap().is_some());
    assert!(doc.get_change_id_for_hash(&early).unwrap().is_some());

    // an unstored interior hash errors rather than guessing
    let (bytes, _orig, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let list = doc
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    doc.commit();
    // the object op made after load is known; interior hashes are not
    let list_id = doc.change_id_for_opid(&list).unwrap();
    assert!(doc.get_hash_for_change_id(&list_id).unwrap().is_some());
    assert!(matches!(
        doc.get_change_id_for_hash(&unknown),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    // a foreign id has no hash to look up: a definitive error
    assert!(matches!(
        doc.get_hashes_for_change_ids(&[foreign_id()]),
        Err(AutomergeError::InvalidChangeId(_))
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
        .map(|c| c.id())
        .filter(|id| orig_heads.contains(id))
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

    let patches = doc.diff(&before, &after).unwrap();
    assert!(!patches.is_empty());

    // unknown ids are an error — same semantics as a checked doc given a
    // foreign id
    assert!(matches!(
        doc.diff(&[foreign_id()], &after),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

/// The full lifecycle: load unchecked (which imports the saved hash
/// columns, entering the fragment-hashes state), append changes, verify
/// every fallible API errors for unknown interior history but works when
/// referencing the load heads, post-load hashes, or column-carried
/// hashes — then rebuild the hash graph and verify everything works.
#[test]
fn unchecked_lifecycle_all_fallible_functions() {
    use automerge::sync::SyncDoc;
    use automerge::HashGraphState;

    let (bytes, _orig, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    let load_heads = doc.get_heads();
    assert!(!doc.hash_graph_is_checked());
    assert_eq!(doc.hash_graph_state(), HashGraphState::FragmentHashes);

    // ── add a few changes after the load ──
    doc.put(ROOT, "k", 100_000).unwrap();
    let new1 = doc.commit().unwrap();
    doc.put(ROOT, "k", 200_000).unwrap();
    let new2 = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![new2.clone()]);

    // ── everything that needs unknown interior hashes errors ──
    let err = |r: Result<(), AutomergeError>| {
        assert!(matches!(r, Err(AutomergeError::UncheckedHashGraph)));
    };
    err(doc.get_changes(&[]).map(|_| ()));
    err(doc.get_changes_meta(&[]).map(|_| ()));
    err(doc.get_change_by_hash(&unknown).map(|_| ()));
    err(doc.get_change_meta_by_hash(&unknown).map(|_| ()));
    err(doc.get_change_id_for_hash(&unknown).map(|_| ()));
    let mut state = automerge::sync::State::new();
    err(doc.sync().generate_sync_message(&mut state).map(|_| ()));
    let mut other = AutoCommit::new();
    other.put(ROOT, "x", 1).unwrap();
    other.commit();
    err(doc.get_changes_added(&mut other).map(|_| ()));
    err(doc.merge(&mut other).map(|_| ()));

    // ── referencing the load heads or post-load hashes works ──
    let since_load = doc.get_changes(&load_heads).unwrap();
    assert_eq!(
        since_load.iter().map(|c| c.id()).collect::<Vec<_>>(),
        vec![new1.clone(), new2.clone()]
    );
    assert_eq!(
        doc.get_changes(std::slice::from_ref(&new1)).unwrap().len(),
        1
    );
    assert_eq!(
        doc.get_changes(std::slice::from_ref(&new2)).unwrap().len(),
        0
    );
    assert_eq!(doc.get_changes_meta(&load_heads).unwrap().len(), 2);
    // post-load ids convert to hashes, and those hashes look up changes
    let new1_hash = doc.get_hash_for_change_id(&new1).unwrap().unwrap();
    let new2_hash = doc.get_hash_for_change_id(&new2).unwrap().unwrap();
    assert!(doc.get_change_by_hash(&new1_hash).unwrap().is_some());
    assert!(doc.get_change_meta_by_hash(&new2_hash).unwrap().is_some());
    assert!(!doc.save_after(&load_heads).unwrap().is_empty());
    assert!(!doc
        .save_after(std::slice::from_ref(&new1))
        .unwrap()
        .is_empty());
    assert!(doc.get_missing_deps(&load_heads).unwrap().is_empty());
    assert!(doc
        .get_missing_deps(std::slice::from_ref(&new2))
        .unwrap()
        .is_empty());
    // the new changes are local, so the last local change is reachable
    assert_eq!(doc.get_last_local_change().unwrap().unwrap().id(), new2);

    // ── fragments work in the fragment-hashes state ──
    let mid_fragments = doc.fragments(..).unwrap();
    assert!(!mid_fragments.is_empty());
    assert!(!doc
        .bundle_fragments(mid_fragments.clone())
        .unwrap()
        .is_empty());

    // ── rebuild the graph: every failing call above now succeeds ──
    doc.rebuild_hash_graph().unwrap();
    assert!(doc.hash_graph_is_checked());
    assert_eq!(doc.hash_graph_state(), HashGraphState::Checked);

    assert_eq!(doc.get_changes(&[]).unwrap().len(), 4002);
    let unknown_id = doc.get_change_id_for_hash(&unknown).unwrap().unwrap();
    assert!(!doc
        .get_changes(std::slice::from_ref(&unknown_id))
        .unwrap()
        .is_empty());
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
    assert!(!doc
        .save_after(std::slice::from_ref(&unknown_id))
        .unwrap()
        .is_empty());
    assert!(doc
        .sync()
        .generate_sync_message(&mut state)
        .unwrap()
        .is_some());
    assert!(!doc.get_changes_added(&mut other).unwrap().is_empty());
    doc.merge(&mut other).unwrap();
    let (v, _) = doc.get(ROOT, "x").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(1));

    // the fragment index survives the rebuild: identical to the
    // fragments of the same document loaded fully checked
    let fragments = doc.fragments(..).unwrap();
    let checked = AutoCommit::load(&doc.save()).unwrap();
    assert_eq!(fragments, checked.fragments(..).unwrap());
    // and the middle-state fragments were already the checked ones
    // (modulo the two changes committed after the middle-state call)
    assert!(!fragments.is_empty());
}

/// A single-change doc stores no hash columns (its only loose commit is
/// the head, which the head-index suffix already carries) — loading it
/// unchecked lands in the plain Unchecked state where fragment APIs
/// refuse.
#[test]
fn plain_unchecked_state_without_hash_columns() {
    use automerge::HashGraphState;

    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let bytes = doc.save();

    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert_eq!(doc.hash_graph_state(), HashGraphState::Unchecked);
    assert!(matches!(
        doc.fragments(..),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    let head = doc.get_head_hashes()[0];
    assert!(matches!(
        doc.get_fragment(head),
        Err(AutomergeError::UncheckedHashGraph)
    ));
    assert!(matches!(
        doc.bundle_fragments([]),
        Err(AutomergeError::UncheckedHashGraph)
    ));

    doc.rebuild_hash_graph().unwrap();
    assert_eq!(doc.hash_graph_state(), HashGraphState::Checked);
    assert_eq!(doc.fragments(..).unwrap().len(), 1);
}

/// A saved document whose recorded head hash has a flipped bit (with the
/// chunk checksum patched to match) loads fine unchecked — the head
/// hashes are taken on trust — but `rebuild_hash_graph` recomputes the
/// real hashes and refuses.
#[test]
fn bit_flipped_head_loads_unchecked_but_fails_rebuild() {
    use sha2::{Digest, Sha256};

    let (mut bytes, mut orig) = saved_doc();
    let head = orig.get_head_hashes()[0];

    // flip one bit in the stored head hash
    let pos = bytes
        .windows(32)
        .position(|w| w == head.as_ref())
        .expect("head hash bytes present in saved doc");
    bytes[pos] ^= 0x01;

    // re-derive the chunk checksum: first 4 bytes of
    // sha256(chunk_type . leb(data_len) . data)
    // layout: [magic 4][checksum 4][type 1][leb len][data]
    let mut hasher = Sha256::new();
    hasher.update(&bytes[8..]);
    let digest = hasher.finalize();
    bytes[4..8].copy_from_slice(&digest[..4]);

    // a checked load rejects the forged head outright
    assert!(AutoCommit::load(&bytes).is_err());

    // an unchecked load takes the recorded heads on trust
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert!(!doc.hash_graph_is_checked());
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));
    assert_ne!(
        doc.get_head_hashes(),
        vec![head],
        "head should be the forged one"
    );

    // ...but rebuilding the graph recomputes the true hashes and refuses
    assert!(doc.rebuild_hash_graph().is_err());
}

/// The fragment-hashes state survives save/load round trips: a
/// middle-state doc re-emits the hash columns it imported.
#[test]
fn fragment_hashes_state_round_trips() {
    use automerge::HashGraphState;

    let (bytes, _orig, _unknown) = saved_big_doc_with_unknown_hash();
    let mid1 = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert_eq!(mid1.hash_graph_state(), HashGraphState::FragmentHashes);
    let frags1 = mid1.fragments(..).unwrap();
    assert!(!frags1.is_empty());

    // middle state → save → unchecked load → still middle, same fragments
    let mut mid1 = mid1;
    let resaved = mid1.save();
    let mid2 = AutoCommit::load_with_options(&resaved, unchecked_opts()).unwrap();
    assert_eq!(mid2.hash_graph_state(), HashGraphState::FragmentHashes);
    assert_eq!(mid2.fragments(..).unwrap(), frags1);

    // and both match the fully checked fragments
    let checked = AutoCommit::load(&bytes).unwrap();
    assert_eq!(checked.fragments(..).unwrap(), frags1);
}

/// A checked load verifies the stored hash columns against the
/// recomputed hashes; an unchecked load trusts them but rebuild refuses.
#[test]
fn forged_hash_column_rejected() {
    use automerge::HashGraphState;
    use sha2::{Digest, Sha256};

    let (mut bytes, orig, _unknown) = saved_big_doc_with_unknown_hash();
    // a cached fragment head is a stored, non-head hash
    let stored = orig.fragments(1..).unwrap()[0].head;
    drop(orig);
    let pos = bytes
        .windows(32)
        .position(|w| w == stored.as_ref())
        .expect("stored hash bytes present in saved doc");
    bytes[pos] ^= 0x01;
    let mut hasher = Sha256::new();
    hasher.update(&bytes[8..]);
    let digest = hasher.finalize();
    bytes[4..8].copy_from_slice(&digest[..4]);

    // checked load recomputes hashes and rejects the forged column
    assert!(AutoCommit::load(&bytes).is_err());

    // unchecked load trusts it (like the head pairing) ...
    let mut doc = AutoCommit::load_with_options(&bytes, unchecked_opts()).unwrap();
    assert_eq!(doc.hash_graph_state(), HashGraphState::FragmentHashes);
    // ... but rebuild recomputes and refuses
    assert!(doc.rebuild_hash_graph().is_err());
}

/// `HashGraphRebuild::Fragments` uses the stored hash columns when they exist
/// (as fast as `Skip`, fragments work immediately) and falls back to a
/// full checked rebuild when they don't.
#[test]
fn fragments_mode_uses_columns_or_rebuilds() {
    use automerge::HashGraphState;

    let fragments_opts = || LoadOptions::new().hash_graph(HashGraphRebuild::Fragments);

    // a doc with hash columns comes up in the middle state
    let (bytes, orig, unknown) = saved_big_doc_with_unknown_hash();
    let doc = AutoCommit::load_with_options(&bytes, fragments_opts()).unwrap();
    assert_eq!(doc.hash_graph_state(), HashGraphState::FragmentHashes);
    assert_eq!(doc.fragments(..).unwrap(), orig.fragments(..).unwrap());
    // interior history stays unknown — no silent rebuild happened
    assert!(matches!(
        doc.get_change_by_hash(&unknown),
        Err(AutomergeError::UncheckedHashGraph)
    ));

    // a doc without hash columns (single change) gets a full rebuild
    let mut small = AutoCommit::new();
    small.put(ROOT, "k", 1).unwrap();
    small.commit();
    let small_bytes = small.save();
    let doc = AutoCommit::load_with_options(&small_bytes, fragments_opts()).unwrap();
    assert_eq!(doc.hash_graph_state(), HashGraphState::Checked);
    assert_eq!(doc.fragments(..).unwrap().len(), 1);
}
