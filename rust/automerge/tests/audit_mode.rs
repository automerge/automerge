use automerge::{
    transaction::Transactable, ActorId, AuditMode, AutoCommit, Automerge, AutomergeError,
    ChangeHash, ChangeId, LoadOptions, ReadDoc, ROOT,
};

fn audit_opts() -> LoadOptions {
    LoadOptions::new().with_audit_mode()
}

/// A doc with 3 sequential changes by one actor. The doc is kept in
/// audit mode so its full history stays enumerable.
fn saved_doc() -> (Vec<u8>, AutoCommit) {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc.enable_audit_mode().unwrap();
    for i in 0..3 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();
    (bytes, doc)
}

/// The hash of the audit doc's first (pre-load, non-head) change
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

/// A large linear doc plus one of its interior hashes that is *not* in
/// the retained set (covered by a cached fragment, level 0, not an
/// anchor) — i.e. freed outside audit mode. Small docs have no such
/// hashes: their whole history is loose commits, which stay retained.
fn saved_big_doc_with_unknown_hash() -> (Vec<u8>, AutoCommit, ChangeHash) {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc.enable_audit_mode().unwrap();
    for i in 0..4000 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();
    let probe = AutoCommit::load(&bytes).unwrap();
    let unknown = doc
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| c.hash())
        .find(|h| {
            matches!(
                probe.get_change_by_hash(h),
                Err(AutomergeError::AuditModeRequired)
            )
        })
        .expect("a 4000-change doc has covered interior hashes outside the retained set");
    (bytes, doc, unknown)
}

/// A doc with two concurrent branches, saved with two heads
fn saved_multi_head_doc() -> (Vec<u8>, AutoCommit) {
    let mut doc1 = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc1.enable_audit_mode().unwrap();
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
fn default_load_is_disabled_and_reads_work() {
    let (bytes, mut orig) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);

    // current state reads
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));

    // the heads are known and match the audit doc
    let mut heads = doc.get_heads();
    let mut orig_heads = orig.get_heads();
    heads.sort();
    orig_heads.sort();
    assert_eq!(heads, orig_heads);

    // historical reads at the load heads work
    let (v, _) = doc.get_at(ROOT, "k", &heads).unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));

    // ids this document has never seen are an error in the `*_at`
    // methods, exactly like in audit mode
    assert!(matches!(
        doc.get_at(
            ROOT,
            "k",
            &[ChangeId::from_parts(
                ActorId::random(),
                std::num::NonZeroU64::new(999).unwrap()
            )]
        ),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

#[test]
fn audit_load_is_enabled() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load_with_options(&bytes, audit_opts()).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);
    // every hash resolves
    assert_eq!(doc.get_changes(&[]).unwrap().len(), 3);
}

#[test]
fn new_doc_is_disabled() {
    let doc = AutoCommit::new();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert_eq!(Automerge::new().audit_mode(), AuditMode::Disabled);
}

#[test]
fn disabled_load_transactions_work() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    doc.put(ROOT, "k", 100).unwrap();
    let id = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![id]);

    // a second commit chains on the first (same fresh actor)
    doc.put(ROOT, "k", 101).unwrap();
    doc.commit().unwrap();
}

#[test]
fn disabled_transaction_at_load_heads_works() {
    let (bytes, _) = saved_doc();
    let mut doc = Automerge::load(&bytes).unwrap();

    // isolating at the load heads works
    let load_heads = doc.get_heads();
    let tx = doc.transaction_at(&load_heads).unwrap();
    drop(tx);

    // make a post-load change, then isolate at it
    let mut tx = doc.transaction();
    tx.put(ROOT, "k", 50).unwrap();
    let id = tx.commit().unwrap();
    let tx = doc.transaction_at(&[id]).unwrap();
    drop(tx);
}

#[test]
fn disabled_save_incremental_is_infallible() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    // immediately after load there is nothing new to save
    assert!(doc.save_incremental().is_empty());

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let incr = doc.save_incremental();
    assert!(!incr.is_empty());

    // the incremental bytes apply cleanly onto an audit copy
    let mut audit = AutoCommit::load_with_options(&bytes, audit_opts()).unwrap();
    audit.load_incremental(&incr).unwrap();
    let (v, _) = audit.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(100));
}

#[test]
fn disabled_save_after_narrow_failure() {
    let (bytes, _orig, early) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    let load_heads = doc.get_heads();

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    // everything since the load heads is exportable
    assert!(doc.save_after(&load_heads).is_ok());
    // exporting pre-load history is not: the early hash is freed, so the
    // pre-load changes must be emitted, and their hashes are unavailable
    assert!(matches!(
        doc.hashes_to_change_ids(std::slice::from_ref(&early))
            .and_then(|ids| doc.save_after(&ids)),
        Err(AutomergeError::AuditModeRequired)
    ));

    // same for get_changes
    assert!(doc.get_changes(&load_heads).is_ok());
    assert!(matches!(
        doc.hashes_to_change_ids(&[early])
            .and_then(|ids| doc.get_changes(&ids)),
        Err(AutomergeError::AuditModeRequired)
    ));
    // all changes needs all hashes
    assert!(matches!(
        doc.get_changes(&[]),
        Err(AutomergeError::AuditModeRequired)
    ));
}

#[test]
fn sync_requires_audit_mode() {
    use automerge_sync::Sync;

    // the gate is deterministic: even a fresh one-change doc, whose
    // retained hashes would suffice, refuses outside audit mode
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let mut state = automerge_sync::State::new();
    assert!(matches!(
        Sync::generate_sync_message(doc.document(), &mut state),
        Err(AutomergeError::AuditModeRequired)
    ));

    // receiving refuses too
    let mut other = AutoCommit::new();
    other.enable_audit_mode().unwrap();
    other.put(ROOT, "k", 2).unwrap();
    other.commit();
    let mut other_state = automerge_sync::State::new();
    let msg = Sync::generate_sync_message(other.document(), &mut other_state)
        .unwrap()
        .unwrap();
    assert!(matches!(
        Sync::receive_sync_message(doc.document_mut(), &mut state, msg.clone()),
        Err(AutomergeError::AuditModeRequired)
    ));

    // enabling audit mode unlocks both directions — the same message
    // the receive above refused now applies
    doc.enable_audit_mode().unwrap();
    Sync::receive_sync_message(doc.document_mut(), &mut state, msg).unwrap();
    assert!(Sync::generate_sync_message(doc.document(), &mut state)
        .unwrap()
        .is_some());
}

#[test]
fn disabled_set_actor_guard() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    // actor "aaaa" made the last change, which IS the current (single) head,
    // so resurrecting it is fine
    assert!(doc.set_actor(ActorId::from(&b"aaaa"[..])).is_ok());
    doc.put(ROOT, "k", 100).unwrap();
    assert!(doc.commit().is_some());

    // a fresh actor is always fine
    assert!(doc.set_actor(ActorId::random()).is_ok());
}

#[test]
fn disabled_set_actor_errors_for_non_head_tip() {
    // actor aaaa's last change is buried deep under actor bbbb's changes,
    // covered by cached fragments and so not in the retained set
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .unwrap();
    doc.enable_audit_mode().unwrap();
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

    let mut doc = AutoCommit::load(&bytes).unwrap();
    if matches!(
        doc.get_change_by_hash(&aaaa_tip),
        Err(AutomergeError::AuditModeRequired)
    ) {
        // aaaa's tip hash is freed: resurrecting the actor would need it
        assert!(matches!(
            doc.set_actor(ActorId::from(&b"aaaa"[..])),
            Err(AutomergeError::AuditModeRequired)
        ));
    } else {
        // (vanishingly unlikely: the tip happened to be retained as a
        // fragment hash or anchor — then resurrecting is legal)
        assert!(doc.set_actor(ActorId::from(&b"aaaa"[..])).is_ok());
    }
    // bbbb's tip is the head: fine
    assert!(doc.set_actor(ActorId::from(&b"bbbb"[..])).is_ok());
    doc.put(ROOT, "k", 2).unwrap();
    assert!(doc.commit().is_some());
}

#[test]
fn disabled_hash_lookups() {
    let (bytes, mut orig) = saved_doc();
    let early = early_hash(&mut orig);
    let mut doc = AutoCommit::load(&bytes).unwrap();

    // the load heads are known hashes
    let head = orig.get_head_hashes()[0];
    assert_eq!(doc.get_head_hashes(), vec![head]);

    // the current op belongs to the head change, whose hash is known
    let opid = doc.get(ROOT, "k").unwrap().unwrap().1;
    assert_eq!(doc.hash_for_opid(&opid).unwrap(), Some(head));

    // small docs' interior hashes are all loose commits, carried by the
    // hash columns and retained after load
    assert!(doc.get_change_by_hash(&early).unwrap().is_some());

    // an op from a covered, freed interior change errors rather than
    // guessing
    let (bytes, _orig, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    let list = doc
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    doc.commit();
    // the object op made after load is known; freed interior hashes are not
    assert!(doc.hash_for_opid(&list).unwrap().is_some());
    assert!(matches!(
        doc.hashes_to_change_ids(&[unknown])
            .and_then(|ids| doc.get_changes(&ids)),
        Err(AutomergeError::AuditModeRequired)
    ));
}

#[test]
fn enable_audit_mode_unlocks_everything() {
    let (bytes, mut orig) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    // make some post-load changes first
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    doc.enable_audit_mode().unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);

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

/// Live GC: a document built from scratch outside audit mode frees the
/// hashes of changes covered by cached fragments as it goes, while the
/// same edits made in audit mode keep everything.
#[test]
fn usurped_fragment_hashes_are_freed_on_live_docs() {
    let build = |audit: bool| {
        let mut doc = AutoCommit::new()
            .with_actor(ActorId::from(&b"aaaa"[..]))
            .unwrap();
        if audit {
            doc.enable_audit_mode().unwrap();
        }
        for i in 0..4000 {
            doc.put(ROOT, "k", i as i64).unwrap();
            doc.commit();
        }
        doc
    };
    // same actor + same edits → identical change hashes
    let mut audit = build(true);
    let mut plain = build(false);
    assert_eq!(audit.get_head_hashes(), plain.get_head_hashes());

    let all: Vec<_> = audit
        .get_changes(&[])
        .unwrap()
        .iter()
        .map(|c| c.hash())
        .collect();
    let freed: Vec<_> = all
        .iter()
        .filter(|h| {
            matches!(
                plain.get_change_by_hash(h),
                Err(AutomergeError::AuditModeRequired)
            )
        })
        .collect();
    // the interior of every cached fragment was freed as the doc grew
    assert!(
        !freed.is_empty(),
        "a 4000-change doc must free covered hashes outside audit mode"
    );
    // ... but the heads always resolve
    let head = plain.get_head_hashes()[0];
    assert!(plain.get_change_by_hash(&head).unwrap().is_some());
    // and the fragment index is intact: same fragments as the audit doc
    assert_eq!(plain.fragments(..).unwrap(), audit.fragments(..).unwrap());
}

/// enable → disable → enable round trip: disabling frees the interior
/// hashes again, re-enabling recomputes and verifies them.
#[test]
fn enable_disable_enable_cycle() {
    let (bytes, _orig, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert!(matches!(
        doc.get_change_by_hash(&unknown),
        Err(AutomergeError::AuditModeRequired)
    ));

    doc.enable_audit_mode().unwrap();
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
    assert_eq!(doc.get_changes(&[]).unwrap().len(), 4000);

    doc.disable_audit_mode();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert!(matches!(
        doc.get_change_by_hash(&unknown),
        Err(AutomergeError::AuditModeRequired)
    ));
    // heads and current reads keep working
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(3999));
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_change_by_hash(&head).unwrap().is_some());

    doc.enable_audit_mode().unwrap();
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
}

#[test]
fn disabled_multi_head_commit_and_roundtrip() {
    let (bytes, mut orig) = saved_multi_head_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.get_heads().len(), 2);

    // committing merges both pre-load heads as deps (their hashes come
    // from the retained set)
    doc.put(ROOT, "merged", true).unwrap();
    let id = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![id]);

    // the incremental bytes (whose deps embed the pre-load head hashes)
    // apply cleanly onto an audit copy: dep hashes must be exactly right
    let incr = doc.save_incremental();
    let mut audit = AutoCommit::load_with_options(&bytes, audit_opts()).unwrap();
    audit.load_incremental(&incr).unwrap();
    let mut audit_heads = audit.get_heads();
    let mut heads = doc.get_heads();
    audit_heads.sort();
    heads.sort();
    assert_eq!(audit_heads, heads);

    // full save of the disabled doc round-trips through a verifying load
    let saved = doc.save();
    let reloaded = AutoCommit::load_with_options(&saved, audit_opts()).unwrap();
    drop(reloaded);

    // and enabling audit mode validates the whole graph: the original
    // heads resolve
    doc.enable_audit_mode().unwrap();
    let mut orig_heads = orig.get_head_hashes();
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
fn disabled_diff_works() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    let before = doc.get_heads();
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let after = doc.get_heads();

    let patches = doc.diff(&before, &after);
    assert!(!patches.is_empty());

    // foreign ids are an error rather than being silently skipped
    let foreign = ChangeId::from_parts(ActorId::random(), std::num::NonZeroU64::new(7).unwrap());
    assert!(matches!(
        doc.document().diff(&[foreign], &after),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

/// The full lifecycle: load outside audit mode (importing the saved hash
/// columns as the retained set), append changes, verify every fallible
/// API errors for freed interior history but works when referencing the
/// load heads, post-load ids, or retained hashes — then enable audit
/// mode and verify everything works.
#[test]
fn disabled_lifecycle_all_fallible_functions() {
    use automerge_sync::Sync;

    let (bytes, _orig, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    let load_heads = doc.get_heads();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);

    // ── add a few changes after the load ──
    doc.put(ROOT, "k", 100_000).unwrap();
    let new1 = doc.commit().unwrap();
    doc.put(ROOT, "k", 200_000).unwrap();
    let new2 = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![new2.clone()]);

    // ── everything that needs freed interior hashes errors ──
    let err = |r: Result<(), AutomergeError>| {
        assert!(matches!(r, Err(AutomergeError::AuditModeRequired)));
    };
    err(doc.get_changes(&[]).map(|_| ()));
    err(doc.hashes_to_change_ids(&[unknown]).map(|_| ()));
    err(doc.get_changes_meta(&[]).map(|_| ()));

    err(doc.get_change_by_hash(&unknown).map(|_| ()));
    err(doc.get_change_meta_by_hash(&unknown).map(|_| ()));

    let mut state = automerge_sync::State::new();
    err(Sync::generate_sync_message(doc.document(), &mut state).map(|_| ()));

    // merge and get_changes_added are hash-free (they identify changes
    // by (actor, seq)) and work outside audit mode — check on a fork so
    // the main doc's state is undisturbed for the assertions below
    let mut other = AutoCommit::new();
    other.put(ROOT, "x", 1).unwrap();
    other.commit();
    assert_eq!(doc.get_changes_added(&mut other).unwrap().len(), 1);
    let mut fork = doc.fork();
    fork.merge(&mut other).unwrap();
    let (v, _) = fork.get(ROOT, "x").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(1));
    drop(fork);

    // ── referencing the load heads or post-load ids works ──
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
    let new1_hash = doc.change_id_to_hash(&new1).unwrap().unwrap();
    assert!(doc.get_change_by_hash(&new1_hash).unwrap().is_some());
    let new2_hash = doc.change_id_to_hash(&new2).unwrap().unwrap();
    assert!(doc.get_change_meta_by_hash(&new2_hash).unwrap().is_some());
    assert!(!doc.save_after(&load_heads).unwrap().is_empty());
    assert!(!doc.save_after(&[new1]).unwrap().is_empty());
    assert!(doc.get_missing_deps(&load_heads).unwrap().is_empty());
    assert!(doc
        .get_missing_deps(std::slice::from_ref(&new2))
        .unwrap()
        .is_empty());
    // the new changes are local, so the last local change is reachable
    assert_eq!(doc.get_last_local_change().unwrap().unwrap().id(), new2);

    // ── fragments work outside audit mode: the retained set is
    // fragment-sufficient by construction ──
    let mid_fragments = doc.fragments(..).unwrap();
    assert!(!mid_fragments.is_empty());
    assert!(!doc
        .bundle_fragments(mid_fragments.clone())
        .unwrap()
        .is_empty());

    // ── enable audit mode: every failing call above now succeeds ──
    doc.enable_audit_mode().unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);

    assert_eq!(doc.get_changes(&[]).unwrap().len(), 4002);
    let unknown_id = doc.hash_to_change_id(&unknown).unwrap().unwrap();
    assert!(!doc
        .get_changes(std::slice::from_ref(&unknown_id))
        .unwrap()
        .is_empty());
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
    assert!(!doc.save_after(&[unknown_id]).unwrap().is_empty());
    let mut state = automerge_sync::State::new();
    assert!(Sync::generate_sync_message(doc.document(), &mut state)
        .unwrap()
        .is_some());
    assert!(!doc.get_changes_added(&mut other).unwrap().is_empty());
    doc.merge(&mut other).unwrap();
    let (v, _) = doc.get(ROOT, "x").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(1));

    // the fragment index survives the transition: identical to the
    // fragments of the same document loaded in audit mode
    let fragments = doc.fragments(..).unwrap();
    let audit = AutoCommit::load_with_options(&doc.save(), audit_opts()).unwrap();
    assert_eq!(fragments, audit.fragments(..).unwrap());
    // and the disabled-state fragments were already the audit ones
    // (modulo the changes committed after the disabled-state call)
    assert!(!fragments.is_empty());
}

/// Fragments work even on documents without stored hash columns: a
/// single-change doc's whole history is loose, so the retained set
/// covers it.
#[test]
fn fragments_work_without_hash_columns() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let bytes = doc.save();

    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert_eq!(doc.fragments(..).unwrap().len(), 1);
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_fragment(head).unwrap().is_some());
    assert!(!doc
        .bundle_fragments(doc.fragments(..).unwrap())
        .unwrap()
        .is_empty());

    doc.enable_audit_mode().unwrap();
    assert_eq!(doc.fragments(..).unwrap().len(), 1);
}

/// A saved document whose recorded head hash has a flipped bit (with the
/// chunk checksum patched to match) loads fine outside audit mode — the
/// head hashes are taken on trust — but an audit load (or
/// `enable_audit_mode`) recomputes the real hashes and refuses.
#[test]
fn bit_flipped_head_loads_disabled_but_fails_audit() {
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

    // an audit load rejects the forged head outright
    assert!(AutoCommit::load_with_options(&bytes, audit_opts()).is_err());

    // a default load takes the recorded heads on trust
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.to_i64(), Some(2));
    assert_ne!(
        doc.get_head_hashes(),
        vec![head],
        "head should be the forged one"
    );

    // ...but enabling audit mode recomputes the true hashes and refuses
    assert!(doc.enable_audit_mode().is_err());
}

/// The disabled state survives save/load round trips: a disabled doc
/// re-emits the hash columns it imported.
#[test]
fn disabled_state_round_trips() {
    let (bytes, _orig, _unknown) = saved_big_doc_with_unknown_hash();
    let mid1 = AutoCommit::load(&bytes).unwrap();
    assert_eq!(mid1.audit_mode(), AuditMode::Disabled);
    let frags1 = mid1.fragments(..).unwrap();
    assert!(!frags1.is_empty());

    // disabled → save → default load → still disabled, same fragments
    let mut mid1 = mid1;
    let resaved = mid1.save();
    let mid2 = AutoCommit::load(&resaved).unwrap();
    assert_eq!(mid2.audit_mode(), AuditMode::Disabled);
    assert_eq!(mid2.fragments(..).unwrap(), frags1);

    // and both match the audit-mode fragments
    let audit = AutoCommit::load_with_options(&bytes, audit_opts()).unwrap();
    assert_eq!(audit.fragments(..).unwrap(), frags1);
}

/// An audit load verifies the stored hash columns against the recomputed
/// hashes; a default load trusts them but `enable_audit_mode` refuses.
#[test]
fn forged_hash_column_rejected() {
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

    // an audit load recomputes hashes and rejects the forged column
    assert!(AutoCommit::load_with_options(&bytes, audit_opts()).is_err());

    // a default load trusts it (like the head pairing) ...
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    // ... but enabling audit mode recomputes and refuses
    assert!(doc.enable_audit_mode().is_err());
}

/// A default load of a doc without hash columns (or without the
/// head-index suffix) computes the hashes once, then keeps only the
/// retained set — the mode invariant holds: `audit_mode()` always equals
/// what was requested.
#[test]
fn default_load_computes_then_retains_without_columns() {
    // a single-change doc stores no hash columns (its only loose commit
    // is the head, which the head-index suffix already carries)
    let mut small = AutoCommit::new();
    small.put(ROOT, "k", 1).unwrap();
    small.commit();
    let small_bytes = small.save();
    let mut doc = AutoCommit::load(&small_bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert_eq!(doc.fragments(..).unwrap().len(), 1);
    // its single change is the head: retained
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_change_by_hash(&head).unwrap().is_some());
}

/// Applying the same Bundle chain in audit mode (to_changes +
/// apply_changes, hashing everything) and outside it (the manifold fast
/// path) produces identical documents — and the audit doc keeps every
/// hash.
#[test]
fn audit_and_manifold_fragment_apply_agree() {
    let (bytes, mut src, _unknown) = saved_big_doc_with_unknown_hash();
    drop(bytes);

    let fragments = src.fragments(..).unwrap();
    let bundles: Vec<_> = fragments
        .into_iter()
        .map(|f| src.document().bundle_fragment(&f).unwrap())
        .collect();
    assert!(bundles.len() > 1);

    let mut plain = Automerge::new();
    let mut audit = Automerge::new();
    audit.enable_audit_mode().unwrap();

    for b in &bundles {
        plain.apply_fragment(b).unwrap();
        audit.apply_fragment(b).unwrap();
    }

    assert_eq!(plain.audit_mode(), AuditMode::Disabled);
    assert_eq!(audit.audit_mode(), AuditMode::Enabled);
    assert_eq!(plain.get_heads(), audit.get_heads());
    assert_eq!(plain.get_heads(), src.get_heads());

    // the audit doc hashed every member — full history enumerable
    assert_eq!(audit.get_changes(&[]).unwrap().len(), 4000);

    // both serialize identically once the plain doc's hashes are
    // computed
    let mut plain = plain;
    plain.enable_audit_mode().unwrap();
    assert_eq!(plain.save(), audit.save());
}

/// The audit fragment path enforces the manifold path's no-missing-deps
/// contract: an out-of-order bundle errors instead of queueing.
#[test]
fn audit_fragment_apply_missing_deps() {
    let (bytes, mut src, _unknown) = saved_big_doc_with_unknown_hash();
    drop(bytes);

    let fragments = src.fragments(..).unwrap();
    let bundles: Vec<_> = fragments
        .into_iter()
        .map(|f| src.document().bundle_fragment(&f).unwrap())
        .collect();
    assert!(bundles.len() > 1);

    let mut audit = Automerge::new();
    audit.enable_audit_mode().unwrap();
    // the second bundle's boundary dep is missing
    assert!(matches!(
        audit.apply_fragment(&bundles[1]),
        Err(AutomergeError::MissingDeps)
    ));
    // heads unchanged — nothing applied
    assert!(audit.get_heads().is_empty());
}
