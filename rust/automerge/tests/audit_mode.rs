use automerge::next::transaction::CommitOptions;
use automerge::next::{
    transaction::Transactable, AuditMode, AuditedAutoCommit, AutoCommit, Automerge, ReadDoc,
};
use automerge::{ActorId, AutomergeError, ChangeHash, ChangeId, TextEncoding, ROOT};

fn load_audited(bytes: &[u8]) -> Result<AuditedAutoCommit, AutomergeError> {
    AutoCommit::load(bytes)?
        .enable_audit_mode()
        .map_err(|e| e.error)
}

/// Three sequential changes by one actor, in audit mode.
fn saved_doc() -> (Vec<u8>, AuditedAutoCommit) {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .enable_audit_mode()
        .unwrap();
    for i in 0..3 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();
    (bytes, doc)
}

/// The hash of the audit doc's first (pre-load, non-head) change
fn early_hash(orig: &mut AuditedAutoCommit) -> ChangeHash {
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

/// A large linear doc and an interior hash freed outside audit mode.
fn saved_big_doc_with_unknown_hash() -> (Vec<u8>, ChangeHash) {
    static FIXTURE: std::sync::OnceLock<(Vec<u8>, ChangeHash)> = std::sync::OnceLock::new();
    let (bytes, unknown) = FIXTURE.get_or_init(|| {
        let mut doc = AutoCommit::new()
            .with_actor(ActorId::from(&b"aaaa"[..]))
            .enable_audit_mode()
            .unwrap();
        for i in 0..4000 {
            doc.put(ROOT, "k", i as i64).unwrap();
            // pinned actor and time fix which commits form fragments
            doc.commit_with(CommitOptions::default().with_time(0));
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
        (bytes, unknown)
    });
    (bytes.clone(), *unknown)
}

fn big_doc_in_audit_mode() -> AuditedAutoCommit {
    let (bytes, _) = saved_big_doc_with_unknown_hash();
    load_audited(&bytes).unwrap()
}

/// A doc with two concurrent branches, saved with two heads
fn saved_multi_head_doc() -> (Vec<u8>, AuditedAutoCommit) {
    let mut doc1 = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .enable_audit_mode()
        .unwrap();
    doc1.put(ROOT, "base", 0).unwrap();
    doc1.commit();
    let mut doc2 = doc1.fork().with_actor(ActorId::from(&b"bbbb"[..]));
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

    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(2));

    let mut heads = doc.get_heads();
    let mut orig_heads = orig.get_heads();
    heads.sort();
    orig_heads.sort();
    assert_eq!(heads, orig_heads);

    let (v, _) = doc.get_at(ROOT, "k", &heads).unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(2));

    // unknown ids are an error, as in audit mode
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
    let mut doc = load_audited(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);
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

    doc.put(ROOT, "k", 101).unwrap();
    doc.commit().unwrap();
}

#[test]
fn disabled_transaction_at_load_heads_works() {
    let (bytes, _) = saved_doc();
    let mut doc = Automerge::load(&bytes).unwrap();

    let load_heads = doc.get_heads();
    let tx = doc.transaction_at(&load_heads).unwrap();
    drop(tx);

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

    assert!(doc.save_incremental().is_empty());

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let incr = doc.save_incremental();
    assert!(!incr.is_empty());

    let mut audit = load_audited(&bytes).unwrap();
    audit.load_incremental(&incr).unwrap();
    let (v, _) = audit.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(100));
}

#[test]
fn disabled_save_after_narrow_failure() {
    let (bytes, early) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    let load_heads = doc.get_heads();

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    assert!(doc.save_after(&load_heads).is_ok());
    // pre-load history needs freed hashes
    assert!(matches!(
        doc.hashes_to_change_ids(std::slice::from_ref(&early))
            .and_then(|ids| doc.save_after(&ids)),
        Err(AutomergeError::AuditModeRequired)
    ));

    assert!(doc.get_changes(&load_heads).is_ok());
    assert!(matches!(
        doc.hashes_to_change_ids(&[early])
            .and_then(|ids| doc.get_changes(&ids)),
        Err(AutomergeError::AuditModeRequired)
    ));
    assert!(matches!(
        doc.get_changes(&[]),
        Err(AutomergeError::AuditModeRequired)
    ));
}

/// Diffing from unknown heads errors rather than panicking or diffing partially.
#[test]
fn diff_rejects_foreign_change_ids() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let before = doc.get_heads();
    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();
    let after = doc.get_heads();

    assert!(!doc.diff(&before, &after).unwrap().is_empty());

    let foreign = ChangeId::from_parts(ActorId::random(), std::num::NonZeroU64::new(7).unwrap());
    assert!(matches!(
        doc.diff(std::slice::from_ref(&foreign), &after),
        Err(AutomergeError::InvalidChangeId(_))
    ));
    assert!(matches!(
        doc.document().diff(&[foreign], &after),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

#[test]
fn set_actor_accepts_any_actor() {
    let (bytes, _) = saved_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();

    // aaaa's last change is the head
    doc.set_actor(ActorId::from(&b"aaaa"[..]));
    doc.put(ROOT, "k", 100).unwrap();
    assert!(doc.commit().is_some());

    doc.set_actor(ActorId::random());
}

/// An actor whose tip is buried under another's history must stay resumable.
#[test]
fn set_actor_resurrects_an_actor_whose_tip_is_buried() {
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(&b"aaaa"[..]))
        .enable_audit_mode()
        .unwrap();
    for i in 0..2000 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit();
    }
    doc.set_actor(ActorId::from(&b"bbbb"[..]));
    for i in 0..2000 {
        doc.put(ROOT, "k", 10_000 + i as i64).unwrap();
        doc.commit();
    }
    let bytes = doc.save();

    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    doc.set_actor(ActorId::from(&b"aaaa"[..]));
    doc.put(ROOT, "k", 1).unwrap();
    // the commit's dep on aaaa's old tip needs that hash
    assert!(doc.commit().is_some());

    doc.set_actor(ActorId::from(&b"bbbb"[..]));
    doc.put(ROOT, "k", 2).unwrap();
    assert!(doc.commit().is_some());
}

#[test]
fn disabled_hash_lookups() {
    let (bytes, mut orig) = saved_doc();
    let early = early_hash(&mut orig);
    let mut doc = AutoCommit::load(&bytes).unwrap();

    let head = orig.get_head_hashes()[0];
    assert_eq!(doc.get_head_hashes(), vec![head]);

    let opid = doc.get(ROOT, "k").unwrap().unwrap().1;
    assert_eq!(doc.hash_for_opid(&opid).unwrap(), Some(head));

    // a small doc's whole history is loose commits, so every hash is retained
    assert_eq!(
        doc.hashes_to_change_ids(&[early]).unwrap(),
        orig.hashes_to_change_ids(&[early]).unwrap()
    );

    let (bytes, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    let list = doc
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    doc.commit();
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

    doc.put(ROOT, "k", 100).unwrap();
    doc.commit();

    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    let mut doc = doc.enable_audit_mode().unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);

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

    let reloaded = AutoCommit::load(&doc.save()).unwrap();
    drop(reloaded);
}

/// Outside audit mode, live GC frees hashes covered by cached fragments.
#[test]
fn usurped_fragment_hashes_are_freed_on_live_docs() {
    // plain `Automerge`: `AutoCommit` defers its GC
    let actor = ActorId::from(&b"aaaa"[..]);
    let mut plain = Automerge::new().with_actor(actor.clone());
    let mut audit = Automerge::new()
        .with_actor(actor)
        .enable_audit_mode()
        .unwrap();
    // same actor and edits give identical hashes
    for i in 0..4000 {
        plain
            .transact::<_, _, AutomergeError>(|tx| tx.put(ROOT, "k", i as i64))
            .unwrap();
        audit
            .transact::<_, _, AutomergeError>(|tx| tx.put(ROOT, "k", i as i64))
            .unwrap();
    }
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
    assert!(
        !freed.is_empty(),
        "a 4000-change doc must free covered hashes outside audit mode"
    );
    let head = plain.get_head_hashes()[0];
    assert!(plain.get_change_by_hash(&head).unwrap().is_some());
    assert_eq!(plain.fragments(..), audit.fragments(..));
}

/// Disabling frees interior hashes; re-enabling recomputes and verifies them.
#[test]
fn enable_disable_enable_cycle() {
    let (bytes, unknown) = saved_big_doc_with_unknown_hash();
    let doc = AutoCommit::load(&bytes).unwrap();
    assert!(matches!(
        doc.get_change_by_hash(&unknown),
        Err(AutomergeError::AuditModeRequired)
    ));

    let mut doc = doc.enable_audit_mode().unwrap();
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
    assert_eq!(doc.get_changes(&[]).unwrap().len(), 4000);

    let mut doc = doc.disable_audit_mode();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert!(matches!(
        doc.get_change_by_hash(&unknown),
        Err(AutomergeError::AuditModeRequired)
    ));
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(3999));
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_change_by_hash(&head).unwrap().is_some());

    let doc = doc.enable_audit_mode().unwrap();
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
}

#[test]
fn disabled_multi_head_commit_and_roundtrip() {
    let (bytes, mut orig) = saved_multi_head_doc();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.get_heads().len(), 2);

    doc.put(ROOT, "merged", true).unwrap();
    let id = doc.commit().unwrap();
    assert_eq!(doc.get_heads(), vec![id]);

    // dep hashes in the incremental bytes must be exactly right
    let incr = doc.save_incremental();
    let mut audit = load_audited(&bytes).unwrap();
    audit.load_incremental(&incr).unwrap();
    let mut audit_heads = audit.get_heads();
    let mut heads = doc.get_heads();
    audit_heads.sort();
    heads.sort();
    assert_eq!(audit_heads, heads);

    let saved = doc.save();
    let reloaded = load_audited(&saved).unwrap();
    drop(reloaded);

    let mut doc = doc.enable_audit_mode().unwrap();
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

    let patches = doc.diff(&before, &after).unwrap();
    assert!(!patches.is_empty());

    let foreign = ChangeId::from_parts(ActorId::random(), std::num::NonZeroU64::new(7).unwrap());
    assert!(matches!(
        doc.document().diff(&[foreign], &after),
        Err(AutomergeError::InvalidChangeId(_))
    ));
}

/// Load outside audit mode, append changes, check which fallible APIs work,
/// then enable audit mode.
#[test]
fn disabled_lifecycle_all_fallible_functions() {
    use automerge_sync::{AutoCommitSync, SyncDoc};

    let (bytes, unknown) = saved_big_doc_with_unknown_hash();
    let mut doc = AutoCommit::load(&bytes).unwrap();
    // pinned actor and time keep both commits loose;
    // `unlucky_commit_frees_loose_hashes` covers the other case
    doc.set_actor(ActorId::from(&b"lifecycle"[..]));
    let load_heads = doc.get_heads();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);

    doc.put(ROOT, "k", 100_000).unwrap();
    let new1 = doc
        .commit_with(CommitOptions::default().with_time(0))
        .unwrap();
    let h1 = doc.get_head_hashes()[0];
    doc.put(ROOT, "k", 200_000).unwrap();
    let new2 = doc
        .commit_with(CommitOptions::default().with_time(0))
        .unwrap();
    let h2 = doc.get_head_hashes()[0];
    assert_eq!(
        (h1.fragment_level(), h2.fragment_level()),
        (0, 0),
        "the pinned actor must keep both commits loose; if this fires, \
         pick another actor rather than deleting the assertion"
    );
    assert_eq!(doc.get_heads(), vec![new2.clone()]);

    let err = |r: Result<(), AutomergeError>| {
        assert!(matches!(r, Err(AutomergeError::AuditModeRequired)));
    };
    err(doc.get_changes(&[]).map(|_| ()));
    err(doc.hashes_to_change_ids(&[unknown]).map(|_| ()));
    err(doc.get_changes_meta(&[]).map(|_| ()));

    err(doc.get_change_by_hash(&unknown).map(|_| ()));
    err(doc.get_change_meta_by_hash(&unknown).map(|_| ()));

    // merge and get_changes_added identify changes by (actor, seq), not hash
    let mut other = AutoCommit::new();
    other.put(ROOT, "x", 1).unwrap();
    other.commit();
    let added = doc.get_changes_added(&mut other).unwrap().unwrap();
    assert_eq!(added.iter_changes().unwrap().len(), 1);
    let mut fork = doc.fork();
    fork.merge(&mut other).unwrap();
    let (v, _) = fork.get(ROOT, "x").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(1));
    drop(fork);

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
    assert_eq!(
        doc.get_last_local_change()
            .unwrap()
            .unwrap()
            .heads()
            .collect::<Vec<_>>(),
        doc.change_ids_to_hashes(std::slice::from_ref(&new2))
            .unwrap()
    );

    let mid_fragments = doc.fragments(..);
    assert!(!mid_fragments.is_empty());
    assert!(!doc
        .change_sets_for_fragments(mid_fragments.clone())
        .unwrap()
        .is_empty());

    let mut doc = doc.enable_audit_mode().unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Enabled);

    assert_eq!(doc.get_changes(&[]).unwrap().len(), 4002);
    let unknown_id = doc.hash_to_change_id(&unknown).unwrap().unwrap();
    assert!(!doc
        .get_changes(std::slice::from_ref(&unknown_id))
        .unwrap()
        .is_empty());
    assert!(doc.get_change_by_hash(&unknown).unwrap().is_some());
    assert!(!doc.save_after(&[unknown_id]).unwrap().is_empty());
    let mut audited = doc.clone().into_audit();
    let mut state = automerge_sync::State::new();
    assert!(audited.sync().generate_sync_message(&mut state).is_some());
    assert!(doc.get_changes_added(&mut other).unwrap().is_some());
    doc.merge(&mut other).unwrap();
    let (v, _) = doc.get(ROOT, "x").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(1));

    let fragments = doc.fragments(..);
    let audit = load_audited(&doc.save()).unwrap();
    // apply order depends on each document's own node indexes
    let mut a = fragments.clone();
    let mut b = audit.fragments(..);
    a.sort_by_key(|f| f.head);
    b.sort_by_key(|f| f.head);
    assert_eq!(a, b);
    assert!(!fragments.is_empty());
}

/// A single-change doc stores no hash columns.
#[test]
fn fragments_work_without_hash_columns() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "k", 1).unwrap();
    doc.commit();
    let bytes = doc.save();

    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert_eq!(doc.fragments(..).len(), 1);
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_fragment(head).is_some());
    assert!(!doc
        .change_sets_for_fragments(doc.fragments(..))
        .unwrap()
        .is_empty());

    let doc = doc.enable_audit_mode().unwrap();
    assert_eq!(doc.fragments(..).len(), 1);
}

/// A forged head hash loads on trust, but audit mode recomputes hashes and
/// refuses it.
#[test]
fn bit_flipped_head_loads_disabled_but_fails_audit() {
    use sha2::{Digest, Sha256};

    let (mut bytes, mut orig) = saved_doc();
    let head = orig.get_head_hashes()[0];

    let pos = bytes
        .windows(32)
        .position(|w| w == head.as_ref())
        .expect("head hash bytes present in saved doc");
    bytes[pos] ^= 0x01;

    // recompute the chunk checksum: sha256 of everything after it, first 4 bytes
    let mut hasher = Sha256::new();
    hasher.update(&bytes[8..]);
    let digest = hasher.finalize();
    bytes[4..8].copy_from_slice(&digest[..4]);

    assert!(load_audited(&bytes).is_err());

    let mut doc = AutoCommit::load(&bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(2));
    assert_ne!(
        doc.get_head_hashes(),
        vec![head],
        "head should be the forged one"
    );

    assert!(doc.enable_audit_mode().is_err());
}

/// A disabled doc re-emits the hash columns it imported.
#[test]
fn disabled_state_round_trips() {
    let (bytes, _unknown) = saved_big_doc_with_unknown_hash();
    let mut mid1 = AutoCommit::load(&bytes).unwrap();
    assert_eq!(mid1.audit_mode(), AuditMode::Disabled);
    let frags1 = mid1.fragments(..);

    let resaved = mid1.save();
    let mid2 = AutoCommit::load(&resaved).unwrap();
    assert_eq!(mid2.audit_mode(), AuditMode::Disabled);
    // apply order is per document: compare as sets
    let mut a = mid2.fragments(..);
    let mut b = frags1.clone();
    a.sort_by_key(|f| f.head);
    b.sort_by_key(|f| f.head);
    assert_eq!(a, b);
}

/// A default load without hash columns computes the hashes once, then keeps
/// only the retained set.
#[test]
fn default_load_computes_then_retains_without_columns() {
    // a single-change doc stores no hash columns
    let mut small = AutoCommit::new();
    small.put(ROOT, "k", 1).unwrap();
    small.commit();
    let small_bytes = small.save();
    let mut doc = AutoCommit::load(&small_bytes).unwrap();
    assert_eq!(doc.audit_mode(), AuditMode::Disabled);
    assert_eq!(doc.fragments(..).len(), 1);
    let head = doc.get_head_hashes()[0];
    assert!(doc.get_change_by_hash(&head).unwrap().is_some());
}

/// Applying a change set chain with and without audit mode gives identical documents.
#[test]
fn audit_and_manifold_fragment_apply_agree() {
    let mut src = big_doc_in_audit_mode();

    let fragments = src.fragments(..);
    let change_sets: Vec<_> = fragments
        .into_iter()
        .map(|f| src.document().change_set_for_fragment(&f).unwrap())
        .collect();
    assert!(change_sets.len() > 1);

    let mut plain = Automerge::new();
    let mut audit = Automerge::new().enable_audit_mode().unwrap();

    for b in &change_sets {
        plain.apply_change_set(b.clone()).unwrap();
        audit.apply_change_set(b.clone()).unwrap();
    }

    assert_eq!(plain.audit_mode(), AuditMode::Disabled);
    assert_eq!(audit.audit_mode(), AuditMode::Enabled);
    assert_eq!(plain.get_heads(), audit.get_heads());
    assert_eq!(plain.get_heads(), src.get_heads());

    assert_eq!(audit.get_changes(&[]).unwrap().len(), 4000);

    let plain = plain.enable_audit_mode().unwrap();
    assert_eq!(plain.save(), audit.save());
}

/// An out-of-order change set errors instead of queueing.
#[test]
fn audit_fragment_apply_missing_deps() {
    let mut src = big_doc_in_audit_mode();

    let fragments = src.fragments(..);
    let change_sets: Vec<_> = fragments
        .into_iter()
        .map(|f| src.document().change_set_for_fragment(&f).unwrap())
        .collect();
    assert!(change_sets.len() > 1);

    let mut audit = Automerge::new().enable_audit_mode().unwrap();
    assert!(matches!(
        audit.apply_change_set(change_sets[1].clone()),
        Err(AutomergeError::MissingDeps)
    ));
    assert!(audit.get_heads().is_empty());
}

/// A commit whose hash starts with a zero byte becomes a fragment head and frees
/// the hashes it covers, so emitting those changes errors. The actor is pinned
/// to trigger it.
#[test]
fn unlucky_commit_frees_loose_hashes() {
    let (bytes, _unknown) = saved_big_doc_with_unknown_hash();
    // plain `Automerge`: `AutoCommit` defers its GC
    let mut doc = Automerge::load(&bytes).unwrap();
    doc.set_actor(ActorId::from(&63u32.to_be_bytes()[..]));
    let load_heads = doc.get_heads();

    let commit = |doc: &mut Automerge, v: i64| {
        doc.transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_time(0),
            |tx| {
                tx.put(ROOT, "k", v)?;
                Ok(())
            },
        )
        .unwrap();
    };
    commit(&mut doc, 100_000);
    let h1 = doc.get_head_hashes()[0];
    commit(&mut doc, 200_000);
    let h2 = doc.get_head_hashes()[0];

    assert_eq!(h1.fragment_level(), 0, "the first commit stays loose");
    assert_eq!(
        h2.fragment_level(),
        1,
        "this actor is pinned because its second commit hashes to a \
         fragment head (00493e…)"
    );

    // h2 is a fragment head covering h1
    assert!(matches!(
        doc.get_changes(&load_heads),
        Err(AutomergeError::AuditModeRequired)
    ));

    let (v, _) = doc.get(ROOT, "k").unwrap().unwrap();
    assert_eq!(v.as_i64(), Some(200_000));
}

/// `merge` must work when the GC freed a boundary change's hash. Plain
/// `Automerge` because `AutoCommit` defers its GC; actors are swept since the
/// trigger is a 1/256 hash.
#[test]
fn merge_outside_audit_mode_survives_a_freed_boundary() {
    let encoding = TextEncoding::UnicodeCodePoint;
    let pin = || CommitOptions::default().with_time(0);

    for a in 0..300u32 {
        let mut target = Automerge::new_with_encoding(encoding);
        target.set_actor(ActorId::from(&a.to_be_bytes()[..]));
        let object = target
            .transact_with::<_, _, AutomergeError, _>(
                |_| pin(),
                |tx| tx.put_object(ROOT, "value", automerge::ObjType::Text),
            )
            .unwrap()
            .result;

        let mut left = target.fork();
        left.set_actor(ActorId::from(&(a ^ 0xa5a5_a5a5).to_be_bytes()[..]));
        left.transact_with::<_, _, AutomergeError, _>(
            |_| pin(),
            |tx| tx.splice_text(&object, 0, 0, "left"),
        )
        .unwrap();

        let mut right = target.fork();
        right.set_actor(ActorId::from(&(a ^ 0x5a5a_5a5a).to_be_bytes()[..]));
        right
            .transact_with::<_, _, AutomergeError, _>(
                |_| pin(),
                |tx| tx.splice_text(&object, 0, 0, "right"),
            )
            .unwrap();

        target
            .merge(&mut left)
            .unwrap_or_else(|e| panic!("actor={a}: merge(left) failed: {e}"));
        target
            .merge(&mut right)
            .unwrap_or_else(|e| panic!("actor={a}: merge(right) failed: {e}"));

        assert_eq!(target.audit_mode(), AuditMode::Disabled);
        assert_eq!(target.get_heads().len(), 2, "actor={a}");
        assert_eq!(
            target.text(&object).unwrap().len(),
            "left".len() + "right".len()
        );
    }
}
