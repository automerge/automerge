//! Signing handle, signature insertion, and automatic local acceptance tests.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message, Insert};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, Automerge, Change, ChangeHash, PatchLog, ReadDoc, ROOT};

use super::error;
use crate::{SignatureBatchOutcome, SignatureInsertError};

mod batch;
mod fork;
mod fragments;
mod identity;
mod incoming;
mod persistence;
mod requests;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

fn sig(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

fn sign_doc(doc: &mut Automerge, hash: ChangeHash, key: &SigningKey) {
    insert_one(doc, hash, sig(key, &hash)).unwrap();
}

fn insert_one(
    doc: &mut Automerge,
    hash: ChangeHash,
    signature: ed25519::Signature,
) -> Result<Insert, SignatureInsertError> {
    match doc.insert_signatures([(hash, signature)]) {
        SignatureBatchOutcome::Completed { mut insertions }
        | SignatureBatchOutcome::PublicationFailed { mut insertions, .. } => {
            insertions
                .pop()
                .expect("one input yields one outcome")
                .result
        }
        SignatureBatchOutcome::Rejected { error } => panic!("batch rejected: {error}"),
    }
}

fn replica(doc: &Automerge, author: Author<'static>, actor: ActorId) -> Automerge {
    let mut remote = Automerge::from_config(crate::Config::default().author(Some(author)))
        .with_actor(actor)
        .unwrap();
    remote.apply_changes(doc.get_changes(&[])).unwrap();
    remote
}

fn requested_hashes(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.signing_requests()
        .unwrap()
        .iter()
        .map(|request| request.hash())
        .collect()
}

fn append_at_level(
    doc: &mut Automerge,
    heads: Option<&[ChangeHash]>,
    value: i64,
    level: usize,
) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = match heads {
            Some(heads) => next.transaction_at(PatchLog::inactive(), heads).unwrap(),
            None => next.transaction(),
        };
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted for level {level}");
}

#[test]
fn unsigned_document_rejects_try_signing_without_converting_mode() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1])).unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", 1).unwrap();
    tx.commit();
    let heads = doc.get_heads();
    assert!(matches!(
        doc.try_signing(),
        Err(error::SigningMode::Required)
    ));
    assert_eq!(doc.get_heads(), heads);
}

#[test]
fn local_edits_are_visible_immediately_and_accepted_automatically_when_signed() {
    let k = key(7);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a1 = append_at_level(&mut doc, None, 1, 0);

    assert_eq!(doc.get_heads(), [a1]);
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0, 1.into());
    assert!(!doc.try_signing().unwrap().is_accepted(&a1));

    assert_eq!(
        doc.try_signing()
            .unwrap()
            .insert_signature(&a1, &sig(&k, &a1))
            .unwrap(),
        Insert::Stored,
    );
    assert!(doc.try_signing().unwrap().is_accepted(&a1));
}

#[test]
fn insertion_enforces_recorded_change_authors_and_rejects_bad_inputs() {
    let alice = key(7);
    let bob = key(8);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&alice)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a1 = append_at_level(&mut doc, None, 1, 0);

    // Bob's change arrives from his own replica as a signed incoming group;
    // it waits on `a1`, so it is applied only once `a1` is published.
    let mut bobs = Automerge::from_config(crate::Config::default().author(Some(author(&bob))))
        .with_actor(ActorId::from([2]))
        .unwrap();
    bobs.apply_changes([doc.get_change_by_hash(&a1).unwrap()])
        .unwrap();
    let b1 = append_at_level(&mut bobs, None, 2, 0);
    let b1_bytes = [
        bobs.get_change_by_hash(&b1).unwrap().raw_bytes(),
        crate::storage::signatures::SignaturesChunk::encode(
            crate::storage::signatures::HashEntries::canonicalize([(
                b1,
                crate::Signature::ed25519(sig(&bob, &b1)),
            )])
            .unwrap(),
        )
        .bytes(),
    ]
    .concat();
    assert_eq!(doc.load_incremental(&b1_bytes).unwrap(), 0);

    {
        let mut signing = doc.try_signing().unwrap();
        assert!(matches!(
            signing.insert_signature(&a1, &sig(&bob, &a1)),
            Err(error::InsertSignature::Verification { change, .. }) if change == a1
        ));
        let unknown = ChangeHash([0xff; 32]);
        assert!(matches!(
            signing.insert_signature(&unknown, &sig(&alice, &unknown)),
            Err(error::InsertSignature::UnknownChange { change }) if change == unknown
        ));
        assert!(matches!(
            signing.insert_signature(&b1, &sig(&bob, &b1)),
            Err(error::InsertSignature::UnknownChange { change }) if change == b1
        ));
    }
    // Publish `a1`, then retry delivery (batch retry is introduced later); its
    // recorded author is Bob, so only Bob's signature verifies it.
    sign_doc(&mut doc, a1, &alice);
    doc.load_incremental(&b1_bytes).unwrap();
    assert!(doc.try_signing().unwrap().is_accepted(&a1));
    assert!(doc.try_signing().unwrap().is_accepted(&b1));
    assert_eq!(
        doc.get_author_for_actor(&ActorId::from([2])),
        Some(author(&bob))
    );
    assert!(matches!(
        doc.try_signing().unwrap().insert_signature(&b1, &sig(&alice, &b1)),
        Err(error::InsertSignature::Verification { change, .. }) if change == b1
    ));

    // Signing documents cannot be constructed without a usable author, so the
    // missing- and invalid-author insertion failures only arise for imported
    // history; see `incoming`.
    assert!(crate::Config::default()
        .signing(Author::from(vec![1, 2, 3]))
        .map(Automerge::from_config)
        .is_err());
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn acceptance_survives_actor_shifts_rollbacks_and_clones() {
    let k = key(7);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([2]))
        .unwrap();
    let first = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, first, &k);
    assert!(doc.try_signing().unwrap().is_accepted(&first));

    doc.set_actor(ActorId::from([0])).unwrap();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "value", 2).unwrap();
        assert_eq!(tx.rollback(), 1);
    }
    assert!(doc.try_signing().unwrap().is_accepted(&first));

    let second = append_at_level(&mut doc, None, 3, 0);
    let mut cloned = doc.clone();
    assert!(cloned.try_signing().unwrap().is_accepted(&first));
    assert!(!cloned.try_signing().unwrap().is_accepted(&second));

    sign_doc(&mut cloned, second, &k);
    assert!(cloned.try_signing().unwrap().is_accepted(&second));
    assert!(!doc.try_signing().unwrap().is_accepted(&second));
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn local_acceptance_handles_gaps_loose_fallbacks_and_unaccepted_boundaries() {
    let k1 = key(1);

    // 1. Loose gap: signing 0 and 2 accepts only 0 until 1 is signed.
    let mut chain = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let c0 = append_at_level(&mut chain, None, 1, 0);
    let c1 = append_at_level(&mut chain, None, 2, 0);
    let c2 = append_at_level(&mut chain, None, 3, 0);
    sign_doc(&mut chain, c0, &k1);
    sign_doc(&mut chain, c2, &k1);
    assert!(chain.try_signing().unwrap().is_accepted(&c0));
    assert!(!chain.try_signing().unwrap().is_accepted(&c1));
    assert!(!chain.try_signing().unwrap().is_accepted(&c2));
    sign_doc(&mut chain, c1, &k1);
    assert!(chain.try_signing().unwrap().is_accepted(&c1));
    assert!(chain.try_signing().unwrap().is_accepted(&c2));

    // 2. Proved container {a, c} waiting on unaccepted boundary b does not hide
    //    local fallback a, and signing b completes acceptance and reclaims sig(a)!
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1])).unwrap();
    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);

    sign_doc(&mut doc, c, &k1);
    assert!(!doc.try_signing().unwrap().is_accepted(&a));
    assert!(!doc.try_signing().unwrap().is_accepted(&c));
    assert_eq!(doc.fragments(..).len(), 1);

    // Signing fallback `a` accepts `a` even though container `{a, c}` is already
    // ready for outgoing export while waiting on unaccepted boundary `b`!
    sign_doc(&mut doc, a, &k1);
    assert!(doc.try_signing().unwrap().is_accepted(&a));
    assert!(!doc.try_signing().unwrap().is_accepted(&b));
    assert!(!doc.try_signing().unwrap().is_accepted(&c));

    sign_doc(&mut doc, b, &k1);
    assert!(doc.try_signing().unwrap().is_accepted(&b));
    assert!(doc.try_signing().unwrap().is_accepted(&c));
    assert_eq!(
        doc.try_signing()
            .unwrap()
            .state
            .signatures
            .verified_hashes()
            .count(),
        2
    );

    // 3. Multi-actor scaling: 20 actors x 15 commits inspect only live verified_hashes.
    let mut many = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap());
    let mut frontiers: Vec<(ChangeHash, SigningKey)> = Vec::new();
    for actor_num in 1..=20u8 {
        let k = k1.clone();
        many.set_actor(ActorId::from([actor_num])).unwrap();
        for step in 0..15 {
            let h = append_at_level(&mut many, None, i64::from(actor_num) * 100 + step, 0);
            if step == 14 {
                frontiers.push((h, k.clone()));
            }
        }
    }
    let tip = append_at_level(&mut many, None, 9_999, 1);
    frontiers.pop();
    frontiers.push((tip, k1.clone()));
    for (h, k) in &frontiers {
        sign_doc(&mut many, *h, k);
    }
    assert!(many.try_signing().unwrap().is_accepted(&tip));
    assert_eq!(
        many.try_signing()
            .unwrap()
            .state
            .signatures
            .verified_hashes()
            .count(),
        20
    );
    let _: Vec<Change> = Vec::new();
}

#[test]
fn required_signatures_never_returns_non_local_changes_even_with_matching_author() {
    let k1 = key(1);
    let alice = author(&k1);

    // Build a change on a separate document claiming `alice` as its author.
    let mut remote = Automerge::from_config(crate::Config::default().author(Some(alice.clone())))
        .with_actor(ActorId::from([9]))
        .unwrap();
    let mut tx = remote.transaction();
    tx.put(ROOT, "sneaky", "forged").unwrap();
    let remote_hash = tx.commit().0.unwrap();
    let remote_change = remote.get_change_by_hash(&remote_hash).unwrap();

    // Even if an unverified non-local change bearing `alice`'s author somehow
    // entered `ChangeGraph` directly via `History::update`, it was not locally
    // authored in `doc` and must never be returned by `required_signatures`.
    let mut doc = Automerge::from_config(crate::Config::default().signing(alice).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    doc.put_actor(ActorId::from([9]));
    let _ = crate::automerge::history::History::new(
        &mut doc.change_graph,
        &mut doc.deps,
        &mut doc.authors,
        &doc.ops.actors,
        &doc.visibility,
        &mut doc.signing,
    )
    .update(&remote_change);

    assert!(doc.try_signing().unwrap().required_signatures().is_empty());

    // A commit actually authored locally on `doc` is tracked and cleared once signed.
    let local_hash = append_at_level(&mut doc, None, 10, 0);
    assert_eq!(
        doc.try_signing().unwrap().required_signatures(),
        vec![local_hash]
    );
    sign_doc(&mut doc, local_hash, &k1);
    assert!(doc.try_signing().unwrap().required_signatures().is_empty());
}
