//! Signing handle, signature insertion, and automatic local acceptance tests.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message, Insert};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, Automerge, Change, ChangeHash, PatchLog, ReadDoc, ROOT};

use super::error;

mod author_invariant;
mod batch;
mod fork;
mod fragments;
mod incoming;
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
    doc.try_signing()
        .unwrap()
        .insert_signature(&hash, &sig(key, &hash))
        .unwrap();
}

/// An unsigned replica of `doc`'s applied history writing as `author` with
/// `actor`, for producing foreign-authored changes to deliver back to `doc`.
fn replica(doc: &Automerge, author: Author<'static>, actor: ActorId) -> Automerge {
    let mut remote = Automerge::new_with_author(Some(author)).with_actor(actor);
    remote.apply_changes(doc.get_changes(&[])).unwrap();
    remote
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
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", 1).unwrap();
    tx.commit();
    let heads = doc.get_heads();
    assert!(matches!(
        doc.try_signing(),
        Err(error::SigningAccess::Mode(error::SigningMode::Required))
    ));
    assert_eq!(doc.get_heads(), heads);
}

#[test]
fn local_edits_are_visible_immediately_and_accepted_automatically_when_signed() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
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
    let mut doc = Automerge::signing(author(&alice))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, None, 1, 0);

    // Bob's change arrives from his own replica as a signed incoming group;
    // it waits on `a1`, so it is applied only once `a1` is published.
    let mut bobs = Automerge::new_with_author(Some(author(&bob))).with_actor(ActorId::from([2]));
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
    // Publishing `a1` through the batch API admits Bob's pending group; its
    // recorded author is Bob, so only Bob's signature verifies it.
    doc.insert_signatures([(a1, sig(&alice, &a1))]);
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
    assert!(Automerge::signing(Author::from(vec![1, 2, 3])).is_err());
}

#[test]
fn acceptance_survives_actor_shifts_rollbacks_and_clones() {
    let k = key(7);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([2]));
    let first = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, first, &k);
    assert!(doc.try_signing().unwrap().is_accepted(&first));

    doc.set_actor(ActorId::from([0]));
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
fn local_acceptance_handles_gaps_loose_fallbacks_and_unaccepted_boundaries() {
    let k1 = key(1);

    // 1. Loose gap: signing 0 and 2 accepts only 0 until 1 is signed.
    let mut chain = Automerge::signing(author(&k1))
        .unwrap()
        .with_actor(ActorId::from([1]));
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
    let mut doc = Automerge::signing(author(&k1))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1]));
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
    assert_eq!(doc.try_signing().unwrap().state.signatures.len(), 2);

    // 3. Multi-actor scaling: 20 actors x 15 commits inspect only live verified_hashes.
    let mut many = Automerge::signing(author(&k1)).unwrap();
    let mut frontiers: Vec<(ChangeHash, SigningKey)> = Vec::new();
    for actor_num in 1..=20u8 {
        let k = k1.clone();
        many.set_actor(ActorId::from([actor_num]));
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
    assert_eq!(many.try_signing().unwrap().state.signatures.len(), 20);
    let _: Vec<Change> = Vec::new();
}

#[test]
fn required_signatures_never_returns_non_local_changes_even_with_matching_author() {
    let k1 = key(1);
    let alice = author(&k1);

    // Build a change on a separate document claiming `alice` as its author.
    let mut remote = Automerge::new_with_author(Some(alice.clone())).with_actor(ActorId::from([9]));
    let mut tx = remote.transaction();
    tx.put(ROOT, "sneaky", "forged").unwrap();
    let remote_hash = tx.commit().0.unwrap();
    let remote_change = remote.get_change_by_hash(&remote_hash).unwrap();

    // Even if an unverified non-local change bearing `alice`'s author somehow
    // entered `ChangeGraph` directly via `History::update`, it was not locally
    // authored in `doc` and must never be returned by `required_signatures`.
    let mut doc = Automerge::signing(alice)
        .unwrap()
        .with_actor(ActorId::from([1]));
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
    assert!(!doc
        .try_signing()
        .unwrap()
        .state
        .local_drafts
        .contains(&remote_hash));
    assert!(doc.has_unpublished_changes());

    // A commit on an independent local branch is requested, while a local
    // commit depending on the injected (unpublished, unprovable) change is
    // blocked: the injected hash is never requested to unblock it.
    let dependent = append_at_level(&mut doc, Some(&[remote_hash]), 11, 0);
    let local_hash = append_at_level(&mut doc, Some(&[]), 10, 0);
    assert!(doc
        .try_signing()
        .unwrap()
        .state
        .local_drafts
        .contains(&dependent));
    assert_eq!(
        doc.try_signing().unwrap().required_signatures(),
        vec![local_hash]
    );
    sign_doc(&mut doc, local_hash, &k1);
    assert!(doc.try_signing().unwrap().is_accepted(&local_hash));
    assert!(!doc.try_signing().unwrap().is_accepted(&dependent));
    assert!(doc.try_signing().unwrap().required_signatures().is_empty());
    assert!(doc.has_unpublished_changes());
}

#[test]
fn with_required_skips_unsigned_and_propagates_required_results() {
    use std::cell::Cell;

    let unsigned = Automerge::new();
    let calls = Cell::new(0);
    assert_eq!(
        unsigned.signing.with_required(|| {
            calls.set(calls.get() + 1);
            Err::<(), &str>("never evaluated")
        }),
        Ok(())
    );
    assert_eq!(calls.get(), 0);

    let required = Automerge::signing(author(&key(7))).unwrap();
    assert_eq!(
        required.signing.with_required(|| {
            calls.set(calls.get() + 1);
            Ok::<(), &str>(())
        }),
        Ok(())
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        required.signing.with_required(|| {
            calls.set(calls.get() + 1);
            Err::<(), &str>("rejected")
        }),
        Err("rejected")
    );
    assert_eq!(calls.get(), 2);
}
