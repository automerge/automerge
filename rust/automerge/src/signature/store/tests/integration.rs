//! Exercise codec → unchecked store → author association → verification → selection.
//! This is a cross-module integration test kept inside the crate for private APIs.
//! It does not integrate ordinary document loading, atomic application, or export policy.

use std::collections::BTreeMap;

use ed25519_dalek::SigningKey;

use crate::admission::{select, AcceptedHistory, CandidateHistory};
use crate::signature::store::{
    Ed25519VerificationReport, ResolvedChangeAuthor, VerifyEd25519Error,
};
use crate::storage::parse::Input;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::Chunk;
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, AutoCommit, Change, ChangeHash, Fragment, Signature, ROOT};

use super::{sign, Insert, SignatureStore};

struct NoAcceptedHistory;

impl AcceptedHistory for NoAcceptedHistory {
    fn contains(&self, _: &ChangeHash) -> bool {
        false
    }
}

struct Fixture {
    changes: Vec<Change>,
    fragment: Fragment,
    key: SigningKey,
}

fn append_at_level(doc: &mut AutoCommit, value: i64, level: usize) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        next.put(ROOT, "value", value).unwrap();
        let hash = next
            .commit_with(CommitOptions::default().with_time(time))
            .unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture search exhausted for level {level}");
}

fn fixture() -> Fixture {
    let key = SigningKey::from_bytes(&[7; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    let mut doc = AutoCommit::new()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, 1, 0);
    let a2 = append_at_level(&mut doc, 2, 0);
    let a3 = append_at_level(&mut doc, 3, 1);
    let changes = doc.get_changes(&[]);
    assert_eq!(
        changes.iter().map(Change::hash).collect::<Vec<_>>(),
        [a1, a2, a3]
    );
    assert_eq!(changes[1].deps(), &[a1]);
    assert_eq!(changes[2].deps(), &[a2]);
    assert!(changes[0].author().is_some());
    assert!(changes[1].author().is_none());
    assert!(changes[2].author().is_none());
    let fragment = doc.get_fragment(a3).unwrap();
    assert!(fragment.boundary.is_empty());
    assert_eq!(fragment.members.len(), 3);
    assert!([a1, a2, a3]
        .iter()
        .all(|hash| fragment.members.contains(hash)));
    Fixture {
        changes,
        fragment,
        key,
    }
}

// The caller reconstructs this binding from the sequence-1 change, not from
// whichever key happens to verify a signature. The fixture has one valid actor.
fn authors_from_history(changes: &[Change]) -> BTreeMap<ActorId, Author<'static>> {
    assert_eq!(changes[0].seq(), 1);
    let actor = changes[0].actor_id().clone();
    assert!(changes.iter().all(|change| change.actor_id() == &actor));
    BTreeMap::from([(actor, changes[0].author().unwrap().into_owned())])
}

fn encoded_signatures(entries: impl IntoIterator<Item = (ChangeHash, Signature)>) -> Vec<u8> {
    SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap())
        .bytes()
        .to_vec()
}

// Test-only adapter. No author or verification status is assigned by decoding.
fn ingest(store: &mut SignatureStore, bytes: &[u8]) -> Vec<Insert> {
    let (remaining, chunk) = Chunk::parse(Input::new(bytes)).unwrap();
    assert!(remaining.is_empty());
    assert!(chunk.checksum_valid());
    let Chunk::Signatures(signatures) = chunk else {
        panic!("expected signatures chunk");
    };
    signatures
        .iter()
        .map(|(hash, signature)| store.insert_unchecked(hash, signature.clone()))
        .collect()
}

fn assert_pending(candidates: &[CandidateHistory<'_>], store: &SignatureStore) {
    let result = select(&NoAcceptedHistory, candidates, store);
    assert!(result.selected().is_empty());
    assert_eq!(result.pending().len(), 1);
    assert!(std::ptr::eq(result.pending()[0], &candidates[0]));
}

fn assert_selected(candidates: &[CandidateHistory<'_>], store: &SignatureStore) {
    let result = select(&NoAcceptedHistory, candidates, store);
    assert_eq!(result.selected().len(), 1);
    assert!(std::ptr::eq(result.selected()[0], &candidates[0]));
    assert!(result.pending().is_empty());
}

#[test]
fn delayed_signature_chunk_makes_the_same_fragment_selectable_only_after_verification() {
    let fixture = fixture();
    let earlier = &fixture.changes[1];
    let tip = &fixture.changes[2];
    let authors = authors_from_history(&fixture.changes);
    let candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fixture.fragment,
        members: fixture.changes.iter().collect(),
    }];
    let mut store = SignatureStore::default();

    let first = encoded_signatures([(
        earlier.hash(),
        Signature::ed25519(sign(&fixture.key, earlier)),
    )]);
    assert_eq!(ingest(&mut store, &first), [Insert::Stored]);
    assert!(store.author(&earlier.hash()).is_none());
    assert_eq!(store.unchecked_entries(&earlier.hash()).count(), 1);
    assert_eq!(store.ed25519_entries(&earlier.hash()).count(), 0);
    assert_pending(&candidates, &store);

    assert!(matches!(store.verify_ed25519(&earlier.hash()),
        Err(VerifyEd25519Error::UnresolvedAuthor { change }) if change == earlier.hash()));
    assert_eq!(store.unchecked_entries(&earlier.hash()).count(), 1);
    for change in &fixture.changes {
        store
            .associate_author(ResolvedChangeAuthor::resolve(change, &authors).unwrap())
            .unwrap();
    }
    assert_eq!(
        store.verify_ed25519(&earlier.hash()).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 0,
            skipped_other_schemes: 0,
        }
    );
    assert_pending(&candidates, &store); // sig(a2) does not attest a3.

    let delayed = encoded_signatures([(tip.hash(), Signature::ed25519(sign(&fixture.key, tip)))]);
    assert_eq!(ingest(&mut store, &delayed), [Insert::Stored]);
    assert_eq!(store.unchecked_entries(&tip.hash()).count(), 1);
    assert_eq!(store.ed25519_entries(&tip.hash()).count(), 0);
    assert_pending(&candidates, &store); // Decoding alone must not establish evidence.

    assert_eq!(
        store.verify_ed25519(&tip.hash()).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 0,
            skipped_other_schemes: 0,
        }
    );
    assert_selected(&candidates, &store);

    // Re-delivery deduplicates without downgrading the existing evidence.
    assert_eq!(ingest(&mut store, &delayed), [Insert::Duplicate]);
    assert_eq!(store.unchecked_entries(&tip.hash()).count(), 0);
    assert_eq!(store.ed25519_entries(&tip.hash()).count(), 1);
    assert_eq!(
        store.verify_ed25519(&tip.hash()).unwrap(),
        Ed25519VerificationReport::default()
    );
    assert_selected(&candidates, &store);
}

#[test]
fn selected_signature_round_trip_requires_receiver_authorship_and_verification() {
    let fixture = fixture();
    let earlier = &fixture.changes[1];
    let tip = &fixture.changes[2];
    let expected_tip_signature = Signature::ed25519(sign(&fixture.key, tip));
    let mut sender = SignatureStore::default();
    let received = encoded_signatures([
        (
            earlier.hash(),
            Signature::ed25519(sign(&fixture.key, earlier)),
        ),
        (tip.hash(), expected_tip_signature.clone()),
    ]);
    assert_eq!(
        ingest(&mut sender, &received),
        [Insert::Stored, Insert::Stored]
    );
    let authors = authors_from_history(&fixture.changes);
    for change in &fixture.changes {
        sender
            .associate_author(ResolvedChangeAuthor::resolve(change, &authors).unwrap())
            .unwrap();
        sender.verify_ed25519(&change.hash()).unwrap();
    }
    let sender_candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fixture.fragment,
        members: fixture.changes.iter().collect(),
    }];
    assert_selected(&sender_candidates, &sender);

    // The caller deliberately chooses only the tip hash. This is not a store
    // dump, a pruning operation, or a production fragment-export policy.
    let chosen_hashes = [tip.hash()];
    let exported = encoded_signatures(chosen_hashes.into_iter().flat_map(|hash| {
        sender
            .ed25519_entries(&hash)
            .map(move |(_, signature)| (hash, Signature::ed25519(*signature)))
    }));
    assert_eq!(sender.ed25519_entries(&earlier.hash()).count(), 1);

    // Supply change history separately. A signature chunk carries no changes,
    // actor-author mapping, verification verdicts, or prepared keys.
    let receiver_changes: Vec<_> = fixture
        .changes
        .iter()
        .map(|change| Change::from_bytes(change.raw_bytes().to_vec()).unwrap())
        .collect();
    let receiver_candidates = [CandidateHistory::CanonicalFragment {
        fragment: &fixture.fragment,
        members: receiver_changes.iter().collect(),
    }];
    let mut receiver = SignatureStore::default();
    assert_eq!(ingest(&mut receiver, &exported), [Insert::Stored]);
    assert_eq!(
        receiver.unchecked_entries(&tip.hash()).collect::<Vec<_>>(),
        [&expected_tip_signature]
    );
    assert_eq!(receiver.unchecked_entries(&earlier.hash()).count(), 0);
    assert_eq!(receiver.ed25519_entries(&tip.hash()).count(), 0);
    assert!(receiver.author(&tip.hash()).is_none());
    assert!(matches!(receiver.verify_ed25519(&tip.hash()),
        Err(VerifyEd25519Error::UnresolvedAuthor { change }) if change == tip.hash()));
    assert_pending(&receiver_candidates, &receiver);

    let receiver_authors = authors_from_history(&receiver_changes);
    for change in &receiver_changes {
        receiver
            .associate_author(ResolvedChangeAuthor::resolve(change, &receiver_authors).unwrap())
            .unwrap();
    }
    // Associating history does not import the sender's successful verification.
    assert_eq!(receiver.ed25519_entries(&tip.hash()).count(), 0);
    assert_pending(&receiver_candidates, &receiver);
    assert_eq!(
        receiver.verify_ed25519(&tip.hash()).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 1,
            discarded_invalid: 0,
            skipped_other_schemes: 0,
        }
    );
    assert_eq!(receiver.unchecked_entries(&tip.hash()).count(), 0);
    assert_selected(&receiver_candidates, &receiver);
    let reencoded = encoded_signatures(
        receiver
            .ed25519_entries(&tip.hash())
            .map(|(_, signature)| (tip.hash(), Signature::ed25519(*signature))),
    );
    assert_eq!(reencoded, exported);

    // Negative control: valid framing with the same signature paired to a
    // different available hash parses, but fails independent receiver checking.
    let wrong_hash = earlier.hash();
    let mispaired = encoded_signatures([(wrong_hash, expected_tip_signature)]);
    let mut wrong_receiver = SignatureStore::default();
    assert_eq!(ingest(&mut wrong_receiver, &mispaired), [Insert::Stored]);
    wrong_receiver
        .associate_author(
            ResolvedChangeAuthor::resolve(&receiver_changes[1], &receiver_authors).unwrap(),
        )
        .unwrap();
    assert_eq!(
        wrong_receiver.verify_ed25519(&wrong_hash).unwrap(),
        Ed25519VerificationReport {
            established_ed25519_evidence: 0,
            discarded_invalid: 1,
            skipped_other_schemes: 0,
        }
    );
    assert_eq!(wrong_receiver.ed25519_entries(&wrong_hash).count(), 0);
    assert_pending(&receiver_candidates, &wrong_receiver);
}
