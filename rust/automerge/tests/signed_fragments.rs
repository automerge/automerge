//! Public API integration tests for required-mode fragment signing and admission.

use automerge::signature::error::SigningError;
use automerge::signature::error::SigningMessage;
use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    signing, ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash,
    InvalidFragmentBytes, LoadChangeError, LoadOptions, LoadedGroup, ReadDoc,
    SignatureBatchOutcome, SignatureInsertError, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};

fn key_and_author(seed: u8) -> (SigningKey, Author<'static>) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    (key, author)
}

/// A receiving replica with its own (never signing) author.
fn receiver_author() -> Author<'static> {
    key_and_author(200).1
}

fn signing_load() -> LoadOptions<'static> {
    LoadOptions::new().signing().author(receiver_author())
}

fn sign(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

/// Insert one signature through the public batch API, returning its owned
/// per-entry result. The batch itself must not be rejected.
fn insert_one<D: Batch>(
    doc: &mut D,
    hash: ChangeHash,
    signature: ed25519::Signature,
) -> Result<Insert, SignatureInsertError> {
    match doc.batch([(hash, signature)]) {
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

/// Hashes the fixed author should sign next, in hash order.
fn requested<D: Batch>(doc: &mut D) -> Vec<ChangeHash> {
    doc.requests().iter().map(|r| r.hash()).collect()
}

/// Whether `hash` is published: its canonical fragment exports with proof.
fn is_published<D: Batch>(doc: &D, hash: &ChangeHash) -> bool {
    doc.fragment_exports(hash)
}

/// Shared access to the public batch surface of both document types.
trait Batch {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome;
    fn requests(&mut self) -> automerge::SigningRequests;
    fn fragment_exports(&self, hash: &ChangeHash) -> bool;
}

impl Batch for Automerge {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures(inputs)
    }
    fn requests(&mut self) -> automerge::SigningRequests {
        self.request_signatures().unwrap()
    }
    fn fragment_exports(&self, hash: &ChangeHash) -> bool {
        self.get_fragment(*hash)
            .is_some_and(|f| !self.bundle_fragments([f]).is_empty())
    }
}

impl Batch for AutoCommit {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures(inputs)
    }
    fn requests(&mut self) -> automerge::SigningRequests {
        self.request_signatures().unwrap()
    }
    fn fragment_exports(&self, hash: &ChangeHash) -> bool {
        self.get_fragment(*hash)
            .is_some_and(|f| !self.bundle_fragments([f]).is_empty())
    }
}

#[test]
fn public_automerge_and_autocommit_signing_workflow_roundtrips_authenticated_fragments() {
    let (alice_key, alice_author) = key_and_author(11);
    let (bob_key, bob_author) = key_and_author(22);

    let mut alice = AutoCommit::signing(alice_author)
        .unwrap()
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "title", "fragments").unwrap();
    let a1 = alice.commit().unwrap();

    assert!(!is_published(&alice, &a1));
    assert!(alice.has_unpublished_changes());
    assert!(alice.fragments(..).is_empty());

    assert_eq!(
        insert_one(&mut alice, a1, sign(&alice_key, &a1)).unwrap(),
        Insert::Stored,
    );
    assert!(is_published(&alice, &a1));
    assert!(!alice.has_unpublished_changes());

    let alice_wire = alice.bundle_fragments(alice.fragments(..)).concat();

    let mut bob =
        AutoCommit::load_with_options(&alice_wire, LoadOptions::new().signing().author(bob_author))
            .unwrap();
    assert!(is_published(&bob, &a1));
    assert_eq!(
        bob.get(ROOT, "title").unwrap().unwrap().0.as_str(),
        Some("fragments")
    );

    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "count", 2_i64).unwrap();
    let b1 = bob.commit().unwrap();
    insert_one(&mut bob, b1, sign(&bob_key, &b1)).unwrap();

    let full_wire = bob.bundle_fragments(bob.fragments(..)).concat();

    alice.load_incremental(&full_wire).unwrap();
    assert_eq!(alice.get_heads(), bob.get_heads());
    assert!(is_published(&alice, &b1));
    assert_eq!(
        alice.get(ROOT, "count").unwrap().unwrap().0.as_i64(),
        Some(2)
    );

    let mut unsigned = Automerge::new();
    assert!(matches!(
        unsigned.request_signatures().unwrap_err(),
        signing::error::SigningAccess::Mode(signing::error::SigningMode::Required)
    ));
    assert!(matches!(
        unsigned.insert_signatures([(a1, sign(&alice_key, &a1))]),
        SignatureBatchOutcome::Rejected { .. }
    ));
}

#[test]
fn public_signing_documents_reject_bare_snapshots_and_unauthenticated_entry_points() {
    let (key, author) = key_and_author(33);
    let mut source = Automerge::signing(author)
        .unwrap()
        .with_actor(ActorId::from([1]));
    let mut tx = source.transaction();
    tx.put(ROOT, "k", "v").unwrap();
    let (hash, _) = tx.commit();
    let hash = hash.unwrap();
    insert_one(&mut source, hash, sign(&key, &hash)).unwrap();

    // A required-mode save is a signed snapshot that reloads in required mode;
    // a bare snapshot of the same history is rejected without silent adoption.
    let signed_snapshot = source.save();
    let from_snapshot = Automerge::load_with_options(&signed_snapshot, signing_load()).unwrap();
    assert_eq!(from_snapshot.get_heads(), vec![hash]);
    assert!(is_published(&from_snapshot, &hash));
    assert!(Automerge::load(&signed_snapshot).is_err());
    let mut unsigned_copy = Automerge::new();
    unsigned_copy
        .apply_changes(source.get_changes(&[]))
        .unwrap();
    let bare_snapshot = unsigned_copy.save();
    assert!(matches!(
        Automerge::load_with_options(&bare_snapshot, signing_load()),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));

    let mut receiver = Automerge::signing(receiver_author()).unwrap();
    assert!(matches!(
        receiver.load_incremental(&bare_snapshot),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    assert!(receiver.get_heads().is_empty());
    assert!(matches!(
        receiver.apply_changes(source.get_changes(&[])),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    // Merge is the authenticated transport between required-mode documents:
    // it moves the published history with its proof. Mixing modes is
    // rejected in both directions.
    assert!(matches!(
        receiver.merge(&mut unsigned_copy),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(matches!(
        unsigned_copy.merge(&mut source),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(receiver.get_heads().is_empty());
    assert_eq!(receiver.merge(&mut source).unwrap(), vec![hash]);
    assert!(is_published(&receiver, &hash));
    assert!(receiver.request_signatures().unwrap().is_empty());
    assert!(matches!(
        source.fork_at(&[hash]),
        Err(AutomergeError::Signing(SigningError::ForkAtNotSupported))
    ));

    let mut unsigned_peer = Automerge::new().with_actor(ActorId::from([9]));
    {
        let mut tx = unsigned_peer.transaction();
        tx.put(ROOT, "u", 1).unwrap();
        tx.commit();
    }
    let mut peer_state = sync::State::new();
    let msg = unsigned_peer
        .generate_sync_message(&mut peer_state)
        .unwrap()
        .unwrap();
    let mut receiver_state = sync::State::new();
    assert!(matches!(
        receiver.generate_sync_message(&mut receiver_state),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
    assert!(matches!(
        receiver.receive_sync_message(&mut receiver_state, msg),
        Err(AutomergeError::Signing(SigningError::SyncNotSupported))
    ));
}

#[test]
fn required_signatures_tracks_locally_authored_commits_under_the_fixed_author() {
    let (alice_key, alice_author) = key_and_author(44);
    let (bob_key, bob_author) = key_and_author(55);

    let mut alice = AutoCommit::signing(alice_author.clone())
        .unwrap()
        .with_actor(ActorId::from([1]));
    let mut all_hashes = Vec::with_capacity(400);
    for step in 0..400_i64 {
        alice.put(ROOT, "step", step).unwrap();
        let hash = alice
            .commit_with(CommitOptions::default().with_time(step))
            .unwrap();
        all_hashes.push(hash);
    }

    // Requests cover canonical fragment frontiers only: every requested hash
    // is a local commit, in hash order, and signing exactly those publishes
    // the whole local history, interior members included.
    let requests = alice.request_signatures().unwrap();
    let required: Vec<_> = requests.iter().map(|r| r.hash()).collect();
    assert!(!required.is_empty());
    assert!(required.len() <= all_hashes.len());
    assert!(required.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(required.iter().all(|hash| all_hashes.contains(hash)));
    assert!(alice.has_unpublished_changes());
    let batch: Vec<_> = requests
        .iter()
        .map(|request| {
            assert_eq!(request.author(), &alice_author);
            assert_eq!(request.message(), signing_message(&request.hash()));
            assert_eq!(parse_signing_message(request.message()), Ok(request.hash()));
            let sig = ed25519::Signature::from_bytes(alice_key.sign(request.message()).to_bytes());
            (request.hash(), sig)
        })
        .collect();
    let SignatureBatchOutcome::Completed { insertions } = alice.insert_signatures(batch) else {
        panic!("batch should complete");
    };
    assert_eq!(insertions.len(), required.len());
    assert!(insertions
        .iter()
        .all(|i| matches!(i.result, Ok(Insert::Stored))));
    assert!(requested(&mut alice).is_empty());
    assert!(all_hashes.iter().all(|hash| is_published(&alice, hash)));
    assert!(!alice.has_unpublished_changes());

    assert_eq!(
        parse_signing_message(b"wrong-prefix"),
        Err(SigningMessage::InvalidPrefix)
    );
    assert_eq!(
        parse_signing_message(b"automerge-change-signature-v1short"),
        Err(SigningMessage::InvalidHashLength { actual: 5 })
    );

    let wire = alice.bundle_fragments(alice.fragments(..)).concat();
    let mut bob = AutoCommit::load_with_options(
        &wire,
        LoadOptions::new().signing().author(bob_author.clone()),
    )
    .unwrap();
    assert!(requested(&mut bob).is_empty());

    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "bob", 1_i64).unwrap();
    let b1 = bob.commit().unwrap();
    assert_eq!(requested(&mut bob), vec![b1]);

    // The author is fixed per instance: while Bob's draft is unpublished no
    // other author may take over this history, and a signing document always
    // needs a current author.
    assert!(matches!(
        bob.fork_with_author(Some(alice_author)),
        Err(AutomergeError::Signing(
            SigningError::UnpublishedLocalChanges
        ))
    ));
    assert!(matches!(
        bob.fork_with_author(None),
        Err(AutomergeError::SigningAuthor(_))
    ));
    assert_eq!(bob.get_author(), Some(&bob_author));
    assert_eq!(requested(&mut bob), vec![b1]);

    insert_one(&mut bob, b1, sign(&bob_key, &b1)).unwrap();
    assert!(is_published(&bob, &b1));
    assert!(!bob.has_unpublished_changes());
}

#[test]
fn change_and_fragment_parse_with_signatures_validate_framing_and_metadata() {
    let (key, author) = key_and_author(77);
    let mut doc = AutoCommit::signing(author)
        .unwrap()
        .with_actor(ActorId::from([1]));

    let mut step = 0_i64;
    loop {
        step += 1;
        doc.put(ROOT, "step", step).unwrap();
        let hash = doc
            .commit_with(CommitOptions::default().with_time(step))
            .unwrap();
        if step >= 2 && hash.0[0] == 0 {
            break;
        }
    }
    doc.put(ROOT, "tail", 1_i64).unwrap();
    let loose_hash = doc
        .commit_with(CommitOptions::default().with_time(step + 1))
        .unwrap();

    let batch: Vec<_> = requested(&mut doc)
        .into_iter()
        .map(|hash| (hash, sign(&key, &hash)))
        .collect();
    assert!(matches!(
        doc.insert_signatures(batch),
        SignatureBatchOutcome::Completed { .. }
    ));

    let loose_meta = doc.fragments(0..=0).pop().unwrap();
    let signed_commit = doc.bundle_fragments([loose_meta.clone()]).pop().unwrap();
    let parsed_change = Change::parse_with_signatures(&signed_commit).unwrap();
    assert_eq!(parsed_change.change().hash(), loose_hash);
    assert_eq!(
        Change::parse_with_signatures(parsed_change.change().raw_bytes())
            .unwrap()
            .change()
            .hash(),
        loose_hash
    );

    // Reject duplicate groups, snapshots, and corrupted trailing signature chunks.
    let mut two_commits = signed_commit.clone();
    two_commits.extend_from_slice(&signed_commit);
    assert!(matches!(
        Change::parse_with_signatures(&two_commits),
        Err(LoadChangeError::LeftoverData)
    ));
    assert!(matches!(
        Change::parse_with_signatures(&doc.save()),
        Err(LoadChangeError::WrongChunkType)
    ));
    let mut bad_sig_tail = signed_commit.clone();
    *bad_sig_tail.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        Change::parse_with_signatures(&bad_sig_tail),
        Err(LoadChangeError::Parse(_))
    ));

    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();
    let parsed_bundle = bundle_meta.parse_with_signatures(&signed_bundle).unwrap();
    assert_eq!(parsed_bundle.changes().len(), bundle_meta.members.len());

    // Applying the already-parsed groups into a signing document verifies and retains their proofs
    // without re-parsing raw bytes.
    let mut target = AutoCommit::signing(receiver_author()).unwrap();
    target
        .apply_changes([parsed_bundle, LoadedGroup::from(parsed_change)])
        .unwrap();
    assert_eq!(target.get_heads(), doc.get_heads());
    assert!(is_published(&target, &loose_hash));

    let mut doubled_bundle = signed_bundle.clone();
    doubled_bundle.extend_from_slice(&signed_bundle);
    assert!(matches!(
        bundle_meta.parse_with_signatures(&doubled_bundle),
        Err(InvalidFragmentBytes::LeftoverData)
    ));
    assert!(matches!(
        bundle_meta.parse_with_signatures(&doc.save()),
        Err(InvalidFragmentBytes::WrongChunkType)
    ));

    let mut wrong_boundary = bundle_meta.clone();
    wrong_boundary.boundary = vec![ChangeHash([0xff; 32])];
    assert!(matches!(
        wrong_boundary.parse_with_signatures(&signed_bundle),
        Err(InvalidFragmentBytes::BoundaryMismatch)
    ));
}
