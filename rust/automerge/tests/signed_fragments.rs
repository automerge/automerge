//! Public API integration tests for required-mode fragment signing and admission.

use automerge::signature::error::SigningError;
use automerge::signature::error::SigningMessage;
use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    signing, ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash,
    InvalidFragmentBytes, LoadChangeError, LoadOptions, LoadedGroup, ReadDoc, ROOT,
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

#[test]
fn public_automerge_and_autocommit_signing_workflow_roundtrips_authenticated_fragments() {
    let (alice_key, alice_author) = key_and_author(11);
    let (bob_key, bob_author) = key_and_author(22);

    let mut alice = AutoCommit::signing(alice_author)
        .unwrap()
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "title", "fragments").unwrap();
    let a1 = alice.commit().unwrap();

    assert!(!alice.try_signing().unwrap().is_accepted(&a1));
    assert!(alice.fragments(..).is_empty());

    let sig_a1 = ed25519::Signature::from_bytes(alice_key.sign(&signing_message(&a1)).to_bytes());
    assert_eq!(
        alice
            .try_signing()
            .unwrap()
            .insert_signature(&a1, &sig_a1)
            .unwrap(),
        Insert::Stored,
    );
    assert!(alice.try_signing().unwrap().is_accepted(&a1));

    let alice_wire = alice.bundle_fragments(alice.fragments(..)).concat();

    let mut bob =
        AutoCommit::load_with_options(&alice_wire, LoadOptions::new().signing().author(bob_author))
            .unwrap();
    assert!(bob.try_signing().unwrap().is_accepted(&a1));
    assert_eq!(
        bob.get(ROOT, "title").unwrap().unwrap().0.as_str(),
        Some("fragments")
    );

    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "count", 2_i64).unwrap();
    let b1 = bob.commit().unwrap();
    let sig_b1 = ed25519::Signature::from_bytes(bob_key.sign(&signing_message(&b1)).to_bytes());
    bob.try_signing()
        .unwrap()
        .insert_signature(&b1, &sig_b1)
        .unwrap();

    let full_wire = bob.bundle_fragments(bob.fragments(..)).concat();

    alice.load_incremental(&full_wire).unwrap();
    assert_eq!(alice.get_heads(), bob.get_heads());
    assert!(alice.try_signing().unwrap().is_accepted(&b1));
    assert_eq!(
        alice.get(ROOT, "count").unwrap().unwrap().0.as_i64(),
        Some(2)
    );

    let mut unsigned = Automerge::new();
    assert!(matches!(
        unsigned.try_signing().unwrap_err(),
        signing::error::SigningAccess::Mode(signing::error::SigningMode::Required)
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
    let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    source
        .try_signing()
        .unwrap()
        .insert_signature(&hash, &sig)
        .unwrap();

    // A required-mode save is a signed snapshot that reloads in required mode;
    // a bare snapshot of the same history is rejected without silent adoption.
    let signed_snapshot = source.save();
    let mut from_snapshot = Automerge::load_with_options(&signed_snapshot, signing_load()).unwrap();
    assert_eq!(from_snapshot.get_heads(), vec![hash]);
    assert!(from_snapshot.try_signing().unwrap().is_accepted(&hash));
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
    assert!(receiver.try_signing().unwrap().is_accepted(&hash));
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
    let required = alice.try_signing().unwrap().required_signatures();
    assert!(!required.is_empty());
    assert!(required.len() <= all_hashes.len());
    assert!(required.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(required.iter().all(|hash| all_hashes.contains(hash)));
    assert!(alice.has_unpublished_changes());
    for hash in &required {
        let msg = signing_message(hash);
        assert_eq!(parse_signing_message(&msg), Ok(*hash));
        let sig = ed25519::Signature::from_bytes(alice_key.sign(&msg).to_bytes());
        alice
            .try_signing()
            .unwrap()
            .insert_signature(hash, &sig)
            .unwrap();
    }
    assert!(alice
        .try_signing()
        .unwrap()
        .required_signatures()
        .is_empty());
    assert!(all_hashes
        .iter()
        .all(|hash| alice.try_signing().unwrap().is_accepted(hash)));
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
    assert!(bob.try_signing().unwrap().required_signatures().is_empty());

    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "bob", 1_i64).unwrap();
    let b1 = bob.commit().unwrap();
    assert_eq!(bob.try_signing().unwrap().required_signatures(), vec![b1]);

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
    assert_eq!(bob.try_signing().unwrap().required_signatures(), vec![b1]);

    let sig_b1 = ed25519::Signature::from_bytes(bob_key.sign(&signing_message(&b1)).to_bytes());
    bob.try_signing()
        .unwrap()
        .insert_signature(&b1, &sig_b1)
        .unwrap();
    assert!(bob.try_signing().unwrap().is_accepted(&b1));
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

    for hash in doc.try_signing().unwrap().required_signatures() {
        let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
        doc.try_signing()
            .unwrap()
            .insert_signature(&hash, &sig)
            .unwrap();
    }

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
    assert!(target.try_signing().unwrap().is_accepted(&loose_hash));

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
