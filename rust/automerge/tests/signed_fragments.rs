//! Public API integration tests for required-mode fragment signing and admission.

use automerge::signature::error::{SigningError, SigningMessage};
use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    signing, ActorId, Author, AutoCommit, AutomergeError, ChangeHash, InvalidFragmentBytes,
    LoadChangeError, LoadOptions, LoadedGroup, ReadDoc, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};

fn key_and_author(seed: u8) -> (SigningKey, Author<'static>) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    (key, author)
}

fn sign_all_required(doc: &mut AutoCommit, key: &SigningKey) {
    for hash in doc.try_signing().unwrap().required_signatures() {
        let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
        doc.try_signing()
            .unwrap()
            .insert_signature(&hash, &sig)
            .unwrap();
    }
}

#[test]
fn a_signing_autocommit_withholds_fragments_until_the_commit_is_signed() {
    let (alice_key, alice_author) = key_and_author(11);
    let mut alice = AutoCommit::signing()
        .with_author(Some(alice_author))
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "title", "fragments").unwrap();
    let a1 = alice.commit().unwrap();

    assert!(!alice.try_signing().unwrap().is_published(&a1));
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
    assert!(alice.try_signing().unwrap().is_published(&a1));
    assert!(!alice.fragments(..).is_empty());
}

#[test]
fn a_signed_autocommit_fragment_stream_loads_into_another_document() {
    let (alice_key, alice_author) = key_and_author(11);
    let mut alice = AutoCommit::signing()
        .with_author(Some(alice_author))
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "title", "fragments").unwrap();
    let a1 = alice.commit().unwrap();
    sign_all_required(&mut alice, &alice_key);
    let wire = alice.bundle_fragments(alice.fragments(..)).concat();

    let mut bob = AutoCommit::load_with_options(&wire, LoadOptions::new().signing()).unwrap();

    assert!(bob.try_signing().unwrap().is_published(&a1));
    assert_eq!(
        bob.get(ROOT, "title").unwrap().unwrap().0.as_str(),
        Some("fragments")
    );
}

#[test]
fn a_receiving_document_can_sign_its_own_commit_back_to_the_originator() {
    let (alice_key, alice_author) = key_and_author(11);
    let (bob_key, bob_author) = key_and_author(22);
    let mut alice = AutoCommit::signing()
        .with_author(Some(alice_author))
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "title", "fragments").unwrap();
    alice.commit().unwrap();
    sign_all_required(&mut alice, &alice_key);
    let mut bob = AutoCommit::load_with_options(
        &alice.bundle_fragments(alice.fragments(..)).concat(),
        LoadOptions::new().signing(),
    )
    .unwrap();

    bob.set_author(Some(bob_author));
    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "count", 2_i64).unwrap();
    let b1 = bob.commit().unwrap();
    sign_all_required(&mut bob, &bob_key);

    alice
        .load_incremental(&bob.bundle_fragments(bob.fragments(..)).concat())
        .unwrap();
    assert_eq!(alice.get_heads(), bob.get_heads());
    assert!(alice.try_signing().unwrap().is_published(&b1));
    assert_eq!(
        alice.get(ROOT, "count").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
}

#[test]
fn required_signatures_lists_only_the_current_authors_unsigned_commits() {
    let (_, alice_author) = key_and_author(44);
    let (bob_key, bob_author) = key_and_author(55);
    let mut alice = AutoCommit::signing()
        .with_author(Some(alice_author.clone()))
        .with_actor(ActorId::from([1]));
    alice.put(ROOT, "step", 1_i64).unwrap();
    alice.commit().unwrap();
    sign_all_required(&mut alice, &SigningKey::from_bytes(&[44; 32]));
    let wire = alice.bundle_fragments(alice.fragments(..)).concat();

    let mut bob = AutoCommit::load_with_options(&wire, LoadOptions::new().signing()).unwrap();
    // Imported history is never a local signing request.
    assert!(bob.try_signing().unwrap().required_signatures().is_empty());

    bob.set_author(Some(bob_author));
    bob.set_actor(ActorId::from([2]));
    bob.put(ROOT, "bob", 1_i64).unwrap();
    let b1 = bob.commit().unwrap();
    assert_eq!(bob.try_signing().unwrap().required_signatures(), vec![b1]);

    // Switching the document's current author hides unsigned commits from other authors.
    bob.set_author(Some(alice_author));
    assert!(bob.try_signing().unwrap().required_signatures().is_empty());
    bob.set_author(None);
    assert!(bob.try_signing().unwrap().required_signatures().is_empty());

    bob.set_author(Some(key_and_author(55).1));
    let sig_b1 = ed25519::Signature::from_bytes(bob_key.sign(&signing_message(&b1)).to_bytes());
    bob.try_signing()
        .unwrap()
        .insert_signature(&b1, &sig_b1)
        .unwrap();
    assert!(bob.try_signing().unwrap().is_published(&b1));
    assert!(bob.try_signing().unwrap().required_signatures().is_empty());
}

#[test]
fn a_snapshot_of_a_signing_document_cannot_be_loaded_back_in_signing_mode() {
    // Public-API smoke test for the entry-point restrictions; the full matrix of
    // rejected entry points is pinned in `automerge::signing::tests::incoming`.
    let (key, author) = key_and_author(33);
    let mut source = AutoCommit::signing()
        .with_author(Some(author))
        .with_actor(ActorId::from([1]));
    source.put(ROOT, "k", "v").unwrap();
    source.commit().unwrap();
    sign_all_required(&mut source, &key);

    let snapshot = source.save();

    assert!(matches!(
        AutoCommit::load_with_options(&snapshot, LoadOptions::new().signing()),
        Err(AutomergeError::Signing(SigningError::SnapshotNotSupported))
    ));
}

#[test]
fn a_non_signing_document_has_no_signing_handle() {
    let mut unsigned = AutoCommit::new();

    assert_eq!(
        unsigned.try_signing().unwrap_err(),
        signing::error::SigningMode::Required
    );
}

#[test]
fn a_signing_message_round_trips_through_parse_signing_message() {
    let hash = ChangeHash([3; 32]);

    assert_eq!(parse_signing_message(&signing_message(&hash)), Ok(hash));
}

#[test]
fn parse_signing_message_rejects_a_foreign_prefix_or_a_short_hash() {
    assert_eq!(
        parse_signing_message(b"wrong-prefix"),
        Err(SigningMessage::InvalidPrefix)
    );
    assert_eq!(
        parse_signing_message(b"automerge-change-signature-v1short"),
        Err(SigningMessage::InvalidHashLength { actual: 5 })
    );
}

/// A signing document with one multi-commit bundle fragment (level >= 1) and
/// one loose tail commit, all signed. Returns `(doc, loose_tail_hash)`.
fn doc_with_a_bundle_and_a_loose_tail() -> (AutoCommit, ChangeHash) {
    let (key, author) = key_and_author(77);
    let mut doc = AutoCommit::signing()
        .with_author(Some(author))
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
    sign_all_required(&mut doc, &key);
    (doc, loose_hash)
}

fn signed_loose_commit(doc: &AutoCommit) -> Vec<u8> {
    let meta = doc.fragments(0..=0).pop().unwrap();
    doc.bundle_fragments([meta]).pop().unwrap()
}

#[test]
fn change_parse_with_signatures_reads_back_a_signed_loose_commit() {
    let (doc, loose_hash) = doc_with_a_bundle_and_a_loose_tail();
    let signed_commit = signed_loose_commit(&doc);

    let parsed = automerge::Change::parse_with_signatures(&signed_commit).unwrap();

    assert_eq!(parsed.change().hash(), loose_hash);
    // The same parser accepts the bare change without a signature trailer.
    assert_eq!(
        automerge::Change::parse_with_signatures(parsed.change().raw_bytes())
            .unwrap()
            .change()
            .hash(),
        loose_hash
    );
}

#[test]
fn change_parse_with_signatures_rejects_bad_framing() {
    let (mut doc, _) = doc_with_a_bundle_and_a_loose_tail();
    let signed_commit = signed_loose_commit(&doc);

    let mut two_commits = signed_commit.clone();
    two_commits.extend_from_slice(&signed_commit);
    assert!(matches!(
        automerge::Change::parse_with_signatures(&two_commits),
        Err(LoadChangeError::LeftoverData)
    ));
    assert!(matches!(
        automerge::Change::parse_with_signatures(&doc.save()),
        Err(LoadChangeError::WrongChunkType)
    ));
    let mut bad_sig_tail = signed_commit.clone();
    *bad_sig_tail.last_mut().unwrap() ^= 0xff;
    assert!(matches!(
        automerge::Change::parse_with_signatures(&bad_sig_tail),
        Err(LoadChangeError::Parse(_))
    ));
}

#[test]
fn fragment_parse_with_signatures_reads_back_every_declared_member() {
    let (doc, _) = doc_with_a_bundle_and_a_loose_tail();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();

    let parsed = bundle_meta.parse_with_signatures(&signed_bundle).unwrap();

    assert_eq!(parsed.changes().len(), bundle_meta.members.len());
}

#[test]
fn fragment_parse_with_signatures_rejects_bad_framing_or_a_foreign_boundary() {
    let (mut doc, _) = doc_with_a_bundle_and_a_loose_tail();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();

    let mut doubled = signed_bundle.clone();
    doubled.extend_from_slice(&signed_bundle);
    assert!(matches!(
        bundle_meta.parse_with_signatures(&doubled),
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

#[test]
fn already_parsed_groups_apply_with_their_retained_proofs() {
    let (mut doc, loose_hash) = doc_with_a_bundle_and_a_loose_tail();
    let parsed_change =
        automerge::Change::parse_with_signatures(&signed_loose_commit(&doc)).unwrap();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();
    let parsed_bundle = bundle_meta.parse_with_signatures(&signed_bundle).unwrap();

    let mut target = AutoCommit::signing();
    target
        .apply_changes([parsed_bundle, LoadedGroup::from(parsed_change)])
        .unwrap();

    assert_eq!(target.get_heads(), doc.get_heads());
    assert!(target.try_signing().unwrap().is_published(&loose_hash));
}
