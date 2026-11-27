//! Public API integration tests for attested fragment signing and admission.

use automerge::signature::error::AttestationError;
use automerge::signature::error::SigningMessage;
use automerge::signature::{ed25519, parse_signing_message, signing_message, Insert};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash,
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

fn attested_load() -> LoadOptions<'static> {
    LoadOptions::new().attested(receiver_author()).unwrap()
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
fn requested(doc: &mut AutoCommit) -> Vec<ChangeHash> {
    doc.signing_requests()
        .unwrap()
        .iter()
        .map(|r| r.hash())
        .collect()
}

/// Whether `hash` is *published*: an attested save is the published
/// projection, so the change is published iff a fresh attested load of
/// the saved bytes contains it. Interior members of a published cover count
/// even when they no longer export on their own.
fn is_published<D: Batch>(doc: &mut D, hash: &ChangeHash) -> bool {
    loaded_publication(doc).get_change_by_hash(hash).is_some()
}

/// The published projection of `doc`, reloaded once from its saved bytes.
/// Use for bulk checks instead of calling [`is_published`] per hash.
fn loaded_publication<D: Batch>(doc: &mut D) -> Automerge {
    Automerge::load_with_options(&doc.saved(), attested_load())
        .expect("an attested save reloads in an attested document")
}

/// Whether `hash` is *exportable*: its canonical fragment currently bundles
/// with proof. Weaker than [`is_published`]: a signed change blocked behind an
/// unpublished dependency exports but is not published.
fn is_exportable(doc: &AutoCommit, hash: &ChangeHash) -> bool {
    doc.get_fragment(*hash)
        .is_some_and(|f| !doc.bundle_fragments([f]).is_empty())
}

/// The parts of the public signing surface both document types share, so the
/// generic helpers above work against either.
trait Batch {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome;
    fn saved(&mut self) -> Vec<u8>;
}

impl Batch for Automerge {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures(inputs)
    }
    fn saved(&mut self) -> Vec<u8> {
        self.save()
    }
}

impl Batch for AutoCommit {
    fn batch(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures(inputs)
    }
    fn saved(&mut self) -> Vec<u8> {
        self.save()
    }
}

#[test]
fn automerge_withholds_fragments_until_the_commit_is_signed() {
    let (key, author) = key_and_author(11);
    let mut doc = Automerge::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "title", "fragments").unwrap();
    let hash = tx.commit().0.unwrap();

    assert!(!is_published(&mut doc, &hash));
    assert!(doc.has_unpublished_changes());
    assert!(doc.fragments(..).is_empty());

    assert_eq!(
        insert_one(&mut doc, hash, sign(&key, &hash)).unwrap(),
        Insert::Stored,
    );
    assert!(is_published(&mut doc, &hash));
    assert!(!doc.has_unpublished_changes());
    assert_eq!(doc.fragments(..).len(), 1);
}

#[test]
fn autocommit_withholds_fragments_until_the_commit_is_signed() {
    let (key, author) = key_and_author(11);
    let mut doc = AutoCommit::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    doc.put(ROOT, "title", "fragments").unwrap();
    let hash = doc.commit().unwrap();

    assert!(!is_published(&mut doc, &hash));
    assert!(doc.has_unpublished_changes());
    assert!(doc.fragments(..).is_empty());

    assert_eq!(
        insert_one(&mut doc, hash, sign(&key, &hash)).unwrap(),
        Insert::Stored,
    );
    assert!(is_published(&mut doc, &hash));
    assert!(!doc.has_unpublished_changes());
    assert_eq!(doc.fragments(..).len(), 1);
}

/// Alice's single published commit, as exported fragment bytes.
fn published_wire() -> (SigningKey, ChangeHash, Vec<u8>) {
    let (key, author) = key_and_author(11);
    let mut alice = AutoCommit::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    alice.put(ROOT, "title", "fragments").unwrap();
    let a1 = alice.commit().unwrap();
    insert_one(&mut alice, a1, sign(&key, &a1)).unwrap();
    let wire = alice.bundle_fragments(alice.fragments(..)).concat();
    (key, a1, wire)
}

#[test]
fn signed_fragments_load_into_another_attested_document() {
    let (_alice_key, a1, wire) = published_wire();
    let (_bob_key, bob_author) = key_and_author(22);

    let mut bob =
        AutoCommit::load_with_options(&wire, LoadOptions::new().attested(bob_author).unwrap())
            .unwrap();
    assert!(is_published(&mut bob, &a1));
    assert_eq!(
        bob.get(ROOT, "title").unwrap().unwrap().0.as_str(),
        Some("fragments")
    );
}

#[test]
#[allow(deprecated)]
fn a_receiver_signs_its_reply_back_into_the_original_author() {
    let (_alice_key, a1, wire) = published_wire();
    let (_, alice_author) = key_and_author(11);
    let (bob_key, bob_author) = key_and_author(22);

    let mut bob =
        AutoCommit::load_with_options(&wire, LoadOptions::new().attested(bob_author).unwrap())
            .unwrap();
    bob.set_actor(ActorId::from([2])).unwrap();
    bob.put(ROOT, "count", 2_i64).unwrap();
    let b1 = bob.commit().unwrap();
    insert_one(&mut bob, b1, sign(&bob_key, &b1)).unwrap();
    let full_wire = bob.bundle_fragments(bob.fragments(..)).concat();

    // Alice, rebuilt from her own published history, admits Bob's reply.
    let mut alice =
        AutoCommit::load_with_options(&wire, LoadOptions::new().attested(alice_author).unwrap())
            .unwrap();
    assert!(is_published(&mut alice, &a1));
    alice.load_incremental(&full_wire).unwrap();
    assert_eq!(alice.get_heads(), bob.get_heads());
    assert!(is_published(&mut alice, &b1));
    assert_eq!(
        alice.get(ROOT, "count").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
}

#[test]
fn unattested_documents_have_no_attestation_surface() {
    let (key, _author) = key_and_author(11);
    let hash = ChangeHash([3; 32]);
    let mut unsigned = Automerge::new();
    assert!(matches!(
        unsigned.signing_requests().unwrap_err(),
        automerge::attestation::error::NotAttested
    ));
    assert!(matches!(
        unsigned.insert_signatures([(hash, sign(&key, &hash))]),
        SignatureBatchOutcome::Rejected { .. }
    ));
}

#[test]
fn attested_load_rejects_a_bare_snapshot_of_signed_history() {
    let (key, author) = key_and_author(33);
    let mut source = Automerge::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut tx = source.transaction();
    tx.put(ROOT, "k", "v").unwrap();
    let hash = tx.commit().0.unwrap();
    insert_one(&mut source, hash, sign(&key, &hash)).unwrap();

    // An attested save is a signed snapshot that reloads in an attested document;
    // a bare snapshot of the same history is rejected without silent adoption.
    let signed_snapshot = source.save();
    let mut from_snapshot =
        Automerge::load_with_options(&signed_snapshot, attested_load()).unwrap();
    assert_eq!(from_snapshot.get_heads(), vec![hash]);
    assert!(is_published(&mut from_snapshot, &hash));
    assert!(Automerge::load(&signed_snapshot).is_err());

    let mut unsigned_copy = Automerge::new();
    unsigned_copy
        .apply_changes(source.get_changes(&[]))
        .unwrap();
    let bare_snapshot = unsigned_copy.save();
    assert!(matches!(
        Automerge::load_with_options(&bare_snapshot, attested_load()),
        Err(AutomergeError::Signing(
            AttestationError::UnauthenticatedChanges
        ))
    ));
}

#[test]
fn signing_every_required_hash_publishes_the_whole_local_history() {
    let (key, author) = key_and_author(44);
    let mut doc = AutoCommit::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut all_hashes = Vec::new();
    for step in 0..5_i64 {
        doc.put(ROOT, "step", step).unwrap();
        all_hashes.push(
            doc.commit_with(CommitOptions::default().with_time(step))
                .unwrap(),
        );
    }

    // Requests cover canonical fragment frontiers only: every requested hash
    // is a local commit, in hash order, and signing exactly those publishes
    // the whole local history, interior members included.
    let requests = doc.signing_requests().unwrap();
    let required: Vec<_> = requests.iter().map(|r| r.hash()).collect();
    assert!(!required.is_empty());
    assert!(required.len() <= all_hashes.len());
    assert!(required.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(required.iter().all(|hash| all_hashes.contains(hash)));
    assert!(doc.has_unpublished_changes());

    let batch: Vec<_> = requests
        .iter()
        .map(|request| {
            assert_eq!(request.message(), signing_message(&request.hash()));
            assert_eq!(parse_signing_message(request.message()), Ok(request.hash()));
            (request.hash(), sign(&key, &request.hash()))
        })
        .collect();
    let SignatureBatchOutcome::Completed { insertions } = doc.insert_signatures(batch) else {
        panic!("batch should complete");
    };
    assert_eq!(insertions.len(), required.len());
    assert!(insertions
        .iter()
        .all(|i| matches!(i.result, Ok(Insert::Stored))));
    assert!(requested(&mut doc).is_empty());
    let published = loaded_publication(&mut doc);
    assert!(all_hashes
        .iter()
        .all(|hash| published.get_change_by_hash(hash).is_some()));
    assert!(!doc.has_unpublished_changes());
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

/// Bob loads Alice's published history and writes one unpublished draft.
#[allow(deprecated)]
fn receiver_with_a_draft() -> (SigningKey, Author<'static>, AutoCommit, ChangeHash) {
    let (_alice_key, _a1, wire) = published_wire();
    let (bob_key, bob_author) = key_and_author(55);
    let mut bob = AutoCommit::load_with_options(
        &wire,
        LoadOptions::new().attested(bob_author.clone()).unwrap(),
    )
    .unwrap();
    assert!(requested(&mut bob).is_empty());
    bob.set_actor(ActorId::from([2])).unwrap();
    bob.put(ROOT, "bob", 1_i64).unwrap();
    let b1 = bob.commit().unwrap();
    (bob_key, bob_author, bob, b1)
}

#[test]
fn a_receiver_with_a_fixed_author_requests_only_its_own_drafts() {
    let (bob_key, _bob_author, mut bob, b1) = receiver_with_a_draft();
    assert_eq!(requested(&mut bob), vec![b1]);

    insert_one(&mut bob, b1, sign(&bob_key, &b1)).unwrap();
    assert!(is_published(&mut bob, &b1));
    assert!(!bob.has_unpublished_changes());
    assert!(requested(&mut bob).is_empty());
}

#[test]
fn an_unpublished_draft_blocks_changing_or_removing_the_fixed_author() {
    let (_bob_key, bob_author, mut bob, b1) = receiver_with_a_draft();
    let (_, alice_author) = key_and_author(11);

    // The author is fixed per instance: while Bob's draft is unpublished no
    // other author may take over this history, and an attested document always
    // needs a current author.
    assert!(matches!(
        bob.fork_with_author(Some(alice_author)),
        Err(AutomergeError::Signing(
            AttestationError::UnpublishedLocalChanges
        ))
    ));
    assert!(matches!(
        bob.fork_with_author(None),
        Err(AutomergeError::SigningAuthor(_))
    ));
    assert_eq!(bob.get_author(), Some(&bob_author));
    assert_eq!(requested(&mut bob), vec![b1]);
}

#[test]
fn signed_change_behind_unpublished_dependency_is_exportable_but_not_published() {
    let (key, author) = key_and_author(66);
    let mut doc = AutoCommit::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    doc.put(ROOT, "a", 1_i64).unwrap();
    let a = doc.commit().unwrap();
    doc.put(ROOT, "b", 2_i64).unwrap();
    let b = doc.commit().unwrap();

    // Sign only the newer commit: `b` is signature-ready and bundles with
    // proof, yet publication is blocked behind its unsigned dependency `a`.
    insert_one(&mut doc, b, sign(&key, &b)).unwrap();
    assert!(is_exportable(&doc, &b));
    assert!(!is_published(&mut doc, &b));
    assert!(!is_published(&mut doc, &a));
    assert!(doc.has_unpublished_changes());
    assert_eq!(requested(&mut doc), vec![a]);

    // Publishing the dependency publishes both without re-signing `b`.
    insert_one(&mut doc, a, sign(&key, &a)).unwrap();
    assert!(is_published(&mut doc, &a));
    assert!(is_published(&mut doc, &b));
    assert!(!doc.has_unpublished_changes());
    assert!(requested(&mut doc).is_empty());
}

/// A fully published document whose fragments are one level-1 cover (index
/// 1..) plus one loose level-0 commit on top (index 0..=0).
fn published_cover_and_loose_commit() -> (AutoCommit, ChangeHash) {
    let (key, author) = key_and_author(77);
    let mut doc = AutoCommit::from_config(automerge::Config::default().attested(author).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
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
    (doc, loose_hash)
}

#[test]
fn change_parse_with_signatures_recovers_the_signed_commit() {
    let (doc, loose_hash) = published_cover_and_loose_commit();
    let loose_meta = doc.fragments(0..=0).pop().unwrap();
    let signed_commit = doc.bundle_fragments([loose_meta]).pop().unwrap();

    let parsed_change = Change::parse_with_signatures(&signed_commit).unwrap();
    assert_eq!(parsed_change.change().hash(), loose_hash);
    // The bare change inside also parses on its own.
    assert_eq!(
        Change::parse_with_signatures(parsed_change.change().raw_bytes())
            .unwrap()
            .change()
            .hash(),
        loose_hash
    );
}

#[test]
fn change_parse_with_signatures_rejects_bad_framing() {
    let (mut doc, _loose_hash) = published_cover_and_loose_commit();
    let loose_meta = doc.fragments(0..=0).pop().unwrap();
    let signed_commit = doc.bundle_fragments([loose_meta]).pop().unwrap();

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
}

#[test]
fn fragment_parse_with_signatures_recovers_every_cover_member() {
    let (mut doc, _loose_hash) = published_cover_and_loose_commit();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();

    let parsed_bundle = bundle_meta.parse_with_signatures(&signed_bundle).unwrap();
    assert_eq!(parsed_bundle.changes().len(), bundle_meta.members.len());

    // Interior members of the published cover are published although they
    // no longer export independently: the cover reclaimed their proofs.
    let interior: Vec<_> = bundle_meta
        .members
        .iter()
        .copied()
        .filter(|member| *member != bundle_meta.head)
        .collect();
    assert!(!interior.is_empty());
    let published = loaded_publication(&mut doc);
    for member in &interior {
        assert!(published.get_change_by_hash(member).is_some());
        assert!(!is_exportable(&doc, member));
    }
}

#[test]
fn fragment_parse_with_signatures_rejects_bad_framing_and_boundary() {
    let (mut doc, _loose_hash) = published_cover_and_loose_commit();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();

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

#[test]
fn already_parsed_groups_apply_into_a_attested_document_with_their_proofs() {
    let (mut doc, loose_hash) = published_cover_and_loose_commit();
    let loose_meta = doc.fragments(0..=0).pop().unwrap();
    let signed_commit = doc.bundle_fragments([loose_meta]).pop().unwrap();
    let parsed_change = Change::parse_with_signatures(&signed_commit).unwrap();
    let bundle_meta = doc.fragments(1..).pop().unwrap();
    let signed_bundle = doc.bundle_fragments([bundle_meta.clone()]).pop().unwrap();
    let parsed_bundle = bundle_meta.parse_with_signatures(&signed_bundle).unwrap();

    // Applying the already-parsed groups verifies and retains their proofs
    // without re-parsing raw bytes.
    let mut target = AutoCommit::from_config(
        automerge::Config::default()
            .attested(receiver_author())
            .unwrap(),
    );
    target
        .apply_changes([parsed_bundle, LoadedGroup::from(parsed_change)])
        .unwrap();
    assert_eq!(target.get_heads(), doc.get_heads());
    assert!(is_published(&mut target, &loose_hash));
}

#[test]
#[allow(deprecated)]
fn publication_failed_when_local_publication_enables_two_equivocating_groups() {
    // Source holds an unpublished draft `a`. Two same-author clones share the
    // SAME remote actor and each commit a different successor of `a`, so the
    // two signed level-0 groups are an equivocation that becomes admissible
    // only once `a` publishes.
    let (key, author) = key_and_author(88);
    let mut source = AutoCommit::from_config(
        automerge::Config::default()
            .attested(author.clone())
            .unwrap(),
    )
    .with_actor(ActorId::from([1]))
    .unwrap();
    source.put(ROOT, "a", 1_i64).unwrap();
    let a = source.commit().unwrap();

    let mut branch = |value: i64| {
        let mut clone = source.fork_with_author(Some(author.clone())).unwrap();
        clone.set_actor(ActorId::from([2])).unwrap();
        clone.put(ROOT, "b", value).unwrap();
        let b = clone.commit().unwrap();
        insert_one(&mut clone, b, sign(&key, &b)).unwrap();
        let fragment = clone.get_fragment(b).expect("fragment is signature-ready");
        clone.bundle_fragments([fragment]).pop().unwrap()
    };
    let (left_wire, right_wire) = (branch(2), branch(3));
    assert_eq!(source.load_incremental(&left_wire).unwrap(), 0);
    assert_eq!(source.load_incremental(&right_wire).unwrap(), 0);

    let outcome = source.insert_signatures([(a, sign(&key, &a))]);
    let SignatureBatchOutcome::PublicationFailed { insertions, error } = &outcome else {
        panic!("expected PublicationFailed, got {outcome:?}");
    };
    assert_eq!(insertions.len(), 1);
    assert_eq!(insertions[0].hash, a);
    assert!(matches!(insertions[0].result, Ok(Insert::Stored)));
    assert!(matches!(error, automerge::PublicationError::Admission(_)));
    // Local publication retained; neither pending conflict applied.
    assert!(is_published(&mut source, &a));
    assert_eq!(source.get_heads(), vec![a]);
}
