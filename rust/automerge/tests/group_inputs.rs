//! Existing apply_changes entry points accept both wire groups and decoded inputs.
use automerge::signature::{ed25519, signing_message};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash, LoadOptions,
    LoadedGroup, PatchLog, ReadDoc, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};

fn author(seed: u8) -> Author<'static> {
    let key = SigningKey::from_bytes(&[seed; 32]);
    Author::from(key.verifying_key().to_bytes().to_vec())
}

/// A receiving replica with its own (never signing) author.
fn receiver_author() -> Author<'static> {
    author(200)
}

/// An attested receiver that never signs anything itself.
fn receiver() -> Automerge {
    Automerge::from_config(
        automerge::Config::default()
            .attested(receiver_author())
            .unwrap(),
    )
}

/// Whether `hash` is published: an attested save is the published
/// projection, so the change is published iff a fresh attested load of
/// the saved bytes contains it.
fn is_published(doc: &Automerge, hash: &ChangeHash) -> bool {
    Automerge::load_with_options(
        &doc.save(),
        LoadOptions::new().attested(receiver_author()).unwrap(),
    )
    .expect("an attested save reloads in an attested document")
    .get_change_by_hash(hash)
    .is_some()
}

fn signed_history() -> (Automerge, Vec<Vec<u8>>, Vec<ChangeHash>) {
    let key = SigningKey::from_bytes(&[91; 32]);
    let mut doc =
        Automerge::from_config(automerge::Config::default().attested(author(91)).unwrap())
            .with_actor(ActorId::from([1]))
            .unwrap();
    let mut hashes = Vec::new();
    // Keep both commits loose, so each needs an independent proof.
    for value in 1..=2_i64 {
        for time in 0.. {
            let mut trial = doc.clone();
            let mut tx = trial.transaction();
            tx.put(ROOT, "value", value).unwrap();
            let hash = tx
                .commit_with(CommitOptions::default().with_time(time))
                .0
                .unwrap();
            if hash.0[0] != 0 {
                doc = trial;
                hashes.push(hash);
                break;
            }
        }
    }
    let batch = hashes.iter().map(|hash| {
        let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes());
        (*hash, sig)
    });
    assert!(matches!(
        doc.insert_signatures(batch),
        automerge::SignatureBatchOutcome::Completed { .. }
    ));
    assert!(!doc.has_unpublished_changes());
    let wire = hashes
        .iter()
        .map(|hash| {
            let fragment = doc.get_fragment(*hash).unwrap();
            doc.bundle_fragments([fragment]).pop().unwrap()
        })
        .collect();
    (doc, wire, hashes)
}

#[test]
fn apply_changes_accepts_owned_group_bytes_and_retains_their_evidence() {
    let (source, wire, hashes) = signed_history();

    let mut target = receiver();
    target.apply_changes(wire.clone()).unwrap();

    assert_eq!(target.get_heads(), source.get_heads());
    for (bytes, hash) in wire.iter().zip(&hashes) {
        assert!(is_published(&target, hash));
        // Attached evidence survived parsing, conversion, and admission.
        let fragment = target.get_fragment(*hash).unwrap();
        assert_eq!(target.bundle_fragments([fragment]).pop().unwrap(), *bytes);
    }
}

#[test]
fn apply_changes_accepts_borrowed_group_bytes() {
    let (source, wire, hashes) = signed_history();

    let mut target = receiver();
    target
        .apply_changes(wire.iter().map(Vec::as_slice))
        .unwrap();

    assert_eq!(target.get_heads(), source.get_heads());
    for hash in &hashes {
        assert!(is_published(&target, hash));
    }
}

#[test]
fn apply_changes_accepts_preparsed_changes_from_a_non_clone_iterator() {
    let (source, wire, _) = signed_history();
    let parsed: Vec<_> = wire
        .iter()
        .map(|bytes| Change::parse_with_signatures(bytes).unwrap())
        .collect();

    let mut target = AutoCommit::from_config(
        automerge::Config::default()
            .attested(receiver_author())
            .unwrap(),
    );
    // No Clone bound on the iterator or the inputs is needed.
    target
        .apply_changes(parsed.into_iter().filter(|_| true))
        .unwrap();

    assert_eq!(target.get_heads(), source.get_heads());
    assert!(!target.has_unpublished_changes());
}

#[test]
fn apply_changes_still_accepts_bare_changes_in_an_unattested_document() {
    let (source, _, _) = signed_history();
    let changes = source.get_changes(&[]);

    let mut unsigned = Automerge::new();
    unsigned.apply_changes(changes.clone()).unwrap();
    let mut unsigned_auto = AutoCommit::new();
    unsigned_auto.apply_changes_batch(changes).unwrap();

    assert_eq!(unsigned.get_heads(), source.get_heads());
    assert_eq!(unsigned_auto.get_heads(), source.get_heads());
}

#[test]
fn decoded_groups_keep_boundaries_and_patch_logging() {
    let (source, wire, hashes) = signed_history();
    let groups: Vec<LoadedGroup> = wire
        .iter()
        .map(|b| b.as_slice().try_into().unwrap())
        .collect();
    let mut target = receiver();
    let mut log = PatchLog::active();
    // The child group stays pending, with its own attached evidence, across calls.
    target
        .apply_changes_log_patches([groups[1].clone()], &mut log)
        .unwrap();
    assert!(target.get_heads().is_empty());
    assert!(target.make_patches(&mut log).is_empty());
    target
        .apply_changes_batch_log_patches([groups[0].clone()], &mut log)
        .unwrap();
    assert_eq!(target.get_heads(), source.get_heads());
    assert!(!target.make_patches(&mut log).is_empty());
    assert!(is_published(&target, &hashes[1]));
    let mut retry_log = PatchLog::active();
    target
        .apply_changes_log_patches(groups, &mut retry_log)
        .unwrap(); // Idempotent redelivery.
    assert!(target.make_patches(&mut retry_log).is_empty());
    target.apply_changes(Vec::<LoadedGroup>::new()).unwrap();
}

#[test]
fn unparseable_group_bytes_leave_the_whole_batch_unapplied() {
    let (_, wire, hashes) = signed_history();

    let mut target = receiver();
    assert!(target.apply_changes([wire[0].clone(), vec![0]]).is_err());

    assert!(target.get_heads().is_empty());
    assert!(!is_published(&target, &hashes[0]));
}

#[test]
fn a_bare_change_in_a_signed_batch_leaves_the_whole_batch_unapplied() {
    let (source, wire, hashes) = signed_history();
    let bare = source.get_change_by_hash(&hashes[1]).unwrap();
    let mixed = [
        LoadedGroup::try_from(wire[0].clone()).unwrap(),
        LoadedGroup::from(bare),
    ];

    let mut target = receiver();
    assert!(target.apply_changes(mixed).is_err());

    assert!(target.get_heads().is_empty());
    // The same document still accepts the fully proved batch afterwards.
    target.apply_changes(wire).unwrap();
    assert_eq!(target.get_heads(), source.get_heads());
}

#[test]
fn snapshots_and_multi_group_byte_blobs_do_not_convert_to_a_loaded_group() {
    let (source, wire, _) = signed_history();

    assert!(LoadedGroup::try_from(source.save()).is_err());
    assert!(LoadedGroup::try_from([wire[0].clone(), wire[1].clone()].concat()).is_err());
}

#[test]
fn unattested_apply_changes_converts_the_whole_batch_before_mutating() {
    let (_, wire, _) = signed_history();

    let mut unsigned = Automerge::new();
    assert!(unsigned.apply_changes([wire[0].clone(), vec![]]).is_err());

    assert!(unsigned.get_heads().is_empty());
}

#[test]
fn a_caller_defined_conversion_failure_leaves_the_batch_unapplied() {
    struct Rejected;
    impl TryFrom<Rejected> for LoadedGroup {
        type Error = AutomergeError;
        fn try_from(_: Rejected) -> Result<Self, Self::Error> {
            Err(AutomergeError::Fail)
        }
    }

    let mut target = receiver();
    assert!(target.apply_changes([Rejected]).is_err());

    assert!(target.get_heads().is_empty());
}
