//! Existing apply_changes entry points accept both wire groups and decoded inputs.
use automerge::signature::{ed25519, signing_message};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, AutoCommit, Automerge, AutomergeError, Change, ChangeHash, LoadedGroup,
    PatchLog, ReadDoc, ROOT,
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

fn signed_history() -> (Automerge, Vec<Vec<u8>>, Vec<ChangeHash>) {
    let key = SigningKey::from_bytes(&[91; 32]);
    let mut doc = Automerge::from_config(automerge::Config::default().signing(author(91)).unwrap())
        .with_actor(ActorId::from([1]));
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
    for hash in &hashes {
        let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes());
        doc.try_signing()
            .unwrap()
            .insert_signature(hash, &sig)
            .unwrap();
    }
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
fn apply_changes_accepts_owned_borrowed_and_preparsed_groups() {
    let (source, wire, hashes) = signed_history();
    let mut owned = Automerge::from_config(
        automerge::Config::default()
            .signing(receiver_author())
            .unwrap(),
    );
    owned.apply_changes(wire.clone()).unwrap();
    let mut borrowed = Automerge::from_config(
        automerge::Config::default()
            .signing(receiver_author())
            .unwrap(),
    );
    borrowed
        .apply_changes(wire.iter().map(Vec::as_slice))
        .unwrap();
    let parsed: Vec<_> = wire
        .iter()
        .map(|bytes| Change::parse_with_signatures(bytes).unwrap())
        .collect();
    let mut autocommit = AutoCommit::from_config(
        automerge::Config::default()
            .signing(receiver_author())
            .unwrap(),
    );
    // No Clone bound on the iterator or the inputs is needed.
    autocommit
        .apply_changes(parsed.into_iter().filter(|_| true))
        .unwrap();
    for target in [&mut owned, &mut borrowed] {
        assert_eq!(target.get_heads(), source.get_heads());
        for hash in &hashes {
            assert!(target.try_signing().unwrap().is_accepted(hash));
        }
        // Attached evidence survived parsing, conversion, and admission.
        for (bytes, hash) in wire.iter().zip(&hashes) {
            let fragment = target.get_fragment(*hash).unwrap();
            assert_eq!(target.bundle_fragments([fragment]).pop().unwrap(), *bytes);
        }
    }
    assert_eq!(autocommit.get_heads(), source.get_heads());
    assert!(autocommit.try_signing().unwrap().is_accepted(&hashes[1]));

    // Bare Changes remain supported in unsigned mode, including the batch alias.
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
    let mut target = Automerge::from_config(
        automerge::Config::default()
            .signing(receiver_author())
            .unwrap(),
    );
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
    assert!(target.try_signing().unwrap().is_accepted(&hashes[1]));
    let mut retry_log = PatchLog::active();
    target
        .apply_changes_log_patches(groups, &mut retry_log)
        .unwrap(); // Idempotent redelivery.
    assert!(target.make_patches(&mut retry_log).is_empty());
    target.apply_changes(Vec::<LoadedGroup>::new()).unwrap();
}

#[test]
fn conversion_errors_and_missing_proofs_leave_the_whole_batch_unapplied() {
    let (source, wire, hashes) = signed_history();
    let bare = source.get_change_by_hash(&hashes[1]).unwrap();
    let mut target = Automerge::from_config(
        automerge::Config::default()
            .signing(receiver_author())
            .unwrap(),
    );
    assert!(target.apply_changes([wire[0].clone(), vec![0]]).is_err());
    assert!(target.get_heads().is_empty());
    assert!(!target.try_signing().unwrap().is_accepted(&hashes[0]));

    let mixed = [
        LoadedGroup::try_from(wire[0].clone()).unwrap(),
        LoadedGroup::from(bare),
    ];
    assert!(target.apply_changes(mixed).is_err());
    assert!(target.get_heads().is_empty());
    // No conversion or byte input can bypass the snapshot restriction.
    assert!(LoadedGroup::try_from(source.save()).is_err());
    assert!(LoadedGroup::try_from([wire[0].clone(), wire[1].clone()].concat()).is_err());

    // Even unsigned application must convert the entire batch before mutation.
    let mut unsigned = Automerge::new();
    assert!(unsigned.apply_changes([wire[0].clone(), vec![]]).is_err());
    assert!(unsigned.get_heads().is_empty());

    // The conversion bound also supports caller-defined fallible input types.
    struct Rejected;
    impl TryFrom<Rejected> for LoadedGroup {
        type Error = AutomergeError;
        fn try_from(_: Rejected) -> Result<Self, Self::Error> {
            Err(AutomergeError::Fail)
        }
    }
    assert!(target.apply_changes([Rejected]).is_err());
    assert!(target.get_heads().is_empty());
    target.apply_changes(wire).unwrap();
    assert_eq!(target.get_heads(), source.get_heads());
}
