//! Alternate native entry points cannot bypass required-mode admission, and
//! unsigned entry points never swallow signed content.

use crate::signature::error::SigningError;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{AutoCommit, AutomergeError, LoadOptions, LoadedGroup, Signature};

use super::*;

fn signed_group(doc: &Automerge, hash: ChangeHash, key: &SigningKey) -> Vec<u8> {
    [
        doc.get_change_by_hash(&hash).unwrap().raw_bytes(),
        SignaturesChunk::encode(
            HashEntries::canonicalize([(hash, Signature::ed25519(sig(key, &hash)))]).unwrap(),
        )
        .bytes(),
    ]
    .concat()
}

#[test]
fn raw_changes_are_rejected_in_required_mode_and_signed_groups_in_unsigned_mode() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let raw = source.get_change_by_hash(&a).unwrap();
    let signed = signed_group(&source, a, &k);
    let parsed = Change::parse_with_signatures(&signed).unwrap();
    let group = LoadedGroup::try_from(signed.as_slice()).unwrap();

    // Required receiver: raw Change, raw bytes and bare LoadedGroup all reject.
    let mut required =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    let mut auto_required =
        AutoCommit::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    let bare_group = LoadedGroup::from(raw.clone());
    for result in [
        required.apply_changes([raw.clone()]),
        required.apply_changes_batch([raw.clone()]),
        required.apply_changes([raw.raw_bytes().to_vec()]),
        required.apply_changes([bare_group.clone()]),
        auto_required.apply_changes([raw.clone()]),
        auto_required.apply_changes_batch([bare_group]),
    ] {
        assert!(matches!(
            result,
            Err(AutomergeError::Signing(
                SigningError::UnauthenticatedChanges
            ))
        ));
    }
    assert!(required.get_heads().is_empty());
    // The same content with its proof is admitted through every form.
    let mut by_bytes =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    by_bytes.apply_changes([signed.clone()]).unwrap();
    let mut by_parsed =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    by_parsed.apply_changes([parsed.clone()]).unwrap();
    let mut by_group =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    by_group.apply_changes_batch([group.clone()]).unwrap();
    for doc in [&mut by_bytes, &mut by_parsed, &mut by_group] {
        assert_eq!(doc.get_heads(), [a]);
        assert!(doc.try_signing().unwrap().is_accepted(&a));
    }

    // Unsigned receiver: signed bytes, parsed and decoded groups all reject.
    let mut unsigned = Automerge::new();
    let mut auto_unsigned = AutoCommit::new();
    for result in [
        unsigned.apply_changes([signed.clone()]),
        unsigned.apply_changes([parsed.clone()]),
        unsigned.apply_changes_batch([group.clone()]),
        unsigned.load_incremental(&signed).map(drop),
        auto_unsigned.apply_changes([parsed]),
        auto_unsigned.apply_changes_batch([group]),
        auto_unsigned.load_incremental(&signed).map(drop),
    ] {
        assert!(matches!(
            result,
            Err(AutomergeError::Load(
                crate::storage::load::Error::SignedContent
            ))
        ));
    }
    assert!(unsigned.get_heads().is_empty());
    assert!(auto_unsigned.get_heads().is_empty());
    assert!(matches!(
        Automerge::load(&signed),
        Err(AutomergeError::Load(
            crate::storage::load::Error::SignedContent
        ))
    ));
    // Raw changes still apply to unsigned receivers as before.
    unsigned.apply_changes([raw]).unwrap();
    assert_eq!(unsigned.get_heads(), [a]);
}

#[test]
fn rescue_refuses_signed_bytes_rather_than_dropping_their_guarantees() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let signed_snapshot = source.save();
    let b = append_at_level(&mut source, None, 2, 0);
    sign_doc(&mut source, b, &k);
    let signed_group = source.save_after(&[a]);

    for bytes in [signed_snapshot.as_slice(), signed_group.as_slice()] {
        assert!(matches!(
            Automerge::rescue(bytes),
            Err(AutomergeError::Signing(SigningError::RescueNotSupported))
        ));
    }
    // Unsigned snapshots still rescue.
    let mut unsigned = Automerge::new().with_actor(ActorId::from([2])).unwrap();
    append_at_level(&mut unsigned, None, 7, 0);
    let value = Automerge::rescue(&unsigned.save()).unwrap();
    assert_eq!(value, unsigned.hydrate(None));
}

#[test]
fn arbitrary_bundles_reject_signing_mode_but_fragment_export_still_works() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 1);
    for hashes in [vec![], vec![a], vec![a, b]] {
        assert!(matches!(
            doc.bundle(hashes),
            Err(AutomergeError::Signing(SigningError::BundleNotSupported))
        ));
    }
    sign_doc(&mut doc, b, &k);
    assert!(matches!(
        doc.bundle([a, b]),
        Err(AutomergeError::Signing(SigningError::BundleNotSupported))
    ));
    let mut peer =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    peer.load_incremental(&doc.bundle_fragments(doc.fragments(..)).concat())
        .unwrap();
    assert_eq!(peer.get_heads(), doc.get_heads());
    assert!(!peer.has_unpublished_changes());

    let mut unsigned = Automerge::new();
    unsigned.apply_changes(doc.get_changes(&[])).unwrap();
    let bundle = unsigned.bundle([a, b]).unwrap();
    assert_eq!(bundle.iter_changes().count(), 2);
    let mut target = Automerge::new();
    target.apply_changes([bundle.bytes()]).unwrap();
    assert_eq!(target.get_heads(), unsigned.get_heads());
}

#[test]
fn inspectors_expose_applied_drafts_but_transfers_do_not() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let d = append_at_level(&mut source, None, 2, 0);

    // Inspection: raw changes include the draft.
    assert_eq!(source.get_changes(&[]).len(), 2);
    assert!(source.get_change_by_hash(&d).is_some());
    // Transfers: only published history.
    let mut peer =
        Automerge::from_config(crate::Config::default().signing(author(&key(2))).unwrap());
    peer.merge(&mut source).unwrap();
    assert_eq!(peer.get_heads(), [a]);
    let loaded = Automerge::load_with_options(
        &source.save(),
        LoadOptions::new().signing(author(&key(3))).unwrap(),
    )
    .unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert!(source.save_after(&[a]).is_empty());
    // Signature-ready export may expose blocked-but-proved groups, but a
    // bare draft is not ready and is never exported.
    assert!(source.get_fragment(d).is_none());
    assert_eq!(source.bundle_fragments(source.fragments(..)).len(), 1);
}

#[test]
fn anonymize_yields_a_new_unsigned_diagnostic_history_without_proofs_or_drafts() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let d = append_at_level(&mut source, None, 2, 0);
    let before = source.save();

    let mut anonymous = source.anonymize().unwrap();
    // Diagnostic copy: rewritten hashes, unsigned mode, no author identity,
    // no signatures, no local drafts. Structural shape (two changes) kept.
    assert!(!anonymous.signing.is_required());
    assert_eq!(anonymous.get_changes(&[]).len(), 2);
    assert!(anonymous.get_change_by_hash(&a).is_none());
    assert!(anonymous.get_change_by_hash(&d).is_none());
    assert!(anonymous.get_authors().is_empty());
    assert!(anonymous.get_author().is_none());
    assert!(!anonymous.has_unpublished_changes());
    assert!(anonymous.signing_requests().is_err());
    // Exports carry no proof: each is a bare change/bundle chunk, and an
    // unsigned load of the whole save succeeds (it would reject any attached
    // signatures chunk).
    let exports = anonymous.bundle_fragments(anonymous.fragments(..));
    assert_eq!(exports.len(), 2);
    for bytes in &exports {
        let (rest, chunk) = crate::storage::Chunk::parse(crate::storage::parse::Input::new(bytes))
            .expect("valid chunk");
        assert!(
            !matches!(chunk, crate::storage::Chunk::Signatures(_)) && rest.is_empty(),
            "single bare chunk without a trailing signatures chunk"
        );
    }
    assert!(Automerge::load(&anonymous.save()).is_ok());
    // It cannot flow back into required-mode documents, and the source is
    // unchanged (still signed, draft still live).
    assert!(matches!(
        source.merge(&mut anonymous),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(Automerge::load_with_options(
        &anonymous.save(),
        LoadOptions::new().signing(author(&key(3))).unwrap(),
    )
    .is_err());
    assert_eq!(source.save(), before);
    assert_eq!(source.get_heads(), [d]);
    assert!(source.has_unpublished_changes());
}
