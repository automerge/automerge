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
    let mut source = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let raw = source.get_change_by_hash(&a).unwrap();
    let signed = signed_group(&source, a, &k);
    let parsed = Change::parse_with_signatures(&signed).unwrap();
    let group = LoadedGroup::try_from(signed.as_slice()).unwrap();

    // Required receiver: raw Change, raw bytes and bare LoadedGroup all reject.
    let mut required = Automerge::signing(author(&key(2))).unwrap();
    let mut auto_required = AutoCommit::signing(author(&key(2))).unwrap();
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
    let mut by_bytes = Automerge::signing(author(&key(2))).unwrap();
    by_bytes.apply_changes([signed.clone()]).unwrap();
    let mut by_parsed = Automerge::signing(author(&key(2))).unwrap();
    by_parsed.apply_changes([parsed.clone()]).unwrap();
    let mut by_group = Automerge::signing(author(&key(2))).unwrap();
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
    let mut source = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
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
    let mut unsigned = Automerge::new().with_actor(ActorId::from([2]));
    append_at_level(&mut unsigned, None, 7, 0);
    let value = Automerge::rescue(&unsigned.save()).unwrap();
    assert_eq!(value, unsigned.hydrate(None));
}

#[test]
fn arbitrary_bundles_in_required_mode_are_canonical_groups_with_proof() {
    let k = key(1);
    let mut doc = Automerge::signing(author(&k)).unwrap();
    doc.set_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1]));
    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);

    // Noncanonical sets are refused outright, proved or not.
    use crate::signature::error::InvalidFragment;
    assert!(matches!(
        doc.bundle([a, b]),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::NotSingleHeaded
        )))
    ));
    assert!(matches!(
        doc.bundle([]),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::Empty
        )))
    ));
    for members in [vec![c], vec![a, b, c]] {
        assert!(matches!(
            doc.bundle(members),
            Err(AutomergeError::Signing(SigningError::InvalidFragment(
                InvalidFragment::NonCanonicalMembership(h)
            ))) if h == c
        ));
    }
    assert!(matches!(
        doc.bundle([ChangeHash([0xee; 32])]),
        Err(AutomergeError::ChangeGraph(_))
    ));
    // A canonical group without complete proof is not transferable data.
    let canonical = doc.get_fragment(c);
    assert!(canonical.is_none());
    assert!(matches!(
        doc.bundle([a, c]),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            crate::signature::error::ProofError::MissingSignature(h)
        ))) if h == c
    ));
    assert!(matches!(
        doc.bundle([b]),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            crate::signature::error::ProofError::MissingSignature(h)
        ))) if h == b
    ));

    // Once proved, the bundle bytes carry the group's complete proof and
    // admit into an empty required peer without any other delivery.
    sign_doc(&mut doc, c, &k);
    sign_doc(&mut doc, b, &k);
    let bundle_b = doc.bundle([b]).unwrap();
    let bundle_ac = doc.bundle([a, c]).unwrap();
    let mut peer = Automerge::signing(author(&key(2))).unwrap();
    peer.load_incremental(bundle_ac.bytes()).unwrap();
    assert!(peer.signing.has_pending_groups());
    peer.load_incremental(bundle_b.bytes()).unwrap();
    for hash in [a, b, c] {
        assert!(peer.try_signing().unwrap().is_accepted(&hash));
    }
    // The bundle API agrees byte-for-byte with the fragment exporter.
    assert_eq!(
        doc.bundle_fragments([doc.get_fragment(c).unwrap()]),
        [bundle_ac.bytes().to_vec()]
    );
    // Accessors still see the bundle chunk itself.
    assert_eq!(bundle_ac.iter_changes().count(), 2);

    // Unsigned arbitrary bundling is unchanged.
    let mut unsigned = Automerge::new();
    unsigned.apply_changes(doc.get_changes(&[])).unwrap();
    let loose = unsigned.bundle([a, b]).unwrap();
    assert_eq!(loose.iter_changes().count(), 2);
    let mut other = Automerge::new();
    other.apply_changes([loose.bytes().to_vec()]).unwrap();
    assert_eq!(other.get_heads().len(), 2);
}

#[test]
fn inspectors_expose_applied_drafts_but_transfers_do_not() {
    let k = key(1);
    let mut source = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let d = append_at_level(&mut source, None, 2, 0);

    // Inspection: raw changes include the draft.
    assert_eq!(source.get_changes(&[]).len(), 2);
    assert!(source.get_change_by_hash(&d).is_some());
    // Transfers: only published history.
    let mut peer = Automerge::signing(author(&key(2))).unwrap();
    peer.merge(&mut source).unwrap();
    assert_eq!(peer.get_heads(), [a]);
    let loaded = Automerge::load_with_options(
        &source.save(),
        LoadOptions::new().signing().author(author(&key(3))),
    )
    .unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert!(source.save_after(&[a]).is_empty());
    // Signature-ready export may expose blocked-but-proved groups, but a
    // bare draft is not ready and is never exported.
    assert!(source.get_fragment(d).is_none());
    assert_eq!(source.bundle_fragments(source.fragments(..)).len(), 1);
}
