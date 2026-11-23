//! Signed merge: published source history only, through receiver admission.

use crate::signature::error::SigningError;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{AutoCommit, AutomergeError, LoadOptions, Signature, SigningRequest};

use super::*;

fn is_accepted(doc: &mut Automerge, hash: &ChangeHash) -> bool {
    doc.try_signing().unwrap().is_accepted(hash)
}

fn drafts(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.try_signing()
        .unwrap()
        .state
        .local_drafts
        .iter()
        .collect()
}

fn requests(doc: &mut Automerge) -> Vec<ChangeHash> {
    doc.signing_requests()
        .unwrap()
        .iter()
        .map(SigningRequest::hash)
        .collect()
}

fn signature_count(doc: &mut Automerge) -> usize {
    doc.try_signing()
        .unwrap()
        .state
        .signatures
        .verified_hashes()
        .count()
}

/// `a` loose, `b` an independent hard head, `c` a hard head on `[a, b]`
/// whose canonical fragment is `{a, c}`.
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn container_shape(doc: &mut Automerge) -> (ChangeHash, ChangeHash, ChangeHash) {
    doc.set_actor(ActorId::from([1])).unwrap();
    let a = append_at_level(doc, None, 1, 0);
    doc.set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1])).unwrap();
    let c = append_at_level(doc, Some(&[a, b]), 3, 1);
    (a, b, c)
}

#[test]
fn mode_mismatch_is_rejected_in_both_directions_before_any_mutation() {
    let k = key(1);
    let mut signed = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut unsigned = Automerge::new().with_actor(ActorId::from([2])).unwrap();

    // Empty documents still mismatch.
    assert!(matches!(
        signed.merge(&mut unsigned),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(matches!(
        unsigned.merge(&mut signed),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));

    let s = append_at_level(&mut signed, None, 1, 0);
    sign_doc(&mut signed, s, &k);
    let d = append_at_level(&mut signed, None, 2, 0);
    let u = append_at_level(&mut unsigned, None, 3, 0);

    let mut log = PatchLog::active();
    assert!(matches!(
        signed.merge_and_log_patches(&mut unsigned, &mut log),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(signed.make_patches(&mut log).is_empty());
    let mut log = PatchLog::active();
    assert!(matches!(
        unsigned.merge_and_log_patches(&mut signed, &mut log),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(unsigned.make_patches(&mut log).is_empty());

    assert_eq!(signed.get_heads(), [d]);
    assert_eq!(unsigned.get_heads(), [u]);
    assert!(is_accepted(&mut signed, &s));
    assert_eq!(drafts(&mut signed), [d]);
    assert!(!signed.signing.has_pending_groups());

    // AutoCommit decides before closing either document's transaction.
    let mut auto_signed =
        AutoCommit::from_config(crate::Config::default().signing(author(&k)).unwrap());
    auto_signed.put(ROOT, "x", 1).unwrap();
    let mut auto_unsigned = AutoCommit::new();
    auto_unsigned.put(ROOT, "y", 2).unwrap();
    assert!(matches!(
        auto_signed.merge(&mut auto_unsigned),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    assert!(matches!(
        auto_unsigned.merge(&mut auto_signed),
        Err(AutomergeError::Signing(SigningError::MergeModeMismatch))
    ));
    // Pending edits are still open: nothing was committed by the rejection.
    assert_eq!(auto_signed.pending_ops(), 1);
    assert_eq!(auto_unsigned.pending_ops(), 1);
}

#[test]
fn merge_transfers_published_prefix_but_never_source_drafts_or_pending() {
    let k = key(1);
    let rk = key(2);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let b = append_at_level(&mut source, None, 2, 0);

    // A speculative orphan retained by the source is not merge material.
    let malloy = key(9);
    let mut forged = Automerge::from_config(crate::Config::default().author(Some(author(&malloy))))
        .with_actor(ActorId::from([7]))
        .unwrap();
    append_at_level(&mut forged, None, 1, 0);
    let f2 = append_at_level(&mut forged, None, 2, 0);
    let orphan = [
        forged.get_change_by_hash(&f2).unwrap().raw_bytes(),
        SignaturesChunk::encode(
            HashEntries::canonicalize([(f2, Signature::ed25519(sig(&malloy, &f2)))]).unwrap(),
        )
        .bytes(),
    ]
    .concat();
    assert_eq!(source.load_incremental(&orphan).unwrap(), 0);
    assert!(source.signing.has_pending_groups());

    let mut receiver =
        Automerge::from_config(crate::Config::default().signing(author(&rk)).unwrap())
            .with_actor(ActorId::from([2]))
            .unwrap();
    let mut log = PatchLog::active();
    let heads = receiver
        .merge_and_log_patches(&mut source, &mut log)
        .unwrap();
    assert_eq!(heads, [a]);
    assert_eq!(receiver.get_heads(), [a]);
    assert!(is_accepted(&mut receiver, &a));
    assert!(!receiver.has_change(&b));
    assert!(!receiver.signing.has_pending_groups());
    assert!(!receiver.make_patches(&mut log).is_empty());

    // The receiver learned no provenance and has nothing to sign; the source
    // is untouched.
    assert!(drafts(&mut receiver).is_empty());
    assert!(requests(&mut receiver).is_empty());
    assert!(!receiver.has_unpublished_changes());
    assert_eq!(source.get_heads(), [b]);
    assert_eq!(drafts(&mut source), [b]);
    assert_eq!(requests(&mut source), [b]);
    assert!(source.signing.has_pending_groups());

    // Repeating the merge applies nothing and logs nothing.
    let mut log = PatchLog::active();
    assert_eq!(
        receiver
            .merge_and_log_patches(&mut source, &mut log)
            .unwrap(),
        [a]
    );
    assert!(receiver.make_patches(&mut log).is_empty());

    // Parity with persistence: a loader of the source's save sees the same.
    let mut loaded = Automerge::load_with_options(
        &source.save(),
        LoadOptions::new().signing(author(&rk)).unwrap(),
    )
    .unwrap();
    assert_eq!(loaded.get_heads(), receiver.get_heads());
    assert_eq!(loaded.published_heads(), receiver.published_heads());
    assert!(requests(&mut loaded).is_empty());
}

#[test]
fn proof_only_merge_publishes_a_shared_draft_without_new_hashes() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    // A same-author clone shares the applied draft and its eligibility.
    let mut receiver = source.clone();
    assert_eq!(drafts(&mut receiver), [a]);
    sign_doc(&mut source, a, &k);
    assert!(!is_accepted(&mut receiver, &a));

    let mut log = PatchLog::active();
    let heads = receiver
        .merge_and_log_patches(&mut source, &mut log)
        .unwrap();
    assert_eq!(heads, [a]);
    assert!(is_accepted(&mut receiver, &a));
    assert!(drafts(&mut receiver).is_empty());
    assert!(requests(&mut receiver).is_empty());
    assert!(!receiver.has_unpublished_changes());
    assert!(receiver.make_patches(&mut log).is_empty());
    assert_eq!(signature_count(&mut receiver), 1);
}

#[test]
fn full_canonical_group_with_receiver_known_members_applies_and_patches_once() {
    let k = key(1);
    let rk = key(2);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap());
    let (a, b, c) = container_shape(&mut source);
    sign_doc(&mut source, a, &k);

    let mut receiver =
        Automerge::from_config(crate::Config::default().signing(author(&rk)).unwrap())
            .with_actor(ActorId::from([5]))
            .unwrap();
    assert_eq!(receiver.merge(&mut source).unwrap(), [a]);
    // The receiver's own draft must survive every merge.
    let d = {
        let mut tx = receiver.transaction();
        tx.put(ROOT, "draft", 50).unwrap();
        tx.commit().0.unwrap()
    };

    sign_doc(&mut source, c, &k);
    sign_doc(&mut source, b, &k);
    // Publishing the cover `{a, c}` retired the loose proof of `a` in the source.
    assert_eq!(signature_count(&mut source), 2);
    assert!(!source.has_unpublished_changes());

    let mut log = PatchLog::active();
    let heads = receiver
        .merge_and_log_patches(&mut source, &mut log)
        .unwrap();
    let mut expected = source.get_heads();
    expected.push(d);
    expected.sort();
    assert_eq!(heads, expected);
    for hash in [a, b, c] {
        assert!(is_accepted(&mut receiver, &hash));
    }
    assert!(!receiver.make_patches(&mut log).is_empty());
    assert!(!is_accepted(&mut receiver, &d));
    assert_eq!(drafts(&mut receiver), [d]);
    assert_eq!(requests(&mut receiver), [d]);
    assert_eq!(receiver.get(ROOT, "draft").unwrap().unwrap().0, 50.into());

    let mut log = PatchLog::active();
    assert_eq!(
        receiver
            .merge_and_log_patches(&mut source, &mut log)
            .unwrap(),
        expected
    );
    assert!(receiver.make_patches(&mut log).is_empty());

    // The replacement cover's proof is complete: an empty peer is reachable
    // from either the source or the merged receiver.
    for peer_source in [&mut source, &mut receiver] {
        let mut empty =
            Automerge::from_config(crate::Config::default().signing(author(&key(3))).unwrap());
        empty.merge(peer_source).unwrap();
        for hash in [a, b, c] {
            assert!(is_accepted(&mut empty, &hash));
        }
        assert!(!empty.has_change(&d));
    }
}

#[test]
fn unsigned_merge_is_unchanged() {
    let mut a = Automerge::new().with_actor(ActorId::from([1])).unwrap();
    let mut b = Automerge::new().with_actor(ActorId::from([2])).unwrap();
    let ha = append_at_level(&mut a, None, 1, 0);
    let hb = append_at_level(&mut b, None, 2, 0);
    let mut log = PatchLog::active();
    let mut expected = vec![ha, hb];
    expected.sort();
    assert_eq!(a.merge_and_log_patches(&mut b, &mut log).unwrap(), expected);
    assert!(!a.make_patches(&mut log).is_empty());
    assert_eq!(a.merge(&mut b).unwrap(), expected);
    assert_eq!(b.merge(&mut a).unwrap(), expected);
    assert!(!a.signing.is_required());
}
