//! End-to-end incoming group validation, causal admission, GC, and bypass tests.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::error::{InvalidFragment, ProofError, SigningError};
use crate::signature::{ed25519, signing_message, Signature, SignatureBytes, SignatureTag};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::ChunkType;
use crate::sync::{self, SyncDoc};
use crate::transaction::{CommitOptions, Transactable};
use crate::{
    ActorId, Author, Automerge, AutomergeError, Change, ChangeHash, ExpandedChange, LoadOptions,
    OnPartialLoad, PatchLog, ReadDoc, ROOT,
};

use super::{author, framed, key, sign_doc};

fn sig(key: &SigningKey, hash: ChangeHash) -> Signature {
    Signature::ed25519(ed25519::Signature::from_bytes(
        key.sign(&signing_message(&hash)).to_bytes(),
    ))
}

fn append_at_level(
    doc: &mut Automerge,
    heads: Option<&[ChangeHash]>,
    prop: &str,
    value: i64,
    level: usize,
) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = match heads {
            Some(heads) => next.transaction_at(PatchLog::inactive(), heads).unwrap(),
            None => next.transaction(),
        };
        tx.put(ROOT, prop, value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted for level {level}");
}

/// A receiving replica with its own (never signing) author.
fn new_receiver() -> Automerge {
    Automerge::from_config(crate::Config::default().signing(author(&key(200))).unwrap())
}

fn signing_load() -> LoadOptions<'static> {
    LoadOptions::new().signing(author(&key(200))).unwrap()
}

fn attach_signatures(
    content: &[u8],
    entries: impl IntoIterator<Item = (ChangeHash, Signature)>,
) -> Vec<u8> {
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap());
    [content, chunk.bytes()].concat()
}

fn verified_count(doc: &mut Automerge) -> usize {
    doc.try_signing()
        .unwrap()
        .state
        .signatures
        .verified_hashes()
        .count()
}

/// Deliver `stream` to a fresh signing document and assert that the rejection
/// left no trace: no heads, no missing deps, no pending groups, no evidence.
fn rejected_without_state_change(stream: &[u8]) -> AutomergeError {
    let mut receiver = new_receiver();
    let error = receiver
        .load_incremental(stream)
        .expect_err("stream must be rejected");
    assert!(receiver.get_heads().is_empty());
    assert!(receiver.get_missing_deps(&[]).is_empty());
    assert!(!receiver.signing.has_pending_groups());
    assert_eq!(verified_count(&mut receiver), 0);
    error
}

/// Three signed commits by two authors: `a1` (level 0, key 1), `b1` (level 1,
/// key 2) and `a2` (level 0, key 1). The sender keeps authoring as key 1.
fn mixed_author_source() -> (Automerge, [ChangeHash; 3]) {
    let k1 = key(1);
    let mut sender = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    sender.set_actor(ActorId::from([1]));
    let a2 = append_at_level(&mut sender, None, "c", 3, 0);

    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, b1, &k1);
    sign_doc(&mut sender, a2, &k1);
    (sender, [a1, b1, a2])
}

#[test]
fn signed_fragment_stream_loads_the_whole_history_with_published_proofs() {
    let (sender, [a1, b1, a2]) = mixed_author_source();
    let encoded = sender.bundle_fragments(sender.fragments(..));
    assert_eq!(encoded.len(), 2);

    let mut log = PatchLog::active();
    let mut loaded =
        Automerge::load_with_options(&encoded.concat(), signing_load().patch_log(&mut log))
            .unwrap();

    assert_eq!(loaded.get_heads(), sender.get_heads());
    assert_eq!(loaded.hydrate(None), sender.hydrate(None));
    assert!(!loaded.make_patches(&mut log).is_empty());
    for hash in [a1, b1, a2] {
        assert!(loaded.try_signing().unwrap().is_published(&hash));
    }
}

#[test]
fn loading_a_signed_stream_reexports_byte_identical_fragments() {
    let (sender, _) = mixed_author_source();
    let fragments = sender.fragments(..);
    let encoded = sender.bundle_fragments(fragments.clone());

    let loaded = Automerge::load_with_options(&encoded.concat(), signing_load()).unwrap();

    assert_eq!(loaded.fragments(..), fragments);
    assert_eq!(loaded.bundle_fragments(loaded.fragments(..)), encoded);
}

#[test]
fn superseding_delivery_reclaims_resident_signatures_it_covers() {
    let k1 = key(1);
    let (mut sender, [a1, _, a2]) = mixed_author_source();

    // Resident loose evidence for `a1` only.
    let mut incremental = new_receiver();
    let a1_loose = attach_signatures(
        sender.get_change_by_hash(&a1).unwrap().raw_bytes(),
        [(a1, sig(&k1, a1))],
    );
    assert_eq!(incremental.load_incremental(&a1_loose).unwrap(), 1);
    assert_eq!(verified_count(&mut incremental), 1);

    // A later level-1 frontier supersedes the interior commits it covers.
    let a3 = append_at_level(&mut sender, None, "d", 4, 1);
    sign_doc(&mut sender, a3, &k1);
    let full = sender.bundle_fragments(sender.fragments(..)).concat();
    incremental.load_incremental(&full).unwrap();

    assert_eq!(incremental.get_heads(), sender.get_heads());
    assert_eq!(
        incremental
            .try_signing()
            .unwrap()
            .state
            .signatures
            .ed25519_entries(&a2)
            .count(),
        0,
    );
}

/// Two groups: `f` holds `a1` (actor 1) and `b1` (actor 2, level 1), `g` holds
/// the dependent `b2` (actor 2, level 1). Returns `(sender, f_bytes, g_bytes, b1, b2)`.
fn two_group_source() -> (Automerge, Vec<u8>, Vec<u8>, ChangeHash, ChangeHash) {
    let k1 = key(1);
    let mut sender = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    let b2 = append_at_level(&mut sender, None, "b2", 3, 1);

    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, b1, &k1);
    sign_doc(&mut sender, b2, &k1);

    let mut encoded = sender.bundle_fragments(sender.fragments(..));
    let g_bytes = encoded.pop().unwrap();
    let f_bytes = encoded.pop().unwrap();
    (sender, f_bytes, g_bytes, b1, b2)
}

#[test]
fn a_group_whose_dependency_is_absent_reports_missing_deps() {
    let (_, _, g_bytes, b1, _) = two_group_source();

    assert!(matches!(
        Automerge::load_with_options(&g_bytes, signing_load()),
        Err(AutomergeError::MissingDeps)
    ));

    let mut receiver = new_receiver();
    assert_eq!(receiver.load_incremental(&g_bytes).unwrap(), 0);
    assert!(receiver.get_heads().is_empty());
    assert_eq!(receiver.get_missing_deps(&[]), vec![b1]);
}

#[test]
fn a_pending_group_applies_once_its_dependency_group_arrives() {
    let (_, f_bytes, g_bytes, _, b2) = two_group_source();

    let mut receiver = new_receiver();
    assert_eq!(receiver.load_incremental(&g_bytes).unwrap(), 0);
    assert_eq!(receiver.load_incremental(&f_bytes).unwrap(), 3);

    assert_eq!(receiver.get_heads(), [b2]);
    assert!(receiver.try_signing().unwrap().is_published(&b2));
}

#[test]
fn a_partially_loaded_document_can_complete_from_a_later_dependency_group() {
    let (_, f_bytes, g_bytes, b1, b2) = two_group_source();

    let mut from_ignore = Automerge::load_with_options(
        &g_bytes,
        signing_load().on_partial_load(OnPartialLoad::Ignore),
    )
    .unwrap();
    assert!(from_ignore.get_heads().is_empty());
    assert_eq!(from_ignore.get_missing_deps(&[]), vec![b1]);

    assert_eq!(from_ignore.load_incremental(&f_bytes).unwrap(), 3);
    assert_eq!(from_ignore.get_heads(), [b2]);
    assert!(from_ignore.try_signing().unwrap().is_published(&b2));
}

#[test]
fn out_of_order_groups_in_one_stream_are_admitted_in_a_single_call() {
    let (_, f_bytes, g_bytes, _, b2) = two_group_source();
    let reversed = [g_bytes.as_slice(), f_bytes.as_slice()].concat();

    let mut single_call = Automerge::load_with_options(&reversed, signing_load()).unwrap();

    assert_eq!(single_call.get_heads(), [b2]);
    assert!(single_call.try_signing().unwrap().is_published(&b2));
}

/// A valid level-1 group `f` plus the raw material needed to forge a second
/// group for `a2`. Returns `(sender, f_bytes, a1, a2)`.
fn one_valid_group_and_a_detached_commit() -> (Automerge, Vec<u8>, ChangeHash, ChangeHash) {
    let k1 = key(1);
    let mut sender = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 1);
    let a2 = append_at_level(&mut sender, None, "b", 2, 1);
    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, a2, &k1);
    let f_bytes = sender.bundle_fragments(sender.fragments(..)).remove(0);
    (sender, f_bytes, a1, a2)
}

#[test]
fn a_group_with_no_attached_proof_is_rejected_without_mutating_state() {
    let (sender, f_bytes, _, a2) = one_valid_group_and_a_detached_commit();
    let bundle_a2 = sender.bundle([a2]).unwrap();
    let stream = [f_bytes.as_slice(), bundle_a2.bytes()].concat();

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Signing(SigningError::InvalidProof(ProofError::MissingSignature(h)))
            if h == a2
    ));
    assert!(Automerge::load_with_options(&stream, signing_load()).is_err());
}

#[test]
fn a_group_signed_by_the_wrong_key_is_rejected_without_mutating_state() {
    let (sender, f_bytes, _, a2) = one_valid_group_and_a_detached_commit();
    let wrong = key(9);
    let stream = [
        f_bytes.as_slice(),
        attach_signatures(
            sender.bundle([a2]).unwrap().bytes(),
            [(a2, sig(&wrong, a2))],
        )
        .as_slice(),
    ]
    .concat();

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Signing(SigningError::InvalidProof(ProofError::MissingSignature(h)))
            if h == a2
    ));
    assert!(Automerge::load_with_options(&stream, signing_load()).is_err());
}

#[test]
fn a_group_proved_with_an_unsupported_signature_tag_is_rejected_without_mutating_state() {
    let (sender, f_bytes, _, a2) = one_valid_group_and_a_detached_commit();
    let unsupported = Signature::new(
        SignatureTag::new(7),
        SignatureBytes::new(vec![1; 64]).unwrap(),
    );
    let stream = [
        f_bytes.as_slice(),
        attach_signatures(sender.bundle([a2]).unwrap().bytes(), [(a2, unsupported)]).as_slice(),
    ]
    .concat();

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Signing(SigningError::InvalidProof(ProofError::MissingSignature(h)))
            if h == a2
    ));
    assert!(Automerge::load_with_options(&stream, signing_load()).is_err());
}

#[test]
fn a_truncated_signature_chunk_is_rejected_without_mutating_state() {
    let (sender, f_bytes, _, a2) = one_valid_group_and_a_detached_commit();
    let stream = [
        f_bytes.as_slice(),
        sender.get_change_by_hash(&a2).unwrap().raw_bytes(),
        framed(ChunkType::Signatures, &[1]).as_slice(),
    ]
    .concat();

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Load(_)
    ));
    assert!(Automerge::load_with_options(&stream, signing_load()).is_err());
}

#[test]
fn a_signed_change_without_any_author_is_rejected_as_missing_author() {
    let k1 = key(1);
    let mut authorless = Automerge::new().with_actor(ActorId::from([3]));
    let hash = append_at_level(&mut authorless, None, "x", 1, 0);
    let change = authorless.get_change_by_hash(&hash).unwrap();
    let stream = attach_signatures(change.raw_bytes(), [(hash, sig(&k1, hash))]);

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Signing(SigningError::InvalidProof(ProofError::MissingAuthor(h)))
            if h == hash
    ));
}

#[test]
fn a_signed_change_whose_author_is_not_a_public_key_is_rejected() {
    let k1 = key(1);
    let mut bad_author =
        Automerge::from_config(crate::Config::default().author(Some(Author::from(vec![1, 2, 3]))))
            .with_actor(ActorId::from([4]));
    let hash = append_at_level(&mut bad_author, None, "y", 1, 0);
    let change = bad_author.get_change_by_hash(&hash).unwrap();
    let stream = attach_signatures(change.raw_bytes(), [(hash, sig(&k1, hash))]);

    assert!(matches!(
        rejected_without_state_change(&stream),
        AutomergeError::Signing(SigningError::InvalidProof(ProofError::InvalidAuthorKey {
            hash: h,
            ..
        })) if h == hash
    ));
}

/// A group `f` covering `a1` and `b1` but proving only `b1`, alongside a group
/// `g` for `a2` that carries the signature `f` is missing for `a1`.
fn incomplete_group_and_donor_group() -> (Automerge, Vec<u8>, Vec<u8>, ChangeHash) {
    let k1 = key(1);
    let mut sender = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    sender.set_actor(ActorId::from([1]));
    let a2 = append_at_level(&mut sender, None, "c", 3, 1);

    let f_incomplete = attach_signatures(
        sender.bundle([a1, b1]).unwrap().bytes(),
        [(b1, sig(&k1, b1))],
    );
    let g_with_extra = attach_signatures(
        sender.bundle([a2]).unwrap().bytes(),
        [(a1, sig(&k1, a1)), (a2, sig(&k1, a2))],
    );
    (sender, f_incomplete, g_with_extra, a1)
}

#[test]
fn a_sibling_groups_extra_signature_does_not_repair_an_incomplete_proof() {
    let (_, f_incomplete, g_with_extra, a1) = incomplete_group_and_donor_group();

    for stream in [
        [f_incomplete.as_slice(), g_with_extra.as_slice()].concat(),
        [g_with_extra.as_slice(), f_incomplete.as_slice()].concat(),
    ] {
        assert!(matches!(
            Automerge::load_with_options(&stream, signing_load()),
            Err(AutomergeError::Signing(SigningError::InvalidProof(
                ProofError::MissingSignature(h)
            ))) if h == a1
        ));
    }

    let mut receiver = new_receiver();
    assert_eq!(receiver.load_incremental(&g_with_extra).unwrap(), 0);
    assert!(matches!(
        receiver.load_incremental(&f_incomplete),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::MissingSignature(h)
        ))) if h == a1
    ));
}

#[test]
fn resident_evidence_does_not_repair_an_incomplete_proof() {
    let k1 = key(1);
    let (sender, f_incomplete, _, a1) = incomplete_group_and_donor_group();
    let a1_loose = attach_signatures(
        sender.get_change_by_hash(&a1).unwrap().raw_bytes(),
        [(a1, sig(&k1, a1))],
    );

    let mut with_resident = new_receiver();
    with_resident.load_incremental(&a1_loose).unwrap();

    assert!(matches!(
        with_resident.load_incremental(&f_incomplete),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::MissingSignature(h)
        ))) if h == a1
    ));
    // Nor does an already-admitted commit let an unproved copy back in.
    assert!(with_resident
        .load_incremental(sender.get_change_by_hash(&a1).unwrap().raw_bytes())
        .is_err());
}

/// Honest diamond over three authors: `db1` (L1, key 2) depends on `da1`
/// (L0, key 1); `dc1` (L1, key 3) depends on both. `db1_group` carries
/// `[da1, db1]`, `dc1_canonical` carries only `dc1`.
struct Diamond {
    doc: Automerge,
    da1: ChangeHash,
    db1: ChangeHash,
    dc1: ChangeHash,
    db1_group: Vec<u8>,
}

fn diamond() -> Diamond {
    let (k1, k2, k3) = (key(1), key(2), key(3));
    let mut doc = Automerge::from_config(crate::Config::default().author(Some(author(&k1))))
        .with_actor(ActorId::from([1]));
    let da1 = append_at_level(&mut doc, None, "a", 1, 0);
    doc = doc.fork_with_author(Some(author(&k2))).unwrap();
    doc.set_actor(ActorId::from([2]));
    let db1 = append_at_level(&mut doc, Some(&[da1]), "b", 2, 1);
    doc = doc.fork_with_author(Some(author(&k3))).unwrap();
    doc.set_actor(ActorId::from([3]));
    let dc1 = append_at_level(&mut doc, Some(&[da1, db1]), "c", 3, 1);

    let db1_group = attach_signatures(
        doc.bundle([da1, db1]).unwrap().bytes(),
        [(da1, sig(&k1, da1)), (db1, sig(&k2, db1))],
    );
    Diamond {
        doc,
        da1,
        db1,
        dc1,
        db1_group,
    }
}

#[test]
fn a_group_omitting_an_independent_ancestor_is_non_canonical() {
    let (k1, k2, k3) = (key(1), key(2), key(3));
    // Independent a1 (L0) and b1 (L1); c1 (L1) depends on both.
    let mut doc = Automerge::from_config(crate::Config::default().author(Some(author(&k1))))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut doc, None, "a", 1, 0);
    doc = doc.fork_with_author(Some(author(&k2))).unwrap();
    doc.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut doc, Some(&[]), "b", 2, 1);
    doc = doc.fork_with_author(Some(author(&k3))).unwrap();
    doc.set_actor(ActorId::from([3]));
    let c1 = append_at_level(&mut doc, Some(&[a1, b1]), "c", 3, 1);

    let stream = [
        attach_signatures(
            doc.get_change_by_hash(&a1).unwrap().raw_bytes(),
            [(a1, sig(&k1, a1))],
        )
        .as_slice(),
        attach_signatures(doc.bundle([b1]).unwrap().bytes(), [(b1, sig(&k2, b1))]).as_slice(),
        // c1's group omits a1, which is not covered by any other member.
        attach_signatures(doc.bundle([c1]).unwrap().bytes(), [(c1, sig(&k3, c1))]).as_slice(),
    ]
    .concat();

    assert!(matches!(
        Automerge::load_with_options(&stream, signing_load()),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::NonCanonicalMembership(h)
        ))) if h == c1
    ));
}

#[test]
fn a_canonical_diamond_tip_loads_once_its_ancestor_group_arrives() {
    let d = diamond();
    let k3 = key(3);
    let dc1_canonical = attach_signatures(
        d.doc.bundle([d.dc1]).unwrap().bytes(),
        [(d.dc1, sig(&k3, d.dc1))],
    );

    let mut receiver = new_receiver();
    assert_eq!(receiver.load_incremental(&dc1_canonical).unwrap(), 0);
    assert_eq!(receiver.load_incremental(&d.db1_group).unwrap(), 3);

    assert_eq!(receiver.get_heads(), d.doc.get_heads());
}

#[test]
fn a_diamond_tip_restating_an_already_covered_ancestor_is_non_canonical() {
    let d = diamond();
    let (k1, k3) = (key(1), key(3));
    let dc1_stuffed = attach_signatures(
        d.doc.bundle([d.da1, d.dc1]).unwrap().bytes(),
        [(d.da1, sig(&k1, d.da1)), (d.dc1, sig(&k3, d.dc1))],
    );

    assert!(matches!(
        Automerge::load_with_options(
            &[d.db1_group.as_slice(), dc1_stuffed.as_slice()].concat(),
            signing_load(),
        ),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::NonCanonicalMembership(h)
        ))) if h == d.dc1
    ));
}

#[test]
fn a_change_not_depending_on_its_own_actor_predecessor_breaks_the_actor_chain() {
    let mut d = diamond();
    let k1 = key(1);
    // X:2 rewritten to drop the direct X:1 predecessor edge.
    d.doc = d.doc.fork_with_author(Some(author(&k1))).unwrap();
    d.doc.set_actor(ActorId::from([1]));
    let dx2 = append_at_level(&mut d.doc, Some(&[d.db1]), "x2", 4, 0);
    let mut expanded = ExpandedChange::from(&d.doc.get_change_by_hash(&dx2).unwrap());
    expanded.deps = vec![d.db1];
    expanded.hash = None;
    let unlinked = Change::from(expanded);
    let bad_bytes = attach_signatures(
        unlinked.raw_bytes(),
        [(unlinked.hash(), sig(&k1, unlinked.hash()))],
    );

    assert!(matches!(
        Automerge::load_with_options(
            &[d.db1_group.as_slice(), bad_bytes.as_slice()].concat(),
            signing_load(),
        ),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::BrokenActorChain(h)
        ))) if h == unlinked.hash()
    ));
}

/// A signed four-commit fork: `root` (L1, key 1), `a1` (L0, key 1) and two
/// siblings off `a1` — `b1` (L1, key 2) and `c1` (L1, key 3). Returns the
/// document, key 1, and `[root, a1, b1, c1]`.
fn signed_source() -> (Automerge, SigningKey, [ChangeHash; 4]) {
    let k1 = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k1)).unwrap())
        .with_actor(ActorId::from([1]));
    let root = append_at_level(&mut source, None, "root", 0, 1);
    let a1 = append_at_level(&mut source, None, "a1", 1, 0);
    source.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut source, Some(&[a1]), "b1", 2, 1);
    source.set_actor(ActorId::from([3]));
    let c1 = append_at_level(&mut source, Some(&[a1]), "c1", 3, 1);
    for (h, k) in [(root, &k1), (a1, &k1), (b1, &k1), (c1, &k1)] {
        sign_doc(&mut source, h, k);
    }
    (source, k1, [root, a1, b1, c1])
}

fn fragment_bytes(doc: &Automerge, head: ChangeHash) -> Vec<u8> {
    doc.bundle_fragments([doc.get_fragment(head).unwrap()])
        .remove(0)
}

#[test]
fn sibling_fragments_delivered_before_their_root_apply_once_it_arrives() {
    let (source, _, [root, _, b1, c1]) = signed_source();

    let mut receiver = new_receiver();
    assert_eq!(
        receiver
            .load_incremental(&fragment_bytes(&source, b1))
            .unwrap(),
        0
    );
    // Redelivering the same pending group changes nothing.
    assert_eq!(
        receiver
            .load_incremental(&fragment_bytes(&source, b1))
            .unwrap(),
        0
    );
    assert_eq!(
        receiver
            .load_incremental(&fragment_bytes(&source, c1))
            .unwrap(),
        0
    );

    assert_eq!(
        receiver
            .load_incremental(&fragment_bytes(&source, root))
            .unwrap(),
        4
    );
    assert_eq!(receiver.get_heads(), source.get_heads());
    assert!(!receiver.signing.has_pending_groups());
}

#[test]
fn a_forged_orphan_claiming_an_unknown_actor_is_purged_when_the_real_actor_arrives() {
    let (source, _, [root, _, b1, _]) = signed_source();
    let malloy = key(99);

    // Malloy forges Actor 2 seq 2 under their own key, before the real Actor 2 is known.
    let mut forged = Automerge::from_config(crate::Config::default().author(Some(author(&malloy))))
        .with_actor(ActorId::from([2]));
    append_at_level(&mut forged, None, "f1", 1, 0);
    let forged_seq2 = append_at_level(&mut forged, None, "f2", 2, 0);
    let forged_orphan = attach_signatures(
        forged.get_change_by_hash(&forged_seq2).unwrap().raw_bytes(),
        [(forged_seq2, sig(&malloy, forged_seq2))],
    );

    let mut receiver = new_receiver();
    assert_eq!(receiver.load_incremental(&forged_orphan).unwrap(), 0);
    receiver
        .load_incremental(&fragment_bytes(&source, b1))
        .unwrap();
    receiver
        .load_incremental(&fragment_bytes(&source, root))
        .unwrap();

    assert!(!receiver.signing.has_pending_groups());
    assert!(!receiver.try_signing().unwrap().is_published(&forged_seq2));
}

#[test]
fn fork_preserves_required_mode_and_published_history() {
    let (source, _, [root, ..]) = signed_source();

    let mut forked = source.fork();

    assert!(forked.signing.is_required());
    assert!(forked.try_signing().unwrap().is_published(&root));
}

#[test]
fn a_partial_load_withholds_bare_content_preceding_a_truncated_prefix() {
    let (source, _, [root, ..]) = signed_source();
    let bare_root = source.get_change_by_hash(&root).unwrap();
    let bare_truncated = [bare_root.raw_bytes(), &[0x85, 0x6f, 0x4a, 0x83]].concat();

    let recovered = Automerge::load_with_options(
        &bare_truncated,
        signing_load().on_partial_load(OnPartialLoad::Ignore),
    )
    .unwrap();

    assert!(recovered.get_heads().is_empty());
}

#[test]
fn loading_a_snapshot_into_signing_mode_is_rejected() {
    let (source, _, _) = signed_source();
    let snapshot = source.save();

    assert!(matches!(
        Automerge::load_with_options(&snapshot, signing_load()),
        Err(AutomergeError::Signing(SigningError::SnapshotNotSupported))
    ));
    let mut target = new_receiver();
    assert!(matches!(
        target.load_incremental(&snapshot),
        Err(AutomergeError::Signing(SigningError::SnapshotNotSupported))
    ));
}

#[test]
fn merging_another_document_in_signing_mode_is_rejected_as_unauthenticated() {
    let (mut source, _, _) = signed_source();

    let mut target = new_receiver();

    assert!(matches!(
        target.merge(&mut source),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    assert!(target.get_heads().is_empty());
}

#[test]
fn sync_with_a_signing_document_neither_generates_nor_accepts_messages() {
    let mut unsigned_peer = Automerge::new().with_actor(ActorId::from([9]));
    append_at_level(&mut unsigned_peer, None, "p", 1, 0);
    let mut peer_state = sync::State::new();
    let msg = unsigned_peer
        .generate_sync_message(&mut peer_state)
        .unwrap();

    let mut target = new_receiver();
    let mut target_state = sync::State::new();

    assert!(target.generate_sync_message(&mut target_state).is_none());
    assert!(matches!(
        target.receive_sync_message(&mut target_state, msg),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
}
