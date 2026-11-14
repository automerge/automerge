//! Signing handle, signature insertion, and automatic local publication tests.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message, Insert};
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Author, Automerge, Change, ChangeHash, PatchLog, ReadDoc, ROOT};

use super::error;

mod fragments;

/// A deterministic signing key derived from `seed`.
pub(super) fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

/// The author identity (raw verifying key bytes) of `key`.
pub(super) fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

/// An ed25519 signature by `key` over the signing message for `hash`.
pub(super) fn sig(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

/// Insert `key`'s signature for `hash` into `doc`, panicking if it is rejected.
pub(super) fn sign_doc(doc: &mut Automerge, hash: ChangeHash, key: &SigningKey) {
    doc.try_signing()
        .unwrap()
        .insert_signature(&hash, &sig(key, &hash))
        .unwrap();
}

/// Whether `doc` has locally published `hash`.
pub(super) fn is_published(doc: &mut Automerge, hash: &ChangeHash) -> bool {
    doc.try_signing().unwrap().is_published(hash)
}

/// How many Ed25519 signatures `doc` retains for `hash`.
pub(super) fn signature_count(doc: &mut Automerge, hash: &ChangeHash) -> usize {
    doc.try_signing()
        .unwrap()
        .state
        .signatures
        .ed25519_entries(hash)
        .count()
}

/// `content` followed by one signatures chunk carrying `entries`, i.e. a
/// proof-bearing group as it travels on the wire.
pub(super) fn attach(
    content: &[u8],
    entries: impl IntoIterator<Item = (ChangeHash, crate::Signature)>,
) -> Vec<u8> {
    let chunk = crate::storage::signatures::SignaturesChunk::encode(
        crate::storage::signatures::HashEntries::canonicalize(entries).unwrap(),
    );
    [content, chunk.bytes()].concat()
}

/// A chunk of `chunk_type` framed with a valid header and checksum.
pub(super) fn framed(chunk_type: crate::storage::ChunkType, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    crate::storage::Header::new(chunk_type, body).write(&mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

pub(super) fn sorted(hashes: &[ChangeHash]) -> Vec<ChangeHash> {
    let mut hashes = hashes.to_vec();
    hashes.sort_unstable();
    hashes
}

/// Insert `sig` for `hash` into `doc`, returning the raw insertion outcome.
pub(super) fn insert_one(
    doc: &mut Automerge,
    hash: ChangeHash,
    sig: &ed25519::Signature,
) -> Result<Insert, error::InsertSignature> {
    doc.try_signing().unwrap().insert_signature(&hash, sig)
}

/// Commit `put(ROOT, "value", value)` onto `heads` at fragment level `level`.
///
/// A change's "level" is the number of leading zero bytes in its hash, i.e.
/// [`ChangeHash::fragment_level`]. Since the hash covers the commit time, this
/// varies the commit time until the resulting hash lands on `level`, then keeps
/// that transaction.
pub(super) fn append_at_level(
    doc: &mut Automerge,
    heads: Option<&[ChangeHash]>,
    value: i64,
    level: usize,
) -> ChangeHash {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = match heads {
            Some(heads) => next.transaction_at(PatchLog::inactive(), heads).unwrap(),
            None => next.transaction(),
        };
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            *doc = next;
            return hash;
        }
    }
    panic!("fixture level search exhausted for level {level}");
}

/// A required-mode document authored by `key`, plus its first level-0 change.
fn signed_by(key: &SigningKey) -> (Automerge, ChangeHash) {
    let mut doc = Automerge::signing()
        .with_author(Some(author(key)))
        .with_actor(ActorId::from([1]));
    let first = append_at_level(&mut doc, None, 1, 0);
    (doc, first)
}

#[test]
fn unsigned_document_rejects_try_signing_without_converting_mode() {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", 1).unwrap();
    tx.commit();
    let heads = doc.get_heads();
    assert!(matches!(
        doc.try_signing(),
        Err(error::SigningMode::Required)
    ));
    assert_eq!(doc.get_heads(), heads);
}

#[test]
fn local_edits_are_visible_immediately_and_published_automatically_when_signed() {
    let k = key(7);
    let (mut doc, a1) = signed_by(&k);

    assert_eq!(doc.get_heads(), [a1]);
    assert_eq!(doc.get(ROOT, "value").unwrap().unwrap().0, 1.into());
    assert!(!is_published(&mut doc, &a1));

    assert_eq!(
        insert_one(&mut doc, a1, &sig(&k, &a1)).unwrap(),
        Insert::Stored
    );
    assert!(is_published(&mut doc, &a1));
}

#[test]
fn signature_by_other_than_recorded_author_is_a_verification_error() {
    let alice = key(7);
    let bob = key(8);
    let (mut doc, a1) = signed_by(&alice);

    assert!(matches!(
        insert_one(&mut doc, a1, &sig(&bob, &a1)),
        Err(error::InsertSignature::Verification { change, .. }) if change == a1
    ));
}

#[test]
fn signature_for_hash_outside_the_document_is_unknown_change() {
    let alice = key(7);
    let (mut doc, _) = signed_by(&alice);
    let unknown = ChangeHash([0xff; 32]);

    assert!(matches!(
        insert_one(&mut doc, unknown, &sig(&alice, &unknown)),
        Err(error::InsertSignature::UnknownChange { change }) if change == unknown
    ));
}

#[test]
fn signing_one_change_accepts_only_that_change() {
    let alice = key(7);
    let bob = key(8);
    let (mut doc, a1) = signed_by(&alice);

    doc.set_author(Some(author(&bob)));
    doc.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut doc, None, 2, 0);

    sign_doc(&mut doc, a1, &alice);
    assert!(is_published(&mut doc, &a1));
    assert!(!is_published(&mut doc, &b1));
}

#[test]
fn signature_for_change_recorded_without_an_author_is_missing_author() {
    let alice = key(7);
    let mut doc = Automerge::signing().with_actor(ActorId::from([1]));
    let h_none = append_at_level(&mut doc, None, 1, 0);
    doc.set_author(Some(author(&alice)));

    assert!(matches!(
        insert_one(&mut doc, h_none, &sig(&alice, &h_none)),
        Err(error::InsertSignature::MissingAuthor { change, .. }) if change == h_none
    ));
}

#[test]
fn signature_for_change_with_unparseable_author_is_invalid_author_key() {
    let alice = key(7);
    let mut doc = Automerge::signing()
        .with_author(Some(Author::from(vec![1, 2, 3])))
        .with_actor(ActorId::from([1]));
    let h_bad = append_at_level(&mut doc, None, 1, 0);

    assert!(matches!(
        insert_one(&mut doc, h_bad, &sig(&alice, &h_bad)),
        Err(error::InsertSignature::InvalidAuthorKey { change, .. }) if change == h_bad
    ));
}

#[test]
fn publication_survives_actor_shift_and_rolled_back_transaction() {
    let k = key(7);
    let mut doc = Automerge::signing()
        .with_author(Some(author(&k)))
        .with_actor(ActorId::from([2]));
    let first = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, first, &k);
    assert!(is_published(&mut doc, &first));

    doc.set_actor(ActorId::from([0]));
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "value", 2).unwrap();
        assert_eq!(tx.rollback(), 1);
    }

    assert!(is_published(&mut doc, &first));
}

#[test]
fn clone_copies_publication_and_later_signatures_do_not_leak_between_copies() {
    let k = key(7);
    let (mut doc, first) = signed_by(&k);
    sign_doc(&mut doc, first, &k);
    let second = append_at_level(&mut doc, None, 3, 0);

    let mut cloned = doc.clone();
    assert!(is_published(&mut cloned, &first));
    assert!(!is_published(&mut cloned, &second));

    sign_doc(&mut cloned, second, &k);
    assert!(is_published(&mut cloned, &second));
    assert!(!is_published(&mut doc, &second));
}

#[test]
fn publication_waits_for_the_gap_in_a_partially_signed_chain() {
    let k1 = key(1);
    let mut chain = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let c0 = append_at_level(&mut chain, None, 1, 0);
    let c1 = append_at_level(&mut chain, None, 2, 0);
    let c2 = append_at_level(&mut chain, None, 3, 0);

    sign_doc(&mut chain, c0, &k1);
    sign_doc(&mut chain, c2, &k1);
    assert!(is_published(&mut chain, &c0));
    assert!(!is_published(&mut chain, &c1));
    assert!(!is_published(&mut chain, &c2));

    sign_doc(&mut chain, c1, &k1);
    assert!(is_published(&mut chain, &c1));
    assert!(is_published(&mut chain, &c2));
}

#[test]
fn local_fallback_is_published_while_its_container_waits_on_an_unpublished_boundary() {
    let k1 = key(1);
    let k2 = key(2);

    // `c` contains {a, c} but depends on boundary `b`, authored by someone else.
    let mut doc = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_author(Some(author(&k2)));
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_author(Some(author(&k1)));
    doc.set_actor(ActorId::from([1]));
    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);

    sign_doc(&mut doc, c, &k1);
    assert!(!is_published(&mut doc, &a));
    assert!(!is_published(&mut doc, &c));
    assert_eq!(doc.fragments(..).len(), 1);

    // Signing fallback `a` publishes `a` even though container `{a, c}` is already
    // ready for outgoing export while waiting on unpublished boundary `b`.
    sign_doc(&mut doc, a, &k1);
    assert!(is_published(&mut doc, &a));
    assert!(!is_published(&mut doc, &b));
    assert!(!is_published(&mut doc, &c));

    // Signing the boundary completes publication and reclaims sig(a).
    sign_doc(&mut doc, b, &k2);
    assert!(is_published(&mut doc, &b));
    assert!(is_published(&mut doc, &c));
    assert_eq!(
        doc.try_signing()
            .unwrap()
            .state
            .signatures
            .verified_hashes()
            .count(),
        2
    );
}
