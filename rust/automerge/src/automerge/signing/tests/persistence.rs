//! Signed snapshot and incremental persistence tests.

use std::collections::HashMap;

use crate::signature::error::SigningError;
use crate::storage::parse::Input;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::{Chunk, ChunkType, Header};
use crate::{AutoCommit, AutomergeError, LoadOptions, OnPartialLoad, Signature, SigningRequest};

use super::*;

fn load_opts(seed: u8) -> LoadOptions<'static> {
    LoadOptions::new().signing(author(&key(seed))).unwrap()
}

fn is_accepted(doc: &mut Automerge, hash: &ChangeHash) -> bool {
    doc.try_signing().unwrap().is_accepted(hash)
}

/// Split a byte stream into its chunk types in order.
fn chunk_types(bytes: &[u8]) -> Vec<ChunkType> {
    let mut input = Input::new(bytes);
    let mut types = Vec::new();
    while !input.is_empty() {
        let (rest, chunk) = Chunk::parse(input).unwrap();
        types.push(match chunk {
            Chunk::Document(_) => ChunkType::Document,
            Chunk::Change(_) => ChunkType::Change,
            Chunk::CompressedChange(..) => ChunkType::Compressed,
            Chunk::Bundle(_) => ChunkType::Bundle,
            Chunk::Signatures(_) => ChunkType::Signatures,
        });
        input = rest.reset();
    }
    types
}

fn framed(chunk_type: ChunkType, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    Header::new(chunk_type, body).write(&mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

/// A correctly checksummed chunk with an arbitrary (possibly unknown) type tag.
fn framed_raw(tag: u8, body: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hashed = vec![tag];
    leb128::write::unsigned(&mut hashed, body.len() as u64).unwrap();
    hashed.extend_from_slice(body);
    let hash = Sha256::digest(&hashed);
    let mut bytes = vec![0x85, 0x6f, 0x4a, 0x83];
    bytes.extend_from_slice(&hash[..4]);
    bytes.extend_from_slice(&hashed);
    bytes
}

#[test]
fn signed_save_retains_only_published_history_with_proof() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    sign_doc(&mut doc, a, &k);
    assert!(is_accepted(&mut doc, &a));
    assert!(!is_accepted(&mut doc, &b));

    let bytes = doc.save();
    assert_eq!(
        chunk_types(&bytes),
        [ChunkType::Document, ChunkType::Signatures]
    );

    // The source keeps its draft and its request.
    assert_eq!(doc.get_heads(), [b]);
    assert_eq!(
        doc.signing_requests()
            .unwrap()
            .iter()
            .map(SigningRequest::hash)
            .collect::<Vec<_>>(),
        [b]
    );

    let mut loaded = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert_eq!(
        loaded.get_change_by_hash(&a).unwrap().raw_bytes(),
        doc.get_change_by_hash(&a).unwrap().raw_bytes()
    );
    assert!(is_accepted(&mut loaded, &a));
    assert!(!loaded.has_unpublished_changes());
    assert!(loaded.signing_requests().unwrap().is_empty());
    assert!(loaded.try_signing().unwrap().state.local_drafts.is_empty());
    assert_eq!(loaded.get(ROOT, "value").unwrap().unwrap().0, 1.into());

    // Re-export from the reloaded copy yields a snapshot a third peer accepts.
    let again = loaded.save();
    let third = Automerge::load_with_options(&again, load_opts(201)).unwrap();
    assert_eq!(third.get_heads(), [a]);

    // Unsigned loaders do not silently discard the attached proof.
    assert!(Automerge::load(&bytes).is_err());
    let mut unsigned = Automerge::new();
    assert!(unsigned.load_incremental(&bytes).is_err());
    assert!(unsigned.get_heads().is_empty());
}

#[test]
fn empty_signed_save_round_trips_and_mode_mismatch_is_rejected_both_ways() {
    let doc = Automerge::from_config(crate::Config::default().signing(author(&key(1))).unwrap());
    let bytes = doc.save();
    assert_eq!(
        chunk_types(&bytes),
        [ChunkType::Document, ChunkType::Signatures]
    );
    let loaded = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert!(loaded.get_heads().is_empty());
    assert!(loaded.signing.is_required());
    assert!(!loaded.has_unpublished_changes());

    // Signed bytes in an unsigned loader: no silent discard.
    assert!(Automerge::load(&bytes).is_err());

    // Unsigned bytes in a signed loader: no bare snapshot admitted.
    let unsigned = Automerge::new().save();
    assert_eq!(chunk_types(&unsigned), [ChunkType::Document]);
    assert!(matches!(
        Automerge::load_with_options(&unsigned, load_opts(200)),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    // Unsigned fixtures unchanged.
    assert_eq!(Automerge::new().save(), unsigned);
    assert!(Automerge::load(&unsigned).unwrap().get_heads().is_empty());

    // A required document can receive a signed empty snapshot incrementally.
    let mut receiver =
        Automerge::from_config(crate::Config::default().signing(author(&key(3))).unwrap());
    assert_eq!(receiver.load_incremental(&bytes).unwrap(), 0);
    // But not a bare one.
    assert!(matches!(
        receiver.load_incremental(&unsigned),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn ready_unpublished_cover_does_not_hide_published_fallback_in_snapshot() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    // F = loose a (published); G = hard fragment {a? no: c} over boundary b (unpublished).
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2])).unwrap();
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1])).unwrap();
    let c = append_at_level(&mut doc, Some(&[a, b]), 3, 1);
    sign_doc(&mut doc, c, &k); // G = {a, c} is ready, blocked on b.
    sign_doc(&mut doc, a, &k); // F = {a} published.
    assert!(is_accepted(&mut doc, &a));
    assert!(!is_accepted(&mut doc, &c));
    assert_eq!(doc.fragments(..).len(), 1, "ready G is exportable");

    let bytes = doc.save();
    let mut loaded = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert!(is_accepted(&mut loaded, &a));

    // Publish b (hence c), then the snapshot carries everything and reloads.
    sign_doc(&mut doc, b, &k);
    assert!(is_accepted(&mut doc, &c));
    let bytes = doc.save();
    let mut loaded = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert_eq!(loaded.get_heads(), doc.get_heads());
    for hash in [a, b, c] {
        assert!(is_accepted(&mut loaded, &hash));
    }
    let reexport = loaded.save();
    let third = Automerge::load_with_options(&reexport, load_opts(201)).unwrap();
    assert_eq!(third.get_heads(), doc.get_heads());
}

#[test]
fn tampered_or_missing_required_proof_fails_but_bad_extras_are_ignored() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, a, &k);
    let bytes = doc.save();
    let (rest, Chunk::Document(_)) = Chunk::parse(Input::new(&bytes)).unwrap() else {
        panic!("expected document");
    };
    let doc_len = bytes.len() - rest.unconsumed_bytes().len();
    let document = &bytes[..doc_len];

    // Missing proof entirely (bare snapshot).
    assert!(Automerge::load_with_options(document, load_opts(200)).is_err());
    // Empty proof for non-empty history.
    let empty = [document, framed(ChunkType::Signatures, &[0]).as_slice()].concat();
    assert!(Automerge::load_with_options(&empty, load_opts(200)).is_err());
    // Wrong key.
    let wrong = [
        document,
        SignaturesChunk::encode(
            HashEntries::canonicalize([(a, Signature::ed25519(sig(&key(9), &a)))]).unwrap(),
        )
        .bytes(),
    ]
    .concat();
    assert!(Automerge::load_with_options(&wrong, load_opts(200)).is_err());
    // Malformed signatures chunk, under both partial-load policies.
    let malformed = [document, framed(ChunkType::Signatures, &[1]).as_slice()].concat();
    for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
        assert!(
            Automerge::load_with_options(&malformed, load_opts(200).on_partial_load(policy))
                .is_err()
        );
    }
    // Valid required proof plus garbage extras succeeds.
    let extras = [
        document,
        SignaturesChunk::encode(
            HashEntries::canonicalize([
                (a, Signature::ed25519(sig(&k, &a))),
                (a, Signature::ed25519(sig(&key(9), &a))),
                (ChangeHash([7; 32]), Signature::ed25519(sig(&key(9), &a))),
            ])
            .unwrap(),
        )
        .bytes(),
    ]
    .concat();
    let mut loaded = Automerge::load_with_options(&extras, load_opts(200)).unwrap();
    assert!(is_accepted(&mut loaded, &a));
    // No clock trust: DontCheck does not grant acceptance to an unsigned snapshot.
    assert!(Automerge::load_with_options(
        &empty,
        load_opts(200).verification_mode(crate::VerificationMode::DontCheck)
    )
    .is_err());
}

/// Encode `doc`'s applied history as an unsigned snapshot followed by one
/// signatures chunk holding `entries`.
fn snapshot_with_entries(
    doc: &Automerge,
    entries: impl IntoIterator<Item = (ChangeHash, Signature)>,
) -> Vec<u8> {
    let document = crate::storage::Document::new(
        &doc.ops,
        &doc.change_graph,
        crate::storage::document::CompressConfig::None,
    );
    let mut bytes = document.into_bytes();
    bytes.extend_from_slice(
        SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap()).bytes(),
    );
    bytes
}

/// A snapshot whose history violates the actor-chain rule (X:2 omits its
/// direct X:1 predecessor edge) is rejected even when every frontier is
/// signed by its author and even under `VerificationMode::DontCheck`: legacy
/// chain-incomplete history is loadable unsigned, never as signed trust.
#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn chain_incomplete_snapshot_signed_at_frontier_is_rejected_even_with_dont_check() {
    let kx = key(1);
    let ky = key(2);
    let mut doc = Automerge::from_config(crate::Config::default().author(Some(author(&kx))))
        .with_actor(ActorId::from([1]))
        .unwrap();
    let x1 = append_at_level(&mut doc, None, 1, 0);
    let mut other = doc.fork_with_author(Some(author(&ky))).unwrap();
    other.set_actor(ActorId::from([2])).unwrap();
    let y1 = append_at_level(&mut other, None, 2, 0);
    doc.merge(&mut other).unwrap();
    let x2 = append_at_level(&mut doc, Some(&[y1]), 3, 0);
    // Legacy producer shape: X:2 depends on Y:1 (which depends on X:1) only.
    let mut expanded = crate::ExpandedChange::from(&doc.get_change_by_hash(&x2).unwrap());
    expanded.deps = vec![y1];
    expanded.hash = None;
    let legacy_x2 = Change::from(expanded);
    let legacy_bytes = [
        doc.get_change_by_hash(&x1).unwrap().raw_bytes(),
        doc.get_change_by_hash(&y1).unwrap().raw_bytes(),
        legacy_x2.raw_bytes(),
    ]
    .concat();
    // Unsigned loading of the legacy history is unchanged.
    let legacy = Automerge::load(&legacy_bytes).unwrap();
    assert_eq!(legacy.get_heads(), [legacy_x2.hash()]);

    // Every loose frontier signed by its author: the only defect is the chain.
    let lx2 = legacy_x2.hash();
    let bytes = snapshot_with_entries(
        &legacy,
        [
            (x1, Signature::ed25519(sig(&kx, &x1))),
            (y1, Signature::ed25519(sig(&ky, &y1))),
            (lx2, Signature::ed25519(sig(&kx, &lx2))),
        ],
    );
    for mode in [
        crate::VerificationMode::Check,
        crate::VerificationMode::DontCheck,
    ] {
        let error = Automerge::load_with_options(&bytes, load_opts(200).verification_mode(mode))
            .expect_err("chain-incomplete signed snapshot must be rejected");
        assert!(
            matches!(
                error,
                AutomergeError::Signing(SigningError::InvalidFragment(
                    crate::signature::error::InvalidFragment::BrokenActorChain(h)
                )) if h == lx2
            ),
            "{mode:?}: {error:?}"
        );
        let mut receiver =
            Automerge::from_config(crate::Config::default().signing(author(&key(200))).unwrap());
        assert!(receiver.load_incremental(&bytes).is_err());
        assert!(receiver.get_heads().is_empty());
    }
}

/// Author tampering inside a snapshot is caught before any receiver mutation:
/// a signature from a key other than the bound author, a swapped author
/// footer on `seq == 1` (hash mismatch at reconstruction), and an author
/// footer on `seq > 1` (reconstruction rule) all fail, in initial and
/// incremental signed loads.
#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn snapshot_author_tampering_is_rejected_before_receiver_mutation() {
    let k = key(1);
    let mallory = key(9);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    sign_doc(&mut doc, a, &k);
    sign_doc(&mut doc, b, &k);
    let honest = doc.save();
    let (rest, Chunk::Document(_)) = Chunk::parse(Input::new(&honest)).unwrap() else {
        panic!("expected document");
    };
    let document = &honest[..honest.len() - rest.unconsumed_bytes().len()];

    // 1. Proof for the head produced by a key that is not the bound author.
    let foreign_proof = [
        document,
        SignaturesChunk::encode(
            HashEntries::canonicalize([
                (a, Signature::ed25519(sig(&k, &a))),
                (b, Signature::ed25519(sig(&mallory, &b))),
            ])
            .unwrap(),
        )
        .bytes(),
    ]
    .concat();

    // 2. Author footer swapped on seq 1 while the snapshot still claims the
    //    honest hashes: reconstruction detects the mismatch; Mallory's proof
    //    for the claimed hash cannot repair it.
    let mut expanded = crate::ExpandedChange::from(&doc.get_change_by_hash(&a).unwrap());
    expanded.extra_bytes = crate::change::encode_author_footer(&Some(author(&mallory))).to_vec();
    expanded.hash = None;
    let forged_a = Change::from(expanded);
    let mut single = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let only = append_at_level(&mut single, None, 1, 0);
    assert_eq!(only, a, "same deterministic first change");
    single
        .change_graph
        .forge_head_extra_bytes_for_test(&a, &forged_a, false);
    let swapped_bytes =
        snapshot_with_entries(&single, [(a, Signature::ed25519(sig(&mallory, &a)))]);

    // 3. Author footer on seq 2 reconstructed from a snapshot.
    let mut expanded = crate::ExpandedChange::from(&doc.get_change_by_hash(&b).unwrap());
    expanded.extra_bytes = crate::change::encode_author_footer(&Some(author(&k))).to_vec();
    expanded.hash = None;
    let forged_b = Change::from(expanded);
    assert_eq!(forged_b.seq(), 2);
    assert!(forged_b.author().is_some());
    let mut forged_doc = doc.clone();
    forged_doc
        .change_graph
        .forge_head_extra_bytes_for_test(&b, &forged_b, true);
    let fb = forged_b.hash();
    let seq2_author_bytes = snapshot_with_entries(
        &forged_doc,
        [
            (a, Signature::ed25519(sig(&k, &a))),
            (fb, Signature::ed25519(sig(&k, &fb))),
        ],
    );

    for (label, bytes) in [
        ("foreign proof", foreign_proof),
        ("swapped seq-1 author", swapped_bytes),
        ("author on seq 2", seq2_author_bytes),
    ] {
        let error = Automerge::load_with_options(&bytes, load_opts(200))
            .err()
            .unwrap_or_else(|| panic!("{label}: tampered snapshot must be rejected"));
        if label == "author on seq 2" {
            assert!(
                matches!(error, AutomergeError::AuthorOnNonInitialSeq(2, _)),
                "{label}: {error:?}"
            );
        }
        // Incremental into a receiver holding the honest history and a draft:
        // nothing changes.
        let mut receiver = Automerge::load_with_options(&honest, load_opts(200)).unwrap();
        receiver.set_actor(ActorId::from([200])).unwrap();
        let draft = append_at_level(&mut receiver, None, 7, 0);
        let heads_before = receiver.get_heads();
        let accepted_before = is_accepted(&mut receiver, &b);
        let mut log = PatchLog::active();
        assert!(
            receiver
                .load_incremental_log_patches(&bytes, &mut log)
                .is_err(),
            "{label}"
        );
        assert_eq!(receiver.get_heads(), heads_before, "{label}");
        assert_eq!(is_accepted(&mut receiver, &b), accepted_before, "{label}");
        assert!(!is_accepted(&mut receiver, &draft), "{label}");
        assert!(receiver.make_patches(&mut log).is_empty(), "{label}");
        assert!(receiver.get_change_by_hash(&fb).is_none(), "{label}");
        assert!(receiver.get_author_for_actor(&ActorId::from([1])) == Some(author(&k)));
    }
    // The honest snapshot still loads, pinning that only the tampering failed.
    assert_eq!(
        Automerge::load_with_options(&honest, load_opts(200))
            .unwrap()
            .get_heads(),
        [b]
    );
}

/// `StringMigration::ConvertToText` under a signing load creates new local
/// work: an unpublished draft with a request for the configured author. The
/// loaded history stays published and saves omit the migration until signed.
#[test]
fn string_migration_under_signing_load_is_new_local_unpublished_work() {
    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "s", "hello").unwrap();
    let a = tx.commit().0.unwrap();
    sign_doc(&mut doc, a, &k);
    let bytes = doc.save();

    let loader = key(200);
    let mut loaded = Automerge::load_with_options(
        &bytes,
        load_opts(200).migrate_strings(crate::StringMigration::ConvertToText),
    )
    .unwrap();
    // The migration is a new local change by the loader, not loaded history.
    let heads = loaded.get_heads();
    assert_eq!(heads.len(), 1);
    let migration = heads[0];
    assert_ne!(migration, a);
    assert!(is_accepted(&mut loaded, &a));
    assert!(!is_accepted(&mut loaded, &migration));
    assert!(loaded.has_unpublished_changes());
    let requests = loaded.signing_requests().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(SigningRequest::hash)
            .collect::<Vec<_>>(),
        [migration]
    );
    assert_eq!(
        loaded.get_change_by_hash(&migration).unwrap().author(),
        Some(author(&loader))
    );
    let (value, _) = loaded.get(ROOT, "s").unwrap().unwrap();
    assert!(matches!(value, crate::Value::Object(crate::ObjType::Text)));

    // Saving omits the unsigned migration; the original history is intact.
    let resaved = loaded.save();
    let mut reloaded = Automerge::load_with_options(&resaved, load_opts(201)).unwrap();
    assert_eq!(reloaded.get_heads(), [a]);
    assert!(is_accepted(&mut reloaded, &a));
    assert_eq!(
        reloaded.get(ROOT, "s").unwrap().unwrap().0,
        crate::Value::Scalar(std::borrow::Cow::Owned("hello".into()))
    );

    // Once signed, the migration is published and persists.
    sign_doc(&mut loaded, migration, &loader);
    assert!(!loaded.has_unpublished_changes());
    let published = Automerge::load_with_options(&loaded.save(), load_opts(202)).unwrap();
    assert_eq!(published.get_heads(), [migration]);

    // Without migration configured no local work is manufactured.
    let plain = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert!(!plain.has_unpublished_changes());
    assert_eq!(plain.get_heads(), [a]);
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn autocommit_incremental_cursor_tracks_published_emission_only() {
    let k = key(1);
    let mut doc = AutoCommit::from_config(crate::Config::default().signing(author(&k)).unwrap());
    doc.set_actor(ActorId::from([1])).unwrap();
    doc.put(ROOT, "v", 1).unwrap();
    let a = doc.commit().unwrap();
    let a_sig = sig(&k, &a);
    doc.insert_signatures([(a, a_sig)]);
    let full = doc.save();
    assert_eq!(
        chunk_types(&full),
        [ChunkType::Document, ChunkType::Signatures]
    );

    // A local unsigned draft is not emitted and must not move the cursor.
    doc.put(ROOT, "v", 2).unwrap();
    let b = doc.commit().unwrap();
    assert!(doc.save_incremental().is_empty());
    assert!(doc.has_unpublished_changes());

    // Publishing b emits it incrementally with its proof.
    doc.insert_signatures([(b, sig(&k, &b))]);
    let inc = doc.save_incremental();
    assert_eq!(
        chunk_types(&inc),
        [ChunkType::Change, ChunkType::Signatures]
    );
    assert!(doc.save_incremental().is_empty());

    let mut loaded = Automerge::load_with_options(&full, load_opts(200)).unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert_eq!(loaded.load_incremental(&inc).unwrap(), 1);
    assert_eq!(loaded.get_heads(), [b]);
    assert!(!loaded.has_unpublished_changes());
    // Replaying the same increment applies nothing new.
    assert_eq!(loaded.load_incremental(&inc).unwrap(), 0);
    assert_eq!(loaded.get_heads(), [b]);

    // Full save after increments also moves the cursor to the published heads.
    doc.put(ROOT, "v", 3).unwrap();
    let c = doc.commit().unwrap();
    doc.insert_signatures([(c, sig(&k, &c))]);
    let _ = doc.save();
    assert!(doc.save_incremental().is_empty());

    // save_and_verify validates the published projection in required mode.
    doc.put(ROOT, "v", 4).unwrap();
    doc.commit().unwrap();
    assert!(doc.save_and_verify().is_ok());
}

/// Two authors, two actors each, overlapping hard fragments, a message, an
/// empty change and a mask: everything published, snapshot round trip exact.
#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn fully_published_multi_actor_snapshot_round_trips_metadata_and_masks() {
    let ka = key(1);
    let kb = key(2);
    let mut alice = Automerge::from_config(crate::Config::default().signing(author(&ka)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a1 = append_at_level(&mut alice, None, 1, 0);
    let mut tx = alice.transaction();
    tx.put(ROOT, "msg", "hello").unwrap();
    let a2 = tx
        .commit_with(CommitOptions::default().with_message("with a message"))
        .0
        .unwrap();
    let a3 = alice.empty_commit(CommitOptions::default().with_time(5));
    alice.set_actor(ActorId::from([3])).unwrap();
    let a4 = append_at_level(&mut alice, None, 4, 1);

    // Bob works from Alice's published prefix as an unsigned replica, then his
    // signed groups are delivered to Alice.
    for hash in [a1, a2, a3, a4] {
        sign_doc(&mut alice, hash, &ka);
    }
    assert!(!alice.has_unpublished_changes());
    let mut bob = replica(&alice, author(&kb), ActorId::from([2]));
    let b1 = append_at_level(&mut bob, None, 10, 0);
    bob.set_actor(ActorId::from([4])).unwrap();
    let b2 = append_at_level(&mut bob, None, 11, 1);
    // b2 is a hard head whose canonical fragment contains b1; deliver the
    // loose b1 and the canonical bundle {b1, b2} with its frontier proofs.
    let b2_fragment = bob.get_fragment(b2).unwrap();
    assert!(b2_fragment.members.contains(&b1));
    let bundle = bob.bundle(b2_fragment.members.iter().copied()).unwrap();
    let delivery: Vec<u8> = [
        bob.get_change_by_hash(&b1).unwrap().raw_bytes(),
        SignaturesChunk::encode(
            HashEntries::canonicalize([(b1, Signature::ed25519(sig(&kb, &b1)))]).unwrap(),
        )
        .bytes(),
        bundle.bytes(),
        SignaturesChunk::encode(
            HashEntries::canonicalize([
                (b1, Signature::ed25519(sig(&kb, &b1))),
                (b2, Signature::ed25519(sig(&kb, &b2))),
            ])
            .unwrap(),
        )
        .bytes(),
    ]
    .concat();
    alice.load_incremental(&delivery).unwrap();
    assert_eq!(alice.get_heads(), bob.get_heads());
    assert!(!alice.has_unpublished_changes());

    let bytes = alice.save();
    let mut loaded = Automerge::load_with_options(
        &bytes,
        load_opts(200).write_frontier(HashMap::from([(author(&kb), vec![b1])])),
    )
    .unwrap();
    assert_eq!(loaded.get_heads(), alice.get_heads());
    assert_eq!(
        loaded.get_change_by_hash(&a2).unwrap().message(),
        Some("with a message")
    );
    assert_eq!(loaded.get_change_by_hash(&a3).unwrap().timestamp(), 5);
    for hash in [a1, a2, a3, a4, b1, b2] {
        assert!(is_accepted(&mut loaded, &hash), "{hash}");
        assert_eq!(
            loaded.get_change_by_hash(&hash).unwrap().raw_bytes(),
            alice.get_change_by_hash(&hash).unwrap().raw_bytes()
        );
    }
    assert!(!loaded.has_unpublished_changes());
    // The mask hides b2 (actor 4, Bob) but not b1.
    assert_eq!(loaded.get(ROOT, "value").unwrap().unwrap().0, 10.into());
    assert_eq!(
        loaded.get_author_for_actor(&ActorId::from([4])),
        Some(author(&kb))
    );
    assert_eq!(
        loaded.get_author_for_actor(&ActorId::from([2])),
        Some(author(&kb))
    );
}

#[test]
fn speculative_pending_groups_are_not_persisted() {
    let k = key(1);
    let other = key(2);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    sign_doc(&mut doc, a, &k);
    // A signed group that depends on an unknown change waits as pending.
    let mut remote = replica(&doc, author(&other), ActorId::from([2]));
    let _r1 = append_at_level(&mut remote, None, 2, 0);
    let r2 = append_at_level(&mut remote, None, 3, 0);
    let r2_bytes = [
        remote.get_change_by_hash(&r2).unwrap().raw_bytes(),
        SignaturesChunk::encode(
            HashEntries::canonicalize([(r2, Signature::ed25519(sig(&other, &r2)))]).unwrap(),
        )
        .bytes(),
    ]
    .concat();
    assert_eq!(doc.load_incremental(&r2_bytes).unwrap(), 0);
    assert!(doc.signing.has_pending_groups());

    let bytes = doc.save();
    let loaded = Automerge::load_with_options(&bytes, load_opts(200)).unwrap();
    assert_eq!(loaded.get_heads(), [a]);
    assert!(!loaded.signing.has_pending_groups());
    assert!(loaded.get_missing_deps(&[]).is_empty());
    assert!(doc.save_after(&[]).len() < bytes.len() + r2_bytes.len());
    assert!(
        doc.signing.has_pending_groups(),
        "save did not disturb pending"
    );
}

/// Published history without resident proof is an internal inconsistency.
/// Save must fail loudly in every build instead of emitting a snapshot whose
/// attached proof cannot cover it (bytes no required-mode loader accepts).
#[test]
fn save_panics_in_all_builds_when_published_history_lacks_resident_proof() {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    let k = key(1);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut doc, None, 1, 0);
    let b = append_at_level(&mut doc, None, 2, 0);
    sign_doc(&mut doc, a, &k);
    sign_doc(&mut doc, b, &k);
    assert!(is_accepted(&mut doc, &b));
    let honest = doc.save();
    let honest_groups = doc.published_groups(&[]).len();

    // Corrupt internal state: forget b's proof while b stays published.
    let crate::automerge::signing::SigningState::Required(signing) = &mut doc.signing else {
        panic!("required mode");
    };
    signing.signatures.retain_hashes(|hash| *hash != b);
    assert!(is_accepted(&mut doc, &b));

    let save = catch_unwind(AssertUnwindSafe(|| doc.save()));
    let groups = catch_unwind(AssertUnwindSafe(|| doc.published_groups(&[])));
    let incremental = catch_unwind(AssertUnwindSafe(|| doc.save_after(&[])));
    for (label, outcome) in [
        ("save", save.as_ref().map(|_| ())),
        ("published_groups", groups.as_ref().map(|_| ())),
        ("save_after", incremental.as_ref().map(|_| ())),
    ] {
        let error = outcome.expect_err(label);
        let message = error
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| error.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            message.contains("published") && message.contains("proof"),
            "{label}: inconsistency must be explained, got {message:?}"
        );
    }
    // The untouched document produced a complete, loadable snapshot.
    assert!(honest_groups >= 1);
    assert_eq!(
        Automerge::load_with_options(&honest, load_opts(200))
            .unwrap()
            .get_heads(),
        [b]
    );
}

#[test]
#[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
fn signed_snapshot_increments_into_nonempty_receiver_with_overlap_and_drafts() {
    let k = key(1);
    let rk = key(2);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);

    // Receiver already knows `a` and has its own draft `d`.
    let mut receiver = Automerge::load_with_options(
        &source.save(),
        LoadOptions::new().signing(author(&rk)).unwrap(),
    )
    .unwrap();
    receiver.set_actor(ActorId::from([9])).unwrap();
    let d = append_at_level(&mut receiver, None, 99, 0);
    assert!(receiver.has_unpublished_changes());

    let b = append_at_level(&mut source, None, 2, 0);
    let c = append_at_level(&mut source, None, 3, 1);
    sign_doc(&mut source, b, &k);
    sign_doc(&mut source, c, &k);
    let snapshot = source.save();

    // A mismatched patch log rejects before any mutation.
    let mut foreign = Automerge::new().with_actor(ActorId::from([7])).unwrap();
    append_at_level(&mut foreign, None, 1, 0);
    let mut bad_log = PatchLog::active();
    foreign.log_current_state(crate::types::ObjMeta::root(), &mut bad_log, true);
    let heads_before = receiver.get_heads();
    assert!(matches!(
        receiver.load_incremental_log_patches(&snapshot, &mut bad_log),
        Err(AutomergeError::PatchLogMismatch(_))
    ));
    assert_eq!(receiver.get_heads(), heads_before);

    let mut log = PatchLog::active();
    let applied = receiver
        .load_incremental_log_patches(&snapshot, &mut log)
        .unwrap();
    assert!(applied > 0);
    let mut expected = source.get_heads();
    expected.push(d);
    expected.sort();
    assert_eq!(receiver.get_heads(), expected);
    for hash in [a, b, c] {
        assert!(is_accepted(&mut receiver, &hash));
    }
    assert!(!is_accepted(&mut receiver, &d));
    assert!(receiver.has_unpublished_changes());
    assert_eq!(
        receiver
            .signing_requests()
            .unwrap()
            .iter()
            .map(|r| r.hash())
            .collect::<Vec<_>>(),
        [d]
    );
    assert!(!receiver.make_patches(&mut log).is_empty());

    // Repeating the same snapshot applies nothing.
    assert_eq!(receiver.load_incremental(&snapshot).unwrap(), 0);
    assert_eq!(receiver.get_heads(), expected);

    // Incremental groups from the source after a known cut resend complete
    // groups and apply once.
    let e = append_at_level(&mut source, None, 4, 0);
    sign_doc(&mut source, e, &k);
    let inc = source.save_after(&[c]);
    assert_eq!(
        chunk_types(&inc),
        [ChunkType::Change, ChunkType::Signatures]
    );
    assert_eq!(receiver.load_incremental(&inc).unwrap(), 1);
    assert_eq!(receiver.load_incremental(&inc).unwrap(), 0);
    assert!(is_accepted(&mut receiver, &e));
}

#[test]
fn malformed_or_unknown_chunks_in_signed_streams_fail_without_mutation() {
    let k = key(1);
    let mut source = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let snapshot = source.save();
    let b = append_at_level(&mut source, None, 2, 0);
    sign_doc(&mut source, b, &k);
    let group = source.save_after(&[a]);

    let unknown = framed(ChunkType::Signatures, &[0]);
    // Valid framing (magic, checksum over type+length+body) with an
    // unrecognised type, so the failure is the type itself, not a checksum.
    let unknown_type = framed_raw(9, &[]);
    let mut bad_checksum = framed(ChunkType::Signatures, &[0]);
    bad_checksum[4] ^= 0xff;
    let truncated_sigs = &group[..group.len() - 1];
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("truncated signatures", truncated_sigs.to_vec()),
        (
            "unknown chunk after snapshot",
            [snapshot.as_slice(), &unknown_type].concat(),
        ),
        (
            "unknown chunk after group",
            [group.as_slice(), &unknown_type].concat(),
        ),
        (
            "bad checksum after snapshot",
            [snapshot.as_slice(), &bad_checksum].concat(),
        ),
        (
            "bare group prefix",
            group[..group.len() - unknown.len()].to_vec(),
        ),
    ];
    for (label, bytes) in &cases {
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            let error = Automerge::load_with_options(bytes, load_opts(200).on_partial_load(policy))
                .err()
                .unwrap_or_else(|| {
                    panic!("{label} {policy:?}: damaged signed stream must not load")
                });
            if label.starts_with("unknown chunk") {
                assert!(
                    matches!(
                        &error,
                        AutomergeError::Load(crate::storage::load::Error::UnknownChunkType(9))
                    ),
                    "{label} {policy:?}: {error}"
                );
            }
            if label.starts_with("bad checksum") {
                assert!(
                    matches!(
                        &error,
                        AutomergeError::Load(crate::storage::load::Error::BadChecksum)
                    ),
                    "{label} {policy:?}: {error}"
                );
            }
        }
        let mut receiver = Automerge::load_with_options(&snapshot, load_opts(200)).unwrap();
        assert!(receiver.load_incremental(bytes).is_err(), "{label}");
        assert_eq!(receiver.get_heads(), [a], "{label}");
        assert!(!receiver.signing.has_pending_groups(), "{label}");
    }
}

mod properties {
    //! Generated commit/sign/save/reload/append schedules checked against the
    //! semantic contract: saved bytes carry exactly the published history,
    //! reload re-derives publication for all of it with no drafts, and an
    //! incremental stream converges a stale copy to the same published state.

    use proptest::prelude::*;
    use std::collections::BTreeSet;

    use super::*;

    #[derive(Clone, Debug)]
    enum Step {
        /// Commit with actor index (0..3); the level search gives a mix of
        /// loose and hard heads naturally.
        Commit(u8),
        /// Sign the n-th outstanding (unsigned, unpublished) hash.
        Sign(u8),
        /// Full save, reload, compare; also snapshot the incremental cursor.
        Save,
        /// Incremental save into the stale copy made at the last `Save`.
        Increment,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => (0u8..3).prop_map(Step::Commit),
            4 => (0u8..6).prop_map(Step::Sign),
            1 => Just(Step::Save),
            1 => Just(Step::Increment),
        ]
    }

    fn published_set(doc: &mut Automerge) -> BTreeSet<ChangeHash> {
        let hashes: Vec<_> = doc.get_changes(&[]).iter().map(Change::hash).collect();
        hashes
            .into_iter()
            .filter(|hash| is_accepted(doc, hash))
            .collect()
    }

    fn all_set(doc: &Automerge) -> BTreeSet<ChangeHash> {
        doc.get_changes(&[]).iter().map(Change::hash).collect()
    }

    fn check_reload(doc: &mut Automerge, bytes: &[u8]) -> Automerge {
        let mut loaded = Automerge::load_with_options(bytes, load_opts(200)).unwrap();
        let published = published_set(doc);
        assert_eq!(
            all_set(&loaded),
            published,
            "reload carries exactly published history"
        );
        assert_eq!(
            published_set(&mut loaded),
            published,
            "reload re-derives publication"
        );
        assert!(!loaded.has_unpublished_changes());
        assert!(loaded.try_signing().unwrap().state.local_drafts.is_empty());
        assert!(loaded.signing_requests().unwrap().is_empty());
        for hash in &published {
            assert_eq!(
                loaded.get_change_by_hash(hash).unwrap().raw_bytes(),
                doc.get_change_by_hash(hash).unwrap().raw_bytes()
            );
        }
        assert_eq!(loaded.get_heads(), doc.published_heads());
        // An empty third peer accepts the re-export, and it equals a direct save.
        let again = loaded.save();
        let third = Automerge::load_with_options(&again, load_opts(201)).unwrap();
        assert_eq!(all_set(&third), published);
        loaded
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]
        #[test]
        #[allow(deprecated)] // Deterministic identity fixtures and legacy-option regression coverage.
        fn signed_persistence_tracks_publication(steps in prop::collection::vec(step(), 1..24)) {
            let k = key(1);
            let mut doc = Automerge::from_config(crate::Config::default().signing(author(&k)).unwrap()).with_actor(ActorId::from([1])).unwrap();
            let mut value = 0i64;
            // Stale copy from the last full save plus the cursor at that time.
            let mut stale: Option<(Automerge, Vec<ChangeHash>)> = None;
            for s in steps {
                match s {
                    Step::Commit(actor) => {
                        doc.set_actor(ActorId::from([actor + 1])).unwrap();
                        value += 1;
                        let mut tx = doc.transaction();
                        tx.put(ROOT, "v", value).unwrap();
                        tx.commit();
                    }
                    Step::Sign(n) => {
                        let hashes: Vec<ChangeHash> =
                            doc.get_changes(&[]).iter().map(Change::hash).collect();
                        let mut outstanding: Vec<ChangeHash> = Vec::new();
                        for h in hashes {
                            if is_accepted(&mut doc, &h) {
                                continue;
                            }
                            let signing = doc.try_signing().unwrap();
                            if !crate::automerge::signing::has_signature(
                                signing.graph,
                                signing.authors,
                                &signing.state.signatures,
                                &h,
                            ) {
                                outstanding.push(h);
                            }
                        }
                        outstanding.sort();
                        if !outstanding.is_empty() {
                            let hash = outstanding[usize::from(n) % outstanding.len()];
                            doc.insert_signatures([(hash, sig(&k, &hash))]);
                        }
                    }
                    Step::Save => {
                        let bytes = doc.save();
                        let loaded = check_reload(&mut doc, &bytes);
                        stale = Some((loaded, doc.published_heads()));
                    }
                    Step::Increment => {
                        if let Some((copy, cursor)) = stale.as_mut() {
                            let inc = doc.save_after(cursor);
                            copy.load_incremental(&inc).unwrap();
                            let published = published_set(&mut doc);
                            prop_assert_eq!(all_set(copy), published.clone());
                            prop_assert_eq!(published_set(copy), published);
                            prop_assert!(!copy.has_unpublished_changes());
                            *cursor = doc.published_heads();
                            // Replay is idempotent.
                            prop_assert_eq!(copy.load_incremental(&inc).unwrap(), 0);
                        }
                    }
                }
                // Invariant: published history is always a reloadable projection.
                let published = published_set(&mut doc);
                let bytes = doc.save();
                prop_assert_eq!(all_set(&check_reload(&mut doc, &bytes)), published);
            }
        }
    }
}
