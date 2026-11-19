//! Signed snapshot and incremental persistence tests.

use std::collections::HashMap;

use crate::signature::error::SigningError;
use crate::storage::parse::Input;
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::{Chunk, ChunkType, Header};
use crate::{AutoCommit, AutomergeError, LoadOptions, OnPartialLoad, Signature, SigningRequest};

use super::*;

fn load_opts(seed: u8) -> LoadOptions<'static> {
    LoadOptions::new().signing().author(author(&key(seed)))
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

#[test]
fn signed_save_retains_only_published_history_with_proof() {
    let k = key(1);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
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
        doc.request_signatures()
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
    assert!(loaded.request_signatures().unwrap().is_empty());
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
    let doc = Automerge::signing(author(&key(1))).unwrap();
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
    let mut receiver = Automerge::signing(author(&key(3))).unwrap();
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
fn ready_unpublished_cover_does_not_hide_published_fallback_in_snapshot() {
    let k = key(1);
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    // F = loose a (published); G = hard fragment {a? no: c} over boundary b (unpublished).
    let a = append_at_level(&mut doc, None, 1, 0);
    doc.set_actor(ActorId::from([2]));
    let b = append_at_level(&mut doc, Some(&[]), 2, 1);
    doc.set_actor(ActorId::from([1]));
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
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
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

#[test]
fn autocommit_incremental_cursor_tracks_published_emission_only() {
    let k = key(1);
    let mut doc = AutoCommit::signing(author(&k)).unwrap();
    doc.set_actor(ActorId::from([1]));
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
fn fully_published_multi_actor_snapshot_round_trips_metadata_and_masks() {
    let ka = key(1);
    let kb = key(2);
    let mut alice = Automerge::signing(author(&ka))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut alice, None, 1, 0);
    let mut tx = alice.transaction();
    tx.put(ROOT, "msg", "hello").unwrap();
    let a2 = tx
        .commit_with(CommitOptions::default().with_message("with a message"))
        .0
        .unwrap();
    let a3 = alice.empty_commit(CommitOptions::default().with_time(5));
    alice.set_actor(ActorId::from([3]));
    let a4 = append_at_level(&mut alice, None, 4, 1);

    // Bob works from Alice's published prefix as an unsigned replica, then his
    // signed groups are delivered to Alice.
    for hash in [a1, a2, a3, a4] {
        sign_doc(&mut alice, hash, &ka);
    }
    assert!(!alice.has_unpublished_changes());
    let mut bob = replica(&alice, author(&kb), ActorId::from([2]));
    let b1 = append_at_level(&mut bob, None, 10, 0);
    bob.set_actor(ActorId::from([4]));
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
        loaded.get_change_by_hash(&a2).unwrap().message().as_deref(),
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
    let mut doc = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
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

#[test]
fn signed_snapshot_increments_into_nonempty_receiver_with_overlap_and_drafts() {
    let k = key(1);
    let rk = key(2);
    let mut source = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);

    // Receiver already knows `a` and has its own draft `d`.
    let mut receiver = Automerge::load_with_options(
        &source.save(),
        LoadOptions::new().signing().author(author(&rk)),
    )
    .unwrap();
    receiver.set_actor(ActorId::from([9]));
    let d = append_at_level(&mut receiver, None, 99, 0);
    assert!(receiver.has_unpublished_changes());

    let b = append_at_level(&mut source, None, 2, 0);
    let c = append_at_level(&mut source, None, 3, 1);
    sign_doc(&mut source, b, &k);
    sign_doc(&mut source, c, &k);
    let snapshot = source.save();

    // A mismatched patch log rejects before any mutation.
    let mut foreign = Automerge::new().with_actor(ActorId::from([7]));
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
    assert!(receiver
        .try_signing()
        .unwrap()
        .state
        .local_drafts
        .contains(&d));
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
    let mut source = Automerge::signing(author(&k))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let a = append_at_level(&mut source, None, 1, 0);
    sign_doc(&mut source, a, &k);
    let snapshot = source.save();
    let b = append_at_level(&mut source, None, 2, 0);
    sign_doc(&mut source, b, &k);
    let group = source.save_after(&[a]);

    let unknown = framed(ChunkType::Signatures, &[0]);
    let mut unknown_type = unknown.clone();
    unknown_type[8] = 9; // unknown chunk type with a now-bad checksum
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
            "bare group prefix",
            group[..group.len() - unknown.len()].to_vec(),
        ),
    ];
    for (label, bytes) in &cases {
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            let result =
                Automerge::load_with_options(bytes, load_opts(200).on_partial_load(policy));
            match result {
                Err(_) => {}
                Ok(doc) => assert!(
                    doc.get_heads().is_empty() || doc.get_heads() == [a],
                    "{label} {policy:?}: bare or damaged content must not be admitted"
                ),
            }
            if *label == "bare group prefix" {
                assert!(
                    Automerge::load_with_options(bytes, load_opts(200).on_partial_load(policy))
                        .is_err(),
                    "{label}: a bare change is never admitted"
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
        assert!(loaded.request_signatures().unwrap().is_empty());
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
        fn signed_persistence_tracks_publication(steps in prop::collection::vec(step(), 1..24)) {
            let k = key(1);
            let mut doc = Automerge::signing(author(&k)).unwrap().with_actor(ActorId::from([1]));
            let mut value = 0i64;
            // Stale copy from the last full save plus the cursor at that time.
            let mut stale: Option<(Automerge, Vec<ChangeHash>)> = None;
            for s in steps {
                match s {
                    Step::Commit(actor) => {
                        doc.set_actor(ActorId::from([actor + 1]));
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
