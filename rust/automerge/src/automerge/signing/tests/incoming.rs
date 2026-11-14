//! End-to-end incoming group validation, causal admission, GC, and bypass tests.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::error::{InvalidFragment, ProofError, SigningError};
use crate::signature::{ed25519, signing_message, Signature, SignatureBytes, SignatureTag};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::{ChunkType, Header};
use crate::sync::{self, SyncDoc};
use crate::transaction::{CommitOptions, Transactable};
use crate::{
    ActorId, Author, Automerge, AutomergeError, Change, ChangeHash, ExpandedChange, LoadOptions,
    OnPartialLoad, PatchLog, ReadDoc, ROOT,
};

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

fn sig(key: &SigningKey, hash: ChangeHash) -> Signature {
    Signature::ed25519(ed25519::Signature::from_bytes(
        key.sign(&signing_message(&hash)).to_bytes(),
    ))
}

fn sign_doc(doc: &mut Automerge, hash: ChangeHash, key: &SigningKey) {
    let raw = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    doc.try_signing()
        .unwrap()
        .insert_signature(&hash, &raw)
        .unwrap();
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

fn attach_signatures(
    content: &[u8],
    entries: impl IntoIterator<Item = (ChangeHash, Signature)>,
) -> Vec<u8> {
    let chunk = SignaturesChunk::encode(HashEntries::canonicalize(entries).unwrap());
    [content, chunk.bytes()].concat()
}

fn framed(chunk_type: ChunkType, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    Header::new(chunk_type, body).write(&mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn signed_fragment_stream_loads_reexports_and_reclaims_superseded_signatures() {
    let k1 = key(1);
    let k2 = key(2);
    let mut sender = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_author(Some(author(&k2)));
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    sender.set_author(Some(author(&k1)));
    sender.set_actor(ActorId::from([1]));
    let a2 = append_at_level(&mut sender, None, "c", 3, 0);

    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, b1, &k2);
    sign_doc(&mut sender, a2, &k1);

    let fragments = sender.fragments(..);
    let encoded = sender.bundle_fragments(fragments.clone());
    assert_eq!(encoded.len(), 2);
    let stream = encoded.concat();

    let mut initial_log = PatchLog::active();
    let mut loaded = Automerge::load_with_options(
        &stream,
        LoadOptions::new().signing().patch_log(&mut initial_log),
    )
    .unwrap();
    assert_eq!(loaded.get_heads(), sender.get_heads());
    assert_eq!(loaded.hydrate(None), sender.hydrate(None));
    assert!(!loaded.make_patches(&mut initial_log).is_empty());
    for hash in [a1, b1, a2] {
        assert!(loaded.try_signing().unwrap().is_accepted(&hash));
    }
    assert_eq!(loaded.fragments(..), fragments);
    assert_eq!(loaded.bundle_fragments(loaded.fragments(..)), encoded);

    // Incremental superseding delivery reclaims covered resident signatures.
    let mut incremental = Automerge::signing();
    let a1_loose = attach_signatures(
        sender.get_change_by_hash(&a1).unwrap().raw_bytes(),
        [(a1, sig(&k1, a1))],
    );
    assert_eq!(incremental.load_incremental(&a1_loose).unwrap(), 1);
    assert_eq!(incremental.try_signing().unwrap().state.signatures.len(), 1);
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

#[test]
fn out_of_order_groups_wait_for_causal_dependencies_and_author_bindings() {
    let k1 = key(1);
    let k2 = key(2);
    let mut sender = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_author(Some(author(&k2)));
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    let b2 = append_at_level(&mut sender, None, "b2", 3, 1);

    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, b1, &k2);
    sign_doc(&mut sender, b2, &k2);

    let encoded = sender.bundle_fragments(sender.fragments(..));
    let (f_bytes, g_bytes) = (&encoded[0], &encoded[1]);

    assert!(matches!(
        Automerge::load_with_options(g_bytes, LoadOptions::new().signing()),
        Err(AutomergeError::MissingDeps)
    ));

    let mut from_ignore = Automerge::load_with_options(
        g_bytes,
        LoadOptions::new()
            .signing()
            .on_partial_load(OnPartialLoad::Ignore),
    )
    .unwrap();
    assert!(from_ignore.get_heads().is_empty());
    assert_eq!(from_ignore.get_missing_deps(&[]), vec![b1]);
    assert_eq!(from_ignore.load_incremental(f_bytes).unwrap(), 3);
    assert_eq!(from_ignore.get_heads(), [b2]);
    assert!(from_ignore.try_signing().unwrap().is_accepted(&b2));

    let reversed = [g_bytes.as_slice(), f_bytes.as_slice()].concat();
    let mut single_call =
        Automerge::load_with_options(&reversed, LoadOptions::new().signing()).unwrap();
    assert_eq!(single_call.get_heads(), [b2]);
    assert!(single_call.try_signing().unwrap().is_accepted(&b2));

    let mut receiver = Automerge::signing();
    assert_eq!(receiver.load_incremental(g_bytes).unwrap(), 0);
    assert_eq!(receiver.get_missing_deps(&[]), vec![b1]);
    assert_eq!(receiver.load_incremental(f_bytes).unwrap(), 3);
    for hash in [a1, b1, b2] {
        assert!(receiver.try_signing().unwrap().is_accepted(&hash));
    }
}

#[test]
fn missing_malformed_or_invalid_proofs_do_not_mutate_state() {
    let k1 = key(1);
    let wrong = key(9);
    let mut sender = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 1);
    let a2 = append_at_level(&mut sender, None, "b", 2, 1);
    sign_doc(&mut sender, a1, &k1);
    sign_doc(&mut sender, a2, &k1);
    let encoded = sender.bundle_fragments(sender.fragments(..));
    let valid_f = &encoded[0];
    let change_a2 = sender.get_change_by_hash(&a2).unwrap();
    let bundle_a2 = sender.bundle([a2]).unwrap();

    for bad_stream in [
        [valid_f.as_slice(), bundle_a2.bytes()].concat(),
        [
            valid_f.as_slice(),
            attach_signatures(bundle_a2.bytes(), [(a2, sig(&wrong, a2))]).as_slice(),
        ]
        .concat(),
        [
            valid_f.as_slice(),
            attach_signatures(
                bundle_a2.bytes(),
                [(
                    a2,
                    Signature::new(
                        SignatureTag::new(7),
                        SignatureBytes::new(vec![1; 64]).unwrap(),
                    ),
                )],
            )
            .as_slice(),
        ]
        .concat(),
        [
            valid_f.as_slice(),
            change_a2.raw_bytes(),
            framed(ChunkType::Signatures, &[1]).as_slice(),
        ]
        .concat(),
    ] {
        assert!(Automerge::load_with_options(&bad_stream, LoadOptions::new().signing()).is_err());
        let mut receiver = Automerge::signing();
        assert!(receiver.load_incremental(&bad_stream).is_err());
        assert!(receiver.get_heads().is_empty());
        assert!(receiver.get_missing_deps(&[]).is_empty());
        assert!(!receiver.try_signing().unwrap().is_accepted(&a1));
        assert_eq!(receiver.try_signing().unwrap().state.signatures.len(), 0);
    }

    let mut authorless = Automerge::new().with_actor(ActorId::from([3]));
    let h_none = append_at_level(&mut authorless, None, "x", 1, 0);
    let raw_none = authorless.get_change_by_hash(&h_none).unwrap();
    let mut receiver = Automerge::signing();
    assert!(matches!(
        receiver.load_incremental(&attach_signatures(raw_none.raw_bytes(), [(h_none, sig(&k1, h_none))])),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::MissingAuthor(h)
        ))) if h == h_none
    ));

    let mut bad_author = Automerge::new()
        .with_author(Some(Author::from(vec![1, 2, 3])))
        .with_actor(ActorId::from([4]));
    let h_bad = append_at_level(&mut bad_author, None, "y", 1, 0);
    let raw_bad = bad_author.get_change_by_hash(&h_bad).unwrap();
    assert!(matches!(
        receiver.load_incremental(&attach_signatures(raw_bad.raw_bytes(), [(h_bad, sig(&k1, h_bad))])),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::InvalidAuthorKey { hash, .. }
        ))) if hash == h_bad
    ));
}

#[test]
fn neither_other_groups_nor_resident_evidence_repairs_an_incomplete_proof() {
    let k1 = key(1);
    let k2 = key(2);
    let mut sender = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut sender, None, "a", 1, 0);
    sender.set_author(Some(author(&k2)));
    sender.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut sender, None, "b", 2, 1);
    sender.set_author(Some(author(&k1)));
    sender.set_actor(ActorId::from([1]));
    let a2 = append_at_level(&mut sender, None, "c", 3, 1);

    let f_incomplete = attach_signatures(
        sender.bundle([a1, b1]).unwrap().bytes(),
        [(b1, sig(&k2, b1))],
    );
    let g_with_extra = attach_signatures(
        sender.bundle([a2]).unwrap().bytes(),
        [(a1, sig(&k1, a1)), (a2, sig(&k1, a2))],
    );

    for stream in [
        [f_incomplete.as_slice(), g_with_extra.as_slice()].concat(),
        [g_with_extra.as_slice(), f_incomplete.as_slice()].concat(),
    ] {
        assert!(matches!(
            Automerge::load_with_options(&stream, LoadOptions::new().signing()),
            Err(AutomergeError::Signing(SigningError::InvalidProof(
                ProofError::MissingSignature(h)
            ))) if h == a1
        ));
    }

    let mut receiver = Automerge::signing();
    assert_eq!(receiver.load_incremental(&g_with_extra).unwrap(), 0);
    assert!(matches!(
        receiver.load_incremental(&f_incomplete),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::MissingSignature(h)
        ))) if h == a1
    ));

    let a1_loose = attach_signatures(
        sender.get_change_by_hash(&a1).unwrap().raw_bytes(),
        [(a1, sig(&k1, a1))],
    );
    let mut with_resident = Automerge::signing();
    with_resident.load_incremental(&a1_loose).unwrap();
    assert!(matches!(
        with_resident.load_incremental(&f_incomplete),
        Err(AutomergeError::Signing(SigningError::InvalidProof(
            ProofError::MissingSignature(h)
        ))) if h == a1
    ));
    assert!(with_resident
        .load_incremental(sender.get_change_by_hash(&a1).unwrap().raw_bytes())
        .is_err());
}

#[test]
fn canonical_membership_diamond_boundaries_and_broken_chains_are_enforced() {
    let k1 = key(1);
    let k2 = key(2);
    let k3 = key(3);

    // 1. Non-diamond: independent a1 (L0) and b1 (L1); c1 (L1) depends on [a1, b1].
    let mut non_diamond = Automerge::new()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let a1 = append_at_level(&mut non_diamond, None, "a", 1, 0);
    non_diamond.set_author(Some(author(&k2)));
    non_diamond.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut non_diamond, Some(&[]), "b", 2, 1);
    non_diamond.set_author(Some(author(&k3)));
    non_diamond.set_actor(ActorId::from([3]));
    let c1 = append_at_level(&mut non_diamond, Some(&[a1, b1]), "c", 3, 1);

    let a1_loose = attach_signatures(
        non_diamond.get_change_by_hash(&a1).unwrap().raw_bytes(),
        [(a1, sig(&k1, a1))],
    );
    let b1_group = attach_signatures(
        non_diamond.bundle([b1]).unwrap().bytes(),
        [(b1, sig(&k2, b1))],
    );
    let truncated_c1 = attach_signatures(
        non_diamond.bundle([c1]).unwrap().bytes(),
        [(c1, sig(&k3, c1))],
    );
    let stream = [
        a1_loose.as_slice(),
        b1_group.as_slice(),
        truncated_c1.as_slice(),
    ]
    .concat();
    assert!(matches!(
        Automerge::load_with_options(&stream, LoadOptions::new().signing()),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::NonCanonicalMembership(h)
        ))) if h == c1
    ));

    // 2. Honest diamond: db1 (L1) depends on da1 (L0); dc1 (L1) depends on [da1, db1].
    let mut diamond = Automerge::new()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let da1 = append_at_level(&mut diamond, None, "a", 1, 0);
    diamond.set_author(Some(author(&k2)));
    diamond.set_actor(ActorId::from([2]));
    let db1 = append_at_level(&mut diamond, Some(&[da1]), "b", 2, 1);
    diamond.set_author(Some(author(&k3)));
    diamond.set_actor(ActorId::from([3]));
    let dc1 = append_at_level(&mut diamond, Some(&[da1, db1]), "c", 3, 1);

    let db1_group = attach_signatures(
        diamond.bundle([da1, db1]).unwrap().bytes(),
        [(da1, sig(&k1, da1)), (db1, sig(&k2, db1))],
    );
    let dc1_canonical = attach_signatures(
        diamond.bundle([dc1]).unwrap().bytes(),
        [(dc1, sig(&k3, dc1))],
    );
    let dc1_stuffed = attach_signatures(
        diamond.bundle([da1, dc1]).unwrap().bytes(),
        [(da1, sig(&k1, da1)), (dc1, sig(&k3, dc1))],
    );

    let mut receiver = Automerge::signing();
    assert_eq!(receiver.load_incremental(&dc1_canonical).unwrap(), 0);
    assert_eq!(receiver.load_incremental(&db1_group).unwrap(), 3);
    assert_eq!(receiver.get_heads(), diamond.get_heads());

    assert!(matches!(
        Automerge::load_with_options(
            &[db1_group.as_slice(), dc1_stuffed.as_slice()].concat(),
            LoadOptions::new().signing(),
        ),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::NonCanonicalMembership(h)
        ))) if h == dc1
    ));

    // 3. Cross-group broken actor chain (X:2 missing direct X:1 predecessor edge).
    diamond.set_author(Some(author(&k1)));
    diamond.set_actor(ActorId::from([1]));
    let dx2 = append_at_level(&mut diamond, Some(&[db1]), "x2", 4, 0);
    let mut expanded = ExpandedChange::from(&diamond.get_change_by_hash(&dx2).unwrap());
    expanded.deps = vec![db1];
    expanded.hash = None;
    let unlinked_dx2 = Change::from(expanded);
    let bad_dx2_bytes = attach_signatures(
        unlinked_dx2.raw_bytes(),
        [(unlinked_dx2.hash(), sig(&k1, unlinked_dx2.hash()))],
    );
    assert!(matches!(
        Automerge::load_with_options(
            &[db1_group.as_slice(), bad_dx2_bytes.as_slice()].concat(),
            LoadOptions::new().signing(),
        ),
        Err(AutomergeError::Signing(SigningError::InvalidFragment(
            InvalidFragment::BrokenActorChain(h)
        ))) if h == unlinked_dx2.hash()
    ));
}

#[test]
fn overlaps_forged_orphans_local_boundaries_partial_tails_and_bypasses() {
    let k1 = key(1);
    let k2 = key(2);
    let k3 = key(3);
    let malloy = key(99);

    let mut source = Automerge::signing()
        .with_author(Some(author(&k1)))
        .with_actor(ActorId::from([1]));
    let root = append_at_level(&mut source, None, "root", 0, 1);
    let a1 = append_at_level(&mut source, None, "a1", 1, 0);
    source.set_author(Some(author(&k2)));
    source.set_actor(ActorId::from([2]));
    let b1 = append_at_level(&mut source, Some(&[a1]), "b1", 2, 1);
    source.set_author(Some(author(&k3)));
    source.set_actor(ActorId::from([3]));
    let c1 = append_at_level(&mut source, Some(&[a1]), "c1", 3, 1);
    for (h, k) in [(root, &k1), (a1, &k1), (b1, &k2), (c1, &k3)] {
        sign_doc(&mut source, h, k);
    }

    let root_bytes = &source.bundle_fragments([source.get_fragment(root).unwrap()])[0];
    let fb_bytes = &source.bundle_fragments([source.get_fragment(b1).unwrap()])[0];
    let fc_bytes = &source.bundle_fragments([source.get_fragment(c1).unwrap()])[0];

    let mut receiver = Automerge::signing();

    // Forged orphan claiming unknown Actor 2 seq 2 is purged once real Actor 2 arrives.
    let mut forged = Automerge::new()
        .with_author(Some(author(&malloy)))
        .with_actor(ActorId::from([2]));
    append_at_level(&mut forged, None, "f1", 1, 0);
    let forged_seq2 = append_at_level(&mut forged, None, "f2", 2, 0);
    let forged_orphan = attach_signatures(
        forged.get_change_by_hash(&forged_seq2).unwrap().raw_bytes(),
        [(forged_seq2, sig(&malloy, forged_seq2))],
    );
    assert_eq!(receiver.load_incremental(&forged_orphan).unwrap(), 0);

    assert_eq!(receiver.load_incremental(fb_bytes).unwrap(), 0);
    assert_eq!(receiver.load_incremental(fb_bytes).unwrap(), 0);
    assert_eq!(receiver.load_incremental(fc_bytes).unwrap(), 0);
    assert_eq!(receiver.load_incremental(root_bytes).unwrap(), 4);
    assert_eq!(receiver.get_heads(), source.get_heads());
    assert!(!receiver.signing.has_pending_groups());

    // fork() preserves required mode at current heads, while fork_at() is rejected
    // because an interior historical commit may lack its own frontier signature.
    let mut forked = source.fork();
    assert!(forked.signing.is_required());
    assert!(forked.try_signing().unwrap().is_accepted(&root));
    assert!(matches!(
        source.fork_at(&[root]),
        Err(AutomergeError::Signing(SigningError::ForkAtNotSupported))
    ));

    // Partial-load boundary: bare content before < 9-byte truncated prefix is withheld.
    let bare_root = source.get_change_by_hash(&root).unwrap();
    let bare_truncated = [bare_root.raw_bytes(), &[0x85, 0x6f, 0x4a, 0x83]].concat();
    let recovered = Automerge::load_with_options(
        &bare_truncated,
        LoadOptions::new()
            .signing()
            .on_partial_load(OnPartialLoad::Ignore),
    )
    .unwrap();
    assert!(recovered.get_heads().is_empty());

    // Entry-point bypasses: snapshots, apply_changes, merge, and sync all reject.
    let snapshot = source.save();
    assert!(matches!(
        Automerge::load_with_options(&snapshot, LoadOptions::new().signing()),
        Err(AutomergeError::Signing(SigningError::SnapshotNotSupported))
    ));
    let mut target = Automerge::signing();
    assert!(matches!(
        target.load_incremental(&snapshot),
        Err(AutomergeError::Signing(SigningError::SnapshotNotSupported))
    ));
    assert!(matches!(
        target.apply_changes(source.get_changes(&[])),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    assert!(matches!(
        target.merge(&mut source),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
    let mut unsigned_peer = Automerge::new().with_actor(ActorId::from([9]));
    append_at_level(&mut unsigned_peer, None, "p", 1, 0);
    let mut peer_state = sync::State::new();
    let msg = unsigned_peer
        .generate_sync_message(&mut peer_state)
        .unwrap();
    let mut target_state = sync::State::new();
    assert!(target.generate_sync_message(&mut target_state).is_none());
    assert!(matches!(
        target.receive_sync_message(&mut target_state, msg),
        Err(AutomergeError::Signing(
            SigningError::UnauthenticatedChanges
        ))
    ));
}
