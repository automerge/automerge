//! The detached-signature tests.
//!
//! These live in the crate rather than in `tests/` because they assemble chunk
//! streams by hand — groupings the export API would never produce, which is the
//! point of them — and inspect which records a document retains. Both need
//! `signature_chunk_for`, which is deliberately not public.

use crate::{
    sync::SyncDoc, transaction::Transactable, ActorId, Author, AutoCommit, AutomergeError, Change,
    ChangeHash, Config, Fragment, LoadOptions, ReadDoc, SignatureInput, SignatureMode, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};

fn signing_key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

fn author(byte: u8) -> Author<'static> {
    Author::from(signing_key(byte).verifying_key().to_bytes().to_vec())
}

fn ed25519(hash: ChangeHash, key: u8) -> SignatureInput {
    SignatureInput::ed25519(hash, signing_key(key).sign(&hash.0).to_bytes())
}

/// Sign `hash` with key `key` and attach the record.
fn sign(doc: &mut AutoCommit, hash: ChangeHash, key: u8) {
    doc.add_signatures([ed25519(hash, key)]).unwrap();
}

fn authenticated() -> AutoCommit {
    AutoCommit::new_with_config(Config::new().signature_mode(SignatureMode::Authenticated))
}

/// The canonical encoding of exactly `inputs`, as the chunk a document holding
/// only those records emits for `hash`.
///
/// Records sort by hash and then by bytes, so this is what any document holding
/// the same records emits — which is how a test says which records are retained
/// without reaching inside the store.
fn records(hash: ChangeHash, inputs: impl IntoIterator<Item = SignatureInput>) -> Vec<u8> {
    let mut reference = AutoCommit::new();
    reference.add_signatures(inputs).unwrap();
    reference.signature_chunk_for([hash])
}

/// Assert the export invariant: every fragment the document offers is one it can
/// actually encode, so no caller can be handed a fragment which is then dropped.
fn assert_fragments_are_exportable(doc: &AutoCommit) {
    let offered = doc.fragments(..);
    let encoded = doc.bundle_fragments(offered.clone());
    assert_eq!(
        offered.len(),
        encoded.len(),
        "{} of {} fragments were not exportable",
        offered.len() - encoded.len(),
        offered.len()
    );
}

fn requested(doc: &AutoCommit) -> Vec<ChangeHash> {
    doc.signing_requests()
        .iter()
        .map(|request| request.change_hash())
        .collect()
}

/// Encode `hashes` from `source` as the chunk stream an authenticated importer
/// accepts: the history, followed by the records which authenticate it.
///
/// One change chunk for a lone change, one bundle chunk for several, which is
/// the same shape the fragments API produces.
fn chunk(source: &AutoCommit, hashes: &[ChangeHash]) -> Vec<u8> {
    let mut bytes = match hashes {
        [hash] => source
            .get_change_by_hash(hash)
            .unwrap()
            .raw_bytes()
            .to_vec(),
        _ => source.bundle(hashes.to_vec()).unwrap().bytes().to_vec(),
    };
    bytes.extend(source.signature_chunk_for(hashes.iter().copied()));
    bytes
}

/// Encode history which belongs to no document, paired with records held by
/// `signer`. Records are independent of history, so a document can encode
/// records for changes it does not hold.
fn loose_chunk(signer: &AutoCommit, change: &Change) -> Vec<u8> {
    let mut bytes = change.raw_bytes().to_vec();
    bytes.extend(signer.signature_chunk_for([change.hash()]));
    bytes
}

#[test]
fn signing_request_names_the_actor_maximum_and_the_raw_hash() {
    let mut doc = AutoCommit::new().with_author(Some(author(7)));
    doc.put(ROOT, "one", 1).unwrap();
    doc.commit().unwrap();
    doc.put(ROOT, "two", 2).unwrap();
    let second = doc.commit().unwrap();

    let requests = doc.signing_requests();
    let request = requests
        .iter()
        .find(|request| request.change_hash() == second)
        .unwrap();
    assert_eq!(request.author(), &author(7));
    assert_eq!(request.bytes_to_sign(), &second.0);
}

#[test]
fn a_valid_signature_satisfies_the_request_without_changing_heads() {
    let mut doc = AutoCommit::new().with_author(Some(author(7)));
    doc.put(ROOT, "value", 1).unwrap();
    let hash = doc.commit().unwrap();
    let heads = doc.get_heads();

    sign(&mut doc, hash, 7);

    assert_eq!(doc.get_heads(), heads);
    assert!(requested(&doc).is_empty());
}

#[test]
fn actors_with_the_same_author_are_requested_independently() {
    let shared = author(9);
    let mut doc = AutoCommit::new().with_author(Some(shared.clone()));
    doc.put(ROOT, "one", 1).unwrap();
    let first = doc.commit().unwrap();

    doc.set_actor(ActorId::random());
    doc.put(ROOT, "two", 2).unwrap();
    let second = doc.commit().unwrap();

    // Two actors, two requests, one author: an actor's record covers only its own
    // chain, so each needs its own even though the key is the same.
    let requests = doc.signing_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|r| r.change_hash())
            .collect::<std::collections::BTreeSet<_>>(),
        [first, second].into_iter().collect()
    );
    assert!(requests.iter().all(|r| r.author() == &shared));

    // One actor's signature says nothing about the other's, even though both
    // actors share an author.
    sign(&mut doc, second, 9);
    assert_eq!(requested(&doc), vec![first]);
}

#[test]
fn only_a_record_under_the_actors_own_key_authenticates() {
    let mut doc = AutoCommit::new().with_author(Some(author(3)));
    doc.put(ROOT, "value", 1).unwrap();
    let hash = doc.commit().unwrap();

    // Every record is retained and relayed, whatever its scheme and whether or
    // not it verifies, so a process can carry records it cannot interpret and a
    // valid record for the same change can still arrive later.
    doc.add_signatures([
        ed25519(hash, 4),
        SignatureInput::custom(hash, b"application bytes"),
        SignatureInput::ed25519(hash, [0; 64]),
    ])
    .unwrap();
    assert_eq!(
        doc.signature_chunk_for([hash]),
        records(
            hash,
            [
                ed25519(hash, 4),
                SignatureInput::custom(hash, b"application bytes"),
                SignatureInput::ed25519(hash, [0; 64]),
            ]
        )
    );
    assert_eq!(requested(&doc), vec![hash]);

    sign(&mut doc, hash, 3);
    assert_eq!(
        doc.signature_chunk_for([hash]),
        records(
            hash,
            [
                ed25519(hash, 4),
                SignatureInput::custom(hash, b"application bytes"),
                SignatureInput::ed25519(hash, [0; 64]),
                ed25519(hash, 3),
            ]
        )
    );
    assert!(requested(&doc).is_empty());
}

#[test]
fn an_author_which_is_not_an_ed25519_key_cannot_be_signed() {
    let mut doc = authenticated().with_author(Some(Author::from(vec![0xAA; 16])));
    doc.put(ROOT, "value", 1).unwrap();
    let hash = doc.commit().unwrap();

    // Nothing to verify against, so the change is never requested and its
    // fragment never becomes exportable: there is no signature which could
    // authenticate it, so it can only be withheld.
    assert!(requested(&doc).is_empty());
    assert!(doc.fragments(..).is_empty());
    assert!(doc
        .bundle_fragments(vec![Fragment {
            head: hash,
            level: 0,
            boundary: vec![],
            checkpoints: vec![],
            members: vec![hash],
        }])
        .is_empty());
}

#[test]
fn fragment_streams_carry_their_records_automatically() {
    let mut source = AutoCommit::new().with_author(Some(author(5)));
    source.put(ROOT, "value", 1).unwrap();
    let hash = source.commit().unwrap();
    source
        .add_signatures([SignatureInput::custom(hash, b"relayed")])
        .unwrap();
    sign(&mut source, hash, 5);

    let fragment = Fragment {
        head: hash,
        level: 0,
        boundary: vec![],
        checkpoints: vec![],
        members: vec![hash],
    };
    // One stream carries the fragment and the records which authenticate it,
    // which is the whole import contract: everything a chunk needs travels with
    // it, so the receiver never reasons about partial coverage.
    let encoded = source.bundle_fragments([fragment]).pop().unwrap();

    let mut target = authenticated();
    target.load_incremental(&encoded).unwrap();
    assert_eq!(target.get_heads(), vec![hash]);
    // The custom record was relayed alongside the one that did the work.
    assert_eq!(
        target.signature_chunk_for([hash]),
        source.signature_chunk_for([hash])
    );
}

#[test]
fn snapshot_round_trip_preserves_records() {
    let mut source = AutoCommit::new().with_author(Some(author(6)));
    source.put(ROOT, "value", 1).unwrap();
    let hash = source.commit().unwrap();
    source
        .add_signatures([SignatureInput::custom(hash, b"also retained")])
        .unwrap();
    sign(&mut source, hash, 6);

    let loaded = AutoCommit::load(&source.save()).unwrap();
    assert_eq!(
        loaded.signature_chunk_for([hash]),
        source.signature_chunk_for([hash])
    );
    assert!(requested(&loaded).is_empty());
}

#[test]
fn a_document_chunk_must_be_covered() {
    let mut source = AutoCommit::new().with_author(Some(author(14)));
    source.put(ROOT, "value", "snapshot").unwrap();
    let hash = source.commit().unwrap();

    // A document contains its own authors, so whether it is covered is always
    // decidable on arrival: unsigned, it is refused rather than held.
    let unsigned = source.save();
    assert!(matches!(
        AutoCommit::load_with_options(
            &unsigned,
            LoadOptions::new().signature_mode(SignatureMode::Authenticated),
        ),
        Err(AutomergeError::UnsignedDocument(h, _)) if h == hash
    ));

    // Legacy load is unaffected.
    assert_eq!(AutoCommit::load(&unsigned).unwrap().get_heads(), vec![hash]);

    // Signed, the records in the same stream cover it before the load returns.
    sign(&mut source, hash, 14);
    let mut target = AutoCommit::load_with_options(
        &source.save(),
        LoadOptions::new().signature_mode(SignatureMode::Authenticated),
    )
    .unwrap();
    assert_eq!(target.get_heads(), vec![hash]);
    assert_eq!(
        target.get(ROOT, "value").unwrap().unwrap().0.as_str(),
        Some("snapshot")
    );
}

#[test]
fn own_storage_keeps_unsigned_local_commits_across_a_restart() {
    let mut doc = authenticated().with_author(Some(author(24)));
    doc.put(ROOT, "draft", "unsigned").unwrap();
    let draft = doc.commit().unwrap();

    // An unsigned local commit has nothing to verify, so a load which has not
    // been told whose bytes these are refuses the snapshot outright.
    let bytes = doc.save();
    assert!(matches!(
        AutoCommit::load_with_options(
            &bytes,
            LoadOptions::new().signature_mode(SignatureMode::Authenticated),
        ),
        Err(AutomergeError::UnsignedDocument(h, _)) if h == draft
    ));

    // Asserting one's own storage admits unsigned history under one's own
    // author, which is the only way an unsigned draft survives a restart.
    let mut own = AutoCommit::load_with_options(
        &bytes,
        LoadOptions::new()
            .signature_mode(SignatureMode::Authenticated)
            .author(author(24))
            .own_storage(true),
    )
    .unwrap();
    assert_eq!(own.get_heads(), vec![draft]);
    assert_eq!(
        own.get(ROOT, "draft").unwrap().unwrap().0.as_str(),
        Some("unsigned")
    );
    assert_eq!(requested(&own), vec![draft]);
}

#[test]
fn own_storage_admits_unsigned_history_only_under_the_declared_author() {
    let mut doc = authenticated().with_author(Some(author(25)));
    doc.put(ROOT, "draft", "unsigned").unwrap();
    let draft = doc.commit().unwrap();
    let bytes = doc.save();

    // An author is a public key which any chain may claim, so the assertion is
    // about whose bytes these are. It does not extend to another author's
    // unsigned history, which is what stops a peer's snapshot from putting
    // unsigned history into this document under someone else's name.
    assert!(matches!(
        AutoCommit::load_with_options(
            &bytes,
            LoadOptions::new()
                .signature_mode(SignatureMode::Authenticated)
                .author(author(26))
                .own_storage(true),
        ),
        Err(AutomergeError::UnsignedDocument(h, _)) if h == draft
    ));

    // Nor does it admit anything when no author was declared: there is then no
    // history the loader can claim as its own.
    assert!(matches!(
        AutoCommit::load_with_options(
            &bytes,
            LoadOptions::new()
                .signature_mode(SignatureMode::Authenticated)
                .own_storage(true),
        ),
        Err(AutomergeError::UnsignedDocument(h, _)) if h == draft
    ));
}

#[test]
fn saving_persists_held_history_as_the_chunk_it_arrived_as() {
    // An actor's chain, signed at its first and last change but not in the
    // middle, which is the ordinary shape: one record covers an actor's whole
    // prefix, so only a chunk's maxima carry one.
    let mut orphan = AutoCommit::new().with_author(Some(author(16)));
    orphan.put(ROOT, "a", 1).unwrap();
    let first = orphan.commit().unwrap();
    orphan.put(ROOT, "b", 2).unwrap();
    let second = orphan.commit().unwrap();
    orphan.put(ROOT, "c", 3).unwrap();
    let third = orphan.commit().unwrap();
    sign(&mut orphan, first, 16);
    sign(&mut orphan, third, 16);

    // Hold a chunk by withholding the actor's first change, which carries the
    // author, so there is no key to verify its record against yet.
    let mut target = authenticated();
    target
        .load_incremental(&chunk(&orphan, &[second, third]))
        .unwrap();
    assert!(target.get_heads().is_empty());

    // Held history is written, like a change whose dependencies are missing: it
    // may not be re-fetchable, and dropping it would lose it. It goes back after
    // the document chunk rather than inside it, so a load decides again rather
    // than taking it on trust.
    let mut reloaded = AutoCommit::load_with_options(
        &target.save(),
        LoadOptions::new().signature_mode(SignatureMode::Authenticated),
    )
    .unwrap();
    assert!(reloaded.get_heads().is_empty());
    assert_eq!(reloaded.get(ROOT, "c").unwrap(), None);
    // Its records travelled with it.
    assert_eq!(
        reloaded.signature_chunk_for([third]),
        orphan.signature_chunk_for([third])
    );

    // It also came back as the chunk it arrived as, which is what makes it
    // releasable: `second` carries no record of its own, so had the save
    // regrouped the chunk's changes into one chunk each, `second` would have
    // needed a record this document never had.
    reloaded
        .load_incremental(&chunk(&orphan, &[first]))
        .unwrap();
    assert_eq!(reloaded.get_heads(), vec![third]);
    assert_eq!(
        reloaded.get(ROOT, "b").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
    assert_eq!(
        reloaded.get(ROOT, "c").unwrap().unwrap().0.as_i64(),
        Some(3)
    );
}

#[test]
fn saving_persists_authenticated_history_waiting_on_a_dependency_as_its_chunk() {
    // A chunk can be authenticated and still not causally ready: here the
    // author's key is known from `first`, but `second` depends on a change by
    // another actor which has not arrived.
    let mut a = AutoCommit::new().with_author(Some(author(16)));
    a.put(ROOT, "a", 1).unwrap();
    let first = a.commit().unwrap();
    let mut b = AutoCommit::load(&a.save())
        .unwrap()
        .with_author(Some(author(17)));
    b.put(ROOT, "b", 2).unwrap();
    let from_b = b.commit().unwrap();
    sign(&mut b, from_b, 17);
    a.merge(&mut b).unwrap();
    a.put(ROOT, "c", 3).unwrap();
    let second = a.commit().unwrap();
    a.put(ROOT, "d", 4).unwrap();
    let third = a.commit().unwrap();
    sign(&mut a, first, 16);
    sign(&mut a, third, 16);

    let mut target = authenticated();
    target.load_incremental(&chunk(&a, &[first])).unwrap();
    target
        .load_incremental(&chunk(&a, &[second, third]))
        .unwrap();
    assert_eq!(target.get_heads(), vec![first]);

    // The waiting chunk goes back as it arrived. Written one change at a time
    // it would be refused on reload: `second` carries no record of its own.
    let mut reloaded = AutoCommit::load_with_options(
        &target.save(),
        LoadOptions::new().signature_mode(SignatureMode::Authenticated),
    )
    .unwrap();
    assert_eq!(reloaded.get_heads(), vec![first]);
    reloaded.load_incremental(&chunk(&b, &[from_b])).unwrap();
    assert_eq!(reloaded.get_heads(), vec![third]);
    assert_eq!(
        reloaded.get(ROOT, "d").unwrap().unwrap().0.as_i64(),
        Some(4)
    );
}

#[test]
fn a_held_chunk_whose_record_fails_once_its_author_arrives_is_dropped() {
    let mut orphan = AutoCommit::new().with_author(Some(author(16)));
    orphan.put(ROOT, "a", 1).unwrap();
    let first = orphan.commit().unwrap();
    orphan.put(ROOT, "b", 2).unwrap();
    let second = orphan.commit().unwrap();
    sign(&mut orphan, first, 16);
    // Signed under the wrong key. The record is present, so with the author
    // out of reach the chunk is held rather than refused.
    sign(&mut orphan, second, 17);

    let mut target = authenticated();
    target.load_incremental(&chunk(&orphan, &[second])).unwrap();
    assert_eq!(target.add_signatures([]).unwrap().pending_chunks(), 1);

    // The author arrives, the record is checked, and the chunk is dropped:
    // nothing will ever make that record verify, so holding it would only
    // hide the failure.
    target.load_incremental(&chunk(&orphan, &[first])).unwrap();
    assert_eq!(target.get_heads(), vec![first]);
    assert_eq!(target.add_signatures([]).unwrap().pending_chunks(), 0);

    // The same history, properly signed, is still welcome.
    sign(&mut orphan, second, 16);
    target.load_incremental(&chunk(&orphan, &[second])).unwrap();
    assert_eq!(target.get_heads(), vec![second]);
}

#[test]
fn authenticated_bundling_rejects_forged_fragment_metadata() {
    let mut doc = authenticated().with_author(Some(author(17)));
    doc.put(ROOT, "one", 1).unwrap();
    let signed = doc.commit().unwrap();
    sign(&mut doc, signed, 17);
    doc.put(ROOT, "two", 2).unwrap();
    let unsigned = doc.commit().unwrap();

    let forged = Fragment {
        head: unsigned,
        level: 0,
        boundary: vec![signed],
        checkpoints: vec![],
        members: vec![signed],
    };
    assert!(doc.bundle_fragments([forged]).is_empty());
}

#[test]
fn a_record_may_arrive_before_its_change() {
    let mut source = AutoCommit::new().with_author(Some(author(4)));
    source.put(ROOT, "value", 1).unwrap();
    let hash = source.commit().unwrap();

    // The author signs its own change and sends the record on its own: a stream of
    // records with no history is a legitimate shape.
    sign(&mut source, hash, 4);
    let mut target = authenticated();
    target
        .load_incremental(&source.signature_chunk_for([hash]))
        .unwrap();
    assert_eq!(
        target.signature_chunk_for([hash]),
        source.signature_chunk_for([hash])
    );
    assert!(target.get_heads().is_empty());

    // The history arrives later carrying no records, and the record already held
    // covers it.
    let bytes = source
        .get_change_by_hash(&hash)
        .unwrap()
        .raw_bytes()
        .to_vec();
    target.load_incremental(&bytes).unwrap();
    assert_eq!(target.get_heads(), vec![hash]);

    // A record for a change this document does not hold is retained too, so it
    // can be relayed, and it makes nothing visible on its own.
    let mut other = AutoCommit::new().with_author(Some(author(31)));
    other.put(ROOT, "theirs", 1).unwrap();
    let theirs = other.commit().unwrap();
    target.add_signatures([ed25519(theirs, 31)]).unwrap();
    assert_eq!(
        target.signature_chunk_for([theirs]),
        records(theirs, [ed25519(theirs, 31)])
    );
    assert_eq!(target.get_heads(), vec![hash]);

    // So is a further record on a change already covered, which authenticates
    // nothing new: only a record verifying under the author of its own change
    // ever does.
    target
        .add_signatures([SignatureInput::custom(hash, b"late")])
        .unwrap();
    assert_eq!(
        target.signature_chunk_for([hash]),
        records(
            hash,
            [ed25519(hash, 4), SignatureInput::custom(hash, b"late")]
        )
    );
    assert_eq!(target.get_heads(), vec![hash]);
}

#[test]
fn remote_history_is_invisible_until_a_record_covers_it() {
    let mut source = AutoCommit::new().with_author(Some(author(8)));
    source.put(ROOT, "value", "remote").unwrap();
    let hash = source.commit().unwrap();

    // Unsigned history is refused outright rather than held, so a peer which
    // strips records cannot withhold authentication quietly.
    let mut target = authenticated();
    assert!(matches!(
        target.load_incremental(&chunk(&source, &[hash])),
        Err(AutomergeError::UnsignedChunk(h)) if h == hash
    ));
    assert!(target.get_heads().is_empty());
    assert!(target.get(ROOT, "value").unwrap().is_none());
    assert!(target.diff_incremental().is_empty());

    // Signed, it is verified and admitted inside the import call.
    sign(&mut source, hash, 8);
    target.load_incremental(&chunk(&source, &[hash])).unwrap();
    assert_eq!(target.get_heads(), vec![hash]);
    assert_eq!(
        target.get(ROOT, "value").unwrap().unwrap().0.as_str(),
        Some("remote")
    );
    assert!(!target.diff_incremental().is_empty());
}

#[test]
fn one_signed_actor_maximum_authenticates_its_contiguous_prefix() {
    let mut source = AutoCommit::new().with_author(Some(author(10)));
    source.put(ROOT, "one", 1).unwrap();
    let first = source.commit().unwrap();
    source.put(ROOT, "two", 2).unwrap();
    let second = source.commit().unwrap();

    // One record, at the actor's maximum, authenticates the whole chunk.
    sign(&mut source, second, 10);
    let mut target = authenticated();
    target
        .load_incremental(&chunk(&source, &[first, second]))
        .unwrap();

    assert_eq!(target.get_heads(), vec![second]);
    assert_eq!(
        target.get(ROOT, "one").unwrap().unwrap().0.as_i64(),
        Some(1)
    );
    assert_eq!(
        target.get(ROOT, "two").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
}

#[test]
fn a_chunk_waits_until_its_authors_first_change_is_reachable() {
    let mut source = AutoCommit::new().with_author(Some(author(18)));
    source.put(ROOT, "one", 1).unwrap();
    let first = source.commit().unwrap();
    source.put(ROOT, "two", 2).unwrap();
    let second = source.commit().unwrap();
    sign(&mut source, first, 18);
    sign(&mut source, second, 18);

    // Sequence two alone names an author nobody can resolve, so its record
    // cannot be checked and the chunk is held rather than refused.
    let mut target = authenticated();
    target.load_incremental(&chunk(&source, &[second])).unwrap();
    assert!(target.get_heads().is_empty());

    // The first change carries the author, which resolves both. The chunk was
    // held rather than dropped, which is what lets it arrive now.
    target.load_incremental(&chunk(&source, &[first])).unwrap();
    assert_eq!(target.get_heads(), vec![second]);
}

#[test]
fn chunk_members_must_be_covered_by_their_actors_maximum() {
    let mut source = AutoCommit::new().with_author(Some(author(19)));
    source.put(ROOT, "one", 1).unwrap();
    let first = source.commit().unwrap();
    source.put(ROOT, "two", 2).unwrap();
    let second = source.commit().unwrap();
    let actor = source.get_actor().clone();
    sign(&mut source, second, 19);

    let mut target = authenticated();

    // A sequence two which does not depend on its own sequence one. Bundling the
    // two together would smuggle the unsigned sequence one in: it has no
    // dependencies, so it is causally ready the moment the chunk is admitted,
    // while a record on the maximum says nothing about it.
    let unlinked = Change::from(crate::ExpandedChange {
        operations: vec![crate::legacy::Op {
            action: crate::legacy::OpType::Put("unlinked".into()),
            obj: crate::legacy::ObjectId::Root,
            key: crate::legacy::Key::Map("unlinked".into()),
            pred: crate::legacy::SortedVec::new(),
            insert: false,
        }],
        actor_id: actor.clone(),
        author: None,
        hash: None,
        seq: 2,
        start_op: std::num::NonZero::new(2).unwrap(),
        time: 0,
        message: None,
        deps: vec![],
        extra_bytes: vec![],
    });
    let unlinked_hash = unlinked.hash();

    let mut relay = AutoCommit::new();
    relay
        .apply_changes([source.get_change_by_hash(&first).unwrap(), unlinked])
        .unwrap();
    relay.add_signatures([ed25519(unlinked_hash, 19)]).unwrap();
    let mut bundled = relay
        .bundle(vec![first, unlinked_hash])
        .unwrap()
        .bytes()
        .to_vec();
    bundled.extend(relay.signature_chunk_for([first, unlinked_hash]));
    assert!(matches!(
        target.load_incremental(&bundled),
        Err(AutomergeError::UncoveredChunk(hash, uncovered))
            if hash == first && uncovered == actor
    ));
    assert!(target.get_heads().is_empty());

    // The genuine chunk, whose members are all in the maximum's causal history,
    // is admitted on the one record at that maximum.
    target
        .load_incremental(&chunk(&source, &[first, second]))
        .unwrap();
    assert_eq!(target.get_heads(), vec![second]);
    assert!(target.get(ROOT, "unlinked").unwrap().is_none());
}

#[test]
fn several_change_chunks_in_one_stream_each_need_a_record() {
    let mut source = AutoCommit::new().with_author(Some(author(20)));
    source.put(ROOT, "one", 1).unwrap();
    let first = source.commit().unwrap();
    source.put(ROOT, "two", 2).unwrap();
    let second = source.commit().unwrap();

    // Each change chunk is its own level-0 fragment, and so its own unit of
    // authentication. Signing only the later one is not enough.
    sign(&mut source, second, 20);
    let mut stream = source
        .get_change_by_hash(&first)
        .unwrap()
        .raw_bytes()
        .to_vec();
    stream.extend(source.get_change_by_hash(&second).unwrap().raw_bytes());
    stream.extend(source.signature_chunk_for([first, second]));

    let mut target = authenticated();
    assert!(matches!(
        target.load_incremental(&stream),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == first
    ));

    sign(&mut source, first, 20);
    let mut stream = source
        .get_change_by_hash(&first)
        .unwrap()
        .raw_bytes()
        .to_vec();
    stream.extend(source.get_change_by_hash(&second).unwrap().raw_bytes());
    stream.extend(source.signature_chunk_for([first, second]));
    target.load_incremental(&stream).unwrap();
    assert_eq!(target.get_heads(), vec![second]);
}

#[test]
fn records_are_keyed_by_hash_not_sequence_number() {
    let mut source = authenticated().with_author(Some(author(21)));
    source.put(ROOT, "a1", 1).unwrap();
    let a1 = source.commit().unwrap();
    sign(&mut source, a1, 21);
    let actor_a = source.get_actor().clone();

    // A fork inherits the mode; it cannot be switched on afterwards.
    let mut target = source.clone();
    assert_eq!(target.signature_mode(), SignatureMode::Authenticated);
    target.set_actor(actor_a.clone());

    source.put(ROOT, "remote_a2", 2).unwrap();
    let remote_a2 = source.commit().unwrap();
    sign(&mut source, remote_a2, 21);

    // Claim sequence two locally, on a branch the record says nothing about.
    target.put(ROOT, "local_a2", 2).unwrap();
    let local_a2 = target.commit().unwrap();

    // An alternate sequence two is a different hash, and requirements are keyed
    // by hash, so the record over the remote sequence two cannot cover it.
    let forged = Change::from(crate::ExpandedChange {
        operations: vec![crate::legacy::Op {
            action: crate::legacy::OpType::Put("forged".into()),
            obj: crate::legacy::ObjectId::Root,
            key: crate::legacy::Key::Map("forged".into()),
            pred: crate::legacy::SortedVec::new(),
            insert: false,
        }],
        actor_id: actor_a,
        author: None,
        hash: None,
        seq: 3,
        start_op: std::num::NonZero::new(3).unwrap(),
        time: 0,
        message: None,
        deps: vec![local_a2],
        extra_bytes: vec![],
    });
    let unsigned = AutoCommit::new();
    assert!(matches!(
        target.load_incremental(&loose_chunk(&unsigned, &forged)),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == forged.hash()
    ));
    assert!(target.get(ROOT, "forged").unwrap().is_none());
    assert_eq!(target.get_heads(), vec![local_a2]);
}

#[test]
fn a_record_does_not_authenticate_another_actors_dependency() {
    let mut source = AutoCommit::new().with_author(Some(author(11)));
    source.put(ROOT, "alice", 1).unwrap();
    let alice = source.commit().unwrap();

    source.set_author(Some(author(12)));
    source.put(ROOT, "bob", 2).unwrap();
    let bob = source.commit().unwrap();

    // Bob's record covers Bob's change only, so a chunk containing both needs
    // Alice's too.
    sign(&mut source, bob, 12);
    let mut target = authenticated();
    assert!(matches!(
        target.load_incremental(&chunk(&source, &[alice, bob])),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == alice
    ));

    // Bob cannot stand in for Alice even by signing her change: the key is fixed
    // by her own author.
    source.add_signatures([ed25519(alice, 12)]).unwrap();
    assert!(matches!(
        target.load_incremental(&chunk(&source, &[alice, bob])),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == alice
    ));

    sign(&mut source, alice, 11);
    target
        .load_incremental(&chunk(&source, &[alice, bob]))
        .unwrap();
    assert_eq!(target.get_heads(), vec![bob]);
}

#[test]
fn authenticated_mode_keeps_local_unsigned_changes_visible() {
    let mut doc = authenticated().with_author(Some(author(13)));
    doc.put(ROOT, "draft", true).unwrap();
    let hash = doc.commit().unwrap();

    // Local commits are visible immediately, since signing them may need a key
    // held outside Automerge, but they cannot cross an authenticated export.
    assert_eq!(doc.get_heads(), vec![hash]);
    assert_eq!(
        doc.get(ROOT, "draft").unwrap().unwrap().0.as_bool(),
        Some(true)
    );
    assert!(doc.fragments(..).is_empty());
    assert_eq!(requested(&doc), vec![hash]);

    sign(&mut doc, hash, 13);
    assert!(!doc.fragments(..).is_empty());
}

#[test]
fn an_actor_with_no_author_can_never_be_exported() {
    let mut doc = authenticated().with_author(Some(author(22)));
    doc.put(ROOT, "signed", 1).unwrap();
    let signed = doc.commit().unwrap();
    sign(&mut doc, signed, 22);

    doc.set_author(None);
    doc.put(ROOT, "anonymous", 2).unwrap();
    let anonymous = doc.commit().unwrap();

    // The signed actor's fragment exports; the authorless one's never will, and
    // is not requested either, since no key exists to sign it against.
    let exportable: Vec<_> = doc.fragments(..).iter().map(|f| f.head).collect();
    assert!(exportable.contains(&signed));
    assert!(!exportable.contains(&anonymous));
    assert!(requested(&doc).is_empty());
}

#[test]
fn authenticated_mode_closes_the_loose_change_import_paths() {
    let mut source = AutoCommit::new().with_author(Some(author(23)));
    source.put(ROOT, "value", 23).unwrap();
    let hash = source.commit().unwrap();
    let change = source.get_change_by_hash(&hash).unwrap().clone();
    sign(&mut source, hash, 23);

    let mut target = authenticated();

    // These hand over `Change` values, not chunks, so there is no stream to find
    // records in and nothing to calculate a requirement against.
    assert!(matches!(
        target.apply_changes([change.clone()]),
        Err(AutomergeError::UnauthenticatedImport)
    ));
    assert!(matches!(
        target.merge(&mut source),
        Err(AutomergeError::UnauthenticatedImport)
    ));

    // An incremental load is a chunk stream, so it is the way in.
    target.load_incremental(&chunk(&source, &[hash])).unwrap();
    assert_eq!(target.get_heads(), vec![hash]);

    // Sync is refused: its messages carry no records, and a whole-document sync
    // message would merge into history rather than populate an empty document.
    let mut fresh = authenticated();
    let mut source_state = crate::sync::State::new();
    let mut target_state = crate::sync::State::new();
    let message = source
        .sync()
        .generate_sync_message(&mut source_state)
        .unwrap();
    assert!(matches!(
        fresh
            .sync()
            .receive_sync_message(&mut target_state, message),
        Err(AutomergeError::UnauthenticatedImport)
    ));
    assert!(fresh.get_heads().is_empty());

    // A legacy document still accepts all of it.
    let mut legacy = AutoCommit::new();
    legacy.apply_changes([change]).unwrap();
    assert_eq!(legacy.get_heads(), vec![hash]);
}

#[test]
fn an_incremental_save_falls_back_to_fragments_and_withholds_unsigned_history() {
    let mut source = authenticated().with_author(Some(author(25)));
    source.put(ROOT, "first", 1).unwrap();
    let first = source.commit().unwrap();
    sign(&mut source, first, 25);

    let mut target = authenticated();
    let signed_only = source.save_incremental();
    target.load_incremental(&signed_only).unwrap();
    assert_eq!(target.get_heads(), vec![first]);

    // An unsigned commit is in no exportable fragment, so an incremental save
    // withholds it: loose changes carry no signature set, and the fragment path
    // is the only authenticated way out.
    source.put(ROOT, "draft", 2).unwrap();
    let draft = source.commit().unwrap();
    assert!(source.save_incremental().is_empty());
    assert_eq!(requested(&source), vec![draft]);

    // The cursor did not move past it, so signing later still sends it.
    sign(&mut source, draft, 25);
    let rest = source.save_incremental();
    assert!(!rest.is_empty());
    target.load_incremental(&rest).unwrap();
    assert_eq!(target.get_heads(), vec![draft]);
    assert_eq!(
        target.get(ROOT, "draft").unwrap().unwrap().0.as_i64(),
        Some(2)
    );

    // Nothing further to send, and a legacy document is unaffected.
    assert!(source.save_incremental().is_empty());
    let mut legacy = AutoCommit::new().with_author(Some(author(26)));
    legacy.put(ROOT, "unsigned", 1).unwrap();
    let unsigned = legacy.commit().unwrap();
    let mut legacy_target = AutoCommit::new();
    legacy_target
        .load_incremental(&legacy.save_incremental())
        .unwrap();
    assert_eq!(legacy_target.get_heads(), vec![unsigned]);
}

#[test]
fn oversending_a_straddling_fragment_is_harmless() {
    let mut source = authenticated().with_author(Some(author(27)));
    source.put(ROOT, "one", 1).unwrap();
    let first = source.commit().unwrap();
    sign(&mut source, first, 27);

    // Ship the first change, then ask again from before it: the fragment covering
    // it goes whole, so the receiver sees a change it already has and skips it.
    let mut target = authenticated();
    target.load_incremental(&source.save_incremental()).unwrap();
    source.put(ROOT, "two", 2).unwrap();
    let second = source.commit().unwrap();
    sign(&mut source, second, 27);

    let from_scratch = source.save_after(&[]);
    target.load_incremental(&from_scratch).unwrap();
    assert_eq!(target.get_heads(), vec![second]);
    assert_eq!(
        target.get(ROOT, "one").unwrap().unwrap().0.as_i64(),
        Some(1)
    );
    assert_eq!(
        target.get(ROOT, "two").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
}

#[test]
fn a_save_concatenated_with_an_incremental_save_loads_as_one_stream() {
    let mut source = authenticated().with_author(Some(author(28)));
    source.put(ROOT, "first", 1).unwrap();
    let first = source.commit().unwrap();
    sign(&mut source, first, 28);

    let snapshot = source.save();
    source.put(ROOT, "second", 2).unwrap();
    let second = source.commit().unwrap();
    sign(&mut source, second, 28);
    let increment = source.save_incremental();

    // A document chunk followed by change chunks is an ordinary stream shape, and
    // every chunk in it declares its own signature set.
    let mut both = snapshot.clone();
    both.extend_from_slice(&increment);

    let mut target = AutoCommit::load_with_options(
        &both,
        LoadOptions::new().signature_mode(SignatureMode::Authenticated),
    )
    .unwrap();
    assert_eq!(target.get_heads(), vec![second]);
    assert_eq!(
        target.get(ROOT, "first").unwrap().unwrap().0.as_i64(),
        Some(1)
    );
    assert_eq!(
        target.get(ROOT, "second").unwrap().unwrap().0.as_i64(),
        Some(2)
    );

    // The same stream reaches an already-populated document incrementally.
    let mut incremental = authenticated();
    incremental.load_incremental(&both).unwrap();
    assert_eq!(incremental.get_heads(), vec![second]);
}

#[test]
fn change_chunks_after_a_document_chunk_are_not_trusted_by_association() {
    let mut source = authenticated().with_author(Some(author(29)));
    source.put(ROOT, "covered", 1).unwrap();
    let covered = source.commit().unwrap();
    sign(&mut source, covered, 29);
    let snapshot = source.save();

    // An unsigned change appended to a covered snapshot must not ride in on the
    // document chunk's coverage: it was not visible when the snapshot was saved,
    // so it is no more trusted than anything else off the wire.
    let mut forger = AutoCommit::load(&snapshot).unwrap();
    forger.set_actor(crate::ActorId::random());
    forger.set_author(Some(author(30)));
    forger.put(ROOT, "smuggled", 2).unwrap();
    let smuggled = forger.commit().unwrap();

    let mut stream = snapshot.clone();
    stream.extend_from_slice(forger.get_change_by_hash(&smuggled).unwrap().raw_bytes());

    assert!(matches!(
        AutoCommit::load_with_options(
            &stream,
            LoadOptions::new().signature_mode(SignatureMode::Authenticated),
        ),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == smuggled
    ));

    // Asserting one's own storage says where the document chunk came from, not
    // that whatever follows it in the stream is also ours.
    assert!(matches!(
        AutoCommit::load_with_options(
            &stream,
            LoadOptions::new()
                .signature_mode(SignatureMode::Authenticated)
                .author(author(29))
                .own_storage(true),
        ),
        Err(AutomergeError::UnsignedChunk(hash)) if hash == smuggled
    ));

    // A legacy load is unaffected.
    let legacy = AutoCommit::load(&stream).unwrap();
    assert_eq!(
        legacy.get(ROOT, "smuggled").unwrap().unwrap().0.as_i64(),
        Some(2)
    );
}

#[test]
fn an_incremental_save_does_not_strand_a_change_it_withheld() {
    let mut source = authenticated().with_author(Some(author(32)));
    source.put(ROOT, "signed", 1).unwrap();
    let signed = source.commit().unwrap();
    sign(&mut source, signed, 32);
    source.put(ROOT, "draft", 2).unwrap();
    let draft = source.commit().unwrap();

    // Whether the signed change goes out now depends on whether the draft's hash
    // formed a higher-level fragment which absorbed it — a fragment is exported
    // whole or not at all. Either way the draft is withheld, and either way the
    // cursor must not advance past it.
    let mut target = authenticated();
    target.load_incremental(&source.save_incremental()).unwrap();
    assert!(target.get(ROOT, "draft").unwrap().is_none());

    sign(&mut source, draft, 32);
    let rest = source.save_incremental();
    assert!(!rest.is_empty(), "the withheld change was stranded");
    target.load_incremental(&rest).unwrap();
    assert_eq!(target.get_heads(), vec![draft]);
    assert_eq!(
        target.get(ROOT, "signed").unwrap().unwrap().0.as_i64(),
        Some(1)
    );
    assert_eq!(
        target.get(ROOT, "draft").unwrap().unwrap().0.as_i64(),
        Some(2)
    );

    // And now there is genuinely nothing left.
    assert!(source.save_incremental().is_empty());
}

#[test]
fn a_fragment_boundary_below_the_signed_tip_is_requested_and_withheld() {
    let mut doc = authenticated().with_author(Some(author(40)));

    // Commit until one change lands at fragment level >= 1, then carry on, so
    // that boundary sits below the actor's latest change.
    let mut boundary = None;
    let mut after = 0;
    for i in 0..5000 {
        doc.put(ROOT, "n", i).unwrap();
        let hash = doc.commit().unwrap();
        if boundary.is_none() {
            if hash.0[0] == 0 {
                boundary = Some(hash);
            }
        } else {
            after += 1;
            if after >= 10 {
                break;
            }
        }
    }
    let boundary = boundary.expect("no fragment boundary in 5000 commits");
    let tip = doc.get_heads()[0];

    // Signing only the latest change lifts the frontier over everything below
    // it, which is all admission needs. Export needs more: a fragment travels
    // alone, so its receiver can only check the record on the hash it is given.
    sign(&mut doc, tip, 40);
    assert!(requested(&doc).contains(&boundary));
    let fragment = doc
        .fragments(1..)
        .into_iter()
        .find(|f| f.head == boundary)
        .expect("the boundary's fragment formed");
    assert!(
        doc.bundle_fragments([fragment.clone()]).is_empty(),
        "a fragment with no record of its own is withheld, not sent unusable"
    );

    // Signing the boundary itself releases it, and a peer takes it.
    sign(&mut doc, boundary, 40);
    assert!(!requested(&doc).contains(&boundary));
    let encoded = doc.bundle_fragments([fragment]);
    assert_eq!(encoded.len(), 1);
    let mut peer = authenticated();
    peer.load_incremental(&encoded[0]).unwrap();
    assert!(peer.get_changes(&[]).iter().any(|c| c.hash() == boundary));
}

#[test]
fn an_incremental_save_does_not_strand_a_fragment_it_could_not_authenticate() {
    let mut source = authenticated().with_author(Some(author(41)));

    // As above: a fragment boundary below the actor's latest change.
    let mut boundary = None;
    let mut after = 0;
    for i in 0..5000 {
        source.put(ROOT, "n", i).unwrap();
        let hash = source.commit().unwrap();
        if boundary.is_none() {
            if hash.0[0] == 0 {
                boundary = Some(hash);
            }
        } else {
            after += 1;
            if after >= 10 {
                break;
            }
        }
    }
    let boundary = boundary.expect("no fragment boundary in 5000 commits");
    let tip = source.get_heads()[0];

    // Signing only the tip leaves nothing unsigned, so there is no unsigned
    // change for a cursor to notice — and yet the boundary's own fragment is
    // withheld, because exporting it needs a record on the boundary itself.
    sign(&mut source, tip, 41);
    let mut target = authenticated();
    target.load_incremental(&source.save_incremental()).unwrap();
    assert!(
        !target.get_heads().contains(&tip),
        "the fragments above the withheld one cannot apply without it"
    );

    // So the cursor must not have advanced past it. Signing what the document
    // asks for — every fragment maximum, the boundary among them — releases the
    // rest, and it is a later incremental save which has to carry it.
    assert!(requested(&source).contains(&boundary));
    for hash in requested(&source) {
        sign(&mut source, hash, 41);
    }
    let rest = source.save_incremental();
    assert!(!rest.is_empty(), "the withheld fragments were stranded");
    target.load_incremental(&rest).unwrap();
    assert_eq!(target.get_heads(), vec![tip]);
    assert!(source.save_incremental().is_empty());
}

#[test]
fn an_unsigned_commit_never_withdraws_history_which_was_exportable() {
    fn cover(doc: &AutoCommit) -> std::collections::BTreeSet<ChangeHash> {
        doc.fragments(..)
            .into_iter()
            .flat_map(|fragment| fragment.members)
            .collect()
    }

    let mut doc = authenticated().with_author(Some(author(33)));
    let mut signed: std::collections::BTreeSet<ChangeHash> = Default::default();

    // A change whose hash has a leading zero byte forms a higher-level fragment
    // which absorbs the ones below it. Were that allowed to happen while the change
    // was still unsigned, the fragment would be unexportable and the history it
    // absorbed would stop being exportable with it — so its formation waits. One
    // commit in 256 has such a hash; commit until one does.
    let mut higher_level = None;
    for i in 0..5_000 {
        doc.put(ROOT, format!("k{i}"), i as i64).unwrap();
        let tip = doc.commit().unwrap();

        if tip.0[0] == 0 {
            // The interesting moment: an unsigned tip which wants to absorb
            // everything before it. Nothing already signed may disappear, and the
            // tip itself must not be exportable.
            let exportable = cover(&doc);
            assert!(
                signed.is_subset(&exportable),
                "an unsigned higher-level tip withdrew {} signed changes",
                signed.difference(&exportable).count()
            );
            assert!(!exportable.contains(&tip));
            higher_level = Some(tip);
        }

        sign(&mut doc, tip, 33);
        signed.insert(tip);
        if higher_level.is_some() {
            break;
        }
    }
    let higher_level = higher_level.expect("no higher-level fragment in 5000 commits");

    // Once signed it forms, absorbing what came before, and the cover is complete.
    assert_eq!(cover(&doc), signed);
    assert!(doc.fragments(1..).iter().any(|f| f.head == higher_level));
    assert_fragments_are_exportable(&doc);
}

#[test]
fn replaying_history_does_not_offer_an_unsigned_commit_as_exportable() {
    fn cover(doc: &AutoCommit) -> std::collections::BTreeSet<ChangeHash> {
        doc.fragments(..)
            .into_iter()
            .flat_map(|fragment| fragment.members)
            .collect()
    }

    let mut doc = authenticated().with_author(Some(author(34)));
    let mut signed: std::collections::BTreeSet<ChangeHash> = Default::default();

    // Commit until an *unsigned* tip would form a higher-level fragment, and leave
    // it unsigned. Any path which replays this history has to reach the same index
    // the document has now, or it would offer that fragment as exportable and, by
    // absorbing the signed history beneath it, take that history with it.
    let mut unsigned = None;
    for i in 0..5_000 {
        doc.put(ROOT, format!("k{i}"), i as i64).unwrap();
        let tip = doc.commit().unwrap();
        if tip.0[0] == 0 {
            unsigned = Some(tip);
            break;
        }
        sign(&mut doc, tip, 34);
        signed.insert(tip);
    }
    let unsigned = unsigned.expect("no higher-level fragment in 5000 commits");
    assert!(!cover(&doc).contains(&unsigned));
    assert!(signed.is_subset(&cover(&doc)));

    // A snapshot of one's own storage reconstructs the document chunk.
    let bytes = doc.save();
    let reloaded = AutoCommit::load_with_options(
        &bytes,
        LoadOptions::new()
            .config(Config::new().signature_mode(SignatureMode::Authenticated))
            .author(author(34))
            .own_storage(true),
    )
    .unwrap();
    assert!(!cover(&reloaded).contains(&unsigned));
    assert!(signed.is_subset(&cover(&reloaded)));
    assert_eq!(requested(&reloaded), vec![unsigned]);
    assert_fragments_are_exportable(&reloaded);

    // A fork copies the document; a fork at heads replays its changes.
    assert!(!cover(&doc.fork()).contains(&unsigned));
    let heads = doc.get_heads();
    let at = doc.fork_at(&heads).unwrap();
    assert!(!cover(&at).contains(&unsigned));
    assert!(signed.is_subset(&cover(&at)));
    assert_fragments_are_exportable(&doc);
    assert_fragments_are_exportable(&at);

    // Signing it brings it in, everywhere.
    sign(&mut doc, unsigned, 34);
    signed.insert(unsigned);
    assert_eq!(cover(&doc), signed);
    assert_fragments_are_exportable(&doc);
}
