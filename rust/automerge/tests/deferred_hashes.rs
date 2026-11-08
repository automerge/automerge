//! Loading with `LoadOptions::defer_hashes`: a document chunk is loaded
//! without rebuilding its changes, the stored head is trusted, and the other
//! hashes are computed (and the head checked) when first needed.
//!
//! The rebuild counters show which operations rebuild the changes.
use automerge::rebuild_counters;
use automerge::sync::{self, SyncDoc};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, Author, AutoCommit, Automerge, ChangeHash, LoadOptions, ObjType, ReadDoc, ROOT,
};
use sha2::{Digest, Sha256};

/// A small, fixed pseudo-random number generator (xorshift64*), so the
/// generated documents are the same, byte for byte, on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn commit(doc: &mut AutoCommit) -> Option<ChangeHash> {
    doc.commit_with(CommitOptions::default().with_time(0))
}

/// The number of ops in the change [`one_big_change`] makes.
const BIG_CHANGE_OPS: u64 = 20_000;

/// A document whose whole history is one change of [`BIG_CHANGE_OPS`] ops:
/// maps of scalars, lists and text, as an import into an empty document
/// makes.
fn one_big_change() -> AutoCommit {
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"importer".as_slice()));
    let items = doc.put_object(&ROOT, "items", ObjType::Map).unwrap();
    let mut ops = 1;
    let mut i = 0;
    // Each pass makes at most 4 + 8 + 6 ops; the rest are made one at a time.
    while ops + 18 < BIG_CHANGE_OPS {
        let item = doc
            .put_object(&items, format!("item{i:05}"), ObjType::Map)
            .unwrap();
        doc.put(&item, "kind", ["a", "b", "c"][rng.below(3) as usize])
            .unwrap();
        doc.put(&item, "x", rng.below(1000) as i64).unwrap();
        doc.put(&item, "y", rng.below(1000) as i64).unwrap();
        ops += 4;
        if rng.below(4) == 0 {
            let list = doc.put_object(&item, "points", ObjType::List).unwrap();
            for j in 0..rng.below(8) {
                doc.insert(&list, j as usize, rng.below(100) as i64)
                    .unwrap();
                ops += 1;
            }
            ops += 1;
        }
        if rng.below(8) == 0 {
            let text = doc.put_object(&item, "label", ObjType::Text).unwrap();
            doc.splice_text(&text, 0, 0, "label").unwrap();
            ops += 6;
        }
        i += 1;
    }
    while ops < BIG_CHANGE_OPS {
        doc.put(&items, format!("extra{ops}"), ops as i64).unwrap();
        ops += 1;
    }
    commit(&mut doc);
    assert_eq!(doc.get_changes(&[])[0].len() as u64, BIG_CHANGE_OPS);
    doc
}

/// A document of several changes by two actors, with one head.
fn some_history() -> AutoCommit {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"first".as_slice()));
    let map = doc.put_object(&ROOT, "map", ObjType::Map).unwrap();
    let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
    for i in 0..10 {
        doc.put(&map, format!("k{i}"), i as i64).unwrap();
        doc.insert(&list, i, i as i64).unwrap();
        commit(&mut doc);
    }
    let mut other = doc.fork().with_actor(ActorId::from(b"second".as_slice()));
    other.put(&map, "k3", "replaced").unwrap();
    other.delete(&map, "k4").unwrap();
    commit(&mut other);
    doc.merge(&mut other).unwrap();
    doc.put(&ROOT, "version", 2).unwrap();
    commit(&mut doc);
    assert_eq!(doc.get_heads().len(), 1);
    doc
}

fn deferred(bytes: &[u8]) -> AutoCommit {
    AutoCommit::load_with_options(bytes, LoadOptions::new().defer_hashes(true)).unwrap()
}

/// Rebuilds counted while running `f`.
fn ops_rebuilt_by<T>(f: impl FnOnce() -> T) -> (T, u64) {
    rebuild_counters::reset();
    let result = f();
    (result, rebuild_counters::ops_rebuilt())
}

#[test]
fn a_deferred_load_does_not_rebuild_the_changes() {
    let bytes = one_big_change().save();

    let (_, full) = ops_rebuilt_by(|| Automerge::load(&bytes).unwrap());
    assert_eq!(full, BIG_CHANGE_OPS);

    let (_, late) = ops_rebuilt_by(|| deferred(&bytes));
    // A full load rebuilds all 20,000 ops; a deferred load none. The gate is
    // a tenth of the way from the second to the first.
    let gate = BIG_CHANGE_OPS / 10;
    assert!(late <= gate, "a deferred load rebuilt {late} ops");
}

#[test]
fn a_deferred_load_reads_and_saves_as_a_full_load() {
    for mut doc in [one_big_change(), some_history()] {
        let bytes = doc.save();
        let mut full = AutoCommit::load(&bytes).unwrap();
        let (mut late, rebuilt) = ops_rebuilt_by(|| deferred(&bytes));
        assert_eq!(rebuilt, 0);
        assert_eq!(late.get_heads(), full.get_heads());
        assert_eq!(
            late.hydrate(&ROOT, None).unwrap(),
            full.hydrate(&ROOT, None).unwrap()
        );
        let (saved, rebuilt) = ops_rebuilt_by(|| late.save());
        assert_eq!(saved, bytes);
        assert_eq!(late.save_nocompress(), full.save_nocompress());
        assert_eq!(rebuilt, 0);
    }
}

#[test]
fn local_changes_and_diffs_need_no_hashes() {
    let bytes = some_history().save();
    let mut full = AutoCommit::load(&bytes).unwrap();
    let mut late = deferred(&bytes);
    let loaded_heads = full.get_heads();
    rebuild_counters::reset();
    for d in [&mut late, &mut full] {
        d.set_actor(ActorId::from(b"third".as_slice()));
        d.put(&ROOT, "edited", true).unwrap();
        let list = d.get(&ROOT, "list").unwrap().unwrap().1;
        d.insert(&list, 0, "new").unwrap();
        commit(d);
        d.put(&ROOT, "edited", false).unwrap();
        commit(d);
    }
    // Only `full`'s load is behind it, so only rebuilds since are counted.
    assert_eq!(rebuild_counters::ops_rebuilt(), 0);
    assert_eq!(late.get_heads(), full.get_heads());
    assert_eq!(late.save(), full.save());
    let heads = full.get_heads();
    assert_eq!(
        late.diff(&loaded_heads, &heads),
        full.diff(&loaded_heads, &heads)
    );
    assert_eq!(late.get_last_local_change(), full.get_last_local_change());
    assert_eq!(rebuild_counters::ops_rebuilt(), 0);
}

#[test]
fn getting_the_changes_computes_the_hashes_once() {
    let bytes = one_big_change().save();
    let mut late = deferred(&bytes);
    let mut full = AutoCommit::load(&bytes).unwrap();
    let (changes, rebuilt) = ops_rebuilt_by(|| late.get_changes(&[]));
    assert_eq!(changes, full.get_changes(&[]));
    assert_eq!(rebuilt, BIG_CHANGE_OPS);
    let (_, rebuilt) = ops_rebuilt_by(|| {
        late.get_changes(&[]);
        late.verify_hashes().unwrap();
    });
    assert_eq!(rebuilt, 0);
}

#[test]
fn every_operation_that_needs_the_hashes_gets_them() {
    let mut doc = some_history();
    let bytes = doc.save();
    let changes = doc.get_changes(&[]);
    let first = changes[0].hash();
    let head = doc.get_heads()[0];

    assert_eq!(deferred(&bytes).get_changes(&[]), changes);
    assert_eq!(
        deferred(&bytes).get_changes(&[first]),
        doc.get_changes(&[first])
    );
    assert_eq!(
        deferred(&bytes).get_change_by_hash(&first),
        doc.get_change_by_hash(&first)
    );
    assert_eq!(
        deferred(&bytes).get_change_by_hash(&head),
        doc.get_change_by_hash(&head)
    );
    assert_eq!(
        deferred(&bytes).fork_at(&[first]).unwrap().save(),
        doc.fork_at(&[first]).unwrap().save()
    );
    assert_eq!(
        deferred(&bytes).save_after(&[first]),
        doc.save_after(&[first])
    );
    assert_eq!(deferred(&bytes).fragments(..), doc.fragments(..));

    // Merging both ways.
    let mut late = deferred(&bytes);
    let mut other = doc.fork().with_actor(ActorId::from(b"other".as_slice()));
    other.put(&ROOT, "other", 1).unwrap();
    commit(&mut other);
    late.merge(&mut other).unwrap();
    assert_eq!(late.get_heads(), other.get_heads());
    let mut target = doc.fork().with_actor(ActorId::from(b"target".as_slice()));
    let mut late = deferred(&bytes);
    target.merge(&mut late).unwrap();
    assert_eq!(target.get_heads(), late.get_heads());

    // A change from elsewhere which depends on a loaded change other than
    // the head, so its dependency's hash is not among those known.
    let mut late = deferred(&bytes);
    let mut branch = doc
        .fork_at(&[first])
        .unwrap()
        .with_actor(ActorId::from(b"branch".as_slice()));
    branch.put(&ROOT, "branch", true).unwrap();
    commit(&mut branch);
    late.apply_changes(branch.get_changes(&[first])).unwrap();
    doc.apply_changes(branch.get_changes(&[first])).unwrap();
    assert_eq!(late.get_heads(), doc.get_heads());
    assert_eq!(late.save(), doc.save());

    // A change already in the document is recognised as such.
    let mut late = deferred(&bytes);
    late.apply_changes(changes.clone()).unwrap();
    assert_eq!(late.get_changes(&[]), changes);
}

#[test]
fn a_deferred_document_syncs() {
    let mut doc = some_history();
    let bytes = doc.save();
    let mut late = deferred(&bytes);
    late.set_actor(ActorId::from(b"local".as_slice()));
    late.put(&ROOT, "local", true).unwrap();
    commit(&mut late);
    let fourth = doc.get_changes(&[])[3].hash();
    let mut peer = doc.fork_at(&[fourth]).unwrap();
    let (mut ours, mut theirs) = (sync::State::new(), sync::State::new());
    for _ in 0..10 {
        if let Some(m) = late.sync().generate_sync_message(&mut ours) {
            peer.sync().receive_sync_message(&mut theirs, m).unwrap();
        }
        if let Some(m) = peer.sync().generate_sync_message(&mut theirs) {
            late.sync().receive_sync_message(&mut ours, m).unwrap();
        }
    }
    assert_eq!(peer.get_heads(), late.get_heads());
    assert_eq!(peer.save(), late.save());
}

#[test]
fn verify_hashes_makes_the_document_whole() {
    let bytes = some_history().save();
    let mut late = deferred(&bytes);
    late.set_actor(ActorId::from(b"local".as_slice()));
    late.put(&ROOT, "local", true).unwrap();
    commit(&mut late);
    let (result, rebuilt) = ops_rebuilt_by(|| late.verify_hashes());
    result.unwrap();
    assert!(rebuilt > 0);
    let mut full = AutoCommit::load(&late.save()).unwrap();
    assert_eq!(late.get_changes(&[]), full.get_changes(&[]));
    // Nothing is left to compute.
    let (result, rebuilt) = ops_rebuilt_by(|| late.verify_hashes());
    result.unwrap();
    assert_eq!(rebuilt, 0);
    // A document loaded in full has nothing to verify.
    full.verify_hashes().unwrap();
}

#[test]
fn a_forked_deferred_document_is_independent() {
    let mut doc = some_history();
    let bytes = doc.save();
    let mut late = deferred(&bytes);
    let mut fork = late.fork().with_actor(ActorId::from(b"forked".as_slice()));
    fork.put(&ROOT, "forked", true).unwrap();
    commit(&mut fork);
    late.verify_hashes().unwrap();
    doc.merge(&mut fork).unwrap();
    assert_eq!(fork.get_changes(&[]), doc.get_changes(&[]));
}

#[test]
fn authors_are_loaded_from_the_changes_without_their_hashes() {
    let author = Author::from(b"an author".to_vec());
    let mut doc = AutoCommit::new()
        .with_actor(ActorId::from(b"writer".as_slice()))
        .with_author(Some(author.clone()));
    doc.put(&ROOT, "written", true).unwrap();
    commit(&mut doc);
    let bytes = doc.save();
    let late = deferred(&bytes);
    let mut full = AutoCommit::load(&bytes).unwrap();
    assert_eq!(late.get_authors(), full.get_authors());
    let writer = full.get_changes(&[])[0].actor_id().clone();
    assert_eq!(late.get_author_for_actor(&writer), Some(author.clone()));
    assert_eq!(late.get_actors_for_author(&author), vec![writer]);
}

#[test]
fn a_document_of_two_heads_loads_in_full() {
    let mut doc = some_history();
    let mut left = doc.fork().with_actor(ActorId::from(b"left".as_slice()));
    let mut right = doc.fork().with_actor(ActorId::from(b"right".as_slice()));
    left.put(&ROOT, "side", "left").unwrap();
    right.put(&ROOT, "side", "right").unwrap();
    commit(&mut left);
    commit(&mut right);
    left.merge(&mut right).unwrap();
    assert_eq!(left.get_heads().len(), 2);
    let bytes = left.save();
    let (_, full) = ops_rebuilt_by(|| AutoCommit::load(&bytes).unwrap());
    let (mut late, rebuilt) = ops_rebuilt_by(|| deferred(&bytes));
    assert_eq!(rebuilt, full);
    assert_eq!(late.get_changes(&[]), left.get_changes(&[]));
}

#[test]
fn trailing_change_chunks_are_applied() {
    let mut doc = some_history();
    let mut bytes = doc.save();
    doc.put(&ROOT, "after", 1).unwrap();
    commit(&mut doc);
    bytes.extend(doc.save_incremental());
    let mut late = deferred(&bytes);
    assert_eq!(late.get_heads(), doc.get_heads());
    assert_eq!(late.save(), doc.save());
}

#[test]
fn empty_documents_load() {
    let mut late = deferred(&[]);
    assert!(late.get_heads().is_empty());
    let mut late = deferred(&AutoCommit::new().save());
    assert!(late.get_heads().is_empty());
    late.verify_hashes().unwrap();
}

/// `bytes`, one uncompressed document chunk, with one bit of its stored
/// head flipped and its checksum made good again: a document whose stored
/// head is not what its changes rebuild into.
fn with_a_wrong_head(bytes: &[u8]) -> Vec<u8> {
    fn leb(bytes: &[u8], at: &mut usize) -> u64 {
        let (mut value, mut shift) = (0u64, 0);
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                return value;
            }
        }
    }
    // Magic bytes, checksum, chunk type, then the data's length.
    let mut at = 9;
    let len = leb(bytes, &mut at);
    let data_start = at;
    assert_eq!(bytes.len() - data_start, len as usize, "one chunk");
    // The actors, then the heads.
    let mut p = data_start;
    for _ in 0..leb(bytes, &mut p) {
        let actor_len = leb(bytes, &mut p) as usize;
        p += actor_len;
    }
    assert_eq!(leb(bytes, &mut p), 1, "one head");
    let mut out = bytes.to_vec();
    out[p] ^= 1;
    let digest = Sha256::new()
        .chain_update(&out[8..data_start])
        .chain_update(&out[data_start..])
        .finalize();
    out[4..8].copy_from_slice(&digest[..4]);
    out
}

#[test]
fn a_wrong_head_is_caught_when_the_hashes_are_verified() {
    let mut doc = some_history();
    let crafted = with_a_wrong_head(&doc.save_nocompress());
    // A full load refuses it at once.
    assert!(AutoCommit::load(&crafted).is_err());
    // A deferred load opens it, and it reads and saves.
    let mut late = deferred(&crafted);
    assert_ne!(late.get_heads(), doc.get_heads());
    assert_eq!(
        late.hydrate(&ROOT, None).unwrap(),
        doc.hydrate(&ROOT, None).unwrap()
    );
    assert!(AutoCommit::load(&late.save()).is_err());
    // Verifying reports what the full load did, and keeps doing so.
    let full_error = AutoCommit::load(&crafted).unwrap_err().to_string();
    assert_eq!(late.verify_hashes().unwrap_err().to_string(), full_error);
    assert_eq!(late.verify_hashes().unwrap_err().to_string(), full_error);
}

#[test]
#[should_panic(expected = "could not be verified")]
fn a_wrong_head_panics_where_the_hashes_are_needed() {
    let crafted = with_a_wrong_head(&some_history().save_nocompress());
    let mut late = deferred(&crafted);
    late.get_changes(&[]);
}

#[test]
#[should_panic(expected = "could not be verified")]
fn a_wrong_head_panics_on_sync() {
    let crafted = with_a_wrong_head(&some_history().save_nocompress());
    let mut late = deferred(&crafted);
    let mut state = sync::State::new();
    late.sync().generate_sync_message(&mut state);
}

#[test]
fn fragments_cover_changes_made_before_the_hashes_are_known() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"many".as_slice()));
    for i in 0..1000 {
        doc.put(&ROOT, "counter", i).unwrap();
        commit(&mut doc);
    }
    let bytes = doc.save();
    let mut late = deferred(&bytes);
    let mut full = AutoCommit::load(&bytes).unwrap();
    for d in [&mut late, &mut full] {
        // An actor which sorts first, so the loaded actors move up.
        d.set_actor(ActorId::from(b"early".as_slice()));
        for i in 0..300 {
            d.put(&ROOT, "more", i).unwrap();
            commit(d);
        }
    }
    let fragments = full.fragments(..);
    assert!(fragments.iter().any(|f| f.level > 0));
    assert_eq!(late.fragments(..), fragments);
    for f in &fragments {
        assert_eq!(late.get_fragment(f.head), full.get_fragment(f.head));
    }
    late.verify_hashes().unwrap();
    assert_eq!(late.fragments(..), fragments);
}

/// Changes committed while the hashes are deferred get no fragments then;
/// computing the hashes builds the fragments of every change, those
/// included, and changes committed afterwards build on them. The document
/// then gives the same changes, heads, fragments and sync as one loaded in
/// full.
#[test]
fn changes_made_while_deferred_are_whole_once_the_hashes_are_known() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"many".as_slice()));
    for i in 0..1000 {
        doc.put(&ROOT, "counter", i).unwrap();
        commit(&mut doc);
    }
    let bytes = doc.save();
    let mut late = deferred(&bytes);
    let mut full = AutoCommit::load(&bytes).unwrap();
    rebuild_counters::reset();
    for d in [&mut late, &mut full] {
        d.set_actor(ActorId::from(b"local".as_slice()));
        for i in 0..300 {
            d.put(&ROOT, "before", i).unwrap();
            commit(d);
        }
    }
    // Nothing needed the loaded changes' hashes yet.
    assert_eq!(rebuild_counters::ops_rebuilt(), 0);
    late.verify_hashes().unwrap();
    for d in [&mut late, &mut full] {
        for i in 0..300 {
            d.put(&ROOT, "after", i).unwrap();
            commit(d);
        }
    }
    assert_eq!(late.get_heads(), full.get_heads());
    assert_eq!(late.get_changes(&[]), full.get_changes(&[]));
    let fragments = full.fragments(..);
    assert!(fragments.iter().any(|f| f.level > 0));
    assert_eq!(late.fragments(..), fragments);
    for f in &fragments {
        assert_eq!(late.get_fragment(f.head), full.get_fragment(f.head));
    }

    let mut peer = AutoCommit::new();
    let (mut ours, mut theirs) = (sync::State::new(), sync::State::new());
    for _ in 0..10 {
        if let Some(m) = late.sync().generate_sync_message(&mut ours) {
            peer.sync().receive_sync_message(&mut theirs, m).unwrap();
        }
        if let Some(m) = peer.sync().generate_sync_message(&mut theirs) {
            late.sync().receive_sync_message(&mut ours, m).unwrap();
        }
    }
    assert_eq!(peer.get_heads(), full.get_heads());
    assert_eq!(peer.save(), full.save());
}
