//! Regression test for <https://github.com/automerge/automerge/issues/697>.
//!
//! Documents written by pre-1.0 JS automerge can contain stored heads which
//! don't match the change hashes recomputed at load time, so `Automerge::load`
//! fails with "mismatching heads". The documented escape hatch for such
//! documents is `Automerge::load_unverified_heads`, which succeeds — but
//! calling `save()` on the resulting document panics in
//! `ChangeGraph::head_indexes`, because the change graph keeps the *stored*
//! head hashes while its node index is keyed by the *recomputed* hashes.

use automerge::{transaction::Transactable, Automerge, ReadDoc, ROOT};
use sha2::{Digest, Sha256};

/// Produce document bytes whose stored heads don't match the hashes of the
/// contained changes, mimicking a document written by an old JS automerge.
///
/// We corrupt one byte of the head hash stored in the document chunk and then
/// fix up the chunk checksum so the bytes still parse.
fn doc_with_mismatching_heads() -> Vec<u8> {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "key", "value").unwrap();
    tx.commit();

    let mut saved = doc.save();
    let heads = doc.get_heads();
    assert_eq!(heads.len(), 1);

    // Locate the stored head hash inside the document chunk and flip a byte.
    let head_bytes: &[u8] = heads[0].as_ref();
    let pos = saved
        .windows(head_bytes.len())
        .position(|w| w == head_bytes)
        .expect("saved document should contain the head hash");
    saved[pos] ^= 0xff;

    // Fix up the chunk checksum: the first 4 bytes of
    // sha256(chunk_type ++ uleb(data_len) ++ data), i.e. everything after
    // magic (4 bytes) + checksum (4 bytes).
    let digest = Sha256::digest(&saved[8..]);
    saved[4..8].copy_from_slice(&digest[..4]);

    saved
}

#[test]
fn load_rejects_mismatching_heads() {
    // Sanity check that the fixture reproduces the state reported in
    // issue #697: a default `load` fails with "mismatching heads".
    let data = doc_with_mismatching_heads();
    let err = Automerge::load(&data).unwrap_err();
    assert!(
        err.to_string().contains("mismatching heads"),
        "expected 'mismatching heads' error, got: {}",
        err
    );
}

#[test]
fn save_after_load_unverified_heads_does_not_panic() {
    let data = doc_with_mismatching_heads();

    let doc = Automerge::load_unverified_heads(&data).unwrap();
    assert_eq!(doc.keys(ROOT).count(), 1);

    // This currently panics with `called \`Option::unwrap()\` on a \`None\`
    // value` in `ChangeGraph::head_indexes` (change_graph.rs), leaving no
    // panic-free way to migrate such a document to a fresh, valid save.
    let resaved = doc.save();

    // Once saving works, the round-tripped document should load normally.
    let reloaded = Automerge::load(&resaved).unwrap();
    assert_eq!(reloaded.keys(ROOT).count(), 1);
}
