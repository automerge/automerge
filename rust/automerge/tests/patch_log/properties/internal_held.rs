//! Property test for the patch-stream contract for internally held
//! [`PatchLog`]s.
//!
//! A materialized view maintained by only applying the patches emitted by
//! [`Automerge::diff_incremental`] must always equl the document's actual
//! state. In other words, the set of patches should never drift, e.g.,
//! duplicated patch, missing patch.
//! The Javascript wrapper relies on this contract, and it is the contract that
//! is being violated by a bug class witnessed with stale actor indices.
//!
//! Operations are generated with raw values and clamped/interpreted at
//! execution time so that every generated sequence is valid (this also gives
//! good shrinking behaviour).

use automerge::{transaction::Transactable, AutoCommit, ObjType, ReadDoc, ROOT};
use proptest::prelude::*;

use crate::properties::{actor, apply_patches, gen_text};

#[derive(Debug, Clone)]
enum Op {
    /// Splice into the text object in a normal transaction.
    Splice {
        pos: usize,
        del: usize,
        text: String,
    },
    /// Change the actor used for subsequent transactions.
    SetActor {
        /// Determines where the new actor sorts relative to others.
        sort_byte: u8,
    },

    /// Simulates the `changeAt` pattern in JS, which is composed of:
    ///   1. Isolate at some earlier heads,
    ///   2. Splice,
    ///   3. And integrate.
    ChangeAt {
        heads_idx: usize,
        pos: usize,
        text: String,
    },
    /// Simulates the `clone` pattern in JS, which is composed of:
    ///   1. Fork with a fresh actor,
    ///   2. Abandon the original,
    ///   3. And, re-materialize the view.
    Fork {
        /// Determines where the new actor sorts relative to others.
        sort_byte: u8,
    },
    /// Fork off a concurrent peer document with a fresh actor.
    SpawnPeer {
        /// Determines where the new actor sorts relative to others.
        sort_byte: u8,
    },
    /// A peer makes a concurrent edit (not yet visible to the main doc).
    PeerSplice {
        peer: usize,
        pos: usize,
        text: String,
    },
    /// Merge a peer's changes into the main doc, exercising the batch apply
    /// path.
    MergePeer { peer: usize },
    /// Apply a peer's changes via:
    ///   1. [`Autocommit::save`],
    ///   2. And [`Autocommit::load_incremental`].
    LoadIncrementalPeer { peer: usize },
    /// Save the doc and reload it from bytes, re-materializing the view.
    SaveLoad {
        /// Determines where the new actor sorts relative to others.
        sort_byte: u8,
    },
    /// Drain patches and check the view against the document.
    Sync,
}

fn gen_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any::<usize>(), any::<usize>(), gen_text())
            .prop_map(|(pos, del, text)| Op::Splice { pos, del, text }),
        1 => any::<u8>().prop_map(|sort_byte| Op::SetActor { sort_byte }),
        3 => (any::<usize>(), any::<usize>(), gen_text())
            .prop_map(|(heads_idx, pos, text)| Op::ChangeAt { heads_idx, pos, text }),
        2 => any::<u8>().prop_map(|sort_byte| Op::Fork { sort_byte }),
        2 => any::<u8>().prop_map(|sort_byte| Op::SpawnPeer { sort_byte }),
        3 => (any::<usize>(), any::<usize>(), gen_text())
            .prop_map(|(peer, pos, text)| Op::PeerSplice { peer, pos, text }),
        2 => any::<usize>().prop_map(|peer| Op::MergePeer { peer }),
        1 => any::<usize>().prop_map(|peer| Op::LoadIncrementalPeer { peer }),
        1 => any::<u8>().prop_map(|sort_byte| Op::SaveLoad { sort_byte }),
        3 => Just(Op::Sync),
    ]
}

fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut uniq = 0u32;

    // Set up: a document with a text object, then activate the patch log the
    // way the JS wrapper does.
    let mut doc = AutoCommit::new().with_actor(actor(128, &mut uniq));
    let txt = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.update_diff_cursor();
    let mut model = doc.text(&txt).unwrap();

    // Heads history for ChangeAt targets; concurrent peers for merge ops.
    let mut history = vec![doc.get_heads()];
    let mut peers: Vec<AutoCommit> = Vec::new();

    for op in ops {
        match op {
            Op::Splice { pos, del, text } => {
                let cur = doc.text(&txt).unwrap();
                let len = cur.chars().count();
                let pos = pos % (len + 1);
                let del = del % (len - pos + 1);
                doc.splice_text(&txt, pos, del as isize, &text).unwrap();
                history.push(doc.get_heads());
            }
            Op::SetActor { sort_byte } => {
                let a = actor(sort_byte, &mut uniq);
                doc.set_actor(a);
            }
            Op::ChangeAt {
                heads_idx,
                pos,
                text,
            } => {
                let heads = history[heads_idx % history.len()].clone();
                doc.isolate(&heads);
                // Reads are scoped to the isolation point.
                let cur = doc.text(&txt).unwrap();
                let len = cur.chars().count();
                let pos = pos % (len + 1);
                doc.splice_text(&txt, pos, 0, &text).unwrap();
                doc.integrate();
                history.push(doc.get_heads());
            }
            Op::Fork { sort_byte } => {
                let a = actor(sort_byte, &mut uniq);
                doc = doc.fork().with_actor(a);
                // As the JS wrapper does after clone(): fresh cursor, fresh
                // materialized view.
                doc.update_diff_cursor();
                model = doc.text(&txt).unwrap();
            }
            Op::SpawnPeer { sort_byte } => {
                let a = actor(sort_byte, &mut uniq);
                peers.push(doc.fork().with_actor(a));
            }
            Op::PeerSplice { peer, pos, text } => {
                if peers.is_empty() {
                    continue;
                }
                let n = peers.len();
                let peer = &mut peers[peer % n];
                let cur = peer.text(&txt).unwrap();
                let len = cur.chars().count();
                let pos = pos % (len + 1);
                peer.splice_text(&txt, pos, 0, &text).unwrap();
            }
            Op::MergePeer { peer } => {
                if peers.is_empty() {
                    continue;
                }
                let n = peers.len();
                doc.merge(&mut peers[peer % n]).unwrap();
                history.push(doc.get_heads());
            }
            Op::LoadIncrementalPeer { peer } => {
                if peers.is_empty() {
                    continue;
                }
                let n = peers.len();
                let bytes = peers[peer % n].save();
                doc.load_incremental(&bytes).unwrap();
                history.push(doc.get_heads());
            }
            Op::SaveLoad { sort_byte } => {
                let bytes = doc.save();
                doc = AutoCommit::load(&bytes).unwrap();
                doc.set_actor(actor(sort_byte, &mut uniq));
                // Fresh document, fresh cursor, fresh view.
                doc.update_diff_cursor();
                model = doc.text(&txt).unwrap();
            }
            Op::Sync => {
                let patches = doc.diff_incremental();
                apply_patches(&mut model, &patches);
                let actual = doc.text(&txt).unwrap();
                prop_assert_eq!(
                    &model,
                    &actual,
                    "view diverged from document after applying patches: {:#?}",
                    patches
                );
            }
        }
    }

    // Final drain and check.
    let patches = doc.diff_incremental();
    apply_patches(&mut model, &patches);
    let actual = doc.text(&txt).unwrap();
    prop_assert_eq!(
        &model,
        &actual,
        "view diverged from document at end of scenario: {:#?}",
        patches
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn document_and_patches_parity(
        ops in proptest::collection::vec(gen_op(), 1..40)
    ) {
        run(ops)?;
    }
}
