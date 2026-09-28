//! Patch/hydrate parity under write-frontier.
//!
//! Two peers commit map/scalar/counter operations; the main document merges
//! them under generated interleavings of mask_author (including pending, unseen
//! boundaries), reveal_author, isolate and integrate. A model maintained ONLY
//! from `diff_incremental()` patches must always equal the hydrated
//! document at the currently observed heads.

use automerge::{
    hydrate,
    marks::{ExpandMark, Mark as TextMark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, ChangeHash, ObjType, ReadDoc, ScalarValue,
    TextEncoding, Value, ROOT,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

#[path = "support/rich_model.rs"]
mod rich_model;
use rich_model::RichText;

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

#[derive(Debug, Clone)]
enum Op {
    PeerPut {
        peer: usize,
        key: u8,
        val: i64,
    },
    PeerNested {
        peer: usize,
        key: u8,
        child: u8,
        val: i64,
    },
    PeerCounter {
        peer: usize,
        key: u8,
        delta: i64,
    },
    PeerDelete {
        peer: usize,
        key: u8,
    },
    Merge {
        peer: usize,
    },
    /// `at_history % (history.len() + 1) == history.len()` selects an unseen
    /// head: the peer commits a change the main doc has not merged and the
    /// write-frontier is registered at that hash (pending until a later merge).
    MaskAuthor {
        peer: usize,
        at_history: usize,
    },
    RevealAuthor {
        peer: usize,
    },
    Isolate {
        at_history: usize,
    },
    Integrate,
    Sync,
    /// A local edit by the main doc's own author. Generated regardless of
    /// whether that author is masked: when it is, the write is recorded
    /// but masked (no patches, no view change); when it is not, it is an
    /// ordinary visible edit.
    LocalEdit {
        key: u8,
        val: i64,
    },
    /// Nested-map variant of [`Op::LocalEdit`].
    LocalNested {
        key: u8,
        child: u8,
        val: i64,
    },
    /// Mask the main doc's own author at a previously held head set.
    MaskAuthorLocal {
        at_history: usize,
    },
    RevealAuthorLocal,
}

fn gen_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..2usize, any::<u8>(), any::<i64>())
            .prop_map(|(peer, key, val)| Op::PeerPut { peer, key, val }),
        2 => (0..2usize, any::<u8>(), any::<u8>(), any::<i64>())
            .prop_map(|(peer, key, child, val)| Op::PeerNested { peer, key, child, val }),
        2 => (0..2usize, any::<u8>(), -100..100i64)
            .prop_map(|(peer, key, delta)| Op::PeerCounter { peer, key, delta }),
        2 => (0..2usize, any::<u8>())
            .prop_map(|(peer, key)| Op::PeerDelete { peer, key }),
        3 => (0..2usize).prop_map(|peer| Op::Merge { peer }),
        2 => (0..2usize, any::<usize>())
            .prop_map(|(peer, at_history)| Op::MaskAuthor { peer, at_history }),
        1 => (0..2usize).prop_map(|peer| Op::RevealAuthor { peer }),
        1 => any::<usize>().prop_map(|at_history| Op::Isolate { at_history }),
        1 => Just(Op::Integrate),
        3 => Just(Op::Sync),
        2 => (any::<u8>(), any::<i64>())
            .prop_map(|(key, val)| Op::LocalEdit { key, val }),
        1 => (any::<u8>(), any::<u8>(), any::<i64>())
            .prop_map(|(key, child, val)| Op::LocalNested { key, child, val }),
        2 => any::<usize>().prop_map(|at_history| Op::MaskAuthorLocal { at_history }),
        1 => Just(Op::RevealAuthorLocal),
    ]
}

/// A small key space so that generated operations collide across peers.
fn key_name(key: u8) -> String {
    format!("k{}", key % 5)
}

fn child_name(child: u8) -> String {
    format!("c{}", child % 3)
}

struct Runner {
    main: AutoCommit,
    peers: [AutoCommit; 2],
    authors: [Author<'static>; 2],
    /// The main doc's own author, target of `MaskAuthorLocal`/`LocalEdit`.
    local_author: Author<'static>,
    /// Peer head snapshots, recorded after every peer commit. Entries may be
    /// unseen by the main doc, so masking at them exercises pending
    /// boundaries resolved by later merges.
    history: Vec<Vec<ChangeHash>>,
    /// Head snapshots the main doc has actually held; isolation targets.
    main_history: Vec<Vec<ChangeHash>>,
    /// Heads the patch log currently observes, if isolated.
    isolated: Option<Vec<ChangeHash>>,
    /// Model updated only from `diff_incremental()` patches.
    model: hydrate::Value,
    pending: u32,
}

impl Runner {
    fn new() -> Self {
        let alice = Author::try_from("aaaa").unwrap();
        let bob = Author::try_from("bbbb").unwrap();
        let peer = |author: &Author<'static>, actor: u8| {
            AutoCommit::new_with_encoding(ENCODING)
                .with_author(Some(author.clone()))
                .with_actor(ActorId::from(vec![actor]))
        };
        let carol = Author::try_from("cccc").unwrap();
        // Set the author before the actor: `set_author` regenerates the
        // actor id.
        let mut main = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(carol.clone()))
            .with_actor(ActorId::from(vec![0x01]));
        let model = main.hydrate(ROOT, None).unwrap();
        main.update_diff_cursor();
        Runner {
            main,
            peers: [peer(&alice, 0xA0), peer(&bob, 0xB0)],
            authors: [alice, bob],
            local_author: carol,
            history: Vec::new(),
            main_history: vec![Vec::new()],
            isolated: None,
            model,
            pending: 0,
        }
    }

    fn record_peer_heads(&mut self, peer: usize) {
        let heads = self.peers[peer].get_heads();
        self.history.push(heads);
    }

    fn step(&mut self, op: Op) -> Result<(), TestCaseError> {
        match op {
            Op::PeerPut { peer, key, val } => {
                self.peers[peer].put(ROOT, key_name(key), val).unwrap();
                self.peers[peer].commit();
                self.record_peer_heads(peer);
            }
            Op::PeerNested {
                peer,
                key,
                child,
                val,
            } => {
                let doc = &mut self.peers[peer];
                let key = key_name(key);
                let obj = match doc.get(ROOT, &key).unwrap() {
                    Some((Value::Object(ObjType::Map), id)) => id,
                    _ => doc.put_object(ROOT, &key, ObjType::Map).unwrap(),
                };
                doc.put(&obj, child_name(child), val).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::PeerCounter { peer, key, delta } => {
                let doc = &mut self.peers[peer];
                let key = key_name(key);
                let counter = matches!(
                    doc.get(ROOT, &key).unwrap(),
                    Some((Value::Scalar(ref s), _)) if matches!(s.as_ref(), ScalarValue::Counter(_))
                );
                if counter {
                    doc.increment(ROOT, &key, delta).unwrap();
                } else {
                    doc.put(ROOT, &key, ScalarValue::counter(delta)).unwrap();
                }
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::PeerDelete { peer, key } => {
                let doc = &mut self.peers[peer];
                let key = key_name(key);
                if doc.get(ROOT, &key).unwrap().is_some() {
                    doc.delete(ROOT, &key).unwrap();
                    doc.commit();
                    self.record_peer_heads(peer);
                }
            }
            Op::Merge { peer } => {
                self.main.merge(&mut self.peers[peer]).unwrap();
                let heads = self.main.document().get_heads();
                self.main_history.push(heads);
            }
            Op::MaskAuthor { peer, at_history } => {
                let idx = at_history % (self.history.len() + 1);
                let heads = if idx == self.history.len() {
                    // Unseen boundary: the peer commits a change the main doc
                    // has not merged; the write-frontier stays pending until a
                    // later merge imports this hash.
                    self.pending += 1;
                    let marker = format!("pending{}", self.pending);
                    self.peers[peer].put(ROOT, marker, true).unwrap();
                    self.peers[peer].commit();
                    self.record_peer_heads(peer);
                    self.history.last().unwrap().clone()
                } else {
                    self.history[idx].clone()
                };
                self.main.mask_author(self.authors[peer].clone(), &heads);
            }
            Op::RevealAuthor { peer } => {
                let author = self.authors[peer].clone();
                self.main.reveal_author(&author);
            }
            Op::Isolate { at_history } => {
                let idx = at_history % self.main_history.len();
                let heads = self.main_history[idx].clone();
                self.main.isolate(&heads);
                self.isolated = Some(heads);
            }
            Op::Integrate => {
                self.main.integrate();
                self.isolated = None;
            }
            Op::Sync => self.sync()?,
            Op::LocalEdit { key, val } => {
                self.end_isolation();
                self.main.put(ROOT, key_name(key), val).unwrap();
                self.record_main_heads();
            }
            Op::LocalNested { key, child, val } => {
                self.end_isolation();
                let key = key_name(key);
                let obj = match self.main.get(ROOT, &key).unwrap() {
                    Some((Value::Object(ObjType::Map), id)) => id,
                    _ => self.main.put_object(ROOT, &key, ObjType::Map).unwrap(),
                };
                self.main.put(&obj, child_name(child), val).unwrap();
                self.record_main_heads();
            }
            Op::MaskAuthorLocal { at_history } => {
                let idx = at_history % self.main_history.len();
                let heads = self.main_history[idx].clone();
                self.main.mask_author(self.local_author.clone(), &heads);
            }
            Op::RevealAuthorLocal => {
                let author = self.local_author.clone();
                self.main.reveal_author(&author);
            }
        }
        Ok(())
    }

    /// Local edits during isolation are visible inside the isolation scope
    /// (the session's own actor), which `hydrate(Some(heads))` cannot
    /// express; integrate first so the model's oracle stays applicable.
    fn end_isolation(&mut self) {
        if self.isolated.take().is_some() {
            self.main.integrate();
        }
    }

    /// Record the main doc's heads after a local commit; isolation targets.
    fn record_main_heads(&mut self) {
        let heads = self.main.get_heads();
        self.main_history.push(heads);
    }

    fn sync(&mut self) -> Result<(), TestCaseError> {
        let patches = self.main.diff_incremental();
        self.model
            .apply_patches(ENCODING, patches.clone())
            .map_err(|e| {
                TestCaseError::fail(format!("model rejected patches: {e} ({patches:?})"))
            })?;
        let expected = match &self.isolated {
            Some(heads) => self.main.hydrate(ROOT, Some(heads)).unwrap(),
            None => self.main.hydrate(ROOT, None).unwrap(),
        };
        prop_assert_eq!(&self.model, &expected, "patches: {:?}", patches);
        // Reload parity against an independent materialisation: a fresh
        // `Automerge::load` of the saved bytes under the same write_frontier
        // policy re-materializes through `from_parts` + index recomputation.
        // (Comparing `hydrate(Some(current_heads))` was vacuous: `read_at`
        // maps the current heads back to `ReadAt::Current`, so both sides
        // shared the indexed fast path.) The reload oracle shares the
        // positional *read* path with the live document: whole-result
        // fast/slow comparisons run only under
        // `--features slow_path_assertions` (CI: `scripts/ci/build-test`).
        if self.isolated.is_none() {
            let reload = Automerge::load(&self.main.save())
                .expect("saved document reloads")
                .with_write_frontier(self.main.document().get_write_frontier());
            let slow = reload.hydrate(None);
            prop_assert_eq!(&expected, &slow, "live doc disagrees with reload oracle");
        }
        Ok(())
    }
}

fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut runner = Runner::new();
    for op in ops {
        runner.step(op)?;
    }
    runner.sync()
}

/// Shrunk from `patches_replay_to_hydrate`: bob overwrites his own `k3` put,
/// and is then masked (pending) at the heads between the two writes. The
/// overwrite is masked, so the earlier put stays visible and conflicts with
/// alice's concurrent `k3` map. The batch walk used the raw successor list to
/// judge doc-op visibility, dropping bob's put from both candidate summaries
/// and emitting alice's winning patch with `conflict: false`.
#[test]
fn regression_masked_overwrite_keeps_doc_op_a_conflict_candidate() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(alice))
        .with_actor(ActorId::from(vec![0xA0]));
    let child = a.put_object(ROOT, "k3", ObjType::Map).unwrap();
    a.put(&child, "c0", 0).unwrap();
    a.commit();
    let mut b = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(bob.clone()))
        .with_actor(ActorId::from(vec![0xB0]));
    b.put(ROOT, "k3", 0).unwrap();
    b.commit();
    let boundary = b.get_heads();
    b.put(ROOT, "k3", 1).unwrap();
    b.commit();

    let mut main = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
    main.mask_author(bob, &boundary); // pending until bob's changes arrive
    let mut model = main.hydrate(ROOT, None).unwrap();
    main.update_diff_cursor();
    main.merge(&mut b).unwrap();
    model
        .apply_patches(ENCODING, main.diff_incremental())
        .unwrap();
    assert_eq!(model, main.hydrate(ROOT, None).unwrap());
    main.merge(&mut a).unwrap();
    model
        .apply_patches(ENCODING, main.diff_incremental())
        .unwrap();
    assert_eq!(model, main.hydrate(ROOT, None).unwrap());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..Default::default() })]
    #[test]
    fn patches_replay_to_hydrate(ops in proptest::collection::vec(gen_op(), 0..40)) {
        run(ops)?;
    }
}

/// Distilled from `regression_shrunk_reveal_sequence_replays`: masked text
/// revealed under a mark that stays visible across the transition must
/// carry that mark in its `SpliceText` patch. The spans-diff state machine
/// compared active marks by their before/after *delta* when deciding
/// whether a new text run starts, so an unchanged mark (empty delta) merged
/// newly visible marked text into the surrounding unmarked run. This clock
/// shape — a mark visible while the text it covers is hidden — only arises
/// under masks; causally a mark always follows the text it marks.
#[test]
fn regression_reveal_under_visible_mark_carries_the_mark() {
    let alice = Author::try_from("aaaa").unwrap();
    let carol = Author::try_from("cccc").unwrap();
    let mut main = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(carol.clone()))
        .with_actor(ActorId::from(vec![0x01]));
    let text = main.put_object(ROOT, "text", ObjType::Text).unwrap();
    main.commit();
    let boundary = main.get_heads();
    main.splice_text(&text, 0, 0, "aaa").unwrap();
    main.commit();
    // alice marks the middle character; her mark is never masked.
    let mut peer = main
        .fork()
        .with_author(Some(alice.clone()))
        .with_actor(ActorId::from(vec![0xA0]));
    peer.mark(
        &text,
        TextMark::new("bold".into(), 0i64, 1, 2),
        ExpandMark::None,
    )
    .unwrap();
    peer.commit();
    main.merge(&mut peer).unwrap();
    // Mask carol's text; the mark stays visible over hidden chars.
    main.mask_author(carol.clone(), &boundary);
    assert_eq!(main.text(&text).unwrap(), "");
    main.update_diff_cursor();
    let mut rich = RichText::from_doc(main.document(), &text);
    main.reveal_author(&carol);
    for patch in main.diff_incremental() {
        assert_eq!(patch.obj, text, "only the text object changes");
        rich.apply(&patch);
    }
    assert_eq!(
        rich,
        RichText::from_doc(main.document(), &text),
        "patch-replayed rich text disagrees with spans() after the reveal"
    );
}
