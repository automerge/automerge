//! Patch/hydrate parity under write-frontier.
//!
//! Two peers (forked from a shared init commit that creates a text and a
//! list object) commit map/scalar/counter/list/text/mark/block operations;
//! the main document merges them under generated interleavings of mask_author
//! (including pending, unseen boundaries), reveal_author, isolate, integrate,
//! batch delivery, bulk policy replacement and save/load round trips. A
//! model maintained ONLY from `diff_incremental()` patches must always
//! equal the hydrated document at the currently observed heads, and after
//! every op the document must agree with a fresh reload under the same
//! policy (oracle 2) and with a forced slow walk (oracle 3). Text is
//! additionally compared through the rich-text model (oracle 4), because
//! `hydrate::Value` cannot hold marks.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use automerge::{
    hydrate,
    marks::{ExpandMark, Mark as TextMark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, ChangeHash, LoadOptions, ObjId, ObjType, Patch,
    PatchAction, PatchLog, ReadDoc, ScalarValue, TextEncoding, Value, ROOT,
};
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};

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
    /// Insert into the shared list object.
    InsertList {
        peer: usize,
        pos: usize,
        val: i64,
    },
    /// Delete from the shared list object.
    DeleteList {
        peer: usize,
        pos: usize,
    },
    /// Splice the shared text object.
    SpliceText {
        peer: usize,
        pos: usize,
        del: usize,
        text: String,
    },
    /// Mark a range of the shared text object.
    Mark {
        peer: usize,
        start: usize,
        len: usize,
        name: u8,
        val: u8,
        expand: u8,
    },
    /// Insert a block marker into the shared text object.
    SplitBlock {
        peer: usize,
        pos: usize,
    },
    Merge {
        peer: usize,
    },
    /// Real change exchange between documents other than into main
    /// (main-bound delivery is [`Op::Merge`]): peer↔peer and main→peer.
    SyncChanges {
        pair: u8,
    },
    /// A boundary change plus the author's pre/post changes delivered as
    /// one `apply_changes` call, in generated order: the peer commits a
    /// pre-boundary change, the write-frontier is registered at the heads
    /// between the two commits (pending), and both changes arrive together.
    BatchBoundary {
        peer: usize,
        key: u8,
        val: i64,
        swap: bool,
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
    /// Replace the whole write-frontier policy at once. Verified against a twin
    /// document driven through `set_write_frontier_log_patches` (one logged
    /// transition replays to hydrate) and a fresh load under the new policy;
    /// the main doc is then moved to the same policy through individual
    /// mask_author/reveal_author transitions, which must land on the same view.
    BulkReplacePolicy {
        alice_at: Option<usize>,
        bob_at: Option<usize>,
        carol_at: Option<usize>,
    },
    Isolate {
        at_history: usize,
    },
    Integrate,
    /// Drain accumulated patches into the model and check oracles 1 and 4.
    Drain,
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
    /// A local text splice by the main doc's own author; positions refer to
    /// the visible document, so when the author is masked this exercises
    /// the masked positional-edit path.
    LocalMaskedSplice {
        pos: usize,
        del: usize,
        text: String,
    },
    /// A local edit committed while isolated, without integrating first:
    /// covers `isolate_actor`, including its masked branch when the local
    /// author is masked. Isolates first when not already isolated.
    EditDuringIsolation {
        key: u8,
        val: i64,
        at_history: usize,
    },
    /// Replace the main doc with a fresh load of its saved bytes under the
    /// same policy (the document-chunk `LoadOptions::write_frontier` arm).
    SaveLoadRoundTrip,
    /// Mask the main doc's own author at a previously held head set.
    MaskAuthorLocal {
        at_history: usize,
    },
    RevealAuthorLocal,
}

impl Op {
    fn name(&self) -> &'static str {
        match self {
            Op::PeerPut { .. } => "PeerPut",
            Op::PeerNested { .. } => "PeerNested",
            Op::PeerCounter { .. } => "PeerCounter",
            Op::PeerDelete { .. } => "PeerDelete",
            Op::InsertList { .. } => "InsertList",
            Op::DeleteList { .. } => "DeleteList",
            Op::SpliceText { .. } => "SpliceText",
            Op::Mark { .. } => "Mark",
            Op::SplitBlock { .. } => "SplitBlock",
            Op::Merge { .. } => "Merge",
            Op::SyncChanges { .. } => "SyncChanges",
            Op::BatchBoundary { .. } => "BatchBoundary",
            Op::MaskAuthor { .. } => "MaskAuthor",
            Op::RevealAuthor { .. } => "RevealAuthor",
            Op::BulkReplacePolicy { .. } => "BulkReplacePolicy",
            Op::Isolate { .. } => "Isolate",
            Op::Integrate => "Integrate",
            Op::Drain => "Drain",
            Op::LocalEdit { .. } => "LocalEdit",
            Op::LocalNested { .. } => "LocalNested",
            Op::LocalMaskedSplice { .. } => "LocalMaskedSplice",
            Op::EditDuringIsolation { .. } => "EditDuringIsolation",
            Op::SaveLoadRoundTrip => "SaveLoadRoundTrip",
            Op::MaskAuthorLocal { .. } => "MaskAuthorLocal",
            Op::RevealAuthorLocal => "RevealAuthorLocal",
        }
    }
}

/// Every counter that must be exercised over a whole proptest run. Ops that
/// reduce to a no-op against the live state (e.g. `DeleteList` on an empty
/// list) are not counted, so a `> 0` count proves the scenario was reached.
const COVERAGE: &[&str] = &[
    "PeerPut",
    "PeerNested",
    "PeerCounter",
    "PeerDelete",
    "InsertList",
    "DeleteList",
    "SpliceText",
    "Mark",
    "SplitBlock",
    "Merge",
    "SyncChanges",
    "BatchBoundary",
    "MaskAuthor",
    "RevealAuthor",
    "BulkReplacePolicy",
    "Isolate",
    "Integrate",
    "Drain",
    "LocalEdit",
    "LocalNested",
    "LocalMaskedSplice",
    "LocalMaskedSplice(masked)",
    "EditDuringIsolation",
    "EditDuringIsolation(masked)",
    "SaveLoadRoundTrip",
    "MaskAuthorLocal",
    "RevealAuthorLocal",
];

fn gen_text() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::char::range('a', 'z'), 0..4)
        .prop_map(|chars| chars.into_iter().collect())
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
        2 => (0..2usize, any::<usize>(), any::<i64>())
            .prop_map(|(peer, pos, val)| Op::InsertList { peer, pos, val }),
        2 => (0..2usize, any::<usize>())
            .prop_map(|(peer, pos)| Op::DeleteList { peer, pos }),
        3 => (0..2usize, any::<usize>(), any::<usize>(), gen_text())
            .prop_map(|(peer, pos, del, text)| Op::SpliceText { peer, pos, del, text }),
        2 => (0..2usize, any::<usize>(), any::<usize>(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(peer, start, len, name, val, expand)| Op::Mark {
                peer, start, len, name, val, expand
            }),
        1 => (0..2usize, any::<usize>())
            .prop_map(|(peer, pos)| Op::SplitBlock { peer, pos }),
        3 => (0..2usize).prop_map(|peer| Op::Merge { peer }),
        2 => any::<u8>().prop_map(|pair| Op::SyncChanges { pair }),
        2 => (0..2usize, any::<u8>(), any::<i64>(), any::<bool>())
            .prop_map(|(peer, key, val, swap)| Op::BatchBoundary { peer, key, val, swap }),
        2 => (0..2usize, any::<usize>())
            .prop_map(|(peer, at_history)| Op::MaskAuthor { peer, at_history }),
        1 => (0..2usize).prop_map(|peer| Op::RevealAuthor { peer }),
        1 => (
            proptest::option::of(any::<usize>()),
            proptest::option::of(any::<usize>()),
            proptest::option::of(any::<usize>()),
        )
            .prop_map(|(alice_at, bob_at, carol_at)| Op::BulkReplacePolicy {
                alice_at, bob_at, carol_at
            }),
        1 => any::<usize>().prop_map(|at_history| Op::Isolate { at_history }),
        1 => Just(Op::Integrate),
        3 => Just(Op::Drain),
        2 => (any::<u8>(), any::<i64>())
            .prop_map(|(key, val)| Op::LocalEdit { key, val }),
        1 => (any::<u8>(), any::<u8>(), any::<i64>())
            .prop_map(|(key, child, val)| Op::LocalNested { key, child, val }),
        2 => (any::<usize>(), any::<usize>(), gen_text())
            .prop_map(|(pos, del, text)| Op::LocalMaskedSplice { pos, del, text }),
        2 => (any::<u8>(), any::<i64>(), any::<usize>())
            .prop_map(|(key, val, at_history)| Op::EditDuringIsolation { key, val, at_history }),
        1 => Just(Op::SaveLoadRoundTrip),
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

fn mark_name(name: u8) -> &'static str {
    if name.is_multiple_of(2) {
        "bold"
    } else {
        "em"
    }
}

fn expand_mark(expand: u8) -> ExpandMark {
    match expand % 4 {
        0 => ExpandMark::None,
        1 => ExpandMark::Before,
        2 => ExpandMark::After,
        _ => ExpandMark::Both,
    }
}

struct Runner {
    main: AutoCommit,
    peers: [AutoCommit; 2],
    authors: [Author<'static>; 2],
    /// The main doc's own author, target of `MaskAuthorLocal`/`LocalEdit`.
    local_author: Author<'static>,
    /// The shared text object, created by the init commit.
    text: ObjId,
    /// The shared list object, created by the init commit.
    list: ObjId,
    /// Peer head snapshots, recorded after every peer commit. Entries may be
    /// unseen by the main doc, so masking at them exercises pending
    /// boundaries resolved by later merges.
    history: Vec<Vec<ChangeHash>>,
    /// Head snapshots the main doc has actually held; isolation targets.
    main_history: Vec<Vec<ChangeHash>>,
    /// Heads the patch log currently observes, if isolated. Local commits
    /// made during isolation extend this set (their deps are the isolation
    /// heads), keeping `hydrate(Some(..))` applicable as the oracle.
    isolated: Option<Vec<ChangeHash>>,
    /// Whether `isolated` has been rewritten to isolated commit hashes.
    isolated_edits: bool,
    /// Model updated only from `diff_incremental()` patches.
    model: hydrate::Value,
    /// Rich-text model of the shared text object, updated from the same
    /// patches; unlike `hydrate::Value` it holds marks and block markers.
    rich: RichText,
    pending: u32,
    /// Per-scenario coverage counts, merged across cases by the harness.
    cov: BTreeMap<&'static str, u64>,
}

impl Runner {
    fn new() -> Self {
        let alice = Author::try_from("aaaa").unwrap();
        let bob = Author::try_from("bbbb").unwrap();
        let carol = Author::try_from("cccc").unwrap();
        // Set the author before the actor: `set_author` regenerates the
        // actor id.
        let mut main = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(carol.clone()))
            .with_actor(ActorId::from(vec![0x01]));
        // A shared init commit creates the sequence objects, so peer and
        // local write-frontier boundaries (all at or after these heads) never
        // hide the containers themselves — only their contents.
        let text = main.put_object(ROOT, "text", ObjType::Text).unwrap();
        let list = main.put_object(ROOT, "list", ObjType::List).unwrap();
        main.commit();
        let init_heads = main.get_heads();
        let peer = |main: &mut AutoCommit, author: &Author<'static>, actor: u8| {
            main.fork()
                .with_author(Some(author.clone()))
                .with_actor(ActorId::from(vec![actor]))
        };
        let peers = [peer(&mut main, &alice, 0xA0), peer(&mut main, &bob, 0xB0)];
        let model = main.hydrate(ROOT, None).unwrap();
        let rich = RichText::from_doc(main.document(), &text);
        main.update_diff_cursor();
        Runner {
            main,
            peers,
            authors: [alice, bob],
            local_author: carol,
            text,
            list,
            history: vec![init_heads.clone()],
            main_history: vec![init_heads],
            isolated: None,
            isolated_edits: false,
            model,
            rich,
            pending: 0,
            cov: BTreeMap::new(),
        }
    }

    fn hit(&mut self, name: &'static str) {
        *self.cov.entry(name).or_insert(0) += 1;
    }

    fn record_peer_heads(&mut self, peer: usize) {
        let heads = self.peers[peer].get_heads();
        self.history.push(heads);
    }

    fn step(&mut self, op: Op) -> Result<(), TestCaseError> {
        let name = op.name();
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
                if doc.get(ROOT, &key).unwrap().is_none() {
                    return Ok(());
                }
                doc.delete(ROOT, &key).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::InsertList { peer, pos, val } => {
                let list = self.list.clone();
                let doc = &mut self.peers[peer];
                let pos = pos % (doc.length(&list) + 1);
                doc.insert(&list, pos, val).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::DeleteList { peer, pos } => {
                let list = self.list.clone();
                let doc = &mut self.peers[peer];
                let len = doc.length(&list);
                if len == 0 {
                    return Ok(());
                }
                doc.delete(&list, pos % len).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::SpliceText {
                peer,
                pos,
                del,
                text,
            } => {
                let obj = self.text.clone();
                let doc = &mut self.peers[peer];
                let len = doc.length(&obj);
                let pos = pos % (len + 1);
                let del = del % (len - pos + 1);
                if del == 0 && text.is_empty() {
                    return Ok(());
                }
                doc.splice_text(&obj, pos, del as isize, &text).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::Mark {
                peer,
                start,
                len,
                name: mark,
                val,
                expand,
            } => {
                let obj = self.text.clone();
                let doc = &mut self.peers[peer];
                let text_len = doc.length(&obj);
                if text_len == 0 {
                    return Ok(());
                }
                let start = start % text_len;
                let end = start + 1 + len % (text_len - start);
                let value = (val % 3) as i64;
                doc.mark(
                    &obj,
                    TextMark::new(mark_name(mark).into(), value, start, end),
                    expand_mark(expand),
                )
                .unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::SplitBlock { peer, pos } => {
                let obj = self.text.clone();
                let doc = &mut self.peers[peer];
                let pos = pos % (doc.length(&obj) + 1);
                doc.split_block(&obj, pos).unwrap();
                doc.commit();
                self.record_peer_heads(peer);
            }
            Op::Merge { peer } => {
                self.main.merge(&mut self.peers[peer]).unwrap();
                let heads = self.main.document().get_heads();
                self.main_history.push(heads);
            }
            Op::SyncChanges { pair } => {
                match pair % 4 {
                    0 => {
                        let [p0, p1] = &mut self.peers;
                        p0.merge(p1).unwrap();
                        self.record_peer_heads(0);
                    }
                    1 => {
                        let [p0, p1] = &mut self.peers;
                        p1.merge(p0).unwrap();
                        self.record_peer_heads(1);
                    }
                    n => {
                        let target = (n - 2) as usize;
                        self.peers[target].merge(&mut self.main).unwrap();
                        self.record_peer_heads(target);
                    }
                };
            }
            Op::BatchBoundary {
                peer,
                key,
                val,
                swap,
            } => {
                let doc = &mut self.peers[peer];
                doc.put(ROOT, key_name(key), val).unwrap();
                // Writing an equal value resolves to no op and commits to
                // nothing; the batch needs a real boundary, so skip.
                let Some(pre) = doc.commit() else {
                    return Ok(());
                };
                let boundary = doc.get_heads();
                doc.put(ROOT, key_name(key), val.wrapping_add(1)).unwrap();
                let post = doc.commit().expect("post-boundary put changes the value");
                let pre = doc.get_change_by_hash(&pre).unwrap();
                let post = doc.get_change_by_hash(&post).unwrap();
                self.history.push(boundary.clone());
                self.record_peer_heads(peer);
                // Pending until the batch below delivers the boundary hash.
                self.main.mask_author(self.authors[peer].clone(), &boundary);
                let batch = if swap {
                    vec![post, pre]
                } else {
                    vec![pre, post]
                };
                self.main.apply_changes(batch).unwrap();
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
            Op::BulkReplacePolicy {
                alice_at,
                bob_at,
                carol_at,
            } => {
                let mut new_policy = HashMap::new();
                for (peer, at) in [alice_at, bob_at].into_iter().enumerate() {
                    if let Some(at) = at {
                        let heads = self.history[at % self.history.len()].clone();
                        new_policy.insert(self.authors[peer].clone(), heads);
                    }
                }
                if let Some(at) = carol_at {
                    let heads = self.main_history[at % self.main_history.len()].clone();
                    new_policy.insert(self.local_author.clone(), heads);
                }
                self.bulk_replace_policy(new_policy)?;
            }
            Op::Isolate { at_history } => {
                let idx = at_history % self.main_history.len();
                let heads = self.main_history[idx].clone();
                self.main.isolate(&heads);
                self.isolated = Some(heads);
                self.isolated_edits = false;
            }
            Op::Integrate => {
                self.main.integrate();
                self.isolated = None;
                self.isolated_edits = false;
            }
            Op::Drain => self.drain()?,
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
            Op::LocalMaskedSplice { pos, del, text } => {
                self.end_isolation();
                let obj = self.text.clone();
                // Positions refer to the visible document, also for a
                // masked (masked) local author.
                let len = self.main.length(&obj);
                let pos = pos % (len + 1);
                let del = del % (len - pos + 1);
                if del == 0 && text.is_empty() {
                    return Ok(());
                }
                if self.main.is_author_masked(&self.local_author) {
                    self.hit("LocalMaskedSplice(masked)");
                }
                self.main
                    .splice_text(&obj, pos, del as isize, &text)
                    .unwrap();
                self.record_main_heads();
            }
            Op::EditDuringIsolation {
                key,
                val,
                at_history,
            } => {
                if self.isolated.is_none() {
                    let idx = at_history % self.main_history.len();
                    let heads = self.main_history[idx].clone();
                    self.main.isolate(&heads);
                    self.isolated = Some(heads);
                    self.isolated_edits = false;
                }
                if self.main.is_author_masked(&self.local_author) {
                    self.hit("EditDuringIsolation(masked)");
                }
                self.main.put(ROOT, key_name(key), val).unwrap();
                // Writing an equal value resolves to no op; nothing commits
                // and the observed view is unchanged.
                let Some(hash) = self.main.commit() else {
                    return Ok(());
                };
                // The commit's deps are the isolation heads, so the observed
                // view moves to the isolated commit hashes.
                if self.isolated_edits {
                    self.isolated.as_mut().expect("isolated").push(hash);
                } else {
                    self.isolated = Some(vec![hash]);
                    self.isolated_edits = true;
                }
                self.record_main_heads();
            }
            Op::SaveLoadRoundTrip => {
                self.end_isolation();
                self.drain()?;
                self.save_load_round_trip()?;
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
        self.hit(name);
        Ok(())
    }

    /// Verify a whole-policy replacement three ways: `set_write_frontier_log_patches`
    /// on a twin is one logged transition that replays to hydrate; a fresh
    /// load under the new policy hydrates identically; and moving the main
    /// doc through individual mask_author/reveal_author transitions lands on the same
    /// view.
    fn bulk_replace_policy(
        &mut self,
        new_policy: HashMap<Author<'static>, Vec<ChangeHash>>,
    ) -> Result<(), TestCaseError> {
        let old_policy = self.main.document().get_write_frontier();
        let bytes = self.main.save();
        let mut twin =
            Automerge::load_with_options(&bytes, LoadOptions::new().text_encoding(ENCODING))
                .expect("saved document reloads")
                .with_write_frontier(old_policy.clone());
        let mut view = twin.hydrate(None);
        let mut log = PatchLog::active();
        twin.set_write_frontier_log_patches(new_policy.clone(), &mut log)
            .expect("a fresh patch log belongs to any document");
        let patches = twin.make_patches(&mut log);
        // `hydrate::Value` cannot replay text `Mark` patches; both sides of
        // the comparison leave marks out of the hydrated text value.
        let replayable = patches
            .iter()
            .filter(|p| !(p.obj == self.text && matches!(p.action, PatchAction::Mark { .. })))
            .cloned();
        view.apply_patches(ENCODING, replayable).map_err(|e| {
            TestCaseError::fail(format!("twin rejected policy patches: {e} ({patches:?})"))
        })?;
        prop_assert_eq!(
            &view,
            &twin.hydrate(None),
            "one logged policy replacement does not replay to hydrate"
        );
        let fresh = Automerge::load_with_options(
            &bytes,
            LoadOptions::new()
                .text_encoding(ENCODING)
                .write_frontier(new_policy.clone()),
        )
        .expect("saved document reloads");
        prop_assert_eq!(
            &twin.hydrate(None),
            &fresh.hydrate(None),
            "replacing the write-frontier differs from a fresh load with the new policy"
        );
        // Move the main doc to the same policy through individual
        // transitions; each is logged in the main doc's own patch log and
        // drained by the model later.
        for author in old_policy.keys() {
            if !new_policy.contains_key(author) {
                self.main.reveal_author(author);
            }
        }
        for (author, heads) in &new_policy {
            self.main.mask_author(author.clone(), heads);
        }
        prop_assert_eq!(
            &self.main.document().hydrate(None),
            &twin.hydrate(None),
            "individual transitions to the same policy disagree with the bulk replacement"
        );
        Ok(())
    }

    /// Replace the main doc with a fresh load of its saved bytes under the
    /// same policy. Requires a drained patch log (the old log dies with the
    /// old document).
    fn save_load_round_trip(&mut self) -> Result<(), TestCaseError> {
        let policy = self.main.document().get_write_frontier();
        let before = self.main.document().hydrate(None);
        let bytes = self.main.save();
        let mut doc = AutoCommit::load_with_options(
            &bytes,
            LoadOptions::new()
                .text_encoding(ENCODING)
                .author(self.local_author.clone())
                .write_frontier(policy),
        )
        .expect("saved document reloads")
        // Same actor as before: this is the same session continuing, and a
        // fixed actor keeps generated runs deterministic.
        .with_actor(ActorId::from(vec![0x01]));
        prop_assert_eq!(
            &before,
            &doc.document().hydrate(None),
            "save/load round trip changed the view"
        );
        doc.update_diff_cursor();
        self.main = doc;
        Ok(())
    }

    /// Local edits during isolation are visible inside the isolation scope
    /// (the session's own actor), which `hydrate(Some(heads))` cannot
    /// express; integrate first so the model's oracle stays applicable.
    /// ([`Op::EditDuringIsolation`] instead extends the observed head set
    /// with its commit hashes, which `hydrate(Some(heads))` can express.)
    fn end_isolation(&mut self) {
        if self.isolated.take().is_some() {
            self.main.integrate();
            self.isolated_edits = false;
        }
    }

    /// Record the main doc's heads after a local commit; isolation targets.
    fn record_main_heads(&mut self) {
        let heads = self.main.get_heads();
        self.main_history.push(heads);
    }

    fn drain(&mut self) -> Result<(), TestCaseError> {
        let patches = self.main.diff_incremental();
        // Oracle 1: the model, maintained only from patches, equals hydrate
        // at the observed heads. Text patches also feed the rich-text model
        // (oracle 4); `hydrate::Value` cannot replay `Mark` patches, and
        // hydrated text values carry no marks on either side.
        for patch in patches.iter().filter(|p| p.obj == self.text) {
            self.rich.apply(patch);
        }
        let replayable: Vec<Patch> = patches
            .iter()
            .filter(|p| !(p.obj == self.text && matches!(p.action, PatchAction::Mark { .. })))
            .cloned()
            .collect();
        self.model
            .apply_patches(ENCODING, replayable)
            .map_err(|e| {
                TestCaseError::fail(format!("model rejected patches: {e} ({patches:?})"))
            })?;
        let expected = match &self.isolated {
            Some(heads) => self.main.hydrate(ROOT, Some(heads)).unwrap(),
            None => self.main.hydrate(ROOT, None).unwrap(),
        };
        prop_assert_eq!(&self.model, &expected, "patches: {:?}", patches);
        if self.isolated.is_none() {
            // Oracle 4: `spans()` (text runs, marks and block markers)
            // agrees with the patch-replayed rich-text model. `spans()`
            // reads the current document, so this only applies outside
            // isolation; the model catches up at integration.
            let spans = RichText::from_doc(self.main.document(), &self.text);
            prop_assert_eq!(
                &self.rich,
                &spans,
                "rich-text model disagrees with spans(); patches: {:?}",
                patches
            );
        }
        Ok(())
    }

    /// Oracles 2 and 3, checked after every op: the document agrees with a
    /// fresh `Automerge::load` of its saved bytes under the same write_frontier
    /// policy (an independent materialisation through `from_parts` + index
    /// recomputation), and the indexed builder agrees with a forced slow
    /// walk at the same resolved clock. Whole-result fast/slow comparisons
    /// inside the op set additionally run under
    /// `--features slow_path_assertions` (CI: `scripts/ci/build-test`).
    fn check_doc_oracles(&mut self) -> Result<(), TestCaseError> {
        let doc = self.main.document();
        let fast = doc.hydrate(None);
        let slow = doc.read_forced_slow_hydrate();
        prop_assert_eq!(
            &fast,
            &slow,
            "indexed hydrate disagrees with forced slow walk"
        );
        let policy = doc.get_write_frontier();
        let live_marks = doc.marks(&self.text).unwrap();
        let live_spans = RichText::from_doc(doc, &self.text);
        let bytes = self.main.save();
        let reload = Automerge::load_with_options(
            &bytes,
            LoadOptions::new()
                .text_encoding(ENCODING)
                .write_frontier(policy),
        )
        .expect("saved document reloads");
        prop_assert_eq!(
            &fast,
            &reload.hydrate(None),
            "live doc disagrees with reload oracle"
        );
        prop_assert_eq!(
            &live_marks,
            &reload.marks(&self.text).unwrap(),
            "marks() disagrees with reload oracle"
        );
        prop_assert_eq!(
            &live_spans,
            &RichText::from_doc(&reload, &self.text),
            "spans() disagrees with reload oracle"
        );
        Ok(())
    }
}

fn run(ops: Vec<Op>, coverage: &RefCell<BTreeMap<&'static str, u64>>) -> Result<(), TestCaseError> {
    let mut runner = Runner::new();
    for op in ops {
        runner.step(op)?;
        runner.check_doc_oracles()?;
    }
    runner.drain()?;
    let mut coverage = coverage.borrow_mut();
    for (name, count) in runner.cov {
        *coverage.entry(name).or_insert(0) += count;
    }
    Ok(())
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

#[test]
fn patches_replay_to_hydrate() {
    // An explicit `TestRunner` (rather than the `proptest!` macro) so the
    // per-scenario coverage counters can be asserted over the whole run:
    // an op variant that never reaches its scenario fails loudly instead of
    // silently testing nothing. Cases come from `PROPTEST_CASES` (default
    // 256), persistence from `write_frontier_parity.proptest-regressions`.
    let config = ProptestConfig {
        source_file: Some(file!()),
        ..Default::default()
    };
    let mut runner = TestRunner::new(config);
    let coverage = RefCell::new(BTreeMap::new());
    let result = runner.run(&proptest::collection::vec(gen_op(), 0..40), |ops| {
        run(ops, &coverage)
    });
    if let Err(e) = result {
        panic!("{}\n{}", e, runner);
    }
    let coverage = coverage.borrow();
    eprintln!("scenario coverage: {coverage:?}");
    for name in COVERAGE {
        let count = coverage.get(name).copied().unwrap_or(0);
        assert!(
            count > 0,
            "scenario {name:?} was never reached: {coverage:?}"
        );
    }
}

proptest! {
    /// The mask-vs-clock pin (spec W3, "endpoint = clock, not clock + mask"):
    /// patches recorded under policy A and materialised after switching to a
    /// policy B with the same visible clock are identical to the ones policy
    /// A would have produced. Here B mask_authors alice at heads she has no ops
    /// after, so the mask stays empty and `visible_current()` is unchanged.
    #[test]
    fn mask_change_with_equal_visible_clock_keeps_patches_identical(
        alice_vals in proptest::collection::vec(any::<i64>(), 1..5),
        bob_vals in proptest::collection::vec(any::<i64>(), 1..5),
    ) {
        let alice = Author::try_from("aaaa").unwrap();
        let bob = Author::try_from("bbbb").unwrap();
        let mut a = AutoCommit::new_with_encoding(ENCODING)
            .with_author(Some(alice.clone()))
            .with_actor(ActorId::from(vec![0xA0]));
        for (i, val) in alice_vals.iter().enumerate() {
            a.put(ROOT, key_name(i as u8), *val).unwrap();
            a.commit();
        }
        // Alice's last change: she has no ops after this boundary.
        let boundary = a.get_heads();
        let mut b = a
            .fork()
            .with_author(Some(bob))
            .with_actor(ActorId::from(vec![0xB0]));
        for (i, val) in bob_vals.iter().enumerate() {
            b.put(ROOT, key_name(i as u8), *val).unwrap();
            b.commit();
        }
        a.merge(&mut b).unwrap();
        let bytes = a.save();

        // Record patches under policy A (no write-frontier)...
        let mut log = PatchLog::active();
        let mut doc = Automerge::load_with_options(
            &bytes,
            LoadOptions::new().text_encoding(ENCODING).patch_log(&mut log),
        )
        .expect("saved document reloads");
        let under_a = doc.make_patches(&mut log.clone());
        // ...switch to policy B with the same visible clock...
        let view_a = doc.hydrate(None);
        doc.set_write_frontier(HashMap::from([(alice, boundary)]));
        prop_assert_eq!(&view_a, &doc.hydrate(None), "policies A and B must be observationally equal");
        // ...and materialise the same recorded events under B.
        let under_b = doc.make_patches(&mut log);
        prop_assert_eq!(under_a, under_b);
    }
}
/// Shrunk from `patches_replay_to_hydrate` (seed `beecc57c…`), preserved
/// verbatim: the policy churn (mask_author carol, bulk-replace, drain at the
/// save/load round trip) revealed carol's text under alice's still-visible
/// mark with `marks: None`; see
/// [`regression_reveal_under_visible_mark_carries_the_mark`] for the
/// distilled cause.
#[test]
fn regression_shrunk_reveal_sequence_replays() {
    let ops = vec![
        Op::PeerPut {
            peer: 0,
            key: 0,
            val: 0,
        },
        Op::LocalMaskedSplice {
            pos: 0,
            del: 0,
            text: "aaa".into(),
        },
        Op::SyncChanges { pair: 246 },
        Op::Merge { peer: 0 },
        Op::LocalEdit { key: 0, val: 0 },
        Op::LocalEdit { key: 0, val: 0 },
        Op::Mark {
            peer: 0,
            start: 2607799235206070065,
            len: 0,
            name: 0,
            val: 0,
            expand: 0,
        },
        Op::Merge { peer: 0 },
        Op::MaskAuthorLocal {
            at_history: 1157280109772358564,
        },
        Op::BatchBoundary {
            peer: 0,
            key: 0,
            val: 0,
            swap: false,
        },
        Op::SyncChanges { pair: 10 },
        Op::BulkReplacePolicy {
            alice_at: None,
            bob_at: Some(4601837744069311024),
            carol_at: Some(8043579550870084571),
        },
        Op::SaveLoadRoundTrip,
    ];
    run(ops, &RefCell::new(BTreeMap::new())).unwrap();
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
