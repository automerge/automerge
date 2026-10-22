//! An audited and a `next` cluster of actors, fed the same edits and the
//! same syncs, must agree on fragments and whole saves byte for byte, and
//! the `next` one must hold exactly the hashes it needs.
//!
//! `AUTOMERGE_FUZZ_CHANGES` (default 400) sets the run length and
//! `AUTOMERGE_FUZZ_SEED` the seed. Build with `small_fragments` for deep
//! fragment hierarchies in short runs.

use super::Automerge;
use crate::autocommit::AutoCommit;
use crate::hash_retention::{Full, HashRetention, Retained};
use crate::marks::{ExpandMark, Mark};
use crate::read::ReadDoc;
use crate::tx::{CommitOptions, Transactable};
use crate::{ActorId, ObjType, ScalarValue, Value, ROOT};

const ACTORS: usize = 4;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn percent(&mut self, p: usize) -> bool {
        self.below(100) < p
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |v| v.parse().expect(name))
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Merge,
    MissingChanges,
    WholeSave,
    FragmentStack,
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Edit {
        actor: usize,
        seed: u64,
    },
    Sync {
        from: usize,
        to: usize,
        how: Delivery,
    },
}

fn obj<H: HashRetention>(doc: &AutoCommit<H>, key: &str) -> crate::ObjId {
    match doc.get(ROOT, key).unwrap() {
        Some((Value::Object(_), id)) => id,
        other => panic!("{key}: {other:?}"),
    }
}

/// A few random ops, chosen against `doc`'s own state, so both sides make
/// the same choices while they agree.
fn edit<H: HashRetention>(doc: &mut AutoCommit<H>, seed: u64) {
    let mut rng = Rng(seed | 1);
    let map = obj(doc, "map");
    let list = obj(doc, "list");
    let text = obj(doc, "text");
    for _ in 0..1 + rng.below(3) {
        let key = format!("k{}", rng.below(8));
        let n = rng.next() as i64;
        match rng.below(10) {
            0 => doc.put(&map, key, n).unwrap(),
            1 => {
                if doc.get(&map, &key).unwrap().is_some() {
                    doc.delete(&map, key).unwrap();
                }
            }
            2 => {
                let len = doc.length(&list);
                doc.insert(&list, rng.below(len + 1), n).unwrap();
            }
            3 => {
                let len = doc.length(&list);
                if len > 0 {
                    doc.delete(&list, rng.below(len)).unwrap();
                }
            }
            4 => {
                let len = doc.length(&list);
                if len > 0 {
                    doc.put(&list, rng.below(len), n).unwrap();
                }
            }
            5 => {
                let len = doc.length(&text);
                doc.splice_text(&text, rng.below(len + 1), 0, "ab").unwrap();
            }
            6 => {
                let len = doc.length(&text);
                if len > 0 {
                    let del = 1 + rng.below(len.min(3));
                    doc.splice_text(&text, rng.below(len + 1 - del), del as isize, "")
                        .unwrap();
                }
            }
            7 => {
                let len = doc.length(&text);
                if len >= 2 {
                    let start = rng.below(len - 1);
                    let end = start + 1 + rng.below(len - start - 1);
                    let mark = Mark::new("bold".into(), rng.percent(70), start, end);
                    doc.mark(&text, mark, ExpandMark::Both).unwrap();
                }
            }
            8 => doc.increment(&map, "counter", n % 10).unwrap(),
            _ => {
                let inner = doc.put_object(&map, key, ObjType::Map).unwrap();
                doc.put(&inner, "x", n).unwrap();
            }
        }
    }
    doc.commit_with(CommitOptions::default().with_time(0));
}

fn sync<H: HashRetention>(docs: &mut [AutoCommit<H>], from: usize, to: usize, how: Delivery) {
    let (a, b) = if from < to {
        let (l, r) = docs.split_at_mut(to);
        (&mut l[from], &mut r[0])
    } else {
        let (l, r) = docs.split_at_mut(from);
        (&mut r[0], &mut l[to])
    };
    match how {
        Delivery::Merge => {
            b.merge(a).unwrap();
        }
        Delivery::MissingChanges => {
            let source = a.document();
            let known: Vec<_> = b
                .get_heads()
                .into_iter()
                .filter(|h| {
                    let graph = &source.change_graph;
                    graph.node_for_change_id(h, &source.ops.actors).is_some()
                })
                .collect();
            let bytes = a.save_after(&known).unwrap();
            b.load_incremental(&bytes).unwrap();
        }
        Delivery::WholeSave => {
            let bytes = a.document().save();
            b.load_incremental(&bytes).unwrap();
        }
        Delivery::FragmentStack => {
            for bundle in stack(a) {
                b.load_incremental(&bundle).unwrap();
            }
        }
    }
}

/// Every fragment's bundle, highest level first, then the loose commits.
fn stack<H: HashRetention>(doc: &AutoCommit<H>) -> Vec<Vec<u8>> {
    let fragments = doc.fragments(1..).into_iter().chain(doc.fragments(0..=0));
    doc.bundles_for_fragments(fragments).unwrap()
}

fn cluster<H: HashRetention>(make: impl Fn() -> AutoCommit<H>) -> Vec<AutoCommit<H>> {
    let mut docs: Vec<AutoCommit<H>> = (0..ACTORS)
        .map(|i| make().with_actor(ActorId::from(vec![i as u8 + 1; 4])))
        .collect();
    let first = &mut docs[0];
    first.put_object(ROOT, "map", ObjType::Map).unwrap();
    first.put_object(ROOT, "list", ObjType::List).unwrap();
    first.put_object(ROOT, "text", ObjType::Text).unwrap();
    let map = obj(first, "map");
    first.put(&map, "counter", ScalarValue::counter(0)).unwrap();
    first.commit_with(CommitOptions::default().with_time(0));
    for i in 1..ACTORS {
        sync(&mut docs, 0, i, Delivery::Merge);
    }
    docs
}

struct Plan {
    rng: Rng,
    /// While set, syncs stay within one half of the actors.
    partition: usize,
}

impl Plan {
    fn step(&mut self) -> Step {
        let rng = &mut self.rng;
        if self.partition > 0 {
            self.partition -= 1;
        } else if rng.percent(1) {
            self.partition = 50 + rng.below(300);
        }
        if rng.percent(65) {
            return Step::Edit {
                actor: rng.below(ACTORS),
                seed: rng.next(),
            };
        }
        let from = rng.below(ACTORS);
        let mut to = rng.below(ACTORS - 1);
        if to >= from {
            to += 1;
        }
        if self.partition > 0 && (from < ACTORS / 2) != (to < ACTORS / 2) {
            to = from ^ 1;
        }
        let how = match rng.below(20) {
            0..=9 => Delivery::Merge,
            10..=15 => Delivery::MissingChanges,
            16..=17 => Delivery::WholeSave,
            _ => Delivery::FragmentStack,
        };
        Step::Sync { from, to, how }
    }
}

fn apply<H: HashRetention>(docs: &mut [AutoCommit<H>], step: Step) {
    match step {
        Step::Edit { actor, seed } => edit(&mut docs[actor], seed),
        Step::Sync { from, to, how } => sync(docs, from, to, how),
    }
}

/// Fragments in a comparable form: their members' order follows the
/// document's node order, which depends on arrival order.
fn normalized(fragments: Vec<crate::change_graph::Fragment>) -> Vec<crate::change_graph::Fragment> {
    let mut fragments: Vec<_> = fragments
        .into_iter()
        .map(|mut f| {
            f.members.sort();
            f.checkpoints.sort();
            f.boundary.sort();
            f
        })
        .collect();
    fragments.sort_by_key(|f| f.head);
    fragments
}

fn assert_same_fragments(
    a: Vec<crate::change_graph::Fragment>,
    b: Vec<crate::change_graph::Fragment>,
    what: &str,
) {
    let (a, b) = (normalized(a), normalized(b));
    let differ =
        |x: &[crate::change_graph::Fragment], y: &[crate::change_graph::Fragment]| -> Vec<String> {
            x.iter()
                .filter(|f| !y.contains(f))
                .map(|f| match y.iter().find(|g| g.head == f.head) {
                    None => format!("{:?} level {} only here", f.head, f.level),
                    Some(g) => format!(
                        "{:?} level {}: boundary {:?}/{:?} checkpoints {}/{} members {}/{}",
                        f.head,
                        f.level,
                        f.boundary,
                        g.boundary,
                        f.checkpoints.len(),
                        g.checkpoints.len(),
                        f.members.len(),
                        g.members.len()
                    ),
                })
                .collect()
        };
    let (only_a, only_b) = (differ(&a, &b), differ(&b, &a));
    assert!(
        only_a.is_empty() && only_b.is_empty(),
        "{what}: first {only_a:#?} second {only_b:#?}"
    );
}

fn assert_same_held<H1: HashRetention, H2: HashRetention>(
    a: &Automerge<H1>,
    b: &Automerge<H2>,
    what: &str,
) {
    let (a_held, b_held) = (a.change_graph.held_hashes(), b.change_graph.held_hashes());
    let only_a: Vec<_> = a_held.difference(&b_held).collect();
    let only_b: Vec<_> = b_held.difference(&a_held).collect();
    assert!(
        only_a.is_empty() && only_b.is_empty(),
        "{what}: {} held only by the first {only_a:?}, {} only by the second {only_b:?}",
        only_a.len(),
        only_b.len()
    );
}

/// Each actor's `save_incremental` chunks, replayed in order into a fresh
/// document of each mode.
struct Archives {
    audit: Vec<Automerge<Full>>,
    next: Vec<Automerge<Retained>>,
    widened: usize,
    extra_changes: usize,
}

fn members(chunk: &[u8]) -> std::collections::BTreeSet<(ActorId, u64)> {
    let bundle = crate::storage::Bundle::try_from(chunk).unwrap();
    bundle
        .member_actors
        .iter()
        .zip(&bundle.member_seqs)
        .map(|(a, s)| (bundle.actors()[usize::from(*a)].clone(), s.get()))
        .collect()
}

/// A `next` chunk equals the audited one unless the document has freed a
/// dep the audited chunk names; then it starts further back, carrying only
/// changes the archive already has.
fn check_point(
    audit: &mut [AutoCommit<Full>],
    next: &mut [AutoCommit<Retained>],
    archives: &mut Archives,
    at: &str,
) {
    for i in 0..ACTORS {
        let a_chunk = audit[i].save_incremental();
        let n_chunk = next[i].save_incremental();
        if a_chunk != n_chunk {
            assert!(
                !a_chunk.is_empty(),
                "{at}: actor {i} saved changes the audited copy didn't"
            );
            let deps = crate::storage::Bundle::try_from(&a_chunk[..])
                .unwrap()
                .deps()
                .to_vec();
            let held = next[i].document().change_graph.held_hashes();
            assert!(
                deps.iter().any(|d| !held.contains(d)),
                "{at}: actor {i} save_incremental widened while holding every dep"
            );
            let (a_members, n_members) = (members(&a_chunk), members(&n_chunk));
            assert!(
                a_members.is_subset(&n_members),
                "{at}: actor {i} widened chunk lost changes"
            );
            let archive = &archives.next[i];
            for (actor, seq) in n_members.difference(&a_members) {
                let id = crate::ChangeId::from_parts(
                    actor.clone(),
                    std::num::NonZeroU64::new(*seq).unwrap(),
                );
                assert!(
                    archive.node_for_change_id(&id).is_some(),
                    "{at}: actor {i} widened past the archive"
                );
            }
            archives.widened += 1;
            archives.extra_changes += n_members.len() - a_members.len();
        }
        if !a_chunk.is_empty() {
            archives.audit[i].load_incremental(&a_chunk).unwrap();
        }
        if !n_chunk.is_empty() {
            archives.next[i].load_incremental(&n_chunk).unwrap();
        }
        let heads = next[i].get_heads();
        assert_eq!(
            archives.audit[i].get_heads(),
            heads,
            "{at}: actor {i} audited archive heads"
        );
        assert_eq!(
            archives.next[i].get_heads(),
            heads,
            "{at}: actor {i} next archive heads"
        );
        let mut actors: Vec<_> = heads.iter().map(|h| h.actor()).collect();
        actors.sort();
        actors.dedup();
        assert_eq!(
            actors.len(),
            heads.len(),
            "{at}: actor {i} heads {heads:?} include an ancestor"
        );
    }
}

fn check_archives(audit: &mut [AutoCommit<Full>], archives: &Archives, at: &str) {
    for (i, doc) in audit.iter_mut().enumerate() {
        let save = doc.document().save();
        assert!(
            archives.audit[i].save() == save,
            "{at}: actor {i} audited archive save differs"
        );
        assert!(
            archives.next[i].save() == save,
            "{at}: actor {i} next archive save differs"
        );
    }
}

fn deep_check(audit: &mut [AutoCommit<Full>], next: &mut [AutoCommit<Retained>], at: &str) {
    for i in 0..ACTORS {
        let a = &mut audit[i];
        let n = &mut next[i];
        let stack_a = stack(a);
        assert!(
            stack_a == stack(n),
            "{at}: actor {i} fragment stacks differ"
        );
        let save = n.document().save();
        assert!(a.document().save() == save, "{at}: actor {i} saves differ");

        let expected = a.document().clone().disable_audit_mode();
        assert_same_held(
            n.document(),
            &expected,
            &format!("{at}: actor {i} held hashes"),
        );

        let reloaded = Automerge::<Retained>::load(&stack_a.concat()).unwrap();
        assert_eq!(
            reloaded.get_heads(),
            n.get_heads(),
            "{at}: actor {i} stack reload heads"
        );
        assert_same_fragments(
            reloaded.fragments(..),
            n.fragments(..),
            &format!("{at}: actor {i} stack reload fragments"),
        );
        assert_same_held(
            &reloaded,
            n.document(),
            &format!("{at}: actor {i} stack reload held"),
        );

        let mut incremental = Automerge::<Retained>::new();
        for bundle in &stack_a {
            incremental.load_incremental(bundle).unwrap();
        }
        assert_eq!(
            incremental.get_heads(),
            n.get_heads(),
            "{at}: actor {i} incremental heads"
        );

        let mut doc = AutoCommit::load(&save).unwrap();
        for actor in 0..ACTORS {
            doc.set_actor(ActorId::from(vec![actor as u8 + 1; 4]));
            doc.put(ROOT, "probe", actor as i64).unwrap();
            doc.commit_with(CommitOptions::default().with_time(0));
        }
    }
}

#[test]
fn audited_and_next_clusters_agree_byte_for_byte() {
    let changes = env("AUTOMERGE_FUZZ_CHANGES", 400) as usize;
    let seed = env("AUTOMERGE_FUZZ_SEED", 0x5eed_1234_abcd_0001);
    let mut audit = cluster(|| AutoCommit::new().enable_audit_mode().unwrap());
    let mut next = cluster(AutoCommit::new);
    let mut archives = Archives {
        audit: (0..ACTORS)
            .map(|_| {
                Automerge::new()
                    .enable_audit_mode()
                    .unwrap_or_else(|_| panic!())
            })
            .collect(),
        next: (0..ACTORS).map(|_| Automerge::new()).collect(),
        widened: 0,
        extra_changes: 0,
    };
    let mut plan = Plan {
        rng: Rng(seed),
        partition: 0,
    };
    let incremental_every = ACTORS * 25;
    let deep_every = (changes / 5).max(incremental_every);
    let mut edits = 0;
    let mut step_no = 0;
    while edits < changes {
        let step = plan.step();
        step_no += 1;
        let at = format!("seed {seed:#x} step {step_no} ({step:?})");
        let applied = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply(&mut audit, step);
            apply(&mut next, step);
        }));
        assert!(applied.is_ok(), "{at}: step failed");
        for (i, d) in next.iter_mut().enumerate() {
            let unhashed = d.document().change_graph.pinned_without_hash();
            assert!(
                unhashed.is_empty(),
                "{at}: actor {i} pins without hashes {unhashed:?}"
            );
        }
        if matches!(step, Step::Edit { .. }) {
            edits += 1;
            if edits % incremental_every == 0 {
                check_point(&mut audit, &mut next, &mut archives, &at);
            }
            if edits % deep_every == 0 {
                check_archives(&mut audit, &archives, &at);
                deep_check(&mut audit, &mut next, &at);
            }
        }
    }
    let at = format!("seed {seed:#x} end");
    deep_check(&mut audit, &mut next, &at);
    eprintln!(
        "{changes} changes: {} incremental saves widened, {} extra changes",
        archives.widened, archives.extra_changes
    );
    for (mut a, mut n) in audit.into_iter().zip(next) {
        let converted = a.document().clone().disable_audit_mode();
        assert_same_held(
            &converted,
            n.document(),
            &format!("{at}: into_next held hashes"),
        );
        let mut back = n
            .enable_audit_mode()
            .unwrap_or_else(|_| panic!("{at}: enable_audit_mode"));
        assert!(
            back.save() == a.save(),
            "{at}: enable_audit_mode save differs"
        );
    }
}
