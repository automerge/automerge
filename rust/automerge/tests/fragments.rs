//! Public-API property tests for fragment generation and encoding.
//!
//! Histories, checkpoint locations and delivery schedules are generated, not
//! seeded with the regression fixture. Hash nonces bias selected commits to
//! levels 1 and 2 so small CI runs exercise fragmentation and coalescing.
use std::collections::{BTreeMap, BTreeSet};

use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, AutoCommit, Automerge, Change, ChangeHash, ExpandedChange, Fragment, ObjId, ObjType,
    ReadDoc, ROOT,
};
use proptest::prelude::*;
use rand::{seq::SliceRandom, RngExt, SeedableRng};

const PEER_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PeerId(usize);

#[derive(Clone, Copy, Debug)]
enum Transport {
    None,
    ApplyChanges,
    Merge,
    LoadIncremental,
}

#[derive(Clone, Copy, Debug)]
struct Step {
    /// Peer receiving changes and making the subsequent edits.
    peer: PeerId,
    receive_from: PeerId,
    transport: Transport,
    /// Drives both the edits and the checkpoint timestamp search.
    seed: u64,
    /// Create a level-1 checkpoint after this step's edits.
    make_checkpoint: bool,
}

fn arbitrary_step() -> impl Strategy<Value = Step> {
    (
        (0..PEER_COUNT).prop_map(PeerId),
        (0..PEER_COUNT).prop_map(PeerId),
        prop::sample::select(vec![
            Transport::None,
            Transport::ApplyChanges,
            Transport::Merge,
            Transport::LoadIncremental,
        ]),
        any::<u64>(),
        any::<bool>(),
    )
        .prop_map(
            |(peer, receive_from, transport, seed, make_checkpoint)| Step {
                peer,
                receive_from,
                transport,
                seed,
                make_checkpoint,
            },
        )
}
type Snapshot = Vec<(Fragment, Vec<u8>)>;
type Dag = BTreeMap<ChangeHash, Vec<ChangeHash>>;

fn level(hash: ChangeHash) -> usize {
    hash.0.iter().take_while(|b| **b == 0).count()
}

fn ancestors(dag: &Dag, heads: impl IntoIterator<Item = ChangeHash>) -> BTreeSet<ChangeHash> {
    let mut pending: Vec<_> = heads.into_iter().collect();
    let mut seen = BTreeSet::new();
    while let Some(hash) = pending.pop() {
        if seen.insert(hash) {
            pending.extend(&dag[&hash]);
        }
    }
    seen
}

fn snapshot(doc: &Automerge) -> Snapshot {
    let changes: BTreeMap<_, _> = doc
        .get_changes(&[])
        .into_iter()
        .map(|c| (c.hash(), c))
        .collect();
    let dag: Dag = changes
        .iter()
        .map(|(hash, c)| (*hash, c.deps().to_vec()))
        .collect();
    let mut fragments = doc.fragments(..);
    // Enumeration order is not part of the comparison; ordering *within*
    // metadata and the actual bundle bytes must nevertheless be identical.
    fragments.sort_by_key(|f| f.head);
    let mut covered = BTreeSet::new();
    for f in &fragments {
        assert_eq!(doc.get_fragment(f.head).as_ref(), Some(f));
        assert_eq!(f.level, level(f.head));
        assert!(f.members.contains(&f.head));

        // Independent, deliberately simple oracle using only public change
        // dependencies. No production fragment/cache traversal is reused.
        let mut pending = dag[&f.head].clone();
        let mut seen = BTreeSet::new();
        let mut boundary = BTreeSet::new();
        while let Some(hash) = pending.pop() {
            if !seen.insert(hash) {
                continue;
            }
            if level(hash) >= f.level {
                boundary.insert(hash);
            } else {
                pending.extend(&dag[&hash]);
            }
        }
        assert_eq!(
            f.boundary.iter().copied().collect::<BTreeSet<_>>(),
            boundary
        );
        assert_eq!(f.boundary.len(), boundary.len());
        let members = if f.level == 0 {
            // A loose commit has only itself as a member. Avoid traversing
            // the entire history twice per loose commit in this slow oracle.
            BTreeSet::from([f.head])
        } else {
            let excluded = ancestors(&dag, boundary);
            ancestors(&dag, [f.head])
                .difference(&excluded)
                .copied()
                .collect()
        };
        assert_eq!(f.members.iter().copied().collect::<BTreeSet<_>>(), members);
        assert_eq!(f.members.len(), members.len());
        let checkpoints: BTreeSet<_> = members
            .iter()
            .copied()
            .filter(|h| *h != f.head && level(*h) > 0)
            .collect();
        assert!(checkpoints.iter().all(|h| level(*h) < f.level));
        assert_eq!(
            f.checkpoints.iter().copied().collect::<BTreeSet<_>>(),
            checkpoints
        );
        assert_eq!(f.checkpoints.len(), checkpoints.len());
        covered.extend(members);
    }
    assert_eq!(covered, dag.keys().copied().collect());
    let bundled = doc.bundle_fragments(fragments.iter().filter(|f| f.level > 0).cloned());
    assert_eq!(
        bundled.len(),
        fragments.iter().filter(|f| f.level > 0).count()
    );
    let mut bundled = bundled.into_iter();
    fragments
        .into_iter()
        .map(|f| {
            // Loose commits already have immutable wire bytes. Only nonzero-level
            // fragments need bundling; don't re-encode every loose commit on each
            // snapshot (which would dominate this test's runtime).
            let bytes = if f.level == 0 {
                changes[&f.head].raw_bytes().to_vec()
            } else {
                bundled.next().unwrap()
            };
            (f, bytes)
        })
        .collect()
}

fn assert_snapshot(doc: &Automerge, expected: &Snapshot) {
    let actual = snapshot(doc);
    assert_eq!(actual.len(), expected.len());
    for ((meta, bytes), (expected_meta, expected_bytes)) in actual.iter().zip(expected) {
        assert_eq!(meta, expected_meta);
        // Don't dump thousands of wire bytes on a proptest failure.
        assert!(bytes == expected_bytes, "bundle differs for {}", meta.head);
    }
}

fn edit(doc: &mut AutoCommit, list: &ObjId, text: &ObjId, rng: &mut rand::rngs::SmallRng) {
    let key = format!("key{}", rng.random_range(0..8));
    match rng.random_range(0..7) {
        0 | 1 => doc.put(ROOT, key, rng.random::<i64>()).unwrap(),
        2 => doc.delete(ROOT, key).unwrap(),
        3 => {
            let at = rng.random_range(0..=doc.length(list));
            doc.insert(list, at, rng.random::<i64>()).unwrap();
        }
        4 => {
            let len = doc.length(list);
            if len > 0 {
                doc.delete(list, rng.random_range(0..len)).unwrap();
            }
        }
        5 => {
            let len = doc.length(text);
            let at = rng.random_range(0..=len);
            let delete = usize::from(at < len && rng.random::<bool>());
            doc.splice_text(text, at, delete as isize, "x").unwrap();
        }
        _ => {} // Exercise empty changes and equal operation counters too.
    }
    if doc.commit().is_none() {
        doc.empty_change(CommitOptions::default());
    }
}

fn checkpoint(doc: &mut AutoCommit, target_level: usize, seed: u64) {
    // Construct a real empty change on this peer's current heads. Search only
    // its advisory timestamp; the actor, sequence and dependencies stay as
    // generated by the public API. Don't clone/apply the whole doc per nonce.
    let mut candidate_doc = doc.clone();
    let hash = candidate_doc.empty_change(CommitOptions::default());
    let change = candidate_doc.get_change_by_hash(&hash).unwrap();
    let mut expanded: ExpandedChange = (&change).into();
    // Keep timestamp deltas in range; extreme i64 timestamps exercise codec
    // overflow rather than fragment generation.
    expanded.time = i64::from(seed as u32);
    loop {
        let candidate = Change::from(expanded.clone());
        if level(candidate.hash()) == target_level {
            doc.apply_changes([candidate]).unwrap();
            return;
        }
        expanded.time = expanded.time.wrapping_add(1);
    }
}

fn history(steps: &[Step], higher_checkpoint: usize) -> (Automerge, Vec<Automerge>) {
    let mut base = AutoCommit::new().with_actor(ActorId::from(&[0][..]));
    let list = base.put_object(ROOT, "list", ObjType::List).unwrap();
    let text = base.put_object(ROOT, "text", ObjType::Text).unwrap();
    base.commit();
    let mut peers: Vec<_> = (1..=PEER_COUNT)
        .map(|actor| base.fork().with_actor(ActorId::from(&[actor as u8][..])))
        .collect();

    for (index, step) in steps.iter().enumerate() {
        let Step {
            peer: PeerId(to),
            receive_from: PeerId(from),
            transport,
            seed,
            make_checkpoint,
        } = *step;
        if to != from {
            match transport {
                Transport::ApplyChanges => {
                    let changes = peers[from].get_changes(&[]);
                    peers[to].apply_changes(changes).unwrap();
                }
                Transport::Merge => {
                    let mut other = peers[from].clone();
                    peers[to].merge(&mut other).unwrap();
                }
                Transport::LoadIncremental => {
                    let bytes = peers[from].save();
                    peers[to].load_incremental(&bytes).unwrap();
                }
                Transport::None => {}
            }
        }
        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        for _ in 0..16 {
            edit(&mut peers[to], &list, &text, &mut rng);
        }
        // Both the position and ancestry of each checkpoint vary by case.
        // Limit the expensive level-2 hash search to one checkpoint per case.
        if index == higher_checkpoint {
            checkpoint(&mut peers[to], 2, seed);
        } else if make_checkpoint {
            checkpoint(&mut peers[to], 1, seed);
        }
    }
    let mut source = Automerge::new();
    let mut branches = Vec::new();
    for mut peer in peers {
        // Keep the peers' causal histories for a separate merge-order replay.
        let mut branch = Automerge::new();
        branch.apply_changes(peer.get_changes(&[])).unwrap();
        source.merge(&mut branch).unwrap();
        branches.push(branch);
    }
    (source, branches)
}

fn config() -> ProptestConfig {
    let mut config = ProptestConfig {
        // Integration tests live beside (not under) src/lib.rs, so use an
        // adjacent regression file rather than SourceParallel's root search.
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"),
        )),
        ..ProptestConfig::default()
    };
    // Keep normal CI bounded, without overriding an extended fuzz campaign.
    if std::env::var_os("PROPTEST_CASES").is_none() {
        config.cases = 8;
    }
    config
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn fragments_converge_across_delivery_schedules(
        steps in proptest::collection::vec(arbitrary_step(), 16..33),
        higher_checkpoint in 0usize..32,
        delivery_seed in any::<u64>(),
    ) {
        let (source, mut branches) = history(&steps, higher_checkpoint % steps.len());
        let changes = source.get_changes(&[]);
        assert!(changes.len() >= 256);
        let expected = snapshot(&source);
        assert!(expected.iter().any(|(f, _)| f.level >= 2));
        assert_snapshot(&Automerge::load(&source.save()).unwrap(), &expected);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(delivery_seed);

        // Reverse delivery forces dependencies to arrive late. Randomized
        // delivery varies batch boundaries and duplicates transmissions.
        for schedule in 0..3 {
            let mut delivery = changes.clone();
            if schedule == 0 {
                delivery.reverse();
            } else {
                delivery.shuffle(&mut rng);
            }
            let mut replica = Automerge::new();
            let mut offset = 0;
            let mut reloaded = false;
            while offset < delivery.len() {
                let size = if schedule == 1 { 1 } else { rng.random_range(1..=64) };
                let end = (offset + size).min(delivery.len());
                let batch = &delivery[offset..end];
                replica.apply_changes(batch.iter().cloned()).unwrap();
                if rng.random::<bool>() {
                    replica.apply_changes(batch.iter().cloned()).unwrap();
                }
                offset = end;
                if !reloaded && offset >= delivery.len() / 2 {
                    // Test metadata and cache reconstruction mid-stream, while
                    // there may still be queued changes with missing parents.
                    let before = snapshot(&replica);
                    replica = Automerge::load(&replica.save()).unwrap();
                    assert_snapshot(&replica, &before);
                    reloaded = true;
                }
            }
            assert_eq!(replica.get_heads(), source.get_heads());
            assert_eq!(replica.hydrate(None), source.hydrate(None));
            assert_snapshot(&replica, &expected);
            assert_snapshot(&Automerge::load(&replica.save()).unwrap(), &expected);
        }

        branches.shuffle(&mut rng);
        let mut merged = Automerge::new();
        for branch in &mut branches {
            merged.merge(branch).unwrap();
        }
        assert_snapshot(&merged, &expected);

        // Fragments must cover all changes and remain loadable even when the
        // bundles themselves are delivered in a different order.
        let mut bundles: Vec<_> = expected.iter().map(|(_, bytes)| bytes).collect();
        bundles.shuffle(&mut rng);
        let mut loaded = Automerge::new();
        for bytes in bundles {
            loaded.load_incremental(bytes).unwrap();
        }
        assert_eq!(loaded.get_heads(), source.get_heads());
        assert_eq!(loaded.hydrate(None), source.hydrate(None));
        assert_snapshot(&loaded, &expected);
    }
}
