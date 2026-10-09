//! [`NameHashes::All`] and [`NameHashes::Anchors`] must load to the same
//! document; `Anchors` should be smaller.

use super::{Automerge, NameHashes, SaveOptions};
use crate::autocommit::AutoCommit;
use crate::tx::{CommitOptions, Transactable};
use crate::{ActorId, ROOT};

fn build(n: usize) -> AutoCommit {
    let mut doc = AutoCommit::new();
    for i in 0..n {
        doc.put(&ROOT, format!("k{i}"), i as i64).unwrap();
        doc.commit();
    }
    doc
}

#[test]
fn small_round_trips_through_the_rebuild() {
    let mut live = build(2000);
    let (lvl0, lvl1) = (live.fragments(0..=0).len(), live.fragments(1..).len());
    assert!(lvl0 > 0 && lvl1 > 0, "test needs both bands populated");

    let fast = live
        .document()
        .save_with(SaveOptions::default(), NameHashes::All);
    let small = live.save();
    assert!(
        small.len() < fast.len(),
        "NameHashes::Anchors should drop bytes ({} vs {})",
        small.len(),
        fast.len()
    );

    let rebuilt = Automerge::load(&small).unwrap();
    // level >= 1 fragments don't survive a round trip
    assert_eq!(
        rebuilt.fragments(0..=0).len(),
        lvl0,
        "loose commits rebuilt"
    );
    assert_eq!(
        rebuilt.save(),
        Automerge::load(&fast).unwrap().save(),
        "both paths land on the same document"
    );
}

/// Unlike a linear history, loose commits here depend on changes inside
/// fragments.
fn mesh(actors: usize, rounds: usize, burst: usize) -> AutoCommit {
    let mut peers: Vec<AutoCommit> = (0..actors)
        .map(|i| AutoCommit::new().with_actor(ActorId::from(&[i as u8, 1, 2, 3][..])))
        .collect();
    for r in 0..rounds {
        for (i, p) in peers.iter_mut().enumerate() {
            for b in 0..burst {
                p.put(&ROOT, format!("k{i}"), (r * burst + b) as i64)
                    .unwrap();
                p.commit_with(CommitOptions::default().with_time(0));
            }
        }
        let snaps: Vec<Vec<u8>> = peers.iter_mut().map(|p| p.save()).collect();
        for (i, p) in peers.iter_mut().enumerate() {
            for (j, s) in snaps.iter().enumerate() {
                if i != j {
                    let mut o = AutoCommit::load(s).unwrap();
                    p.merge(&mut o).unwrap();
                }
            }
        }
    }
    peers.swap_remove(0)
}

/// Missing anchors trip the `debug_assert` in
/// `Automerge::rebuild_missing_hashes` on load.
#[test]
fn anchors_floor_the_rebuild_on_a_concurrent_document() {
    let mut doc = mesh(16, 8, 3);
    assert!(
        !doc.fragments(1..).is_empty(),
        "fixture needs a fragment for the loose commits to reach into"
    );

    let fast = doc
        .document()
        .save_with(SaveOptions::default(), NameHashes::All);
    let small = doc.save();

    assert!(
        small.len() < fast.len(),
        "expected Small < Fast, got {} / {}",
        small.len(),
        fast.len()
    );
    let from_fast = Automerge::load(&fast).unwrap();
    let from_small = Automerge::load(&small).unwrap();
    assert_eq!(from_small.save(), from_fast.save());
}

#[test]
fn small_keeps_hashes_for_op_heavy_changes() {
    // The actor is pinned because which commits end up loose depends on
    // hashes. 400 changes stays inside `REHASH_BUDGET_US`, so only the op
    // count decides whether a hash is omitted.
    let build = |ops: usize| {
        let mut doc = AutoCommit::new().with_actor(ActorId::from(&b"frag"[..]));
        for c in 0..400 {
            for o in 0..ops {
                doc.put(&ROOT, format!("k{o}"), (c * ops + o) as i64)
                    .unwrap();
            }
            doc.commit();
        }
        let fast = doc
            .document()
            .save_with(SaveOptions::default(), NameHashes::All);
        let small = doc.save();
        assert!(
            doc.fragments(0..=0).len() > 5,
            "fixture needs loose commits"
        );
        assert_eq!(
            Automerge::load(&small).unwrap().save(),
            Automerge::load(&fast).unwrap().save(),
            "both formats land on the same document at {ops} ops"
        );
        (fast.len(), small.len())
    };

    let (fast, small) = build(2);
    assert!(
        small < fast,
        "small changes drop their hashes ({small} vs {fast})"
    );

    let (fast, small) = build(64);
    assert_eq!(small, fast, "op-heavy changes keep theirs");
}
