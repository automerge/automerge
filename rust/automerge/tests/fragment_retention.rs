//! Hash retention of level-0 loose commits across a save/load round trip.
//!
//! A change set carries the hashes its receiver has to retain: its heads,
//! its checkpoints (the level >= 1 members), and the retained section —
//! the loose commits, their anchors and the actor tips. Before that
//! section existed a change set named only heads and checkpoints, so a
//! whole-document change set (what `save` writes) delivered a run of loose
//! commits with the interior ones unnamed, and a loaded document reported
//! no level-0 fragments at all while its level >= 1 fragments survived
//! intact.
//!
//! The loss was silent: `ChangeGraph::loose_commit` resolves the commit's
//! own hash and every parent's through `hashes` and yields nothing if one
//! is missing, and `loose_commits` collects it with `filter_map` — an
//! empty `Vec`, not `AuditModeRequired`. So these tests pin the round
//! trip; that the hashes come off the wire rather than out of the
//! rebuild fallback is `change_sets_name_every_retained_hash` and the
//! debug assertion in `apply_change_set_inner`.

use automerge::{transaction::Transactable, AuditMode, Automerge, SaveFormat, SaveOptions, ROOT};

fn build(n: usize) -> automerge::AutoCommit {
    let mut doc = automerge::AutoCommit::new();
    for i in 0..n {
        doc.put(&ROOT, format!("k{i}"), i as i64).unwrap();
        doc.commit();
    }
    doc
}

/// A single parentless change always survived; two changes lost both. The
/// head's parent hash was not delivered, so `loose_commit` returned `None`
/// for the head, and the parent had no hash of its own either.
#[test]
fn two_changes_survive_save_load_without_audit_mode() {
    let mut one = build(1);
    let loaded_one = Automerge::load(&one.save()).unwrap();
    assert_eq!(
        loaded_one.fragments(0..).len(),
        1,
        "a single parentless change survives the round trip"
    );

    let mut two = build(2);
    assert_eq!(two.fragments(0..).len(), 2, "live doc reports both changes");

    let loaded_two = Automerge::load(&two.save()).unwrap();
    assert_eq!(loaded_two.audit_mode(), AuditMode::Disabled);
    assert_eq!(
        loaded_two.get_head_hashes().len(),
        1,
        "the head hash is retained across load"
    );
    assert_eq!(
        loaded_two.fragments(0..).len(),
        2,
        "both changes should still be reported"
    );
}

/// The fragment index and the loose commits both survive the round trip.
#[test]
fn loose_commits_survive_save_load_without_audit_mode() {
    // ~1 in 256 changes lands at fragment_level >= 1.
    let mut live = build(2000);
    let live_lvl0 = live.fragments(0..=0).len();
    let live_lvl1 = live.fragments(1..).len();
    assert!(
        live_lvl0 > 0 && live_lvl1 > 0,
        "test needs both bands populated (got {live_lvl0} / {live_lvl1})"
    );

    let saved = live.save();
    let loaded = Automerge::load(&saved).unwrap();

    assert!(
        !loaded.fragments(1..).is_empty(),
        "the fragment index survives the round trip"
    );
    // level 0 is the part that must match exactly — the level >= 1 shape
    // can gain usurped fragments, see `merge_keeps_the_loose_band`
    assert_eq!(
        loaded.fragments(0..=0).len(),
        live_lvl0,
        "level-0 loose commits survive too"
    );
}

/// Audit mode recovers the full set, confirming the data is present in the
/// document whatever the retained-hash set holds.
#[test]
fn audit_mode_recovers_the_full_fragment_set() {
    let mut live = build(2000);
    let expected = (live.fragments(0..=0).len(), live.fragments(1..).len());

    let saved = live.save();
    let mut audited = Automerge::load(&saved).unwrap();
    audited.enable_audit_mode().unwrap();

    assert_eq!(
        (audited.fragments(0..=0).len(), audited.fragments(1..).len()),
        expected,
        "audit mode should reproduce the live document's fragment set"
    );
}

/// `get_changes` reports `AuditModeRequired` when it needs hashes it does
/// not have; `fragments` has no such channel, so an under-retained loose
/// band could only show up as an empty `Vec`.
#[test]
fn fragments_does_not_silently_under_report() {
    let mut two = build(2);
    let loaded = Automerge::load(&two.save()).unwrap();
    assert!(
        !loaded.fragments(0..).is_empty(),
        "fragments() returned an empty Vec for a 2-change document"
    );
}

/// The same hole reached the document through any multi-change change set,
/// not just `save`: a merge delivers a run of loose commits with only its
/// heads named.
///
/// Level 0 only. The level >= 1 index shape does *not* always survive a
/// merge — a sender ships every level > 0 hash it knows as a checkpoint,
/// including heads of fragments its own index has since usurped, and the
/// receiver caches those as fragments again. Widening this assertion makes
/// it flake on ~1.5% of random 2000-change documents.
#[test]
fn merge_keeps_the_loose_band() {
    let mut src = build(2000);
    let mut dest = automerge::AutoCommit::new();
    dest.merge(&mut src).unwrap();
    assert_eq!(
        dest.fragments(0..=0).len(),
        src.fragments(0..=0).len(),
        "a merged document reports the loose commits its source does"
    );
}

/// `SaveFormat::Small` leaves the loose commits out and the load
/// rehashes it, so the band survives either way — the bytes are the only
/// difference.
#[test]
fn small_round_trips_through_the_rebuild() {
    let mut live = build(2000);
    let (lvl0, lvl1) = (live.fragments(0..=0).len(), live.fragments(1..).len());
    assert!(lvl0 > 0 && lvl1 > 0, "test needs both bands populated");
    // level 0 only: a loaded document's level >= 1 index can hold
    // fragments the sender had usurped, so its shape does not survive a
    // round trip — see `merge_keeps_the_loose_band`.

    let fast = live.save_with_options(SaveOptions {
        format: SaveFormat::Fast,
        ..Default::default()
    });
    let small = live.save();
    assert!(
        small.len() < fast.len(),
        "SaveFormat::Small should drop bytes ({} vs {})",
        small.len(),
        fast.len()
    );

    let rebuilt = Automerge::load(&small).unwrap();
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

/// `actors` peers; each round every peer commits `burst` changes and then
/// learns every other peer's round. Deps fan out across branches, so a
/// loose commit can depend on a change interior to a fragment — which a
/// linear history never produces.
fn mesh(actors: usize, rounds: usize, burst: usize) -> automerge::AutoCommit {
    use automerge::{transaction::CommitOptions, ActorId};
    let mut peers: Vec<automerge::AutoCommit> = (0..actors)
        .map(|i| automerge::AutoCommit::new().with_actor(ActorId::from(&[i as u8, 1, 2, 3][..])))
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
                    let mut o = automerge::AutoCommit::load(s).unwrap();
                    p.merge(&mut o).unwrap();
                }
            }
        }
    }
    peers.swap_remove(0)
}

/// On a concurrent document the loose commits depend on changes *inside*
/// fragments, so the load's rehash walk needs a floor or it descends
/// through those fragments and rehashes most of the document.
/// [`SaveFormat::Small`] names exactly those floor changes.
///
/// The floor property itself is the `debug_assert` in
/// `Automerge::rebuild_missing_hashes`, which this test's `Small` load
/// exercises — drop the anchors from the change set and it fires here.
#[test]
fn anchors_floor_the_rebuild_on_a_concurrent_document() {
    let mut doc = mesh(16, 8, 3);
    assert!(
        !doc.fragments(1..).is_empty(),
        "fixture needs a fragment for the loose commits to reach into"
    );

    let fast = doc.save_with_options(SaveOptions {
        format: SaveFormat::Fast,
        ..Default::default()
    });
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

/// A loose commit big enough to be expensive to rehash keeps its hash
/// even under [`SaveFormat::Small`]: the hash is a flat ~33 bytes but the
/// rehash costs ~0.6us per op, so past a point the trade stops paying.
#[test]
fn small_keeps_hashes_for_op_heavy_changes() {
    // ops per change either side of the threshold
    let build = |ops: usize| {
        let mut doc = automerge::AutoCommit::new();
        for c in 0..(16_000 / ops).max(600) {
            for o in 0..ops {
                doc.put(&ROOT, format!("k{o}"), (c * ops + o) as i64)
                    .unwrap();
            }
            doc.commit();
        }
        let fast = doc.save_with_options(SaveOptions {
            format: SaveFormat::Fast,
            ..Default::default()
        });
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

/// An actor's tip keeps its hash once a fragment covers it. Committing
/// as an actor names its latest change by hash, and a tip with no
/// loose-commit child below the frontier has nothing else holding it.
///
/// Plain `Automerge` deliberately: it is the `GcMode::Auto` type, so the
/// GC actually runs here.
#[test]
fn an_actor_tip_keeps_its_hash_once_a_fragment_covers_it() {
    use automerge::ActorId;
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from(&b"aaaa"[..]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    tx.commit();
    let tip = doc.get_head_hashes()[0];

    // bury it: another actor commits until fragments form over it
    doc.set_actor(ActorId::from(&b"bbbb"[..]));
    for i in 0..2000 {
        let mut tx = doc.transaction();
        tx.put(ROOT, "b", i as i64).unwrap();
        tx.commit();
    }
    assert!(
        !doc.fragments(1..).is_empty(),
        "fixture needs a fragment over the tip"
    );

    assert!(
        doc.hashes_to_change_ids(&[tip]).is_ok(),
        "the tip of actor aaaa lost its hash once a fragment covered it"
    );
}

/// The loose band must not depend on audit mode. `loose_commit` needs
/// every parent's hash, so an anchor — a covered level-0 parent — that
/// the GC frees silently drops its child from the band.
///
/// Concurrent deliberately: a linear history has no level-0 anchors, so
/// the sibling test over `build()` cannot see this. Observed on the live
/// document rather than across a load, because the load rebuilds what
/// the change set failed to name and would mask the loss.
#[test]
fn the_loose_band_does_not_depend_on_audit_mode_when_concurrent() {
    let mut doc = mesh(6, 16, 4);
    // plain `Automerge`: the `GcMode::Auto` type, so the GC runs
    let mut loaded = Automerge::load(&doc.save()).unwrap();
    let band = loaded.fragments(0..=0).len();
    assert!(
        band > 0 && !loaded.fragments(1..).is_empty(),
        "fixture needs both bands populated"
    );

    loaded.enable_audit_mode().unwrap();
    assert_eq!(
        loaded.fragments(0..=0).len(),
        band,
        "audit mode found loose commits the retained set had dropped"
    );
}
