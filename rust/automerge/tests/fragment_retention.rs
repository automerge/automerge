//! Hash retention of level-0 loose commits across save/load and merge.
//! A missing hash silently drops a loose commit from `fragments` rather
//! than erroring, so these tests pin the counts.

use automerge::ROOT;

use automerge::next::transaction::CommitOptions;
use automerge::next::{transaction::Transactable, AuditMode, Automerge};

fn build(n: usize) -> automerge::next::AutoCommit {
    let mut doc =
        automerge::next::AutoCommit::new().with_actor(automerge::ActorId::from(&[7u8; 16][..]));
    for i in 0..n {
        doc.put(&ROOT, format!("k{i}"), i as i64).unwrap();
        doc.commit_with(CommitOptions::default().with_time(0));
    }
    doc
}

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

#[test]
fn loose_commits_survive_save_load_without_audit_mode() {
    // ~1 in 256 changes lands at fragment_level >= 1
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
    // only level 0 must match exactly: level >= 1 can regain usurped fragments
    assert_eq!(
        loaded.fragments(0..=0).len(),
        live_lvl0,
        "level-0 loose commits survive too"
    );
}

#[test]
fn audit_mode_recovers_the_full_fragment_set() {
    let mut live = build(2000);
    let expected = (live.fragments(0..=0).len(), live.fragments(1..).len());

    let saved = live.save();
    let audited = Automerge::load(&saved)
        .unwrap()
        .enable_audit_mode()
        .unwrap();

    assert_eq!(
        (audited.fragments(0..=0).len(), audited.fragments(1..).len()),
        expected,
        "audit mode should reproduce the live document's fragment set"
    );
}

#[test]
fn fragments_does_not_silently_under_report() {
    let mut two = build(2);
    let loaded = Automerge::load(&two.save()).unwrap();
    assert!(
        !loaded.fragments(0..).is_empty(),
        "fragments() returned an empty Vec for a 2-change document"
    );
}

/// Level 0 only: a merge can regain usurped level >= 1 fragments.
#[test]
fn merge_keeps_the_loose_band() {
    let mut src = build(2000);
    let mut dest = automerge::next::AutoCommit::new();
    dest.merge(&mut src).unwrap();
    assert_eq!(
        dest.fragments(0..=0).len(),
        src.fragments(0..=0).len(),
        "a merged document reports the loose commits its source does"
    );
}

/// Plain `Automerge` so the hash GC runs.
#[test]
fn an_actor_tip_keeps_its_hash_once_a_fragment_covers_it() {
    use automerge::ActorId;
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from(&b"aaaa"[..]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    tx.commit();
    let tip = doc.get_head_hashes()[0];

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

/// Concurrent because a linear history has no level-0 anchors.
#[test]
fn the_loose_band_does_not_depend_on_audit_mode_when_concurrent() {
    let mut doc = mesh(6, 16, 4);
    // plain `Automerge` so the hash GC runs
    let loaded = Automerge::load(&doc.save()).unwrap();
    let band = loaded.fragments(0..=0).len();
    assert!(
        band > 0 && !loaded.fragments(1..).is_empty(),
        "fixture needs both bands populated"
    );

    let loaded = loaded.enable_audit_mode().unwrap();
    assert_eq!(
        loaded.fragments(0..=0).len(),
        band,
        "audit mode found loose commits the retained set had dropped"
    );
}

/// Each round every peer commits `burst` changes, then merges every other peer.
fn mesh(actors: usize, rounds: usize, burst: usize) -> automerge::next::AutoCommit {
    let mut peers: Vec<automerge::next::AutoCommit> = (0..actors)
        .map(|i| {
            automerge::next::AutoCommit::new()
                .with_actor(automerge::ActorId::from(&[i as u8, 1, 2, 3][..]))
        })
        .collect();
    for r in 0..rounds {
        for (i, p) in peers.iter_mut().enumerate() {
            for b in 0..burst {
                p.put(&ROOT, format!("k{i}"), (r * burst + b) as i64)
                    .unwrap();
                p.commit_with(automerge::next::transaction::CommitOptions::default().with_time(0));
            }
        }
        let snaps: Vec<Vec<u8>> = peers.iter_mut().map(|p| p.save()).collect();
        for (i, p) in peers.iter_mut().enumerate() {
            for (j, s) in snaps.iter().enumerate() {
                if i != j {
                    let mut o = automerge::next::AutoCommit::load(s).unwrap();
                    p.merge(&mut o).unwrap();
                }
            }
        }
    }
    peers.swap_remove(0)
}
