//! Hash retention lifecycle of `next` documents.

use automerge::next::transaction::Transactable;
use automerge::next::{AutoCommit, Automerge, ReadDoc};
use automerge::{ActorId, AutomergeError, ChangeId, Value, ROOT};
use std::num::NonZeroU64;

use crate::common::{actor, id, pair_mut, panic_msg, t0};

fn freed(doc: &AutoCommit, id: &ChangeId) -> bool {
    matches!(
        doc.change_id_to_hash(id),
        Err(AutomergeError::AuditModeRequired)
    )
}

/// An AutoCommit with `n` changes by actor 1 and fragments, never saved.
fn unsaved_history(n: u64) -> AutoCommit {
    let mut doc = AutoCommit::new().with_actor(actor(1));
    for i in 0..n {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit_with(t0());
    }
    assert!(!doc.fragments(1..).is_empty(), "fixture needs a fragment");
    doc
}

/// Isolating at a change whose hash is held, then saving (which collects
/// garbage), must not make the next edit in isolation panic.
#[test]
fn editing_in_isolation_after_a_save_does_not_panic() {
    let mut doc = unsaved_history(1500);
    let at = id(1, 10);
    assert!(!freed(&doc, &at), "AutoCommit has not collected yet");
    doc.isolate(std::slice::from_ref(&at)).unwrap();
    let _ = doc.save();
    assert!(!freed(&doc, &at), "an isolation head keeps its hash");
    doc.put(ROOT, "iso", 1).unwrap();
    doc.commit_with(t0());
    assert_eq!(
        doc.get(ROOT, "iso").unwrap().map(|v| v.0),
        Some(Value::from(1))
    );
    doc.integrate();
}

/// `fork_at` the current heads needs no history the document freed,
/// so it works after a save has collected garbage.
#[test]
fn fork_at_current_heads_works_after_a_save() {
    let mut doc = unsaved_history(1500);
    let _ = doc.save();
    let heads = doc.get_heads();
    let res = doc.fork_at(&heads);
    let mut forked = res.expect("fork_at current heads");
    assert_eq!(forked.get_heads(), heads);
}

/// Outside audit mode a legacy change whose dep hasn't arrived is rejected
/// rather than queued, so a bundle that later delivers the dep applies
/// cleanly and the document keeps loading its own save.
#[test]
fn a_legacy_change_ahead_of_its_dep_is_rejected_and_the_dep_bundle_applies() {
    let mut base = AutoCommit::new().with_actor(actor(9));
    base.put(ROOT, "base", 1).unwrap();
    base.commit_with(t0());
    let base_bytes = base.save();
    let base_heads = base.get_heads();

    let mut w = AutoCommit::load(&base_bytes).unwrap().with_actor(actor(1));
    let mut prefix = None;
    for i in 0..1500 {
        w.put(ROOT, "k", i as i64).unwrap();
        w.commit_with(t0());
        if i == 9 {
            prefix = Some(w.save());
        }
    }
    assert!(!w.fragments(1..).is_empty(), "fixture needs a fragment");
    let mut c = AutoCommit::load(&prefix.unwrap())
        .unwrap()
        .with_actor(actor(3));
    c.put(ROOT, "c", 1).unwrap();
    c.commit_with(t0());
    let legacy = c.get_last_local_change_legacy().unwrap().unwrap();

    let mut r = AutoCommit::load(&base_bytes).unwrap().with_actor(actor(2));
    assert_eq!(
        r.apply_changes([legacy.clone()]),
        Err(AutomergeError::MissingDeps)
    );
    assert_eq!(r.get_heads(), base_heads);
    r.load_incremental(&w.save_after(&base_heads).unwrap())
        .expect("the bundle applies");
    assert_eq!(r.get_heads(), w.get_heads());
    AutoCommit::load(&r.save()).expect("the receiver reloads its own save");
    // its dep's hash was never computed here: only audit mode can match it
    assert_eq!(
        r.apply_changes([legacy.clone()]),
        Err(AutomergeError::AuditModeRequired)
    );
    let mut audited = r.enable_audit_mode().unwrap();
    audited.apply_changes([legacy]).unwrap();
    assert!(audited.has_change_id(&id(3, 1)));
}

/// The last local change is always exportable as a bundle, including
/// when it is a fragment head whose parent's hash was freed.
#[test]
fn get_last_local_change_works_when_it_is_a_fragment_head() {
    let mut doc = AutoCommit::new().with_actor(actor(1));
    let mut i = 0i64;
    let mut parent_level = usize::MAX;
    loop {
        doc.put(ROOT, "k", i).unwrap();
        i += 1;
        let id = doc.commit_with(t0()).unwrap();
        let level = doc
            .change_id_to_hash(&id)
            .unwrap()
            .unwrap()
            .fragment_level();
        if level > 0 && parent_level == 0 && i > 300 {
            break;
        }
        parent_level = level;
    }
    let _ = doc.save();
    assert!(
        freed(&doc, &id(1, i as u64 - 1)),
        "fixture: the tip's parent is freed"
    );
    let b = doc
        .get_last_local_change()
        .expect("export the last local change");
    let b = b.expect("there is one");
    assert_eq!(b.heads().count(), 1);
    let legacy = doc.get_last_local_change_legacy();
    eprintln!(
        "legacy: {:?}",
        legacy.as_ref().map(|c| c.as_ref().map(|c| c.seq()))
    );
}

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
}

fn peer_actor(p: usize, k: usize) -> ActorId {
    ActorId::from(&[p as u8 + 1, k as u8, 7, 7][..])
}

fn commit(doc: &mut Automerge, rng: &mut Rng) -> ChangeId {
    let mut tx = doc.transaction();
    for _ in 0..1 + rng.below(3) {
        let key = format!("k{}", rng.below(6));
        tx.put(ROOT, key, rng.next() as i64).unwrap();
    }
    tx.commit_with(t0()).unwrap()
}

fn sync(peers: &mut [Automerge], a: usize, b: usize, how: usize, log: &[String]) {
    let (to, from) = pair_mut(peers, a, b);
    let res: Result<(), AutomergeError> = match how {
        0 => to.merge(from).map(|_| ()),
        1 => from
            .save_after(&to.get_heads())
            .and_then(|bytes| to.load_incremental(&bytes).map(|_| ())),
        2 => to.load_incremental(&from.save()).map(|_| ()),
        3 => {
            let stack = from.bundle_after(&to.get_heads()).unwrap_or_else(|e| {
                panic!("bundle_after export failed: {e:?} (log len {})", log.len())
            });
            stack
                .iter()
                .try_for_each(|b| to.load_incremental(b).map(|_| ()))
        }
        _ => match from.get_last_local_change_legacy() {
            // outside audit mode a change ahead of its deps is rejected
            Ok(Some(c)) if !c.deps().iter().all(|h| to.has_change(h).unwrap_or(false)) => Ok(()),
            Ok(Some(c)) => {
                let has = to.has_change(&c.hash());
                let has_id = to.has_change_id(&c.id());
                let deps: Vec<_> = c.deps().iter().map(|h| to.has_change(h)).collect();
                let r = to.apply_changes([c.clone()]);
                if r.is_err() {
                    eprintln!("legacy apply {r:?}: has {has:?} has_id {has_id} deps {deps:?} seq {} actor {}", c.seq(), c.actor_id());
                }
                r
            }
            Ok(None) | Err(AutomergeError::AuditModeRequired) => Ok(()),
            Err(e) => Err(e),
        },
    };
    if let Err(e) = res {
        panic!(
            "sync {b}->{a} how {how} failed: {e:?} ({:?} log len {})",
            log.first(),
            log.len()
        );
    }
}

fn check(doc: &mut Automerge, p: usize, log: &[String]) {
    let what = || format!("peer {p}, log {log:?}");
    let bytes = doc.save();
    let reloaded = Automerge::load(&bytes).unwrap_or_else(|e| panic!("reload {e:?} {}", what()));
    assert_eq!(reloaded.get_heads(), doc.get_heads(), "{}", what());
    doc.bundles_for_fragments(doc.fragments(..))
        .unwrap_or_else(|e| panic!("re-export {e:?} {}", what()));
}

fn held(doc: &Automerge, n: usize) -> Vec<ChangeId> {
    let mut out = Vec::new();
    for p in 0..n {
        for k in 0..2 {
            let a = peer_actor(p, k);
            for seq in 1.. {
                let id = ChangeId::from_parts(a.clone(), NonZeroU64::new(seq).unwrap());
                if !doc.has_change_id(&id) {
                    break;
                }
                if doc.change_id_to_hash(&id).is_ok() {
                    out.push(id);
                }
            }
        }
    }
    out
}

fn no(what: &str) -> bool {
    std::env::var(format!("C2_NO_{what}")).is_ok()
}

/// A randomized cluster of plain `next::Automerge` peers (GC runs inside
/// commits and applies), each switching between two actors of its own,
/// syncing by every route, converting through audit mode and back.
fn cluster_fuzz(seed: u64, rounds: usize) {
    const N: usize = 3;
    let mut rng = Rng(seed.wrapping_mul(0x9e3779b97f4a7c15) | 1);
    let mut peers: Vec<Automerge> = (0..N)
        .map(|p| Automerge::new().with_actor(peer_actor(p, 0)))
        .collect();
    let mut log = vec![format!("seed {seed}")];
    for round in 0..rounds {
        let p = rng.below(N);
        let mut kind = rng.below(20);
        if (kind == 18 && no("AUDIT")) || (kind == 19 && no("GC")) {
            kind = 0;
        }
        match kind {
            0..=11 => {
                if rng.below(4) == 0 && !no("SETACTOR") {
                    let k = rng.below(2);
                    log.push(format!("{round}: {p} set_actor {k}"));
                    peers[p].set_actor(peer_actor(p, k));
                }
                commit(&mut peers[p], &mut rng);
            }
            12..=17 => {
                let q = (p + 1 + rng.below(N - 1)) % N;
                let hows: Vec<usize> = std::env::var("C2_HOWS").map_or(vec![0, 1, 2, 3, 4], |v| {
                    v.chars().map(|c| c as usize - 48).collect()
                });
                let how = hows[rng.below(hows.len())];
                log.push(format!("{round}: sync {q}->{p} how {how}"));
                sync(&mut peers, p, q, how, &log);
            }
            18 => {
                log.push(format!("{round}: {p} audit round trip"));
                let doc = std::mem::take(&mut peers[p]);
                let actor = doc.get_actor().clone();
                let audited = doc
                    .enable_audit_mode()
                    .map_err(|e| format!("{:?}", e.error))
                    .unwrap();
                peers[p] = audited.disable_audit_mode().with_actor(actor);
            }
            _ => {
                log.push(format!("{round}: {p} gc"));
                peers[p].gc();
            }
        }
        if round % 97 == 0 {
            check(&mut peers[p], p, &log);
        }
    }
    // converge
    for _ in 0..2 {
        for p in 0..N {
            for q in 0..N {
                if p != q {
                    sync(&mut peers, p, q, 0, &log);
                }
            }
        }
    }
    let heads = peers[0].get_heads();
    let save0 = peers[0].save();
    let frags0 = {
        let mut f: Vec<_> = peers[0]
            .fragments(..)
            .into_iter()
            .map(|f| (f.head, f.level, f.boundary, f.checkpoints))
            .collect();
        f.sort();
        f
    };
    for (p, peer) in peers.iter_mut().enumerate() {
        assert_eq!(peer.get_heads(), heads);
        check(peer, p, &log);
        let mut f: Vec<_> = peer
            .fragments(..)
            .into_iter()
            .map(|f| (f.head, f.level, f.boundary, f.checkpoints))
            .collect();
        f.sort();
        assert_eq!(f, frags0, "peer {p} fragments differ");
        assert!(peer.save() == save0, "peer {p} whole save differs");
        peer.gc();
        let fresh = Automerge::load(&save0).unwrap();
        let (mine, theirs) = (held(peer, N), held(&fresh, N));
        let extra: Vec<_> = mine.iter().filter(|i| !theirs.contains(i)).collect();
        let missing: Vec<_> = theirs.iter().filter(|i| !mine.contains(i)).collect();
        assert!(
            extra.is_empty() && missing.is_empty(),
            "seed {seed} peer {p} holds {} hashes, a fresh load {}: extra {:?} missing {:?}",
            mine.len(),
            theirs.len(),
            extra,
            missing
        );
        for k in 0..2 {
            peer.set_actor(peer_actor(p, k));
            commit(peer, &mut rng);
        }
    }
}

#[test]
fn a_randomized_next_cluster_stays_consistent() {
    let seeds: u64 = std::env::var("C2_SEEDS").map_or(2, |v| v.parse().unwrap());
    let rounds: usize = std::env::var("C2_ROUNDS").map_or(1000, |v| v.parse().unwrap());
    let first: u64 = std::env::var("C2_FIRST").map_or(0, |v| v.parse().unwrap());
    for seed in first..first + seeds {
        cluster_fuzz(seed, rounds);
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Commit {
        p: usize,
        actor: Option<usize>,
        seed: u64,
    },
    Sync {
        to: usize,
        from: usize,
        how: usize,
    },
}

#[cfg(feature = "small_fragments")]
fn replay(steps: &[Step]) -> Result<(), String> {
    let mut peers: Vec<Automerge> = (0..3)
        .map(|p| Automerge::new().with_actor(peer_actor(p, 0)))
        .collect();
    for (i, s) in steps.iter().enumerate() {
        match *s {
            Step::Commit { p, actor, seed } => {
                if let Some(k) = actor {
                    peers[p].set_actor(peer_actor(p, k));
                }
                commit(&mut peers[p], &mut Rng(seed | 1));
            }
            Step::Sync { to, from, how } => {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    sync(&mut peers, to, from, how, &[])
                }));
                if let Err(e) = r {
                    return Err(format!("step {i}: {}", panic_msg(&e)));
                }
            }
        }
    }
    Ok(())
}

/// Three peers, each committing as one of two actors of its own, syncing
/// by `merge` and `save_after`: found by a randomized cluster (seed 7)
/// and reduced to these steps. Needs `small_fragments` for the
/// fragments to form.
#[cfg(feature = "small_fragments")]
#[test]
fn peers_switching_between_their_own_actors_can_always_sync() {
    let steps = STEPS_SEED7;
    replay(&steps).unwrap();
}

/// [`replay`], choosing each commit's content so that exactly the changes
/// in `heads` are fragment heads, whatever the fragment level function.
fn replay_mined(steps: &[Step], heads: &[ChangeId]) -> Result<(), String> {
    let mut peers: Vec<Automerge> = (0..3)
        .map(|p| Automerge::new().with_actor(peer_actor(p, 0)))
        .collect();
    for (i, s) in steps.iter().enumerate() {
        match *s {
            Step::Commit { p, actor, seed } => {
                if let Some(k) = actor {
                    peers[p].set_actor(peer_actor(p, k));
                }
                for attempt in 0u64.. {
                    let mut trial = peers[p].clone();
                    let id = commit(
                        &mut trial,
                        &mut Rng((seed ^ attempt.wrapping_mul(0x51ed27)) | 1),
                    );
                    let level = trial
                        .change_id_to_hash(&id)
                        .unwrap()
                        .unwrap()
                        .fragment_level();
                    if (level > 0) == heads.contains(&id) {
                        peers[p] = trial;
                        break;
                    }
                }
            }
            Step::Sync { to, from, how } => {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    sync(&mut peers, to, from, how, &[])
                }));
                if let Err(e) = r {
                    return Err(format!("step {i}: {}", panic_msg(&e)));
                }
            }
        }
    }
    Ok(())
}

/// Three peers, each committing as one of two actors of its own, syncing
/// by `merge` and `save_after`; fragment heads are mined so the history has
/// the shape the seed-7 reduction found. The last `merge` (and a
/// `save_after` at the same point) must succeed.
#[test]
fn merging_peers_that_switch_between_their_own_actors_succeeds() {
    let steps = STEPS_SEED7;
    let heads = [
        ChangeId::from_parts(peer_actor(2, 0), NonZeroU64::new(8).unwrap()),
        ChangeId::from_parts(peer_actor(1, 1), NonZeroU64::new(2).unwrap()),
        ChangeId::from_parts(peer_actor(2, 1), NonZeroU64::new(7).unwrap()),
    ];
    replay_mined(&steps, &heads).unwrap();
}

const STEPS_SEED7: [Step; 48] = [
    Step::Commit {
        p: 2,
        actor: None,
        seed: 13165196916295303411,
    },
    Step::Sync {
        to: 0,
        from: 2,
        how: 1,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 14419403938668968827,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 13651476809141368427,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 9444050314735959461,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 6815094158518984050,
    },
    Step::Sync {
        to: 1,
        from: 2,
        how: 1,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 15894081069743486736,
    },
    Step::Commit {
        p: 0,
        actor: None,
        seed: 1460617338289189263,
    },
    Step::Commit {
        p: 2,
        actor: Some(0),
        seed: 3142929745814754704,
    },
    Step::Commit {
        p: 0,
        actor: None,
        seed: 12860539643552868817,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 17300375228320278917,
    },
    Step::Commit {
        p: 1,
        actor: Some(0),
        seed: 1107764310000111218,
    },
    Step::Sync {
        to: 0,
        from: 1,
        how: 1,
    },
    Step::Commit {
        p: 2,
        actor: Some(0),
        seed: 4548650953707322529,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 14038684666028896270,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 10779198763931235557,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 12605804560480853155,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 4338661475551071572,
    },
    Step::Sync {
        to: 2,
        from: 1,
        how: 1,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 2259959360931025954,
    },
    Step::Commit {
        p: 0,
        actor: None,
        seed: 13381461266097227148,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 13647642612254781290,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 10210399377648505468,
    },
    Step::Sync {
        to: 0,
        from: 2,
        how: 0,
    },
    Step::Commit {
        p: 0,
        actor: Some(1),
        seed: 1637254770852964523,
    },
    Step::Sync {
        to: 2,
        from: 0,
        how: 1,
    },
    Step::Commit {
        p: 2,
        actor: Some(1),
        seed: 1621883661956427059,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 7771949798826346886,
    },
    Step::Sync {
        to: 1,
        from: 0,
        how: 0,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 18031948220382796144,
    },
    Step::Commit {
        p: 0,
        actor: None,
        seed: 1410856783337490831,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 7546329719967483477,
    },
    Step::Sync {
        to: 2,
        from: 0,
        how: 0,
    },
    Step::Commit {
        p: 0,
        actor: None,
        seed: 5388991195764792833,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 11118342443665235626,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 11809646218156725330,
    },
    Step::Sync {
        to: 2,
        from: 0,
        how: 0,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 18200666970880016441,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 5937314498948457720,
    },
    Step::Commit {
        p: 1,
        actor: Some(0),
        seed: 3473144302076367697,
    },
    Step::Sync {
        to: 1,
        from: 0,
        how: 0,
    },
    Step::Commit {
        p: 1,
        actor: Some(1),
        seed: 9813200143715528173,
    },
    Step::Commit {
        p: 1,
        actor: None,
        seed: 4882077682963716788,
    },
    Step::Commit {
        p: 2,
        actor: None,
        seed: 11583422068086599700,
    },
    Step::Sync {
        to: 2,
        from: 1,
        how: 0,
    },
    Step::Sync {
        to: 0,
        from: 2,
        how: 0,
    },
    Step::Sync {
        to: 1,
        from: 0,
        how: 0,
    },
];
