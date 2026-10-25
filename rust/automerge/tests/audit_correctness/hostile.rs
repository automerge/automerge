//! Hostile bundles.

use std::panic::{catch_unwind, AssertUnwindSafe};

use automerge::next::transaction::Transactable;
use automerge::next::Automerge;
use automerge::{AutomergeError, Bundle, MalformedBundle, ROOT};
use rand::prelude::*;

use crate::common::{actor, id, panic_msg, t0, Prefix};

fn commit(doc: &mut Automerge, key: &str, v: i64) {
    let mut tx = doc.transaction();
    tx.put(ROOT, key, v).unwrap();
    tx.commit_with(t0());
}

/// `(receiver, honest bundle bytes)` pairs.
fn cases() -> Vec<(String, Automerge, Vec<u8>)> {
    let mut out = Vec::new();

    // two actors, concurrent, merged
    let mut w = Automerge::new().with_actor(actor(1));
    commit(&mut w, "a", 1);
    let r1 = w.clone();
    let mut b = w.clone().with_actor(actor(2));
    commit(&mut w, "a", 2);
    commit(&mut w, "a", 3);
    commit(&mut b, "b", 1);
    commit(&mut b, "b", 2);
    w.merge(&mut b).unwrap();
    commit(&mut w, "a", 4);
    let heads = w.get_heads();
    out.push((
        "pair/after-a1".into(),
        r1.clone(),
        w.make_bundle(&heads, &[id(1, 1)]).unwrap().bytes(),
    ));
    out.push((
        "pair/save_after".into(),
        r1.clone(),
        w.save_after(&[id(1, 1)]).unwrap(),
    ));
    out.push((
        "pair/b-holder".into(),
        b.clone(),
        w.save_after(&b.get_heads()).unwrap(),
    ));
    let mut big = Automerge::new().with_actor(actor(3));
    for i in 0..600 {
        commit(&mut big, "x", i);
    }
    out.push((
        "big/empty-into-r1".into(),
        r1.clone(),
        big.bundle_document().unwrap().bytes(),
    ));

    // fragments
    let mut f = Automerge::new().with_actor(actor(1));
    let mut g = Automerge::new().with_actor(actor(2));
    for i in 0..1700 {
        commit(&mut f, "f", i);
        if i % 97 == 0 {
            commit(&mut g, "g", i);
            f.merge(&mut g).unwrap();
            g.merge(&mut f).unwrap();
        }
    }
    commit(&mut g, "g", -1);
    f.merge(&mut g).unwrap();
    let frags = f.fragments(..);
    let stack = f.bundles_for_fragments(frags.clone()).unwrap();
    eprintln!(
        "fragments: {} ({} level>0)",
        frags.len(),
        f.fragments(1..).len()
    );
    let mut r = Automerge::new();
    for (k, bytes) in stack.iter().enumerate() {
        if k % 3 == 0 || k + 1 == stack.len() {
            out.push((format!("frag/{k}"), r.clone(), bytes.clone()));
        }
        r.load_incremental(bytes).unwrap();
    }
    let mut gcd = r.clone();
    gcd.gc();
    let mid = f.get_heads();
    commit(&mut f, "f", 9999);
    out.push(("frag/after-gc".into(), gcd, f.save_after(&mid).unwrap()));
    out
}

#[derive(Debug, Clone)]
enum Mutation {
    HeadIndex(usize, u64),
    HeadHash(usize, [u8; 32]),
    AddHead([u8; 32], u64),
    AddCheckpoint(u64, [u8; 32]),
    DropCheckpoint(usize),
    CheckpointIndex(usize, u64),
    AddRetained(u64, [u8; 32]),
    RetainedIndex(usize, u64),
    RetainedHash(usize, [u8; 32]),
    DupRetained(usize),
    DropBoundary(usize),
    AddBoundary([u8; 32], u64, u64),
    BoundarySeq(usize, u64),
    BoundaryHash(usize, [u8; 32]),
    DepSeq(usize, u64),
    DepActor(usize, u64),
    SwapActors(usize, usize),
    ActorBytes(usize, Vec<u8>),
}

fn pool(p: &Prefix) -> Vec<[u8; 32]> {
    let mut v: Vec<[u8; 32]> = p.heads.iter().map(|(h, _)| *h).collect();
    v.extend(p.checkpoints.iter().map(|(_, h)| *h));
    v.extend(p.retained.iter().map(|(_, h)| *h));
    v.extend(p.boundary.iter().map(|(h, ..)| *h));
    v
}

fn random_mutation(rng: &mut SmallRng, p: &Prefix, members: u64) -> Option<Mutation> {
    let pool = pool(p);
    let hash = |rng: &mut SmallRng, level: bool| -> [u8; 32] {
        if !pool.is_empty() && rng.random_bool(0.6) {
            return pool[rng.random_range(0..pool.len())];
        }
        let mut h = [0u8; 32];
        rng.fill(&mut h[..]);
        if level {
            h[0] = 0;
        } else if h[0] == 0 {
            h[0] = 1;
        }
        h
    };
    let idx = |rng: &mut SmallRng| rng.random_range(0..members.max(1) + 1);
    let pick = |rng: &mut SmallRng, n: usize| (n > 0).then(|| rng.random_range(0..n));
    let na = p.actors.len();
    Some(match rng.random_range(0..18) {
        0 => Mutation::HeadIndex(pick(rng, p.heads.len())?, idx(rng)),
        1 => Mutation::HeadHash(pick(rng, p.heads.len())?, {
            let l = rng.random_bool(0.3);
            hash(rng, l)
        }),
        2 => {
            let lvl = rng.random_bool(0.3);
            Mutation::AddHead(hash(rng, lvl), idx(rng))
        }
        3 => Mutation::AddCheckpoint(idx(rng), hash(rng, true)),
        4 => Mutation::DropCheckpoint(pick(rng, p.checkpoints.len())?),
        5 => Mutation::CheckpointIndex(pick(rng, p.checkpoints.len())?, idx(rng)),
        6 => Mutation::AddRetained(idx(rng), hash(rng, false)),
        7 => Mutation::RetainedIndex(pick(rng, p.retained.len())?, idx(rng)),
        8 => Mutation::RetainedHash(pick(rng, p.retained.len())?, hash(rng, false)),
        9 => Mutation::DupRetained(pick(rng, p.retained.len())?),
        10 => Mutation::DropBoundary(pick(rng, p.boundary.len())?),
        11 => Mutation::AddBoundary(
            {
                let l = rng.random_bool(0.3);
                hash(rng, l)
            },
            rng.random_range(0..na.max(1) as u64),
            rng.random_range(1..40),
        ),
        12 => Mutation::BoundarySeq(pick(rng, p.boundary.len())?, rng.random_range(1..40)),
        13 => Mutation::BoundaryHash(pick(rng, p.boundary.len())?, {
            let l = rng.random_bool(0.3);
            hash(rng, l)
        }),
        14 => Mutation::DepSeq(pick(rng, p.deps.len())?, rng.random_range(1..40)),
        15 => Mutation::DepActor(
            pick(rng, p.deps.len())?,
            rng.random_range(0..na.max(1) as u64),
        ),
        16 if na >= 2 => {
            let a = rng.random_range(0..na);
            let b = rng.random_range(0..na);
            (a != b).then_some(Mutation::SwapActors(a, b))?
        }
        17 => Mutation::ActorBytes(pick(rng, na)?, vec![1, 1, 1, 1]),
        _ => return None,
    })
}

fn apply_mutation(p: &mut Prefix, m: &Mutation) {
    match m.clone() {
        Mutation::HeadIndex(i, x) => p.heads[i].1 = x,
        Mutation::HeadHash(i, h) => p.heads[i].0 = h,
        Mutation::AddHead(h, x) => p.heads.push((h, x)),
        Mutation::AddCheckpoint(x, h) => p.checkpoints.push((x, h)),
        Mutation::DropCheckpoint(i) => {
            p.checkpoints.remove(i);
        }
        Mutation::CheckpointIndex(i, x) => p.checkpoints[i].0 = x,
        Mutation::AddRetained(x, h) => p.retained.push((x, h)),
        Mutation::RetainedIndex(i, x) => p.retained[i].0 = x,
        Mutation::RetainedHash(i, h) => p.retained[i].1 = h,
        Mutation::DupRetained(i) => {
            let e = p.retained[i];
            p.retained.push(e)
        }
        Mutation::DropBoundary(i) => {
            p.boundary.remove(i);
        }
        Mutation::AddBoundary(h, a, s) => p.boundary.push((h, a, s)),
        Mutation::BoundarySeq(i, s) => p.boundary[i].2 = s,
        Mutation::BoundaryHash(i, h) => p.boundary[i].0 = h,
        Mutation::DepSeq(i, s) => p.deps[i].1 = s,
        Mutation::DepActor(i, a) => p.deps[i].0 = a,
        Mutation::SwapActors(a, b) => p.actors.swap(a, b),
        Mutation::ActorBytes(a, v) => p.actors[a] = v,
    }
}

/// What's wrong with `doc` after it accepted a hostile bundle, compared
/// to `honest` (the receiver after the honest bundle). Next mode trusts
/// hash claims it cannot check, so a forged hash it kept may be caught
/// later by audit mode or by a load that rehashes the save.
fn postconditions(doc: &mut Automerge, honest: &Automerge) -> Vec<String> {
    let mut bad = Vec::new();
    let r = catch_unwind(AssertUnwindSafe(|| {
        let mut bad = Vec::new();
        let forged = match doc.clone().enable_audit_mode() {
            Ok(a) => {
                a.validate_document();
                false
            }
            Err(_) => true,
        };
        if doc.get_heads() != honest.get_heads() {
            bad.push(format!(
                "heads {:?} != honest {:?}",
                doc.get_heads(),
                honest.get_heads()
            ));
        }
        if doc.hydrate(None).ok() != honest.hydrate(None).ok() {
            bad.push("values differ".into());
        }
        let saved = doc.save();
        match Automerge::load(&saved) {
            Ok(re) => {
                if re.get_heads() != doc.get_heads() {
                    bad.push("reload changes heads".into());
                }
            }
            Err(AutomergeError::MalformedBundle(
                MalformedBundle::WrongHeadHash { .. }
                | MalformedBundle::WrongCheckpointHash { .. }
                | MalformedBundle::WrongRetainedHash { .. },
            )) if forged => {}
            Err(e) => bad.push(format!("own save does not reload: {e}")),
        }
        let mut c = doc.clone().with_actor(actor(9));
        commit(&mut c, "z", 1);
        for a in [1u8, 2, 3] {
            let mut c = doc.clone().with_actor(actor(a));
            if c.has_change_id(&id(a, 1)) {
                commit(&mut c, "z", 2);
            }
        }
        doc.gc();
        let _ = doc.save();
        bad
    }));
    match r {
        Ok(b) => bad.extend(b),
        Err(e) => bad.push(format!("postcondition panicked: {}", panic_msg(&e))),
    }
    bad
}

fn fuzz(seed: u64, iters: usize) -> Vec<String> {
    let cases = cases();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut findings = Vec::new();
    for (name, recv, honest_bytes) in &cases {
        let mut honest_next = recv.clone();
        honest_next
            .load_incremental(honest_bytes)
            .expect("honest bundle applies");
        let honest_bundle = Bundle::try_from(&honest_bytes[..]).unwrap();
        let members = honest_bundle.iter_changes().unwrap().len() as u64;
        let base = Prefix::parse(honest_bytes);
        for it in 0..iters {
            let n = rng.random_range(1..=2);
            let mut p = base.clone();
            let mut muts = Vec::new();
            for _ in 0..n {
                if let Some(m) = random_mutation(&mut rng, &p, members) {
                    apply_mutation(&mut p, &m);
                    muts.push(m);
                }
            }
            if muts.is_empty() {
                continue;
            }
            let bytes = p.bytes();
            for audit in [false, true] {
                let Ok(parsed) = Bundle::try_from(&bytes[..]) else {
                    continue;
                };
                let res = catch_unwind(AssertUnwindSafe(|| {
                    let parsed = parsed.clone();
                    if audit {
                        let mut d = recv.clone().enable_audit_mode().unwrap();
                        let ok = d.apply_bundle(parsed).is_ok();
                        (ok, None)
                    } else {
                        let mut d = recv.clone();
                        let ok = d.apply_bundle(parsed).is_ok();
                        (ok, Some(d))
                    }
                }));
                let tag = format!("{name} it{it} audit={audit} {muts:?}");
                match res {
                    Err(e) => {
                        let m = panic_msg(&e);
                        if !m.contains("bundle left out") {
                            findings.push(format!("PANIC {tag}: {m}"));
                        }
                    }
                    Ok((true, Some(mut d))) => {
                        let bad = postconditions(&mut d, &honest_next);
                        if !bad.is_empty() {
                            findings.push(format!("ACCEPTED {tag}: {bad:?}"));
                        }
                    }
                    Ok(_) => {}
                }
            }
        }
    }
    findings
}

#[test]
#[cfg_attr(
    not(feature = "deep_fuzz"),
    ignore = "deep fuzz: run with --features deep_fuzz"
)]
fn structured_hostile_bundles_are_rejected_or_harmless() {
    let seed = std::env::var("SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let iters = std::env::var("ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let f = fuzz(seed, iters);
    for x in &f {
        eprintln!("{x}");
    }
    assert!(f.is_empty(), "{} findings", f.len());
}

/// `(writer, receiver)`: one actor, 1700 changes; the receiver loaded
/// the first 1600 from a save, so its interior hashes are freed.
fn long_history() -> (Automerge, Automerge) {
    let mut w = Automerge::new().with_actor(actor(1));
    for i in 0..1600 {
        commit(&mut w, "k", i);
    }
    let r = Automerge::load(&w.save()).unwrap();
    for i in 1600..1700 {
        commit(&mut w, "k", i);
    }
    assert!(!r.fragments(1..).is_empty(), "fixture needs fragments");
    assert!(
        matches!(
            r.change_id_to_hash(&id(1, 2)),
            Err(automerge::AutomergeError::AuditModeRequired)
        ),
        "fixture needs change 2's hash freed in the receiver"
    );
    (w, r)
}

/// A boundary entry can name any change the receiver already has. When
/// the receiver freed that change's hash, nothing checks the claim, and a
/// level > 0 claim makes an old change a new fragment head behind the
/// fragments that already cover it.
#[test]
fn a_bundle_claiming_a_fragment_level_hash_for_an_old_change_is_rejected_without_panicking() {
    let (w, r) = long_history();
    let honest = w.save_after(&r.get_heads()).unwrap();
    let mut p = Prefix::parse(&honest);
    let mut forged = [7u8; 32];
    forged[0] = 0; // level 1
    p.boundary.push((forged, 0, 2));
    let hostile = Bundle::try_from(&p.bytes()[..]).expect("parses");
    let res = catch_unwind(AssertUnwindSafe(|| {
        let mut r = r.clone();
        r.apply_bundle(hostile).map(|_| r)
    }));
    let Ok(res) = res else {
        panic!("applying the hostile bundle panicked");
    };
    if let Ok(r) = res {
        assert_eq!(
            r.fragments(1..).len(),
            w.fragments(1..).len(),
            "the forged level hash made a fragment"
        );
    }
}

/// Honest bundles applied again, or to a receiver that already holds
/// more, change nothing.
#[test]
fn reapplying_honest_bundles_changes_nothing() {
    let cs = cases();
    for (name, recv, bytes) in &cs {
        let mut once = recv.clone();
        once.load_incremental(bytes).unwrap();
        for audit in [false, true] {
            let mut twice = once.clone();
            let b = Bundle::try_from(&bytes[..]).unwrap();
            if audit {
                let mut a = twice.clone().enable_audit_mode().unwrap();
                a.apply_bundle(b)
                    .unwrap_or_else(|e| panic!("{name} audit: {e}"));
                assert_eq!(a.get_heads(), once.get_heads(), "{name} audit");
            } else {
                twice
                    .apply_bundle(b)
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(twice.get_heads(), once.get_heads(), "{name}");
                assert_eq!(twice.save(), once.save(), "{name}");
            }
        }
    }
    // every bundle into the last receiver (which holds everything)
    let last = cs.iter().find(|c| c.0 == "frag/after-gc").unwrap();
    let mut full = last.1.clone();
    full.load_incremental(&last.2).unwrap();
    let before = full.save();
    for (name, _, bytes) in cs.iter().filter(|c| c.0.starts_with("frag/")) {
        full.apply_bundle(Bundle::try_from(&bytes[..]).unwrap())
            .unwrap_or_else(|e| panic!("{name} into full: {e}"));
    }
    full.gc();
    assert_eq!(full.save(), before);
}
