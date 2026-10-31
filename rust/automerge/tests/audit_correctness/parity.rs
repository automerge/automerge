//! Audit vs next parity and fragment determinism.

use automerge::marks::{ExpandMark, Mark};
use automerge::next::transaction::Transactable;
use automerge::next::{AuditedAutoCommit, AutoCommit, Automerge, ReadDoc};
use automerge::{ObjType, ScalarValue, Value, ROOT};
use rand::prelude::*;

use crate::common::{actor, id, normalized, pair_mut, t0};

fn obj<T: ReadDoc>(doc: &T, key: &str) -> automerge::ObjId {
    match doc.get(ROOT, key).unwrap() {
        Some((Value::Object(_), id)) => id,
        other => panic!("{key}: {other:?}"),
    }
}

fn setup<T: Transactable>(doc: &mut T) {
    doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let map = obj(doc, "map");
    doc.put(&map, "counter", ScalarValue::counter(0)).unwrap();
}

fn edit<T: Transactable>(doc: &mut T, seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let map = obj(doc, "map");
    let list = obj(doc, "list");
    let text = obj(doc, "text");
    for _ in 0..1 + rng.random_range(0..3) {
        let key = format!("k{}", rng.random_range(0..8));
        let n: i64 = rng.random_range(0..1000);
        match rng.random_range(0..10) {
            0 => doc.put(&map, key, n).unwrap(),
            1 => {
                if doc.get(&map, &key).unwrap().is_some() {
                    doc.delete(&map, key).unwrap();
                }
            }
            2 => {
                let len = doc.length(&list);
                doc.insert(&list, rng.random_range(0..=len), n).unwrap();
            }
            3 => {
                let len = doc.length(&list);
                if len > 0 {
                    doc.delete(&list, rng.random_range(0..len)).unwrap();
                }
            }
            4 => {
                let len = doc.length(&text);
                doc.splice_text(&text, rng.random_range(0..=len), 0, "ab")
                    .unwrap();
            }
            5 => {
                let len = doc.length(&text);
                if len > 0 {
                    let del = 1 + rng.random_range(0..len.min(3));
                    doc.splice_text(&text, rng.random_range(0..=len - del), del as isize, "")
                        .unwrap();
                }
            }
            6 => {
                let len = doc.length(&text);
                if len >= 2 {
                    let start = rng.random_range(0..len - 1);
                    let end = start + 1 + rng.random_range(0..len - start - 1);
                    let mark = Mark::new("bold".into(), rng.random_bool(0.7), start, end);
                    doc.mark(&text, mark, ExpandMark::Both).unwrap();
                }
            }
            7 => doc.increment(&map, "counter", n % 10).unwrap(),
            8 => {
                let len = doc.length(&list);
                if len > 0 {
                    doc.put(&list, rng.random_range(0..len), n).unwrap();
                }
            }
            _ => {
                let inner = doc.put_object(&map, key, ObjType::Map).unwrap();
                doc.put(&inner, "x", n).unwrap();
            }
        }
    }
}

/// A shared history of `n` rounds across three actors, built in an
/// audited document set; returns the audited peers.
fn history(seed: u64, rounds: usize) -> Vec<AuditedAutoCommit> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut first = AutoCommit::new()
        .with_actor(actor(1))
        .enable_audit_mode()
        .unwrap();
    setup(&mut first);
    first.commit_with(t0());
    let mut peers = vec![first];
    for a in 2..=3u8 {
        let mut p = AutoCommit::new()
            .with_actor(actor(a))
            .enable_audit_mode()
            .unwrap();
        p.merge(&mut peers[0]).unwrap();
        peers.push(p);
    }
    for _ in 0..rounds {
        let i = rng.random_range(0..3);
        if rng.random_bool(0.8) {
            let s = rng.random();
            edit(&mut peers[i], s);
            peers[i].commit_with(t0());
        } else {
            let j = (i + 1 + rng.random_range(0..2)) % 3;
            let (a, b) = pair_mut(&mut peers, i, j);
            a.merge(b).unwrap();
        }
    }
    for i in 1..3 {
        let (first, other) = pair_mut(&mut peers, 0, i);
        first.merge(other).unwrap();
    }
    peers
}

/// Legacy changes, applied one at a time in a random topological order
/// into a `next` document, give the fragments, fragment bundles and save
/// that the audited document gives.
#[test]
fn one_legacy_change_at_a_time_in_any_order_matches_the_audited_document() {
    let mut peers = history(7, 2500);
    let audited = &mut peers[0];
    let reference_fragments = normalized(audited.fragments(..));
    assert!(audited.fragments(1..).len() >= 2, "fixture needs fragments");
    let reference_bundles = audited
        .bundles_for_fragments(audited.fragments(1..))
        .unwrap();
    let reference_save = audited.save();
    let changes = audited.get_changes(&[]).unwrap();
    for order_seed in 0..3u64 {
        // a random topological order
        let mut order = changes.clone();
        order.shuffle(&mut SmallRng::seed_from_u64(order_seed));
        let mut done = std::collections::HashSet::new();
        let mut topo = Vec::new();
        while topo.len() < order.len() {
            for c in &order {
                if !done.contains(&c.hash()) && c.deps().iter().all(|d| done.contains(d)) {
                    done.insert(c.hash());
                    topo.push(c.clone());
                }
            }
        }
        // manual GC: a legacy change may name a hash GC would free
        let mut next = Automerge::new().with_manual_gc();
        for c in topo.iter().cloned() {
            next.apply_changes([c]).unwrap();
        }
        next.gc();
        assert_eq!(
            normalized(next.fragments(..)),
            reference_fragments,
            "order {order_seed}: fragments"
        );
        let bundles = next
            .bundles_for_fragments(normalized(next.fragments(1..)))
            .unwrap();
        let mut rb = reference_bundles.clone();
        rb.sort();
        let mut nb = bundles.clone();
        nb.sort();
        assert!(
            rb == nb,
            "order {order_seed}: fragment bundles byte-identical"
        );
        assert!(
            next.save() == reference_save,
            "order {order_seed}: save byte-identical"
        );
    }
}

/// One step on a peer set, the same for audited and next peers.
macro_rules! step {
    ($peers:expr, $kind:expr, $i:expr, $j:expr, $seed:expr) => {{
        match $kind {
            0..=5 => {
                edit(&mut $peers[$i], $seed);
                $peers[$i].commit_with(t0());
            }
            6 => {
                let (a, b) = pair_mut(&mut $peers, $i, $j);
                a.merge(b).unwrap();
            }
            7 => {
                let (a, b) = pair_mut(&mut $peers, $i, $j);
                let heads: Vec<_> = a
                    .get_heads()
                    .into_iter()
                    .filter(|h| b.has_change_id(h))
                    .collect();
                let bytes = b.save_after(&heads).unwrap();
                a.load_incremental(&bytes).unwrap();
            }
            8 => {
                let (a, b) = pair_mut(&mut $peers, $i, $j);
                let heads: Vec<_> = a
                    .get_heads()
                    .into_iter()
                    .filter(|h| b.has_change_id(h))
                    .collect();
                for bytes in b.bundle_after(&heads).unwrap() {
                    a.load_incremental(&bytes).unwrap();
                }
            }
            _ => {
                let (a, b) = pair_mut(&mut $peers, $i, $j);
                let bytes = b.save();
                a.load_incremental(&bytes).unwrap();
            }
        }
    }};
}

fn cluster_parity(seed: u64, steps: usize) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut first = AutoCommit::new().with_actor(actor(1));
    setup(&mut first);
    first.commit_with(t0());
    let mut next: Vec<AutoCommit> = vec![first];
    for a in 2..=3u8 {
        let mut p = AutoCommit::new().with_actor(actor(a));
        p.merge(&mut next[0]).unwrap();
        next.push(p);
    }
    let mut audit: Vec<AuditedAutoCommit> = next
        .iter()
        .map(|d| d.clone().enable_audit_mode().unwrap())
        .collect();
    for p in next.iter_mut() {
        p.update_diff_cursor();
    }
    for p in audit.iter_mut() {
        p.update_diff_cursor();
    }
    for s in 0..steps {
        let kind = rng.random_range(0..10);
        let i = rng.random_range(0..3);
        let j = (i + 1 + rng.random_range(0..2)) % 3;
        let es: u64 = rng.random();
        step!(audit, kind, i, j, es);
        step!(next, kind, i, j, es);
        let what = format!("seed {seed} step {s} kind {kind} {i}<-{j}");
        assert_eq!(audit[i].get_heads(), next[i].get_heads(), "{what}: heads");
        let pa = audit[i].diff_incremental();
        let pn = next[i].diff_incremental();
        assert_eq!(format!("{pa:?}"), format!("{pn:?}"), "{what}: patches");
        if s % 50 == 0 {
            assert_eq!(
                audit[i].hydrate(ROOT, None).unwrap(),
                next[i].hydrate(ROOT, None).unwrap(),
                "{what}: values"
            );
            assert_eq!(
                normalized(audit[i].fragments(..)),
                normalized(next[i].fragments(..)),
                "{what}: fragments"
            );
            assert!(audit[i].save() == next[i].save(), "{what}: save bytes");
        }
    }
}

#[test]
fn audited_and_next_clusters_agree_on_patches_values_and_saves_seed_1() {
    cluster_parity(1, 2500);
}

#[test]
fn audited_and_next_clusters_agree_on_patches_values_and_saves_seed_2() {
    cluster_parity(2, 2500);
}

/// Every history query on a `next` document, at heads whose hashes were
/// freed, returns a result or an error; none panics.
#[test]
fn history_queries_at_freed_heads_never_panic() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let mut doc = AutoCommit::new().with_actor(actor(1));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.commit_with(t0());
    for i in 0..1500 {
        doc.splice_text(&text, 0, 0, "x").unwrap();
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit_with(t0());
    }
    let _ = doc.save();
    let old = [id(1, 700)];
    let cur = doc.get_heads();
    let mut panics = Vec::new();
    macro_rules! probe {
        ($name:expr, $e:expr) => {
            let mut d = doc.clone();
            #[allow(unused_mut)]
            let r = catch_unwind(AssertUnwindSafe(|| {
                let d = &mut d;
                let _ = $e(d);
            }));
            if r.is_err() {
                panics.push($name);
            }
        };
    }
    probe!("fork_at", |d: &mut AutoCommit| d.fork_at(&old).map(|_| ()));
    probe!("diff", |d: &mut AutoCommit| {
        let _ = d.diff(&old, &cur);
    });
    probe!("diff back", |d: &mut AutoCommit| {
        let _ = d.diff(&cur, &old);
    });
    probe!("get_changes", |d: &mut AutoCommit| d
        .get_changes(&old)
        .map(|_| ()));
    probe!("save_after", |d: &mut AutoCommit| d
        .save_after(&old)
        .map(|_| ()));
    probe!("bundle_after", |d: &mut AutoCommit| d
        .bundle_after(&old)
        .map(|_| ()));
    probe!("fragments_after", |d: &mut AutoCommit| d
        .fragments_after(&old)
        .map(|_| ()));
    probe!("hydrate at", |d: &mut AutoCommit| d
        .hydrate(ROOT, Some(&old))
        .map(|_| ()));
    probe!("text_at", |d: &mut AutoCommit| d
        .text_at(&text, &old)
        .map(|_| ()));
    probe!("get_cursor at", |d: &mut AutoCommit| d
        .get_cursor(&text, 3, Some(&old))
        .map(|_| ()));
    probe!("get_change_by_id", |d: &mut AutoCommit| d
        .get_change_by_id(&old[0])
        .map(|_| ()));
    probe!("change_ids_to_hashes", |d: &mut AutoCommit| d
        .change_ids_to_hashes(&old)
        .map(|_| ()));
    probe!("get_last_local_change", |d: &mut AutoCommit| d
        .get_last_local_change()
        .map(|_| ()));
    probe!("get_last_local_change_legacy", |d: &mut AutoCommit| d
        .get_last_local_change_legacy()
        .map(|_| ()));
    probe!("isolate", |d: &mut AutoCommit| d.isolate(&old));
    probe!("mask_author", |d: &mut AutoCommit| {
        d.mask_author(automerge::Author::from(&b"al"[..]), &old);
        d.put(ROOT, "m", 1).unwrap();
        d.commit_with(t0());
        d.save();
    });
    probe!("hash_for_opid", |d: &mut AutoCommit| {
        let (_, id) = d.get(ROOT, "k").unwrap().unwrap();
        d.hash_for_opid(&id).map(|_| ())
    });
    probe!("set_actor existing then commit", |d: &mut AutoCommit| {
        d.set_actor(actor(1));
        d.put(ROOT, "z", 1).unwrap();
        d.commit_with(t0());
    });
    probe!("legacy save reload", |d: &mut AutoCommit| {
        let b = d.save_with_options(automerge::next::SaveOptions {
            format: automerge::next::SaveFormat::Legacy,
            ..Default::default()
        });
        AutoCommit::load(&b).map(|_| ())
    });
    assert!(panics.is_empty(), "panicked: {panics:?}");
}

fn legacy() -> automerge::next::SaveOptions {
    automerge::next::SaveOptions {
        format: automerge::next::SaveFormat::Legacy,
        ..Default::default()
    }
}

/// A `next` document's legacy save equals the audited document's, loads
/// in audit mode with the same heads, and a `next` load of it has the
/// same fragments.
#[test]
fn legacy_save_of_a_next_document_matches_audit_mode() {
    let mut peers = history(3, 2500);
    let audited = &mut peers[0];
    let mut next = AutoCommit::load(&audited.save()).unwrap();
    let _ = next.save();
    // node order follows arrival order, so compare against a document
    // that arrived the same way
    let mut same_path = AutoCommit::load(&audited.save())
        .unwrap()
        .enable_audit_mode()
        .unwrap();
    let a = same_path.save_with_options(legacy());
    let n = next.save_with_options(legacy());
    assert!(a == n, "legacy saves byte-identical");
    let root = automerge::Automerge::load(&n).expect("legacy save loads in audit mode");
    assert_eq!(root.get_heads(), audited.get_head_hashes());
    let reloaded = AutoCommit::load(&n).expect("legacy save loads in next");
    assert_eq!(
        normalized(reloaded.fragments(..)),
        normalized(audited.fragments(..))
    );
    let mut un = AutoCommit::load_unverified_heads(&next.save()).unwrap();
    assert_eq!(un.get_heads(), next.get_heads());
    // round trips through modes
    let mut back = next.clone().enable_audit_mode().expect("into audit");
    assert!(back.save() == audited.save(), "audit round trip save");
    let mut again = back.disable_audit_mode();
    assert!(again.save() == next.save(), "next round trip save");
}

/// A masked author's history arriving in a `next` receiver (with saves,
/// so GC runs, and fragments forming) shows the same values and patches
/// as in an audited receiver, before and after reveal.
#[test]
fn masked_author_history_matches_audit_mode_across_gc() {
    let al: automerge::Author<'static> = automerge::Author::from(b"al".to_vec());
    let mut w = AutoCommit::new().with_author(Some(al.clone()));
    let text = w.put_object(ROOT, "text", ObjType::Text).unwrap();
    w.commit_with(t0());
    let mut next = AutoCommit::new().with_actor(actor(9));
    next.merge(&mut w).unwrap();
    let mut audit = next.clone().enable_audit_mode().unwrap();
    let boundary = next.get_heads();
    next.mask_author(al.clone(), &boundary);
    audit.mask_author(al.clone(), &boundary);
    next.update_diff_cursor();
    audit.update_diff_cursor();
    let mut rng = SmallRng::seed_from_u64(5);
    for round in 0..30 {
        for _ in 0..60 {
            let len = w.length(&text);
            w.splice_text(&text, rng.random_range(0..=len), 0, "w")
                .unwrap();
            w.put(ROOT, "k", rng.random_range(0..100i64)).unwrap();
            w.commit_with(t0());
        }
        // the receivers edit too
        next.put(ROOT, "r", round as i64).unwrap();
        next.commit_with(t0());
        audit.put(ROOT, "r", round as i64).unwrap();
        audit.commit_with(t0());
        if round % 2 == 0 {
            next.merge(&mut w).unwrap();
            audit.merge(&mut w).unwrap();
        } else {
            let b = w.save();
            next.load_incremental(&b).unwrap();
            audit.load_incremental(&b).unwrap();
        }
        let _ = next.save();
        let _ = audit.save();
        if round == 20 {
            next.reveal_author(&al);
            audit.reveal_author(&al);
        }
        let what = format!("round {round}");
        assert_eq!(next.get_heads(), audit.get_heads(), "{what}: heads");
        assert_eq!(
            format!("{:?}", next.diff_incremental()),
            format!("{:?}", audit.diff_incremental()),
            "{what}: patches"
        );
        assert_eq!(
            next.hydrate(ROOT, None).unwrap(),
            audit.hydrate(ROOT, None).unwrap(),
            "{what}: values"
        );
        assert_eq!(
            next.get_write_frontier(),
            audit.get_write_frontier(),
            "{what}: frontier"
        );
    }
    let n = AutoCommit::load(&next.save()).unwrap();
    assert_eq!(
        n.hydrate(ROOT, None).unwrap(),
        audit.hydrate(ROOT, None).unwrap()
    );
}

/// An audited and a `next` peer exchanging changes by every path agree
/// with two audited peers doing the same.
#[test]
fn mixed_mode_peers_agree_with_audited_peers() {
    let mut rng = SmallRng::seed_from_u64(11);
    let mut a = AutoCommit::new()
        .with_actor(actor(1))
        .enable_audit_mode()
        .unwrap();
    setup(&mut a);
    a.commit_with(t0());
    let mut n = AutoCommit::new().with_actor(actor(2));
    n.merge(&mut a).unwrap();
    let mut ra = a.clone();
    let mut rn = n.clone().enable_audit_mode().unwrap();
    for s in 0..3000 {
        let kind = rng.random_range(0..12);
        let seed: u64 = rng.random();
        let what = format!("step {s} kind {kind}");
        match kind {
            0..=3 => {
                edit(&mut a, seed);
                a.commit_with(t0());
                edit(&mut ra, seed);
                ra.commit_with(t0());
            }
            4..=7 => {
                edit(&mut n, seed);
                n.commit_with(t0());
                edit(&mut rn, seed);
                rn.commit_with(t0());
            }
            8 => {
                a.merge(&mut n).unwrap_or_else(|e| panic!("{what}: {e}"));
                ra.merge(&mut rn).unwrap();
            }
            9 => {
                n.merge(&mut a).unwrap_or_else(|e| panic!("{what}: {e}"));
                rn.merge(&mut ra).unwrap();
            }
            10 => {
                let heads: Vec<_> = n
                    .get_heads()
                    .into_iter()
                    .filter(|h| a.has_change_id(h))
                    .collect();
                for b in a
                    .bundle_after(&heads)
                    .unwrap_or_else(|e| panic!("{what}: {e}"))
                {
                    n.load_incremental(&b)
                        .unwrap_or_else(|e| panic!("{what}: {e}"));
                }
                rn.merge(&mut ra).unwrap();
            }
            _ => {
                let heads: Vec<_> = a
                    .get_heads()
                    .into_iter()
                    .filter(|h| n.has_change_id(h))
                    .collect();
                let b = n
                    .save_after(&heads)
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                a.load_incremental(&b)
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                ra.merge(&mut rn).unwrap();
            }
        }
        if s % 100 == 0 {
            let _ = n.save();
        }
        assert_eq!(a.get_heads(), ra.get_heads(), "{what}: a heads");
        assert_eq!(n.get_heads(), rn.get_heads(), "{what}: n heads");
        if s % 100 == 0 {
            assert_eq!(
                n.hydrate(ROOT, None).unwrap(),
                rn.hydrate(ROOT, None).unwrap(),
                "{what}"
            );
            assert_eq!(
                a.hydrate(ROOT, None).unwrap(),
                ra.hydrate(ROOT, None).unwrap(),
                "{what}"
            );
            assert_eq!(
                normalized(n.fragments(..)),
                normalized(rn.fragments(..)),
                "{what}: n fragments"
            );
            assert_eq!(
                normalized(a.fragments(..)),
                normalized(ra.fragments(..)),
                "{what}: a fragments"
            );
        }
    }
}

/// A `next` receiver holding a prefix of a peer's history applies the
/// peer's `bundle_after(its heads)` one bundle at a time, and both modes
/// of the sender produce the same bundles.
#[test]
fn bundle_after_applies_one_at_a_time_and_matches_audit_mode() {
    let mut peers = history(21, 2500);
    let audited = &mut peers[0];
    let mut sender = AutoCommit::load(&audited.save()).unwrap();
    let _ = sender.save();
    let changes = audited.get_changes(&[]).unwrap();
    for cut in [10usize, 300, 1000, 1700, 2200] {
        let prefix: Vec<_> = changes[..cut.min(changes.len())].to_vec();
        let mut r = AutoCommit::new().enable_audit_mode().unwrap();
        r.apply_changes(prefix).unwrap();
        let heads = r.get_heads();
        let mut r = r.disable_audit_mode();
        let _ = r.save();
        let sent = sender.bundle_after(&heads).unwrap();
        let reference = audited.bundle_after(&heads).unwrap();
        let (mut ss, mut rs) = (sent.clone(), reference.clone());
        ss.sort();
        rs.sort();
        if ss != rs {
            let fs = normalized(sender.fragments_after(&heads).unwrap());
            let fr = normalized(audited.fragments_after(&heads).unwrap());
            eprintln!(
                "cut {cut}: {} vs {} bundles; fragments equal: {}",
                sent.len(),
                reference.len(),
                fs == fr
            );
            for (x, y) in fs.iter().zip(fr.iter()) {
                if x != y {
                    eprintln!("  differ: {x:?}\n   vs   {y:?}");
                    break;
                }
            }
            let same: usize = ss.iter().filter(|b| rs.contains(b)).count();
            eprintln!("  {same} identical bundles");
        }
        assert!(
            ss == rs,
            "cut {cut}: bundle_after bytes equal in both modes"
        );
        for (k, b) in sent.iter().enumerate() {
            r.load_incremental(b)
                .unwrap_or_else(|e| panic!("cut {cut}: bundle {k}/{}: {e}", sent.len()));
        }
        assert_eq!(r.get_heads(), sender.get_heads(), "cut {cut}");
        assert_eq!(
            normalized(r.fragments(..)),
            normalized(audited.fragments(..)),
            "cut {cut}: fragments"
        );
    }
}

fn held(doc: &AutoCommit, all: &[automerge::ChangeId]) -> usize {
    all.iter()
        .filter(|id| matches!(doc.change_id_to_hash(id), Ok(Some(_))))
        .count()
}

/// A long-lived `next` AutoCommit that commits, merges and saves holds
/// no more hashes than a fresh load of its own save.
#[test]
fn a_long_lived_next_document_holds_what_a_fresh_load_holds() {
    let mut peers = history(5, 300);
    let mut doc = AutoCommit::load(&peers[0].save())
        .unwrap()
        .with_actor(actor(1));
    let mut other = AutoCommit::load(&peers[1].save())
        .unwrap()
        .with_actor(actor(2));
    let mut rng = SmallRng::seed_from_u64(3);
    for round in 0..40 {
        for _ in 0..50 {
            edit(&mut doc, rng.random());
            doc.commit_with(t0());
            edit(&mut other, rng.random());
            other.commit_with(t0());
        }
        match round % 3 {
            0 => {
                doc.merge(&mut other).unwrap();
            }
            1 => {
                let b = other.save_incremental();
                doc.load_incremental(&b).unwrap();
            }
            _ => {
                other.merge(&mut doc).unwrap();
            }
        }
        let _ = doc.save_incremental();
        let _ = other.save();
    }
    let _ = doc.save();
    let mut ids = Vec::new();
    for a in 1..=3u8 {
        for s in 1.. {
            if !doc.has_change_id(&id(a, s)) {
                break;
            }
            ids.push(id(a, s));
        }
    }
    let fresh = AutoCommit::load(&doc.save()).unwrap();
    let (h, f) = (held(&doc, &ids), held(&fresh, &ids));
    eprintln!(
        "{} changes, live holds {h}, fresh load holds {f}",
        ids.len()
    );
    assert!(h <= f, "live document holds {h} hashes, a fresh load {f}");
}
