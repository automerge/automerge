//! Correctness audit tests for the `hashless` branch. Each test's name
//! states the expected behaviour; a failing test is a finding.

use std::panic::{catch_unwind, AssertUnwindSafe};

use automerge::next::transaction::Transactable;
use automerge::next::{AutoCommit, Automerge, ReadDoc};
use automerge::{Bundle, ChangeHash, ROOT};
use rand::prelude::*;

mod common;
mod hostile;
mod legacy;
mod parity;
mod retention;
mod second;

use common::{
    actor, assert_no_panics, fix_checksum, mutate, mutation_fuzz, normalized, pair_mut, read_uleb,
    t0,
};

/// The writer has heads {A1, C1}, and `first` is its save holding only A1.
/// A receiver that loaded `first` and committed B1 must end up with heads
/// {B1, C1} after receiving the writer's document.
struct SharedHead {
    writer: AutoCommit,
    first: Vec<u8>,
    first_legacy: Vec<u8>,
    c1: ChangeHash,
}

fn writer_sharing_a_head() -> SharedHead {
    let mut writer = AutoCommit::new().with_actor(actor(1));
    writer.put(ROOT, "a", 1).unwrap();
    writer.commit_with(t0());
    let first = writer.save();
    let first_legacy = writer.save_with_options(legacy_format());
    let mut c = AutoCommit::new().with_actor(actor(3));
    c.put(ROOT, "c", 1).unwrap();
    c.commit_with(t0());
    let c1 = c.get_head_hashes()[0];
    writer.merge(&mut c).unwrap();
    assert_eq!(writer.get_head_hashes().len(), 2, "writer has two heads");
    SharedHead {
        writer,
        first,
        first_legacy,
        c1,
    }
}

fn sorted(mut hashes: Vec<ChangeHash>) -> Vec<ChangeHash> {
    hashes.sort();
    hashes
}

fn legacy_format() -> automerge::next::SaveOptions {
    automerge::next::SaveOptions {
        format: automerge::next::SaveFormat::Legacy,
        ..Default::default()
    }
}

#[test]
fn load_incremental_of_a_save_sharing_a_head_keeps_heads_minimal_next() {
    let mut s = writer_sharing_a_head();
    let mut r = AutoCommit::load(&s.first).unwrap().with_actor(actor(2));
    r.put(ROOT, "b", 1).unwrap();
    r.commit_with(t0());
    let b1 = r.get_head_hashes()[0];
    r.load_incremental(&s.writer.save()).unwrap();
    assert_eq!(
        r.get_head_hashes(),
        sorted(vec![b1, s.c1]),
        "a non-head change was re-added as a head"
    );
}

/// An audited receiver, given the writer's bundle-format and legacy saves.
#[test]
fn load_incremental_of_a_save_sharing_a_head_keeps_heads_minimal_audit() {
    use automerge::transaction::Transactable as _;
    let mut s = writer_sharing_a_head();
    let saves = [
        (s.first.clone(), s.writer.save()),
        (
            s.first_legacy.clone(),
            s.writer.save_with_options(legacy_format()),
        ),
    ];
    for (first, whole) in saves {
        let mut r = automerge::AutoCommit::load(&first)
            .unwrap()
            .with_actor(actor(2));
        r.put(ROOT, "b", 1).unwrap();
        r.commit();
        let expected = sorted(vec![r.get_heads()[0], s.c1]);
        r.load_incremental(&whole).unwrap();
        assert_eq!(
            r.get_heads(),
            expected,
            "a non-head change was re-added as a head"
        );
        let mut reloaded = automerge::AutoCommit::load(&r.save()).unwrap();
        assert_eq!(reloaded.get_heads(), expected);
    }
}

#[test]
fn apply_bundle_of_a_document_sharing_a_head_keeps_heads_minimal() {
    let mut s = writer_sharing_a_head();
    let mut r = Automerge::load(&s.first).unwrap().with_actor(actor(2));
    let mut tx = r.transaction();
    tx.put(ROOT, "b", 1).unwrap();
    tx.commit_with(t0());
    let b1 = r.get_head_hashes()[0];
    r.apply_bundle(s.writer.document().bundle_document().unwrap())
        .unwrap();
    assert_eq!(r.get_head_hashes(), sorted(vec![b1, s.c1]));
}

/// A few documents with varied content, plus a dependent bundle for each.
fn corpus() -> Vec<(Vec<u8>, Automerge)> {
    let mut out = Vec::new();

    // map puts by one actor
    let mut d = AutoCommit::new().with_actor(actor(1));
    for i in 0..5 {
        d.put(ROOT, format!("k{i}"), i as i64).unwrap();
        d.commit_with(t0());
    }
    let base = Automerge::load(&d.save()).unwrap();
    let heads = d.get_heads();
    d.put(ROOT, "x", "y").unwrap();
    d.commit_with(t0());
    out.push((d.save(), Automerge::new()));
    out.push((d.save_after(&heads).unwrap(), base));

    // text, marks, list, counter, two actors
    let mut d = AutoCommit::new().with_actor(actor(2));
    let text = d
        .put_object(ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    d.splice_text(&text, 0, 0, "hello world").unwrap();
    d.commit_with(t0());
    let list = d
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    d.insert(&list, 0, 1).unwrap();
    d.insert(&list, 1, "two").unwrap();
    d.put(ROOT, "ctr", automerge::ScalarValue::counter(1))
        .unwrap();
    d.commit_with(t0());
    let mut e = d.fork().with_actor(actor(3));
    e.splice_text(&text, 5, 1, "_").unwrap();
    e.increment(ROOT, "ctr", 2).unwrap();
    e.mark(
        &text,
        automerge::marks::Mark::new("bold".to_string(), true, 0, 4),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    e.commit_with(t0());
    d.delete(&list, 0).unwrap();
    d.commit_with(t0());
    let base = Automerge::load(&d.save()).unwrap();
    let heads = d.get_heads();
    d.merge(&mut e).unwrap();
    d.put(ROOT, "done", true).unwrap();
    d.commit_with(t0());
    out.push((d.save(), Automerge::new()));
    out.push((d.save_after(&heads).unwrap(), base));
    out
}

fn exercise(bytes: &[u8], base: &Automerge) -> Result<(), String> {
    let Ok(bundle) = Bundle::try_from(bytes) else {
        return Ok(());
    };
    let _ = bundle.heads().count();
    let _ = bundle.deps();
    let _ = bundle.iter_changes().map(|i| i.count());
    let _ = bundle.to_changes();
    let _ = bundle.audit::<ChangeHash>();
    let _ = bundle.bytes();
    let _ = bundle.bytes_uncompressed();

    let mut next = base.clone();
    if next.apply_bundle(bundle.clone()).is_ok() {
        let _ = next.get_heads();
        let _ = next.fragments(..);
        let saved = next.save();
        let _ = Automerge::load(&saved).map_err(|e| e.to_string())?;
        next.gc();
        let mut tx = next.transaction();
        tx.put(ROOT, "after", 1).unwrap();
        tx.commit_with(t0());
        let _ = next.save();
    }
    let mut fresh = Automerge::new();
    let _ = fresh.load_incremental(bytes);
    let _ = Automerge::load(bytes);

    let mut audit = base
        .clone()
        .enable_audit_mode()
        .map_err(|e| e.error.to_string())?;
    if audit.apply_bundle(bundle).is_ok() {
        let _ = audit.get_heads();
        let _ = audit.save();
    }
    Ok(())
}

#[test]
fn mutated_bundles_never_panic() {
    let outcome = mutation_fuzz(&corpus(), 1, 400, "", exercise);
    assert_no_panics(&outcome.panics);
}

fn fixture_base(ci: usize) -> Automerge {
    corpus().swap_remove(ci).1
}

/// Bundles the mutation fuzzer found, one per panic site, with the corpus
/// entry each applies to.
const HOSTILE_BUNDLES: &[(&str, &[u8], usize)] = &[
    (
        "c0_i19",
        include_bytes!("../fixtures/audit_hostile/c0_i19.bundle"),
        0,
    ),
    (
        "c0_i39",
        include_bytes!("../fixtures/audit_hostile/c0_i39.bundle"),
        0,
    ),
    (
        "c0_i52",
        include_bytes!("../fixtures/audit_hostile/c0_i52.bundle"),
        0,
    ),
    (
        "c0_i127",
        include_bytes!("../fixtures/audit_hostile/c0_i127.bundle"),
        0,
    ),
    (
        "c0_i432",
        include_bytes!("../fixtures/audit_hostile/c0_i432.bundle"),
        0,
    ),
    (
        "c0_i615",
        include_bytes!("../fixtures/audit_hostile/c0_i615.bundle"),
        0,
    ),
    (
        "c0_i645",
        include_bytes!("../fixtures/audit_hostile/c0_i645.bundle"),
        0,
    ),
    (
        "c0_i1300",
        include_bytes!("../fixtures/audit_hostile/c0_i1300.bundle"),
        0,
    ),
    (
        "c1_i209",
        include_bytes!("../fixtures/audit_hostile/c1_i209.bundle"),
        1,
    ),
    (
        "c1_i817",
        include_bytes!("../fixtures/audit_hostile/c1_i817.bundle"),
        1,
    ),
    (
        "c1_i846",
        include_bytes!("../fixtures/audit_hostile/c1_i846.bundle"),
        1,
    ),
    (
        "c2_i33",
        include_bytes!("../fixtures/audit_hostile/c2_i33.bundle"),
        2,
    ),
    (
        "c2_i978",
        include_bytes!("../fixtures/audit_hostile/c2_i978.bundle"),
        2,
    ),
    (
        "c3_i682",
        include_bytes!("../fixtures/audit_hostile/c3_i682.bundle"),
        3,
    ),
    (
        "c1_i51",
        include_bytes!("../fixtures/audit_hostile/c1_i51.bundle"),
        1,
    ),
    (
        "c3_i161",
        include_bytes!("../fixtures/audit_hostile/c3_i161.bundle"),
        3,
    ),
    (
        "c3_i2239",
        include_bytes!("../fixtures/audit_hostile/c3_i2239.bundle"),
        3,
    ),
    (
        "c3_i3724",
        include_bytes!("../fixtures/audit_hostile/c3_i3724.bundle"),
        3,
    ),
    (
        "c3_i5157",
        include_bytes!("../fixtures/audit_hostile/c3_i5157.bundle"),
        3,
    ),
];

#[test]
fn hostile_bundles_are_rejected_not_panicking() {
    let corpus = corpus();
    let panicked: Vec<&str> = HOSTILE_BUNDLES
        .iter()
        .filter(|(_, bytes, ci)| {
            catch_unwind(AssertUnwindSafe(|| exercise(bytes, &corpus[*ci].1))).is_err()
        })
        .map(|(name, ..)| *name)
        .collect();
    assert!(panicked.is_empty(), "panicked: {panicked:?}");
}

/// Byte range of the change-metadata column with `spec` in an
/// uncompressed bundle's bytes.
fn change_column_range(bytes: &[u8], spec: u64) -> std::ops::Range<usize> {
    let mut at = 4 + 4 + 1;
    read_uleb(bytes, &mut at); // chunk length
    let n_actors = read_uleb(bytes, &mut at);
    for _ in 0..n_actors {
        let n = read_uleb(bytes, &mut at) as usize;
        at += n;
    }
    let n_heads = read_uleb(bytes, &mut at);
    for _ in 0..n_heads {
        at += 32;
        read_uleb(bytes, &mut at);
    }
    for _ in 0..2 {
        // checkpoints, retained
        let n = read_uleb(bytes, &mut at);
        for _ in 0..n {
            read_uleb(bytes, &mut at);
            at += 32;
        }
    }
    let n_boundary = read_uleb(bytes, &mut at);
    for _ in 0..n_boundary {
        at += 32;
        read_uleb(bytes, &mut at);
        read_uleb(bytes, &mut at);
    }
    let n_deps = read_uleb(bytes, &mut at);
    for _ in 0..n_deps {
        read_uleb(bytes, &mut at);
        read_uleb(bytes, &mut at);
    }
    // the column section: dep hashes, actors, change columns
    let n = read_uleb(bytes, &mut at);
    at += 32 * n as usize;
    let n = read_uleb(bytes, &mut at);
    for _ in 0..n {
        let len = read_uleb(bytes, &mut at) as usize;
        at += len;
    }
    let n_cols = read_uleb(bytes, &mut at);
    let mut cols = Vec::new();
    for _ in 0..n_cols {
        let s = read_uleb(bytes, &mut at);
        let len = read_uleb(bytes, &mut at) as usize;
        cols.push((s, len));
    }
    let mut off = at;
    for (s, len) in cols {
        if s == spec {
            return off..off + len;
        }
        off += len;
    }
    panic!("column {spec:#x} not in bundle");
}

const NUM_OPS_SPEC: u64 = (1 << 4) | 2;

/// A one-change bundle whose `num_ops` column is a null run.
fn bundle_with_undecodable_change_meta() -> Vec<u8> {
    let mut w = AutoCommit::new().with_actor(actor(9));
    w.put(ROOT, "a", 1).unwrap();
    w.commit_with(t0());
    let bytes = w.document_mut().bundle_document().unwrap().bytes();
    let range = change_column_range(&bytes, NUM_OPS_SPEC);
    assert_eq!(range.len(), 2, "one member: run of one small value");
    let mut bad = bytes.clone();
    bad[range.start] = 0; // null run ..
    bad[range.start + 1] = 1; // .. of one
    fix_checksum(&bad)
}

/// A legacy save writes the actor table verbatim, and asserts (in debug
/// builds) that no actor is unused.
fn assert_no_stray_actor(r: &Automerge) {
    let saved = r.save_with_options(automerge::next::SaveOptions {
        format: automerge::next::SaveFormat::Legacy,
        ..Default::default()
    });
    let reloaded = Automerge::load(&saved).unwrap();
    assert_eq!(reloaded.get_heads(), r.get_heads());
}

#[test]
fn undecodable_change_metadata_is_rejected_when_parsed() {
    let bad = bundle_with_undecodable_change_meta();
    assert!(Bundle::try_from(&bad[..]).is_err());
}

/// c3_i682 parses, then fails while its ops are resolved: after its
/// actors were added.
#[test]
fn a_bundle_rejected_after_its_actors_are_added_leaves_no_stray_actor() {
    let bytes = include_bytes!("../fixtures/audit_hostile/c3_i682.bundle");
    let bundle = Bundle::try_from(&bytes[..]).unwrap();
    let mut r = fixture_base(3).with_actor(actor(1));
    let heads = r.get_heads();
    let err = r
        .apply_bundle(bundle)
        .expect_err("a pred the document lacks");
    eprintln!("rejected with: {err}");
    assert_eq!(r.get_heads(), heads);
    assert_no_stray_actor(&r);
}

/// Two bundles: a whole-document save and a dependent delta that inserts
/// into a list and text.
fn two_bundle_stack() -> (Vec<u8>, Vec<u8>) {
    let mut d = AutoCommit::new().with_actor(actor(4));
    let list = d
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    for i in 0..4 {
        d.insert(&list, i, i as i64).unwrap();
    }
    let text = d
        .put_object(ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    d.splice_text(&text, 0, 0, "abcd").unwrap();
    d.commit_with(t0());
    let first = d.save_nocompress();
    let heads = d.get_heads();
    let mut e = d.fork().with_actor(actor(5));
    e.insert(&list, 2, "x").unwrap();
    e.splice_text(&text, 2, 0, "Y").unwrap();
    e.commit_with(t0());
    d.insert(&list, 1, "w").unwrap();
    d.splice_text(&text, 1, 1, "Z").unwrap();
    d.commit_with(t0());
    d.merge(&mut e).unwrap();
    let second = d
        .save_after_with_options(
            &heads,
            automerge::next::SaveOptions {
                deflate: false,
                ..Default::default()
            },
        )
        .unwrap();
    (first, second)
}

#[test]
fn a_bundle_stack_loads_exactly_when_its_bundles_load_one_at_a_time() {
    let (first, second) = two_bundle_stack();
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let dump_dir = std::env::var("AUDIT_FUZZ_DUMP").ok();
    let iterations: usize = std::env::var("AUDIT_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let mut rng = SmallRng::seed_from_u64(42);
    let mut accepted_but_rejected_singly = Vec::new();
    let mut diverged = Vec::new();
    for it in 0..iterations {
        let mutated = mutate(&mut rng, &second);
        let mut stack = first.clone();
        stack.extend(&mutated);
        let feeds = catch_unwind(AssertUnwindSafe(|| Automerge::load(&stack)));
        let singly = catch_unwind(AssertUnwindSafe(|| {
            let mut doc = Automerge::load(&first).unwrap();
            doc.load_incremental(&mutated).map(|_| doc)
        }));
        match (feeds, singly) {
            (Ok(Ok(stacked)), Ok(Err(e))) => {
                if let Some(dir) = &dump_dir {
                    std::fs::write(format!("{dir}/stack_{it}.bundle"), &mutated).unwrap();
                }
                accepted_but_rejected_singly.push(format!(
                    "iter {it}: stack loaded with heads {:?}, but one at a time: {e}",
                    stacked.get_heads()
                ));
            }
            (Ok(Ok(stacked)), Ok(Ok(single))) => {
                if stacked.get_heads() != single.get_heads() || stacked.save() != single.save() {
                    if let Some(dir) = &dump_dir {
                        std::fs::write(format!("{dir}/diverge_{it}.bundle"), &mutated).unwrap();
                    }
                    diverged.push(format!("iter {it}"));
                }
            }
            _ => {}
        }
    }
    std::panic::set_hook(prev);
    assert!(
        accepted_but_rejected_singly.is_empty() && diverged.is_empty(),
        "{} stacks accepted that are rejected one bundle at a time; {} diverged:\n{}\n{}",
        accepted_but_rejected_singly.len(),
        diverged.len(),
        accepted_but_rejected_singly
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n"),
        diverged
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

struct Peer {
    doc: AutoCommit,
    /// every commit of this peer as a one-change bundle, in order
    outbox: Vec<Vec<u8>>,
}

fn commit_random(p: &mut Peer, rng: &mut SmallRng, n: u64) {
    match rng.random_range(0..3) {
        0 => p.doc.put(ROOT, format!("k{}", n % 7), n as i64).unwrap(),
        1 => {
            let list = match p.doc.get(ROOT, "list").unwrap() {
                Some((_, id)) => id,
                None => p
                    .doc
                    .put_object(ROOT, "list", automerge::ObjType::List)
                    .unwrap(),
            };
            let len = p.doc.length(&list);
            p.doc
                .insert(&list, rng.random_range(0..=len), n as i64)
                .unwrap();
        }
        _ => {
            let text = match p.doc.get(ROOT, "text").unwrap() {
                Some((_, id)) => id,
                None => p
                    .doc
                    .put_object(ROOT, "text", automerge::ObjType::Text)
                    .unwrap(),
            };
            let len = p.doc.length(&text);
            p.doc
                .splice_text(&text, rng.random_range(0..=len), 0, &format!("{n}"))
                .unwrap();
        }
    }
    p.doc.commit_with(t0());
    let bundle = p
        .doc
        .get_last_local_change()
        .expect("own last change is exportable")
        .expect("just committed");
    p.outbox.push(bundle.bytes());
}

/// Checks that hold for every document at every step.
fn check_peer(p: &mut Peer, what: &str) {
    p.doc.document_mut().gc();
    let saved = p.doc.save();
    let mut reloaded = AutoCommit::load(&saved).unwrap_or_else(|e| panic!("{what}: reload: {e}"));
    assert_eq!(
        reloaded.get_heads(),
        p.doc.get_heads(),
        "{what}: heads round-trip"
    );
    let fragments = p.doc.fragments(..);
    let bundles = p
        .doc
        .bundles_for_fragments(fragments.clone())
        .unwrap_or_else(|e| panic!("{what}: bundles_for_fragments: {e}"));
    let mut rebuilt = Automerge::new();
    for b in &bundles {
        rebuilt
            .load_incremental(b)
            .unwrap_or_else(|e| panic!("{what}: reload fragment bundle: {e}"));
    }
    assert_eq!(
        rebuilt.get_heads(),
        p.doc.get_heads(),
        "{what}: fragments cover the document"
    );
}

fn exchange(peers: &mut [Peer], a: usize, b: usize, mode: usize) {
    let (pa, pb) = pair_mut(peers, a, b);
    match mode {
        0 => {
            pa.doc.merge(&mut pb.doc).unwrap();
        }
        1 => {
            pa.doc.load_incremental(&pb.doc.save()).unwrap();
        }
        2 => {
            let bundles = pb
                .doc
                .bundles_for_fragments(pb.doc.fragments(..))
                .unwrap_or_else(|e| panic!("bundles_for_fragments on peer {b}: {e}"));
            for bytes in bundles {
                pa.doc.load_incremental(&bytes).unwrap();
            }
        }
        3 => match pb.doc.save_after(&pa.doc.get_heads()) {
            Ok(bytes) => {
                pa.doc.load_incremental(&bytes).unwrap();
            }
            Err(_) => {
                pa.doc.merge(&mut pb.doc).unwrap();
            }
        },
        4 => {
            let mut fresh = AutoCommit::load(&pb.doc.save()).unwrap();
            pa.doc.merge(&mut fresh).unwrap();
        }
        _ => {
            // one change per bundle, in commit order; deps from third
            // peers may be missing, those are skipped
            for bytes in pb.outbox.clone() {
                let bundle = Bundle::try_from(&bytes[..]).unwrap();
                match pa.doc.document_mut().apply_bundle(bundle) {
                    Ok(()) | Err(automerge::AutomergeError::MissingDeps) => {}
                    Err(e) => panic!("one-change bundle: {e}"),
                }
            }
        }
    }
}

fn retention_fuzz(seed: u64, n_peers: usize, rounds: usize, burst: usize) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut peers: Vec<Peer> = (0..n_peers)
        .map(|i| Peer {
            doc: AutoCommit::new().with_actor(actor(10 + i as u8)),
            outbox: Vec::new(),
        })
        .collect();
    let mut n = 0u64;
    for round in 0..rounds {
        for p in peers.iter_mut() {
            for _ in 0..burst {
                n += 1;
                commit_random(p, &mut rng, n);
            }
        }
        for _ in 0..n_peers {
            let a = rng.random_range(0..n_peers);
            let b = rng.random_range(0..n_peers);
            if a == b {
                continue;
            }
            let mode = rng.random_range(0..6);
            exchange(&mut peers, a, b, mode);
            check_peer(
                &mut peers[a],
                &format!("seed {seed} round {round} mode {mode}"),
            );
        }
    }
    // converge
    for _ in 0..2 {
        for a in 0..n_peers {
            for b in 0..n_peers {
                if a != b {
                    exchange(&mut peers, a, b, 0);
                }
            }
        }
    }
    let heads = peers[0].doc.get_heads();
    for (i, p) in peers.iter_mut().enumerate() {
        assert_eq!(p.doc.get_heads(), heads, "peer {i} converged");
        check_peer(p, &format!("seed {seed} final peer {i}"));
    }
    // audit parity: fragments and saves agree with an audited copy
    let mut audited: Vec<_> = peers
        .iter()
        .map(|p| p.doc.clone().enable_audit_mode().unwrap())
        .collect();
    // a whole save lists changes in arrival order, so compare content
    let reference = normalized(audited[0].fragments(..));
    for (i, a) in audited.iter_mut().enumerate() {
        assert_eq!(
            normalized(a.fragments(..)),
            reference,
            "audited fragments of peer {i}"
        );
        let reloaded = automerge::Automerge::load(&a.save()).unwrap();
        assert_eq!(
            reloaded.get_heads(),
            a.get_head_hashes(),
            "audited save of peer {i}"
        );
        assert_eq!(
            normalized(peers[i].doc.fragments(..)),
            normalized(a.fragments(..)),
            "peer {i}: next fragments equal the audited fragments"
        );
    }
}

#[test]
fn retention_survives_random_multi_peer_exchange_seed_1() {
    retention_fuzz(1, 4, 40, 6);
}

#[test]
fn retention_survives_random_multi_peer_exchange_seed_2() {
    retention_fuzz(2, 3, 60, 8);
}

#[test]
fn retention_survives_random_multi_peer_exchange_seed_3() {
    retention_fuzz(3, 5, 30, 10);
}

#[test]
fn save_nocompress_output_parses_as_a_bundle() {
    for n in [1, 50] {
        let mut w = AutoCommit::new().with_actor(actor(9));
        for i in 0..n {
            w.put(ROOT, format!("k{i}"), i as i64).unwrap();
            w.commit_with(t0());
        }
        let plain = w.save_nocompress();
        Bundle::try_from(&w.save()[..]).expect("deflated save parses");
        Bundle::try_from(&plain[..]).expect("uncompressed save parses");
        AutoCommit::load(&plain).expect("uncompressed save loads");
    }
}

#[test]
fn audit_save_nocompress_loads() {
    use automerge::transaction::Transactable as _;
    let mut w = automerge::AutoCommit::new().with_actor(actor(9));
    w.put(ROOT, "a", 1).unwrap();
    w.commit();
    let plain = w.save_nocompress();
    automerge::AutoCommit::load(&plain).expect("audit save_nocompress loads");
}

#[test]
fn bytes_uncompressed_of_a_bundle_parses() {
    let mut w = Automerge::new().with_actor(actor(9));
    {
        let mut tx = w.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit_with(t0());
    }
    let bundle = w.bundle_document().unwrap();
    let plain = bundle.bytes_uncompressed();
    Bundle::try_from(&plain[..]).expect("bytes_uncompressed parses");
    // and of a parsed bundle
    let reparsed = Bundle::try_from(&bundle.bytes()[..]).unwrap();
    Bundle::try_from(&reparsed.bytes_uncompressed()[..])
        .expect("reparsed bytes_uncompressed parses");
}

/// Returns `(receiver, writer)`: the receiver holds actor a's long
/// history with fragments (interior hashes freed); the writer forked at
/// a's 10th change and committed one change on top of it.
fn overlapping_peer_with_freed_parent() -> (AutoCommit, AutoCommit) {
    let mut a = AutoCommit::new().with_actor(actor(1));
    let mut prefix = None;
    for i in 0..1500 {
        a.put(ROOT, "k", i as i64).unwrap();
        a.commit_with(t0());
        if i == 9 {
            prefix = Some(a.save());
        }
    }
    let prefix = prefix.unwrap();
    assert!(!a.fragments(1..).is_empty(), "fixture needs a fragment");
    let p = automerge::ChangeId::from_parts(actor(1), std::num::NonZeroU64::new(10).unwrap());
    a.document_mut().gc();
    assert!(
        matches!(
            a.change_id_to_hash(&p),
            Err(automerge::AutomergeError::AuditModeRequired)
        ),
        "fixture needs the 10th change's hash freed in the receiver"
    );
    let mut w = AutoCommit::load(&prefix).unwrap().with_actor(actor(2));
    w.put(ROOT, "w", 1).unwrap();
    w.commit_with(t0());
    (a, w)
}

#[test]
fn loading_a_peers_whole_save_over_a_freed_interior_parent_does_not_panic() {
    let (mut r, mut w) = overlapping_peer_with_freed_parent();
    let before = r.get_heads();
    r.load_incremental(&w.save()).unwrap();
    assert_eq!(r.get_heads().len(), before.len() + 1);
    r.document_mut().gc();
    let _ = r.save();
}

#[test]
fn merging_a_peer_over_a_freed_interior_parent_does_not_panic() {
    let (mut r, mut w) = overlapping_peer_with_freed_parent();
    let before = r.get_heads();
    r.merge(&mut w).unwrap();
    assert_eq!(r.get_heads().len(), before.len() + 1);
    r.document_mut().gc();
    let _ = r.save();
}

#[test]
fn compressed_and_uncompressed_bytes_of_a_tiny_bundle_differ_only_in_compression() {
    let mut w = Automerge::new().with_actor(actor(9));
    {
        let mut tx = w.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit_with(t0());
    }
    let bundle = w.bundle_document().unwrap();
    let c = bundle.bytes();
    let u = bundle.bytes_uncompressed();
    eprintln!("compressed   ({}): {:02x?}", c.len(), &c[..]);
    eprintln!("uncompressed ({}): {:02x?}", u.len(), &u[..]);
    assert_eq!(c, u, "nothing in a tiny bundle is large enough to deflate");
}

/// `(a, b)`: b has a long history with fragments; a holds b's first five
/// changes plus a concurrent change c2 that b holds loose.
fn peers_sharing_a_loose_change_above_a_fragment() -> (AutoCommit, AutoCommit) {
    let mut b = AutoCommit::new().with_actor(actor(2));
    for i in 0..3 {
        b.put(ROOT, "b", i).unwrap();
        b.commit_with(t0());
    }
    let mut c = AutoCommit::load(&b.save()).unwrap().with_actor(actor(3));
    c.put(ROOT, "c", 1).unwrap();
    c.commit_with(t0());
    b.merge(&mut c).unwrap();
    for i in 3..5 {
        b.put(ROOT, "b", i).unwrap();
        b.commit_with(t0());
    }
    let mut a = AutoCommit::load(&b.save()).unwrap().with_actor(actor(1));
    for i in 5..1500 {
        b.put(ROOT, "b", i).unwrap();
        b.commit_with(t0());
    }
    assert!(
        !b.fragments(1..).is_empty(),
        "fixture needs a fragment in b"
    );
    // c2 depends only on c1: concurrent with everything b did after c1
    c.put(ROOT, "c", 2).unwrap();
    c.commit_with(t0());
    a.merge(&mut c).unwrap();
    b.merge(&mut c).unwrap();
    b.document_mut().gc();
    let b5 = automerge::ChangeId::from_parts(actor(2), std::num::NonZeroU64::new(5).unwrap());
    assert!(
        matches!(
            b.change_id_to_hash(&b5),
            Err(automerge::AutomergeError::AuditModeRequired)
        ),
        "fixture needs b's 5th change interior to a fragment, hash freed"
    );
    (a, b)
}

#[test]
fn merge_from_a_next_document_whose_fragment_the_receivers_frontier_cuts() {
    let (mut a, mut b) = peers_sharing_a_loose_change_above_a_fragment();
    a.merge(&mut b).expect("merge between next documents");
    assert_eq!(a.get_heads(), b.get_heads());
}

#[test]
fn save_after_from_a_next_document_whose_fragment_the_receivers_frontier_cuts() {
    let (mut a, mut b) = peers_sharing_a_loose_change_above_a_fragment();
    let bytes = b
        .save_after(&a.get_heads())
        .expect("save_after between next documents");
    a.load_incremental(&bytes).unwrap();
    assert_eq!(a.get_heads(), b.get_heads());
}

/// Outside audit mode a change whose deps are missing is rejected, not
/// queued, and the document is left as it was.
#[test]
fn a_next_document_rejects_a_change_whose_deps_are_missing() {
    use automerge::transaction::Transactable as _;
    let mut legacy = automerge::AutoCommit::new().with_actor(actor(5));
    legacy.put(ROOT, "x", 1).unwrap();
    legacy.commit();
    legacy.put(ROOT, "x", 2).unwrap();
    legacy.commit();
    let changes = legacy.get_changes(&[]);

    let mut next = AutoCommit::new().with_actor(actor(1));
    next.put(ROOT, "a", 1).unwrap();
    next.commit_with(t0());
    let heads = next.get_heads();
    assert_eq!(
        next.apply_changes([changes[1].clone()]),
        Err(automerge::AutomergeError::MissingDeps)
    );
    assert_eq!(next.get_heads(), heads);
    let mut reloaded = AutoCommit::load(&next.save()).expect("a document reloads its own save");
    assert_eq!(reloaded.get_heads(), heads);
    next.apply_changes(changes).unwrap();
    assert_eq!(
        next.get(ROOT, "x").unwrap().unwrap().0,
        automerge::Value::from(2)
    );
}

/// Loading two whole-document saves that overlap, in one `load`, must give
/// the same document as loading them one after the other.
#[test]
fn loading_overlapping_saves_in_one_stack_equals_loading_them_in_turn() {
    let mut a = AutoCommit::new().with_actor(actor(1));
    let list = a
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    for i in 0..5 {
        a.insert(&list, i, i as i64).unwrap();
        a.commit_with(t0());
    }
    let first = a.save();
    let mut b = AutoCommit::load(&first).unwrap().with_actor(actor(2));
    b.insert(&list, 2, "b").unwrap();
    b.commit_with(t0());
    a.insert(&list, 3, "a").unwrap();
    a.commit_with(t0());
    let second = b.save();
    let third = a.save();

    let mut stack = first.clone();
    stack.extend(&second);
    stack.extend(&third);
    let stacked = Automerge::load(&stack).expect("stack loads");

    let mut seq = Automerge::load(&first).unwrap();
    seq.load_incremental(&second).unwrap();
    seq.load_incremental(&third).unwrap();
    assert_eq!(stacked.get_heads(), seq.get_heads());
    let list_id = seq.get(ROOT, "list").unwrap().unwrap().1;
    assert_eq!(
        stacked
            .list_range(&list_id, ..)
            .map(|e| format!("{:?}", e.value))
            .collect::<Vec<_>>(),
        seq.list_range(&list_id, ..)
            .map(|e| format!("{:?}", e.value))
            .collect::<Vec<_>>()
    );
    assert_eq!(stacked.save(), seq.save());
}

/// A stack mixing two peers' fragment bundles (different histories) loads
/// like applying them in turn.
#[test]
fn loading_a_mixed_fragment_stack_equals_loading_it_in_turn() {
    let mut a = AutoCommit::new().with_actor(actor(1));
    let text = a
        .put_object(ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    a.splice_text(&text, 0, 0, "hello").unwrap();
    a.commit_with(t0());
    let mut b = AutoCommit::load(&a.save()).unwrap().with_actor(actor(2));
    let mut rng = SmallRng::seed_from_u64(9);
    for round in 0..300 {
        for (p, tag) in [(&mut a, "a"), (&mut b, "b")] {
            let len = p.length(&text);
            p.splice_text(&text, rng.random_range(0..=len), 0, tag)
                .unwrap();
            p.commit_with(t0());
        }
        if round % 37 == 0 {
            a.merge(&mut b).unwrap();
        }
        if round % 53 == 0 {
            b.merge(&mut a).unwrap();
        }
    }
    let frags_a = a.bundles_for_fragments(a.fragments(..)).unwrap();
    let frags_b = b.bundles_for_fragments(b.fragments(..)).unwrap();
    let mut stack = Vec::new();
    let mut seq = Automerge::new();
    for bytes in frags_a.iter().chain(frags_b.iter()) {
        stack.extend(bytes);
        seq.load_incremental(bytes).unwrap();
    }
    let stacked = Automerge::load(&stack).expect("mixed stack loads");
    assert_eq!(stacked.get_heads(), seq.get_heads());
    assert_eq!(stacked.text(&text).unwrap(), seq.text(&text).unwrap());
    assert_eq!(stacked.save(), seq.save());
}
