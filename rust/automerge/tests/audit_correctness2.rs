//! Second correctness audit of the `hashless` branch. Each test's name
//! states the expected behaviour; a failing test is a finding.

use automerge::next::transaction::{CommitOptions, Transactable};
use automerge::next::{AutoCommit, Automerge};
use automerge::{ActorId, Bundle, ChangeId, ROOT};

fn actor(n: u8) -> ActorId {
    ActorId::from(&[n, n, n, n][..])
}

fn t0() -> CommitOptions {
    CommitOptions::default().with_time(0)
}

fn id(a: u8, seq: u64) -> ChangeId {
    ChangeId::from_parts(actor(a), std::num::NonZeroU64::new(seq).unwrap())
}

fn linear(a: u8, n: usize) -> Automerge {
    let mut doc = Automerge::new().with_actor(actor(a));
    for i in 0..n {
        let mut tx = doc.transaction();
        tx.put(ROOT, "k", i as i64).unwrap();
        tx.commit_with(t0());
    }
    doc
}

/// `make_bundle` given a head that another head depends on must not
/// produce a bundle that makes its receiver's heads non-minimal.
#[test]
fn make_bundle_with_a_redundant_head_keeps_the_receivers_heads_minimal() {
    let doc = linear(1, 3);
    let res = doc.make_bundle(&[id(1, 2), id(1, 3)], &[id(1, 1)]);
    eprintln!(
        "make_bundle: {:?}",
        res.as_ref().map(|b| b.heads().collect::<Vec<_>>())
    );
    let Ok(bundle) = res else { return };
    let mut r = Automerge::new();
    r.apply_bundle(doc.make_bundle(&[id(1, 1)], &[]).unwrap())
        .unwrap();
    let res = r.apply_bundle(bundle);
    eprintln!("apply: {:?}", res);
    if res.is_ok() {
        assert_eq!(r.get_heads(), doc.get_heads(), "heads are minimal");
    }
}

// ───────────────────────────────────────────────────────────────────────
// A bundle prefix editor, for hostile bundles that still parse.
// ───────────────────────────────────────────────────────────────────────

fn read_uleb(b: &[u8], at: &mut usize) -> u64 {
    let mut out = 0u64;
    let mut shift = 0;
    loop {
        let byte = b[*at];
        *at += 1;
        out |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return out;
        }
        shift += 7;
    }
}

fn write_uleb(out: &mut Vec<u8>, v: u64) {
    leb128::write::unsigned(out, v).unwrap();
}

fn read_hash(b: &[u8], at: &mut usize) -> [u8; 32] {
    let h: [u8; 32] = b[*at..*at + 32].try_into().unwrap();
    *at += 32;
    h
}

#[derive(Debug, Clone)]
struct Prefix {
    actors: Vec<Vec<u8>>,
    heads: Vec<([u8; 32], u64)>,
    checkpoints: Vec<(u64, [u8; 32])>,
    retained: Vec<(u64, [u8; 32])>,
    boundary: Vec<([u8; 32], u64, u64)>,
    deps: Vec<(u64, u64)>,
    columns: Vec<u8>,
}

impl Prefix {
    fn parse(chunk: &[u8]) -> Prefix {
        let mut at = 8;
        assert_eq!(chunk[at], 4, "bundle chunk type");
        at += 1;
        let len = read_uleb(chunk, &mut at) as usize;
        let data = &chunk[at..at + len];
        let mut at = 0;
        let n = read_uleb(data, &mut at);
        let mut actors = Vec::new();
        for _ in 0..n {
            let l = read_uleb(data, &mut at) as usize;
            actors.push(data[at..at + l].to_vec());
            at += l;
        }
        let mut heads = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let h = read_hash(data, &mut at);
            heads.push((h, read_uleb(data, &mut at)));
        }
        let indexed = |at: &mut usize| {
            let mut v = Vec::new();
            for _ in 0..read_uleb(data, at) {
                let i = read_uleb(data, at);
                v.push((i, read_hash(data, at)));
            }
            v
        };
        let checkpoints = indexed(&mut at);
        let retained = indexed(&mut at);
        let mut boundary = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let h = read_hash(data, &mut at);
            let a = read_uleb(data, &mut at);
            boundary.push((h, a, read_uleb(data, &mut at)));
        }
        let mut deps = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let a = read_uleb(data, &mut at);
            deps.push((a, read_uleb(data, &mut at)));
        }
        Prefix {
            actors,
            heads,
            checkpoints,
            retained,
            boundary,
            deps,
            columns: data[at..].to_vec(),
        }
    }

    fn bytes(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut d = Vec::new();
        write_uleb(&mut d, self.actors.len() as u64);
        for a in &self.actors {
            write_uleb(&mut d, a.len() as u64);
            d.extend(a);
        }
        write_uleb(&mut d, self.heads.len() as u64);
        for (h, i) in &self.heads {
            d.extend(h);
            write_uleb(&mut d, *i);
        }
        for list in [&self.checkpoints, &self.retained] {
            write_uleb(&mut d, list.len() as u64);
            for (i, h) in list {
                write_uleb(&mut d, *i);
                d.extend(h);
            }
        }
        write_uleb(&mut d, self.boundary.len() as u64);
        for (h, a, s) in &self.boundary {
            d.extend(h);
            write_uleb(&mut d, *a);
            write_uleb(&mut d, *s);
        }
        write_uleb(&mut d, self.deps.len() as u64);
        for (a, s) in &self.deps {
            write_uleb(&mut d, *a);
            write_uleb(&mut d, *s);
        }
        d.extend(&self.columns);
        let mut len = Vec::new();
        write_uleb(&mut len, d.len() as u64);
        let mut hasher = Sha256::new();
        hasher.update([4u8]);
        hasher.update(&len);
        hasher.update(&d);
        let sum = hasher.finalize();
        let mut out = vec![0x85, 0x6f, 0x4a, 0x83];
        out.extend(&sum[..4]);
        out.push(4);
        out.extend(len);
        out.extend(d);
        out
    }
}

#[test]
fn prefix_editor_round_trips() {
    let doc = linear(1, 3);
    let b = doc.make_bundle(&[id(1, 3)], &[id(1, 1)]).unwrap().bytes();
    assert_eq!(Prefix::parse(&b).bytes(), b);
}

/// A bundle whose declared heads leave out the member nothing depends on
/// must be rejected, or leave the receiver with its true heads.
#[test]
fn a_bundle_declaring_an_interior_member_as_its_only_head_is_rejected() {
    let doc = linear(1, 3);
    let bundle = doc.make_bundle(&[id(1, 3)], &[id(1, 1)]).unwrap();
    let mut p = Prefix::parse(&bundle.bytes());
    assert_eq!(p.heads.len(), 1);
    let a2 = doc.change_id_to_hash(&id(1, 2)).unwrap().unwrap();
    p.heads = vec![(a2.as_bytes().try_into().unwrap(), 0)];
    // name the true tip as retained, so only the heads are wrong
    let a3 = doc.change_id_to_hash(&id(1, 3)).unwrap().unwrap();
    p.retained = vec![(1, a3.as_bytes().try_into().unwrap())];
    let hostile = match Bundle::try_from(&p.bytes()[..]) {
        Ok(b) => b,
        Err(e) => return eprintln!("rejected at parse: {e}"),
    };

    let mut r = Automerge::new();
    r.apply_bundle(doc.make_bundle(&[id(1, 1)], &[]).unwrap())
        .unwrap();
    match r.apply_bundle(hostile) {
        Err(e) => eprintln!("rejected: {e}"),
        Ok(()) => assert_eq!(r.get_heads(), vec![id(1, 3)], "heads name the last change"),
    }
}

/// Two actors: actor 1's last change is interior to the bundle, so the
/// receiver must keep it (it is that actor's tip) and the bundle names it
/// in `retained`. Debug builds assert on a bundle that leaves it out;
/// release builds rebuild the hash and carry on.
#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "debug builds assert on a non-conforming bundle"
)]
fn a_bundle_leaving_out_a_receiver_tip_is_handled_in_release() {
    let mut a = linear(1, 2);
    let mut b = Automerge::new().with_actor(actor(2));
    b.merge(&mut a).unwrap();
    {
        let mut tx = b.transaction();
        tx.put(ROOT, "b", 1).unwrap();
        tx.commit_with(t0());
    }
    let bundle = b.make_bundle(&[id(2, 1)], &[id(1, 1)]).unwrap();
    let mut p = Prefix::parse(&bundle.bytes());
    eprintln!("retained: {:?}", p.retained.len());
    assert!(!p.retained.is_empty(), "fixture names actor 1's tip");
    p.retained.clear();
    let hostile = Bundle::try_from(&p.bytes()[..]).expect("parses");

    let mut r = Automerge::new();
    r.apply_bundle(a.make_bundle(&[id(1, 1)], &[]).unwrap())
        .unwrap();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| r.apply_bundle(hostile)));
    assert!(res.is_ok(), "applying a hostile bundle panicked");
}

// ───────────────────────────────────────────────────────────────────────
// Hostile legacy changes.
// ───────────────────────────────────────────────────────────────────────

/// A change whose op inserts into the root map with a sequence key.
fn change_inserting_into_the_root_map(base: &automerge::Automerge) -> automerge::Change {
    use automerge::legacy::{ElementId, Key, ObjectId, Op, OpType, SortedVec};
    let expanded = automerge::ExpandedChange {
        operations: vec![Op {
            action: OpType::Put(automerge::ScalarValue::Str("x".into())),
            obj: ObjectId::Root,
            key: Key::Seq(ElementId::Head),
            pred: SortedVec::new(),
            insert: true,
        }],
        actor_id: actor(2),
        hash: None,
        seq: 1,
        start_op: std::num::NonZeroU64::new(3).unwrap(),
        time: 0,
        message: None,
        deps: base.get_heads(),
        extra_bytes: vec![],
        author: None,
    };
    automerge::Change::from(expanded)
}

fn root_map_base() -> automerge::Automerge {
    use automerge::transaction::Transactable as _;
    let mut doc = automerge::Automerge::new().with_actor(actor(1));
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    tx.put(ROOT, "b", 2).unwrap();
    tx.commit();
    doc
}

/// Audit mode (the crate-root API): a change with a sequence key in a map
/// must be rejected, not corrupt the op order.
#[test]
fn a_change_inserting_into_a_map_is_rejected_audit() {
    let mut doc = root_map_base();
    let change = change_inserting_into_the_root_map(&doc);
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let r = doc.apply_changes([change]);
        (r, doc.save())
    }));
    match res {
        Err(_) => panic!("applying a hostile change panicked"),
        Ok((Err(_), _)) => {}
        Ok((Ok(()), saved)) => {
            automerge::Automerge::load(&saved).expect("an accepted change reloads");
        }
    }
}

#[test]
fn a_change_inserting_into_a_map_is_rejected_next() {
    let base = root_map_base();
    let change = change_inserting_into_the_root_map(&base);
    let mut doc = Automerge::load(&base.save()).unwrap();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let r = doc.apply_changes([change]);
        (r, doc.save())
    }));
    match res {
        Err(_) => panic!("applying a hostile change panicked"),
        Ok((Err(_), _)) => {}
        Ok((Ok(()), saved)) => {
            Automerge::load(&saved).expect("an accepted change reloads");
        }
    }
}

// ───────────────────────────────────────────────────────────────────────
// GC mode across load_incremental into an empty document.
// ───────────────────────────────────────────────────────────────────────

#[test]
fn load_incremental_into_an_empty_autocommit_keeps_manual_gc() {
    let mut src = AutoCommit::new().with_actor(actor(1));
    src.put(ROOT, "k", 0).unwrap();
    src.commit_with(t0());
    let mut doc = AutoCommit::new();
    assert_eq!(doc.document().gc_mode(), automerge::next::GcMode::Manual);
    doc.load_incremental(&src.save()).unwrap();
    assert_eq!(
        doc.document().gc_mode(),
        automerge::next::GcMode::Manual,
        "an AutoCommit collects only in save"
    );
}

#[test]
fn load_incremental_into_an_empty_manual_gc_document_keeps_manual_gc() {
    let src = linear(1, 1);
    let mut doc = Automerge::new().with_manual_gc();
    doc.load_incremental(&src.save()).unwrap();
    assert_eq!(doc.gc_mode(), automerge::next::GcMode::Manual);
}

/// The consequence: an AutoCommit that started with `load_incremental`
/// frees hashes outside `save`, while one that started with `load` does not.
#[test]
fn an_autocommit_started_by_load_incremental_frees_no_hash_before_saving() {
    let mut src = AutoCommit::new().with_actor(actor(1));
    src.put(ROOT, "k", 0).unwrap();
    src.commit_with(t0());
    let first = src.save();
    let mut doc = AutoCommit::new().with_actor(actor(2));
    doc.load_incremental(&first).unwrap();
    for i in 0..1500 {
        doc.put(ROOT, "k", i as i64).unwrap();
        doc.commit_with(t0());
    }
    assert!(!doc.fragments(1..).is_empty(), "fixture needs a fragment");
    let res = doc.change_id_to_hash(&id(2, 10));
    assert!(
        matches!(res, Ok(Some(_))),
        "a hash was freed before any save: {res:?}"
    );
}

#[test]
fn fork_at_of_an_autocommit_keeps_manual_gc() {
    let mut doc = AutoCommit::new().with_actor(actor(1));
    doc.put(ROOT, "k", 0).unwrap();
    doc.commit_with(t0());
    let heads = doc.get_heads();
    let mut forked = doc.fork_at(&heads).unwrap();
    assert_eq!(
        forked.document().gc_mode(),
        automerge::next::GcMode::Manual,
        "an AutoCommit collects only in save"
    );
}

// ───────────────────────────────────────────────────────────────────────
// `load` into an empty document takes chunks in any order.
// ───────────────────────────────────────────────────────────────────────

fn two_bundles_reversed() -> (Automerge, Vec<u8>) {
    let doc = linear(1, 3);
    let first = doc.make_bundle(&[id(1, 1)], &[]).unwrap().bytes();
    let second = doc.make_bundle(&[id(1, 3)], &[id(1, 1)]).unwrap().bytes();
    let mut stack = second;
    stack.extend(first);
    (doc, stack)
}

#[test]
fn load_takes_a_bundle_stack_in_any_order_next() {
    let (doc, stack) = two_bundles_reversed();
    let loaded = Automerge::load(&stack).expect("load takes chunks in any order");
    assert_eq!(loaded.get_heads(), doc.get_heads());
}

#[test]
fn load_takes_a_bundle_stack_in_any_order_audit() {
    let (doc, stack) = two_bundles_reversed();
    let loaded = automerge::Automerge::load(&stack).expect("load takes chunks in any order");
    assert_eq!(loaded.get_heads(), doc.get_head_hashes());
}

#[test]
fn load_incremental_into_an_empty_document_takes_a_bundle_stack_in_any_order() {
    let (doc, stack) = two_bundles_reversed();
    let mut loaded = Automerge::new();
    loaded
        .load_incremental(&stack)
        .expect("into an empty document load_incremental is load");
    assert_eq!(loaded.get_heads(), doc.get_heads());
}

// ───────────────────────────────────────────────────────────────────────
// Malformed legacy chunks (document, BundleV0) found by mutation fuzzing
// (`tests/audit_c2_legacy.rs`). Each must load to an error, promptly.
// ───────────────────────────────────────────────────────────────────────

/// Loads `bytes` as a `next` and as an audited document on a thread with a
/// deadline; returns a description of any panic or hang.
fn load_outcome(bytes: &'static [u8]) -> Option<String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let r = std::panic::catch_unwind(|| {
            let _ = Automerge::load(bytes);
            let _ = automerge::Automerge::load(bytes);
            let mut d = Automerge::new();
            let _ = d.load_incremental(bytes);
        });
        let _ = tx.send(r.err().map(|e| {
            e.downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        }));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(20)) {
        Ok(None) => None,
        Ok(Some(msg)) => Some(format!("panicked: {msg}")),
        Err(_) => Some("did not return within 20 s".to_string()),
    }
}

macro_rules! malformed_legacy_chunk_test {
    ($name:ident, $file:literal) => {
        #[test]
        fn $name() {
            let bytes: &'static [u8] = include_bytes!(concat!("fixtures/audit_c2_hostile/", $file));
            if let Some(failure) = load_outcome(bytes) {
                panic!("loading {} {failure}", $file);
            }
        }
    };
}

malformed_legacy_chunk_test!(
    a_document_chunk_with_a_bad_change_column_varint_is_an_error,
    "legacy_c0_i202.bin"
);
malformed_legacy_chunk_test!(
    a_document_chunk_whose_value_column_is_short_is_an_error,
    "legacy_c1_i59.bin"
);
malformed_legacy_chunk_test!(
    a_document_chunk_whose_value_lengths_cross_a_slab_is_an_error,
    "legacy_c1_i107.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_an_invalid_action_is_an_error,
    "legacy_c3_i0.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_a_truncated_column_is_an_error,
    "legacy_c3_i74.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_an_overlong_run_is_an_error,
    "legacy_c3_i131.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_short_value_bytes_is_an_error,
    "legacy_c3_i39.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_short_extra_bytes_is_an_error,
    "legacy_c3_i181.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_an_undecodable_op_is_an_error,
    "legacy_c3_i135.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_unordered_ops_is_an_error,
    "legacy_c3_i163.bin"
);
malformed_legacy_chunk_test!(
    a_bundle_v0_with_a_truncated_bool_run_is_an_error_not_a_hang,
    "legacy_c3_i1389_hang.bin"
);

/// The primitive behind the hang: a boolean column ending in an
/// unterminated run length.
#[test]
fn a_bool_decoder_over_a_truncated_run_length_terminates() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let data: &[u8] = &[0x80];
        let n = hexane::decoder::<bool>(data).count();
        let _ = tx.send(n);
    });
    let res = rx.recv_timeout(std::time::Duration::from_secs(5));
    assert!(res.is_ok(), "BoolDecoder::next loops forever on [0x80]");
}

// Areas explored with larger fixtures and fuzzers, one module each.
#[path = "audit_correctness2/hostile.rs"]
mod hostile;
#[path = "audit_correctness2/legacy.rs"]
mod legacy;
#[path = "audit_correctness2/parity.rs"]
mod parity;
#[path = "audit_correctness2/retention.rs"]
mod retention;
