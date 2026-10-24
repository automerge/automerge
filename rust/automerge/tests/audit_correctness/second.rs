use automerge::next::transaction::Transactable;
use automerge::next::{AutoCommit, Automerge};
use automerge::{Bundle, ROOT};

use crate::common::{actor, id, panic_msg, t0, Prefix};

fn linear(a: u8, n: usize) -> Automerge {
    let mut doc = Automerge::new().with_actor(actor(a));
    for i in 0..n {
        let mut tx = doc.transaction();
        tx.put(ROOT, "k", i as i64).unwrap();
        tx.commit_with(t0());
    }
    doc
}

/// `make_bundle` given a head that another head depends on makes a bundle
/// that leaves its receiver with the writer's heads.
#[test]
fn make_bundle_with_a_redundant_head_keeps_the_receivers_heads_minimal() {
    let doc = linear(1, 3);
    let bundle = doc.make_bundle(&[id(1, 2), id(1, 3)], &[id(1, 1)]).unwrap();
    let mut r = Automerge::new();
    r.apply_bundle(doc.make_bundle(&[id(1, 1)], &[]).unwrap())
        .unwrap();
    r.apply_bundle(bundle).unwrap();
    assert_eq!(r.get_heads(), doc.get_heads(), "heads are minimal");
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
        let _ = tx.send(r.err().map(|e| panic_msg(&e)));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(20)) {
        Ok(None) => None,
        Ok(Some(msg)) => Some(format!("panicked: {msg}")),
        Err(_) => Some("did not return within 20 s".to_string()),
    }
}

/// Malformed legacy chunks (document, BundleV0) found by mutation fuzzing.
const MALFORMED_LEGACY_CHUNKS: &[(&str, &[u8])] = &[
    (
        "a document chunk with a bad change column varint is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c0_i202.bin"),
    ),
    (
        "a document chunk whose value column is short is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c1_i59.bin"),
    ),
    (
        "a document chunk whose value lengths cross a slab is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c1_i107.bin"),
    ),
    (
        "a bundle v0 with an invalid action is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i0.bin"),
    ),
    (
        "a bundle v0 with a truncated column is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i74.bin"),
    ),
    (
        "a bundle v0 with an overlong run is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i131.bin"),
    ),
    (
        "a bundle v0 with short value bytes is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i39.bin"),
    ),
    (
        "a bundle v0 with short extra bytes is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i181.bin"),
    ),
    (
        "a bundle v0 with an undecodable op is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i135.bin"),
    ),
    (
        "a bundle v0 with unordered ops is an error",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i163.bin"),
    ),
    (
        "a bundle v0 with a truncated bool run is an error not a hang",
        include_bytes!("../fixtures/audit_c2_hostile/legacy_c3_i1389_hang.bin"),
    ),
];

#[test]
fn malformed_legacy_chunks_load_to_an_error_promptly() {
    let failures: Vec<String> = MALFORMED_LEGACY_CHUNKS
        .iter()
        .filter_map(|(name, bytes)| load_outcome(bytes).map(|f| format!("{name}: {f}")))
        .collect();
    assert!(failures.is_empty(), "{failures:#?}");
}

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
