//! Known automerge bugs with no fix yet.
//!
//! Each test here fails, and each names one defect. They are `#[ignore]`d
//! so CI stays green; run them with
//!
//! ```text
//! cargo test -p automerge --test pending -- --ignored
//! ```
//!
//! Drop the attribute as each fix lands. A test that starts passing means
//! the bug is gone — or that the test stopped reaching it, which is worth
//! checking before deleting. They were found by differential fuzzing of
//! automerge over two versions of hexane (2026-11-06); every one happens
//! with either.

use std::fs;

use automerge::sync::{State, SyncDoc};
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{
    ActorId, AutoCommit, LoadOptions, ObjType, ReadDoc, ScalarValue, VerificationMode, ROOT,
};
use sha2::{Digest, Sha256};

/// Load `bytes` every way there is and use what loads (hydrate, list the
/// changes, save and reload): the ways that panic.
fn panics_loading(bytes: &[u8]) -> Vec<&'static str> {
    fn exercise(d: &mut AutoCommit) {
        let _ = d.hydrate(&ROOT, None);
        let _ = d.get_heads();
        let _ = d.get_changes(&[]).len();
        let saved = d.save();
        let _ = AutoCommit::load(&saved).map(|x| x.hydrate(&ROOT, None).is_ok());
    }
    let mut panics = vec![];
    let load = std::panic::catch_unwind(|| {
        if let Ok(mut d) = AutoCommit::load(bytes) {
            exercise(&mut d)
        }
    });
    if load.is_err() {
        panics.push("load");
    }
    let unchecked = std::panic::catch_unwind(|| {
        let opts = LoadOptions::new().verification_mode(VerificationMode::DontCheck);
        if let Ok(mut d) = AutoCommit::load_with_options(bytes, opts) {
            exercise(&mut d)
        }
    });
    if unchecked.is_err() {
        panics.push("load_with_options(DontCheck)");
    }
    let incremental = std::panic::catch_unwind(|| {
        let mut d = AutoCommit::new();
        if d.load_incremental(bytes).is_ok() {
            exercise(&mut d)
        }
    });
    if incremental.is_err() {
        panics.push("load_incremental");
    }
    panics
}

/// Malformed documents (with valid checksums) that panic instead of
/// failing to load: one per place it panics, which each file is named
/// after. Several come from columns the load reads without checking their
/// lengths against each other (the value column against the value
/// metadata, change metadata against the op columns), and from indexes
/// taken from the bytes without a bounds check. Some panic only with
/// `VerificationMode::DontCheck`. When one is fixed, move its file to
/// `tests/fuzz-crashers`, whose test requires every file to fail to load.
#[test]
#[ignore = "known bug: malformed documents panic instead of failing to load"]
fn malformed_documents_fail_to_load() {
    std::panic::set_hook(Box::new(|_| {}));
    let mut paths: Vec<_> = fs::read_dir("./tests/pending-crashers")
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut failures = vec![];
    for p in paths {
        let panics = panics_loading(&fs::read(&p).unwrap());
        if !panics.is_empty() {
            failures.push(format!("{}: {:?}", p.display(), panics));
        }
    }
    let _ = std::panic::take_hook();
    assert!(failures.is_empty(), "panics:\n{}", failures.join("\n"));
}

fn leb(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// A change chunk of seq 1 with no deps and no ops, starting at op
/// `start_op`.
fn change_with_no_ops(start_op: u64) -> Vec<u8> {
    let mut data = vec![0x00, 0x10]; // no deps; a 16-byte actor
    data.extend_from_slice(&[0xaa; 16]);
    data.push(0x01); // seq 1
    leb(start_op, &mut data);
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // time, message, other actors, columns
    let mut body = vec![0x01]; // a change chunk
    leb(data.len() as u64, &mut body);
    body.extend_from_slice(&data);
    let hash = Sha256::digest(&body);
    let mut out = vec![0x85, 0x6f, 0x4a, 0x83];
    out.extend_from_slice(&hash[..4]);
    out.extend_from_slice(&body);
    out
}

/// A change of seq 1 with no deps that starts at op N > 1 claims ops it
/// doesn't have. `load` accepts it as a change, and `save` writes a
/// document; whether that document then loads depends on how many ops it
/// claims. The document loader checks for missing ops only when it
/// collects them in a `Vec` (`VecEncoder`, op_set2/change/collector.rs),
/// and it picks that or `ProgressiveEncoder` by comparing their sizes in
/// bytes (`OpEncoderStrategy::new`). So with the hexane on main, N of 2 to
/// 19 give a document that fails to load (`MissingOps`) and N of 20 and up
/// one that loads; the switch point moves whenever those types change
/// size. Either the change should be rejected, or the document should
/// load, whatever N.
#[test]
#[ignore = "known bug: whether missing ops are caught depends on an encoder's size"]
fn a_saved_document_loads_whatever_the_op_count() {
    let mut fail = vec![];
    for start_op in 2..=40u64 {
        if let Ok(mut doc) = AutoCommit::load(&change_with_no_ops(start_op)) {
            if AutoCommit::load(&doc.save()).is_err() {
                fail.push(start_op);
            }
        }
    }
    assert!(
        fail.is_empty(),
        "saved documents that don't load, by start_op: {:?}",
        fail
    );
}

fn sync(x: &mut AutoCommit, y: &mut AutoCommit) {
    let (mut sx, mut sy) = (State::new(), State::new());
    loop {
        let m1 = x.sync().generate_sync_message(&mut sx);
        let sent1 = m1.is_some();
        if let Some(m) = m1 {
            y.sync().receive_sync_message(&mut sy, m).unwrap();
        }
        let m2 = y.sync().generate_sync_message(&mut sy);
        let sent2 = m2.is_some();
        if let Some(m) = m2 {
            x.sync().receive_sync_message(&mut sx, m).unwrap();
        }
        if !sent1 && !sent2 {
            return;
        }
    }
}

/// `bundle` of the same changes gives different bytes from one call to the
/// next when a key was deleted concurrently: `flush_deletes`
/// (storage/bundle/builder.rs) walks a `HashMap`, whose order is random.
/// The changes it holds (`to_changes`) are the same.
#[test]
#[ignore = "known bug: bundle bytes depend on HashMap order"]
fn bundles_of_the_same_changes_are_the_same_bytes() {
    let bundle = || {
        let mut a = AutoCommit::new().with_actor(ActorId::from(vec![1u8; 16]));
        let mut b = AutoCommit::new().with_actor(ActorId::from(vec![2u8; 16]));
        a.put(&ROOT, "k", ScalarValue::Timestamp(i64::MIN)).unwrap();
        a.commit();
        sync(&mut a, &mut b);
        a.delete(&ROOT, "k").unwrap();
        a.commit();
        b.delete(&ROOT, "k").unwrap();
        b.commit();
        sync(&mut b, &mut a);
        let hashes: Vec<_> = a.get_changes(&[]).iter().map(|c| c.hash()).collect();
        a.bundle(hashes).unwrap().bytes().to_vec()
    };
    let first = bundle();
    for _ in 0..20 {
        assert_eq!(bundle(), first);
    }
}

/// Commit timestamps are stored in a delta column, whose values must lie
/// within 2^63 of each other. Timestamps further apart make `commit`
/// panic ("delta values out of domain: they would span 2^63 or more"; in
/// a debug build of main, an overflow first).
#[test]
#[ignore = "known bug: commit timestamps 2^63 apart panic"]
fn commits_far_apart_in_time() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(vec![1u8; 16]));
    doc.put(&ROOT, "a", 1).unwrap();
    doc.commit_with(CommitOptions::default().with_time(i64::MIN));
    doc.put(&ROOT, "a", 2).unwrap();
    doc.commit_with(CommitOptions::default().with_time(1));
    let times: Vec<i64> = AutoCommit::load(&doc.save())
        .unwrap()
        .get_changes(&[])
        .iter()
        .map(|c| c.timestamp())
        .collect();
    assert_eq!(times, vec![i64::MIN, 1]);
}

/// Incrementing a counter past `i64::MAX` overflows when it is read
/// (op_set2/op.rs): a debug build panics, a release build wraps to
/// `i64::MIN`. (So this test fails only in a debug build.)
#[test]
#[ignore = "known bug: a counter incremented past i64::MAX overflows"]
fn incrementing_a_counter_past_i64_max() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(vec![1u8; 16]));
    doc.put(&ROOT, "c", ScalarValue::counter(i64::MAX)).unwrap();
    let _ = doc.increment(&ROOT, "c", 1);
    let _ = doc.get(&ROOT, "c");
}

/// `parents_at` an object at heads from before it existed unwraps a
/// `None` (op_set2/parents.rs) rather than returning an error.
#[test]
#[ignore = "known bug: parents_at panics on an object that doesn't exist yet"]
fn parents_at_heads_before_the_object_existed() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(vec![1u8; 16]));
    let list = doc.put_object(&ROOT, "l", ObjType::List).unwrap();
    let inner = doc.insert_object(&list, 0, ObjType::List).unwrap();
    doc.commit();
    let _ = doc.parents_at(&inner, &[]).map(|p| p.count());
}

/// Isolating at older heads and committing there (here, empty changes
/// between edits) forks the actor's own history: two changes get the same
/// seq. The document then can't be saved and loaded, merged into another,
/// or forked at its heads: they panic on an assertion
/// (`seq_index[actor].len() + 1 == change.seq()`, change_graph.rs) or
/// "Change's actor not already in the document" (automerge.rs).
#[test]
#[ignore = "known bug: committing while isolated at older heads forks the actor's seq"]
fn committing_while_isolated_keeps_the_history_loadable() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(vec![1u8; 16]));
    doc.isolate(&[]);
    let list = doc.put_object(&ROOT, "a", ObjType::List).unwrap();
    doc.empty_change(CommitOptions::default());
    doc.empty_change(CommitOptions::default());
    doc.insert_object(&list, 0, ObjType::Map).unwrap();
    doc.empty_change(CommitOptions::default());
    let bytes = doc.save();
    AutoCommit::load(&bytes).unwrap();
    let mut other = AutoCommit::new();
    other.merge(&mut doc).unwrap();
}
