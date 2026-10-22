//! The rebuild counters count the changes, and their ops, that a load
//! rebuilds from a document chunk's op set.
use automerge::rebuild_counters;
use automerge::transaction::{CommitOptions, Transactable};
use automerge::{ActorId, AutoCommit, Automerge, ObjType, ROOT};

/// A document of three changes, of 5, 7 and 1 ops, by two actors.
fn three_changes() -> AutoCommit {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"actor-one".as_slice()));
    let list = doc.put_object(&ROOT, "list", ObjType::List).unwrap();
    for i in 0..4 {
        doc.insert(&list, i, i as i64).unwrap();
    }
    doc.commit_with(CommitOptions::default().with_time(0));
    let mut other = doc
        .fork()
        .with_actor(ActorId::from(b"actor-two".as_slice()));
    for i in 0..7 {
        other.put(&ROOT, format!("key{i}"), i as i64).unwrap();
    }
    other.commit_with(CommitOptions::default().with_time(0));
    doc.merge(&mut other).unwrap();
    doc.put(&ROOT, "done", true).unwrap();
    doc.commit_with(CommitOptions::default().with_time(0));
    doc
}

#[test]
fn a_full_load_counts_each_change_and_op_it_rebuilds() {
    let bytes = three_changes().save();
    rebuild_counters::reset();
    let doc = Automerge::load(&bytes).unwrap();
    assert_eq!(doc.get_changes(&[]).len(), 3);
    assert_eq!(rebuild_counters::changes_rebuilt(), 3);
    assert_eq!(rebuild_counters::ops_rebuilt(), 5 + 7 + 1);

    // Each load rebuilds them again.
    Automerge::load(&bytes).unwrap();
    assert_eq!(rebuild_counters::changes_rebuilt(), 6);
    assert_eq!(rebuild_counters::ops_rebuilt(), 26);

    rebuild_counters::reset();
    assert_eq!(rebuild_counters::changes_rebuilt(), 0);
    assert_eq!(rebuild_counters::ops_rebuilt(), 0);
}

#[test]
fn loading_change_chunks_rebuilds_nothing() {
    let mut doc = three_changes();
    let changes = doc.get_changes(&[]);
    let bytes: Vec<u8> = changes
        .iter()
        .flat_map(|c| c.raw_bytes().to_vec())
        .collect();
    rebuild_counters::reset();
    let loaded = Automerge::load(&bytes).unwrap();
    assert_eq!(loaded.get_heads(), doc.get_heads());
    assert_eq!(rebuild_counters::changes_rebuilt(), 0);
    assert_eq!(rebuild_counters::ops_rebuilt(), 0);
}
