use crate::storage::document::{CompressConfig, Document};
use crate::storage::parse::Input;
use crate::storage::Chunk;
use crate::transaction::Transactable;
use crate::{ActorId, Automerge, ObjType, ReadDoc, ROOT};

#[test]
#[allow(deprecated)]
fn clock_heads_remove_cross_actor_ancestors_and_keep_concurrent_empty_changes() {
    use crate::transaction::CommitOptions;
    let mut doc = Automerge::new().with_actor(ActorId::from([1])).unwrap();
    let a = doc.empty_commit(CommitOptions::default());
    doc.set_actor(ActorId::from([2])).unwrap();
    let b = doc.empty_commit(CommitOptions::default());
    doc.set_actor(ActorId::from([3])).unwrap();
    let mut tx = doc
        .transaction_at(crate::PatchLog::inactive(), &[])
        .unwrap();
    tx.put(ROOT, "concurrent", 1).unwrap();
    let c = tx.commit().0.unwrap();
    let clock = doc.change_graph.seq_clock_for_heads(&[a, b, c]);
    let mut expected = vec![b, c];
    expected.sort_unstable();
    assert_eq!(doc.change_graph.heads_at_clock(&clock), expected);
    let bytes =
        Document::at_clock(&doc.ops, &doc.change_graph, &clock, CompressConfig::None).into_bytes();
    let loaded = Automerge::load(&bytes).unwrap();
    assert_eq!(loaded.get_heads(), expected);
    assert_eq!(loaded.get_changes(&[]).len(), 3);
}

#[test]
#[allow(deprecated)]
fn clock_projection_excludes_later_rows_and_successors_without_replaying_changes() {
    let mut doc = Automerge::new().with_actor(ActorId::from([2])).unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "keep", 1).unwrap();
    tx.put(ROOT, "overwritten", 1).unwrap();
    tx.put(ROOT, "deleted", 1).unwrap();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.insert(&list, 0, "first").unwrap();
    let published = tx.commit().0.unwrap();
    let expected = doc.get_change_by_hash(&published).unwrap();
    doc.set_actor(ActorId::from([1])).unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "overwritten", 2).unwrap();
    tx.delete(ROOT, "deleted").unwrap();
    tx.delete(&list, 0).unwrap();
    tx.put_object(ROOT, "draft", ObjType::Map).unwrap();
    tx.commit();
    // Use the current actor-table alignment, as live published-clock maintenance does.
    let clock = doc.change_graph.seq_clock_for_heads(&[published]);
    let bytes =
        Document::at_clock(&doc.ops, &doc.change_graph, &clock, CompressConfig::None).into_bytes();
    let loaded = Automerge::load(&bytes).unwrap();
    assert_eq!(loaded.get_heads(), [published]);
    assert_eq!(loaded.get_changes(&[]).len(), 1);
    assert_eq!(
        loaded.get_change_by_hash(&published).unwrap().raw_bytes(),
        expected.raw_bytes()
    );
    assert_eq!(
        loaded.get(ROOT, "overwritten").unwrap().unwrap().0.as_i64(),
        Some(1)
    );
    assert!(loaded.get(ROOT, "deleted").unwrap().is_some());
    assert!(loaded.get(ROOT, "draft").unwrap().is_none());
    assert_eq!(loaded.length(&list), 1);
    assert!(matches!(
        Chunk::parse(Input::new(&bytes)).unwrap().1,
        Chunk::Document(_)
    ));
}
