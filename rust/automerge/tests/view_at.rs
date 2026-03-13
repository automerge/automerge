//! Tests for the `view_at` API.

use automerge::{transaction::Transactable, AutoCommit, ChangeHash, ReadDoc, ROOT};

#[test]
fn test_view_at_basic() {
    let mut doc = AutoCommit::new();

    // Make first change
    doc.put(&ROOT, "key", "value1").unwrap();
    let heads1 = doc.get_heads();

    // Make second change
    doc.put(&ROOT, "key", "value2").unwrap();
    let heads2 = doc.get_heads();

    // View at first state
    let view1 = doc.view_at(&heads1).unwrap();
    let (value, _) = view1.get(&ROOT, "key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("value1"));

    // View at second state
    let view2 = doc.view_at(&heads2).unwrap();
    let (value, _) = view2.get(&ROOT, "key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("value2"));

    // Current state should still be value2
    let (value, _) = doc.get(&ROOT, "key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("value2"));
}

#[test]
fn test_view_at_invalid_heads() {
    let doc = AutoCommit::new();
    let fake_hash = ChangeHash([0u8; 32]);

    let result = doc.view_at(&[fake_hash]);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(err.missing, fake_hash);
}

#[test]
fn test_view_at_empty_heads() {
    let mut doc = AutoCommit::new();
    doc.put(&ROOT, "key", "value").unwrap();

    // Empty heads represents state before any changes
    let view = doc.view_at(&[]).unwrap();

    // Key should not exist in empty state
    let result = view.get(&ROOT, "key").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_view_at_multiple_keys() {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "a", "1").unwrap();
    doc.put(&ROOT, "b", "2").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&ROOT, "a", "10").unwrap();
    doc.put(&ROOT, "c", "3").unwrap();

    let view = doc.view_at(&heads1).unwrap();

    // Check values at heads1
    let (val_a, _) = view.get(&ROOT, "a").unwrap().unwrap();
    assert_eq!(val_a.to_str(), Some("1"));

    let (val_b, _) = view.get(&ROOT, "b").unwrap().unwrap();
    assert_eq!(val_b.to_str(), Some("2"));

    // "c" didn't exist at heads1
    let val_c = view.get(&ROOT, "c").unwrap();
    assert!(val_c.is_none());
}

#[test]
fn test_view_at_keys() {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "a", "1").unwrap();
    doc.put(&ROOT, "b", "2").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&ROOT, "c", "3").unwrap();

    let view = doc.view_at(&heads1).unwrap();

    let keys: Vec<_> = view.keys(&ROOT).collect();
    assert_eq!(keys, vec!["a", "b"]);
}

#[test]
fn test_view_at_length() {
    let mut doc = AutoCommit::new();

    let list = doc
        .put_object(&ROOT, "list", automerge::ObjType::List)
        .unwrap();
    doc.insert(&list, 0, "a").unwrap();
    doc.insert(&list, 1, "b").unwrap();
    let heads1 = doc.get_heads();

    doc.insert(&list, 2, "c").unwrap();

    let view = doc.view_at(&heads1).unwrap();
    assert_eq!(view.length(&list), 2);

    // Current length should be 3
    assert_eq!(doc.length(&list), 3);
}

#[test]
fn test_view_at_text() {
    let mut doc = AutoCommit::new();

    let text = doc
        .put_object(&ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    let heads1 = doc.get_heads();

    doc.splice_text(&text, 5, 0, " world").unwrap();

    let view = doc.view_at(&heads1).unwrap();
    assert_eq!(view.text(&text).unwrap(), "hello");

    // Current text should be "hello world"
    assert_eq!(doc.text(&text).unwrap(), "hello world");
}

#[test]
fn test_view_at_nested_objects() {
    let mut doc = AutoCommit::new();

    let map = doc
        .put_object(&ROOT, "map", automerge::ObjType::Map)
        .unwrap();
    doc.put(&map, "nested_key", "nested_value1").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&map, "nested_key", "nested_value2").unwrap();

    let view = doc.view_at(&heads1).unwrap();
    let (value, _) = view.get(&map, "nested_key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("nested_value1"));
}

#[test]
fn test_view_of_view() {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "key", "value1").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&ROOT, "key", "value2").unwrap();
    let heads2 = doc.get_heads();

    doc.put(&ROOT, "key", "value3").unwrap();

    // Create a view at heads2
    let view2 = doc.view_at(&heads2).unwrap();

    // Create a view at heads1 from the view at heads2
    let view1 = view2.view_at(&heads1).unwrap();

    let (value, _) = view1.get(&ROOT, "key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("value1"));
}

#[test]
fn test_view_at_with_automerge() {
    use automerge::Automerge;

    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(&ROOT, "key", "value1").unwrap();
    tx.commit();
    let heads1 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.put(&ROOT, "key", "value2").unwrap();
    tx.commit();

    let view = doc.view_at(&heads1).unwrap();
    let (value, _) = view.get(&ROOT, "key").unwrap().unwrap();
    assert_eq!(value.to_str(), Some("value1"));
}

#[test]
fn test_view_at_list_range() {
    let mut doc = AutoCommit::new();

    let list = doc
        .put_object(&ROOT, "list", automerge::ObjType::List)
        .unwrap();
    doc.insert(&list, 0, "a").unwrap();
    doc.insert(&list, 1, "b").unwrap();
    doc.insert(&list, 2, "c").unwrap();
    let heads1 = doc.get_heads();

    doc.insert(&list, 3, "d").unwrap();

    let view = doc.view_at(&heads1).unwrap();

    // Check the count of items in list_range
    let count = view.list_range(&list, ..).count();
    assert_eq!(count, 3);
}

#[test]
fn test_view_at_map_range() {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "a", "1").unwrap();
    doc.put(&ROOT, "b", "2").unwrap();
    doc.put(&ROOT, "c", "3").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&ROOT, "d", "4").unwrap();

    let view = doc.view_at(&heads1).unwrap();
    let keys: Vec<_> = view.map_range(&ROOT, ..).map(|item| item.key).collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn test_view_at_values() {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "a", "1").unwrap();
    doc.put(&ROOT, "b", "2").unwrap();
    let heads1 = doc.get_heads();

    doc.put(&ROOT, "c", "3").unwrap();

    let view = doc.view_at(&heads1).unwrap();
    let values: Vec<_> = view
        .values(&ROOT)
        .map(|(v, _)| v.to_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(values, vec![Some("1".to_string()), Some("2".to_string())]);
}

#[test]
fn test_view_at_object_type() {
    let mut doc = AutoCommit::new();

    let list = doc
        .put_object(&ROOT, "list", automerge::ObjType::List)
        .unwrap();
    let heads = doc.get_heads();

    let view = doc.view_at(&heads).unwrap();
    assert_eq!(view.object_type(&list).unwrap(), automerge::ObjType::List);
}

#[test]
fn test_view_at_parents() {
    let mut doc = AutoCommit::new();

    let map = doc
        .put_object(&ROOT, "parent", automerge::ObjType::Map)
        .unwrap();
    let nested = doc
        .put_object(&map, "child", automerge::ObjType::Map)
        .unwrap();
    let heads = doc.get_heads();

    let view = doc.view_at(&heads).unwrap();
    let parents: Vec<_> = view.parents(&nested).unwrap().collect();
    assert_eq!(parents.len(), 2); // child -> parent -> ROOT
}

#[test]
fn test_view_at_get_all() {
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(automerge::ActorId::from([1]));
    doc1.put(&ROOT, "key", "value1").unwrap();

    let mut doc2 = doc1.fork();
    doc2.set_actor(automerge::ActorId::from([2]));

    // Both make concurrent changes
    doc1.put(&ROOT, "key", "doc1_value").unwrap();
    doc2.put(&ROOT, "key", "doc2_value").unwrap();

    doc1.merge(&mut doc2).unwrap();
    let heads = doc1.get_heads();

    // Now there are two concurrent values
    let view = doc1.view_at(&heads).unwrap();
    let all_values = view.get_all(&ROOT, "key").unwrap();
    assert_eq!(all_values.len(), 2);
}

#[test]
fn test_view_at_stats() {
    let mut doc = AutoCommit::new();
    doc.put(&ROOT, "key", "value").unwrap();
    let heads = doc.get_heads();

    let view = doc.view_at(&heads).unwrap();
    let stats = view.stats();

    // Stats should reflect the underlying document
    assert!(stats.num_ops > 0);
}
