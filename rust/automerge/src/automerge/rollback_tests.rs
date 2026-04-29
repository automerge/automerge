use crate::transaction::Transactable;
use crate::{ActorId, Automerge, ObjType, ReadDoc, ScalarValue, ROOT};

fn assert_checkpoint_eq(
    a: &std::collections::HashMap<&'static str, Vec<u8>>,
    b: &std::collections::HashMap<&'static str, Vec<u8>>,
) {
    assert_eq!(a.len(), b.len(), "checkpoint column count mismatch");
    for (key, va) in a {
        let vb = b
            .get(key)
            .unwrap_or_else(|| panic!("column {key:?} missing after rollback"));
        assert_eq!(va, vb, "column {key:?} differs after rollback");
    }
}

#[test]
fn rollback_create_map_and_put() {
    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "existing", "keep").unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        let map = tx.put_object(ROOT, "mymap", ObjType::Map).unwrap();
        tx.put(&map, "a", 1).unwrap();
        tx.put(&map, "b", "hello").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert!(doc.get(ROOT, "mymap").unwrap().is_none());
}

#[test]
fn rollback_create_list_and_insert() {
    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "x", 42).unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "mylist", ObjType::List).unwrap();
        tx.insert(&list, 0, 10).unwrap();
        tx.insert(&list, 1, 20).unwrap();
        tx.insert(&list, 2, 30).unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert!(doc.get(ROOT, "mylist").unwrap().is_none());
}

#[test]
fn rollback_create_text_and_splice() {
    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "keep", true).unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "mytext", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello world").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert!(doc.get(ROOT, "mytext").unwrap().is_none());
}

#[test]
fn rollback_splice_text_multiple_locations() {
    let mut doc = Automerge::new();

    let text;
    {
        let mut tx = doc.transaction();
        text = tx.put_object(ROOT, "content", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "abcdef").unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.splice_text(&text, 3, 0, "XYZ").unwrap();
        tx.splice_text(&text, 0, 2, "").unwrap();
        tx.splice_text(&text, 5, 0, "!!!").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert_eq!(doc.text(&text).unwrap(), "abcdef");
}

#[test]
fn rollback_increment_counter() {
    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "count", ScalarValue::counter(0)).unwrap();
        tx.increment(ROOT, "count", 5).unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.increment(ROOT, "count", 10).unwrap();
        tx.increment(ROOT, "count", 20).unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
}

#[test]
fn rollback_delete_map_key() {
    let mut doc = Automerge::new();
    {
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.put(ROOT, "b", 2).unwrap();
        tx.put(ROOT, "c", 3).unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.delete(ROOT, "b").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert!(doc.get(ROOT, "b").unwrap().is_some());
}

#[test]
fn rollback_delete_list_item() {
    let mut doc = Automerge::new();

    let list;
    {
        let mut tx = doc.transaction();
        list = tx.put_object(ROOT, "items", ObjType::List).unwrap();
        tx.insert(&list, 0, "a").unwrap();
        tx.insert(&list, 1, "b").unwrap();
        tx.insert(&list, 2, "c").unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.delete(&list, 1).unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert_eq!(doc.length(&list), 3);
}

#[test]
fn rollback_delete_text_range() {
    let mut doc = Automerge::new();

    let text;
    {
        let mut tx = doc.transaction();
        text = tx.put_object(ROOT, "t", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello world").unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.splice_text(&text, 5, 6, "").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert_eq!(doc.text(&text).unwrap(), "hello world");
}

#[test]
fn rollback_delete_conflicted_field() {
    let mut doc1 = Automerge::new();
    doc1.set_actor(ActorId::from(b"aaaa"));
    let mut doc2 = Automerge::new();
    doc2.set_actor(ActorId::from(b"bbbb"));

    {
        let mut tx = doc1.transaction();
        tx.put(ROOT, "field", "original").unwrap();
        tx.commit();
    }
    doc2.merge(&mut doc1).unwrap();

    {
        let mut tx = doc1.transaction();
        tx.put(ROOT, "field", "from_a").unwrap();
        tx.commit();
    }
    {
        let mut tx = doc2.transaction();
        tx.put(ROOT, "field", "from_b").unwrap();
        tx.commit();
    }
    doc1.merge(&mut doc2).unwrap();

    let conflict_count = doc1.get_all(ROOT, "field").unwrap().len();
    assert!(conflict_count >= 2, "expected conflict");

    let before = doc1.save_checkpoint();

    {
        let mut tx = doc1.transaction();
        tx.delete(ROOT, "field").unwrap();
        tx.rollback();
    }

    let after = doc1.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert_eq!(doc1.get_all(ROOT, "field").unwrap().len(), conflict_count);
}

#[test]
fn rollback_increment_counter_conflicted_by_non_counter() {
    // doc1 gets the higher actor so its counter wins over doc2's string.
    let mut doc1 = Automerge::new();
    doc1.set_actor(ActorId::from(b"zzzz"));
    let mut doc2 = Automerge::new();
    doc2.set_actor(ActorId::from(b"aaaa"));

    {
        let mut tx = doc1.transaction();
        tx.put(ROOT, "val", ScalarValue::counter(0)).unwrap();
        tx.commit();
    }
    doc2.merge(&mut doc1).unwrap();

    {
        let mut tx = doc1.transaction();
        tx.increment(ROOT, "val", 5).unwrap();
        tx.commit();
    }
    {
        let mut tx = doc2.transaction();
        tx.put(ROOT, "val", "not a counter").unwrap();
        tx.commit();
    }
    doc1.merge(&mut doc2).unwrap();

    let before = doc1.save_checkpoint();

    {
        let mut tx = doc1.transaction();
        // Increment may fail with MissingCounter if the non-counter wins.
        // Either way, rollback should restore the checkpoint.
        let _ = tx.increment(ROOT, "val", 10);
        tx.rollback();
    }

    let after = doc1.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
}

#[test]
fn rollback_delete_object() {
    let mut doc = Automerge::new();

    let map;
    {
        let mut tx = doc.transaction();
        map = tx.put_object(ROOT, "obj", ObjType::Map).unwrap();
        tx.put(&map, "nested_key", "nested_value").unwrap();
        tx.put(&map, "num", 42).unwrap();
        tx.commit();
    }

    let before = doc.save_checkpoint();

    {
        let mut tx = doc.transaction();
        tx.delete(ROOT, "obj").unwrap();
        tx.rollback();
    }

    let after = doc.save_checkpoint();
    assert_checkpoint_eq(&before, &after);
    assert!(doc.get(ROOT, "obj").unwrap().is_some());
    assert_eq!(
        doc.get(&map, "nested_key")
            .unwrap()
            .unwrap()
            .0
            .to_str()
            .unwrap(),
        "nested_value"
    );
}

#[test]
fn rollback_combined_operations() {
    let mut doc1 = Automerge::new();
    doc1.set_actor(ActorId::from(b"aaaa"));
    let mut doc2 = Automerge::new();
    doc2.set_actor(ActorId::from(b"bbbb"));

    let list;
    let text;
    {
        let mut tx = doc1.transaction();
        let map = tx.put_object(ROOT, "config", ObjType::Map).unwrap();
        tx.put(&map, "version", 1).unwrap();
        list = tx.put_object(ROOT, "items", ObjType::List).unwrap();
        tx.insert(&list, 0, "first").unwrap();
        tx.insert(&list, 1, "second").unwrap();
        text = tx.put_object(ROOT, "content", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "the quick brown fox").unwrap();
        tx.put(ROOT, "counter", ScalarValue::counter(10)).unwrap();
        tx.put(ROOT, "contested", "base").unwrap();
        tx.commit();
    }
    doc2.merge(&mut doc1).unwrap();

    {
        let mut tx = doc1.transaction();
        tx.put(ROOT, "contested", "from_a").unwrap();
        tx.commit();
    }
    {
        let mut tx = doc2.transaction();
        tx.put(ROOT, "contested", "from_b").unwrap();
        tx.commit();
    }
    doc1.merge(&mut doc2).unwrap();

    let config = doc1.get(ROOT, "config").unwrap().unwrap().1;

    let before = doc1.save_checkpoint();

    {
        let mut tx = doc1.transaction();

        tx.put(&config, "version", 2).unwrap();
        tx.put(&config, "new_key", "new_val").unwrap();

        tx.insert(&list, 1, "inserted").unwrap();
        tx.delete(&list, 0).unwrap();

        tx.splice_text(&text, 4, 5, "").unwrap();
        tx.splice_text(&text, 4, 0, "slow").unwrap();
        tx.splice_text(&text, 0, 0, ">>> ").unwrap();

        tx.increment(ROOT, "counter", 100).unwrap();

        tx.delete(ROOT, "contested").unwrap();

        let new_map = tx.put_object(ROOT, "temp", ObjType::Map).unwrap();
        tx.put(&new_map, "ephemeral", true).unwrap();

        tx.delete(ROOT, "config").unwrap();

        tx.rollback();
    }

    let after = doc1.save_checkpoint();
    assert_checkpoint_eq(&before, &after);

    assert!(doc1.get(ROOT, "config").unwrap().is_some());
    assert_eq!(doc1.length(&list), 2);
    assert_eq!(doc1.text(&text).unwrap(), "the quick brown fox");
    assert!(doc1.get(ROOT, "contested").unwrap().is_some());
    assert!(doc1.get(ROOT, "temp").unwrap().is_none());
    assert!(doc1.get_all(ROOT, "contested").unwrap().len() >= 2);
}
