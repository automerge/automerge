use itertools::Itertools;
use pretty_assertions::assert_eq;

use super::*;
use crate::op_tree::B;
use crate::transaction::Transactable;
use crate::*;
use std::convert::TryInto;

#[test]
fn insert_op() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::random());
    let mut tx = doc.transaction();
    tx.put(ROOT, "hello", "world")?;
    tx.get(ROOT, "hello")?;
    tx.commit();
    Ok(())
}

#[test]
fn test_set() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    // setting a scalar value shouldn't return an opid as no object was created.
    tx.put(ROOT, "a", 1)?;

    // setting the same value shouldn't return an opid as there is no change.
    tx.put(ROOT, "a", 1)?;

    assert_eq!(tx.pending_ops(), 1);

    let map = tx.put_object(ROOT, "b", ObjType::Map)?;
    // object already exists at b but setting a map again overwrites it so we get an opid.
    tx.put(map, "a", 2)?;

    tx.put_object(ROOT, "b", ObjType::Map)?;

    assert_eq!(tx.pending_ops(), 4);
    let map = tx.get(ROOT, "b").unwrap().unwrap().1;
    assert_eq!(tx.get(&map, "a")?, None);

    tx.commit();
    Ok(())
}

#[test]
fn test_list() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::random());
    let mut tx = doc.transaction();
    let list_id = tx.put_object(ROOT, "items", ObjType::List)?;
    tx.put(ROOT, "zzz", "zzzval")?;
    assert!(tx.get(ROOT, "items")?.unwrap().1 == list_id);
    tx.insert(&list_id, 0, "a")?;
    tx.insert(&list_id, 0, "b")?;
    tx.insert(&list_id, 2, "c")?;
    tx.insert(&list_id, 1, "d")?;
    assert!(tx.get(&list_id, 0)?.unwrap().0 == "b".into());
    assert!(tx.get(&list_id, 1)?.unwrap().0 == "d".into());
    assert!(tx.get(&list_id, 2)?.unwrap().0 == "a".into());
    assert!(tx.get(&list_id, 3)?.unwrap().0 == "c".into());
    assert!(tx.length(&list_id) == 4);
    tx.commit();
    doc.save();
    Ok(())
}

#[test]
fn test_del() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::random());
    let mut tx = doc.transaction();
    tx.put(ROOT, "xxx", "xxx")?;
    assert!(tx.get(ROOT, "xxx")?.is_some());
    tx.delete(ROOT, "xxx")?;
    assert!(tx.get(ROOT, "xxx")?.is_none());
    tx.commit();
    Ok(())
}

#[test]
fn test_inc() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", ScalarValue::counter(10))?;
    assert!(tx.get(ROOT, "counter")?.unwrap().0 == Value::counter(10));
    tx.increment(ROOT, "counter", 10)?;
    assert!(tx.get(ROOT, "counter")?.unwrap().0 == Value::counter(20));
    tx.increment(ROOT, "counter", -5)?;
    assert!(tx.get(ROOT, "counter")?.unwrap().0 == Value::counter(15));
    tx.commit();
    Ok(())
}

#[test]
fn test_save_incremental() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();

    let mut tx = doc.transaction();
    tx.put(ROOT, "foo", 1)?;
    tx.commit();

    let save1 = doc.save();

    let mut tx = doc.transaction();
    tx.put(ROOT, "bar", 2)?;
    tx.commit();

    let save2 = doc.save_incremental();

    let mut tx = doc.transaction();
    tx.put(ROOT, "baz", 3)?;
    tx.commit();

    let save3 = doc.save_incremental();

    let mut save_a: Vec<u8> = vec![];
    save_a.extend(&save1);
    save_a.extend(&save2);
    save_a.extend(&save3);

    assert!(doc.save_incremental().is_empty());

    let save_b = doc.save();

    assert!(save_b.len() < save_a.len());

    let mut doc_a = Automerge::load(&save_a)?;
    let mut doc_b = Automerge::load(&save_b)?;

    assert!(doc_a.get_all(ROOT, "baz")? == doc_b.get_all(ROOT, "baz")?);

    assert!(doc_a.save() == doc_b.save());

    Ok(())
}

#[test]
fn test_save_text() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text)?;
    tx.commit();
    let heads1 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.splice_text(&text, 0, 0, "hello world")?;
    tx.commit();
    let heads2 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.splice_text(&text, 6, 0, "big bad ")?;
    tx.commit();
    let heads3 = doc.get_heads();

    assert!(&doc.text(&text)? == "hello big bad world");
    assert!(&doc.text_at(&text, &heads1)?.is_empty());
    assert!(&doc.text_at(&text, &heads2)? == "hello world");
    assert!(&doc.text_at(&text, &heads3)? == "hello big bad world");

    Ok(())
}

#[test]
fn test_props_vals_at() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    doc.set_actor("aaaa".try_into().unwrap());
    let mut tx = doc.transaction();
    tx.put(ROOT, "prop1", "val1")?;
    tx.commit();
    doc.get_heads();
    let heads1 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(ROOT, "prop1", "val2")?;
    tx.commit();
    doc.get_heads();
    let heads2 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(ROOT, "prop2", "val3")?;
    tx.commit();
    doc.get_heads();
    let heads3 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.delete(ROOT, "prop1")?;
    tx.commit();
    doc.get_heads();
    let heads4 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(ROOT, "prop3", "val4")?;
    tx.commit();
    doc.get_heads();
    let heads5 = doc.get_heads();
    assert!(doc.keys_at(ROOT, &heads1).collect_vec() == vec!["prop1".to_owned()]);
    assert_eq!(doc.length_at(ROOT, &heads1), 1);
    assert!(doc.get_at(ROOT, "prop1", &heads1)?.unwrap().0 == Value::str("val1"));
    assert!(doc.get_at(ROOT, "prop2", &heads1)? == None);
    assert!(doc.get_at(ROOT, "prop3", &heads1)? == None);

    assert!(doc.keys_at(ROOT, &heads2).collect_vec() == vec!["prop1".to_owned()]);
    assert_eq!(doc.length_at(ROOT, &heads2), 1);
    assert!(doc.get_at(ROOT, "prop1", &heads2)?.unwrap().0 == Value::str("val2"));
    assert!(doc.get_at(ROOT, "prop2", &heads2)? == None);
    assert!(doc.get_at(ROOT, "prop3", &heads2)? == None);

    assert!(
        doc.keys_at(ROOT, &heads3).collect_vec() == vec!["prop1".to_owned(), "prop2".to_owned()]
    );
    assert_eq!(doc.length_at(ROOT, &heads3), 2);
    assert!(doc.get_at(ROOT, "prop1", &heads3)?.unwrap().0 == Value::str("val2"));
    assert!(doc.get_at(ROOT, "prop2", &heads3)?.unwrap().0 == Value::str("val3"));
    assert!(doc.get_at(ROOT, "prop3", &heads3)? == None);

    assert!(doc.keys_at(ROOT, &heads4).collect_vec() == vec!["prop2".to_owned()]);
    assert_eq!(doc.length_at(ROOT, &heads4), 1);
    assert!(doc.get_at(ROOT, "prop1", &heads4)? == None);
    assert!(doc.get_at(ROOT, "prop2", &heads4)?.unwrap().0 == Value::str("val3"));
    assert!(doc.get_at(ROOT, "prop3", &heads4)? == None);

    assert!(
        doc.keys_at(ROOT, &heads5).collect_vec() == vec!["prop2".to_owned(), "prop3".to_owned()]
    );
    assert_eq!(doc.length_at(ROOT, &heads5), 2);
    assert_eq!(doc.length(ROOT), 2);
    assert!(doc.get_at(ROOT, "prop1", &heads5)? == None);
    assert!(doc.get_at(ROOT, "prop2", &heads5)?.unwrap().0 == Value::str("val3"));
    assert!(doc.get_at(ROOT, "prop3", &heads5)?.unwrap().0 == Value::str("val4"));

    assert_eq!(doc.keys_at(ROOT, &[]).count(), 0);
    assert_eq!(doc.length_at(ROOT, &[]), 0);
    assert!(doc.get_at(ROOT, "prop1", &[])? == None);
    assert!(doc.get_at(ROOT, "prop2", &[])? == None);
    assert!(doc.get_at(ROOT, "prop3", &[])? == None);
    Ok(())
}

#[test]
fn test_len_at() -> Result<(), AutomergeError> {
    let mut doc = Automerge::new();
    doc.set_actor("aaaa".try_into().unwrap());

    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List)?;
    tx.commit();
    let heads1 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.insert(&list, 0, 10)?;
    tx.commit();
    let heads2 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.put(&list, 0, 20)?;
    tx.insert(&list, 0, 30)?;
    tx.commit();
    let heads3 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.put(&list, 1, 40)?;
    tx.insert(&list, 1, 50)?;
    tx.commit();
    let heads4 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.delete(&list, 2)?;
    tx.commit();
    let heads5 = doc.get_heads();

    let mut tx = doc.transaction();
    tx.delete(&list, 0)?;
    tx.commit();
    let heads6 = doc.get_heads();

    assert!(doc.length_at(&list, &heads1) == 0);
    assert!(doc.get_at(&list, 0, &heads1)?.is_none());

    assert!(doc.length_at(&list, &heads2) == 1);
    assert!(doc.get_at(&list, 0, &heads2)?.unwrap().0 == Value::int(10));

    assert!(doc.length_at(&list, &heads3) == 2);
    assert!(doc.get_at(&list, 0, &heads3)?.unwrap().0 == Value::int(30));
    assert!(doc.get_at(&list, 1, &heads3)?.unwrap().0 == Value::int(20));

    assert!(doc.length_at(&list, &heads4) == 3);
    assert!(doc.get_at(&list, 0, &heads4)?.unwrap().0 == Value::int(30));
    assert!(doc.get_at(&list, 1, &heads4)?.unwrap().0 == Value::int(50));
    assert!(doc.get_at(&list, 2, &heads4)?.unwrap().0 == Value::int(40));

    assert!(doc.length_at(&list, &heads5) == 2);
    assert!(doc.get_at(&list, 0, &heads5)?.unwrap().0 == Value::int(30));
    assert!(doc.get_at(&list, 1, &heads5)?.unwrap().0 == Value::int(50));

    assert!(doc.length_at(&list, &heads6) == 1);
    assert!(doc.length(&list) == 1);
    assert!(doc.get_at(&list, 0, &heads6)?.unwrap().0 == Value::int(50));

    Ok(())
}

#[test]
fn keys_iter_map() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 3).unwrap();
    tx.put(ROOT, "b", 4).unwrap();
    tx.put(ROOT, "c", 5).unwrap();
    tx.put(ROOT, "d", 6).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 7).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 8).unwrap();
    tx.put(ROOT, "d", 9).unwrap();
    tx.commit();
    assert_eq!(doc.keys(ROOT).count(), 4);

    let mut keys = doc.keys(ROOT);
    assert_eq!(keys.next(), Some("a".into()));
    assert_eq!(keys.next(), Some("b".into()));
    assert_eq!(keys.next(), Some("c".into()));
    assert_eq!(keys.next(), Some("d".into()));
    assert_eq!(keys.next(), None);

    let mut keys = doc.keys(ROOT);
    assert_eq!(keys.next_back(), Some("d".into()));
    assert_eq!(keys.next_back(), Some("c".into()));
    assert_eq!(keys.next_back(), Some("b".into()));
    assert_eq!(keys.next_back(), Some("a".into()));
    assert_eq!(keys.next_back(), None);

    let mut keys = doc.keys(ROOT);
    assert_eq!(keys.next(), Some("a".into()));
    assert_eq!(keys.next_back(), Some("d".into()));
    assert_eq!(keys.next_back(), Some("c".into()));
    assert_eq!(keys.next_back(), Some("b".into()));
    assert_eq!(keys.next_back(), None);

    let mut keys = doc.keys(ROOT);
    assert_eq!(keys.next_back(), Some("d".into()));
    assert_eq!(keys.next(), Some("a".into()));
    assert_eq!(keys.next(), Some("b".into()));
    assert_eq!(keys.next(), Some("c".into()));
    assert_eq!(keys.next(), None);
    let keys = doc.keys(ROOT);
    assert_eq!(keys.collect::<Vec<_>>(), vec!["a", "b", "c", "d"]);
}

#[test]
fn keys_iter_seq() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.insert(&list, 0, 3).unwrap();
    tx.insert(&list, 1, 4).unwrap();
    tx.insert(&list, 2, 5).unwrap();
    tx.insert(&list, 3, 6).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(&list, 0, 7).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(&list, 0, 8).unwrap();
    tx.put(&list, 3, 9).unwrap();
    tx.commit();
    let actor = doc.get_actor();
    assert_eq!(doc.keys(&list).count(), 4);

    let mut keys = doc.keys(&list);
    assert_eq!(keys.next(), Some(format!("2@{}", actor)));
    assert_eq!(keys.next(), Some(format!("3@{}", actor)));
    assert_eq!(keys.next(), Some(format!("4@{}", actor)));
    assert_eq!(keys.next(), Some(format!("5@{}", actor)));
    assert_eq!(keys.next(), None);

    let mut keys = doc.keys(&list);
    assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("4@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("3@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("2@{}", actor)));
    assert_eq!(keys.next_back(), None);

    let mut keys = doc.keys(&list);
    assert_eq!(keys.next(), Some(format!("2@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("4@{}", actor)));
    assert_eq!(keys.next_back(), Some(format!("3@{}", actor)));
    assert_eq!(keys.next_back(), None);

    let mut keys = doc.keys(&list);
    assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
    assert_eq!(keys.next(), Some(format!("2@{}", actor)));
    assert_eq!(keys.next(), Some(format!("3@{}", actor)));
    assert_eq!(keys.next(), Some(format!("4@{}", actor)));
    assert_eq!(keys.next(), None);

    let keys = doc.keys(&list);
    assert_eq!(
        keys.collect::<Vec<_>>(),
        vec![
            format!("2@{}", actor),
            format!("3@{}", actor),
            format!("4@{}", actor),
            format!("5@{}", actor)
        ]
    );
}

#[test]
fn range_iter_map() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 3).unwrap();
    tx.put(ROOT, "b", 4).unwrap();
    tx.put(ROOT, "c", 5).unwrap();
    tx.put(ROOT, "d", 6).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 7).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 8).unwrap();
    tx.put(ROOT, "d", 9).unwrap();
    tx.commit();
    let actor = doc.get_actor();
    assert_eq!(doc.map_range(ROOT, ..).count(), 4);

    let mut range = doc.map_range(ROOT, "b".to_owned().."d".into());
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let mut range = doc.map_range(ROOT, "b".to_owned()..="d".into());
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("d", 9.into(), ExId::Id(7, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let mut range = doc.map_range(ROOT, ..="c".to_owned());
    assert_eq!(
        range.next(),
        Some(("a", 8.into(), ExId::Id(6, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let range = doc.map_range(ROOT, "a".to_owned()..);
    assert_eq!(
        range.collect::<Vec<_>>(),
        vec![
            ("a", 8.into(), ExId::Id(6, actor.clone(), 0)),
            ("b", 4.into(), ExId::Id(2, actor.clone(), 0)),
            ("c", 5.into(), ExId::Id(3, actor.clone(), 0)),
            ("d", 9.into(), ExId::Id(7, actor.clone(), 0)),
        ]
    );
}

#[test]
fn map_range_back_and_forth_single() {
    let mut doc = AutoCommit::new();
    let actor = doc.get_actor().clone();

    doc.put(ROOT, "1", "a").unwrap();
    doc.put(ROOT, "2", "b").unwrap();
    doc.put(ROOT, "3", "c").unwrap();

    let mut range_all = doc.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", Value::str("b"), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range(ROOT, ..);
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("1", "a".into(), ExId::Id(1, actor, 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);
}

#[test]
fn map_range_back_and_forth_double() {
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([0]));

    doc1.put(ROOT, "1", "a").unwrap();
    doc1.put(ROOT, "2", "b").unwrap();
    doc1.put(ROOT, "3", "c").unwrap();

    // actor 2 should win in all conflicts here
    let mut doc2 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    let actor2 = doc2.get_actor().clone();
    doc2.put(ROOT, "1", "aa").unwrap();
    doc2.put(ROOT, "2", "bb").unwrap();
    doc2.put(ROOT, "3", "cc").unwrap();

    doc1.merge(&mut doc2).unwrap();

    let mut range_all = doc1.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range(ROOT, ..);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range(ROOT, ..);
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("1", "aa".into(), ExId::Id(1, actor2, 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);
}

#[test]
fn map_range_at_back_and_forth_single() {
    let mut doc = AutoCommit::new();
    let actor = doc.get_actor().clone();

    doc.put(ROOT, "1", "a").unwrap();
    doc.put(ROOT, "2", "b").unwrap();
    doc.put(ROOT, "3", "c").unwrap();

    let heads = doc.get_heads();

    let mut range_all = doc.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", Value::str("b"), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "a".into(), ExId::Id(1, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next_back(),
        Some(("3", "c".into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "b".into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("1", "a".into(), ExId::Id(1, actor, 0)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);
}

#[test]
fn map_range_at_back_and_forth_double() {
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([0]));

    doc1.put(ROOT, "1", "a").unwrap();
    doc1.put(ROOT, "2", "b").unwrap();
    doc1.put(ROOT, "3", "c").unwrap();

    // actor 2 should win in all conflicts here
    let mut doc2 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    let actor2 = doc2.get_actor().clone();
    doc2.put(ROOT, "1", "aa").unwrap();
    doc2.put(ROOT, "2", "bb").unwrap();
    doc2.put(ROOT, "3", "cc").unwrap();

    doc1.merge(&mut doc2).unwrap();
    let heads = doc1.get_heads();

    let mut range_all = doc1.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next(),
        Some(("1", "aa".into(), ExId::Id(1, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);

    let mut range_all = doc1.map_range_at(ROOT, .., &heads);
    assert_eq!(
        range_all.next_back(),
        Some(("3", "cc".into(), ExId::Id(3, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("2", "bb".into(), ExId::Id(2, actor2.clone(), 1)))
    );
    assert_eq!(
        range_all.next_back(),
        Some(("1", "aa".into(), ExId::Id(1, actor2, 1)))
    );
    assert_eq!(range_all.next_back(), None);
    assert_eq!(range_all.next(), None);
}

#[test]
fn insert_at_index() {
    let mut doc = AutoCommit::new();

    let list = &doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(list, 0, 0).unwrap();
    doc.insert(list, 0, 1).unwrap(); // both inserts at the same index

    assert_eq!(doc.length(list), 2);
    assert_eq!(doc.keys(list).count(), 2);
    assert_eq!(doc.list_range(list, ..).count(), 2);
}

#[test]
fn get_list_values() -> Result<(), AutomergeError> {
    let mut doc1 = Automerge::new();
    let mut tx = doc1.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List)?;

    // insert elements
    tx.insert(&list, 0, "First")?;
    tx.insert(&list, 1, "Second")?;
    tx.insert(&list, 2, "Third")?;
    tx.insert(&list, 3, "Forth")?;
    tx.insert(&list, 4, "Fith")?;
    tx.insert(&list, 5, "Sixth")?;
    tx.insert(&list, 6, "Seventh")?;
    tx.insert(&list, 7, "Eights")?;
    tx.commit();

    let v1 = doc1.get_heads();
    let mut doc2 = doc1.fork();

    let mut tx = doc1.transaction();
    tx.put(&list, 2, "Third V2")?;
    tx.commit();

    let mut tx = doc2.transaction();
    tx.put(&list, 2, "Third V3")?;
    tx.commit();

    doc1.merge(&mut doc2)?;

    assert_eq!(doc1.list_range(&list, ..).count(), 8);

    for (i, val1, id) in doc1.list_range(&list, ..) {
        let val2 = doc1.get(&list, i)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.list_range(&list, 3..6).count(), 3);
    assert_eq!(doc1.list_range(&list, 3..6).next().unwrap().0, 3);
    assert_eq!(doc1.list_range(&list, 3..6).last().unwrap().0, 5);

    for (i, val1, id) in doc1.list_range(&list, 3..6) {
        let val2 = doc1.get(&list, i)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.list_range_at(&list, .., &v1).count(), 8);
    for (i, val1, id) in doc1.list_range_at(&list, .., &v1) {
        let val2 = doc1.get_at(&list, i, &v1)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.list_range_at(&list, 3..6, &v1).count(), 3);
    assert_eq!(doc1.list_range_at(&list, 3..6, &v1).next().unwrap().0, 3);
    assert_eq!(doc1.list_range_at(&list, 3..6, &v1).last().unwrap().0, 5);

    for (i, val1, id) in doc1.list_range_at(&list, 3..6, &v1) {
        let val2 = doc1.get_at(&list, i, &v1)?;
        assert_eq!(Some((val1, id)), val2);
    }

    let range: Vec<_> = doc1
        .list_range(&list, ..)
        .map(|(_, val, id)| (val, id))
        .collect();
    let values = doc1.values(&list);
    let values: Vec<_> = values.collect();
    assert_eq!(range, values);

    let range: Vec<_> = doc1
        .list_range_at(&list, .., &v1)
        .map(|(_, val, id)| (val, id))
        .collect();
    let values: Vec<_> = doc1.values_at(&list, &v1).collect();
    assert_eq!(range, values);

    Ok(())
}

#[test]
fn get_range_values() -> Result<(), AutomergeError> {
    let mut doc1 = Automerge::new();
    let mut tx = doc1.transaction();
    tx.put(ROOT, "aa", "aaa")?;
    tx.put(ROOT, "bb", "bbb")?;
    tx.put(ROOT, "cc", "ccc")?;
    tx.put(ROOT, "dd", "ddd")?;
    tx.commit();

    let v1 = doc1.get_heads();
    let mut doc2 = doc1.fork();

    let mut tx = doc1.transaction();
    tx.put(ROOT, "cc", "ccc V2")?;
    tx.commit();

    let mut tx = doc2.transaction();
    tx.put(ROOT, "cc", "ccc V3")?;
    tx.commit();

    doc1.merge(&mut doc2)?;

    let range = "b".to_string().."d".to_string();

    assert_eq!(doc1.map_range(ROOT, range.clone()).count(), 2);

    for (key, val1, id) in doc1.map_range(ROOT, range.clone()) {
        let val2 = doc1.get(ROOT, key)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.map_range(ROOT, range.clone()).rev().count(), 2);

    for (key, val1, id) in doc1.map_range(ROOT, range.clone()).rev() {
        let val2 = doc1.get(ROOT, key)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.map_range_at(ROOT, range.clone(), &v1).count(), 2);

    for (key, val1, id) in doc1.map_range_at(ROOT, range.clone(), &v1) {
        let val2 = doc1.get_at(ROOT, key, &v1)?;
        assert_eq!(Some((val1, id)), val2);
    }

    assert_eq!(doc1.map_range_at(ROOT, range.clone(), &v1).rev().count(), 2);

    for (key, val1, id) in doc1.map_range_at(ROOT, range, &v1).rev() {
        let val2 = doc1.get_at(ROOT, key, &v1)?;
        assert_eq!(Some((val1, id)), val2);
    }

    let range: Vec<_> = doc1
        .map_range(ROOT, ..)
        .map(|(_, val, id)| (val, id))
        .collect();
    let values: Vec<_> = doc1.values(ROOT).collect();
    assert_eq!(range, values);

    let range: Vec<_> = doc1
        .map_range_at(ROOT, .., &v1)
        .map(|(_, val, id)| (val, id))
        .collect();
    let values: Vec<_> = doc1.values_at(ROOT, &v1).collect();
    assert_eq!(range, values);

    Ok(())
}

#[test]
fn range_iter_map_rev() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 3).unwrap();
    tx.put(ROOT, "b", 4).unwrap();
    tx.put(ROOT, "c", 5).unwrap();
    tx.put(ROOT, "d", 6).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 7).unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 8).unwrap();
    tx.put(ROOT, "d", 9).unwrap();
    tx.commit();
    let actor = doc.get_actor();
    assert_eq!(doc.map_range(ROOT, ..).rev().count(), 4);

    let mut range = doc.map_range(ROOT, "b".to_owned().."d".into()).rev();
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let mut range = doc.map_range(ROOT, "b".to_owned()..="d".into()).rev();
    assert_eq!(
        range.next(),
        Some(("d", 9.into(), ExId::Id(7, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let mut range = doc.map_range(ROOT, ..="c".to_owned()).rev();
    assert_eq!(
        range.next(),
        Some(("c", 5.into(), ExId::Id(3, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("b", 4.into(), ExId::Id(2, actor.clone(), 0)))
    );
    assert_eq!(
        range.next(),
        Some(("a", 8.into(), ExId::Id(6, actor.clone(), 0)))
    );
    assert_eq!(range.next(), None);

    let range = doc.map_range(ROOT, "a".to_owned()..).rev();
    assert_eq!(
        range.collect::<Vec<_>>(),
        vec![
            ("d", 9.into(), ExId::Id(7, actor.clone(), 0)),
            ("c", 5.into(), ExId::Id(3, actor.clone(), 0)),
            ("b", 4.into(), ExId::Id(2, actor.clone(), 0)),
            ("a", 8.into(), ExId::Id(6, actor.clone(), 0)),
        ]
    );
}

#[test]
fn rolling_back_transaction_has_no_effect() {
    let mut doc = Automerge::new();
    let old_states = doc.states.clone();
    let bytes = doc.save();
    let tx = doc.transaction();
    tx.rollback();
    let new_states = doc.states.clone();
    assert_eq!(old_states, new_states);
    let new_bytes = doc.save();
    assert_eq!(bytes, new_bytes);
}

#[test]
fn mutate_old_objects() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    // create a map
    let map1 = tx.put_object(ROOT, "a", ObjType::Map).unwrap();
    tx.put(&map1, "b", 1).unwrap();
    // overwrite the first map with a new one
    let map2 = tx.put_object(ROOT, "a", ObjType::Map).unwrap();
    tx.put(&map2, "c", 2).unwrap();
    tx.commit();

    // we can get the new map by traversing the tree
    let map = doc.get(&ROOT, "a").unwrap().unwrap().1;
    assert_eq!(doc.get(&map, "b").unwrap(), None);
    // and get values from it
    assert_eq!(
        doc.get(&map, "c").unwrap().map(|s| s.0),
        Some(ScalarValue::Int(2).into())
    );

    // but we can still access the old one if we know the ID!
    assert_eq!(doc.get(&map1, "b").unwrap().unwrap().0, Value::int(1));
    // and even set new things in it!
    let mut tx = doc.transaction();
    tx.put(&map1, "c", 3).unwrap();
    tx.commit();

    assert_eq!(doc.get(&map1, "c").unwrap().unwrap().0, Value::int(3));
}

#[test]
fn delete_nothing_in_map_is_noop() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    // deleting a missing key in a map should just be a noop
    assert!(tx.delete(ROOT, "a",).is_ok());
    tx.commit();
    let last_change = doc.get_last_local_change().unwrap();
    assert_eq!(last_change.len(), 0);

    let bytes = doc.save();
    assert!(Automerge::load(&bytes,).is_ok());

    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1).unwrap();
    tx.commit();
    let last_change = doc.get_last_local_change().unwrap();
    assert_eq!(last_change.len(), 1);

    let mut tx = doc.transaction();
    // a real op
    tx.delete(ROOT, "a").unwrap();
    // a no-op
    tx.delete(ROOT, "a").unwrap();
    tx.commit();
    let last_change = doc.get_last_local_change().unwrap();
    assert_eq!(last_change.len(), 1);
}

#[test]
fn delete_nothing_in_list_returns_error() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    // deleting an element in a list that does not exist is an error
    assert!(tx.delete(ROOT, 0,).is_err());
}

#[test]
fn loaded_doc_changes_have_hash() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "a", 1_u64).unwrap();
    tx.commit();
    let hash = doc.get_last_local_change().unwrap().hash();
    let bytes = doc.save();
    let doc = Automerge::load(&bytes).unwrap();
    assert_eq!(doc.get_change_by_hash(&hash).unwrap().hash(), hash);
}

#[test]
fn load_change_with_zero_start_op() {
    let bytes = &[
        133, 111, 74, 131, 202, 50, 52, 158, 2, 96, 163, 163, 83, 255, 255, 255, 50, 50, 50, 50,
        50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 255, 255, 245, 53, 1, 0, 0, 0, 0, 0, 0, 4, 233,
        245, 239, 255, 1, 0, 0, 0, 133, 111, 74, 131, 163, 96, 0, 0, 2, 10, 202, 144, 125, 19, 48,
        89, 133, 49, 10, 10, 67, 91, 111, 10, 74, 131, 96, 0, 163, 131, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 1, 153, 0, 0, 246, 255, 255, 255, 157, 157, 157, 157, 157, 157, 157,
        157, 157, 157, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 48, 254, 208,
    ];
    let _ = Automerge::load(bytes);
}

#[test]
fn load_broken_list() {
    enum Action {
        InsertText(usize, char),
        DelText(usize),
    }
    use Action::*;
    let actions = [
        InsertText(0, 'a'),
        InsertText(0, 'b'),
        DelText(1),
        InsertText(0, 'c'),
        DelText(1),
        DelText(0),
        InsertText(0, 'd'),
        InsertText(0, 'e'),
        InsertText(1, 'f'),
        DelText(2),
        DelText(1),
        InsertText(0, 'g'),
        DelText(1),
        DelText(0),
        InsertText(0, 'h'),
        InsertText(1, 'i'),
        DelText(1),
        DelText(0),
        InsertText(0, 'j'),
        InsertText(0, 'k'),
        DelText(1),
        DelText(0),
        InsertText(0, 'l'),
        DelText(0),
        InsertText(0, 'm'),
        InsertText(0, 'n'),
        DelText(1),
        DelText(0),
        InsertText(0, 'o'),
        DelText(0),
        InsertText(0, 'p'),
        InsertText(1, 'q'),
        InsertText(1, 'r'),
        InsertText(1, 's'),
        InsertText(3, 't'),
        InsertText(5, 'u'),
        InsertText(0, 'v'),
        InsertText(3, 'w'),
        InsertText(4, 'x'),
        InsertText(0, 'y'),
        InsertText(6, 'z'),
        InsertText(11, '1'),
        InsertText(0, '2'),
        InsertText(0, '3'),
        InsertText(0, '4'),
        InsertText(13, '5'),
        InsertText(11, '6'),
        InsertText(17, '7'),
    ];
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    for action in actions {
        match action {
            Action::InsertText(index, c) => {
                tx.insert(&list, index, c).unwrap();
            }
            Action::DelText(index) => {
                tx.delete(&list, index).unwrap();
            }
        }
    }
    tx.commit();
    let bytes = doc.save();
    let mut doc2 = Automerge::load(&bytes).unwrap();
    let bytes2 = doc2.save();
    assert_eq!(doc.text(&list).unwrap(), doc2.text(&list).unwrap());

    assert_eq!(doc.queue, doc2.queue);
    assert_eq!(doc.history, doc2.history);
    assert_eq!(doc.history_index, doc2.history_index);
    assert_eq!(doc.states, doc2.states);
    assert_eq!(doc.deps, doc2.deps);
    assert_eq!(doc.saved, doc2.saved);
    assert_eq!(doc.ops, doc2.ops);
    assert_eq!(doc.max_op, doc2.max_op);

    assert_eq!(bytes, bytes2);
}

#[test]
fn load_broken_list_short() {
    // breaks when the B constant in OpSet is 3
    enum Action {
        InsertText(usize, char),
        DelText(usize),
    }
    use Action::*;
    let actions = [
        InsertText(0, 'a'),
        InsertText(1, 'b'),
        DelText(1),
        InsertText(1, 'c'),
        InsertText(2, 'd'),
        InsertText(2, 'e'),
        InsertText(0, 'f'),
        DelText(4),
        InsertText(4, 'g'),
    ];
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    for action in actions {
        match action {
            Action::InsertText(index, c) => {
                tx.insert(&list, index, c).unwrap();
            }
            Action::DelText(index) => {
                tx.delete(&list, index).unwrap();
            }
        }
    }
    tx.commit();
    let bytes = doc.save();
    let mut doc2 = Automerge::load(&bytes).unwrap();
    let bytes2 = doc2.save();
    assert_eq!(doc.text(&list).unwrap(), doc2.text(&list).unwrap());

    assert_eq!(doc.queue, doc2.queue);
    assert_eq!(doc.history, doc2.history);
    assert_eq!(doc.history_index, doc2.history_index);
    assert_eq!(doc.states, doc2.states);
    assert_eq!(doc.deps, doc2.deps);
    assert_eq!(doc.saved, doc2.saved);
    assert_eq!(doc.ops, doc2.ops);
    assert_eq!(doc.max_op, doc2.max_op);

    assert_eq!(bytes, bytes2);
}

#[test]
fn compute_list_indexes_correctly_when_list_element_is_split_across_tree_nodes() {
    let max = B as u64 * 2;
    let actor1 = ActorId::from(b"aaaa");
    let mut doc1 = AutoCommit::new().with_actor(actor1.clone());
    let actor2 = ActorId::from(b"bbbb");
    let mut doc2 = AutoCommit::new().with_actor(actor2.clone());
    let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();
    doc1.insert(&list, 0, 0).unwrap();
    doc2.load_incremental(&doc1.save_incremental()).unwrap();
    for i in 1..=max {
        doc1.put(&list, 0, i).unwrap()
    }
    for i in 1..=max {
        doc2.put(&list, 0, i).unwrap()
    }
    let change1 = doc1.save_incremental();
    let change2 = doc2.save_incremental();
    doc2.load_incremental(&change1).unwrap();
    doc1.load_incremental(&change2).unwrap();
    assert_eq!(doc1.length(&list), 1);
    assert_eq!(doc2.length(&list), 1);
    assert_eq!(
        doc1.get_all(&list, 0).unwrap(),
        vec![
            (max.into(), ExId::Id(max + 2, actor1.clone(), 0)),
            (max.into(), ExId::Id(max + 2, actor2.clone(), 1))
        ]
    );
    assert_eq!(
        doc2.get_all(&list, 0).unwrap(),
        vec![
            (max.into(), ExId::Id(max + 2, actor1, 0)),
            (max.into(), ExId::Id(max + 2, actor2, 1))
        ]
    );
    assert!(doc1.get(&list, 1).unwrap().is_none());
    assert!(doc2.get(&list, 1).unwrap().is_none());
}

#[test]
fn get_parent_objects() {
    let mut doc = AutoCommit::new();
    let map = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
    let list = doc.insert_object(&map, 0, ObjType::List).unwrap();
    doc.insert(&list, 0, 2).unwrap();
    let text = doc.put_object(&list, 0, ObjType::Text).unwrap();

    assert_eq!(
        doc.parents(&map).unwrap().next(),
        Some((ROOT, Prop::Map("a".into())))
    );
    assert_eq!(
        doc.parents(&list).unwrap().next(),
        Some((map, Prop::Seq(0)))
    );
    assert_eq!(
        doc.parents(&text).unwrap().next(),
        Some((list, Prop::Seq(0)))
    );
}

#[test]
fn get_path_to_object() {
    let mut doc = AutoCommit::new();
    let map = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
    let list = doc.insert_object(&map, 0, ObjType::List).unwrap();
    doc.insert(&list, 0, 2).unwrap();
    let text = doc.put_object(&list, 0, ObjType::Text).unwrap();

    assert_eq!(
        doc.path_to_object(&map).unwrap(),
        vec![(ROOT, Prop::Map("a".into()))]
    );
    assert_eq!(
        doc.path_to_object(&list).unwrap(),
        vec![(ROOT, Prop::Map("a".into())), (map.clone(), Prop::Seq(0)),]
    );
    assert_eq!(
        doc.path_to_object(&text).unwrap(),
        vec![
            (ROOT, Prop::Map("a".into())),
            (map, Prop::Seq(0)),
            (list, Prop::Seq(0)),
        ]
    );
}

#[test]
fn parents_iterator() {
    let mut doc = AutoCommit::new();
    let map = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
    let list = doc.insert_object(&map, 0, ObjType::List).unwrap();
    doc.insert(&list, 0, 2).unwrap();
    let text = doc.put_object(&list, 0, ObjType::Text).unwrap();

    let mut parents = doc.parents(text).unwrap();
    assert_eq!(parents.next(), Some((list, Prop::Seq(0))));
    assert_eq!(parents.next(), Some((map, Prop::Seq(0))));
    assert_eq!(parents.next(), Some((ROOT, Prop::Map("a".into()))));
    assert_eq!(parents.next(), None);
}

#[test]
fn can_insert_a_grapheme_into_text() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    let polar_bear = "🐻‍❄️";
    tx.insert(&text, 0, polar_bear).unwrap();
    tx.commit();
    let s = doc.text(&text).unwrap();
    assert_eq!(s, polar_bear);
    let len = doc.length(&text);
    assert_eq!(len, 1); // just one grapheme
}

#[test]
fn can_insert_long_string_into_text() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    let polar_bear = "🐻‍❄️";
    let polar_bear_army = polar_bear.repeat(100);
    tx.insert(&text, 0, &polar_bear_army).unwrap();
    tx.commit();
    let s = doc.text(&text).unwrap();
    assert_eq!(s, polar_bear_army);
    let len = doc.length(&text);
    assert_eq!(len, 1); // many graphemes
}

#[test]
fn splice_text_uses_unicode_scalars() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    let polar_bear = "🐻‍❄️";
    tx.splice_text(&text, 0, 0, polar_bear).unwrap();
    tx.commit();
    let s = doc.text(&text).unwrap();
    assert_eq!(s, polar_bear);
    let len = doc.length(&text);
    assert_eq!(len, 4); // 4 chars
}

#[test]
fn observe_counter_change_application_overwrite() {
    let mut doc1 = AutoCommit::new();
    doc1.set_actor(ActorId::from([1]));
    doc1.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc1.commit();

    let mut doc2 = doc1.fork();
    doc2.set_actor(ActorId::from([2]));
    doc2.put(ROOT, "counter", "mystring").unwrap();
    doc2.commit();

    doc1.increment(ROOT, "counter", 2).unwrap();
    doc1.commit();
    doc1.increment(ROOT, "counter", 5).unwrap();
    doc1.commit();

    let mut observer = VecOpObserver::default();
    let mut doc3 = doc1.clone();
    doc3.merge_with(
        &mut doc2,
        ApplyOptions::default().with_op_observer(&mut observer),
    )
    .unwrap();

    assert_eq!(
        observer.take_patches(),
        vec![Patch::Put {
            obj: ExId::Root,
            key: Prop::Map("counter".into()),
            value: (
                ScalarValue::Str("mystring".into()).into(),
                ExId::Id(2, doc2.get_actor().clone(), 1)
            ),
            conflict: false
        }]
    );

    let mut observer = VecOpObserver::default();
    let mut doc4 = doc2.clone();
    doc4.merge_with(
        &mut doc1,
        ApplyOptions::default().with_op_observer(&mut observer),
    )
    .unwrap();

    // no patches as the increments operate on an invisible counter
    assert_eq!(observer.take_patches(), vec![]);
}

#[test]
fn observe_counter_change_application() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc.increment(ROOT, "counter", 2).unwrap();
    doc.increment(ROOT, "counter", 5).unwrap();
    let changes = doc.get_changes(&[]).unwrap().into_iter().cloned();

    let mut new_doc = AutoCommit::new();
    let mut observer = VecOpObserver::default();
    new_doc
        .apply_changes_with(
            changes,
            ApplyOptions::default().with_op_observer(&mut observer),
        )
        .unwrap();
    assert_eq!(
        observer.take_patches(),
        vec![
            Patch::Put {
                obj: ExId::Root,
                key: Prop::Map("counter".into()),
                value: (
                    ScalarValue::counter(1).into(),
                    ExId::Id(1, doc.get_actor().clone(), 0)
                ),
                conflict: false
            },
            Patch::Increment {
                obj: ExId::Root,
                key: Prop::Map("counter".into()),
                value: (2, ExId::Id(2, doc.get_actor().clone(), 0)),
            },
            Patch::Increment {
                obj: ExId::Root,
                key: Prop::Map("counter".into()),
                value: (5, ExId::Id(3, doc.get_actor().clone(), 0)),
            }
        ]
    );
}

#[test]
fn get_changes_heads_empty() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "key1", 1).unwrap();
    doc.commit();
    doc.put(ROOT, "key2", 1).unwrap();
    doc.commit();
    let heads = doc.get_heads();
    assert_eq!(doc.get_changes(&heads).unwrap(), Vec::<&Change>::new());
}
