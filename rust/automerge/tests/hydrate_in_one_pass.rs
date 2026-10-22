//! `hydrate_in_one_pass`, and `hydrate_gathered` object by object, read what
//! `hydrate` reads with no heads, for every kind of object and value, after
//! deletions, overwrites, increments, merges with conflicts, a save and
//! load, and under isolation and a write-frontier mask.
use automerge::marks::{ExpandMark, Mark};
use automerge::transaction::Transactable;
use automerge::{ActorId, Author, AutoCommit, ObjType, ReadDoc, ScalarValue, ROOT};

fn same(doc: &AutoCommit) {
    assert_eq!(doc.hydrate_in_one_pass(), doc.hydrate(&ROOT, None).unwrap());
    same_one_at_a_time(doc);
}

/// Each object under `children`, built from one gathering in turn, is what
/// `hydrate` gives for it alone, and building takes it out.
fn same_one_at_a_time(doc: &AutoCommit) {
    let Some((_, children)) = doc.get(&ROOT, "children").unwrap() else {
        return;
    };
    let mut gathered = doc.gather_in_one_pass();
    let before = gathered.len();
    for item in doc.map_range(&children, ..) {
        let id = item.id();
        if !matches!(item.value, automerge::ValueRef::Object(_)) {
            continue;
        }
        let built = doc.hydrate_gathered(&mut gathered, &id).unwrap();
        assert_eq!(built, doc.hydrate(&id, None).unwrap(), "{}", item.key);
        // Built once; a second build finds nothing left.
        let again = doc.hydrate_gathered(&mut gathered, &id).unwrap();
        assert!(is_empty(&again), "{} built twice: {again:?}", item.key);
    }
    assert!(gathered.len() < before);
}

fn is_empty(value: &automerge::hydrate::Value) -> bool {
    match value {
        automerge::hydrate::Value::Map(map) => map.iter().next().is_none(),
        automerge::hydrate::Value::List(list) => list.iter().next().is_none(),
        _ => false,
    }
}

/// A document holding one of everything, some of it changed after.
fn everything(actor: &str) -> AutoCommit {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(actor.as_bytes()));
    doc.put(&ROOT, "version", 2).unwrap();
    doc.put(&ROOT, "name", "example").unwrap();
    doc.put(&ROOT, "bytes", ScalarValue::Bytes(vec![1, 2, 3]))
        .unwrap();
    doc.put(&ROOT, "count", ScalarValue::counter(5)).unwrap();
    doc.increment(&ROOT, "count", 3).unwrap();
    let children = doc.put_object(&ROOT, "children", ObjType::Map).unwrap();
    for i in 0..40 {
        let child = doc
            .put_object(&children, format!("c{i:02}"), ObjType::Map)
            .unwrap();
        doc.put(&child, "kind", "leaf").unwrap();
        doc.put(&child, "at", i as i64).unwrap();
        let attrs = doc.put_object(&child, "attrs", ObjType::Map).unwrap();
        if i % 3 == 0 {
            doc.put(&attrs, "width", 500).unwrap();
        }
        let list = doc.put_object(&child, "items", ObjType::List).unwrap();
        for j in 0..(i % 4) {
            doc.insert(&list, j, j as i64).unwrap();
        }
        if i % 5 == 0 {
            let inner = doc.insert_object(&list, 0, ObjType::Map).unwrap();
            doc.put(&inner, "deep", true).unwrap();
        }
        if i % 7 == 0 {
            let text = doc.put_object(&child, "lines", ObjType::Text).unwrap();
            doc.splice_text(&text, 0, 0, "hello").unwrap();
            // Marks are ops of the text, read with it.
            doc.mark(
                &text,
                Mark::new("bold".to_owned(), true, 0, 3),
                ExpandMark::After,
            )
            .unwrap();
            doc.split_block(&text, 2).unwrap();
        }
    }
    doc.put_object(&ROOT, "empty map", ObjType::Map).unwrap();
    doc.put_object(&ROOT, "empty list", ObjType::List).unwrap();
    doc.put_object(&ROOT, "table", ObjType::Table).unwrap();
    // Deletions and overwrites leave ops that are no longer on top.
    doc.delete(&children, "c03").unwrap();
    doc.put(&children, "c04", "replaced by a scalar").unwrap();
    let c05 = doc.get(&children, "c05").unwrap().unwrap().1;
    let items = doc.get(&c05, "items").unwrap().unwrap().1;
    doc.delete(&items, 0).unwrap();
    doc.put(&ROOT, "name", "renamed").unwrap();
    doc.commit();
    doc
}

#[test]
fn one_of_everything_reads_alike() {
    let mut doc = everything("aaaa");
    same(&doc);
    // Uncommitted ops are read too.
    doc.put(&ROOT, "pending", 1).unwrap();
    same(&doc);
}

#[test]
fn a_merge_with_conflicts_reads_alike() {
    let mut left = everything("aaaa");
    let mut right = left.fork().with_actor(ActorId::from(b"bbbb".as_slice()));
    left.put(&ROOT, "name", "left").unwrap();
    right.put(&ROOT, "name", "right").unwrap();
    let children = left.get(&ROOT, "children").unwrap().unwrap().1;
    left.put(&children, "c10", "left wins?").unwrap();
    right.put_object(&children, "c10", ObjType::List).unwrap();
    right.increment(&ROOT, "count", 10).unwrap();
    left.merge(&mut right).unwrap();
    let hydrated = left.hydrate_in_one_pass();
    assert_eq!(hydrated, left.hydrate(&ROOT, None).unwrap());
    let automerge::hydrate::Value::Map(root) = hydrated else {
        panic!("the root is a map");
    };
    assert!(root.iter().any(|(_, value)| value.conflict));
    same(&left);
}

#[test]
fn a_loaded_document_reads_alike() {
    let mut doc = everything("aaaa");
    let loaded = AutoCommit::load(&doc.save()).unwrap();
    same(&loaded);
    assert_eq!(loaded.hydrate_in_one_pass(), doc.hydrate_in_one_pass());
    // `Automerge` reads alike too.
    let am = automerge::Automerge::load(&doc.save()).unwrap();
    assert_eq!(am.hydrate_in_one_pass(), am.hydrate(None));
}

#[test]
fn an_empty_document_reads_alike() {
    same(&AutoCommit::new());
    let am = automerge::Automerge::new();
    assert_eq!(am.hydrate_in_one_pass(), am.hydrate(None));
}

/// Two actors setting the same map keys at once, to scalars and to
/// objects, and keys deleted and set again: the one-pass read picks the
/// winner `hydrate` picks.
#[test]
fn concurrent_and_reset_keys_read_alike() {
    let mut left = everything("aaaa");
    let mut right = left.fork().with_actor(ActorId::from(b"bbbb".as_slice()));
    let children = left.get(&ROOT, "children").unwrap().unwrap().1;
    for i in 0..10 {
        let key = format!("c{i:02}");
        left.put(&children, &key, format!("left {i}")).unwrap();
        if i % 2 == 0 {
            let map = right.put_object(&children, &key, ObjType::Map).unwrap();
            right.put(&map, "from", "right").unwrap();
        } else {
            right.put(&children, &key, i as i64).unwrap();
        }
    }
    // Deleted on one side, set on the other.
    left.delete(&children, "c20").unwrap();
    right.put(&children, "c20", "set by right").unwrap();
    // Deleted and set again on one side.
    left.delete(&children, "c21").unwrap();
    left.put(&children, "c21", "set again").unwrap();
    let c22 = left.get(&children, "c22").unwrap().unwrap().1;
    left.delete(&c22, "at").unwrap();
    left.put(&c22, "at", 99).unwrap();
    left.merge(&mut right).unwrap();
    same(&left);
    let mut loaded = AutoCommit::load(&left.save()).unwrap();
    same(&loaded);
    loaded.put(&ROOT, "pending", 1).unwrap();
    same(&loaded);
}

/// An isolated document reads its isolated view, committed and with a
/// transaction in flight, not the ops merged in after its heads.
#[test]
fn an_isolated_document_reads_its_view() {
    let mut doc = everything("aaaa");
    let heads = doc.get_heads();
    let mut other = doc.fork().with_actor(ActorId::from(b"bbbb".as_slice()));
    let children = other.get(&ROOT, "children").unwrap().unwrap().1;
    other.put(&ROOT, "name", "later").unwrap();
    other.put(&children, "c01", "later").unwrap();
    other.increment(&ROOT, "count", 10).unwrap();
    doc.isolate(&heads);
    doc.merge(&mut other).unwrap();
    same(&doc);
    assert_eq!(
        doc.hydrate_in_one_pass(),
        everything("aaaa").hydrate_in_one_pass()
    );
    // A transaction in flight under isolation.
    doc.put(&ROOT, "pending", 1).unwrap();
    doc.increment(&ROOT, "count", 100).unwrap();
    same(&doc);
    doc.integrate();
    same(&doc);
}

/// A masked author's ops, increments included, are hidden from the
/// one-pass read as from `hydrate`.
#[test]
fn a_masked_author_is_hidden() {
    let one = Author::from(vec![1]);
    let two = Author::from(vec![2]);
    let mut doc = everything("aaaa").with_author(Some(one));
    doc.put(&ROOT, "by one", true).unwrap();
    doc.commit();
    let heads = doc.get_heads();
    doc.set_author(Some(two.clone()));
    let children = doc.get(&ROOT, "children").unwrap().unwrap().1;
    doc.put(&ROOT, "name", "by two").unwrap();
    doc.put(&children, "c01", "by two").unwrap();
    doc.increment(&ROOT, "count", 10).unwrap();
    doc.commit();
    let before = doc.hydrate_in_one_pass();
    doc.mask_author(two, &heads);
    let masked = doc.hydrate_in_one_pass();
    assert_ne!(masked, before);
    same(&doc);
}

/// Two actors putting at and inserting objects at the same list indexes.
#[test]
fn concurrent_list_edits_read_alike() {
    let mut left = everything("aaaa");
    let mut right = left.fork().with_actor(ActorId::from(b"bbbb".as_slice()));
    let children = left.get(&ROOT, "children").unwrap().unwrap().1;
    let c06 = left.get(&children, "c06").unwrap().unwrap().1;
    let items = left.get(&c06, "items").unwrap().unwrap().1;
    left.put(&items, 0, "left").unwrap();
    right.put(&items, 0, "right").unwrap();
    let map = left.insert_object(&items, 1, ObjType::Map).unwrap();
    left.put(&map, "from", "left").unwrap();
    let list = right.insert_object(&items, 1, ObjType::List).unwrap();
    right.insert(&list, 0, "from right").unwrap();
    right.insert(&items, 0, "first").unwrap();
    left.merge(&mut right).unwrap();
    same(&left);
    let automerge::hydrate::Value::List(list) = left.hydrate(&items, None).unwrap() else {
        panic!("a list");
    };
    assert!(list.iter().any(|item| item.conflict));
}

/// A masked author who made, changed and deleted objects is hidden from the
/// one-pass read as from `hydrate`, also with a transaction in flight and
/// under isolation.
#[test]
fn a_masked_author_making_and_deleting_objects_is_hidden() {
    let one = Author::from(vec![1]);
    let two = Author::from(vec![2]);
    let mut doc = everything("aaaa").with_author(Some(one.clone()));
    let first = doc.get_heads();
    doc.put(&ROOT, "by one", true).unwrap();
    doc.commit();
    let heads = doc.get_heads();
    doc.set_author(Some(two.clone()));
    let children = doc.get(&ROOT, "children").unwrap().unwrap().1;
    let made = doc.put_object(&children, "made", ObjType::Map).unwrap();
    doc.put(&made, "x", 1).unwrap();
    let list = doc.put_object(&made, "list", ObjType::List).unwrap();
    doc.insert_object(&list, 0, ObjType::Map).unwrap();
    doc.delete(&children, "c02").unwrap();
    doc.put(&children, "c06", "replaced").unwrap();
    let c08 = doc.get(&children, "c08").unwrap().unwrap().1;
    doc.put_object(&c08, "attrs", ObjType::List).unwrap();
    doc.commit();
    let before = doc.hydrate_in_one_pass();
    doc.mask_author(two, &heads);
    assert_ne!(doc.hydrate_in_one_pass(), before);
    same(&doc);
    // A transaction in flight, by an author who is not masked.
    doc.set_author(Some(one));
    doc.put(&ROOT, "pending", 1).unwrap();
    doc.increment(&ROOT, "count", 100).unwrap();
    same(&doc);
    doc.commit();
    // Isolated at earlier heads, with the mask still in place.
    doc.isolate(&first);
    same(&doc);
    doc.put(&ROOT, "pending", 2).unwrap();
    same(&doc);
}

/// A document with no deleted or replaced objects: building the root takes
/// every gathered object out, block maps inside text included.
#[test]
fn building_the_root_takes_everything() {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(b"blocks".as_slice()));
    let map = doc.put_object(&ROOT, "map", ObjType::Map).unwrap();
    doc.put(&map, "x", 1).unwrap();
    let list = doc.put_object(&map, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "a").unwrap();
    doc.insert_object(&list, 1, ObjType::Map).unwrap();
    let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    // Block maps are objects inside the text, with objects of their own.
    for at in [5, 0] {
        let block = doc.split_block(&text, at).unwrap();
        doc.put(&block, "type", "paragraph").unwrap();
        let parents = doc.put_object(&block, "parents", ObjType::List).unwrap();
        doc.insert(&parents, 0, "quote").unwrap();
        doc.put_object(&block, "attrs", ObjType::Map).unwrap();
    }
    doc.mark(
        &text,
        Mark::new("bold".to_owned(), true, 0, 3),
        ExpandMark::After,
    )
    .unwrap();
    doc.commit();
    same(&doc);

    let mut gathered = doc.gather_in_one_pass();
    assert!(!gathered.is_empty());
    doc.hydrate_gathered(&mut gathered, &ROOT).unwrap();
    assert!(gathered.is_empty(), "{} objects left", gathered.len());
}
