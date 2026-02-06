use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Author, AutoCommit, ObjType, PatchAction, ReadDoc, ScalarValue, ROOT,
};

#[test]
fn revoke_apply_changes() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    doc.put(ROOT, "counter", ScalarValue::counter(1)).unwrap();
    doc.increment(ROOT, "counter", 2).unwrap();
    doc.put(&map, "key1", "value1").unwrap();
    doc.put(&map, "key2", "value2").unwrap();
    doc.put(&map, "key3", "value3").unwrap();
    doc.splice(&list, 0, 0, [1.into(), 2.into(), 3.into(), 4.into()])
        .unwrap();
    doc.splice_text(&text, 0, 0, "the quick fox jumped over the lazy dog")
        .unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.increment(ROOT, "counter", 4).unwrap();

    doc.merge(&mut fork).unwrap();

    let epoc = doc.get_heads();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoc).unwrap();
    remote.update_diff_cursor();

    doc.increment(ROOT, "counter", 8).unwrap();
    fork.increment(ROOT, "counter", 16).unwrap();
    fork.delete(&map, "key1").unwrap();
    fork.put(&map, "key2", "value4").unwrap();
    fork.put(&map, "key3", "value5").unwrap();
    fork.delete(&list, 1).unwrap();
    fork.insert(&list, 1, 100).unwrap();
    fork.splice_text(&text, 4, 5, "free").unwrap();
    let bad_map = fork.put_object(ROOT, "bad_map", ObjType::Map).unwrap();

    doc.merge(&mut fork).unwrap();

    doc.put(&map, "key3", "value6").unwrap();
    doc.put(&bad_map, "bad_key", "bad_val").unwrap();
    doc.insert(&list, 2, 200).unwrap();
    doc.splice_text(&text, 6, 2, "endly").unwrap();

    remote
        .load_incremental(&[doc.save_incremental(), fork.save_incremental()].concat())
        .unwrap();

    assert_eq!(
        remote.get(ROOT, "counter").unwrap().unwrap().0.to_i64(),
        Some(15)
    );
    assert_eq!(
        remote.get(&map, "key1").unwrap().unwrap().0,
        "value1".into()
    );
    assert_eq!(
        remote.get(&map, "key2").unwrap().unwrap().0,
        "value2".into()
    );
    assert_eq!(
        remote.get(&map, "key3").unwrap().unwrap().0,
        "value6".into()
    );
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, 200.into());
    assert_eq!(
        remote.text(&text).unwrap(),
        "the endlyquick fox jumped over the lazy dog"
    );

    let patches = remote.diff_incremental();
    println!("patches: {:?}", patches);

    assert_eq!(patches.len(), 4);
    assert!(matches!(
        patches[0].action,
        PatchAction::Increment { value: 8, .. }
    ));
    assert!(matches!(&patches[1].action, PatchAction::SpliceText { .. }));
    assert!(
        matches!(&patches[2].action, PatchAction::Insert { values, .. } if values.get(0).unwrap().0 == 200.into() )
    );
    assert!(
        matches!(&patches[3].action, PatchAction::PutMap { value, .. } if value.0 == "value6".into() )
    );
}

#[test]
fn unrevoke_restores_changes() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "value1").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "key", "value2").unwrap();
    doc.merge(&mut fork).unwrap();

    let epoch = doc.get_heads();

    // After epoch, bad author makes more changes
    fork.put(ROOT, "new_key", "new_bad_value").unwrap();
    doc.merge(&mut fork).unwrap();

    // Revoke bad author
    doc.revoke(&bad, &epoch).unwrap();
    // The revoke should undo the post-epoch put
    assert!(doc.get(ROOT, "new_key").unwrap().is_none());
    // value2 was set before epoch so it should still be there
    let all = doc.get_all(ROOT, "key").unwrap();
    assert!(all.iter().any(|(v, _)| *v == "value2".into()));
    // iter() should show "key" but not "new_key"
    let iter_keys: Vec<_> = doc
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(iter_keys.contains(&"key".to_string()));
    assert!(!iter_keys.contains(&"new_key".to_string()));

    // Unrevoke should restore it
    let patches = doc.unrevoke(&bad);
    assert_eq!(
        doc.get(ROOT, "new_key").unwrap().unwrap().0,
        "new_bad_value".into()
    );
    // iter() should show both "key" and "new_key" after unrevoke
    let iter_keys: Vec<_> = doc
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(iter_keys.contains(&"key".to_string()));
    assert!(iter_keys.contains(&"new_key".to_string()));
    // Should have a patch for the restored value
    assert!(!patches.is_empty());
}

#[test]
fn revoke_list_insert_delete() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.splice(&list, 0, 0, [1.into(), 2.into(), 3.into()])
        .unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Bad author inserts and deletes in list
    fork.insert(&list, 1, 99).unwrap(); // [1, 99, 2, 3]
    fork.delete(&list, 3).unwrap(); // [1, 99, 2]
    doc.merge(&mut fork).unwrap();

    // Good author inserts after position affected by bad's changes
    doc.insert(&list, 0, 0).unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // Bad's insert of 99 should be revoked, bad's delete of 3 should be revoked
    assert_eq!(remote.length(&list), 4);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, 0.into());
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, 1.into());
    assert_eq!(remote.get(&list, 2).unwrap().unwrap().0, 2.into());
    assert_eq!(remote.get(&list, 3).unwrap().unwrap().0, 3.into());
}

#[test]
fn revoke_map_put_conflict() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Both authors concurrently update the same key after epoch
    doc.put(ROOT, "key", "good_update").unwrap();
    fork.put(ROOT, "key", "bad_update").unwrap();
    doc.merge(&mut fork).unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // Good author's value should win since bad's is revoked
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        "good_update".into()
    );
}

#[test]
fn revoke_text_splice() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Bad author modifies text
    fork.splice_text(&text, 5, 6, " everyone").unwrap(); // "hello everyone"

    // Good author also modifies text
    doc.splice_text(&text, 0, 0, "say ").unwrap(); // "say hello world"

    doc.merge(&mut fork).unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // Bad's splice should be revoked: the delete of " world" and insert of " everyone"
    // Good's "say " prefix should remain
    let result = remote.text(&text).unwrap();
    assert_eq!(result, "say hello world");
}

#[test]
fn revoke_text_mark() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    // Good author adds an italic mark on "world" before the epoch
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 6, 11),
        ExpandMark::Both,
    )
    .unwrap();

    let epoch = doc.get_heads();
    let mut fork = doc.fork().with_author(Some(bad.clone()));

    // Bad author adds a bold mark on "hello"
    fork.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();

    doc.merge(&mut fork).unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    let patches = remote.revoke(&bad, &epoch).unwrap();

    // Patches should reflect the removal of bad's bold mark but not italic
    let mark_patches: Vec<_> = patches
        .iter()
        .filter_map(|p| {
            if let PatchAction::Mark { marks } = &p.action {
                Some(marks)
            } else {
                None
            }
        })
        .collect();
    assert!(!mark_patches.is_empty(), "expected at least one Mark patch");
    let patch_names: Vec<&str> = mark_patches
        .iter()
        .flat_map(|marks| marks.iter().map(|m| m.name()))
        .collect();
    assert!(
        patch_names.contains(&"bold"),
        "expected a patch removing bold"
    );
    assert!(
        !patch_names.contains(&"italic"),
        "expected no patch touching italic"
    );

    // Bad's mark should be revoked
    let marks = remote.marks(&text).unwrap();
    assert!(marks.iter().all(|m| m.name() != "bold"));

    // Good's italic mark should still exist
    assert!(marks.iter().any(|m| m.name() == "italic"));

    // spans() should show the italic mark on "world" but not bold on "hello"
    let spans: Vec<_> = remote.spans(&text).unwrap().collect();
    let has_italic = spans.iter().any(|s| {
        if let automerge::Span::Text { marks: Some(m), .. } = s {
            m.iter().any(|(name, _)| name == "italic")
        } else {
            false
        }
    });
    assert!(has_italic, "italic mark should survive revoke");
    let has_bold = spans.iter().any(|s| {
        if let automerge::Span::Text { marks: Some(m), .. } = s {
            m.iter().any(|(name, _)| name == "bold")
        } else {
            false
        }
    });
    assert!(!has_bold, "bold mark should be revoked");
}

#[test]
fn revoke_counter_increment() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "counter", ScalarValue::counter(10)).unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Both authors increment
    doc.increment(ROOT, "counter", 5).unwrap();
    fork.increment(ROOT, "counter", 100).unwrap();
    doc.merge(&mut fork).unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // Only good's increment should count: 10 + 5 = 15
    assert_eq!(
        remote.get(ROOT, "counter").unwrap().unwrap().0.to_i64(),
        Some(15)
    );
}

#[test]
fn revoke_valid_put_in_revoked_object() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Bad author creates a map object
    let bad_map = fork.put_object(ROOT, "bad_map", ObjType::Map).unwrap();
    doc.merge(&mut fork).unwrap();

    // Good author puts into the bad object
    doc.put(&bad_map, "good_key", "good_val").unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // The bad_map itself was created by bad, so it should be revoked
    // Good author's put into it won't be visible since the parent is gone
    assert!(remote.get(ROOT, "bad_map").unwrap().is_none());
}

#[test]
fn revoke_valid_insert_after_revoked_insert() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "first").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Bad author inserts at index 1
    fork.insert(&list, 1, "bad_insert").unwrap();
    doc.merge(&mut fork).unwrap();

    // Good author inserts after bad's insert
    doc.insert(&list, 2, "good_insert").unwrap();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // Bad's insert should be gone, good's should remain
    assert_eq!(remote.length(&list), 2);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, "first".into());
    assert_eq!(
        remote.get(&list, 1).unwrap().unwrap().0,
        "good_insert".into()
    );
}

#[test]
fn revoke_valid_delete_of_insert() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let mut remote = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "first").unwrap();
    doc.insert(&list, 1, "second").unwrap();
    doc.insert(&list, 2, "third").unwrap();

    let epoch = doc.get_heads();

    remote.merge(&mut doc).unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.delete(&list, 1).unwrap();
    fork.insert(&list, 1, "bad_insert").unwrap();

    // Bad author inserts something
    doc.insert(&list, 0, "zero").unwrap();
    doc.merge(&mut fork).unwrap();

    remote.revoke(&bad, &epoch).unwrap();
    remote.merge(&mut doc).unwrap();

    // Both the insert and delete target the same element; result should be just "first"
    assert_eq!(remote.length(&list), 4);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, "zero".into());
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, "first".into());
    assert_eq!(remote.get(&list, 2).unwrap().unwrap().0, "second".into());
    assert_eq!(remote.get(&list, 3).unwrap().unwrap().0, "third".into());
}

#[test]
fn revoke_with_new_actor_same_author() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    let epoch = doc.get_heads();

    // Bad author makes changes with first actor
    fork.put(ROOT, "key1", "bad1").unwrap();

    // Create a new doc with the same bad author (different actor)
    let mut fork2 = doc.fork().with_author(Some(bad.clone()));
    fork2.put(ROOT, "key2", "bad2").unwrap();

    doc.merge(&mut fork).unwrap();

    let mut remote = AutoCommit::new();
    remote.merge(&mut doc).unwrap();
    remote.revoke(&bad, &epoch).unwrap();

    // new actor already revoked
    remote.update_diff_cursor();
    remote.merge(&mut fork2).unwrap();
    let patches = remote.diff_incremental();
    // patches should not contain bad keys
    for p in &patches {
        if let PatchAction::PutMap { key, .. } = &p.action {
            assert_ne!(key.as_str(), "key1");
            assert_ne!(key.as_str(), "key2");
        }
    }

    // Both actors under the bad author should be revoked
    assert!(remote.get(ROOT, "key1").unwrap().is_none());
    assert!(remote.get(ROOT, "key2").unwrap().is_none());
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        "original".into()
    );
    // iter() should not contain bad keys
    let iter_keys: Vec<_> = remote
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(!iter_keys.contains(&"key1".to_string()));
    assert!(!iter_keys.contains(&"key2".to_string()));
    assert!(iter_keys.contains(&"key".to_string()));
}

#[test]
fn revoke_only_after_epoch() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));

    // Bad author makes changes before epoch
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "before_epoch", "allowed").unwrap();
    doc.merge(&mut fork).unwrap();

    let epoch = doc.get_heads();

    // Bad author makes changes after epoch
    fork.put(ROOT, "after_epoch", "revoked").unwrap();
    doc.merge(&mut fork).unwrap();

    doc.revoke(&bad, &epoch).unwrap();

    // Pre-epoch changes should remain
    assert_eq!(
        doc.get(ROOT, "before_epoch").unwrap().unwrap().0,
        "allowed".into()
    );
    // Post-epoch changes should be revoked
    assert!(doc.get(ROOT, "after_epoch").unwrap().is_none());
}

#[test]
fn revoke_patches_reflect_undo() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    fork.put(ROOT, "key", "bad_override").unwrap();
    doc.merge(&mut fork).unwrap();

    // Verify bad's value is visible before revoke
    assert_eq!(
        doc.get(ROOT, "key").unwrap().unwrap().0,
        "bad_override".into()
    );

    let patches = doc.revoke(&bad, &epoch).unwrap();

    // Should get a patch restoring "original"
    assert!(!patches.is_empty());
    assert!(
        matches!(&patches[0].action, PatchAction::PutMap { value, .. } if value.0 == "original".into())
    );
    assert_eq!(doc.get(ROOT, "key").unwrap().unwrap().0, "original".into());
}

#[test]
fn revoke_then_load_incremental() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    // Bad author must make a change before epoch so its actor is known at revoke time
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "pre_epoch", "pre_val").unwrap();
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.revoke(&bad, &epoch).unwrap();
    remote.update_diff_cursor();

    // Bad author makes changes after the revocation point
    fork.put(ROOT, "bad_key", "bad_val").unwrap();
    fork.put(ROOT, "key", "bad_override").unwrap();

    // Good author also makes changes
    doc.merge(&mut fork).unwrap();
    doc.put(ROOT, "good_key", "good_val").unwrap();

    // Load new changes into the doc with revocation
    remote
        .load_incremental(&[doc.save_incremental(), fork.save_incremental()].concat())
        .unwrap();

    // Bad's post-epoch changes should be revoked
    assert!(remote.get(ROOT, "bad_key").unwrap().is_none());
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        "original".into()
    );
    // Bad's pre-epoch change should remain
    assert_eq!(
        remote.get(ROOT, "pre_epoch").unwrap().unwrap().0,
        "pre_val".into()
    );
    // Good's changes should be visible
    assert_eq!(
        remote.get(ROOT, "good_key").unwrap().unwrap().0,
        "good_val".into()
    );

    // Diff should only show good's changes
    let patches = remote.diff_incremental();
    assert!(!patches.is_empty());
    // Should not contain any patches for bad's post-epoch keys
    for p in &patches {
        if let PatchAction::PutMap { key, .. } = &p.action {
            assert_ne!(key.as_str(), "bad_key");
            assert_ne!(key.as_str(), "bad_override");
        }
    }
    // iter() should not contain bad keys
    let iter_keys: Vec<_> = remote
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(!iter_keys.contains(&"bad_key".to_string()));
    assert!(iter_keys.contains(&"key".to_string()));
    assert!(iter_keys.contains(&"good_key".to_string()));
    assert!(iter_keys.contains(&"pre_epoch".to_string()));
    // get() should not return bad values
    assert!(remote.get(ROOT, "bad_key").unwrap().is_none());
    assert_ne!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        "bad_override".into()
    );
}
