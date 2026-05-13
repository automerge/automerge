//! Tests for the visibility filter API.
//!
//! The first group covers the general shapes of the [`Filter`] API:
//! [`Rule::Deny`], [`Rule::AllowUpTo`] used as the *default* rule
//! ("schema validation" pattern), `actors`-keyed rules overriding
//! `authors`-keyed rules, and `update_filter` / save-load round-trips.
//!
//! The second group focuses on the `authors`-keyed [`Rule::AllowUpTo`]
//! pattern — "render the document as it was before this author started
//! editing" — across the various op kinds (lists, text, marks, counters,
//! nested objects).

use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, ChangeHash, Filter, ObjType, PatchAction, ReadDoc, Rule,
    ScalarValue, ROOT,
};

/// Install an `AllowUpTo` rule for `author`. Captures the recurring
/// "hide everything this author wrote after `heads`" pattern these tests
/// exercise.
fn hide_author_after(doc: &mut AutoCommit, author: &Author, heads: &[ChangeHash]) {
    doc.update_filter(|f| {
        f.authors.insert(
            author.clone(),
            Rule::AllowUpTo {
                heads: heads.to_vec(),
            },
        );
    });
}

/// Drop any existing rule for `author`, restoring full visibility for
/// their changes.
fn unhide_author(doc: &mut AutoCommit, author: &Author) {
    doc.update_filter(|f| {
        f.authors.remove(author);
    });
}

/// Convenience: install a fresh empty filter, used to clear all rules.
#[allow(dead_code)]
fn clear_filter(doc: &mut AutoCommit) {
    doc.set_filter(Filter::default());
}

// ---------------------------------------------------------------------------
// General filter API
// ---------------------------------------------------------------------------

#[test]
fn deny_rule_hides_authors_changes() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "alice_key", "alice_value").unwrap();

    let mut fork = doc.fork().with_author(Some(bob.clone()));
    fork.put(ROOT, "bob_key", "bob_value").unwrap();
    doc.merge(&mut fork).unwrap();

    // Hide every change made by Bob.
    doc.set_filter(Filter::default().with_author(bob.clone(), Rule::Deny));

    assert!(doc.get(ROOT, "bob_key").unwrap().is_none());
    assert_eq!(
        doc.get(ROOT, "alice_key").unwrap().unwrap().0,
        "alice_value".into()
    );

    // Restoring an empty filter brings Bob's changes back.
    doc.set_filter(Filter::default());
    assert_eq!(
        doc.get(ROOT, "bob_key").unwrap().unwrap().0,
        "bob_value".into()
    );
}

#[test]
fn default_allow_up_to_acts_as_validated_prefix() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();

    // Build a "validated" prefix.
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "validated", "yes").unwrap();
    let validated_heads = doc.get_heads();

    // Both authors keep editing past the validated prefix.
    doc.put(ROOT, "alice_late", "late").unwrap();
    let mut fork = doc.fork().with_author(Some(bob.clone()));
    fork.put(ROOT, "bob_late", "late").unwrap();
    doc.merge(&mut fork).unwrap();

    // Apply the schema-validation pattern: render only the validated
    // prefix by default.
    doc.set_filter(Filter::default().with_default(Rule::AllowUpTo {
        heads: validated_heads.clone(),
    }));

    assert_eq!(
        doc.get(ROOT, "validated").unwrap().unwrap().0,
        "yes".into()
    );
    assert!(doc.get(ROOT, "alice_late").unwrap().is_none());
    assert!(doc.get(ROOT, "bob_late").unwrap().is_none());

    // Bring Alice's late changes back without affecting Bob's.
    doc.update_filter(|f| {
        f.authors.insert(alice.clone(), Rule::Allow);
    });

    assert_eq!(
        doc.get(ROOT, "alice_late").unwrap().unwrap().0,
        "late".into()
    );
    assert!(doc.get(ROOT, "bob_late").unwrap().is_none());
}

#[test]
fn actor_rule_overrides_author_rule() {
    // Two actors share an author. We deny the author globally but allow
    // one specific actor — the per-actor rule should win.
    let shared = Author::try_from("aaaa").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(shared.clone()));
    doc.put(ROOT, "shared_actor1", "v1").unwrap();
    let actor1 = doc.get_actor().clone();

    // A second actor under the same author. `set_author` to the same
    // value is a no-op, so we use a fork to spawn a fresh actor that the
    // existing author rule still covers.
    let mut fork = doc.fork().with_author(Some(shared.clone()));
    fork.put(ROOT, "shared_actor2", "v2").unwrap();
    let actor2 = fork.get_actor().clone();
    doc.merge(&mut fork).unwrap();
    assert_ne!(actor1, actor2);

    // Deny the author wholesale, then allow actor1 specifically.
    doc.set_filter(
        Filter::default()
            .with_author(shared.clone(), Rule::Deny)
            .with_actor(actor1.clone(), Rule::Allow),
    );

    assert_eq!(
        doc.get(ROOT, "shared_actor1").unwrap().unwrap().0,
        "v1".into()
    );
    assert!(doc.get(ROOT, "shared_actor2").unwrap().is_none());
}

#[test]
fn update_filter_reads_current_state() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "k", "v").unwrap();

    doc.set_filter(Filter::default().with_author(alice.clone(), Rule::Deny));

    // `update_filter` should observe the existing rule rather than start
    // from a default-constructed `Filter`.
    doc.update_filter(|f| {
        assert_eq!(f.authors.get(&alice), Some(&Rule::Deny));
        f.authors.insert(bob.clone(), Rule::Deny);
    });

    let current = doc.filter();
    assert_eq!(current.authors.get(&alice), Some(&Rule::Deny));
    assert_eq!(current.authors.get(&bob), Some(&Rule::Deny));
}

#[test]
fn filter_survives_save_load_round_trip() {
    let alice = Author::try_from("aaaa").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "before", "1").unwrap();
    let heads = doc.get_heads();
    doc.put(ROOT, "after", "2").unwrap();

    doc.set_filter(Filter::default().with_author(
        alice.clone(),
        Rule::AllowUpTo {
            heads: heads.clone(),
        },
    ));

    // The filter should be stable across an in-place reload (which is the
    // path used by `Automerge::clone`-via-load and by some sync
    // bookkeeping paths in autocommit).
    let bytes = doc.save();
    let mut reloaded = AutoCommit::load(&bytes).unwrap();
    reloaded.set_filter(doc.filter().clone());

    assert!(reloaded.get(ROOT, "after").unwrap().is_none());
    assert_eq!(
        reloaded.get(ROOT, "before").unwrap().unwrap().0,
        "1".into()
    );
}

// ---------------------------------------------------------------------------
// Author-keyed AllowUpTo across op kinds
// ---------------------------------------------------------------------------

#[test]
fn hidden_author_changes_have_no_effect_on_apply() {
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
    doc.splice(&list, 0, 0, [1, 2, 3, 4]).unwrap();
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
    hide_author_after(&mut remote, &bad, &epoc);
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
fn unhiding_author_restores_changes() {
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

    // Hide bad author
    hide_author_after(&mut doc, &bad, &epoch);
    // The filter should undo the post-epoch put
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

    // Unhiding should restore it
    doc.update_diff_cursor();
    unhide_author(&mut doc, &bad);
    let patches = doc.diff_incremental();
    assert_eq!(
        doc.get(ROOT, "new_key").unwrap().unwrap().0,
        "new_bad_value".into()
    );
    // iter() should show both "key" and "new_key" after unhiding
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
fn list_insert_delete_by_hidden_author() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.splice(&list, 0, 0, [1, 2, 3]).unwrap();

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
    hide_author_after(&mut remote, &bad, &epoch);

    // Bad's insert of 99 should be filtered out, bad's delete of 3 should be filtered out
    assert_eq!(remote.length(&list), 4);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, 0.into());
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, 1.into());
    assert_eq!(remote.get(&list, 2).unwrap().unwrap().0, 2.into());
    assert_eq!(remote.get(&list, 3).unwrap().unwrap().0, 3.into());
}

#[test]
fn map_put_conflict_with_hidden_author() {
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
    hide_author_after(&mut remote, &bad, &epoch);

    // Good author's value should win since bad's is filtered out
    assert_eq!(
        remote.get(ROOT, "key").unwrap().unwrap().0,
        "good_update".into()
    );
}

#[test]
fn text_splice_by_hidden_author() {
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
    hide_author_after(&mut remote, &bad, &epoch);

    // Bad's splice should be filtered out: the delete of " world" and insert of " everyone"
    // Good's "say " prefix should remain
    let result = remote.text(&text).unwrap();
    assert_eq!(result, "say hello world");
}

#[test]
fn text_mark_by_hidden_author() {
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
    remote.update_diff_cursor();
    hide_author_after(&mut remote, &bad, &epoch);
    let patches = remote.diff_incremental();

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

    // Bad's mark should be filtered out
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
    assert!(has_italic, "italic mark should survive filtering");
    let has_bold = spans.iter().any(|s| {
        if let automerge::Span::Text { marks: Some(m), .. } = s {
            m.iter().any(|(name, _)| name == "bold")
        } else {
            false
        }
    });
    assert!(!has_bold, "bold mark should be filtered out");
}

#[test]
fn counter_increment_by_hidden_author() {
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
    hide_author_after(&mut remote, &bad, &epoch);

    // Only good's increment should count: 10 + 5 = 15
    assert_eq!(
        remote.get(ROOT, "counter").unwrap().unwrap().0.to_i64(),
        Some(15)
    );
}

#[test]
fn valid_put_in_object_created_by_hidden_author() {
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
    hide_author_after(&mut remote, &bad, &epoch);

    // The bad_map itself was created by bad, so it should be filtered out
    // Good author's put into it won't be visible since the parent is gone
    assert!(remote.get(ROOT, "bad_map").unwrap().is_none());
}

#[test]
fn valid_insert_after_hidden_authors_insert() {
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
    hide_author_after(&mut remote, &bad, &epoch);

    // Bad's insert should be gone, good's should remain
    assert_eq!(remote.length(&list), 2);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, "first".into());
    assert_eq!(
        remote.get(&list, 1).unwrap().unwrap().0,
        "good_insert".into()
    );
}

#[test]
fn valid_delete_of_hidden_authors_insert() {
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

    hide_author_after(&mut remote, &bad, &epoch);
    remote.merge(&mut doc).unwrap();

    // Both the insert and delete target the same element; result should be just "first"
    assert_eq!(remote.length(&list), 4);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, "zero".into());
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, "first".into());
    assert_eq!(remote.get(&list, 2).unwrap().unwrap().0, "second".into());
    assert_eq!(remote.get(&list, 3).unwrap().unwrap().0, "third".into());
}

#[test]
fn hide_with_new_actor_same_author() {
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
    hide_author_after(&mut remote, &bad, &epoch);
    remote.merge(&mut doc).unwrap();

    // new actor already filtered out
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

    // Both actors under the bad author should be filtered out
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
fn hide_only_after_epoch() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));

    // Bad author makes changes before epoch
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "before_epoch", "allowed").unwrap();
    doc.merge(&mut fork).unwrap();

    let epoch = doc.get_heads();

    // Bad author makes changes after epoch
    fork.put(ROOT, "after_epoch", "filtered_out").unwrap();
    doc.merge(&mut fork).unwrap();

    hide_author_after(&mut doc, &bad, &epoch);

    // Pre-epoch changes should remain
    assert_eq!(
        doc.get(ROOT, "before_epoch").unwrap().unwrap().0,
        "allowed".into()
    );
    // Post-epoch changes should be filtered out
    assert!(doc.get(ROOT, "after_epoch").unwrap().is_none());
}

#[test]
fn hide_patches_reflect_undo() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    fork.put(ROOT, "key", "bad_override").unwrap();
    doc.merge(&mut fork).unwrap();

    // Verify bad's value is visible before filtering
    assert_eq!(
        doc.get(ROOT, "key").unwrap().unwrap().0,
        "bad_override".into()
    );

    doc.update_diff_cursor();
    hide_author_after(&mut doc, &bad, &epoch);
    let patches = doc.diff_incremental();

    // Should get a patch restoring "original"
    assert!(!patches.is_empty());
    assert!(
        matches!(&patches[0].action, PatchAction::PutMap { value, .. } if value.0 == "original".into())
    );
    assert_eq!(doc.get(ROOT, "key").unwrap().unwrap().0, "original".into());
}

#[test]
fn hide_then_load_incremental() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    doc.put(ROOT, "key", "original").unwrap();

    // Bad author must make a change before epoch so its actor is known at filter time
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "pre_epoch", "pre_val").unwrap();
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();
    fork.put(ROOT, "post_epoch", "post_val").unwrap();

    let mut remote = AutoCommit::new();
    hide_author_after(&mut remote, &bad, &epoch);
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    let patches = remote.diff_incremental();
    assert!(!patches.is_empty());
    for p in &patches {
        if let PatchAction::PutMap { key, .. } = &p.action {
            assert_ne!(key.as_str(), "post_epoch");
        }
    }
    assert!(patches.iter().any(|p| {
        matches!(&p.action, PatchAction::PutMap { key, .. } if key.as_str() == "pre_epoch")
    }));

    // Bad author makes changes after the filter point
    fork.put(ROOT, "bad_key", "bad_val").unwrap();
    fork.put(ROOT, "key", "bad_override").unwrap();

    // Good author also makes changes
    doc.merge(&mut fork).unwrap();
    doc.put(ROOT, "good_key", "good_val").unwrap();

    // Load new changes into the doc with filter
    remote
        .load_incremental(&[doc.save_incremental(), fork.save_incremental()].concat())
        .unwrap();

    // Bad's post-epoch changes should be filtered out
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

// Regression test for a bug in `change_graph::rebuild_filter_cached_clock`.
//
// `ResolvedFilter::cached_clock` is an `OpClock` (indexed by actor, values
// are op counters), but `rebuild_filter_cached_clock` populates it directly
// from `ResolvedFilter::actor_mask`, whose values are *seq numbers* (not op
// counters). When a filtered-out actor's last accepted change has multiple
// ops, the seq number (e.g. `1`) is much smaller than the op counters of
// those ops (e.g. `2`, `3`, ...), so `Clock::covers` returns `false` for
// ops that should be visible.
//
// The bug surfaces on the slow query path that consults the active filter
// clock (e.g. `keys()`, `marks()`); the indexed fast path (e.g. `get()`,
// `iter()`, `length()`) is unaffected because the index is built from
// `clock_at(heads)` which goes through `to_op_clock` and converts seq → max
// op counter correctly.
#[test]
fn cached_filter_clock_handles_multi_op_changes() {
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new();
    doc.put(ROOT, "good_key", "good").unwrap();

    // Bad author makes a SINGLE change containing multiple ops. The change
    // has seq=1, but its ops have global op-counters > 1 (the global counter
    // is incremented per op, across all actors).
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "k1", "v1").unwrap();
    fork.put(ROOT, "k2", "v2").unwrap();
    fork.put(ROOT, "k3", "v3").unwrap();
    fork.commit();

    let pre_filter_heads = fork.get_heads();

    // A second change which should actually be filtered out.
    fork.put(ROOT, "post_filter", "post").unwrap();
    fork.commit();

    doc.merge(&mut fork).unwrap();

    // Filter at heads after the multi-op change, so k1/k2/k3 stay;
    // post_filter goes.
    hide_author_after(&mut doc, &bad, &pre_filter_heads);

    // Sanity check: the indexed fast path correctly preserves k1/k2/k3.
    assert_eq!(doc.get(ROOT, "k1").unwrap().unwrap().0, "v1".into());
    assert_eq!(doc.get(ROOT, "k2").unwrap().unwrap().0, "v2".into());
    assert_eq!(doc.get(ROOT, "k3").unwrap().unwrap().0, "v3".into());
    assert!(doc.get(ROOT, "post_filter").unwrap().is_none());

    // The slow path (keys → visible_slow → filter cached clock) should
    // agree. With the bug, `cached_clock[bad-actor] = 1` (the seq) but
    // bad's ops have op-counters 2, 3, 4 — so `covers` returns false for
    // all of them and they all disappear from `keys()`.
    let keys: Vec<String> = doc.keys(ROOT).collect();
    assert!(keys.contains(&"good_key".to_string()));
    assert!(
        keys.contains(&"k1".to_string()),
        "k1 should be visible (before filter point); got keys={:?}",
        keys
    );
    assert!(
        keys.contains(&"k2".to_string()),
        "k2 should be visible (before filter point); got keys={:?}",
        keys
    );
    assert!(
        keys.contains(&"k3".to_string()),
        "k3 should be visible (before filter point); got keys={:?}",
        keys
    );
    assert!(
        !keys.contains(&"post_filter".to_string()),
        "post_filter should be filtered; got keys={:?}",
        keys
    );
}

// Regression test for a bug in `change_graph::insert_actor`.
//
// When a new actor is inserted at a sorted position lower than existing
// actors, all existing actor indices shift up. `insert_actor` updates
// `self.actors`, `self.seq_index`, `self.actor_author`, and the per-node
// clocks in `clock_cache`, but it must also re-key the per-actor maps in
// `ResolvedFilter` (`actor_rules` and `actor_mask`). If it doesn't, the
// mask still has entries keyed at the old (now stale) indices.
//
// As a result, `cached_clock` (rebuilt from the stale mask) marks the wrong
// actors as filtered out. The slow query path that consults the active
// filter clock then filters incorrectly.
#[test]
fn filter_mask_survives_actor_reordering() {
    let bad = Author::try_from("ffff").unwrap();

    // Two actor IDs with deterministic sort order: `actor_late` sorts AFTER
    // `actor_early`. We add `actor_late` first, then `actor_early`, forcing
    // an actor reordering on the second add.
    let actor_late = ActorId::try_from("ff").unwrap();
    let actor_early = ActorId::try_from("00").unwrap();

    let mut doc = AutoCommit::new();
    doc.set_actor(ActorId::try_from("aa").unwrap()); // doc's own actor, between early and late
    doc.put(ROOT, "good_key", "good").unwrap();

    let pre_change = doc.get_heads();

    // First bad actor publishes a change.
    let mut fork_late = doc.fork().with_author(Some(bad.clone()));
    fork_late.set_actor(actor_late);
    fork_late.put(ROOT, "late_actor_key", "from_late").unwrap();
    fork_late.commit();

    doc.merge(&mut fork_late).unwrap();

    // Filter out bad after `pre_change` — bad has no pre-change history,
    // so all of bad's changes (current and future) should be filtered
    // out.
    hide_author_after(&mut doc, &bad, &pre_change);

    // Sanity check: the late-actor key is correctly hidden.
    assert!(doc.get(ROOT, "late_actor_key").unwrap().is_none());

    // Now add a second bad actor with an ID that sorts BEFORE the existing
    // bad actor. This forces `insert_actor` to insert at a low index,
    // shifting the existing bad actor (and the doc actor) up.
    let mut fork_early = doc.fork().with_author(Some(bad.clone()));
    fork_early.set_actor(actor_early);
    fork_early
        .put(ROOT, "early_actor_key", "from_early")
        .unwrap();
    fork_early.commit();
    doc.merge(&mut fork_early).unwrap();

    // After the actor reordering, both bad-actor keys should still be
    // filtered out (same author).
    assert!(
        doc.get(ROOT, "late_actor_key").unwrap().is_none(),
        "late_actor_key should remain filtered out after actor reordering"
    );
    assert!(
        doc.get(ROOT, "early_actor_key").unwrap().is_none(),
        "early_actor_key should be filtered out (same author)"
    );

    // The slow path consults `ResolvedFilter::cached_clock`, which is
    // rebuilt from the per-actor mask. With the bug the mask is keyed at
    // the old indices: the actor at the *new* position 1 (the doc's own
    // actor) ends up flagged as filtered out, while the actor at the
    // *new* position that previously belonged to `actor_late` does not.
    let keys: Vec<String> = doc.keys(ROOT).collect();
    assert!(
        keys.contains(&"good_key".to_string()),
        "good_key should remain visible; got keys={:?}",
        keys
    );
    assert!(
        !keys.contains(&"late_actor_key".to_string()),
        "late_actor_key should be filtered by keys(); got keys={:?}",
        keys
    );
    assert!(
        !keys.contains(&"early_actor_key".to_string()),
        "early_actor_key should be filtered by keys(); got keys={:?}",
        keys
    );
}

#[test]
fn values_skip_filtered_out_ops() {
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new();
    doc.put(ROOT, "good_key", "good_val").unwrap();

    let pre_change = doc.get_heads();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "bad_key", "bad_val").unwrap();
    doc.merge(&mut fork).unwrap();

    hide_author_after(&mut doc, &bad, &pre_change);

    // get() and keys() correctly hide the filtered-out op.
    assert!(doc.get(ROOT, "bad_key").unwrap().is_none());
    let keys: Vec<String> = doc.keys(ROOT).collect();
    assert!(!keys.contains(&"bad_key".to_string()));

    // values() must agree.
    let values: Vec<_> = doc
        .values(ROOT)
        .map(|(v, _)| v.into_string().unwrap_or_default())
        .collect();
    assert!(
        values.contains(&"good_val".to_string()),
        "good_val should be visible; got values={:?}",
        values
    );
    assert!(
        !values.contains(&"bad_val".to_string()),
        "bad_val should be filtered by values(); got values={:?}",
        values
    );
}
