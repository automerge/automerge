use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, ObjType, PatchAction, PatchLog, ReadDoc, ScalarValue,
    TextEncoding, ROOT,
};

// ============================= reads =============================

fn two_authors() -> (Automerge, Author<'static>, Vec<automerge::ChangeHash>) {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let epoch = a.get_heads();
    a.put(ROOT, "y", 2).unwrap();
    a.commit();
    let mut b = a.fork().with_author(Some(bob));
    b.put(ROOT, "z", 3).unwrap();
    b.commit();
    a.merge(&mut b).unwrap();
    // The write-frontier is process-local, so applying `mask_author` on a
    // freshly loaded document exercises the same state as the original.
    let doc = Automerge::load(&a.save()).unwrap();
    (doc, alice, epoch)
}

#[test]
fn pending_change_hides_author_entirely() {
    let (mut doc, alice, _) = two_authors();
    let unseen = automerge::ChangeHash([7; 32]);
    doc.mask_author(alice, &[unseen], &mut PatchLog::inactive())
        .unwrap();
    assert!(doc.get(ROOT, "x").unwrap().is_none());
    assert!(doc.get(ROOT, "y").unwrap().is_none());
    assert_eq!(doc.get(ROOT, "z").unwrap().unwrap().0.as_i64(), Some(3));
}

/// A boundary resolves only when ALL of its heads are known: mixing one
/// known head with one unknown head must still hide the author entirely,
/// not derive a partial clock from the known subset.
#[test]
fn partially_unknown_boundary_hides_author_entirely() {
    let (mut doc, alice, epoch) = two_authors();
    let unseen = automerge::ChangeHash([7; 32]);
    doc.mask_author(alice, &[epoch[0], unseen], &mut PatchLog::inactive())
        .unwrap();
    assert!(
        doc.get(ROOT, "x").unwrap().is_none(),
        "alice must be fully hidden while part of her boundary is unknown"
    );
    assert!(doc.get(ROOT, "y").unwrap().is_none());
    assert_eq!(doc.get(ROOT, "z").unwrap().unwrap().0.as_i64(), Some(3));
}

#[test]
fn current_slow_paths_honour_mask() {
    // alice creates text + marks, bob appends; mask alice at epoch
    // (before her mark), so slow read paths on CURRENT heads must apply
    // the mask.
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    let text = a.put_object(ROOT, "text", ObjType::Text).unwrap();
    a.splice_text(&text, 0, 0, "hello").unwrap();
    a.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    a.commit();
    let epoch = a.get_heads();
    a.mark(
        &text,
        Mark::new("bold".into(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();
    a.splice_text(&text, 5, 0, " alice").unwrap();
    a.increment(ROOT, "c", 5).unwrap();
    a.put(ROOT, "sig", "alice").unwrap();
    a.commit();
    let mut b = a.fork().with_author(Some(bob));
    b.splice_text(&text, 0, 0, "bob ").unwrap();
    b.increment(ROOT, "c", 1).unwrap();
    b.commit();
    a.merge(&mut b).unwrap();
    let mut doc = Automerge::load(&a.save()).unwrap();
    doc.mask_author(alice, &epoch, &mut PatchLog::inactive())
        .unwrap();

    assert_eq!(doc.text(&text).unwrap(), "bob hello");
    assert!(doc.marks(&text).unwrap().is_empty());
    let spans: String = doc
        .spans(&text)
        .unwrap()
        .filter_map(|s| match s {
            automerge::iter::Span::Text { text, .. } => Some(text),
            _ => None,
        })
        .collect();
    assert_eq!(spans, "bob hello");
    assert_eq!(doc.length(&text), 9);
    assert_eq!(doc.keys(ROOT).count(), 2);
    // counter slow path: alice's post-epoch increment is masked out
    assert_eq!(doc.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(11));
    // values() must agree with keys(): alice's post-epoch put is filtered
    let values: Vec<_> = doc
        .values(ROOT)
        .map(|(v, _)| v.into_string().unwrap_or_default())
        .collect();
    assert!(
        !values.contains(&"alice".to_string()),
        "sig should be filtered by values(); got values={:?}",
        values
    );

    // transaction-scoped reads see the mask too
    let tx = doc.transaction();
    assert_eq!(tx.text(&text).unwrap(), "bob hello");
    assert_eq!(tx.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(11));
    tx.rollback();
}

#[test]
fn current_state_patches_honour_mask() {
    // alice's post-epoch change is a counter increment; patches produced
    // by the current-state walk must report the masked counter value.
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    a.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    a.commit();
    let epoch = a.get_heads();
    a.increment(ROOT, "c", 5).unwrap();
    a.commit();
    let mut b = a.fork().with_author(Some(bob));
    b.increment(ROOT, "c", 1).unwrap();
    b.commit();
    a.merge(&mut b).unwrap();
    let mut doc = Automerge::load(&a.save()).unwrap();
    doc.mask_author(alice, &epoch, &mut PatchLog::inactive())
        .unwrap();

    // sanity: the direct read path is masked
    assert_eq!(doc.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(11));

    let patches = doc.current_state();
    let counter = patches.iter().find_map(|p| match &p.action {
        PatchAction::PutMap { key, value, .. } if key == "c" => value.0.as_i64(),
        _ => None,
    });
    assert_eq!(counter, Some(11));
}

#[test]
fn autocommit_isolation_reads_honour_mask() {
    let (doc, alice, epoch) = two_authors();
    let mut doc = AutoCommit::load(&doc.save()).unwrap();
    let heads = doc.get_heads();
    doc.mask_author(alice, &epoch);
    doc.isolate(&heads);
    assert!(doc.get(ROOT, "y").unwrap().is_none());
    // Open a transaction: scoped reads inside it must honour the mask too.
    doc.put(ROOT, "w", 4).unwrap();
    assert!(doc.get(ROOT, "y").unwrap().is_none());
    assert_eq!(doc.get(ROOT, "x").unwrap().unwrap().0.as_i64(), Some(1));
    doc.integrate();
}

#[test]
fn mask_list_insert_delete() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(good.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.splice(&list, 0, 0, [1, 2, 3]).unwrap();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();

    // Bad author inserts and deletes in the list. The delete targets an
    // element inserted before the epoch, so masking bad must undo it.
    fork.insert(&list, 1, 99).unwrap(); // [1, 99, 2, 3]
    fork.delete(&list, 3).unwrap(); // [1, 99, 2]
    doc.merge(&mut fork).unwrap();

    // Good author inserts anchored after bad's (to-be-masked) insert, and
    // at the head of the list.
    doc.insert(&list, 2, 100).unwrap(); // [1, 99, 100, 2]
    doc.insert(&list, 0, 0).unwrap(); // [0, 1, 99, 100, 2]

    let mut remote = AutoCommit::new();
    remote
        .load_incremental(&[doc.save(), fork.save()].concat())
        .unwrap();
    remote.mask_author(bad, &epoch);

    // Bad's insert of 99 is hidden, bad's delete of 3 is undone, and good's
    // inserts keep their positions even though 100 anchors after the hidden
    // 99.
    assert_eq!(remote.length(&list), 5);
    assert_eq!(remote.get(&list, 0).unwrap().unwrap().0, 0.into());
    assert_eq!(remote.get(&list, 1).unwrap().unwrap().0, 1.into());
    assert_eq!(remote.get(&list, 2).unwrap().unwrap().0, 100.into());
    assert_eq!(remote.get(&list, 3).unwrap().unwrap().0, 2.into());
    assert_eq!(remote.get(&list, 4).unwrap().unwrap().0, 3.into());
}

#[test]
fn mask_text_mark() {
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
    remote.mask_author(bad, &epoch);
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

    // Bad's mark should be masked
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
    assert!(has_italic, "italic mark should survive mask");
    let has_bold = spans.iter().any(|s| {
        if let automerge::Span::Text { marks: Some(m), .. } = s {
            m.iter().any(|(name, _)| name == "bold")
        } else {
            false
        }
    });
    assert!(!has_bold, "bold mark should be masked");
}

#[test]
fn mask_valid_put_in_masked_object() {
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
    remote.mask_author(bad, &epoch);

    // The bad_map itself was created by bad, so it should be masked
    // Good author's put into it won't be visible since the parent is gone
    assert!(remote.get(ROOT, "bad_map").unwrap().is_none());
}

#[test]
fn cached_mask_handles_multi_op_changes() {
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

    let pre_mask_heads = fork.get_heads();

    // A second change which should actually be masked.
    fork.put(ROOT, "post_mask", "post").unwrap();
    fork.commit();

    doc.merge(&mut fork).unwrap();

    // Mask at heads after the multi-op change, so k1/k2/k3 stay; post_mask
    // goes.
    doc.mask_author(bad, &pre_mask_heads);

    // Sanity check: the indexed fast path correctly preserves k1/k2/k3.
    assert_eq!(doc.get(ROOT, "k1").unwrap().unwrap().0, "v1".into());
    assert_eq!(doc.get(ROOT, "k2").unwrap().unwrap().0, "v2".into());
    assert_eq!(doc.get(ROOT, "k3").unwrap().unwrap().0, "v3".into());
    assert!(doc.get(ROOT, "post_mask").unwrap().is_none());

    let keys: Vec<String> = doc.keys(ROOT).collect();
    assert!(keys.contains(&"good_key".to_string()));
    assert!(
        keys.contains(&"k1".to_string()),
        "k1 should be visible (before mask point); got keys={:?}",
        keys
    );
    assert!(
        keys.contains(&"k2".to_string()),
        "k2 should be visible (before mask point); got keys={:?}",
        keys
    );
    assert!(
        keys.contains(&"k3".to_string()),
        "k3 should be visible (before mask point); got keys={:?}",
        keys
    );
    assert!(
        !keys.contains(&"post_mask".to_string()),
        "post_mask should be filtered; got keys={:?}",
        keys
    );
}

// TODO(finto): test mask at empty heads, i.e. document inception

// Regression: hydration with no heads and with explicitly supplied current
// heads must both apply the active mask, including when summing counter
// increments (which cannot rely on the visibility index alone).
#[test]
fn mask_reflected_in_hydrate_with_implicit_and_explicit_current_heads() {
    let ffff = Author::try_from("ffff").unwrap();
    let aaaa = Author::try_from("aaaa").unwrap();

    let mut doc = AutoCommit::new().with_author(Some(ffff.clone()));
    doc.put(ROOT, "key1", "val1").unwrap();
    doc.put(ROOT, "counter", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "counter", 1).unwrap();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.splice(&list, 0, 0, [1, 2, 3, 4]).unwrap();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    let heads1 = doc.get_heads();
    let before = doc.hydrate(&ROOT, None).unwrap();

    doc.set_author(Some(aaaa.clone()));
    doc.put(ROOT, "key1", "val2").unwrap();
    doc.increment(ROOT, "counter", 2).unwrap();
    doc.insert(&list, 3, "cat").unwrap();
    doc.splice_text(&text, 6, 0, "big ").unwrap();
    let heads2 = doc.get_heads();
    let after = doc.hydrate(&ROOT, None).unwrap();
    assert_ne!(before, after);

    doc.mask_author(aaaa.clone(), &heads1);
    assert_eq!(
        doc.hydrate(&ROOT, None).unwrap(),
        before,
        "materializing with no heads must hide masked ops"
    );
    assert_eq!(doc.get_heads(), heads2);
    assert_eq!(
        doc.hydrate(&ROOT, Some(&heads2)).unwrap(),
        before,
        "current heads must also exclude masked counter increments"
    );
    assert_eq!(doc.document().hydrate(Some(&heads2)), before);
    assert_eq!(doc.hydrate(&ROOT, Some(&heads1)).unwrap(), before);

    doc.reveal_author(&aaaa);
    assert_eq!(
        doc.hydrate(&ROOT, None).unwrap(),
        after,
        "unmask must restore the materialized state"
    );
    assert_eq!(doc.get_heads(), heads2);
    assert_eq!(doc.hydrate(&ROOT, Some(&heads2)).unwrap(), after);
    assert_eq!(doc.document().hydrate(Some(&heads2)), after);
    assert_eq!(doc.hydrate(&ROOT, Some(&heads1)).unwrap(), before);
}

/// Positional reads share the indexed fast path with `text()`/`length()`,
/// but register selection within an element is op-by-op: a delete from a
/// masked author must not hide the character it targeted. The reload
/// oracle (fresh `load` + the same policy) materializes independently of
/// the live document's index state.
#[test]
fn masked_text_delete_preserves_positional_get() {
    let good = Author::try_from("900d").unwrap();
    let bad = Author::try_from("badd").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "abc").unwrap();
    doc.commit();

    let mut b = doc.fork().with_author(Some(bad.clone()));
    b.splice_text(&text, 0, 1, "").unwrap();
    b.commit();

    doc.merge(&mut b).unwrap();
    doc.mask_author(bad, &[]);

    assert_eq!(doc.text(&text).unwrap(), "abc");
    assert_eq!(doc.length(&text), 3);

    let oracle = Automerge::load(&doc.document().save())
        .unwrap()
        .with_write_frontier(doc.document().get_write_frontier());
    let heads = doc.get_heads();
    let live = doc.get(&text, 0).unwrap().map(|v| v.0.into_owned());
    let at = doc
        .get_at(&text, 0, &heads)
        .unwrap()
        .map(|v| v.0.into_owned());
    let reload = oracle.get(&text, 0).unwrap().map(|v| v.0.into_owned());
    assert_eq!(live, at, "get vs get_at at current heads");
    assert_eq!(live, reload, "get vs fresh reload oracle");
    assert_eq!(
        live.and_then(|v| v.into_string().ok()),
        Some("a".to_string()),
        "positional get must see the restored first character"
    );
}

/// The list fast seeker's counter aggregation (`FixCounters`) must not sum
/// increments from a masked author: a counter of 10 incremented by 5 by a
/// masked author must still read 10.
#[test]
fn masked_list_increment_does_not_leak() {
    let good = Author::try_from("900d").unwrap();
    let bad = Author::try_from("badd").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, ScalarValue::counter(10)).unwrap();
    doc.commit();

    let mut b = doc.fork().with_author(Some(bad.clone()));
    b.increment(&list, 0, 5).unwrap();
    b.commit();

    doc.merge(&mut b).unwrap();
    doc.mask_author(bad, &[]);

    let oracle = Automerge::load(&doc.document().save())
        .unwrap()
        .with_write_frontier(doc.document().get_write_frontier());
    let live = doc.get(&list, 0).unwrap().map(|v| v.0.into_owned());
    let reload = oracle.get(&list, 0).unwrap().map(|v| v.0.into_owned());
    assert_eq!(live, reload, "get vs fresh reload oracle");
    assert_eq!(
        live.and_then(|v| v.as_i64()),
        Some(10),
        "masked increment must not leak into positional counter reads"
    );
}

// ==================== transitions & patch log ====================

#[test]
fn mask_is_included_in_incremental_diff() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    doc.put(ROOT, "key", "original").unwrap();
    let epoch = doc.get_heads();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "key", "replacement").unwrap();
    doc.merge(&mut fork).unwrap();
    doc.update_diff_cursor();
    let heads = doc.get_heads();

    doc.mask_author(bad, &epoch);
    // The write-frontier changes the visible state without advancing the heads,
    // but incremental diffs must still include the transition.
    let patches = doc.diff_incremental();
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(doc.get(ROOT, "key").unwrap().unwrap().0, "original".into());
    assert_eq!(patches.len(), 1);
    assert!(matches!(
        &patches[0].action,
        PatchAction::PutMap { key, value, .. }
            if key == "key" && value.0 == "original".into()
    ));

    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn unmask_is_included_in_incremental_diff() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    doc.put(ROOT, "key", "original").unwrap();

    // Bad overwrites the key before the epoch, then makes more changes
    // after it.
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "key", "value2").unwrap();
    doc.merge(&mut fork).unwrap();
    let epoch = doc.get_heads();
    fork.put(ROOT, "key", "replacement").unwrap();
    fork.put(ROOT, "new_key", "new_bad_value").unwrap();
    doc.merge(&mut fork).unwrap();

    doc.mask_author(bad.clone(), &epoch);
    // Post-epoch changes are hidden; the pre-epoch overwrite survives.
    assert_eq!(doc.get(ROOT, "key").unwrap().unwrap().0, "value2".into());
    assert!(doc.get(ROOT, "new_key").unwrap().is_none());
    let all = doc.get_all(ROOT, "key").unwrap();
    assert!(all.iter().any(|(v, _)| *v == "value2".into()));
    let iter_keys: Vec<_> = doc
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(iter_keys.contains(&"key".to_string()));
    assert!(!iter_keys.contains(&"new_key".to_string()));

    doc.update_diff_cursor();
    let heads = doc.get_heads();

    doc.reveal_author(&bad);
    let patches = doc.diff_incremental();
    assert_eq!(doc.get_heads(), heads);
    assert_eq!(
        doc.get(ROOT, "key").unwrap().unwrap().0,
        "replacement".into()
    );
    assert_eq!(
        doc.get(ROOT, "new_key").unwrap().unwrap().0,
        "new_bad_value".into()
    );
    let iter_keys: Vec<_> = doc
        .iter()
        .filter_map(|item| item.key().map(String::from))
        .collect();
    assert!(iter_keys.contains(&"key".to_string()));
    assert!(iter_keys.contains(&"new_key".to_string()));
    assert_eq!(patches.len(), 2);
    assert!(patches.iter().any(|p| matches!(
        &p.action,
        PatchAction::PutMap { key, value, .. }
            if key == "key" && value.0 == "replacement".into()
    )));
    assert!(patches.iter().any(|p| matches!(
        &p.action,
        PatchAction::PutMap { key, value, .. }
            if key == "new_key" && value.0 == "new_bad_value".into()
    )));

    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn historical_diff_reflects_current_write_frontier_state() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    doc.put(ROOT, "key", "original").unwrap();
    let epoch = doc.get_heads();
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.put(ROOT, "key", "replacement").unwrap();
    doc.merge(&mut fork).unwrap();
    let heads = doc.get_heads();

    // Repeating a historical diff reflects changes in write-frontier state even
    // though the heads have not changed.
    let original = doc.diff(&[], &heads);
    doc.mask_author(bad.clone(), &epoch);
    let masked = doc.diff(&[], &heads);
    assert_ne!(masked, original);
    doc.reveal_author(&bad);
    assert_eq!(doc.diff(&[], &heads), original);

    doc.update_diff_cursor();
    assert!(doc.diff(&heads, &heads).is_empty());
    doc.mask_author(bad.clone(), &epoch);
    assert_eq!(doc.diff_incremental(), masked);
    assert!(doc.diff_incremental().is_empty());
    doc.reveal_author(&bad);
    assert_eq!(doc.diff_incremental(), original);
    assert!(doc.diff_incremental().is_empty());

    // Historical diffs never return cursor-relative visibility transitions.
    // Resetting the cursor discards those transitions, so the next incremental
    // diff instead returns the current state from the empty document.
    doc.mask_author(bad, &epoch);
    assert!(doc.diff(&heads, &heads).is_empty());
    doc.reset_diff_cursor();
    assert!(doc.diff(&heads, &heads).is_empty());
    assert_eq!(doc.diff_incremental(), masked);
    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn write_frontier_preserves_pending_patch_paths_and_order() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    let good_map = doc.insert_object(&list, 0, ObjType::Map).unwrap();
    doc.put(&good_map, "value", "initial").unwrap();
    let epoch = doc.get_heads();

    let mut fork = doc.fork().with_author(Some(bad.clone()));
    let bad_map = fork.insert_object(&list, 0, ObjType::Map).unwrap();
    fork.put(&bad_map, "bad", true).unwrap();
    doc.merge(&mut fork).unwrap();
    let mut view = doc.hydrate(&ROOT, None).unwrap();
    doc.update_diff_cursor();

    // The same object's path moves from list[1] to list[0] and back. Keep
    // edits pending across both transitions to exercise path resolution and
    // ordering.
    doc.put(&good_map, "value", "before mask").unwrap();
    doc.mask_author(bad.clone(), &epoch);
    doc.put(&good_map, "value", "before unmask").unwrap();
    doc.reveal_author(&bad);
    doc.put(&good_map, "value", "after unmask").unwrap();

    view.apply_patches(TextEncoding::UnicodeCodePoint, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(&ROOT, None).unwrap());
    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn caller_owned_write_frontier_log_tracks_actor_reordering() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new_with_encoding(TextEncoding::UnicodeCodePoint)
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from(vec![0x80]));
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    tx.commit();
    doc.mask_author(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();
    doc.reveal_author(&author, &mut log).unwrap();

    // The fresh write-frontier log already references actor 0x80. A subsequent
    // transaction inserts actor 0x00 before it in the actor table. These fixed
    // identities make the required reindexing deterministic.
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.set_actor(ActorId::from(vec![0x00]));
    let mut tx = doc.transaction_log_patches(log).unwrap();
    tx.put(ROOT, "other", 1).unwrap();
    let (_, mut log) = tx.commit();

    assert_eq!(doc.text(&text).unwrap(), "abc");
    assert_eq!(doc.get(ROOT, "other").unwrap().unwrap().0, 1.into());
    view.apply_patches(TextEncoding::UnicodeCodePoint, doc.make_patches(&mut log))
        .unwrap();
    assert_eq!(view, doc.hydrate(None));
}

#[test]
fn mask_tracks_the_isolated_view() {
    let good = Author::try_from("aaaa").unwrap();
    let bad = Author::try_from("ffff").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(good));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, 1).unwrap();
    let epoch = doc.get_heads();
    let mut fork = doc.fork().with_author(Some(bad.clone()));
    fork.insert(&list, 0, 2).unwrap();
    doc.merge(&mut fork).unwrap();

    doc.isolate(&epoch);
    let mut view = doc.hydrate(&ROOT, Some(&epoch)).unwrap();
    doc.update_diff_cursor();
    // Neither transition changes the isolated view. Only integration should
    // produce the insertion patch in the internal log.
    doc.mask_author(bad.clone(), &epoch);
    doc.reveal_author(&bad);
    doc.integrate();
    view.apply_patches(TextEncoding::UnicodeCodePoint, doc.diff_incremental())
        .unwrap();
    assert_eq!(view, doc.hydrate(&ROOT, None).unwrap());
}

// ============================ policy ============================

#[test]
fn replacing_write_frontier_equals_fresh_load_with_new_policy() {
    use std::collections::HashMap;

    let (mut doc, alice, epoch) = two_authors();
    let bob = Author::try_from("bbbb").unwrap();
    let bytes = doc.save();
    doc.set_write_frontier(HashMap::from([(alice.clone(), epoch.clone())]));

    let new_policy = HashMap::from([(bob, Vec::new())]);
    doc.set_write_frontier(new_policy.clone());

    let fresh = Automerge::load(&bytes)
        .unwrap()
        .with_write_frontier(new_policy.clone());
    assert_eq!(doc.get_write_frontier(), new_policy);
    assert_eq!(doc.hydrate(None), fresh.hydrate(None));
}

#[test]
fn set_write_frontier_log_patches_replays_to_hydrate() {
    use std::collections::HashMap;

    let (mut doc, alice, epoch) = two_authors();
    let bob = Author::try_from("bbbb").unwrap();
    doc.set_write_frontier(HashMap::from([(alice.clone(), epoch.clone())]));

    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();
    doc.set_write_frontier_log_patches(HashMap::from([(bob, Vec::new())]), &mut log)
        .unwrap();
    view.apply_patches(doc.text_encoding(), doc.make_patches(&mut log))
        .unwrap();
    assert_eq!(view, doc.hydrate(None));

    // Replacing with the empty policy is also one logged transition, and
    // restores full visibility.
    let mut log = PatchLog::active();
    doc.set_write_frontier_log_patches(HashMap::new(), &mut log)
        .unwrap();
    view.apply_patches(doc.text_encoding(), doc.make_patches(&mut log))
        .unwrap();
    assert_eq!(view, doc.hydrate(None));
    assert!(doc.get_write_frontier().is_empty());
    assert_eq!(doc.get(ROOT, "y").unwrap().unwrap().0.as_i64(), Some(2));
    assert_eq!(doc.get(ROOT, "z").unwrap().unwrap().0.as_i64(), Some(3));
}

#[test]
fn load_change_chunks_applies_write_frontier() {
    use std::collections::HashMap;

    let (doc, alice, epoch) = two_authors();
    // Save as raw change chunks (no document chunk).
    let mut bytes = Vec::new();
    for c in doc.get_changes(&[]) {
        bytes.extend(c.raw_bytes());
    }
    let write_frontier: HashMap<_, _> = [(alice, epoch)].into_iter().collect();
    let mut log = PatchLog::active();
    let loaded = Automerge::load_with_options(
        &bytes,
        automerge::LoadOptions::new()
            .patch_log(&mut log)
            .write_frontier(write_frontier.clone()),
    )
    .unwrap();
    assert!(loaded.get(ROOT, "y").unwrap().is_none());
    assert_eq!(loaded.keys(ROOT).count(), 2);
    assert_eq!(loaded.get_write_frontier(), write_frontier);
    // The current-state patches logged during load must be masked too.
    let patches = loaded.make_patches(&mut log);
    assert!(!patches
        .iter()
        .any(|p| matches!(&p.action, PatchAction::PutMap { key, .. } if key == "y")));
}

/// The `load_incremental` empty-document fast path replaces the document
/// wholesale; it must carry over the local author (before the actor, so the
/// actor is not regenerated) as well as the actor and write-frontier policy.
/// Otherwise a masked local author's next edit is indexed visible and its
/// change loses the author footer.
#[test]
fn load_incremental_into_empty_doc_preserves_author() {
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new().with_author(Some(alice.clone()));
    doc.mask_author(alice.clone(), &[], &mut PatchLog::inactive())
        .unwrap();

    // A non-empty document from another author.
    let mut other = AutoCommit::new().with_author(Some(Author::try_from("bbbb").unwrap()));
    other.put(ROOT, "seed", 1).unwrap();
    let bytes = other.document().save();

    doc.load_incremental(&bytes).unwrap();
    assert_eq!(
        doc.get_author(),
        Some(&alice),
        "the empty-doc fast path must keep the local author"
    );
    assert!(doc.is_author_masked(&alice));

    doc.transact(|tx| {
        use automerge::transaction::Transactable;
        tx.put(ROOT, "x", 1)
    })
    .unwrap();
    let change = doc.get_last_local_change().expect("local change exists");
    assert_eq!(
        change.author(),
        Some(alice),
        "the change must carry the author footer"
    );
}

/// Both fork flavours carry the process-local write-frontier policy: `fork`
/// clones it, and `fork_at` re-applies it after rebuilding from the
/// selected changes.
#[test]
fn fork_and_fork_at_carry_write_frontier() {
    let (mut doc, alice, epoch) = two_authors();
    doc.mask_author(alice.clone(), &epoch, &mut PatchLog::inactive())
        .unwrap();
    assert!(doc.get(ROOT, "y").unwrap().is_none());

    let fork = doc.fork();
    assert!(fork.is_author_masked(&alice));
    assert!(fork.get(ROOT, "y").unwrap().is_none());

    let fork_at = doc.fork_at(&doc.get_heads()).unwrap();
    assert!(fork_at.is_author_masked(&alice));
    assert!(
        fork_at.get(ROOT, "y").unwrap().is_none(),
        "fork_at must not materialize the masked author"
    );
}
