use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, ObjType, PatchAction, PatchLog, ReadDoc, TextEncoding,
    ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

// =============== write-frontier & historical diff semantics ===============

fn historical_and_incremental_diffs_are_distinct(isolated: bool) {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
    doc.put(ROOT, "x", 1).unwrap();
    let before = doc.get_heads();

    if isolated {
        doc.set_author(Some(Author::try_from("bbbb").unwrap()));
        doc.put(ROOT, "y", 2).unwrap();
        let after = doc.get_heads();
        doc.put(ROOT, "z", 3).unwrap();
        doc.commit();
        doc.isolate(&before);
        doc.update_diff_cursor();
        let mut view = doc.hydrate(ROOT, Some(&before)).unwrap();
        doc.mask_author(author, &[]);
        doc.isolate(&after);
        check_diffs(&mut doc, &before, &after, &mut view);
    } else {
        doc.update_diff_cursor();
        let mut view = doc.hydrate(ROOT, None).unwrap();
        doc.mask_author(author, &[]);
        doc.set_author(Some(Author::try_from("bbbb").unwrap()));
        doc.put(ROOT, "y", 2).unwrap();
        let after = doc.get_heads();
        check_diffs(&mut doc, &before, &after, &mut view);
    }
}
fn check_diffs(
    doc: &mut AutoCommit,
    before: &[automerge::ChangeHash],
    after: &[automerge::ChangeHash],
    view: &mut automerge::hydrate::Value,
) {
    // Both historical states already hide x. Only y differs, even though the
    // requested range matches the cursor and the current view heads.
    let historical = doc.diff(before, after);
    assert_eq!(historical.len(), 1);
    assert!(matches!(
        &historical[0].action,
        PatchAction::PutMap { key, value, .. } if key == "y" && value.0 == 2.into()
    ));
    assert_eq!(doc.diff(before, after), historical);
    for recursive in [false, true] {
        assert_eq!(
            doc.diff_obj(&ROOT, before, after, recursive).unwrap(),
            historical
        );
    }
    assert!(doc.diff(after, after).is_empty());

    // The previously observed view still has x. Its incremental update needs
    // the deletion as well, unaffected by the historical calls above.
    let incremental = doc.diff_incremental();
    assert_eq!(incremental.len(), 2);
    assert!(matches!(
        &incremental[0].action,
        PatchAction::DeleteMap { key } if key == "x"
    ));
    assert_eq!(incremental[1], historical[0]);
    view.apply_patches(ENCODING, incremental).unwrap();
    assert_eq!(*view, doc.hydrate(ROOT, Some(after)).unwrap());
    assert_eq!(doc.diff_cursor(), after);
    assert!(doc.diff_incremental().is_empty());
}

#[test]
fn historical_diff_does_not_use_accumulated_transitions() {
    historical_and_incremental_diffs_are_distinct(false);
}

#[test]
fn isolated_incremental_diff_tracks_transitions_between_different_heads() {
    historical_and_incremental_diffs_are_distinct(true);
}

#[test]
fn historical_object_diff_respects_object_and_recursive_scope() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    let map = doc
        .put_object(ROOT, "map", automerge::ObjType::Map)
        .unwrap();
    let child = doc
        .put_object(&map, "child", automerge::ObjType::Map)
        .unwrap();
    let epoch = doc.get_heads();
    doc.set_author(Some(author.clone()));
    doc.put(&map, "x", 1).unwrap();
    doc.put(&child, "x", 1).unwrap();
    let before = doc.get_heads();
    let mut observed = doc.hydrate(ROOT, None).unwrap();
    doc.update_diff_cursor();

    doc.mask_author(author, &epoch);
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.put(&map, "y", 2).unwrap();
    doc.put(&child, "y", 2).unwrap();
    doc.put(ROOT, "unrelated", true).unwrap();
    let after = doc.get_heads();

    let shallow = doc.diff_obj(&map, &before, &after, false).unwrap();
    assert_eq!(shallow.len(), 1);
    assert_eq!(shallow[0].obj, map);
    let recursive = doc.diff_obj(&map, &before, &after, true).unwrap();
    assert_eq!(recursive.len(), 2);
    assert!(recursive.iter().all(|patch| {
        (patch.obj == map || patch.obj == child)
            && matches!(&patch.action, PatchAction::PutMap { key, .. } if key == "y")
    }));
    // Object-scoped patches retain their paths from ROOT, so apply them to a
    // root view and compare just the map (the unrelated root edit is excluded).
    let mut root = doc.hydrate(ROOT, Some(&before)).unwrap();
    root.apply_patches(ENCODING, recursive).unwrap();
    let automerge::hydrate::Value::Map(root) = root else {
        panic!("expected root map");
    };
    assert_eq!(
        root.get("map").unwrap(),
        &doc.hydrate(&map, Some(&after)).unwrap()
    );

    observed
        .apply_patches(ENCODING, doc.diff_incremental())
        .unwrap();
    assert_eq!(observed, doc.hydrate(ROOT, None).unwrap());
}

// ==================== restored-subtree exposure ====================

fn authored_doc() -> (Automerge, Author<'static>) {
    let author = Author::try_from("aaaa").unwrap();
    let doc = Automerge::new_with_encoding(ENCODING)
        .with_author(Some(author.clone()))
        .with_actor(ActorId::from(vec![0x80]));
    (doc, author)
}

fn other_author(doc: &mut Automerge) {
    doc.set_author(Some(Author::try_from("bbbb").unwrap()));
    doc.set_actor(ActorId::from(vec![0x10]));
}

#[test]
fn restored_nested_lists_include_unchanged_other_author_descendants() {
    let (mut doc, author) = authored_doc();
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    let nested = tx.insert_object(&list, 0, ObjType::List).unwrap();
    tx.insert(&nested, 0, "original").unwrap();
    tx.commit();

    other_author(&mut doc);
    let mut tx = doc.transaction();
    tx.insert(&nested, 1, "other").unwrap();
    let map = tx.insert_object(&list, 1, ObjType::Map).unwrap();
    tx.put(&map, "value", 42).unwrap();
    let deeper = tx.put_object(&map, "deeper", ObjType::List).unwrap();
    tx.insert(&deeper, 0, true).unwrap();
    tx.commit();

    doc.mask_author(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();
    doc.reveal_author(&author, &mut log).unwrap();
    let patches = doc.make_patches(&mut log);
    assert_eq!(patches, doc.diff(&[], &doc.get_heads()));
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(view, doc.hydrate(None));
}

#[test]
fn restored_text_preserves_other_author_marks_and_block_children() {
    let (mut doc, author) = authored_doc();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abc").unwrap();
    let block = tx.split_block(&text, 0).unwrap();
    tx.commit();

    other_author(&mut doc);
    let mut tx = doc.transaction();
    tx.splice_text(&text, 4, 0, "XYZ").unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, 1, 7),
        ExpandMark::Both,
    )
    .unwrap();
    tx.put(&block, "type", "paragraph").unwrap();
    let attrs = tx.put_object(&block, "attrs", ObjType::Map).unwrap();
    tx.put(&attrs, "level", 2).unwrap();
    tx.commit();

    doc.mask_author(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut log = PatchLog::active();
    doc.reveal_author(&author, &mut log).unwrap();
    let patches = doc.make_patches(&mut log);
    // Hydrated text currently does not model marks/block children, so verify
    // the actual rich-text patches, including their complete paths and IDs.
    assert_eq!(patches, doc.diff(&[], &doc.get_heads()));
    assert!(patches.iter().any(|p| p.obj == text && matches!(
        &p.action,
        PatchAction::SpliceText { value, marks: Some(marks), .. }
            if value.make_string() == "abcXYZ" && marks.iter().any(|(name, value)| name == "bold" && value == &automerge::ScalarValue::Boolean(true))
    )));
    assert!(patches.iter().any(|p| p.obj == text
        && matches!(
            &p.action, PatchAction::Insert { index: 0, values }
                if values.len() == 1 && values.get(0).unwrap().1 == block
        )));
    assert!(patches.iter().any(|p| p.obj == attrs
        && matches!(
            &p.action, PatchAction::PutMap { key, value, .. }
                if key == "level" && value.0 == 2.into()
        )));
}

#[test]
fn historical_exposure_preserves_marks_and_block_children() {
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    let list = doc.put_object(&map, "list", ObjType::List).unwrap();
    let text = doc.insert_object(&list, 0, ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".into(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();
    let block_text = doc.put_object(&map, "text", ObjType::Text).unwrap();
    let block = doc.split_block(&block_text, 0).unwrap();
    doc.put(&block, "type", "paragraph").unwrap();
    let visible = doc.get_heads();
    doc.delete(ROOT, "map").unwrap();
    doc.delete(&map, "list").unwrap();
    doc.delete(&list, 0).unwrap();
    doc.join_block(&block_text, 0).unwrap();
    let hidden = doc.get_heads();

    // The existing exposure queue also serves historical diffs. Its text
    // reconstruction must preserve the same rich content as write-frontier patches.
    let recursive = doc.diff_obj(&ROOT, &hidden, &visible, true).unwrap();
    assert!(recursive.iter().any(|p| p.obj == text
        && matches!(
            &p.action, PatchAction::SpliceText { value, marks: Some(marks), .. }
                if value.make_string() == "hello" && !marks.is_empty()
        )));
    assert!(recursive.iter().any(|p| p.obj == block));
}

#[test]
fn historical_diff_restored_siblings_do_not_snapshot_retained_objects() {
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    // Interleave exposed and retained objects of each kind to check that the
    // exposure queue snapshots only restored subtrees, not retained siblings.
    let mut restored = Vec::new();
    let mut retained = Vec::new();
    for i in 0..3 {
        let map = doc
            .put_object(ROOT, format!("restored{i}"), ObjType::Map)
            .unwrap();
        let list = doc.put_object(&map, "list", ObjType::List).unwrap();
        let text = doc.put_object(&map, "text", ObjType::Text).unwrap();
        doc.insert(&list, 0, "unchanged").unwrap();
        doc.splice_text(&text, 0, 0, "unchanged").unwrap();
        restored.push(map);
        let map = doc
            .put_object(ROOT, format!("retained{i}"), ObjType::Map)
            .unwrap();
        let list = doc.put_object(&map, "list", ObjType::List).unwrap();
        let text = doc.put_object(&map, "text", ObjType::Text).unwrap();
        doc.put(&map, "unchanged", true).unwrap();
        doc.insert(&list, 0, "unchanged").unwrap();
        doc.splice_text(&text, 0, 0, "unchanged").unwrap();
        retained.push((map, list, text));
    }
    let after = doc.get_heads();
    for (i, (map, list, text)) in retained.iter().enumerate() {
        doc.delete(ROOT, format!("restored{i}")).unwrap();
        doc.put(map, "changed", true).unwrap();
        doc.insert(list, 1, "new").unwrap();
        doc.splice_text(text, 9, 0, "new").unwrap();
    }
    let before = doc.get_heads();
    // A later edit must not leak into the historical after-state snapshot.
    doc.put(&restored[0], "future", true).unwrap();
    doc.commit();

    let patches = doc.diff(&before, &after);
    for (map, list, text) in &retained {
        let map_patches: Vec<_> = patches.iter().filter(|p| &p.obj == map).collect();
        assert_eq!(map_patches.len(), 1);
        assert!(
            matches!(&map_patches[0].action, PatchAction::DeleteMap { key } if key == "changed")
        );
        for (obj, expected_length) in [(list, 1), (text, 3)] {
            let delta: Vec<_> = patches.iter().filter(|p| &p.obj == obj).collect();
            assert_eq!(delta.len(), 1);
            assert!(
                matches!(&delta[0].action, PatchAction::DeleteSeq { length, .. } if *length == expected_length)
            );
        }
    }
    let mut view = doc.hydrate(ROOT, Some(&before)).unwrap();
    view.apply_patches(ENCODING, patches).unwrap();
    assert_eq!(view, doc.hydrate(ROOT, Some(&after)).unwrap());
}
