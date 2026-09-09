use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, ObjType, PatchAction, PatchLog, ReadDoc, TextEncoding,
    ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

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

    doc.revoke(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut view = doc.hydrate(None);
    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();
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

    doc.revoke(author.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut log = PatchLog::active();
    doc.unrevoke(&author, &mut log).unwrap();
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
    // reconstruction must preserve the same rich content as revocation patches.
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
    for i in 0..3 {
        doc.delete(ROOT, format!("restored{i}")).unwrap();
        let (map, list, text) = &retained[i];
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
