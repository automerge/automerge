use automerge::{
    transaction::Transactable, Author, AutoCommit, PatchAction, ReadDoc, TextEncoding, ROOT,
};

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

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
        doc.revoke(author, &[]);
        doc.isolate(&after);
        check_diffs(&mut doc, &before, &after, &mut view);
    } else {
        doc.update_diff_cursor();
        let mut view = doc.hydrate(ROOT, None).unwrap();
        doc.revoke(author, &[]);
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
fn equal_heads_are_historically_equal_despite_pending_revocation_patches() {
    let author = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new_with_encoding(ENCODING).with_author(Some(author.clone()));
    doc.put(ROOT, "x", 1).unwrap();
    let heads = doc.get_heads();
    doc.update_diff_cursor();
    doc.revoke(author, &[]);

    assert!(doc.diff(&heads, &heads).is_empty());
    assert!(doc
        .diff_obj(&ROOT, &heads, &heads, true)
        .unwrap()
        .is_empty());
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert!(matches!(&patches[0].action, PatchAction::DeleteMap { key } if key == "x"));
    assert!(doc.diff_incremental().is_empty());
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

    doc.revoke(author, &epoch);
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

#[test]
fn inactive_incremental_diff_starts_from_empty_at_isolated_heads() {
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    doc.put(ROOT, "x", 1).unwrap();
    let heads = doc.get_heads();
    doc.put(ROOT, "y", 2).unwrap();
    doc.commit();
    doc.isolate(&heads);

    let expected = doc.diff(&[], &heads);
    assert_eq!(expected.len(), 1);
    assert_eq!(doc.diff_incremental(), expected);
    assert!(doc.diff_incremental().is_empty());
    doc.reset_diff_cursor();
    assert_eq!(doc.diff_incremental(), expected);
}

#[test]
fn incremental_tracking_can_start_and_advance_at_empty_heads() {
    let mut doc = AutoCommit::new_with_encoding(ENCODING);
    doc.update_diff_cursor();
    doc.put(ROOT, "x", 1).unwrap();
    let heads = doc.get_heads();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);

    doc.isolate(&[]);
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    assert!(matches!(&patches[0].action, PatchAction::DeleteMap { key } if key == "x"));
    assert!(doc.diff_cursor().is_empty());
    assert!(doc.diff_incremental().is_empty());

    doc.isolate(&heads);
    assert_eq!(doc.diff_incremental().len(), 1);
    assert!(doc.diff_incremental().is_empty());
}
