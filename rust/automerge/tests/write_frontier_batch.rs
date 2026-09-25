use automerge::{
    hydrate,
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ActorId, Author, AutoCommit, Automerge, Change, ChangeHash, ObjType, Patch, PatchAction,
    PatchLog, PatchLogMismatch, ReadDoc, TextEncoding, ROOT,
};
use itertools::Itertools;

const ENCODING: TextEncoding = TextEncoding::UnicodeCodePoint;

fn good_author() -> Author<'static> {
    Author::from(vec![0xbb])
}

fn masked_author() -> Author<'static> {
    Author::from(vec![0xaa])
}

fn source_doc() -> AutoCommit {
    // Setting an author generates a new actor, so set the deterministic actor last.
    AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x10]))
}

fn assert_patches_reproduce_doc(mut view: hydrate::Value, doc: &AutoCommit, patches: &[Patch]) {
    view.apply_patches(ENCODING, patches.iter().cloned())
        .unwrap();
    assert_eq!(
        view,
        doc.hydrate(&ROOT, None).unwrap(),
        "incremental patches must reproduce the visible document: {patches:#?}"
    );
}

#[test]
fn masked_incoming_list_insert_does_not_advance_patch_index() {
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap(); // R
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(masked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.insert(&list, 0, "X").unwrap(); // XR
    source.commit();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    // Y is anchored after X, but X must occupy no space in the receiver's view.
    source.insert(&list, 1, "Y").unwrap(); // XYR
    source.commit();

    // The boundary is already imported: this is not pending write-frontier resolution.
    target.mask_author(masked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn masked_existing_list_insert_does_not_advance_patch_index() {
    // Sibling of `masked_incoming_list_insert_does_not_advance_patch_index`,
    // but the masked "X" is *already imported* as a doc op before the batch
    // arrives. There is no incoming ChangeOp carrying a `masked` flag for X;
    // its invisibility can only come from the active write-frontier clock while the
    // untangler walks the pre-existing doc ops. This is the case that a naive
    // fix (treating every doc op as revealed) would silently get wrong while
    // the incoming-op test still passes.
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap(); // R  (good author)
    let base = source.get_heads();

    // The masked author inserts X ahead of R.
    source = source
        .with_author(Some(masked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.insert(&list, 0, "X").unwrap(); // XR
    source.commit();
    let with_x = source.get_heads();

    // Clone *after* X is committed: the target holds X as a doc op, then hides
    // it by masking the author at the boundary just before X.
    let mut target = source.clone();
    target.mask_author(masked_author(), &base);
    assert_eq!(target.length(&list), 1);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "R".into());

    // A later good-authored insert, anchored after the (hidden) X, arrives in
    // the batch on its own. X occupies no space in the receiver's view.
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.insert(&list, 1, "Y").unwrap(); // XYR
    source.commit();

    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&with_x))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn masked_incoming_text_insert_does_not_advance_patch_index() {
    let mut source = source_doc();
    let text = source.put_object(ROOT, "text", ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "R").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(masked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.splice_text(&text, 0, 0, "X").unwrap();
    source.commit();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.splice_text(&text, 1, 0, "Y").unwrap();
    source.commit();

    target.mask_author(masked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.text(&text).unwrap(), "YR");
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

#[test]
fn masked_incoming_replacement_does_not_displace_visible_insert_patch() {
    let mut source = source_doc();
    let list = source.put_object(ROOT, "list", ObjType::List).unwrap();
    source.insert(&list, 0, "R").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source.insert(&list, 0, "Y").unwrap();
    source.commit();
    source = source
        .with_author(Some(masked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source.put(&list, 0, "X").unwrap();
    source.commit();

    target.mask_author(masked_author(), &base);
    let view = target.hydrate(&ROOT, None).unwrap();
    target.update_diff_cursor();
    // Both Y's insertion and its masked overwrite arrive in the same batch.
    target
        .apply_changes_batch(source.get_changes(&base))
        .unwrap();

    assert_eq!(target.length(&list), 2);
    assert_eq!(target.get(&list, 0).unwrap().unwrap().0, "Y".into());
    assert_eq!(target.get_all(&list, 0).unwrap().len(), 1);
    assert_eq!(target.get(&list, 1).unwrap().unwrap().0, "R".into());
    let patches = target.diff_incremental();
    assert_patches_reproduce_doc(view, &target, &patches);
}

fn assert_masked_marks_not_patched(import_marks_first: bool) {
    let mut source = source_doc();
    let text = source.put_object(ROOT, "text", ObjType::Text).unwrap();
    source.splice_text(&text, 0, 0, "ab").unwrap();
    let base = source.get_heads();
    let mut target = source.clone();

    source = source
        .with_author(Some(masked_author()))
        .with_actor(ActorId::from(vec![0x20]));
    source
        .mark(
            &text,
            Mark::new("bold".into(), true, 0, 2),
            ExpandMark::Both,
        )
        .unwrap();
    source.commit();
    let mark_change = source.get_last_local_change().unwrap();
    source = source
        .with_author(Some(good_author()))
        .with_actor(ActorId::from(vec![0x30]));
    source.splice_text(&text, 1, 0, "X").unwrap();
    source.commit();
    let insert_change = source.get_last_local_change().unwrap();

    target.mask_author(masked_author(), &base);
    if import_marks_first {
        target.apply_changes_batch([mark_change.clone()]).unwrap();
        assert_eq!(target.text(&text).unwrap(), "ab");
        assert!(target.marks(&text).unwrap().is_empty());
    }
    target.update_diff_cursor();
    if import_marks_first {
        target.apply_changes_batch([insert_change]).unwrap();
    } else {
        target
            .apply_changes_batch([mark_change, insert_change])
            .unwrap();
    }

    assert_eq!(target.text(&text).unwrap(), "aXb");
    assert!(target.marks(&text).unwrap().is_empty());
    let patches = target.diff_incremental();
    // Hydration does not implement Mark patches; inspect both ways patches can
    // carry marks instead of applying these patches to a hydrated document.
    assert!(
        patches.iter().any(|p| p.obj == text
            && matches!(
                &p.action,
                PatchAction::SpliceText { index: 1, value, .. } if value.make_string() == "X"
            )),
        "missing allowed insertion: {patches:#?}"
    );
    assert!(
        patches.iter().all(|p| match &p.action {
            PatchAction::SpliceText { marks: Some(marks), .. } => marks.is_empty(),
            PatchAction::Mark { marks } => marks.is_empty(),
            _ => true,
        }),
        "masked marks must not appear in patches (import_marks_first={import_marks_first}): {patches:#?}"
    );
}

#[test]
fn masked_incoming_marks_do_not_leak_into_batch_patches() {
    assert_masked_marks_not_patched(false);
}

#[test]
fn already_imported_masked_marks_do_not_leak_into_allowed_insertion_patches() {
    assert_masked_marks_not_patched(true);
}

// ==================== batch permutations ====================

/// Alice's three changes: `a1` (pre-boundary), `h` (the boundary) and `a2`
/// (post-boundary), plus the boundary hash.
fn alice_changes() -> (AutoCommit, ChangeHash, Author<'static>) {
    let alice = Author::try_from("aaaa").unwrap();
    let mut a = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(alice.clone()))
        .with_actor(ActorId::from(vec![0xA0]));
    a.put(ROOT, "a1", 1).unwrap();
    a.commit();
    a.put(ROOT, "h", 2).unwrap();
    a.commit();
    let h = a.get_heads()[0];
    a.put(ROOT, "a2", 3).unwrap();
    a.commit();
    (a, h, alice)
}

fn put_keys(patches: &[Patch]) -> Vec<String> {
    patches
        .iter()
        .filter_map(|p| match &p.action {
            PatchAction::PutMap { key, .. } => Some(key.clone()),
            _ => None,
        })
        .sorted()
        .collect()
}

fn has_delete(patches: &[Patch]) -> bool {
    patches
        .iter()
        .any(|p| matches!(&p.action, PatchAction::DeleteMap { .. }))
}

/// The observer masks Alice at `h` before seeing any of her changes, then
/// applies all three in one batch in every order. Expected: `a1` and `h`
/// visible, `a2` hidden, and the net patches never expose or retract `a2`.
#[test]
fn boundary_in_batch_every_order() {
    let (mut a, h, alice) = alice_changes();
    let changes: Vec<Change> = a.get_changes(&[]);
    for order in (0..changes.len()).permutations(changes.len()) {
        let mut obs = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
        obs.mask_author(alice.clone(), &[h]);
        let mut view = obs.hydrate(ROOT, None).unwrap();
        obs.update_diff_cursor();
        let batch: Vec<Change> = order.iter().map(|i| changes[*i].clone()).collect();
        obs.apply_changes_batch(batch).unwrap();
        let patches = obs.diff_incremental();
        assert_eq!(
            obs.get(ROOT, "a1").unwrap().unwrap().0.as_i64(),
            Some(1),
            "{order:?}"
        );
        assert_eq!(
            obs.get(ROOT, "h").unwrap().unwrap().0.as_i64(),
            Some(2),
            "{order:?}"
        );
        assert!(obs.get(ROOT, "a2").unwrap().is_none(), "{order:?}");
        assert_eq!(
            put_keys(&patches),
            vec!["a1", "h"],
            "{order:?}: {patches:?}"
        );
        assert!(!has_delete(&patches), "{order:?}: {patches:?}");
        view.apply_patches(ENCODING, patches).unwrap();
        assert_eq!(view, obs.hydrate(ROOT, None).unwrap(), "{order:?}");
    }
}

/// As above, but the batch also carries Bob's concurrent write to the same
/// key as Alice's hidden `a2`. Bob is not masked, so his value must win
/// visibly in every order, with a single net put for `a2`.
#[test]
fn boundary_in_batch_with_concurrent_winner_every_order() {
    let (mut a, h, alice) = alice_changes();
    let bob = Author::try_from("bbbb").unwrap();
    let mut b = AutoCommit::new_with_encoding(ENCODING)
        .with_author(Some(bob))
        .with_actor(ActorId::from(vec![0xB0]));
    b.put(ROOT, "a2", 99).unwrap();
    b.commit();
    let mut changes: Vec<Change> = a.get_changes(&[]);
    changes.extend(b.get_changes(&[]));
    for order in (0..changes.len()).permutations(changes.len()) {
        let mut obs = AutoCommit::new_with_encoding(ENCODING).with_actor(ActorId::from(vec![0x01]));
        obs.mask_author(alice.clone(), &[h]);
        let mut view = obs.hydrate(ROOT, None).unwrap();
        obs.update_diff_cursor();
        let batch: Vec<Change> = order.iter().map(|i| changes[*i].clone()).collect();
        obs.apply_changes_batch(batch).unwrap();
        let patches = obs.diff_incremental();
        assert_eq!(
            obs.get(ROOT, "a1").unwrap().unwrap().0.as_i64(),
            Some(1),
            "{order:?}"
        );
        assert_eq!(
            obs.get(ROOT, "h").unwrap().unwrap().0.as_i64(),
            Some(2),
            "{order:?}"
        );
        // Alice's a2 is hidden, so Bob's concurrent write wins visibly.
        assert_eq!(
            obs.get(ROOT, "a2").unwrap().unwrap().0.as_i64(),
            Some(99),
            "{order:?}"
        );
        assert_eq!(
            put_keys(&patches),
            vec!["a1", "a2", "h"],
            "{order:?}: {patches:?}"
        );
        assert!(!has_delete(&patches), "{order:?}: {patches:?}");
        view.apply_patches(ENCODING, patches).unwrap();
        assert_eq!(view, obs.hydrate(ROOT, None).unwrap(), "{order:?}");
    }
}

// ==================== error-path atomicity ====================

fn patch_log_from_actor(actor: &[u8]) -> PatchLog {
    let mut source = AutoCommit::new();
    source.set_actor(ActorId::from(actor));
    source.put(ROOT, "source", "value").unwrap();
    let source_changes = source.get_changes(&[]);

    let mut patch_log = PatchLog::active();
    let mut doc = Automerge::new();
    doc.apply_changes_log_patches(source_changes, &mut patch_log)
        .unwrap();
    patch_log
}

#[test]
fn mismatched_write_frontier_logs_leave_visibility_and_log_unchanged() {
    // Exercise missing actors on either side of the target's actor, as well as
    // an empty target. A missing trailing actor must not be silently accepted.
    for foreign_actor in [0x00, 0xff] {
        for empty_target in [false, true] {
            let mut source = Automerge::new().with_actor(ActorId::from(vec![foreign_actor]));
            let mut tx = source.transaction_log_patches(PatchLog::active()).unwrap();
            tx.put(ROOT, "foreign", true).unwrap();
            let (_, mut log) = tx.commit();
            let expected_patches = source.make_patches(&mut log);

            let author = Author::try_from("aaaa").unwrap();
            let mut target = Automerge::new()
                .with_author(Some(author.clone()))
                .with_actor(ActorId::from(vec![0x80]));
            if !empty_target {
                let mut tx = target.transaction();
                tx.put(ROOT, "x", 1).unwrap();
                tx.commit();
            }
            let heads = target.get_heads();
            let visible = target.hydrate(None);
            assert_eq!(
                target.mask_author(author.clone(), &[], &mut log),
                Err(PatchLogMismatch)
            );
            assert!(target.get_write_frontier().is_empty());
            assert_eq!(target.hydrate(None), visible);
            assert_eq!(target.get_heads(), heads);
            assert_eq!(source.make_patches(&mut log), expected_patches);

            target
                .mask_author(author.clone(), &[], &mut PatchLog::inactive())
                .unwrap();
            let write_frontier = target.get_write_frontier();
            let hidden = target.hydrate(None);
            assert_eq!(
                target.reveal_author(&author, &mut log),
                Err(PatchLogMismatch)
            );
            assert_eq!(target.get_write_frontier(), write_frontier);
            assert_eq!(target.hydrate(None), hidden);
            assert_eq!(target.get_heads(), heads);
            assert_eq!(source.make_patches(&mut log), expected_patches);
        }
    }
}

#[test]
fn patch_log_mismatch_preserves_previously_queued_changes() {
    let mut source = AutoCommit::new();
    source.put(ROOT, "parent", 1).unwrap();
    source.commit();
    let parent = source.get_last_local_change().unwrap().clone();
    source.put(ROOT, "child", 2).unwrap();
    source.commit();
    let child = source.get_last_local_change().unwrap().clone();

    let mut foreign_log = patch_log_from_actor(b"bbbbbb");

    let mut receiver = Automerge::new();
    receiver.apply_changes_batch([child]).unwrap(); // Waiting for its parent.
    assert!(receiver
        .apply_changes_batch_log_patches([parent.clone()], &mut foreign_log)
        .is_err());
    receiver.apply_changes_batch([parent]).unwrap();
    assert!(
        receiver.get(ROOT, "child").unwrap().is_some(),
        "log rejection must not discard a previously queued child"
    );
}

/// Sequence width and top/conflict bookkeeping in the batch walk must use
/// mask-aware visibility, not raw successors: a value restored by masking
/// its deleter occupies one slot, so a later insertion after it patches at
/// the right index. (GPT finding: the walker gave restored "a" zero width
/// and emitted the insert of "X" at index 0 instead of 1.)
#[test]
fn batch_insert_after_value_restored_by_mask() {
    let good = Author::try_from("900d").unwrap();
    let bad = Author::try_from("badd").unwrap();
    let mut g = AutoCommit::new().with_author(Some(good));
    let list = g.put_object(ROOT, "list", ObjType::List).unwrap();
    g.insert(&list, 0, "a").unwrap();
    g.insert(&list, 1, "b").unwrap();
    g.commit();

    // Bad author deletes "a" on a fork.
    let mut b = g.fork().with_author(Some(bad.clone()));
    b.delete(&list, 0).unwrap();
    b.commit();

    // Observer imports the deletion, then masks the bad author fully.
    let mut obs = g.fork();
    obs.merge(&mut b).unwrap();
    obs.mask_author(bad, &[]);
    let strings = |doc: &Automerge| -> Vec<String> {
        (0..doc.length(&list))
            .map(|i| {
                doc.get(&list, i)
                    .unwrap()
                    .expect("list element present")
                    .0
                    .into_owned()
                    .into_string()
                    .expect("list element is a string")
            })
            .collect()
    };
    assert_eq!(strings(obs.document()), vec!["a", "b"]);

    // Good author (never saw the deletion) inserts "X" at index 1.
    g.insert(&list, 1, "X").unwrap();
    g.commit();

    obs.update_diff_cursor();
    let mut model = obs.document().hydrate(None);
    obs.merge(&mut g).unwrap();
    let patches = obs.diff_incremental();
    model.apply_patches(ENCODING, patches.clone()).unwrap();

    assert_eq!(
        strings(obs.document()),
        vec!["a", "X", "b"],
        "document order itself is wrong"
    );
    assert_eq!(
        model,
        obs.document().hydrate(None),
        "patch replay disagrees with document (patches: {patches:?})"
    );
}
