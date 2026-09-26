//! Local edits by a masked author: recorded and synced, never shown.
//! The view is a function of `(history, policy)` — the same history under
//! the same write-frontier looks identical for everyone, so a masked local
//! author's own edits are indexed as hidden from the moment they are made.

use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Author, AutoCommit, Automerge, ObjType, PatchLog, ReadDoc, ScalarValue, Value, ROOT,
};

/// Reload `doc` from its saved bytes with the same write-frontier policy.
fn reload_with_policy(doc: &Automerge) -> Automerge {
    Automerge::load(&doc.save())
        .expect("saved document reloads")
        .with_write_frontier(doc.get_write_frontier())
}

/// Owned value of `get` for comparison across documents (drops the `ExId`
/// only in favour of the value; object ids are still compared via the
/// value's shape in `hydrate`).
fn get_value<O: AsRef<automerge::ObjId>, P: Into<automerge::Prop>>(
    doc: &Automerge,
    obj: O,
    prop: P,
) -> Option<Value<'static>> {
    doc.get(obj, prop)
        .expect("get succeeds")
        .map(|(v, _)| v.into_owned())
}

/// After every masked local edit the indexed fast path (the live document)
/// must agree with a genuinely independent slow materialisation: a fresh
/// `Automerge::load` of the saved bytes with the same write-frontier policy
/// (`reload_with_policy`). Comparing `text()` with
/// `text_at(&get_heads())` was vacuous — `read_at` maps current heads back
/// to `ReadAt::Current`, so both sides used the same indexed path.
#[test]
fn masked_local_edits_keep_fast_and_slow_paths_in_agreement() {
    fn check(doc: &mut AutoCommit, text: &automerge::ObjId, list: &automerge::ObjId) {
        let live = doc.document();
        let oracle = reload_with_policy(live);
        assert_eq!(
            live.hydrate(None),
            oracle.hydrate(None),
            "hydrate disagrees with fresh reload under same policy"
        );
        assert_eq!(live.text(text).unwrap(), oracle.text(text).unwrap());
        assert_eq!(live.length(text), oracle.length(text));
        assert_eq!(live.length(list), oracle.length(list));
        assert_eq!(get_value(live, ROOT, "c"), get_value(&oracle, ROOT, "c"));
        assert_eq!(get_value(live, list, 0), get_value(&oracle, list, 0));
        assert_eq!(live.marks(text).unwrap(), oracle.marks(text).unwrap());
        let spans: Vec<_> = live.spans(text).unwrap().collect();
        let spans_oracle: Vec<_> = oracle.spans(text).unwrap().collect();
        assert_eq!(spans, spans_oracle);
    }

    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, 1).unwrap();
    doc.insert(&list, 1, 2).unwrap();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.commit();
    let epoch = doc.get_heads();
    doc.mask_author(alice, &epoch);
    doc.update_diff_cursor();
    let before = doc.document().hydrate(None);

    // All positions refer to the visible document ("hello", [1, 2], c=10).
    doc.splice_text(&text, 2, 0, "abc").unwrap();
    check(&mut doc, &text, &list);
    doc.splice_text(&text, 4, 1, "X").unwrap();
    check(&mut doc, &text, &list);
    doc.insert(&list, 1, 99).unwrap();
    check(&mut doc, &text, &list);
    doc.splice(&list, 0, 1, Vec::<ScalarValue>::new()).unwrap();
    check(&mut doc, &text, &list);
    doc.mark(
        &text,
        Mark::new("bold".into(), true, 0, 3),
        ExpandMark::Both,
    )
    .unwrap();
    check(&mut doc, &text, &list);
    doc.increment(ROOT, "c", 5).unwrap();
    check(&mut doc, &text, &list);
    doc.commit();
    check(&mut doc, &text, &list);

    // The visible view never changed, and no patches were emitted.
    assert_eq!(doc.text(&text).unwrap(), "hello");
    assert_eq!(doc.length(&list), 2);
    assert_eq!(doc.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(10));
    let patches = doc.diff_incremental();
    assert!(patches.is_empty(), "masked edits emitted: {patches:?}");
    assert_eq!(doc.document().hydrate(None), before);
}

/// The view is a function of `(history, policy)`: the same history under
/// the same write-frontier hydrates identically whether the masked edits were
/// made locally, loaded fresh, or merged by a peer.
#[test]
fn masked_local_edits_do_not_change_the_view_for_any_peer() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut a = AutoCommit::new().with_author(Some(alice.clone()));
    let text = a.put_object(ROOT, "text", ObjType::Text).unwrap();
    a.splice_text(&text, 0, 0, "hi").unwrap();
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let epoch = a.get_heads();
    let mut b = a.fork().with_author(Some(bob));
    b.put(ROOT, "z", 3).unwrap();
    b.commit();
    a.merge(&mut b).unwrap();
    a.mask_author(alice.clone(), &epoch);
    let before = a.document().hydrate(None);

    // Masked local edits: recorded and synced, never shown.
    a.splice_text(&text, 0, 0, "XY").unwrap();
    a.put(ROOT, "w", 9).unwrap();
    a.commit();
    assert_eq!(a.document().hydrate(None), before);

    // Same history loaded fresh under the same policy.
    let mut fresh = Automerge::load(&a.save()).unwrap();
    fresh
        .mask_author(alice.clone(), &epoch, &mut PatchLog::inactive())
        .unwrap();
    assert_eq!(fresh.hydrate(None), before);

    // A peer holding the policy sees the same thing after merge.
    b.mask_author(alice, &epoch);
    b.merge(&mut a).unwrap();
    assert_eq!(b.document().hydrate(None), before);
}

/// Rolling back a transaction full of masked edits restores the op set and
/// its indexes exactly: hydrate is unchanged and fast/slow paths agree.
#[test]
fn masked_local_edits_roll_back_cleanly() {
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = Automerge::new().with_author(Some(alice.clone()));
    let text = doc
        .transact(|tx| {
            let text = tx.put_object(ROOT, "text", ObjType::Text)?;
            tx.splice_text(&text, 0, 0, "hello")?;
            tx.put(ROOT, "c", ScalarValue::counter(10))?;
            Ok::<_, automerge::AutomergeError>(text)
        })
        .unwrap()
        .result;
    let epoch = doc.get_heads();
    doc.mask_author(alice, &epoch, &mut PatchLog::inactive())
        .unwrap();
    let before = doc.hydrate(None);

    let mut tx = doc.transaction();
    tx.splice_text(&text, 2, 0, "abc").unwrap();
    tx.splice_text(&text, 4, 1, "X").unwrap();
    tx.put(ROOT, "y", 2).unwrap();
    tx.increment(ROOT, "c", 5).unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, 0, 3),
        ExpandMark::Both,
    )
    .unwrap();
    tx.rollback();

    assert_eq!(doc.hydrate(None), before);
    let heads = doc.get_heads();
    assert_eq!(
        doc.text(&text).unwrap(),
        doc.text_at(&text, &heads).unwrap()
    );
    assert_eq!(doc.length(&text), doc.length_at(&text, &heads));
    assert_eq!(doc.text(&text).unwrap(), "hello");
    assert_eq!(doc.get(ROOT, "c").unwrap().unwrap().0.as_i64(), Some(10));
}

/// An isolated session by a masked local author also reads the visible
/// view: `isolate_actor` must not isolate the masked actor's counter,
/// which would unmask the session's own ops inside its scoped reads.
#[test]
fn masked_local_edits_in_isolation_stay_hidden() {
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hi").unwrap();
    doc.commit();
    let epoch = doc.get_heads();
    doc.mask_author(alice, &epoch);

    let heads = doc.get_heads();
    doc.isolate(&heads);
    // Positions refer to the visible document ("hi").
    doc.splice_text(&text, 2, 0, "!!").unwrap();
    assert_eq!(doc.text(&text).unwrap(), "hi");
    doc.put(ROOT, "y", 2).unwrap();
    assert!(doc.get(ROOT, "y").unwrap().is_none());
    doc.integrate();
    assert_eq!(doc.text(&text).unwrap(), "hi");
    assert!(doc.get(ROOT, "y").unwrap().is_none());
}

/// Revealing the author reveals the recorded edits, with patches that
/// replay to the hydrated document.
#[test]
fn reveal_author_reveals_masked_local_edits_with_replaying_patches() {
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hi").unwrap();
    doc.commit();
    let epoch = doc.get_heads();
    doc.mask_author(alice.clone(), &epoch);

    doc.splice_text(&text, 2, 0, " there").unwrap();
    doc.put(ROOT, "y", 2).unwrap();
    doc.commit();
    assert_eq!(doc.text(&text).unwrap(), "hi");

    doc.update_diff_cursor();
    let mut model = doc.document().hydrate(None);
    doc.reveal_author(&alice);
    let patches = doc.diff_incremental();
    model.apply_patches(doc.text_encoding(), patches).unwrap();
    assert_eq!(model, doc.document().hydrate(None));

    assert_eq!(doc.text(&text).unwrap(), "hi there");
    assert_eq!(doc.get(ROOT, "y").unwrap().unwrap().0.as_i64(), Some(2));
}

#[test]
fn local_transaction_by_masked_author_with_new_actor_is_hidden() {
    let alice = Author::try_from("aaaa").unwrap();
    let bob = Author::try_from("bbbb").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "x", 1).unwrap();
    doc.commit();
    let epoch = doc.get_heads();
    doc.mask_author(alice.clone(), &epoch);

    // Switching authors and back gives alice a brand-new actor: its first
    // (seq == 1) change must still enter the mask at her write-frontier boundary.
    doc.set_author(Some(bob));
    doc.set_author(Some(alice));

    doc.update_diff_cursor();
    doc.put(ROOT, "y", 2).unwrap();
    doc.commit();

    assert_eq!(doc.get(ROOT, "x").unwrap().unwrap().0.as_i64(), Some(1));
    assert!(doc.get(ROOT, "y").unwrap().is_none());

    let patches = doc.diff_incremental();
    let last_y = patches
        .iter()
        .filter_map(|p| match &p.action {
            automerge::PatchAction::PutMap { key, .. } if key == "y" => Some(true),
            automerge::PatchAction::DeleteMap { key } if key == "y" => Some(false),
            _ => None,
        })
        .next_back();
    assert_ne!(last_y, Some(true), "patches leave `y` visible: {patches:?}");
}

/// A masked splice must select its deletion targets from the visible
/// pre-splice sequence: masked inserts have no visible width and masked
/// deletes do not collapse positions, so the delete cursor must not be
/// advanced by inserted width nor re-used for consecutive deletes.
/// Revealing reveals what was actually recorded.
#[test]
fn masked_splice_deletes_the_visible_targets() {
    fn i64s(doc: &Automerge, list: &automerge::ObjId) -> Vec<i64> {
        (0..doc.length(list))
            .map(|i| {
                doc.get(list, i)
                    .unwrap()
                    .expect("list element present")
                    .0
                    .as_i64()
                    .expect("list element is an integer")
            })
            .collect()
    }

    // Case 1: multi-delete.
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, 1).unwrap();
    doc.insert(&list, 1, 2).unwrap();
    doc.insert(&list, 2, 3).unwrap();
    doc.commit();
    let h = doc.get_heads();
    doc.mask_author(alice.clone(), &h);
    doc.splice(&list, 0, 2, Vec::<ScalarValue>::new()).unwrap();
    doc.commit();
    doc.reveal_author(&alice);
    assert_eq!(
        i64s(doc.document(), &list),
        vec![3],
        "masked splice(0, 2, []) must delete elements 1 and 2"
    );

    // Case 2: replacement.
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, 1).unwrap();
    doc.insert(&list, 1, 2).unwrap();
    doc.insert(&list, 2, 3).unwrap();
    doc.commit();
    let h = doc.get_heads();
    doc.mask_author(alice.clone(), &h);
    doc.splice(&list, 0, 1, vec![ScalarValue::from(9)]).unwrap();
    doc.commit();
    doc.reveal_author(&alice);
    assert_eq!(
        i64s(doc.document(), &list),
        vec![9, 2, 3],
        "masked splice(0, 1, [9]) must replace element 1"
    );

    // Case 3: text. Masked text splices follow the same rules.
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "abc").unwrap();
    doc.commit();
    let h = doc.get_heads();
    doc.mask_author(alice.clone(), &h);
    doc.splice_text(&text, 0, 2, "X").unwrap();
    doc.commit();
    assert_eq!(doc.text(&text).unwrap(), "abc", "masked edits stay hidden");
    doc.reveal_author(&alice);
    assert_eq!(
        doc.text(&text).unwrap(),
        "Xc",
        "masked splice_text(0, 2, \"X\") must replace the first two characters"
    );
}

/// Masked isolation at earlier heads must not break the actor dependency
/// chain: choosing a causally valid actor for the isolation heads is
/// separate from not lifting the mask for the read scope. A change claiming
/// the actor's next sequence number while depending only on earlier heads
/// would make `fork_at` (or any peer import) of that head fail.
#[test]
fn masked_edit_at_earlier_heads_preserves_actor_chain() {
    let alice = Author::try_from("aaaa").unwrap();
    let mut doc = AutoCommit::new().with_author(Some(alice.clone()));
    doc.put(ROOT, "x", 1).unwrap();
    doc.commit();
    let h1 = doc.get_heads();
    doc.put(ROOT, "x", 2).unwrap();
    doc.commit();
    let h2 = doc.get_heads();

    doc.mask_author(alice, &h1);
    doc.isolate(&h1);
    doc.put(ROOT, "y", 3).unwrap();
    doc.integrate();

    // The isolated commit's head is the one that is not H2.
    let heads = doc.get_heads();
    let h3 = *heads
        .iter()
        .find(|head| **head != h2[0])
        .expect("isolated commit produced a new head");

    // Forking at H3 traverses its dependencies; importing the masked
    // change must find every predecessor of its actor.
    let mut fork = doc
        .fork_at(&[h3])
        .expect("fork_at of a head of this document must succeed");
    assert_eq!(
        fork.get_heads(),
        vec![h3],
        "fork_at should reproduce exactly the requested head"
    );

    // The same history round-trips through save/load.
    let reloaded = Automerge::load(&doc.document().save()).expect("saved document reloads");
    assert_eq!(reloaded.get_heads(), doc.get_heads());
}

/// Hydrating inside a transaction isolated at earlier heads must show the
/// isolated view, not the current document: the `ReadDoc::hydrate` route
/// for transactions has to honour the transaction's scope.
#[test]
fn hydrate_inside_isolated_transaction_sees_isolated_view() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "x", 1).unwrap();
    tx.commit();
    let h1 = doc.get_heads();
    let mut tx = doc.transaction();
    tx.put(ROOT, "x", 2).unwrap();
    tx.commit();

    let at_h1 = doc.hydrate(Some(&h1));
    let current = doc.hydrate(None);
    assert_ne!(at_h1, current, "the two states must differ for this test");

    let tx = doc
        .transaction_at(PatchLog::inactive(), &h1)
        .expect("a fresh patch log belongs to any document");
    let inside = tx.hydrate(ROOT, None).unwrap();
    assert_eq!(
        inside, at_h1,
        "hydrate inside an isolated transaction must see the isolated view"
    );
}
