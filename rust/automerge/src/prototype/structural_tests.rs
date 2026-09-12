use super::core::{record_control, Capture, Eligibility, Input, Session, Transition};
use crate::{
    transaction::Transactable, ActorId, Automerge, ChangeHash, ObjId, ObjType, ReadDoc, ROOT,
};

fn actor(n: u8) -> ActorId {
    ActorId::from(vec![n])
}
fn content(doc: &Automerge, hash: ChangeHash) -> Input {
    let c = doc.get_change_by_hash(&hash).unwrap();
    let author = if c.actor_id() == &actor(20) {
        b"alice".to_vec()
    } else {
        b"other".to_vec()
    };
    Input::Content(c, author)
}
fn replay(s: &Session, t: &Transition) {
    let mut observer = s.observe(&t.before).unwrap();
    t.apply(&mut observer).unwrap();
    assert_eq!(
        observer.value,
        s.hydrate(&t.after),
        "patches: {:?}",
        t.content
    );
}
fn deliver(s: &mut Session, inputs: Vec<Input>) -> Transition {
    let t = s.deliver(inputs).unwrap();
    replay(s, &t);
    t
}
fn load(doc: &Automerge, r: ChangeHash) -> Session {
    let mut s = Session::new();
    let mut inputs: Vec<_> = doc
        .get_changes(&[])
        .iter()
        .map(|c| content(doc, c.hash()))
        .collect();
    inputs.push(Input::Authorize(r));
    deliver(&mut s, inputs);
    s
}
fn list(s: &Session, cap: &Capture, obj: &ObjId) -> Vec<(String, ObjId)> {
    s.doc
        .list_range_for(obj, .., Some(s.scope(cap).unwrap()))
        .map(|v| (v.value.to_value().as_str().unwrap().to_owned(), v.id()))
        .collect()
}
#[test]
fn ex10_excluded_increment_and_empty_duplicate_self_diff() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let mut tx = doc.transaction();
    tx.put(ROOT, "count", crate::ScalarValue::counter(10))
        .unwrap();
    let h = tx.commit().0.unwrap();
    let base = doc.get(ROOT, "count").unwrap().unwrap().1;
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.increment(ROOT, "count", 100).unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.increment(ROOT, "count", 5).unwrap();
    let b = tx.commit().0.unwrap();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let cap = s.capture();
    assert_eq!(cap.eligibility[&a], Eligibility::Excluded);
    assert_eq!(cap.eligibility[&b], Eligibility::Eligible);
    let value = s.candidates(&cap, &ROOT, "count");
    assert_eq!(value.len(), 1);
    assert_eq!(value[0].0.as_i64(), Some(15));
    assert_eq!(value[0].1, base);
    for inputs in [vec![], vec![content(&doc, a)], vec![Input::Authorize(r)]] {
        let t = deliver(&mut s, inputs);
        assert!(t.content.is_empty());
        assert!(t.status.is_empty());
        assert_eq!(t.before, t.after);
    }
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(
        s.candidates(&s.capture(), &ROOT, "count")[0].0.as_i64(),
        Some(115)
    );
    assert_eq!(s.candidates(&cap, &ROOT, "count")[0].0.as_i64(), Some(15));
}
#[test]
fn ex11_increment_cannot_migrate_from_excluded_base() {
    for with_old in [false, true] {
        let mut doc = Automerge::new().with_actor(actor(10));
        let h = if with_old {
            let mut tx = doc.transaction();
            tx.put(ROOT, "count", crate::ScalarValue::counter(10))
                .unwrap();
            tx.commit().0.unwrap()
        } else {
            doc.empty_commit(Default::default())
        };
        doc.set_actor(actor(20));
        let mut tx = doc.transaction();
        tx.put(ROOT, "count", crate::ScalarValue::counter(100))
            .unwrap();
        let b = tx.commit().0.unwrap();
        let base = doc.get(ROOT, "count").unwrap().unwrap().1;
        doc.set_actor(actor(40));
        let mut tx = doc.transaction();
        tx.increment(ROOT, "count", 5).unwrap();
        let c = tx.commit().0.unwrap();
        assert_eq!(
            doc.get_change_by_hash(&c).unwrap().decode().operations[0]
                .pred
                .get(0)
                .unwrap()
                .to_string(),
            base.to_string()
        );
        let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
        let mut s = load(&doc, r);
        let cap = s.capture();
        assert_eq!(cap.eligibility[&b], Eligibility::Excluded);
        assert_eq!(cap.eligibility[&c], Eligibility::Eligible);
        let vals = s.candidates(&cap, &ROOT, "count");
        if with_old {
            assert_eq!(vals[0].0.as_i64(), Some(10));
        } else {
            assert!(vals.is_empty());
        }
        deliver(&mut s, vec![Input::Invalidate(r)]);
        assert_eq!(
            s.candidates(&s.capture(), &ROOT, "count")[0].0.as_i64(),
            Some(105)
        );
        let (_, t) = s
            .edit(&cap, actor(60), b"bob", |tx| {
                tx.put(ROOT, "count", crate::ScalarValue::counter(50))
            })
            .unwrap();
        replay(&s, &t);
        let vals = s.candidates(&s.capture(), &ROOT, "count");
        assert_eq!(
            vals.iter()
                .map(|v| v.0.as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![105, 50]
        );
    }
}
#[test]
fn ex11_mixed_scalar_counter_increment_keeps_recorded_suppression() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = doc.empty_commit(Default::default());
    let mut scalar = doc.fork();
    scalar.set_actor(actor(20));
    let mut tx = scalar.transaction();
    tx.put(ROOT, "count", "scalar").unwrap();
    let a = tx.commit().0.unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "count", crate::ScalarValue::counter(10))
        .unwrap();
    tx.commit();
    doc.merge(&mut scalar).unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.increment(ROOT, "count", 5).unwrap();
    let c = tx.commit().0.unwrap();
    assert_eq!(
        doc.get_change_by_hash(&c).unwrap().decode().operations[0]
            .pred
            .len(),
        2
    );
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    assert_eq!(s.capture().eligibility[&a], Eligibility::Excluded);
    assert_eq!(
        s.candidates(&s.capture(), &ROOT, "count")[0].0.as_i64(),
        Some(15)
    );
    deliver(&mut s, vec![Input::Invalidate(r)]);
    let vals = s.candidates(&s.capture(), &ROOT, "count");
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0].0.as_i64(), Some(15));
}
#[test]
fn ex07_formatted_attachment_exclusion_replays_parent_delete() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 0, 0, "abcd").unwrap();
    tx.commit();
    mark(&mut doc, &text, 1, 3);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    rich_deliver(
        &mut s,
        doc.get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect(),
        &text,
    );
    rich_deliver(&mut s, vec![Input::Authorize(r)], &text);
    rich_deliver(&mut s, vec![Input::Invalidate(r)], &text);
}
#[test]
fn ex07_formatted_text_attachment_restores_marks_and_child_edits_once() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 0, 0, "abcd").unwrap();
    tx.commit();
    let m = mark(&mut doc, &text, 1, 3);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    let mut inputs: Vec<_> = doc
        .get_changes(&[])
        .iter()
        .map(|c| content(&doc, c.hash()))
        .collect();
    inputs.push(Input::Authorize(r));
    rich_deliver(&mut s, inputs, &text);
    let hidden = s.capture();
    assert_eq!(hidden.eligibility[&a], Eligibility::Excluded);
    assert_eq!(hidden.eligibility[&m], Eligibility::Eligible);
    let t = rich_deliver(&mut s, vec![Input::Invalidate(r)], &text);
    let rich =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(rich.plain(), "abcd");
    assert_eq!(rich.marked("bold"), "bc");
    let inserted: usize = t
        .content
        .iter()
        .filter_map(|p| match &p.action {
            crate::PatchAction::SpliceText { value, .. } => {
                Some(value.make_string().chars().count())
            }
            _ => None,
        })
        .sum();
    assert_eq!(inserted, 4);
}
#[test]
fn ex07_restored_list_element_exposes_eligible_children() {
    let (mut doc, list, h) = list_base();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let map = tx.insert_object(&list, 1, ObjType::Map).unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.put(&map, "child", "kept").unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(
        s.candidates(&s.capture(), &map, "child")[0].0.as_str(),
        Some("kept")
    );
}
#[test]
fn control_free_restored_formatted_text_preserves_block_marker_and_contents() {
    let (mut doc, text, _) = text_base("abcd");
    mark(&mut doc, &text, 1, 3);
    let mut tx = doc.transaction();
    let block = tx.split_block(&text, 2).unwrap();
    tx.put(&block, "kind", "paragraph").unwrap();
    tx.commit();
    let visible = doc.get_heads();
    let mut tx = doc.transaction();
    tx.delete(ROOT, "text").unwrap();
    tx.commit();
    let hidden = doc.get_heads();
    let patches = doc.diff(&hidden, &visible);
    let mut observer = super::format_observer::RichText::default();
    observer.apply(&text, &patches, doc.text_encoding());
    let expected = super::format_observer::RichText::read(&doc, &text, doc.clock_at(&visible));
    assert_eq!(observer, expected, "patches: {patches:?}");
    assert_eq!(observer.plain(), "ab\u{fffc}cd");
    assert_eq!(observer.marked("bold"), "bc");
}
#[test]
fn ex07_excluded_text_block_restores_eligible_child_contents() {
    let (mut doc, text, h) = text_base("ab");
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let block = tx.split_block(&text, 1).unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.put(&block, "kind", "paragraph").unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    let mut inputs: Vec<_> = doc
        .get_changes(&[])
        .iter()
        .map(|c| content(&doc, c.hash()))
        .collect();
    inputs.push(Input::Authorize(r));
    rich_deliver(&mut s, inputs, &text);
    rich_deliver(&mut s, vec![Input::Invalidate(r)], &text);
}
#[test]
fn ex13_collapsed_gap_selected_authoring_characterizes_expansion_modes() {
    for expand in [
        crate::marks::ExpandMark::None,
        crate::marks::ExpandMark::Before,
        crate::marks::ExpandMark::After,
        crate::marks::ExpandMark::Both,
    ] {
        let (mut doc, obj, h) = text_base("abcd");
        doc.set_actor(actor(20));
        let mut tx = doc.transaction();
        tx.splice_text(&obj, 2, 0, "XY").unwrap();
        tx.commit();
        doc.set_actor(actor(40));
        let mut tx = doc.transaction();
        tx.mark(
            &obj,
            crate::marks::Mark::new("bold".into(), true, 2, 4),
            expand,
        )
        .unwrap();
        tx.commit();
        let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
        let mut s = load(&doc, r);
        let cap = s.capture();
        let (_, t) = s
            .edit(&cap, actor(60), b"bob", |tx| {
                tx.splice_text(&obj, 2, 0, "Q")
            })
            .unwrap();
        replay(&s, &t);
        let mut rich =
            super::format_observer::RichText::read(&s.doc, &obj, Some(s.scope(&cap).unwrap()));
        rich.apply(&obj, &t.content, s.doc.text_encoding());
        let after = super::format_observer::RichText::read(
            &s.doc,
            &obj,
            Some(s.scope(&s.capture()).unwrap()),
        );
        assert_eq!(rich, after);
        assert_eq!(after.plain(), "abQcd");
        // Characterization, distinct from pre-exclusion interior Q: the chosen
        // structural insertion is outside the dormant range for all four modes.
        assert_eq!(after.marked("bold"), "", "{expand:?}");
    }
}
#[test]
fn ex12_unicode_widths_use_captured_encoding() {
    let (mut doc, obj, h) = text_base("a😀cd");
    assert_eq!(doc.text_encoding(), crate::TextEncoding::UnicodeCodePoint);
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.splice_text(&obj, 2, 0, "XY").unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    mark(&mut doc, &obj, 1, 5);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    rich_deliver(
        &mut s,
        doc.get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect(),
        &obj,
    );
    rich_deliver(&mut s, vec![Input::Authorize(r)], &obj);
    let rich =
        super::format_observer::RichText::read(&s.doc, &obj, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(rich.plain(), "a😀cd");
    assert_eq!(rich.marked("bold"), "😀c");
}
#[test]
fn ex12_unicode_selected_capture_roundtrip_in_three_encodings() {
    for encoding in [
        crate::TextEncoding::UnicodeCodePoint,
        crate::TextEncoding::Utf8CodeUnit,
        crate::TextEncoding::Utf16CodeUnit,
    ] {
        let mut doc = Automerge::new_with_encoding(encoding).with_actor(actor(10));
        let mut tx = doc.transaction();
        let obj = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        tx.splice_text(&obj, 0, 0, "a😀cd").unwrap();
        let h = tx.commit().0.unwrap();
        let gap = encoding.width("a😀");
        doc.set_actor(actor(20));
        let mut tx = doc.transaction();
        tx.splice_text(&obj, gap, 0, "XY").unwrap();
        tx.commit();
        doc.set_actor(actor(40));
        mark(&mut doc, &obj, 1, encoding.width("a😀XYc"));
        let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
        let mut s = Session::with_encoding(encoding);
        rich_deliver(
            &mut s,
            doc.get_changes(&[])
                .iter()
                .map(|c| content(&doc, c.hash()))
                .collect(),
            &obj,
        );
        rich_deliver(&mut s, vec![Input::Authorize(r)], &obj);
        let hidden = s.capture();
        let rich =
            super::format_observer::RichText::read(&s.doc, &obj, Some(s.scope(&hidden).unwrap()));
        assert_eq!(rich.plain(), "a😀cd");
        assert_eq!(rich.marked("bold"), "😀c");
        let restored = s.reconstruct().unwrap();
        assert_eq!(restored.doc.text_encoding(), encoding);
        assert_eq!(restored.capture(), hidden);
        let (_, t) = s
            .edit(&hidden, actor(60), b"bob", |tx| {
                tx.splice_text(&obj, gap, 0, "Q")
            })
            .unwrap();
        replay(&s, &t);
        let mut before = rich;
        before.apply(&obj, &t.content, encoding);
        assert_eq!(
            before,
            super::format_observer::RichText::read(
                &s.doc,
                &obj,
                Some(s.scope(&s.capture()).unwrap())
            )
        );
    }
}
#[test]
fn ex07_nested_formatted_subtree_restores_with_simultaneous_child_delta_once() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let h = doc.empty_commit(Default::default());
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let map = tx.put_object(ROOT, "section", ObjType::Map).unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    let text = tx.put_object(&map, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abcd").unwrap();
    tx.commit();
    mark(&mut doc, &text, 1, 3);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    let mut inputs: Vec<_> = doc
        .get_changes(&[])
        .iter()
        .map(|c| content(&doc, c.hash()))
        .collect();
    inputs.push(Input::Authorize(r));
    rich_deliver(&mut s, inputs, &text);
    let hidden = s.capture();
    let mut tx = doc.transaction();
    tx.splice_text(&text, 2, 0, "Q").unwrap();
    let late = tx.commit().0.unwrap();
    let t = rich_deliver(
        &mut s,
        vec![content(&doc, late), Input::Invalidate(r)],
        &text,
    );
    let rich =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(rich.plain(), "abQcd");
    assert_eq!(rich.marked("bold"), "bQc");
    let inserted: usize = t
        .content
        .iter()
        .filter_map(|p| match &p.action {
            crate::PatchAction::SpliceText { value, .. } => {
                Some(value.make_string().chars().count())
            }
            _ => None,
        })
        .sum();
    assert_eq!(inserted, 5);
    assert_eq!(
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&hidden).unwrap()))
            .plain(),
        ""
    );
}
#[test]
fn ex14_selected_authoring_does_not_copy_excluded_mark() {
    let (mut doc, text, h) = text_base("ab");
    doc.set_actor(actor(20));
    mark(&mut doc, &text, 0, 2);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let cap = s.capture();
    let (new, t) = s
        .edit(&cap, actor(60), b"bob", |tx| {
            assert!(tx.marks(&text)?.is_empty());
            tx.splice_text(&text, 1, 0, "Q")
        })
        .unwrap();
    replay(&s, &t);
    let mut before =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&cap).unwrap()));
    before.apply(&text, &t.content, s.doc.text_encoding());
    let after =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(before, after);
    assert_eq!(after.plain(), "aQb");
    assert_eq!(after.marked("bold"), "");
    assert!(s
        .doc
        .get_change_by_hash(&new)
        .unwrap()
        .decode()
        .operations
        .iter()
        .all(|op| !matches!(
            op.action,
            crate::legacy::OpType::MarkBegin(_) | crate::legacy::OpType::MarkEnd(_)
        )));
}
#[test]
fn native_structural_routes_preserve_original_ops_and_selected_reconstruction() {
    let (mut doc, text, h) = text_base("abcd");
    let mut tx = doc.transaction();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.insert(&list, 0, "L").unwrap();
    tx.put(ROOT, "counter", crate::ScalarValue::counter(10))
        .unwrap();
    tx.commit();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 2, 0, "XY").unwrap();
    tx.insert(&list, 1, "X").unwrap();
    tx.increment(ROOT, "counter", 100).unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    mark(&mut doc, &text, 1, 5);
    let mut tx = doc.transaction();
    tx.insert(&list, 2, "Y").unwrap();
    tx.increment(ROOT, "counter", 5).unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let changes = doc.get_changes(&[]);
    let mut batch = Automerge::new();
    batch.apply_changes(changes.clone()).unwrap();
    let mut direct = Automerge::new();
    for c in changes.clone() {
        direct.apply_changes([c]).unwrap();
    }
    let mut merged = Automerge::new();
    merged.merge(&mut doc.clone()).unwrap();
    let compact = Automerge::load(&doc.save()).unwrap();
    let bytes: Vec<_> = changes
        .iter()
        .flat_map(|c| c.raw_bytes().to_vec())
        .collect();
    let mut incremental = Automerge::new();
    incremental.load_incremental(&bytes).unwrap();
    let mut expected = None;
    for route in [direct, batch, merged, compact, incremental] {
        assert_eq!(route.get_heads(), doc.get_heads());
        for c in &changes {
            assert_eq!(
                route.get_change_by_hash(&c.hash()).unwrap().raw_bytes(),
                c.raw_bytes()
            );
        }
        let mut s = Session::new();
        let mut inputs: Vec<_> = route
            .get_changes(&[])
            .iter()
            .map(|c| content(&route, c.hash()))
            .collect();
        inputs.push(Input::Authorize(r));
        rich_deliver(&mut s, inputs, &text);
        let cap = s.capture();
        assert_eq!(cap.eligibility[&a], Eligibility::Excluded);
        let rich =
            super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&cap).unwrap()));
        assert_eq!(rich.plain(), "abcd");
        assert_eq!(rich.marked("bold"), "bc");
        assert_eq!(s.candidates(&cap, &ROOT, "counter")[0].0.as_i64(), Some(15));
        let vals = s
            .doc
            .list_range_for(&list, .., Some(s.scope(&cap).unwrap()))
            .map(|v| v.value.to_value().as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(vals, vec!["L", "Y"]);
        let current = s.hydrate(&cap);
        if let Some(ref old) = expected {
            assert_eq!(&current, old);
        } else {
            expected = Some(current);
        }
        assert_eq!(s.reconstruct().unwrap().hydrate(&cap), s.hydrate(&cap));
    }
}
#[test]
fn ex11_excluded_increment_does_not_suppress_targeted_scalar_conflict() {
    let mut doc = Automerge::new().with_actor(actor(10));
    let base = doc.empty_commit(Default::default());
    let mut scalar = doc.fork();
    scalar.set_actor(actor(50));
    let mut tx = scalar.transaction();
    tx.put(ROOT, "counter", "scalar").unwrap();
    tx.commit();
    let mut tx = doc.transaction();
    tx.put(ROOT, "counter", crate::ScalarValue::counter(10))
        .unwrap();
    tx.commit();
    doc.merge(&mut scalar).unwrap();
    let h = doc.empty_commit(Default::default());
    let before: Vec<_> = doc
        .get_all(ROOT, "counter")
        .unwrap()
        .into_iter()
        .map(|(v, id)| (v.into_owned(), id))
        .collect();
    assert_eq!(before.len(), 2);
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.increment(ROOT, "counter", 5).unwrap();
    let a = tx.commit().0.unwrap();
    assert_eq!(
        doc.get_change_by_hash(&a).unwrap().decode().operations[0]
            .pred
            .len(),
        2
    );
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let cap = s.capture();
    assert_eq!(s.candidates(&cap, &ROOT, "counter"), before);
    assert!(cap.integrated.contains(&base));
    deliver(&mut s, vec![Input::Invalidate(r)]);
    let after = s.candidates(&s.capture(), &ROOT, "counter");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].0.as_i64(), Some(15));
}
#[test]
fn ex14_excluded_unmark_over_partial_range_restores_only_recorded_formatting() {
    let (mut doc, text, _) = text_base("abcd");
    let h = mark(&mut doc, &text, 0, 4);
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.unmark(&text, "bold", 1, 3, crate::marks::ExpandMark::None)
        .unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.unmark(&text, "bold", 3, 4, crate::marks::ExpandMark::None)
        .unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    rich_deliver(
        &mut s,
        doc.get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect(),
        &text,
    );
    rich_deliver(&mut s, vec![Input::Authorize(r)], &text);
    let rich =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(rich.marked("bold"), "abc");
    rich_deliver(&mut s, vec![Input::Invalidate(r)], &text);
    assert_eq!(
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()))
            .marked("bold"),
        "a"
    );
}
#[test]
fn structural_text_receipt_orders_and_batches_replay_formatted_endpoints() {
    use itertools::Itertools;
    let (mut doc, text, h) = text_base("abcd");
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 2, 0, "XY").unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.mark(
        &text,
        crate::marks::Mark::new("bold".into(), true, 1, 5),
        crate::marks::ExpandMark::Both,
    )
    .unwrap();
    tx.splice_text(&text, 3, 0, "Q").unwrap();
    let b = tx.commit().0.unwrap();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let events = [content(&doc, a), content(&doc, b), content(&doc, r)];
    let mut count = 0;
    for order in (0..3).permutations(3) {
        for partition in 0..4 {
            count += 1;
            let mut s = Session::new();
            rich_deliver(&mut s, vec![content(&doc, h), Input::Authorize(r)], &text);
            let mut seen = [false; 3];
            let mut group = Vec::new();
            let mut frozen = Vec::new();
            for (i, event) in order.iter().copied().enumerate() {
                seen[event] = true;
                group.push(events[event].clone());
                if i != 2 && partition & (1 << i) == 0 {
                    continue;
                }
                rich_deliver(&mut s, std::mem::take(&mut group), &text);
                let cap = s.capture();
                let rich = super::format_observer::RichText::read(
                    &s.doc,
                    &text,
                    Some(s.scope(&cap).unwrap()),
                );
                let expected = if seen[0] && seen[1] {
                    if seen[2] {
                        "abQcd"
                    } else {
                        "abXQYcd"
                    }
                } else if seen[0] && !seen[2] {
                    "abXYcd"
                } else {
                    "abcd"
                };
                assert_eq!(rich.plain(), expected);
                assert_eq!(
                    rich.marked("bold"),
                    if seen[0] && seen[1] {
                        if seen[2] {
                            "bQc"
                        } else {
                            "bXQYc"
                        }
                    } else {
                        ""
                    }
                );
                frozen.push((cap, rich));
            }
            for event in &events {
                let t = rich_deliver(&mut s, vec![event.clone()], &text);
                assert!(t.content.is_empty());
                assert!(t.status.is_empty());
            }
            for (cap, rich) in frozen {
                assert_eq!(
                    super::format_observer::RichText::read(
                        &s.doc,
                        &text,
                        Some(s.scope(&cap).unwrap())
                    ),
                    rich
                );
            }
        }
    }
    assert_eq!(count, 24);
}
#[test]
fn text_capture_recompiles_after_earlier_actor_and_rejects_encoding_mismatch() {
    let (mut doc, text, h) = text_base("abcd");
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 2, 0, "XY").unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    mark(&mut doc, &text, 1, 5);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let frozen = s.capture();
    let expected =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&frozen).unwrap()));
    let mut earlier = Automerge::new().with_actor(actor(1));
    let mut tx = earlier.transaction();
    tx.put(ROOT, "early", true).unwrap();
    let e = tx.commit().0.unwrap();
    rich_deliver(&mut s, vec![content(&earlier, e)], &text);
    for group in [vec![], vec![content(&doc, a)], vec![Input::Authorize(r)]] {
        let t = rich_deliver(&mut s, group, &text);
        assert!(t.content.is_empty());
        assert!(t.status.is_empty());
    }
    assert_eq!(
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&frozen).unwrap())),
        expected
    );
    let mut incompatible = s.clone();
    incompatible.doc = Automerge::new_with_encoding(crate::TextEncoding::Utf16CodeUnit);
    assert_eq!(
        incompatible.scope(&frozen).err().unwrap(),
        "incompatible capture text encoding"
    );
}
#[test]
fn ex10_list_counter_selection_has_no_self_diff_or_duplicate_increment() {
    let (mut doc, obj, _) = list_base();
    let mut tx = doc.transaction();
    tx.put(&obj, 0, crate::ScalarValue::counter(10)).unwrap();
    let h = tx.commit().0.unwrap();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.increment(&obj, 0, 100).unwrap();
    let a = tx.commit().0.unwrap();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.increment(&obj, 0, 5).unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    for group in [vec![], vec![content(&doc, a)]] {
        let t = deliver(&mut s, group);
        assert!(t.content.is_empty());
    }
    let cap = s.capture();
    assert_eq!(
        s.doc
            .get_all_for(&obj, 0, Some(s.scope(&cap).unwrap()))
            .unwrap()[0]
            .0
            .as_i64(),
        Some(15)
    );
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(
        s.doc
            .get_all_for(&obj, 0, Some(s.scope(&s.capture()).unwrap()))
            .unwrap()[0]
            .0
            .as_i64(),
        Some(115)
    );
}
#[test]
fn restored_text_splice_splits_at_unchanged_mark_boundary() {
    // Minimized generated trace: aXXXbcd, bold the middle X, insert Q after it.
    let (mut doc, text, h) = text_base("abcd");
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.splice_text(&text, 1, 0, "XXX").unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    mark(&mut doc, &text, 2, 3);
    let mut tx = doc.transaction();
    tx.splice_text(&text, 3, 0, "Q").unwrap();
    tx.commit();
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = Session::new();
    rich_deliver(
        &mut s,
        doc.get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect(),
        &text,
    );
    rich_deliver(&mut s, vec![Input::Authorize(r)], &text);
    rich_deliver(&mut s, vec![Input::Invalidate(r)], &text);
    let rich =
        super::format_observer::RichText::read(&s.doc, &text, Some(s.scope(&s.capture()).unwrap()));
    assert_eq!(rich.plain(), "aXXQXbcd");
    assert_eq!(rich.marked("bold"), "XQ");
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn model_formatted_survivors_keep_structural_range_and_replay(gap in 1usize..4, extra in 1usize..7, start_seed in 0usize..16, len_seed in 0usize..16) {
        let (mut doc,text,h)=text_base("abcd");doc.set_actor(actor(20));let inserted="X".repeat(extra);let mut tx=doc.transaction();tx.splice_text(&text,gap,0,&inserted).unwrap();tx.commit();
        let full_len=4+extra;let start=start_seed%full_len;let end=start+1+len_seed%(full_len-start);doc.set_actor(actor(40));mark(&mut doc,&text,start,end);
        let interior=start+1;let mut tx=doc.transaction();tx.splice_text(&text,interior,0,"Q").unwrap();tx.commit();
        let r=record_control(&mut doc,actor(30),b"alice",vec![h]).unwrap();let mut s=Session::new();rich_deliver(&mut s,doc.get_changes(&[]).iter().map(|c|content(&doc,c.hash())).collect(),&text);rich_deliver(&mut s,vec![Input::Authorize(r)],&text);
        // Independent linear fixture model: preserve original range memberships, then
        // remove X tokens without shifting the recorded range onto their neighbors.
        let mut tokens:Vec<_>="abcd".chars().map(|c|(c,false,false)).collect();tokens.splice(gap..gap,inserted.chars().map(|c|(c,true,false)));for token in &mut tokens[start..end]{token.2=true;}tokens.insert(interior,('Q',false,true));
        let plain:String=tokens.iter().filter(|t|!t.1).map(|t|t.0).collect();let bold:String=tokens.iter().filter(|t|!t.1&&t.2).map(|t|t.0).collect();
        let cap=s.capture();let rich=super::format_observer::RichText::read(&s.doc,&text,Some(s.scope(&cap).unwrap()));proptest::prop_assert_eq!(rich.plain(),plain);proptest::prop_assert_eq!(rich.marked("bold"),bold);
        rich_deliver(&mut s,vec![Input::Invalidate(r)],&text);proptest::prop_assert_eq!(super::format_observer::RichText::read(&s.doc,&text,Some(s.scope(&cap).unwrap())),rich);
    }
}
fn text_base(text: &str) -> (Automerge, ObjId, ChangeHash) {
    let mut doc = Automerge::new().with_actor(actor(10));
    let mut tx = doc.transaction();
    let obj = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&obj, 0, 0, text).unwrap();
    let h = tx.commit().0.unwrap();
    (doc, obj, h)
}
fn rich_deliver(s: &mut Session, inputs: Vec<Input>, obj: &ObjId) -> Transition {
    let t = deliver(s, inputs);
    let mut rich =
        super::format_observer::RichText::read(&s.doc, obj, Some(s.scope(&t.before).unwrap()));
    let path = s
        .doc
        .parents_for(obj, Some(s.scope(&t.before).unwrap()))
        .unwrap()
        .path();
    rich.apply_at_path(obj, path, &t.content, s.doc.text_encoding());
    let after =
        super::format_observer::RichText::read(&s.doc, obj, Some(s.scope(&t.after).unwrap()));
    assert_eq!(rich, after, "format replay patches: {:?}", t.content);
    t
}
fn mark(doc: &mut Automerge, obj: &ObjId, start: usize, end: usize) -> ChangeHash {
    let mut tx = doc.transaction();
    tx.mark(
        obj,
        crate::marks::Mark::new("bold".into(), true, start, end),
        crate::marks::ExpandMark::Both,
    )
    .unwrap();
    tx.commit().0.unwrap()
}
#[test]
fn ex12_ex13_structural_mark_ranges_over_excluded_text() {
    for (only_xy, q) in [(false, false), (true, false), (true, true)] {
        let (mut doc, obj, h) = text_base("abcd");
        doc.set_actor(actor(20));
        let mut tx = doc.transaction();
        tx.splice_text(&obj, 2, 0, "XY").unwrap();
        let a = tx.commit().0.unwrap();
        doc.set_actor(actor(40));
        let m = mark(
            &mut doc,
            &obj,
            if only_xy { 2 } else { 1 },
            if only_xy { 4 } else { 5 },
        );
        assert_eq!(doc.get_change_by_hash(&m).unwrap().len(), 2);
        if q {
            let mut tx = doc.transaction();
            tx.splice_text(&obj, 3, 0, "Q").unwrap();
            tx.commit();
        }
        let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
        let mut s = Session::new();
        let inputs = doc
            .get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect();
        rich_deliver(&mut s, inputs, &obj);
        let before = s.capture();
        rich_deliver(&mut s, vec![Input::Authorize(r)], &obj);
        let hidden = s.capture();
        assert_eq!(hidden.eligibility[&a], Eligibility::Excluded);
        assert_eq!(hidden.eligibility[&m], Eligibility::Eligible);
        let rich =
            super::format_observer::RichText::read(&s.doc, &obj, Some(s.scope(&hidden).unwrap()));
        assert_eq!(rich.plain(), if q { "abQcd" } else { "abcd" });
        assert_eq!(
            rich.marked("bold"),
            if q {
                "Q"
            } else if only_xy {
                ""
            } else {
                "bc"
            }
        );
        assert_eq!(
            s.doc
                .text_for(&obj, Some(s.scope(&hidden).unwrap()))
                .unwrap(),
            rich.plain()
        );
        let marks = s
            .doc
            .marks_for(&obj, Some(s.scope(&hidden).unwrap()))
            .unwrap();
        assert_eq!(marks.len(), if only_xy && !q { 0 } else { 1 });
        rich_deliver(&mut s, vec![Input::Invalidate(r)], &obj);
        assert_eq!(
            super::format_observer::RichText::read(&s.doc, &obj, Some(s.scope(&before).unwrap()))
                .plain(),
            if q { "abXQYcd" } else { "abXYcd" }
        );
        let restored = s.reconstruct().unwrap();
        assert_eq!(
            super::format_observer::RichText::read(
                &restored.doc,
                &obj,
                Some(restored.scope(&hidden).unwrap())
            ),
            rich
        );
    }
}
#[test]
fn ex14_excluded_mark_and_unmark_replay_formatting_not_just_text() {
    for unmark in [false, true] {
        let (mut doc, obj, mut h) = text_base("ab");
        if unmark {
            h = mark(&mut doc, &obj, 0, 2);
        }
        doc.set_actor(actor(20));
        let a = if unmark {
            let mut tx = doc.transaction();
            tx.unmark(&obj, "bold", 0, 2, crate::marks::ExpandMark::Both)
                .unwrap();
            tx.commit().0.unwrap()
        } else {
            mark(&mut doc, &obj, 0, 2)
        };
        doc.set_actor(actor(40));
        let mut tx = doc.transaction();
        tx.splice_text(&obj, 1, 0, "X").unwrap();
        tx.commit();
        let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
        let mut s = Session::new();
        rich_deliver(
            &mut s,
            doc.get_changes(&[])
                .iter()
                .map(|c| content(&doc, c.hash()))
                .collect(),
            &obj,
        );
        let t = rich_deliver(&mut s, vec![Input::Authorize(r)], &obj);
        assert!(t
            .content
            .iter()
            .any(|p| matches!(p.action, crate::PatchAction::Mark { .. })));
        assert_eq!(s.capture().eligibility[&a], Eligibility::Excluded);
        let rich = super::format_observer::RichText::read(
            &s.doc,
            &obj,
            Some(s.scope(&s.capture()).unwrap()),
        );
        assert_eq!(rich.plain(), "aXb");
        assert_eq!(rich.marked("bold"), if unmark { "aXb" } else { "" });
        rich_deliver(&mut s, vec![Input::Invalidate(r)], &obj);
        // Cold import of already-excluded marks must not leak formatting on splice patches.
        let mut cold = Session::new();
        let mut inputs: Vec<_> = doc
            .get_changes(&[])
            .iter()
            .map(|c| content(&doc, c.hash()))
            .collect();
        inputs.push(Input::Authorize(r));
        rich_deliver(&mut cold, inputs, &obj);
    }
}
fn element_ids(s: &Session, cap: &Capture, obj: &ObjId) -> Vec<ObjId> {
    s.doc
        .ops
        .top_ops(
            &s.doc.exid_to_obj(obj).unwrap().id,
            Some(s.scope(cap).unwrap()),
        )
        .map(|op| {
            let elem = op.elemid_or_key().elemid().unwrap();
            s.doc.id_to_exid(elem.0)
        })
        .collect()
}
fn list_base() -> (Automerge, ObjId, ChangeHash) {
    let mut doc = Automerge::new().with_actor(actor(10));
    let mut tx = doc.transaction();
    let obj = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.insert(&obj, 0, "L").unwrap();
    tx.insert(&obj, 1, "R").unwrap();
    let h = tx.commit().0.unwrap();
    (doc, obj, h)
}
#[test]
fn ex08_hidden_anchor_preserves_surviving_insertion_order_and_capture() {
    let (mut doc, obj, h) = list_base();
    let base_ids: Vec<_> = doc.list_range(&obj, ..).map(|v| v.id()).collect();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.insert(&obj, 1, "X").unwrap();
    let a = tx.commit().0.unwrap();
    let x = doc.get(&obj, 1).unwrap().unwrap().1;
    let mut concurrent = doc.fork();
    concurrent.set_actor(actor(50));
    let mut tx = concurrent.transaction();
    tx.insert(&obj, 2, "Z").unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.insert(&obj, 2, "Y").unwrap();
    let b = tx.commit().0.unwrap();
    assert_eq!(
        doc.get_change_by_hash(&b).unwrap().decode().operations[0]
            .key
            .to_opid()
            .unwrap()
            .to_string(),
        x.to_string()
    );
    let y = doc.get(&obj, 2).unwrap().unwrap().1;
    doc.merge(&mut concurrent).unwrap();
    let z = concurrent.get(&obj, 2).unwrap().unwrap().1;
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let hidden = s.capture();
    assert_eq!(hidden.eligibility[&a], Eligibility::Excluded);
    assert_eq!(
        list(&s, &hidden, &obj),
        vec![
            ("L".into(), base_ids[0].clone()),
            ("Z".into(), z),
            ("Y".into(), y),
            ("R".into(), base_ids[1].clone())
        ]
    );
    let values = list(&s, &hidden, &obj);
    let elements = element_ids(&s, &hidden, &obj);
    assert_eq!(
        elements,
        values.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>()
    );
    let mut early = Automerge::new().with_actor(actor(1));
    let mut tx = early.transaction();
    tx.put(ROOT, "early", true).unwrap();
    let e = tx.commit().0.unwrap();
    deliver(&mut s, vec![content(&early, e)]);
    assert_eq!(list(&s, &hidden, &obj), values);
    let (_, t) = s
        .edit(&s.capture(), actor(60), b"bob", |tx| {
            tx.put(&obj, 1, "selected-index")
        })
        .unwrap();
    replay(&s, &t);
    assert_eq!(element_ids(&s, &s.capture(), &obj), elements); // value ID changed, element did not
    assert_ne!(list(&s, &s.capture(), &obj)[1].1, elements[1]);
    assert_eq!(
        list(&s, &s.capture(), &obj)
            .iter()
            .map(|v| v.0.as_str())
            .collect::<Vec<_>>(),
        vec!["L", "selected-index", "Y", "R"]
    );
    deliver(&mut s, vec![Input::Invalidate(r)]);
    assert_eq!(list(&s, &hidden, &obj), values);
    assert_eq!(
        list(&s, &s.capture(), &obj)
            .iter()
            .map(|v| v.0.as_str())
            .collect::<Vec<_>>(),
        vec!["L", "X", "selected-index", "Y", "R"]
    );
    assert_eq!(
        s.reconstruct().unwrap().hydrate(&hidden),
        s.hydrate(&hidden)
    );
}
#[test]
fn ex09_replacement_uses_excluded_insertion_element_not_new_identity() {
    let (mut doc, obj, h) = list_base();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    tx.insert(&obj, 1, "X").unwrap();
    let a = tx.commit().0.unwrap();
    let x = doc.get(&obj, 1).unwrap().unwrap().1;
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    tx.put(&obj, 1, "Y").unwrap();
    let b = tx.commit().0.unwrap();
    let y = doc.get(&obj, 1).unwrap().unwrap().1;
    let op = doc.get_change_by_hash(&b).unwrap().decode().operations[0].clone();
    assert!(!op.insert);
    assert_eq!(op.key.to_opid().unwrap().to_string(), x.to_string());
    assert_eq!(op.pred.get(0).unwrap().to_string(), x.to_string());
    assert_ne!(x, y);
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let cap = s.capture();
    assert_eq!(cap.eligibility[&a], Eligibility::Excluded);
    assert_eq!(
        list(&s, &cap, &obj)
            .iter()
            .map(|v| v.0.as_str())
            .collect::<Vec<_>>(),
        vec!["L", "Y", "R"]
    );
    assert_eq!(list(&s, &cap, &obj)[1].1, y);
    assert_eq!(element_ids(&s, &cap, &obj)[1], x);
    assert_ne!(element_ids(&s, &cap, &obj)[1], list(&s, &cap, &obj)[1].1);
    let t = deliver(&mut s, vec![Input::Invalidate(r)]);
    assert!(t.content.is_empty());
}
#[test]
fn ex09_eligible_replacement_map_does_not_adopt_old_children() {
    let (mut doc, obj, h) = list_base();
    doc.set_actor(actor(20));
    let mut tx = doc.transaction();
    let old = tx.insert_object(&obj, 1, ObjType::Map).unwrap();
    tx.put(&old, "old", true).unwrap();
    tx.commit();
    doc.set_actor(actor(40));
    let mut tx = doc.transaction();
    let new = tx.put_object(&obj, 1, ObjType::Map).unwrap();
    tx.put(&new, "new", true).unwrap();
    let b = tx.commit().0.unwrap();
    let op = doc.get_change_by_hash(&b).unwrap().decode().operations[0].clone();
    assert!(!op.insert);
    assert_eq!(op.key.to_opid().unwrap().to_string(), old.to_string());
    let r = record_control(&mut doc, actor(30), b"alice", vec![h]).unwrap();
    let mut s = load(&doc, r);
    let cap = s.capture();
    let vals = s
        .doc
        .get_all_for(&obj, 1, Some(s.scope(&cap).unwrap()))
        .unwrap();
    assert_eq!(vals[0].1, new);
    assert!(s.candidates(&cap, &new, "old").is_empty());
    assert_eq!(s.candidates(&cap, &new, "new").len(), 1);
    let t = deliver(&mut s, vec![Input::Invalidate(r)]);
    assert!(t.content.is_empty());
}
