use super::*;

fn text_value(s: &str) -> Value {
    Value::scalar(s)
}

/// Marks after the batch only: bold appears across the element.
fn bold_appearing(fx: &Fixture) -> RichTextDiff<'static> {
    let mut marks = RichTextDiff::default();
    marks.after.mark_begin(
        fx.text_elem,
        crate::op_set2::types::MarkData {
            name: "bold".into(),
            value: crate::op_set2::types::ScalarValue::Boolean(true),
        },
    );
    marks
}

/// Bold both before and after: no delta, but a nonempty after format.
fn bold_unchanged(fx: &Fixture) -> RichTextDiff<'static> {
    let mut marks = bold_appearing(fx);
    marks.before.mark_begin(
        fx.text_elem,
        crate::op_set2::types::MarkData {
            name: "bold".into(),
            value: crate::op_set2::types::ScalarValue::Boolean(true),
        },
    );
    marks
}

fn splice_of(action: &PatchAction) -> (usize, String, Vec<(String, ScalarValue)>) {
    let PatchAction::SpliceText {
        index,
        value,
        marks,
    } = action
    else {
        panic!("expected SpliceText, got {action:?}");
    };
    let marks = marks
        .iter()
        .flat_map(|m| m.iter())
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    (*index, value.make_string(), marks)
}

#[test]
fn text_absent_and_disappeared_process_no_marks() {
    let fx = fixture();
    let marks = bold_appearing(&fx);
    assert!(text_patches(
        &fx,
        CandidateSummary::default(),
        CandidateSummary::default(),
        &marks
    )
    .is_empty());
    let before = summary(fx.text_elem, text_value("é"));
    let action = one(text_patches(
        &fx,
        before,
        CandidateSummary::default(),
        &marks,
    ));
    let expected = fx.doc.text_encoding().width("é");
    assert!(
        matches!(action, PatchAction::DeleteSeq { index: 0, length } if length == expected),
        "{action:?}"
    );
}

#[test]
fn text_replaced_deletes_before_width_and_splices_with_complete_after_format() {
    let fx = fixture();
    let before = summary(fx.winner, text_value("é"));
    let mut after = CandidateSummary::default();
    after.add_incoming(fx.text_elem, text_value("b"));
    let actions = text_patches(&fx, before, after, &bold_unchanged(&fx));
    assert_eq!(actions.len(), 2, "{actions:?}");
    let expected = fx.doc.text_encoding().width("é");
    assert!(
        matches!(&actions[0], PatchAction::DeleteSeq { index: 0, length } if *length == expected)
    );
    let (index, text, marks) = splice_of(&actions[1]);
    assert_eq!(index, 0);
    assert_eq!(text, "b");
    assert_eq!(
        marks,
        vec![("bold".to_string(), ScalarValue::Boolean(true))]
    );
}

#[test]
fn text_retained_conflict_clear_replaces_with_complete_after_format() {
    let fx = fixture();
    let mut before = summary(fx.text_elem, text_value("a"));
    before.add_existing(fx.lower, text_value("z"));
    let after = summary(fx.text_elem, text_value("a"));
    let actions = text_patches(&fx, before, after, &bold_unchanged(&fx));
    assert_eq!(actions.len(), 2, "{actions:?}");
    assert!(matches!(
        &actions[0],
        PatchAction::DeleteSeq {
            index: 0,
            length: 1
        }
    ));
    let (index, text, marks) = splice_of(&actions[1]);
    assert_eq!(index, 0);
    assert_eq!(text, "a");
    assert_eq!(
        marks,
        vec![("bold".to_string(), ScalarValue::Boolean(true))]
    );
}

#[test]
fn text_retained_unchanged_emits_only_mark_delta_over_retained_width() {
    let fx = fixture();
    let before = summary(fx.text_elem, text_value("é"));
    let after = summary(fx.text_elem, text_value("é"));
    let width = fx.doc.text_encoding().width("é");
    let action = one(text_patches(
        &fx,
        before.clone(),
        after.clone(),
        &bold_appearing(&fx),
    ));
    let PatchAction::Mark { marks } = action else {
        panic!("expected Mark, got {action:?}");
    };
    assert_eq!(marks.len(), 1);
    assert_eq!(
        (marks[0].start, marks[0].end, marks[0].name()),
        (0, width, "bold")
    );
    // No delta: nothing at all.
    assert!(text_patches(&fx, before, after, &bold_unchanged(&fx)).is_empty());
}

#[test]
fn text_retained_counter_increment_is_suppressed_but_conflict_and_marks_emit() {
    let fx = fixture();
    let before = summary(fx.text_elem, counter(10));
    let mut after = summary(fx.text_elem, counter(15));
    after.add_incoming(fx.lower, Value::scalar(false));
    let actions = text_patches(&fx, before, after, &bold_appearing(&fx));
    assert_eq!(actions.len(), 2, "{actions:?}");
    assert!(matches!(&actions[0], PatchAction::Mark { .. }));
    assert!(matches!(
        &actions[1],
        PatchAction::Conflict { prop: Prop::Seq(0) }
    ));
}

#[test]
fn text_retained_appearing_conflict_only_flags_without_marks() {
    let fx = fixture();
    let before = summary(fx.text_elem, text_value("a"));
    let mut after = summary(fx.text_elem, text_value("a"));
    after.add_incoming(fx.lower, text_value("z"));
    let action = one(text_patches(&fx, before, after, &RichTextDiff::default()));
    assert!(matches!(
        action,
        PatchAction::Conflict { prop: Prop::Seq(0) }
    ));
}

#[test]
fn text_appeared_object_inserts_without_marks() {
    let fx = fixture();
    let mut after = CandidateSummary::default();
    after.add_incoming(fx.text_elem, Value::map());
    let action = one(text_patches(
        &fx,
        CandidateSummary::default(),
        after,
        &bold_unchanged(&fx),
    ));
    let (index, values) = insert_of(&action);
    assert_eq!(index, 0);
    assert!(values[0].0.is_object());
}

/// Newly visible text needs its complete after-format, including marks
/// which were already active before the text appeared.
#[test]
fn text_appeared_scalar_splices_with_complete_after_format() {
    let fx = fixture();
    let mut after = CandidateSummary::default();
    after.add_incoming(fx.text_elem, text_value("q"));
    let action = one(text_patches(
        &fx,
        CandidateSummary::default(),
        after.clone(),
        &RichTextDiff::default(),
    ));
    let (index, text, marks) = splice_of(&action);
    assert_eq!((index, text.as_str()), (0, "q"));
    assert!(marks.is_empty());

    let action = one(text_patches(
        &fx,
        CandidateSummary::default(),
        after.clone(),
        &bold_appearing(&fx),
    ));
    let (_, _, marks) = splice_of(&action);
    assert_eq!(
        marks,
        vec![("bold".to_string(), ScalarValue::Boolean(true))]
    );

    // Unchanged bold must be supplied even though its delta is empty.
    let action = one(text_patches(
        &fx,
        CandidateSummary::default(),
        after,
        &bold_unchanged(&fx),
    ));
    let (_, _, marks) = splice_of(&action);
    assert_eq!(
        marks,
        vec![("bold".to_string(), ScalarValue::Boolean(true))]
    );
}

/// The `bold_unchanged` helper stands in for the parent's mark state. Check
/// it against a real mark operation: feeding the document's actual mark op
/// to both `RichTextDiff` state machines yields the same after format the
/// document reports, the same empty delta the helper produces, and the same
/// complete-format replacement splice from the encoder.
#[test]
fn fixture_marks_match_real_mark_op_state_and_encoding() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "ab").unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, 0, 2),
        ExpandMark::Both,
    )
    .unwrap();
    tx.commit();
    let text_id: ObjId = opid(&doc, &text).into();
    let elem = opid(&doc, &doc.get(&text, 0).unwrap().unwrap().1);

    // Real mark op, as the parent would process it for both endpoints.
    let mark_op = doc
        .ops()
        .iter_obj(&text_id)
        .find(|op| op.action == crate::op_set2::types::Action::Mark)
        .expect("fixture has a mark op");
    let mut real = RichTextDiff::default();
    real.before.process(mark_op.id, mark_op.action());
    real.after.process(mark_op.id, mark_op.action());

    let bold = crate::marks::MarkSet::from_iter([("bold".to_string(), ScalarValue::Boolean(true))]);
    let spans: Vec<_> = doc.spans(&text).unwrap().collect();
    assert!(
        matches!(&spans[..], [crate::iter::Span::Text { text, marks: Some(m) }]
        if text == "ab" && **m == bold),
        "{spans:?}"
    );
    assert_eq!(
        real.after.current().map(|m| (**m).clone()),
        Some(bold.clone())
    );
    assert!(
        real.current().export().is_none(),
        "unchanged marks have no delta"
    );

    // Helper on the same element agrees with the real state machines.
    let fx = Fixture {
        text: text_id,
        text_elem: elem,
        ..fixture()
    };
    let helper = bold_unchanged(&fx);
    assert_eq!(
        helper.after.current().map(|m| (**m).clone()),
        real.after.current().map(|m| (**m).clone())
    );
    assert!(helper.current().export().is_none());

    // Both drive the encoder to the same complete-format replacement.
    let encode = |marks: &RichTextDiff<'_>| {
        let mut before = summary(elem, text_value("a"));
        before.add_existing(fx.lower, text_value("z"));
        let after = summary(elem, text_value("a"));
        let mut log = PatchLog::active();
        ValueTransition::new(before, after).emit_sequence(
            text_id,
            0,
            SequenceType::Text,
            doc.text_encoding(),
            marks,
            &mut log,
        );
        let actions: Vec<_> = doc
            .make_patches(&mut log)
            .into_iter()
            .map(|p| p.action)
            .collect();
        assert_eq!(actions.len(), 2, "{actions:?}");
        splice_of(&actions[1])
    };
    let expected = (
        0,
        "a".to_string(),
        vec![("bold".to_string(), ScalarValue::Boolean(true))],
    );
    assert_eq!(encode(&real), expected);
    assert_eq!(encode(&helper), expected);
}
