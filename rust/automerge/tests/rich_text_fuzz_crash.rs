use automerge::iter::Span;
use automerge::marks::{ExpandMark, Mark};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{
    hydrate, hydrate_map, hydrate_text, ActorId, AutoCommit, Automerge, ObjType, ReadDoc, ROOT,
};

fn commit_as(doc: &mut AutoCommit, actor: &[u8]) {
    doc.set_actor(ActorId::from(actor.to_vec()));
    doc.commit();
}

fn save_load(doc: &mut AutoCommit) {
    let before = doc.hydrate(&ROOT, None).unwrap();
    let bytes = doc.save();
    let loaded = AutoCommit::load(&bytes).unwrap();
    let after = loaded.hydrate(&ROOT, None).unwrap();
    assert_eq!(before, after);
    *doc = loaded;
}

#[test]
fn zero_width_unmark_on_empty_text_sync_from_fuzz_trace() {
    // Minimized from fuzz/corpus/trace/crashes/crash-00000000.amtrace. Syncing a new
    // change into a document containing a zero-width unmark on empty text used to panic
    // in BatchApply::apply when validating op order.
    let mut left = AutoCommit::new();
    let text = left.put_object(ROOT, "text", ObjType::Text).unwrap();
    left.unmark(&text, "color", 0, 0, ExpandMark::After)
        .unwrap();
    left.commit();

    let mut right = left.fork();
    right.put(ROOT, "x", 1).unwrap();
    right.commit();

    let mut left_state = sync::State::new();
    let mut right_state = sync::State::new();
    let message = left.sync().generate_sync_message(&mut left_state).unwrap();
    right
        .sync()
        .receive_sync_message(&mut right_state, message)
        .unwrap();
    let message = right
        .sync()
        .generate_sync_message(&mut right_state)
        .unwrap();
    left.sync()
        .receive_sync_message(&mut left_state, message)
        .unwrap();
}

#[test]
fn zero_width_mark_on_empty_text_from_fuzz_trace() {
    // Minimized from a fuzzing crash . This needs two committed changes: a zero-width unmark on an
    // empty text object, then a zero-width mark on the same text.
    let mut doc = AutoCommit::new();

    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.unmark(&text, "italic", 0, 0, ExpandMark::After)
        .unwrap();
    commit_as(&mut doc, &[1]);

    doc.mark(
        &text,
        Mark::new("link".to_string(), true, 0, 0),
        ExpandMark::Before,
    )
    .unwrap();
    commit_as(&mut doc, &[1]);

    save_load(&mut doc);
}

#[test]
fn zero_width_mark_does_not_leak_to_later_text_object_from_fuzz_trace() {
    // Minimized from fuzz/corpus/trace/crashes/crash-00000000.amtrace. The trace creates a
    // zero-width expanding mark on one empty text object, then inserts into the middle of a
    // different text object.
    let mut doc = AutoCommit::new();

    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    commit_as(&mut doc, &[0]);

    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 0, 0),
        ExpandMark::After,
    )
    .unwrap();
    commit_as(&mut doc, &[0]);

    let other_text = doc.put_object(ROOT, "o1", ObjType::Text).unwrap();
    commit_as(&mut doc, &[0]);

    doc.splice_text(&other_text, 0, 0, "tsxs").unwrap();
    commit_as(&mut doc, &[1]);

    doc.splice_text(&other_text, 2, 0, "bbqiolaf").unwrap();

    assert_eq!(
        doc.spans(&other_text).unwrap().collect::<Vec<_>>(),
        vec![Span::Text {
            text: "tsbbqiolafxs".to_string(),
            marks: None,
        }]
    );
}

#[test]
fn mark_within_single_multi_width_element_from_fuzz_trace() {
    // Minimized from a fuzz trace. A string inserted into a text object as a single
    // op is one element with a text width > 1 (here "hello world", width 11). A mark
    // whose [start, end) range falls entirely inside that one element has both its
    // begin and end anchors resolve to the same op-set position. The MarkEnd op used
    // to be spliced before its MarkBegin, which inverted the mark index (corrupting
    // its span-tree weight) and produced a document that failed to reload with
    // "mark end before begin". `TransactionInner::mark` now anchors the end after the
    // begin in that case, as the zero-width branch already did.
    let mut doc = AutoCommit::new();
    let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.insert(&text, 0, "hello world").unwrap();
    commit_as(&mut doc, &[0]);

    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 3, 8),
        ExpandMark::None,
    )
    .unwrap();
    commit_as(&mut doc, &[0]);

    // Must round-trip through save/load rather than being rejected as corrupt.
    Automerge::load(&doc.save()).unwrap();
    save_load(&mut doc);
}

#[test]
fn load_rejects_historical_zero_width_mark_end_before_begin() {
    let bytes = include_bytes!("fixtures/broken_zero_width_mark.automerge");
    let err = Automerge::load(bytes).unwrap_err();
    assert!(
        err.to_string().contains("invalid mark operation order"),
        "unexpected error: {err}"
    );
}

#[test]
fn rescue_recovers_snapshot_from_historical_zero_width_mark_order() {
    let bytes = include_bytes!("fixtures/broken_zero_width_mark.automerge");
    let value = Automerge::rescue(bytes).unwrap();

    assert_eq!(
        value,
        hydrate::Value::Map(hydrate_map! {
            "text" => hydrate_text! {""},
            "o1" => hydrate_text! {"tsxs"},
        })
    );
}
