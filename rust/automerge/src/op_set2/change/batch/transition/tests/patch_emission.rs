use super::*;

use crate::marks::{ExpandMark, Mark};
use crate::transaction::Transactable;
use crate::{Automerge, ObjType, PatchAction, PatchLog, Prop, ReadDoc, ROOT};

/// A document with two root fixtures whose IDs are `winner > lower`, plus
/// a list and a text object with one element each.
struct Fixture {
    doc: Automerge,
    winner: OpId,
    lower: OpId,
    list: ObjId,
    text: ObjId,
    text_elem: OpId,
}

fn opid(doc: &Automerge, exid: &crate::ObjId) -> OpId {
    doc.exid_to_opid(exid).unwrap()
}

fn fixture() -> Fixture {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    tx.put(ROOT, "lower", false).unwrap();
    tx.put(ROOT, "n", ScalarValue::counter(15)).unwrap();
    let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
    tx.insert(&list, 0, ScalarValue::counter(15)).unwrap();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "ab").unwrap();
    tx.commit();
    let lower = opid(&doc, &doc.get(ROOT, "lower").unwrap().unwrap().1);
    let winner = opid(&doc, &doc.get(ROOT, "n").unwrap().unwrap().1);
    assert!(winner > lower);
    let text_elem = opid(&doc, &doc.get(&text, 0).unwrap().unwrap().1);
    Fixture {
        list: opid(&doc, &list).into(),
        text: opid(&doc, &text).into(),
        doc,
        winner,
        lower,
        text_elem,
    }
}

fn patches(fx: &Fixture, log: &mut PatchLog) -> Vec<PatchAction> {
    fx.doc
        .make_patches(log)
        .into_iter()
        .map(|p| p.action)
        .collect()
}

fn map_patches(
    fx: &Fixture,
    before: CandidateSummary,
    after: CandidateSummary,
) -> Vec<PatchAction> {
    let mut log = PatchLog::active();
    ValueTransition::new(before, after).emit_map(ObjId::root(), "n", &mut log);
    patches(fx, &mut log)
}

fn list_patches(
    fx: &Fixture,
    before: CandidateSummary,
    after: CandidateSummary,
) -> Vec<PatchAction> {
    let mut log = PatchLog::active();
    ValueTransition::new(before, after).emit_sequence(
        fx.list,
        0,
        SequenceType::List,
        fx.doc.text_encoding(),
        &RichTextDiff::default(),
        &mut log,
    );
    patches(fx, &mut log)
}

fn text_patches(
    fx: &Fixture,
    before: CandidateSummary,
    after: CandidateSummary,
    marks: &RichTextDiff<'_>,
) -> Vec<PatchAction> {
    let mut log = PatchLog::active();
    ValueTransition::new(before, after).emit_sequence(
        fx.text,
        0,
        SequenceType::Text,
        fx.doc.text_encoding(),
        marks,
        &mut log,
    );
    patches(fx, &mut log)
}

fn one(mut patches: Vec<PatchAction>) -> PatchAction {
    assert_eq!(patches.len(), 1, "expected exactly one patch: {patches:?}");
    patches.pop().unwrap()
}

fn summary(id: OpId, value: Value) -> CandidateSummary {
    let mut summary = CandidateSummary::default();
    summary.add_existing(id, value);
    summary
}

/// Same winner at both endpoints, with surviving or newly incoming losers.
fn retained_endpoints(
    fx: &Fixture,
    old: &Value,
    new: &Value,
    before_count: usize,
    after_count: usize,
) -> (CandidateSummary, CandidateSummary) {
    let mut before = summary(fx.text_elem, old.clone());
    let mut after = summary(fx.text_elem, new.clone());
    for (n, loser) in [fx.lower, fx.winner].into_iter().enumerate() {
        if n + 1 < before_count {
            before.add_existing(loser, Value::scalar(false));
        }
        if n + 1 < after_count {
            if n + 1 < before_count {
                after.add_existing(loser, Value::scalar(false));
            } else {
                after.add_incoming(loser, Value::scalar(false));
            }
        }
    }
    let ValueTransition(Transition::WinnerUnchanged {
        before: b,
        after: a,
    }) = ValueTransition::new(before.clone(), after.clone())
    else {
        panic!("same ID must remain Retained");
    };
    assert_eq!(b.winner.id, fx.text_elem);
    assert_eq!(a.winner.id, fx.text_elem);
    assert_eq!(b.winner.value, *old);
    assert_eq!(a.winner.value, *new);
    assert_eq!(a.winner.origin, CandidateOrigin::Existing);
    assert_eq!(b.other_candidates + 1, before_count);
    assert_eq!(a.other_candidates + 1, after_count);
    (before, after)
}

fn put_map_of(action: &PatchAction) -> (&str, &crate::Value<'static>, bool) {
    let PatchAction::PutMap {
        key,
        value,
        conflict,
    } = action
    else {
        panic!("expected PutMap, got {action:?}");
    };
    (key, &value.0, *conflict)
}

fn put_seq_of(action: &PatchAction) -> (usize, &crate::Value<'static>, bool) {
    let PatchAction::PutSeq {
        index,
        value,
        conflict,
    } = action
    else {
        panic!("expected PutSeq, got {action:?}");
    };
    (*index, &value.0, *conflict)
}

fn insert_of(action: &PatchAction) -> (usize, Vec<(crate::Value<'static>, bool)>) {
    let PatchAction::Insert { index, values } = action else {
        panic!("expected Insert, got {action:?}");
    };
    (
        *index,
        values.iter().map(|(v, _, c)| (v.clone(), *c)).collect(),
    )
}

mod list;
mod map;
mod object_exposure;
mod retained_replay;
mod text;
