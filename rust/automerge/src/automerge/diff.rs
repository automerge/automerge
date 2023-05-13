use itertools::Itertools;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::ops::RangeBounds;

use crate::{
    exid::ExId,
    iter::{Keys, ListRange, MapRange, Values},
    marks::{Mark, MarkStateMachine},
    types::{Clock, ListEncoding, MarkData, ObjId, Op, Prop, ScalarValue},
    value::Value,
    Automerge, AutomergeError, ChangeHash, Cursor, ObjType, OpObserver, OpType, ReadDoc,
};

#[derive(Clone, Debug)]
struct Winner<'a> {
    op: &'a Op,
    clock: &'a Clock,
    cross_visible: bool,
    conflict: bool,
}

impl<'a> Deref for Winner<'a> {
    type Target = Op;

    fn deref(&self) -> &'a Self::Target {
        self.op
    }
}

fn process<'a, T: Iterator<Item = &'a Op>>(
    ops: T,
    before: &'a Clock,
    after: &'a Clock,
) -> Option<Patch<'a>> {
    let mut before_op = None;
    let mut after_op = None;

    for op in ops {
        let predates_before = op.predates(before);
        let predates_after = op.predates(after);

        if predates_before && !op.was_deleted_before(before) {
            push_top(&mut before_op, op, predates_after, before);
        }

        if predates_after && !op.was_deleted_before(after) {
            push_top(&mut after_op, op, predates_before, after);
        }
    }
    resolve(before_op, after_op)
}

fn push_top<'a>(top: &mut Option<Winner<'a>>, op: &'a Op, cross_visible: bool, clock: &'a Clock) {
    match &op.action {
        OpType::Increment(_) => {} // can ignore - info captured inside Counter
        _ => {
            top.replace(Winner {
                op,
                clock,
                cross_visible,
                conflict: top.is_some(),
            });
        }
    }
}

fn resolve<'a>(before: Option<Winner<'a>>, after: Option<Winner<'a>>) -> Option<Patch<'a>> {
    match (before, after) {
        (None, Some(after)) if after.is_mark() => Some(Patch::Mark(after, MarkType::Add)),
        (None, Some(after)) => Some(Patch::New(after)),
        (Some(before), None) if before.is_mark() => Some(Patch::Mark(before, MarkType::Del)),
        (Some(before), None) => Some(Patch::Delete(before)),
        (Some(_), Some(after)) if after.is_mark() => Some(Patch::Mark(after, MarkType::Old)),
        (Some(before), Some(after)) if before.op.id == after.op.id => {
            Some(Patch::Old { before, after })
        }
        (Some(before), Some(after)) if before.op.id != after.op.id => {
            Some(Patch::Update { before, after })
        }
        _ => None,
    }
}

#[derive(Debug, Copy, Clone)]
enum MarkType {
    Add,
    Old,
    Del,
}

#[derive(Debug, Clone)]
enum Patch<'a> {
    New(Winner<'a>),
    Old {
        before: Winner<'a>,
        after: Winner<'a>,
    },
    Update {
        before: Winner<'a>,
        after: Winner<'a>,
    },
    Delete(Winner<'a>),
    Mark(Winner<'a>, MarkType),
}

impl<'a> Patch<'a> {
    fn op(&'a self) -> &'a Op {
        match self {
            Patch::New(op) => op,
            Patch::Update { after, .. } => after,
            Patch::Old { after, .. } => after,
            Patch::Delete(op) => op,
            Patch::Mark(op, _) => op,
        }
    }
}

pub(crate) fn observe_diff<O: OpObserver>(
    doc: &Automerge,
    before_heads: &[ChangeHash],
    after_heads: &[ChangeHash],
    observer: &mut O,
) {
    let before = doc.clock_at(before_heads);
    let after = doc.clock_at(after_heads);
    // FIXME - this fork is expensive
    // we really need a Doc::At object to make this cheap and easy
    // this is critical to keep paths accurate when rendering patches
    let doc_at_after = ReadDocAt {
        doc,
        heads: after_heads,
    }; //doc.fork_at(after_heads).unwrap();
    for (obj, typ, ops) in doc.ops().iter_objs() {
        let ops_by_key = ops.group_by(|o| o.elemid_or_key());
        let diffs = ops_by_key
            .into_iter()
            .filter_map(|(_key, key_ops)| process(key_ops, &before, &after));

        if typ == ObjType::Text && !observer.text_as_seq() {
            observe_text_diff(doc_at_after, observer, obj, diffs)
        } else if typ.is_sequence() {
            observe_list_diff(doc_at_after, observer, obj, diffs);
        } else {
            observe_map_diff(doc_at_after, observer, obj, diffs);
        }
    }
}

fn observe_list_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: ReadDocAt<'_, '_>,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    let mut marks = MarkDiff::default();
    let exid = doc.as_ref().id_to_exid(obj.0);
    patches.fold(0, |index, patch| match patch {
        Patch::Mark(op, mark_type) => {
            marks.process(index, mark_type, op.op, doc.as_ref());
            index
        }
        Patch::New(op) => {
            observer.insert(
                &doc,
                exid.clone(),
                index,
                doc.as_ref().export_value(&op, Some(op.clock)),
                op.conflict,
            );
            index + 1
        }
        Patch::Update { before, after } => {
            let exid = exid.clone();
            let prop = index.into();
            let value = doc.as_ref().export_value(&after, Some(after.clock));
            let conflict = !before.conflict && after.conflict;
            if after.cross_visible {
                observer.expose(&doc, exid, prop, value, conflict);
            } else {
                observer.put(&doc, exid, prop, value, conflict);
            }
            index + 1
        }
        Patch::Old { before, after } => {
            if !before.conflict && after.conflict {
                observer.flag_conflict(&doc, exid.clone(), index.into());
            }
            if let Some(n) = get_inc(&before, &after) {
                observer.increment(
                    &doc,
                    exid.clone(),
                    index.into(),
                    (n, doc.as_ref().id_to_exid(after.id)),
                );
            }
            index + 1
        }
        Patch::Delete(_) => {
            observer.delete_seq(&doc, exid.clone(), index, 1);
            index
        }
    });
    if let Some(m) = marks.finish() {
        observer.mark(&doc, exid, m.into_iter());
    }
}

fn observe_text_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: ReadDocAt<'_, '_>,
    observer: &mut O,
    obj: &ObjId,
    patches: I,
) {
    let mut marks = MarkDiff::default();
    let exid = doc.as_ref().id_to_exid(obj.0);
    let encoding = ListEncoding::Text;
    patches.fold(0, |index, patch| match &patch {
        Patch::Mark(op, mark_type) => {
            marks.process(index, *mark_type, op.op, doc.as_ref());
            index
        }
        Patch::New(op) => {
            observer.splice_text(&doc, exid.clone(), index, op.to_str());
            index + op.width(encoding)
        }
        Patch::Update { before, after } => {
            observer.delete_seq(&doc, exid.clone(), index, before.width(encoding));
            observer.splice_text(&doc, exid.clone(), index, after.to_str());
            index + after.width(encoding)
        }
        Patch::Old { after, .. } => index + after.width(encoding),
        Patch::Delete(before) => {
            observer.delete_seq(&doc, exid.clone(), index, before.width(encoding));
            index
        }
    });
    if let Some(m) = marks.finish() {
        observer.mark(&doc, exid, m.into_iter());
    }
}

fn observe_map_diff<'a, I: Iterator<Item = Patch<'a>>, O: OpObserver>(
    doc: ReadDocAt<'_, '_>,
    observer: &mut O,
    obj: &ObjId,
    diffs: I,
) {
    let exid = doc.as_ref().id_to_exid(obj.0);
    diffs
        .filter_map(|patch| Some((get_prop(doc.doc, patch.op())?, patch)))
        .for_each(|(prop, patch)| match patch {
            Patch::New(op) => observer.put(
                &doc,
                exid.clone(),
                prop.into(),
                doc.as_ref().export_value(&op, Some(op.clock)),
                op.conflict,
            ),
            Patch::Update { before, after } => {
                let exid = exid.clone();
                let prop = prop.into();
                let value = doc.as_ref().export_value(&after, Some(after.clock));
                let conflict = !before.conflict && after.conflict;
                if after.cross_visible {
                    observer.expose(&doc, exid, prop, value, conflict);
                } else {
                    observer.put(&doc, exid, prop, value, conflict);
                }
            }
            Patch::Old { before, after } => {
                if !before.conflict && after.conflict {
                    observer.flag_conflict(&doc, exid.clone(), prop.into());
                }
                if let Some(n) = get_inc(&before, &after) {
                    observer.increment(
                        &doc,
                        exid.clone(),
                        prop.into(),
                        (n, doc.as_ref().id_to_exid(after.id)),
                    );
                }
            }
            Patch::Delete(_) => observer.delete_map(&doc, exid.clone(), prop),
            Patch::Mark(_, _) => {}
        });
}

fn get_prop<'a>(doc: &'a Automerge, op: &Op) -> Option<&'a str> {
    Some(doc.ops().m.props.safe_get(op.key.prop_index()?)?)
}

fn get_inc(before: &Winner<'_>, after: &Winner<'_>) -> Option<i64> {
    if let (Some(ScalarValue::Counter(before_c)), Some(ScalarValue::Counter(after_c))) =
        (before.scalar_value(), after.scalar_value())
    {
        let n = after_c.value_at(after.clock) - before_c.value_at(before.clock);
        if n != 0 {
            return Some(n);
        }
    }
    None
}

// this implementation of MarkDiff creates two sets of marks - before and then after
// and then compares them to generate a diff
// this has a O(n2) performance vs the number of marks which isn't ideal
// im confident theres a single pass solution to this that is O(n) but I will
// leave it to a future person to figure out how to implement that :)

#[derive(Default, Debug, Clone, PartialEq)]
struct MarkDiff<'a> {
    before: MarkStateMachine<'a>,
    after: MarkStateMachine<'a>,
    before_marks: Vec<Mark<'a>>,
    after_marks: Vec<Mark<'a>>,
}

impl<'a> MarkDiff<'a> {
    fn process(&mut self, index: usize, mark_type: MarkType, op: &'a Op, doc: &'a Automerge) {
        match mark_type {
            MarkType::Add => self.add(index, op, doc),
            MarkType::Old => self.before(index, op, doc),
            MarkType::Del => self.del(index, op, doc),
        }
    }

    fn add(&mut self, index: usize, op: &'a Op, doc: &'a Automerge) {
        let mark = match &op.action {
            OpType::MarkBegin(_, data) => self.after.mark_begin(op.id, index, data, doc),
            OpType::MarkEnd(_) => self.after.mark_end(op.id, index, doc),
            _ => None,
        };
        if let Some(m) = mark {
            self.after_marks.push(m);
        }
    }

    fn before(&mut self, index: usize, op: &'a Op, doc: &'a Automerge) {
        let marks = match &op.action {
            OpType::MarkBegin(_, data) => (
                self.before.mark_begin(op.id, index, data, doc),
                self.after.mark_begin(op.id, index, data, doc),
            ),
            OpType::MarkEnd(_) => (
                self.before.mark_end(op.id, index, doc),
                self.after.mark_end(op.id, index, doc),
            ),
            _ => (None, None),
        };
        match marks {
            (Some(before), Some(after)) if before != after => {
                self.after_marks.push(after);
                self.before_marks.push(before)
            }
            (Some(before), None) => self.before_marks.push(before),
            (None, Some(after)) => self.after_marks.push(after),
            _ => {}
        }
    }

    fn del(&mut self, index: usize, op: &'a Op, doc: &'a Automerge) {
        let mark = match &op.action {
            OpType::MarkBegin(_, data) => self.before.mark_begin(op.id, index, data, doc),
            OpType::MarkEnd(_) => self.before.mark_end(op.id, index, doc),
            _ => None,
        };
        if let Some(m) = mark {
            self.before_marks.push(m);
        }
    }

    fn finish(&mut self) -> Option<Vec<Mark<'a>>> {
        let mut f = BTreeMap::new();
        'after_marks: for after in &mut self.after_marks {
            for before in &mut self.before_marks {
                if after.start > before.end {
                    continue; // 'after_marks; // too far - next mark
                }
                if after.data.name != before.data.name || before.start >= before.end {
                    continue;
                }
                if after.end >= before.start {
                    // before       ------------*
                    // after   ----------------*
                    mark(&mut f, after.start, before.start, after);
                    if after.end > before.start {
                        after.start = std::cmp::max(after.start, before.start);
                    } else {
                        continue 'after_marks;
                    }
                }
                if after.start >= before.start {
                    // before ------------*
                    // after    ---------*
                    if after.start > before.start {
                        before.start = std::cmp::min(before.start, after.start);
                        unmark(&mut f, before.start, after.start, before);
                    }
                    if after.end > before.end {
                        if after.data.value != before.data.value {
                            mark(&mut f, after.start, before.end, after);
                        }
                        after.start = before.end;
                        before.start = before.end;
                        continue;
                    } else {
                        if after.data.value != before.data.value {
                            mark(&mut f, after.start, after.end, after);
                        }
                        before.start = after.end;
                        continue 'after_marks;
                    }
                }
            }
            // mark after
            mark(&mut f, after.start, after.end, after);
        }
        for before in &self.before_marks {
            if before.start != before.end {
                unmark(&mut f, before.start, before.end, before);
            }
        }
        if !f.is_empty() {
            Some(f.into_values().flat_map(|v| v.into_iter()).collect())
        } else {
            None
        }
    }
}

fn unmark<'a>(
    finished: &mut BTreeMap<String, Vec<Mark<'a>>>,
    start: usize,
    end: usize,
    from: &Mark<'a>,
) {
    let f_vec = finished.entry(from.data.name.to_string()).or_default();
    if start < end {
        if let Some(last) = f_vec.last_mut() {
            if last.data.name == from.data.name && last.data.value.is_null() && last.end == start {
                last.end = end;
                return;
            }
        }
        f_vec.push(Mark {
            start,
            end,
            data: Cow::Owned(MarkData {
                name: from.data.name.clone(),
                value: ScalarValue::Null,
            }),
        });
    }
}

fn mark<'a>(
    finished: &mut BTreeMap<String, Vec<Mark<'a>>>,
    start: usize,
    end: usize,
    from: &Mark<'a>,
) {
    let f_vec = finished.entry(from.data.name.to_string()).or_default();
    if start < end {
        if let Some(last) = f_vec.last_mut() {
            if last.data == from.data && last.end == start {
                last.end = end;
                return;
            }
        }
        f_vec.push(Mark {
            start,
            end,
            data: from.data.clone(),
        });
    }
}

#[derive(Debug, Clone, Copy)]
struct ReadDocAt<'a, 'b> {
    doc: &'a Automerge,
    heads: &'b [ChangeHash],
}

impl<'a, 'b> AsRef<Automerge> for ReadDocAt<'a, 'b> {
    fn as_ref(&self) -> &Automerge {
        self.doc
    }
}

impl<'a, 'b> ReadDoc for ReadDocAt<'a, 'b> {
    fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_> {
        self.doc.keys_at(obj, self.heads)
    }

    fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Keys<'_> {
        self.doc.keys_at(obj, heads)
    }

    fn map_range<'c, O: AsRef<ExId>, R: RangeBounds<String> + 'c>(
        &'c self,
        obj: O,
        range: R,
    ) -> MapRange<'c, R> {
        self.doc.map_range_at(obj, range, self.heads)
    }

    fn map_range_at<'c, O: AsRef<ExId>, R: RangeBounds<String> + 'c>(
        &'c self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRange<'c, R> {
        self.doc.map_range_at(obj, range, heads)
    }

    fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        self.doc.list_range_at(obj, range, self.heads)
    }

    fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRange<'_, R> {
        self.doc.list_range_at(obj, range, heads)
    }

    fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        self.doc.values_at(obj, self.heads)
    }

    fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        self.doc.values_at(obj, heads)
    }

    fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        self.doc.length_at(obj, self.heads)
    }

    fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        self.doc.length_at(obj, heads)
    }

    fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Result<ObjType, AutomergeError> {
        self.doc.object_type(obj)
    }

    fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        self.doc.text_at(obj, self.heads)
    }

    fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        self.doc.text_at(obj, heads)
    }

    fn marks<O: AsRef<ExId>>(&self, obj: O) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc.marks_at(obj, self.heads)
    }

    fn marks_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<Vec<Mark<'_>>, AutomergeError> {
        self.doc.marks_at(obj, heads)
    }

    fn get_cursor<O: AsRef<ExId>>(
        &self,
        obj: O,
        position: usize,
        at: Option<&[ChangeHash]>,
    ) -> Result<Cursor, AutomergeError> {
        self.doc.get_cursor(obj, position, at)
    }

    fn get_cursor_position<O: AsRef<ExId>>(
        &self,
        obj: O,
        cursor: &Cursor,
        at: Option<&[ChangeHash]>,
    ) -> Result<usize, AutomergeError> {
        self.doc.get_cursor_position(obj, cursor, at)
    }

    fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_at(obj, prop, self.heads)
    }

    fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_at(obj, prop, heads)
    }

    fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_all_at(obj, prop, self.heads)
    }

    fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        self.doc.get_all_at(obj, prop, heads)
    }

    fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<crate::Parents<'_>, AutomergeError> {
        self.doc.parents_at(obj, self.heads)
    }

    fn parents_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<crate::Parents<'_>, AutomergeError> {
        self.doc.parents_at(obj, heads)
    }

    fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.doc.get_missing_deps(heads)
    }

    fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&crate::Change> {
        self.doc.get_change_by_hash(hash)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        marks::Mark, op_observer::HasPatches, transaction::Observed, transaction::Transactable,
        types::MarkData, AutoCommitWithObs, ObjType, Patch, PatchAction, Prop, ScalarValue, Value,
        VecOpObserver, ROOT,
    };
    use itertools::Itertools;

    #[derive(Debug, Clone, PartialEq)]
    struct ObservedPatch {
        action: ObservedAction,
        path: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ObservedAction {
        PutMap {
            value: Value<'static>,
            conflict: bool,
        },
        PutSeq {
            value: Value<'static>,
            conflict: bool,
        },
        Insert {
            values: Vec<Value<'static>>,
            conflict: bool,
        },
        DelMap,
        DelSeq,
        Increment(i64),
        SpliceText(String),
        Mark(Vec<ObservedMark>),
    }

    #[derive(Debug, Clone, PartialEq)]
    struct ObservedMark {
        start: usize,
        end: usize,
        name: String,
        value: ScalarValue,
    }

    fn ex_path_and<I: Iterator<Item = Prop>, V: Into<Prop>>(props: I, val: V) -> String {
        format!("/{}", props.chain(Some(val.into())).join("/"))
    }

    impl From<&Patch> for ObservedPatch {
        fn from(patch: &Patch) -> Self {
            let path = patch.path.iter().map(|(_, prop)| prop).cloned();
            match patch.action.clone() {
                PatchAction::PutMap {
                    key,
                    value,
                    conflict,
                    ..
                } => ObservedPatch {
                    action: ObservedAction::PutMap {
                        value: value.0,
                        conflict,
                    },
                    path: ex_path_and(path, key),
                },
                PatchAction::PutSeq {
                    index,
                    value,
                    conflict,
                    ..
                } => ObservedPatch {
                    action: ObservedAction::PutSeq {
                        value: value.0,
                        conflict,
                    },
                    path: ex_path_and(path, index),
                },
                PatchAction::DeleteMap { key } => ObservedPatch {
                    action: ObservedAction::DelMap,
                    path: ex_path_and(path, key),
                },
                PatchAction::DeleteSeq { index, .. } => ObservedPatch {
                    action: ObservedAction::DelSeq,
                    path: ex_path_and(path, index),
                },
                PatchAction::Increment { prop, value } => ObservedPatch {
                    action: ObservedAction::Increment(value),
                    path: ex_path_and(path, prop),
                },
                PatchAction::Insert {
                    index,
                    values,
                    conflict,
                } => ObservedPatch {
                    action: ObservedAction::Insert {
                        values: values.into_iter().map(|(v, _)| v.clone()).collect(),
                        conflict,
                    },
                    path: ex_path_and(path, index),
                },
                PatchAction::SpliceText { index, value } => ObservedPatch {
                    action: ObservedAction::SpliceText(value.make_string()),
                    path: ex_path_and(path, index),
                },
                PatchAction::Mark { marks } => ObservedPatch {
                    action: ObservedAction::Mark(
                        marks
                            .into_iter()
                            .map(|Mark { start, end, data }| {
                                let MarkData { name, value } = data.as_ref();
                                ObservedMark {
                                    start,
                                    end,
                                    name: name.to_string(),
                                    value: value.clone(),
                                }
                            })
                            .collect(),
                    ),
                    path: format!("/{}", path.clone().join("/")),
                },
            }
        }
    }

    fn exp(patches: Vec<Patch>) -> Vec<ObservedPatch> {
        patches.iter().map(|p| p.into()).collect()
    }

    #[test]
    fn basic_diff_map_put1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.put(ROOT, "key", "value2c").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: "value2c".into(),
                    conflict: false,
                },
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: "v2_value2c".into(),
                    conflict: true,
                },
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_with_del() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key1", "value1").unwrap();
        doc1.put(ROOT, "key2", "value2").unwrap();
        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();

        doc2.put(ROOT, "key1", "doc2_value2").unwrap();
        doc2.delete(ROOT, "key2").unwrap();

        doc1.delete(ROOT, "key1").unwrap();
        doc1.put(ROOT, "key2", "doc1_value2").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/key1".into(),
                    action: ObservedAction::PutMap {
                        value: "doc2_value2".into(),
                        conflict: false,
                    },
                },
                ObservedPatch {
                    path: "/key2".into(),
                    action: ObservedAction::PutMap {
                        value: "doc1_value2".into(),
                        conflict: false,
                    },
                },
            ]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_old_value() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();

        let mut doc2 = doc1.fork();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        let heads1 = doc1.get_heads();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: "v2_value2c".into(),
                    conflict: true,
                },
            }]
        );
    }

    #[test]
    fn basic_diff_map_put_conflict_old_value_and_del() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "value1").unwrap();

        let mut doc2 = doc1.fork();

        doc1.put(ROOT, "key", "v1_value2a").unwrap();

        let heads1 = doc1.get_heads();

        doc2.put(ROOT, "key", "v2_value2a").unwrap();
        doc2.put(ROOT, "key", "v2_value2b").unwrap();
        doc2.put(ROOT, "key", "v2_value2c").unwrap();
        doc2.delete(ROOT, "key").unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads2 = doc1.get_heads();
        doc1.put(ROOT, "key", "value3").unwrap();
        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(exp(patches), vec![],);
    }

    #[test]
    fn basic_diff_map_del1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.delete(ROOT, "key").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::DelMap,
            }]
        );
    }

    #[test]
    fn basic_diff_map_del2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.delete(ROOT, "key").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::DelMap,
            }]
        );
    }

    #[test]
    fn basic_diff_map_del3() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", "value1").unwrap();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", "value2a").unwrap();
        doc.put(ROOT, "key", "value2b").unwrap();
        doc.delete(ROOT, "key").unwrap();
        doc.put(ROOT, "key", "value2c").unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "value3").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: "value2c".into(),
                    conflict: false,
                },
            }]
        );
    }

    #[test]
    fn basic_diff_map_counter1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc.put(ROOT, "key", ScalarValue::counter(10)).unwrap();
        let heads1 = doc.get_heads();
        doc.increment(ROOT, "key", 3).unwrap();
        doc.increment(ROOT, "key", 4).unwrap();
        doc.increment(ROOT, "key", 5).unwrap();
        let heads2 = doc.get_heads();
        doc.put(ROOT, "key", "overwrite").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::Increment(12),
            }]
        );
    }

    #[test]
    fn basic_diff_map_counter2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let heads1 = doc.get_heads();
        doc.put(ROOT, "key", ScalarValue::counter(10)).unwrap();
        doc.increment(ROOT, "key", 3).unwrap();
        doc.increment(ROOT, "key", 4).unwrap();
        let heads2 = doc.get_heads();
        doc.increment(ROOT, "key", 5).unwrap();
        doc.put(ROOT, "key", "overwrite").unwrap();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: ScalarValue::counter(17).into(),
                    conflict: false,
                },
            }]
        );
    }

    #[test]
    fn basic_diff_list_insert1() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 10).unwrap();
        doc.insert(&list, 1, 20).unwrap();
        doc.insert(&list, 2, 30).unwrap();
        doc.insert(&list, 3, 40).unwrap();
        let heads1 = doc.get_heads();
        doc.insert(&list, 1, 25).unwrap();
        doc.insert(&list, 3, 35).unwrap();
        doc.delete(&list, 0).unwrap();
        let heads2 = doc.get_heads();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();
        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/list/0".into(),
                    action: ObservedAction::DelSeq,
                },
                ObservedPatch {
                    path: "/list/0".into(),
                    action: ObservedAction::Insert {
                        values: vec![25.into()],
                        conflict: false
                    },
                },
                ObservedPatch {
                    path: "/list/2".into(),
                    action: ObservedAction::Insert {
                        values: vec![35.into()],
                        conflict: false
                    },
                },
            ]
        );
    }

    #[test]
    fn basic_diff_list_insert2() {
        let mut doc = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, 10).unwrap();
        doc.insert(&list, 1, 20).unwrap();
        doc.insert(&list, 2, 30).unwrap();
        doc.insert(&list, 3, 40).unwrap();
        let heads1 = doc.get_heads();
        doc.insert(&list, 1, 25).unwrap();
        doc.insert(&list, 1, 26).unwrap();
        doc.insert(&list, 1, 27).unwrap();
        doc.insert(&list, 1, 28).unwrap();
        let heads2 = doc.get_heads();
        let patches = doc.diff(&heads1, &heads2).unwrap().take_patches();
        assert_eq!(
            exp(patches),
            vec![ObservedPatch {
                path: "/list/1".into(),
                action: ObservedAction::Insert {
                    values: vec![28.into(), 27.into(), 26.into(), 25.into(),],
                    conflict: false,
                }
            },]
        );
    }

    #[test]
    fn diff_list_concurrent_update() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.insert(&list, 2, 35).unwrap();
        doc2.put(&list, 2, 36).unwrap();
        doc2.put(&list, 1, 21).unwrap();

        doc3.put(&list, 1, 19).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        assert_eq!(
            exp(patches),
            vec![
                ObservedPatch {
                    path: "/list/1".into(),
                    action: ObservedAction::PutSeq {
                        value: 21.into(),
                        conflict: true,
                    },
                },
                ObservedPatch {
                    path: "/list/2".into(),
                    action: ObservedAction::Insert {
                        values: vec![36.into()],
                        conflict: false
                    },
                },
            ]
        );
    }

    #[test]
    fn diff_list_interleaved_concurrent_counters() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();
        doc1.insert(&list, 5, 60).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        // doc 2 makes a conflicting counter and incrments it
        doc2.put(&list, 2, ScalarValue::counter(10)).unwrap();
        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 2, 1).unwrap();

        doc2.put(&list, 3, ScalarValue::counter(100)).unwrap();
        doc2.increment(&list, 3, 10).unwrap();
        doc2.increment(&list, 3, 10).unwrap();
        doc2.increment(&list, 3, 10).unwrap();

        doc2.increment(&list, 2, 1).unwrap();
        doc2.increment(&list, 3, 10).unwrap();

        // doc 3 does the same in the opposite order so we'll have reversed winners

        doc3.put(&list, 3, ScalarValue::counter(101)).unwrap();
        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 3, 11).unwrap();

        doc3.put(&list, 2, ScalarValue::counter(11)).unwrap();
        doc3.increment(&list, 2, 2).unwrap();
        doc3.increment(&list, 2, 2).unwrap();
        doc3.increment(&list, 2, 2).unwrap();

        doc3.increment(&list, 3, 11).unwrap();
        doc3.increment(&list, 2, 2).unwrap();

        doc3.put(&list, 4, ScalarValue::counter(99)).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.increment(&list, 4, 1).unwrap();
        doc3.delete(&list, 4).unwrap();

        doc3.insert(&list, 5, ScalarValue::counter(199)).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.increment(&list, 5, 3).unwrap();
        doc3.delete(&list, 5).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        doc1.put(&list, 2, 0).unwrap();
        doc1.put(&list, 3, 0).unwrap();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();

        let exp = exp(patches);
        assert_eq!(
            exp.get(0),
            Some(ObservedPatch {
                path: "/list/2".into(),
                action: ObservedAction::PutSeq {
                    value: ScalarValue::counter(19).into(),
                    conflict: true
                },
            })
            .as_ref()
        );
        assert_eq!(
            exp.get(1),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: ObservedAction::PutSeq {
                    value: ScalarValue::counter(140).into(),
                    conflict: true,
                },
            })
            .as_ref()
        );
        assert_eq!(
            exp.get(2),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: ObservedAction::DelSeq,
            })
            .as_ref()
        );
        assert_eq!(exp.get(3), None);
    }

    #[test]
    fn diff_of_lists_with_concurrent_deletes_and_puts() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();

        doc1.insert(&list, 0, 10).unwrap();
        doc1.insert(&list, 1, 20).unwrap();
        doc1.insert(&list, 2, 30).unwrap();
        doc1.insert(&list, 3, 40).unwrap();
        doc1.insert(&list, 4, 50).unwrap();
        doc1.insert(&list, 5, 60).unwrap();

        let heads1 = doc1.get_heads();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.put(&list, 3, "A").unwrap();
        doc2.put(&list, 3, "B").unwrap();
        doc2.put(&list, 3, "C").unwrap();
        doc2.put(&list, 4, "!").unwrap();
        doc2.delete(&list, 4).unwrap();

        let heads1a = doc2.get_heads();

        doc3.put(&list, 3, "!").unwrap();
        doc3.delete(&list, 3).unwrap();
        doc3.put(&list, 3, "X").unwrap();
        doc3.put(&list, 3, "Y").unwrap();
        doc3.put(&list, 3, "Z").unwrap();

        let heads1b = doc3.get_heads();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2 = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: ObservedAction::PutSeq {
                    value: ScalarValue::Str("C".into()).into(),
                    conflict: false,
                },
            })
            .as_ref()
        );
        assert_eq!(
            exp1.get(1),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: ObservedAction::PutSeq {
                    value: ScalarValue::Str("Z".into()).into(),
                    conflict: false,
                },
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads1a, &heads2).unwrap().take_patches();
        let exp2 = exp(patches);
        assert_eq!(
            exp2.get(0),
            Some(ObservedPatch {
                path: "/list/4".into(),
                action: ObservedAction::Insert {
                    values: vec![ScalarValue::Str("Z".into()).into()],
                    conflict: false,
                },
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads1b, &heads2).unwrap().take_patches();
        let exp3 = exp(patches);
        assert_eq!(
            exp3.get(0),
            Some(ObservedPatch {
                path: "/list/3".into(),
                action: ObservedAction::Insert {
                    values: vec![ScalarValue::Str("C".into()).into()],
                    conflict: false,
                }
            })
            .as_ref()
        );
    }

    #[test]
    fn diff_counter_exposed() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        doc1.put(ROOT, "key", "x").unwrap();

        let mut doc2 = doc1.fork();
        let mut doc3 = doc1.fork();

        doc2.put(ROOT, "key", ScalarValue::counter(10)).unwrap();

        doc1.merge(&mut doc2).unwrap();

        let heads1 = doc1.get_heads();

        doc2.increment(ROOT, "key", 1).unwrap();
        doc2.increment(ROOT, "key", 1).unwrap();

        doc3.put(ROOT, "key", 1).unwrap();
        doc3.put(ROOT, "key", 2).unwrap();
        doc3.put(ROOT, "key", 3).unwrap();
        doc3.put(ROOT, "key", 4).unwrap();

        doc1.merge(&mut doc2).unwrap();
        doc1.merge(&mut doc3).unwrap();

        doc2.increment(ROOT, "key", 1).unwrap();
        doc2.increment(ROOT, "key", 1).unwrap();

        let heads2a = doc1.get_heads();

        doc3.delete(ROOT, "key").unwrap();
        doc1.merge(&mut doc3).unwrap();

        let heads2b = doc1.get_heads();

        let patches = doc1.diff(&heads1, &heads2a).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: ScalarValue::Int(4).into(),
                    conflict: true,
                },
            })
            .as_ref()
        );

        let patches = doc1.diff(&heads2a, &heads2b).unwrap().take_patches();
        let exp1 = exp(patches);
        assert_eq!(
            exp1.get(0),
            Some(ObservedPatch {
                path: "/key".into(),
                action: ObservedAction::PutMap {
                    value: ScalarValue::Counter(12.into()).into(),
                    conflict: false,
                },
            })
            .as_ref()
        );
    }

    #[test]
    fn simple_marks() {
        let mut doc1 = AutoCommitWithObs::<Observed<VecOpObserver>>::default();
        let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc1.splice_text(&text, 0, 0, "the quick fox jumps over the lazy dog")
            .unwrap();
        let heads1 = doc1.get_heads();
        doc1.mark(
            text,
            Mark::new("bold".into(), ScalarValue::Boolean(true), 3, 6),
            crate::marks::ExpandMark::After,
        )
        .unwrap();

        let heads2 = doc1.get_heads();
        let patches12 = doc1.diff(&heads1, &heads2).unwrap().take_patches();
        let exp1 = exp(patches12);
        assert_eq!(
            exp1,
            vec![ObservedPatch {
                path: "/text".into(),
                action: ObservedAction::Mark(vec![ObservedMark {
                    start: 3,
                    end: 6,
                    name: "bold".to_string(),
                    value: ScalarValue::Boolean(true),
                }]),
            }]
        );

        let patches21 = doc1.diff(&heads2, &heads1).unwrap().take_patches();
        let exp2 = exp(patches21);
        assert_eq!(
            exp2,
            vec![ObservedPatch {
                path: "/text".into(),
                action: ObservedAction::Mark(vec![ObservedMark {
                    start: 3,
                    end: 6,
                    name: "bold".to_string(),
                    value: ScalarValue::Null,
                }]),
            }]
        );
    }
}
