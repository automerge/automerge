use std::collections::{BTreeMap, HashMap};

use crate::automerge::view::ReadAt;
use crate::automerge::Automerge;
use crate::exid::ExId;
use crate::iter::SpanInternal;
use crate::marks::{Mark, MarkSet};
use crate::op_set2::{Op, OpQuery, OpQueryTerm, OpType};
use crate::text_value::ConcreteTextValue;
use crate::types::{ObjId as InternalObjId, SequenceType};
use crate::value::{ScalarValue, Value as PublicValue};
use crate::{ChangeId, ObjType, Patch, PatchAction, Prop, TextEncoding};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EffectValue {
    Scalar(ScalarValue),
    Map { id: ExId, value: EffectMap },
    List { id: ExId, value: EffectList },
    Text { id: ExId, value: EffectText },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct EffectMap(HashMap<String, EffectMapValue>);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectMapValue {
    value: EffectValue,
    conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct EffectList(Vec<EffectListValue>);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectListValue {
    value: EffectValue,
    marks: EffectMarks,
    conflict: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectText {
    encoding: TextEncoding,
    runs: Vec<EffectTextRun>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EffectTextRun {
    Text {
        value: ConcreteTextValue,
        marks: EffectMarks,
        conflict: bool,
    },
    Object {
        value: EffectValue,
        marks: EffectMarks,
        conflict: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct EffectMarks(BTreeMap<String, ScalarValue>);

#[track_caller]
pub(crate) fn assert_patches_have_same_effect<H: crate::hash_retention::HashRetention>(
    doc: &Automerge<H>,
    before: &[ChangeId],
    after: &[ChangeId],
    left_label: &str,
    left: &[Patch],
    right_label: &str,
    right: &[Patch],
) {
    let before_value = EffectValue::from_doc(doc, Some(before));
    let expected = EffectValue::from_doc(doc, Some(after));
    let mut left_value = before_value.clone();
    left_value.apply_patches(doc.text_encoding(), left.iter().cloned());
    let mut right_value = before_value;
    right_value.apply_patches(doc.text_encoding(), right.iter().cloned());
    assert_eq!(
        left_value, expected,
        "{left_label} patches did not produce the expected after-state"
    );
    assert_eq!(
        right_value, expected,
        "{right_label} patches did not produce the expected after-state"
    );
}

impl EffectValue {
    pub(crate) fn from_doc<H: crate::hash_retention::HashRetention>(
        doc: &Automerge<H>,
        heads: Option<&[ChangeId]>,
    ) -> Self {
        let read = doc
            .read_at(heads)
            .expect("effect heads must be change ids in the document");
        EffectMaterializer::new(doc).hydrate_map(&InternalObjId::root(), &read)
    }

    pub(crate) fn apply_patches<I>(&mut self, encoding: TextEncoding, patches: I)
    where
        I: IntoIterator<Item = Patch>,
    {
        for patch in patches {
            self.apply(
                patch.path.iter().map(|(_, prop)| prop),
                encoding,
                patch.action,
            );
        }
    }

    fn from_patch_value(value: PublicValue<'static>, id: ExId, encoding: TextEncoding) -> Self {
        match value {
            PublicValue::Object(ObjType::Map | ObjType::Table) => Self::Map {
                id,
                value: EffectMap::default(),
            },
            PublicValue::Object(ObjType::List) => Self::List {
                id,
                value: EffectList::default(),
            },
            PublicValue::Object(ObjType::Text) => Self::Text {
                id,
                value: EffectText::new(encoding),
            },
            PublicValue::Scalar(value) => Self::Scalar(value.into_owned()),
        }
    }

    fn width(&self, seq_type: SequenceType, encoding: TextEncoding) -> usize {
        match seq_type {
            SequenceType::List => 1,
            SequenceType::Text => match self {
                EffectValue::Scalar(ScalarValue::Str(value)) => encoding.width(value),
                _ => 1,
            },
        }
    }

    fn apply<'a, I>(&mut self, mut path: I, encoding: TextEncoding, action: PatchAction)
    where
        I: Iterator<Item = &'a Prop>,
    {
        match (path.next(), self) {
            (Some(Prop::Map(key)), EffectValue::Map { value: map, .. }) => map
                .get_mut(key)
                .expect("invalid patch path")
                .apply(path, encoding, action),
            (Some(Prop::Seq(index)), EffectValue::List { value: list, .. }) => list
                .get_mut(*index)
                .unwrap_or_else(|| panic!("invalid sequence index {}", index))
                .apply(path, encoding, action),
            (Some(Prop::Seq(index)), EffectValue::Text { value: text, .. }) => {
                text.get_mut(*index).apply(path, encoding, action)
            }
            (None, EffectValue::Map { value: map, .. }) => map.apply(encoding, action),
            (None, EffectValue::List { value: list, .. }) => list.apply(encoding, action),
            (None, EffectValue::Text { value: text, .. }) => text.apply(encoding, action),
            _ => panic!("invalid patch path"),
        }
    }

    fn increment(&mut self, value: i64) {
        match self {
            EffectValue::Scalar(ScalarValue::Counter(counter)) => {
                counter.increment(value);
            }
            _ => panic!("increment target is not a counter"),
        }
    }
}

impl EffectMap {
    fn insert(&mut self, key: String, value: EffectValue, conflict: bool) {
        self.0.insert(key, EffectMapValue { value, conflict });
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut EffectValue> {
        self.0.get_mut(key).map(|value| &mut value.value)
    }

    fn apply(&mut self, encoding: TextEncoding, action: PatchAction) {
        match action {
            PatchAction::PutMap {
                key,
                value,
                conflict,
            } => {
                self.insert(
                    key,
                    EffectValue::from_patch_value(value.0, value.1, encoding),
                    conflict,
                );
            }
            PatchAction::DeleteMap { key } => {
                self.0.remove(&key);
            }
            PatchAction::Increment {
                prop: Prop::Map(key),
                value,
            } => self
                .0
                .get_mut(&key)
                .expect("invalid patch path")
                .value
                .increment(value),
            PatchAction::Conflict {
                prop: Prop::Map(key),
            } => {
                self.0.get_mut(&key).expect("invalid patch path").conflict = true;
            }
            _ => panic!("invalid map patch action"),
        }
    }
}

impl EffectList {
    fn push(&mut self, value: EffectValue, marks: EffectMarks, conflict: bool) {
        self.0.push(EffectListValue {
            value,
            marks,
            conflict,
        });
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut EffectValue> {
        self.0.get_mut(index).map(|value| &mut value.value)
    }

    fn apply(&mut self, encoding: TextEncoding, action: PatchAction) {
        match action {
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => {
                let current = self
                    .0
                    .get_mut(index)
                    .unwrap_or_else(|| panic!("invalid sequence index {}", index));
                current.value = EffectValue::from_patch_value(value.0, value.1, encoding);
                current.conflict = conflict;
            }
            PatchAction::Insert { index, values } => {
                if index > self.0.len() {
                    panic!("invalid sequence index {}", index);
                }
                for (offset, value) in values.into_iter().enumerate() {
                    self.0.insert(
                        index + offset,
                        EffectListValue {
                            value: EffectValue::from_patch_value(
                                value.0.clone(),
                                value.1.clone(),
                                encoding,
                            ),
                            marks: EffectMarks::default(),
                            conflict: value.2,
                        },
                    );
                }
            }
            PatchAction::DeleteSeq { index, length } => {
                if index + length > self.0.len() {
                    panic!("invalid sequence index {}", index);
                }
                self.0.drain(index..index + length);
            }
            PatchAction::Increment {
                prop: Prop::Seq(index),
                value,
            } => self
                .0
                .get_mut(index)
                .unwrap_or_else(|| panic!("invalid sequence index {}", index))
                .value
                .increment(value),
            PatchAction::Conflict {
                prop: Prop::Seq(index),
            } => {
                self.0
                    .get_mut(index)
                    .unwrap_or_else(|| panic!("invalid sequence index {}", index))
                    .conflict = true;
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    self.apply_mark(mark);
                }
            }
            _ => panic!("invalid list patch action"),
        }
    }

    fn apply_mark(&mut self, mark: Mark) {
        if mark.end > self.0.len() {
            panic!("invalid sequence index {}", mark.end);
        }
        for value in &mut self.0[mark.start..mark.end] {
            value.marks.apply_mark(&mark);
        }
    }
}

impl EffectText {
    fn new(encoding: TextEncoding) -> Self {
        Self {
            encoding,
            runs: Vec::new(),
        }
    }

    fn push_text(&mut self, value: &str, marks: EffectMarks, conflict: bool) {
        if value.is_empty() {
            return;
        }
        self.runs.push(EffectTextRun::Text {
            value: ConcreteTextValue::new(value, self.encoding),
            marks,
            conflict,
        });
        self.normalize();
    }

    fn push_object(&mut self, value: EffectValue, marks: EffectMarks, conflict: bool) {
        self.runs.push(EffectTextRun::Object {
            value,
            marks,
            conflict,
        });
    }

    fn get_mut(&mut self, index: usize) -> &mut EffectValue {
        self.split_at(index);
        let run_index = self.run_index_at(index);
        match &mut self.runs[run_index] {
            EffectTextRun::Object { value, .. } => value,
            EffectTextRun::Text { .. } => panic!("invalid patch path"),
        }
    }

    fn apply(&mut self, encoding: TextEncoding, action: PatchAction) {
        if encoding != self.encoding {
            panic!("mismatched text encoding");
        }
        match action {
            PatchAction::SpliceText {
                index,
                value,
                marks,
            } => {
                let marks = marks.as_ref().map(EffectMarks::from).unwrap_or_default();
                self.insert_text_value(index, value, marks, false)
            }
            PatchAction::Insert { index, values } => {
                let mut insertion_index = index;
                for value in values.iter().cloned() {
                    let width =
                        EffectValue::from_patch_value(value.0.clone(), value.1.clone(), encoding)
                            .width(SequenceType::Text, encoding);
                    self.insert_value(
                        insertion_index,
                        value.0,
                        value.1,
                        EffectMarks::default(),
                        value.2,
                    );
                    insertion_index += width;
                }
            }
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => {
                let marks = self.marks_at(index);
                self.delete(index, 1);
                self.insert_value(index, value.0, value.1, marks, conflict)
            }
            PatchAction::DeleteSeq { index, length } => self.delete(index, length),
            PatchAction::Conflict {
                prop: Prop::Seq(index),
            } => {
                self.split_at(index);
                let run_index = self.run_index_at(index);
                self.runs[run_index].set_conflict(true);
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    self.apply_mark(mark);
                }
            }
            _ => panic!("invalid text patch action"),
        }
    }

    fn insert_value(
        &mut self,
        index: usize,
        value: PublicValue<'static>,
        id: ExId,
        marks: EffectMarks,
        conflict: bool,
    ) {
        match value {
            PublicValue::Scalar(value) => match value.into_owned() {
                ScalarValue::Str(value) => self.insert_text(index, value.as_str(), marks, conflict),
                scalar => self.insert_text(index, scalar.to_string().as_str(), marks, conflict),
            },
            object @ PublicValue::Object(_) => self.insert_object(
                index,
                EffectValue::from_patch_value(object, id, self.encoding),
                marks,
                conflict,
            ),
        }
    }

    fn insert_text(&mut self, index: usize, value: &str, marks: EffectMarks, conflict: bool) {
        self.insert_text_value(
            index,
            ConcreteTextValue::new(value, self.encoding),
            marks,
            conflict,
        )
    }

    fn insert_text_value(
        &mut self,
        index: usize,
        value: ConcreteTextValue,
        marks: EffectMarks,
        conflict: bool,
    ) {
        let run_index = self.split_at(index);
        if value.len() == 0 {
            return;
        }
        self.runs.insert(
            run_index,
            EffectTextRun::Text {
                value,
                marks,
                conflict,
            },
        );
        self.normalize();
    }

    fn insert_object(
        &mut self,
        index: usize,
        value: EffectValue,
        marks: EffectMarks,
        conflict: bool,
    ) {
        let run_index = self.split_at(index);
        self.runs.insert(
            run_index,
            EffectTextRun::Object {
                value,
                marks,
                conflict,
            },
        );
    }

    fn delete(&mut self, index: usize, length: usize) {
        if length == 0 {
            return;
        }
        let end = index + length;
        self.split_at(end);
        let start_run = self.split_at(index);
        let end_run = self.run_index_at_boundary(end);
        self.runs.drain(start_run..end_run);
        self.normalize();
    }

    fn apply_mark(&mut self, mark: Mark) {
        self.split_at(mark.end);
        let start_run = self.split_at(mark.start);
        let end_run = self.run_index_at_boundary(mark.end);
        for run in &mut self.runs[start_run..end_run] {
            run.marks_mut().apply_mark(&mark);
        }
        self.normalize();
    }

    fn marks_at(&mut self, index: usize) -> EffectMarks {
        self.split_at(index);
        let run_index = self.run_index_at(index);
        self.runs[run_index].marks().clone()
    }

    fn split_at(&mut self, index: usize) -> usize {
        let mut pos = 0;
        for run_index in 0..self.runs.len() {
            let width = self.runs[run_index].width();
            if index == pos {
                return run_index;
            }
            if index > pos && index < pos + width {
                self.split_run(run_index, index - pos);
                return run_index + 1;
            }
            pos += width;
        }
        if index == pos {
            self.runs.len()
        } else {
            panic!("invalid sequence index {}", index)
        }
    }

    fn split_run(&mut self, run_index: usize, offset: usize) {
        let EffectTextRun::Text {
            value,
            marks,
            conflict,
        } = &self.runs[run_index]
        else {
            panic!("invalid sequence index {}", offset);
        };
        let left = take_prefix(value, offset);
        let right = take_suffix(value, offset);
        let marks = marks.clone();
        let conflict = *conflict;
        self.runs[run_index] = EffectTextRun::Text {
            value: left,
            marks: marks.clone(),
            conflict,
        };
        self.runs.insert(
            run_index + 1,
            EffectTextRun::Text {
                value: right,
                marks,
                conflict,
            },
        );
    }

    fn run_index_at(&self, index: usize) -> usize {
        let mut pos = 0;
        for (run_index, run) in self.runs.iter().enumerate() {
            let width = run.width();
            if index >= pos && index < pos + width {
                return run_index;
            }
            pos += width;
        }
        panic!("invalid sequence index {}", index)
    }

    fn run_index_at_boundary(&self, index: usize) -> usize {
        let mut pos = 0;
        for (run_index, run) in self.runs.iter().enumerate() {
            if index == pos {
                return run_index;
            }
            pos += run.width();
        }
        if index == pos {
            self.runs.len()
        } else {
            panic!("invalid sequence index {}", index)
        }
    }

    fn normalize(&mut self) {
        let mut i = 0;
        while i + 1 < self.runs.len() {
            if self.runs[i].is_empty_text() {
                self.runs.remove(i);
                continue;
            }
            if self.runs[i].can_merge(&self.runs[i + 1]) {
                let next = self.runs.remove(i + 1);
                self.runs[i].merge(next);
            } else {
                i += 1;
            }
        }
        if self.runs.last().is_some_and(EffectTextRun::is_empty_text) {
            self.runs.pop();
        }
    }
}

impl EffectTextRun {
    fn width(&self) -> usize {
        match self {
            EffectTextRun::Text { value, .. } => value.len(),
            EffectTextRun::Object { .. } => 1,
        }
    }

    fn is_empty_text(&self) -> bool {
        matches!(self, EffectTextRun::Text { value, .. } if value.len() == 0)
    }

    fn marks(&self) -> &EffectMarks {
        match self {
            EffectTextRun::Text { marks, .. } | EffectTextRun::Object { marks, .. } => marks,
        }
    }

    fn marks_mut(&mut self) -> &mut EffectMarks {
        match self {
            EffectTextRun::Text { marks, .. } | EffectTextRun::Object { marks, .. } => marks,
        }
    }

    fn set_conflict(&mut self, new_conflict: bool) {
        match self {
            EffectTextRun::Text { conflict, .. } | EffectTextRun::Object { conflict, .. } => {
                *conflict = new_conflict
            }
        }
    }

    fn can_merge(&self, other: &Self) -> bool {
        match (self, other) {
            (
                EffectTextRun::Text {
                    marks: marks1,
                    conflict: conflict1,
                    ..
                },
                EffectTextRun::Text {
                    marks: marks2,
                    conflict: conflict2,
                    ..
                },
            ) => marks1 == marks2 && conflict1 == conflict2,
            _ => false,
        }
    }

    fn merge(&mut self, other: Self) {
        let EffectTextRun::Text { value: other, .. } = other else {
            return;
        };
        let EffectTextRun::Text { value, .. } = self else {
            return;
        };
        value
            .splice_text_value(value.len(), &other)
            .expect("merged text runs have the same encoding");
    }
}

impl EffectMarks {
    fn apply_mark(&mut self, mark: &Mark) {
        if mark.value.is_null() {
            self.0.remove(mark.name());
        } else {
            self.0.insert(mark.name().to_string(), mark.value().clone());
        }
    }
}

impl From<&MarkSet> for EffectMarks {
    fn from(value: &MarkSet) -> Self {
        EffectMarks(
            value
                .iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
        )
    }
}

struct EffectMaterializer<
    'a,
    H: crate::hash_retention::HashRetention = crate::hash_retention::Retained,
> {
    doc: &'a Automerge<H>,
}

impl<'a, H: crate::hash_retention::HashRetention> EffectMaterializer<'a, H> {
    fn new(doc: &'a Automerge<H>) -> Self {
        Self { doc }
    }

    fn exid_for_obj(&self, obj: &InternalObjId) -> ExId {
        if obj.is_root() {
            ExId::Root
        } else {
            self.doc.id_to_exid(obj.0)
        }
    }

    fn hydrate_map(&self, obj: &InternalObjId, read: &ReadAt<'_>) -> EffectValue {
        let mut map = EffectMap::default();
        for top in self.doc.ops().top_ops(obj, read.borrow()) {
            let key = self.doc.ops().to_string(top.elemid_or_key());
            let conflict = top.conflict;
            let value = self.hydrate_op(top, read);
            map.insert(key, value, conflict);
        }
        EffectValue::Map {
            id: self.exid_for_obj(obj),
            value: map,
        }
    }

    fn hydrate_list(&self, obj: &InternalObjId, read: &ReadAt<'_>) -> EffectValue {
        let mut list = EffectList::default();
        let mut top_ops = self.doc.ops().top_ops(obj, read.borrow()).marks();
        while let Some(top) = top_ops.next() {
            let marks = top_ops
                .get_marks()
                .map(|marks| EffectMarks::from(marks.as_ref()))
                .unwrap_or_default();
            let conflict = top.conflict;
            let value = self.hydrate_op(top, read);
            list.push(value, marks, conflict);
        }
        EffectValue::List {
            id: self.exid_for_obj(obj),
            value: list,
        }
    }

    fn hydrate_text(&self, obj: &InternalObjId, read: &ReadAt<'_>) -> EffectValue {
        let mut text = EffectText::new(self.doc.text_encoding());
        for span in self.doc.ops().spans(obj, read.borrow()) {
            match span {
                SpanInternal::Text(value, _, marks) => {
                    let marks = marks
                        .export()
                        .as_deref()
                        .map(EffectMarks::from)
                        .unwrap_or_default();
                    text.push_text(value.as_str(), marks, false);
                }
                SpanInternal::Obj(id, _, _) => {
                    let value = self.hydrate_obj(&InternalObjId(id), read);
                    text.push_object(value, EffectMarks::default(), false);
                }
            }
        }
        for mark in self.sequence_marks(obj, ObjType::Text, read) {
            text.apply_mark(mark);
        }
        EffectValue::Text {
            id: self.exid_for_obj(obj),
            value: text,
        }
    }

    fn hydrate_obj(&self, obj: &InternalObjId, read: &ReadAt<'_>) -> EffectValue {
        match self
            .doc
            .ops()
            .object_type(obj)
            .expect("visible child object should have an object type")
        {
            ObjType::Map | ObjType::Table => self.hydrate_map(obj, read),
            ObjType::List => self.hydrate_list(obj, read),
            ObjType::Text => self.hydrate_text(obj, read),
        }
    }

    fn hydrate_op(&self, op: Op<'_>, read: &ReadAt<'_>) -> EffectValue {
        match op.action() {
            OpType::Make(ObjType::Map | ObjType::Table) => {
                self.hydrate_map(&InternalObjId(op.id), read)
            }
            OpType::Make(ObjType::List) => self.hydrate_list(&InternalObjId(op.id), read),
            OpType::Make(ObjType::Text) => self.hydrate_text(&InternalObjId(op.id), read),
            OpType::Put(value) => EffectValue::Scalar(value.into()),
            _ => panic!("invalid op to hydrate"),
        }
    }

    fn sequence_marks(
        &self,
        obj: &InternalObjId,
        obj_type: ObjType,
        read: &ReadAt<'_>,
    ) -> Vec<Mark> {
        let Some(seq_type) = obj_type.as_sequence_type() else {
            return Vec::new();
        };
        let mut top_ops = self.doc.ops().top_ops(obj, read.borrow()).marks();
        let mut index = 0;
        let mut result = Vec::new();
        let mut last_marks = EffectMarks::default();
        let mut mark_index = 0;
        let mut mark_len = 0;
        while let Some(op) = top_ops.next() {
            let marks = top_ops
                .get_marks()
                .map(|marks| EffectMarks::from(marks.as_ref()))
                .unwrap_or_default();
            let len = op.width(seq_type, self.doc.text_encoding());
            if marks != last_marks {
                append_mark_runs(&mut result, mark_index, mark_len, &last_marks);
                last_marks = marks;
                mark_index = index;
                mark_len = 0;
            }
            mark_len += len;
            index += len;
        }
        append_mark_runs(&mut result, mark_index, mark_len, &last_marks);
        result
    }
}

fn append_mark_runs(result: &mut Vec<Mark>, index: usize, len: usize, marks: &EffectMarks) {
    if len == 0 {
        return;
    }
    for (name, value) in &marks.0 {
        result.push(Mark::new(name.clone(), value.clone(), index, index + len));
    }
}

fn take_prefix(value: &ConcreteTextValue, len: usize) -> ConcreteTextValue {
    let mut prefix = value.clone();
    while prefix.len() > len {
        prefix.remove(len);
    }
    prefix
}

fn take_suffix(value: &ConcreteTextValue, start: usize) -> ConcreteTextValue {
    let mut suffix = value.clone();
    for _ in 0..start {
        suffix.remove(0);
    }
    suffix
}
