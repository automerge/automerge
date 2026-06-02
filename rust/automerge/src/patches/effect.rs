#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::exid::ExId;
use crate::iter::SpanInternal;
use crate::marks::{Mark, MarkSet};
use crate::op_set2::{Op, OpQuery, OpQueryTerm, OpType};
use crate::text_value::ConcreteTextValue;
use crate::types::{Clock, ObjId as InternalObjId, SequenceType};
use crate::value::{ScalarValue, Value as PublicValue};
use crate::{
    Automerge, AutomergeError, ChangeHash, ObjType, Patch, PatchAction, Prop, TextEncoding,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PatchEffectError {
    InvalidPath,
    InvalidMapOp,
    InvalidListOp,
    InvalidTextOp,
    InvalidIndex(usize),
    InvalidIncrement,
    MismatchedTextEncoding,
}

impl fmt::Display for PatchEffectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchEffectError::InvalidPath => write!(f, "invalid patch path"),
            PatchEffectError::InvalidMapOp => write!(f, "invalid map patch action"),
            PatchEffectError::InvalidListOp => write!(f, "invalid list patch action"),
            PatchEffectError::InvalidTextOp => write!(f, "invalid text patch action"),
            PatchEffectError::InvalidIndex(index) => write!(f, "invalid sequence index {index}"),
            PatchEffectError::InvalidIncrement => write!(f, "increment target is not a counter"),
            PatchEffectError::MismatchedTextEncoding => write!(f, "mismatched text encoding"),
        }
    }
}

impl std::error::Error for PatchEffectError {}

#[track_caller]
pub(crate) fn assert_patches_have_same_effect(
    doc: &Automerge,
    before: &[ChangeHash],
    after: &[ChangeHash],
    left_label: &str,
    left: &[Patch],
    right_label: &str,
    right: &[Patch],
) {
    let before_value = EffectValue::from_doc(doc, Some(before));
    let expected = EffectValue::from_doc(doc, Some(after));
    let left_value = apply_patch_effects(doc, before_value.clone(), left_label, left);
    let right_value = apply_patch_effects(doc, before_value, right_label, right);
    assert_eq!(
        left_value, expected,
        "{left_label} patches did not produce the expected after-state"
    );
    assert_eq!(
        right_value, expected,
        "{right_label} patches did not produce the expected after-state"
    );
    assert_eq!(
        left_value, right_value,
        "{left_label} and {right_label} patches have different effects"
    );
}

#[track_caller]
pub(crate) fn assert_patches_have_same_effect_for_obj(
    doc: &Automerge,
    obj: &ExId,
    before: &[ChangeHash],
    after: &[ChangeHash],
    left_label: &str,
    left: &[Patch],
    right_label: &str,
    right: &[Patch],
) {
    let before_value = EffectValue::from_doc_obj(doc, obj, Some(before))
        .expect("object should materialize at before heads");
    let expected = EffectValue::from_doc_obj(doc, obj, Some(after))
        .expect("object should materialize at after heads");
    let left_value = apply_patch_effects_for_obj(doc, obj, before_value.clone(), left_label, left);
    let right_value = apply_patch_effects_for_obj(doc, obj, before_value, right_label, right);
    assert_eq!(
        left_value, expected,
        "{left_label} patches did not produce the expected object after-state"
    );
    assert_eq!(
        right_value, expected,
        "{right_label} patches did not produce the expected object after-state"
    );
    assert_eq!(
        left_value, right_value,
        "{left_label} and {right_label} patches have different object effects"
    );
}

#[track_caller]
fn apply_patch_effects(
    doc: &Automerge,
    mut value: EffectValue,
    label: &str,
    patches: &[Patch],
) -> EffectValue {
    value
        .apply_patches(doc.text_encoding(), patches.iter().cloned())
        .unwrap_or_else(|err| panic!("{label} patches failed to apply: {err}"));
    value
}

#[track_caller]
fn apply_patch_effects_for_obj(
    doc: &Automerge,
    obj: &ExId,
    mut value: EffectValue,
    label: &str,
    patches: &[Patch],
) -> EffectValue {
    value
        .apply_patches_for_obj(obj, doc.text_encoding(), patches.iter().cloned())
        .unwrap_or_else(|err| panic!("{label} object patches failed to apply: {err}"));
    value
}

impl EffectValue {
    pub(crate) fn from_doc(doc: &Automerge, heads: Option<&[ChangeHash]>) -> Self {
        let clock = heads.map(|heads| doc.clock_at(heads));
        EffectMaterializer::new(doc).hydrate_map(&InternalObjId::root(), clock.as_ref())
    }

    pub(crate) fn from_doc_obj(
        doc: &Automerge,
        obj: &ExId,
        heads: Option<&[ChangeHash]>,
    ) -> Result<Self, AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        let clock = heads.map(|heads| doc.clock_at(heads));
        Ok(EffectMaterializer::new(doc).hydrate_obj(&obj.id, clock.as_ref()))
    }

    pub(crate) fn apply_patches<I>(
        &mut self,
        encoding: TextEncoding,
        patches: I,
    ) -> Result<(), PatchEffectError>
    where
        I: IntoIterator<Item = Patch>,
    {
        for patch in patches {
            self.apply(
                patch.path.iter().map(|(_, prop)| prop),
                encoding,
                patch.action,
            )?;
        }
        Ok(())
    }

    pub(crate) fn apply_patches_for_obj<I>(
        &mut self,
        obj: &ExId,
        encoding: TextEncoding,
        patches: I,
    ) -> Result<(), PatchEffectError>
    where
        I: IntoIterator<Item = Patch>,
    {
        for patch in patches {
            let props: Vec<Prop> = scoped_props(obj, &patch)?;
            self.apply(props.iter(), encoding, patch.action)?;
        }
        Ok(())
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

    fn apply<'a, I>(
        &mut self,
        mut path: I,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError>
    where
        I: Iterator<Item = &'a Prop>,
    {
        match (path.next(), self) {
            (Some(Prop::Map(key)), EffectValue::Map { value: map, .. }) => map
                .get_mut(key)
                .ok_or(PatchEffectError::InvalidPath)?
                .apply(path, encoding, action),
            (Some(Prop::Seq(index)), EffectValue::List { value: list, .. }) => list
                .get_mut(*index)
                .ok_or(PatchEffectError::InvalidIndex(*index))?
                .apply(path, encoding, action),
            (Some(Prop::Seq(index)), EffectValue::Text { value: text, .. }) => {
                text.get_mut(*index)?.apply(path, encoding, action)
            }
            (None, EffectValue::Map { value: map, .. }) => map.apply(encoding, action),
            (None, EffectValue::List { value: list, .. }) => list.apply(encoding, action),
            (None, EffectValue::Text { value: text, .. }) => text.apply(encoding, action),
            _ => Err(PatchEffectError::InvalidPath),
        }
    }

    fn increment(&mut self, value: i64) -> Result<(), PatchEffectError> {
        match self {
            EffectValue::Scalar(ScalarValue::Counter(counter)) => {
                counter.increment(value);
                Ok(())
            }
            _ => Err(PatchEffectError::InvalidIncrement),
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

    fn apply(
        &mut self,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError> {
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
                Ok(())
            }
            PatchAction::DeleteMap { key } => {
                self.0.remove(&key);
                Ok(())
            }
            PatchAction::Increment {
                prop: Prop::Map(key),
                value,
            } => self
                .0
                .get_mut(&key)
                .ok_or(PatchEffectError::InvalidPath)?
                .value
                .increment(value),
            PatchAction::Conflict {
                prop: Prop::Map(key),
            } => {
                self.0
                    .get_mut(&key)
                    .ok_or(PatchEffectError::InvalidPath)?
                    .conflict = true;
                Ok(())
            }
            _ => Err(PatchEffectError::InvalidMapOp),
        }
    }
}

impl EffectMapValue {
    fn apply<'a, I>(
        &mut self,
        path: I,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError>
    where
        I: Iterator<Item = &'a Prop>,
    {
        self.value.apply(path, encoding, action)
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

    fn apply(
        &mut self,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError> {
        match action {
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => {
                let current = self
                    .0
                    .get_mut(index)
                    .ok_or(PatchEffectError::InvalidIndex(index))?;
                current.value = EffectValue::from_patch_value(value.0, value.1, encoding);
                current.conflict = conflict;
                Ok(())
            }
            PatchAction::Insert { index, values } => {
                if index > self.0.len() {
                    return Err(PatchEffectError::InvalidIndex(index));
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
                Ok(())
            }
            PatchAction::DeleteSeq { index, length } => {
                if index + length > self.0.len() {
                    return Err(PatchEffectError::InvalidIndex(index));
                }
                self.0.drain(index..index + length);
                Ok(())
            }
            PatchAction::Increment {
                prop: Prop::Seq(index),
                value,
            } => self
                .0
                .get_mut(index)
                .ok_or(PatchEffectError::InvalidIndex(index))?
                .value
                .increment(value),
            PatchAction::Conflict {
                prop: Prop::Seq(index),
            } => {
                self.0
                    .get_mut(index)
                    .ok_or(PatchEffectError::InvalidIndex(index))?
                    .conflict = true;
                Ok(())
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    self.apply_mark(mark)?;
                }
                Ok(())
            }
            _ => Err(PatchEffectError::InvalidListOp),
        }
    }

    fn apply_mark(&mut self, mark: Mark) -> Result<(), PatchEffectError> {
        if mark.end > self.0.len() {
            return Err(PatchEffectError::InvalidIndex(mark.end));
        }
        for value in &mut self.0[mark.start..mark.end] {
            value.marks.apply_mark(&mark);
        }
        Ok(())
    }
}

impl EffectListValue {
    fn apply<'a, I>(
        &mut self,
        path: I,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError>
    where
        I: Iterator<Item = &'a Prop>,
    {
        self.value.apply(path, encoding, action)
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

    fn get_mut(&mut self, index: usize) -> Result<&mut EffectValue, PatchEffectError> {
        self.split_at(index)?;
        let run_index = self.run_index_at(index)?;
        match &mut self.runs[run_index] {
            EffectTextRun::Object { value, .. } => Ok(value),
            EffectTextRun::Text { .. } => Err(PatchEffectError::InvalidPath),
        }
    }

    fn apply(
        &mut self,
        encoding: TextEncoding,
        action: PatchAction,
    ) -> Result<(), PatchEffectError> {
        if encoding != self.encoding {
            return Err(PatchEffectError::MismatchedTextEncoding);
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
                    )?;
                    insertion_index += width;
                }
                Ok(())
            }
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => {
                let marks = self.marks_at(index)?;
                self.delete(index, 1)?;
                self.insert_value(index, value.0, value.1, marks, conflict)
            }
            PatchAction::DeleteSeq { index, length } => self.delete(index, length),
            PatchAction::Conflict {
                prop: Prop::Seq(index),
            } => {
                self.split_at(index)?;
                let run_index = self.run_index_at(index)?;
                self.runs[run_index].set_conflict(true);
                Ok(())
            }
            PatchAction::Mark { marks } => {
                for mark in marks {
                    self.apply_mark(mark)?;
                }
                Ok(())
            }
            _ => Err(PatchEffectError::InvalidTextOp),
        }
    }

    fn insert_value(
        &mut self,
        index: usize,
        value: PublicValue<'static>,
        id: ExId,
        marks: EffectMarks,
        conflict: bool,
    ) -> Result<(), PatchEffectError> {
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

    fn insert_text(
        &mut self,
        index: usize,
        value: &str,
        marks: EffectMarks,
        conflict: bool,
    ) -> Result<(), PatchEffectError> {
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
    ) -> Result<(), PatchEffectError> {
        let run_index = self.split_at(index)?;
        if value.len() == 0 {
            return Ok(());
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
        Ok(())
    }

    fn insert_object(
        &mut self,
        index: usize,
        value: EffectValue,
        marks: EffectMarks,
        conflict: bool,
    ) -> Result<(), PatchEffectError> {
        let run_index = self.split_at(index)?;
        self.runs.insert(
            run_index,
            EffectTextRun::Object {
                value,
                marks,
                conflict,
            },
        );
        Ok(())
    }

    fn delete(&mut self, index: usize, length: usize) -> Result<(), PatchEffectError> {
        if length == 0 {
            return Ok(());
        }
        let end = index + length;
        self.split_at(end)?;
        let start_run = self.split_at(index)?;
        let end_run = self.run_index_at_boundary(end)?;
        self.runs.drain(start_run..end_run);
        self.normalize();
        Ok(())
    }

    fn apply_mark(&mut self, mark: Mark) -> Result<(), PatchEffectError> {
        self.split_at(mark.end)?;
        let start_run = self.split_at(mark.start)?;
        let end_run = self.run_index_at_boundary(mark.end)?;
        for run in &mut self.runs[start_run..end_run] {
            run.marks_mut().apply_mark(&mark);
        }
        self.normalize();
        Ok(())
    }

    fn marks_at(&mut self, index: usize) -> Result<EffectMarks, PatchEffectError> {
        self.split_at(index)?;
        let run_index = self.run_index_at(index)?;
        Ok(self.runs[run_index].marks().clone())
    }

    fn split_at(&mut self, index: usize) -> Result<usize, PatchEffectError> {
        let mut pos = 0;
        for run_index in 0..self.runs.len() {
            let width = self.runs[run_index].width();
            if index == pos {
                return Ok(run_index);
            }
            if index > pos && index < pos + width {
                self.split_run(run_index, index - pos)?;
                return Ok(run_index + 1);
            }
            pos += width;
        }
        if index == pos {
            Ok(self.runs.len())
        } else {
            Err(PatchEffectError::InvalidIndex(index))
        }
    }

    fn split_run(&mut self, run_index: usize, offset: usize) -> Result<(), PatchEffectError> {
        let EffectTextRun::Text {
            value,
            marks,
            conflict,
        } = &self.runs[run_index]
        else {
            return Err(PatchEffectError::InvalidIndex(offset));
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
        Ok(())
    }

    fn run_index_at(&self, index: usize) -> Result<usize, PatchEffectError> {
        let mut pos = 0;
        for (run_index, run) in self.runs.iter().enumerate() {
            let width = run.width();
            if index >= pos && index < pos + width {
                return Ok(run_index);
            }
            pos += width;
        }
        Err(PatchEffectError::InvalidIndex(index))
    }

    fn run_index_at_boundary(&self, index: usize) -> Result<usize, PatchEffectError> {
        let mut pos = 0;
        for (run_index, run) in self.runs.iter().enumerate() {
            if index == pos {
                return Ok(run_index);
            }
            pos += run.width();
        }
        if index == pos {
            Ok(self.runs.len())
        } else {
            Err(PatchEffectError::InvalidIndex(index))
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

struct EffectMaterializer<'a> {
    doc: &'a Automerge,
}

impl<'a> EffectMaterializer<'a> {
    fn new(doc: &'a Automerge) -> Self {
        Self { doc }
    }

    fn exid_for_obj(&self, obj: &InternalObjId) -> ExId {
        if obj.is_root() {
            ExId::Root
        } else {
            self.doc.id_to_exid(obj.0)
        }
    }

    fn hydrate_map(&self, obj: &InternalObjId, clock: Option<&Clock>) -> EffectValue {
        let mut map = EffectMap::default();
        for top in self.doc.ops().top_ops(obj, clock.cloned()) {
            let key = self.doc.ops().to_string(top.elemid_or_key());
            let conflict = top.conflict;
            let value = self.hydrate_op(top, clock);
            map.insert(key, value, conflict);
        }
        EffectValue::Map {
            id: self.exid_for_obj(obj),
            value: map,
        }
    }

    fn hydrate_list(&self, obj: &InternalObjId, clock: Option<&Clock>) -> EffectValue {
        let mut list = EffectList::default();
        let mut top_ops = self.doc.ops().top_ops(obj, clock.cloned()).marks();
        while let Some(top) = top_ops.next() {
            let marks = top_ops
                .get_marks()
                .map(|marks| EffectMarks::from(marks.as_ref()))
                .unwrap_or_default();
            let conflict = top.conflict;
            let value = self.hydrate_op(top, clock);
            list.push(value, marks, conflict);
        }
        EffectValue::List {
            id: self.exid_for_obj(obj),
            value: list,
        }
    }

    fn hydrate_text(&self, obj: &InternalObjId, clock: Option<&Clock>) -> EffectValue {
        let mut text = EffectText::new(self.doc.text_encoding());
        for span in self.doc.ops().spans(obj, clock.cloned()) {
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
                    let value = self.hydrate_obj(&InternalObjId(id), clock);
                    text.push_object(value, EffectMarks::default(), false);
                }
            }
        }
        for mark in self.sequence_marks(obj, ObjType::Text, clock) {
            text.apply_mark(mark)
                .expect("document-derived text marks should fit the hydrated text");
        }
        EffectValue::Text {
            id: self.exid_for_obj(obj),
            value: text,
        }
    }

    fn hydrate_obj(&self, obj: &InternalObjId, clock: Option<&Clock>) -> EffectValue {
        match self
            .doc
            .ops()
            .object_type(obj)
            .expect("visible child object should have an object type")
        {
            ObjType::Map | ObjType::Table => self.hydrate_map(obj, clock),
            ObjType::List => self.hydrate_list(obj, clock),
            ObjType::Text => self.hydrate_text(obj, clock),
        }
    }

    fn hydrate_op(&self, op: Op<'_>, clock: Option<&Clock>) -> EffectValue {
        match op.action() {
            OpType::Make(ObjType::Map | ObjType::Table) => {
                self.hydrate_map(&InternalObjId(op.id), clock)
            }
            OpType::Make(ObjType::List) => self.hydrate_list(&InternalObjId(op.id), clock),
            OpType::Make(ObjType::Text) => self.hydrate_text(&InternalObjId(op.id), clock),
            OpType::Put(value) => EffectValue::Scalar(value.into()),
            _ => panic!("invalid op to hydrate"),
        }
    }

    fn sequence_marks(
        &self,
        obj: &InternalObjId,
        obj_type: ObjType,
        clock: Option<&Clock>,
    ) -> Vec<Mark> {
        let Some(seq_type) = obj_type.as_sequence_type() else {
            return Vec::new();
        };
        let mut top_ops = self.doc.ops().top_ops(obj, clock.cloned()).marks();
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

fn scoped_props(obj: &ExId, patch: &Patch) -> Result<Vec<Prop>, PatchEffectError> {
    if obj == &ExId::Root {
        return Ok(patch.path.iter().map(|(_, prop)| prop.clone()).collect());
    }
    if &patch.obj == obj {
        return Ok(Vec::new());
    }
    let Some(index) = patch.path.iter().position(|(parent, _)| parent == obj) else {
        return Err(PatchEffectError::InvalidPath);
    };
    Ok(patch.path[index..]
        .iter()
        .map(|(_, prop)| prop.clone())
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marks::ExpandMark;
    use crate::transaction::Transactable;
    use crate::{AutoCommit, ROOT};

    fn assert_diff_patches_have_effect(
        doc: &mut AutoCommit,
        before: &[ChangeHash],
        after: &[ChangeHash],
    ) {
        let mut materialized = EffectValue::from_doc(&doc.doc, Some(before));
        let expected = EffectValue::from_doc(&doc.doc, Some(after));
        let patches = doc.diff(before, after);
        materialized
            .apply_patches(doc.doc.text_encoding(), patches)
            .expect("patches should apply to materialized before-state");
        assert_eq!(materialized, expected);
    }

    fn assert_obj_diff_patches_have_effect(
        doc: &mut AutoCommit,
        obj: &ExId,
        before: &[ChangeHash],
        after: &[ChangeHash],
        recursive: bool,
    ) {
        let mut materialized = EffectValue::from_doc_obj(&doc.doc, obj, Some(before)).unwrap();
        let expected = EffectValue::from_doc_obj(&doc.doc, obj, Some(after)).unwrap();
        let patches = doc.diff_obj(obj, before, after, recursive).unwrap();
        materialized
            .apply_patches_for_obj(obj, doc.doc.text_encoding(), patches)
            .expect("object patches should apply to materialized before-state");
        assert_eq!(materialized, expected);
    }

    #[test]
    fn materializer_applies_text_mark_patches() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "hello world").unwrap();
        let before = doc.get_heads();

        doc.mark(
            &text,
            Mark::new("bold".to_string(), true, 3, 8),
            ExpandMark::default(),
        )
        .unwrap();
        let after = doc.get_heads();

        assert_diff_patches_have_effect(&mut doc, &before, &after);
    }

    #[test]
    fn materializer_applies_text_splices_around_marks() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "hello world").unwrap();
        doc.mark(
            &text,
            Mark::new("bold".to_string(), true, 3, 8),
            ExpandMark::default(),
        )
        .unwrap();
        let before = doc.get_heads();

        doc.splice_text(&text, 5, 1, " brave new ").unwrap();
        let after = doc.get_heads();

        assert_diff_patches_have_effect(&mut doc, &before, &after);
    }

    #[test]
    fn materializer_applies_text_put_seq_patches() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "abc").unwrap();
        doc.mark(
            &text,
            Mark::new("bold".to_string(), true, 1, 2),
            ExpandMark::default(),
        )
        .unwrap();
        let before = doc.get_heads();

        doc.put(&text, 1, "Z").unwrap();
        let after = doc.get_heads();

        assert_diff_patches_have_effect(&mut doc, &before, &after);
    }

    #[test]
    fn materializer_tracks_object_identity() {
        let mut doc = AutoCommit::new();
        let before = doc.get_heads();

        doc.put_object(ROOT, "map", ObjType::Map).unwrap();
        let after = doc.get_heads();

        assert_diff_patches_have_effect(&mut doc, &before, &after);
    }

    #[test]
    fn materializer_applies_nonrecursive_object_scoped_patches() {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        doc.insert(&list, 0, "one").unwrap();
        let before = doc.get_heads();

        doc.put(&list, 0, "two").unwrap();
        let after = doc.get_heads();

        assert_obj_diff_patches_have_effect(&mut doc, &list, &before, &after, false);
    }

    #[test]
    fn materializer_applies_recursive_object_scoped_patches() {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        let map = doc.insert_object(&list, 0, ObjType::Map).unwrap();
        let before = doc.get_heads();

        doc.put(&map, "key", "value").unwrap();
        let after = doc.get_heads();

        assert_obj_diff_patches_have_effect(&mut doc, &list, &before, &after, true);
    }

    #[test]
    fn materializer_routes_child_patches_through_text_object_runs() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        let block = doc.split_block(&text, 0).unwrap();
        let before = doc.get_heads();

        doc.put(&block, "kind", "paragraph").unwrap();
        let after = doc.get_heads();

        assert_diff_patches_have_effect(&mut doc, &before, &after);
    }

    #[test]
    fn materializer_applies_list_conflicts() {
        let mut doc1 = AutoCommit::new();
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, "one").unwrap();
        let mut doc2 = doc1.fork();
        let before = doc1.get_heads();

        doc1.put(&list, 0, "left").unwrap();
        doc2.put(&list, 0, "right").unwrap();
        doc1.merge(&mut doc2).unwrap();
        let after = doc1.get_heads();

        assert_diff_patches_have_effect(&mut doc1, &before, &after);
    }
}
