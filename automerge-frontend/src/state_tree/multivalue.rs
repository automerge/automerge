use std::{cmp::Ordering, collections::HashMap, iter::Iterator};

use automerge_protocol as amp;
use unicode_segmentation::UnicodeSegmentation;

use super::{
    CursorState, Cursors, DiffableSequence, ResolvedPath, ResolvedPathMut, StateTreeComposite,
    StateTreeList, StateTreeMap, StateTreeTable, StateTreeText, StateTreeValue,
};
use crate::{
    error,
    path::PathElement,
    value::{Primitive, Value},
};

pub(crate) struct NewValueRequest<'a, 'c> {
    pub(crate) actor: &'a amp::ActorId,
    pub(crate) start_op: u64,
    pub(crate) key: amp::Key,
    pub(crate) value: Value,
    pub(crate) parent_obj: &'c amp::ObjectId,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<amp::OpId>,
}

/// A set of conflicting values for the same key, indexed by OpID
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MultiValue {
    winning_value: (amp::OpId, StateTreeValue),
    conflicts: HashMap<amp::OpId, StateTreeValue>,
}

impl MultiValue {
    pub fn check_new_from_diff(
        _opid: &amp::OpId,
        diff: &amp::Diff,
    ) -> Result<(), error::InvalidPatch> {
        StateTreeValue::check_new_from_diff(diff)
    }

    pub fn new_from_diff(opid: amp::OpId, diff: amp::Diff) -> MultiValue {
        let value = StateTreeValue::new_from_diff(diff);
        MultiValue {
            winning_value: (opid, value),
            conflicts: HashMap::new(),
        }
    }

    pub(super) fn from_statetree_value(
        statetree_val: StateTreeValue,
        opid: amp::OpId,
    ) -> MultiValue {
        MultiValue {
            winning_value: (opid, statetree_val),
            conflicts: HashMap::new(),
        }
    }

    pub(super) fn new_from_value_2(req: NewValueRequest) -> NewValue {
        Self::new_from_value(
            req.actor,
            req.start_op,
            req.parent_obj.clone(),
            req.key,
            req.value,
            req.insert,
            req.pred,
        )
    }

    pub(super) fn new_from_value(
        actor: &amp::ActorId,
        start_op: u64,
        parent_id: amp::ObjectId,
        key: amp::Key,
        value: Value,
        insert: bool,
        pred: Vec<amp::OpId>,
    ) -> NewValue {
        NewValueContext {
            start_op,
            actor,
            key,
            insert,
            pred,
            parent_obj: &parent_id,
        }
        .create(value)
    }

    pub(super) fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
    ) -> Result<(), error::InvalidPatch> {
        self.check_diff_iter(&mut std::iter::once((opid, diff)))
    }

    pub(super) fn check_diff_iter<'a, 'b, I>(&self, diff: &mut I) -> Result<(), error::InvalidPatch>
    where
        I: Iterator<Item = (&'a amp::OpId, &'b amp::Diff)>,
    {
        for (opid, subdiff) in diff {
            if let Some(existing_value) = self.get(opid) {
                match existing_value {
                    StateTreeValue::Leaf(_) => {
                        StateTreeValue::check_new_from_diff(subdiff)?;
                    }
                    StateTreeValue::Composite(composite) => {
                        composite.check_diff(subdiff)?;
                    }
                }
            } else {
                StateTreeValue::check_new_from_diff(subdiff)?;
            };
        }
        Ok(())
    }

    pub(super) fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff) {
        self.apply_diff_iter(&mut std::iter::once((opid, diff)))
    }

    pub(super) fn apply_diff_iter<I>(&mut self, diff: &mut I)
    where
        I: Iterator<Item = (amp::OpId, amp::Diff)>,
    {
        for (opid, subdiff) in diff {
            if let Some(existing_value) = self.get_mut(&opid) {
                match existing_value {
                    StateTreeValue::Leaf(_) => {
                        let value = StateTreeValue::new_from_diff(subdiff);
                        self.update(&opid, value)
                    }
                    StateTreeValue::Composite(composite) => {
                        composite.apply_diff(subdiff);
                    }
                }
            } else {
                let value = StateTreeValue::new_from_diff(subdiff);
                self.update(&opid, value)
            };
        }
    }

    fn get(&self, opid: &amp::OpId) -> Option<&StateTreeValue> {
        if opid == &self.winning_value.0 {
            Some(&self.winning_value.1)
        } else {
            self.conflicts.get(opid)
        }
    }

    fn get_mut(&mut self, opid: &amp::OpId) -> Option<&mut StateTreeValue> {
        if opid == &self.winning_value.0 {
            Some(&mut self.winning_value.1)
        } else {
            self.conflicts.get_mut(opid)
        }
    }

    fn update(&mut self, opid: &amp::OpId, value: StateTreeValue) {
        if *opid >= self.winning_value.0 {
            self.conflicts
                .insert(self.winning_value.0.clone(), self.winning_value.1.clone());
            self.winning_value.0 = opid.clone();
            self.winning_value.1 = value;
        } else {
            self.conflicts.insert(opid.clone(), value);
        }
    }

    pub(super) fn default_statetree_value(&self) -> &StateTreeValue {
        &self.winning_value.1
    }

    pub(super) fn default_statetree_value_mut(&mut self) -> &mut StateTreeValue {
        &mut self.winning_value.1
    }

    pub(super) fn default_value(&self) -> Value {
        self.winning_value.1.realise_value()
    }

    pub(super) fn default_opid(&self) -> amp::OpId {
        self.winning_value.0.clone()
    }

    pub(super) fn update_default(&self, val: StateTreeValue) -> MultiValue {
        MultiValue {
            winning_value: (self.winning_value.0.clone(), val),
            conflicts: self.conflicts.clone(),
        }
    }

    fn iter(&self) -> impl std::iter::Iterator<Item = (&amp::OpId, &StateTreeValue)> {
        std::iter::once((&(self.winning_value).0, &(self.winning_value.1)))
            .chain(self.conflicts.iter())
    }

    pub(super) fn realise_values(&self) -> std::collections::HashMap<amp::OpId, Value> {
        self.iter()
            .map(|(opid, v)| (opid.clone(), v.realise_value()))
            .collect()
    }

    pub(crate) fn resolve_path(
        &self,
        path: Vec<PathElement>,
        parent_object_id: amp::ObjectId,
        key: amp::Key,
    ) -> Option<ResolvedPath> {
        if path.is_empty() {
            if let StateTreeValue::Leaf(Primitive::Counter(_)) = self.winning_value.1 {
                return Some(ResolvedPath::new_counter(parent_object_id, key, self));
            } else if let StateTreeValue::Leaf(_) = self.winning_value.1 {
                return Some(ResolvedPath::new_primitive(self));
            }

            if let StateTreeValue::Composite(composite) = &self.winning_value.1 {
                match composite {
                    StateTreeComposite::Map(map) => {
                        return Some(ResolvedPath::new_map(self, map.object_id.clone()))
                    }
                    StateTreeComposite::Table(table) => {
                        return Some(ResolvedPath::new_table(self, table.object_id.clone()))
                    }
                    StateTreeComposite::Text(text) => {
                        return Some(ResolvedPath::new_text(self, text.object_id.clone()))
                    }
                    StateTreeComposite::List(list) => {
                        return Some(ResolvedPath::new_list(self, list.object_id.clone()))
                    }
                }
            }
        } else if let StateTreeValue::Composite(ref composite) = self.winning_value.1 {
            return composite.resolve_path(path);
        }
        None
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        path: Vec<PathElement>,
        parent_object_id: amp::ObjectId,
        key: amp::Key,
    ) -> Option<ResolvedPathMut> {
        if path.is_empty() {
            if let StateTreeValue::Leaf(Primitive::Counter(_)) = self.winning_value.1 {
                return Some(ResolvedPathMut::new_counter(parent_object_id, key, self));
            } else if let StateTreeValue::Leaf(_) = self.winning_value.1 {
                return Some(ResolvedPathMut::new_primitive(self));
            }

            if let StateTreeValue::Composite(composite) = &self.winning_value.1 {
                match composite {
                    StateTreeComposite::Map(map) => {
                        let oid = map.object_id.clone();
                        return Some(ResolvedPathMut::new_map(self, oid));
                    }
                    StateTreeComposite::Table(table) => {
                        let oid = table.object_id.clone();
                        return Some(ResolvedPathMut::new_table(self, oid));
                    }
                    StateTreeComposite::Text(text) => {
                        let oid = text.object_id.clone();
                        return Some(ResolvedPathMut::new_text(self, oid));
                    }
                    StateTreeComposite::List(list) => {
                        let oid = list.object_id.clone();
                        return Some(ResolvedPathMut::new_list(self, oid));
                    }
                }
            }
        } else if let StateTreeValue::Composite(ref mut composite) = self.winning_value.1 {
            return composite.resolve_path_mut(path);
        }
        None
    }

    pub(super) fn opids(&self) -> impl Iterator<Item = &amp::OpId> {
        std::iter::once(&self.winning_value.0).chain(self.conflicts.keys())
    }

    pub(super) fn has_opid(&self, opid: &amp::OpId) -> bool {
        self.opids().any(|o| o == opid)
    }

    pub(super) fn only_for_opid(&self, opid: amp::OpId) -> Option<MultiValue> {
        if opid == self.winning_value.0 {
            Some(MultiValue {
                winning_value: self.winning_value.clone(),
                conflicts: HashMap::new(),
            })
        } else {
            self.conflicts.get(&opid).map(|value| MultiValue {
                winning_value: (opid.clone(), value.clone()),
                conflicts: HashMap::new(),
            })
        }
    }

    pub(super) fn add_values_from(&mut self, other: MultiValue) {
        for (opid, value) in other.iter() {
            match opid.cmp(&self.winning_value.0) {
                Ordering::Greater => {
                    let mut temp = (opid.clone(), value.clone());
                    std::mem::swap(&mut temp, &mut self.winning_value);
                    self.conflicts.insert(temp.0, temp.1);
                }
                Ordering::Less => {
                    self.conflicts.insert(opid.clone(), value.clone());
                }
                Ordering::Equal => {}
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct NewValue {
    value: StateTreeValue,
    opid: amp::OpId,
    ops: Vec<amp::Op>,
    new_cursors: Cursors,
    max_op: u64,
}

impl NewValue {
    pub(super) fn max_op(&self) -> u64 {
        self.max_op
    }

    pub(super) fn finish(self) -> (MultiValue, Vec<amp::Op>, Cursors) {
        (
            MultiValue::from_statetree_value(self.value, self.opid),
            self.ops,
            self.new_cursors,
        )
    }
}

/// This struct exists to constrain the values of a text type to just containing
/// sequences of grapheme clusters
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MultiGrapheme {
    winning_value: (amp::OpId, String),
    conflicts: HashMap<amp::OpId, String>,
}

impl MultiGrapheme {
    pub(super) fn new_from_grapheme_cluster(opid: amp::OpId, s: String) -> MultiGrapheme {
        debug_assert_eq!(s.graphemes(true).count(), 1);
        MultiGrapheme {
            winning_value: (opid, s),
            conflicts: HashMap::new(),
        }
    }

    pub(super) fn check_new_from_diff(
        _opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), error::InvalidPatch> {
        match diff {
            amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                if s.graphemes(true).count() != 1 {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: parent_object_id.clone(),
                        diff: diff.clone(),
                    });
                } else {
                    s
                }
            }
            _ => {
                return Err(error::InvalidPatch::InsertNonTextInTextObject {
                    object_id: parent_object_id.clone(),
                    diff: diff.clone(),
                });
            }
        };
        Ok(())
    }

    pub(super) fn new_from_diff(opid: amp::OpId, diff: amp::Diff) -> MultiGrapheme {
        let winning_value = match diff {
            amp::Diff::Value(amp::ScalarValue::Str(s)) => s,
            _ => unreachable!("insert non text in text object"),
        };
        MultiGrapheme {
            winning_value: (opid, winning_value),
            conflicts: HashMap::new(),
        }
    }

    pub(super) fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), error::InvalidPatch> {
        self.check_diff_iter(&mut std::iter::once((opid, diff)), parent_object_id)
    }

    pub(super) fn check_diff_iter<'a, 'b, I>(
        &self,
        diff: &mut I,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), error::InvalidPatch>
    where
        I: Iterator<Item = (&'a amp::OpId, &'b amp::Diff)>,
    {
        for (_opid, subdiff) in diff {
            match subdiff {
                amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                    if s.graphemes(true).count() != 1 {
                        return Err(error::InvalidPatch::InsertNonTextInTextObject {
                            object_id: parent_object_id.clone(),
                            diff: subdiff.clone(),
                        });
                    }
                }
                _ => {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: parent_object_id.clone(),
                        diff: subdiff.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff) {
        self.apply_diff_iter(&mut std::iter::once((opid, diff)))
    }

    pub(super) fn apply_diff_iter<I>(&mut self, diff: &mut I)
    where
        I: Iterator<Item = (amp::OpId, amp::Diff)>,
    {
        for (opid, subdiff) in diff {
            match subdiff {
                amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                    self.update(&opid, s);
                }
                _ => unreachable!("insert non text in text object"),
            }
        }
    }

    fn update(&mut self, key: &amp::OpId, value: String) {
        match key.cmp(&self.winning_value.0) {
            Ordering::Equal => {
                self.winning_value.1 = value;
            }
            Ordering::Greater => {
                self.conflicts
                    .insert(self.winning_value.0.clone(), self.winning_value.1.clone());
                self.winning_value.0 = key.clone();
                self.winning_value.1 = value;
            }
            Ordering::Less => {
                self.conflicts.insert(key.clone(), value);
            }
        }
    }

    pub(super) fn default_grapheme(&self) -> String {
        self.winning_value.1.clone()
    }

    pub fn default_opid(&self) -> &amp::OpId {
        &self.winning_value.0
    }

    fn iter(&self) -> impl std::iter::Iterator<Item = (&amp::OpId, &String)> {
        std::iter::once((&(self.winning_value).0, &(self.winning_value.1)))
            .chain(self.conflicts.iter())
    }

    pub(super) fn realise_values(&self) -> std::collections::HashMap<amp::OpId, Value> {
        self.iter()
            .map(|(opid, v)| (opid.clone(), Value::Primitive(Primitive::Str(v.to_owned()))))
            .collect()
    }

    pub(super) fn has_opid(&self, opid: &amp::OpId) -> bool {
        self.winning_value.0 == *opid || self.conflicts.keys().any(|o| o == opid)
    }

    pub(super) fn only_for_opid(&self, opid: amp::OpId) -> Option<MultiGrapheme> {
        if opid == self.winning_value.0 {
            Some(MultiGrapheme {
                winning_value: self.winning_value.clone(),
                conflicts: HashMap::new(),
            })
        } else {
            self.conflicts.get(&opid).map(|value| MultiGrapheme {
                winning_value: (opid, value.clone()),
                conflicts: HashMap::new(),
            })
        }
    }

    pub(super) fn add_values_from(&mut self, other: MultiGrapheme) {
        for (opid, value) in other.iter() {
            match opid.cmp(&self.winning_value.0) {
                Ordering::Greater => {
                    let mut temp = (opid.clone(), value.to_owned());
                    std::mem::swap(&mut temp, &mut self.winning_value);
                    self.conflicts.insert(temp.0, temp.1);
                }
                Ordering::Less => {
                    self.conflicts.insert(opid.clone(), value.to_owned());
                }
                Ordering::Equal => {}
            }
        }
    }

    pub(crate) fn resolve_path(&self, path: Vec<PathElement>) -> Option<ResolvedPath> {
        if path.is_empty() {
            Some(ResolvedPath::new_character(self))
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(&mut self, path: Vec<PathElement>) -> Option<ResolvedPathMut> {
        if path.is_empty() {
            Some(ResolvedPathMut::new_character(self))
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub(crate) struct NewValueContext<'a, O>
where
    O: Into<amp::ObjectId>,
    O: Clone,
{
    pub(crate) actor: &'a amp::ActorId,
    pub(crate) start_op: u64,
    pub(crate) key: amp::Key,
    pub(crate) parent_obj: O,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<amp::OpId>,
}

impl<'a, O> NewValueContext<'a, O>
where
    O: Into<amp::ObjectId>,
    O: Clone,
{
    fn create(self, value: Value) -> NewValue {
        match value {
            Value::Map(props) => self.new_map_or_table(props, amp::MapType::Map),
            Value::Table(props) => self.new_map_or_table(props, amp::MapType::Table),
            Value::Sequence(values) => self.new_list(values),
            Value::Text(graphemes) => self.new_text(graphemes),
            Value::Primitive(p) => self.new_primitive(p),
        }
    }

    fn new_map_or_table(
        self,
        props: std::collections::HashMap<String, Value>,
        map_type: amp::MapType,
    ) -> NewValue {
        let make_op_id = amp::OpId(self.start_op, self.actor.clone());
        let make_op = amp::Op {
            action: amp::OpType::Make(amp::ObjType::Map(map_type)),
            obj: self.parent_obj.clone().into(),
            key: self.key.clone(),
            insert: self.insert,
            pred: self.pred,
        };
        let mut ops = vec![make_op];
        let mut current_max_op = self.start_op;
        let mut cursors = Cursors::new();
        let mut result_props: HashMap<String, MultiValue> = HashMap::new();
        for (prop, value) in props {
            let context = NewValueContext {
                actor: self.actor,
                parent_obj: &make_op_id,
                start_op: current_max_op + 1,
                key: amp::Key::Map(prop.clone()),
                pred: Vec::new(),
                insert: false,
            };
            let next_value = context.create(value);
            current_max_op = next_value.max_op;
            let (multivalue, new_ops, new_cursors) = next_value.finish();
            cursors.extend(new_cursors);
            ops.extend(new_ops);
            result_props.insert(prop, multivalue);
        }
        let map = match map_type {
            amp::MapType::Map => StateTreeComposite::Map(StateTreeMap {
                object_id: make_op_id.clone().into(),
                props: result_props,
            }),
            amp::MapType::Table => StateTreeComposite::Table(StateTreeTable {
                object_id: make_op_id.clone().into(),
                props: result_props,
            }),
        };
        let value = StateTreeValue::Composite(map);
        NewValue {
            value,
            opid: make_op_id,
            max_op: current_max_op,
            new_cursors: cursors,
            ops,
        }
    }

    fn new_list(self, values: Vec<Value>) -> NewValue {
        let make_list_opid = amp::OpId::new(self.start_op, self.actor);
        let make_op = amp::Op {
            action: amp::OpType::Make(amp::ObjType::list()),
            obj: self.parent_obj.into(),
            key: self.key.clone(),
            insert: self.insert,
            pred: self.pred,
        };
        let mut ops = vec![make_op];
        let mut current_max_op = self.start_op;
        let mut cursors = Cursors::new();
        let mut result_elems: Vec<MultiValue> = Vec::with_capacity(values.len());
        let mut last_elemid = amp::ElementId::Head;
        for value in values {
            let elem_opid = self.actor.op_id_at(current_max_op + 1);
            let context = NewValueContext {
                start_op: current_max_op + 1,
                pred: Vec::new(),
                insert: true,
                key: amp::Key::Seq(last_elemid),
                actor: self.actor,
                parent_obj: make_list_opid.clone(),
            };
            last_elemid = elem_opid.clone().into();
            let next_value = context.create(value);
            current_max_op = next_value.max_op;
            let (multivalue, new_ops, new_cursors) = next_value.finish();
            cursors.extend(new_cursors);
            ops.extend(new_ops);
            result_elems.push(multivalue);
        }
        let list = StateTreeComposite::List(StateTreeList {
            object_id: make_list_opid.clone().into(),
            elements: DiffableSequence::new_from(result_elems),
        });
        let value = StateTreeValue::Composite(list);
        NewValue {
            value,
            opid: make_list_opid,
            max_op: current_max_op,
            new_cursors: cursors,
            ops,
        }
    }

    fn new_text(self, graphemes: Vec<String>) -> NewValue {
        let make_text_opid = self.actor.op_id_at(self.start_op);
        let mut ops: Vec<amp::Op> = vec![amp::Op {
            action: amp::OpType::Make(amp::ObjType::text()),
            obj: self.parent_obj.into(),
            key: self.key.clone(),
            insert: self.insert,
            pred: self.pred,
        }];
        let mut current_max_op = self.start_op;
        let mut last_elemid = amp::ElementId::Head;
        let mut multigraphemes: Vec<MultiGrapheme> = Vec::with_capacity(graphemes.len());
        for grapheme in graphemes.iter() {
            current_max_op += 1;
            let opid = self.actor.op_id_at(current_max_op);
            let op = amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(grapheme.clone())),
                obj: make_text_opid.clone().into(),
                key: amp::Key::Seq(last_elemid),
                insert: true,
                pred: Vec::new(),
            };
            multigraphemes.push(MultiGrapheme::new_from_grapheme_cluster(
                opid.clone(),
                grapheme.clone(),
            ));
            ops.push(op);
            last_elemid = opid.clone().into();
        }
        let seq = DiffableSequence::new_from(multigraphemes);
        let text = StateTreeComposite::Text(StateTreeText {
            object_id: make_text_opid.clone().into(),
            graphemes: seq,
        });
        let value = StateTreeValue::Composite(text);
        NewValue {
            value,
            opid: make_text_opid,
            ops,
            new_cursors: Cursors::new(),
            max_op: current_max_op,
        }
    }

    fn new_primitive(self, primitive: Primitive) -> NewValue {
        let new_cursors = match primitive {
            Primitive::Cursor(ref c) => Cursors::new_from(CursorState {
                index: c.index as usize,
                referring_object_id: self.parent_obj.clone().into(),
                referring_key: self.key.clone(),
                referred_opid: c.elem_opid.clone(),
                referred_object_id: c.object.clone(),
            }),
            _ => Cursors::new(),
        };
        let value = match &primitive {
            Primitive::Bytes(b) => amp::ScalarValue::Bytes(b.clone()),
            Primitive::Str(s) => amp::ScalarValue::Str(s.clone()),
            Primitive::Int(i) => amp::ScalarValue::Int(*i),
            Primitive::Uint(u) => amp::ScalarValue::Uint(*u),
            Primitive::F64(f) => amp::ScalarValue::F64(*f),
            Primitive::F32(f) => amp::ScalarValue::F32(*f),
            Primitive::Counter(i) => amp::ScalarValue::Counter(*i),
            Primitive::Timestamp(t) => amp::ScalarValue::Timestamp(*t),
            Primitive::Boolean(b) => amp::ScalarValue::Boolean(*b),
            Primitive::Cursor(c) => amp::ScalarValue::Cursor(c.elem_opid.clone()),
            Primitive::Null => amp::ScalarValue::Null,
        };
        let opid = self.actor.op_id_at(self.start_op);
        NewValue {
            value: StateTreeValue::Leaf(primitive),
            opid,
            ops: vec![amp::Op {
                action: amp::OpType::Set(value),
                obj: self.parent_obj.into(),
                key: self.key,
                insert: self.insert,
                pred: self.pred.clone(),
            }],
            max_op: self.start_op,
            new_cursors,
        }
    }
}
