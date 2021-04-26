use std::iter::Iterator;

use automerge_protocol as amp;
use unicode_segmentation::UnicodeSegmentation;

use super::{
    CursorState, Cursors, DiffApplicationResult, DiffToApply, DiffableSequence, StateTreeChange,
    StateTreeComposite, StateTreeList, StateTreeMap, StateTreeTable, StateTreeText, StateTreeValue,
};
use crate::{
    error,
    value::{Primitive, Value},
};

pub(crate) struct NewValueRequest<'a, 'b, 'c, 'd> {
    pub(crate) actor: &'a amp::ActorId,
    pub(crate) start_op: u64,
    pub(crate) key: &'b amp::Key,
    pub(crate) value: &'c Value,
    pub(crate) parent_obj: &'d amp::ObjectId,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<amp::OpId>,
}

/// A set of conflicting values for the same key, indexed by OpID
#[derive(Debug, Clone)]
pub(super) struct MultiValue {
    winning_value: (amp::OpId, StateTreeValue),
    conflicts: im_rc::HashMap<amp::OpId, StateTreeValue>,
}

impl MultiValue {
    pub fn new_from_diff<K>(
        opid: amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
    ) -> Result<DiffApplicationResult<MultiValue>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        StateTreeValue::new_from_diff(diff)?.try_map(move |value| {
            Ok(MultiValue {
                winning_value: (opid, value),
                conflicts: im_rc::HashMap::new(),
            })
        })
    }

    pub fn from_statetree_value(statetree_val: StateTreeValue, opid: amp::OpId) -> MultiValue {
        MultiValue {
            winning_value: (opid, statetree_val),
            conflicts: im_rc::HashMap::new(),
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
            req.pred.into_iter().collect(),
        )
    }

    pub(super) fn new_from_value(
        actor: &amp::ActorId,
        start_op: u64,
        parent_id: amp::ObjectId,
        key: &amp::Key,
        value: &Value,
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

    pub(super) fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        subdiff: DiffToApply<K, &amp::Diff>,
    ) -> Result<DiffApplicationResult<MultiValue>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        self.apply_diff_iter(&mut std::iter::once((opid, subdiff)))
    }

    pub(super) fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
    ) -> Result<DiffApplicationResult<MultiValue>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>,
    {
        let mut changes = StateTreeChange::empty();
        let mut updated = self.tree_values();
        for (opid, subdiff) in diff {
            let u = if let Some(existing_value) = updated.get(opid) {
                match existing_value {
                    StateTreeValue::Leaf(_) => StateTreeValue::new_from_diff(subdiff),
                    StateTreeValue::Link(obj_id) => subdiff
                        .current_objects
                        .get(obj_id)
                        .expect("link to nonexistent object")
                        .apply_diff(&subdiff)
                        .map(|c| c.map(|c| StateTreeValue::Link(c.object_id()))),
                }
            } else {
                StateTreeValue::new_from_diff(subdiff)
            }?;
            changes += u.change;
            updated = updated.update(opid, &u.value)
        }
        Ok(DiffApplicationResult::pure(updated.result()).with_changes(changes))
    }

    pub(super) fn default_statetree_value(&self) -> StateTreeValue {
        self.winning_value.1.clone()
    }

    pub(super) fn default_value(
        &self,
        objects: &im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Value {
        self.winning_value.1.realise_value(objects)
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

    fn tree_values(&self) -> MultiValueTreeValues {
        MultiValueTreeValues {
            current: self.clone(),
        }
    }

    pub(super) fn realise_values(
        &self,
        objects: &im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> std::collections::HashMap<amp::OpId, Value> {
        self.tree_values()
            .iter()
            .map(|(opid, v)| (opid.clone(), v.realise_value(objects)))
            .collect()
    }

    pub(super) fn opids(&self) -> impl Iterator<Item = &amp::OpId> {
        std::iter::once(&self.winning_value.0).chain(self.conflicts.keys())
    }

    pub(super) fn has_opid(&self, opid: &amp::OpId) -> bool {
        self.opids().any(|o| o == opid)
    }
}

#[derive(Clone)]
struct MultiValueTreeValues {
    current: MultiValue,
}

impl MultiValueTreeValues {
    fn get(&self, opid: &amp::OpId) -> Option<&StateTreeValue> {
        if opid == &self.current.winning_value.0 {
            Some(&self.current.winning_value.1)
        } else {
            self.current.conflicts.get(opid)
        }
    }

    fn iter(&self) -> impl std::iter::Iterator<Item = (&amp::OpId, &StateTreeValue)> {
        std::iter::once((
            &(self.current.winning_value).0,
            &(self.current.winning_value.1),
        ))
        .chain(self.current.conflicts.iter())
    }

    fn update(mut self, key: &amp::OpId, value: &StateTreeValue) -> MultiValueTreeValues {
        if *key >= self.current.winning_value.0 {
            self.current
                .conflicts
                .insert(self.current.winning_value.0, self.current.winning_value.1);
            self.current.winning_value.0 = key.clone();
            self.current.winning_value.1 = value.clone();
        } else {
            self.current.conflicts.insert(key.clone(), value.clone());
        }
        self
    }

    fn result(self) -> MultiValue {
        self.current
    }
}

#[derive(Debug)]
pub(super) struct NewValue {
    value: StateTreeValue,
    opid: amp::OpId,
    ops: Vec<amp::Op>,
    new_objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    new_cursors: Cursors,
    max_op: u64,
}

impl NewValue {
    pub(super) fn ops(self) -> Vec<amp::Op> {
        self.ops
    }

    fn multivalue(&self) -> MultiValue {
        MultiValue::from_statetree_value(self.value.clone(), self.opid.clone())
    }

    pub(super) fn diff_app_result(&self) -> DiffApplicationResult<MultiValue> {
        DiffApplicationResult::pure(self.multivalue()).with_changes(
            StateTreeChange::from_updates(self.new_objects.clone())
                .with_cursors(self.new_cursors.clone()),
        )
    }
}

/// This struct exists to constrain the values of a text type to just containing
/// sequences of grapheme clusters
#[derive(Debug, Clone)]
pub(super) struct MultiGrapheme {
    winning_value: (amp::OpId, String),
    conflicts: Option<im_rc::HashMap<amp::OpId, String>>,
}

impl MultiGrapheme {
    pub(super) fn new_from_grapheme_cluster(opid: amp::OpId, s: String) -> MultiGrapheme {
        debug_assert_eq!(s.graphemes(true).count(), 1);
        MultiGrapheme {
            winning_value: (opid, s),
            conflicts: None,
        }
    }

    pub(super) fn new_from_diff<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
    ) -> Result<MultiGrapheme, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        let winning_value = match diff.diff {
            amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                if s.graphemes(true).count() != 1 {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: diff.parent_object_id.clone(),
                        diff: diff.diff.clone(),
                    });
                } else {
                    s.clone()
                }
            }
            _ => {
                return Err(error::InvalidPatch::InsertNonTextInTextObject {
                    object_id: diff.parent_object_id.clone(),
                    diff: diff.diff.clone(),
                });
            }
        };
        Ok(MultiGrapheme {
            winning_value: (opid.clone(), winning_value),
            conflicts: None,
        })
    }

    pub(super) fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
    ) -> Result<MultiGrapheme, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        self.apply_diff_iter(&mut std::iter::once((opid, diff)))
            .map(|d| d.value)
    }

    pub(super) fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
    ) -> Result<DiffApplicationResult<MultiGrapheme>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>,
    {
        let mut updated = self.values();
        for (opid, subdiff) in diff {
            match subdiff.diff {
                amp::Diff::Value(amp::ScalarValue::Str(s)) => {
                    if s.graphemes(true).count() != 1 {
                        return Err(error::InvalidPatch::InsertNonTextInTextObject {
                            object_id: subdiff.parent_object_id.clone(),
                            diff: subdiff.diff.clone(),
                        });
                    } else {
                        updated = updated.update(opid, s.clone());
                    }
                }
                _ => {
                    return Err(error::InvalidPatch::InsertNonTextInTextObject {
                        object_id: subdiff.parent_object_id.clone(),
                        diff: subdiff.diff.clone(),
                    })
                }
            }
        }
        Ok(DiffApplicationResult::pure(updated.result()))
    }

    pub(super) fn default_grapheme(&self) -> String {
        self.winning_value.1.clone()
    }

    pub fn default_opid(&self) -> &amp::OpId {
        &self.winning_value.0
    }

    fn values(&self) -> MultiGraphemeValues {
        MultiGraphemeValues {
            current: self.clone(),
        }
    }

    pub(super) fn has_opid(&self, opid: &amp::OpId) -> bool {
        if let Some(ref conflicts) = self.conflicts {
            let mut opids = std::iter::once(&self.winning_value.0).chain(conflicts.keys());
            opids.any(|o| o == opid)
        } else {
            self.winning_value.0 == *opid
        }
    }
}

struct MultiGraphemeValues {
    current: MultiGrapheme,
}

impl MultiGraphemeValues {
    fn update(mut self, key: &amp::OpId, value: String) -> MultiGraphemeValues {
        let mut conflicts = self.current.conflicts.unwrap_or_else(im_rc::HashMap::new);
        if *key >= self.current.winning_value.0 {
            conflicts.insert(self.current.winning_value.0, self.current.winning_value.1);
            self.current.winning_value.0 = key.clone();
            self.current.winning_value.1 = value;
        } else {
            conflicts.insert(key.clone(), value);
        }
        self.current.conflicts = Some(conflicts);
        self
    }

    fn result(self) -> MultiGrapheme {
        self.current
    }
}

#[derive(Clone)]
pub(crate) struct NewValueContext<'a, 'b, O>
where
    O: Into<amp::ObjectId>,
    O: Clone,
{
    pub(crate) actor: &'a amp::ActorId,
    pub(crate) start_op: u64,
    pub(crate) key: &'b amp::Key,
    pub(crate) parent_obj: O,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<amp::OpId>,
}

impl<'a, 'b, O> NewValueContext<'a, 'b, O>
where
    O: Into<amp::ObjectId>,
    O: Clone,
{
    fn create(self, value: &Value) -> NewValue {
        match value {
            Value::Map(props, map_type) => self.new_map_or_table(props, map_type),
            Value::Sequence(values) => self.new_list(values),
            Value::Text(graphemes) => self.new_text(graphemes),
            Value::Primitive(p) => self.new_primitive(p),
        }
    }

    fn new_map_or_table(
        self,
        props: &std::collections::HashMap<String, Value>,
        map_type: &amp::MapType,
    ) -> NewValue {
        let make_op_id = amp::OpId(self.start_op, self.actor.clone());
        let make_op = amp::Op {
            action: amp::OpType::Make(amp::ObjType::Map(*map_type)),
            obj: self.parent_obj.clone().into(),
            key: self.key.clone(),
            insert: self.insert,
            pred: self.pred.clone(),
        };
        let mut ops = vec![make_op];
        let mut current_max_op = self.start_op;
        let mut cursors = Cursors::new();
        let mut objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite> = im_rc::HashMap::new();
        let mut result_props: im_rc::HashMap<String, MultiValue> = im_rc::HashMap::new();
        for (prop, value) in props.iter() {
            let context = NewValueContext {
                actor: self.actor,
                parent_obj: &make_op_id,
                start_op: current_max_op + 1,
                key: &prop.into(),
                pred: Vec::new(),
                insert: false,
            };
            let next_value = context.create(value);
            current_max_op = next_value.max_op;
            cursors = next_value.new_cursors.clone().union(cursors);
            objects = next_value.new_objects.clone().union(objects.clone());
            ops.extend_from_slice(&next_value.ops[..]);
            result_props = result_props.update(prop.clone(), next_value.multivalue())
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
        let value = StateTreeValue::Link(make_op_id.clone().into());
        objects = objects.update(make_op_id.clone().into(), map);
        NewValue {
            value,
            opid: make_op_id,
            max_op: current_max_op,
            new_cursors: cursors,
            new_objects: objects,
            ops,
        }
    }

    fn new_list(self, values: &[Value]) -> NewValue {
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
        let mut objects = im_rc::HashMap::new();
        let mut result_elems: Vec<(amp::OpId, MultiValue)> = Vec::with_capacity(values.len());
        let mut last_elemid = amp::ElementId::Head;
        for value in values.iter() {
            let elem_opid = self.actor.op_id_at(current_max_op + 1);
            let context = NewValueContext {
                start_op: current_max_op + 1,
                pred: Vec::new(),
                insert: true,
                key: &last_elemid.into(),
                actor: self.actor,
                parent_obj: make_list_opid.clone(),
            };
            last_elemid = elem_opid.clone().into();
            let next_value = context.create(value);
            current_max_op = next_value.max_op;
            result_elems.push((elem_opid, next_value.multivalue()));
            objects = next_value.new_objects.union(objects.clone());
            cursors = next_value.new_cursors.union(cursors);
            ops.extend(next_value.ops);
        }
        let list = StateTreeComposite::List(StateTreeList {
            object_id: make_list_opid.clone().into(),
            elements: DiffableSequence::new_from(result_elems),
        });
        objects = objects.update(make_list_opid.clone().into(), list);
        let value = StateTreeValue::Link(make_list_opid.clone().into());
        NewValue {
            value,
            opid: make_list_opid,
            max_op: current_max_op,
            new_cursors: cursors,
            new_objects: objects,
            ops,
        }
    }

    fn new_text(self, graphemes: &[String]) -> NewValue {
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
        let mut multigraphemes: Vec<(amp::OpId, MultiGrapheme)> =
            Vec::with_capacity(graphemes.len());
        for grapheme in graphemes.iter() {
            current_max_op += 1;
            let opid = self.actor.op_id_at(current_max_op);
            let op = amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(grapheme.clone())),
                obj: make_text_opid.clone().into(),
                key: last_elemid.clone().into(),
                insert: true,
                pred: Vec::new(),
            };
            multigraphemes.push((
                opid.clone(),
                MultiGrapheme::new_from_grapheme_cluster(opid.clone(), grapheme.clone()),
            ));
            ops.push(op);
            last_elemid = opid.clone().into();
        }
        let seq = DiffableSequence::new_from(multigraphemes);
        let text = StateTreeComposite::Text(StateTreeText {
            object_id: make_text_opid.clone().into(),
            graphemes: seq,
        });
        let value = StateTreeValue::Link(make_text_opid.clone().into());
        NewValue {
            value,
            opid: make_text_opid.clone(),
            ops,
            new_objects: im_rc::hashmap! {make_text_opid.into() => text},
            new_cursors: Cursors::new(),
            max_op: current_max_op,
        }
    }

    fn new_primitive(self, primitive: &Primitive) -> NewValue {
        let new_cursors = match primitive {
            Primitive::Cursor(c) => Cursors::new_from(CursorState {
                index: c.index as usize,
                referring_object_id: self.parent_obj.clone().into(),
                referring_key: self.key.clone(),
                referred_opid: c.elem_opid.clone(),
                referred_object_id: c.object.clone(),
            }),
            _ => Cursors::new(),
        };
        let value = match primitive {
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
            value: StateTreeValue::Leaf(primitive.clone()),
            opid,
            ops: vec![amp::Op {
                action: amp::OpType::Set(value),
                obj: self.parent_obj.into(),
                key: self.key.clone(),
                insert: self.insert,
                pred: self.pred.clone(),
            }],
            max_op: self.start_op,
            new_cursors,
            new_objects: im_rc::HashMap::new(),
        }
    }
}
