use std::{borrow::Cow, collections::HashMap, convert::TryInto};

use amp::{MapDiff, ObjectId};
use automerge_protocol as amp;

use crate::{error, Cursor, Path, PathElement, Primitive, Value};

mod diff_application_result;
mod diffable_sequence;
mod focus;
mod multivalue;
mod resolved_path;
mod state_tree_change;

use diff_application_result::DiffApplicationResult;
use diffable_sequence::DiffableSequence;
use focus::Focus;
use multivalue::{MultiGrapheme, MultiValue, NewValueRequest};
pub(crate) use resolved_path::SetOrInsertPayload;
pub use resolved_path::{ResolvedPath, Target};
use state_tree_change::StateTreeChange;

/// Represents the result of running a local operation (i.e one that happens within the frontend
/// before any interaction with a backend).
pub(crate) struct LocalOperationResult {
    /// The new state tree after the operation is executed
    new_state: StateTree,
    /// Any operations which need to be sent to the backend to reconcile this change
    pub new_ops: Vec<amp::Op>,
}

impl LocalOperationResult {
    pub(crate) fn new_state(&self) -> StateTree {
        self.new_state.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTree {
    objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    cursors: Cursors,
}

impl StateTree {
    pub fn new() -> StateTree {
        StateTree {
            objects: im_rc::hashmap! {
                amp::ObjectId::Root => StateTreeComposite::Map(StateTreeMap {
                    object_id: amp::ObjectId::Root,
                    props: im_rc::HashMap::new(),
                })
            },
            cursors: Cursors::new(),
        }
    }

    pub fn apply_root_diff(&mut self, diff: amp::RootDiff) -> Result<(), error::InvalidPatch> {
        self.apply_map_diff(MapDiff {
            object_id: ObjectId::Root,
            obj_type: amp::MapType::Map,
            props: diff.props,
        })
    }

    fn apply_map_diff(&mut self, diff: amp::MapDiff) -> Result<(), error::InvalidPatch> {
        let object = self.objects.get(&diff.object_id).cloned();
        match object {
            Some(StateTreeComposite::Map(mut m)) => {
                let diffapp = m.apply_diff(
                    DiffToApply {
                        parent_key: &"",
                        parent_object_id: &amp::ObjectId::Root,
                        diff: diff.props,
                    },
                    &mut self.objects,
                )?;
                for (id, composite) in diffapp.change.objects() {
                    self.objects.insert(id, composite);
                }
                Ok(())
            }
            Some(o) => Err(error::InvalidPatch::MismatchingObjectType {
                object_id: diff.object_id,
                actual_type: Some(o.obj_type()),
                patch_expected_type: Some(amp::ObjType::map()),
            }),
            None => {
                let mut map = StateTreeMap {
                    object_id: diff.object_id,
                    props: im_rc::HashMap::new(),
                };
                let diffapp = map.apply_diff(
                    DiffToApply {
                        parent_key: &"",
                        parent_object_id: &amp::ObjectId::Root,
                        diff: diff.props,
                    },
                    &mut self.objects,
                )?;
                for (id, composite) in diffapp.change.objects() {
                    self.objects.insert(id, composite);
                }
                Ok(())
            }
        }
    }

    fn update(&mut self, k: String, diffapp: DiffApplicationResult<MultiValue>) -> &mut StateTree {
        for (k, v) in diffapp.change.objects() {
            self.objects.insert(k, v);
        }
        self.cursors = diffapp.change.new_cursors().union(self.cursors.clone());
        match self.objects.get_mut(&amp::ObjectId::Root) {
            Some(StateTreeComposite::Map(root_map)) => root_map.insert(k, diffapp.value),
            _ => panic!("Root map did not exist or was wrong type"),
        };
        self.update_cursors();
        self
    }

    fn update_cursors(&mut self) {
        for cursor in self.cursors.iter_mut() {
            if let Some(referred_object) = self.objects.get(&cursor.referred_object_id) {
                match referred_object {
                    StateTreeComposite::List(l) => {
                        if let Some(index) = l.index_of(&cursor.referred_opid) {
                            cursor.index = index;
                        }
                    }
                    StateTreeComposite::Text(t) => {
                        if let Some(index) = t.index_of(&cursor.referred_opid) {
                            cursor.index = index;
                        }
                    }
                    _ => {}
                }
            }
            if let Some(referring_object) = self.objects.get_mut(&cursor.referring_object_id) {
                referring_object.mutably_update_cursor(cursor);
            }
        }
    }

    fn remove(&mut self, k: &str) {
        match self.objects.get_mut(&amp::ObjectId::Root) {
            Some(StateTreeComposite::Map(root_map)) => {
                root_map.remove(k);
                let root = root_map.clone();
                self.objects
                    .insert(amp::ObjectId::Root, StateTreeComposite::Map(root));
            }
            _ => panic!("Root map did not exist or was wrong type"),
        }
    }

    fn get(&self, k: &str) -> Option<&MultiValue> {
        match self.objects.get(&amp::ObjectId::Root) {
            Some(StateTreeComposite::Map(root)) => root.get(k),
            _ => panic!("Root map did not exist or was wrong type"),
        }
    }

    fn apply(&mut self, change: StateTreeChange) -> StateTree {
        let cursors = change.new_cursors().union(self.cursors.clone());
        let objects = change.objects().union(self.objects.clone());
        let mut new_tree = StateTree { objects, cursors };
        new_tree.update_cursors();
        new_tree
    }

    pub(crate) fn resolve_path(&self, path: &Path) -> Option<resolved_path::ResolvedPath> {
        if path.is_root() {
            return Some(ResolvedPath::new_root(self));
        }
        let mut stack = path.clone().elements();
        stack.reverse();
        if let Some(PathElement::Key(k)) = stack.pop() {
            let mut parent_object_id = amp::ObjectId::Root.clone();
            let mut key_in_container: amp::Key = k.clone().into();
            let first_obj = self.get(&k);
            if let Some(o) = first_obj {
                let mut focus = Focus::new_root(self.clone(), k.clone());
                let mut current_obj: MultiValue = o.clone();
                while let Some(next_elem) = stack.pop() {
                    match next_elem {
                        PathElement::Key(k) => {
                            key_in_container = k.clone().into();
                            match current_obj.default_statetree_value() {
                                StateTreeValue::Link(target_id) => {
                                    match self.objects.get(&target_id) {
                                        Some(StateTreeComposite::Map(map)) => {
                                            if let Some(target) = map.props.get(&k) {
                                                focus = Focus::new_map(
                                                    self.clone(),
                                                    map.clone(),
                                                    k,
                                                    target.clone(),
                                                );
                                                parent_object_id = map.object_id.clone();
                                                current_obj = target.clone();
                                            } else {
                                                return None;
                                            }
                                        }
                                        Some(StateTreeComposite::Table(table)) => {
                                            if let Some(target) = table.props.get(&k) {
                                                parent_object_id = table.object_id.clone();
                                                current_obj = target.clone();
                                                focus = Focus::new_table(
                                                    self.clone(),
                                                    table.clone(),
                                                    k,
                                                    target.clone(),
                                                );
                                            } else {
                                                return None;
                                            }
                                        }
                                        _ => return None,
                                    }
                                }
                                _ => return None,
                            }
                        }
                        PathElement::Index(i) => match current_obj.default_statetree_value() {
                            StateTreeValue::Link(target_id) => match self.objects.get(&target_id) {
                                Some(StateTreeComposite::List(list)) => {
                                    let index = i.try_into().unwrap();
                                    if let Ok((elemid, target)) = list.elem_at(index) {
                                        key_in_container = elemid.into();
                                        parent_object_id = list.object_id.clone();
                                        current_obj = target.clone();
                                        focus = Focus::new_list(
                                            self.clone(),
                                            list.clone(),
                                            i.try_into().unwrap(),
                                            target.clone(),
                                        );
                                    } else {
                                        return None;
                                    }
                                }
                                Some(StateTreeComposite::Text(StateTreeText {
                                    graphemes, ..
                                })) => {
                                    if graphemes.get(i as usize).is_some() {
                                        if stack.is_empty() {
                                            return Some(ResolvedPath::new_character(
                                                self,
                                                current_obj,
                                            ));
                                        } else {
                                            return None;
                                        }
                                    } else {
                                        return None;
                                    };
                                }
                                _ => return None,
                            },
                            _ => return None,
                        },
                    };
                }
                let resolved_path = match current_obj.default_statetree_value() {
                    StateTreeValue::Leaf(v) => match v {
                        Primitive::Counter(v) => ResolvedPath::new_counter(
                            self,
                            parent_object_id,
                            key_in_container,
                            current_obj,
                            focus,
                            v,
                        ),
                        _ => ResolvedPath::new_primitive(self, current_obj),
                    },
                    StateTreeValue::Link(target_id) => match self.objects.get(&target_id) {
                        Some(StateTreeComposite::Map(m)) => {
                            ResolvedPath::new_map(self, current_obj, focus, m.clone())
                        }
                        Some(StateTreeComposite::Table(t)) => {
                            ResolvedPath::new_table(self, current_obj, focus, t.clone())
                        }
                        Some(StateTreeComposite::List(l)) => {
                            ResolvedPath::new_list(self, current_obj, focus, l.clone())
                        }
                        Some(StateTreeComposite::Text(t)) => ResolvedPath::new_text(
                            self,
                            current_obj,
                            Box::new(move |d| focus.update(d)),
                            t.clone(),
                        ),
                        None => return None,
                    },
                };
                Some(resolved_path)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn value(&self) -> Value {
        self.realise_value(&amp::ObjectId::Root).unwrap()
    }

    fn realise_value(&self, object_id: &amp::ObjectId) -> Option<Value> {
        self.objects
            .get(object_id)
            .map(|o| o.realise_value(&self.objects))
    }
}

/// A node in the state tree is either a leaf node containing a scalarvalue,
/// or an internal composite type (e.g a Map or a List)
#[derive(Debug, Clone, PartialEq)]
enum StateTreeValue {
    Leaf(Primitive),
    Link(amp::ObjectId),
}

#[derive(Debug, Clone, PartialEq)]
enum StateTreeComposite {
    Map(StateTreeMap),
    Table(StateTreeTable),
    Text(StateTreeText),
    List(StateTreeList),
}

impl StateTreeComposite {
    fn apply_diff<K>(
        &mut self,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeComposite>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        if diff_object_id(&diff.diff) != Some(self.object_id()) {
            return Err(error::InvalidPatch::MismatchingObjectIDs {
                patch_expected_id: diff_object_id(&diff.diff),
                actual_id: self.object_id(),
            });
        };
        match diff.diff {
            amp::Diff::Map(amp::MapDiff {
                obj_type,
                props: prop_diffs,
                object_id,
            }) => match self {
                StateTreeComposite::Map(map) => {
                    if obj_type != amp::MapType::Map {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: map.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        map.apply_diff(
                            DiffToApply {
                                parent_object_id: diff.parent_object_id,
                                parent_key: diff.parent_key,
                                diff: prop_diffs,
                            },
                            current_objects,
                        )
                        .map(|d| d.map(StateTreeComposite::Map))
                    }
                }
                StateTreeComposite::Table(table) => {
                    if obj_type != amp::MapType::Table {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: table.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        table
                            .apply_diff(
                                DiffToApply {
                                    parent_object_id: diff.parent_object_id,
                                    parent_key: diff.parent_key,
                                    diff: prop_diffs,
                                },
                                current_objects,
                            )
                            .map(|d| d.map(StateTreeComposite::Table))
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: diff_object_type(&amp::Diff::Map(amp::MapDiff {
                        object_id,
                        obj_type,
                        props: prop_diffs,
                    })),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Seq(amp::SeqDiff {
                edits,
                obj_type,
                object_id,
            }) => match self {
                StateTreeComposite::List(list) => {
                    if obj_type != amp::SequenceType::List {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: list.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        list.apply_diff(edits, current_objects)
                            .map(|d| d.map(StateTreeComposite::List))
                    }
                }
                StateTreeComposite::Text(text) => {
                    if obj_type != amp::SequenceType::Text {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: text.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        text.apply_diff(edits, current_objects)
                            .map(|d| d.map(StateTreeComposite::Text))
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: diff_object_type(&amp::Diff::Seq(amp::SeqDiff {
                        object_id,
                        obj_type,
                        edits,
                    })),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Value(..) => {
                // TODO throw an error
                panic!("SHould never be called")
            }
            // TODO throw an error
            amp::Diff::Cursor(..) => panic!("Should never be called"),
        }
    }

    fn obj_type(&self) -> amp::ObjType {
        match self {
            Self::Map(..) => amp::ObjType::map(),
            Self::Table(..) => amp::ObjType::table(),
            Self::Text(..) => amp::ObjType::text(),
            Self::List(..) => amp::ObjType::list(),
        }
    }

    fn object_id(&self) -> amp::ObjectId {
        match self {
            Self::Map(StateTreeMap { object_id, .. }) => object_id.clone(),
            Self::Table(StateTreeTable { object_id, .. }) => object_id.clone(),
            Self::Text(StateTreeText { object_id, .. }) => object_id.clone(),
            Self::List(StateTreeList { object_id, .. }) => object_id.clone(),
        }
    }

    fn realise_value(&self, objects: &im_rc::HashMap<amp::ObjectId, StateTreeComposite>) -> Value {
        match self {
            Self::Map(StateTreeMap { props, .. }) => Value::Map(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value(objects)))
                    .collect(),
                amp::MapType::Map,
            ),
            Self::Table(StateTreeTable { props, .. }) => Value::Map(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value(objects)))
                    .collect(),
                amp::MapType::Table,
            ),
            Self::List(StateTreeList {
                elements: elems, ..
            }) => Value::Sequence(elems.iter().map(|e| e.default_value(objects)).collect()),
            Self::Text(StateTreeText { graphemes, .. }) => {
                Value::Text(graphemes.iter().map(|c| c.default_grapheme()).collect())
            }
        }
    }

    fn mutably_update_cursor(&mut self, cursor: &CursorState) {
        let cursor_value = Primitive::Cursor(Cursor::new(
            cursor.index as u32,
            cursor.referred_object_id.clone(),
            cursor.referred_opid.clone(),
        ));
        match (self, &cursor.referring_key) {
            (StateTreeComposite::Map(m), amp::Key::Map(k)) => {
                m.mutably_update_cursor(&k, cursor_value);
            }
            (StateTreeComposite::Table(t), amp::Key::Map(k)) => {
                t.mutably_update_cursor(&k, cursor_value);
            }
            (StateTreeComposite::List(l), amp::Key::Seq(elem_id)) => {
                l.mutably_update_cursor(&elem_id, cursor_value);
            }
            _ => {}
        }
    }
}

impl StateTreeValue {
    fn new_from_diff<K>(
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeValue>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        let diff_app = match diff.diff {
            amp::Diff::Value(v) => {
                let value = match v {
                    amp::ScalarValue::Str(s) => Primitive::Str(s),
                    amp::ScalarValue::Int(i) => Primitive::Int(i),
                    amp::ScalarValue::Uint(u) => Primitive::Uint(u),
                    amp::ScalarValue::F64(f) => Primitive::F64(f),
                    amp::ScalarValue::F32(f) => Primitive::F32(f),
                    amp::ScalarValue::Counter(i) => Primitive::Counter(i),
                    amp::ScalarValue::Timestamp(i) => Primitive::Timestamp(i),
                    amp::ScalarValue::Boolean(b) => Primitive::Boolean(b),
                    amp::ScalarValue::Null => Primitive::Null,
                    amp::ScalarValue::Cursor(..) => {
                        return Err(error::InvalidPatch::ValueDiffContainedCursor)
                    }
                };
                DiffApplicationResult::pure(StateTreeValue::Leaf(value))
            }
            amp::Diff::Map(amp::MapDiff {
                ref object_id,
                obj_type,
                ..
            }) => match obj_type {
                amp::MapType::Map => StateTreeComposite::Map(StateTreeMap {
                    object_id: object_id.clone(),
                    props: im_rc::HashMap::new(),
                }),
                amp::MapType::Table => StateTreeComposite::Table(StateTreeTable {
                    object_id: object_id.clone(),
                    props: im_rc::HashMap::new(),
                }),
            }
            .apply_diff(diff, current_objects)?
            .map(|c| StateTreeValue::Link(c.object_id())),
            amp::Diff::Seq(amp::SeqDiff {
                ref object_id,
                obj_type,
                ..
            }) => match obj_type {
                amp::SequenceType::Text => StateTreeComposite::Text(StateTreeText {
                    object_id: object_id.clone(),
                    graphemes: DiffableSequence::new(),
                }),
                amp::SequenceType::List => StateTreeComposite::List(StateTreeList {
                    object_id: object_id.clone(),
                    elements: DiffableSequence::new(),
                }),
            }
            .apply_diff(diff, current_objects)?
            .map(|c| StateTreeValue::Link(c.object_id())),
            amp::Diff::Cursor(ref c) => DiffApplicationResult::pure(StateTreeValue::Leaf(c.into())),
        };
        Ok(diff_app)
    }

    fn realise_value(&self, objects: &im_rc::HashMap<amp::ObjectId, StateTreeComposite>) -> Value {
        match self {
            StateTreeValue::Leaf(p) => p.clone().into(),
            StateTreeValue::Link(target_id) => objects
                .get(target_id)
                .expect("missing object")
                .realise_value(objects),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeMap {
    object_id: amp::ObjectId,
    props: im_rc::HashMap<String, MultiValue>,
}

impl StateTreeMap {
    fn insert(&mut self, key: String, value: MultiValue) {
        self.props.insert(key, value);
    }

    fn remove(&mut self, key: &str) {
        self.props.remove(key);
    }

    fn get<S: AsRef<str>>(&self, key: S) -> Option<&MultiValue> {
        self.props.get(key.as_ref())
    }

    fn apply_diff<K>(
        &mut self,
        prop_diffs: DiffToApply<K, HashMap<String, HashMap<amp::OpId, amp::Diff>>>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeMap>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        for (prop, prop_diff) in prop_diffs.diff {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    let node = match self.props.get_mut(&prop) {
                        Some(n) => {
                            let diff_result = n.apply_diff(
                                &opid,
                                DiffToApply {
                                    parent_key: &prop,
                                    parent_object_id: &self.object_id,
                                    diff,
                                },
                                current_objects,
                            )?;

                            for (id, composite) in diff_result.change.objects() {
                                current_objects.insert(id, composite);
                            }

                            self.props.insert(prop.clone(), diff_result.value.clone());
                            diff_result.value
                        }
                        None => {
                            let diff_result = MultiValue::new_from_diff(
                                opid.clone(),
                                DiffToApply {
                                    parent_key: &prop,
                                    parent_object_id: &self.object_id,
                                    diff,
                                },
                                current_objects,
                            )?;

                            for (id, composite) in diff_result.change.objects() {
                                current_objects.insert(id, composite);
                            }

                            self.props.insert(prop.clone(), diff_result.value.clone());
                            diff_result.value
                        }
                    };
                    let other_changes = node.apply_diff_iter(
                        &mut diff_iter.map(|(oid, diff)| {
                            (
                                Cow::Owned(oid),
                                DiffToApply {
                                    parent_key: &prop,
                                    parent_object_id: &self.object_id,
                                    diff,
                                },
                            )
                        }),
                        current_objects,
                    )?;

                    for (id, composite) in other_changes.change.objects() {
                        current_objects.insert(id, composite);
                    }

                    self.props.insert(prop.clone(), other_changes.value);
                }
            }
        }

        Ok(
            DiffApplicationResult::pure(self.clone()).with_changes(StateTreeChange::single(
                self.object_id.clone(),
                StateTreeComposite::Map(self.clone()),
            )),
        )
    }

    pub fn pred_for_key(&self, key: &str) -> Vec<amp::OpId> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new)
    }

    pub fn mutably_update_cursor(&mut self, key: &str, cursor: Primitive) {
        let new_mv = self
            .props
            .get(key)
            .map(|mv| mv.update_default(StateTreeValue::Leaf(cursor)));
        if let Some(new_mv) = new_mv {
            self.props.insert(key.to_string(), new_mv);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeTable {
    object_id: amp::ObjectId,
    props: im_rc::HashMap<String, MultiValue>,
}

impl StateTreeTable {
    fn update(&self, key: String, value: MultiValue) -> StateTreeTable {
        StateTreeTable {
            object_id: self.object_id.clone(),
            props: self.props.update(key, value),
        }
    }

    fn without(&self, key: &str) -> StateTreeTable {
        StateTreeTable {
            object_id: self.object_id.clone(),
            props: self.props.without(key),
        }
    }

    fn apply_diff<K>(
        &self,
        prop_diffs: DiffToApply<K, HashMap<String, HashMap<amp::OpId, amp::Diff>>>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeTable>, error::InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        let mut new_props = self.props.clone();
        let mut changes = StateTreeChange::empty();
        for (prop, prop_diff) in prop_diffs.diff {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => new_props = new_props.without(&prop),
                Some((opid, diff)) => {
                    let mut node_diffapp = match new_props.get_mut(&prop) {
                        Some(n) => n.apply_diff(
                            &opid,
                            DiffToApply {
                                parent_object_id: &self.object_id,
                                parent_key: &prop,
                                diff,
                            },
                            current_objects,
                        )?,
                        None => MultiValue::new_from_diff(
                            opid.clone(),
                            DiffToApply {
                                parent_object_id: &self.object_id,
                                parent_key: &prop,
                                diff,
                            },
                            current_objects,
                        )?,
                    };
                    node_diffapp = node_diffapp.try_and_then(|n| {
                        n.apply_diff_iter(
                            &mut diff_iter.map(|(oid, diff)| {
                                (
                                    Cow::Owned(oid),
                                    DiffToApply {
                                        parent_object_id: &self.object_id,
                                        parent_key: &prop,
                                        diff,
                                    },
                                )
                            }),
                            current_objects,
                        )
                    })?;
                    changes.update_with(node_diffapp.change);
                    new_props.insert(prop.to_string(), node_diffapp.value);
                }
            }
        }
        let new_table = StateTreeTable {
            object_id: self.object_id.clone(),
            props: new_props,
        };
        Ok(
            DiffApplicationResult::pure(new_table.clone()).with_changes(StateTreeChange::single(
                self.object_id.clone(),
                StateTreeComposite::Table(new_table),
            )),
        )
    }

    pub fn pred_for_key(&self, key: &str) -> Vec<amp::OpId> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new)
    }

    pub fn mutably_update_cursor(&mut self, key: &str, cursor: Primitive) {
        let new_mv = self
            .props
            .get(key)
            .map(|mv| mv.update_default(StateTreeValue::Leaf(cursor)));
        if let Some(new_mv) = new_mv {
            self.props.insert(key.to_string(), new_mv);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeText {
    object_id: amp::ObjectId,
    graphemes: DiffableSequence<MultiGrapheme>,
}

impl StateTreeText {
    fn remove(&self, index: usize) -> Result<StateTreeText, error::MissingIndexError> {
        if index >= self.graphemes.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        } else {
            let mut new_chars = self.graphemes.clone();
            new_chars.remove(index);
            Ok(StateTreeText {
                object_id: self.object_id.clone(),
                graphemes: new_chars,
            })
        }
    }

    fn set(
        &mut self,
        index: usize,
        value: MultiGrapheme,
    ) -> Result<StateTreeText, error::MissingIndexError> {
        if self.graphemes.len() > index {
            self.graphemes.update(index, value);

            Ok(self.clone())
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        }
    }

    pub(crate) fn elem_at(
        &self,
        index: usize,
    ) -> Result<(&amp::OpId, String), error::MissingIndexError> {
        self.graphemes
            .get(index)
            .map(|mc| (&mc.0, mc.1.default_grapheme()))
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
    }

    fn insert(
        &self,
        index: usize,
        value: MultiGrapheme,
    ) -> Result<StateTreeText, error::MissingIndexError> {
        self.insert_many(index, std::iter::once(value))
    }

    fn insert_many<I>(
        &self,
        index: usize,
        values: I,
    ) -> Result<StateTreeText, error::MissingIndexError>
    where
        I: IntoIterator<Item = MultiGrapheme>,
    {
        if index > self.graphemes.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        } else {
            let mut new_chars = self.graphemes.clone();
            for (i, grapheme) in values.into_iter().enumerate() {
                new_chars.insert(index + i, grapheme);
            }
            Ok(StateTreeText {
                object_id: self.object_id.clone(),
                graphemes: new_chars,
            })
        }
    }

    fn apply_diff(
        &mut self,
        edits: Vec<amp::DiffEdit>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeText>, error::InvalidPatch> {
        let new_graphemes = self
            .graphemes
            .apply_diff(&self.object_id, edits, current_objects)?;
        Ok(new_graphemes.and_then(|new_graphemes| {
            let text = StateTreeText {
                object_id: self.object_id.clone(),
                graphemes: new_graphemes,
            };
            DiffApplicationResult::pure(text.clone()).with_changes(StateTreeChange::single(
                self.object_id.clone(),
                StateTreeComposite::Text(text),
            ))
        }))
    }

    pub fn pred_for_index(&self, index: u32) -> Vec<amp::OpId> {
        self.graphemes
            .get(index.try_into().unwrap())
            .map(|v| vec![v.1.default_opid().clone()])
            .unwrap_or_else(Vec::new)
    }

    pub(crate) fn index_of(&self, opid: &amp::OpId) -> Option<usize> {
        self.graphemes.iter().position(|e| e.has_opid(opid))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeList {
    object_id: amp::ObjectId,
    elements: DiffableSequence<MultiValue>,
}

impl StateTreeList {
    fn remove(&self, index: usize) -> Result<StateTreeList, error::MissingIndexError> {
        if index >= self.elements.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        } else {
            let mut new_elems = self.elements.clone();
            new_elems.remove(index);
            Ok(StateTreeList {
                object_id: self.object_id.clone(),
                elements: new_elems,
            })
        }
    }

    fn set(
        &mut self,
        index: usize,
        value: MultiValue,
    ) -> Result<StateTreeList, error::MissingIndexError> {
        if self.elements.len() > index {
            self.elements.update(index, value);
            Ok(self.clone())
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        }
    }

    fn insert(
        &mut self,
        index: usize,
        value: MultiValue,
    ) -> Result<StateTreeList, error::MissingIndexError> {
        self.insert_many(index, std::iter::once(value))
    }

    fn insert_many<I>(
        &mut self,
        index: usize,
        values: I,
    ) -> Result<StateTreeList, error::MissingIndexError>
    where
        I: IntoIterator<Item = MultiValue>,
    {
        if index > self.elements.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        } else {
            for (i, value) in values.into_iter().enumerate() {
                self.elements.insert(index + i, value);
            }
            Ok(StateTreeList {
                object_id: self.object_id.clone(),
                elements: self.elements.clone(),
            })
        }
    }

    fn apply_diff(
        &mut self,
        edits: Vec<amp::DiffEdit>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<StateTreeList>, error::InvalidPatch> {
        let new_elements = self
            .elements
            .apply_diff(&self.object_id, edits, current_objects)?;
        Ok(new_elements.and_then(|new_elements| {
            let new_list = StateTreeList {
                object_id: self.object_id.clone(),
                elements: new_elements,
            };
            DiffApplicationResult::pure(new_list.clone()).with_changes(StateTreeChange::single(
                self.object_id.clone(),
                StateTreeComposite::List(new_list),
            ))
        }))
    }

    pub fn pred_for_index(&self, index: u32) -> Vec<amp::OpId> {
        self.elements
            .get(index.try_into().unwrap())
            .map(|v| vec![v.1.default_opid()])
            .unwrap_or_else(Vec::new)
    }

    pub(crate) fn elem_at(
        &self,
        index: usize,
    ) -> Result<&(amp::OpId, MultiValue), error::MissingIndexError> {
        self.elements
            .get(index)
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
    }

    pub(crate) fn index_of(&self, opid: &amp::OpId) -> Option<usize> {
        self.elements.iter().position(|e| e.has_opid(opid))
    }

    fn mutably_update_cursor(&mut self, key: &amp::ElementId, cursor: Primitive) {
        if let amp::ElementId::Id(oid) = key {
            if let Some(index) = self.index_of(oid) {
                self.elements
                    .mutate(index, |m| m.update_default(StateTreeValue::Leaf(cursor)))
            }
        }
    }
}

/// Helper method to get the object type of an amp::Diff
fn diff_object_type(diff: &amp::Diff) -> Option<amp::ObjType> {
    match diff {
        amp::Diff::Map(mapdiff) => Some(amp::ObjType::Map(mapdiff.obj_type)),
        amp::Diff::Seq(seqdiff) => Some(amp::ObjType::Sequence(seqdiff.obj_type)),
        amp::Diff::Value(..) => None,
        amp::Diff::Cursor(..) => None,
    }
}

/// Helper method to get the object ID of an amp::Diff
fn diff_object_id(diff: &amp::Diff) -> Option<amp::ObjectId> {
    match diff {
        amp::Diff::Map(mapdiff) => Some(mapdiff.object_id.clone()),
        amp::Diff::Seq(seqdiff) => Some(seqdiff.object_id.clone()),
        amp::Diff::Value(..) => None,
        amp::Diff::Cursor(amp::CursorDiff { object_id, .. }) => Some(object_id.clone()),
    }
}

pub fn random_op_id() -> amp::OpId {
    amp::OpId::new(1, &amp::ActorId::random())
}

struct DiffToApply<'a, K, T>
where
    K: Into<amp::Key>,
{
    diff: T,
    parent_object_id: &'a amp::ObjectId,
    parent_key: &'a K,
}

#[derive(Clone, Debug, PartialEq)]
struct CursorState {
    referring_object_id: amp::ObjectId,
    referring_key: amp::Key,
    referred_object_id: amp::ObjectId,
    referred_opid: amp::OpId,
    index: usize,
}

#[derive(Debug, PartialEq, Clone)]
struct Cursors(im_rc::HashMap<amp::ObjectId, Vec<CursorState>>);

impl Cursors {
    fn new() -> Cursors {
        Cursors(im_rc::HashMap::new())
    }

    fn new_from(cursor: CursorState) -> Cursors {
        Cursors(im_rc::hashmap! {
            cursor.referred_object_id.clone() => vec![cursor],
        })
    }

    fn union(&self, other: Cursors) -> Cursors {
        Cursors(self.0.clone().union_with(other.0, |mut c1, c2| {
            c1.extend(c2);
            c1
        }))
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut CursorState> {
        self.0.iter_mut().flat_map(|e| e.1)
    }
}
