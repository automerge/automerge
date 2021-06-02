use std::{collections::HashMap, convert::TryInto};

use amp::{ElementId, MapType};
use automerge_protocol as amp;

use crate::{error, Cursor, Path, PathElement, Primitive, Value};

mod diffable_sequence;
mod multivalue;
mod resolved_path;

use diffable_sequence::DiffableSequence;
use multivalue::{MultiGrapheme, MultiValue, NewValueRequest};
pub(crate) use resolved_path::SetOrInsertPayload;
pub use resolved_path::{ResolvedPath, Target};

/// Represents the result of running a local operation (i.e one that happens within the frontend
/// before any interaction with a backend).
pub(crate) struct LocalOperationResult {
    /// Any operations which need to be sent to the backend to reconcile this change
    pub new_ops: Vec<amp::Op>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTree {
    root_props: im_rc::HashMap<String, MultiValue>,
    cursors: Cursors,
}

impl Default for StateTree {
    fn default() -> Self {
        Self {
            root_props: im_rc::HashMap::new(),
            cursors: Cursors::new(),
        }
    }
}

impl StateTree {
    pub fn new() -> StateTree {
        StateTree {
            root_props: im_rc::HashMap::new(),
            cursors: Cursors::new(),
        }
    }

    pub fn check_diff(&self, diff: &amp::RootDiff) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in &diff.props {
            let mut diff_iter = prop_diff.iter();
            match diff_iter.next() {
                None => {
                    // all ok here
                }
                Some((opid, diff)) => {
                    match self.root_props.get(prop) {
                        Some(n) => n.check_diff(opid, diff)?,
                        None => {
                            MultiValue::check_new_from_diff(opid, diff)?;
                        }
                    };
                    // TODO: somehow get this working
                    // self.root_props
                    //     .get(prop)
                    //     .unwrap()
                    //     .check_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
    }

    pub fn apply_diff(&mut self, diff: amp::RootDiff) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in diff.props {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.root_props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.root_props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff)?,
                        None => {
                            let value = MultiValue::new_from_diff(opid.clone(), diff)?;
                            self.root_props.insert(prop.clone(), value);
                        }
                    };
                    self.root_props
                        .get_mut(&prop)
                        .unwrap()
                        .apply_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
    }

    fn update_cursors(&mut self) {
        // for cursor in self.cursors.iter_mut() {
        //     if let Some(referred_object) = self.objects.get(&cursor.referred_object_id) {
        //         match referred_object {
        //             StateTreeComposite::List(l) => {
        //                 if let Some(index) = l.index_of(&cursor.referred_opid) {
        //                     cursor.index = index;
        //                 }
        //             }
        //             StateTreeComposite::Text(t) => {
        //                 if let Some(index) = t.index_of(&cursor.referred_opid) {
        //                     cursor.index = index;
        //                 }
        //             }
        //             _ => {}
        //         }
        //     }
        //     if let Some(referring_object) = self.objects.get_mut(&cursor.referring_object_id) {
        //         referring_object.mutably_update_cursor(cursor);
        //     }
        // }
    }

    fn remove(&mut self, k: &str) {
        self.root_props.remove(k);
    }

    fn get(&self, k: &str) -> Option<&MultiValue> {
        self.root_props.get(k)
    }

    pub(crate) fn resolve_path<'a>(
        &'a mut self,
        path: &Path,
    ) -> Option<resolved_path::ResolvedPath<'a>> {
        if path.is_root() {
            return Some(ResolvedPath::new_root(self));
        }
        let mut stack = path.clone().elements();
        stack.reverse();

        if let Some(PathElement::Key(k)) = stack.pop() {
            let o = self.root_props.get_mut(&k)?;

            o.resolve_path(stack, amp::ObjectId::Root, amp::Key::Map(k))
        } else {
            None
        }
    }

    pub fn value(&self) -> Value {
        let mut m = HashMap::new();
        for (k, v) in &self.root_props {
            m.insert(k.clone(), v.default_value());
        }
        Value::Map(m, MapType::Map)
    }
}

/// A node in the state tree is either a leaf node containing a scalarvalue,
/// or an internal composite type (e.g a Map or a List)
#[derive(Debug, Clone, PartialEq)]
enum StateTreeValue {
    Leaf(Primitive),
    Composite(StateTreeComposite),
}

#[derive(Debug, Clone, PartialEq)]
enum StateTreeComposite {
    Map(StateTreeMap),
    Table(StateTreeTable),
    Text(StateTreeText),
    List(StateTreeList),
}

impl StateTreeComposite {
    fn check_diff(&self, diff: &amp::Diff) -> Result<(), error::InvalidPatch> {
        if diff_object_id(&diff) != Some(self.object_id()) {
            return Err(error::InvalidPatch::MismatchingObjectIDs {
                patch_expected_id: diff_object_id(&diff),
                actual_id: self.object_id(),
            });
        };
        match diff {
            amp::Diff::Map(amp::MapDiff {
                obj_type,
                props: prop_diffs,
                object_id: _,
            }) => match self {
                StateTreeComposite::Map(map) => {
                    if *obj_type != amp::MapType::Map {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: map.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        map.check_diff(prop_diffs)
                    }
                }
                StateTreeComposite::Table(table) => {
                    if *obj_type != amp::MapType::Table {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: table.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        table.check_diff(prop_diffs)
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: Some(amp::ObjType::Map(*obj_type)),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Seq(amp::SeqDiff {
                edits,
                obj_type,
                object_id: _,
            }) => match self {
                StateTreeComposite::List(list) => {
                    if *obj_type != amp::SequenceType::List {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: list.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        list.check_diff(edits)
                    }
                }
                StateTreeComposite::Text(text) => {
                    if *obj_type != amp::SequenceType::Text {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: text.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        text.check_diff(edits)
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: Some(amp::ObjType::Sequence(*obj_type)),
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

    fn apply_diff(&mut self, diff: amp::Diff) -> Result<(), error::InvalidPatch> {
        if diff_object_id(&diff) != Some(self.object_id()) {
            return Err(error::InvalidPatch::MismatchingObjectIDs {
                patch_expected_id: diff_object_id(&diff),
                actual_id: self.object_id(),
            });
        };
        match diff {
            amp::Diff::Map(amp::MapDiff {
                obj_type,
                props: prop_diffs,
                object_id: _,
            }) => match self {
                StateTreeComposite::Map(map) => {
                    if obj_type != amp::MapType::Map {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: map.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        map.apply_diff(prop_diffs)
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
                        table.apply_diff(prop_diffs)
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: Some(amp::ObjType::Map(obj_type)),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Seq(amp::SeqDiff {
                edits,
                obj_type,
                object_id: _,
            }) => match self {
                StateTreeComposite::List(list) => {
                    if obj_type != amp::SequenceType::List {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: list.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        list.apply_diff(edits)
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
                        text.apply_diff(edits)
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: Some(amp::ObjType::Sequence(obj_type)),
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

    fn realise_value(&self) -> Value {
        match self {
            Self::Map(StateTreeMap { props, .. }) => Value::Map(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value()))
                    .collect(),
                amp::MapType::Map,
            ),
            Self::Table(StateTreeTable { props, .. }) => Value::Map(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value()))
                    .collect(),
                amp::MapType::Table,
            ),
            Self::List(StateTreeList {
                elements: elems, ..
            }) => Value::Sequence(elems.iter().map(|e| e.default_value()).collect()),
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

    fn resolve_path(&mut self, path: Vec<PathElement>) -> Option<ResolvedPath> {
        match self {
            Self::Map(map) => map.resolve_path(path),
            Self::Table(table) => table.resolve_path(path),
            Self::List(list) => list.resolve_path(path),
            Self::Text(text) => text.resolve_path(path),
        }
    }
}

impl StateTreeValue {
    fn check_new_from_diff(diff: &amp::Diff) -> Result<(), error::InvalidPatch> {
        match diff {
            amp::Diff::Value(v) => match v {
                amp::ScalarValue::Bytes(_)
                | amp::ScalarValue::Str(_)
                | amp::ScalarValue::Int(_)
                | amp::ScalarValue::Uint(_)
                | amp::ScalarValue::F64(_)
                | amp::ScalarValue::F32(_)
                | amp::ScalarValue::Counter(_)
                | amp::ScalarValue::Timestamp(_)
                | amp::ScalarValue::Boolean(_)
                | amp::ScalarValue::Null => Ok(()),
                amp::ScalarValue::Cursor(..) => Err(error::InvalidPatch::ValueDiffContainedCursor),
            },
            amp::Diff::Map(amp::MapDiff {
                object_id,
                obj_type,
                props: _,
            }) => {
                let map = match obj_type {
                    amp::MapType::Map => StateTreeComposite::Map(StateTreeMap {
                        object_id: object_id.clone(),
                        props: im_rc::HashMap::new(),
                    }),
                    amp::MapType::Table => StateTreeComposite::Table(StateTreeTable {
                        object_id: object_id.clone(),
                        props: im_rc::HashMap::new(),
                    }),
                };
                map.check_diff(diff)?;
                Ok(())
            }
            amp::Diff::Seq(amp::SeqDiff {
                object_id,
                obj_type,
                edits: _,
            }) => {
                let seq = match obj_type {
                    amp::SequenceType::Text => StateTreeComposite::Text(StateTreeText {
                        object_id: object_id.clone(),
                        graphemes: DiffableSequence::new(),
                    }),
                    amp::SequenceType::List => StateTreeComposite::List(StateTreeList {
                        object_id: object_id.clone(),
                        elements: DiffableSequence::new(),
                    }),
                };
                seq.check_diff(diff)?;
                Ok(())
            }

            amp::Diff::Cursor(_) => Ok(()),
        }
    }

    fn new_from_diff(diff: amp::Diff) -> Result<StateTreeValue, error::InvalidPatch> {
        match diff {
            amp::Diff::Value(v) => {
                let value = match v {
                    amp::ScalarValue::Bytes(b) => Primitive::Bytes(b),
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
                Ok(StateTreeValue::Leaf(value))
            }
            amp::Diff::Map(amp::MapDiff {
                object_id,
                obj_type,
                props,
            }) => {
                let mut map = match obj_type {
                    amp::MapType::Map => StateTreeComposite::Map(StateTreeMap {
                        object_id: object_id.clone(),
                        props: im_rc::HashMap::new(),
                    }),
                    amp::MapType::Table => StateTreeComposite::Table(StateTreeTable {
                        object_id: object_id.clone(),
                        props: im_rc::HashMap::new(),
                    }),
                };
                map.apply_diff(amp::Diff::Map(amp::MapDiff {
                    object_id,
                    obj_type,
                    props,
                }))?;
                Ok(StateTreeValue::Composite(map))
            }
            amp::Diff::Seq(amp::SeqDiff {
                object_id,
                obj_type,
                edits,
            }) => {
                let mut seq = match obj_type {
                    amp::SequenceType::Text => StateTreeComposite::Text(StateTreeText {
                        object_id: object_id.clone(),
                        graphemes: DiffableSequence::new(),
                    }),
                    amp::SequenceType::List => StateTreeComposite::List(StateTreeList {
                        object_id: object_id.clone(),
                        elements: DiffableSequence::new(),
                    }),
                };
                seq.apply_diff(amp::Diff::Seq(amp::SeqDiff {
                    object_id,
                    obj_type,
                    edits,
                }))?;
                Ok(StateTreeValue::Composite(seq))
            }

            amp::Diff::Cursor(ref c) => Ok(StateTreeValue::Leaf(c.into())),
        }
    }

    fn realise_value(&self) -> Value {
        match self {
            StateTreeValue::Leaf(p) => p.clone().into(),
            StateTreeValue::Composite(composite) => composite.realise_value(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeMap {
    object_id: amp::ObjectId,
    props: im_rc::HashMap<String, MultiValue>,
}

impl StateTreeMap {
    fn check_diff(
        &self,
        prop_diffs: &HashMap<String, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {}
                Some((opid, diff)) => {
                    match self.props.get(prop) {
                        Some(n) => n.check_diff(opid, diff)?,
                        None => {
                            MultiValue::check_new_from_diff(opid, diff)?;
                        }
                    };
                    // TODO: get this working
                    // self.props
                    //     .get(prop)
                    //     .unwrap()
                    //     .check_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
    }

    fn apply_diff(
        &mut self,
        prop_diffs: HashMap<String, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff)?,
                        None => {
                            let value = MultiValue::new_from_diff(opid.clone(), diff)?;
                            self.props.insert(prop.clone(), value);
                        }
                    };
                    self.props
                        .get_mut(&prop)
                        .unwrap()
                        .apply_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
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

    pub(crate) fn resolve_path(&mut self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props
                .get_mut(&key)?
                .resolve_path(path, self.object_id.clone(), amp::Key::Map(key))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeTable {
    object_id: amp::ObjectId,
    props: im_rc::HashMap<String, MultiValue>,
}

impl StateTreeTable {
    fn check_diff(
        &self,
        prop_diffs: &HashMap<String, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {}
                Some((opid, diff)) => {
                    match self.props.get(prop) {
                        Some(n) => n.check_diff(opid, diff)?,
                        None => {
                            MultiValue::check_new_from_diff(opid, diff)?;
                        }
                    };
                    // self.props
                    //     .get(prop)
                    //     .unwrap()
                    //     .check_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
    }

    fn apply_diff(
        &mut self,
        prop_diffs: HashMap<String, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff)?,
                        None => {
                            let value = MultiValue::new_from_diff(opid.clone(), diff)?;
                            self.props.insert(prop.clone(), value);
                        }
                    };
                    self.props
                        .get_mut(&prop)
                        .unwrap()
                        .apply_diff_iter(&mut diff_iter)?;
                }
            }
        }
        Ok(())
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

    pub(crate) fn resolve_path(&mut self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props
                .get_mut(&key)?
                .resolve_path(path, self.object_id.clone(), amp::Key::Map(key))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeText {
    object_id: amp::ObjectId,
    graphemes: DiffableSequence<MultiGrapheme>,
}

impl StateTreeText {
    fn remove(&mut self, index: usize) -> Result<(), error::MissingIndexError> {
        if index >= self.graphemes.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        } else {
            self.graphemes.remove(index);
            Ok(())
        }
    }

    fn set(&mut self, index: usize, value: MultiGrapheme) -> Result<(), error::MissingIndexError> {
        if self.graphemes.len() > index {
            self.graphemes.update(index, value);
            Ok(())
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
            .map(|mc| (mc.0, mc.1.default_grapheme()))
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
    }

    fn insert(
        &mut self,
        index: usize,
        value: MultiGrapheme,
    ) -> Result<(), error::MissingIndexError> {
        self.insert_many(index, std::iter::once(value))
    }

    fn insert_many<I>(&mut self, index: usize, values: I) -> Result<(), error::MissingIndexError>
    where
        I: IntoIterator<Item = MultiGrapheme>,
    {
        if index > self.graphemes.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        } else {
            for (i, grapheme) in values.into_iter().enumerate() {
                self.graphemes.insert(index + i, grapheme);
            }
            Ok(())
        }
    }

    fn check_diff(&self, edits: &[amp::DiffEdit]) -> Result<(), error::InvalidPatch> {
        self.graphemes.check_diff(&self.object_id, edits)?;
        Ok(())
    }

    fn apply_diff(&mut self, edits: Vec<amp::DiffEdit>) -> Result<(), error::InvalidPatch> {
        self.graphemes.apply_diff(&self.object_id, edits)?;
        Ok(())
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

    pub(crate) fn resolve_path(&mut self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Index(i)) = path.pop() {
            if path.is_empty() {
                self.graphemes.get_mut(i as usize)?.1.resolve_path(path)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateTreeList {
    object_id: amp::ObjectId,
    elements: DiffableSequence<MultiValue>,
}

impl StateTreeList {
    fn remove(&mut self, index: usize) -> Result<(), error::MissingIndexError> {
        if index >= self.elements.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        } else {
            self.elements.remove(index);
            Ok(())
        }
    }

    fn set(&mut self, index: usize, value: MultiValue) -> Result<(), error::MissingIndexError> {
        if self.elements.len() > index {
            self.elements.update(index, value);
            Ok(())
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        }
    }

    fn insert(&mut self, index: usize, value: MultiValue) -> Result<(), error::MissingIndexError> {
        self.insert_many(index, std::iter::once(value))
    }

    fn insert_many<I>(&mut self, index: usize, values: I) -> Result<(), error::MissingIndexError>
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
            Ok(())
        }
    }

    fn check_diff(&self, edits: &[amp::DiffEdit]) -> Result<(), error::InvalidPatch> {
        self.elements.check_diff(&self.object_id, edits)
    }

    fn apply_diff(&mut self, edits: Vec<amp::DiffEdit>) -> Result<(), error::InvalidPatch> {
        self.elements.apply_diff(&self.object_id, edits)?;
        Ok(())
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
    ) -> Result<(&amp::OpId, &MultiValue), error::MissingIndexError> {
        self.elements
            .get(index)
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
    }

    pub(crate) fn elem_at_mut(
        &mut self,
        index: usize,
    ) -> Result<(&mut amp::OpId, &mut MultiValue), error::MissingIndexError> {
        let len = self.elements.len();
        self.elements
            .get_mut(index)
            .ok_or(error::MissingIndexError {
                missing_index: index,
                size_of_collection: len,
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

    pub(crate) fn resolve_path(&mut self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Index(i)) = path.pop() {
            let elem_id = self
                .elem_at(i as usize)
                .ok()
                .map(|(e, _)| e.into())
                .unwrap_or(ElementId::Head);
            self.elements.get_mut(i as usize)?.1.resolve_path(
                path,
                self.object_id.clone(),
                amp::Key::Seq(elem_id),
            )
        } else {
            None
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
