use std::{
    collections::{BTreeMap, HashMap},
    convert::TryInto,
};

use amp::{ElementId, SortedVec};
use automerge_protocol as amp;
use automerge_protocol::RootDiff;
use diffable_sequence::DiffableSequence;
use multivalue::NewValueRequest;
use smol_str::SmolStr;

use crate::{
    error, frontend::Schema, path::PathElement, value_ref::RootRef, Path, Primitive, Value,
};

mod diffable_sequence;
mod multivalue;
mod optimistic;
mod resolved_path;

pub use multivalue::{MultiGrapheme, MultiValue};
pub(crate) use optimistic::{LocalOperationForRollback, OptimisticStateTree};
pub(crate) use resolved_path::SetOrInsertPayload;
pub use resolved_path::{ResolvedPath, ResolvedPathMut};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct CheckedRootDiff(RootDiff);

/// Represents the result of running a local operation (i.e one that happens within the frontend
/// before any interaction with a backend).
pub(crate) struct LocalOperationResult {
    /// Any operations which need to be sent to the backend to reconcile this change
    pub new_ops: Vec<amp::Op>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTree {
    pub(crate) root_props: HashMap<SmolStr, MultiValue>,
    cursors: Cursors,
}

impl Default for StateTree {
    fn default() -> Self {
        Self {
            root_props: HashMap::new(),
            cursors: Cursors::new(),
        }
    }
}

impl StateTree {
    pub fn new() -> StateTree {
        StateTree {
            root_props: HashMap::new(),
            cursors: Cursors::new(),
        }
    }

    pub fn check_diff(&self, diff: amp::RootDiff) -> Result<CheckedRootDiff, error::InvalidPatch> {
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
        Ok(CheckedRootDiff(diff))
    }

    pub fn apply_diff(&mut self, diff: CheckedRootDiff, schema: &Schema) {
        for (prop, prop_diff) in diff.0.props {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.root_props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.root_props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff, schema, Path::root().key(prop.clone())),
                        None => {
                            let value = MultiValue::new_from_diff(
                                opid.clone(),
                                diff,
                                schema,
                                Path::root().key(prop.clone()),
                            );
                            self.root_props.insert(prop.clone(), value);
                        }
                    };
                    self.root_props.get_mut(&prop).unwrap().apply_diff_iter(
                        &mut diff_iter,
                        schema,
                        Path::root().key(prop),
                    );
                }
            }
        }
    }

    fn remove(&mut self, k: &str) -> Option<MultiValue> {
        self.root_props.remove(k)
    }

    fn get(&self, k: &str) -> Option<&MultiValue> {
        self.root_props.get(k)
    }

    pub(crate) fn resolve_path<'a>(
        &'a self,
        path: &Path,
    ) -> Option<resolved_path::ResolvedPath<'a>> {
        if path.is_root() {
            return Some(ResolvedPath::new_root(self));
        }
        let mut stack = path.clone().elements();
        stack.reverse();

        if let Some(PathElement::Key(k)) = stack.pop() {
            let o = self.root_props.get(&k)?;

            o.resolve_path(stack, amp::ObjectId::Root, amp::Key::Map(k))
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut<'a>(
        &'a mut self,
        path: &Path,
    ) -> Option<resolved_path::ResolvedPathMut<'a>> {
        if path.is_root() {
            return Some(ResolvedPathMut::new_root(self));
        }
        let mut stack = path.clone().elements();
        stack.reverse();

        if let Some(PathElement::Key(k)) = stack.pop() {
            let o = self.root_props.get_mut(&k)?;

            o.resolve_path_mut(stack, amp::ObjectId::Root, amp::Key::Map(k))
        } else {
            None
        }
    }

    pub fn value(&self) -> Value {
        let mut m = HashMap::new();
        for (k, v) in &self.root_props {
            m.insert(k.clone(), v.default_value());
        }
        Value::Map(m)
    }

    pub(crate) fn value_ref(&self) -> RootRef {
        RootRef::new(self)
    }
}

/// A node in the state tree is either a leaf node containing a scalarvalue,
/// or an internal composite type (e.g a Map or a List)
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateTreeValue {
    Leaf(Primitive),
    Composite(StateTreeComposite),
}

impl Default for StateTreeValue {
    fn default() -> Self {
        Self::Leaf(Primitive::Null)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateTreeComposite {
    Map(StateTreeMap),
    SortedMap(StateTreeSortedMap),
    Table(StateTreeTable),
    Text(StateTreeText),
    List(StateTreeList),
}

impl StateTreeComposite {
    fn check_diff(&self, diff: &amp::Diff) -> Result<(), error::InvalidPatch> {
        if diff.object_id() != Some(self.object_id()) {
            return Err(error::InvalidPatch::MismatchingObjectIDs {
                patch_expected_id: diff.object_id(),
                actual_id: self.object_id(),
            });
        };
        match (diff, self) {
            (
                amp::Diff::Map(amp::MapDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::Map(map),
            ) => map.check_diff(prop_diffs),
            (
                amp::Diff::Map(amp::MapDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::SortedMap(map),
            ) => map.check_diff(prop_diffs),
            (
                amp::Diff::Table(amp::TableDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::Table(table),
            ) => table.check_diff(prop_diffs),
            (
                amp::Diff::List(amp::ListDiff {
                    edits,
                    object_id: _,
                }),
                StateTreeComposite::List(list),
            ) => list.check_diff(edits),
            (
                amp::Diff::Text(amp::TextDiff {
                    edits,
                    object_id: _,
                }),
                StateTreeComposite::Text(text),
            ) => text.check_diff(edits),
            // TODO throw an error
            (amp::Diff::Value(..), _) => unreachable!(),
            // TODO throw an error
            (amp::Diff::Cursor(..), _) => unreachable!(),
            (amp::Diff::Map(_), _)
            | (amp::Diff::Table(_), _)
            | (amp::Diff::List(_), _)
            | (amp::Diff::Text(_), _) => Err(error::InvalidPatch::MismatchingObjectType {
                object_id: self.object_id(),
                patch_expected_type: diff.object_type(),
                actual_type: Some(self.obj_type()),
            }),
        }
    }

    fn apply_diff(&mut self, diff: amp::Diff, schema: &Schema, path: Path) {
        match (diff, self) {
            (
                amp::Diff::Map(amp::MapDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::Map(map),
            ) => map.apply_diff(prop_diffs, schema, path),
            (
                amp::Diff::Map(amp::MapDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::SortedMap(map),
            ) => map.apply_diff(prop_diffs, schema, path),
            (
                amp::Diff::Table(amp::TableDiff {
                    props: prop_diffs,
                    object_id: _,
                }),
                StateTreeComposite::Table(table),
            ) => table.apply_diff(prop_diffs, schema, path),
            (
                amp::Diff::List(amp::ListDiff {
                    edits,
                    object_id: _,
                }),
                StateTreeComposite::List(list),
            ) => list.apply_diff(edits, schema, path),
            (
                amp::Diff::Text(amp::TextDiff {
                    edits,
                    object_id: _,
                }),
                StateTreeComposite::Text(text),
            ) => text.apply_diff(edits, schema, path),
            // TODO throw an error
            (amp::Diff::Value(..), _) => unreachable!(),
            // TODO throw an error
            (amp::Diff::Cursor(..), _) => unreachable!(),
            (amp::Diff::Map(_), _)
            | (amp::Diff::Table(_), _)
            | (amp::Diff::List(_), _)
            | (amp::Diff::Text(_), _) => unreachable!(),
        }
    }

    fn obj_type(&self) -> amp::ObjType {
        match self {
            Self::Map(..) | Self::SortedMap(..) => amp::ObjType::Map,
            Self::Table(..) => amp::ObjType::Table,
            Self::Text(..) => amp::ObjType::Text,
            Self::List(..) => amp::ObjType::List,
        }
    }

    fn object_id(&self) -> amp::ObjectId {
        match self {
            Self::Map(StateTreeMap { object_id, .. })
            | Self::SortedMap(StateTreeSortedMap { object_id, .. }) => object_id.clone(),
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
            ),
            Self::SortedMap(StateTreeSortedMap { props, .. }) => Value::SortedMap(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value()))
                    .collect(),
            ),
            Self::Table(StateTreeTable { props, .. }) => Value::Table(
                props
                    .iter()
                    .map(|(k, v)| (k.clone(), v.default_value()))
                    .collect(),
            ),
            Self::List(StateTreeList {
                elements: elems, ..
            }) => Value::List(elems.iter().map(|e| e.default_value()).collect()),
            Self::Text(StateTreeText { graphemes, .. }) => Value::Text(
                graphemes
                    .iter()
                    .map(|c| c.default_grapheme().clone())
                    .collect(),
            ),
        }
    }

    fn resolve_path(&self, path: Vec<PathElement>) -> Option<ResolvedPath> {
        match self {
            Self::Map(map) => map.resolve_path(path),
            Self::SortedMap(map) => map.resolve_path(path),
            Self::Table(table) => table.resolve_path(path),
            Self::List(list) => list.resolve_path(path),
            Self::Text(text) => text.resolve_path(path),
        }
    }

    fn resolve_path_mut(&mut self, path: Vec<PathElement>) -> Option<ResolvedPathMut> {
        match self {
            Self::Map(map) => map.resolve_path_mut(path),
            Self::SortedMap(map) => map.resolve_path_mut(path),
            Self::Table(table) => table.resolve_path_mut(path),
            Self::List(list) => list.resolve_path_mut(path),
            Self::Text(text) => text.resolve_path_mut(path),
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
                | amp::ScalarValue::Counter(_)
                | amp::ScalarValue::Timestamp(_)
                | amp::ScalarValue::Boolean(_)
                | amp::ScalarValue::Null => Ok(()),
                amp::ScalarValue::Cursor(..) => Err(error::InvalidPatch::ValueDiffContainedCursor),
            },
            amp::Diff::Map(_)
            | amp::Diff::Table(_)
            | amp::Diff::List(_)
            | amp::Diff::Text(_)
            | amp::Diff::Cursor(_) => Ok(()),
        }
    }

    fn new_from_diff(diff: amp::Diff, schema: &Schema, path: Path) -> StateTreeValue {
        match diff {
            amp::Diff::Value(v) => {
                let value = match v {
                    amp::ScalarValue::Bytes(b) => Primitive::Bytes(b),
                    amp::ScalarValue::Str(s) => Primitive::Str(s),
                    amp::ScalarValue::Int(i) => Primitive::Int(i),
                    amp::ScalarValue::Uint(u) => Primitive::Uint(u),
                    amp::ScalarValue::F64(f) => Primitive::F64(f),
                    amp::ScalarValue::Counter(i) => Primitive::Counter(i),
                    amp::ScalarValue::Timestamp(i) => Primitive::Timestamp(i),
                    amp::ScalarValue::Boolean(b) => Primitive::Boolean(b),
                    amp::ScalarValue::Null => Primitive::Null,
                    amp::ScalarValue::Cursor(..) => {
                        unreachable!("value diff contained a cursor")
                    }
                };
                StateTreeValue::Leaf(value)
            }
            amp::Diff::Map(amp::MapDiff { object_id, props }) => {
                if schema.is_sorted_map(&path) {
                    let mut map = StateTreeSortedMap {
                        object_id,
                        props: BTreeMap::new(),
                    };
                    map.apply_diff(props, schema, path);
                    StateTreeValue::Composite(StateTreeComposite::SortedMap(map))
                } else {
                    let mut map = StateTreeMap {
                        object_id,
                        props: HashMap::new(),
                    };
                    map.apply_diff(props, schema, path);
                    StateTreeValue::Composite(StateTreeComposite::Map(map))
                }
            }
            amp::Diff::Table(amp::TableDiff { object_id, props }) => {
                let mut table = StateTreeTable {
                    object_id,
                    props: HashMap::new(),
                };
                table.apply_diff(props, schema, path);
                StateTreeValue::Composite(StateTreeComposite::Table(table))
            }
            amp::Diff::List(amp::ListDiff { object_id, edits }) => {
                let mut list = StateTreeList {
                    object_id,
                    elements: DiffableSequence::new(),
                };
                list.apply_diff(edits, schema, path);
                StateTreeValue::Composite(StateTreeComposite::List(list))
            }
            amp::Diff::Text(amp::TextDiff { object_id, edits }) => {
                let mut text = StateTreeText {
                    object_id,
                    graphemes: DiffableSequence::new(),
                };
                text.apply_diff(edits, schema, path);
                StateTreeValue::Composite(StateTreeComposite::Text(text))
            }

            amp::Diff::Cursor(ref c) => StateTreeValue::Leaf(c.into()),
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
pub(crate) struct StateTreeMap {
    object_id: amp::ObjectId,
    pub(crate) props: HashMap<SmolStr, MultiValue>,
}

impl StateTreeMap {
    fn check_diff(
        &self,
        prop_diffs: &HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.iter();
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
        prop_diffs: HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
        schema: &Schema,
        path: Path,
    ) {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff, schema, path.clone().key(prop.clone())),
                        None => {
                            let value = MultiValue::new_from_diff(
                                opid.clone(),
                                diff,
                                schema,
                                path.clone().key(prop.clone()),
                            );
                            self.props.insert(prop.clone(), value);
                        }
                    };
                    self.props.get_mut(&prop).unwrap().apply_diff_iter(
                        &mut diff_iter,
                        schema,
                        path.clone().key(prop),
                    );
                }
            }
        }
    }

    pub fn pred_for_key(&self, key: &str) -> SortedVec<amp::OpId> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()].into())
            .unwrap_or_else(SortedVec::new)
    }

    pub(crate) fn resolve_path(&self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props
                .get(&key)?
                .resolve_path(path, self.object_id.clone(), amp::Key::Map(key))
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        mut path: Vec<PathElement>,
    ) -> Option<ResolvedPathMut> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props.get_mut(&key)?.resolve_path_mut(
                path,
                self.object_id.clone(),
                amp::Key::Map(key),
            )
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTreeSortedMap {
    object_id: amp::ObjectId,
    pub(crate) props: BTreeMap<SmolStr, MultiValue>,
}

impl StateTreeSortedMap {
    fn check_diff(
        &self,
        prop_diffs: &HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.iter();
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
        prop_diffs: HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
        schema: &Schema,
        path: Path,
    ) {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff, schema, path.clone().key(prop.clone())),
                        None => {
                            let value = MultiValue::new_from_diff(
                                opid.clone(),
                                diff,
                                schema,
                                path.clone().key(prop.clone()),
                            );
                            self.props.insert(prop.clone(), value);
                        }
                    };
                    self.props.get_mut(&prop).unwrap().apply_diff_iter(
                        &mut diff_iter,
                        schema,
                        path.clone().key(prop),
                    );
                }
            }
        }
    }

    pub fn pred_for_key(&self, key: &str) -> SortedVec<amp::OpId> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()].into())
            .unwrap_or_else(SortedVec::new)
    }

    pub(crate) fn resolve_path(&self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props
                .get(&key)?
                .resolve_path(path, self.object_id.clone(), amp::Key::Map(key))
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        mut path: Vec<PathElement>,
    ) -> Option<ResolvedPathMut> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props.get_mut(&key)?.resolve_path_mut(
                path,
                self.object_id.clone(),
                amp::Key::Map(key),
            )
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTreeTable {
    object_id: amp::ObjectId,
    pub(crate) props: HashMap<SmolStr, MultiValue>,
}

impl StateTreeTable {
    fn check_diff(
        &self,
        prop_diffs: &HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
    ) -> Result<(), error::InvalidPatch> {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.iter();
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
        prop_diffs: HashMap<SmolStr, HashMap<amp::OpId, amp::Diff>>,
        schema: &Schema,
        path: Path,
    ) {
        for (prop, prop_diff) in prop_diffs {
            let mut diff_iter = prop_diff.into_iter();
            match diff_iter.next() {
                None => {
                    self.props.remove(&prop);
                }
                Some((opid, diff)) => {
                    match self.props.get_mut(&prop) {
                        Some(n) => n.apply_diff(opid, diff, schema, path.clone().key(prop.clone())),
                        None => {
                            let value = MultiValue::new_from_diff(
                                opid.clone(),
                                diff,
                                schema,
                                path.clone().key(prop.clone()),
                            );
                            self.props.insert(prop.clone(), value);
                        }
                    };
                    self.props.get_mut(&prop).unwrap().apply_diff_iter(
                        &mut diff_iter,
                        schema,
                        path.clone().key(prop),
                    );
                }
            }
        }
    }

    pub fn pred_for_key(&self, key: &str) -> SortedVec<amp::OpId> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()].into())
            .unwrap_or_else(SortedVec::new)
    }

    pub(crate) fn resolve_path(&self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props
                .get(&key)?
                .resolve_path(path, self.object_id.clone(), amp::Key::Map(key))
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        mut path: Vec<PathElement>,
    ) -> Option<ResolvedPathMut> {
        if let Some(PathElement::Key(key)) = path.pop() {
            self.props.get_mut(&key)?.resolve_path_mut(
                path,
                self.object_id.clone(),
                amp::Key::Map(key),
            )
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTreeText {
    object_id: amp::ObjectId,
    pub(crate) graphemes: DiffableSequence<MultiGrapheme>,
}

impl StateTreeText {
    fn remove(&mut self, index: usize) -> Result<MultiGrapheme, error::MissingIndexError> {
        if index >= self.graphemes.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.graphemes.len(),
            })
        } else {
            let old = self.graphemes.remove(index);
            Ok(old)
        }
    }

    fn set(
        &mut self,
        index: usize,
        value: MultiGrapheme,
    ) -> Result<MultiGrapheme, error::MissingIndexError> {
        if self.graphemes.len() > index {
            let old = self.graphemes.set(index, value);
            Ok(old)
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
    ) -> Result<(&amp::OpId, &SmolStr), error::MissingIndexError> {
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

    fn apply_diff(&mut self, edits: Vec<amp::DiffEdit>, schema: &Schema, path: Path) {
        self.graphemes
            .apply_diff(&self.object_id, edits, schema, path)
    }

    pub fn pred_for_index(&self, index: u32) -> SortedVec<amp::OpId> {
        self.graphemes
            .get(index.try_into().unwrap())
            .map(|v| vec![v.1.default_opid().clone()].into())
            .unwrap_or_else(SortedVec::new)
    }

    pub(crate) fn resolve_path(&self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Index(i)) = path.pop() {
            if path.is_empty() {
                self.graphemes.get(i as usize)?.1.resolve_path(path)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        mut path: Vec<PathElement>,
    ) -> Option<ResolvedPathMut> {
        if let Some(PathElement::Index(i)) = path.pop() {
            if path.is_empty() {
                self.graphemes.get_mut(i as usize)?.1.resolve_path_mut(path)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateTreeList {
    object_id: amp::ObjectId,
    pub(crate) elements: DiffableSequence<MultiValue>,
}

impl StateTreeList {
    fn remove(&mut self, index: usize) -> Result<MultiValue, error::MissingIndexError> {
        if index >= self.elements.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        } else {
            let old = self.elements.remove(index);
            Ok(old)
        }
    }

    fn set(
        &mut self,
        index: usize,
        value: MultiValue,
    ) -> Result<MultiValue, error::MissingIndexError> {
        if self.elements.len() > index {
            let old = self.elements.set(index, value);
            Ok(old)
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

    fn apply_diff(&mut self, edits: Vec<amp::DiffEdit>, schema: &Schema, path: Path) {
        self.elements
            .apply_diff(&self.object_id, edits, schema, path);
    }

    pub fn pred_for_index(&self, index: u32) -> SortedVec<amp::OpId> {
        self.elements
            .get(index.try_into().unwrap())
            .map(|v| vec![v.1.default_opid()].into())
            .unwrap_or_else(SortedVec::new)
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

    pub(crate) fn resolve_path(&self, mut path: Vec<PathElement>) -> Option<ResolvedPath> {
        if let Some(PathElement::Index(i)) = path.pop() {
            let elem_id = self
                .elem_at(i as usize)
                .ok()
                .map(|(e, _)| e.into())
                .unwrap_or(ElementId::Head);
            self.elements.get(i as usize)?.1.resolve_path(
                path,
                self.object_id.clone(),
                amp::Key::Seq(elem_id),
            )
        } else {
            None
        }
    }

    pub(crate) fn resolve_path_mut(
        &mut self,
        mut path: Vec<PathElement>,
    ) -> Option<ResolvedPathMut> {
        if let Some(PathElement::Index(i)) = path.pop() {
            let elem_id = self
                .elem_at(i as usize)
                .ok()
                .map(|(e, _)| e.into())
                .unwrap_or(ElementId::Head);
            self.elements.get_mut(i as usize)?.1.resolve_path_mut(
                path,
                self.object_id.clone(),
                amp::Key::Seq(elem_id),
            )
        } else {
            None
        }
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
struct Cursors(HashMap<amp::ObjectId, Vec<CursorState>>);

impl Cursors {
    fn new() -> Cursors {
        Cursors(HashMap::new())
    }

    fn new_from(cursor: CursorState) -> Cursors {
        Cursors(maplit::hashmap! {
            cursor.referred_object_id.clone() => vec![cursor],
        })
    }

    fn extend(&mut self, other: Cursors) {
        for (k, v) in other.0 {
            if let Some(c1) = self.0.get_mut(&k) {
                c1.extend(v)
            } else {
                self.0.insert(k, v);
            }
        }
    }
}
