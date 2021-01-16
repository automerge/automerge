use crate::error;
use crate::Value;
use crate::{Path, PathElement};
use automerge_protocol as amp;
use im::hashmap;
use std::collections::HashMap;
use std::convert::TryInto;

mod focus;
mod multivalue;
mod resolved_path;
mod state_tree_change;
use multivalue::{MultiChar, MultiValue, NewValueRequest};
pub use resolved_path::ResolvedPath;
pub(crate) use resolved_path::SetOrInsertPayload;
use resolved_path::{
    ResolvedChar, ResolvedCounter, ResolvedList, ResolvedMap, ResolvedPrimitive, ResolvedRoot,
    ResolvedTable, ResolvedText,
};
use state_tree_change::StateTreeChange;

/// Represents the result of running a local operation (i.e one that happens within the frontend
/// before any interaction with a backend).
pub(crate) struct LocalOperationResult {
    /// The new state tree after the operation is executed
    pub new_state: StateTree,
    /// Any operations which need to be sent to the backend to reconcile this change
    pub new_ops: Vec<amp::Op>,
}

#[derive(Debug, Clone)]
pub(crate) struct StateTree {
    root_map: im::HashMap<String, MultiValue>,
    object_index: im::HashMap<amp::ObjectID, StateTreeComposite>,
}

impl StateTree {
    pub fn new() -> StateTree {
        StateTree {
            root_map: im::HashMap::new(),
            object_index: im::HashMap::new(),
        }
    }

    pub fn apply_diff(&self, diff: &amp::Diff) -> Result<StateTree, error::InvalidPatch> {
        match diff {
            amp::Diff::Map(mapdiff) => {
                let amp::MapDiff {
                    object_id,
                    obj_type,
                    props: _props,
                } = mapdiff;
                if *object_id != amp::ObjectID::Root {
                    Err(error::InvalidPatch::PatchDidNotBeginAtRoot)
                } else if *obj_type != amp::MapType::Map {
                    Err(error::InvalidPatch::MismatchingObjectType {
                        object_id: amp::ObjectID::Root,
                        patch_expected_type: Some(amp::ObjType::map()),
                        actual_type: Some(amp::ObjType::Map(*obj_type)),
                    })
                } else {
                    self.apply_map_diff(diff)
                }
            }
            _ => Err(error::InvalidPatch::PatchDidNotBeginAtRoot),
        }
    }

    fn apply_map_diff(&self, diff: &amp::Diff) -> Result<StateTree, error::InvalidPatch> {
        let application_result = StateTreeComposite::Map(StateTreeMap {
            object_id: amp::ObjectID::Root,
            props: self.root_map.clone(),
        })
        .apply_diff(diff)?;
        match application_result.value() {
            StateTreeComposite::Map(StateTreeMap { props: values, .. }) => {
                let new_object_index = match application_result.index_updates() {
                    None => self.object_index.clone(),
                    Some(updates) => updates.clone().union(self.object_index.clone()),
                };
                Ok(StateTree {
                    root_map: values.clone(),
                    object_index: new_object_index,
                })
            }
            _ => panic!("Bad type returned from apply_diff"),
        }
    }

    fn update(&self, k: String, diffapp: StateTreeChange<MultiValue>) -> StateTree {
        let new_index = match diffapp.index_updates() {
            Some(u) => u.clone().union(self.object_index.clone()),
            None => self.object_index.clone(),
        };
        StateTree {
            root_map: self.root_map.update(k, diffapp.value().clone()),
            object_index: new_index,
        }
    }

    fn remove(&self, k: &str) -> StateTree {
        let mut new_root = self.root_map.clone();
        new_root.remove(k);
        StateTree {
            root_map: new_root,
            object_index: self.object_index.clone(),
        }
    }

    pub(crate) fn resolve_path(&self, path: &Path) -> Option<resolved_path::ResolvedPath> {
        if path.is_root() {
            return Some(ResolvedPath::Root(ResolvedRoot { root: self.clone() }));
        }
        let mut stack = path.clone().elements();
        stack.reverse();
        if let Some(PathElement::Key(k)) = stack.pop() {
            let mut parent_object_id = amp::ObjectID::Root.clone();
            let mut key_in_container: amp::Key = k.clone().into();
            let first_obj = self.root_map.get(&k);
            if let Some(o) = first_obj {
                let mut focus = Box::new(focus::Focus::new_root(self.clone(), k.clone()));
                let mut current_obj: MultiValue = o.clone();
                while let Some(next_elem) = stack.pop() {
                    match next_elem {
                        PathElement::Key(k) => {
                            key_in_container = k.clone().into();
                            match current_obj.default_statetree_value() {
                                StateTreeValue::Composite(StateTreeComposite::Map(map)) => {
                                    if let Some(target) = map.props.get(&k) {
                                        let new_focus = focus::Focus::new_map(
                                            focus.clone(),
                                            map.clone(),
                                            k.clone(),
                                            target.clone(),
                                        );
                                        focus = Box::new(new_focus);
                                        parent_object_id = map.object_id.clone();
                                        current_obj = target.clone();
                                    } else {
                                        return None;
                                    }
                                }
                                StateTreeValue::Composite(StateTreeComposite::Table(table)) => {
                                    if let Some(target) = table.props.get(&k) {
                                        let new_focus = focus::Focus::new_table(
                                            focus.clone(),
                                            table.clone(),
                                            k.clone(),
                                            target.clone(),
                                        );
                                        focus = Box::new(new_focus);
                                        parent_object_id = table.object_id.clone();
                                        current_obj = target.clone();
                                    } else {
                                        return None;
                                    }
                                }
                                _ => return None,
                            }
                        }
                        PathElement::Index(i) => match current_obj.default_statetree_value() {
                            StateTreeValue::Composite(StateTreeComposite::List(list)) => {
                                let index = i.try_into().unwrap();
                                if let Ok((elemid, target)) = list.elem_at(index) {
                                    key_in_container = elemid.into();
                                    parent_object_id = list.object_id.clone();
                                    current_obj = target.clone();
                                    let new_focus = focus::Focus::new_list(
                                        focus.clone(),
                                        list,
                                        index,
                                        current_obj.clone(),
                                    );
                                    focus = Box::new(new_focus);
                                } else {
                                    return None;
                                }
                            }
                            StateTreeValue::Composite(StateTreeComposite::Text(
                                StateTreeText { chars, .. },
                            )) => {
                                if chars.get(i as usize).is_some() {
                                    if stack.is_empty() {
                                        return Some(ResolvedPath::Character(ResolvedChar {
                                            multivalue: current_obj,
                                        }));
                                    } else {
                                        return None;
                                    }
                                } else {
                                    return None;
                                };
                            }
                            _ => return None,
                        },
                    };
                }
                let resolved_path = match current_obj.default_statetree_value() {
                    StateTreeValue::Leaf(v) => match v {
                        amp::ScalarValue::Counter(v) => ResolvedPath::Counter(ResolvedCounter {
                            containing_object_id: parent_object_id,
                            key_in_container,
                            current_value: v,
                            multivalue: current_obj,
                            focus,
                        }),
                        _ => ResolvedPath::Primitive(ResolvedPrimitive {
                            multivalue: current_obj,
                        }),
                    },
                    StateTreeValue::Composite(composite) => match composite {
                        StateTreeComposite::Map(m) => ResolvedPath::Map(ResolvedMap {
                            value: m,
                            multivalue: current_obj,
                            focus,
                        }),
                        StateTreeComposite::Table(t) => ResolvedPath::Table(ResolvedTable {
                            value: t,
                            multivalue: current_obj,
                            focus,
                        }),
                        StateTreeComposite::List(l) => ResolvedPath::List(ResolvedList {
                            value: l,
                            multivalue: current_obj,
                            focus,
                        }),
                        StateTreeComposite::Text(t) => ResolvedPath::Text(ResolvedText {
                            multivalue: current_obj,
                            value: t,
                            update: Box::new(move |d| focus.update(d)),
                        }),
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
        StateTreeValue::Composite(StateTreeComposite::Map(StateTreeMap {
            object_id: amp::ObjectID::Root,
            props: self.root_map.clone(),
        }))
        .value()
    }
}

/// A node in the state tree is either a leaf node containing a scalarvalue,
/// or an internal composite type (e.g a Map or a List)
#[derive(Debug, Clone)]
enum StateTreeValue {
    Leaf(amp::ScalarValue),
    Composite(StateTreeComposite),
}

#[derive(Debug, Clone)]
enum StateTreeComposite {
    Map(StateTreeMap),
    Table(StateTreeTable),
    Text(StateTreeText),
    List(StateTreeList),
}

impl StateTreeComposite {
    fn apply_diff(
        &self,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<StateTreeComposite>, error::InvalidPatch> {
        if diff_object_id(diff) != Some(self.object_id()) {
            return Err(error::InvalidPatch::MismatchingObjectIDs {
                patch_expected_id: diff_object_id(diff),
                actual_id: self.object_id(),
            });
        };
        match diff {
            amp::Diff::Map(amp::MapDiff {
                obj_type,
                props: prop_diffs,
                ..
            }) => match self {
                StateTreeComposite::Map(map) => {
                    if *obj_type != amp::MapType::Map {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: map.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Map(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        map.apply_diff(prop_diffs)
                            .map(|d| d.map(StateTreeComposite::Map))
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
                        table
                            .apply_diff(prop_diffs)
                            .map(|d| d.map(StateTreeComposite::Table))
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: diff_object_type(diff),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Seq(amp::SeqDiff {
                edits,
                props: new_props,
                obj_type,
                ..
            }) => match self {
                StateTreeComposite::List(list) => {
                    if *obj_type != amp::SequenceType::List {
                        Err(error::InvalidPatch::MismatchingObjectType {
                            object_id: list.object_id.clone(),
                            patch_expected_type: Some(amp::ObjType::Sequence(*obj_type)),
                            actual_type: Some(self.obj_type()),
                        })
                    } else {
                        list.apply_diff(edits, new_props)
                            .map(|d| d.map(StateTreeComposite::List))
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
                        text.apply_diff(edits, new_props)
                            .map(|d| d.map(StateTreeComposite::Text))
                    }
                }
                _ => Err(error::InvalidPatch::MismatchingObjectType {
                    object_id: self.object_id(),
                    patch_expected_type: diff_object_type(diff),
                    actual_type: Some(self.obj_type()),
                }),
            },
            amp::Diff::Unchanged(..) => Ok(StateTreeChange::pure(self.clone())),
            amp::Diff::Value(..) => {
                panic!("SHould never be called")
            }
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

    fn object_id(&self) -> amp::ObjectID {
        match self {
            Self::Map(StateTreeMap { object_id, .. }) => object_id.clone(),
            Self::Table(StateTreeTable { object_id, .. }) => object_id.clone(),
            Self::Text(StateTreeText { object_id, .. }) => object_id.clone(),
            Self::List(StateTreeList { object_id, .. }) => object_id.clone(),
        }
    }

    fn value(&self) -> Value {
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
            Self::Text(StateTreeText { chars, .. }) => {
                Value::Text(chars.iter().map(|c| c.default_char()).collect())
            }
        }
    }
}

impl StateTreeValue {
    fn new_from_diff(
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<StateTreeValue>, error::InvalidPatch> {
        match diff {
            amp::Diff::Value(v) => Ok(StateTreeChange::pure(StateTreeValue::Leaf(v.clone()))),
            amp::Diff::Map(amp::MapDiff {
                object_id,
                obj_type,
                ..
            }) => match obj_type {
                amp::MapType::Map => StateTreeComposite::Map(StateTreeMap {
                    object_id: object_id.clone(),
                    props: im::HashMap::new(),
                }),
                amp::MapType::Table => StateTreeComposite::Table(StateTreeTable {
                    object_id: object_id.clone(),
                    props: im::HashMap::new(),
                }),
            }
            .apply_diff(diff)
            .map(|d| d.map(StateTreeValue::Composite)),
            amp::Diff::Seq(amp::SeqDiff {
                object_id,
                obj_type,
                ..
            }) => match obj_type {
                amp::SequenceType::Text => StateTreeComposite::Text(StateTreeText {
                    object_id: object_id.clone(),
                    chars: im::Vector::new(),
                }),
                amp::SequenceType::List => StateTreeComposite::List(StateTreeList {
                    object_id: object_id.clone(),
                    elements: im::Vector::new(),
                }),
            }
            .apply_diff(diff)
            .map(|d| d.map(StateTreeValue::Composite)),
            amp::Diff::Unchanged(..) => Err(error::InvalidPatch::UnchangedDiffForNonExistentObject),
        }
    }

    fn value(&self) -> Value {
        match self {
            StateTreeValue::Leaf(p) => p.into(),
            StateTreeValue::Composite(composite) => composite.value(),
        }
    }
}

#[derive(Debug, Clone)]
struct StateTreeMap {
    object_id: amp::ObjectID,
    props: im::HashMap<String, MultiValue>,
}

impl StateTreeMap {
    fn update(&self, key: String, value: MultiValue) -> StateTreeMap {
        StateTreeMap {
            object_id: self.object_id.clone(),
            props: self.props.update(key, value),
        }
    }

    fn without(&self, key: &str) -> StateTreeMap {
        StateTreeMap {
            object_id: self.object_id.clone(),
            props: self.props.without(key),
        }
    }

    fn apply_diff(
        &self,
        prop_diffs: &HashMap<String, HashMap<amp::OpID, amp::Diff>>,
    ) -> Result<StateTreeChange<StateTreeMap>, error::InvalidPatch> {
        let init_changed_props = Ok(StateTreeChange::pure(self.props.clone()));
        let changed_props =
            prop_diffs
                .iter()
                .fold(init_changed_props, |changes_so_far, (prop, prop_diff)| {
                    let mut diff_iter = prop_diff.iter();
                    match diff_iter.next() {
                        None => changes_so_far.map(|cr| cr.map(|c| c.without(prop))),
                        Some((opid, diff)) => {
                            changes_so_far?.fallible_and_then(move |changes_so_far| {
                                let mut node: StateTreeChange<MultiValue> =
                                    match changes_so_far.get(prop) {
                                        Some(n) => n.apply_diff(opid, diff)?,
                                        None => MultiValue::new_from_diff(opid.clone(), diff)?,
                                    };
                                node = node.fallible_and_then(move |n| {
                                    n.apply_diff_iter(&mut diff_iter)
                                })?;
                                Ok(node.map(|n| changes_so_far.update(prop.to_string(), n)))
                            })
                        }
                    }
                })?;
        Ok(changed_props.and_then(|new_props| {
            let new_map = StateTreeMap {
                object_id: self.object_id.clone(),
                props: new_props,
            };
            StateTreeChange::pure(new_map.clone()).with_updates(Some(
                hashmap! {self.object_id.clone() => StateTreeComposite::Map(new_map)},
            ))
        }))
    }

    pub fn pred_for_key(&self, key: &str) -> Vec<amp::OpID> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new)
    }
}

#[derive(Debug, Clone)]
struct StateTreeTable {
    object_id: amp::ObjectID,
    props: im::HashMap<String, MultiValue>,
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

    fn apply_diff(
        &self,
        prop_diffs: &HashMap<String, HashMap<amp::OpID, amp::Diff>>,
    ) -> Result<StateTreeChange<StateTreeTable>, error::InvalidPatch> {
        let init_changed_props = Ok(StateTreeChange::pure(self.props.clone()));
        let changed_props =
            prop_diffs
                .iter()
                .fold(init_changed_props, |changes_so_far, (prop, prop_diff)| {
                    let mut diff_iter = prop_diff.iter();
                    match diff_iter.next() {
                        None => changes_so_far.map(|cr| cr.map(|c| c.without(prop))),
                        Some((opid, diff)) => {
                            changes_so_far?.fallible_and_then(move |changes_so_far| {
                                let mut node = match changes_so_far.get(prop) {
                                    Some(n) => n.apply_diff(opid, diff)?,
                                    None => MultiValue::new_from_diff(opid.clone(), diff)?,
                                };
                                node = node.fallible_and_then(move |n| {
                                    n.apply_diff_iter(&mut diff_iter)
                                })?;
                                Ok(node.map(|n| changes_so_far.update(prop.to_string(), n)))
                            })
                        }
                    }
                })?;
        Ok(changed_props.and_then(|new_props| {
            let new_table = StateTreeTable {
                object_id: self.object_id.clone(),
                props: new_props,
            };
            StateTreeChange::pure(new_table.clone()).with_updates(Some(
                hashmap! {self.object_id.clone() => StateTreeComposite::Table(new_table)},
            ))
        }))
    }

    pub fn pred_for_key(&self, key: &str) -> Vec<amp::OpID> {
        self.props
            .get(key)
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new)
    }
}

#[derive(Debug, Clone)]
struct StateTreeText {
    object_id: amp::ObjectID,
    chars: im::Vector<MultiChar>,
}

impl StateTreeText {
    fn remove(&self, index: usize) -> Result<StateTreeText, error::MissingIndexError> {
        if index >= self.chars.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.chars.len(),
            })
        } else {
            let mut new_chars = self.chars.clone();
            new_chars.remove(index);
            Ok(StateTreeText {
                object_id: self.object_id.clone(),
                chars: new_chars,
            })
        }
    }

    fn set(
        &self,
        index: usize,
        value: MultiChar,
    ) -> Result<StateTreeText, error::MissingIndexError> {
        if self.chars.len() > index {
            Ok(StateTreeText {
                object_id: self.object_id.clone(),
                chars: self.chars.update(index, value),
            })
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.chars.len(),
            })
        }
    }

    pub(crate) fn elem_at(
        &self,
        index: usize,
    ) -> Result<(amp::ElementID, char), error::MissingIndexError> {
        self.chars
            .get(index)
            .map(|mc| (mc.default_opid().into(), mc.default_char()))
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.chars.len(),
            })
    }

    fn insert(
        &self,
        index: usize,
        value: MultiChar,
    ) -> Result<StateTreeText, error::MissingIndexError> {
        if self.chars.len() > index {
            let mut new_chars = self.chars.clone();
            new_chars.insert(index, value);
            Ok(StateTreeText {
                object_id: self.object_id.clone(),
                chars: new_chars,
            })
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.chars.len(),
            })
        }
    }

    fn apply_diff(
        &self,
        edits: &[amp::DiffEdit],
        props: &HashMap<usize, HashMap<amp::OpID, amp::Diff>>,
    ) -> Result<StateTreeChange<StateTreeText>, error::InvalidPatch> {
        let mut new_chars: im::Vector<(amp::OpID, Option<MultiChar>)> = self
            .chars
            .iter()
            .map(|c| (c.default_opid().clone(), Some(c.clone())))
            .collect();
        //let mut new_chars = self.chars.clone();
        for edit in edits.iter() {
            match edit {
                amp::DiffEdit::Remove { index } => {
                    if *index >= new_chars.len() {
                        return Err(error::InvalidPatch::InvalidIndex {
                            object_id: self.object_id.clone(),
                            index: *index,
                        });
                    } else {
                        new_chars.remove(*index);
                    }
                }
                amp::DiffEdit::Insert { index, elem_id } => {
                    if *index > new_chars.len() {
                        return Err(error::InvalidPatch::InvalidIndex {
                            object_id: self.object_id.clone(),
                            index: *index,
                        });
                    } else {
                        match elem_id {
                            amp::ElementID::Head => {
                                return Err(error::InvalidPatch::DiffEditWithHeadElemID)
                            }
                            amp::ElementID::ID(opid) => {
                                new_chars.insert(*index, (opid.clone(), None))
                            }
                        }
                    }
                }
            }
        }
        for (index, prop_diff) in props {
            if let Some((opid, maybe_char)) = new_chars.get(*index) {
                let new_char = match maybe_char {
                    Some(c) => c.apply_diff(&self.object_id, prop_diff)?,
                    None => MultiChar::new_from_diff(&self.object_id, prop_diff)?,
                };
                new_chars = new_chars.update(*index, (opid.clone(), Some(new_char)));
            } else {
                return Err(error::InvalidPatch::InvalidIndex {
                    object_id: self.object_id.clone(),
                    index: *index,
                });
            }
        }
        let mut new_chars_2: im::Vector<MultiChar> = im::Vector::new();
        for (index, (_, maybe_char)) in new_chars.into_iter().enumerate() {
            match maybe_char {
                Some(c) => {
                    new_chars_2.push_back(c.clone());
                }
                None => {
                    return Err(error::InvalidPatch::InvalidIndex {
                        object_id: self.object_id.clone(),
                        index,
                    });
                }
            };
        }
        let text = StateTreeText {
            object_id: self.object_id.clone(),
            chars: new_chars_2,
        };
        let object_index_updates = im::HashMap::new().update(
            self.object_id.clone(),
            StateTreeComposite::Text(text.clone()),
        );
        Ok(StateTreeChange::pure(text).with_updates(Some(object_index_updates)))
    }

    pub fn pred_for_index(&self, index: u32) -> Vec<amp::OpID> {
        self.chars
            .get(index.try_into().unwrap())
            .map(|v| vec![v.default_opid().clone()])
            .unwrap_or_else(Vec::new)
    }
}

#[derive(Debug, Clone)]
struct StateTreeList {
    object_id: amp::ObjectID,
    elements: im::Vector<MultiValue>,
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
        &self,
        index: usize,
        value: MultiValue,
    ) -> Result<StateTreeList, error::MissingIndexError> {
        if self.elements.len() > index {
            Ok(StateTreeList {
                object_id: self.object_id.clone(),
                elements: self.elements.update(index, value),
            })
        } else {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        }
    }

    fn insert(
        &self,
        index: usize,
        value: MultiValue,
    ) -> Result<StateTreeList, error::MissingIndexError> {
        let mut new_elems = self.elements.clone();
        if index > self.elements.len() {
            Err(error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
        } else {
            new_elems.insert(index, value);
            Ok(StateTreeList {
                object_id: self.object_id.clone(),
                elements: new_elems,
            })
        }
    }

    fn apply_diff(
        &self,
        edits: &[amp::DiffEdit],
        new_props: &HashMap<usize, HashMap<amp::OpID, amp::Diff>>,
    ) -> Result<StateTreeChange<StateTreeList>, error::InvalidPatch> {
        let mut init_new_elements: im::Vector<(amp::OpID, Option<MultiValue>)> = self
            .elements
            .iter()
            .map(|e| (e.default_opid(), Some(e.clone())))
            .collect();
        for edit in edits.iter() {
            match edit {
                amp::DiffEdit::Remove { index } => {
                    init_new_elements.remove(*index);
                }
                amp::DiffEdit::Insert { index, elem_id } => {
                    let op_id = match elem_id {
                        amp::ElementID::Head => {
                            return Err(error::InvalidPatch::DiffEditWithHeadElemID)
                        }
                        amp::ElementID::ID(oid) => oid.clone(),
                    };
                    if (*index) == init_new_elements.len() {
                        init_new_elements.push_back((op_id, None));
                    } else {
                        init_new_elements.insert(*index, (op_id, None));
                    }
                }
            };
        }
        let init_changed_props = Ok(StateTreeChange::pure(init_new_elements));
        let updated =
            new_props
                .iter()
                .fold(init_changed_props, |changes_so_far, (index, prop_diff)| {
                    let mut diff_iter = prop_diff.iter();
                    match diff_iter.next() {
                        None => changes_so_far.map(|cr| {
                            cr.map(|c| {
                                let mut result = c;
                                result.remove(*index);
                                result
                            })
                        }),
                        Some((opid, diff)) => {
                            changes_so_far?.fallible_and_then(move |changes_so_far| {
                                let mut node = match changes_so_far.get(*index) {
                                    Some((_, Some(n))) => n.apply_diff(opid, diff)?,
                                    Some((_, None)) => {
                                        MultiValue::new_from_diff(opid.clone(), diff)?
                                    }
                                    None => {
                                        return Err(error::InvalidPatch::InvalidIndex {
                                            object_id: self.object_id.clone(),
                                            index: *index,
                                        })
                                    }
                                };
                                node = node.fallible_and_then(move |n| {
                                    n.apply_diff_iter(&mut diff_iter)
                                })?;
                                Ok(node.map(|n| {
                                    changes_so_far.update(*index, (n.default_opid(), Some(n)))
                                }))
                            })
                        }
                    }
                })?;
        updated.fallible_and_then(|new_elements_and_opids| {
            let mut new_elements: im::Vector<MultiValue> = im::Vector::new();
            for (index, (_, maybe_elem)) in new_elements_and_opids.into_iter().enumerate() {
                match maybe_elem {
                    Some(e) => {
                        new_elements.push_back(e.clone());
                    }
                    None => {
                        return Err(error::InvalidPatch::InvalidIndex {
                            object_id: self.object_id.clone(),
                            index,
                        });
                    }
                }
            }
            let new_list = StateTreeList {
                object_id: self.object_id.clone(),
                elements: new_elements,
            };
            Ok(StateTreeChange::pure(new_list.clone()).with_updates(Some(
                hashmap! {self.object_id.clone() => StateTreeComposite::List(new_list)},
            )))
        })
    }

    pub fn pred_for_index(&self, index: u32) -> Vec<amp::OpID> {
        self.elements
            .get(index.try_into().unwrap())
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new)
    }

    pub(crate) fn elem_at(
        &self,
        index: usize,
    ) -> Result<(amp::ElementID, &MultiValue), error::MissingIndexError> {
        self.elements
            .get(index)
            .map(|mv| (mv.default_opid().into(), mv))
            .ok_or_else(|| error::MissingIndexError {
                missing_index: index,
                size_of_collection: self.elements.len(),
            })
    }
}

/// Helper method to get the object type of an amp::Diff
fn diff_object_type(diff: &amp::Diff) -> Option<amp::ObjType> {
    match diff {
        amp::Diff::Map(mapdiff) => Some(amp::ObjType::Map(mapdiff.obj_type)),
        amp::Diff::Seq(seqdiff) => Some(amp::ObjType::Sequence(seqdiff.obj_type)),
        amp::Diff::Unchanged(udiff) => Some(udiff.obj_type),
        amp::Diff::Value(..) => None,
    }
}

/// Helper method to get the object ID of an amp::Diff
fn diff_object_id(diff: &amp::Diff) -> Option<amp::ObjectID> {
    match diff {
        amp::Diff::Map(mapdiff) => Some(mapdiff.object_id.clone()),
        amp::Diff::Seq(seqdiff) => Some(seqdiff.object_id.clone()),
        amp::Diff::Unchanged(udiff) => Some(udiff.object_id.clone()),
        amp::Diff::Value(..) => None,
    }
}

pub fn random_op_id() -> amp::OpID {
    amp::OpID::new(1, &amp::ActorID::random())
}
