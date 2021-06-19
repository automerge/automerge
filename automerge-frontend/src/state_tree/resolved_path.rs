use std::{convert::TryInto, num::NonZeroU32};

use amp::SortedVec;
use automerge_protocol as amp;
use smol_str::SmolStr;

use super::{
    random_op_id, LocalOperationResult, MultiGrapheme, MultiValue, NewValueRequest, StateTree,
    StateTreeComposite, StateTreeValue,
};
use crate::{error, Cursor, Primitive, Value};

pub enum ResolvedPath<'a> {
    Root(ResolvedRoot<'a>),
    Map(ResolvedMap<'a>),
    Table(ResolvedTable<'a>),
    List(ResolvedList<'a>),
    Text(ResolvedText<'a>),
    Character(ResolvedChar<'a>),
    Counter(ResolvedCounter<'a>),
    Primitive(ResolvedPrimitive<'a>),
}

impl<'a> std::fmt::Debug for ResolvedPath<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResolvedPath::Root(_) => write!(f, "Root"),
            ResolvedPath::Map(maptarget) => {
                write!(f, "Map {:?}", maptarget.object_id)
            }
            ResolvedPath::Table(tabletarget) => {
                write!(f, "Table {:?}", tabletarget.object_id)
            }
            ResolvedPath::List(listtarget) => {
                write!(f, "list {:?}", listtarget.object_id)
            }
            ResolvedPath::Text(texttarget) => {
                write!(f, "text {:?}", texttarget.object_id)
            }
            ResolvedPath::Counter(countertarget) => write!(
                f,
                "counter {0}:{1:?}",
                countertarget.containing_object_id, countertarget.key_in_container
            ),
            ResolvedPath::Primitive(p) => write!(f, "primitive: {:?}", p.multivalue),
            ResolvedPath::Character(ctarget) => write!(f, "character {:?}", ctarget.multivalue),
        }
    }
}

impl<'a> ResolvedPath<'a> {
    pub(super) fn new_root(root: &StateTree) -> ResolvedPath {
        ResolvedPath::Root(ResolvedRoot { root })
    }

    pub(super) fn new_map(value: &'a MultiValue, object_id: amp::ObjectId) -> ResolvedPath<'a> {
        ResolvedPath::Map(ResolvedMap {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_list(value: &'a MultiValue, object_id: amp::ObjectId) -> ResolvedPath<'a> {
        ResolvedPath::List(ResolvedList {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_text(mv: &'a MultiValue, object_id: amp::ObjectId) -> ResolvedPath<'a> {
        ResolvedPath::Text(ResolvedText {
            multivalue: mv,
            object_id,
        })
    }

    pub(super) fn new_table(value: &'a MultiValue, object_id: amp::ObjectId) -> ResolvedPath<'a> {
        ResolvedPath::Table(ResolvedTable {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_counter(
        object_id: amp::ObjectId,
        key: amp::Key,
        mv: &'a MultiValue,
    ) -> ResolvedPath<'a> {
        ResolvedPath::Counter(ResolvedCounter {
            multivalue: mv,
            key_in_container: key,
            containing_object_id: object_id,
        })
    }

    pub(super) fn new_primitive(value: &'a MultiValue) -> ResolvedPath<'a> {
        ResolvedPath::Primitive(ResolvedPrimitive { multivalue: value })
    }

    pub(super) fn new_character(c: &'a MultiGrapheme) -> ResolvedPath<'a> {
        ResolvedPath::Character(ResolvedChar { multivalue: c })
    }

    pub fn default_value(&self) -> Value {
        match &self {
            ResolvedPath::Map(maptarget) => maptarget.multivalue.default_value(),
            ResolvedPath::Root(root) => root.root.value(),
            ResolvedPath::Table(tabletarget) => tabletarget.multivalue.default_value(),
            ResolvedPath::List(listtarget) => listtarget.multivalue.default_value(),
            ResolvedPath::Text(texttarget) => texttarget.multivalue.default_value(),
            ResolvedPath::Counter(countertarget) => countertarget.multivalue.default_value(),
            ResolvedPath::Primitive(p) => p.multivalue.default_value(),
            ResolvedPath::Character(ctarget) => Value::Primitive(Primitive::Str(
                ctarget.multivalue.default_grapheme().clone(),
            )),
        }
    }

    pub fn values(&self) -> std::collections::HashMap<amp::OpId, Value> {
        match &self {
            ResolvedPath::Map(maptarget) => maptarget.multivalue.realise_values(),
            ResolvedPath::Root(root) => {
                let mut result = std::collections::HashMap::new();
                result.insert(random_op_id(), root.root.value());
                result
            }
            ResolvedPath::Table(tabletarget) => tabletarget.multivalue.realise_values(),
            ResolvedPath::List(listtarget) => listtarget.multivalue.realise_values(),
            ResolvedPath::Text(texttarget) => texttarget.multivalue.realise_values(),
            ResolvedPath::Counter(countertarget) => countertarget.multivalue.realise_values(),
            ResolvedPath::Primitive(p) => p.multivalue.realise_values(),
            ResolvedPath::Character(ctarget) => ctarget.multivalue.realise_values(),
        }
    }

    pub fn object_id(&self) -> Option<amp::ObjectId> {
        match &self {
            ResolvedPath::Map(maptarget) => Some(maptarget.object_id.clone()),
            ResolvedPath::Root(_) => Some(amp::ObjectId::Root),
            ResolvedPath::Table(tabletarget) => Some(tabletarget.object_id.clone()),
            ResolvedPath::List(listtarget) => Some(listtarget.object_id.clone()),
            ResolvedPath::Text(texttarget) => Some(texttarget.object_id.clone()),
            ResolvedPath::Counter(_) | ResolvedPath::Primitive(_) | ResolvedPath::Character(_) => {
                None
            }
        }
    }
}

pub enum ResolvedPathMut<'a> {
    Root(ResolvedRootMut<'a>),
    Map(ResolvedMapMut<'a>),
    Table(ResolvedTableMut<'a>),
    List(ResolvedListMut<'a>),
    Text(ResolvedTextMut<'a>),
    Character(ResolvedCharMut<'a>),
    Counter(ResolvedCounterMut<'a>),
    Primitive(ResolvedPrimitiveMut<'a>),
}

impl<'a> std::fmt::Debug for ResolvedPathMut<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResolvedPathMut::Root(_) => write!(f, "Root"),
            ResolvedPathMut::Map(maptarget) => {
                write!(f, "Map {:?}", maptarget.object_id)
            }
            ResolvedPathMut::Table(tabletarget) => {
                write!(f, "Table {:?}", tabletarget.object_id)
            }
            ResolvedPathMut::List(listtarget) => {
                write!(f, "list {:?}", listtarget.object_id)
            }
            ResolvedPathMut::Text(texttarget) => {
                write!(f, "text {:?}", texttarget.object_id)
            }
            ResolvedPathMut::Counter(countertarget) => write!(
                f,
                "counter {0}:{1:?}",
                countertarget.containing_object_id, countertarget.key_in_container
            ),
            ResolvedPathMut::Primitive(p) => write!(f, "primitive: {:?}", p.multivalue),
            ResolvedPathMut::Character(ctarget) => write!(f, "character {:?}", ctarget.multivalue),
        }
    }
}

impl<'a> ResolvedPathMut<'a> {
    pub(super) fn new_root(root: &mut StateTree) -> ResolvedPathMut {
        ResolvedPathMut::Root(ResolvedRootMut { root })
    }

    pub(super) fn new_map(
        value: &'a mut MultiValue,
        object_id: amp::ObjectId,
    ) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Map(ResolvedMapMut {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_list(
        value: &'a mut MultiValue,
        object_id: amp::ObjectId,
    ) -> ResolvedPathMut<'a> {
        ResolvedPathMut::List(ResolvedListMut {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_text(
        mv: &'a mut MultiValue,
        object_id: amp::ObjectId,
    ) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Text(ResolvedTextMut {
            multivalue: mv,
            object_id,
        })
    }

    pub(super) fn new_table(
        value: &'a mut MultiValue,
        object_id: amp::ObjectId,
    ) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Table(ResolvedTableMut {
            multivalue: value,
            object_id,
        })
    }

    pub(super) fn new_counter(
        object_id: amp::ObjectId,
        key: amp::Key,
        mv: &'a mut MultiValue,
    ) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Counter(ResolvedCounterMut {
            multivalue: mv,
            key_in_container: key,
            containing_object_id: object_id,
        })
    }

    pub(super) fn new_primitive(value: &'a mut MultiValue) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Primitive(ResolvedPrimitiveMut { multivalue: value })
    }

    pub(super) fn new_character(c: &'a mut MultiGrapheme) -> ResolvedPathMut<'a> {
        ResolvedPathMut::Character(ResolvedCharMut { multivalue: c })
    }

    pub fn default_value(&self) -> Value {
        match &self {
            ResolvedPathMut::Map(maptarget) => maptarget.multivalue.default_value(),
            ResolvedPathMut::Root(root) => root.root.value(),
            ResolvedPathMut::Table(tabletarget) => tabletarget.multivalue.default_value(),
            ResolvedPathMut::List(listtarget) => listtarget.multivalue.default_value(),
            ResolvedPathMut::Text(texttarget) => texttarget.multivalue.default_value(),
            ResolvedPathMut::Counter(countertarget) => countertarget.multivalue.default_value(),
            ResolvedPathMut::Primitive(p) => p.multivalue.default_value(),
            ResolvedPathMut::Character(ctarget) => Value::Primitive(Primitive::Str(
                ctarget.multivalue.default_grapheme().clone(),
            )),
        }
    }
}

pub(crate) struct SetOrInsertPayload<'a, T> {
    pub start_op: u64,
    pub actor: &'a amp::ActorId,
    pub value: T,
}

pub struct ResolvedRoot<'a> {
    pub(super) root: &'a StateTree,
}

pub struct ResolvedRootMut<'a> {
    pub(super) root: &'a mut StateTree,
}

impl<'a> ResolvedRootMut<'a> {
    pub(crate) fn set_key(
        &mut self,
        key: SmolStr,
        payload: SetOrInsertPayload<Value>,
    ) -> (Option<MultiValue>, LocalOperationResult) {
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            key: amp::Key::Map(key.clone()),
            parent_obj: &amp::ObjectId::Root,
            value: payload.value,
            insert: false,
            pred: self
                .root
                .get(&key)
                .map(|mv| vec![mv.default_opid()].into())
                .unwrap_or_else(SortedVec::new),
        });
        let (multivalue, new_ops, _new_cursors) = newvalue.finish();
        let old = self.root.root_props.insert(key, multivalue);
        (old, LocalOperationResult { new_ops })
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> (MultiValue, LocalOperationResult) {
        let existing_value = self.root.get(key);
        let pred = existing_value
            .map(|v| vec![v.default_opid()].into())
            .unwrap_or_else(SortedVec::new);
        let old = self
            .root
            .remove(key)
            .expect("Removing non existent key from map");
        (
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                    obj: amp::ObjectId::Root,
                    key: key.into(),
                    insert: false,
                    pred,
                }],
            },
        )
    }

    pub(crate) fn rollback_set(&mut self, key: SmolStr, value: Option<MultiValue>) {
        match value {
            Some(old) => {
                self.root.root_props.insert(key, old);
            }
            None => {
                self.root.root_props.remove(&key);
            }
        }
    }

    pub(crate) fn rollback_delete(&mut self, key: SmolStr, value: MultiValue) {
        self.root.root_props.insert(key, value);
    }
}

pub struct ResolvedCounter<'a> {
    pub(super) multivalue: &'a MultiValue,
    pub(super) containing_object_id: amp::ObjectId,
    pub(super) key_in_container: amp::Key,
}

pub struct ResolvedCounterMut<'a> {
    pub(super) multivalue: &'a mut MultiValue,
    pub(super) containing_object_id: amp::ObjectId,
    pub(super) key_in_container: amp::Key,
}

impl<'a> ResolvedCounterMut<'a> {
    pub(crate) fn increment(&mut self, by: i64) -> LocalOperationResult {
        let counter = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Leaf(Primitive::Counter(c)) => c,
            _ => unreachable!(),
        };
        *counter += by;
        LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Inc(by),
                obj: self.containing_object_id.clone(),
                key: self.key_in_container.clone(),
                insert: false,
                pred: vec![self.multivalue.default_opid()].into(),
            }],
        }
    }

    pub(crate) fn rollback_increment(&mut self, by: i64) {
        let counter = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Leaf(Primitive::Counter(c)) => c,
            _ => unreachable!(),
        };
        *counter -= by;
    }
}

pub struct ResolvedMap<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a MultiValue,
}

pub struct ResolvedMapMut<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedMapMut<'a> {
    pub(crate) fn set_key(
        &mut self,
        key: SmolStr,
        payload: SetOrInsertPayload<Value>,
    ) -> (Option<MultiValue>, LocalOperationResult) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            parent_obj: &state_tree_map.object_id,
            key: amp::Key::Map(key.clone()),
            value: payload.value,
            insert: false,
            pred: state_tree_map.pred_for_key(&key),
        });
        let (multivalue, new_ops, _new_cursors) = newvalue.finish();
        let old = state_tree_map.props.insert(key, multivalue);
        (old, LocalOperationResult { new_ops })
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> (MultiValue, LocalOperationResult) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        let old = state_tree_map
            .props
            .remove(key)
            .expect("Removing non existent key from map");
        (
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                    obj: state_tree_map.object_id.clone(),
                    key: key.into(),
                    insert: false,
                    pred: state_tree_map.pred_for_key(key),
                }],
            },
        )
    }

    pub(crate) fn rollback_set(&mut self, key: SmolStr, value: Option<MultiValue>) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        match value {
            Some(old) => {
                state_tree_map.props.insert(key, old);
            }
            None => {
                state_tree_map.props.remove(&key);
            }
        }
    }

    pub(crate) fn rollback_delete(&mut self, key: SmolStr, value: MultiValue) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        state_tree_map.props.insert(key, value);
    }
}

pub struct ResolvedTable<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a MultiValue,
}

pub struct ResolvedTableMut<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedTableMut<'a> {
    pub(crate) fn set_key(
        &mut self,
        key: SmolStr,
        payload: SetOrInsertPayload<Value>,
    ) -> (Option<MultiValue>, LocalOperationResult) {
        let state_tree_table = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            parent_obj: &state_tree_table.object_id,
            key: amp::Key::Map(key.clone()),
            value: payload.value,
            insert: false,
            pred: state_tree_table.pred_for_key(&key),
        });
        let (multivalue, new_ops, _new_cursors) = newvalue.finish();
        let old = state_tree_table.props.insert(key, multivalue);
        (old, LocalOperationResult { new_ops })
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> (MultiValue, LocalOperationResult) {
        let state_tree_table = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        let old = state_tree_table
            .props
            .remove(key)
            .expect("Removing non existent key from table");
        (
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                    obj: state_tree_table.object_id.clone(),
                    key: key.into(),
                    insert: false,
                    pred: state_tree_table.pred_for_key(key),
                }],
            },
        )
    }

    pub(crate) fn rollback_set(&mut self, key: SmolStr, value: Option<MultiValue>) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        match value {
            Some(old) => {
                state_tree_map.props.insert(key, old);
            }
            None => {
                state_tree_map.props.remove(&key);
            }
        }
    }

    pub(crate) fn rollback_delete(&mut self, key: SmolStr, value: MultiValue) {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        state_tree_map.props.insert(key, value);
    }
}

pub struct ResolvedText<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a MultiValue,
}

pub struct ResolvedTextMut<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedTextMut<'a> {
    #[allow(dead_code)]
    pub(crate) fn insert(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<SmolStr>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let current_elemid = match index {
            0 => amp::ElementId::Head,
            i => state_tree_text
                .elem_at((i - 1).try_into().unwrap())?
                .0
                .into(),
        };
        let insert_op = amp::OpId::new(payload.start_op, payload.actor);
        let c = MultiGrapheme::new_from_grapheme_cluster(insert_op, payload.value.clone());
        state_tree_text.insert(index.try_into().unwrap(), c)?;
        Ok(LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value)),
                obj: state_tree_text.object_id.clone(),
                key: current_elemid.into(),
                insert: true,
                pred: SortedVec::new(),
            }],
        })
    }

    pub(crate) fn insert_many<I>(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<I>,
    ) -> Result<LocalOperationResult, error::MissingIndexError>
    where
        I: ExactSizeIterator<Item = SmolStr>,
    {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let current_elemid = match index {
            0 => amp::ElementId::Head,
            i => state_tree_text
                .elem_at((i - 1).try_into().unwrap())?
                .0
                .into(),
        };
        let mut values = Vec::with_capacity(payload.value.len());
        let mut chars: Vec<amp::ScalarValue> = Vec::with_capacity(payload.value.len());
        for (i, c) in payload.value.enumerate() {
            let insert_op = amp::OpId::new(payload.start_op + i as u64, payload.actor);
            chars.push(amp::ScalarValue::Str(c.clone()));
            let c = MultiGrapheme::new_from_grapheme_cluster(insert_op, c);
            values.push(c)
        }
        state_tree_text.insert_many(index.try_into().unwrap(), values)?;
        let action = match chars.len() {
            1 => amp::OpType::Set(chars[0].clone()),
            _ => amp::OpType::MultiSet(chars),
        };
        Ok(LocalOperationResult {
            new_ops: vec![amp::Op {
                action,
                obj: state_tree_text.object_id.clone(),
                key: current_elemid.into(),
                insert: true,
                pred: SortedVec::new(),
            }],
        })
    }

    pub(crate) fn set(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<SmolStr>,
    ) -> Result<(MultiGrapheme, LocalOperationResult), error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let index: usize = index.try_into().unwrap();
        let (current_elemid, _) = state_tree_text.elem_at(index)?;
        let current_elemid = current_elemid.clone();
        let update_op = amp::OpId::new(payload.start_op, payload.actor);
        let c = MultiGrapheme::new_from_grapheme_cluster(update_op, payload.value.clone());
        let pred = state_tree_text.pred_for_index(index as u32);
        let old = state_tree_text.set(index, c)?;
        Ok((
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str(payload.value)),
                    obj: state_tree_text.object_id.clone(),
                    key: current_elemid.into(),
                    pred,
                    insert: false,
                }],
            },
        ))
    }

    pub(crate) fn remove(
        &mut self,
        index: u32,
    ) -> Result<(MultiGrapheme, LocalOperationResult), error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_text.elem_at(index.try_into().unwrap())?;
        let current_elemid = current_elemid.clone();
        let pred = state_tree_text.pred_for_index(index as u32);
        let old = state_tree_text.remove(index.try_into().unwrap())?;
        Ok((
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                    obj: state_tree_text.object_id.clone(),
                    key: current_elemid.into(),
                    insert: false,
                    pred,
                }],
            },
        ))
    }

    pub(crate) fn rollback_set(&mut self, index: usize, value: MultiGrapheme) {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        state_tree_text
            .set(index, value)
            .expect("Failed to rollback set");
    }

    pub(crate) fn rollback_delete(&mut self, index: usize, value: MultiGrapheme) {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        state_tree_text
            .insert(index, value)
            .expect("Failed to rollback delete");
    }

    pub(crate) fn rollback_insert(&mut self, index: usize) {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        state_tree_text
            .remove(index)
            .expect("Failed to rollback insert");
    }
}

impl<'a> ResolvedText<'a> {
    pub(crate) fn get_cursor(&self, index: u32) -> Result<Cursor, error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_text.elem_at(index.try_into().unwrap())?;
        Ok(Cursor::new(
            index,
            state_tree_text.object_id.clone(),
            current_elemid.clone(),
        ))
    }
}

pub struct ResolvedList<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a MultiValue,
}

pub struct ResolvedListMut<'a> {
    object_id: amp::ObjectId,
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedListMut<'a> {
    pub(crate) fn set(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<Value>,
    ) -> Result<(MultiValue, LocalOperationResult), error::MissingIndexError> {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_list.elem_at(index.try_into().unwrap())?;
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            value: payload.value,
            pred: state_tree_list.pred_for_index(index),
            parent_obj: &state_tree_list.object_id.clone(),
            key: current_elemid.into(),
            insert: false,
        });
        let (multivalue, new_ops, _new_cursors) = newvalue.finish();
        let old = state_tree_list.set(index as usize, multivalue)?;
        Ok((old, LocalOperationResult { new_ops }))
    }

    #[allow(dead_code)]
    pub(crate) fn insert(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<Value>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let current_elemid = match index {
            0 => amp::ElementId::Head,
            i => state_tree_list
                .elem_at((i - 1).try_into().unwrap())?
                .0
                .clone()
                .into(),
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            value: payload.value,
            parent_obj: &state_tree_list.object_id,
            key: current_elemid.into(),
            insert: true,
            pred: SortedVec::new(),
        });
        let (multivalue, new_ops, _new_cursors) = newvalue.finish();
        state_tree_list.insert(index as usize, multivalue)?;
        Ok(LocalOperationResult { new_ops })
    }

    pub(crate) fn insert_many<I>(
        &'a mut self,
        index: u32,
        payload: SetOrInsertPayload<I>,
    ) -> Result<LocalOperationResult, error::MissingIndexError>
    where
        I: ExactSizeIterator<Item = Value>,
    {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let mut last_elemid = match index {
            0 => amp::ElementId::Head,
            i => state_tree_list
                .elem_at((i - 1).try_into().unwrap())?
                .0
                .clone()
                .into(),
        };
        let mut newvalues = Vec::with_capacity(payload.value.len());
        let mut op_num = payload.start_op;
        let mut ops = Vec::new();
        for value in payload.value {
            let newvalue = MultiValue::new_from_value_2(NewValueRequest {
                actor: payload.actor,
                start_op: op_num,
                value,
                parent_obj: &state_tree_list.object_id,
                key: last_elemid.clone().into(),
                insert: true,
                pred: SortedVec::new(),
            });
            last_elemid = amp::OpId::new(op_num, payload.actor).into();
            op_num = newvalue.max_op() + 1;
            let (multivalue, new_ops, _new_cursors) = newvalue.finish();
            newvalues.push(multivalue);
            ops.extend(new_ops);
        }
        state_tree_list.insert_many(index as usize, newvalues)?;
        Ok(LocalOperationResult {
            new_ops: condense_insert_ops(ops),
        })
    }

    pub(crate) fn remove(
        &mut self,
        index: u32,
    ) -> Result<(MultiValue, LocalOperationResult), error::MissingIndexError> {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_list.elem_at(index.try_into().unwrap())?;
        let current_elemid = current_elemid.clone();
        let pred = state_tree_list.pred_for_index(index);
        let old = state_tree_list.remove(index as usize)?;
        Ok((
            old,
            LocalOperationResult {
                new_ops: vec![amp::Op {
                    action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                    obj: state_tree_list.object_id.clone(),
                    key: current_elemid.into(),
                    insert: false,
                    pred,
                }],
            },
        ))
    }

    pub(crate) fn rollback_set(&mut self, index: usize, value: MultiValue) {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        state_tree_list
            .set(index, value)
            .expect("Failed to rollback set");
    }

    pub(crate) fn rollback_delete(&mut self, index: usize, value: MultiValue) {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        state_tree_list
            .insert(index, value)
            .expect("Failed to rollback delete");
    }

    pub(crate) fn rollback_insert(&mut self, index: usize) {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        state_tree_list
            .remove(index)
            .expect("Failed to rollback insert");
    }
}

impl<'a> ResolvedList<'a> {
    pub(crate) fn get_cursor(&self, index: u32) -> Result<Cursor, error::MissingIndexError> {
        let state_tree_list = match self.multivalue.default_statetree_value() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_list.elem_at(index.try_into().unwrap())?;
        Ok(Cursor::new(
            index,
            state_tree_list.object_id.clone(),
            current_elemid.clone(),
        ))
    }
}

pub struct ResolvedChar<'a> {
    pub(super) multivalue: &'a MultiGrapheme,
}

pub struct ResolvedCharMut<'a> {
    pub(super) multivalue: &'a mut MultiGrapheme,
}

pub struct ResolvedPrimitive<'a> {
    pub(super) multivalue: &'a MultiValue,
}

pub struct ResolvedPrimitiveMut<'a> {
    pub(super) multivalue: &'a mut MultiValue,
}

fn condense_insert_ops(ops: Vec<amp::Op>) -> Vec<amp::Op> {
    if ops.len() == 1 {
        return ops;
    }
    let mut op_iter = ops.iter();
    let mut prim_vals = Vec::with_capacity(ops.len());
    let mut preds = Vec::new();
    if let Some(v) = op_iter.next() {
        if let Some(prim) = prim_from_op_action(&v.action) {
            prim_vals.push(prim);
            preds.extend(v.pred.iter());
        }
        for o in op_iter {
            if let Some(prim) = prim_from_op_action(&o.action) {
                prim_vals.push(prim);
                preds.extend(o.pred.iter());
            }
        }
        if prim_vals.len() == ops.len() {
            vec![amp::Op {
                action: amp::OpType::MultiSet(prim_vals),
                pred: preds.into_iter().cloned().collect(),
                insert: true,
                key: v.key.clone(),
                obj: v.obj.clone(),
            }]
        } else {
            ops
        }
    } else {
        ops
    }
}

fn prim_from_op_action(action: &amp::OpType) -> Option<amp::ScalarValue> {
    match action {
        amp::OpType::Set(v) => match v {
            amp::ScalarValue::Bytes(_) => Some(v.clone()),
            amp::ScalarValue::Str(_) => Some(v.clone()),
            amp::ScalarValue::Int(_) => Some(v.clone()),
            amp::ScalarValue::Uint(_) => Some(v.clone()),
            amp::ScalarValue::F64(_) => Some(v.clone()),
            amp::ScalarValue::F32(_) => Some(v.clone()),
            amp::ScalarValue::Counter(_) => None,
            amp::ScalarValue::Timestamp(_) => None,
            amp::ScalarValue::Cursor(_) => None,
            amp::ScalarValue::Boolean(_) => Some(v.clone()),
            amp::ScalarValue::Null => Some(v.clone()),
        },
        _ => None,
    }
}
