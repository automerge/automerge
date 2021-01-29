use super::focus::Focus;
use super::{
    random_op_id, LocalOperationResult, MultiChar, MultiValue, NewValueRequest, StateTree,
    StateTreeChange, StateTreeComposite, StateTreeList, StateTreeMap, StateTreeTable,
    StateTreeText, StateTreeValue,
};
use crate::error;
use crate::Value;
use automerge_protocol as amp;
use im_rc::hashmap;
use std::convert::TryInto;

pub enum ResolvedPath {
    Root(ResolvedRoot),
    Map(ResolvedMap),
    Table(ResolvedTable),
    List(ResolvedList),
    Text(ResolvedText),
    Character(ResolvedChar),
    Counter(ResolvedCounter),
    Primitive(ResolvedPrimitive),
}

impl std::fmt::Debug for ResolvedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResolvedPath::Map(maptarget) => write!(f, "MapTarget {:?}", maptarget.value.object_id),
            ResolvedPath::Root(_) => write!(f, "Root"),
            ResolvedPath::Table(tabletarget) => {
                write!(f, "Table {:?}", tabletarget.value.object_id)
            }
            ResolvedPath::List(listtarget) => write!(f, "list {:?}", listtarget.value.object_id),
            ResolvedPath::Text(texttarget) => write!(f, "text {:?}", texttarget.value.object_id),
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

impl ResolvedPath {
    pub fn default_value(&self) -> Value {
        match self {
            ResolvedPath::Map(maptarget) => maptarget.multivalue.default_value(),
            ResolvedPath::Root(root) => root.root.value(),
            ResolvedPath::Table(tabletarget) => tabletarget.multivalue.default_value(),
            ResolvedPath::List(listtarget) => listtarget.multivalue.default_value(),
            ResolvedPath::Text(texttarget) => texttarget.multivalue.default_value(),
            ResolvedPath::Counter(countertarget) => countertarget.multivalue.default_value(),
            ResolvedPath::Primitive(p) => p.multivalue.default_value(),
            ResolvedPath::Character(ctarget) => ctarget.multivalue.default_value(),
        }
    }

    pub fn values(&self) -> std::collections::HashMap<amp::OpID, Value> {
        match self {
            ResolvedPath::Map(maptarget) => maptarget.multivalue.values(),
            ResolvedPath::Root(root) => {
                let mut result = std::collections::HashMap::new();
                result.insert(random_op_id(), root.root.value());
                result
            }
            ResolvedPath::Table(tabletarget) => tabletarget.multivalue.values(),
            ResolvedPath::List(listtarget) => listtarget.multivalue.values(),
            ResolvedPath::Text(texttarget) => texttarget.multivalue.values(),
            ResolvedPath::Counter(countertarget) => countertarget.multivalue.values(),
            ResolvedPath::Primitive(p) => p.multivalue.values(),
            ResolvedPath::Character(ctarget) => ctarget.multivalue.values(),
        }
    }

    pub fn object_id(&self) -> Option<amp::ObjectID> {
        match self {
            ResolvedPath::Map(maptarget) => Some(maptarget.value.object_id.clone()),
            ResolvedPath::Root(_) => Some(amp::ObjectID::Root),
            ResolvedPath::Table(tabletarget) => Some(tabletarget.value.object_id.clone()),
            ResolvedPath::List(listtarget) => Some(listtarget.value.object_id.clone()),
            ResolvedPath::Text(texttarget) => Some(texttarget.value.object_id.clone()),
            ResolvedPath::Counter(_) => None,
            ResolvedPath::Primitive(_) => None,
            ResolvedPath::Character(_) => None,
        }
    }
}

pub(crate) struct SetOrInsertPayload<'a, T> {
    pub start_op: u64,
    pub actor: &'a amp::ActorID,
    pub value: T,
}

pub struct ResolvedRoot {
    pub(super) root: StateTree,
}

impl ResolvedRoot {
    pub(crate) fn set_key(
        &self,
        key: &str,
        payload: SetOrInsertPayload<&Value>,
    ) -> LocalOperationResult {
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            key: &key.into(),
            parent_obj: &amp::ObjectID::Root,
            value: payload.value,
            insert: false,
            pred: self
                .root
                .root_map
                .get(key)
                .map(|mv| vec![mv.default_opid()])
                .unwrap_or_else(Vec::new),
        });
        let new_state = self
            .root
            .update(key.to_string(), newvalue.state_tree_change());
        LocalOperationResult {
            new_state,
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&self, key: &str) -> LocalOperationResult {
        let existing_value = self.root.root_map.get(key);
        let pred = existing_value
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new);
        LocalOperationResult {
            new_state: self.root.remove(key),
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: amp::ObjectID::Root,
                key: key.into(),
                insert: false,
                pred,
            }],
        }
    }
}

pub struct ResolvedCounter {
    pub(super) current_value: i64,
    pub(super) multivalue: MultiValue,
    pub(super) containing_object_id: amp::ObjectID,
    pub(super) key_in_container: amp::Key,
    pub(super) focus: Box<Focus>,
}

impl ResolvedCounter {
    pub(crate) fn increment(&self, by: i64) -> LocalOperationResult {
        let diffapp = StateTreeChange::pure(self.multivalue.update_default(StateTreeValue::Leaf(
            amp::ScalarValue::Counter(self.current_value + by),
        )));
        let new_state = self.focus.update(diffapp);
        LocalOperationResult {
            new_state,
            new_ops: vec![amp::Op {
                action: amp::OpType::Inc(by),
                obj: self.containing_object_id.clone(),
                key: self.key_in_container.clone(),
                insert: false,
                pred: vec![self.multivalue.default_opid()],
            }],
        }
    }
}

pub struct ResolvedMap {
    pub(super) value: StateTreeMap,
    pub(super) multivalue: MultiValue,
    pub(super) focus: Box<Focus>,
}

impl ResolvedMap {
    pub(crate) fn set_key(
        &self,
        key: &str,
        payload: SetOrInsertPayload<&Value>,
    ) -> LocalOperationResult {
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            parent_obj: &self.value.object_id,
            key: &key.into(),
            value: payload.value,
            insert: false,
            pred: self.value.pred_for_key(key),
        });
        let diffapp = newvalue.state_tree_change().and_then(|v| {
            let new_value = self.value.update(key.to_string(), v);
            let new_composite = StateTreeComposite::Map(new_value);
            let new_mv = self
                .multivalue
                .update_default(StateTreeValue::Composite(new_composite.clone()));
            StateTreeChange::pure(new_mv).with_updates(Some(
                im_rc::HashMap::new().update(self.value.object_id.clone(), new_composite),
            ))
        });
        LocalOperationResult {
            new_state: self.focus.update(diffapp),
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&self, key: &str) -> LocalOperationResult {
        let new_value = self.value.without(key);
        let new_composite = StateTreeComposite::Map(new_value);
        let new_mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(new_composite));
        let diffapp = StateTreeChange::pure(new_mv);
        LocalOperationResult {
            new_state: self.focus.update(diffapp),
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: self.value.object_id.clone(),
                key: key.into(),
                insert: false,
                pred: self.value.pred_for_key(key),
            }],
        }
    }
}

pub struct ResolvedTable {
    pub(super) value: StateTreeTable,
    pub(super) multivalue: MultiValue,
    pub(super) focus: Box<Focus>,
}

impl ResolvedTable {
    pub(crate) fn set_key(
        &self,
        key: &str,
        payload: SetOrInsertPayload<&Value>,
    ) -> LocalOperationResult {
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            parent_obj: &self.value.object_id,
            key: &key.into(),
            value: payload.value,
            insert: false,
            pred: self.value.pred_for_key(key),
        });
        let treechange = newvalue.state_tree_change().and_then(|v| {
            let new_value = self.value.update(key.to_string(), v);
            let new_composite = StateTreeComposite::Table(new_value);
            let new_mv = self
                .multivalue
                .update_default(StateTreeValue::Composite(new_composite.clone()));
            StateTreeChange::pure(new_mv).with_updates(Some(
                hashmap!(self.value.object_id.clone() => new_composite),
            ))
        });
        LocalOperationResult {
            new_state: self.focus.update(treechange),
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&self, key: &str) -> LocalOperationResult {
        let new_value = self.value.without(key);
        let new_composite = StateTreeComposite::Table(new_value);
        let new_mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(new_composite));
        let diffapp = StateTreeChange::pure(new_mv);
        LocalOperationResult {
            new_state: self.focus.update(diffapp),
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: self.value.object_id.clone(),
                key: key.into(),
                insert: false,
                pred: self.value.pred_for_key(key),
            }],
        }
    }
}

pub struct ResolvedText {
    pub(super) value: StateTreeText,
    pub(super) multivalue: MultiValue,
    pub(super) update: Box<dyn Fn(StateTreeChange<MultiValue>) -> StateTree>,
}

impl ResolvedText {
    pub(crate) fn insert(
        &self,
        index: u32,
        payload: SetOrInsertPayload<char>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let current_elemid = match index {
            0 => amp::ElementID::Head,
            i => self.value.elem_at((i - 1).try_into().unwrap())?.0,
        };
        let insert_op = amp::OpID::new(payload.start_op, payload.actor);
        let c = MultiChar::new_from_char(insert_op, payload.value);
        let new_text = self.value.insert(index.try_into().unwrap(), c)?;
        let updated = StateTreeComposite::Text(new_text);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(updated.clone()));
        let treechange = StateTreeChange::pure(mv)
            .with_updates(Some(hashmap!(self.value.object_id.clone() => updated)));
        Ok(LocalOperationResult {
            new_state: (self.update)(treechange),
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value.to_string())),
                obj: self.value.object_id.clone(),
                key: current_elemid.into(),
                insert: true,
                pred: Vec::new(),
            }],
        })
    }

    pub(crate) fn set(
        &self,
        index: u32,
        payload: SetOrInsertPayload<char>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let index: usize = index.try_into().unwrap();
        let (current_elemid, _) = self.value.elem_at(index)?;
        let update_op = amp::OpID::new(payload.start_op, payload.actor);
        let c = MultiChar::new_from_char(update_op, payload.value);
        let updated = StateTreeComposite::Text(self.value.set(index, c)?);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(updated.clone()));
        let diffapp = StateTreeChange::pure(mv)
            .with_updates(Some(hashmap!(self.value.object_id.clone() => updated)));
        let new_state = (self.update)(diffapp);
        Ok(LocalOperationResult {
            new_state,
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value.to_string())),
                obj: self.value.object_id.clone(),
                key: current_elemid.into(),
                pred: self.value.pred_for_index(index as u32),
                insert: false,
            }],
        })
    }

    pub(crate) fn remove(
        &self,
        index: u32,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let (current_elemid, _) = self.value.elem_at(index.try_into().unwrap())?;
        let updated = StateTreeComposite::Text(self.value.remove(index.try_into().unwrap())?);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(updated.clone()));
        let diffapp = StateTreeChange::pure(mv)
            .with_updates(Some(hashmap!(self.value.object_id.clone() => updated)));
        let new_state = (self.update)(diffapp);
        Ok(LocalOperationResult {
            new_state,
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: self.value.object_id.clone(),
                key: current_elemid.into(),
                insert: false,
                pred: self.value.pred_for_index(index as u32),
            }],
        })
    }
}

pub struct ResolvedList {
    pub(super) value: StateTreeList,
    pub(super) multivalue: MultiValue,
    pub(super) focus: Box<Focus>,
}

impl ResolvedList {
    pub(crate) fn set(
        &self,
        index: u32,
        payload: SetOrInsertPayload<&Value>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let (current_elemid, _) = self.value.elem_at(index.try_into().unwrap())?;
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            value: payload.value,
            pred: self.value.pred_for_index(index),
            parent_obj: &self.value.object_id.clone(),
            key: &current_elemid.into(),
            insert: false,
        });
        let treechange = newvalue.state_tree_change().fallible_and_then(|v| {
            let new_value = StateTreeComposite::List(self.value.set(index.try_into().unwrap(), v)?);
            let mv = self
                .multivalue
                .update_default(StateTreeValue::Composite(new_value.clone()));
            Ok(StateTreeChange::pure(mv)
                .with_updates(Some(hashmap!(self.value.object_id.clone() => new_value))))
        })?;
        let new_state = self.focus.update(treechange);
        Ok(LocalOperationResult {
            new_state,
            new_ops: newvalue.ops(),
        })
    }

    pub(crate) fn insert(
        &self,
        index: u32,
        payload: SetOrInsertPayload<&Value>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let current_elemid = match index {
            0 => amp::ElementID::Head,
            i => self.value.elem_at((i - 1).try_into().unwrap())?.0,
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            value: payload.value,
            parent_obj: &self.value.object_id,
            key: &current_elemid.into(),
            insert: true,
            pred: Vec::new(),
        });
        let treechange = newvalue.state_tree_change().fallible_and_then(|v| {
            let new_value =
                StateTreeComposite::List(self.value.insert(index.try_into().unwrap(), v)?);
            let mv = self
                .multivalue
                .update_default(StateTreeValue::Composite(new_value.clone()));
            Ok(StateTreeChange::pure(mv)
                .with_updates(Some(hashmap!(self.value.object_id.clone() => new_value))))
        })?;
        Ok(LocalOperationResult {
            new_state: self.focus.update(treechange),
            new_ops: newvalue.ops(),
        })
    }

    pub(crate) fn remove(
        &self,
        index: u32,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let (current_elemid, _) = self.value.elem_at(index.try_into().unwrap())?;
        let new_value = StateTreeComposite::List(self.value.remove(index.try_into().unwrap())?);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Composite(new_value.clone()));
        let treechange = StateTreeChange::pure(mv)
            .with_updates(Some(hashmap!(self.value.object_id.clone() => new_value)));
        Ok(LocalOperationResult {
            new_state: self.focus.update(treechange),
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: self.value.object_id.clone(),
                key: current_elemid.into(),
                insert: false,
                pred: self.value.pred_for_index(index),
            }],
        })
    }
}

pub struct ResolvedChar {
    pub(super) multivalue: MultiValue,
}

pub struct ResolvedPrimitive {
    pub(super) multivalue: MultiValue,
}
