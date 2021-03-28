use super::focus::Focus;
use super::{
    random_op_id, DiffApplicationResult, LocalOperationResult, MultiGrapheme, MultiValue,
    NewValueRequest, StateTree, StateTreeChange, StateTreeComposite, StateTreeList, StateTreeMap,
    StateTreeTable, StateTreeText, StateTreeValue,
};
use crate::error;
use crate::{Cursor, Primitive, Value};
use automerge_protocol as amp;
use std::convert::TryInto;

#[derive(Debug)]
pub struct ResolvedPath<'a> {
    root: &'a StateTree,
    pub(crate) target: Target,
}

pub enum Target {
    Root(ResolvedRoot),
    Map(ResolvedMap),
    Table(ResolvedTable),
    List(ResolvedList),
    Text(ResolvedText),
    Character(ResolvedChar),
    Counter(ResolvedCounter),
    Primitive(ResolvedPrimitive),
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Target::Map(maptarget) => write!(f, "MapTarget {:?}", maptarget.value.object_id),
            Target::Root(_) => write!(f, "Root"),
            Target::Table(tabletarget) => {
                write!(f, "Table {:?}", tabletarget.value.object_id)
            }
            Target::List(listtarget) => write!(f, "list {:?}", listtarget.value.object_id),
            Target::Text(texttarget) => write!(f, "text {:?}", texttarget.value.object_id),
            Target::Counter(countertarget) => write!(
                f,
                "counter {0}:{1:?}",
                countertarget.containing_object_id, countertarget.key_in_container
            ),
            Target::Primitive(p) => write!(f, "primitive: {:?}", p.multivalue),
            Target::Character(ctarget) => write!(f, "character {:?}", ctarget.multivalue),
        }
    }
}

impl<'a> ResolvedPath<'a> {
    pub(super) fn new_root(tree: &StateTree) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Root(ResolvedRoot { root: tree.clone() }),
        }
    }

    pub(super) fn new_map(
        tree: &StateTree,
        mv: MultiValue,
        focus: Focus,
        map: StateTreeMap,
    ) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Map(ResolvedMap {
                multivalue: mv,
                value: map,
                focus,
            }),
        }
    }

    pub(super) fn new_list(
        tree: &StateTree,
        mv: MultiValue,
        focus: Focus,
        list: StateTreeList,
    ) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::List(ResolvedList {
                multivalue: mv,
                focus,
                value: list,
            }),
        }
    }

    pub(super) fn new_text(
        tree: &StateTree,
        mv: MultiValue,
        update: Box<dyn Fn(DiffApplicationResult<MultiValue>) -> StateTree>,
        text: StateTreeText,
    ) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Text(ResolvedText {
                multivalue: mv,
                value: text,
                update,
            }),
        }
    }

    pub(super) fn new_table(
        tree: &StateTree,
        mv: MultiValue,
        focus: Focus,
        table: StateTreeTable,
    ) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Table(ResolvedTable {
                multivalue: mv,
                focus,
                value: table,
            }),
        }
    }

    pub(super) fn new_counter(
        tree: &StateTree,
        object_id: amp::ObjectId,
        key: amp::Key,
        mv: MultiValue,
        focus: Focus,
        value: i64,
    ) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Counter(ResolvedCounter {
                multivalue: mv,
                key_in_container: key,
                containing_object_id: object_id,
                current_value: value,
                focus,
            }),
        }
    }

    pub(super) fn new_primitive(tree: &StateTree, value: MultiValue) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Primitive(ResolvedPrimitive { multivalue: value }),
        }
    }

    pub(super) fn new_character(tree: &StateTree, c: MultiValue) -> ResolvedPath {
        ResolvedPath {
            root: tree,
            target: Target::Character(ResolvedChar { multivalue: c }),
        }
    }

    pub fn default_value(&self) -> Value {
        match &self.target {
            Target::Map(maptarget) => maptarget.multivalue.default_value(&self.root.objects),
            Target::Root(root) => root.root.value(),
            Target::Table(tabletarget) => tabletarget.multivalue.default_value(&self.root.objects),
            Target::List(listtarget) => listtarget.multivalue.default_value(&self.root.objects),
            Target::Text(texttarget) => texttarget.multivalue.default_value(&self.root.objects),
            Target::Counter(countertarget) => {
                countertarget.multivalue.default_value(&self.root.objects)
            }
            Target::Primitive(p) => p.multivalue.default_value(&self.root.objects),
            Target::Character(ctarget) => ctarget.multivalue.default_value(&self.root.objects),
        }
    }

    pub fn values(&self) -> std::collections::HashMap<amp::OpId, Value> {
        match &self.target {
            Target::Map(maptarget) => maptarget.multivalue.realise_values(&self.root.objects),
            Target::Root(root) => {
                let mut result = std::collections::HashMap::new();
                result.insert(random_op_id(), root.root.value());
                result
            }
            Target::Table(tabletarget) => tabletarget.multivalue.realise_values(&self.root.objects),
            Target::List(listtarget) => listtarget.multivalue.realise_values(&self.root.objects),
            Target::Text(texttarget) => texttarget.multivalue.realise_values(&self.root.objects),
            Target::Counter(countertarget) => {
                countertarget.multivalue.realise_values(&self.root.objects)
            }
            Target::Primitive(p) => p.multivalue.realise_values(&self.root.objects),
            Target::Character(ctarget) => ctarget.multivalue.realise_values(&self.root.objects),
        }
    }

    pub fn object_id(&self) -> Option<amp::ObjectId> {
        match &self.target {
            Target::Map(maptarget) => Some(maptarget.value.object_id.clone()),
            Target::Root(_) => Some(amp::ObjectId::Root),
            Target::Table(tabletarget) => Some(tabletarget.value.object_id.clone()),
            Target::List(listtarget) => Some(listtarget.value.object_id.clone()),
            Target::Text(texttarget) => Some(texttarget.value.object_id.clone()),
            Target::Counter(_) => None,
            Target::Primitive(_) => None,
            Target::Character(_) => None,
        }
    }
}

pub(crate) struct SetOrInsertPayload<'a, T> {
    pub start_op: u64,
    pub actor: &'a amp::ActorId,
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
            parent_obj: &amp::ObjectId::Root,
            value: payload.value,
            insert: false,
            pred: self
                .root
                .get(key)
                .map(|mv| vec![mv.default_opid()])
                .unwrap_or_else(Vec::new),
        });
        let new_state = self
            .root
            .update(key.to_string(), newvalue.diff_app_result());
        LocalOperationResult {
            new_state,
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&self, key: &str) -> LocalOperationResult {
        let existing_value = self.root.get(key);
        let pred = existing_value
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new);
        LocalOperationResult {
            new_state: self.root.remove(key),
            new_ops: vec![amp::Op {
                action: amp::OpType::Del,
                obj: amp::ObjectId::Root,
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
    pub(super) containing_object_id: amp::ObjectId,
    pub(super) key_in_container: amp::Key,
    pub(super) focus: Focus,
}

impl ResolvedCounter {
    pub(crate) fn increment(&self, by: i64) -> LocalOperationResult {
        let diffapp = DiffApplicationResult::pure(self.multivalue.update_default(
            StateTreeValue::Leaf(Primitive::Counter(self.current_value + by)),
        ));
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
    pub(super) focus: Focus,
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
        let diffapp = newvalue.diff_app_result().and_then(|v| {
            let new_value = self.value.update(key.to_string(), v);
            let new_composite = StateTreeComposite::Map(new_value);
            let new_mv = self
                .multivalue
                .update_default(StateTreeValue::Link(new_composite.object_id()));
            DiffApplicationResult::pure(new_mv).with_changes(StateTreeChange::single(
                self.value.object_id.clone(),
                new_composite,
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
            .update_default(StateTreeValue::Link(new_composite.object_id()));
        let diffapp = DiffApplicationResult::pure(new_mv).with_changes(StateTreeChange::single(
            new_composite.object_id(),
            new_composite,
        ));
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
    pub(super) focus: Focus,
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
        let treechange = newvalue.diff_app_result().and_then(|v| {
            let new_value = self.value.update(key.to_string(), v);
            let new_composite = StateTreeComposite::Table(new_value);
            let new_mv = self
                .multivalue
                .update_default(StateTreeValue::Link(new_composite.object_id()));
            DiffApplicationResult::pure(new_mv).with_changes(StateTreeChange::single(
                self.value.object_id.clone(),
                new_composite,
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
            .update_default(StateTreeValue::Link(new_composite.object_id()));
        let diffapp = DiffApplicationResult::pure(new_mv).with_changes(StateTreeChange::single(
            new_composite.object_id(),
            new_composite,
        ));
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
    pub(super) update: Box<dyn Fn(DiffApplicationResult<MultiValue>) -> StateTree>,
}

impl ResolvedText {
    pub(crate) fn insert(
        &self,
        index: u32,
        payload: SetOrInsertPayload<String>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let current_elemid = match index {
            0 => amp::ElementId::Head,
            i => self.value.elem_at((i - 1).try_into().unwrap())?.0.into(),
        };
        let insert_op = amp::OpId::new(payload.start_op, payload.actor);
        let c = MultiGrapheme::new_from_grapheme_cluster(insert_op, payload.value.clone());
        let new_text = self.value.insert(index.try_into().unwrap(), c)?;
        let updated = StateTreeComposite::Text(new_text);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Link(updated.object_id()));
        let treechange = DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
            self.value.object_id.clone(),
            updated,
        ));
        Ok(LocalOperationResult {
            new_state: (self.update)(treechange),
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value)),
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
        payload: SetOrInsertPayload<String>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let index: usize = index.try_into().unwrap();
        let (current_elemid, _) = self.value.elem_at(index)?;
        let update_op = amp::OpId::new(payload.start_op, payload.actor);
        let c = MultiGrapheme::new_from_grapheme_cluster(update_op, payload.value.clone());
        let updated = StateTreeComposite::Text(self.value.set(index, c)?);
        let mv = self
            .multivalue
            .update_default(StateTreeValue::Link(updated.object_id()));
        let diffapp = DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
            self.value.object_id.clone(),
            updated,
        ));
        let new_state = (self.update)(diffapp);
        Ok(LocalOperationResult {
            new_state,
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value)),
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
            .update_default(StateTreeValue::Link(updated.object_id()));
        let diffapp = DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
            self.value.object_id.clone(),
            updated,
        ));
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

    pub(crate) fn get_cursor(&self, index: u32) -> Result<Cursor, error::MissingIndexError> {
        let (current_elemid, _) = self.value.elem_at(index.try_into().unwrap())?;
        Ok(Cursor::new(
            index,
            self.value.object_id.clone(),
            current_elemid.clone(),
        ))
    }
}

pub struct ResolvedList {
    pub(super) value: StateTreeList,
    pub(super) multivalue: MultiValue,
    pub(super) focus: Focus,
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
        let treechange = newvalue.diff_app_result().try_and_then(|v| {
            let new_value = StateTreeComposite::List(self.value.set(index.try_into().unwrap(), v)?);
            let mv = self
                .multivalue
                .update_default(StateTreeValue::Link(new_value.object_id()));
            Ok(
                DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
                    self.value.object_id.clone(),
                    new_value,
                )),
            )
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
            0 => amp::ElementId::Head,
            i => self.value.elem_at((i - 1).try_into().unwrap())?.0.into(),
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
        let treechange = newvalue.diff_app_result().try_and_then(|v| {
            let new_value =
                StateTreeComposite::List(self.value.insert(index.try_into().unwrap(), v)?);
            let mv = self
                .multivalue
                .update_default(StateTreeValue::Link(new_value.object_id()));
            Ok(
                DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
                    self.value.object_id.clone(),
                    new_value,
                )),
            )
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
            .update_default(StateTreeValue::Link(new_value.object_id()));
        let treechange = DiffApplicationResult::pure(mv).with_changes(StateTreeChange::single(
            self.value.object_id.clone(),
            new_value,
        ));
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

    pub(crate) fn get_cursor(&self, index: u32) -> Result<Cursor, error::MissingIndexError> {
        let (current_elemid, _) = self.value.elem_at(index.try_into().unwrap())?;
        Ok(Cursor::new(
            index,
            self.value.object_id.clone(),
            current_elemid,
        ))
    }
}

pub struct ResolvedChar {
    pub(super) multivalue: MultiValue,
}

pub struct ResolvedPrimitive {
    pub(super) multivalue: MultiValue,
}
