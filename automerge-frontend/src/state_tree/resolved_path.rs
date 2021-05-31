use std::{convert::TryInto, num::NonZeroU32};

use automerge_protocol as amp;

use super::{
    focus::Focus, random_op_id, DiffApplicationResult, LocalOperationResult, MultiGrapheme,
    MultiValue, NewValueRequest, StateTree, StateTreeChange, StateTreeComposite, StateTreeList,
    StateTreeMap, StateTreeTable, StateTreeText, StateTreeValue,
};
use crate::{error, Cursor, Primitive, Value};

#[derive(Debug)]
pub struct ResolvedPath<'a> {
    pub(crate) target: Target<'a>,
}

pub enum Target<'a> {
    Root(ResolvedRoot<'a>),
    Map(ResolvedMap<'a>),
    Table(ResolvedTable<'a>),
    List(ResolvedList<'a>),
    Text(ResolvedText<'a>),
    Character(ResolvedChar<'a>),
    Counter(ResolvedCounter<'a>),
    Primitive(ResolvedPrimitive<'a>),
}

impl<'a> std::fmt::Debug for Target<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Target::Root(_) => write!(f, "Root"),
            Target::Map(maptarget) => {
                write!(f, "Map {:?}", maptarget.multivalue.default_object_id())
            }
            Target::Table(tabletarget) => {
                write!(f, "Table {:?}", tabletarget.multivalue.default_object_id())
            }
            Target::List(listtarget) => {
                write!(f, "list {:?}", listtarget.multivalue.default_object_id())
            }
            Target::Text(texttarget) => {
                write!(f, "text {:?}", texttarget.multivalue.default_object_id())
            }
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
    pub(super) fn new_root(root: &mut StateTree) -> ResolvedPath {
        ResolvedPath {
            target: Target::Root(ResolvedRoot { root }),
        }
    }

    pub(super) fn new_map(value: &'a mut MultiValue) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Map(ResolvedMap { multivalue: value }),
        }
    }

    pub(super) fn new_list(value: &'a mut MultiValue) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::List(ResolvedList { multivalue: value }),
        }
    }

    pub(super) fn new_text(mv: &'a mut MultiValue) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Text(ResolvedText { multivalue: mv }),
        }
    }

    pub(super) fn new_table(value: &'a mut MultiValue) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Table(ResolvedTable { multivalue: value }),
        }
    }

    pub(super) fn new_counter(
        object_id: amp::ObjectId,
        key: amp::Key,
        mv: &'a mut MultiValue,
        focus: Focus,
        value: i64,
    ) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Counter(ResolvedCounter {
                multivalue: mv,
                key_in_container: key,
                containing_object_id: object_id,
                current_value: value,
                focus,
            }),
        }
    }

    pub(super) fn new_primitive(value: &'a mut MultiValue) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Primitive(ResolvedPrimitive { multivalue: value }),
        }
    }

    pub(super) fn new_character(c: &'a mut MultiGrapheme) -> ResolvedPath<'a> {
        ResolvedPath {
            target: Target::Character(ResolvedChar { multivalue: c }),
        }
    }

    pub fn default_value(&self) -> Value {
        match &self.target {
            Target::Map(maptarget) => maptarget.multivalue.default_value(),
            Target::Root(root) => root.root.value(),
            Target::Table(tabletarget) => tabletarget.multivalue.default_value(),
            Target::List(listtarget) => listtarget.multivalue.default_value(),
            Target::Text(texttarget) => texttarget.multivalue.default_value(),
            Target::Counter(countertarget) => countertarget.multivalue.default_value(),
            Target::Primitive(p) => p.multivalue.default_value(),
            Target::Character(ctarget) => {
                Value::Primitive(Primitive::Str(ctarget.multivalue.default_grapheme()))
            }
        }
    }

    pub fn values(&self) -> std::collections::HashMap<amp::OpId, Value> {
        match &self.target {
            Target::Map(maptarget) => maptarget.multivalue.realise_values(),
            Target::Root(root) => {
                let mut result = std::collections::HashMap::new();
                result.insert(random_op_id(), root.root.value());
                result
            }
            Target::Table(tabletarget) => tabletarget.multivalue.realise_values(),
            Target::List(listtarget) => listtarget.multivalue.realise_values(),
            Target::Text(texttarget) => texttarget.multivalue.realise_values(),
            Target::Counter(countertarget) => countertarget.multivalue.realise_values(),
            Target::Primitive(p) => p.multivalue.realise_values(),
            Target::Character(ctarget) => ctarget.multivalue.realise_values(),
        }
    }

    pub fn object_id(&self) -> Option<amp::ObjectId> {
        match &self.target {
            Target::Map(maptarget) => Some(maptarget.multivalue.default_object_id().unwrap()),
            Target::Root(_) => Some(amp::ObjectId::Root),
            Target::Table(tabletarget) => Some(tabletarget.multivalue.default_object_id().unwrap()),
            Target::List(listtarget) => Some(listtarget.multivalue.default_object_id().unwrap()),
            Target::Text(texttarget) => Some(texttarget.multivalue.default_object_id().unwrap()),
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

pub struct ResolvedRoot<'a> {
    pub(super) root: &'a mut StateTree,
}

impl<'a> ResolvedRoot<'a> {
    pub(crate) fn set_key(
        &mut self,
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
        self.root
            .root_props
            .insert(key.to_string(), newvalue.multivalue());
        LocalOperationResult {
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> LocalOperationResult {
        let existing_value = self.root.get(key);
        let pred = existing_value
            .map(|v| vec![v.default_opid()])
            .unwrap_or_else(Vec::new);
        self.root.remove(key);
        LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: amp::ObjectId::Root,
                key: key.into(),
                insert: false,
                pred,
            }],
        }
    }
}

pub struct ResolvedCounter<'a> {
    pub(super) current_value: i64,
    pub(super) multivalue: &'a mut MultiValue,
    pub(super) containing_object_id: amp::ObjectId,
    pub(super) key_in_container: amp::Key,
    pub(super) focus: Focus,
}

impl<'a> ResolvedCounter<'a> {
    pub(crate) fn increment(&mut self, by: i64) -> LocalOperationResult {
        let diffapp = DiffApplicationResult::pure(self.multivalue.update_default(
            StateTreeValue::Leaf(Primitive::Counter(self.current_value + by)),
        ));
        LocalOperationResult {
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

pub struct ResolvedMap<'a> {
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedMap<'a> {
    pub(crate) fn set_key(
        &mut self,
        key: &str,
        payload: SetOrInsertPayload<&Value>,
    ) -> LocalOperationResult {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            // parent_obj: &self.multivalue.object_id(),
            parent_obj: &amp::ObjectId::Root,
            key: &key.into(),
            value: payload.value,
            insert: false,
            pred: state_tree_map.pred_for_key(key),
        });
        state_tree_map
            .props
            .insert(key.to_string(), newvalue.multivalue());
        LocalOperationResult {
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> LocalOperationResult {
        let state_tree_map = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Map(map)) => map,
            _ => unreachable!(),
        };
        state_tree_map.props.remove(key);
        LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: state_tree_map.object_id.clone(),
                key: key.into(),
                insert: false,
                pred: state_tree_map.pred_for_key(key),
            }],
        }
    }
}

pub struct ResolvedTable<'a> {
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedTable<'a> {
    pub(crate) fn set_key(
        &mut self,
        key: &str,
        payload: SetOrInsertPayload<&Value>,
    ) -> LocalOperationResult {
        let state_tree_table = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        let newvalue = MultiValue::new_from_value_2(NewValueRequest {
            actor: payload.actor,
            start_op: payload.start_op,
            parent_obj: &state_tree_table.object_id,
            key: &key.into(),
            value: payload.value,
            insert: false,
            pred: state_tree_table.pred_for_key(key),
        });
        state_tree_table
            .props
            .insert(key.to_owned(), newvalue.multivalue());
        LocalOperationResult {
            new_ops: newvalue.ops(),
        }
    }

    pub(crate) fn delete_key(&mut self, key: &str) -> LocalOperationResult {
        let state_tree_table = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Table(map)) => map,
            _ => unreachable!(),
        };
        state_tree_table.props.remove(key);
        LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: state_tree_table.object_id.clone(),
                key: key.into(),
                insert: false,
                pred: state_tree_table.pred_for_key(key),
            }],
        }
    }
}

pub struct ResolvedText<'a> {
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedText<'a> {
    #[allow(dead_code)]
    pub(crate) fn insert(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<String>,
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
                pred: Vec::new(),
            }],
        })
    }

    pub(crate) fn insert_many<I>(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<I>,
    ) -> Result<LocalOperationResult, error::MissingIndexError>
    where
        I: ExactSizeIterator<Item = String>,
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
            chars.push(amp::ScalarValue::Str(c.to_string()));
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
                pred: Vec::new(),
            }],
        })
    }

    pub(crate) fn set(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<String>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let index: usize = index.try_into().unwrap();
        let (current_elemid, _) = state_tree_text.elem_at(index)?;
        let current_elemid = current_elemid.clone();
        let update_op = amp::OpId::new(payload.start_op, payload.actor);
        let c = MultiGrapheme::new_from_grapheme_cluster(update_op, payload.value.clone());
        state_tree_text.set(index, c)?;
        Ok(LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Set(amp::ScalarValue::Str(payload.value)),
                obj: state_tree_text.object_id.clone(),
                key: current_elemid.into(),
                pred: state_tree_text.pred_for_index(index as u32),
                insert: false,
            }],
        })
    }

    pub(crate) fn remove(
        &mut self,
        index: u32,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let state_tree_text = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::Text(text)) => text,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_text.elem_at(index.try_into().unwrap())?;
        let current_elemid = current_elemid.clone();
        state_tree_text.remove(index.try_into().unwrap())?;
        Ok(LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: state_tree_text.object_id.clone(),
                key: current_elemid.into(),
                insert: false,
                pred: state_tree_text.pred_for_index(index as u32),
            }],
        })
    }

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
    pub(super) multivalue: &'a mut MultiValue,
}

impl<'a> ResolvedList<'a> {
    pub(crate) fn set(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<&Value>,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
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
            key: &current_elemid.into(),
            insert: false,
        });
        state_tree_list.set(index as usize, newvalue.multivalue())?;
        Ok(LocalOperationResult {
            new_ops: newvalue.ops(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn insert(
        &mut self,
        index: u32,
        payload: SetOrInsertPayload<&Value>,
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
            key: &current_elemid.into(),
            insert: true,
            pred: Vec::new(),
        });
        state_tree_list.insert(index as usize, newvalue.multivalue())?;
        Ok(LocalOperationResult {
            new_ops: newvalue.ops(),
        })
    }

    pub(crate) fn insert_many<'b, I>(
        &'a mut self,
        index: u32,
        payload: SetOrInsertPayload<I>,
    ) -> Result<LocalOperationResult, error::MissingIndexError>
    where
        I: ExactSizeIterator<Item = &'b Value>,
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
                value: &value,
                parent_obj: &state_tree_list.object_id,
                key: &last_elemid.clone().into(),
                insert: true,
                pred: Vec::new(),
            });
            last_elemid = amp::OpId::new(op_num, payload.actor).into();
            op_num = newvalue.max_op() + 1;
            newvalues.push(newvalue.multivalue());
            ops.extend(newvalue.ops());
        }
        state_tree_list.insert_many(index as usize, newvalues)?;
        Ok(LocalOperationResult {
            new_ops: condense_insert_ops(ops),
        })
    }

    pub(crate) fn remove(
        &mut self,
        index: u32,
    ) -> Result<LocalOperationResult, error::MissingIndexError> {
        let state_tree_list = match self.multivalue.default_statetree_value_mut() {
            StateTreeValue::Composite(StateTreeComposite::List(list)) => list,
            _ => unreachable!(),
        };
        let (current_elemid, _) = state_tree_list.elem_at(index.try_into().unwrap())?;
        let current_elemid = current_elemid.clone();
        state_tree_list.remove(index as usize)?;
        Ok(LocalOperationResult {
            new_ops: vec![amp::Op {
                action: amp::OpType::Del(NonZeroU32::new(1).unwrap()),
                obj: state_tree_list.object_id.clone(),
                key: current_elemid.into(),
                insert: false,
                pred: state_tree_list.pred_for_index(index),
            }],
        })
    }

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
    pub(super) multivalue: &'a mut MultiGrapheme,
}

pub struct ResolvedPrimitive<'a> {
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
            preds.extend(v.pred.clone());
        }
        for o in op_iter {
            if let Some(prim) = prim_from_op_action(&o.action) {
                prim_vals.push(prim);
                preds.extend(o.pred.clone());
            }
        }
        if prim_vals.len() == ops.len() {
            vec![amp::Op {
                action: amp::OpType::MultiSet(prim_vals),
                pred: preds,
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
