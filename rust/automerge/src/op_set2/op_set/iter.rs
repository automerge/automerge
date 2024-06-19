use crate::{
    exid::ExId,
    marks::MarkSet,
    op_set2::{
        self,
        columns::{ColumnDataIter, RawReader, RunStep, Seek},
        op::{Op, SuccCursors},
        rle::{ActionCursor, ActorCursor},
        types::{ActorIdx, Key, ScalarValue},
        BooleanCursor, DeltaCursor, IntCursor, MetaCursor, RleCursor, Run, StrCursor,
    },
    storage::ColumnSpec,
    types::{Clock, ElemId, ObjId, ObjType, OpId},
    value,
};

use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::{
    ACTION_COL_SPEC, ALL_COLUMN_SPECS, EXPAND_COL_SPEC, ID_ACTOR_COL_SPEC, ID_COUNTER_COL_SPEC,
    INSERT_COL_SPEC, KEY_ACTOR_COL_SPEC, KEY_COUNTER_COL_SPEC, KEY_STR_COL_SPEC,
    MARK_NAME_COL_SPEC, OBJ_ID_ACTOR_COL_SPEC, OBJ_ID_COUNTER_COL_SPEC, SUCC_ACTOR_COL_SPEC,
    SUCC_COUNTER_COL_SPEC, SUCC_COUNT_COL_SPEC, VALUE_COL_SPEC, VALUE_META_COL_SPEC,
};

pub(crate) trait OpReadState {}
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Verified;
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct Unverified;
impl OpReadState for Verified {}
impl OpReadState for Unverified {}

#[derive(Clone, Debug, Default)]
pub(crate) struct OpIter<'a, T: OpReadState> {
    pub(super) index: usize,
    pub(super) id_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) id_counter: ColumnDataIter<'a, DeltaCursor>,
    pub(super) obj_id_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) obj_id_counter: ColumnDataIter<'a, IntCursor>,
    pub(super) key_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) key_counter: ColumnDataIter<'a, DeltaCursor>,
    pub(super) key_str: ColumnDataIter<'a, StrCursor>,
    pub(super) succ_count: ColumnDataIter<'a, IntCursor>,
    pub(super) succ_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) succ_counter: ColumnDataIter<'a, DeltaCursor>,
    pub(super) insert: ColumnDataIter<'a, BooleanCursor>,
    pub(super) action: ColumnDataIter<'a, ActionCursor>,
    pub(super) value_meta: ColumnDataIter<'a, MetaCursor>,
    pub(super) value: RawReader<'a>,
    pub(super) mark_name: ColumnDataIter<'a, StrCursor>,
    pub(super) expand: ColumnDataIter<'a, BooleanCursor>,
    pub(super) _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Object(ObjType),
    Scalar(ScalarValue<'a>),
}

impl<'a> Value<'a> {
    pub(crate) fn into_owned(&self) -> value::Value<'static> {
        match self {
            Self::Object(o) => value::Value::Object(*o),
            Self::Scalar(s) => value::Value::Scalar(Cow::Owned(s.into_owned())),
        }
    }
}

pub struct Values<'a> {
    iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
}

impl<'a> Default for Values<'a> {
    fn default() -> Self {
        Self {
            iter: Default::default(),
        }
    }
}

impl<'a> Values<'a> {
    pub(crate) fn new(
        iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
        clock: Option<Clock>,
    ) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: &'a str,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
}

pub struct MapRange<'a, R: RangeBounds<String>> {
    iter: KeyOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
    range: Option<R>,
}

impl<'a, R: RangeBounds<String>> Default for MapRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: Default::default(),
            range: None,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new(
        iter: KeyOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
        range: R,
    ) -> Self {
        Self {
            iter,
            range: Some(range),
        }
    }
}

#[derive(Debug)]
pub struct ListRangeItem<'a> {
    pub index: usize,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

pub struct ListRange<'a, R: RangeBounds<usize>> {
    iter: KeyOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
    range: Option<R>,
}

impl<'a, R: RangeBounds<usize>> Default for ListRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: Default::default(),
            range: None,
        }
    }
}

impl<'a, R: RangeBounds<usize>> ListRange<'a, R> {
    pub(crate) fn new(
        iter: KeyOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
        range: R,
    ) -> Self {
        Self {
            iter,
            range: Some(range),
        }
    }
}

impl<'a, R: RangeBounds<usize>> Iterator for ListRange<'a, R> {
    type Item = ListRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!();
    }
}

#[derive(Default)]
pub struct Keys<'a> {
    pub(crate) iter: KeyOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        todo!();
    }
}

/*
impl<'a, R: RangeBounds<usize>> ListRange<'a,R> {
  pub(crate) fn new(iter: KeyIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>, range: R) {
    Self { iter, range }
  }
}
*/

#[derive(Debug)]
pub(crate) struct KeyIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    head: Option<Op<'a>>,
    iter: I,
}

impl<'a, I: OpScope<'a>> KeyIter<'a, I> {
    pub(crate) fn new(op: Op<'a>, iter: I) -> Self {
        KeyIter {
            head: Some(op),
            iter,
        }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for KeyIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = self.head.take()?;
        if let Some(next) = self.iter.next() {
            if next.elemid_or_key() == head.elemid_or_key() {
                self.head = Some(next);
            }
        }
        Some(head)
    }
}

#[derive(Default)]
pub(crate) struct KeyOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
    next_op: Option<Op<'a>>,
    count: usize,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for KeyOpIter<'a, I> {
    type Item = KeyIter<'a, I>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = match self.next_op.take() {
            Some(head) => head,
            None => self.iter.next()?,
        };
        let iter = self.iter.clone();
        //log!("KeyOpIter head = {:?}", head);
        let key = head.elemid_or_key();
        while let Some(next) = self.iter.next() {
            if next.elemid_or_key() != key {
                //log!("next_op = {:?}", next);
                self.next_op = Some(next);
                break;
            }
        }
        Some(KeyIter {
            head: Some(head),
            iter,
        })
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct TopOpIter<'a, I: Iterator<Item = Op<'a>> + Clone + Default> {
    iter: I,
    last_op: Option<Op<'a>>,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone + Default> Iterator for TopOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            let mut op = Some(op);
            std::mem::swap(&mut op, &mut self.last_op);
            let key1 = self.last_op.as_ref().map(|op| op.elemid_or_key());
            let key2 = op.as_ref().map(|op| op.elemid_or_key());
            //log!("key1 {:?} vs key2 {:?}", key1, key2);
            if key1 != key2 && key2.is_some() {
                //log!("DONE: {:?}", op);
                return op;
            }
            if key1 == key2 {
                if let Some(last) = &mut self.last_op {
                    last.conflict == true;
                }
            }
        }
        //log!("FINAL: {:?}", self.last_op);
        self.last_op.take()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct VisibleOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    clock: Option<Clock>,
    iter: I,
}

impl<'a, I: OpScope<'a>> Iterator for VisibleOpIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(op) = self.iter.next() {
            if op.visible_at(self.clock.as_ref()) {
                return Some(op);
            }
        }
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadOpError {
    #[error("invalid OpId: {0}")]
    InvalidOpId(String),
    #[error("invalid key")]
    InvalidKey,
    #[error("missing key")]
    MissingKey,
    #[error("missing action")]
    MissingAction,
    #[error("error reading value column: {0}")]
    ReadValue(#[from] op_set2::columns::ReadRawError),
    #[error("invalid value: {0}")]
    InvalidValue(#[from] op_set2::types::ReadScalarError),
    #[error("missing object ID")]
    MissingObjId,
    #[error("missing mark name")]
    MissingMarkName,
}

pub(crate) trait OpScope<'a>: Iterator<Item = Op<'a>> + Clone + Default {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        todo!()
    }

    fn top_ops(self) -> TopOpIter<'a, Self> {
        TopOpIter {
            iter: self,
            last_op: None,
        }
    }

    fn key_ops(self) -> KeyOpIter<'a, Self> {
        KeyOpIter {
            iter: self,
            next_op: None,
            count: 0,
        }
    }

    fn visible_ops(self, clock: Option<Clock>) -> VisibleOpIter<'a, Self> {
        VisibleOpIter { iter: self, clock }
    }
}

impl<'a> OpScope<'a> for OpIter<'a, Verified> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self
    }
}

impl<'a, I: OpScope<'a>> OpScope<'a> for TopOpIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }
}

impl<'a, I: OpScope<'a>> OpScope<'a> for VisibleOpIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a, Verified> {
        self.iter.get_opiter()
    }
}

impl<'a> OpIter<'a, Verified> {
    fn try_next(&mut self) -> Result<Option<Op<'a>>, ReadOpError> {
        let Some(id) = self.read_opid()? else {
            return Ok(None);
        };
        let key = self.read_key()?;
        let insert = self.read_insert()?;
        let action = self.read_action()?;
        let obj = self.read_obj()?;
        let value = self.read_value()?;
        let expand = self.read_expand()?;
        let mark_name = self.read_mark_name()?;
        let successors = self.read_successors()?;
        let index = self.index;
        let conflict = false;
        self.index += 1;
        Ok(Some(Op {
            index,
            conflict,
            id,
            key,
            insert,
            action,
            obj,
            value,
            expand,
            mark_name,
            succ_cursors: successors,
        }))
    }

    fn read_opid(&mut self) -> Result<Option<OpId>, ReadOpError> {
        let id_actor = self.id_actor.next();
        let id_counter = self.id_counter.next();
        match (id_actor, id_counter) {
            (Some(Some(actor_idx)), Some(Some(counter))) => {
                if counter < 0 {
                    Err(ReadOpError::InvalidOpId("negative counter".to_string()))
                } else {
                    Ok(Some(OpId::new(
                        counter as u64,
                        u64::from(actor_idx) as usize,
                    )))
                }
            }
            (None, None) => Ok(None),
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }

    fn read_key(&mut self) -> Result<op_set2::types::Key<'a>, ReadOpError> {
        let key_str = self.key_str.next().flatten();
        let key_counter = self.key_counter.next();
        let key_actor = self.key_actor.next();
        match (key_str, key_counter, key_actor) {
            (Some(key_str), None | Some(None), None | Some(None)) => {
                Ok(op_set2::types::Key::Map(key_str))
            }
            (None, Some(Some(0)) | None, Some(None) | None) => {
                // ElemId::Head is represented as a counter of 0 and a null actor
                Ok(op_set2::types::Key::Seq(ElemId(OpId::new(0, 0))))
            }
            (None, Some(Some(counter)), Some(Some(actor))) if counter > 0 => {
                Ok(op_set2::types::Key::Seq(ElemId(OpId::new(
                    counter as u64,
                    u64::from(actor) as usize,
                ))))
            }
            (None, Some(None), None | Some(None)) => Err(ReadOpError::MissingKey),
            other => {
                println!("InvalidKey: {:?}", other);
                Err(ReadOpError::InvalidKey)
            }
        }
    }

    fn read_insert(&mut self) -> Result<bool, ReadOpError> {
        match self.insert.next() {
            Some(Some(b)) => Ok(b),
            Some(None) => Ok(false),
            None => Ok(false),
        }
    }

    fn read_action(&mut self) -> Result<op_set2::types::Action, ReadOpError> {
        match self.action.next() {
            Some(Some(a)) => Ok(a),
            _ => Err(ReadOpError::MissingAction),
        }
    }

    fn read_value(&mut self) -> Result<op_set2::types::ScalarValue<'a>, ReadOpError> {
        let Some(Some(meta)) = self.value_meta.next() else {
            return Ok(op_set2::types::ScalarValue::Null);
        };
        let raw_data = if meta.length() == 0 {
            &[]
        } else {
            self.value.read_next(meta.length())?
        };
        Ok(op_set2::types::ScalarValue::from_raw(meta, raw_data)?)
    }

    fn read_obj(&mut self) -> Result<ObjId, ReadOpError> {
        let obj_id_actor = self.obj_id_actor.next();
        let obj_id_counter = self.obj_id_counter.next();
        match (obj_id_actor, obj_id_counter) {
            (Some(Some(actor_idx)), Some(Some(counter))) => {
                if counter == 0 {
                    Ok(ObjId::root())
                } else {
                    Ok(OpId::new(counter as u64, u64::from(actor_idx) as usize).into())
                }
            }
            (Some(None), Some(None)) => Ok(ObjId::root()),
            // This case occurs when the only object ID in the column is the root object ID,
            // which results in a run of all null values. In this case we entirely omit the
            // column
            (None, None) => Ok(ObjId::root()),
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }

    fn read_expand(&mut self) -> Result<bool, ReadOpError> {
        Ok(self.expand.next().flatten().unwrap_or(false))
    }

    fn read_mark_name(&mut self) -> Result<Option<&'a str>, ReadOpError> {
        Ok(self.mark_name.next().flatten())
    }

    fn read_successors(&mut self) -> Result<SuccCursors<'a>, ReadOpError> {
        let num_succ = self.succ_count.next().flatten().unwrap_or(0);
        let result = SuccCursors {
            len: num_succ as usize,
            succ_actor: self.succ_actor.clone(),
            succ_counter: self.succ_counter.clone(),
        };
        for _ in 0..num_succ {
            self.succ_actor.next();
            self.succ_counter.next();
        }
        Ok(result)
    }
}

impl<'a> Iterator for OpIter<'a, Verified> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().unwrap()
    }
}

/*
impl<'a> Iterator for OpIter<'a, Unverified> {
    type Item = Result<Op<'a>, ReadOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    //#[test]
    //fn foo_bar() {}
}
