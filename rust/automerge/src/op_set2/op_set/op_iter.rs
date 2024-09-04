use crate::{
    op_set2,
    op_set2::{
        columns::{ColumnDataIter, RawReader},
        op::SuccCursors,
        rle::{ActionCursor, ActorCursor},
        BooleanCursor, DeltaCursor, IntCursor, MetaCursor, StrCursor,
    },
    types::{ElemId, ObjId, OpId},
};

use super::{Op, OpSet};

use std::fmt::Debug;

/*
use super::{
    ACTION_COL_SPEC, ALL_COLUMN_SPECS, EXPAND_COL_SPEC, ID_ACTOR_COL_SPEC, ID_COUNTER_COL_SPEC,
    INSERT_COL_SPEC, KEY_ACTOR_COL_SPEC, KEY_COUNTER_COL_SPEC, KEY_STR_COL_SPEC,
    MARK_NAME_COL_SPEC, OBJ_ID_ACTOR_COL_SPEC, OBJ_ID_COUNTER_COL_SPEC, SUCC_ACTOR_COL_SPEC,
    SUCC_COUNTER_COL_SPEC, SUCC_COUNT_COL_SPEC, VALUE_COL_SPEC, VALUE_META_COL_SPEC,
};
*/

//pub(crate) trait OpReadState {}
//#[derive(Debug, Clone, PartialEq, Default)]
//pub(crate) struct Verified;
//#[derive(Debug, Clone, PartialEq, Default)]
//pub(crate) struct Unverified;
//impl OpReadState for Verified {}
//impl OpReadState for Unverified {}

#[derive(Clone, Debug)]
pub(crate) struct OpIter<'a> {
    pub(super) pos: usize,
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
    pub(super) op_set: &'a OpSet,
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

impl<'a> OpIter<'a> {
    pub(crate) fn end_pos(&self) -> usize {
        self.id_actor.end_pos()
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

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
        let op_set = self.op_set;
        let pos = self.pos;
        let conflict = false;
        let index = 0;
        self.pos += 1;
        Ok(Some(Op {
            pos,
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
            op_set,
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

    fn read_key(&mut self) -> Result<op_set2::types::KeyRef<'a>, ReadOpError> {
        let key_str = self.key_str.next().flatten();
        let key_counter = self.key_counter.next();
        let key_actor = self.key_actor.next();
        match (key_str, key_counter, key_actor) {
            (Some(key_str), None | Some(None), None | Some(None)) => {
                Ok(op_set2::types::KeyRef::Map(key_str))
            }
            (None, Some(Some(0)) | None, Some(None) | None) => {
                // ElemId::Head is represented as a counter of 0 and a null actor
                Ok(op_set2::types::KeyRef::Seq(ElemId(OpId::new(0, 0))))
            }
            (None, Some(Some(counter)), Some(Some(actor))) if counter > 0 => {
                Ok(op_set2::types::KeyRef::Seq(ElemId(OpId::new(
                    counter as u64,
                    u64::from(actor) as usize,
                ))))
            }
            (None, Some(None), None | Some(None)) => Err(ReadOpError::MissingKey),
            other => {
                log!("InvalidKey: {:?}", other);
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

impl<'a> Iterator for OpIter<'a> {
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
