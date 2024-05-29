use crate::{
    op_set2::{
        self,
        columns::{ColumnDataIter, RawReader, RunStep, Seek},
        op::SuccCursors,
        rle::{ActionCursor, ActorCursor},
        types::ActorIdx,
        BooleanCursor, DeltaCursor, GroupCursor, IntCursor, MetaCursor, RleCursor, Run, StrCursor,
    },
    storage::ColumnSpec,
    types::{ElemId, ObjId, OpId},
};

use super::{
    ACTION_COL_SPEC, ALL_COLUMN_SPECS, EXPAND_COL_SPEC, ID_ACTOR_COL_SPEC, ID_COUNTER_COL_SPEC,
    INSERT_COL_SPEC, KEY_ACTOR_COL_SPEC, KEY_COUNTER_COL_SPEC, KEY_STR_COL_SPEC,
    MARK_NAME_COL_SPEC, OBJ_ID_ACTOR_COL_SPEC, OBJ_ID_COUNTER_COL_SPEC, SUCC_ACTOR_COL_SPEC,
    SUCC_COUNTER_COL_SPEC, SUCC_COUNT_COL_SPEC, VALUE_COL_SPEC, VALUE_META_COL_SPEC,
};

pub(crate) trait OpReadState {}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Verified;
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Unverified;
impl OpReadState for Verified {}
impl OpReadState for Unverified {}

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

impl<'a, T: OpReadState> OpIter<'a, T> {
    fn try_next(&mut self) -> Result<Option<op_set2::op::Op<'a>>, ReadOpError> {
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
        self.index += 1;
        Ok(Some(op_set2::op::Op {
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

    pub(crate) fn seek_to_obj(&mut self, obj: &ObjId) {
        let counter_skipped = self.obj_id_counter.seek_to_value(obj.0.counter() as u64);
        println!("counter_skipped: {:?}", counter_skipped);
        self.advance_all_columns_except(&[OBJ_ID_COUNTER_COL_SPEC], counter_skipped);
        let actor_skipped = self
            .obj_id_actor
            .seek_to_value(ActorIdx::from(obj.0.actor()));
        println!("actor_skipped: {:?}", actor_skipped);
        self.advance_all_columns_except(
            &[OBJ_ID_ACTOR_COL_SPEC, OBJ_ID_COUNTER_COL_SPEC],
            actor_skipped,
        );
    }

    fn advance_all_columns_except(&mut self, except_columns: &[ColumnSpec], advance_by: usize) {
        for column_spec in ALL_COLUMN_SPECS {
            if except_columns
                .iter()
                .position(|&c| c == column_spec)
                .is_some()
            {
                continue;
            }
            match column_spec {
                ID_ACTOR_COL_SPEC => {
                    self.id_actor.advance_by(advance_by);
                }
                ID_COUNTER_COL_SPEC => {
                    self.id_counter.advance_by(advance_by);
                }
                OBJ_ID_ACTOR_COL_SPEC => {
                    self.obj_id_actor.advance_by(advance_by);
                }
                OBJ_ID_COUNTER_COL_SPEC => {
                    self.obj_id_counter.advance_by(advance_by);
                }
                KEY_ACTOR_COL_SPEC => {
                    self.key_actor.advance_by(advance_by);
                }
                KEY_COUNTER_COL_SPEC => {
                    self.key_counter.advance_by(advance_by);
                }
                KEY_STR_COL_SPEC => {
                    self.key_str.advance_by(advance_by);
                }
                SUCC_COUNT_COL_SPEC => {
                    self.succ_count.advance_by(advance_by);
                }
                SUCC_ACTOR_COL_SPEC => {
                    self.succ_actor.advance_by(advance_by);
                }
                SUCC_COUNTER_COL_SPEC => {
                    self.succ_counter.advance_by(advance_by);
                }
                INSERT_COL_SPEC => {
                    self.insert.advance_by(advance_by);
                }
                ACTION_COL_SPEC => {
                    self.action.advance_by(advance_by);
                }
                MARK_NAME_COL_SPEC => {
                    self.mark_name.advance_by(advance_by);
                }
                EXPAND_COL_SPEC => {
                    self.expand.advance_by(advance_by);
                }
                _ => {}
            };
        }
        for _ in 0..advance_by {
            self.read_value();
        }
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
            //_ => Err(ReadOpError::InvalidKey),
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
    type Item = op_set2::op::Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().unwrap()
    }
}

impl<'a> Iterator for OpIter<'a, Unverified> {
    type Item = Result<op_set2::op::Op<'a>, ReadOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
