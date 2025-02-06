use crate::op_set2::packer::{
    BooleanCursor, ColumnDataIter, DeltaCursor, IntCursor, RawReader, SlabWeight, StrCursor,
    UIntCursor,
};
use crate::{
    op_set2,
    op_set2::{
        meta::MetaCursor,
        op::SuccCursors,
        types::{Action, ActionCursor, ActorCursor, ActorIdx, KeyRef, ScalarValue},
        OpSet,
    },
    types::{ElemId, ObjId, OpId},
};

use super::Op;

use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;

#[derive(Clone, Debug)]
pub(crate) struct OpIter<'a> {
    pub(super) pos: usize,
    pub(super) id: OpIdIter<'a>,
    pub(super) obj: ObjIdIter<'a>,
    pub(super) key: KeyIter<'a>,
    pub(super) succ: SuccIterIter<'a>,
    pub(super) insert: InsertIter<'a>,
    pub(super) action: ActionIter<'a>,
    pub(super) value: ValueIter<'a>,
    pub(super) marks: MarkInfoIter<'a>,
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
    #[error("missing value: {0}")]
    MissingValue(&'static str),
    #[error("error reading value column: {0}")]
    ReadValue(#[from] op_set2::packer::ReadRawError),
    #[error("invalid value: {0}")]
    InvalidValue(#[from] op_set2::types::ReadScalarError),
}

impl<'a> ExactSizeIterator for OpIter<'a> {
    fn len(&self) -> usize {
        self.end_pos() - self.pos()
    }
}

impl<'a> OpIter<'a> {
    pub(crate) fn range(&self) -> Range<usize> {
        self.pos()..self.end_pos()
    }

    pub(crate) fn end_pos(&self) -> usize {
        self.id.actor.end_pos()
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn try_next(&mut self) -> Result<Option<Op<'a>>, ReadOpError> {
        let Some(id) = self.id.maybe_try_next()? else {
            return Ok(None);
        };
        let key = self.key.try_next()?;
        let insert = self.insert.try_next()?;
        let action = self.action.try_next()?;
        let obj = self.obj.try_next()?;
        let value = self.value.try_next()?;
        let (mark_name, expand) = self.marks.try_next()?;
        let succ_cursors = self.succ.try_next()?;
        let pos = self.pos;
        let conflict = false;
        self.pos += 1;
        Ok(Some(Op {
            pos,
            conflict,
            id,
            key,
            insert,
            action,
            obj,
            value,
            expand,
            mark_name,
            succ_cursors,
        }))
    }

    #[inline(never)]
    pub(crate) fn try_nth(&mut self, n: usize) -> Result<Option<Op<'a>>, ReadOpError> {
        let Some(id) = self.id.maybe_try_nth(n)? else {
            return Ok(None);
        };
        let key = self.key.try_nth(n)?;
        let insert = self.insert.try_nth(n)?;
        let action = self.action.try_nth(n)?;
        let obj = self.obj.try_nth(n)?;
        let value = self.value.try_nth(n)?;
        let (mark_name, expand) = self.marks.try_nth(n)?;
        let succ_cursors = self.succ.try_nth(n)?;
        let pos = self.pos + n;
        let conflict = false;
        self.pos += n + 1;
        Ok(Some(Op {
            pos,
            conflict,
            id,
            key,
            insert,
            action,
            obj,
            value,
            expand,
            mark_name,
            succ_cursors,
        }))
    }
}

impl<'a> Iterator for OpIter<'a> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.try_next();
        if result.is_err() {
            log!("Key ERR!");
            let key_str = self.op_set.cols.key_str.export();
            let key_actor = self.op_set.cols.key_actor.export();
            let key_ctr = self.op_set.cols.key_ctr.export();
            log!(" :: key_str = {:?}", key_str.as_slice());
            log!(" :: key_actor = {:?}", key_actor.as_slice());
            log!(" :: key_ctr = {:?}", key_ctr.as_slice());
        }
        result.unwrap()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).unwrap()
    }
}

impl OpId {
    pub(crate) fn try_load<'a>(
        id_actor: Option<Cow<'a, ActorIdx>>,
        id_counter: Option<Cow<'a, i64>>,
    ) -> Result<OpId, ReadOpError> {
        match (id_actor, id_counter) {
            (Some(actor_idx), Some(counter)) => {
                if *counter < 0 {
                    Err(ReadOpError::InvalidOpId("negative counter".to_string()))
                } else {
                    Ok(OpId::new(*counter as u64, u64::from(*actor_idx) as usize))
                }
            }
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }

    pub(crate) fn load<'a>(
        id_actor: Option<Option<Cow<'a, ActorIdx>>>,
        id_counter: Option<Option<Cow<'a, i64>>>,
    ) -> Option<OpId> {
        Self::try_load(id_actor?, id_counter?).ok()
    }
}

impl ObjId {
    fn try_load(
        actor: Option<Cow<'_, ActorIdx>>,
        ctr: Option<Cow<'_, u64>>,
    ) -> Result<ObjId, ReadOpError> {
        match (actor, ctr) {
            (Some(actor_idx), Some(counter)) => {
                if *counter == 0 {
                    Ok(ObjId::root())
                } else {
                    Ok(ObjId(OpId::new(*counter, u64::from(*actor_idx) as usize)))
                }
            }
            (None, None) => Ok(ObjId::root()),
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }
}

impl ElemId {
    fn try_load(
        key_actor: Option<Cow<'_, ActorIdx>>,
        key_counter: Option<Cow<'_, i64>>,
    ) -> Result<Option<ElemId>, ReadOpError> {
        match (key_counter, key_actor) {
            (None, None) => Ok(None),
            (Some(Cow::Owned(0)), None) => Ok(Some(ElemId(OpId::new(0, 0)))),
            (Some(counter), Some(actor)) if *counter > 0 => Ok(Some(ElemId(OpId::new(
                *counter as u64,
                usize::from(*actor),
            )))),
            _ => Err(ReadOpError::InvalidKey),
        }
    }
}

impl<'a> KeyRef<'a> {
    fn try_load(
        key_str: Option<Cow<'a, str>>,
        key_actor: Option<Cow<'a, ActorIdx>>,
        key_counter: Option<Cow<'a, i64>>,
    ) -> Result<KeyRef<'a>, ReadOpError> {
        let elemid = ElemId::try_load(key_actor, key_counter)?;
        match (key_str, elemid) {
            (Some(key_str), None) => Ok(KeyRef::Map(key_str)),
            (None, Some(elemid)) => Ok(KeyRef::Seq(elemid)),
            (None, None) => Err(ReadOpError::MissingKey),
            (Some(_), Some(_)) => Err(ReadOpError::InvalidKey),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpIdIter<'a> {
    actor: ColumnDataIter<'a, ActorCursor>,
    ctr: ColumnDataIter<'a, DeltaCursor>,
}

impl<'a> OpIdIter<'a> {
    pub(crate) fn new(
        actor: ColumnDataIter<'a, ActorCursor>,
        ctr: ColumnDataIter<'a, DeltaCursor>,
    ) -> Self {
        Self { actor, ctr }
    }

    pub(crate) fn maybe_try_next(&mut self) -> Result<Option<OpId>, ReadOpError> {
        let actor = self.actor.next();
        let ctr = self.ctr.next();
        match (actor, ctr) {
            (Some(actor), Some(ctr)) => Ok(Some(OpId::try_load(actor, ctr)?)),
            (None, _) => Ok(None),
            (Some(_), None) => Err(ReadOpError::MissingValue("id_counter")),
        }
    }

    pub(crate) fn maybe_try_nth(&mut self, n: usize) -> Result<Option<OpId>, ReadOpError> {
        let actor = self.actor.nth(n);
        let ctr = self.ctr.nth(n);
        match (actor, ctr) {
            (Some(actor), Some(ctr)) => Ok(Some(OpId::try_load(actor, ctr)?)),
            (None, _) => Ok(None),
            (Some(_), None) => Err(ReadOpError::MissingValue("id_counter")),
        }
    }
}

impl<'a> Iterator for OpIdIter<'a> {
    type Item = OpId;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_try_next().ok().flatten()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.maybe_try_nth(n).ok().flatten()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InsertIter<'a> {
    iter: ColumnDataIter<'a, BooleanCursor>,
}

impl<'a> InsertIter<'a> {
    pub(crate) fn new(iter: ColumnDataIter<'a, BooleanCursor>) -> Self {
        Self { iter }
    }

    fn try_next(&mut self) -> Result<bool, ReadOpError> {
        self.iter
            .next()
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("insert"))
    }

    fn try_nth(&mut self, n: usize) -> Result<bool, ReadOpError> {
        self.iter
            .nth(n)
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("insert"))
    }
}

impl<'a> Iterator for InsertIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyIter<'a> {
    key_str: ColumnDataIter<'a, StrCursor>,
    key_actor: ColumnDataIter<'a, ActorCursor>,
    key_ctr: ColumnDataIter<'a, DeltaCursor>,
}

impl<'a> KeyIter<'a> {
    pub(crate) fn new(
        key_str: ColumnDataIter<'a, StrCursor>,
        key_actor: ColumnDataIter<'a, ActorCursor>,
        key_ctr: ColumnDataIter<'a, DeltaCursor>,
    ) -> Self {
        Self {
            key_str,
            key_actor,
            key_ctr,
        }
    }

    pub(crate) fn try_next(&mut self) -> Result<KeyRef<'a>, ReadOpError> {
        let key_str = self
            .key_str
            .next()
            .ok_or(ReadOpError::MissingValue("key_str"))?;
        let key_actor = self
            .key_actor
            .next()
            .ok_or(ReadOpError::MissingValue("key_actor"))?;
        let key_ctr = self
            .key_ctr
            .next()
            .ok_or(ReadOpError::MissingValue("key_ctr"))?;
        let result = KeyRef::try_load(key_str.clone(), key_actor.clone(), key_ctr.clone());
        if result.is_err() {
            log!(
                "Key error key={:?} actor={:?} ctr={:?}",
                key_str,
                key_actor,
                key_ctr
            );
            log!(
                "str={} actor={} ctr={}",
                self.key_str.pos(),
                self.key_actor.pos(),
                self.key_ctr.pos()
            );
        }
        result
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<KeyRef<'a>, ReadOpError> {
        let key_str = self
            .key_str
            .nth(n)
            .ok_or(ReadOpError::MissingValue("key_str"))?;
        let key_actor = self
            .key_actor
            .nth(n)
            .ok_or(ReadOpError::MissingValue("key_actor"))?;
        let key_ctr = self
            .key_ctr
            .nth(n)
            .ok_or(ReadOpError::MissingValue("key_ctr"))?;
        KeyRef::try_load(key_str, key_actor, key_ctr)
    }
}

impl<'a> Iterator for KeyIter<'a> {
    type Item = KeyRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ObjIdIter<'a> {
    actor: ColumnDataIter<'a, ActorCursor>,
    ctr: ColumnDataIter<'a, UIntCursor>,
}

impl<'a> ObjIdIter<'a> {
    pub(crate) fn new(
        actor: ColumnDataIter<'a, ActorCursor>,
        ctr: ColumnDataIter<'a, UIntCursor>,
    ) -> Self {
        Self { actor, ctr }
    }

    pub(crate) fn try_next(&mut self) -> Result<ObjId, ReadOpError> {
        let actor = self
            .actor
            .next()
            .ok_or(ReadOpError::MissingValue("obj_actor"))?;
        let ctr = self
            .ctr
            .next()
            .ok_or(ReadOpError::MissingValue("obj_ctr"))?;
        ObjId::try_load(actor, ctr)
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<ObjId, ReadOpError> {
        let actor = self
            .actor
            .nth(n)
            .ok_or(ReadOpError::MissingValue("obj_actor"))?;
        let ctr = self
            .ctr
            .nth(n)
            .ok_or(ReadOpError::MissingValue("obj_ctr"))?;
        ObjId::try_load(actor, ctr)
    }
}

impl<'a> Iterator for ObjIdIter<'a> {
    type Item = ObjId;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MarkInfoIter<'a> {
    name: ColumnDataIter<'a, StrCursor>,
    expand: ColumnDataIter<'a, BooleanCursor>,
}

impl<'a> MarkInfoIter<'a> {
    pub(crate) fn new(
        name: ColumnDataIter<'a, StrCursor>,
        expand: ColumnDataIter<'a, BooleanCursor>,
    ) -> Self {
        Self { name, expand }
    }

    pub(crate) fn try_next(&mut self) -> Result<(Option<Cow<'a, str>>, bool), ReadOpError> {
        let expand = self
            .expand
            .next()
            .ok_or(ReadOpError::MissingValue("expand"))?
            .as_deref()
            .cloned()
            .unwrap_or(false);
        let mark_name = self
            .name
            .next()
            .ok_or(ReadOpError::MissingValue("mark_name"))?;
        Ok((mark_name, expand))
    }

    pub(crate) fn try_nth(
        &mut self,
        n: usize,
    ) -> Result<(Option<Cow<'a, str>>, bool), ReadOpError> {
        let expand = self
            .expand
            .nth(n)
            .ok_or(ReadOpError::MissingValue("expand"))?
            .as_deref()
            .cloned()
            .unwrap_or(false);
        let mark_name = self
            .name
            .nth(n)
            .ok_or(ReadOpError::MissingValue("mark_name"))?;
        Ok((mark_name, expand))
    }
}

impl<'a> Iterator for MarkInfoIter<'a> {
    type Item = (Option<Cow<'a, str>>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ActionIter<'a> {
    iter: ColumnDataIter<'a, ActionCursor>,
}

impl<'a> ActionIter<'a> {
    pub(crate) fn new(iter: ColumnDataIter<'a, ActionCursor>) -> Self {
        Self { iter }
    }

    fn try_next(&mut self) -> Result<Action, ReadOpError> {
        self.iter
            .next()
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("action"))
    }

    fn try_nth(&mut self, n: usize) -> Result<Action, ReadOpError> {
        self.iter
            .nth(n)
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("action"))
    }
}

impl<'a> Iterator for ActionIter<'a> {
    type Item = Action;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValueIter<'a> {
    meta: ColumnDataIter<'a, MetaCursor>,
    raw: RawReader<'a, SlabWeight>,
}

impl<'a> ValueIter<'a> {
    pub(crate) fn new(
        meta: ColumnDataIter<'a, MetaCursor>,
        raw: RawReader<'a, SlabWeight>,
    ) -> Self {
        Self { meta, raw }
    }

    pub(crate) fn try_next(&mut self) -> Result<ScalarValue<'a>, ReadOpError> {
        let meta = self.meta.next().flatten();
        let meta = meta.ok_or(ReadOpError::MissingValue("value_meta"))?;
        let raw = self.raw.read_next(meta.length())?;
        Ok(ScalarValue::from_raw(*meta, raw)?)
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<ScalarValue<'a>, ReadOpError> {
        if n == 0 {
            self.try_next()
        } else {
            let meta = self.meta.nth(n).flatten();
            let meta = meta.ok_or(ReadOpError::MissingValue("value_meta"))?;
            let raw_len = meta.length();
            let raw_pos = self.meta.calculate_acc().as_usize();
            self.raw.seek_to(raw_pos - raw_len);
            let raw = self.raw.read_next(raw_len)?;
            let value = ScalarValue::from_raw(*meta, raw)?;
            Ok(value)
        }
    }
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = ScalarValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SuccIterIter<'a> {
    count: ColumnDataIter<'a, UIntCursor>,
    actor: ColumnDataIter<'a, ActorCursor>,
    ctr: ColumnDataIter<'a, DeltaCursor>,
    incs: ColumnDataIter<'a, IntCursor>,
}

impl<'a> SuccIterIter<'a> {
    pub(crate) fn new(
        count: ColumnDataIter<'a, UIntCursor>,
        actor: ColumnDataIter<'a, ActorCursor>,
        ctr: ColumnDataIter<'a, DeltaCursor>,
        incs: ColumnDataIter<'a, IntCursor>,
    ) -> Self {
        Self {
            count,
            actor,
            ctr,
            incs,
        }
    }

    pub(crate) fn try_next(&mut self) -> Result<SuccCursors<'a>, ReadOpError> {
        let num_succ = self.count.next().flatten();
        let num_succ = *num_succ.ok_or(ReadOpError::MissingValue("succ_count"))?;
        let result = SuccCursors {
            len: num_succ as usize,
            succ_actor: self.actor.clone(),
            succ_counter: self.ctr.clone(),
            inc_values: self.incs.clone(),
        };

        self.actor.advance_by(num_succ as usize);
        self.ctr.advance_by(num_succ as usize);
        self.incs.advance_by(num_succ as usize);

        Ok(result)
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<SuccCursors<'a>, ReadOpError> {
        if n == 0 {
            self.try_next()
        } else {
            let sub_pos1 = self.count.calculate_acc().as_u64();
            let num_succ = self.count.nth(n).flatten();
            let sub_pos2 = self.count.calculate_acc().as_u64();
            let num_succ = *num_succ.ok_or(ReadOpError::MissingValue("succ_count"))?;

            let seek = sub_pos2 - sub_pos1 - num_succ;
            self.actor.advance_by(seek as usize);
            self.ctr.advance_by(seek as usize);
            self.incs.advance_by(seek as usize);

            let result = SuccCursors {
                len: num_succ as usize,
                succ_actor: self.actor.clone(),
                succ_counter: self.ctr.clone(),
                inc_values: self.incs.clone(),
            };

            self.actor.advance_by(num_succ as usize);
            self.ctr.advance_by(num_succ as usize);
            self.incs.advance_by(num_succ as usize);

            Ok(result)
        }
    }
}

impl<'a> Iterator for SuccIterIter<'a> {
    type Item = SuccCursors<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}
