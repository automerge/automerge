use super::Op;
use crate::iter::tools::Shiftable;
use crate::{
    error::AutomergeError,
    op_set2,
    op_set2::{
        op::SuccCursors,
        types::{Action, ActorIdx, KeyRef, ScalarValue},
        OpSet,
    },
    types::{ElemId, ObjId, OpId},
};

use std::borrow::Cow;
use std::fmt::Debug;
use std::iter::Peekable;
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
    pub(super) range: Range<usize>,
    pub(super) op_set: &'a OpSet,
}

pub(crate) struct OpIterState {
    pos: usize,
    id: OpIdIterState,
    obj: ObjIdIterState,
    key: KeyIterState,
    succ: SuccIterIterState,
    insert: InsertIterState,
    action: ActionIterState,
    value: ValueIterState,
    marks: MarkInfoIterState,
    range: Range<usize>,
}

impl OpIterState {
    #[allow(unused)]
    pub(crate) fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<OpIter<'a>, AutomergeError> {
        Ok(OpIter {
            pos: self.pos,
            id: self.id.try_resume(op_set)?,
            obj: self.obj.try_resume(op_set)?,
            key: self.key.try_resume(op_set)?,
            succ: self.succ.try_resume(op_set)?,
            insert: self.insert.try_resume(op_set)?,
            action: self.action.try_resume(op_set)?,
            value: self.value.try_resume(op_set)?,
            marks: self.marks.try_resume(op_set)?,
            range: self.range.clone(),
            op_set,
        })
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
    #[error("missing value: {0}")]
    MissingValue(&'static str),
    #[error("error reading value column: {0}")]
    ReadValue(#[from] hexane::ReadRawError),
    #[error("invalid value: {0}")]
    InvalidValue(#[from] op_set2::types::ReadScalarError),
}

impl ExactSizeIterator for OpIter<'_> {
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

    #[allow(unused)]
    pub(crate) fn suspend(&self) -> OpIterState {
        OpIterState {
            pos: self.pos,
            id: self.id.suspend(),
            obj: self.obj.suspend(),
            key: self.key.suspend(),
            succ: self.succ.suspend(),
            insert: self.insert.suspend(),
            action: self.action.suspend(),
            value: self.value.suspend(),
            marks: self.marks.suspend(),
            range: self.range.clone(),
        }
    }
}

impl<'a> Iterator for OpIter<'a> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.try_next();
        if result.is_err() {
            log!("Key ERR!");
            let key_str = self.op_set.cols.key_str.save();
            let key_actor = self.op_set.cols.key_actor.save();
            let key_ctr = self.op_set.cols.key_ctr.save();
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
    pub(crate) fn try_load(
        actor: Option<ActorIdx>,
        ctr: Option<i64>,
    ) -> Result<OpId, ReadOpError> {
        match (actor, ctr) {
            (Some(actor_idx), Some(counter)) if counter >= 0 => {
                Ok(OpId::new(counter as u64, u64::from(actor_idx) as usize))
            }
            (Some(_), Some(_)) => Err(ReadOpError::InvalidOpId("negative counter".to_string())),
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }
}

impl ObjId {
    pub(crate) fn try_load(
        actor: Option<ActorIdx>,
        ctr: Option<i64>,
    ) -> Result<ObjId, ReadOpError> {
        match (actor, ctr) {
            (Some(actor_idx), Some(counter)) if counter >= 0 => {
                if counter == 0 {
                    Ok(ObjId::root())
                } else {
                    Ok(ObjId(OpId::new(
                        counter as u64,
                        u64::from(actor_idx) as usize,
                    )))
                }
            }
            (Some(_), Some(_)) => Err(ReadOpError::InvalidOpId("negative counter".to_string())),
            (None, None) => Ok(ObjId::root()),
            _ => Err(ReadOpError::InvalidOpId(
                "missing actor or counter".to_string(),
            )),
        }
    }
}

impl ElemId {
    fn try_load(
        key_actor: Option<ActorIdx>,
        key_counter: Option<i64>,
    ) -> Result<Option<ElemId>, ReadOpError> {
        match (key_counter, key_actor) {
            (None, None) => Ok(None),
            (Some(0), None) => Ok(Some(ElemId(OpId::new(0, 0)))),
            (Some(counter), Some(actor)) if counter > 0 => Ok(Some(ElemId(OpId::new(
                counter as u64,
                usize::from(actor),
            )))),
            _ => Err(ReadOpError::InvalidKey),
        }
    }
}

impl<'a> KeyRef<'a> {
    pub(crate) fn try_load(
        key_str: Option<&'a str>,
        key_actor: Option<ActorIdx>,
        key_counter: Option<i64>,
    ) -> Result<KeyRef<'a>, ReadOpError> {
        let elemid = ElemId::try_load(key_actor, key_counter)?;
        match (key_str, elemid) {
            (Some(key_str), None) => Ok(KeyRef::Map(Cow::Borrowed(key_str))),
            (None, Some(elemid)) => Ok(KeyRef::Seq(elemid)),
            (None, None) => Err(ReadOpError::MissingKey),
            (Some(_), Some(_)) => Err(ReadOpError::InvalidKey),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct OpIdIter<'a> {
    pub(super) actor: hexane::v1::Iter<'a, ActorIdx>,
    ctr: hexane::v1::DeltaIter<'a, u32>,
}

impl OpIdIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<OpIdIter<'a>, AutomergeError> {
        Ok(OpIdIter {
            actor: self.actor_state.try_resume(&op_set.cols.id_actor)?,
            ctr: self.ctr_state.try_resume(&op_set.cols.id_ctr)?,
        })
    }
}

pub(crate) struct OpIdIterState {
    actor_state: hexane::v1::column::IterState,
    ctr_state: hexane::v1::DeltaIterState,
}

pub(crate) struct CtrWalker<'a> {
    ctr: hexane::v1::FindByRange<'a>,
}

impl<'a> CtrWalker<'a> {
    pub(crate) fn new(col: &'a hexane::v1::DeltaColumn<u32>, range: Range<usize>) -> Self {
        let ctr = col.find_by_range(range.start as u32..range.end as u32);
        Self { ctr }
    }
}

impl Iterator for CtrWalker<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        self.ctr.next()
    }
    fn nth(&mut self, n: usize) -> Option<usize> {
        self.ctr.nth(n)
    }
}

pub(crate) struct SuccWalker<'a> {
    acc: usize,
    count: hexane::v1::PrefixIter<'a, u32>,
    ctr: Peekable<CtrWalker<'a>>,
}

impl<'a> SuccWalker<'a> {
    pub(crate) fn new(op_set: &'a OpSet, range: Range<usize>) -> Self {
        let ctr = CtrWalker::new(&op_set.cols.succ_ctr, range).peekable();
        let count = op_set.cols.succ_count.iter();
        Self { acc: 0, count, ctr }
    }
}

impl Iterator for SuccWalker<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        while self.ctr.peek()? < &self.acc {
            self.ctr.next();
        }
        let delta = *self.ctr.peek()? - self.acc;
        let c = self.count.advance_prefix(delta as u64)?;
        self.acc = c.total as usize;
        Some(c.pos)
    }
}

impl<'a> OpIdIter<'a> {
    pub(crate) fn new(
        actor: hexane::v1::Iter<'a, ActorIdx>,
        ctr: hexane::v1::DeltaIter<'a, u32>,
    ) -> Self {
        Self { actor, ctr }
    }

    pub(crate) fn pos(&self) -> usize {
        self.actor.pos()
    }

    pub(crate) fn maybe_try_next(&mut self) -> Result<Option<OpId>, ReadOpError> {
        let actor = self.actor.next();
        let ctr = self.ctr.next();
        match (actor, ctr) {
            (Some(actor_idx), Some(counter)) => Ok(Some(OpId::new(
                counter as u64,
                u64::from(actor_idx) as usize,
            ))),
            (None, _) => Ok(None),
            (Some(_), None) => Err(ReadOpError::MissingValue("id_counter")),
        }
    }

    pub(crate) fn maybe_try_nth(&mut self, n: usize) -> Result<Option<OpId>, ReadOpError> {
        let actor = self.actor.nth(n);
        let ctr = self.ctr.nth(n);
        match (actor, ctr) {
            (Some(actor_idx), Some(counter)) => Ok(Some(OpId::new(
                counter as u64,
                u64::from(actor_idx) as usize,
            ))),
            (None, _) => Ok(None),
            (Some(_), None) => Err(ReadOpError::MissingValue("id_counter")),
        }
    }

    pub(crate) fn set_max(&mut self, pos: usize) {
        self.actor.set_max(pos);
        self.ctr.set_max(pos);
    }

    fn suspend(&self) -> OpIdIterState {
        OpIdIterState {
            actor_state: self.actor.suspend(),
            ctr_state: self.ctr.suspend(),
        }
    }
}

impl Shiftable for OpIdIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<OpId> {
        let actor_idx = self.actor.shift_next(range.clone())?;
        let counter = self.ctr.shift_next(range)?;
        Some(OpId::new(counter as u64, u64::from(actor_idx) as usize))
    }
}

impl Iterator for OpIdIter<'_> {
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
    iter: hexane::v1::Iter<'a, bool>,
}

pub(crate) struct InsertIterState(hexane::v1::column::IterState);

impl InsertIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<InsertIter<'a>, AutomergeError> {
        Ok(InsertIter {
            iter: self.0.try_resume(op_set.cols.insert.values())?,
        })
    }
}

impl<'a> InsertIter<'a> {
    pub(crate) fn new(iter: hexane::v1::Iter<'a, bool>) -> Self {
        Self { iter }
    }

    fn try_next(&mut self) -> Result<bool, ReadOpError> {
        self.iter.next().ok_or(ReadOpError::MissingValue("insert"))
    }

    fn try_nth(&mut self, n: usize) -> Result<bool, ReadOpError> {
        self.iter.nth(n).ok_or(ReadOpError::MissingValue("insert"))
    }

    fn suspend(&self) -> InsertIterState {
        InsertIterState(self.iter.suspend())
    }
}

impl Iterator for InsertIter<'_> {
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
    key_str: hexane::v1::Iter<'a, Option<String>>,
    key_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
    key_ctr: hexane::v1::DeltaIter<'a, Option<u32>>,
}

pub(crate) struct KeyIterState {
    key_str: hexane::v1::IterState,
    key_actor: hexane::v1::column::IterState,
    key_ctr: hexane::v1::DeltaIterState,
}

impl KeyIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<KeyIter<'a>, AutomergeError> {
        Ok(KeyIter {
            key_str: self.key_str.try_resume(&op_set.cols.key_str)?,
            key_actor: self.key_actor.try_resume(&op_set.cols.key_actor)?,
            key_ctr: self.key_ctr.try_resume(&op_set.cols.key_ctr)?,
        })
    }
}

impl<'a> KeyIter<'a> {
    pub(crate) fn new(
        key_str: hexane::v1::Iter<'a, Option<String>>,
        key_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
        key_ctr: hexane::v1::DeltaIter<'a, Option<u32>>,
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
        KeyRef::try_load(key_str, key_actor, key_ctr.map(i64::from))
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
        KeyRef::try_load(key_str, key_actor, key_ctr.map(i64::from))
    }

    fn suspend(&self) -> KeyIterState {
        KeyIterState {
            key_str: self.key_str.suspend(),
            key_actor: self.key_actor.suspend(),
            key_ctr: self.key_ctr.suspend(),
        }
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

#[derive(Clone, Default, Debug)]
pub(crate) struct ObjIdIter<'a> {
    obj_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
    obj_ctr: hexane::v1::Iter<'a, Option<u32>>,
}

pub(crate) struct ObjIdIterState {
    obj_actor: hexane::v1::column::IterState,
    obj_ctr: hexane::v1::column::IterState,
}

impl ObjIdIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<ObjIdIter<'a>, AutomergeError> {
        Ok(ObjIdIter {
            obj_actor: self.obj_actor.try_resume(&op_set.cols.obj_actor)?,
            obj_ctr: self.obj_ctr.try_resume(&op_set.cols.obj_ctr)?,
        })
    }
}

impl<'a> ObjIdIter<'a> {
    pub(crate) fn new(
        obj_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
        obj_ctr: hexane::v1::Iter<'a, Option<u32>>,
    ) -> Self {
        Self { obj_actor, obj_ctr }
    }

    pub(crate) fn new_range(
        obj_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
        obj_ctr: hexane::v1::Iter<'a, Option<u32>>,
    ) -> Self {
        Self { obj_actor, obj_ctr }
    }

    #[cfg(test)]
    pub(crate) fn pos(&self) -> usize {
        self.obj_actor.pos()
    }

    pub(crate) fn seek_to_value(&mut self, obj: ObjId) -> Range<usize> {
        let cr = self
            .obj_ctr
            .seek_to_value(obj.counter().map(|c| c as u32), ..);
        let range = self.obj_actor.seek_to_value(obj.actor(), cr);
        self.obj_ctr.advance_to(range.start);
        range
    }

    pub(crate) fn try_next(&mut self) -> Result<ObjId, ReadOpError> {
        let actor = self
            .obj_actor
            .next()
            .ok_or(ReadOpError::MissingValue("obj_actor"))?;
        let ctr = self
            .obj_ctr
            .next()
            .ok_or(ReadOpError::MissingValue("obj_ctr"))?;
        ObjId::try_load(actor, ctr.map(i64::from))
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<ObjId, ReadOpError> {
        let actor = self
            .obj_actor
            .nth(n)
            .ok_or(ReadOpError::MissingValue("obj_actor"))?;
        let ctr = self
            .obj_ctr
            .nth(n)
            .ok_or(ReadOpError::MissingValue("obj_ctr"))?;
        ObjId::try_load(actor, ctr.map(i64::from))
    }

    fn suspend(&self) -> ObjIdIterState {
        ObjIdIterState {
            obj_actor: self.obj_actor.suspend(),
            obj_ctr: self.obj_ctr.suspend(),
        }
    }
}

impl Iterator for ObjIdIter<'_> {
    type Item = ObjId;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MarkInfoIter<'a> {
    name: hexane::v1::Iter<'a, Option<String>>,
    expand: hexane::v1::Iter<'a, bool>,
}

pub(crate) struct MarkInfoIterState {
    name: hexane::v1::IterState,
    expand: hexane::v1::column::IterState,
}

impl MarkInfoIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<MarkInfoIter<'a>, AutomergeError> {
        Ok(MarkInfoIter {
            name: self.name.try_resume(&op_set.cols.mark_name)?,
            expand: self.expand.try_resume(&op_set.cols.expand)?,
        })
    }
}

impl Shiftable for MarkInfoIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let mark_name = self.name.shift_next(range.clone());
        let expand = self.expand.shift_next(range).unwrap_or(false);
        Some((mark_name?, expand))
    }
}

impl<'a> MarkInfoIter<'a> {
    pub(crate) fn new(
        name: hexane::v1::Iter<'a, Option<String>>,
        expand: hexane::v1::Iter<'a, bool>,
    ) -> Self {
        Self { name, expand }
    }

    pub(crate) fn set_max(&mut self, pos: usize) {
        self.name.set_max(pos);
        self.expand.set_max(pos);
    }

    pub(crate) fn pos(&self) -> usize {
        self.expand.pos()
    }

    pub(crate) fn try_next(&mut self) -> Result<(Option<&'a str>, bool), ReadOpError> {
        let expand = self.expand.next().unwrap_or(false);
        let mark_name = self
            .name
            .next()
            .ok_or(ReadOpError::MissingValue("mark_name"))?;
        Ok((mark_name, expand))
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<(Option<&'a str>, bool), ReadOpError> {
        let expand = self.expand.nth(n).unwrap_or(false);
        let mark_name = self
            .name
            .nth(n)
            .ok_or(ReadOpError::MissingValue("mark_name"))?;
        Ok((mark_name, expand))
    }

    fn suspend(&self) -> MarkInfoIterState {
        MarkInfoIterState {
            name: self.name.suspend(),
            expand: self.expand.suspend(),
        }
    }
}

impl<'a> Iterator for MarkInfoIter<'a> {
    type Item = (Option<&'a str>, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ActionValueIter<'a> {
    action: ActionIter<'a>,
    value: ValueIter<'a>,
}

impl<'a> ActionValueIter<'a> {
    pub(crate) fn new(action: ActionIter<'a>, value: ValueIter<'a>) -> Self {
        Self { action, value }
    }
}

impl<'a> Iterator for ActionValueIter<'a> {
    type Item = (Action, ScalarValue<'a>, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let action = self.action.next();
        let value = self.value.next();
        let pos = self.action.iter.pos();
        Some((action?, value?, pos - 1))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let action = self.action.nth(n);
        let value = self.value.nth(n);
        let pos = self.action.iter.pos();
        Some((action?, value?, pos - 1))
    }
}

impl Shiftable for ActionValueIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let action = self.action.shift_next(range.clone());
        let value = self.value.shift_next(range);
        let pos = self.action.iter.pos();
        Some((action?, value?, pos - 1))
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct ActionIter<'a> {
    iter: hexane::v1::Iter<'a, Action>,
}

pub(crate) struct ActionIterState {
    state: hexane::v1::column::IterState,
}

impl ActionIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<ActionIter<'a>, AutomergeError> {
        Ok(ActionIter {
            iter: self.state.try_resume(&op_set.cols.action)?,
        })
    }
}

impl Shiftable for ActionIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.iter.shift_next(range)
    }
}

impl<'a> ActionIter<'a> {
    pub(crate) fn new(iter: hexane::v1::Iter<'a, Action>) -> Self {
        Self { iter }
    }

    fn try_next(&mut self) -> Result<Action, ReadOpError> {
        self.iter.next().ok_or(ReadOpError::MissingValue("action"))
    }

    fn try_nth(&mut self, n: usize) -> Result<Action, ReadOpError> {
        self.iter.nth(n).ok_or(ReadOpError::MissingValue("action"))
    }

    fn suspend(&self) -> ActionIterState {
        ActionIterState {
            state: self.iter.suspend(),
        }
    }
}

impl Iterator for ActionIter<'_> {
    type Item = Action;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.try_nth(n).ok()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ValueIter<'a> {
    meta: hexane::v1::PrefixIter<'a, op_set2::meta::ValueMeta>,
    raw: hexane::v1::RawColumnIter<'a>,
}

pub(crate) struct ValueIterState {
    meta: hexane::v1::PrefixIterState<op_set2::meta::ValueMeta>,
    raw: usize,
}

impl ValueIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<ValueIter<'a>, AutomergeError> {
        Ok(ValueIter {
            meta: self.meta.try_resume(&op_set.cols.value_meta)?,
            raw: op_set.cols.value.iter_at(self.raw),
        })
    }
}

impl Shiftable for ValueIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let (prefix, meta) = self.meta.shift_next(range)?;
        let length = meta.length();
        let value_advance = prefix as usize - length;
        self.raw.seek_to(value_advance);
        let raw = self.raw.take(length);
        ScalarValue::from_raw(meta, raw).ok()
    }
}

impl<'a> ValueIter<'a> {
    pub(crate) fn new(
        meta: hexane::v1::PrefixIter<'a, op_set2::meta::ValueMeta>,
        raw: hexane::v1::RawColumnIter<'a>,
    ) -> Self {
        Self { meta, raw }
    }

    pub(crate) fn try_next(&mut self) -> Result<ScalarValue<'a>, ReadOpError> {
        let (_prefix, meta) = self
            .meta
            .next()
            .ok_or(ReadOpError::MissingValue("value_meta"))?;
        let raw = self.raw.take(meta.length());
        Ok(ScalarValue::from_raw(meta, raw)?)
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<ScalarValue<'a>, ReadOpError> {
        if n == 0 {
            self.try_next()
        } else {
            let (prefix, meta) = self
                .meta
                .nth(n)
                .ok_or(ReadOpError::MissingValue("value_meta"))?;
            let raw_len = meta.length();
            let raw_pos = prefix as usize;
            self.raw.seek_to(raw_pos - raw_len);
            let raw = self.raw.take(raw_len);
            Ok(ScalarValue::from_raw(meta, raw)?)
        }
    }

    fn suspend(&self) -> ValueIterState {
        ValueIterState {
            meta: self.meta.suspend(),
            raw: self.raw.pos(),
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
    count: hexane::v1::PrefixIter<'a, u32>,
    actor: hexane::v1::Iter<'a, ActorIdx>,
    ctr: hexane::v1::DeltaIter<'a, u32>,
    incs: hexane::v1::Iter<'a, Option<i64>>,
}

pub(crate) struct SuccIterIterState {
    count: hexane::v1::PrefixIterState<u32>,
    actor: hexane::v1::column::IterState,
    ctr: hexane::v1::DeltaIterState,
    incs: hexane::v1::column::IterState,
}

impl SuccIterIterState {
    fn try_resume<'a>(&self, op_set: &'a OpSet) -> Result<SuccIterIter<'a>, AutomergeError> {
        Ok(SuccIterIter {
            count: self.count.try_resume(&op_set.cols.succ_count)?,
            actor: self.actor.try_resume(&op_set.cols.succ_actor)?,
            ctr: self.ctr.try_resume(&op_set.cols.succ_ctr)?,
            incs: self.incs.try_resume(&op_set.cols.index.inc)?,
        })
    }
}

impl<'a> SuccIterIter<'a> {
    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let (prefix, num_succ) = self.count.shift_next(range)?;
        let num_succ = num_succ as usize;
        let sub_pos = prefix as usize;

        self.actor.advance_to(sub_pos - num_succ);
        self.ctr.advance_to(sub_pos - num_succ);
        self.incs.advance_to(sub_pos - num_succ);

        let iter = SuccCursors {
            len: num_succ,
            succ_actor: self.actor.clone(),
            succ_counter: self.ctr.clone(),
            inc_values: self.incs.clone(),
        };

        self.actor.advance_by(num_succ);
        self.ctr.advance_by(num_succ);
        self.incs.advance_by(num_succ);

        Some(iter)
    }

    pub(crate) fn new(
        count: hexane::v1::PrefixIter<'a, u32>,
        actor: hexane::v1::Iter<'a, ActorIdx>,
        ctr: hexane::v1::DeltaIter<'a, u32>,
        incs: hexane::v1::Iter<'a, Option<i64>>,
    ) -> Self {
        Self {
            count,
            actor,
            ctr,
            incs,
        }
    }

    #[inline(never)]
    pub(crate) fn try_next(&mut self) -> Result<SuccCursors<'a>, ReadOpError> {
        let (_prefix, num_succ) = self
            .count
            .next()
            .ok_or(ReadOpError::MissingValue("succ_count"))?;
        let num_succ = num_succ as usize;

        let result = SuccCursors {
            len: num_succ,
            succ_actor: self.actor.clone(),
            succ_counter: self.ctr.clone(),
            inc_values: self.incs.clone(),
        };

        self.actor.advance_by(num_succ);
        self.ctr.advance_by(num_succ);
        self.incs.advance_by(num_succ);

        Ok(result)
    }

    pub(crate) fn try_nth(&mut self, n: usize) -> Result<SuccCursors<'a>, ReadOpError> {
        if n == 0 {
            self.try_next()
        } else {
            let (prefix, num_succ) = self
                .count
                .nth(n)
                .ok_or(ReadOpError::MissingValue("succ_count"))?;
            let num_succ = num_succ as usize;
            let sub_pos = prefix as usize;
            let items_before = sub_pos - num_succ;
            let actor_pos = self.actor.pos();
            let seek = items_before - actor_pos;

            self.actor.advance_by(seek);
            self.ctr.advance_by(seek);
            self.incs.advance_by(seek);

            let result = SuccCursors {
                len: num_succ,
                succ_actor: self.actor.clone(),
                succ_counter: self.ctr.clone(),
                inc_values: self.incs.clone(),
            };

            self.actor.advance_by(num_succ);
            self.ctr.advance_by(num_succ);
            self.incs.advance_by(num_succ);

            Ok(result)
        }
    }

    fn suspend(&self) -> SuccIterIterState {
        SuccIterIterState {
            count: self.count.suspend(),
            actor: self.actor.suspend(),
            ctr: self.ctr.suspend(),
            incs: self.incs.suspend(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iter::tools::{SkipIter, SkipWrap};
    use crate::transaction::Transactable;
    use crate::types::{ActorId, OpId};
    use crate::{Automerge, ROOT};

    #[test]
    fn skip_op_ids() {
        let actor1 = ActorId::try_from("aaaaaaaa").unwrap();
        let actor2 = ActorId::try_from("bbbbbbbb").unwrap();
        let actor3 = ActorId::try_from("cccccccc").unwrap();
        let actor4 = ActorId::try_from("dddddddd").unwrap();

        let mut doc = Automerge::new().with_actor(actor1);
        let mut tx = doc.transaction();
        tx.put(&ROOT, "key1", "val1").unwrap();
        tx.put(&ROOT, "key2", "val2").unwrap();
        tx.put(&ROOT, "key3", "val3").unwrap();
        tx.put(&ROOT, "key4", "val4").unwrap();
        tx.put(&ROOT, "key5", "val5").unwrap();
        tx.put(&ROOT, "key6", "val6").unwrap();
        tx.put(&ROOT, "key7", "val7").unwrap();
        tx.put(&ROOT, "key8", "val8").unwrap();
        tx.put(&ROOT, "key9", "val9").unwrap();
        tx.commit();

        let mut doc2 = doc.fork().with_actor(actor2);
        let mut tx = doc2.transaction();
        tx.put(&ROOT, "key5", "val10B").unwrap(); // 10
        tx.commit();

        let mut doc3 = doc.fork().with_actor(actor3);
        let mut tx = doc3.transaction();
        tx.delete(&ROOT, "key5").unwrap(); // 10
        tx.commit();

        let mut doc4 = doc.fork().with_actor(actor4);
        let mut tx = doc4.transaction();
        tx.put(&ROOT, "key5", "val10D").unwrap(); // 10
        tx.commit();

        let mut tx = doc.transaction();
        tx.delete(&ROOT, "key5").unwrap(); // 10
        tx.delete(&ROOT, "key7").unwrap(); // 11
        tx.put(&ROOT, "key3", "val12").unwrap(); // 12
        tx.put(&ROOT, "key3", "val13").unwrap(); // 13
        tx.commit();

        doc.merge(&mut doc2).unwrap();
        doc.merge(&mut doc3).unwrap();
        doc.merge(&mut doc4).unwrap();

        doc.dump();

        let id_ctr = &doc.ops.cols.id_ctr;
        let ids = doc.ops.iter().map(|op| op.id);

        let i1 = SkipIter::new(ids.clone(), SkipWrap::new(0, CtrWalker::new(id_ctr, 3..5)));
        assert!(i1.eq([OpId::new(3, 0), OpId::new(4, 0)]));

        let i2 = SkipIter::new(ids.clone(), SkipWrap::new(0, CtrWalker::new(id_ctr, 0..4)));
        assert!(i2.eq([OpId::new(1, 0), OpId::new(2, 0), OpId::new(3, 0)]));

        let i3 = SkipIter::new(ids.clone(), SkipWrap::new(0, CtrWalker::new(id_ctr, 9..20)));
        assert!(i3.eq([
            OpId::new(12, 0),
            OpId::new(13, 0),
            OpId::new(10, 1),
            OpId::new(10, 3),
            OpId::new(9, 0)
        ]));

        let s1 = SkipIter::new(
            ids.clone(),
            SkipWrap::new(0, SuccWalker::new(doc.ops(), 10..12)),
        );
        assert!(s1.eq([OpId::new(5, 0), OpId::new(7, 0)]));

        let s2 = SkipIter::new(
            ids.clone(),
            SkipWrap::new(0, SuccWalker::new(doc.ops(), 10..99)),
        );
        assert!(s2.eq([
            OpId::new(3, 0),
            OpId::new(12, 0),
            OpId::new(5, 0),
            OpId::new(7, 0)
        ]));

        let s3 = SkipIter::new(
            ids.clone(),
            SkipWrap::new(0, SuccWalker::new(doc.ops(), 10..13)),
        );
        assert!(s3.eq([OpId::new(3, 0), OpId::new(5, 0), OpId::new(7, 0)]));

        let s4 = SkipIter::new(
            ids.clone(),
            SkipWrap::new(0, SuccWalker::new(doc.ops(), 0..99)),
        );
        assert!(s4.eq([
            OpId::new(3, 0),
            OpId::new(12, 0),
            OpId::new(5, 0),
            OpId::new(7, 0)
        ]));

        let u1 = doc.ops().iter_ctr_range(9..99).map(|op| op.id);

        assert!(u1.eq([
            OpId::new(3, 0),
            OpId::new(12, 0),
            OpId::new(13, 0),
            OpId::new(5, 0),
            OpId::new(10, 1),
            OpId::new(10, 3),
            OpId::new(7, 0),
            OpId::new(9, 0)
        ]));
    }

    #[test]
    fn obj_id_iter_seek() {
        let r = ObjId::root();
        let o11 = ObjId(OpId::new(1, 1));
        let o12 = ObjId(OpId::new(1, 2));
        let o21 = ObjId(OpId::new(2, 1));
        let o22 = ObjId(OpId::new(2, 2));
        let o31 = ObjId(OpId::new(3, 1));
        let o32 = ObjId(OpId::new(3, 2));
        let objs = [r, r, r, r, o11, o11, o12, o21, o21, o21, o22, o22, o32, o32];
        let v1_actor = hexane::v1::Column::<Option<ActorIdx>>::from_values(
            objs.iter().map(|o| o.actor()).collect::<Vec<_>>(),
        );
        let v1_ctr = hexane::v1::Column::<Option<u32>>::from_values(
            objs.iter()
                .map(|o| o.counter().map(|c| c as u32))
                .collect::<Vec<_>>(),
        );

        // seek each element and read it
        let mut iter = ObjIdIter::new(v1_actor.iter(), v1_ctr.iter());
        let range = iter.seek_to_value(r);
        assert_eq!(range, 0..4);
        assert_eq!(iter.pos(), 0);
        assert_eq!(iter.next(), Some(r));
        assert_eq!(iter.next(), Some(r));
        assert_eq!(iter.next(), Some(r));
        assert_eq!(iter.next(), Some(r));
        let range = iter.seek_to_value(o11);
        assert_eq!(range, 4..6);
        assert_eq!(iter.pos(), 4);
        assert_eq!(iter.next(), Some(o11));
        assert_eq!(iter.next(), Some(o11));
        let range = iter.seek_to_value(o12);
        assert_eq!(range, 6..7);
        assert_eq!(iter.pos(), 6);
        assert_eq!(iter.next(), Some(o12));
        let range = iter.seek_to_value(o21);
        assert_eq!(range, 7..10);
        assert_eq!(iter.pos(), 7);
        assert_eq!(iter.next(), Some(o21));
        assert_eq!(iter.next(), Some(o21));
        assert_eq!(iter.next(), Some(o21));
        let range = iter.seek_to_value(o22);
        assert_eq!(range, 10..12);
        assert_eq!(iter.pos(), 10);
        assert_eq!(iter.next(), Some(o22));
        assert_eq!(iter.next(), Some(o22));
        let range = iter.seek_to_value(o31);
        assert_eq!(range, 12..12);
        assert_eq!(iter.pos(), 12);
        let range = iter.seek_to_value(o32);
        assert_eq!(range, 12..14);
        assert_eq!(iter.pos(), 12);
        assert_eq!(iter.next(), Some(o32));
        assert_eq!(iter.next(), Some(o32));

        // seek each element and DONT read it
        let mut iter = ObjIdIter::new(v1_actor.iter(), v1_ctr.iter());
        let range = iter.seek_to_value(r);
        assert_eq!(range, 0..4);
        assert_eq!(iter.pos(), 0);
        let range = iter.seek_to_value(o11);
        assert_eq!(range, 4..6);
        assert_eq!(iter.pos(), 4);
        let range = iter.seek_to_value(o12);
        assert_eq!(range, 6..7);
        assert_eq!(iter.pos(), 6);
        let range = iter.seek_to_value(o21);
        assert_eq!(range, 7..10);
        assert_eq!(iter.pos(), 7);
        let range = iter.seek_to_value(o22);
        assert_eq!(range, 10..12);
        assert_eq!(iter.pos(), 10);
        let range = iter.seek_to_value(o31);
        assert_eq!(range, 12..12);
        assert_eq!(iter.pos(), 12);
        let range = iter.seek_to_value(o32);
        assert_eq!(range, 12..14);
        assert_eq!(iter.pos(), 12);

        // seek only odd items
        let mut iter = ObjIdIter::new(v1_actor.iter(), v1_ctr.iter());
        let range = iter.seek_to_value(o11);
        assert_eq!(range, 4..6);
        assert_eq!(iter.pos(), 4);
        let range = iter.seek_to_value(o21);
        assert_eq!(range, 7..10);
        assert_eq!(iter.pos(), 7);
        let range = iter.seek_to_value(o31);
        assert_eq!(range, 12..12);
        assert_eq!(iter.pos(), 12);

        // seek only even items
        let mut iter = ObjIdIter::new(v1_actor.iter(), v1_ctr.iter());
        let range = iter.seek_to_value(r);
        assert_eq!(range, 0..4);
        assert_eq!(iter.pos(), 0);
        let range = iter.seek_to_value(o12);
        assert_eq!(range, 6..7);
        assert_eq!(iter.pos(), 6);
        let range = iter.seek_to_value(o22);
        assert_eq!(range, 10..12);
        assert_eq!(iter.pos(), 10);
        let range = iter.seek_to_value(o32);
        assert_eq!(range, 12..14);
        assert_eq!(iter.pos(), 12);
    }
}
