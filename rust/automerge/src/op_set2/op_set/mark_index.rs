use crate::types::{Clock, OpId};
use packer::{
    Acc, ColumnCursor, ColumnData, HasAcc, HasPos, MaybePackable, PackError, Packable, RleCursor,
    Slab, SpanWeight,
};

use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum MarkIndexValue {
    Start(OpId),
    End(OpId),
}

impl MarkIndexValue {
    fn with_new_actor(self, idx: usize) -> Self {
        match self {
            Self::Start(id) => Self::Start(id.with_new_actor(idx)),
            Self::End(id) => Self::End(id.with_new_actor(idx)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct MarkIndexSpanner {
    pub(crate) pos: usize,
    pub(crate) start: HashSet<OpId>,
    pub(crate) end: HashSet<OpId>,
}

impl SpanWeight<Slab> for MarkIndexSpanner {
    fn alloc(slab: &Slab) -> Self {
        // FIXME - need to keep a summary on the slab
        let pos = slab.len();
        let mut start = HashSet::default();
        let mut end = HashSet::default();
        let mut cursor = MarkIndex::default();
        let bytes = slab.as_slice();
        while let Some(run) = cursor.next(bytes) {
            match run.value.as_deref() {
                Some(MarkIndexValue::Start(id)) => {
                    start.insert(*id);
                }
                Some(MarkIndexValue::End(id)) => {
                    if !start.remove(id) {
                        end.insert(*id);
                    }
                }
                None => {}
            }
        }
        Self { pos, end, start }
    }
    fn and(mut self, other: &Self) -> Self {
        self.union(other);
        self
    }
    fn union(&mut self, other: &Self) {
        self.pos += other.pos;
        //let x = self.clone();
        for id in &other.start {
            if !self.end.remove(id) {
                self.start.insert(*id);
            }
        }
        for id in &other.end {
            if !self.start.remove(id) {
                self.end.insert(*id);
            }
        }
    }

    fn maybe_sub(&mut self, other: &Self) -> bool {
        if other.start.is_empty() && other.end.is_empty() {
            self.pos -= other.pos;
            true
        } else {
            false
        }
        // FIXME - this worked when I put ops in one at a time but now it doesnt?
        /*
                assert!(self.pos > other.pos);
                log!(" -- SUB ");
                log!(" -- :: A {:?}", self);
                log!(" -- :: B {:?}", other);
                self.pos -= other.pos;
                for id in &other.start {
                    if !self.start.remove(id) {
                        self.end.insert(*id);
                    }
                }
                for id in &other.end {
                    if !self.end.remove(id) {
                        self.start.insert(*id);
                    }
                }
                true
        */
    }
}

impl HasAcc for MarkIndexSpanner {
    fn acc(&self) -> Acc {
        Acc::new()
    }
}

impl HasPos for MarkIndexSpanner {
    fn pos(&self) -> usize {
        self.pos
    }
}

pub(crate) type MarkIndexInternal<const B: usize> = RleCursor<B, MarkIndexValue, MarkIndexSpanner>;
pub(crate) type MarkIndex = MarkIndexInternal<64>;

#[derive(Clone, Debug, Default)]
pub(crate) struct MarkIndexColumn(ColumnData<MarkIndex>);

impl MarkIndexColumn {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        // FIXME - would be much better to do this by run instead of by value
        let new_col = self
            .0
            .iter()
            .map(|m| m.map(|n| n.with_new_actor(idx)))
            .collect();
        self.0 = new_col
    }

    pub(crate) fn new() -> Self {
        Self(ColumnData::new())
    }

    pub(crate) fn splice<'a, E>(&mut self, index: usize, del: usize, values: Vec<E>)
    where
        E: MaybePackable<'a, MarkIndexValue> + Debug + Clone,
    {
        self.0.splice(index, del, values);
    }

    pub(crate) fn marks_at<'a>(
        &self,
        target: usize,
        clock: Option<&'a Clock>,
    ) -> impl Iterator<Item = OpId> + 'a {
        let sub = self
            .0
            .slabs
            .get_where_or_last(|acc, next| target < acc.pos() + next.pos());
        let mut start = sub.weight.start.into_iter().collect::<BTreeSet<_>>();
        let mut end = sub.weight.end;
        let mut pos = sub.weight.pos;
        let mut cursor = MarkIndex::default();
        let bytes = sub.element.as_slice();
        while let Some(run) = cursor.next(bytes) {
            pos += run.count;
            match run.value.as_deref() {
                Some(MarkIndexValue::Start(id)) => {
                    start.insert(*id);
                }
                Some(MarkIndexValue::End(id)) => {
                    if !start.remove(id) {
                        end.insert(*id);
                    }
                }
                None => {}
            }
            if pos > target {
                break;
            }
        }
        start
            .into_iter()
            .filter(move |id| clock.map(|c| c.covers(id)).unwrap_or(true))
    }
}

impl From<i64> for MarkIndexValue {
    fn from(v: i64) -> Self {
        if v < 0 {
            let v = -v as u64;
            let actor = (v >> 32) as usize;
            let ctr = v & 0xffffffff;
            Self::End(OpId::new(ctr, actor))
        } else {
            let v = v as u64;
            let actor = (v >> 32) as usize;
            let ctr = v & 0xffffffff;
            Self::Start(OpId::new(ctr, actor))
        }
    }
}

impl From<MarkIndexValue> for i64 {
    fn from(v: MarkIndexValue) -> Self {
        match v {
            MarkIndexValue::Start(id) => {
                let tmp = ((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff);
                assert_eq!(v, MarkIndexValue::from(tmp));
                tmp
            }
            MarkIndexValue::End(id) => {
                let tmp = -(((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff));
                assert_eq!(v, MarkIndexValue::from(tmp));
                tmp
            }
        }
    }
}

impl Packable for MarkIndexValue {
    fn width(item: &MarkIndexValue) -> usize {
        packer::lebsize(i64::from(*item)) as usize
    }

    fn pack(item: &MarkIndexValue, out: &mut Vec<u8>) {
        leb128::write::signed(out, i64::from(*item)).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, MarkIndexValue>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        assert_eq!(i64::from(MarkIndexValue::from(val)), val);
        Ok((
            start_len - buff.len(),
            Cow::Owned(MarkIndexValue::from(val)),
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //use super::*;

    #[test]
    fn column_data_delta_simple() {}
}
