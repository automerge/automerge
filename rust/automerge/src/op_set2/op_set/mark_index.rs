use crate::op_set2::op_set::RichTextQueryState;
use crate::op_set2::MarkData;
use crate::types::{Clock, OpId};
use hexane::{
    Acc, ColumnCursor, ColumnData, HasAcc, HasPos, PackError, Packable, RleCursor, Slab, SpanWeight,
};

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum MarkIndexValue {
    Start(OpId),
    End(OpId),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MarkIndexBuilder {
    Start(OpId, MarkData<'static>),
    End(OpId),
}

impl MarkIndexValue {
    fn as_i64(&self) -> i64 {
        match self {
            MarkIndexValue::Start(id) => {
                let tmp = ((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff);
                debug_assert_eq!(self, &MarkIndexValue::load(tmp));
                tmp
            }
            MarkIndexValue::End(id) => {
                let tmp = -(((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff));
                debug_assert_eq!(self, &MarkIndexValue::load(tmp));
                tmp
            }
        }
    }

    fn load(v: i64) -> Self {
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
pub(crate) struct MarkIndexColumn {
    data: ColumnData<MarkIndex>,
    cache: HashMap<OpId, MarkData<'static>>,
}

impl MarkIndexColumn {
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        // FIXME - would be much better to do this by run instead of by value
        let new_data = self
            .data
            .iter()
            .map(|m| m.map(|n| n.with_new_actor(idx)))
            .collect();
        let new_cache = self
            .cache
            .iter()
            .map(|(key, val)| (key.with_new_actor(idx), val.clone()))
            .collect();
        self.data = new_data;
        self.cache = new_cache;
    }

    pub(crate) fn new() -> Self {
        Self {
            data: ColumnData::new(),
            cache: HashMap::new(),
        }
    }

    pub(crate) fn splice(
        &mut self,
        index: usize,
        del: usize,
        values: Vec<Option<MarkIndexBuilder>>,
    ) {
        if del > 0 {
            // actually remove values from self.cache
            // will be needed for proper rollback
            // currently no way to test if code here would work
            // or trigger this panic with public api
            todo!()
        }
        let values = values
            .into_iter()
            .map(|v| match v? {
                MarkIndexBuilder::Start(id, mark) => {
                    self.cache.insert(id, mark);
                    Some(MarkIndexValue::Start(id))
                }
                MarkIndexBuilder::End(id) => Some(MarkIndexValue::End(id)),
            })
            .collect::<Vec<_>>();
        self.data.splice(index, del, values);
    }

    pub(crate) fn rich_text_at(
        &self,
        target: usize,
        clock: Option<&Clock>,
    ) -> RichTextQueryState<'static> {
        let mut marks = RichTextQueryState::default();
        for id in self.marks_at(target, clock) {
            let data = self.cache.get(&id).unwrap();
            marks.map.insert(id, data.clone());
        }
        marks
    }

    pub(crate) fn marks_at<'a>(
        &self,
        target: usize,
        clock: Option<&'a Clock>,
    ) -> impl Iterator<Item = OpId> + 'a {
        let sub = self
            .data
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

impl Packable for MarkIndexValue {
    fn width(item: &MarkIndexValue) -> usize {
        hexane::lebsize(item.as_i64()) as usize
    }

    fn pack(item: &MarkIndexValue, out: &mut Vec<u8>) {
        leb128::write::signed(out, item.as_i64()).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, MarkIndexValue>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        assert_eq!(MarkIndexValue::load(val).as_i64(), val);
        Ok((
            start_len - buff.len(),
            Cow::Owned(MarkIndexValue::load(val)),
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //use super::*;

    #[test]
    fn column_data_delta_simple() {}
}
