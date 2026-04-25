use super::parents::Parents;
use crate::clock::{Clock, ClockRange};
use crate::exid::ExId;
use crate::iter::tools::{MergeIter, SkipIter, SkipWrap};
use crate::marks::{MarkSet, RichTextQueryState};
use crate::storage::columns::BadColumnLayout;
use crate::storage::{columns::compression::Uncompressed, Document, RawColumns};
use crate::types;
use crate::types::{
    ActorId, ElemId, Export, Exportable, ObjId, ObjMeta, ObjType, OpId, Prop, SequenceType,
    TextEncoding,
};
use crate::AutomergeError;

use super::op::{Op, OpLike, SuccCursors, SuccInsert, TxOp};

use super::columns::Columns;

use super::types::{Action, ActorIdx, KeyRef, MarkData, OpType, ScalarValue};

use hexane::PackError;
use itertools::Itertools;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::num::NonZeroUsize;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

mod found_op;
mod index;
mod insert;
mod mark_index;
mod marks;
mod op_iter;
mod op_query;
mod top_op;
mod visible;

pub(crate) use index::{IndexBuilder, ObjIndex, ObjInfo};

pub(crate) use crate::iter::{Keys, ListRange, MapRange, SpansInternal};

pub(crate) use found_op::OpsFoundIter;
pub(crate) use insert::InsertQuery;
pub(crate) use mark_index::{MarkIndexBuilder, MarkIndexColumn};
pub(crate) use marks::{MarkIter, NoMarkIter};
pub(crate) use op_iter::{
    ActionIter, ActionValueIter, CtrWalker, InsertIter, KeyIter, MarkInfoIter, ObjIdIter, OpIdIter,
    OpIter, ReadOpError, SuccIterIter, SuccWalker, ValueIter,
};
pub(crate) use op_query::{OpQuery, OpQueryTerm};
pub(crate) use top_op::TopOpIter;
pub(crate) use visible::{VisIter, VisibleOpIter};

pub(crate) type InsertAcc<'a> = hexane::v1::PrefixIter<'a, bool>;

#[derive(Debug, Clone)]
pub(crate) struct OpSet {
    pub(crate) actors: Vec<ActorId>,
    pub(crate) obj_info: ObjIndex,
    cols: Columns,
    pub(crate) text_encoding: TextEncoding,
}

impl OpSet {
    #[cfg(test)]
    pub(crate) fn debug_cmp(&self, other: &Self) {
        self.cols.debug_cmp(&other.cols)
    }

    #[cfg(test)]
    pub(crate) fn save_checkpoint(&self) -> std::collections::HashMap<&'static str, Vec<u8>> {
        self.cols.save_checkpoint()
    }

    #[cfg(test)]
    pub(crate) fn from_actors(actors: Vec<ActorId>, encoding: TextEncoding) -> Self {
        OpSet {
            actors,
            cols: Columns::default(),
            obj_info: ObjIndex::default(),
            text_encoding: encoding,
        }
    }

    pub(crate) fn dump(&self) {
        log!("OpSet");
        log!("  len: {}", self.len());
        log!("  actors: {:?}", self.actors);
        self.cols.dump();
    }

    pub(crate) fn parents(&self, obj: ObjId, clock: Option<Clock>) -> Parents<'_> {
        Parents {
            obj,
            ops: self,
            clock,
        }
    }

    pub(crate) fn index_builder(&self) -> IndexBuilder {
        IndexBuilder::new(self, self.text_encoding)
    }

    pub(crate) fn reset_top(&mut self, range: Range<usize>) {
        let top = self.cols.index.top.values().iter_range(range.clone());
        let vis = self.cols.index.visible.iter_range(range.clone());

        let mut conflicts = vec![];
        let mut expose = None;
        let mut last_t = None;
        for (i, (v, t)) in vis.zip(top).enumerate() {
            if t {
                assert!(v);
                if let Some(n) = last_t {
                    conflicts.push(n);
                }
                last_t = Some(i);
                expose = None;
            } else if v {
                if let Some(n) = last_t {
                    conflicts.push(n);
                }
                last_t = None;
                expose = Some(i);
            }
        }

        for n in conflicts {
            self.conflict(range.start + n)
        }

        if let Some(n) = expose {
            self.expose(range.start + n)
        }
    }

    pub(crate) fn conflict(&mut self, pos: usize) {
        self.cols.index.top.splice(pos, 1, [false]);
    }

    pub(crate) fn expose(&mut self, pos: usize) {
        self.cols.index.top.splice(pos, 1, [true]);
    }

    pub(crate) fn validate(
        bytes: usize,
        cols: &RawColumns<Uncompressed>,
    ) -> Result<RawColumns<Uncompressed>, BadColumnLayout> {
        Columns::validate(bytes, cols)
    }

    pub(crate) fn validate_op_order(&self) -> bool {
        let mut stepper = Default::default();
        for op in self.iter() {
            if !op.step(&mut stepper) {
                return false;
            }
        }
        true
    }

    pub(crate) fn validate_top_index(&self) -> bool {
        let _top = &self.cols.index.top;
        let _visible = &self.cols.index.visible;

        assert_eq!(_top.len(), _visible.len());
        assert_eq!(_top.len(), self.len());

        let top_iter = _top.values().iter();
        let vis_iter = _visible.iter();
        let op_iter = self.iter();

        let mut last_op = None;
        let mut first_top = None;
        let mut last_vis = None;

        for ((top, vis), op) in top_iter.zip(vis_iter).zip(op_iter) {
            let this_op = Some((op.obj, op.elemid_or_key()));

            if this_op != last_op {
                assert_eq!(first_top, last_vis);
                last_op = this_op;
                first_top = None;
                last_vis = None;
            }

            if top {
                assert!(vis);
                if first_top.is_none() {
                    first_top = Some(op.pos);
                }
            }
            if vis {
                last_vis = Some(op.pos);
            }
        }
        assert_eq!(first_top, last_vis);
        true
    }

    pub(crate) fn set_indexes(&mut self, builder: IndexBuilder) {
        let indexes = builder.finish();

        assert_eq!(indexes.text.len(), self.len());
        assert_eq!(indexes.mark.len(), self.len());
        assert_eq!(indexes.visible.len(), self.len());
        assert_eq!(indexes.inc.len(), self.cols.sub_len());

        self.cols.index.text = indexes.text;
        self.cols.index.top = indexes.top;
        self.cols.index.visible = indexes.visible;
        self.cols.index.inc = indexes.inc;
        self.cols.index.mark = indexes.mark;
        self.obj_info = indexes.obj_info;
    }

    pub(crate) fn splice_objects<O: OpLike>(&mut self, ops: &[O]) {
        for op in ops {
            if let Some(obj_info) = op.obj_info() {
                self.obj_info.insert(op.id(), obj_info);
            }
        }
    }

    pub(crate) fn splice<O: OpLike>(&mut self, pos: usize, ops: &[O]) -> usize {
        let added = self.cols.splice(pos, ops, self.text_encoding);
        self.splice_objects(ops);
        added
    }

    pub(crate) fn undo_op(&mut self, op: &TxOp) {
        if !op.undo.is_empty() {
            self.undo_succ(&op.undo);
        }
        self.cols.remove_ops(op.pos, &[op]);
        if op.obj_info().is_some() {
            self.obj_info.remove(op.id());
        }
    }

    pub(crate) fn add_succ(&mut self, op_pos: &[SuccInsert]) {
        let mut succ_inc = 0;
        let mut last_pos = None;
        for i in op_pos.iter().rev() {
            if last_pos == Some(i.pos) {
                succ_inc += 1;
            } else {
                last_pos = Some(i.pos);
                succ_inc = 1;
            }
            self.cols
                .succ_count
                .splice(i.pos, 1, [(i.len + succ_inc) as u32]);
            self.cols.succ_actor.splice(i.sub_pos, 0, [i.id.actoridx()]);
            self.cols
                .succ_ctr
                .splice(i.sub_pos, 0, [i.id.counter() as u32]);
            self.cols.index.inc.splice(i.sub_pos, 0, [i.inc]);
            if i.inc.is_none() {
                self.cols.index.visible.splice(i.pos, 1, [false]);
                self.cols.index.text.splice(i.pos, 1, [None::<u32>]);
                self.cols.index.top.splice(i.pos, 1, [false]);
            }
        }
    }

    pub(crate) fn add_succ_with_undo(&mut self, op_pos: &[SuccInsert]) -> Vec<SuccUndo> {
        let mut undo = vec![];
        let mut succ_inc = 0;
        let mut last_pos = None;
        let mut expose = false;
        let mut delete = false;
        for i in op_pos.iter().rev() {
            if last_pos == Some(i.pos) {
                succ_inc += 1;
            } else {
                last_pos = Some(i.pos);
                succ_inc = 1;
            }
            self.cols
                .succ_count
                .splice(i.pos, 1, [(i.len + succ_inc) as u32]);
            self.cols.succ_actor.splice(i.sub_pos, 0, [i.id.actoridx()]);
            self.cols
                .succ_ctr
                .splice(i.sub_pos, 0, [i.id.counter() as u32]);
            self.cols.index.inc.splice(i.sub_pos, 0, [i.inc]);
            if i.inc.is_none() {
                delete = true;
                let visible = self.cols.index.visible.get(i.pos);
                let text = self.cols.index.text.values().get(i.pos);
                let top = self.cols.index.top.values().get(i.pos);
                undo.push(SuccUndo::new(*i, visible, text, top));
                self.cols.index.visible.splice(i.pos, 1, [false]);
                self.cols.index.text.splice(i.pos, 1, [None::<u32>]);
                self.cols.index.top.splice(i.pos, 1, [false]);
            } else if delete && !expose {
                expose = true;
                let top = self.cols.index.top.values().get(i.pos);
                undo.push(SuccUndo::new(*i, None, None, top));
                self.cols.index.top.splice(i.pos, 1, [true]);
            } else {
                undo.push(SuccUndo::new(*i, None, None, None));
            }
        }
        undo
    }

    pub(crate) fn undo_succ(&mut self, op_pos: &[SuccUndo]) {
        for undo in op_pos.iter().rev() {
            let i = undo.succ;
            self.cols.succ_count.splice(i.pos, 1, [i.len as u32]);
            self.cols
                .succ_actor
                .splice(i.sub_pos, 1, std::iter::empty::<ActorIdx>());
            self.cols
                .succ_ctr
                .splice(i.sub_pos, 1, std::iter::empty::<u32>());
            self.cols
                .index
                .inc
                .splice(i.sub_pos, 1, std::iter::empty::<Option<i64>>());
            if let Some(vis) = undo.visible {
                self.cols.index.visible.splice(i.pos, 1, [vis]);
            }
            if let Some(text) = undo.text {
                self.cols.index.text.splice(i.pos, 1, [text]);
            }
            if let Some(top) = undo.top {
                self.cols.index.top.splice(i.pos, 1, [top]);
            }
        }
    }

    pub(crate) fn parent_object(&self, child: &ObjId, clock: Option<&Clock>) -> Option<Parent> {
        let (op, visible) = self.find_op_by_id_and_vis(child.id()?, clock)?;
        let obj = op.obj;
        let typ = self.object_type(&obj)?;
        let prop = match op.key {
            KeyRef::Map(k) => Prop::Map(k.to_string()),
            KeyRef::Seq(_) => {
                let seq_type = match typ {
                    ObjType::List => SequenceType::List,
                    ObjType::Text => SequenceType::Text,
                    _ => panic!("unexpected object type {:?} for seq key {:?}", typ, op.key),
                };
                let index = self.seek_list_opid(&op.obj, op.id, seq_type, clock)?.index;
                Prop::Seq(index)
            }
        };
        Some(Parent {
            typ,
            obj: op.obj,
            prop,
            visible,
        })
    }

    pub(crate) fn keys<'a>(&'a self, obj: &ObjId, clock: Option<Clock>) -> Keys<'a> {
        let iter = self.iter_obj(obj).visible_slow(clock).top_ops();
        Keys::new(self, iter)
    }

    pub(crate) fn spans(&self, obj: &ObjId, clock: Option<Clock>) -> SpansInternal<'_> {
        let range = self.scope_to_obj(obj);
        SpansInternal::new(self, range, clock, self.text_encoding)
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> ListRange<'_> {
        let obj_range = self.scope_to_obj(obj);
        ListRange::new(self, obj_range, clock, range)
    }

    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'_> {
        let obj_range = self.scope_to_obj(obj);

        let scope = |s: &str| self.cols.key_str.scope_to_value(Some(s), obj_range.clone());

        let start = match range.start_bound() {
            std::ops::Bound::Unbounded => obj_range.start,
            std::ops::Bound::Included(s) => scope(s.as_str()).start,
            std::ops::Bound::Excluded(s) => scope(s.as_str()).end,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Unbounded => obj_range.end,
            std::ops::Bound::Included(s) => scope(s.as_str()).end,
            std::ops::Bound::Excluded(s) => scope(s.as_str()).start,
        };

        MapRange::new(self, start..end, clock)
    }

    pub(crate) fn len(&self) -> usize {
        self.cols.len()
    }

    pub(crate) fn sub_len(&self) -> usize {
        self.cols.sub_len()
    }

    pub(crate) fn seq_length(
        &self,
        obj: &ObjId,
        text_encoding: TextEncoding,
        clock: Option<Clock>,
    ) -> usize {
        let range = self.scope_to_obj(obj);
        let vis = VisIter::new(self, clock.as_ref(), range.clone());
        let typ = self.object_type(obj).unwrap_or(ObjType::Map);
        if typ == ObjType::Text {
            if clock.is_none() {
                self.cols.index.text.prefix_delta(range.clone()) as usize
            } else {
                self.action_value_iter(range.clone(), clock.as_ref())
                    .map(|(action, value, _)| match (action, &value) {
                        (Action::Set, ScalarValue::Str(s)) => text_encoding.width(s),
                        (Action::Mark, _) => 0,
                        _ => text_encoding.width("\u{fffc}"),
                    })
                    .sum()
            }
        } else if typ == ObjType::List {
            let insert = self
                .cols
                .insert
                .iter_range(range.clone())
                .map(|(prefix, _)| prefix);
            SkipIter::new(insert, vis).dedup().count()
        } else {
            let key = self.cols.key_str.iter_range(range.clone());
            SkipIter::new(key, vis).dedup().count()
        }
    }

    pub(crate) fn query_insert_at_text(
        &self,
        obj: &ObjId,
        index: NonZeroUsize,
    ) -> Option<QueryNth> {
        let range = self.scope_to_obj(obj);

        let mut text_iter = self.cols.index.text.iter_range(range.clone());
        let tx = text_iter.advance_prefix((index.get() - 1) as u64)?;
        let start_pos = tx.pos.min(range.end);
        let current_acc = tx.delta as usize;

        let iter = self.iter_range(&(start_pos..range.end));
        let marks = self.cols.index.mark.rich_text_at(start_pos, None);
        let mut query = InsertQuery::new(
            iter,
            index.get(),
            SequenceType::Text,
            self.text_encoding,
            None,
            marks,
        );
        query.resolve(current_acc).ok()
    }

    // WIP: commented out to unblock the build.  See conversation context.
    // pub(crate) fn query_insert_at_text2(
    //     &self,
    //     obj: &ObjId,
    //     index: NonZeroUsize,
    // ) -> Option<QueryNth> {
    //     let range = self.scope_to_obj(obj);
    //
    //     let mut text_iter = self.cols.index.text.iter_range(range.clone());
    //     let tx = text_iter.advance_prefix((index.get() - 1) as u64)?;
    //     let start_pos = tx1.pos.min(range.end);
    //     let current_acc = tx1.delta as usize;
    //
    //     while let Some(run) = text_iter.next_run() {
    //       match run.value {
    //         (prefix, Some(0)) => (), // mark
    //         (prefix, Some(w)) => break; // end
    //         (prefix, None => (), // deleted char
    //       }
    //     }
    //
    //     let iter = self.iter_range(&(start_pos..range.end));
    //     let marks = self.cols.index.mark.rich_text_at(start_pos, None);
    //     let mut query = InsertQuery::new(
    //         iter,
    //         index.get(),
    //         SequenceType::Text,
    //         self.text_encoding,
    //         None,
    //         marks,
    //     );
    //     query.resolve(current_acc).ok()
    // }

    pub(crate) fn query_insert_at_list(
        &self,
        obj: &ObjId,
        index: NonZeroUsize,
    ) -> Option<QueryNth> {
        let range = self.scope_to_obj(obj);

        let mut top_iter = self.cols.index.top.iter_range(range.clone());
        let tx = top_iter.advance_prefix(index.get() - 1)?;

        let iter = self.iter_range(&(tx.pos..range.end));
        let marks = self.cols.index.mark.rich_text_at(tx.pos, None);
        let mut query = InsertQuery::new(
            iter,
            index.get(),
            SequenceType::List,
            self.text_encoding,
            None,
            marks,
        );
        query.resolve(index.get() - 1).ok()
    }

    pub(crate) fn query_insert_at(
        &self,
        obj: &ObjId,
        index: usize,
        seq_type: SequenceType,
        clock: Option<Clock>,
    ) -> Result<QueryNth, AutomergeError> {
        if clock.is_none() && index > 0 {
            let index = NonZeroUsize::new(index).unwrap();
            let query = if seq_type == SequenceType::List {
                self.query_insert_at_list(obj, index)
            } else {
                self.query_insert_at_text(obj, index)
            };
            if let Some(q) = query {
                debug_assert_eq!(
                    Ok(&q),
                    InsertQuery::new(
                        self.iter_obj(obj),
                        index.get(),
                        seq_type,
                        self.text_encoding,
                        clock,
                        Default::default()
                    )
                    .resolve(0)
                    .as_ref()
                );
                return Ok(q);
            }
        }
        InsertQuery::new(
            self.iter_obj(obj),
            index,
            seq_type,
            self.text_encoding,
            clock,
            Default::default(),
        )
        .resolve(0)
    }

    pub(crate) fn seek_ops_by_map_key<'a>(
        &'a self,
        obj: &ObjId,
        key: &str,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        let range = self.prop_range(obj, key);
        let iter = self.iter_range(&range);
        let end_pos = iter.end_pos();
        let ops = iter.visible(self, clock).collect::<Vec<_>>();
        assert_eq!(end_pos, range.end);
        OpsFound {
            index: 0,
            ops,
            range,
            end_pos,
        }
    }

    // TODO called only needs op ids, width, elemid,
    // this could touch a lot less data
    pub(crate) fn seek_ops_by_index<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
        seq_type: SequenceType,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        if clock.is_none() {
            let found = if seq_type == SequenceType::List {
                self.seek_list_ops_by_index_fast(obj, index)
            } else {
                self.seek_text_ops_by_index_fast(obj, index)
            };
            #[cfg(feature = "slow_path_assertions")]
            {
                let slow = self.seek_ops_by_index_slow(obj, index, seq_type, clock);
                assert_eq!(found, slow, "fast != slow");
            }
            found
        } else {
            self.seek_ops_by_index_slow(obj, index, seq_type, clock)
        }
    }

    pub(crate) fn seek_ops_by_index_slow<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
        seq_type: SequenceType,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        let sub_iter = self.iter_obj(obj);
        let end = sub_iter.range.end;
        let mut end_pos = sub_iter.pos();
        let iter = OpsFoundIter::new(sub_iter.no_marks(), clock.cloned());
        let mut len = 0;
        let mut range = end_pos..end_pos;
        for mut ops in iter {
            let width = ops.width(seq_type, self.text_encoding);
            if len + width > index {
                ops.index = len;
                return ops;
            }
            len += width;
            end_pos = ops.end_pos;
            range = ops.range;
        }
        assert_eq!(range.end, end_pos);
        OpsFound {
            index,
            ops: vec![],
            end_pos: end,
            range: end..end,
        }
    }

    fn list_register_at_pos(&self, pos: usize, range: Range<usize>) -> Range<usize> {
        let mut iter = self.cols.insert.iter_range(0..range.end);
        let at_pos = iter.nth(pos);
        let next_run = iter.next_run();
        match (at_pos, next_run) {
            (Some((_, insert)), Some(run)) if insert && !run.value.1 => pos..pos + run.count + 1,
            (Some((_, insert)), _) if insert => pos..pos + 1,
            (Some((pre, _)), run) => {
                let start = self.cols.insert.get_index_for_total(pre);
                match run {
                    Some(run) if !run.value.1 => start..pos + run.count + 1,
                    _ => start..pos + 1,
                }
            }
            _ => pos..pos,
        }
    }

    pub(crate) fn seek_list_ops_by_index_fast<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
    ) -> OpsFound<'a> {
        let range = self.scope_to_obj(obj);

        let mut top_iter = self.cols.index.top.iter_range(range.clone());
        if let Some(tx) = top_iter.advance_prefix(index) {
            let range = self.list_register_at_pos(tx.pos, range);
            let end_pos = range.end;
            let ops = self.iter_range(&range).visible(self, None).collect();
            OpsFound {
                index,
                ops,
                range,
                end_pos,
            }
        } else {
            let end_pos = range.end;
            OpsFound {
                index,
                ops: vec![],
                range: end_pos..end_pos,
                end_pos,
            }
        }
    }

    pub(crate) fn seek_text_ops_by_index_fast<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
    ) -> OpsFound<'a> {
        let range = self.scope_to_obj(obj);
        let mut text_iter = self.cols.index.text.iter_range(range.clone());
        let seek = text_iter.advance_prefix(index as u64);
        if let Some(tx) = seek {
            if let Some(op) = self.get(tx.pos) {
                return OpsFound {
                    index: tx.delta as usize,
                    ops: vec![op],
                    range: tx.pos..tx.pos + 1,
                    end_pos: tx.pos + 1,
                };
            }
        }
        OpsFound {
            index,
            ops: vec![],
            range: range.end..range.end,
            end_pos: range.end,
        }
    }

    fn get(&self, pos: usize) -> Option<Op<'_>> {
        self.iter_range(&(pos..(pos + 1))).next()
    }

    fn get_op_id_pos(&self, id: OpId) -> Option<usize> {
        self.cols
            .id_ctr
            .find_by_value(id.counter() as u32)
            .find(|&pos| self.cols.id_actor.get(pos) == Some(id.actoridx()))
    }

    pub(crate) fn seek_list_opid(
        &self,
        obj: &ObjId,
        opid: OpId,
        seq_type: SequenceType,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        if clock.is_none() {
            let found = self.seek_list_opid_fast(obj, opid, seq_type);
            debug_assert_eq!(found, self.seek_list_opid_slow(obj, opid, seq_type, clock));
            found
        } else {
            self.seek_list_opid_slow(obj, opid, seq_type, clock)
        }
    }

    pub(crate) fn seek_list_opid_fast(
        &self,
        obj: &ObjId,
        id: OpId,
        encoding: SequenceType,
    ) -> Option<FoundOpId<'_>> {
        let obj_range = self.scope_to_obj(obj);
        let pos = self.get_op_id_pos(id)?;
        let op = self.get(pos)?;
        let visible;
        let index;
        if encoding == SequenceType::List {
            assert!(obj_range.contains(&pos)); // safe to unwrap
            let prefix = self.cols.index.top.delta(obj_range.start, pos).unwrap();
            visible = prefix.value;
            index = prefix.delta;
        } else {
            assert!(obj_range.contains(&pos)); // safe to unwrap
            let prefix = self.cols.index.text.delta(obj_range.start, pos).unwrap();
            visible = prefix.value.is_some();
            index = prefix.delta as usize;
        }
        Some(FoundOpId { op, index, visible })
    }

    pub(crate) fn seek_list_opid_slow(
        &self,
        obj: &ObjId,
        opid: OpId,
        seq_type: SequenceType,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        let op = self.iter_obj(obj).find(|op| op.id == opid)?;
        let iter = OpsFoundIter::new(self.iter_obj(obj).no_marks(), clock.cloned());
        let mut index = 0;
        for ops in iter {
            if ops.end_pos > op.pos {
                let visible = ops.ops.contains(&op);
                return Some(FoundOpId { op, index, visible });
            }
            index += ops.width(seq_type, self.text_encoding);
        }
        None
    }

    pub(crate) fn action_iter_range(&self, range: &Range<usize>) -> ActionIter<'_> {
        ActionIter::new(self.cols.action.iter_range(range.clone()))
    }

    pub(crate) fn insert_acc_range(&self, range: &Range<usize>) -> InsertAcc<'_> {
        self.cols.insert.iter_range(range.clone())
    }

    pub(crate) fn key_str_iter_range(
        &self,
        range: &Range<usize>,
    ) -> hexane::v1::Iter<'_, Option<String>> {
        self.cols.key_str.iter_range(range.clone())
    }

    pub(crate) fn action_value_iter(
        &self,
        range: Range<usize>,
        clock: Option<&Clock>,
    ) -> SkipIter<ActionValueIter<'_>, VisIter<'_>> {
        let value = self.value_iter_range(&range);
        let action = self.action_iter_range(&range);
        let vis = VisIter::new(self, clock, range);
        let iter = ActionValueIter::new(action, value);
        SkipIter::new(iter, vis)
    }

    pub(crate) fn text(&self, obj: &ObjId, clock: Option<Clock>) -> String {
        let range = self.scope_to_obj(obj);
        let skip = self.action_value_iter(range, clock.as_ref());
        skip.map(|item| match item {
            (Action::Set, ScalarValue::Str(s), _) => s,
            (Action::Mark, _, _) => Cow::Borrowed(""),
            (_, _, _) => Cow::Borrowed("\u{fffc}"),
        })
        .collect()
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.counter(), self.actors[id.actor()].clone(), id.actor())
        }
    }

    pub(crate) fn iter_obj_ids(&self) -> IterObjIds<'_> {
        let mut v1_ctr = self.cols.obj_ctr.iter();
        let mut v1_actor = self.cols.obj_actor.iter();
        let next_ctr = v1_ctr.next_run();
        let next_actor = v1_actor.next_run();

        IterObjIds {
            v1_ctr,
            v1_actor,
            next_ctr,
            next_actor,
            pos: 0,
        }
    }

    pub(crate) fn iter_objs(&self) -> impl Iterator<Item = (ObjMeta, OpIter<'_>)> {
        self.iter_obj_ids().filter_map(|(id, range)| {
            let typ = self.object_type(&id)?;
            let obj_meta = ObjMeta { id, typ };
            Some((obj_meta, self.iter_range(&range)))
        })
    }

    pub(crate) fn top_ops<'a>(
        &'a self,
        obj: &ObjId,
        clock: Option<Clock>,
    ) -> TopOpIter<'a, VisibleOpIter<'a, OpIter<'a>>> {
        self.iter_obj(obj).visible_slow(clock).top_ops()
    }

    pub(crate) fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), self.actors[id.actor()]),
            Export::Special(s) => s,
        }
    }

    pub(crate) fn find_op_by_id_and_vis(
        &self,
        id: &OpId,
        clock: Option<&Clock>,
    ) -> Option<(Op<'_>, bool)> {
        if clock.is_none() {
            let result = self.find_op_by_id_and_vis_fast(id);
            debug_assert_eq!(result, self.find_op_by_id_and_vis_slow(id, clock));
            result
        } else {
            self.find_op_by_id_and_vis_slow(id, clock)
        }
    }

    pub(crate) fn find_op_by_id_and_vis_fast(&self, id: &OpId) -> Option<(Op<'_>, bool)> {
        let pos = self.get_op_id_pos(*id)?;
        let visible = self.cols.index.top.values().get(pos).unwrap_or(false);
        let op = self.get(pos)?;
        Some((op, visible))
    }

    pub(crate) fn find_op_by_id_and_vis_slow(
        &self,
        id: &OpId,
        clock: Option<&Clock>,
    ) -> Option<(Op<'_>, bool)> {
        let start = self.get_op_id_pos(*id)?;
        let mut iter = self.iter_range(&(start..self.len()));
        let mut o1 = iter.next()?;
        let mut vis = o1.scope_to_clock(clock);
        for mut o2 in iter {
            if o2.obj != o1.obj || o1.elemid_or_key() != o2.elemid_or_key() {
                break;
            }
            if o2.scope_to_clock(clock) {
                vis = false;
                break;
            }
        }
        Some((o1, vis))
    }

    pub(crate) fn get_increment_diff_at_pos(&self, pos: usize, clock: &ClockRange) -> (i64, i64) {
        if let Some((prefix, count)) = self.cols.succ_count.get(pos) {
            let start = (prefix - count as u64) as usize;
            let len = count as usize;
            let end = prefix as usize;
            let succ = SuccCursors {
                len,
                succ_actor: self.cols.succ_actor.iter_range(start..end),
                succ_counter: self.cols.succ_ctr.iter_range(start..end),
                inc_values: self.cols.index.inc.iter_range(start..end),
            };
            let mut inc1 = 0;
            let mut inc2 = 0;
            for (id, value) in succ.with_inc() {
                if let Some(i) = value {
                    if clock.visible_before(&id) {
                        inc1 += i;
                    }
                    if clock.visible_after(&id) {
                        inc2 += i;
                    }
                }
            }
            (inc1, inc2)
        } else {
            (0, 0)
        }
    }

    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        self.obj_info.object_type(obj)
    }

    pub(crate) fn object_parent(&self, obj: &ObjId) -> Option<ObjId> {
        self.obj_info.object_parent(obj)
    }

    pub(crate) fn get_actor(&self, idx: usize) -> &ActorId {
        &self.actors[idx]
    }

    pub(crate) fn get_actor_safe(&self, idx: usize) -> Option<&ActorId> {
        self.actors.get(idx)
    }

    pub(crate) fn lookup_actor(&self, actor: &ActorId) -> Option<usize> {
        self.actors.binary_search(actor).ok()
    }

    pub(crate) fn new(text_encoding: TextEncoding) -> Self {
        OpSet {
            actors: vec![],
            cols: Columns::default(),
            obj_info: ObjIndex::default(),
            text_encoding,
        }
    }

    pub(crate) fn load(doc: &Document<'_>, text_encoding: TextEncoding) -> Result<Self, PackError> {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = doc.op_raw_bytes();
        let actors = doc.actors().to_vec();
        Self::from_parts(doc.op_metadata.clone(), data, actors, text_encoding)
    }

    #[cfg(test)]
    pub(crate) fn from_doc_ops<
        'a,
        I: Iterator<Item = super::op::Op<'a>> + ExactSizeIterator + Clone,
    >(
        actors: Vec<ActorId>,
        ops: I,
    ) -> Self {
        let cols = Columns::new(ops);
        OpSet {
            actors,
            cols,
            obj_info: ObjIndex::default(),
            text_encoding: TextEncoding::platform_default(),
        }
    }

    fn from_parts(
        cols: RawColumns<Uncompressed>,
        data: &[u8],
        actors: Vec<ActorId>,
        text_encoding: TextEncoding,
    ) -> Result<Self, PackError> {
        let cols = Columns::load(cols.as_map(), data, &actors)?;

        let op_set = OpSet {
            actors,
            cols,
            obj_info: ObjIndex::default(),
            text_encoding,
        };

        Ok(op_set)
    }

    pub(crate) fn export(&self) -> (RawColumns<Uncompressed>, Vec<u8>) {
        self.cols.export()
    }

    pub(crate) fn scope_to_obj(&self, obj: &ObjId) -> Range<usize> {
        let range = self
            .cols
            .obj_ctr
            .scope_to_value(obj.counter().map(|c| c as u32), ..);
        self.cols.obj_actor.scope_to_value(obj.actor(), range)
    }

    pub(crate) fn iter_ctr_range(
        &self,
        range: Range<usize>,
    ) -> SkipIter<OpIter<'_>, SkipWrap<MergeIter<CtrWalker<'_>, SuccWalker<'_>>>> {
        SkipIter::new(
            self.iter(),
            MergeIter::new(
                CtrWalker::new(&self.cols.id_ctr, range.clone()),
                SuccWalker::new(self, range),
            )
            .skip(),
        )
    }

    pub(crate) fn prop_range(&self, obj: &ObjId, prop: &str) -> Range<usize> {
        let range = self.scope_to_obj(obj);
        self.cols.key_str.scope_to_value(Some(prop), range)
    }

    pub(crate) fn iter_obj<'a>(&'a self, obj: &ObjId) -> OpIter<'a> {
        let range = self.scope_to_obj(obj);
        self.iter_range(&range)
    }

    pub(crate) fn value_iter_range(&self, range: &Range<usize>) -> ValueIter<'_> {
        let meta = self.cols.value_meta.iter_range(range.clone());
        let value_advance = self.cols.value_meta.get_prefix(range.start) as usize;
        let value_raw = self.cols.value.iter_at(value_advance);
        ValueIter::new(meta, value_raw)
    }

    pub(crate) fn id_iter_range(&self, range: &Range<usize>) -> OpIdIter<'_> {
        OpIdIter::new(
            self.cols.id_actor.iter_range(range.clone()),
            self.cols.id_ctr.iter_range(range.clone()),
        )
    }

    pub(crate) fn mark_info_iter_range(&self, range: &Range<usize>) -> MarkInfoIter<'_> {
        MarkInfoIter::new(
            self.cols.mark_name.iter_range(range.clone()),
            self.cols.expand.iter_range(range.clone()),
        )
    }

    pub(crate) fn succ_iter_range(&self, range: &Range<usize>) -> SuccIterIter<'_> {
        let succ_start = self.cols.succ_count.get_prefix(range.start) as usize;
        let succ_count = self.cols.succ_count.iter_range(range.clone());
        let succ_actor = self
            .cols
            .succ_actor
            .iter_range(succ_start..self.cols.succ_actor.len());
        let succ_counter = self
            .cols
            .succ_ctr
            .iter_range(succ_start..self.cols.succ_ctr.len());
        let inc_values = self
            .cols
            .index
            .inc
            .iter_range(succ_start..self.cols.index.inc.len());
        SuccIterIter::new(succ_count, succ_actor, succ_counter, inc_values)
    }

    pub(crate) fn iter_range(&self, range: &Range<usize>) -> OpIter<'_> {
        let value = self.value_iter_range(range);
        let succ = self.succ_iter_range(range);

        OpIter {
            pos: range.start,
            id: self.id_iter_range(range),
            obj: ObjIdIter::new_range(
                self.cols.obj_actor.iter_range(range.clone()),
                self.cols.obj_ctr.iter_range(range.clone()),
            ),
            key: KeyIter::new(
                self.cols.key_str.iter_range(range.clone()),
                self.cols.key_actor.iter_range(range.clone()),
                self.cols.key_ctr.iter_range(range.clone()),
            ),
            succ,
            insert: InsertIter::new(self.cols.insert.values().iter_range(range.clone())),
            action: ActionIter::new(self.cols.action.iter_range(range.clone())),
            value,
            marks: self.mark_info_iter_range(range),
            range: range.clone(),
            op_set: self,
        }
    }

    pub(crate) fn obj_id_iter(&self) -> ObjIdIter<'_> {
        ObjIdIter::new(self.cols.obj_actor.iter(), self.cols.obj_ctr.iter())
    }

    pub(crate) fn iter(&self) -> OpIter<'_> {
        OpIter {
            pos: 0,
            id: OpIdIter::new(self.cols.id_actor.iter(), self.cols.id_ctr.iter()),
            obj: ObjIdIter::new(self.cols.obj_actor.iter(), self.cols.obj_ctr.iter()),
            key: KeyIter::new(
                self.cols.key_str.iter(),
                self.cols.key_actor.iter(),
                self.cols.key_ctr.iter(),
            ),
            succ: SuccIterIter::new(
                self.cols.succ_count.iter(),
                self.cols.succ_actor.iter(),
                self.cols.succ_ctr.iter(),
                self.cols.index.inc.iter(),
            ),
            insert: InsertIter::new(self.cols.insert.values().iter()),
            action: ActionIter::new(self.cols.action.iter()),
            value: ValueIter::new(self.cols.value_meta.iter(), self.cols.value.iter()),
            marks: MarkInfoIter::new(self.cols.mark_name.iter(), self.cols.expand.iter()),
            range: 0..self.len(),
            op_set: self,
        }
    }

    // iter ops

    // better error handling
    // export bytes
    // insert op
    // seek nth (read)
    // seek nth (insert)
    // seek prop
    // seek opid
    // seek mark

    // split slabs at some point

    // slab in-place edits
    // slab index vec<cursor>

    // ugly api stuff
    //
    // * boolean packable has unused pack/unpack - maybe we want two traits
    //    one for Rle<> and one for Cursor<> that overlap?
    // * columns that don't handle nulls still take Option<Item> and the
    //    iterator still returns Option<item> - could be nice to more cleanly
    //    handle columns that can't take nulls - currently hide this with
    //    MaybePackable allowing you to pass in Item or Option<Item> to splice
    // * maybe do something with types to make scan required to get
    //    validated bytes

    pub(crate) fn insert_actor(&mut self, idx: usize, actor: ActorId) {
        if self.actors.len() != idx {
            self.rewrite_with_new_actor(idx)
        }
        self.actors.insert(idx, actor)
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        self.cols.rewrite_with_new_actor(idx);
        self.cols.index.mark.rewrite_with_new_actor(idx);
        self.obj_info = ObjIndex(
            self.obj_info
                .0
                .iter()
                .map(|(id, make)| (id.with_new_actor(idx), make.with_new_actor(idx)))
                .collect(),
        );
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        self.actors.remove(idx);
        self.cols.rewrite_without_actor(idx);
        self.obj_info = ObjIndex(
            self.obj_info
                .0
                .iter()
                .filter_map(|(id, make)| Some((id.without_actor(idx)?, make.without_actor(idx)?)))
                .collect(),
        );
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Parent {
    pub(crate) obj: ObjId,
    pub(crate) typ: ObjType,
    pub(crate) prop: Prop,
    pub(crate) visible: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryNth {
    pub(crate) marks: Option<Arc<MarkSet>>,
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) elemid: ElemId,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FoundOpId<'a> {
    pub(crate) op: Op<'a>,
    pub(crate) index: usize,
    pub(crate) visible: bool,
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct OpsFound<'a> {
    pub(crate) index: usize,
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) end_pos: usize,
    pub(crate) range: Range<usize>,
}

impl OpsFound<'_> {
    fn width(&self, seq_type: SequenceType, text_encoding: TextEncoding) -> usize {
        self.ops
            .last()
            .map(|o| o.width(seq_type, text_encoding))
            .unwrap_or(0)
    }

    /// Determine what action to take based on the found operations
    ///
    /// The action provided by the user may actually not be needed, or it may
    /// not result in visible changes to the document. This method determines
    /// what the `ResolvedAction` representing these cases should be and also
    /// updates the `OpsFound::ops` where necessary.
    ///
    /// # Returns
    ///
    /// `Some(ResolvedAction)` if there is an op which needs to be inserted into
    /// the opset, or `None` otherwise
    pub(crate) fn resolve_action(
        &mut self,
        original_action: types::OpType,
    ) -> Option<ResolvedAction> {
        if let Some(op) = self.ops.last() {
            if let types::OpType::Put(v) = &original_action {
                if op.action == Action::Set && &op.value == v {
                    if self.ops.len() == 1 {
                        // There's one operation with the same value as the incoming action,
                        // we don't need to do anything at all
                        return None;
                    } else {
                        // We want to emit a delete op for all the ops which did not "win", i.e.
                        // every op apart from the first one in the found ops - which is first
                        // because it is ordered by lamport timestamp and thus is the winner.
                        // Therefore, pop the winning op off the stack and resolve the action
                        // to a delete for the remaining ops
                        self.ops.pop();
                        return Some(ResolvedAction::ConflictResolution(types::OpType::Delete));
                    }
                }
            }
        } else if original_action == types::OpType::Delete {
            // If the original action is a delete and there are no existing ops we don't need to do anything
            return None;
        }
        Some(ResolvedAction::VisibleUpdate(original_action))
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        self.ops.last().and_then(|o| o.cursor().ok())
    }
}

/// The "resolved" action of an operation returned by the `OpsFound::resolve_action` method.
///
/// This enum is necessary to distinguish between two kinds of action we need to take:
///
/// * Actions which have a visible effect on the document, such as inserting new values
/// * Actions which just resolve conflicts, without changing the document state
///
/// It's useful to distinguish these so that we can tell whether we need to generate
/// patches for the operation or not.
pub(crate) enum ResolvedAction {
    // An operation which resolves a conflict but does not change the observed state
    // I.e. it is invisible to the materialized view
    ConflictResolution(types::OpType),
    // A normal operation which is visible in the document
    VisibleUpdate(types::OpType),
}

impl ResolvedAction {
    pub(crate) fn is_increment(&self) -> bool {
        let action = match self {
            ResolvedAction::ConflictResolution(action) => action,
            ResolvedAction::VisibleUpdate(action) => action,
        };
        matches!(action, types::OpType::Increment { .. })
    }
}

pub(crate) struct IterObjIds<'a> {
    v1_ctr: hexane::v1::Iter<'a, Option<u32>>,
    v1_actor: hexane::v1::Iter<'a, Option<ActorIdx>>,
    next_ctr: Option<hexane::v1::Run<Option<u32>>>,
    next_actor: Option<hexane::v1::Run<Option<ActorIdx>>>,
    pos: usize,
}

impl Iterator for IterObjIds<'_> {
    type Item = (ObjId, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.pos;
        match (self.next_ctr, self.next_actor) {
            (Some(mut run1), Some(mut run2)) => {
                match run1.count.cmp(&run2.count) {
                    Ordering::Less => {
                        run2.count -= run1.count;
                        self.next_actor = Some(run2);
                        self.pos += run1.count;
                        self.next_ctr = self.v1_ctr.next_run();
                    }
                    Ordering::Greater => {
                        run1.count -= run2.count;
                        self.next_ctr = Some(run1);
                        self.pos += run2.count;
                        self.next_actor = self.v1_actor.next_run();
                    }
                    Ordering::Equal => {
                        self.pos += run1.count;
                        self.next_ctr = self.v1_ctr.next_run();
                        self.next_actor = self.v1_actor.next_run();
                    }
                }
                let end = self.pos;
                let obj = ObjId::load(run1.value.map(|c| c as u64), run2.value)?;
                Some((obj, start..end))
            }
            (None, None) => None,
            _ => panic!(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct SuccUndo {
    pub(crate) succ: SuccInsert,
    pub(crate) visible: Option<bool>,
    pub(crate) text: Option<Option<u32>>,
    pub(crate) top: Option<bool>,
}

impl SuccUndo {
    fn new(
        succ: SuccInsert,
        visible: Option<bool>,
        text: Option<Option<u32>>,
        top: Option<bool>,
    ) -> Self {
        Self {
            succ,
            visible,
            text,
            top,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        op_set2::{
            op::SuccCursors,
            types::{Action, ScalarValue},
            KeyRef,
        },
        storage::Document,
        transaction::Transactable,
        types::{ObjId, OpId},
        ActorId, AutoCommit, ObjType,
    };

    use super::OpSet;

    use rand::distr::Alphanumeric;
    use rand::RngExt;

    #[test]
    fn suspend_resume_op_set_iter() {
        // most likely place for errors would be
        // in the values column (raw reader) and succ column
        // make sure to have a mix of small and large values
        // and a mix of succ column values with delets and counters

        let mut doc = AutoCommit::new();
        let rand_text: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(1000)
            .map(char::from)
            .collect();

        doc.put(crate::ROOT, "aaa_int", 123).unwrap();
        doc.put(crate::ROOT, "mid_int", 123).unwrap();
        doc.put(crate::ROOT, "zzz_int", 123).unwrap();
        doc.put(crate::ROOT, "aaa_str", "abc").unwrap();
        doc.put(crate::ROOT, "mid_str", "abc").unwrap();
        doc.put(crate::ROOT, "zzz_str", "abc").unwrap();

        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, &rand_text).unwrap();
        let _ = doc.get_heads(); // force a new change
        doc.splice_text(&text, 100, 100, "").unwrap();
        let _ = doc.get_heads(); // force a new change

        doc.put(crate::ROOT, "a_large", &rand_text).unwrap();
        doc.put(crate::ROOT, "z_large", &rand_text).unwrap();

        doc.put(crate::ROOT, "a_large", ScalarValue::Counter(100))
            .unwrap();
        doc.put(crate::ROOT, "z_large", ScalarValue::Counter(200))
            .unwrap();
        for _ in 0..1000 {
            doc.increment(crate::ROOT, "a_large", 1).unwrap();
            doc.increment(crate::ROOT, "z_large", 1).unwrap();
        }

        let _ = doc.get_heads(); // force a new change

        let iter1 = doc.doc.ops().iter();
        let mut iter2 = doc.doc.ops().iter();

        for op1 in iter1 {
            let op2 = iter2.next().unwrap();
            assert_eq!(op1, op2);
            let suspend = iter2.suspend();
            iter2 = suspend.try_resume(doc.doc.ops()).unwrap();
        }
    }

    #[test]
    fn column_data_basic_iteration() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "hello").unwrap();
        doc.put(crate::ROOT, "key", "value").unwrap();
        doc.put(crate::ROOT, "key2", "value2").unwrap();
        doc.delete(crate::ROOT, "key2").unwrap();
        let saved = doc.save();
        let doc_chunk = load_document_chunk(&saved);
        let opset = super::OpSet::load(&doc_chunk, TextEncoding::platform_default()).unwrap();
        let ops = opset.iter().collect::<Vec<_>>();
        let actual_ops = doc.doc.ops().iter().collect::<Vec<_>>();
        if ops != actual_ops {
            for (i, (a, b)) in actual_ops.iter().zip(ops.iter()).enumerate() {
                if b != a {
                    println!("op {} mismatch", i);
                    println!("expected: {:?}", a);
                    println!("actual: {:?}", b);
                }
            }
        }
        assert_eq!(ops, actual_ops);
    }

    fn load_document_chunk(data: &[u8]) -> Document<'_> {
        let input = crate::storage::parse::Input::new(data);
        let (_i, chunk) = crate::storage::Chunk::parse(input).unwrap();
        let crate::storage::Chunk::Document(doc) = chunk else {
            panic!("expected document chunk");
        };
        doc
    }

    #[derive(Debug, Clone)]
    struct TestOp {
        id: OpId,
        obj: ObjId,
        action: Action,
        value: ScalarValue<'static>,
        key: KeyRef<'static>,
        insert: bool,
        succs: Vec<OpId>,
        expand: bool,
        mark_name: Option<&'static str>,
    }

    impl<'a> PartialEq<super::super::op::Op<'a>> for TestOp {
        fn eq(&self, other: &super::super::op::Op<'a>) -> bool {
            let other_succ = other.succ().collect::<Vec<_>>();
            self.id == other.id
                && self.obj == other.obj
                && self.action == other.action
                && self.value == other.value
                && self.key == other.key
                && self.insert == other.insert
                && self.succs == other_succ
                && self.expand == other.expand
                && self.mark_name == other.mark_name
        }
    }

    fn with_test_ops<F>(actors: Vec<ActorId>, test_ops: &[TestOp], f: F)
    where
        F: FnOnce(super::OpSet),
    {
        let mut ops = Vec::new();

        let group_counts: Vec<u32> = test_ops.iter().map(|o| o.succs.len() as u32).collect();
        let succ_actors: Vec<ActorIdx> = test_ops
            .iter()
            .flat_map(|o| o.succs.iter().map(|s| s.actoridx()))
            .collect();
        let succ_counters: Vec<u32> = test_ops
            .iter()
            .flat_map(|o| o.succs.iter().map(|s| s.counter() as u32))
            .collect();
        let group_col = hexane::v1::PrefixColumn::<u32>::from_values(group_counts);
        let actor_col = hexane::v1::Column::<ActorIdx>::from_values(succ_actors);
        let counter_col = hexane::v1::DeltaColumn::<u32>::from_values(succ_counters);
        let inc_col = hexane::v1::Column::<Option<i64>>::fill(actor_col.len(), None);

        let mut group_iter = group_col.iter();
        let mut actor_iter = actor_col.iter();
        let mut counter_iter = counter_col.iter();
        let mut inc_values = inc_col.iter();

        // first encode the succs
        for test_op in test_ops {
            let (_prefix, group_count) = group_iter.next().unwrap();
            let op = super::super::op::Op {
                pos: 0,
                id: test_op.id,
                obj: test_op.obj,
                action: test_op.action,
                value: test_op.value.clone(),
                key: test_op.key.clone(),
                insert: test_op.insert,
                expand: test_op.expand,
                mark_name: test_op.mark_name,
                conflict: false,
                succ_cursors: SuccCursors {
                    len: group_count as usize,
                    succ_counter: counter_iter.clone(),
                    succ_actor: actor_iter.clone(),
                    inc_values: inc_values.clone(),
                },
            };
            for _ in 0..group_count {
                counter_iter.next();
                actor_iter.next();
                inc_values.next();
            }
            ops.push(op);
        }
        let op_set = OpSet::from_doc_ops(actors, ops.iter().cloned());
        f(op_set);
    }

    #[test]
    fn column_data_iter_range() {
        let actors = vec![crate::ActorId::random(), crate::ActorId::random()];

        let ops = vec![
            TestOp {
                id: OpId::new(1, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: KeyRef::Map("key".into()),
                insert: false,
                succs: vec![OpId::new(5, 1), OpId::new(6, 1), OpId::new(10, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::str("value1"),
                key: KeyRef::Map("key1".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(3, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::str("value2"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![OpId::new(6, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("inner_value1"),
                key: KeyRef::Map("inner_key1".into()),
                insert: false,
                succs: vec![OpId::new(7, 1), OpId::new(8, 2), OpId::new(9, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(5, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("inner_value2"),
                key: KeyRef::Map("inner_key2".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &ops, |opset| {
            let range = opset.scope_to_obj(&ObjId(OpId::new(1, 1)));
            let mut iter = opset.iter_range(&range);
            println!(
                "ITER :: range={:?} pos={} max={}",
                range,
                iter.pos(),
                iter.end_pos()
            );
            for o in &ops {
                println!("OP={:?}", o);
            }
            let op = iter.next().unwrap();
            assert_eq!(ops[3], op);
            let op = iter.next().unwrap();
            assert_eq!(ops[4], op);
            let op = iter.next();
            assert!(op.is_none());
        });
    }

    #[test]
    fn column_data_op_iterators() {
        let actors = vec![crate::ActorId::random(), crate::ActorId::random()];

        let test_ops = vec![
            TestOp {
                id: OpId::new(1, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: KeyRef::Map("map".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: KeyRef::Map("list".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(3, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value1"),
                key: KeyRef::Map("key1".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2a"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2b"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![OpId::new(5, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(5, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2c"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(6, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value3a"),
                key: KeyRef::Map("key3".into()),
                insert: false,
                succs: vec![OpId::new(7, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(7, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value3b"),
                key: KeyRef::Map("key3".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(8, 1),
                obj: ObjId(OpId::new(2, 1)),
                action: Action::Set,
                value: ScalarValue::str("a"),
                key: KeyRef::Seq(ElemId::head()),
                insert: true,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(9, 1),
                obj: ObjId(OpId::new(2, 1)),
                action: Action::Set,
                value: ScalarValue::str("b"),
                key: KeyRef::Seq(ElemId(OpId::new(8, 1))),
                insert: true,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &test_ops, |opset| {
            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[2..8], ops.as_slice());

            let range = opset.prop_range(&ObjId(OpId::new(1, 1)), "key2");
            let iter = opset.iter_range(&range);
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[3..6], ops.as_slice());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.top_ops().collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter
                .key_ops()
                .map(|n| n.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let key1 = ops.first().unwrap().as_slice();
            let key2 = ops.get(1).unwrap().as_slice();
            let key3 = ops.get(2).unwrap().as_slice();
            let key4 = ops.get(3);
            assert_eq!(&test_ops[2..3], key1);
            assert_eq!(&test_ops[3..6], key2);
            assert_eq!(&test_ops[6..8], key3);
            assert!(key4.is_none());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter
                .visible_slow(None)
                .key_ops()
                .map(|n| n.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let key1 = ops.first().unwrap().as_slice();
            let key2 = ops.get(1).unwrap().as_slice();
            let key3 = ops.get(2).unwrap().as_slice();
            let key4 = ops.get(3);
            let key2test = vec![test_ops[3].clone(), test_ops[5].clone()];
            assert_eq!(&test_ops[2..3], key1);
            assert_eq!(&key2test, key2);
            assert_eq!(&test_ops[7..8], key3);
            assert!(key4.is_none());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.visible_slow(None).top_ops().collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());
        });
    }
}
