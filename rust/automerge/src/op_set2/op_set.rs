use super::parents::Parents;
use crate::exid::ExId;
use crate::marks::{MarkSet, MarkStateMachine, RichTextQueryState};
use crate::patches::TextRepresentation;
use crate::storage::{columns::compression::Uncompressed, ColumnSpec, Document, RawColumns};
use crate::types;
use crate::types::{
    ActorId, Clock, ElemId, Export, Exportable, ListEncoding, ObjId, ObjMeta, ObjType, OpId, Prop,
    TextEncoding,
};
use crate::AutomergeError;
use crate::{Automerge, PatchLog};

use super::op::{Op, OpBuilder, OpLike, SuccInsert, TxOp};
use super::packer::{ColumnDataIter, PackError, Run, UIntCursor};

use super::columns::Columns;

use super::types::{Action, ActorCursor, ActorIdx, KeyRef, MarkData, OpType, ScalarValue};

use std::borrow::Cow;
use std::cmp::Ordering;
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

pub(crate) use crate::iter::{Keys, ListRange, MapRange};

pub(crate) use found_op::OpsFoundIter;
pub(crate) use insert::InsertQuery;
pub(crate) use mark_index::{MarkIndexColumn, MarkIndexValue};
pub(crate) use marks::{MarkIter, NoMarkIter};
pub(crate) use op_iter::{
    ActionIter, InsertIter, KeyIter, MarkInfoIter, ObjIdIter, OpIdIter, OpIter, ReadOpError,
    SuccIterIter, ValueIter,
};
pub(crate) use op_query::{OpQuery, OpQueryTerm};
pub(crate) use top_op::TopOpIter;
pub(crate) use visible::{DiffOp, DiffOpIter, VisibleOpIter};

#[derive(Debug, Default, Clone)]
pub(crate) struct OpSet {
    //len: usize,
    pub(crate) actors: Vec<ActorId>,
    //text_index: ColumnData<UIntCursor>,
    //visible_index: ColumnData<BooleanCursor>,
    //inc_index: ColumnData<IntCursor>,
    //mark_index: MarkIndexColumn,
    pub(crate) obj_info: ObjIndex,
    cols: Columns,
    pub(crate) text_encoding: TextEncoding,
}

#[derive(Debug, Clone)]
pub(crate) struct OpSetCheckpoint(OpSet);

impl OpSet {
    #[cfg(test)]
    pub(crate) fn debug_cmp(&self, other: &Self) {
        self.cols.debug_cmp(&other.cols)
    }

    pub(crate) fn save_checkpoint(&self) -> OpSetCheckpoint {
        OpSetCheckpoint(self.clone())
    }

    pub(crate) fn load_checkpoint(&mut self, mut checkpoint: OpSetCheckpoint) {
        std::mem::swap(&mut checkpoint.0, self);
    }

    #[cfg(test)]
    pub(crate) fn from_actors(actors: Vec<ActorId>) -> Self {
        OpSet {
            //len: 0,
            actors,
            cols: Columns::default(),
            //text_index: ColumnData::new(),
            //visible_index: ColumnData::new(),
            //inc_index: ColumnData::new(),
            //mark_index: MarkIndexColumn::new(),
            obj_info: ObjIndex::default(),
            text_encoding: TextEncoding::default(),
        }
    }

    pub(crate) fn dump(&self) {
        log!("OpSet");
        log!("  len: {}", self.len());
        log!("  actors: {:?}", self.actors);
        self.cols.dump();
    }

    pub(crate) fn parents(
        &self,
        obj: ObjId,
        text_rep: TextRepresentation,
        clock: Option<Clock>,
    ) -> Parents<'_> {
        Parents {
            obj,
            ops: self,
            text_rep,
            clock,
        }
    }

    pub(crate) fn index_builder(&self) -> IndexBuilder {
        IndexBuilder::new(self, self.text_encoding)
    }

    pub(crate) fn set_indexes(&mut self, builder: IndexBuilder) {
        let indexes = builder.finish();

        assert_eq!(indexes.text.len(), self.len());
        assert_eq!(indexes.mark.len(), self.len());
        assert_eq!(indexes.visible.len(), self.len());
        assert_eq!(indexes.inc.len(), self.cols.sub_len());

        self.cols.index.text = indexes.text;
        self.cols.index.visible = indexes.visible;
        self.cols.index.inc = indexes.inc;
        self.cols.index.mark = indexes.mark;
        self.obj_info = indexes.obj_info;
    }

    #[inline(never)]
    pub(crate) fn splice_objects<O: OpLike>(&mut self, ops: &[O]) {
        for op in ops {
            if let Some(obj_info) = op.obj_info() {
                self.obj_info.insert(op.id(), obj_info);
            }
        }
    }

    #[inline(never)]
    pub(crate) fn splice<O: OpLike>(&mut self, pos: usize, ops: &[O]) {
        self.cols.splice(pos, ops, self.text_encoding);
        self.splice_objects(ops);
        //self.len += ops.len();
    }

    #[inline(never)]
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
            self.cols.succ_count.splice(i.pos, 1, [i.len + succ_inc]);
            self.cols.succ_actor.splice(i.sub_pos, 0, [i.id.actoridx()]);
            self.cols.succ_ctr.splice(i.sub_pos, 0, [i.id.icounter()]);
            self.cols.index.inc.splice(i.sub_pos, 0, [i.inc]);
            self.cols.index.text.splice(i.pos, 1, [0]);
            if i.inc.is_none() {
                self.cols.index.visible.splice(i.pos, 1, [false]);
            }
        }
    }

    pub(crate) fn parent_object(
        &self,
        child: &ObjId,
        text_rep: TextRepresentation,
        clock: Option<&Clock>,
    ) -> Option<Parent> {
        let (op, visible) = self.find_op_by_id_and_vis(child.id()?, clock)?;
        let obj = op.obj;
        let typ = self.object_type(&obj)?;
        let prop = match op.key {
            KeyRef::Map(k) => Prop::Map(k.to_string()),
            KeyRef::Seq(_) => {
                let index = self
                    .seek_list_opid(&op.obj, op.id, text_rep.encoding(typ), clock)?
                    .index;
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
        let iter = self.iter_obj(obj).visible(clock).top_ops();
        Keys::new(self, iter)
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: &ObjId,
        range: R,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> ListRange<'_, R> {
        let iter = self.iter_obj(obj).visible(clock).top_ops().marks();
        ListRange::new(self, iter, range, encoding)
    }

    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'_, R> {
        let iter = self.iter_obj(obj).visible(clock).top_ops();
        MapRange::new(self, iter, range)
    }

    pub(crate) fn len(&self) -> usize {
        //self.len
        self.cols.len()
    }

    pub(crate) fn sub_len(&self) -> usize {
        self.cols.sub_len()
    }

    pub(crate) fn seq_length(
        &self,
        obj: &ObjId,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> usize {
        self.top_ops(obj, clock).map(|op| op.width(encoding)).sum()
    }

    #[inline(never)]
    pub(crate) fn query_insert_at_text(
        &self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<QueryNth> {
        if encoding == ListEncoding::List || clock.is_some() {
            return None;
        }

        let range = self.scope_to_obj(obj);

        if index == 0 {
            return None;
        }
        let mut iter = self.cols.index.text.iter_range(range.clone()).with_acc();
        let tx = iter.nth(index - 1)?;
        let iter = self.iter_range(&(tx.pos..range.end));
        let marks = self.get_rich_text_at(tx.pos, clock);
        let mut query = InsertQuery::new(iter, index, encoding, clock.cloned(), marks);
        query.resolve(index - 1).ok()
    }

    #[inline(never)]
    pub(crate) fn query_insert_at(
        &self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> Result<QueryNth, AutomergeError> {
        if let Some(query) = self.query_insert_at_text(obj, index, encoding, clock.as_ref()) {
            debug_assert_eq!(
                Ok(&query),
                InsertQuery::new(
                    self.iter_obj(obj),
                    index,
                    encoding,
                    clock,
                    Default::default()
                )
                .resolve(0)
                .as_ref()
            );
            Ok(query)
        } else {
            InsertQuery::new(
                self.iter_obj(obj),
                index,
                encoding,
                clock,
                Default::default(),
            )
            .resolve(0)
        }
    }

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        obj: &ObjId,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        match prop {
            Prop::Map(key_name) => self.seek_ops_by_map_key(obj, &key_name, clock),
            Prop::Seq(index) => self.seek_ops_by_index(obj, index, encoding, clock),
        }
    }

    #[inline(never)]
    pub(crate) fn seek_ops_by_map_key<'a>(
        &'a self,
        obj: &ObjId,
        key: &str,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        let iter = self.iter_prop(obj, key);
        let end_pos = iter.end_pos();
        let ops = iter.visible2(self, clock).collect::<Vec<_>>();
        OpsFound {
            index: 0,
            ops,
            end_pos,
        }
    }

    #[inline(never)]
    pub(crate) fn seek_ops_by_index<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        if let Some(found) = self.seek_ops_by_index_fast(obj, index, encoding, clock) {
            debug_assert_eq!(
                found,
                self.seek_ops_by_index_slow(obj, index, encoding, clock)
            );
            found
        } else {
            self.seek_ops_by_index_slow(obj, index, encoding, clock)
        }
    }

    #[inline(never)]
    pub(crate) fn seek_ops_by_index_slow<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        let sub_iter = self.iter_obj(obj);
        let mut end_pos = sub_iter.pos();
        let iter = OpsFoundIter::new(sub_iter.no_marks(), clock.cloned());
        let mut len = 0;
        for mut ops in iter {
            let width = ops.width(encoding);
            if len + width > index {
                ops.index = len;
                return ops;
            }
            len += width;
            end_pos = ops.end_pos;
        }
        OpsFound {
            index,
            ops: vec![],
            end_pos,
        }
    }

    fn get_value(&self, pos: usize) -> Option<ScalarValue<'_>> {
        let meta = self.cols.value_meta.get_with_acc(pos)?;
        let length = meta.item.as_ref()?.length();
        let raw = if length > 0 {
            self.cols
                .value
                .raw_reader(meta.acc.as_usize())
                .read_next(length)
                .ok()?
        } else {
            &[]
        };
        ScalarValue::from_raw(*meta.item?, raw).ok()
    }

    fn get_mark_name(&self, pos: usize) -> Option<Cow<'_, str>> {
        self.cols.mark_name.get(pos).flatten()
    }

    #[inline(never)]
    fn get_rich_text_at(&self, pos: usize, clock: Option<&Clock>) -> RichTextQueryState<'_> {
        let mut marks = RichTextQueryState::default();
        for id in self.cols.index.mark.marks_at(pos, clock) {
            let pos = self.get_op_id_pos(id).unwrap();
            let name = self.get_mark_name(pos).unwrap();
            let value = self.get_value(pos).unwrap();
            marks.map.insert(id, MarkData { name, value });
        }
        marks
    }

    fn get_marks_at(&self, pos: usize, clock: Option<&Clock>) -> MarkStateMachine<'_> {
        let mut marks = MarkStateMachine::default();
        for id in self.cols.index.mark.marks_at(pos, clock) {
            let pos = self.get_op_id_pos(id).unwrap();
            let name = self.get_mark_name(pos).unwrap();
            let value = self.get_value(pos).unwrap();
            marks.mark_begin(id, MarkData { name, value });
        }
        marks
    }

    #[inline(never)]
    pub(crate) fn seek_ops_by_index_fast<'a>(
        &'a self,
        obj: &ObjId,
        mut index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        if encoding == ListEncoding::List || clock.is_some() {
            return None;
        }

        let range = self.scope_to_obj(obj);

        let mut iter = self.cols.index.text.iter_range(range.clone()).with_acc();

        let mut ops = vec![];
        let mut end_pos = range.end;

        let obj_start = iter.acc();
        if let Some(tx) = iter.nth(index) {
            assert!(tx.acc >= obj_start);
            index = (tx.acc - obj_start).as_usize();
            for op in self.iter_range(&(tx.pos..range.end)) {
                if op.insert && !ops.is_empty() {
                    break;
                }
                end_pos = op.pos + 1;
                if op.succ().len() == 0 {
                    ops.push(op);
                }
            }
        }

        Some(OpsFound {
            index,
            ops,
            end_pos,
        })
    }

    #[inline(never)]
    fn get_op_id_pos(&self, id: OpId) -> Option<usize> {
        let counters = &self.cols.id_ctr;
        let actors = &self.cols.id_actor;
        counters
            .find_by_value(id.counter())
            .into_iter()
            .find(|&pos| actors.get(pos) == Some(Some(Cow::Owned(id.actoridx()))))
    }

    #[inline(never)]
    fn seek_list_op_fast(
        &self,
        obj: &ObjId,
        target: ElemId,
        id: OpId,
        insert: bool,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<SeekOpIdResult<'_>> {
        if encoding == ListEncoding::List || clock.is_some() || target.is_head() {
            return None;
        }

        let range = self.scope_to_obj(obj);

        let op_pos = self.get_op_id_pos(target.0).unwrap();

        let obj_acc = self.cols.index.text.get_acc(range.start).as_usize();
        let op_acc = self.cols.index.text.get_acc(op_pos).as_usize();

        let iter = self.iter_range(&(op_pos..range.end));
        let mut marks = self.get_marks_at(op_pos, clock);
        let mut pos = range.end;
        let mut current = 0;
        let mut index = op_acc - obj_acc;
        let mut ops = vec![];
        let mut found = false;
        if insert {
            for mut op in iter {
                if op.insert {
                    index += current;
                    current = 0;
                    if found && op.id < id {
                        pos = op.pos;
                        break;
                    }
                    if !found && ElemId(op.id) == target {
                        found = true;
                    }
                }

                let visible = op.scope_to_clock(clock);

                if visible {
                    marks.process(op.id(), op.action());
                    current = op.width(encoding);
                }
            }
            index += current;
        } else {
            for mut op in iter {
                if op.insert {
                    if found {
                        pos = op.pos;
                        break;
                    } else {
                        index += current;
                        current = 0;
                        if ElemId(op.id) == target {
                            found = true;
                        }
                    }
                } else if found && op.id > id {
                    pos = op.pos;
                }

                let visible = op.scope_to_clock(clock);

                if found {
                    ops.push((op, visible));
                }
                /*
                if visible && !found {
                  marks.process(op, clock);
                  current = op.width(encoding);
                }
                */
            }
        }

        Some(SeekOpIdResult {
            index,
            pos,
            ops,
            marks: marks.current().cloned(),
        })
    }

    fn seek_list_op(
        &self,
        obj: &ObjId,
        target: ElemId,
        id: OpId,
        insert: bool,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> SeekOpIdResult<'_> {
        if let Some(result) = self.seek_list_op_fast(obj, target, id, insert, encoding, clock) {
            debug_assert_eq!(
                result,
                self.seek_list_op_slow(obj, target, id, insert, encoding, clock)
            );
            result
        } else {
            self.seek_list_op_slow(obj, target, id, insert, encoding, clock)
        }
    }

    #[inline(never)]
    fn seek_list_op_slow(
        &self,
        obj: &ObjId,
        target: ElemId,
        id: OpId,
        insert: bool,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> SeekOpIdResult<'_> {
        let iter = self.iter_obj(obj);
        let mut pos = iter.end_pos();
        let mut ops = vec![];
        let mut found = target.is_head();
        let mut index = 0;
        let mut current = 0;
        let mut marks = MarkStateMachine::default();
        if insert {
            for mut op in iter {
                if op.insert {
                    index += current;
                    current = 0;
                    if found && op.id < id {
                        pos = op.pos;
                        break;
                    }
                    if !found && ElemId(op.id) == target {
                        found = true;
                    }
                }

                let visible = op.scope_to_clock(clock);

                if visible {
                    marks.process(op.id(), op.action());
                    current = op.width(encoding);
                }
            }
            index += current;
        } else {
            for mut op in iter {
                if op.insert {
                    if found {
                        pos = op.pos;
                        break;
                    } else {
                        index += current;
                        current = 0;
                        if ElemId(op.id) == target {
                            found = true;
                        }
                    }
                } else if found && op.id > id {
                    pos = op.pos;
                }

                let visible = op.scope_to_clock(clock);

                if visible && !found {
                    marks.process(op.id(), op.action());
                    current = op.width(encoding);
                }

                if found {
                    ops.push((op, visible));
                }
            }
        }

        SeekOpIdResult {
            index,
            pos,
            ops,
            marks: marks.current().cloned(),
        }
    }

    pub(crate) fn seek_list_opid(
        &self,
        obj: &ObjId,
        opid: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        // this iterates over the ops twice
        // needs faster rewrite
        let op = self.iter_obj(obj).find(|op| op.id == opid)?;
        let iter = OpsFoundIter::new(self.iter_obj(obj).no_marks(), clock.cloned());
        let mut index = 0;
        for ops in iter {
            if ops.end_pos > op.pos {
                let visible = ops.ops.contains(&op);
                return Some(FoundOpId { op, index, visible });
            }
            index += ops.width(encoding);
        }
        None
    }

    pub(crate) fn text(&self, obj: &ObjId, clock: Option<Clock>) -> String {
        self.iter_obj(obj)
            .no_marks()
            .visible(clock)
            .top_ops()
            .map(|op| op.as_str_cow())
            .collect()
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.counter(), self.actors[id.actor()].clone(), id.actor())
        }
    }

    fn iter_obj_ids(&self) -> IterObjIds<'_> {
        let mut ctr = self.cols.obj_ctr.iter();
        let mut actor = self.cols.obj_actor.iter();
        let next_ctr = ctr.next_run();
        let next_actor = actor.next_run();
        let pos = 0;

        IterObjIds {
            ctr,
            actor,
            next_ctr,
            next_actor,
            pos,
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
        self.iter_obj(obj).visible(clock).top_ops()
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
        let start = self.get_op_id_pos(*id)?;
        let mut iter = self.iter_range(&(start..self.len()));
        while let Some(mut o1) = iter.next() {
            if &o1.id == id {
                let mut vis = o1.scope_to_clock(clock);
                for mut o2 in iter {
                    if o2.obj != o1.obj || o1.elemid_or_key() != o2.elemid_or_key() {
                        break;
                    }
                    if o2.scope_to_clock(clock) {
                        vis = false;
                    }
                }
                return Some((o1, vis));
            }
        }
        None
    }

    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        self.obj_info.object_type(obj)
    }

    pub(crate) fn find_op_with_patch_log<'a>(
        &'a self,
        new_op: &OpBuilder<'_>,
        encoding: ListEncoding,
    ) -> FoundOpWithPatchLog<'a> {
        match &new_op.key {
            KeyRef::Seq(e) => {
                let r =
                    self.seek_list_op(&new_op.obj, *e, new_op.id, new_op.insert, encoding, None);
                self.found_op_with_patch_log(new_op, r.ops, r.pos, r.index, r.marks)
            }
            KeyRef::Map(s) => {
                let iter = self.iter_prop(&new_op.obj, s.as_ref());
                let mut pos = iter.end_pos();
                let mut ops = vec![];
                for mut o in iter {
                    let visible = o.scope_to_clock(None);
                    if o.id > new_op.id {
                        pos = o.pos;
                    }
                    ops.push((o, visible));
                }
                self.found_op_with_patch_log(new_op, ops, pos, 0, None)
            }
        }
    }

    pub(crate) fn found_op_with_patch_log<'a>(
        &'a self,
        new_op: &OpBuilder<'_>,
        ops: Vec<(Op<'a>, bool)>,
        end_pos: usize,
        index: usize,
        marks: Option<Arc<MarkSet>>,
    ) -> FoundOpWithPatchLog<'a> {
        let mut found = None;
        let mut before = None;
        let mut num_before = 0;
        let mut overwritten = None;
        let mut after = None;
        let mut succ = vec![];
        for (op, visible) in ops {
            if found.is_none() && op.id > new_op.id {
                found = Some(op.pos);
            }

            if new_op.pred.contains(&op.id) {
                succ.push(op.clone());

                if visible {
                    overwritten = Some(op);
                }
            } else if visible {
                if found.is_none() && overwritten.is_none() {
                    before = Some(op);
                    num_before += 1;
                } else if !op.is_inc() {
                    // increments are a special case where they can be visible
                    // but dont overwrite a value
                    after = Some(op);
                }
            }
        }

        let pos = found.unwrap_or(end_pos);

        FoundOpWithPatchLog {
            before,
            num_before,
            after,
            overwritten,
            succ,
            pos,
            index,
            marks,
        }
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

    #[inline(never)]
    pub(crate) fn from_doc(
        doc: &Document<'_>,
        text_encoding: TextEncoding,
    ) -> Result<Self, PackError> {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = doc.op_raw_bytes();
        let actors = doc.actors().to_vec();
        let op_set = Self::from_parts(doc.op_metadata.clone(), data, actors, text_encoding)?;
        Ok(op_set)
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
            text_encoding: TextEncoding::default(),
        }
    }

    fn from_parts(
        cols: RawColumns<Uncompressed>,
        data: &[u8],
        actors: Vec<ActorId>,
        text_encoding: TextEncoding,
    ) -> Result<Self, PackError> {
        let cols = Columns::load(cols.iter(), data, &actors)?;

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

    #[inline(never)]
    fn scope_to_obj(&self, obj: &ObjId) -> Range<usize> {
        let range = self.cols.obj_ctr.scope_to_value(obj.counter(), ..);
        self.cols.obj_actor.scope_to_value(obj.actor(), range)
    }

    pub(crate) fn iter_prop<'a>(&'a self, obj: &ObjId, prop: &str) -> OpIter<'a> {
        let range = self.scope_to_obj(obj);
        let range = self.cols.key_str.scope_to_value(Some(prop), range);
        self.iter_range(&range)
    }

    pub(crate) fn iter_obj<'a>(&'a self, obj: &ObjId) -> OpIter<'a> {
        let range = self.scope_to_obj(obj);
        self.iter_range(&range)
    }

    pub(crate) fn iter_range(&self, range: &Range<usize>) -> OpIter<'_> {
        let value_meta = self.cols.value_meta.iter_range(range.clone());
        let value_advance = value_meta.calculate_acc().as_usize();
        let value_raw = self.cols.value.raw_reader(value_advance);
        let value = ValueIter::new(value_meta, value_raw);

        let succ_count = self.cols.succ_count.iter_range(range.clone());
        let succ_range = succ_count.calculate_acc().as_usize()..usize::MAX;
        let succ_actor = self.cols.succ_actor.iter_range(succ_range.clone());
        let succ_counter = self.cols.succ_ctr.iter_range(succ_range.clone());
        let inc_values = self.cols.index.inc.iter_range(succ_range);
        let succ = SuccIterIter::new(succ_count, succ_actor, succ_counter, inc_values);

        OpIter {
            pos: range.start,
            id: OpIdIter::new(
                self.cols.id_actor.iter_range(range.clone()),
                self.cols.id_ctr.iter_range(range.clone()),
            ),
            obj: ObjIdIter::new(
                self.cols.obj_actor.iter_range(range.clone()),
                self.cols.obj_ctr.iter_range(range.clone()),
            ),
            key: KeyIter::new(
                self.cols.key_str.iter_range(range.clone()),
                self.cols.key_actor.iter_range(range.clone()),
                self.cols.key_ctr.iter_range(range.clone()),
            ),
            succ,
            insert: InsertIter::new(self.cols.insert.iter_range(range.clone())),
            action: ActionIter::new(self.cols.action.iter_range(range.clone())),
            value,
            marks: MarkInfoIter::new(
                self.cols.mark_name.iter_range(range.clone()),
                self.cols.expand.iter_range(range.clone()),
            ),
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
            insert: InsertIter::new(self.cols.insert.iter()),
            action: ActionIter::new(self.cols.action.iter()),
            value: ValueIter::new(self.cols.value_meta.iter(), self.cols.value.raw_reader(0)),
            marks: MarkInfoIter::new(self.cols.mark_name.iter(), self.cols.expand.iter()),
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

    pub(crate) fn decode(_spec: ColumnSpec, _data: &[u8]) {
        /*
                match spec.col_type() {
                    ColumnType::Actor => ActorCursor::decode(data),
                    ColumnType::String => StrCursor::decode(data),
                    ColumnType::Integer => UIntCursor::decode(data),
                    ColumnType::DeltaInteger => DeltaCursor::decode(data),
                    ColumnType::Boolean => BooleanCursor::decode(data),
                    ColumnType::Group => UIntCursor::decode(data),
                    ColumnType::ValueMetadata => MetaCursor::decode(data),
                    ColumnType::Value => log!("raw :: {:?}", data),
                }
        */
    }

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
    pub(crate) elemid: ElemId,
}

#[derive(Debug, Clone, PartialEq)]
struct SeekOpIdResult<'a> {
    index: usize,
    pos: usize,
    ops: Vec<(Op<'a>, bool)>,
    marks: Option<Arc<MarkSet>>,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithPatchLog<'a> {
    pub(crate) before: Option<Op<'a>>,
    pub(crate) num_before: usize,
    pub(crate) overwritten: Option<Op<'a>>,
    pub(crate) after: Option<Op<'a>>,
    pub(crate) succ: Vec<Op<'a>>,
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

impl<'a> FoundOpWithPatchLog<'a> {
    pub(crate) fn log_patches(
        &self,
        obj: &ObjMeta,
        op: &TxOp,
        pred: &[OpId],
        doc: &Automerge,
        patch_log: &mut PatchLog,
    ) {
        let end_pos = op.pos;
        let op = &op.bld;
        if op.insert {
            if op.is_mark() {
                //if let crate::types::OpType::MarkEnd(_) = op.op_type() {
                if op.is_mark_end() {
                    let encoding = patch_log.text_rep().encoding(obj.typ);
                    let mut index = 0;
                    let mut marks = MarkStateMachine::default();
                    let mut mark_name = None;
                    let mut value = ScalarValue::Null;
                    let mut start = None;
                    let target = op.id.prev();
                    //let end_pos = op.pos;
                    for op in doc.ops().iter_obj(&obj.id).visible(None).top_ops() {
                        if op.pos == end_pos {
                            break;
                        }
                        // if we find our first op
                        if op.id == target {
                            // grab its name and value
                            if let Some(ref mark) = op.mark_name {
                                mark_name = Some(mark.clone());
                                value = op.value.clone();
                                // and if it changes the mark state start recording
                                if marks.process(op.id, op.action()) {
                                    start = Some(index);
                                }
                            }
                        } else if let Some(ref mark) = mark_name {
                            // whenever the mark state changes
                            if marks.process(op.id, op.action()) {
                                match (marks.covered(target, mark), start) {
                                    (true, Some(s)) => {
                                        // the mark is either covered up (so we're done)
                                        let ms = MarkSet::new(mark.as_ref(), &value);
                                        patch_log.mark(obj.id, s, index - s, &ms);
                                        start = None;
                                    }
                                    (false, None) => {
                                        // or revealed - start recording
                                        start = Some(index);
                                    }
                                    _ => {}
                                }
                            }
                        } else {
                            marks.process(op.id, op.action());
                        }
                        index += op.width(encoding);
                    }
                    if let Some(s) = start {
                        if let Some(mark) = mark_name {
                            let ms = MarkSet::new(&mark, &value);
                            patch_log.mark(obj.id, s, index - s, &ms);
                        }
                    }
                }
            // TODO - move this into patch_log()
            } else if obj.typ == ObjType::Text && !op.is_block() {
                patch_log.splice(obj.id, self.index, op.as_str(), self.marks.clone());
            } else {
                patch_log.insert(
                    obj.id,
                    self.index,
                    op.hydrate_value(TextRepresentation::Array),
                    op.id,
                    false,
                );
            }
            return;
        }

        let key: Prop = match &op.key {
            KeyRef::Map(s) => Prop::from(s.as_ref()),
            KeyRef::Seq(_) => Prop::from(self.index),
        };

        if op.is_delete() {
            match (&self.before, &self.overwritten, &self.after) {
                (None, Some(over), None) => match key {
                    Prop::Map(k) => patch_log.delete_map(obj.id, &k),
                    Prop::Seq(index) => patch_log.delete_seq(
                        obj.id,
                        index,
                        over.width(patch_log.text_rep().encoding(obj.typ)),
                    ),
                },
                (Some(before), Some(_), None) => {
                    let conflict = self.num_before > 1;
                    patch_log.put(
                        obj.id,
                        &key,
                        before.hydrate_value(TextRepresentation::Array),
                        before.id,
                        conflict,
                        true,
                    );
                }
                _ => { /* do nothing */ }
            }
        } else if let Some(value) = op.get_increment_value() {
            if self.after.is_none() {
                if let Some(counter) = &self.overwritten {
                    if pred.contains(&counter.id()) {
                        patch_log.increment(obj.id, &key, value, op.id);
                    }
                }
            }
        } else {
            let conflict = self.before.is_some();
            if op.is_list_op()
                && self.overwritten.is_none()
                && self.before.is_none()
                && self.after.is_none()
            {
                patch_log.insert(
                    obj.id,
                    self.index,
                    op.hydrate_value(TextRepresentation::Array),
                    op.id,
                    conflict,
                );
            } else if self.after.is_some() {
                if self.before.is_none() {
                    patch_log.flag_conflict(obj.id, &key);
                }
            } else {
                patch_log.put(
                    obj.id,
                    &key,
                    op.hydrate_value(TextRepresentation::Array),
                    op.id,
                    conflict,
                    false,
                );
            }
        }
    }
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
}

impl<'a> OpsFound<'a> {
    fn width(&self, encoding: ListEncoding) -> usize {
        self.ops.last().map(|o| o.width(encoding)).unwrap_or(0)
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        self.ops.last().and_then(|o| o.cursor().ok())
    }
}

struct IterObjIds<'a> {
    ctr: ColumnDataIter<'a, UIntCursor>,
    actor: ColumnDataIter<'a, ActorCursor>,
    next_ctr: Option<Run<'a, u64>>,
    next_actor: Option<Run<'a, ActorIdx>>,
    pos: usize,
}

impl<'a> Iterator for IterObjIds<'a> {
    type Item = (ObjId, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.pos;
        match (self.next_ctr.clone(), self.next_actor.clone()) {
            (Some(mut run1), Some(mut run2)) => {
                match run1.count.cmp(&run2.count) {
                    Ordering::Less => {
                        run2.count -= run1.count;
                        self.next_actor = Some(run2.clone());
                        self.pos += run1.count;
                        self.next_ctr = self.ctr.next_run();
                    }
                    Ordering::Greater => {
                        run1.count -= run2.count;
                        self.next_ctr = Some(run1.clone());
                        self.pos += run2.count;
                        self.next_actor = self.actor.next_run();
                    }
                    Ordering::Equal => {
                        self.pos += run1.count;
                        self.next_ctr = self.ctr.next_run();
                        self.next_actor = self.actor.next_run();
                    }
                }
                let end = self.pos;
                let obj = ObjId::load(
                    run1.value.as_deref().copied(),
                    run2.value.as_deref().copied(),
                )?;
                Some((obj, start..end))
            }
            (None, None) => None,
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        op_set2::{
            op::SuccCursors,
            packer::{ColumnData, DeltaCursor, IntCursor},
            types::{Action, ActorCursor, ScalarValue},
            KeyRef,
        },
        storage::Document,
        transaction::Transactable,
        types::{ObjId, OpId},
        ActorId, AutoCommit, ObjType,
    };

    use super::OpSet;

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
        let opset = super::OpSet::from_doc(&doc_chunk, TextEncoding::default()).unwrap();
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
        mark_name: Option<Cow<'static, str>>,
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

        let mut group_data = ColumnData::<UIntCursor>::new();
        let mut succ_actor_data = ColumnData::<ActorCursor>::new();
        let mut succ_counter_data = ColumnData::<DeltaCursor>::new();
        group_data.splice(
            0,
            0,
            test_ops
                .iter()
                .map(|o| o.succs.len() as u64)
                .collect::<Vec<_>>(),
        );
        succ_actor_data.splice(
            0,
            0,
            test_ops
                .iter()
                .flat_map(|o| o.succs.iter().map(|s| s.actoridx()))
                .collect::<Vec<_>>(),
        );
        let inc_index = ColumnData::<IntCursor>::init_empty(succ_actor_data.len());
        succ_counter_data.splice(
            0,
            0,
            test_ops
                .iter()
                .flat_map(|o| o.succs.iter().map(|s| s.counter() as i64))
                .collect::<Vec<_>>(),
        );

        let mut group_iter = group_data.iter();
        let mut actor_iter = succ_actor_data.iter();
        let mut counter_iter = succ_counter_data.iter();
        let mut inc_values = inc_index.iter();

        // first encode the succs
        for test_op in test_ops {
            let group_count = group_iter.next().unwrap().unwrap();
            let op = super::super::op::Op {
                pos: 0, // not relevent for this equality test
                id: test_op.id,
                obj: test_op.obj,
                action: test_op.action,
                value: test_op.value.clone(),
                key: test_op.key.clone(),
                insert: test_op.insert,
                expand: test_op.expand,
                mark_name: test_op.mark_name.clone(),
                conflict: false,
                succ_cursors: SuccCursors {
                    len: *group_count as usize,
                    succ_counter: counter_iter.clone(),
                    succ_actor: actor_iter.clone(),
                    inc_values: inc_values.clone(),
                },
            };
            for _ in 0..*group_count {
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

            let iter = opset.iter_prop(&ObjId(OpId::new(1, 1)), "key2");
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
                .visible(None)
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
            let ops = iter.visible(None).top_ops().collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());
        });
    }

    /*
        proptest::proptest! {
            #[test]
            fn column_data_same_as_old_encoding(Scenario{opset, actors, keys} in arbitrary_opset()) {

                // encode with old encoders
                let actor_lookup = actors
                    .iter()
                    .enumerate()
                    //.map(|(a, b)| (a,usize::from(b)))
                    .map(|(a, _)| (a,a))
                    //.collect::<HashMap<_>>();
                    .collect();
                let objs_and_ops = opset
                    .iter_objs()
                    .flat_map(|(_, ops)| ops.map(move |op| op))
                    .collect::<Vec<_>>();
                let doc_ops = objs_and_ops
                    .iter()
                    .map(|op_idx| {
                        let op = op_idx.as_op(&opset.osd);
                        crate::storage::convert::op_as_docop(
                            &actor_lookup,
                            &keys,
                            op,
                        )
                    });
                let mut old_encoding = Vec::new();
                let ops_meta = crate::storage::document::DocOpColumns::encode(doc_ops, &mut old_encoding);

                // decode with new decoders
                let op_set = super::OpSet::from_parts(
                    ops_meta.raw_columns(),
                    Arc::new(old_encoding),
                    actors.clone()
                );

                let actual_ops = objs_and_ops.iter().map(|op_idx| op_idx.as_op(&opset.osd)).collect::<Vec<_>>();
                let ops = op_set.iter().collect::<Vec<_>>();
                if !(ops == actual_ops) {
                    for (i, (a, b)) in actual_ops.iter().zip(ops.iter()).enumerate() {
                        if b != a {
                            println!("first mismatch: {}", i);
                            println!("expected: {:?}", a);
                            println!("actual: {:?}", b);
                            println!("expected successors: {:?}", a.succ().map(|n| *n.id()).collect::<Vec<_>>());
                            println!("actual successors: {:?}", b.succ().collect::<Vec<_>>());
                            break;
                        }
                    }
                    panic!("ops mismatch");
                }
            }
        }
    */

    /*
        struct Scenario {
            opset: crate::op_set::OpSetInternal,
            actors: Vec<crate::ActorId>,
            keys: IndexedCache<String>,
        }
    */

    /*
        prop_compose! {
            fn arbitrary_opset()(
                actors in proptest::collection::vec(arbitrary_actor(), 1..10),
                keys in arbitrary_keys(),
            )(
                obj in arbitrary_objid(&actors),
                ops in proptest::collection::vec(arbitrary_op(&actors, &keys), 0..100),
                actors in Just(actors),
                keys in Just(keys)
            ) -> Scenario {
                let mut opset = crate::op_set::OpSetInternal::new();
                opset.osd.props = keys.clone();
                opset.osd.actors = actors.clone().into_iter().collect();
                for ArbOp{op, succs} in ops {
                    let op_idx = opset.load(obj, op);
                    opset.insert(0, &obj, op_idx);
                    for succ in succs {
                        let succ_idx = opset.load(obj, succ);
                        opset.osd.add_dep(op_idx, succ_idx);
                    }
                }
                Scenario{
                    opset,
                    actors,
                    keys
                }
            }
        }

        impl std::fmt::Debug for Scenario {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let ops_desc = self
                    .opset
                    .iter_objs()
                    .flat_map(|(_, ops)| {
                        ops.map(|op| {
                            let op = op.as_op(&self.opset.osd);
                            let succs = op.succ().map(|n| *n.id()).collect::<Vec<_>>();
                            format!("{:?}, succs: {:?}", op, succs)
                        })
                    })
                    .collect::<Vec<_>>();
                f.debug_struct("Scenario")
                    .field("ops", &ops_desc)
                    .field("actors", &self.actors)
                    .field("keys", &self.keys)
                    .finish()
            }
        }

    fn arbitrary_action() -> impl Strategy<Value = OpType> {
        prop_oneof![
            prop_oneof![Just(ObjType::Text), Just(ObjType::Map), Just(ObjType::List)]
                .prop_map(|t| OpType::Make(t)),
            Just(OpType::Delete),
            (-1000_i64..1000).prop_map(|i| OpType::Increment(i)),
            arbitrary_value().prop_map(|v| OpType::Put(v)),
            (any::<String>(), arbitrary_value(), any::<bool>()).prop_map(|(k, v, expand)| {
                OpType::MarkBegin(
                    expand,
                    crate::marks::OldMarkData {
                        name: k.into(),
                        value: v,
                    },
                )
            }),
        ]
    }

    fn arbitrary_value() -> impl Strategy<Value = crate::ScalarValue> {
        prop_oneof![
            Just(crate::ScalarValue::Null),
            any::<i64>().prop_map(crate::ScalarValue::Int),
            any::<u64>().prop_map(crate::ScalarValue::Uint),
            any::<i64>().prop_map(|c| crate::ScalarValue::Counter(crate::value::Counter::from(c))),
            any::<f64>().prop_map(crate::ScalarValue::F64),
            any::<i64>().prop_map(crate::ScalarValue::Timestamp),
            any::<bool>().prop_map(crate::ScalarValue::Boolean),
            any::<String>().prop_map(|s| crate::ScalarValue::Str(s.into())),
            proptest::collection::vec(any::<u8>(), 0..100)
                .prop_map(|v| crate::ScalarValue::Bytes(v.into())),
        ]
    }

        fn arbitrary_key(
            actors: &[crate::ActorId],
            keys: &crate::indexed_cache::IndexedCache<String>,
        ) -> impl Strategy<Value = crate::types::Key> {
            prop_oneof![
                (0..keys.len()).prop_map(|i| crate::types::Key::Map(i)),
                prop_oneof![
                    Just(crate::types::ElemId::head()),
                    arbitrary_opid(actors).prop_map(crate::types::ElemId)
                ]
                .prop_map(crate::types::Key::Seq)
            ]
        }

    fn arbitrary_opid(actors: &[crate::ActorId]) -> impl Strategy<Value = crate::types::OpId> {
        (0..actors.len()).prop_flat_map(move |actor_idx| {
            (1_u64..1000).prop_map(move |counter| crate::types::OpId::new(counter, actor_idx))
        })
    }

    fn arbitrary_actor() -> impl Strategy<Value = crate::ActorId> {
        proptest::collection::vec(any::<u8>(), 32).prop_map(|v| crate::ActorId::from(&v))
    }

        #[derive(Debug)]
        struct ArbOp {
            op: OpBuilder,
            succs: Vec<OpBuilder>,
        }
    */

    /*
        prop_compose! {
            fn arbitrary_op_builder(actors: &[crate::ActorId], keys: &crate::indexed_cache::IndexedCache<String>)
            (
                action in arbitrary_action(),
                key in arbitrary_key(&actors, &keys),
                id in arbitrary_opid(&actors),
                insert in any::<bool>(),
            )-> OpBuilder {
                OpBuilder {
                    id,
                    action,
                    key,
                    insert,
                }
            }
        }
    */

    /*
        prop_compose! {
            fn arbitrary_op(actors: &[crate::ActorId], keys: &IndexedCache<String>)
            (
                op in arbitrary_op_builder(&actors, &keys),
                succs in proptest::collection::vec(arbitrary_op_builder(&actors, &keys), 0..10),
            )-> ArbOp {
                ArbOp{
                    op,
                    succs,
                }
            }
        }

    fn arbitrary_objid(actors: &[crate::ActorId]) -> impl Strategy<Value = crate::types::ObjId> {
        prop_oneof![
            Just(crate::types::ObjId::root()),
            arbitrary_opid(actors).prop_map(crate::types::ObjId)
        ]
    }

    fn arbitrary_keys() -> impl Strategy<Value = IndexedCache<String>> {
        proptest::collection::vec(proptest::string::string_regex("[a-zA-Z]*").unwrap(), 1..10)
            .prop_map(|v| v.into_iter().collect())
    }
    */
}
