use super::parents::Parents;
use crate::cursor::Cursor;
use crate::exid::ExId;
use crate::marks::{MarkSet, MarkStateMachine};
use crate::patches::TextRepresentation;
use crate::storage::ColumnType;
use crate::storage::{
    columns::compression, columns::ColumnId, ColumnSpec, Document, RawColumn, RawColumns,
};
use crate::types;
use crate::types::{
    ActorId, Clock, ElemId, Export, Exportable, ListEncoding, ObjId, ObjMeta, ObjType, OpId, Prop,
};
use crate::AutomergeError;
use crate::{Automerge, PatchLog};

use super::columns::{ColumnCursor, ColumnData, ColumnDataIter, RawReader, Run};
use super::op::{ChangeOp, Op, OpBuilder2, SuccInsert};
use super::pack::PackError;
use super::rle::{ActionCursor, ActorCursor};
use super::types::{Action, ActorIdx, MarkData, OpType, ScalarValue};
use super::{
    BooleanCursor, Column, DeltaCursor, IntCursor, Key, KeyRef, MetaCursor, StrCursor, ValueMeta,
};

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

mod found_op;
mod insert;
mod keys;
mod list_range;
mod map_range;
mod marks;
mod op_iter;
mod op_query;
mod spans;
mod top_op;
mod values;
mod visible;

pub use keys::Keys;
pub use list_range::{ListRange, ListRangeItem};
pub use map_range::{MapRange, MapRangeItem};
pub use spans::{Span, Spans};
pub use values::Values;

pub(crate) use found_op::OpsFoundIter;
pub(crate) use insert::InsertQuery;
pub(crate) use keys::KeyIter;
pub(crate) use marks::{MarkIter, NoMarkIter};
pub(crate) use op_iter::{OpIter, ReadOpError};
pub(crate) use op_query::{OpQuery, OpQueryTerm};
pub(crate) use spans::{SpanInternal, SpansInternal};
pub(crate) use top_op::TopOpIter;
pub(crate) use visible::{DiffOp, DiffOpIter, VisibleOpIter};

#[derive(Debug, Default, Clone)]
pub(crate) struct OpSet {
    len: usize,
    pub(crate) actors: Vec<ActorId>,
    cols: Columns,
}

#[derive(Debug, Clone)]
pub(crate) struct OpSetCheckpoint(OpSet);

impl OpSet {
    pub(crate) fn save_checkpoint(&self) -> OpSetCheckpoint {
        OpSetCheckpoint(self.clone())
    }

    pub(crate) fn load_checkpoint(&mut self, mut checkpoint: OpSetCheckpoint) {
        std::mem::swap(&mut checkpoint.0, self);
    }

    #[cfg(test)]
    pub(crate) fn from_actors(actors: Vec<ActorId>) -> Self {
        OpSet {
            len: 0,
            actors,
            cols: Columns::default(),
        }
    }

    pub(crate) fn dump(&self) {
        log!("OpSet");
        log!("  len: {}", self.len);
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

    pub(crate) fn insert2<'a>(&'a mut self, op: &OpBuilder2) {
        self.cols.insert(op.pos, op);
        self.len += 1;
        self.validate()
        // do succ later
    }

    pub(crate) fn add_succ(&mut self, op_pos: &[SuccInsert], id: OpId) {
        for i in op_pos.iter().rev() {
            let succ_num = self.cols.get_group_mut(SUCC_COUNT_COL_SPEC);
            succ_num.splice(i.pos, 1, vec![i.len + 1]);

            let succ_actor = self.cols.get_actor_mut(SUCC_ACTOR_COL_SPEC);
            succ_actor.splice(i.sub_pos, 0, vec![id.actoridx()]);

            let succ_counter = self.cols.get_delta_mut(SUCC_COUNTER_COL_SPEC);
            succ_counter.splice(i.sub_pos, 0, vec![id.counter() as i64]);
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
        Keys::new(iter)
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: &ObjId,
        range: R,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> ListRange<'_, R> {
        let iter = self.iter_obj(obj).visible(clock).marks().top_ops();
        ListRange::new(iter, range, encoding)
    }

    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'_, R> {
        let iter = self.iter_obj(obj).visible(clock).top_ops();
        MapRange::new(iter, range)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn seq_length(
        &self,
        obj: &ObjId,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> usize {
        self.top_ops(obj, clock).map(|op| op.width(encoding)).sum()
    }

    pub(crate) fn query_insert_at(
        &self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> Result<QueryNth, AutomergeError> {
        InsertQuery::new(self.iter_obj(obj), index, encoding, clock).resolve()
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

    pub(crate) fn seek_ops_by_map_key<'a>(
        &'a self,
        obj: &ObjId,
        key: &str,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        let iter = self.iter_prop(obj, key);
        let end_pos = iter.end_pos();
        let ops = iter.visible(clock.cloned()).collect::<Vec<_>>();
        let ops_pos = ops.iter().map(|op| op.pos).collect::<Vec<_>>();
        OpsFound {
            index: 0,
            ops,
            ops_pos,
            end_pos,
        }
    }

    pub(crate) fn seek_ops_by_index<'a>(
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
            ops_pos: vec![],
            end_pos,
        }
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
        let mut iter = self.iter_obj(&obj);
        let mut pos = iter.end_pos();
        let mut ops = vec![];
        let mut found = target.is_head();
        let mut index = 0;
        let mut current = 0;
        let mut marks = MarkStateMachine::default();
        if insert {
            while let Some(mut op) = iter.next() {
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

                let visible = op.scope_to_clock(clock, iter.get_opiter());

                if visible {
                    marks.process(op.id(), op.action());
                    current = op.width(encoding);
                }
            }
            index += current;
        } else {
            while let Some(mut op) = iter.next() {
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
                    break;
                }

                let visible = op.scope_to_clock(clock, iter.get_opiter());

                if found {
                    ops.push((op, visible));
                }

                if visible && !found {
                    marks.process(op.id(), op.action());
                    current = op.width(encoding);
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
            .map(|op| op.as_str())
            .collect()
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.counter(), self.actors[id.actor()].clone(), id.actor())
        }
    }

    pub(crate) fn id_to_cursor(&self, id: OpId) -> Cursor {
        Cursor::new(id, self)
    }

    fn get_obj_ctr(&self) -> ColumnDataIter<'_, IntCursor> {
        self.cols.get_integer(OBJ_ID_COUNTER_COL_SPEC)
    }

    fn get_obj_actor(&self) -> ColumnDataIter<'_, ActorCursor> {
        self.cols.get_actor(OBJ_ID_ACTOR_COL_SPEC)
    }

    fn validate(&self) {
        let mut ctr = self.get_obj_ctr();
        let mut last = 0;
        while let Some(Run { value, .. }) = ctr.next_run() {
            let value = value.unwrap_or(0);
            assert!(last <= value);
            last = value;
        }
    }

    fn iter_obj_ids(&self) -> IterObjIds<'_> {
        let mut ctr = self.get_obj_ctr();
        let mut actor = self.get_obj_actor();
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

    pub(crate) fn find_op_by_id(&self, id: &OpId) -> Option<Op<'_>> {
        // FIXME - index goes here
        self.iter().find(|op| &op.id == id)
    }

    pub(crate) fn find_op_by_id_and_vis(
        &self,
        id: &OpId,
        clock: Option<&Clock>,
    ) -> Option<(Op<'_>, bool)> {
        let mut iter = self.iter();
        while let Some(mut o1) = iter.next() {
            if &o1.id == id {
                let mut vis = o1.scope_to_clock(clock, &iter);
                while let Some(mut o2) = iter.next() {
                    if o2.obj != o1.obj || o1.elemid_or_key() != o2.elemid_or_key() {
                        break;
                    }
                    if o2.scope_to_clock(clock, &iter) {
                        vis = false;
                    }
                }
                return Some((o1, vis));
            }
        }
        None
    }

    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        if obj.is_root() {
            Some(ObjType::Map)
        } else {
            self.find_op_by_id(&obj.0)
                .and_then(|op| op.action.try_into().ok())
        }
    }

    pub(crate) fn find_op_with_patch_log<'a>(
        &'a self,
        new_op: &ChangeOp,
        encoding: ListEncoding,
    ) -> FoundOpWithPatchLog<'a> {
        match &new_op.key {
            Key::Seq(e) => {
                let r =
                    self.seek_list_op(&new_op.obj, *e, new_op.id, new_op.insert, encoding, None);
                self.found_op_with_patch_log(new_op, &r.ops, r.pos, r.index, r.marks)
            }
            Key::Map(s) => {
                let mut iter = self.iter_prop(&new_op.obj, &s);
                let mut pos = iter.end_pos();
                let mut ops = vec![];
                while let Some(mut o) = iter.next() {
                    let visible = o.scope_to_clock(None, iter.get_opiter());
                    ops.push((o, visible));
                    if o.id > new_op.id {
                        pos = o.pos;
                        break;
                    }
                }
                self.found_op_with_patch_log(new_op, &ops, pos, 0, None)
            }
        }
    }

    pub(crate) fn found_op_with_patch_log<'a>(
        &'a self,
        new_op: &ChangeOp,
        ops: &[(Op<'a>, bool)],
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
        for i in 0..ops.len() {
            let (op, visible) = &ops[i];

            if found.is_none() && op.id > new_op.id {
                found = Some(op.pos);
            }

            if new_op.pred.contains(&op.id) {
                succ.push(*op);

                if *visible {
                    overwritten = Some(*op);
                }
            } else if *visible {
                if found.is_none() && overwritten.is_none() {
                    before = Some(*op);
                    num_before += 1;
                } else {
                    after = Some(*op);
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

    pub(crate) fn new(doc: &Document<'_>) -> Result<Self, PackError> {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = Arc::new(doc.op_raw_bytes().to_vec());
        let actors = doc.actors().to_vec();
        Self::from_parts(doc.op_metadata.raw_columns(), data, actors)
    }

    #[cfg(test)]
    pub(crate) fn from_doc_ops<
        'a,
        I: Iterator<Item = super::op::Op<'a>> + ExactSizeIterator + Clone,
    >(
        actors: Vec<ActorId>,
        ops: I,
    ) -> Self {
        let len = ops.len();
        let cols = Columns::new(ops);
        OpSet { actors, cols, len }
    }

    fn from_parts(
        cols: RawColumns<compression::Uncompressed>,
        data: Arc<Vec<u8>>,
        actors: Vec<ActorId>,
    ) -> Result<Self, PackError> {
        let mut cols = Columns(
            cols.iter()
                .map(|c| {
                    Ok((
                        c.spec(),
                        Column::external(c.spec(), data.clone(), c.data(), &actors)?,
                    ))
                })
                .collect::<Result<_, PackError>>()?,
        );

        cols.init_missing();

        let len = cols.len();

        let op_set = OpSet { actors, cols, len };

        Ok(op_set)
    }

    pub(crate) fn export(&self) -> (RawColumns<compression::Uncompressed>, Vec<u8>) {
        let mut data = vec![]; // should be able to do with_capacity here
        let mut raw = vec![];
        for (spec, c) in self.cols.iter() {
            if !c.is_empty() || (spec.id() == ColumnId::new(3) && self.len > 0) {
                let range = c.write(&mut data);
                if !range.is_empty() {
                    raw.push(RawColumn::new(*spec, range));
                }
            }
        }
        (raw.into_iter().collect(), data)
    }

    pub(crate) fn iter_prop<'a>(&'a self, obj: &ObjId, prop: &str) -> OpIter<'a> {
        let range = self
            .cols
            .get_integer(OBJ_ID_COUNTER_COL_SPEC)
            .scope_to_value(obj.counter(), ..);
        let range = self
            .cols
            .get_actor(OBJ_ID_ACTOR_COL_SPEC)
            .scope_to_value(obj.actor(), range);
        let range = self
            .cols
            .get_str(KEY_STR_COL_SPEC)
            .scope_to_value(Some(prop), range);
        self.iter_range(&range)
    }

    pub(crate) fn iter_obj<'a>(&'a self, obj: &ObjId) -> OpIter<'a> {
        let range = self
            .cols
            .get_integer(OBJ_ID_COUNTER_COL_SPEC)
            .scope_to_value(obj.counter(), ..);
        let range = self
            .cols
            .get_actor(OBJ_ID_ACTOR_COL_SPEC)
            .scope_to_value(obj.actor(), range);
        self.iter_range(&range)
    }

    pub(crate) fn iter_range<'a>(&'a self, range: &Range<usize>) -> OpIter<'_> {
        let value_meta = self.cols.get_value_meta_range(VALUE_META_COL_SPEC, range);
        let value = self
            .cols
            .get_value_range(VALUE_COL_SPEC, value_meta.group());

        let succ_count = self.cols.get_group_range(SUCC_COUNT_COL_SPEC, range);
        let succ_actor = self
            .cols
            .get_actor_range(SUCC_ACTOR_COL_SPEC, &(succ_count.group()..usize::MAX));
        let succ_counter = self
            .cols
            .get_delta_integer_range(SUCC_COUNTER_COL_SPEC, &(succ_count.group()..usize::MAX));

        OpIter {
            pos: range.start,
            id_actor: self.cols.get_actor_range(ID_ACTOR_COL_SPEC, range),
            id_counter: self
                .cols
                .get_delta_integer_range(ID_COUNTER_COL_SPEC, range),
            obj_id_actor: self.cols.get_actor_range(OBJ_ID_ACTOR_COL_SPEC, range),
            obj_id_counter: self.cols.get_integer_range(OBJ_ID_COUNTER_COL_SPEC, range),
            key_actor: self.cols.get_actor_range(KEY_ACTOR_COL_SPEC, range),
            key_counter: self
                .cols
                .get_delta_integer_range(KEY_COUNTER_COL_SPEC, range),
            key_str: self.cols.get_str_range(KEY_STR_COL_SPEC, range),
            succ_count,
            succ_actor,
            succ_counter,
            insert: self.cols.get_boolean_range(INSERT_COL_SPEC, range),
            action: self.cols.get_action_range(ACTION_COL_SPEC, range),
            value_meta,
            value,
            mark_name: self.cols.get_str_range(MARK_NAME_COL_SPEC, range),
            expand: self.cols.get_boolean_range(EXPAND_COL_SPEC, range),
            op_set: &self,
        }
    }

    pub(crate) fn iter(&self) -> OpIter<'_> {
        OpIter {
            pos: 0,
            id_actor: self.cols.get_actor(ID_ACTOR_COL_SPEC),
            id_counter: self.cols.get_delta_integer(ID_COUNTER_COL_SPEC),
            obj_id_actor: self.cols.get_actor(OBJ_ID_ACTOR_COL_SPEC),
            obj_id_counter: self.cols.get_integer(OBJ_ID_COUNTER_COL_SPEC),
            key_actor: self.cols.get_actor(KEY_ACTOR_COL_SPEC),
            key_counter: self.cols.get_delta_integer(KEY_COUNTER_COL_SPEC),
            key_str: self.cols.get_str(KEY_STR_COL_SPEC),
            succ_count: self.cols.get_group(SUCC_COUNT_COL_SPEC),
            succ_actor: self.cols.get_actor(SUCC_ACTOR_COL_SPEC),
            succ_counter: self.cols.get_delta_integer(SUCC_COUNTER_COL_SPEC),
            insert: self.cols.get_boolean(INSERT_COL_SPEC),
            action: self.cols.get_action(ACTION_COL_SPEC),
            value_meta: self.cols.get_value_meta(VALUE_META_COL_SPEC),
            value: self.cols.get_value(VALUE_COL_SPEC),
            mark_name: self.cols.get_str(MARK_NAME_COL_SPEC),
            expand: self.cols.get_boolean(EXPAND_COL_SPEC),
            op_set: &self,
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

    pub(crate) fn decode(spec: ColumnSpec, data: &[u8]) {
        match spec.col_type() {
            ColumnType::Actor => ActorCursor::decode(data),
            ColumnType::String => StrCursor::decode(data),
            ColumnType::Integer => IntCursor::decode(data),
            ColumnType::DeltaInteger => DeltaCursor::decode(data),
            ColumnType::Boolean => BooleanCursor::decode(data),
            ColumnType::Group => IntCursor::decode(data),
            ColumnType::ValueMetadata => MetaCursor::decode(data),
            ColumnType::Value => log!("raw :: {:?}", data),
        }
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        self.cols.rewrite_with_new_actor(idx)
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        self.actors.remove(idx);
        self.cols.rewrite_without_actor(idx);
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
        op: &OpBuilder2,
        pred: &[OpId],
        doc: &Automerge,
        patch_log: &mut PatchLog,
    ) {
        if op.insert {
            if op.is_mark() {
                if let crate::types::OpType::MarkEnd(_) = op.action {
                    let encoding = patch_log.text_rep().encoding(obj.typ);
                    let mut index = 0;
                    let mut marks = MarkStateMachine::default();
                    let mut mark_name = None;
                    let mut value = ScalarValue::Null;
                    let mut start = None;
                    let target = op.id.prev();
                    let end_id = op.id;
                    for op in doc.ops().iter_obj(&obj.id).visible(None).top_ops() {
                        // if we find our first op
                        if op.id == target {
                            // grab its name and value
                            if let Some(mark) = op.mark_name {
                                mark_name = Some(mark);
                                value = op.value;
                                // and if it changes the mark state start recording
                                if marks.process(op.id, op.action()) {
                                    start = Some(index);
                                }
                            }
                        } else if let Some(mark) = mark_name {
                            // whenever the mark state changes
                            if marks.process(op.id, op.action()) {
                                match (marks.covered(target, mark), start) {
                                    (true, Some(s)) => {
                                        // the mark is either covered up (so we're done)
                                        let ms = MarkSet::new(mark, value);
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
                        if op.id == end_id {
                            break;
                        }
                    }
                    if let Some(s) = start {
                        if let Some(mark) = mark_name {
                            let ms = MarkSet::new(mark, value);
                            patch_log.mark(obj.id, s, index - s, &ms);
                        }
                    }
                }
            // TODO - move this into patch_log()
            } else if obj.typ == ObjType::Text && !op.action.is_block() {
                patch_log.splice(obj.id, self.index, op.as_str(), self.marks.clone());
            } else {
                patch_log.insert(obj.id, self.index, op.hydrate_value(), op.id, false);
            }
            return;
        }

        let key: Prop = match &op.key {
            Key::Map(s) => Prop::from(s),
            Key::Seq(_) => Prop::from(self.index),
        };

        if op.is_delete() {
            match (self.before, self.overwritten, self.after) {
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
                        before.value().into(),
                        before.id,
                        conflict,
                        true,
                    );
                }
                _ => { /* do nothing */ }
            }
        } else if let Some(value) = op.get_increment_value() {
            if self.after.is_none() {
                if let Some(counter) = self.overwritten {
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
                patch_log.insert(obj.id, self.index, op.hydrate_value(), op.id, conflict);
            } else if self.after.is_some() {
                if self.before.is_none() {
                    patch_log.flag_conflict(obj.id, &key);
                }
            } else {
                patch_log.put(obj.id, &key, op.hydrate_value(), op.id, conflict, false);
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

#[derive(Debug, Default, Clone)]
pub(crate) struct OpsFound<'a> {
    pub(crate) index: usize,
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) ops_pos: Vec<usize>,
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

#[derive(Debug, Clone)]
struct Columns(BTreeMap<ColumnSpec, Column>);

impl Default for Columns {
    fn default() -> Self {
        let mut btree = BTreeMap::new();
        for spec in &ALL_COLUMN_SPECS {
            let col = Column::new(*spec);
            assert!(col.slabs().len() > 0);
            btree.insert(*spec, col);
        }
        Self(btree)
    }
}

pub(super) trait OpLike {
    fn id(&self) -> OpId;
    fn obj(&self) -> ObjId;
    fn action(&self) -> Action;
    fn map_key(&self) -> Option<&str>;
    fn elemid(&self) -> Option<ElemId>;
    fn raw_value(&self) -> Option<Cow<'_, [u8]>>; // allocation
    fn meta_value(&self) -> ValueMeta;
    fn insert(&self) -> bool;
    fn expand(&self) -> bool;
    // allocation
    fn succ(&self) -> Vec<OpId> {
        vec![]
    }
    fn mark_name(&self) -> Option<&str>;
}

// TODO? add inc value to the succ column

const NONE: &'static str = ".";

fn fmt<T: std::fmt::Display>(t: Option<Option<T>>) -> String {
    match t {
        None => NONE.to_owned(),
        Some(None) => "-".to_owned(),
        Some(Some(t)) => format!("{}", t).to_owned(),
    }
}

impl Columns {
    // FIXME - this could be much much more efficient
    fn rewrite_with_new_actor(&mut self, idx: usize) {
        for (_spec, col) in &mut self.0 {
            match col {
                Column::Actor(col_data) => {
                    let new_ids = col_data
                        .iter()
                        .map(|a| match a {
                            Some(ActorIdx(id)) if id as usize >= idx => Some(ActorIdx(id + 1)),
                            old => old,
                        })
                        .collect::<Vec<_>>();
                    let mut new_data = ColumnData::<ActorCursor>::new();
                    new_data.splice(0, 0, new_ids);
                    std::mem::swap(col_data, &mut new_data);
                }
                _ => {}
            }
        }
    }

    fn rewrite_without_actor(&mut self, idx: usize) {
        for (_spec, col) in &mut self.0 {
            match col {
                Column::Actor(col_data) => {
                    let new_ids = col_data
                        .iter()
                        .map(|a| match a {
                            Some(ActorIdx(id)) if id as usize > idx => Some(ActorIdx(id - 1)),
                            Some(ActorIdx(id)) if id as usize == idx => {
                                panic!("cant rewrite - actor is present")
                            }
                            old => old,
                        })
                        .collect::<Vec<_>>();
                    let mut new_data = ColumnData::<ActorCursor>::new();
                    new_data.splice(0, 0, new_ids);
                    std::mem::swap(col_data, &mut new_data);
                }
                _ => {}
            }
        }
    }

    fn dump(&self) {
        let mut id_a = self.get_actor(ID_ACTOR_COL_SPEC);
        let mut id_c = self.get_delta_integer(ID_COUNTER_COL_SPEC);
        let mut act = self.get_action(ACTION_COL_SPEC);
        let mut obj_a = self.get_actor(OBJ_ID_ACTOR_COL_SPEC);
        let mut obj_c = self.get_integer(OBJ_ID_COUNTER_COL_SPEC);
        let mut key_str = self.get_str(KEY_STR_COL_SPEC);
        let mut key_a = self.get_actor(KEY_ACTOR_COL_SPEC);
        let mut key_c = self.get_delta_integer(KEY_COUNTER_COL_SPEC);
        let mut meta = self.get_value_meta(VALUE_META_COL_SPEC);
        let mut value = self.get_value(VALUE_COL_SPEC);
        let mut succ = self.get_group(SUCC_COUNT_COL_SPEC);
        let mut insert = self.get_boolean(INSERT_COL_SPEC);
        log!(":: id      obj     key      elem     ins act  suc value");
        loop {
            let id_a = fmt(id_a.next());
            let id_c = fmt(id_c.next());
            let obj_a = fmt(obj_a.next());
            let obj_c = fmt(obj_c.next());
            let act = fmt(act.next());
            let insert = insert.next();
            let insert = if insert == Some(Some(true)) { "t" } else { "-" };
            let key_s = fmt(key_str.next());
            let key_a = fmt(key_a.next());
            let key_c = fmt(key_c.next());
            let succ = fmt(succ.next());
            let m = meta.next();
            let v = if let Some(Some(m)) = m {
                let raw_data = value.read_next(m.length()).unwrap_or(&[]);
                ScalarValue::from_raw(m, raw_data).unwrap()
            } else {
                ScalarValue::Null
            };
            if id_a == NONE && id_c == NONE && obj_a == NONE && obj_c == NONE {
                break;
            }
            log!(
                ":: {:7} {:7} {:8} {:8} {:3} {:3}  {:1}   {}",
                format!("({},{})", id_c, id_a),
                format!("({},{})", obj_c, obj_a),
                key_s,
                format!("({},{})", key_c, key_a),
                insert,
                act,
                succ,
                v,
            );
        }
    }

    fn insert<O: OpLike + std::fmt::Debug>(&mut self, pos: usize, op: &O) {
        let mut group = None;
        let mut group_pos = 0;
        for (spec, col) in self.0.iter_mut() {
            if group == Some(spec.id()) {
                match col {
                    Column::Actor(c) => {
                        let values = if *spec == SUCC_ACTOR_COL_SPEC {
                            op.succ().iter().map(|s| s.actoridx()).collect()
                        } else {
                            vec![]
                        };
                        c.splice(group_pos, 0, values);
                    }
                    Column::Delta(c) => {
                        let values = if *spec == SUCC_COUNTER_COL_SPEC {
                            op.succ().iter().map(|s| s.counter() as i64).collect()
                        } else {
                            vec![]
                        };
                        c.splice(group_pos, 0, values);
                    }
                    Column::Value(c) => {
                        let value = if *spec == VALUE_COL_SPEC {
                            op.raw_value()
                        } else {
                            None
                        };
                        if let Some(v) = value {
                            c.splice(group_pos, 0, vec![v])
                        }
                    }
                    _ => {
                        log!("unknown group column %{:?}", spec);
                    }
                }
            } else {
                group = spec.group_id();
                match col {
                    Column::Actor(c) => {
                        let value = match *spec {
                            ID_ACTOR_COL_SPEC => Some(op.id().actoridx()),
                            OBJ_ID_ACTOR_COL_SPEC => op.obj().actor(),
                            KEY_ACTOR_COL_SPEC => op.elemid().and_then(|e| e.actor()),
                            _ => None,
                        };
                        c.splice(pos, 0, vec![value]);
                    }
                    Column::Delta(c) => {
                        let value = match *spec {
                            ID_COUNTER_COL_SPEC => Some(op.id().counter() as i64),
                            KEY_COUNTER_COL_SPEC => op.elemid().map(|e| e.counter() as i64),
                            _ => None,
                        };
                        c.splice(pos, 0, vec![value]);
                    }
                    Column::Integer(c) => {
                        let value = if *spec == OBJ_ID_COUNTER_COL_SPEC {
                            op.obj().counter()
                        } else {
                            None
                        };
                        c.splice(pos, 0, vec![value])
                    }
                    Column::Str(c) => {
                        let value = match *spec {
                            KEY_STR_COL_SPEC => op.map_key(),
                            MARK_NAME_COL_SPEC => op.mark_name(),
                            _ => None,
                        };
                        c.splice(pos, 0, vec![value])
                    }
                    Column::Bool(c) => {
                        let value = match *spec {
                            INSERT_COL_SPEC => Some(op.insert()),
                            EXPAND_COL_SPEC => Some(op.expand()),
                            _ => None,
                        };
                        c.splice(pos, 0, vec![value])
                    }
                    Column::Action(c) => {
                        let value = if *spec == ACTION_COL_SPEC {
                            Some(op.action())
                        } else {
                            None
                        };
                        c.splice(pos, 0, vec![value])
                    }
                    Column::Value(_c) => {
                        panic!("VALUE spliced outside of a group");
                    }
                    Column::ValueMeta(c) => {
                        let value = if *spec == VALUE_META_COL_SPEC {
                            Some(op.meta_value())
                        } else {
                            None
                        };
                        c.splice(pos, 0, vec![value]);
                        // FIXME if value > 0
                        let mut iter = c.iter();
                        iter.advance_by(pos);
                        group_pos = iter.group();
                    }
                    Column::Group(c) => {
                        let value = if *spec == SUCC_COUNT_COL_SPEC {
                            Some(op.succ().iter().len() as u64)
                        } else {
                            None
                        };
                        c.splice(pos, 0, vec![value]);
                        // FIXME if value > 0
                        // FIXME would be nice if splice did this
                        let mut iter = c.iter();
                        iter.advance_by(pos);
                        group_pos = iter.group();
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn new<'a, I: Iterator<Item = super::op::Op<'a>> + Clone>(ops: I) -> Self {
        // FIXME this should insert NULL values into columns we dont recognize

        let mut columns = BTreeMap::new();

        let mut id_actor = ColumnData::<ActorCursor>::new();
        id_actor.splice(
            0,
            0,
            ops.clone().map(|op| op.id.actoridx()).collect::<Vec<_>>(),
        );
        columns.insert(ID_ACTOR_COL_SPEC, Column::Actor(id_actor));

        let mut id_counter = ColumnData::<DeltaCursor>::new();
        id_counter.splice(
            0,
            0,
            ops.clone()
                .map(|op| op.id.counter() as i64)
                .collect::<Vec<_>>(),
        );
        columns.insert(ID_COUNTER_COL_SPEC, Column::Delta(id_counter));

        let mut obj_actor_col = ColumnData::<ActorCursor>::new();
        obj_actor_col.splice(
            0,
            0,
            ops.clone().map(|op| op.obj.actor()).collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_ACTOR_COL_SPEC, Column::Actor(obj_actor_col));

        let mut obj_counter_col = ColumnData::<IntCursor>::new();
        obj_counter_col.splice(
            0,
            0,
            ops.clone().map(|op| op.obj.0.counter()).collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_COUNTER_COL_SPEC, Column::Integer(obj_counter_col));

        let mut key_actor = ColumnData::<ActorCursor>::new();
        key_actor.splice(
            0,
            0,
            ops.clone()
                .map(|op| match op.key {
                    KeyRef::Map(_) => None,
                    KeyRef::Seq(e) => e.actor(),
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_ACTOR_COL_SPEC, Column::Actor(key_actor));

        let mut key_counter = ColumnData::<DeltaCursor>::new();
        key_counter.splice(
            0,
            0,
            ops.clone()
                .map(|op| match op.key {
                    KeyRef::Map(_) => None,
                    KeyRef::Seq(e) => {
                        if e.is_head() {
                            None
                        } else {
                            Some(e.0.counter() as i64)
                        }
                    }
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_COUNTER_COL_SPEC, Column::Delta(key_counter));

        let mut key_str = ColumnData::<StrCursor>::new();
        key_str.splice(
            0,
            0,
            ops.clone()
                .map(|op| match op.key {
                    KeyRef::Map(s) => Some(s),
                    KeyRef::Seq(_) => None,
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_STR_COL_SPEC, Column::Str(key_str));

        let mut succ_count = ColumnData::<IntCursor>::new();
        succ_count.splice(
            0,
            0,
            ops.clone()
                .map(|op| op.succ().len() as u64)
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNT_COL_SPEC, Column::Group(succ_count));

        let mut succ_actor = ColumnData::<ActorCursor>::new();
        succ_actor.splice(
            0,
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| n.actoridx()))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_ACTOR_COL_SPEC, Column::Actor(succ_actor));

        let mut succ_counter = ColumnData::<DeltaCursor>::new();
        succ_counter.splice(
            0,
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| n.counter() as i64))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNTER_COL_SPEC, Column::Delta(succ_counter));

        let mut insert = ColumnData::<BooleanCursor>::new();
        insert.splice(0, 0, ops.clone().map(|op| op.insert).collect::<Vec<_>>());
        columns.insert(INSERT_COL_SPEC, Column::Bool(insert));

        let mut action = ColumnData::<ActionCursor>::new();
        action.splice(0, 0, ops.clone().map(|op| op.action).collect::<Vec<_>>());
        columns.insert(ACTION_COL_SPEC, Column::Action(action));

        let mut value_meta = ColumnData::<MetaCursor>::new();
        value_meta.splice(
            0,
            0,
            ops.clone()
                .map(|op| ValueMeta::from(&op.value))
                .collect::<Vec<_>>(),
        );
        columns.insert(VALUE_META_COL_SPEC, Column::ValueMeta(value_meta));

        let mut value = ColumnData::<super::RawCursor>::new();
        let values = ops
            .clone()
            .filter_map(|op| op.value.to_raw())
            .collect::<Vec<_>>();
        value.splice(0, 0, values);
        columns.insert(VALUE_COL_SPEC, Column::Value(value));

        let mut mark_name = ColumnData::<StrCursor>::new();
        mark_name.splice(
            0,
            0,
            ops.clone()
                .map(|op| op.mark_name.clone())
                .collect::<Vec<_>>(),
        );
        columns.insert(MARK_NAME_COL_SPEC, Column::Str(mark_name));

        let mut expand = ColumnData::<BooleanCursor>::new();
        expand.splice(0, 0, ops.clone().map(|op| op.expand).collect::<Vec<_>>());
        columns.insert(EXPAND_COL_SPEC, Column::Bool(expand));

        Columns(columns)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.get(&ID_ACTOR_COL_SPEC).map(|c| c.len()).unwrap_or(0)
    }

    fn get_actor_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<ActorCursor> {
        match self.0.get_mut(&spec) {
            Some(Column::Actor(c)) => c,
            _ => panic!(),
        }
    }

    fn get_delta_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<DeltaCursor> {
        match self.0.get_mut(&spec) {
            Some(Column::Delta(c)) => c,
            _ => panic!(),
        }
    }

    /*
        fn get_integer_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<IntCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::Integer(c)) => c,
                _ => panic!(),
            }
        }

        fn get_boolean_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<BooleanCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::Bool(c)) => c,
                _ => panic!(),
            }
        }

        fn get_str_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<StrCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::Str(c)) => c,
                _ => panic!(),
            }
        }

        fn get_action_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<ActionCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::Action(c)) => c,
                _ => panic!(),
            }
        }

        fn get_value_meta_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<MetaCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::ValueMeta(c)) => c,
                _ => panic!(),
            }
        }

        fn get_value_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<RawCursor> {
            match self.0.get_mut(&spec) {
                Some(Column::Value(c)) => c,
                _ => panic!(),
            }
        }
    */

    fn get_actor(&self, spec: ColumnSpec) -> ColumnDataIter<'_, ActorCursor> {
        match self.0.get(&spec) {
            Some(Column::Actor(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_actor_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, ActorCursor> {
        match self.0.get(&spec) {
            Some(Column::Actor(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    /*
        fn get_coldata(&self, spec: ColumnSpec) -> &[Slab] {
            self.0.get(&spec).unwrap().slabs()
        }
    */

    fn get_integer(&self, spec: ColumnSpec) -> ColumnDataIter<'_, IntCursor> {
        match self.0.get(&spec) {
            Some(Column::Integer(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_integer_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, IntCursor> {
        match self.0.get(&spec) {
            Some(Column::Integer(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_action(&self, spec: ColumnSpec) -> ColumnDataIter<'_, ActionCursor> {
        match self.0.get(&spec) {
            Some(Column::Action(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_action_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, ActionCursor> {
        match self.0.get(&spec) {
            Some(Column::Action(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_delta_integer(&self, spec: ColumnSpec) -> ColumnDataIter<'_, DeltaCursor> {
        match self.0.get(&spec) {
            Some(Column::Delta(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_delta_integer_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, DeltaCursor> {
        match self.0.get(&spec) {
            Some(Column::Delta(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_str(&self, spec: ColumnSpec) -> ColumnDataIter<'_, StrCursor> {
        match self.0.get(&spec) {
            Some(Column::Str(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_str_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, StrCursor> {
        match self.0.get(&spec) {
            Some(Column::Str(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_boolean(&self, spec: ColumnSpec) -> ColumnDataIter<'_, BooleanCursor> {
        match self.0.get(&spec) {
            Some(Column::Bool(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_boolean_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, BooleanCursor> {
        match self.0.get(&spec) {
            Some(Column::Bool(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_value_meta(&self, spec: ColumnSpec) -> ColumnDataIter<'_, MetaCursor> {
        match self.0.get(&spec) {
            Some(Column::ValueMeta(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_value_meta_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, MetaCursor> {
        match self.0.get(&spec) {
            Some(Column::ValueMeta(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_value(&self, spec: ColumnSpec) -> RawReader<'_> {
        match self.0.get(&spec) {
            Some(Column::Value(c)) => c.raw_reader(0),
            _ => RawReader::empty(),
        }
    }

    fn get_value_range(&self, spec: ColumnSpec, advance: usize) -> RawReader<'_> {
        // FIXME - range??
        match self.0.get(&spec) {
            Some(Column::Value(c)) => c.raw_reader(advance),
            _ => RawReader::empty(),
        }
    }

    fn get_group_mut(&mut self, spec: ColumnSpec) -> &mut ColumnData<IntCursor> {
        match self.0.get_mut(&spec) {
            Some(Column::Group(c)) => c,
            _ => panic!(),
        }
    }

    fn get_group(&self, spec: ColumnSpec) -> ColumnDataIter<'_, IntCursor> {
        match self.0.get(&spec) {
            Some(Column::Group(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_group_range(
        &self,
        spec: ColumnSpec,
        range: &Range<usize>,
    ) -> ColumnDataIter<'_, IntCursor> {
        match self.0.get(&spec) {
            Some(Column::Group(c)) => c.iter_range(range),
            _ => ColumnDataIter::empty(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&ColumnSpec, &Column)> {
        self.0.iter()
    }

    fn init_missing(&mut self) {
        let len = self.len();
        let mut group = None;
        for spec in &ALL_COLUMN_SPECS {
            if group == Some(spec.id()) {
                if self.0.get(spec).is_none() {
                    let col = Column::new(*spec);
                    self.0.insert(*spec, col);
                }
            } else {
                group = spec.group_id();
                if self.0.get(spec).is_none() {
                    let col = Column::init_empty(*spec, len);
                    self.0.insert(*spec, col);
                }
            }
        }
    }
}

struct IterObjIds<'a> {
    ctr: ColumnDataIter<'a, IntCursor>,
    actor: ColumnDataIter<'a, ActorCursor>,
    next_ctr: Option<Run<'a, u64>>,
    next_actor: Option<Run<'a, ActorIdx>>,
    pos: usize,
}

impl<'a> Iterator for IterObjIds<'a> {
    type Item = (ObjId, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.pos;
        match (self.next_ctr, self.next_actor) {
            (Some(mut run1), Some(mut run2)) => {
                if run1.count < run2.count {
                    run2.count -= run1.count;
                    self.next_actor = Some(run2);
                    self.pos += run1.count;
                    self.next_ctr = self.ctr.next_run();
                } else if run1.count > run2.count {
                    run1.count -= run2.count;
                    self.next_ctr = Some(run1);
                    self.pos += run2.count;
                    self.next_actor = self.actor.next_run();
                } else {
                    // equal
                    self.pos += run1.count;
                    self.next_ctr = self.ctr.next_run();
                    self.next_actor = self.actor.next_run();
                }
                let end = self.pos;
                let obj = ObjId::load(run1.value, run2.value)?;
                Some((obj, start..end))
            }
            (None, None) => None,
            _ => panic!(),
        }
    }
}

// Stick all of the column ID initialization in a module so we can turn off
// rustfmt for the whole thing
#[rustfmt::skip]
pub(crate) mod ids {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(crate) const OBJ_COL_ID:                ColumnId = ColumnId::new(0);
    pub(crate) const KEY_COL_ID:                ColumnId = ColumnId::new(1);
    pub(crate) const ID_COL_ID:                 ColumnId = ColumnId::new(2);
    pub(crate) const INSERT_COL_ID:             ColumnId = ColumnId::new(3);
    pub(crate) const ACTION_COL_ID:             ColumnId = ColumnId::new(4);
    pub(crate) const VAL_COL_ID:                ColumnId = ColumnId::new(5);
    pub(crate) const SUCC_COL_ID:               ColumnId = ColumnId::new(8);
    pub(crate) const EXPAND_COL_ID:             ColumnId = ColumnId::new(9);
    pub(crate) const MARK_NAME_COL_ID:          ColumnId = ColumnId::new(10);

    pub(crate) const ID_ACTOR_COL_SPEC:       ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(crate) const ID_COUNTER_COL_SPEC:     ColumnSpec = ColumnSpec::new_delta(ID_COL_ID);
    pub(crate) const OBJ_ID_ACTOR_COL_SPEC:   ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(crate) const OBJ_ID_COUNTER_COL_SPEC: ColumnSpec = ColumnSpec::new_integer(OBJ_COL_ID);
    pub(crate) const KEY_ACTOR_COL_SPEC:      ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(crate) const KEY_COUNTER_COL_SPEC:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(crate) const KEY_STR_COL_SPEC:        ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(crate) const SUCC_COUNT_COL_SPEC:     ColumnSpec = ColumnSpec::new_group(SUCC_COL_ID);
    pub(crate) const SUCC_ACTOR_COL_SPEC:     ColumnSpec = ColumnSpec::new_actor(SUCC_COL_ID);
    pub(crate) const SUCC_COUNTER_COL_SPEC:   ColumnSpec = ColumnSpec::new_delta(SUCC_COL_ID);
    pub(crate) const INSERT_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(INSERT_COL_ID);
    pub(crate) const ACTION_COL_SPEC:         ColumnSpec = ColumnSpec::new_integer(ACTION_COL_ID);
    pub(crate) const VALUE_META_COL_SPEC:     ColumnSpec = ColumnSpec::new_value_metadata(VAL_COL_ID);
    pub(crate) const VALUE_COL_SPEC:          ColumnSpec = ColumnSpec::new_value(VAL_COL_ID);
    pub(crate) const MARK_NAME_COL_SPEC:      ColumnSpec = ColumnSpec::new_string(MARK_NAME_COL_ID);
    pub(crate) const EXPAND_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(EXPAND_COL_ID);

    pub(crate) const ALL_COLUMN_SPECS: [ColumnSpec; 16] = [
        ID_ACTOR_COL_SPEC,
        ID_COUNTER_COL_SPEC,
        OBJ_ID_ACTOR_COL_SPEC,
        OBJ_ID_COUNTER_COL_SPEC,
        KEY_ACTOR_COL_SPEC,
        KEY_COUNTER_COL_SPEC,
        KEY_STR_COL_SPEC,
        SUCC_COUNT_COL_SPEC,
        SUCC_ACTOR_COL_SPEC,
        SUCC_COUNTER_COL_SPEC,
        INSERT_COL_SPEC,
        ACTION_COL_SPEC,
        VALUE_META_COL_SPEC,
        VALUE_COL_SPEC,
        MARK_NAME_COL_SPEC,
        EXPAND_COL_SPEC,
    ];
}
pub(super) use ids::*;
#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        op_set2::{
            columns::ColumnData,
            op::SuccCursors,
            rle::ActorCursor,
            types::{Action, ActorIdx, ScalarValue},
            DeltaCursor, KeyRef,
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
        let opset = super::OpSet::new(&doc_chunk).unwrap();
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

        let mut group_data = ColumnData::<IntCursor>::new();
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
                mark_name: test_op.mark_name,
                conflict: false,
                succ_cursors: SuccCursors {
                    len: group_count as usize,
                    succ_counter: counter_iter.clone(),
                    succ_actor: actor_iter.clone(),
                },
            };
            for _ in 0..group_count {
                counter_iter.next();
                actor_iter.next();
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
                key: KeyRef::Map("key"),
                insert: false,
                succs: vec![OpId::new(5, 1), OpId::new(6, 1), OpId::new(10, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::Str("value1"),
                key: KeyRef::Map("key1"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(3, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::Str("value2"),
                key: KeyRef::Map("key2"),
                insert: false,
                succs: vec![OpId::new(6, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("inner_value1"),
                key: KeyRef::Map("inner_key1"),
                insert: false,
                succs: vec![OpId::new(7, 1), OpId::new(8, 2), OpId::new(9, 1)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(5, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("inner_value2"),
                key: KeyRef::Map("inner_key2"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &ops, |opset| {
            let range = opset
                .cols
                .get_integer(OBJ_ID_COUNTER_COL_SPEC)
                .scope_to_value(Some(1), ..);
            let range = opset
                .cols
                .get_actor(OBJ_ID_ACTOR_COL_SPEC)
                .scope_to_value(Some(ActorIdx::from(1 as usize)), range);
            let mut iter = opset.iter_range(&range);
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
                key: KeyRef::Map("map"),
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
                key: KeyRef::Map("list"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(3, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value1"),
                key: KeyRef::Map("key1"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value2a"),
                key: KeyRef::Map("key2"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value2b"),
                key: KeyRef::Map("key2"),
                insert: false,
                succs: vec![OpId::new(5, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(5, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value2c"),
                key: KeyRef::Map("key2"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(6, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value3a"),
                key: KeyRef::Map("key3"),
                insert: false,
                succs: vec![OpId::new(7, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(7, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::Str("value3b"),
                key: KeyRef::Map("key3"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(8, 1),
                obj: ObjId(OpId::new(2, 1)),
                action: Action::Set,
                value: ScalarValue::Str("a"),
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
                value: ScalarValue::Str("b"),
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
            let key1 = ops.get(0).unwrap().as_slice();
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
            let key1 = ops.get(0).unwrap().as_slice();
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
