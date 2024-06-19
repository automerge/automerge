use super::parents::Parents;
use crate::cursor::Cursor;
use crate::exid::ExId;
use crate::op_set::{OpBuilder, OpIdx, OpIdxRange};
use crate::patches::TextRepresentation;
use crate::query::TreeQuery;
use crate::storage::ColumnType;
use crate::storage::{columns::compression, ColumnSpec, Document, RawColumn, RawColumns};
use crate::types;
use crate::types::{
    ActorId, Clock, ElemId, Export, Exportable, ListEncoding, ObjId, ObjMeta, ObjType, OpId, Prop,
};

use super::columns::{ColumnData, ColumnDataIter, RawReader, Run};
use super::op::Op;
use super::rle::{ActionCursor, ActorCursor};
use super::types::ActorIdx;
use super::{
    BooleanCursor, Column, DeltaCursor, IntCursor, Key, MetaCursor, RawCursor, Slab, StrCursor,
    ValueMeta,
};

use std::collections::BTreeMap;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

mod iter;
pub(crate) use iter::{
    KeyIter, Keys, ListRange, ListRangeItem, MapRange, MapRangeItem, OpIter, OpScope, TopOpIter,
    Value, Values, Verified, VisibleOpIter,
};
mod spans;
pub(crate) use spans::{SpanInternal, Spans, SpansInternal};

#[derive(Debug, Default, Clone)]
pub(crate) struct OpSet {
    len: usize,
    actors: Vec<ActorId>,
    cols: Columns,
}

impl OpSet {
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

    pub(crate) fn load_with_range(
        &mut self,
        obj: ObjId,
        op: OpBuilder,
        range: &mut OpIdxRange,
    ) -> OpIdx {
        todo!()
    }

    pub(crate) fn insert(&mut self, index: usize, obj: &ObjId, idx: OpIdx) {
        todo!()
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, obj: &ObjId, mut query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        todo!()
    }

    pub(crate) fn add_succ(&mut self, obj: &ObjId, op_indices: &[usize], op: OpIdx) {
        todo!()
    }

    pub(crate) fn parent_object(
        &self,
        obj: &ObjId,
        text_rep: TextRepresentation,
        clock: Option<&Clock>,
    ) -> Option<Parent> {
        let op = self.find_op_by_id(&obj.0)?;
        if op.obj.is_root() {
            return None;
        }
        let (parent_op, visible) = self.find_op_by_id_and_vis(&op.obj.0, clock)?;
        // FIXME remote unwrap
        let parent_typ = parent_op.action.try_into().unwrap();
        let prop = match op.key {
            Key::Map(k) => Prop::Map(k.to_string()),
            Key::Seq(_) => {
                let FoundOpId { index, .. } = self
                    .seek_list_opid(
                        &parent_op.obj,
                        parent_op.id,
                        text_rep.encoding(parent_typ),
                        clock,
                    )
                    .unwrap();
                Prop::Seq(index)
            }
        };
        //Some(crate::op_set::Parent {
        Some(Parent {
            typ: op.action.try_into().unwrap(),
            obj: op.obj,
            prop,
            visible,
        })
    }

    pub(crate) fn keys<'a>(&'a self, obj: &ObjId, clock: Option<Clock>) -> Keys<'a> {
        let iter = self.iter_obj(obj).visible_ops(clock).key_ops();
        Keys { iter }
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: &ObjId,
        range: R,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> ListRange<'_, R> {
        let iter = self.iter_obj(obj).visible_ops(clock).key_ops();
        ListRange::new(iter, range)
    }

    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'_, R> {
        let iter = self.iter_obj(obj).visible_ops(clock).key_ops();
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

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        obj: &ObjId,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        match prop {
            Prop::Map(key_name) => self.seek_ops_by_map_key(obj, &key_name, encoding, clock),
            Prop::Seq(index) => self.seek_ops_by_index(obj, index, encoding, clock),
        }
    }

    pub(crate) fn seek_ops_by_map_key<'a>(
        &'a self,
        obj: &ObjId,
        key: &str,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        todo!()
    }

    pub(crate) fn seek_ops_by_index<'a>(
        &'a self,
        obj: &ObjId,
        index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        todo!()
    }

    pub(crate) fn seek_list_opid(
        &self,
        obj: &ObjId,
        opid: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        todo!()
    }

    pub(crate) fn text(&self, obj: &ObjId, clock: Option<Clock>) -> String {
        self.top_ops(obj, clock).map(|op| op.as_str()).collect()
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.counter(), self.actors[id.actor()].clone(), id.actor())
        }
    }

    pub(crate) fn id_to_cursor(&self, id: OpId) -> Cursor {
        if id == types::ROOT {
            panic!()
        } else {
            Cursor::new(id, self)
            /*
                            ctr: id.counter(),
                            actor: self.actors[id.actor()].clone(),
                        }
            */
        }
    }

    fn get_obj_ctr(&self) -> ColumnDataIter<'_, IntCursor> {
        self.cols.get_integer(OBJ_ID_COUNTER_COL_SPEC)
    }

    fn get_obj_actor(&self) -> ColumnDataIter<'_, ActorCursor> {
        self.cols.get_actor(OBJ_ID_ACTOR_COL_SPEC)
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

    pub(crate) fn iter_objs(&self) -> impl Iterator<Item = (ObjMeta, OpIter<'_, Verified>)> {
        // FIXME - remove unwraps
        self.iter_obj_ids().map(|(id, range)| {
            let obj_meta = self
                .find_op_by_id(&id.0)
                .map(|op| ObjMeta {
                    id,
                    typ: op.action.try_into().unwrap(),
                })
                .unwrap();
            (obj_meta, self.iter_range(&range))
        })
    }

    pub(crate) fn top_ops<'a>(
        &'a self,
        obj: &ObjId,
        clock: Option<Clock>,
    ) -> TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>> {
        self.iter_obj(obj).visible_ops(clock).top_ops()
    }

    pub(crate) fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), self.actors[id.actor()]),
            Export::Prop(index) => panic!(),
            Export::Special(s) => s,
        }
    }

    pub(crate) fn find_op_by_id(&self, id: &OpId) -> Option<Op<'_>> {
        self.iter().find(|op| &op.id == id)
    }

    pub(crate) fn find_op_by_id_and_vis(
        &self,
        id: &OpId,
        clock: Option<&Clock>,
    ) -> Option<(Op<'_>, bool)> {
        let mut iter = self.iter();
        while let Some(op) = iter.next() {
            if &op.id == id {
                return Some((op, op.visible_at(clock)));
            }
        }
        None
    }

    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        todo!()
    }

    pub(crate) fn get_actor(&self, idx: usize) -> &ActorId {
        //&self.actors[usize::from(idx)]
        &self.actors[idx]
    }

    pub(crate) fn get_actor_safe(&self, idx: usize) -> Option<&ActorId> {
        //self.actors.get(usize::from(idx))
        self.actors.get(idx)
    }

    pub(crate) fn lookup_actor(&self, actor: &ActorId) -> Option<usize> {
        self.actors.binary_search(actor).ok() // .map(ActorIdx::from)
    }

    pub(crate) fn put_actor(&mut self, actor: ActorId) -> usize {
        match self.actors.binary_search(&actor) {
            Ok(idx) => idx, //ActorIdx::from(idx),
            Err(idx) => {
                self.actors.insert(idx, actor);
                for (spec, col) in &mut self.cols.0 {
                    match col {
                        Column::Actor(col_data) => {
                            let new_ids = col_data
                                .iter()
                                .map(|a| match a {
                                    Some(ActorIdx(id)) if id as usize >= idx => {
                                        Some(ActorIdx(id + 1))
                                    }
                                    old => old,
                                })
                                .collect::<Vec<_>>();
                            let mut new_data = ColumnData::<ActorCursor>::new();
                            new_data.splice(0, new_ids);
                            std::mem::swap(col_data, &mut new_data);
                        }
                        _ => {}
                    }
                }
                idx //ActorIdx::from(idx)
            }
        }
    }

    pub(crate) fn new(doc: &Document<'_>) -> Self {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = Arc::new(doc.op_raw_bytes().to_vec());
        let actors = doc.actors().to_vec();
        Self::from_parts(doc.op_metadata.raw_columns(), data, actors)
    }

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
    ) -> Self {
        let cols = Columns(
            cols.iter()
                .map(|c| {
                    (
                        c.spec(),
                        Column::external(c.spec(), data.clone(), c.data()).unwrap(),
                    )
                })
                .collect(),
        );
        let len = cols.len();
        let op_set = OpSet { actors, cols, len };
        op_set
    }

    fn export(&self) -> (RawColumns<compression::Uncompressed>, Vec<u8>) {
        let mut data = vec![]; // should be able to do with_capacity here
        let mut raw = vec![];
        for (spec, c) in &self.cols {
            let range = c.write(&mut data);
            if !range.is_empty() {
                raw.push(RawColumn::new(*spec, range));
            }
        }
        (raw.into_iter().collect(), data)
    }

    pub(crate) fn iter_prop<'a>(&'a self, obj: &ObjId, prop: &str) -> OpIter<'a, Verified> {
        let range = self
            .cols
            .get_integer(OBJ_ID_COUNTER_COL_SPEC)
            .scope_to_value(obj.counter(), ..);
        let range = self
            .cols
            .get_actor(OBJ_ID_ACTOR_COL_SPEC)
            .scope_to_value(ActorIdx::from(obj.actor() as usize), range);
        let range = self
            .cols
            .get_str(KEY_STR_COL_SPEC)
            .scope_to_value(prop, range);
        self.iter_range(&range)
    }

    pub(crate) fn query_opid<'a>(&'a self, id: &OpId) -> Option<Op<'a>> {
        self.iter().find(|op| &op.id == id)
    }

    pub(crate) fn iter_obj<'a>(&'a self, obj: &ObjId) -> OpIter<'a, Verified> {
        /*
                let range = self.get_obj_ctr().scope_to_value(obj.counter(), ..);
                let actor = ActorIdx::from(obj.actor() as usize);
                let range = self.get_obj_actor().scope_to_value(actor, range);
        */
        let range = self
            .cols
            .get_integer(OBJ_ID_COUNTER_COL_SPEC)
            .scope_to_value(obj.counter(), ..);
        let range = self
            .cols
            .get_actor(OBJ_ID_ACTOR_COL_SPEC)
            .scope_to_value(ActorIdx::from(obj.actor() as usize), range);
        self.iter_range(&range)
    }

    pub(crate) fn iter_range<'a>(&'a self, range: &Range<usize>) -> OpIter<'_, Verified> {
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
            index: range.start,
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
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn iter(&self) -> OpIter<'_, Verified> {
        OpIter {
            index: 0,
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
            _phantom: std::marker::PhantomData,
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
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Parent {
    pub(crate) obj: ObjId,
    pub(crate) typ: ObjType,
    pub(crate) prop: Prop,
    pub(crate) visible: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FoundOpId<'a> {
    pub(crate) op: Op<'a>,
    pub(crate) index: usize,
    pub(crate) visible: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpsFound<'a> {
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) end_pos: usize,
}

#[derive(Debug, Default, Clone)]
struct Columns(BTreeMap<ColumnSpec, Column>);

impl Columns {
    fn new<'a, I: Iterator<Item = super::op::Op<'a>> + Clone>(ops: I) -> Self {
        let mut columns = BTreeMap::new();

        let mut id_actor = ColumnData::<ActorCursor>::new();
        id_actor.splice(
            0,
            ops.clone()
                .map(|op| ActorIdx::from(op.id.actor()))
                .collect::<Vec<_>>(),
        );
        columns.insert(ID_ACTOR_COL_SPEC, Column::Actor(id_actor));

        let mut id_counter = ColumnData::<DeltaCursor>::new();
        id_counter.splice(
            0,
            ops.clone()
                .map(|op| op.id.counter() as i64)
                .collect::<Vec<_>>(),
        );
        columns.insert(ID_COUNTER_COL_SPEC, Column::Delta(id_counter));

        let mut obj_actor_col = ColumnData::<ActorCursor>::new();
        obj_actor_col.splice(
            0,
            ops.clone()
                .map(|op| ActorIdx::from(op.obj.0.actor()))
                .collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_ACTOR_COL_SPEC, Column::Actor(obj_actor_col));

        let mut obj_counter_col = ColumnData::<IntCursor>::new();
        obj_counter_col.splice(
            0,
            ops.clone().map(|op| op.obj.0.counter()).collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_COUNTER_COL_SPEC, Column::Integer(obj_counter_col));

        let mut key_actor = ColumnData::<ActorCursor>::new();
        key_actor.splice(
            0,
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(_) => None,
                    Key::Seq(e) => {
                        if e.is_head() {
                            None
                        } else {
                            Some(ActorIdx::from(e.0.actor()))
                        }
                    }
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_ACTOR_COL_SPEC, Column::Actor(key_actor));

        let mut key_counter = ColumnData::<DeltaCursor>::new();
        key_counter.splice(
            0,
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(_) => None,
                    Key::Seq(e) => {
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
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(s) => Some(s),
                    Key::Seq(_) => None,
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_STR_COL_SPEC, Column::Str(key_str));

        let mut succ_count = ColumnData::<IntCursor>::new();
        succ_count.splice(
            0,
            ops.clone()
                .map(|op| op.succ().len() as u64)
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNT_COL_SPEC, Column::Group(succ_count));

        let mut succ_actor = ColumnData::<ActorCursor>::new();
        succ_actor.splice(
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| ActorIdx::from(n.actor())))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_ACTOR_COL_SPEC, Column::Actor(succ_actor));

        let mut succ_counter = ColumnData::<DeltaCursor>::new();
        succ_counter.splice(
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| n.counter() as i64))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNTER_COL_SPEC, Column::Delta(succ_counter));

        let mut insert = ColumnData::<BooleanCursor>::new();
        insert.splice(0, ops.clone().map(|op| op.insert).collect::<Vec<_>>());
        columns.insert(INSERT_COL_SPEC, Column::Bool(insert));

        let mut action = ColumnData::<ActionCursor>::new();
        action.splice(0, ops.clone().map(|op| op.action).collect::<Vec<_>>());
        log!(
            "Action IN {:?}",
            ops.clone().map(|op| op.action).collect::<Vec<_>>()
        );
        log!("Action OUT {:?}", action.iter().collect::<Vec<_>>());
        columns.insert(ACTION_COL_SPEC, Column::Action(action));

        let mut value_meta = ColumnData::<MetaCursor>::new();
        value_meta.splice(
            0,
            ops.clone()
                .map(|op| ValueMeta::from(&op.value))
                .collect::<Vec<_>>(),
        );
        columns.insert(VALUE_META_COL_SPEC, Column::ValueMeta(value_meta));

        let mut value = ColumnData::<RawCursor>::new();
        let values = ops
            .clone()
            .filter_map(|op| op.value.to_raw())
            .collect::<Vec<_>>();
        value.splice(0, values);
        columns.insert(VALUE_COL_SPEC, Column::Value(value));

        let mut mark_name = ColumnData::<StrCursor>::new();
        mark_name.splice(
            0,
            ops.clone()
                .map(|op| op.mark_name.clone())
                .collect::<Vec<_>>(),
        );
        columns.insert(MARK_NAME_COL_SPEC, Column::Str(mark_name));

        let mut expand = ColumnData::<BooleanCursor>::new();
        expand.splice(0, ops.clone().map(|op| op.expand).collect::<Vec<_>>());
        columns.insert(EXPAND_COL_SPEC, Column::Bool(expand));

        Columns(columns)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.get(&ID_ACTOR_COL_SPEC).map(|c| c.len()).unwrap_or(0)
    }

    fn get_actor_coldata(&self, spec: ColumnSpec) -> &Column {
        self.0.get(&spec).unwrap()
    }

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

    fn get_coldata(&self, spec: ColumnSpec) -> &[Slab] {
        self.0.get(&spec).unwrap().slabs()
    }

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
}

impl<'a> Iterator for &'a Columns {
    type Item = (&'a ColumnSpec, &'a Column);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter().next()
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
                Some((
                    ObjId(OpId::new(run1.value?, run2.value?.into())),
                    start..end,
                ))
            }
            (None, None) => None,
            _ => panic!(),
        }
    }
}

// Stick all of the column ID initialization in a module so we can turn off
// rustfmt for the whole thing
#[rustfmt::skip]
mod ids {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const OBJ_COL_ID:                ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID:                ColumnId = ColumnId::new(1);
    pub(super) const ID_COL_ID:                 ColumnId = ColumnId::new(2);
    pub(super) const INSERT_COL_ID:             ColumnId = ColumnId::new(3);
    pub(in crate::op_set2) const ACTION_COL_ID: ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID:                ColumnId = ColumnId::new(5);
    pub(super) const SUCC_COL_ID:               ColumnId = ColumnId::new(8);
    pub(super) const EXPAND_COL_ID:             ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID:          ColumnId = ColumnId::new(10);

    pub(super) const ID_ACTOR_COL_SPEC:       ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(super) const ID_COUNTER_COL_SPEC:     ColumnSpec = ColumnSpec::new_delta(ID_COL_ID);
    pub(super) const OBJ_ID_ACTOR_COL_SPEC:   ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_ID_COUNTER_COL_SPEC: ColumnSpec = ColumnSpec::new_integer(OBJ_COL_ID);
    pub(super) const KEY_ACTOR_COL_SPEC:      ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_COUNTER_COL_SPEC:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR_COL_SPEC:        ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(super) const SUCC_COUNT_COL_SPEC:     ColumnSpec = ColumnSpec::new_group(SUCC_COL_ID);
    pub(super) const SUCC_ACTOR_COL_SPEC:     ColumnSpec = ColumnSpec::new_actor(SUCC_COL_ID);
    pub(super) const SUCC_COUNTER_COL_SPEC:   ColumnSpec = ColumnSpec::new_delta(SUCC_COL_ID);
    pub(super) const INSERT_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(INSERT_COL_ID);
    pub(super) const ACTION_COL_SPEC:         ColumnSpec = ColumnSpec::new_integer(ACTION_COL_ID);
    pub(super) const VALUE_META_COL_SPEC:     ColumnSpec = ColumnSpec::new_value_metadata(VAL_COL_ID);
    pub(super) const VALUE_COL_SPEC:          ColumnSpec = ColumnSpec::new_value(VAL_COL_ID);
    pub(super) const MARK_NAME_COL_SPEC:      ColumnSpec = ColumnSpec::new_string(MARK_NAME_COL_ID);
    pub(super) const EXPAND_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(EXPAND_COL_ID);

    pub(super) const ALL_COLUMN_SPECS: [ColumnSpec; 16] = [
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
    use std::sync::Arc;

    use proptest::{
        arbitrary::any,
        prop_compose, prop_oneof,
        strategy::{Just, Strategy},
    };

    use crate::{
        indexed_cache::IndexedCache,
        op_set2::{
            columns::ColumnData,
            op::SuccCursors,
            rle::ActorCursor,
            types::{Action, ActorIdx, ScalarValue},
            ColumnCursor, DeltaCursor, Key, Slab, WritableSlab,
        },
        storage::Document,
        transaction::Transactable,
        types::{ObjId, OpBuilder, OpId},
        ActorId, AutoCommit, ObjType, OpType,
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
        let opset = super::OpSet::new(&doc_chunk);
        let ops = opset.iter().collect::<Vec<_>>();
        let actual_ops = doc
            .doc
            .ops()
            .iter()
            .map(|(_, _, op)| op)
            .collect::<Vec<_>>();
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
        key: Key<'static>,
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
            test_ops
                .iter()
                .map(|o| o.succs.len() as u64)
                .collect::<Vec<_>>(),
        );
        succ_actor_data.splice(
            0,
            test_ops
                .iter()
                .flat_map(|o| o.succs.iter().map(|s| ActorIdx::from(s.actor())))
                .collect::<Vec<_>>(),
        );
        succ_counter_data.splice(
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
                index: 0, // not relevent for this equality test
                id: test_op.id,
                obj: test_op.obj,
                action: test_op.action,
                value: test_op.value.clone(),
                key: test_op.key.clone(),
                insert: test_op.insert,
                expand: test_op.expand,
                mark_name: test_op.mark_name,
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
                key: Key::Map("key"),
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
                key: Key::Map("key1"),
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
                key: Key::Map("key2"),
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
                key: Key::Map("inner_key1"),
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
                key: Key::Map("inner_key2"),
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
                .scope_to_value(1, ..);
            let range = opset
                .cols
                .get_actor(OBJ_ID_ACTOR_COL_SPEC)
                .scope_to_value(ActorIdx::from(1 as usize), range);
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
        use super::super::op_set::iter::OpScope;
        let actors = vec![crate::ActorId::random(), crate::ActorId::random()];

        let test_ops = vec![
            TestOp {
                id: OpId::new(1, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: Key::Map("map"),
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
                key: Key::Map("list"),
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
                key: Key::Map("key1"),
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
                key: Key::Map("key2"),
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
                key: Key::Map("key2"),
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
                key: Key::Map("key2"),
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
                key: Key::Map("key3"),
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
                key: Key::Map("key3"),
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
                key: Key::Seq(ElemId::head()),
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
                key: Key::Seq(ElemId(OpId::new(8, 1))),
                insert: true,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &test_ops, |opset| {
            let mut iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[2..8], ops.as_slice());

            let mut iter = opset.iter_prop(&ObjId(OpId::new(1, 1)), "key2");
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[3..6], ops.as_slice());

            let mut iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.top_ops().collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());

            let mut iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
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
                .visible_ops(None)
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

            let mut iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.visible_ops(None).top_ops().collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());
        });
    }

    proptest::proptest! {
        #[test]
        fn column_data_same_as_old_encoding(Scenario{opset, actors, keys} in arbitrary_opset()) {

            // encode with old encoders
            let actor_lookup = actors
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
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

    struct Scenario {
        opset: crate::op_set::OpSetInternal,
        actors: Vec<crate::ActorId>,
        keys: IndexedCache<String>,
    }

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
                    crate::marks::MarkData {
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
}
