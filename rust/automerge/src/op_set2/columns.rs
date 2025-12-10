use super::meta::MetaCursor;
use super::op::OpLike;
use super::op_set::MarkIndexColumn;
use super::types::{Action, ActionCursor, ActorCursor, ActorIdx, ScalarValue};
use crate::storage::columns::compression::Uncompressed;
use crate::storage::columns::ColumnId;
use crate::storage::columns::{BadColumnLayout, Columns as ColumnFormat};
use crate::storage::ColumnSpec;
use crate::storage::{RawColumn, RawColumns};
use crate::types::{ActorId, SequenceType, TextEncoding};
use hexane::{
    BooleanCursor, ColumnCursor, ColumnData, DeltaCursor, IntCursor, PackError, RawCursor,
    StrCursor, UIntCursor,
};

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Clone)]
pub(super) struct Columns {
    pub(super) id_actor: ColumnData<ActorCursor>,
    pub(super) id_ctr: ColumnData<DeltaCursor>,
    pub(super) obj_actor: ColumnData<ActorCursor>,
    pub(super) obj_ctr: ColumnData<UIntCursor>,
    pub(super) key_actor: ColumnData<ActorCursor>,
    pub(super) key_ctr: ColumnData<DeltaCursor>,
    pub(super) key_str: ColumnData<StrCursor>,
    pub(super) succ_count: ColumnData<UIntCursor>,
    pub(super) succ_actor: ColumnData<ActorCursor>,
    pub(super) succ_ctr: ColumnData<DeltaCursor>,
    pub(super) insert: ColumnData<BooleanCursor>,
    pub(super) action: ColumnData<ActionCursor>,
    pub(super) value_meta: ColumnData<MetaCursor>,
    pub(super) value: ColumnData<RawCursor>,
    pub(super) mark_name: ColumnData<StrCursor>,
    pub(super) expand: ColumnData<BooleanCursor>,
    pub(super) index: Indexes,
}

#[derive(Debug, Clone)]
pub(super) struct Indexes {
    pub(super) text: ColumnData<UIntCursor>,
    pub(super) top: ColumnData<BooleanCursor>,
    pub(super) visible: ColumnData<BooleanCursor>,
    pub(super) inc: ColumnData<IntCursor>,
    pub(super) mark: MarkIndexColumn,
}

impl Default for Indexes {
    fn default() -> Self {
        Self {
            text: ColumnData::new(),
            top: ColumnData::new(),
            visible: ColumnData::new(),
            inc: ColumnData::new(),
            mark: MarkIndexColumn::new(),
        }
    }
}

impl Default for Columns {
    fn default() -> Self {
        Self {
            id_actor: ColumnData::new(),
            id_ctr: ColumnData::new(),
            obj_actor: ColumnData::new(),
            obj_ctr: ColumnData::new(),
            key_actor: ColumnData::new(),
            key_ctr: ColumnData::new(),
            key_str: ColumnData::new(),
            succ_count: ColumnData::new(),
            succ_actor: ColumnData::new(),
            succ_ctr: ColumnData::new(),
            insert: ColumnData::new(),
            action: ColumnData::new(),
            value_meta: ColumnData::new(),
            value: ColumnData::new(),
            mark_name: ColumnData::new(),
            expand: ColumnData::new(),
            index: Indexes::default(),
        }
    }
}

#[cfg(test)]
fn debug_cmp<I: Debug + PartialEq>(tag: &str, a: Vec<I>, b: Vec<I>) -> bool {
    if a == b {
        true
    } else {
        let pos = a.iter().zip(b.iter()).position(|(a, b)| a != b);
        log!("{} diff at {:?}", tag, pos);
        log!(" :: {:?}", b);
        log!(" :: {:?}", a);
        false
    }
}

impl Columns {
    #[cfg(test)]
    pub(super) fn debug_cmp(&self, other: &Self) {
        let mut ok = true;
        ok &= debug_cmp("ID_ACTOR", self.id_actor.to_vec(), other.id_actor.to_vec());
        ok &= debug_cmp("ID_CTR", self.id_ctr.to_vec(), other.id_ctr.to_vec());
        ok &= debug_cmp(
            "OBJ_ACTOR",
            self.obj_actor.to_vec(),
            other.obj_actor.to_vec(),
        );
        ok &= debug_cmp("OBJ_CTR", self.obj_ctr.to_vec(), other.obj_ctr.to_vec());
        assert!(ok);
        log!("KEY_ACTOR");
        assert_eq!(self.key_actor.to_vec(), other.key_actor.to_vec());
        log!("KEY_STR");
        assert_eq!(self.key_str.to_vec(), other.key_str.to_vec());
        log!("INSERT");
        assert_eq!(self.insert.to_vec(), other.insert.to_vec());
        log!("ACTION");
        assert_eq!(self.action.to_vec(), other.action.to_vec());
        log!("MARK_NAME");
        assert_eq!(self.mark_name.to_vec(), other.mark_name.to_vec());
        log!("EXPAND");
        assert_eq!(self.expand.to_vec(), other.expand.to_vec());
        log!("SUCC_COUNT");
        assert_eq!(self.succ_count.to_vec(), other.succ_count.to_vec());
        log!("SUCC_ACTOR");
        assert_eq!(self.succ_actor.to_vec(), other.succ_actor.to_vec());
        log!("SUCC_CTR");
        assert_eq!(self.succ_ctr.to_vec(), other.succ_ctr.to_vec());
        log!("META");
        assert_eq!(self.value_meta.to_vec(), other.value_meta.to_vec());
        log!("VALUE");
        assert_eq!(self.value.to_vec(), other.value.to_vec());
    }

    fn write_unless_empty<C: ColumnCursor>(
        spec: &ColumnSpec,
        c: &ColumnData<C>,
        data: &mut Vec<u8>,
    ) -> Option<RawColumn<Uncompressed>> {
        if !c.is_empty() || spec.id() == ColumnId::new(3) {
            let range = c.save_to(data);
            if !range.is_empty() {
                return Some(RawColumn::new(*spec, range));
            }
        }
        None
    }

    fn export_column(
        &self,
        spec: &ColumnSpec,
        data: &mut Vec<u8>,
    ) -> Option<RawColumn<Uncompressed>> {
        match *spec {
            ID_ACTOR_COL_SPEC => Self::write_unless_empty(spec, &self.id_actor, data),
            ID_COUNTER_COL_SPEC => Self::write_unless_empty(spec, &self.id_ctr, data),
            OBJ_ID_ACTOR_COL_SPEC => Self::write_unless_empty(spec, &self.obj_actor, data),
            OBJ_ID_COUNTER_COL_SPEC => Self::write_unless_empty(spec, &self.obj_ctr, data),
            KEY_ACTOR_COL_SPEC => Self::write_unless_empty(spec, &self.key_actor, data),
            KEY_COUNTER_COL_SPEC => Self::write_unless_empty(spec, &self.key_ctr, data),
            KEY_STR_COL_SPEC => Self::write_unless_empty(spec, &self.key_str, data),
            INSERT_COL_SPEC => Self::write_unless_empty(spec, &self.insert, data),
            ACTION_COL_SPEC => Self::write_unless_empty(spec, &self.action, data),
            MARK_NAME_COL_SPEC => Self::write_unless_empty(spec, &self.mark_name, data),
            EXPAND_COL_SPEC => Self::write_unless_empty(spec, &self.expand, data),
            SUCC_COUNT_COL_SPEC => Self::write_unless_empty(spec, &self.succ_count, data),
            SUCC_ACTOR_COL_SPEC => Self::write_unless_empty(spec, &self.succ_actor, data),
            SUCC_COUNTER_COL_SPEC => Self::write_unless_empty(spec, &self.succ_ctr, data),
            VALUE_META_COL_SPEC => Self::write_unless_empty(spec, &self.value_meta, data),
            VALUE_COL_SPEC => Self::write_unless_empty(spec, &self.value, data),
            _ => None,
        }
    }

    pub(crate) fn export(&self) -> (RawColumns<Uncompressed>, Vec<u8>) {
        let mut data = vec![];

        let mut cols = ALL_COLUMN_SPECS;
        cols.sort();

        let raw = cols
            .iter()
            .filter_map(|spec| self.export_column(spec, &mut data))
            .collect();

        (raw, data)
    }

    pub(crate) fn validate(
        bytes: usize,
        cols: &RawColumns<Uncompressed>,
    ) -> Result<RawColumns<Uncompressed>, BadColumnLayout> {
        use ids::*;
        let _ = ColumnFormat::parse2(bytes, cols.iter())?;
        Ok(cols
            .iter()
            .filter(|col| {
                matches!(
                    col.spec(),
                    ID_ACTOR_COL_SPEC
                        | ID_COUNTER_COL_SPEC
                        | OBJ_ID_ACTOR_COL_SPEC
                        | OBJ_ID_COUNTER_COL_SPEC
                        | KEY_ACTOR_COL_SPEC
                        | KEY_COUNTER_COL_SPEC
                        | KEY_STR_COL_SPEC
                        | SUCC_COUNT_COL_SPEC
                        | SUCC_ACTOR_COL_SPEC
                        | SUCC_COUNTER_COL_SPEC
                        | INSERT_COL_SPEC
                        | ACTION_COL_SPEC
                        | VALUE_META_COL_SPEC
                        | VALUE_COL_SPEC
                        | MARK_NAME_COL_SPEC
                        | EXPAND_COL_SPEC
                )
            })
            .cloned()
            .collect())
    }

    fn load_column<C, T>(
        spec: ColumnSpec,
        cols: &BTreeMap<ColumnSpec, Range<usize>>,
        data: &[u8],
        test: &T,
        len: usize,
    ) -> Result<ColumnData<C>, PackError>
    where
        C: ColumnCursor,
        T: Fn(Option<&C::Item>) -> Option<String>,
    {
        if let Some(range) = cols.get(&spec) {
            ColumnData::load_with(&data[range.clone()], test)
        } else {
            Ok(ColumnData::init_empty(len))
        }
    }

    pub(crate) fn load(
        cols: BTreeMap<ColumnSpec, Range<usize>>,
        data: &[u8],
        actors: &[ActorId],
    ) -> Result<Self, PackError> {
        use crate::validation::{ivalid_u32 as ictr, nil, valid_actors, valid_u32 as ctr};
        let act = valid_actors(actors.len());

        let id_actor = Self::load_column(ID_ACTOR_COL_SPEC, &cols, data, &act, 0)?;
        let len = id_actor.len();

        let id_ctr = Self::load_column(ID_COUNTER_COL_SPEC, &cols, data, &ictr, len)?;
        let obj_actor = Self::load_column(OBJ_ID_ACTOR_COL_SPEC, &cols, data, &act, len)?;
        let obj_ctr = Self::load_column(OBJ_ID_COUNTER_COL_SPEC, &cols, data, &ctr, len)?;
        let key_actor = Self::load_column(KEY_ACTOR_COL_SPEC, &cols, data, &act, len)?;
        let key_ctr = Self::load_column(KEY_COUNTER_COL_SPEC, &cols, data, &ictr, len)?;
        let key_str = Self::load_column(KEY_STR_COL_SPEC, &cols, data, &nil, len)?;
        let insert = Self::load_column(INSERT_COL_SPEC, &cols, data, &nil, len)?;
        let action = Self::load_column(ACTION_COL_SPEC, &cols, data, &nil, len)?;
        let mark_name = Self::load_column(MARK_NAME_COL_SPEC, &cols, data, &nil, len)?;
        let expand = Self::load_column(EXPAND_COL_SPEC, &cols, data, &nil, len)?;

        let succ_count = Self::load_column(SUCC_COUNT_COL_SPEC, &cols, data, &nil, len)?;
        let succ_len = succ_count.acc().as_usize();
        let succ_actor = Self::load_column(SUCC_ACTOR_COL_SPEC, &cols, data, &act, succ_len)?;
        let succ_ctr = Self::load_column(SUCC_COUNTER_COL_SPEC, &cols, data, &ictr, succ_len)?;

        let value_meta = Self::load_column(VALUE_META_COL_SPEC, &cols, data, &nil, len)?;
        let value_len = value_meta.acc().as_usize();
        let value = Self::load_column(VALUE_COL_SPEC, &cols, data, &nil, value_len)?;

        let index = Indexes::default();

        Ok(Self {
            id_actor,
            id_ctr,
            obj_actor,
            obj_ctr,
            key_actor,
            key_ctr,
            key_str,
            succ_count,
            succ_actor,
            succ_ctr,
            insert,
            action,
            value_meta,
            value,
            mark_name,
            expand,
            index,
        })
    }

    fn remap_actors<F>(&mut self, f: F)
    where
        F: Fn(Option<Cow<'_, ActorIdx>>) -> Option<Cow<'_, ActorIdx>>,
    {
        self.id_actor.remap(&f);
        self.obj_actor.remap(&f);
        self.key_actor.remap(&f);
        self.succ_actor.remap(&f);
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        let idx = idx as u32;
        self.remap_actors(move |a| match a.as_deref() {
            Some(&ActorIdx(actor)) if actor >= idx => Some(Cow::Owned(ActorIdx(actor + 1))),
            _ => a,
        });
    }

    pub(crate) fn rewrite_without_actor(&mut self, idx: usize) {
        let idx = idx as u32;
        self.remap_actors(move |a| match a.as_deref() {
            Some(&ActorIdx(id)) if id > idx => Some(Cow::Owned(ActorIdx(id - 1))),
            Some(&ActorIdx(id)) if id == idx => {
                panic!("cant rewrite - actor is present")
            }
            _ => a,
        });
    }

    pub(crate) fn splice<O>(&mut self, pos: usize, ops: &[O], text_encoding: TextEncoding) -> usize
    where
        O: OpLike,
    {
        let ops = ops.iter().filter(|o| O::action(o) != Action::Delete);
        self.id_actor.splice(pos, 0, ops.clone().map(O::id_actor));
        self.id_ctr.splice(pos, 0, ops.clone().map(O::id_ctr));
        self.obj_actor.splice(pos, 0, ops.clone().map(O::obj_actor));
        self.obj_ctr.splice(pos, 0, ops.clone().map(O::obj_ctr));
        self.key_actor.splice(pos, 0, ops.clone().map(O::key_actor));
        self.key_ctr.splice(pos, 0, ops.clone().map(O::key_ctr));
        self.key_str.splice(pos, 0, ops.clone().map(O::key_str));
        self.insert.splice(pos, 0, ops.clone().map(O::insert));
        self.action.splice(pos, 0, ops.clone().map(O::action));
        self.expand.splice(pos, 0, ops.clone().map(O::expand));
        self.mark_name.splice(pos, 0, ops.clone().map(O::mark_name));

        let value_pos = self
            .value_meta
            .splice(pos, 0, ops.clone().map(|o| o.meta_value()))
            .as_usize();

        self.value
            .splice(value_pos, 0, ops.clone().filter_map(|o| o.raw_value()));

        let succ_pos = self
            .succ_count
            .splice(pos, 0, ops.clone().map(|o| o.succ().len() as u64))
            .as_usize();

        let succ_actor = ops
            .clone()
            .flat_map(|o| o.succ().map(|id| id.actoridx()))
            .collect::<Vec<_>>();

        self.succ_actor.splice(succ_pos, 0, &succ_actor);

        let succ_ctr = ops
            .clone()
            .flat_map(|o| o.succ().map(|id| id.icounter()))
            .collect::<Vec<_>>();

        self.succ_ctr.splice(succ_pos, 0, succ_ctr);

        self.index
            .inc
            .splice(succ_pos, 0, ops.clone().flat_map(O::succ_inc));

        self.index
            .mark
            .splice(pos, 0, ops.clone().map(O::mark_index).collect());
        self.index.text.splice(
            pos,
            0,
            ops.clone()
                .map(|s| O::width(s, SequenceType::Text, text_encoding)),
        );
        self.index.top.splice(pos, 0, ops.clone().map(O::top));
        self.index
            .visible
            .splice(pos, 0, ops.clone().map(O::visible));

        ops.count()
    }

    #[cfg(test)]
    pub(crate) fn new<'a, I: Iterator<Item = super::op::Op<'a>> + ExactSizeIterator + Clone>(
        ops: I,
    ) -> Self {
        let mut op_set = Self::default();
        let ops: Vec<_> = ops.collect();
        op_set.splice(0, &ops, TextEncoding::platform_default());
        op_set
    }

    pub(crate) fn len(&self) -> usize {
        self.id_actor.len()
    }

    pub(crate) fn sub_len(&self) -> usize {
        self.succ_actor.len()
    }

    pub(crate) fn dump(&self) {
        let mut id_a = self.id_actor.iter();
        let mut id_c = self.id_ctr.iter();
        let mut act = self.action.iter();
        let mut obj_a = self.obj_actor.iter();
        let mut obj_c = self.obj_ctr.iter();
        let mut key_str = self.key_str.iter();
        let mut key_a = self.key_actor.iter();
        let mut key_c = self.key_ctr.iter();
        let mut meta = self.value_meta.iter();
        let mut value = self.value.raw_reader(0);
        let mut succ = self.succ_count.iter();
        let mut insert = self.insert.iter();
        let mut text = self.index.text.iter();
        let mut vis = self.index.visible.iter();
        let mut top = self.index.top.iter();
        let mut pos = 0;
        log!("::::: id       obj      key        elem     i v t tx act suc value");
        loop {
            let id_a = fmt(id_a.next());
            let id_c = fmt(id_c.next());
            let obj_a = fmt(obj_a.next());
            let obj_c = fmt(obj_c.next());
            let act = fmt(act.next());
            let insert = fmt_bool(insert.next());
            let text = fmt(text.next());
            let vis = fmt_bool(vis.next());
            let top = fmt_bool(top.next());
            let key_s = fmt(key_str.next());
            let key_a = fmt(key_a.next());
            let key_c = fmt(key_c.next());
            let succ = fmt(succ.next());
            let m = meta.next();
            let v = if let Some(Some(m)) = m {
                let raw_data = value.read_next(m.length()).unwrap_or(&[]);
                ScalarValue::from_raw(*m, raw_data).unwrap()
            } else {
                ScalarValue::Null
            };
            if id_a == NONE && id_c == NONE && obj_a == NONE && obj_c == NONE {
                break;
            }
            log!(
                "{:4}: {:8} {:8} {:10} {:8} {:1} {:1} {:1} {:2} {:3} {:1}   {}",
                pos,
                format!("({}, {})", id_c, id_a),
                format!("({}, {})", obj_c, obj_a),
                key_s,
                format!("({}, {})", key_c, key_a),
                insert,
                vis,
                top,
                text,
                act,
                succ,
                v,
            );
            pos += 1;
        }
    }
}

const NONE: &str = ".";

fn fmt<T: std::fmt::Display + hexane::Packable + ?Sized>(t: Option<Option<Cow<'_, T>>>) -> String {
    match t {
        None => NONE.to_owned(),
        Some(None) => "-".to_owned(),
        Some(Some(t)) => format!("{}", t.as_ref()).to_owned(),
    }
}

/*
#[derive(Debug, Clone)]
pub(crate) enum Column {
    Actor(ColumnData<ActorCursor>),
    Str(ColumnData<StrCursor>),
    Integer(ColumnData<UIntCursor>),
    Action(ColumnData<ActionCursor>),
    Delta(ColumnData<DeltaCursor>),
    Bool(ColumnData<BooleanCursor>),
    ValueMeta(ColumnData<MetaCursor>),
    Value(ColumnData<RawCursor>),
    Group(ColumnData<UIntCursor>),
}

impl Column {
    // FIXME
    /*
        pub(crate) fn splice(&mut self, mut index: usize, op: &OpBuilder) {
            todo!()
            match self {
                Self::Actor(col) => col.write(out),
                Self::Str(col) => col.write(out),
                Self::Integer(col) => col.write(out),
                Self::Delta(col) => col.write(out),
                Self::Bool(col) => col.write(out),
                Self::ValueMeta(col) => col.write(out),
                Self::Value(col) => col.write(out),
                Self::Group(col) => col.write(out),
                Self::Action(col) => col.write(out),
            }
        }
    */

    pub(crate) fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        match self {
            Self::Actor(col) => col.write(out),
            Self::Str(col) => col.write(out),
            Self::Integer(col) => col.write(out),
            Self::Delta(col) => col.write(out),
            Self::Bool(col) => col.write(out),
            Self::ValueMeta(col) => col.write(out),
            Self::Value(col) => col.write(out),
            Self::Group(col) => col.write(out),
            Self::Action(col) => col.write(out),
        }
    }

    pub(crate) fn slabs(&self) -> &SlabTree<SlabWeight> {
        match self {
            Self::Actor(col) => &col.slabs,
            Self::Str(col) => &col.slabs,
            Self::Integer(col) => &col.slabs,
            Self::Delta(col) => &col.slabs,
            Self::Bool(col) => &col.slabs,
            Self::ValueMeta(col) => &col.slabs,
            Self::Value(col) => &col.slabs,
            Self::Group(col) => &col.slabs,
            Self::Action(col) => &col.slabs,
        }
    }

    #[allow(unused)]
    pub(crate) fn dump(&self) {
        match self {
            Self::Actor(col) => col.dump(),
            Self::Str(col) => col.dump(),
            Self::Integer(col) => col.dump(),
            Self::Delta(col) => col.dump(),
            Self::Bool(col) => col.dump(),
            Self::ValueMeta(col) => col.dump(),
            Self::Value(col) => col.dump(),
            Self::Group(col) => col.dump(),
            Self::Action(col) => col.dump(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Actor(col) => col.is_empty(),
            Self::Str(col) => col.is_empty(),
            Self::Integer(col) => col.is_empty(),
            Self::Delta(col) => col.is_empty(),
            Self::Bool(col) => col.is_empty(),
            Self::ValueMeta(col) => col.is_empty(),
            Self::Value(col) => col.is_empty(),
            Self::Group(col) => col.is_empty(),
            Self::Action(col) => col.is_empty(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Actor(col) => col.len,
            Self::Str(col) => col.len,
            Self::Integer(col) => col.len,
            Self::Delta(col) => col.len,
            Self::Bool(col) => col.len,
            Self::ValueMeta(col) => col.len,
            Self::Value(col) => col.len,
            Self::Group(col) => col.len,
            Self::Action(col) => col.len,
        }
    }

    pub(crate) fn new(spec: ColumnSpec) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::new()),
            ColumnType::String => Column::Str(ColumnData::new()),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::new())
                } else {
                    Column::Integer(ColumnData::new())
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::new()),
            ColumnType::Boolean => Column::Bool(ColumnData::new()),
            ColumnType::Group => Column::Group(ColumnData::new()),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::new()),
            ColumnType::Value => Column::Value(ColumnData::new()),
        }
    }

    pub(crate) fn external(
        spec: ColumnSpec,
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        actors: &[ActorId],
    ) -> Result<Self, PackError> {
        let m = ScanMeta {
            actors: actors.len(),
        };
        match spec.col_type() {
            ColumnType::Actor => Ok(Column::Actor(ColumnData::external(data, range, &m)?)),
            ColumnType::String => Ok(Column::Str(ColumnData::external(data, range, &m)?)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Ok(Column::Action(ColumnData::external(data, range, &m)?))
                } else {
                    Ok(Column::Integer(ColumnData::external(data, range, &m)?))
                }
            }
            ColumnType::DeltaInteger => Ok(Column::Delta(ColumnData::external(data, range, &m)?)),
            ColumnType::Boolean => Ok(Column::Bool(ColumnData::external(data, range, &m)?)),
            ColumnType::Group => Ok(Column::Group(ColumnData::external(data, range, &m)?)),
            ColumnType::ValueMetadata => {
                Ok(Column::ValueMeta(ColumnData::external(data, range, &m)?))
            }
            ColumnType::Value => Ok(Column::Value(ColumnData::external(data, range, &m)?)),
        }
    }

    pub(crate) fn init_empty(spec: ColumnSpec, len: usize) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::init_empty(len)),
            ColumnType::String => Column::Str(ColumnData::init_empty(len)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::init_empty(len))
                } else {
                    Column::Integer(ColumnData::init_empty(len))
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::init_empty(len)),
            ColumnType::Boolean => Column::Bool(ColumnData::init_empty(len)),
            ColumnType::Group => Column::Group(ColumnData::init_empty(len)),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::init_empty(len)),
            ColumnType::Value => Column::Value(ColumnData::init_empty(len)),
        }
    }
}
*/

fn fmt_bool(val: Option<Option<Cow<'_, bool>>>) -> &'static str {
    if val.flatten().as_deref() == Some(&true) {
        "t"
    } else {
        "-"
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
