use std::{borrow::Cow, convert::TryFrom};

use smol_str::SmolStr;

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, ObjIdEncoder, ObjIdIter, ObjIdRange, OpIdEncoder, OpIdIter,
            OpIdListEncoder, OpIdListIter, OpIdListRange, OpIdRange, RleRange, ValueEncoder,
            ValueIter, ValueRange, ElemRange, ElemEncoder, ElemIter,
        },
        encoding::{
            BooleanDecoder, BooleanEncoder, ColumnDecoder, DecodeColumnError, RleDecoder,
            RleEncoder,
        },
    },
    convert,
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
    types::{ElemId, ObjId, OpId, ScalarValue},
};

const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const ID_COL_ID: ColumnId = ColumnId::new(2);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const SUCC_COL_ID: ColumnId = ColumnId::new(8);

/// The form operations take in the compressed document format.
#[derive(Debug)]
pub(crate) struct DocOp {
    pub(crate) id: OpId,
    pub(crate) object: ObjId,
    pub(crate) prop: Option<SmolStr>,
    pub(crate) elem_id: Option<ElemId>,
    pub(crate) insert: bool,
    pub(crate) action: usize,
    pub(crate) value: ScalarValue,
    pub(crate) succ: Vec<OpId>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocOpColumns {
    obj: Option<ObjIdRange>,
    prop: RleRange<smol_str::SmolStr>,
    elem: ElemRange,
    id: OpIdRange,
    insert: BooleanRange,
    action: RleRange<u64>,
    val: ValueRange,
    succ: OpIdListRange,
    #[allow(dead_code)]
    other: Columns,
}

struct DocId {
    actor: usize,
    counter: u64,
}

impl convert::OpId<usize> for DocId {
    fn actor(&self) -> usize {
        self.actor
    }

    fn counter(&self) -> u64 {
        self.counter
    }
}

/// A row to be encoded as an op in the document format
///
/// The lifetime `'a` is the lifetime of the value and key data types. For types which cannot
/// provide a reference (e.g. because they are decoding from some columnar storage on each
/// iteration) this should be `'static`.
pub(crate) trait AsDocOp<'a> {
    /// The type of the Actor ID component of the op IDs for this impl. This is typically either
    /// `&'a ActorID` or `usize`
    type ActorId;
    /// The type of the op IDs this impl produces.
    type OpId: convert::OpId<Self::ActorId>;
    /// The type of the successor iterator returned by `Self::pred`. This can often be omitted
    type SuccIter: Iterator<Item = Self::OpId> + ExactSizeIterator;

    fn obj(&self) -> convert::ObjId<Self::OpId>;
    fn id(&self) -> Self::OpId;
    fn prop(&self) -> Option<Cow<'a, SmolStr>>;
    fn elem(&self) -> Option<convert::ElemId<Self::OpId>>;
    fn insert(&self) -> bool;
    fn action(&self) -> u64;
    fn val(&self) -> Cow<'a, ScalarValue>;
    fn succ(&self) -> Self::SuccIter;
}

impl DocOpColumns {
    pub(crate) fn encode<'a, I, C, O>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C> + Clone + ExactSizeIterator,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        Self::encode_rowwise(ops, out)
    }

    fn encode_columnwise<'a, I, O, C>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C> + Clone,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        let obj = ObjIdRange::encode(ops.clone().map(|o| o.obj()), out);
        let id = OpIdRange::encode(ops.clone().map(|o| o.id()), out);
        let prop = RleRange::encode(ops.clone().map(|o| o.prop()), out);
        let elem = ElemRange::maybe_encode(ops.clone().map(|o| o.elem()), out);
        let insert = BooleanRange::encode(ops.clone().map(|o| o.insert()), out);
        let action = RleRange::encode(ops.clone().map(|o| Some(o.action())), out);
        let val = ValueRange::encode(ops.clone().map(|o| o.val()), out);
        let succ = OpIdListRange::encode(ops.map(|o| o.succ()), out);
        Self {
            obj,
            prop,
            elem,
            id,
            insert,
            action,
            val,
            succ,
            other: Columns::empty(),
        }
    }

    fn encode_rowwise<'a, I, O, C>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C>,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        let mut obj = ObjIdEncoder::new();
        let mut prop = RleEncoder::<_, SmolStr>::new(Vec::new());
        let mut elem = ElemEncoder::new();
        let mut id = OpIdEncoder::new();
        let mut insert = BooleanEncoder::new();
        let mut action = RleEncoder::<_, u64>::from(Vec::new());
        let mut val = ValueEncoder::new();
        let mut succ = OpIdListEncoder::new();
        for op in ops {
            obj.append(op.obj());
            prop.append(op.prop());
            elem.append(op.elem());
            id.append(Some(op.id()));
            insert.append(op.insert());
            action.append(Some(op.action()));
            val.append(&op.val());
            succ.append(op.succ());
        }
        let obj = obj.finish(out);
        let id = id.finish(out);

        let elem = elem.finish(out);

        let prop_start = out.len();
        let (prop_out, _prop_end) = prop.finish();
        out.extend(prop_out);
        let prop = RleRange::from(prop_start..out.len());

        let insert_start = out.len();
        let (insert_out, _) = insert.finish();
        out.extend(insert_out);
        let insert = BooleanRange::from(insert_start..out.len());

        let action_start = out.len();
        let (action_out, _) = action.finish();
        out.extend(action_out);
        let action = RleRange::from(action_start..out.len());

        let val = val.finish(out);
        let succ = succ.finish(out);
        DocOpColumns {
            obj,
            id,
            elem,
            prop,
            insert,
            action,
            val,
            succ,
            other: Columns::empty(),
        }
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocOpColumnIter<'a> {
        DocOpColumnIter {
            id: self.id.iter(data),
            action: self.action.decoder(data),
            objs: self.obj.as_ref().map(|o| o.iter(data)),
            //keys: self.key.iter(data),
            elem: self.elem.iter(data),
            prop: self.prop.decoder(data),
            insert: self.insert.decoder(data),
            value: self.val.iter(data),
            succ: self.succ.iter(data),
        }
    }

    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        let mut cols = vec![
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Actor, false),
                self.obj
                    .as_ref()
                    .map(|o| o.actor_range().clone().into())
                    .unwrap_or(0..0),
            ),
            RawColumn::new(
                ColumnSpec::new(OBJ_COL_ID, ColumnType::Integer, false),
                self.obj
                    .as_ref()
                    .map(|o| o.counter_range().clone().into())
                    .unwrap_or(0..0),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::Actor, false),
                self.elem.actor_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false),
                self.elem.counter_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::String, false),
                self.prop.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::Actor, false),
                self.id.actor_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ID_COL_ID, ColumnType::DeltaInteger, false),
                self.id.counter_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(INSERT_COL_ID, ColumnType::Boolean, false),
                self.insert.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(ACTION_COL_ID, ColumnType::Integer, false),
                self.action.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::ValueMetadata, false),
                self.val.meta_range().clone().into(),
            ),
        ];
        if !self.val.raw_range().is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(VAL_COL_ID, ColumnType::Value, false),
                self.val.raw_range().clone().into(),
            ));
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(SUCC_COL_ID, ColumnType::Group, false),
            self.succ.group_range().clone().into(),
        ));
        if !self.succ.actor_range().is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::Actor, false),
                    self.succ.actor_range().clone().into(),
                ),
                RawColumn::new(
                    ColumnSpec::new(SUCC_COL_ID, ColumnType::DeltaInteger, false),
                    self.succ.counter_range().clone().into(),
                ),
            ]);
        }
        cols.into_iter().collect()
    }
}

#[derive(Clone)]
pub(crate) struct DocOpColumnIter<'a> {
    id: OpIdIter<'a>,
    action: RleDecoder<'a, u64>,
    objs: Option<ObjIdIter<'a>>,
    prop: RleDecoder<'a, SmolStr>,
    elem: ElemIter<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueIter<'a>,
    succ: OpIdListIter<'a>,
}

impl<'a> DocOpColumnIter<'a> {
    fn done(&self) -> bool {
        self.id.done()
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct ReadDocOpError(#[from] DecodeColumnError);

impl<'a> Iterator for DocOpColumnIter<'a> {
    type Item = Result<DocOp, ReadDocOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            match self.try_next() {
                Ok(Some(op)) => Some(Ok(op)),
                Ok(None) => None,
                Err(e) => Some(Err(e.into())),
            }
        }
    }
}

impl<'a> DocOpColumnIter<'a> {
    fn try_next(&mut self) -> Result<Option<DocOp>, DecodeColumnError> {
        if self.done() {
            Ok(None)
        } else {
            let id = self.id.next_in_col("id")?;
            let action = self.action.next_in_col("action")?;
            let obj = if let Some(ref mut objs) = self.objs {
                objs.next_in_col("obj")?
            } else {
                ObjId::root()
            };
            let prop = self.prop.maybe_next_in_col("key:prop")?;
            let elem_id = self.elem.maybe_next_in_col("key:elem")?;
            let value = self.value.next_in_col("value")?;
            let succ = self.succ.next_in_col("succ")?;
            let insert = self.insert.next_in_col("insert")?;
            Ok(Some(DocOp {
                id,
                value,
                action: action as usize,
                object: obj,
                elem_id: elem_id.map(|i| i.into()),
                prop,
                succ,
                insert,
            }))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("mismatching column at {index}.")]
    MismatchingColumn { index: usize },
}

impl From<MismatchingColumn> for Error {
    fn from(m: MismatchingColumn) -> Self {
        Error::MismatchingColumn { index: m.index }
    }
}

impl TryFrom<Columns> for DocOpColumns {
    type Error = Error;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<RleRange<u64>> = None;
        let mut obj_ctr: Option<RleRange<u64>> = None;
        let mut key_actor: Option<RleRange<u64>> = None;
        let mut key_ctr: Option<DeltaRange> = None;
        let mut key_str: Option<RleRange<smol_str::SmolStr>> = None;
        let mut id_actor: Option<RleRange<u64>> = None;
        let mut id_ctr: Option<DeltaRange> = None;
        let mut insert: Option<BooleanRange> = None;
        let mut action: Option<RleRange<u64>> = None;
        let mut val: Option<ValueRange> = None;
        let mut succ_group: Option<RleRange<u64>> = None;
        let mut succ_actor: Option<RleRange<u64>> = None;
        let mut succ_ctr: Option<DeltaRange> = None;
        let mut other = Columns::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (ID_COL_ID, ColumnType::Actor) => id_actor = Some(col.range().into()),
                (ID_COL_ID, ColumnType::DeltaInteger) => id_ctr = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Actor) => obj_actor = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Integer) => obj_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::Actor) => key_actor = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::DeltaInteger) => key_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::String) => key_str = Some(col.range().into()),
                (INSERT_COL_ID, ColumnType::Boolean) => insert = Some(col.range().into()),
                (ACTION_COL_ID, ColumnType::Integer) => action = Some(col.range().into()),
                (VAL_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(v) => val = Some(v),
                    _ => {
                        tracing::error!("col 9 should be a value column");
                        return Err(Error::MismatchingColumn { index });
                    }
                },
                (SUCC_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        let first = cols.next();
                        let second = cols.next();
                        succ_group = Some(num);
                        match (first, second) {
                            (
                                Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                    actor_range,
                                ))),
                                Some(GroupedColumnRange::Simple(SimpleColRange::Delta(ctr_range))),
                            ) => {
                                succ_actor = Some(actor_range);
                                succ_ctr = Some(ctr_range);
                            }
                            (None, None) => {
                                succ_actor = Some((0..0).into());
                                succ_ctr = Some((0..0).into());
                            }
                            _ => {
                                tracing::error!(
                                    "expected a two column group of (actor, rle int) for index 10"
                                );
                                return Err(Error::MismatchingColumn { index });
                            }
                        };
                        if cols.next().is_some() {
                            return Err(Error::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(Error::MismatchingColumn { index }),
                },
                (other_col, other_type) => {
                    tracing::warn!(id=?other_col, typ=?other_type, "unknown column type");
                    other.append(col)
                }
            }
        }
        Ok(DocOpColumns {
            obj: ObjIdRange::new(
                obj_actor.unwrap_or_else(|| (0..0).into()),
                obj_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            prop: key_str.unwrap_or_else(|| (0..0).into()),
            elem: ElemRange::new(
                key_actor.unwrap_or_else(|| (0..0).into()),
                key_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            id: OpIdRange::new(
                id_actor.unwrap_or_else(|| (0..0).into()),
                id_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            insert: insert.unwrap_or_else(|| (0..0).into()),
            action: action.unwrap_or_else(|| (0..0).into()),
            val: val.unwrap_or_else(|| ValueRange::new((0..0).into(), (0..0).into())),
            succ: OpIdListRange::new(
                succ_group.unwrap_or_else(|| (0..0).into()),
                succ_actor.unwrap_or_else(|| (0..0).into()),
                succ_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            other,
        })
    }
}
