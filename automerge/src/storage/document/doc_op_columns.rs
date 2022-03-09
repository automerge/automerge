use std::{borrow::Cow, convert::TryFrom};

use crate::{
    columnar_2::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, Key, KeyIter, KeyRange, ObjIdIter, ObjIdRange, OpIdIter,
            OpIdListIter, OpIdListRange, OpIdRange, RleRange, ValueIter, ValueRange,
        },
        encoding::{BooleanDecoder, DecodeColumnError, RleDecoder},
    },
    convert,
    storage::{
        column_layout::{compression, ColumnId, ColumnSpec, ColumnType},
        ColumnLayout, MismatchingColumn, RawColumn, RawColumns,
    },
    types::{ObjId, OpId, ScalarValue},
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
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: usize,
    pub(crate) value: ScalarValue,
    pub(crate) succ: Vec<OpId>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocOpColumns {
    obj: Option<ObjIdRange>,
    key: KeyRange,
    id: OpIdRange,
    insert: BooleanRange,
    action: RleRange<u64>,
    val: ValueRange,
    succ: OpIdListRange,
    #[allow(dead_code)]
    other: ColumnLayout,
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
    fn key(&self) -> convert::Key<'a, Self::OpId>;
    fn insert(&self) -> bool;
    fn action(&self) -> u64;
    fn val(&self) -> Cow<'a, ScalarValue>;
    fn succ(&self) -> Self::SuccIter;
}

impl DocOpColumns {
    pub(crate) fn encode<'a, I, C, O>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C> + Clone,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        let obj = ObjIdRange::encode(ops.clone().map(|o| o.obj()), out);
        let key = KeyRange::encode(ops.clone().map(|o| o.key()), out);
        let id = OpIdRange::encode(ops.clone().map(|o| o.id()), out);
        let insert = BooleanRange::encode(ops.clone().map(|o| o.insert()), out);
        let action = RleRange::encode(ops.clone().map(|o| Some(o.action() as u64)), out);
        let val = ValueRange::encode(ops.clone().map(|o| o.val()), out);
        let succ = OpIdListRange::encode(ops.map(|o| o.succ()), out);
        Self {
            obj,
            key,
            id,
            insert,
            action,
            val,
            succ,
            other: ColumnLayout::empty(),
        }
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocOpColumnIter<'a> {
        DocOpColumnIter {
            id: self.id.iter(data),
            action: self.action.decoder(data),
            objs: self.obj.as_ref().map(|o| o.iter(data)),
            keys: self.key.iter(data),
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
                self.key.actor_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false),
                self.key.counter_range().clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY_COL_ID, ColumnType::String, false),
                self.key.string_range().clone().into(),
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
    keys: KeyIter<'a>,
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
pub(crate) enum ReadDocOpError {
    #[error("unexpected null in column {0}")]
    UnexpectedNull(String),
    #[error("invalid value in column {column}: {description}")]
    InvalidValue { column: String, description: String },
}

macro_rules! next_or_invalid({$iter: expr, $col: literal} => {
    match $iter.next() {
        Some(Ok(id)) => id,
        Some(Err(e)) => match e {
            DecodeColumnError::UnexpectedNull(inner_col) => {
                return Some(Err(ReadDocOpError::UnexpectedNull(format!(
                    "{}:{}", $col, inner_col
                ))));
            },
            DecodeColumnError::InvalidValue{column, description} => {
                let col = format!("{}:{}", $col, column);
                return Some(Err(ReadDocOpError::InvalidValue{column: col, description}))
            }
        }
        None => return Some(Err(ReadDocOpError::UnexpectedNull($col.to_string()))),
    }
});

impl<'a> Iterator for DocOpColumnIter<'a> {
    type Item = Result<DocOp, ReadDocOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            let id = next_or_invalid!(self.id, "opid");
            let action = match self.action.next() {
                Some(Some(a)) => a,
                Some(None) | None => {
                    return Some(Err(ReadDocOpError::UnexpectedNull("action".to_string())))
                }
            };
            let obj = if let Some(ref mut objs) = self.objs {
                next_or_invalid!(objs, "obj")
            } else {
                ObjId::root()
            };
            let key = next_or_invalid!(self.keys, "key");
            let value = next_or_invalid!(self.value, "value");
            let succ = next_or_invalid!(self.succ, "succ");
            let insert = self.insert.next().unwrap_or(false);
            Some(Ok(DocOp {
                id,
                value,
                action: action as usize,
                object: obj,
                key,
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

impl TryFrom<ColumnLayout> for DocOpColumns {
    type Error = Error;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
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
        let mut other = ColumnLayout::empty();

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
            key: KeyRange::new(
                key_actor.unwrap_or_else(|| (0..0).into()),
                key_ctr.unwrap_or_else(|| (0..0).into()),
                key_str.unwrap_or_else(|| (0..0).into()),
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
