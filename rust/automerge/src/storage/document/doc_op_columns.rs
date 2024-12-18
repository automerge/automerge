use std::{borrow::Cow, convert::TryFrom};

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, Key, KeyEncoder, KeyIter, KeyRange, MaybeBooleanRange,
            ObjIdEncoder, ObjIdIter, ObjIdRange, OpIdEncoder, OpIdIter, OpIdListEncoder,
            OpIdListIter, OpIdListRange, OpIdRange, RleRange, ValueEncoder, ValueIter, ValueRange,
        },
        encoding::{
            BooleanDecoder, BooleanEncoder, ColumnDecoder, DecodeColumnError, MaybeBooleanDecoder,
            MaybeBooleanEncoder, RleDecoder, RleEncoder,
        },
    },
    convert,
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
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
const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);

/// The form operations take in the compressed document format.
#[derive(Debug)]
pub(crate) struct DocOp {
    pub(crate) id: OpId,
    pub(crate) object: ObjId,
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: u64,
    pub(crate) value: ScalarValue,
    pub(crate) succ: Vec<OpId>,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<smol_str::SmolStr>,
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
    other: Columns,
    expand: MaybeBooleanRange,
    mark_name: RleRange<smol_str::SmolStr>,
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
    fn expand(&self) -> bool;
    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>>;
}

impl DocOpColumns {
    pub(crate) fn encode<'a, I, C, O>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C> + Clone + ExactSizeIterator,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        if ops.len() > 30000 {
            Self::encode_rowwise(ops, out)
        } else {
            Self::encode_columnwise(ops, out)
        }
    }

    fn encode_columnwise<'a, I, O, C>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = C> + Clone,
        O: convert::OpId<usize>,
        C: AsDocOp<'a, OpId = O>,
    {
        let obj = ObjIdRange::encode(ops.clone().map(|o| o.obj()), out);
        let key = KeyRange::encode(ops.clone().map(|o| o.key()), out);
        let id = OpIdRange::encode(ops.clone().map(|o| o.id()), out);
        let insert = BooleanRange::encode(ops.clone().map(|o| o.insert()), out);
        let action = RleRange::encode(ops.clone().map(|o| Some(o.action())), out);
        let val = ValueRange::encode(ops.clone().map(|o| o.val()), out);
        let succ = OpIdListRange::encode(ops.clone().map(|o| o.succ()), out);
        let expand = MaybeBooleanRange::encode(ops.clone().map(|o| o.expand()), out);
        let mark_name = RleRange::encode(ops.map(|o| o.mark_name()), out);
        Self {
            obj,
            key,
            id,
            insert,
            action,
            val,
            succ,
            expand,
            mark_name,
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
        let mut key = KeyEncoder::new();
        let mut id = OpIdEncoder::new();
        let mut insert = BooleanEncoder::new();
        let mut action = RleEncoder::<_, u64>::from(Vec::new());
        let mut val = ValueEncoder::new();
        let mut succ = OpIdListEncoder::new();
        let mut expand = MaybeBooleanEncoder::new();
        let mut mark_name = RleEncoder::<_, smol_str::SmolStr>::new(Vec::new());
        for op in ops {
            obj.append(op.obj());
            key.append(op.key());
            id.append(op.id());
            insert.append(op.insert());
            action.append(Some(op.action()));
            val.append(&op.val());
            succ.append(op.succ());
            expand.append(op.expand());
            mark_name.append(op.mark_name());
        }
        let obj = obj.finish(out);
        let key = key.finish(out);
        let id = id.finish(out);

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

        let expand_start = out.len();
        let (expand_out, _) = expand.finish();
        out.extend(expand_out);
        let expand = MaybeBooleanRange::from(expand_start..out.len());

        let mark_name_start = out.len();
        let (mark_name_out, _) = mark_name.finish();
        out.extend(mark_name_out);
        let mark_name = RleRange::from(mark_name_start..out.len());

        DocOpColumns {
            obj,
            key,
            id,
            insert,
            action,
            val,
            succ,
            expand,
            mark_name,
            other: Columns::empty(),
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
            expand: self.expand.decoder(data),
            mark_name: self.mark_name.decoder(data),
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
        if !self.expand.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXPAND_COL_ID, ColumnType::Boolean, false),
                self.expand.clone().into(),
            ));
        }
        if !self.mark_name.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(MARK_NAME_COL_ID, ColumnType::String, false),
                self.mark_name.clone().into(),
            ));
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
    expand: MaybeBooleanDecoder<'a>,
    mark_name: RleDecoder<'a, smol_str::SmolStr>,
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
            let key = self.keys.next_in_col("key")?;
            let value = self.value.next_in_col("value")?;
            let succ = self.succ.next_in_col("succ")?;
            let insert = self.insert.next_in_col("insert")?;
            let expand = self.expand.maybe_next_in_col("expand")?.unwrap_or(false);
            let mark_name = self.mark_name.maybe_next_in_col("mark_name")?;
            Ok(Some(DocOp {
                id,
                value,
                action,
                object: obj,
                key,
                succ,
                insert,
                expand,
                mark_name,
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
        let mut expand: Option<MaybeBooleanRange> = None;
        let mut mark_name: Option<RleRange<smol_str::SmolStr>> = None;
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
                (EXPAND_COL_ID, ColumnType::Boolean) => expand = Some(col.range().into()),
                (MARK_NAME_COL_ID, ColumnType::String) => mark_name = Some(col.range().into()),
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
            expand: expand.unwrap_or_else(|| (0..0).into()),
            mark_name: mark_name.unwrap_or_else(|| (0..0).into()),
            other,
        })
    }
}
