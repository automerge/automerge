use std::{borrow::Cow, convert::TryFrom, ops::Range};

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, Key, KeyEncoder, KeyIter, KeyRange, MaybeBooleanRange,
            ObjIdEncoder, ObjIdIter, ObjIdRange, OpIdListEncoder, OpIdListIter, OpIdListRange,
            RawRange, RleRange, ValueEncoder, ValueIter, ValueRange,
        },
        encoding::{
            BooleanDecoder, BooleanEncoder, ColumnDecoder, DecodeColumnError, MaybeBooleanDecoder,
            MaybeBooleanEncoder, RleDecoder, RleEncoder,
        },
    },
    convert,
    error::InvalidOpType,
    storage::{
        change::AsChangeOp,
        columns::{
            compression, ColumnId, ColumnSpec, ColumnType, Columns, MismatchingColumn, RawColumn,
        },
        RawColumns,
    },
    types::{ElemId, ObjId, OpId, ScalarValue},
    OpType,
};

const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const PRED_COL_ID: ColumnId = ColumnId::new(7);
const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOp {
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) val: ScalarValue,
    pub(crate) pred: Vec<OpId>,
    pub(crate) action: u64,
    pub(crate) obj: ObjId,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<smol_str::SmolStr>,
}

impl<'a, A: AsChangeOp<'a, ActorId = usize, OpId = OpId>> From<A> for ChangeOp {
    fn from(a: A) -> Self {
        ChangeOp {
            key: match a.key() {
                convert::Key::Prop(s) => Key::Prop(s.into_owned()),
                convert::Key::Elem(convert::ElemId::Head) => Key::Elem(ElemId::head()),
                convert::Key::Elem(convert::ElemId::Op(o)) => Key::Elem(ElemId(o)),
            },
            obj: match a.obj() {
                convert::ObjId::Root => ObjId::root(),
                convert::ObjId::Op(o) => ObjId(o),
            },
            val: a.val().into_owned(),
            pred: a.pred().collect(),
            insert: a.insert(),
            action: a.action(),
            expand: a.expand(),
            mark_name: a.mark_name().map(|n| n.into_owned()),
        }
    }
}

impl<'a> AsChangeOp<'a> for &'a ChangeOp {
    type OpId = &'a crate::types::OpId;
    type ActorId = usize;
    type PredIter = std::slice::Iter<'a, crate::types::OpId>;

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if let Some(id) = self.obj.id() {
            convert::ObjId::Op(id)
        } else {
            convert::ObjId::Root
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match &self.key {
            Key::Prop(s) => convert::Key::Prop(std::borrow::Cow::Borrowed(s)),
            Key::Elem(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Elem(e) => convert::Key::Elem(convert::ElemId::Op(&e.0)),
        }
    }

    fn val(&self) -> std::borrow::Cow<'a, ScalarValue> {
        std::borrow::Cow::Borrowed(&self.val)
    }

    fn pred(&self) -> Self::PredIter {
        self.pred.iter()
    }

    fn insert(&self) -> bool {
        self.insert
    }

    fn action(&self) -> u64 {
        self.action
    }

    fn expand(&self) -> bool {
        self.expand
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        self.mark_name.as_ref().map(Cow::Borrowed)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOpsColumns {
    obj: Option<ObjIdRange>,
    key: KeyRange,
    insert: BooleanRange,
    action: RleRange<u64>,
    val: ValueRange,
    pred: OpIdListRange,
    expand: MaybeBooleanRange,
    mark_name: RleRange<smol_str::SmolStr>,
}

use crate::op_set2::change;
impl From<change::ChangeOpsColumns> for ChangeOpsColumns {
    fn from(other: change::ChangeOpsColumns) -> Self {
        let obj = ObjIdRange::new(
            RleRange::from(other.obj_actor),
            RleRange::from(other.obj_ctr),
        );
        let key = KeyRange::new(
            RleRange::from(other.key_actor),
            DeltaRange::from(other.key_ctr),
            RleRange::from(other.key_str),
        );
        let insert = BooleanRange::from(other.insert);
        let action = RleRange::from(other.action);
        let val = ValueRange::new(
            RleRange::from(other.value_meta),
            RawRange::from(other.value),
        );
        let pred = OpIdListRange::new(
            RleRange::from(other.pred_count),
            RleRange::from(other.pred_actor),
            DeltaRange::from(other.pred_ctr),
        );
        let expand = MaybeBooleanRange::from(other.expand);
        let mark_name = RleRange::from(other.mark_name);
        Self {
            obj,
            key,
            insert,
            action,
            val,
            pred,
            expand,
            mark_name,
        }
    }
}

impl ChangeOpsColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> ChangeOpsIter<'a> {
        ChangeOpsIter {
            failed: false,
            obj: self.obj.as_ref().map(|o| o.iter(data)),
            key: self.key.iter(data),
            insert: self.insert.decoder(data),
            action: self.action.decoder(data),
            val: self.val.iter(data),
            pred: self.pred.iter(data),
            expand: self.expand.decoder(data),
            mark_name: self.mark_name.decoder(data),
        }
    }

    #[tracing::instrument(skip(ops, out))]
    pub(crate) fn encode<'a, 'b, 'c, I, C, Op>(ops: I, out: &'b mut Vec<u8>) -> ChangeOpsColumns
    where
        I: Iterator<Item = C> + Clone + ExactSizeIterator + 'a,
        Op: convert::OpId<usize> + 'a,
        C: AsChangeOp<'c, OpId = Op> + 'a,
    {
        if ops.len() > 10000 {
            Self::encode_rowwise(ops, out)
        } else {
            Self::encode_columnwise(ops, out)
        }
    }

    pub(crate) fn encode_columnwise<'a, 'b, 'c, I, C, Op>(
        ops: I,
        out: &'b mut Vec<u8>,
    ) -> ChangeOpsColumns
    where
        I: Iterator<Item = C> + Clone + 'a,
        Op: convert::OpId<usize> + 'a,
        C: AsChangeOp<'c, OpId = Op> + 'a,
    {
        tracing::trace!(expands = ?ops.clone().map(|op| op.expand()).collect::<Vec<_>>(), "encoding change ops");
        let obj = ObjIdRange::encode(ops.clone().map(|o| o.obj()), out);
        let key = KeyRange::encode(ops.clone().map(|o| o.key()), out);
        let insert = BooleanRange::encode(ops.clone().map(|o| o.insert()), out);
        let action = RleRange::encode(ops.clone().map(|o| Some(o.action())), out);
        let val = ValueRange::encode(ops.clone().map(|o| o.val()), out);
        let pred = OpIdListRange::encode(ops.clone().map(|o| o.pred()), out);
        let expand = MaybeBooleanRange::encode(ops.clone().map(|o| o.expand()), out);
        let mark_name =
            RleRange::encode::<Cow<'_, smol_str::SmolStr>, _>(ops.map(|o| o.mark_name()), out);
        Self {
            obj,
            key,
            insert,
            action,
            val,
            pred,
            expand,
            mark_name,
        }
    }

    fn encode_rowwise<'a, 'b, 'c, I, C, Op>(ops: I, out: &'b mut Vec<u8>) -> ChangeOpsColumns
    where
        I: Iterator<Item = C> + Clone + 'a,
        Op: convert::OpId<usize> + 'a,
        C: AsChangeOp<'c, OpId = Op> + 'a,
    {
        let mut obj = ObjIdEncoder::new();
        let mut key = KeyEncoder::new();
        let mut insert = BooleanEncoder::new();
        let mut action = RleEncoder::<_, u64>::from(Vec::new());
        let mut val = ValueEncoder::new();
        let mut pred = OpIdListEncoder::new();
        let mut expand = MaybeBooleanEncoder::new();
        let mut mark_name = RleEncoder::<_, smol_str::SmolStr>::new(Vec::new());
        for op in ops {
            tracing::trace!(expand=?op.expand(), "expand");
            obj.append(op.obj());
            key.append(op.key());
            insert.append(op.insert());
            action.append_value(op.action());
            val.append(&op.val());
            pred.append(op.pred());
            expand.append(op.expand());
            mark_name.append(op.mark_name());
        }
        let obj = obj.finish(out);
        let key = key.finish(out);

        let insert_start = out.len();
        let (insert, _) = insert.finish();
        out.extend(insert);
        let insert = BooleanRange::from(insert_start..out.len());

        let action_start = out.len();
        let (action, _) = action.finish();
        out.extend(action);
        let action = RleRange::from(action_start..out.len());

        let val = val.finish(out);
        let pred = pred.finish(out);

        let expand_start = out.len();
        let (expand, _) = expand.finish();
        out.extend(expand);
        let expand = MaybeBooleanRange::from(expand_start..out.len());

        let mark_name_start = out.len();
        let (mark_name, _) = mark_name.finish();
        out.extend(mark_name);
        let mark_name = RleRange::from(mark_name_start..out.len());

        Self {
            obj,
            key,
            insert,
            action,
            val,
            pred,
            expand,
            mark_name,
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
            ColumnSpec::new(PRED_COL_ID, ColumnType::Group, false),
            self.pred.group_range().clone().into(),
        ));
        if !self.pred.actor_range().is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(PRED_COL_ID, ColumnType::Actor, false),
                    self.pred.actor_range().clone().into(),
                ),
                RawColumn::new(
                    ColumnSpec::new(PRED_COL_ID, ColumnType::DeltaInteger, false),
                    self.pred.counter_range().clone().into(),
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

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum ReadChangeOpError {
    #[error(transparent)]
    DecodeError(#[from] DecodeColumnError),
    #[error(transparent)]
    InvalidOpType(#[from] InvalidOpType),
    #[error("counter too large")]
    CounterTooLarge,
}

#[derive(Clone)]
pub(crate) struct ChangeOpsIter<'a> {
    failed: bool,
    obj: Option<ObjIdIter<'a>>,
    key: KeyIter<'a>,
    insert: BooleanDecoder<'a>,
    action: RleDecoder<'a, u64>,
    val: ValueIter<'a>,
    pred: OpIdListIter<'a>,
    expand: MaybeBooleanDecoder<'a>,
    mark_name: RleDecoder<'a, smol_str::SmolStr>,
}

impl ChangeOpsIter<'_> {
    fn done(&self) -> bool {
        self.action.done()
    }

    fn try_next(&mut self) -> Result<Option<ChangeOp>, ReadChangeOpError> {
        if self.failed || self.done() {
            Ok(None)
        } else {
            let obj = if let Some(ref mut objs) = self.obj {
                objs.next_in_col("obj")?
            } else {
                ObjId::root()
            };
            let key = self.key.next_in_col("key")?;
            let insert = self.insert.next_in_col("insert")?;
            let action = self.action.next_in_col("action")?;
            let val = self.val.next_in_col("value")?;
            let pred = self.pred.next_in_col("pred")?;
            let expand = self.expand.maybe_next_in_col("expand")?.unwrap_or(false);
            let mark_name = self.mark_name.maybe_next_in_col("mark_name")?;

            // This check is necessary to ensure that OpType::from_action_and_value
            // cannot panic later in the process.
            OpType::validate_action_and_value(action, &val)?;

            Ok(Some(ChangeOp {
                obj,
                key,
                insert,
                action,
                val,
                pred,
                expand,
                mark_name,
            }))
        }
    }
}

impl Iterator for ChangeOpsIter<'_> {
    type Item = Result<ChangeOp, ReadChangeOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.try_next() {
            Ok(v) => v.map(Ok),
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseChangeColumnsError {
    #[error("mismatching column at {index}.")]
    MismatchingColumn { index: usize },
}

impl From<MismatchingColumn> for ParseChangeColumnsError {
    fn from(m: MismatchingColumn) -> Self {
        Self::MismatchingColumn { index: m.index }
    }
}

use super::super::parse;
use super::ParseError;

impl TryFrom<RawColumns<compression::Uncompressed>> for ChangeOpsColumns {
    type Error = parse::ParseError<ParseError>;

    fn try_from(raw: RawColumns<compression::Uncompressed>) -> Result<Self, Self::Error> {
        let len = raw.total_column_len();
        let col_layout = Columns::parse2(len, raw.iter())
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
        ChangeOpsColumns::try_from(col_layout)
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))
    }
}

impl TryFrom<Columns> for ChangeOpsColumns {
    type Error = ParseChangeColumnsError;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<RleRange<u64>> = None;
        let mut obj_ctr: Option<RleRange<u64>> = None;
        let mut key_actor: Option<RleRange<u64>> = None;
        let mut key_ctr: Option<DeltaRange> = None;
        let mut key_str: Option<RleRange<smol_str::SmolStr>> = None;
        let mut insert: Option<Range<usize>> = None;
        let mut action: Option<Range<usize>> = None;
        let mut val: Option<ValueRange> = None;
        let mut pred_group: Option<RleRange<u64>> = None;
        let mut pred_actor: Option<RleRange<u64>> = None;
        let mut pred_ctr: Option<DeltaRange> = None;
        let mut expand: Option<MaybeBooleanRange> = None;
        let mut mark_name: Option<RleRange<smol_str::SmolStr>> = None;
        let mut other = Columns::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (OBJ_COL_ID, ColumnType::Actor) => obj_actor = Some(col.range().into()),
                (OBJ_COL_ID, ColumnType::Integer) => obj_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::Actor) => key_actor = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::DeltaInteger) => key_ctr = Some(col.range().into()),
                (KEY_COL_ID, ColumnType::String) => key_str = Some(col.range().into()),
                (INSERT_COL_ID, ColumnType::Boolean) => insert = Some(col.range()),
                (ACTION_COL_ID, ColumnType::Integer) => action = Some(col.range()),
                (VAL_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(v) => {
                        val = Some(v);
                    }
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn { index }),
                },
                (PRED_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        pred_group = Some(num);
                        // If there was no data in the group at all then the columns won't be
                        // present
                        if cols.len() == 0 {
                            pred_actor = Some((0..0).into());
                            pred_ctr = Some((0..0).into());
                        } else {
                            let first = cols.next();
                            let second = cols.next();
                            match (first, second) {
                                (
                                    Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
                                        actor_range,
                                    ))),
                                    Some(GroupedColumnRange::Simple(SimpleColRange::Delta(
                                        ctr_range,
                                    ))),
                                ) => {
                                    pred_actor = Some(actor_range);
                                    pred_ctr = Some(ctr_range);
                                }
                                _ => {
                                    return Err(ParseChangeColumnsError::MismatchingColumn {
                                        index,
                                    })
                                }
                            }
                        }
                        if cols.next().is_some() {
                            return Err(ParseChangeColumnsError::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn { index }),
                },
                (EXPAND_COL_ID, ColumnType::Boolean) => expand = Some(col.range().into()),
                (MARK_NAME_COL_ID, ColumnType::String) => mark_name = Some(col.range().into()),
                (other_type, other_col) => {
                    tracing::warn!(typ=?other_type, id=?other_col, "unknown column");
                    other.append(col);
                }
            }
        }
        let pred = OpIdListRange::new(
            pred_group.unwrap_or_else(|| (0..0).into()),
            pred_actor.unwrap_or_else(|| (0..0).into()),
            pred_ctr.unwrap_or_else(|| (0..0).into()),
        );
        Ok(ChangeOpsColumns {
            obj: ObjIdRange::new(
                obj_actor.unwrap_or_else(|| (0..0).into()),
                obj_ctr.unwrap_or_else(|| (0..0).into()),
            ),
            key: KeyRange::new(
                key_actor.unwrap_or_else(|| (0..0).into()),
                key_ctr.unwrap_or_else(|| (0..0).into()),
                key_str.unwrap_or_else(|| (0..0).into()),
            ),
            insert: insert.unwrap_or(0..0).into(),
            action: action.unwrap_or(0..0).into(),
            val: val.unwrap_or_else(|| ValueRange::new((0..0).into(), (0..0).into())),
            pred,
            expand: expand.unwrap_or_else(|| (0..0).into()),
            mark_name: mark_name.unwrap_or_else(|| (0..0).into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding::properties::{key, opid, scalar_value};
    use proptest::prelude::*;

    prop_compose! {
        fn change_op()
                    (key in key(),
                     value in scalar_value(),
                     pred in proptest::collection::vec(opid(), 0..20),
                     action in 0_u64..6,
                     obj in opid(),
                     mark_name in proptest::option::of(any::<String>().prop_map(|s| s.into())),
                     expand in any::<bool>(),
                     insert in any::<bool>()) -> ChangeOp {

                    let val = if action == 5 && !(value.is_int() || value.is_uint()) {
                        ScalarValue::Uint(0)
                    } else { value };
            ChangeOp {
                obj: obj.into(),
                key,
                val,
                pred,
                action,
                insert,
                expand,
                mark_name,
            }
        }
    }

    proptest! {
        #[test]
        fn test_encode_decode_change_ops(ops in proptest::collection::vec(change_op(), 0..100)) {
            let mut out = Vec::new();
            let cols2 = ChangeOpsColumns::encode(ops.iter(), &mut out);
            let decoded = cols2.iter(&out[..]).collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(ops, decoded);
        }
    }
}
