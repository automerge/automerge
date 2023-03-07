use std::{convert::TryFrom, ops::Range};

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, Key, KeyEncoder, KeyIter, KeyRange, ObjIdEncoder, ObjIdIter,
            ObjIdRange, OpIdListEncoder, OpIdListIter, OpIdListRange, RleRange, ValueEncoder,
            ValueIter, ValueRange,
        },
        encoding::{
            BooleanDecoder, BooleanEncoder, ColumnDecoder, DecodeColumnError, RleDecoder,
            RleEncoder,
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOp {
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) val: ScalarValue,
    pub(crate) pred: Vec<OpId>,
    pub(crate) action: u64,
    pub(crate) obj: ObjId,
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
        }
    }
}

impl<'a> AsChangeOp<'a> for &'a ChangeOp {
    type OpId = &'a crate::types::OpId;
    type ActorId = usize;
    type PredIter = std::slice::Iter<'a, crate::types::OpId>;

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.obj.is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(self.obj.opid())
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOpsColumns {
    obj: Option<ObjIdRange>,
    key: KeyRange,
    insert: BooleanRange,
    action: RleRange<u64>,
    val: ValueRange,
    pred: OpIdListRange,
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
        let obj = ObjIdRange::encode(ops.clone().map(|o| o.obj()), out);
        let key = KeyRange::encode(ops.clone().map(|o| o.key()), out);
        let insert = BooleanRange::encode(ops.clone().map(|o| o.insert()), out);
        let action = RleRange::encode(ops.clone().map(|o| Some(o.action())), out);
        let val = ValueRange::encode(ops.clone().map(|o| o.val()), out);
        let pred = OpIdListRange::encode(ops.map(|o| o.pred()), out);
        Self {
            obj,
            key,
            insert,
            action,
            val,
            pred,
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
        for op in ops {
            obj.append(op.obj());
            key.append(op.key());
            insert.append(op.insert());
            action.append_value(op.action());
            val.append(&op.val());
            pred.append(op.pred());
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

        Self {
            obj,
            key,
            insert,
            action,
            val,
            pred,
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
}

impl<'a> ChangeOpsIter<'a> {
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
            }))
        }
    }
}

impl<'a> Iterator for ChangeOpsIter<'a> {
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
