use std::{convert::TryFrom, ops::Range};

use crate::{
    columnar_2::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            BooleanRange, DeltaRange, Key, KeyIter, KeyRange, ObjIdIter, ObjIdRange, OpIdListIter,
            OpIdListRange, RleRange, ValueIter, ValueRange,
        },
        encoding::{BooleanDecoder, DecodeColumnError, RleDecoder},
    },
    convert,
    storage::{
        change::AsChangeOp,
        column_layout::{
            compression, ColumnId, ColumnLayout, ColumnSpec, ColumnType, MismatchingColumn,
        },
        raw_column::RawColumn,
        RawColumns,
    },
    types::{ElemId, ObjId, OpId, ScalarValue},
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
pub enum ReadChangeOpError {
    #[error("unexpected null in column {0}")]
    UnexpectedNull(String),
    #[error("invalid value in column {column}: {description}")]
    InvalidValue { column: String, description: String },
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
                objs.next()
                    .transpose()
                    .map_err(|e| self.handle_error("object", e))?
                    .ok_or_else(|| ReadChangeOpError::UnexpectedNull("object".to_string()))?
            } else {
                ObjId::root()
            };
            let key = self
                .key
                .next()
                .transpose()
                .map_err(|e| self.handle_error("key", e))?
                .ok_or_else(|| ReadChangeOpError::UnexpectedNull("key".to_string()))?;
            let insert = self
                .insert
                .next()
                .ok_or_else(|| ReadChangeOpError::UnexpectedNull("insert".to_string()))?;
            let action = self
                .action
                .next()
                .flatten()
                .ok_or_else(|| ReadChangeOpError::UnexpectedNull("action".to_string()))?;
            let val = self
                .val
                .next()
                .transpose()
                .map_err(|e| self.handle_error("value", e))?
                .ok_or_else(|| ReadChangeOpError::UnexpectedNull("value".to_string()))?;
            let pred = self
                .pred
                .next()
                .transpose()
                .map_err(|e| self.handle_error("pred", e))?
                .ok_or_else(|| ReadChangeOpError::UnexpectedNull("pred".to_string()))?;
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

    fn handle_error(
        &mut self,
        outer_col: &'static str,
        err: DecodeColumnError,
    ) -> ReadChangeOpError {
        match err {
            DecodeColumnError::InvalidValue {
                column,
                description,
            } => ReadChangeOpError::InvalidValue {
                column: format!("{}:{}", outer_col, column),
                description,
            },
            DecodeColumnError::UnexpectedNull(col) => {
                ReadChangeOpError::UnexpectedNull(format!("{}:{}", outer_col, col))
            }
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

impl TryFrom<ColumnLayout> for ChangeOpsColumns {
    type Error = ParseChangeColumnsError;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
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
        let mut other = ColumnLayout::empty();

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
    use crate::columnar_2::encoding::properties::{key, opid, scalar_value};
    use proptest::prelude::*;

    prop_compose! {
        fn change_op()
                    (key in key(),
                     value in scalar_value(),
                     pred in proptest::collection::vec(opid(), 0..20),
                     action in 0_u64..6,
                     obj in opid(),
                     insert in any::<bool>()) -> ChangeOp {
            ChangeOp {
                obj: obj.into(),
                key,
                val: value,
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
