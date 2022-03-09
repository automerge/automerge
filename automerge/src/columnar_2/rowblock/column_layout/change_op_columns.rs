use std::{borrow::Borrow, convert::TryFrom, ops::Range};

use crate::{
    columnar_2::{
        column_specification::ColumnType,
        rowblock::{
            column_layout::{column::{GroupColRange, ColumnRanges}, ColumnLayout, MismatchingColumn, assert_col_type},
            column_range::{
                ActorRange, BooleanRange, DeltaIntRange, RawRange, RleIntRange, RleStringRange,
            },
            encoding::{
                BooleanDecoder, DecodeColumnError, Key, KeyDecoder, ObjDecoder,
                OpIdListDecoder, RleDecoder, ValueDecoder,
            },
            PrimVal,
        }, ColumnSpec, ColumnId, storage::ColumnMetadata
    },
    types::{ElemId, ObjId, OpId},
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChangeOp<'a> {
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) val: PrimVal<'a>,
    pub(crate) pred: Vec<OpId>,
    pub(crate) action: u64,
    pub(crate) obj: ObjId,
}

impl<'a> ChangeOp<'a> {
    pub(crate) fn into_owned(self) -> ChangeOp<'static> {
        ChangeOp {
            key: self.key,
            insert: self.insert,
            val: self.val.into_owned(),
            pred: self.pred,
            action: self.action,
            obj: self.obj,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeOpsColumns {
    obj_actor: ActorRange,
    obj_counter: RleIntRange,
    key_actor: ActorRange,
    key_counter: DeltaIntRange,
    key_string: RleStringRange,
    insert: BooleanRange,
    action: RleIntRange,
    val_meta: RleIntRange,
    val_raw: RawRange,
    pred_group: RleIntRange,
    pred_actor: RleIntRange,
    pred_ctr: DeltaIntRange,
}

impl ChangeOpsColumns {
    pub(crate) fn empty() -> Self {
        ChangeOpsColumns {
            obj_actor: (0..0).into(),
            obj_counter: (0..0).into(),
            key_actor: (0..0).into(),
            key_counter: (0..0).into(),
            key_string: (0..0).into(),
            insert: (0..0).into(),
            action: (0..0).into(),
            val_meta: (0..0).into(),
            val_raw: (0..0).into(),
            pred_group: (0..0).into(),
            pred_actor: (0..0).into(),
            pred_ctr: (0..0).into(),
        }
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> ChangeOpsIter<'a> {
        ChangeOpsIter {
            failed: false,
            obj: ObjDecoder::new(self.obj_actor.decoder(data), self.obj_counter.decoder(data)),
            key: KeyDecoder::new(
                self.key_actor.decoder(data),
                self.key_counter.decoder(data),
                self.key_string.decoder(data),
            ),
            insert: self.insert.decoder(data),
            action: self.action.decoder(data),
            val: ValueDecoder::new(self.val_meta.decoder(data), self.val_raw.decoder(data)),
            pred: OpIdListDecoder::new(
                self.pred_group.decoder(data),
                self.pred_actor.decoder(data),
                self.pred_ctr.decoder(data),
            ),
        }
    }

    pub(crate) fn encode<'a, I, C: Borrow<ChangeOp<'a>>>(&self, ops: I, out: &mut Vec<u8>) -> ChangeOpsColumns
    where
        I: Iterator<Item = C> + Clone,
    {
        let obj_actor = self.obj_actor.decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(OpId::from(o.borrow().obj).actor() as u64)),
            out,
        );
        let obj_counter = self.obj_counter.decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(OpId::from(o.borrow().obj).counter())),
            out,
        );
        let key_actor = self.key_actor.decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match o.borrow().key {
                Key::Prop(_) => None,
                Key::Elem(ElemId(o)) => Some(o.actor() as u64),
            }),
            out,
        );
        let key_counter = self.key_counter.decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match o.borrow().key {
                Key::Prop(_) => None,
                Key::Elem(ElemId(o)) => Some(o.counter() as i64),
            }),
            out,
        );
        let key_string = self.key_string.decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match &o.borrow().key {
                Key::Prop(k) => Some(k.clone()),
                Key::Elem(_) => None,
            }),
            out,
        );
        let insert = self
            .insert
            .decoder(&[])
            .splice(0..0, ops.clone().map(|o| o.borrow().insert), out);
        let action =
            self.action
                .decoder(&[])
                .splice(0..0, ops.clone().map(|o| Some(o.borrow().action)), out);
        let mut val_dec = ValueDecoder::new(self.val_meta.decoder(&[]), self.val_raw.decoder(&[]));
        let (val_meta, val_raw) = val_dec.splice(0..0, ops.clone().map(|o| o.borrow().val.clone()), out);
        let mut pred_dec = OpIdListDecoder::new(
            self.pred_group.decoder(&[]),
            self.pred_actor.decoder(&[]),
            self.pred_ctr.decoder(&[]),
        );
        let (pred_group, pred_actor, pred_ctr) =
            pred_dec.splice(0..0, ops.map(|o| o.borrow().pred.clone()), out);
        Self {
            obj_actor: obj_actor.into(),
            obj_counter: obj_counter.into(),
            key_actor: key_actor.into(),
            key_counter: key_counter.into(),
            key_string: key_string.into(),
            insert: insert.into(),
            action: action.into(),
            val_meta: val_meta.into(),
            val_raw: val_raw.into(),
            pred_group: pred_group.into(),
            pred_actor: pred_actor.into(),
            pred_ctr: pred_ctr.into(),
        }
    }

    pub(crate) fn metadata(&self) -> ColumnMetadata {
        const OBJ_COL_ID: ColumnId = ColumnId::new(0);
        const KEY_COL_ID: ColumnId = ColumnId::new(1);
        const INSERT_COL_ID: ColumnId = ColumnId::new(3);
        const ACTION_COL_ID: ColumnId = ColumnId::new(4);
        const VAL_COL_ID: ColumnId = ColumnId::new(5);
        const PRED_COL_ID: ColumnId = ColumnId::new(7); 
        
        let mut cols = vec![
            (ColumnSpec::new(OBJ_COL_ID, ColumnType::Actor, false), self.obj_actor.clone().into()),
            (ColumnSpec::new(OBJ_COL_ID, ColumnType::Integer, false), self.obj_counter.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::Actor, false), self.key_actor.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false), self.key_counter.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::String, false), self.key_string.clone().into()),
            (ColumnSpec::new(INSERT_COL_ID, ColumnType::Boolean, false), self.insert.clone().into()),
            (ColumnSpec::new(ACTION_COL_ID, ColumnType::Integer, false), self.action.clone().into()),
            (ColumnSpec::new(VAL_COL_ID, ColumnType::ValueMetadata, false), self.val_meta.clone().into()),
        ];
        if self.val_raw.len() > 0 {
            cols.push((
                ColumnSpec::new(VAL_COL_ID, ColumnType::Value, false), self.val_raw.clone().into()
            ));
        }
        cols.push(
            (ColumnSpec::new(PRED_COL_ID, ColumnType::Group, false), self.pred_group.clone().into()),
        );
        if self.pred_actor.len() > 0 {
            cols.extend([
                (ColumnSpec::new(PRED_COL_ID, ColumnType::Actor, false), self.pred_actor.clone().into()),
                (ColumnSpec::new(PRED_COL_ID, ColumnType::DeltaInteger, false), self.pred_ctr.clone().into()),
            ]);
        }
        cols.into_iter().collect()
    }
}


#[derive(thiserror::Error, Debug)]
pub(crate) enum ReadChangeOpError {
    #[error("unexpected null in column {0}")]
    UnexpectedNull(String),
    #[error("invalid value in column {column}: {description}")]
    InvalidValue { column: String, description: String },
}

pub(crate) struct ChangeOpsIter<'a> {
    failed: bool,
    obj: ObjDecoder<'a>,
    key: KeyDecoder<'a>,
    insert: BooleanDecoder<'a>,
    action: RleDecoder<'a, u64>,
    val: ValueDecoder<'a>,
    pred: OpIdListDecoder<'a>,
}

impl<'a> ChangeOpsIter<'a> {
    fn done(&self) -> bool {
        [
            self.obj.done(),
            self.key.done(),
            self.insert.done(),
            self.action.done(),
            self.val.done(),
            self.pred.done(),
        ]
        .iter()
        .all(|e| *e)
    }

    fn try_next(&mut self) -> Result<Option<ChangeOp<'a>>, ReadChangeOpError> {
        if self.failed {
            Ok(None)
        } else if self.done() {
            Ok(None)
        } else {
            let obj = self
                .obj
                .next()
                .transpose()
                .map_err(|e| self.handle_error("object", e))?
                .ok_or(ReadChangeOpError::UnexpectedNull("object".to_string()))?;
            let key = self
                .key
                .next()
                .transpose()
                .map_err(|e| self.handle_error("key", e))?
                .ok_or(ReadChangeOpError::UnexpectedNull("key".to_string()))?;
            let insert = self
                .insert
                .next()
                .ok_or(ReadChangeOpError::UnexpectedNull("insert".to_string()))?;
            let action = self
                .action
                .next()
                .flatten()
                .ok_or(ReadChangeOpError::UnexpectedNull("action".to_string()))?;
            let val = self
                .val
                .next()
                .transpose()
                .map_err(|e| self.handle_error("value", e))?
                .ok_or(ReadChangeOpError::UnexpectedNull("value".to_string()))?;
            let pred = self
                .pred
                .next()
                .transpose()
                .map_err(|e| self.handle_error("pred", e))?
                .ok_or(ReadChangeOpError::UnexpectedNull("pred".to_string()))?;
            Ok(Some(ChangeOp {
                obj: obj.into(),
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
    type Item = Result<ChangeOp<'a>, ReadChangeOpError>;

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
    #[error("not enough columns")]
    NotEnoughColumns,
}

impl From<MismatchingColumn> for ParseChangeColumnsError {
    fn from(m: MismatchingColumn) -> Self {
        Self::MismatchingColumn{index: m.index} 
    }
}

impl TryFrom<ColumnLayout> for ChangeOpsColumns {
    type Error = ParseChangeColumnsError;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<Range<usize>> = None;
        let mut obj_ctr: Option<Range<usize>> = None;
        let mut key_actor: Option<Range<usize>> = None;
        let mut key_ctr: Option<Range<usize>> = None;
        let mut key_str: Option<Range<usize>> = None;
        let mut insert: Option<Range<usize>> = None;
        let mut action: Option<Range<usize>> = None;
        let mut val_meta: Option<Range<usize>> = None;
        let mut val_raw: Option<Range<usize>> = None;
        let mut pred_group: Option<Range<usize>> = None;
        let mut pred_actor: Option<Range<usize>> = None;
        let mut pred_ctr: Option<Range<usize>> = None;
        let mut other = ColumnLayout::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match index {
                0 => assert_col_type(index, col, ColumnType::Actor, &mut obj_actor)?,
                1 => assert_col_type(index, col, ColumnType::Integer, &mut obj_ctr)?,
                2 => assert_col_type(index, col, ColumnType::Actor, &mut key_actor)?,
                3 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut key_ctr)?,
                4 => assert_col_type(index, col, ColumnType::String, &mut key_str)?,
                5 => assert_col_type(index, col, ColumnType::Boolean, &mut insert)?,
                6 => assert_col_type(index, col, ColumnType::Integer, &mut action)?,
                7 => match col.ranges() {
                    ColumnRanges::Value{meta, val} => {
                        val_meta = Some(meta);
                        val_raw = Some(val);
                    },
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn{ index }),
                },
                8 => match col.ranges() {
                    ColumnRanges::Group{num, mut cols} => {
                        pred_group = Some(num.into());
                        // If there was no data in the group at all then the columns won't be
                        // present
                        if cols.len() == 0 {
                            pred_actor = Some((0..0).into());
                            pred_ctr = Some((0..0).into());
                        } else {
                            let first = cols.next();
                            let second = cols.next();
                            match (first, second) {
                                (Some(GroupColRange::Single(actor_range)), Some(GroupColRange::Single(ctr_range))) =>
                                {
                                    pred_actor = Some(actor_range.into());
                                    pred_ctr = Some(ctr_range.into());
                                },
                                _ => return Err(ParseChangeColumnsError::MismatchingColumn{ index }),
                            }
                        }
                        if let Some(_) = cols.next() {
                            return Err(ParseChangeColumnsError::MismatchingColumn{ index });
                        }
                    },
                    _ => return Err(ParseChangeColumnsError::MismatchingColumn{ index }),
                },
                _ => {
                    other.append(col);
                }
            }
        }
        Ok(ChangeOpsColumns {
            obj_actor: obj_actor.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            obj_counter: obj_ctr.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            key_actor: key_actor.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            key_counter: key_ctr.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            key_string: key_str.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            insert: insert.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            action: action.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            val_meta: val_meta.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            val_raw: val_raw.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            pred_group: pred_group.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            pred_actor: pred_actor.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
            pred_ctr: pred_ctr.ok_or(ParseChangeColumnsError::NotEnoughColumns)?.into(),
        })
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::rowblock::encoding::properties::{key, opid, value};
    use proptest::prelude::*;

    prop_compose! {
        fn change_op()
                    (key in key(),
                     value in value(),
                     pred in proptest::collection::vec(opid(), 0..20),
                     action in 0_u64..6,
                     obj in opid(),
                     insert in any::<bool>()) -> ChangeOp<'static> {
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
            let cols = ChangeOpsColumns::empty();
            let mut out = Vec::new();
            let cols2 = cols.encode(ops.iter(), &mut out);
            let decoded = cols2.iter(&out[..]).collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(ops, decoded);
        }
    }
}
