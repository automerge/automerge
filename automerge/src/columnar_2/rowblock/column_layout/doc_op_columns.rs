use std::{convert::TryFrom, ops::Range};

use tracing::instrument;

use crate::columnar_2::storage::ColumnMetadata;

use super::{
    super::{
        super::column_specification::ColumnType,
        column_range::{
            ActorRange, BooleanRange, DeltaIntRange, RawRange, RleIntRange, RleStringRange,
        },
        encoding::{
            BooleanDecoder, DecodeColumnError, Key, KeyDecoder, ObjDecoder, OpIdDecoder,
            OpIdListDecoder, RleDecoder, ValueDecoder,
        },
        ColumnSpec, ColumnId,
    },
    assert_col_type,
    column::{ColumnRanges, GroupColRange},
    ColumnLayout, MismatchingColumn,
};

use crate::{
    columnar_2::rowblock::{PrimVal, encoding::{RawDecoder, DeltaDecoder}},
    types::{ObjId, OpId, ElemId},
};

/// The form operations take in the compressed document format.
#[derive(Debug)]
pub(crate) struct DocOp<'a> {
    pub(crate) id: OpId,
    pub(crate) object: ObjId,
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: usize,
    pub(crate) value: PrimVal<'a>,
    pub(crate) succ: Vec<OpId>,
}

pub(crate) struct DocOpColumns {
    actor: ActorRange,
    ctr: RleIntRange,
    key_actor: ActorRange,
    key_ctr: DeltaIntRange,
    key_str: RleStringRange,
    id_actor: RleIntRange,
    id_ctr: DeltaIntRange,
    insert: BooleanRange,
    action: RleIntRange,
    val_meta: RleIntRange,
    val_raw: RawRange,
    succ_group: RleIntRange,
    succ_actor: RleIntRange,
    succ_ctr: DeltaIntRange,
    other: ColumnLayout,
}

impl DocOpColumns {
    pub(crate) fn empty() -> DocOpColumns {
        Self {
            actor: (0..0).into(),
            ctr: (0..0).into(),
            key_actor: (0..0).into(),
            key_ctr: (0..0).into(),
            key_str: (0..0).into(),
            id_actor: (0..0).into(),
            id_ctr: (0..0).into(),
            insert: (0..0).into(),
            action: (0..0).into(),
            val_meta: (0..0).into(),
            val_raw: (0..0).into(),
            succ_group: (0..0).into(),
            succ_actor: (0..0).into(),
            succ_ctr: (0..0).into(),
            other: ColumnLayout::empty(),
        }
    }

    pub(crate) fn encode<'a, I>(ops: I, out: &mut Vec<u8>) -> DocOpColumns
    where
        I: Iterator<Item = DocOp<'a>> + Clone,
    {
        let obj_actor = ActorRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(o.object.opid().actor() as u64)),
            out,
        );
        let obj_ctr = RleIntRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(o.object.opid().counter() as u64)),
            out,
        );
        let key_actor = ActorRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match o.key {
                Key::Prop(_) => None,
                Key::Elem(ElemId(opid)) if opid.actor() == 0 => None,
                Key::Elem(ElemId(opid)) => Some(opid.actor() as u64),
            }),
            out,
        );
        let key_ctr = DeltaIntRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match o.key {
                Key::Prop(_) => None,
                Key::Elem(ElemId(opid)) => Some(opid.counter() as i64),
            }),
            out,
        );
        let key_str = RleStringRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| match o.key {
                Key::Prop(s) => Some(s),
                Key::Elem(_) => None,
            }),
            out,
        );
        let id_actor = RleIntRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(o.id.actor() as u64)),
            out,
        );
        let id_counter = DeltaIntRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(o.id.counter() as i64)),
            out,
        );
        let insert = BooleanRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| o.insert),
            out,
        );
        let action = RleIntRange::from(0..0).decoder(&[]).splice(
            0..0,
            ops.clone().map(|o| Some(o.action as u64)),
            out,
        );
        let (val_meta, val_raw) = ValueDecoder::new(RleDecoder::from(&[] as &[u8]), RawDecoder::from(&[] as &[u8])).splice(
            0..0,
            ops.clone().map(|o| o.value),
            out,
        );
        let mut succ_dec = OpIdListDecoder::new(
            RleDecoder::from(&[] as &[u8]),
            RleDecoder::from(&[] as &[u8]),
            DeltaDecoder::from(&[] as &[u8]),
        );
        let (succ_group, succ_actor, succ_ctr) =
            succ_dec.splice(0..0, ops.map(|o| o.succ.clone()), out);
        Self {
            actor: obj_actor.into(),
            ctr: obj_ctr.into(),
            key_actor: key_actor.into(),
            key_ctr: key_ctr.into(),
            key_str: key_str.into(),
            id_actor: id_actor.into(),
            id_ctr: id_counter.into(),
            insert: insert.into(),
            action: action.into(),
            val_meta: val_meta.into(),
            val_raw: val_raw.into(),
            succ_group: succ_group.into(),
            succ_actor: succ_actor.into(),
            succ_ctr: succ_ctr.into(),
            other: ColumnLayout::empty(),
        }
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocOpColumnIter<'a> {
        DocOpColumnIter {
            id: OpIdDecoder::new(self.id_actor.decoder(data), self.id_ctr.decoder(data)),
            action: self.action.decoder(data),
            objs: ObjDecoder::new(self.actor.decoder(data), self.ctr.decoder(data)),
            keys: KeyDecoder::new(
                self.key_actor.decoder(data),
                self.key_ctr.decoder(data),
                self.key_str.decoder(data),
            ),
            insert: self.insert.decoder(data),
            value: ValueDecoder::new(self.val_meta.decoder(data), self.val_raw.decoder(data)),
            succ: OpIdListDecoder::new(
                self.succ_group.decoder(data),
                self.succ_actor.decoder(data),
                self.succ_ctr.decoder(data),
            ),
        }
    }

    pub(crate) fn metadata(&self) -> ColumnMetadata {
        const OBJ_COL_ID: ColumnId = ColumnId::new(0);
        const KEY_COL_ID: ColumnId = ColumnId::new(1);
        const ID_COL_ID: ColumnId = ColumnId::new(2);
        const INSERT_COL_ID: ColumnId = ColumnId::new(3);
        const ACTION_COL_ID: ColumnId = ColumnId::new(4);
        const VAL_COL_ID: ColumnId = ColumnId::new(5);
        const SUCC_COL_ID: ColumnId = ColumnId::new(8); 
        
        let mut cols = vec![
            (ColumnSpec::new(OBJ_COL_ID, ColumnType::Actor, false), self.actor.clone().into()),
            (ColumnSpec::new(OBJ_COL_ID, ColumnType::Integer, false), self.ctr.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::Actor, false), self.key_actor.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::DeltaInteger, false), self.key_ctr.clone().into()),
            (ColumnSpec::new(KEY_COL_ID, ColumnType::String, false), self.key_str.clone().into()),
            (ColumnSpec::new(ID_COL_ID, ColumnType::Actor, false), self.id_actor.clone().into()),
            (ColumnSpec::new(ID_COL_ID, ColumnType::DeltaInteger, false), self.id_ctr.clone().into()),
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
            (ColumnSpec::new(SUCC_COL_ID, ColumnType::Group, false), self.succ_group.clone().into()),
        );
        if self.succ_actor.len() > 0 {
            cols.extend([
                (ColumnSpec::new(SUCC_COL_ID, ColumnType::Actor, false), self.succ_actor.clone().into()),
                (ColumnSpec::new(SUCC_COL_ID, ColumnType::DeltaInteger, false), self.succ_ctr.clone().into()),
            ]);
        }
        cols.into_iter().collect()
    }
}

pub(crate) struct DocOpColumnIter<'a> {
    id: OpIdDecoder<'a>,
    action: RleDecoder<'a, u64>,
    objs: ObjDecoder<'a>,
    keys: KeyDecoder<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueDecoder<'a>,
    succ: OpIdListDecoder<'a>,
}

impl<'a> DocOpColumnIter<'a> {
    fn done(&self) -> bool {
        [
            self.id.done(),
            self.action.done(),
            self.objs.done(),
            self.keys.done(),
            self.insert.done(),
            self.value.done(),
            self.succ.done(),
        ]
        .iter()
        .all(|c| *c)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DecodeOpError {
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
                return Some(Err(DecodeOpError::UnexpectedNull(format!(
                    "{}:{}", $col, inner_col
                ))));
            },
            DecodeColumnError::InvalidValue{column, description} => {
                let col = format!("{}:{}", $col, column);
                return Some(Err(DecodeOpError::InvalidValue{column: col, description}))
            }
        }
        None => return Some(Err(DecodeOpError::UnexpectedNull($col.to_string()))),
    }
});

impl<'a> Iterator for DocOpColumnIter<'a> {
    type Item = Result<DocOp<'a>, DecodeOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done() {
            None
        } else {
            let id = next_or_invalid!(self.id, "opid");
            let action = match self.action.next() {
                Some(Some(a)) => a,
                Some(None) | None => {
                    return Some(Err(DecodeOpError::UnexpectedNull("action".to_string())))
                }
            };
            let obj = next_or_invalid!(self.objs, "obj").into();
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
    #[error("not enough columns")]
    NotEnoughColumns,
}

impl From<MismatchingColumn> for Error {
    fn from(m: MismatchingColumn) -> Self {
        Error::MismatchingColumn { index: m.index }
    }
}

impl TryFrom<ColumnLayout> for DocOpColumns {
    type Error = Error;

    #[instrument]
    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut obj_actor: Option<Range<usize>> = None;
        let mut obj_ctr: Option<Range<usize>> = None;
        let mut key_actor: Option<Range<usize>> = None;
        let mut key_ctr: Option<Range<usize>> = None;
        let mut key_str: Option<Range<usize>> = None;
        let mut id_actor: Option<Range<usize>> = None;
        let mut id_ctr: Option<Range<usize>> = None;
        let mut insert: Option<Range<usize>> = None;
        let mut action: Option<Range<usize>> = None;
        let mut val_meta: Option<Range<usize>> = None;
        let mut val_raw: Option<Range<usize>> = None;
        let mut succ_group: Option<Range<usize>> = None;
        let mut succ_actor: Option<Range<usize>> = None;
        let mut succ_ctr: Option<Range<usize>> = None;
        let mut other = ColumnLayout::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match index {
                0 => assert_col_type(index, col, ColumnType::Actor, &mut obj_actor)?,
                1 => assert_col_type(index, col, ColumnType::Integer, &mut obj_ctr)?,
                2 => assert_col_type(index, col, ColumnType::Actor, &mut key_actor)?,
                3 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut key_ctr)?,
                4 => assert_col_type(index, col, ColumnType::String, &mut key_str)?,
                5 => assert_col_type(index, col, ColumnType::Actor, &mut id_actor)?,
                6 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut id_ctr)?,
                7 => assert_col_type(index, col, ColumnType::Boolean, &mut insert)?,
                8 => assert_col_type(index, col, ColumnType::Integer, &mut action)?,
                9 => match col.ranges() {
                    ColumnRanges::Value { meta, val } => {
                        val_meta = Some(meta);
                        val_raw = Some(val);
                    }
                    _ => {
                        tracing::error!("col 9 should be a value column");
                        return Err(Error::MismatchingColumn { index });
                    },
                },
                10 => match col.ranges() {
                    ColumnRanges::Group { num, mut cols } => {
                        let first = cols.next();
                        let second = cols.next();
                        succ_group = Some(num.into());
                        match (first, second) {
                            (
                                Some(GroupColRange::Single(actor_range)),
                                Some(GroupColRange::Single(ctr_range)),
                            ) => {
                                succ_actor = Some(actor_range.into());
                                succ_ctr = Some(ctr_range.into());
                            },
                            (None, None) => {
                                succ_actor = Some((0..0).into());
                                succ_ctr = Some((0..0).into());
                            }
                            _ => {
                                tracing::error!("expected a two column group of (actor, rle int) for index 10");
                                return Err(Error::MismatchingColumn { index });
                            }
                        };
                        if let Some(_) = cols.next() {
                            return Err(Error::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(Error::MismatchingColumn { index }),
                },
                _ => {
                    other.append(col);
                }
            }
        }
        Ok(DocOpColumns {
            actor: obj_actor.ok_or(Error::NotEnoughColumns)?.into(),
            ctr: obj_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            key_actor: key_actor.ok_or(Error::NotEnoughColumns)?.into(),
            key_ctr: key_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            key_str: key_str.ok_or(Error::NotEnoughColumns)?.into(),
            id_actor: id_actor.ok_or(Error::NotEnoughColumns)?.into(),
            id_ctr: id_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            insert: insert.ok_or(Error::NotEnoughColumns)?.into(),
            action: action.ok_or(Error::NotEnoughColumns)?.into(),
            val_meta: val_meta.ok_or(Error::NotEnoughColumns)?.into(),
            val_raw: val_raw.ok_or(Error::NotEnoughColumns)?.into(),
            succ_group: succ_group.ok_or(Error::NotEnoughColumns)?.into(),
            succ_actor: succ_actor.ok_or(Error::NotEnoughColumns)?.into(),
            succ_ctr: succ_ctr.ok_or(Error::NotEnoughColumns)?.into(),
            other,
        })
    }
}
