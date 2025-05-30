use std::{borrow::Cow, convert::TryFrom};

use hexane::{ColumnCursor, CursorIter, StrCursor};

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            DeltaRange, DepsIter, DepsRange, RleRange, ValueIter, ValueRange,
        },
        encoding::{ColumnDecoder, DecodeColumnError, DeltaDecoder, RleDecoder},
    },
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
    types::ScalarValue,
};

const ACTOR_COL_ID: ColumnId = ColumnId::new(0);
const SEQ_COL_ID: ColumnId = ColumnId::new(0);
const MAX_OP_COL_ID: ColumnId = ColumnId::new(1);
const TIME_COL_ID: ColumnId = ColumnId::new(2);
const MESSAGE_COL_ID: ColumnId = ColumnId::new(3);
const DEPS_COL_ID: ColumnId = ColumnId::new(4);
const EXTRA_COL_ID: ColumnId = ColumnId::new(5);

#[derive(Debug, Clone)]
pub(crate) struct DocChangeMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    //pub(crate) message: Option<smol_str::SmolStr>,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<u64>,
    pub(crate) extra: Cow<'a, [u8]>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocChangeColumns {
    pub(crate) actor: RleRange<u64>,
    pub(crate) seq: DeltaRange,
    pub(crate) max_op: DeltaRange,
    pub(crate) time: DeltaRange,
    pub(crate) message: RleRange<smol_str::SmolStr>,
    pub(crate) deps: DepsRange,
    pub(crate) extra: ValueRange,
    #[allow(dead_code)]
    pub(crate) other: Columns,
}

impl DocChangeColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocChangeColumnIter<'a> {
        DocChangeColumnIter {
            actors: self.actor.decoder(data),
            seq: self.seq.decoder(data),
            max_op: self.max_op.decoder(data),
            time: self.time.decoder(data),
            /*
                        message: if self.message.is_empty() {
                            None
                        } else {
                            Some(self.message.decoder(data))
                        },
            */
            message: hexane::StrCursor::iter(&data[self.message.as_ref().clone()]),
            deps: self.deps.iter(data),
            extra: ExtraDecoder {
                val: self.extra.iter(data),
            },
        }
    }

    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        let mut cols = vec![
            RawColumn::new(
                ColumnSpec::new(ACTOR_COL_ID, ColumnType::Actor, false),
                self.actor.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(SEQ_COL_ID, ColumnType::DeltaInteger, false),
                self.seq.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(MAX_OP_COL_ID, ColumnType::DeltaInteger, false),
                self.max_op.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(TIME_COL_ID, ColumnType::DeltaInteger, false),
                self.time.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(MESSAGE_COL_ID, ColumnType::String, false),
                self.message.clone().into(),
            ),
            RawColumn::new(
                ColumnSpec::new(DEPS_COL_ID, ColumnType::Group, false),
                self.deps.num_range().clone().into(),
            ),
        ];
        if self.deps.deps_range().len() > 0 {
            cols.push(RawColumn::new(
                ColumnSpec::new(DEPS_COL_ID, ColumnType::DeltaInteger, false),
                self.deps.deps_range().clone().into(),
            ))
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(EXTRA_COL_ID, ColumnType::ValueMetadata, false),
            self.extra.meta_range().clone().into(),
        ));
        if !self.extra.raw_range().is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXTRA_COL_ID, ColumnType::Value, false),
                self.extra.raw_range().clone().into(),
            ))
        }
        cols.into_iter().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadChangeError {
    #[error("unexpected null value for {0}")]
    UnexpectedNull(String),
    #[error("mismatching column types for column {index}")]
    MismatchingColumn { index: usize },
    #[error("incorrect value in extra bytes column")]
    InvalidExtraBytes,
    #[error("max_op is lower than start_op")]
    InvalidMaxOp,
    #[error(transparent)]
    ReadColumn(#[from] DecodeColumnError),
    #[error(transparent)]
    PackError(#[from] hexane::PackError),
}

impl From<MismatchingColumn> for ReadChangeError {
    fn from(m: MismatchingColumn) -> Self {
        Self::MismatchingColumn { index: m.index }
    }
}

#[derive(Clone)]
pub(crate) struct DocChangeColumnIter<'a> {
    actors: RleDecoder<'a, u64>,
    seq: DeltaDecoder<'a>,
    max_op: DeltaDecoder<'a>,
    time: DeltaDecoder<'a>,
    //message: Option<RleDecoder<'a, smol_str::SmolStr>>,
    message: CursorIter<'a, StrCursor>,
    deps: DepsIter<'a>,
    extra: ExtraDecoder<'a>,
}

impl<'a> DocChangeColumnIter<'a> {
    fn try_next(&mut self) -> Result<Option<DocChangeMetadata<'a>>, ReadChangeError> {
        let actor = match self.actors.maybe_next_in_col("actor")? {
            Some(actor) => actor as usize,
            None => {
                // The actor column should always have a value so if the actor iterator returns None that
                // means we should be done, we check by asserting that all the other iterators
                // return none (which is what Self::check_done does).
                if self.check_done() {
                    return Ok(None);
                } else {
                    return Err(ReadChangeError::UnexpectedNull("actor".to_string()));
                }
            }
        };
        let seq = self.seq.next_in_col("seq").and_then(|seq| {
            u64::try_from(seq).map_err(|e| DecodeColumnError::invalid_value("seq", e.to_string()))
        })?;
        let max_op = self.max_op.next_in_col("max_op").and_then(|seq| {
            u64::try_from(seq).map_err(|e| DecodeColumnError::invalid_value("seq", e.to_string()))
        })?;
        let time = self.time.next_in_col("time")?;
        // would be nice if this had per column error handling too
        let message = self.message.next().transpose()?.flatten();
        let deps = self.deps.next_in_col("deps")?;
        let extra = self.extra.next().transpose()?.unwrap_or(Cow::Borrowed(&[]));
        Ok(Some(DocChangeMetadata {
            actor,
            seq,
            max_op,
            timestamp: time,
            message,
            deps,
            extra,
        }))
    }
}

impl<'a> Iterator for DocChangeColumnIter<'a> {
    type Item = Result<DocChangeMetadata<'a>, ReadChangeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DocChangeColumnIter<'_> {
    fn check_done(&mut self) -> bool {
        let other_cols = [
            self.seq.next().is_none(),
            self.max_op.next().is_none(),
            self.time.next().is_none(),
            self.deps.next().is_none(),
        ];
        other_cols.iter().any(|f| *f)
    }
}

#[derive(Clone)]
struct ExtraDecoder<'a> {
    val: ValueIter<'a>,
}

impl<'a> Iterator for ExtraDecoder<'a> {
    type Item = Result<Cow<'a, [u8]>, ReadChangeError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.val.next() {
            Some(Ok(ScalarValue::Bytes(b))) => Some(Ok(Cow::Owned(b))),
            Some(Ok(_)) => Some(Err(ReadChangeError::InvalidExtraBytes)),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

impl TryFrom<Columns> for DocChangeColumns {
    type Error = ReadChangeError;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut actor: Option<RleRange<u64>> = None;
        let mut seq: Option<DeltaRange> = None;
        let mut max_op: Option<DeltaRange> = None;
        let mut time: Option<DeltaRange> = None;
        let mut message: Option<RleRange<smol_str::SmolStr>> = None;
        let mut deps: Option<DepsRange> = None;
        let mut extra: Option<ValueRange> = None;
        let mut other = Columns::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (ACTOR_COL_ID, ColumnType::Actor) => actor = Some(col.range().into()),
                (SEQ_COL_ID, ColumnType::DeltaInteger) => seq = Some(col.range().into()),
                (MAX_OP_COL_ID, ColumnType::DeltaInteger) => max_op = Some(col.range().into()),
                (TIME_COL_ID, ColumnType::DeltaInteger) => time = Some(col.range().into()),
                (MESSAGE_COL_ID, ColumnType::String) => message = Some(col.range().into()),
                (DEPS_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        let deps_group = num;
                        let first = cols.next();
                        let deps_index = match first {
                            Some(GroupedColumnRange::Simple(SimpleColRange::Delta(
                                index_range,
                            ))) => index_range,
                            Some(_) => {
                                tracing::error!(
                                    "deps column contained more than one grouped column"
                                );
                                return Err(ReadChangeError::MismatchingColumn { index: 5 });
                            }
                            None => (0..0).into(),
                        };
                        if cols.next().is_some() {
                            return Err(ReadChangeError::MismatchingColumn { index });
                        }
                        deps = Some(DepsRange::new(deps_group, deps_index));
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (EXTRA_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(val) => {
                        extra = Some(val);
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (other_id, other_type) => {
                    tracing::warn!(id=?other_id, typ=?other_type, "unknown column");
                    other.append(col);
                }
            }
        }
        Ok(DocChangeColumns {
            actor: actor.unwrap_or_else(|| (0..0).into()),
            seq: seq.unwrap_or_else(|| (0..0).into()),
            max_op: max_op.unwrap_or_else(|| (0..0).into()),
            time: time.unwrap_or_else(|| (0..0).into()),
            message: message.unwrap_or_else(|| (0..0).into()),
            deps: deps.unwrap_or_else(|| DepsRange::new((0..0).into(), (0..0).into())),
            extra: extra.unwrap_or_else(|| ValueRange::new((0..0).into(), (0..0).into())),
            other,
        })
    }
}
