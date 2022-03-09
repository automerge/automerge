use std::{borrow::Cow, convert::TryFrom};

use crate::{
    columnar_2::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            DeltaRange, DepsIter, DepsRange, RleRange, ValueIter, ValueRange,
        },
        encoding::{DecodeColumnError, DeltaDecoder, RleDecoder},
    },
    storage::{
        column_layout::{compression, ColumnId, ColumnSpec, ColumnType},
        ColumnLayout, MismatchingColumn, RawColumn, RawColumns,
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

#[derive(Debug)]
pub(crate) struct ChangeMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<smol_str::SmolStr>,
    pub(crate) deps: Vec<u64>,
    pub(crate) extra: Cow<'a, [u8]>,
}

/// A row to be encoded as change metadata in the document format
///
/// The lifetime `'a` is the lifetime of the extra bytes Cow. For types which cannot
/// provide a reference (e.g. because they are decoding from some columnar storage on each
/// iteration) this should be `'static`.
pub(crate) trait AsChangeMeta<'a> {
    /// The type of the iterator over dependency indices
    type DepsIter: Iterator<Item = u64> + ExactSizeIterator;

    fn actor(&self) -> u64;
    fn seq(&self) -> u64;
    fn max_op(&self) -> u64;
    fn timestamp(&self) -> i64;
    fn message(&self) -> Option<Cow<'a, smol_str::SmolStr>>;
    fn deps(&self) -> Self::DepsIter;
    fn extra(&self) -> Cow<'a, [u8]>;
}

#[derive(Debug, Clone)]
pub(crate) struct DocChangeColumns {
    actor: RleRange<u64>,
    seq: DeltaRange,
    max_op: DeltaRange,
    time: DeltaRange,
    message: RleRange<smol_str::SmolStr>,
    deps: DepsRange,
    extra: ValueRange,
    #[allow(dead_code)]
    other: ColumnLayout,
}

impl DocChangeColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocChangeColumnIter<'a> {
        DocChangeColumnIter {
            actors: self.actor.decoder(data),
            seq: self.seq.decoder(data),
            max_op: self.max_op.decoder(data),
            time: self.time.decoder(data),
            message: if self.message.is_empty() {
                None
            } else {
                Some(self.message.decoder(data))
            },
            deps: self.deps.iter(data),
            extra: ExtraDecoder {
                val: self.extra.iter(data),
            },
        }
    }

    pub(crate) fn encode<'a, I, C>(changes: I, out: &mut Vec<u8>) -> DocChangeColumns
    where
        C: AsChangeMeta<'a>,
        I: Iterator<Item = C> + Clone,
    {
        let actor = RleRange::<u64>::encode(
            // TODO: make this fallible once iterators have a try_splice
            changes.clone().map(|c| Some(c.actor())),
            out,
        );
        let seq = DeltaRange::encode(changes.clone().map(|c| Some(c.seq() as i64)), out);
        let max_op = DeltaRange::encode(changes.clone().map(|c| Some(c.max_op() as i64)), out);
        let time = DeltaRange::encode(changes.clone().map(|c| Some(c.timestamp())), out);
        let message = RleRange::encode(changes.clone().map(|c| c.message()), out);
        let deps = DepsRange::encode(changes.clone().map(|c| c.deps()), out);
        let extra = ValueRange::encode(
            changes.map(|c| Cow::Owned(ScalarValue::Bytes(c.extra().to_vec()))),
            out,
        );
        DocChangeColumns {
            actor,
            seq,
            max_op,
            time,
            message,
            deps,
            extra,
            other: ColumnLayout::empty(),
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
    #[error("error reading column {column}: {description}")]
    ReadColumn { column: String, description: String },
}

impl ReadChangeError {
    fn from_decode_col(col: &'static str, err: DecodeColumnError) -> Self {
        match err {
            DecodeColumnError::InvalidValue { description, .. } => Self::ReadColumn {
                column: col.to_string(),
                description,
            },
            DecodeColumnError::UnexpectedNull(inner_col) => {
                Self::UnexpectedNull(format!("{}:{}", col, inner_col))
            }
        }
    }
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
    message: Option<RleDecoder<'a, smol_str::SmolStr>>,
    deps: DepsIter<'a>,
    extra: ExtraDecoder<'a>,
}

macro_rules! next_or_invalid({$iter: expr, $col: literal} => {
    match $iter.next() {
        Some(Some(s)) => s,
        Some(None) => return Some(Err(ReadChangeError::UnexpectedNull($col.to_string()))),
        None => return Some(Err(ReadChangeError::UnexpectedNull($col.to_string()))),
    }
});

impl<'a> Iterator for DocChangeColumnIter<'a> {
    type Item = Result<ChangeMetadata<'a>, ReadChangeError>;

    fn next(&mut self) -> Option<Self::Item> {
        let actor = match self.actors.next() {
            Some(Some(actor)) => actor as usize,
            Some(None) => return Some(Err(ReadChangeError::UnexpectedNull("actor".to_string()))),
            None => {
                // The actor column should always have a value so if the actor iterator returns None that
                // means we should be done, we check by asserting that all the other iterators
                // return none (which is what Self::check_done does).
                if self.check_done() {
                    return None;
                } else {
                    return Some(Err(ReadChangeError::UnexpectedNull("actor".to_string())));
                }
            }
        };
        let seq = match next_or_invalid!(self.seq, "seq").try_into() {
            Ok(s) => s,
            Err(_) => {
                return Some(Err(ReadChangeError::ReadColumn {
                    column: "seq".to_string(),
                    description: "negative value".to_string(),
                }))
            }
        };
        let max_op = match next_or_invalid!(self.max_op, "max_op").try_into() {
            Ok(o) => o,
            Err(_) => {
                return Some(Err(ReadChangeError::ReadColumn {
                    column: "max_op".to_string(),
                    description: "negative value".to_string(),
                }))
            }
        };
        let time = next_or_invalid!(self.time, "time");
        let message = if let Some(ref mut message) = self.message {
            match message.next() {
                Some(Some(s)) => Some(s),
                Some(None) => None,
                None => return Some(Err(ReadChangeError::UnexpectedNull("msg".to_string()))),
            }
        } else {
            None
        };
        let deps = match self.deps.next() {
            Some(Ok(d)) => d,
            Some(Err(e)) => return Some(Err(ReadChangeError::from_decode_col("deps", e))),
            None => return Some(Err(ReadChangeError::UnexpectedNull("deps".to_string()))),
        };
        let extra = match self.extra.next() {
            Some(Ok(e)) => e,
            Some(Err(e)) => return Some(Err(e)),
            None => return Some(Err(ReadChangeError::UnexpectedNull("extra".to_string()))),
        };
        Some(Ok(ChangeMetadata {
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

impl<'a> DocChangeColumnIter<'a> {
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
            Some(Err(e)) => Some(Err(ReadChangeError::from_decode_col("value", e))),
            None => None,
        }
    }
}

impl TryFrom<ColumnLayout> for DocChangeColumns {
    type Error = ReadChangeError;

    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut actor: Option<RleRange<u64>> = None;
        let mut seq: Option<DeltaRange> = None;
        let mut max_op: Option<DeltaRange> = None;
        let mut time: Option<DeltaRange> = None;
        let mut message: Option<RleRange<smol_str::SmolStr>> = None;
        let mut deps: Option<DepsRange> = None;
        let mut extra: Option<ValueRange> = None;
        let mut other = ColumnLayout::empty();

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
