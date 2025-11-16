use std::{borrow::Cow, convert::TryFrom, ops::Range};

use hexane::{ColumnCursor, CursorIter, DeltaCursor, RawCursor, StrCursor, UIntCursor};

use crate::{
    columnar::column_range::generic::{
        GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange,
    },
    op_set2::ActorCursor,
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
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
    pub(crate) actor: Range<usize>,
    pub(crate) seq: Range<usize>,
    pub(crate) max_op: Range<usize>,
    pub(crate) time: Range<usize>,
    pub(crate) message: Range<usize>,
    pub(crate) deps_num: Range<usize>,
    pub(crate) deps: Range<usize>,
    pub(crate) extra_meta: Range<usize>,
    pub(crate) extra_raw: Range<usize>,
    #[allow(dead_code)]
    pub(crate) other: Columns,
}

impl DocChangeColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocChangeColumnIter<'a> {
        DocChangeColumnIter {
            actors: ActorCursor::iter(&data[self.actor.clone()]),
            seq: DeltaCursor::iter(&data[self.seq.clone()]),
            max_op: DeltaCursor::iter(&data[self.max_op.clone()]),
            time: DeltaCursor::iter(&data[self.time.clone()]),
            message: StrCursor::iter(&data[self.message.clone()]),
            deps: DeltaCursor::iter(&data[self.deps.clone()]),
            deps_num: UIntCursor::iter(&data[self.deps_num.clone()]),
            extra: RawCursor::iter(&data[self.extra_raw.clone()]),
        }
    }

    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        let mut cols = vec![
            RawColumn::new(
                ColumnSpec::new(ACTOR_COL_ID, ColumnType::Actor, false),
                self.actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(SEQ_COL_ID, ColumnType::DeltaInteger, false),
                self.seq.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(MAX_OP_COL_ID, ColumnType::DeltaInteger, false),
                self.max_op.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(TIME_COL_ID, ColumnType::DeltaInteger, false),
                self.time.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(MESSAGE_COL_ID, ColumnType::String, false),
                self.message.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(DEPS_COL_ID, ColumnType::Group, false),
                self.deps_num.clone(),
            ),
        ];
        if !self.deps.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(DEPS_COL_ID, ColumnType::DeltaInteger, false),
                self.deps.clone(),
            ))
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(EXTRA_COL_ID, ColumnType::ValueMetadata, false),
            self.extra_meta.clone(),
        ));
        if !self.extra_raw.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXTRA_COL_ID, ColumnType::Value, false),
                self.extra_raw.clone(),
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
    #[error("max_op is lower than start_op")]
    InvalidMaxOp,
    #[error("error reading column: {0}")]
    ReadColumn(String),
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
    actors: CursorIter<'a, ActorCursor>,
    seq: CursorIter<'a, DeltaCursor>,
    max_op: CursorIter<'a, DeltaCursor>,
    time: CursorIter<'a, DeltaCursor>,
    message: CursorIter<'a, StrCursor>,
    deps_num: CursorIter<'a, UIntCursor>,
    deps: CursorIter<'a, DeltaCursor>,
    extra: CursorIter<'a, RawCursor>,
}

fn maybe_next_in_col<'a, C: ColumnCursor>(
    iter: &mut CursorIter<'a, C>,
    col_name: &str,
) -> Result<Option<Cow<'a, C::Item>>, ReadChangeError> {
    let Some(next) = iter.next() else {
        return Ok(None);
    };
    next.map_err(|e| {
        ReadChangeError::ReadColumn(format!("error reading column {}: {}", col_name, e))
    })
}

fn next_in_col<'a, C: ColumnCursor>(
    iter: &mut CursorIter<'a, C>,
    col_name: &str,
) -> Result<Cow<'a, C::Item>, ReadChangeError> {
    maybe_next_in_col(iter, col_name)?
        .ok_or_else(|| ReadChangeError::UnexpectedNull(col_name.to_string()))
}

impl<'a> DocChangeColumnIter<'a> {
    fn try_next(&mut self) -> Result<Option<DocChangeMetadata<'a>>, ReadChangeError> {
        let actor = match maybe_next_in_col(&mut self.actors, "actor")? {
            Some(actor) => actor.0 as usize,
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
        let seq = next_in_col(&mut self.seq, "seq").and_then(|seq| {
            u64::try_from(*seq)
                .map_err(|e| ReadChangeError::ReadColumn(format!("invalid seq {}: {}", *seq, e)))
        })?;
        let max_op = next_in_col(&mut self.max_op, "max_op").and_then(|seq| {
            u64::try_from(*seq)
                .map_err(|e| ReadChangeError::ReadColumn(format!("invalid max_op {}: {}", *seq, e)))
        })?;
        let time = next_in_col(&mut self.time, "time")?.into_owned();
        let message = self.message.next().transpose()?.flatten();
        let deps_count = maybe_next_in_col(&mut self.deps_num, "deps_num")?
            .unwrap_or(Cow::Owned(0))
            .into_owned() as usize;
        let deps = (0..deps_count)
            .map(|_| {
                next_in_col(&mut self.deps, "deps").and_then(|dep| {
                    u64::try_from(*dep).map_err(|e| {
                        ReadChangeError::ReadColumn(format!("invalid dep {}: {}", *dep, e))
                    })
                })
            })
            .collect::<Result<Vec<u64>, ReadChangeError>>()?;
        let extra = maybe_next_in_col(&mut self.extra, "extra")?.unwrap_or(Cow::Borrowed(&[]));
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

impl TryFrom<Columns> for DocChangeColumns {
    type Error = ReadChangeError;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut actor: Option<Range<usize>> = None;
        let mut seq: Option<Range<usize>> = None;
        let mut max_op: Option<Range<usize>> = None;
        let mut time: Option<Range<usize>> = None;
        let mut message: Option<Range<usize>> = None;
        let mut deps: Option<Range<usize>> = None;
        let mut deps_count: Option<Range<usize>> = None;
        let mut extra_meta: Option<Range<usize>> = None;
        let mut extra_raw: Option<Range<usize>> = None;
        let mut other = Columns::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match (col.id(), col.col_type()) {
                (ACTOR_COL_ID, ColumnType::Actor) => actor = Some(col.range()),
                (SEQ_COL_ID, ColumnType::DeltaInteger) => seq = Some(col.range()),
                (MAX_OP_COL_ID, ColumnType::DeltaInteger) => max_op = Some(col.range()),
                (TIME_COL_ID, ColumnType::DeltaInteger) => time = Some(col.range()),
                (MESSAGE_COL_ID, ColumnType::String) => message = Some(col.range()),
                (DEPS_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        deps_count = Some(num.into());
                        let first = cols.next();
                        deps = match first {
                            Some(GroupedColumnRange::Simple(SimpleColRange::Delta(
                                index_range,
                            ))) => Some(index_range.into()),
                            Some(_) => {
                                tracing::error!(
                                    "deps column contained more than one grouped column"
                                );
                                return Err(ReadChangeError::MismatchingColumn { index: 5 });
                            }
                            None => Some(0..0),
                        };
                        if cols.next().is_some() {
                            return Err(ReadChangeError::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (EXTRA_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(val) => {
                        extra_meta = Some(val.meta_range().clone().into());
                        extra_raw = Some(val.raw_range().clone().into());
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
            actor: actor.unwrap_or_else(|| (0..0)),
            seq: seq.unwrap_or_else(|| (0..0)),
            max_op: max_op.unwrap_or_else(|| (0..0)),
            time: time.unwrap_or_else(|| (0..0)),
            message: message.unwrap_or_else(|| (0..0)),
            deps: deps.unwrap_or_else(|| (0..0)),
            deps_num: deps_count.unwrap_or_else(|| (0..0)),
            extra_meta: extra_meta.unwrap_or_else(|| (0..0)),
            extra_raw: extra_raw.unwrap_or_else(|| (0..0)),
            other,
        })
    }
}
