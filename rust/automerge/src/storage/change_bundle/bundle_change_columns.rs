use std::{borrow::Cow, convert::TryFrom, ops::Range};

use hexane::{ColumnCursor, CursorIter, StrCursor};

use crate::{
    columnar::{
        column_range::{
            generic::{GenericColumnRange, GroupRange, GroupedColumnRange, SimpleColRange},
            DepsIter,
        },
        encoding::{ColumnDecoder, DecodeColumnError, DeltaDecoder, RleDecoder},
    },
    storage::{
        columns::{compression, ColumnId, ColumnSpec, ColumnType},
        Columns, MismatchingColumn, RawColumn, RawColumns,
    },
};

use super::{BundleChangeMetadata, CommitRange};

const ACTOR_COL_ID: ColumnId = ColumnId::new(0);
const SEQ_COL_ID: ColumnId = ColumnId::new(1);
const MAX_OP_COL_ID: ColumnId = ColumnId::new(2);
const TIME_COL_ID: ColumnId = ColumnId::new(3);
const MESSAGE_COL_ID: ColumnId = ColumnId::new(4);
const DEPS_COL_ID: ColumnId = ColumnId::new(5);
const EXTERNAL_DEPS_COL_ID: ColumnId = ColumnId::new(6);
const EXTRA_COL_ID: ColumnId = ColumnId::new(7);

#[derive(Debug, Clone)]
pub(crate) struct BundleChangeColumns {
    pub(crate) actor: Range<usize>,
    pub(crate) seq: Range<usize>,
    pub(crate) max_op: Range<usize>,
    pub(crate) time: Range<usize>,
    pub(crate) message: Range<usize>,
    pub(crate) deps_count: Range<usize>,
    pub(crate) deps: Range<usize>,
    pub(crate) external_deps_count: Range<usize>,
    pub(crate) external_deps: Range<usize>,
    pub(crate) extra_meta: Range<usize>,
    pub(crate) extra_raw: Range<usize>,
    #[allow(dead_code)]
    pub(crate) other: Columns,
}

impl BundleChangeColumns {
    pub(crate) fn new(opset: &OpSet, change_graph: &ChangeGraph, ranges: &[CommitRange]) -> Self {}

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> BundleChangeColumnIter<'a> {
        todo!()
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
                self.deps_count.clone(),
            ),
        ];
        if self.deps.len() > 0 {
            cols.push(RawColumn::new(
                ColumnSpec::new(DEPS_COL_ID, ColumnType::DeltaInteger, false),
                self.deps.clone(),
            ))
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(EXTERNAL_DEPS_COL_ID, ColumnType::Group, false),
            self.deps.clone(),
        ));
        if self.external_deps.len() > 0 {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXTERNAL_DEPS_COL_ID, ColumnType::Integer, false),
                self.external_deps.clone(),
            ));
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(EXTRA_COL_ID, ColumnType::ValueMetadata, false),
            self.extra_meta.clone(),
        ));
        if !self.extra_raw.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXTRA_COL_ID, ColumnType::Value, false),
                self.extra_raw.clone().into(),
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
pub(crate) struct BundleChangeColumnIter<'a> {
    actors: RleDecoder<'a, u64>,
    seq: DeltaDecoder<'a>,
    max_op: DeltaDecoder<'a>,
    time: DeltaDecoder<'a>,
    //message: Option<RleDecoder<'a, smol_str::SmolStr>>,
    message: CursorIter<'a, StrCursor>,
    deps: DepsIter<'a>,
    extra: ExtraDecoder<'a>,
}

impl<'a> BundleChangeColumnIter<'a> {
    fn try_next(&mut self) -> Result<Option<BundleChangeMetadata<'a>>, ReadChangeError> {
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
        Ok(Some(BundleChangeMetadata {
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

impl<'a> Iterator for BundleChangeColumnIter<'a> {
    type Item = Result<BundleChangeMetadata<'a>, ReadChangeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl BundleChangeColumnIter<'_> {
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

impl TryFrom<Columns> for BundleChangeColumns {
    type Error = ReadChangeError;

    fn try_from(columns: Columns) -> Result<Self, Self::Error> {
        let mut actor: Option<Range<usize>> = None;
        let mut seq: Option<Range<usize>> = None;
        let mut max_op: Option<Range<usize>> = None;
        let mut time: Option<Range<usize>> = None;
        let mut message: Option<Range<usize>> = None;
        let mut deps_count: Option<Range<usize>> = None;
        let mut deps: Option<Range<usize>> = None;
        let mut external_deps_count: Option<Range<usize>> = None;
        let mut external_deps: Option<Range<usize>> = None;
        let mut extra_meta: Option<Range<usize>> = None;
        let mut extra_value: Option<Range<usize>> = None;
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
                        deps_count = Some(deps_group.into());
                        deps = Some(deps_index.into());
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (EXTERNAL_DEPS_COL_ID, ColumnType::Group) => match col.into_ranges() {
                    GenericColumnRange::Group(GroupRange { num, values }) => {
                        let mut cols = values.into_iter();
                        let deps_group = num;
                        let first = cols.next();
                        let deps_index = match first {
                            Some(GroupedColumnRange::Simple(SimpleColRange::RleInt(
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
                        external_deps_count = Some(deps_group.into());
                        external_deps = Some(deps_index.into());
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (EXTRA_COL_ID, ColumnType::ValueMetadata) => match col.into_ranges() {
                    GenericColumnRange::Value(val) => {
                        extra_meta = Some(val.meta_range().clone().into());
                        extra_value = Some(val.raw_range().clone().into());
                    }
                    _ => return Err(ReadChangeError::MismatchingColumn { index }),
                },
                (other_id, other_type) => {
                    tracing::warn!(id=?other_id, typ=?other_type, "unknown column");
                    other.append(col);
                }
            }
        }
        Ok(BundleChangeColumns {
            other,
            actor: actor.unwrap_or_default(),
            seq: seq.unwrap_or_default(),
            max_op: max_op.unwrap_or_default(),
            time: time.unwrap_or_default(),
            message: message.unwrap_or_default(),
            deps_count: deps_count.unwrap_or_default(),
            deps: deps.unwrap_or_default(),
            external_deps_count: external_deps_count.unwrap_or_default(),
            external_deps: external_deps.unwrap_or_default(),
            extra_meta: extra_meta.unwrap_or_default(),
            extra_raw: extra_value.unwrap_or_default(),
        })
    }
}
