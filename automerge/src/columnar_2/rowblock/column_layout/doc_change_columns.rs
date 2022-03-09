use std::{borrow::{Borrow, Cow}, convert::TryFrom, ops::Range};

use tracing::instrument;

use crate::columnar_2::{
    column_specification::ColumnType,
    rowblock::{
        column_range::{ActorRange, DeltaIntRange, RawRange, RleIntRange, RleStringRange},
        encoding::{DecodeColumnError, DeltaDecoder, RawDecoder, RleDecoder, ValueDecoder},
        PrimVal,
    },
    ColumnId, ColumnSpec,
    storage::ColumnMetadata,
};

use super::{
    assert_col_type,
    column::{ColumnRanges, GroupColRange},
    ColumnLayout, MismatchingColumn,
};

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

pub(crate) struct DocChangeColumns {
    actor: ActorRange,
    seq: DeltaIntRange,
    max_op: DeltaIntRange,
    time: DeltaIntRange,
    message: RleStringRange,
    deps_group: RleIntRange,
    deps_index: DeltaIntRange,
    extra_meta: RleIntRange,
    extra_val: RawRange,
    other: ColumnLayout,
}

impl DocChangeColumns {
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> DocChangeColumnIter<'a> {
        DocChangeColumnIter {
            actors: self.actor.decoder(data),
            seq: self.seq.decoder(data),
            max_op: self.max_op.decoder(data),
            time: self.time.decoder(data),
            message: self.message.decoder(data),
            deps: DepsDecoder {
                group: self.deps_group.decoder(data),
                deps: self.deps_index.decoder(data),
            },
            extra: ExtraDecoder {
                val: ValueDecoder::new(self.extra_meta.decoder(data), self.extra_val.decoder(data)),
            },
        }
    }

    pub(crate) fn encode<'a, I, C: Borrow<ChangeMetadata<'a>>>(changes: I, out: &mut Vec<u8>) -> DocChangeColumns
    where
        I: Iterator<Item = C> + Clone,
    {
        let actor = ActorRange::from(0..0).decoder(&[]).splice(
            0..0,
            // TODO: make this fallible once iterators have a try_splice
            changes.clone().map(|c| Some(c.borrow().actor as u64)),
            out,
        );
        let seq = DeltaDecoder::from(&[] as &[u8]).splice(
            0..0,
            changes.clone().map(|c| Some(c.borrow().seq as i64)),
            out,
        );
        let max_op = DeltaDecoder::from(&[] as &[u8]).splice(
            0..0,
            changes.clone().map(|c| Some(c.borrow().max_op as i64)),
            out,
        );
        let time = DeltaDecoder::from(&[] as &[u8]).splice(
            0..0,
            changes.clone().map(|c| Some(c.borrow().timestamp)),
            out,
        );
        let message = RleDecoder::<'a, smol_str::SmolStr>::from(&[] as &[u8]).splice(
            0..0,
            changes.clone().map(|c| c.borrow().message.clone()),
            out,
        );
        let (deps_group, deps_index) = DepsDecoder {
            group: RleDecoder::from(&[] as &[u8]),
            deps: DeltaDecoder::from(&[] as &[u8]),
        }
        .splice(0..0, changes.clone().map(|c| c.borrow().deps.clone()), out);
        let (extra_meta, extra_val) = ValueDecoder::new(
            RleDecoder::from(&[] as &[u8]),
            RawDecoder::from(&[] as &[u8]),
        )
        .splice(0..0, changes.clone().map(|c| PrimVal::Bytes(c.borrow().extra.clone())), out);
        DocChangeColumns {
            actor: actor.into(),
            seq: seq.into(),
            max_op: max_op.into(),
            time: time.into(),
            message: message.into(),
            deps_group: deps_group.into(),
            deps_index: deps_index.into(),
            extra_meta: extra_meta.into(),
            extra_val: extra_val.into(),
            other: ColumnLayout::empty(),
        }
    }

    pub(crate) fn metadata(&self) -> ColumnMetadata {
        const ACTOR_COL_ID: ColumnId = ColumnId::new(0);
        const SEQ_COL_ID: ColumnId = ColumnId::new(0);
        const MAX_OP_COL_ID: ColumnId = ColumnId::new(1);
        const TIME_COL_ID: ColumnId = ColumnId::new(2);
        const MESSAGE_COL_ID: ColumnId = ColumnId::new(3);
        const DEPS_COL_ID: ColumnId = ColumnId::new(4);
        const EXTRA_COL_ID: ColumnId = ColumnId::new(5);
        
        let mut cols = vec![
            (ColumnSpec::new(ACTOR_COL_ID, ColumnType::Actor, false), self.actor.clone().into()),
            (ColumnSpec::new(SEQ_COL_ID, ColumnType::DeltaInteger, false), self.seq.clone().into()),
            (ColumnSpec::new(MAX_OP_COL_ID, ColumnType::DeltaInteger, false), self.max_op.clone().into()),
            (ColumnSpec::new(TIME_COL_ID, ColumnType::DeltaInteger, false), self.time.clone().into()),
            (ColumnSpec::new(MESSAGE_COL_ID, ColumnType::String, false), self.message.clone().into()),
            (ColumnSpec::new(DEPS_COL_ID, ColumnType::Group, false), self.deps_group.clone().into()),
        ];
        if self.deps_index.len() > 0 {
            cols.push((
                ColumnSpec::new(DEPS_COL_ID, ColumnType::DeltaInteger, false), self.deps_index.clone().into()
            ))
        }
        cols.push(
            (ColumnSpec::new(EXTRA_COL_ID, ColumnType::ValueMetadata, false), self.extra_meta.clone().into()),
        );
        if self.extra_val.len() > 0 {
            cols.push((
                ColumnSpec::new(EXTRA_COL_ID, ColumnType::Value, false), self.extra_val.clone().into()
            ))
        }
        cols.into_iter().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DecodeChangeError {
    #[error("the depenencies column was invalid")]
    InvalidDeps,
    #[error("unexpected null value for {0}")]
    UnexpectedNull(String),
    #[error("mismatching column types for column {index}")]
    MismatchingColumn { index: usize },
    #[error("not enough columns")]
    NotEnoughColumns,
    #[error("incorrect value in extra bytes column")]
    InvalidExtraBytes,
    #[error("error reading column {column}: {description}")]
    ReadColumn { column: String, description: String },
}

impl DecodeChangeError {
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

impl From<MismatchingColumn> for DecodeChangeError {
    fn from(m: MismatchingColumn) -> Self {
        Self::MismatchingColumn { index: m.index }
    }
}

pub(crate) struct DocChangeColumnIter<'a> {
    actors: RleDecoder<'a, u64>,
    seq: DeltaDecoder<'a>,
    max_op: DeltaDecoder<'a>,
    time: DeltaDecoder<'a>,
    message: RleDecoder<'a, smol_str::SmolStr>,
    deps: DepsDecoder<'a>,
    extra: ExtraDecoder<'a>,
}

macro_rules! next_or_invalid({$iter: expr, $col: literal} => {
    match $iter.next() {
        Some(Some(s)) => s,
        Some(None) => return Some(Err(DecodeChangeError::UnexpectedNull($col.to_string()))),
        None => return Some(Err(DecodeChangeError::UnexpectedNull($col.to_string()))),
    }
});

impl<'a> Iterator for DocChangeColumnIter<'a> {
    type Item = Result<ChangeMetadata<'a>, DecodeChangeError>;

    fn next(&mut self) -> Option<Self::Item> {
        let actor = match self.actors.next() {
            Some(Some(actor)) => actor as usize,
            Some(None) => return Some(Err(DecodeChangeError::UnexpectedNull("actor".to_string()))),
            None => {
                // The actor column should always have a value so if the actor iterator returns None that
                // means we should be done, we check by asserting that all the other iterators
                // return none (which is what Self::is_done does).
                if self.is_done() {
                    return None;
                } else {
                    return Some(Err(DecodeChangeError::UnexpectedNull("actor".to_string())));
                }
            }
        };
        let seq = match next_or_invalid!(self.seq, "seq").try_into() {
            Ok(s) => s,
            Err(_) => {
                return Some(Err(DecodeChangeError::ReadColumn {
                    column: "seq".to_string(),
                    description: "negative value".to_string(),
                }))
            }
        };
        let max_op = match next_or_invalid!(self.max_op, "max_op").try_into() {
            Ok(o) => o,
            Err(_) => {
                return Some(Err(DecodeChangeError::ReadColumn {
                    column: "max_op".to_string(),
                    description: "negative value".to_string(),
                }))
            }
        };
        let time = next_or_invalid!(self.time, "time");
        let message = match self.message.next() {
            Some(Some(s)) => Some(s),
            Some(None) => None,
            None => return Some(Err(DecodeChangeError::UnexpectedNull("msg".to_string()))),
        };
        let deps = match self.deps.next() {
            Some(Ok(d)) => d,
            Some(Err(e)) => return Some(Err(e)),
            None => return Some(Err(DecodeChangeError::UnexpectedNull("deps".to_string()))),
        };
        let extra = match self.extra.next() {
            Some(Ok(e)) => e,
            Some(Err(e)) => return Some(Err(e)),
            None => return Some(Err(DecodeChangeError::UnexpectedNull("extra".to_string()))),
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
    /// Given that we have read a `None` value in the actor column, check that every other column
    /// also returns `None`.
    fn is_done(&mut self) -> bool {
        let other_cols = [
            self.seq.next().is_none(),
            self.max_op.next().is_none(),
            self.time.next().is_none(),
            self.message.next().is_none(),
            self.deps.next().is_none(),
        ];
        other_cols.iter().all(|f| *f)
    }
}

struct DepsDecoder<'a> {
    group: RleDecoder<'a, u64>,
    deps: DeltaDecoder<'a>,
}

impl<'a> DepsDecoder<'a> {

    fn encode<'b, I>(deps: I, out: &'a mut Vec<u8>) -> DepsDecoder<'a> 
    where
        I: Iterator<Item=&'b [u64]> + Clone
    {
        let group = RleDecoder::encode(deps.clone().map(|d| d.len() as u64), out);
        let deps = DeltaDecoder::encode(deps.flat_map(|d| d.iter().map(|d| *d as i64)), out);
        DepsDecoder{
            group: RleDecoder::from(&out[group]),
            deps: DeltaDecoder::from(&out[deps]),
        }
    }

    fn splice<'b, I>(
        &self,
        replace_range: Range<usize>,
        items: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>)
    where
        I: Iterator<Item = Vec<u64>> + Clone,
    {
        let mut replace_start = 0_usize;
        let mut replace_len = 0_usize;
        for (index, elems) in self.group.clone().enumerate() {
            if let Some(elems) = elems {
                if index < replace_range.start {
                    replace_start += elems as usize;
                } else if index < replace_range.end {
                    replace_len += elems as usize;
                }
            }
        }
        let val_replace_range = replace_start..(replace_start + replace_len);
        let group = self.group.clone().splice(
            replace_range,
            items.clone().map(|i| Some(i.len() as u64)),
            out
        );
        let items = self.deps.clone().splice(
            val_replace_range,
            items.flat_map(|elems| elems.into_iter().map(|v| Some(v as i64))),
            out,
        );
        (group, items)
    }
}

impl<'a> Iterator for DepsDecoder<'a> {
    type Item = Result<Vec<u64>, DecodeChangeError>;
    fn next(&mut self) -> Option<Self::Item> {
        let num = match self.group.next() {
            Some(Some(n)) => n as usize,
            Some(None) => return Some(Err(DecodeChangeError::InvalidDeps)),
            None => return None,
        };
        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            match self.deps.next() {
                Some(Some(elem)) => {
                    let elem = match u64::try_from(elem) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::error!(err=?e, dep=elem, "error converting dep index to u64");
                            return Some(Err(DecodeChangeError::InvalidDeps));
                        }
                    };
                    result.push(elem);
                }
                _ => return Some(Err(DecodeChangeError::InvalidDeps)),
            }
        }
        Some(Ok(result))
    }
}

struct ExtraDecoder<'a> {
    val: ValueDecoder<'a>,
}

impl<'a> Iterator for ExtraDecoder<'a> {
    type Item = Result<Cow<'a, [u8]>, DecodeChangeError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.val.next() {
            Some(Ok(PrimVal::Bytes(b))) => Some(Ok(b)),
            Some(Ok(_)) => Some(Err(DecodeChangeError::InvalidExtraBytes)),
            Some(Err(e)) => Some(Err(DecodeChangeError::from_decode_col("value", e))),
            None => None,
        }
    }
}

impl TryFrom<ColumnLayout> for DocChangeColumns {
    type Error = DecodeChangeError;

    #[instrument]
    fn try_from(columns: ColumnLayout) -> Result<Self, Self::Error> {
        let mut actor: Option<Range<usize>> = None;
        let mut seq: Option<Range<usize>> = None;
        let mut max_op: Option<Range<usize>> = None;
        let mut time: Option<Range<usize>> = None;
        let mut message: Option<Range<usize>> = None;
        let mut deps_group: Option<Range<usize>> = None;
        let mut deps_index: Option<Range<usize>> = None;
        let mut extra_meta: Option<Range<usize>> = None;
        let mut extra_val: Option<Range<usize>> = None;
        let mut other = ColumnLayout::empty();

        for (index, col) in columns.into_iter().enumerate() {
            match index {
                0 => assert_col_type(index, col, ColumnType::Actor, &mut actor)?,
                1 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut seq)?,
                2 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut max_op)?,
                3 => assert_col_type(index, col, ColumnType::DeltaInteger, &mut time)?,
                4 => assert_col_type(index, col, ColumnType::String, &mut message)?,
                5 => match col.ranges() {
                    ColumnRanges::Group { num, mut cols } => {
                        deps_group = Some(num.into());
                        let first = cols.next();
                        match first {
                            Some(GroupColRange::Single(index_range)) => {
                                deps_index = Some(index_range.into());
                            }
                            Some(_) => {
                                tracing::error!("deps column contained more than one grouped column");
                                return Err(DecodeChangeError::MismatchingColumn{index: 5});
                            }
                            None => {
                                deps_index = (0..0).into()
                            }
                        };
                        if let Some(_) = cols.next() {
                            return Err(DecodeChangeError::MismatchingColumn { index });
                        }
                    }
                    _ => return Err(DecodeChangeError::MismatchingColumn { index }),
                },
                6 => match col.ranges() {
                    ColumnRanges::Value { meta, val } => {
                        extra_meta = Some(meta);
                        extra_val = Some(val);
                    }
                    _ => return Err(DecodeChangeError::MismatchingColumn { index }),
                },
                _ => {
                    other.append(col);
                }
            }
        }
        Ok(DocChangeColumns {
            actor: actor.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            seq: seq.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            max_op: max_op.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            time: time.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            message: message.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            deps_group: deps_group
                .ok_or(DecodeChangeError::NotEnoughColumns)?
                .into(),
            deps_index: deps_index
                .ok_or(DecodeChangeError::NotEnoughColumns)?
                .into(),
            extra_meta: extra_meta
                .ok_or(DecodeChangeError::NotEnoughColumns)?
                .into(),
            extra_val: extra_val.ok_or(DecodeChangeError::NotEnoughColumns)?.into(),
            other,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::collection::vec as propvec;

    fn encodable_u64() -> impl Strategy<Value = u64> + Clone {
        0_u64..((i64::MAX / 2) as u64)
    }

    proptest!{
        #[test]
        fn encode_decode_deps(deps in propvec(propvec(encodable_u64(), 0..100), 0..100)) {
            let mut out = Vec::new();
            let decoder = DepsDecoder::encode(deps.iter().map(|d| &d[..]), &mut out);
            let decoded = decoder.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(deps, decoded);
        }
    }
}
