//! DEPRECATED — the change set format as automerge 3.3.0-3.3.2 shipped it.
//!
//! This whole file is a frozen copy of that release's change set reader,
//! reachable only from [`ChunkType::BundleV0`](crate::storage::ChunkType)
//! (chunk id 3). Documents written by those versions are in circulation,
//! so this must keep parsing them byte-for-byte as they were written.
//!
//! **Do not "improve" anything in here, and do not share code with
//! `storage::change_set`.** The live change set format (chunk id 4, with its op
//! columns in chunk id 5) has already diverged — its object counter is an
//! integer column where this one is a delta column, it carries succ and
//! hint columns this one has never seen, and it elides deletes into succ.
//! Reading a 3.3.2 change set with the live reader silently drops every
//! object id. That is exactly why this copy exists.
//!
//! When chunk id 3 no longer needs supporting, delete this file, the
//! `BundleV0` chunk arm, and the `ChunkType::BundleV0` variant. Nothing
//! else should need touching.
#![allow(dead_code)]

use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::ChangeCollector;
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::op::OpBuilder;
use crate::op_set2::types::{Action, ActorIdx, KeyRef};
use crate::op_set2::{ReadOpError, ScalarValue};
use crate::storage::change::{OpReadState, Unverified, Verified};
use crate::storage::change_set::{ChangeSetChange, ParseError};
use crate::storage::columns::{compression, ColumnId, ColumnType};
use crate::storage::{parse, Header, RawColumns};
use crate::types::{ActorId, ChangeHash, ObjId, OpId};
use crate::Change;

/// `(actor, counter)` order index of the `ID_CTR_INVERSE` column.
const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);

/// Column id of the doc-order `ID_CTR` column.
const ID_COL_ID: ColumnId = ColumnId::new(2);

#[derive(Clone, Debug)]
pub(crate) struct BundleV0Storage<'a, OpReadState> {
    /// Uncompressed in-memory form. Iterators index into this.
    pub(crate) bytes: Cow<'a, [u8]>,
    /// On-disk form, if columns were DEFLATE-compressed. `None` for
    /// change sets that were written or received in fully-uncompressed form
    /// (in which case `bytes` is also the on-disk form).
    pub(crate) compressed_bytes: Option<Cow<'a, [u8]>>,
    pub(crate) header: Header,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) actors: Vec<ActorId>,
    pub(crate) ops_meta: RawColumns<compression::Uncompressed>,
    pub(crate) ops_data: Range<usize>,
    pub(crate) changes_meta: RawColumns<compression::Uncompressed>,
    pub(crate) changes_data: Range<usize>,
    /// Op counters in doc order. Materialised at parse time from the
    /// wire's `ID_CTR_INVERSE` column plus the change metadata, then
    /// handed to `V0OpIter` as a plain slice — no columnar encoding round
    /// trip.
    pub(crate) id_ctr: Vec<i64>,
    pub(crate) _phantom: PhantomData<OpReadState>,
}

impl<O: OpReadState> BundleV0Storage<'_, O> {
    pub(crate) fn into_owned(self) -> BundleV0Storage<'static, O> {
        BundleV0Storage {
            bytes: Cow::Owned(self.bytes.into_owned()),
            compressed_bytes: self.compressed_bytes.map(|c| Cow::Owned(c.into_owned())),
            header: self.header,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            id_ctr: self.id_ctr,
            _phantom: self._phantom,
        }
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }
}

/// Materialise the doc-order id_ctr values. Accepts change sets in either
/// the current format (only `ID_CTR_INVERSE` on the wire — reconstructed
/// here by walking change metadata in canonical `(actor, seq)` order and
/// applying `inverse[k] = doc_pos`) or the legacy format (an explicit
/// doc-order `ID_CTR` column — decoded directly). New format takes
/// precedence if both are somehow present. Returns the counters as a
/// plain `Vec<i64>` for `V0OpIter` to read directly — no columnar round
/// trip.
fn extract_id_ctr_values(
    changes_meta: &RawColumns<compression::Uncompressed>,
    changes_data: &[u8],
    ops_meta: &RawColumns<compression::Uncompressed>,
    ops_data: &[u8],
) -> Result<Vec<i64>, ParseError> {
    let mut inverse_bytes: Option<&[u8]> = None;
    let mut id_ctr_bytes: Option<&[u8]> = None;
    for col in ops_meta.0.iter() {
        let spec = col.spec();
        if spec.col_type() != ColumnType::DeltaInteger {
            continue;
        }
        match spec.id() {
            id if id == ID_CTR_INVERSE_COL_ID => {
                let d = col.data();
                inverse_bytes = Some(&ops_data[d.start..d.end]);
            }
            id if id == ID_COL_ID => {
                let d = col.data();
                id_ctr_bytes = Some(&ops_data[d.start..d.end]);
            }
            _ => {}
        }
    }

    // New format: reconstruct doc-order counters from the inverse
    // permutation column.
    if let Some(inverse_bytes) = inverse_bytes {
        let inverse: Vec<i64> = decode_delta_int(inverse_bytes)?;

        let mut change_meta: Vec<(usize, u64, u64, u64)> =
            V0ChangeIterUnverified::try_new(changes_meta, changes_data)?
                .map(|c| c.map(|c| (c.actor, c.seq, c.start_op, c.max_op)))
                .collect::<Result<_, _>>()?;
        change_meta.sort_unstable_by_key(|(actor, seq, _, _)| (*actor, *seq));

        let mut counters = vec![0i64; inverse.len()];
        let mut k = 0usize;
        for (_actor, _seq, start_op, max_op) in &change_meta {
            for ctr in *start_op..=*max_op {
                if k >= inverse.len() {
                    return Err(ParseError::InverseLengthMismatch);
                }
                let doc_pos = inverse[k] as usize;
                if doc_pos >= counters.len() {
                    return Err(ParseError::InverseDecode);
                }
                counters[doc_pos] = ctr as i64;
                k += 1;
            }
        }
        if k != inverse.len() {
            return Err(ParseError::InverseLengthMismatch);
        }
        return Ok(counters);
    }

    // Legacy format: decode the explicit doc-order id_ctr column.
    if let Some(id_ctr_bytes) = id_ctr_bytes {
        return decode_delta_int(id_ctr_bytes);
    }

    // Empty change set (no ops) — both columns absent.
    Ok(Vec::new())
}

fn decode_delta_int(bytes: &[u8]) -> Result<Vec<i64>, ParseError> {
    hexane::DeltaDecoder::<Option<i64>>::new(bytes)
        .map(|item| item.ok_or(ParseError::InverseDecode))
        .collect()
}

impl<'a> BundleV0Storage<'a, Unverified> {
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, BundleV0Storage<'a, Unverified>, ParseError> {
        // `input.bytes()` returns the full chunk (header + body); positions
        // tracked by the parser are absolute offsets within that buffer.
        let full_bytes = input.bytes();

        // Parse the prefix (deps + actors), capturing its byte range so we
        // know where the change-column metadata begins.
        let (i, prefix_r) = parse::range_of(
            |i| -> parse::ParseResult<'_, _, ParseError> {
                let (i, deps) = parse::length_prefixed(parse::change_hash)(i)?;
                let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
                Ok((i, (deps, actors)))
            },
            input,
        )?;
        let (deps, actors) = prefix_r.value;
        let prefix_end = prefix_r.range.end;

        // Change column metadata + data.
        let (i, changes_meta_raw) = RawColumns::parse(i)?;
        let (i, changes) =
            parse::range_of(|i| parse::take_n(changes_meta_raw.total_column_len(), i), i)?;
        let changes_data_range = changes.range.clone();

        // Op column metadata + data.
        let (i, ops_meta_raw) = RawColumns::parse(i)?;
        let (_, ops) = parse::range_of(|i| parse::take_n(ops_meta_raw.total_column_len(), i), i)?;
        let ops_data_range = ops.range.clone();

        // Fast path: nothing is compressed — keep input bytes as-is.
        if let (Some(changes_meta), Some(ops_meta)) =
            (changes_meta_raw.uncompressed(), ops_meta_raw.uncompressed())
        {
            V0ChangeIterUnverified::try_new(&changes_meta, changes.value)
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            let id_ctr = extract_id_ctr_values(&changes_meta, changes.value, &ops_meta, ops.value)
                .map_err(parse::ParseError::Error)?;
            V0OpIterUnverified::try_new(&ops_meta, ops.value, &id_ctr)
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            return Ok((
                parse::Input::empty(),
                BundleV0Storage {
                    bytes: full_bytes.into(),
                    compressed_bytes: None,
                    header,
                    deps,
                    actors,
                    ops_meta,
                    ops_data: ops_data_range,
                    changes_meta,
                    changes_data: changes_data_range,
                    id_ctr,
                    _phantom: PhantomData,
                },
            ));
        }

        // Slow path: at least one column is DEFLATE-encoded. Reconstruct a
        // fully-uncompressed buffer with the same section layout:
        //   header | deps | actors | change_meta' | change_data' | ops_meta' | ops_data'
        // where the primed sections use uncompressed column specs and
        // inflated data. The header bytes inside `out` are preserved
        // verbatim — they only matter for re-emission, and we keep the
        // compressed input around for that.
        let mut out = Vec::with_capacity(full_bytes.len());
        out.extend_from_slice(&full_bytes[..prefix_end]);

        let mut changes_data_buf = Vec::new();
        let changes_meta = changes_meta_raw
            .uncompress(
                &full_bytes[changes_data_range.clone()],
                &mut changes_data_buf,
            )
            .map_err(|_| parse::ParseError::Error(ParseError::CompressedChangeCols))?;
        changes_meta.write(&mut out);
        let new_changes_start = out.len();
        out.extend_from_slice(&changes_data_buf);
        let new_changes_end = out.len();

        let mut ops_data_buf = Vec::new();
        let ops_meta = ops_meta_raw
            .uncompress(&full_bytes[ops_data_range.clone()], &mut ops_data_buf)
            .map_err(|_| parse::ParseError::Error(ParseError::CompressedOpCols))?;
        ops_meta.write(&mut out);
        let new_ops_start = out.len();
        out.extend_from_slice(&ops_data_buf);
        let new_ops_end = out.len();

        V0ChangeIterUnverified::try_new(&changes_meta, &out[new_changes_start..new_changes_end])
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
        let id_ctr = extract_id_ctr_values(
            &changes_meta,
            &out[new_changes_start..new_changes_end],
            &ops_meta,
            &out[new_ops_start..new_ops_end],
        )
        .map_err(parse::ParseError::Error)?;
        V0OpIterUnverified::try_new(&ops_meta, &out[new_ops_start..new_ops_end], &id_ctr)
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        Ok((
            parse::Input::empty(),
            BundleV0Storage {
                bytes: Cow::Owned(out),
                compressed_bytes: Some(full_bytes.into()),
                header,
                deps,
                actors,
                ops_meta,
                ops_data: new_ops_start..new_ops_end,
                changes_meta,
                changes_data: new_changes_start..new_changes_end,
                id_ctr,
                _phantom: PhantomData,
            },
        ))
    }

    pub(crate) fn verify(self) -> Result<BundleV0Storage<'a, Verified>, ParseError> {
        for c in self.iter_change_meta() {
            let _ = c?;
        }
        for o in self.iter_ops() {
            let _ = o?;
        }
        Ok(BundleV0Storage {
            bytes: self.bytes,
            compressed_bytes: self.compressed_bytes,
            header: self.header,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            id_ctr: self.id_ctr,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn iter_ops(&self) -> V0OpIterUnverified<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        V0OpIterUnverified::new(&self.ops_meta, bytes, &self.id_ctr)
    }

    fn iter_change_meta(&self) -> V0ChangeIterUnverified<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        V0ChangeIterUnverified::new(&self.changes_meta, change_data)
    }
}

impl BundleV0Storage<'_, Verified> {
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let change_meta = self.iter_change_meta().collect();
        let mut collector = ChangeCollector::from_change_set_changes(change_meta, &self.actors);
        for op in self.iter_ops() {
            collector.add(op);
        }
        let change_set = collector
            .decode_change_set(&self.actors, &self.deps)
            .map_err(|e| ParseError::DecodeChangeSet(Box::new(e)))?;
        Ok(change_set)
    }

    pub(crate) fn iter_ops(&self) -> V0OpIter<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        V0OpIter::new(&self.ops_meta, bytes, &self.id_ctr)
    }

    pub(crate) fn iter_change_meta(&self) -> V0ChangeIter<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        V0ChangeIter::new_from_verified(&self.changes_meta, change_data)
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}

pub(crate) struct V0ChangeIter<'a>(V0ChangeIterUnverified<'a>);

impl<'a> V0ChangeIter<'a> {
    // this will panic if passed unverified bytes
    pub(crate) fn new_from_verified(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Self {
        Self(V0ChangeIterUnverified::try_new(columns, data).unwrap())
    }
}

impl<'a> Iterator for V0ChangeIter<'a> {
    type Item = ChangeSetChange<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().transpose().unwrap()
    }
}

#[derive(Debug)]
pub(crate) struct V0ChangeIterUnverified<'a> {
    inner: Option<V0ChangeIterInner<'a>>,
}

#[derive(Debug)]
struct V0ChangeIterInner<'a> {
    actor: hexane::Decoder<'a, Option<ActorIdx>>,
    seq: hexane::DeltaDecoder<'a, Option<i64>>,
    max_op: hexane::DeltaDecoder<'a, Option<i64>>,
    start_op: hexane::DeltaDecoder<'a, Option<i64>>,
    timestamp: hexane::DeltaDecoder<'a, Option<i64>>,
    message: hexane::Decoder<'a, Option<String>>,
    dep_count: hexane::Decoder<'a, Option<u64>>,
    deps: hexane::DeltaDecoder<'a, Option<i64>>,
    extra_count: hexane::Decoder<'a, Option<u64>>,
    extra: &'a [u8],
}

impl<'a> Iterator for V0ChangeIterUnverified<'a> {
    type Item = Result<ChangeSetChange<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .as_mut()?
            .try_next()
            .inspect_err(|_| self.inner = None)
            .transpose()
    }
}

impl<'a> V0ChangeIterUnverified<'a> {
    pub(crate) fn new(columns: &RawColumns<compression::Uncompressed>, data: &'a [u8]) -> Self {
        Self {
            inner: V0ChangeIterInner::try_new(columns, data).ok(),
        }
    }

    pub(crate) fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Some(V0ChangeIterInner::try_new(columns, data)?),
        })
    }
}

impl<'a> V0ChangeIterInner<'a> {
    fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        let mut actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut seq = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut max_op = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut start_op = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut timestamp = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut message = hexane::decoder::<Option<String>>(&[]);
        let mut dep_count = hexane::decoder::<Option<u64>>(&[]);
        let mut deps = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut extra_count = hexane::decoder::<Option<u64>>(&[]);
        let mut extra: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            match col.spec() {
                change::ACTOR => actor = hexane::decoder::<Option<ActorIdx>>(d),
                change::SEQ => seq = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::START_OP => start_op = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::MAX_OP => max_op = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::TIMESTAMP => timestamp = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::MESSAGE => message = hexane::decoder::<Option<String>>(d),
                change::DEP_COUNT => dep_count = hexane::decoder::<Option<u64>>(d),
                change::DEPS => deps = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::EXTRA_COUNT => extra_count = hexane::decoder::<Option<u64>>(d),
                change::EXTRA => extra = d,
                spec => return Err(ParseError::InvalidChangeColumn(u32::from(spec))),
            }
        }
        Ok(Self {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            dep_count,
            deps,
            extra_count,
            extra,
        })
    }

    fn try_next(&mut self) -> Result<Option<ChangeSetChange<'a>>, ParseError> {
        let actor = match self.actor.next().flatten() {
            Some(a) => a.into(),
            None => return Ok(None),
        };
        let seq = self
            .seq
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("seq"))? as u64;
        let start_op = self
            .start_op
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("start_op"))? as u64;
        let max_op = self
            .max_op
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("max_op"))? as u64;
        let timestamp = self.timestamp.next().flatten().unwrap_or(0);
        let message = self.message.next().flatten().map(Cow::Borrowed);
        let dep_count = self.dep_count.next().flatten().unwrap_or(0) as usize;

        let mut deps = Vec::with_capacity(dep_count);
        for _ in 0..dep_count {
            let dep = self
                .deps
                .next()
                .flatten()
                .ok_or(ReadOpError::MissingValue("dep"))? as u64;
            deps.push(dep);
        }

        let extra_count = self.extra_count.next().flatten().unwrap_or(0) as usize;
        let (extra, tail) = self.extra.split_at(extra_count);
        let extra = Cow::Borrowed(extra);
        self.extra = tail;

        Ok(Some(ChangeSetChange {
            actor,
            author: None,
            seq,
            max_op,
            start_op,
            timestamp,
            message,
            deps,
            extra,
        }))
    }
}

pub(crate) struct V0OpIterUnverified<'a> {
    inner: Option<V0OpIterInner<'a>>,
}

impl<'a> V0OpIterUnverified<'a> {
    pub(crate) fn new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Self {
        Self {
            inner: V0OpIterInner::try_new(columns, data, id_ctr_values).ok(),
        }
    }

    pub(crate) fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Some(V0OpIterInner::try_new(columns, data, id_ctr_values)?),
        })
    }
}

struct V0OpIterInner<'a> {
    obj_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    obj_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    key_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    key_str: hexane::Decoder<'a, Option<String>>,
    id_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    /// Doc-order counter values reconstructed at parse time.
    id_ctr: std::slice::Iter<'a, i64>,
    insert: hexane::Decoder<'a, bool>,
    action: hexane::Decoder<'a, Option<Action>>,
    meta: hexane::Decoder<'a, Option<ValueMeta>>,
    pred_count: hexane::Decoder<'a, Option<u64>>,
    pred_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    pred_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    expand: hexane::Decoder<'a, bool>,
    mark_name: hexane::Decoder<'a, Option<String>>,
    value: &'a [u8],
}

pub(crate) struct V0OpIter<'a> {
    iter: V0OpIterUnverified<'a>,
}

impl<'a> V0OpIter<'a> {
    pub(crate) fn new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Self {
        Self {
            iter: V0OpIterUnverified::new(columns, data, id_ctr_values),
        }
    }
}

impl<'a> Iterator for V0OpIter<'a> {
    type Item = OpBuilder<'a>;

    fn next(&mut self) -> Option<OpBuilder<'a>> {
        self.iter.next().map(|v| v.unwrap())
    }
}

impl<'a> Iterator for V0OpIterUnverified<'a> {
    type Item = Result<OpBuilder<'a>, ParseError>;

    fn next(&mut self) -> Option<Result<OpBuilder<'a>, ParseError>> {
        self.inner
            .as_mut()?
            .try_next()
            .inspect_err(|_| self.inner = None)
            .transpose()
    }
}

impl<'a> V0OpIterInner<'a> {
    fn try_next(&mut self) -> Result<Option<OpBuilder<'a>>, ParseError> {
        let id_actor = self.id_actor.next().flatten();
        let id_ctr = self.id_ctr.next().copied();
        let id = match OpId::try_load(id_actor, id_ctr) {
            Ok(id) => id,
            Err(_) => return Ok(None),
        };

        let obj_actor = self.obj_actor.next().flatten();
        let obj_ctr = self.obj_ctr.next().flatten();
        let obj = ObjId::try_load(obj_actor, obj_ctr)?;

        let key_str = self.key_str.next().flatten();
        let key_actor = self.key_actor.next().flatten();
        let key_ctr = self.key_ctr.next().flatten();
        let key = KeyRef::try_load(key_str, key_actor, key_ctr)?;

        let action = self
            .action
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("action"))?;
        let insert = self.insert.next().unwrap_or_default();
        let expand = self.expand.next().unwrap_or_default();
        let mark_name = self.mark_name.next().flatten().map(Cow::Borrowed);

        let value_meta = self
            .meta
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("value_meta"))?;
        let (value_raw, tail) = self.value.split_at(value_meta.length());
        self.value = tail;
        let value = ScalarValue::from_raw(value_meta, value_raw)
            .map_err(|_| ReadOpError::MissingValue("value"))?;

        let pred_count = self.pred_count.next().flatten().unwrap_or(0) as usize;
        let mut pred = Vec::with_capacity(pred_count);
        for _ in 0..pred_count {
            let pred_actor = self.pred_actor.next().flatten();
            let pred_ctr = self.pred_ctr.next().flatten();
            pred.push(OpId::try_load(pred_actor, pred_ctr)?);
        }

        Ok(Some(OpBuilder {
            id,
            obj,
            action,
            key,
            value,
            insert,
            expand,
            mark_name,
            pred,
        }))
    }

    fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Result<Self, ParseError> {
        let mut obj_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut obj_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut key_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut key_str = hexane::decoder::<Option<String>>(&[]);
        let mut id_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let id_ctr = id_ctr_values.iter();
        let mut insert = hexane::decoder::<bool>(&[]);
        let mut action = hexane::decoder::<Option<Action>>(&[]);
        let mut meta = hexane::decoder::<Option<ValueMeta>>(&[]);
        let mut pred_count = hexane::decoder::<Option<u64>>(&[]);
        let mut pred_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut pred_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut expand = hexane::decoder::<bool>(&[]);
        let mut mark_name = hexane::decoder::<Option<String>>(&[]);
        let mut value: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            type C = ColumnType;
            match (col.spec().id(), col.spec().col_type()) {
                (ops::OBJ_COL_ID, C::Actor) => obj_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::OBJ_COL_ID, C::DeltaInteger) => {
                    obj_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::KEY_COL_ID, C::Actor) => key_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::KEY_COL_ID, C::DeltaInteger) => {
                    key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::KEY_COL_ID, C::String) => key_str = hexane::decoder::<Option<String>>(d),
                (ops::ID_COL_ID, C::Actor) => id_actor = hexane::decoder::<Option<ActorIdx>>(d),
                // Both counter encodings are handled at the storage layer.
                (ops::ID_CTR_INVERSE_COL_ID, C::DeltaInteger) => {}
                (ops::ID_COL_ID, C::DeltaInteger) => {}
                (ops::INSERT_COL_ID, C::Boolean) => insert = hexane::decoder::<bool>(d),
                (ops::ACTION_COL_ID, C::Integer) => action = hexane::decoder::<Option<Action>>(d),
                (ops::VAL_COL_ID, C::ValueMetadata) => {
                    meta = hexane::decoder::<Option<ValueMeta>>(d)
                }
                (ops::VAL_COL_ID, C::Value) => value = d,
                (ops::PRED_COL_ID, C::Group) => pred_count = hexane::decoder::<Option<u64>>(d),
                (ops::PRED_COL_ID, C::Actor) => pred_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::PRED_COL_ID, C::DeltaInteger) => {
                    pred_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::EXPAND_COL_ID, C::Boolean) => expand = hexane::decoder::<bool>(d),
                (ops::MARK_NAME_COL_ID, C::String) => {
                    mark_name = hexane::decoder::<Option<String>>(d)
                }
                _ => return Err(ParseError::InvalidOpColumn(u32::from(col.spec()))),
            }
        }
        Ok(Self {
            obj_actor,
            obj_ctr,
            key_actor,
            key_ctr,
            key_str,
            id_actor,
            id_ctr,
            insert,
            action,
            meta,
            value,
            pred_count,
            pred_actor,
            pred_ctr,
            expand,
            mark_name,
        })
    }
}

pub(crate) mod ops {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const OBJ_COL_ID: ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID: ColumnId = ColumnId::new(1);
    pub(super) const ID_COL_ID: ColumnId = ColumnId::new(2);
    pub(super) const INSERT_COL_ID: ColumnId = ColumnId::new(3);
    pub(super) const ACTION_COL_ID: ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID: ColumnId = ColumnId::new(5);
    pub(super) const PRED_COL_ID: ColumnId = ColumnId::new(7);
    pub(super) const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);
    /// Inverse permutation of doc positions. For each op in canonical
    /// `(actor, counter)` order, stores its doc-order index as a
    /// delta-int. Readers reconstruct each op's `counter` from this
    /// column plus the change metadata — no separate `ID_CTR` column on
    /// the wire.
    pub(super) const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);

    pub(super) const ID_ACTOR: ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(super) const ID_CTR_INVERSE: ColumnSpec = ColumnSpec::new_delta(ID_CTR_INVERSE_COL_ID);
    pub(super) const OBJ_ACTOR: ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_CTR: ColumnSpec = ColumnSpec::new_delta(OBJ_COL_ID);
    pub(super) const KEY_ACTOR: ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_CTR: ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR: ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(super) const PRED_COUNT: ColumnSpec = ColumnSpec::new_group(PRED_COL_ID);
    pub(super) const PRED_ACTOR: ColumnSpec = ColumnSpec::new_actor(PRED_COL_ID);
    pub(super) const PRED_CTR: ColumnSpec = ColumnSpec::new_delta(PRED_COL_ID);
    pub(super) const INSERT: ColumnSpec = ColumnSpec::new_boolean(INSERT_COL_ID);
    pub(super) const ACTION: ColumnSpec = ColumnSpec::new_integer(ACTION_COL_ID);
    pub(super) const VALUE_META: ColumnSpec = ColumnSpec::new_value_metadata(VAL_COL_ID);
    pub(super) const VALUE: ColumnSpec = ColumnSpec::new_value(VAL_COL_ID);
    pub(super) const MARK_NAME: ColumnSpec = ColumnSpec::new_string(MARK_NAME_COL_ID);
    pub(super) const EXPAND: ColumnSpec = ColumnSpec::new_boolean(EXPAND_COL_ID);
}

#[rustfmt::skip]
pub(crate) mod change {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const ACTOR_COL_ID:           ColumnId = ColumnId::new(0);
    pub(super) const SEQ_COL_ID:             ColumnId = ColumnId::new(0);
    pub(super) const START_OP_COL_ID:        ColumnId = ColumnId::new(1);
    pub(super) const MAX_OP_COL_ID:          ColumnId = ColumnId::new(2);
    pub(super) const TIME_COL_ID:            ColumnId = ColumnId::new(3);
    pub(super) const MESSAGE_COL_ID:         ColumnId = ColumnId::new(4);
    pub(super) const DEPS_COL_ID:            ColumnId = ColumnId::new(5);
    pub(super) const EXTRA_COL_ID:           ColumnId = ColumnId::new(6);

    pub(super) const ACTOR:       ColumnSpec = ColumnSpec::new_actor(ACTOR_COL_ID);
    pub(super) const SEQ:         ColumnSpec = ColumnSpec::new_delta(SEQ_COL_ID);
    pub(super) const START_OP:    ColumnSpec = ColumnSpec::new_delta(START_OP_COL_ID);
    pub(super) const MAX_OP:      ColumnSpec = ColumnSpec::new_delta(MAX_OP_COL_ID);
    pub(super) const TIMESTAMP:   ColumnSpec = ColumnSpec::new_delta(TIME_COL_ID);
    pub(super) const MESSAGE:     ColumnSpec = ColumnSpec::new_string(MESSAGE_COL_ID);
    pub(super) const DEP_COUNT:   ColumnSpec = ColumnSpec::new_group(DEPS_COL_ID);
    pub(super) const DEPS:        ColumnSpec = ColumnSpec::new_delta(DEPS_COL_ID);
    pub(super) const EXTRA_COUNT: ColumnSpec = ColumnSpec::new_group(EXTRA_COL_ID);
    pub(super) const EXTRA:       ColumnSpec = ColumnSpec::new_value(EXTRA_COL_ID);
}
