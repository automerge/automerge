//! DEPRECATED: BundleV0, the bundle format (chunk id 3) written by the
//! Rust crate 0.8–0.12 and the JS package 3.2–3.5, kept so those bundles
//! still load.
//!
//! Frozen: do not change it or share code with `storage::bundle`, whose
//! format has diverged (its reader silently drops a BundleV0's object
//! ids). When chunk id 3 no longer needs supporting, delete this
//! file, the `BundleV0` chunk arm and the `ChunkType::BundleV0` variant.

use std::borrow::Cow;
use std::ops::Range;

use crate::op_set2::change::ChangeCollector;
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::op::OpBuilder;
use crate::op_set2::types::{Action, ActorIdx, KeyRef};
use crate::op_set2::{ReadOpError, ScalarValue};
use crate::storage::bundle::{BundleChange, ParseError};
use crate::storage::columns::{compression, ColumnType};
use crate::storage::{parse, Header, RawColumns};
use crate::types::{ActorId, ChangeHash, ObjId, OpId};
use crate::Change;

#[derive(Clone, Debug)]
pub(crate) struct BundleV0Storage<'a> {
    /// Uncompressed in-memory form. Iterators index into this.
    pub(crate) bytes: Cow<'a, [u8]>,
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
}

impl BundleV0Storage<'_> {
    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }
}

/// Materialise the doc-order id_ctr values. BundleV0 has two counter
/// encodings: the later one stores only `ID_CTR_INVERSE` (reconstructed
/// here by walking change metadata in canonical `(actor, seq)` order and
/// applying `inverse[k] = doc_pos`), the earlier one an explicit
/// doc-order `ID_CTR` column (decoded directly). The inverse column takes
/// precedence if both are somehow present.
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
            id if id == ops::ID_CTR_INVERSE_COL_ID => {
                let d = col.data();
                inverse_bytes = Some(&ops_data[d.start..d.end]);
            }
            id if id == ops::ID_COL_ID => {
                let d = col.data();
                id_ctr_bytes = Some(&ops_data[d.start..d.end]);
            }
            _ => {}
        }
    }

    // Later encoding: reconstruct doc-order counters from the inverse
    // permutation column.
    if let Some(inverse_bytes) = inverse_bytes {
        let inverse: Vec<i64> = decode_delta_int(inverse_bytes)?;

        let mut change_meta: Vec<(usize, u64, u64, u64)> =
            V0ChangeIter::try_new(changes_meta, changes_data)?
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

    // Earlier encoding: decode the explicit doc-order id_ctr column.
    if let Some(id_ctr_bytes) = id_ctr_bytes {
        return decode_delta_int(id_ctr_bytes);
    }

    // No ops — both columns absent.
    Ok(Vec::new())
}

fn decode_delta_int(bytes: &[u8]) -> Result<Vec<i64>, ParseError> {
    hexane::DeltaDecoder::<Option<i64>>::new(bytes)
        .map(|item| item.ok_or(ParseError::InverseDecode))
        .collect()
}

impl<'a> BundleV0Storage<'a> {
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, BundleV0Storage<'a>, ParseError> {
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
            let id_ctr = check_columns(&changes_meta, changes.value, &ops_meta, ops.value)?;
            return Ok((
                parse::Input::empty(),
                BundleV0Storage {
                    bytes: full_bytes.into(),
                    header,
                    deps,
                    actors,
                    ops_meta,
                    ops_data: ops_data_range,
                    changes_meta,
                    changes_data: changes_data_range,
                    id_ctr,
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

        let id_ctr = check_columns(
            &changes_meta,
            &out[new_changes_start..new_changes_end],
            &ops_meta,
            &out[new_ops_start..new_ops_end],
        )?;

        Ok((
            parse::Input::empty(),
            BundleV0Storage {
                bytes: Cow::Owned(out),
                header,
                deps,
                actors,
                ops_meta,
                ops_data: new_ops_start..new_ops_end,
                changes_meta,
                changes_data: new_changes_start..new_changes_end,
                id_ctr,
            },
        ))
    }

    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let change_meta =
            V0ChangeIter::try_new(&self.changes_meta, &self.bytes[self.changes_data.clone()])?
                .collect::<Result<_, _>>()?;
        let actors = crate::actor::ActorTable::from_sorted(self.actors.clone())?;
        let mut collector = ChangeCollector::from_bundle_changes(change_meta, &actors);
        let ops = &self.bytes[self.ops_data.clone()];
        for op in V0OpIter::try_new(&self.ops_meta, ops, &self.id_ctr)? {
            collector.add(op?);
        }
        let bundle = collector
            .decode_bundle(&actors, &self.deps)
            .map_err(|e| ParseError::DecodeBundle(Box::new(e)))?;
        Ok(bundle)
    }
}

/// Rejects column bytes the decoders would panic on, and returns the ops'
/// id counters in document order.
fn check_columns(
    changes_meta: &RawColumns<compression::Uncompressed>,
    changes: &[u8],
    ops_meta: &RawColumns<compression::Uncompressed>,
    ops: &[u8],
) -> Result<Vec<i64>, parse::ParseError<ParseError>> {
    let invalid = |e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e)));
    validate_columns(changes_meta, changes).map_err(invalid)?;
    validate_columns(ops_meta, ops).map_err(invalid)?;
    V0ChangeIter::try_new(changes_meta, changes).map_err(invalid)?;
    let id_ctr = extract_id_ctr_values(changes_meta, changes, ops_meta, ops)
        .map_err(parse::ParseError::Error)?;
    V0OpIter::try_new(ops_meta, ops, &id_ctr).map_err(invalid)?;
    Ok(id_ctr)
}

/// Rejects column bytes the decoders would panic on.
fn validate_columns(
    columns: &RawColumns<compression::Uncompressed>,
    data: &[u8],
) -> Result<(), ParseError> {
    fn check<T: hexane::ColumnValueRef>(bytes: &[u8]) -> Result<(), ParseError> {
        use hexane::ColumnEncoding;
        T::Encoding::<hexane::Leb128>::validate_encoding(bytes)?;
        Ok(())
    }
    for col in columns.iter() {
        let d = &data[col.data()];
        match (col.spec().id(), col.spec().col_type()) {
            (ops::ACTION_COL_ID, ColumnType::Integer) => check::<Option<Action>>(d)?,
            (_, ColumnType::Actor) => check::<Option<ActorIdx>>(d)?,
            (_, ColumnType::DeltaInteger) => check::<Option<i64>>(d)?,
            (_, ColumnType::Integer | ColumnType::Group) => check::<Option<u64>>(d)?,
            (_, ColumnType::Boolean) => check::<bool>(d)?,
            (_, ColumnType::String) => check::<Option<String>>(d)?,
            (_, ColumnType::ValueMetadata) => check::<Option<ValueMeta>>(d)?,
            (_, ColumnType::Value) => {}
        }
    }
    Ok(())
}

#[derive(Debug)]
struct V0ChangeIter<'a> {
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

impl<'a> Iterator for V0ChangeIter<'a> {
    type Item = Result<BundleChange<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> V0ChangeIter<'a> {
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

    fn try_next(&mut self) -> Result<Option<BundleChange<'a>>, ParseError> {
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
        let (extra, tail) = self
            .extra
            .split_at_checked(extra_count)
            .ok_or(ReadOpError::MissingValue("extra"))?;
        let extra = Cow::Borrowed(extra);
        self.extra = tail;

        Ok(Some(BundleChange {
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

struct V0OpIter<'a> {
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

impl<'a> Iterator for V0OpIter<'a> {
    type Item = Result<OpBuilder<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<'a> V0OpIter<'a> {
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
        let (value_raw, tail) = self
            .value
            .split_at_checked(value_meta.length())
            .ok_or(ReadOpError::MissingValue("value"))?;
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

mod ops {
    use crate::storage::columns::ColumnId;

    pub(super) const OBJ_COL_ID: ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID: ColumnId = ColumnId::new(1);
    pub(super) const ID_COL_ID: ColumnId = ColumnId::new(2);
    pub(super) const INSERT_COL_ID: ColumnId = ColumnId::new(3);
    pub(super) const ACTION_COL_ID: ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID: ColumnId = ColumnId::new(5);
    pub(super) const PRED_COL_ID: ColumnId = ColumnId::new(7);
    pub(super) const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);
    pub(super) const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);
}

#[rustfmt::skip]
mod change {
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
