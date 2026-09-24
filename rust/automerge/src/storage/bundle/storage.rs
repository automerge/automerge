use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::ChangeCollector;
use crate::op_set2::types::ActorIdx;
use crate::storage::change::{OpReadState, Unverified, Verified};
use crate::storage::columns::compression;
use crate::storage::columns::{ColumnId, ColumnType};
use crate::storage::{parse, Header, RawColumns};
use crate::types::{ActorId, ChangeHash};
use crate::Change;

use super::{BundleChangeIter, BundleChangeIterUnverified, OpIter, OpIterUnverified, ParseError};

/// `(actor, counter)` order index of the `ID_CTR_INVERSE` column. Must
/// match the `ID_CTR_INVERSE_COL_ID` constant in `builder.rs`.
const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);

/// Column id of the legacy doc-order `ID_CTR` column. Bundles produced
/// before the inverse-encoding switch carry this directly; new bundles
/// reconstruct it from `ID_CTR_INVERSE`.
const ID_COL_ID: ColumnId = ColumnId::new(2);

#[derive(Clone, Debug)]
pub(crate) struct BundleStorage<'a, OpReadState> {
    /// Uncompressed in-memory form. Iterators index into this.
    pub(crate) bytes: Cow<'a, [u8]>,
    /// On-disk form, if columns were DEFLATE-compressed. `None` for
    /// bundles that were written or received in fully-uncompressed form
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
    /// handed to `OpIter` as a plain slice — no columnar encoding round
    /// trip.
    pub(crate) id_ctr: Vec<i64>,
    pub(crate) _phantom: PhantomData<OpReadState>,
}

impl<O: OpReadState> BundleStorage<'_, O> {
    pub(crate) fn into_owned(self) -> BundleStorage<'static, O> {
        BundleStorage {
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

/// Materialise the doc-order id_ctr values. Accepts bundles in either
/// the current format (only `ID_CTR_INVERSE` on the wire — reconstructed
/// here by walking change metadata in canonical `(actor, seq)` order and
/// applying `inverse[k] = doc_pos`) or the legacy format (an explicit
/// doc-order `ID_CTR` column — decoded directly). New format takes
/// precedence if both are somehow present. Returns the counters as a
/// plain `Vec<i64>` for `OpIter` to read directly — no columnar round
/// trip.
fn extract_id_ctr_values(
    changes_meta: &RawColumns<compression::Uncompressed>,
    changes_data: &[u8],
    ops_meta: &RawColumns<compression::Uncompressed>,
    ops_data: &[u8],
) -> Result<Vec<i64>, ParseError> {
    let mut inverse_bytes: Option<&[u8]> = None;
    let mut id_ctr_bytes: Option<&[u8]> = None;
    let mut id_actor_bytes: &[u8] = &[];
    for col in ops_meta.0.iter() {
        let spec = col.spec();
        if spec.id() == ID_COL_ID && spec.col_type() == ColumnType::Actor {
            id_actor_bytes = &ops_data[col.data()];
        }
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

    // Check row counts before expanding counter runs.
    let num_ops = hexane::Column::<ActorIdx>::load(id_actor_bytes)?.len();

    // New format: reconstruct doc-order counters from the inverse
    // permutation column.
    if let Some(inverse_bytes) = inverse_bytes {
        let inverse: Vec<i64> = decode_delta_int(inverse_bytes, num_ops)?;

        let mut change_meta: Vec<(usize, u64, u64, u64)> =
            BundleChangeIterUnverified::try_new(changes_meta, changes_data)?
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
        return decode_delta_int(id_ctr_bytes, num_ops);
    }

    // Empty bundle (no ops) — both columns absent.
    decode_delta_int(&[], num_ops)
}

fn decode_delta_int(bytes: &[u8], num_ops: usize) -> Result<Vec<i64>, ParseError> {
    // Streaming decoders require validated input.
    let column =
        hexane::DeltaColumn::<i64>::load_with(bytes, hexane::LoadOpts::new().with_length(num_ops))?;
    Ok(column.iter().collect())
}

impl<'a> BundleStorage<'a, Unverified> {
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, BundleStorage<'a, Unverified>, ParseError> {
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
            BundleChangeIterUnverified::try_new(&changes_meta, changes.value)
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            let id_ctr = extract_id_ctr_values(&changes_meta, changes.value, &ops_meta, ops.value)
                .map_err(parse::ParseError::Error)?;
            OpIterUnverified::try_new(&ops_meta, ops.value, &id_ctr)
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            return Ok((
                parse::Input::empty(),
                BundleStorage {
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

        BundleChangeIterUnverified::try_new(
            &changes_meta,
            &out[new_changes_start..new_changes_end],
        )
        .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
        let id_ctr = extract_id_ctr_values(
            &changes_meta,
            &out[new_changes_start..new_changes_end],
            &ops_meta,
            &out[new_ops_start..new_ops_end],
        )
        .map_err(parse::ParseError::Error)?;
        OpIterUnverified::try_new(&ops_meta, &out[new_ops_start..new_ops_end], &id_ctr)
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        Ok((
            parse::Input::empty(),
            BundleStorage {
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

    pub(crate) fn verify(self) -> Result<BundleStorage<'a, Verified>, ParseError> {
        for c in self.iter_change_meta() {
            let _ = c?;
        }
        for o in self.iter_ops() {
            let _ = o?;
        }
        Ok(BundleStorage {
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

    pub(crate) fn iter_ops(&self) -> OpIterUnverified<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIterUnverified::new(&self.ops_meta, bytes, &self.id_ctr)
    }

    fn iter_change_meta(&self) -> BundleChangeIterUnverified<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        BundleChangeIterUnverified::new(&self.changes_meta, change_data)
    }
}

impl BundleStorage<'_, Verified> {
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let change_meta = self.iter_change_meta().collect();
        let mut collector = ChangeCollector::from_bundle_changes(change_meta, &self.actors);
        for op in self.iter_ops() {
            collector.add(op);
        }
        let bundle = collector
            .unbundle(&self.actors, &self.deps)
            .map_err(|e| ParseError::Unbundle(Box::new(e)))?;
        Ok(bundle)
    }

    pub(crate) fn iter_ops(&self) -> OpIter<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIter::new(&self.ops_meta, bytes, &self.id_ctr)
    }

    pub(crate) fn iter_change_meta(&self) -> BundleChangeIter<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        BundleChangeIter::new_from_verified(&self.changes_meta, change_data)
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}

#[cfg(test)]
mod counter_column_tests {
    use super::{
        decode_delta_int, extract_id_ctr_values, ParseError, ID_COL_ID, ID_CTR_INVERSE_COL_ID,
    };
    use crate::storage::columns::{compression, RawColumn};
    use crate::storage::{ColumnSpec, RawColumns};

    fn counter_columns(
        actor_bytes: &[u8],
        counter_spec: ColumnSpec,
        counter_bytes: &[u8],
    ) -> (RawColumns<compression::Uncompressed>, Vec<u8>) {
        let mut data = actor_bytes.to_vec();
        data.extend_from_slice(counter_bytes);
        let columns = [
            RawColumn::new(ColumnSpec::new_actor(ID_COL_ID), 0..actor_bytes.len()),
            RawColumn::new(counter_spec, actor_bytes.len()..data.len()),
        ]
        .into_iter()
        .collect();
        (columns, data)
    }

    #[test]
    fn truncated_counter_columns_return_errors() {
        // One literal delta whose signed LEB128 value is truncated.
        assert!(decode_delta_int(&[0x7f, 0x80], 1).is_err());
    }

    #[test]
    fn overflowing_counter_columns_return_errors() {
        let overflowing = hexane::Column::<i64>::from_values(vec![i64::MAX, 1]).save();
        assert!(decode_delta_int(&overflowing, 2).is_err());
    }

    #[test]
    fn null_counter_columns_return_errors() {
        let nulls = hexane::DeltaColumn::<Option<i64>>::from_values(vec![None]).save();
        assert!(decode_delta_int(&nulls, 1).is_err());
    }

    #[test]
    fn valid_counter_columns_round_trip() {
        for values in [vec![], vec![1, 2, 2, 5, 3], vec![i64::MAX, i64::MAX - 1]] {
            let encoded = hexane::DeltaColumn::<i64>::from_values(values.clone()).save();
            assert_eq!(decode_delta_int(&encoded, values.len()).unwrap(), values);
        }
    }

    #[test]
    fn counter_columns_require_the_operation_count() {
        let encoded = hexane::DeltaColumn::<i64>::from_values(vec![1, 2]).save();
        for expected in [0, 1, 3] {
            assert!(matches!(
                decode_delta_int(&encoded, expected),
                Err(ParseError::Pack(hexane::PackError::InvalidLength(2, n))) if n == expected
            ));
        }
        assert!(matches!(
            decode_delta_int(&[], 1),
            Err(ParseError::Pack(hexane::PackError::InvalidLength(0, 1)))
        ));
    }

    #[test]
    fn both_counter_encodings_reject_amplified_runs() {
        // Repeat -19 for 114,440,652 rows.
        let amplified = [0xcc, 0xf3, 0xc8, 0x36, 0x6d];
        let actor = hexane::Column::<u32>::from_values(vec![0]).save();
        for id in [ID_COL_ID, ID_CTR_INVERSE_COL_ID] {
            for actors in [&[][..], actor.as_slice()] {
                let (columns, data) =
                    counter_columns(actors, ColumnSpec::new_delta(id), &amplified);
                assert!(matches!(
                    extract_id_ctr_values(&RawColumns(vec![]), &[], &columns, &data),
                    Err(ParseError::Pack(hexane::PackError::InvalidLength(
                        114_440_652,
                        _
                    )))
                ));
            }
        }
    }

    #[test]
    fn missing_counters_with_operation_ids_are_rejected() {
        let actor = hexane::Column::<u32>::from_values(vec![0]).save();
        let (columns, data) = counter_columns(&actor, ColumnSpec::new_delta(ID_COL_ID), &[]);
        assert!(matches!(
            extract_id_ctr_values(&RawColumns(vec![]), &[], &columns, &data),
            Err(ParseError::Pack(hexane::PackError::InvalidLength(0, 1)))
        ));
    }

    #[test]
    fn compressed_legacy_counters_with_matching_ids_round_trip() {
        let actor = hexane::Column::<u32>::from_values(vec![0; 256]).save();
        let values: Vec<i64> = (1..=256).collect();
        let counters = hexane::DeltaColumn::<i64>::from_values(values.clone()).save();
        let (columns, data) = counter_columns(&actor, ColumnSpec::new_delta(ID_COL_ID), &counters);
        assert_eq!(
            extract_id_ctr_values(&RawColumns(vec![]), &[], &columns, &data).unwrap(),
            values
        );
    }

    // Regenerate with python3 scripts/generate-malformed-fixtures.py.
    fn fixture_error(bytes: &[u8]) -> ParseError {
        let input = crate::storage::parse::Input::new(bytes);
        let (input, header) = super::Header::parse::<ParseError>(input).unwrap();
        super::BundleStorage::parse_following_header(input, header)
            .unwrap_err()
            .into()
    }

    #[test]
    fn amplified_bundles_return_load_errors() {
        for bytes in [
            include_bytes!("fixtures/timeout-counter-bundle.bin").as_slice(),
            include_bytes!("fixtures/slow-counter-bundle.bin").as_slice(),
        ] {
            assert!(matches!(
                fixture_error(bytes),
                ParseError::Pack(hexane::PackError::InvalidLength(114_440_652, 1))
            ));
            assert!(crate::AutoCommit::load(bytes).is_err());
            assert!(crate::Bundle::try_from(bytes).is_err());
        }
    }

    #[test]
    fn malformed_bundle_returns_a_load_error() {
        let bytes = include_bytes!("fixtures/truncated-counter-bundle.bin");
        assert!(matches!(fixture_error(bytes), ParseError::Pack(_)));
        assert!(crate::AutoCommit::load(bytes).is_err());
    }
}
