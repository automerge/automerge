use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::ChangeCollector;
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
    // permutation column. One entry per member op; nulls are ops with
    // no row (deletes elided into the succ column).
    if let Some(inverse_bytes) = inverse_bytes {
        let inverse: Vec<Option<i64>> = decode_delta_int_opt(inverse_bytes)?;
        let num_rows = inverse.iter().filter(|v| v.is_some()).count();

        let mut change_meta: Vec<(usize, u64, u64, u64)> =
            BundleChangeIterUnverified::try_new(changes_meta, changes_data)?
                .map(|c| c.map(|c| (c.actor, c.seq, c.start_op, c.max_op)))
                .collect::<Result<_, _>>()?;
        change_meta.sort_unstable_by_key(|(actor, seq, _, _)| (*actor, *seq));

        let mut counters = vec![0i64; num_rows];
        let mut k = 0usize;
        for (_actor, _seq, start_op, max_op) in &change_meta {
            for ctr in *start_op..=*max_op {
                if k >= inverse.len() {
                    return Err(ParseError::InverseLengthMismatch);
                }
                if let Some(doc_pos) = inverse[k] {
                    let doc_pos = doc_pos as usize;
                    if doc_pos >= counters.len() {
                        return Err(ParseError::InverseDecode);
                    }
                    counters[doc_pos] = ctr as i64;
                }
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

    // Empty bundle (no ops) — both columns absent.
    Ok(Vec::new())
}

fn decode_delta_int(bytes: &[u8]) -> Result<Vec<i64>, ParseError> {
    hexane::DeltaDecoder::<Option<i64>>::new(bytes)
        .map(|item| item.ok_or(ParseError::InverseDecode))
        .collect()
}

fn decode_delta_int_opt(bytes: &[u8]) -> Result<Vec<Option<i64>>, ParseError> {
    Ok(hexane::DeltaDecoder::<Option<i64>>::new(bytes).collect())
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
    /// Rebuild the member [`Change`]s. The bundle stores in-bundle
    /// relationships in the succ column (and elides delete ops whose
    /// targets are all in-bundle), so this inverts them back into pred
    /// lists: a succ entry `(target -> s)` becomes a pred `target` on
    /// op `s`, and a successor with no row of its own is an elided
    /// delete, resurrected with its group's obj/key. Preds are merged
    /// with the (external-only) pred column in ascending id order —
    /// the order the document visits a group's rows in.
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        use crate::op_set2::op::Op;
        use crate::op_set2::types::KeyRef;
        use crate::types::{ElemId, OpId};
        use std::collections::{HashMap, HashSet};

        let change_meta = self.iter_change_meta().collect();
        let mut collector = ChangeCollector::from_bundle_changes(change_meta, &self.actors);

        // pass 1: row ids + the succ inversion (successor -> targets,
        // accumulated in doc order = ascending id within a group)
        let mut rows: HashSet<OpId> = HashSet::new();
        let mut inverted: HashMap<OpId, Vec<OpId>> = HashMap::new();
        for bop in self.iter_ops() {
            rows.insert(bop.op.id);
            for s in &bop.succ {
                inverted.entry(*s).or_default().push(bop.op.id);
            }
        }

        // pass 2: feed the rows with merged preds; emit each group's
        // elided deletes when the group ends (their position within a
        // change is fixed by their op counter, so only the group's
        // obj/key needs to be current)
        let mut last: Option<(crate::types::ObjId, KeyRef<'_>)> = None;
        let mut group_dels: Vec<OpId> = Vec::new();
        for bop in self.iter_ops() {
            let key = if bop.op.insert {
                KeyRef::Seq(ElemId(bop.op.id))
            } else {
                bop.op.key.clone()
            };
            let next = Some((bop.op.obj, key));
            if last != next {
                if let Some((obj, key)) = last.take() {
                    for d in group_dels.drain(..) {
                        let mut pred = inverted.remove(&d).unwrap_or_default();
                        pred.sort_unstable();
                        collector.add(Op::del(d, obj, key.clone()).build(pred));
                    }
                }
                last = next;
            }
            for s in &bop.succ {
                if !rows.contains(s) && !group_dels.contains(s) {
                    group_dels.push(*s);
                }
            }
            let mut op = bop.op;
            if let Some(internal) = inverted.remove(&op.id) {
                op.pred.extend(internal);
                op.pred.sort_unstable();
            }
            collector.add(op);
        }
        if let Some((obj, key)) = last.take() {
            for d in group_dels.drain(..) {
                let mut pred = inverted.remove(&d).unwrap_or_default();
                pred.sort_unstable();
                collector.add(Op::del(d, obj, key.clone()).build(pred));
            }
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
