use std::borrow::Cow;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::OnceLock;

use crate::op_set2::change::ChangeCollector;
use crate::op_set2::op::Op;
use crate::op_set2::types::ActorIdx;
use crate::op_set2::types::KeyRef;
use crate::op_set2::OpSet;
use crate::storage::change::{OpReadState, Unverified, Verified};
use crate::storage::columns::compression;

use crate::storage::{parse, RawColumns};
use crate::types::{ActorId, ChangeHash, ElemId, ObjId, OpId};
use crate::Change;
use std::collections::{HashMap, HashSet};

use super::{
    ChangeSetChange, ChangeSetChangeCols, ChangeSetChangeIterUnverified, OpIterUnverified,
    ParseError,
};

#[derive(Clone, Debug)]
pub(crate) struct ChangeSetStorage<'a, OpReadState> {
    /// Uncompressed in-memory form. Iterators index into this.
    pub(crate) bytes: Cow<'a, [u8]>,
    /// On-disk form, if columns were DEFLATE-compressed. `None` for
    /// change sets that were written or received in fully-uncompressed form
    /// (in which case `bytes` is also the on-disk form).
    pub(crate) compressed_bytes: Option<Cow<'a, [u8]>>,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) actors: Vec<ActorId>,
    pub(crate) ops_meta: RawColumns<compression::Uncompressed>,
    pub(crate) ops_data: Range<usize>,
    pub(crate) changes_meta: RawColumns<compression::Uncompressed>,
    pub(crate) changes_data: Range<usize>,
    /// Decoded on demand; allocates per member, so the apply path reads
    /// [`Self::change_cols`] instead.
    pub(crate) changes: OnceLock<Vec<ChangeSetChange<'static>>>,
    /// The op columns in change set actor space, loaded at parse time.
    /// Empty once taken, or if built locally; reloaded from `bytes` then.
    pub(crate) change_set_ops: OnceLock<OpSet>,
    pub(crate) _phantom: PhantomData<OpReadState>,
}

impl<O: OpReadState> ChangeSetStorage<'_, O> {
    pub(crate) fn into_owned(self) -> ChangeSetStorage<'static, O> {
        ChangeSetStorage {
            bytes: Cow::Owned(self.bytes.into_owned()),
            compressed_bytes: self.compressed_bytes.map(|c| Cow::Owned(c.into_owned())),
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            changes: self.changes,
            change_set_ops: self.change_set_ops,
            _phantom: self._phantom,
        }
    }

    pub(crate) fn change_cols(&self) -> Result<ChangeSetChangeCols<'_>, ParseError> {
        ChangeSetChangeCols::try_new(&self.changes_meta, &self.bytes[self.changes_data.clone()])
    }

    /// Each member's actor and sequence number, checking that actors are
    /// in range and sequence numbers non-zero. No other column is validated.
    pub(crate) fn member_ids(&self) -> Result<(Vec<ActorIdx>, Vec<NonZeroU64>), ParseError> {
        let bad = ParseError::InvalidChangeMetadata;
        let cols = self.change_cols()?;
        let num_actors = self.actors.len();
        let actors: Vec<ActorIdx> = cols.actors().map(|a| a.unwrap_or_default()).collect();
        let len = actors.len();
        if actors.iter().any(|a| usize::from(*a) >= num_actors) {
            return Err(bad("bad member actor index"));
        }
        let mut seqs = Vec::with_capacity(len);
        for s in cols.seqs().take(len) {
            let s = s
                .filter(|s| *s > 0)
                .and_then(|s| NonZeroU64::new(s as u64))
                .ok_or(bad("change sequence number is zero"))?;
            seqs.push(s);
        }
        if seqs.len() != len {
            return Err(bad("short member seq column"));
        }
        Ok((actors, seqs))
    }

    pub(crate) fn changes(&self) -> Result<&[ChangeSetChange<'static>], ParseError> {
        if self.changes.get().is_none() {
            let decoded =
                decode_change_meta(&self.changes_meta, &self.bytes[self.changes_data.clone()])?;
            // a racing caller may have won; either vector is equivalent
            let _ = self.changes.set(decoded);
        }
        Ok(self
            .changes
            .get()
            .expect("change metadata was just decoded"))
    }

    /// Reloads from `bytes` if the op columns were already taken.
    pub(crate) fn take_change_set_ops(&mut self) -> Result<OpSet, ParseError> {
        if let Some(ops) = self.change_set_ops.take() {
            return Ok(ops);
        }
        self.load_change_set_ops()
    }

    fn load_change_set_ops(&self) -> Result<OpSet, ParseError> {
        OpSet::load_change_set_cols(
            &self.ops_meta,
            &self.bytes[self.ops_data.clone()],
            self.actors.len(),
        )
        .map_err(|e| ParseError::InvalidColumns(Box::new(e)))
    }
}

fn decode_change_meta(
    changes_meta: &RawColumns<compression::Uncompressed>,
    data: &[u8],
) -> Result<Vec<ChangeSetChange<'static>>, ParseError> {
    ChangeSetChangeIterUnverified::try_new(changes_meta, data)?
        .map(|c| c.map(ChangeSetChange::into_owned))
        .collect()
}

fn primed<T>(value: T) -> OnceLock<T> {
    let cell = OnceLock::new();
    let _ = cell.set(value);
    cell
}

impl<'a> ChangeSetStorage<'a, Unverified> {
    pub(crate) fn parse_columns(
        input: parse::Input<'a>,
    ) -> parse::ParseResult<'a, ChangeSetStorage<'a, Unverified>, ParseError> {
        let full_bytes = input.bytes();

        // Parse the leading deps + actors, capturing the byte range so we
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
            let change_set_ops = OpSet::load_change_set_cols(&ops_meta, ops.value, actors.len())
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            return Ok((
                parse::Input::empty(),
                ChangeSetStorage {
                    bytes: full_bytes.into(),
                    compressed_bytes: None,
                    deps,
                    actors,
                    ops_meta,
                    ops_data: ops_data_range,
                    changes_meta,
                    changes_data: changes_data_range,
                    changes: OnceLock::new(),
                    change_set_ops: primed(change_set_ops),
                    _phantom: PhantomData,
                },
            ));
        }

        // Slow path: at least one column is DEFLATE-encoded. Reconstruct a
        // fully-uncompressed buffer with the same section layout:
        //   deps | actors | change_meta' | change_data' | ops_meta' | ops_data'
        // where the primed sections use uncompressed column specs and
        // inflated data. We keep the compressed input around for
        // re-emission.
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

        let change_set_ops =
            OpSet::load_change_set_cols(&ops_meta, &out[new_ops_start..new_ops_end], actors.len())
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        Ok((
            parse::Input::empty(),
            ChangeSetStorage {
                bytes: Cow::Owned(out),
                compressed_bytes: Some(full_bytes.into()),
                deps,
                actors,
                ops_meta,
                ops_data: new_ops_start..new_ops_end,
                changes_meta,
                changes_data: new_changes_start..new_changes_end,
                changes: OnceLock::new(),
                change_set_ops: primed(change_set_ops),
                _phantom: PhantomData,
            },
        ))
    }

    /// `parse_columns` has already validated the columns. Rows malformed in
    /// ways a column decode cannot see (a map op with a null key, say) are
    /// only caught by [`Self::to_changes`].
    pub(crate) fn verify(self) -> Result<ChangeSetStorage<'a, Verified>, ParseError> {
        Ok(ChangeSetStorage {
            bytes: self.bytes,
            compressed_bytes: self.compressed_bytes,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            changes: self.changes,
            change_set_ops: self.change_set_ops,
            _phantom: PhantomData,
        })
    }
}

impl ChangeSetStorage<'_, Verified> {
    /// Rebuilds the member [`Change`]s, including the deletes that have no
    /// row of their own.
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let change_meta = self.changes()?.to_vec();
        let actors = crate::actor::ActorTable::from_sorted(self.actors.clone())?;
        let mut collector = ChangeCollector::from_change_set_changes(change_meta, &actors);

        let mut rows: HashSet<OpId> = HashSet::new();
        let mut inverted: HashMap<OpId, Vec<OpId>> = HashMap::new();
        for bop in self.iter_ops_checked() {
            let bop = bop?;
            rows.insert(bop.op.id);
            for s in &bop.succ {
                inverted.entry(*s).or_default().push(bop.op.id);
            }
        }

        // an elided delete's position within its change is fixed by its
        // counter, so it can be emitted whenever its key group ends
        let mut last: Option<(ObjId, KeyRef<'_>)> = None;
        let mut group_dels: Vec<OpId> = Vec::new();
        for bop in self.iter_ops_checked() {
            let bop = bop?;
            let key = if bop.op.insert {
                KeyRef::Seq(ElemId(bop.op.id))
            } else {
                bop.op.key.clone()
            };
            let next = Some((bop.op.obj, key));
            if last != next {
                if let Some((obj, key)) = last.take() {
                    add_elided_deletes(&mut collector, &mut inverted, &mut group_dels, obj, key);
                }
                last = next;
            }
            for s in &bop.succ {
                if !rows.contains(s) && !group_dels.contains(s) {
                    group_dels.push(*s);
                }
            }
            let mut op = bop.op;
            if let Some(member_preds) = inverted.remove(&op.id) {
                op.pred.extend(member_preds);
                op.pred.sort_unstable();
            }
            collector.add(op);
        }
        if let Some((obj, key)) = last.take() {
            add_elided_deletes(&mut collector, &mut inverted, &mut group_dels, obj, key);
        }

        let change_set = collector
            .decode_change_set(&actors, &self.deps)
            .map_err(|e| ParseError::DecodeChangeSet(Box::new(e)))?;
        Ok(change_set)
    }

    /// Yields an error for each malformed row (see [`Self::verify`]
    /// for which rows the column validation misses).
    pub(crate) fn iter_ops_checked(&self) -> OpIterUnverified<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIterUnverified::new(&self.ops_meta, bytes)
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}

fn add_elided_deletes<'a>(
    collector: &mut ChangeCollector<'a>,
    member_preds: &mut HashMap<OpId, Vec<OpId>>,
    dels: &mut Vec<OpId>,
    obj: ObjId,
    key: KeyRef<'a>,
) {
    for d in dels.drain(..) {
        let mut pred = member_preds.remove(&d).unwrap_or_default();
        pred.sort_unstable();
        collector.add(Op::del(d, obj, key.clone()).build(pred));
    }
}
