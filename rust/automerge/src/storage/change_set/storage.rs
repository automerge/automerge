use std::borrow::Cow;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::OnceLock;

use crate::op_set2::change::ChangeCollector;
use crate::op_set2::types::ActorIdx;
use crate::op_set2::OpSet;
use crate::storage::change::{OpReadState, Unverified, Verified};
use crate::storage::columns::compression;

use crate::storage::{parse, RawColumns};
use crate::types::{ActorId, ChangeHash};
use crate::Change;

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
    /// The member change metadata, decoded on demand and then shared.
    ///
    /// Decoding a member allocates a dep vector for it, so this is the
    /// slow way to read members and nothing on the apply path uses it:
    /// [`Self::member_ids`] gives the member index columnwise and the
    /// change graph reads the rest of the columns directly. What is left
    /// here is the row-shaped view — [`Self::to_changes`] and audit mode
    /// — which wants every field of every member anyway.
    pub(crate) changes: OnceLock<Vec<ChangeSetChange<'static>>>,
    /// The op columns, decoded once, in *change set* actor space and without
    /// indexes — everything [`OpSet::load_change_set_cols`] can establish
    /// without a receiving document.
    ///
    /// The apply path takes this out and finishes it against the document
    /// ([`OpSet::index_change_set`]), which is why `apply_change_set` consumes the
    /// change set: the columns are moved into the document's op set, not read
    /// from. Empty on the builder path (a change set being sent is never
    /// applied) and after an apply has taken it; either way the next
    /// reader re-loads from `bytes`.
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

    /// The member change metadata as raw columns.
    ///
    /// The apply path reads members this way — one pass per column,
    /// straight into the change graph's own columns — rather than through
    /// [`Self::changes`], which materialises a struct (and a dep `Vec`)
    /// per member.
    pub(crate) fn change_cols(&self) -> Result<ChangeSetChangeCols<'_>, ParseError> {
        ChangeSetChangeCols::try_new(&self.changes_meta, &self.bytes[self.changes_data.clone()])
    }

    /// Each member's actor and sequence number, read columnwise.
    ///
    /// This is the member index every applier needs first — to decide
    /// which members the document already has, and to name a member's
    /// node — so it is what a parse reads, in place of decoding every
    /// member's full metadata into a [`ChangeSetChange`]. Two things are
    /// checked, both of which a caller would otherwise have to guard on
    /// every use:
    ///
    /// * actor indexes name an actor the change set carries
    /// * sequence numbers are non-zero (a [`crate::ChangeId`] holds a
    ///   `NonZeroU64`, and seq 0 would break the per-actor chain check)
    ///
    /// The member count is the `actor` column's length; the `seq` column
    /// must match it. The rest of the member columns are validated where
    /// they are read — see
    /// [`ChangeGraph::add_change_set_members_cols`](crate::change_graph::ChangeGraph::add_change_set_members_cols)
    /// and [`Self::changes`] — so that a malformed column costs one decode
    /// rather than two.
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

    /// The member change metadata, decoding it on first call.
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

    /// The op columns in change set actor space, taken by value.
    ///
    /// The parse paths leave them here; this hands them over (the apply
    /// merges them into a document, consuming them). A change set whose cache
    /// is empty — built in-process, or applied once already — loads them
    /// again from `bytes`.
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

/// Decode every member's change metadata, detached from `data` so the
/// result can be cached next to the bytes it came from.
fn decode_change_meta(
    changes_meta: &RawColumns<compression::Uncompressed>,
    data: &[u8],
) -> Result<Vec<ChangeSetChange<'static>>, ParseError> {
    ChangeSetChangeIterUnverified::try_new(changes_meta, data)?
        .map(|c| c.map(ChangeSetChange::into_owned))
        .collect()
}

/// A pre-filled cache, for the parse paths to hand to the storage they
/// build.
fn primed<T>(value: T) -> OnceLock<T> {
    let cell = OnceLock::new();
    let _ = cell.set(value);
    cell
}

impl<'a> ChangeSetStorage<'a, Unverified> {
    /// Parse the column section of a change set chunk — everything after the
    /// fragment metadata prefix. There is no nested chunk header: these
    /// columns are part of chunk [`ChunkType::ChangeSet`], not a chunk of
    /// their own.
    pub(crate) fn parse_columns(
        input: parse::Input<'a>,
    ) -> parse::ParseResult<'a, ChangeSetStorage<'a, Unverified>, ParseError> {
        // positions tracked by the parser are absolute offsets within
        // this buffer
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
            // decoding the op columns is what validates them
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

        // decoding the op columns is what validates them
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

    /// Promote to [`Verified`], checking anything parsing has not already
    /// established.
    ///
    /// Nothing is left to check, and there is deliberately no row-by-row
    /// walk here. `parse_columns` decodes the change metadata
    /// (`Self::changes`) and the op columns (`Self::change_set_ops`, which runs
    /// `column_validation`) — between them the same facts a document
    /// establishes on its load path, reached the same way: by decoding the
    /// columns once rather than materialising every row into a `ChangeSetOp`
    /// and dropping it.
    ///
    /// Rows malformed in ways a column decode cannot see (a map op with a
    /// null key, say) are caught by [`Self::to_changes`], which is the
    /// path untrusted data takes — audit mode always reconstructs the
    /// member changes rather than trusting the columns.
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
    /// Rebuild the member [`Change`]s. The change set stores in-change set
    /// relationships in the succ column (and elides delete ops whose
    /// targets are all in-change set), so this inverts them back into pred
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

        let change_meta = self.changes()?.to_vec();
        let mut collector = ChangeCollector::from_change_set_changes(change_meta, &self.actors);

        // pass 1: row ids + the succ inversion (successor -> targets,
        // accumulated in doc order = ascending id within a group)
        let mut rows: HashSet<OpId> = HashSet::new();
        let mut inverted: HashMap<OpId, Vec<OpId>> = HashMap::new();
        for bop in self.iter_ops_checked() {
            let bop = bop?;
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

        let change_set = collector
            .decode_change_set(&self.actors, &self.deps)
            .map_err(|e| ParseError::DecodeChangeSet(Box::new(e)))?;
        Ok(change_set)
    }

    /// Rows with their decode errors intact.
    ///
    /// `Verified` means the *columns* parsed, not that every row is
    /// meaningful — a malformed row (a map op with a null key, say) only
    /// shows up when it is read. Readers that may see untrusted data use
    /// this and propagate; the fragment fast path takes rows on trust.
    pub(crate) fn iter_ops_checked(&self) -> OpIterUnverified<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIterUnverified::new(&self.ops_meta, bytes)
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}
