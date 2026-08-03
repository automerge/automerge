use crate::change_graph::ChangeGraph;
use crate::op_set2::change::{length_prefixed_bytes, ActorMapper, BuildChangeMetadata};
use crate::op_set2::types::ActorIdx;
use crate::op_set2::OpSet;
use crate::storage::change::Verified;
use crate::storage::{parse, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};
use crate::{AutomergeError, Change, ChangeId};

use std::borrow::Cow;
use std::num::NonZeroU64;

mod builder;
mod error;
mod meta;
mod storage;

pub(crate) use builder::ops;
pub(crate) use builder::{
    ChangeSetBuilder, ChangeSetChangeCols, ChangeSetChangeIterUnverified, ChangeSetOpWriter,
    ManifoldOp, ManifoldOps, OpIterUnverified,
};
pub(crate) use error::ParseError;
pub(crate) use meta::{ChangeSetMetadata, DepRef};
pub(crate) use storage::ChangeSetStorage;

/// EXPERIMENTAL: A set of changes in compressed columnar form, plus the
/// fragment metadata a fragments-mode document needs to apply them.
///
/// The carried changes name their external dependencies by hash alone,
/// which a document in the fragment-hashes state cannot resolve to nodes
/// (only fragment heads have known hashes there). So a change set chunk
/// prefixes the changes with:
///
/// * the **head** hashes it delivers, each paired with its member
///   index — one for a fragment, one per document head for a change set
///   standing in for a whole document
/// * the **checkpoint** hashes (interior fragment-level hashes), each
///   paired with its member index
/// * the fragment's **boundary** hashes, each paired with its
///   [`ChangeId`]
/// * for every external dep of the carried changes (in the same
///   order), its [`ChangeId`] — together with the dep hash list this
///   gives the full `(change id, hash)` pairing, so deps resolve
///   structurally with no hash lookup
///
/// The wire layout after the chunk header is:
///
/// ```text
/// actors      uleb count, then length-prefixed actor ids (only the
///             actors the prefix itself references)
/// heads       uleb count, then per entry: 32-byte hash + uleb member index
/// checkpoints uleb count, then per entry: uleb member index + 32-byte hash
/// boundary    uleb count, then per entry: 32-byte hash + uleb actor + uleb seq
/// deps        uleb count, then per entry: uleb actor + uleb seq
/// changes     a complete legacy change set chunk, header and all — the
///             carried changes in columnar form
/// ```
///
/// Member indexes refer to the carried changes' list, which is in
/// topological order.
///
/// The inner chunk is still written and read in the legacy
/// ([`ChunkType::BundleV0`]) encoding so that a reader which only knows
/// that format can find the changes; nothing but the prefix distinguishes
/// the two.
///
/// This is experimental, the format may still change — do not use it
/// in systems where you expect data to stick around.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// The changes this change set delivers: those no other member depends
    /// on, each paired with its member index. A change set built from a
    /// [`Fragment`](crate::Fragment) has exactly one; a change set standing
    /// in for a whole document has one per document head.
    pub(crate) heads: Vec<(ChangeHash, usize)>,
    /// `(member index, hash)`
    pub(crate) checkpoints: Vec<(usize, ChangeHash)>,
    /// each boundary hash paired with the change it names
    pub(crate) boundary: Vec<(ChangeHash, ChangeId)>,
    /// the change id of each external dep, aligned with [`Self::deps`]
    pub(crate) dep_ids: Vec<ChangeId>,
    /// Each member change's actor, validated in range at parse time and
    /// aligned with [`Self::iter_changes`]. Held alongside
    /// [`Self::member_seqs`] so the apply path can identify members —
    /// scan for the ones already present, resolve heads and checkpoints —
    /// without decoding every member's metadata.
    pub(crate) member_actors: Vec<ActorIdx>,
    /// Each member change's sequence number, validated non-zero at parse
    /// time and aligned with [`Self::iter_changes`].
    pub(crate) member_seqs: Vec<NonZeroU64>,
    /// The carried changes, in the columnar change set encoding.
    pub(crate) storage: ChangeSetStorage<'static, Verified>,
}

impl ChangeSet {
    pub(crate) fn new(
        heads: Vec<(ChangeHash, usize)>,
        checkpoints: Vec<(usize, ChangeHash)>,
        boundary: Vec<(ChangeHash, ChangeId)>,
        dep_ids: Vec<ChangeId>,
        member_actors: Vec<ActorIdx>,
        member_seqs: Vec<NonZeroU64>,
        storage: ChangeSetStorage<'static, Verified>,
    ) -> Self {
        Self {
            heads,
            checkpoints,
            boundary,
            dep_ids,
            member_actors,
            member_seqs,
            storage,
        }
    }

    /// The member change columns, for the columnar apply path.
    pub(crate) fn change_cols(&self) -> ChangeSetChangeCols<'_> {
        self.storage
            .change_cols()
            .expect("a parsed change_set's change columns are well formed")
    }

    /// The changes this change set delivers — the members nothing else in
    /// the change set depends on.
    pub fn heads(&self) -> impl ExactSizeIterator<Item = ChangeHash> + '_ {
        self.heads.iter().map(|(h, _)| *h)
    }

    /// The changes carried by the change set, in topological order.
    pub fn to_changes(&self) -> Result<Vec<Change>, AutomergeError> {
        self.storage
            .to_changes()
            .map_err(|e| AutomergeError::DecodeChangeSet(Box::new(e)))
    }

    /// Metadata for each carried change, in topological order, without
    /// decoding its ops.
    ///
    /// Fallible because it is decoded on demand: a parse validates the
    /// member index (actors and sequence numbers) but leaves the rest of
    /// the member columns for whoever reads them, so that applying a
    /// change set — which reads those columns directly into the change graph
    /// — never pays for a decode it does not use.
    pub fn iter_changes(
        &self,
    ) -> Result<std::slice::Iter<'_, ChangeSetChange<'static>>, AutomergeError> {
        Ok(self.changes()?.iter())
    }

    /// The op columns the parse loaded, taken by value — the apply merges
    /// them into a document, so they are moved rather than read.
    pub(crate) fn take_change_set_ops(&mut self) -> Result<OpSet, AutomergeError> {
        self.storage
            .take_change_set_ops()
            .map_err(|e| AutomergeError::DecodeChangeSet(Box::new(e)))
    }

    /// The member change metadata, decoded on first call (see
    /// [`Self::iter_changes`]).
    pub(crate) fn changes(&self) -> Result<&[ChangeSetChange<'static>], AutomergeError> {
        self.storage
            .changes()
            .map_err(|e| AutomergeError::DecodeChangeSet(Box::new(e)))
    }

    /// The hashes this change set depends on but does not carry.
    pub fn deps(&self) -> &[ChangeHash] {
        self.storage.deps()
    }

    pub(crate) fn actors(&self) -> &[ActorId] {
        &self.storage.actors
    }

    /// Build the carried-change storage from a set of change hashes.
    /// No hint column: hints need a clock, which only the node-keyed
    /// path has.
    pub(crate) fn storage_for_hashes<I>(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        hashes: I,
    ) -> Result<ChangeSetStorage<'static, Verified>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph
            .get_change_set_metadata(hashes)
            .collect::<Result<_, _>>()?;
        Ok(Self::storage_from_meta(op_set, changes))
    }

    /// Build the carried-change storage from member nodes. Only the
    /// boundary (external dep) hashes need to be known, so this works in
    /// the fragment-hashes state. `nodes` must be sorted ascending.
    pub(crate) fn storage_for_nodes(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        nodes: Vec<crate::change_graph::NodeIdx>,
    ) -> Result<ChangeSetStorage<'static, Verified>, AutomergeError> {
        Ok(
            Self::storage_for_node_sets(op_set, change_graph, vec![nodes])?
                .pop()
                .expect("one set in, one out"),
        )
    }

    /// [`Self::storage_for_nodes`] for several change sets at once.
    ///
    /// Each change set's own ops come from a counter-ranged walk, which is
    /// already sparse — a one-change fragment reads a handful of rows.
    /// The hint ranks are the part that does not scale: a rank is a
    /// position among the rows the fragment's clock covers, so finding
    /// one means walking the id column from the start of the document.
    /// Done per change set that is O(change sets x document); done here it is a
    /// single walk with every change set's counters advanced together, and
    /// it stops as soon as the last outstanding target is found.
    pub(crate) fn storage_for_node_sets(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        node_sets: Vec<Vec<crate::change_graph::NodeIdx>>,
    ) -> Result<Vec<ChangeSetStorage<'static, Verified>>, AutomergeError> {
        let mut jobs = Vec::with_capacity(node_sets.len());
        for nodes in node_sets {
            let clock = change_graph.clock_for_nodes(nodes.clone());
            let changes = change_graph
                .change_set_metadata_for_nodes(nodes)
                .collect::<Result<Vec<_>, _>>()?;
            let collector = Self::collect_ops(op_set, changes);
            let needed = collector.hint_targets_by_obj();
            jobs.push(HintJob {
                needed,
                collector,
                clock,
                ranks: std::collections::HashMap::new(),
            });
        }

        Self::resolve_hint_ranks(op_set, &mut jobs);
        Ok(jobs
            .into_iter()
            .map(|j| j.collector.finish_with_ranks(&j.ranks))
            .collect())
    }

    /// One walk of the id column answering every job's outstanding hint
    /// targets. Jobs drop out of `active` as they are satisfied, and the
    /// walk ends with the last of them.
    /// Hint ranks, one object at a time: each object is visited only if
    /// some job targets it, and left at that job's last target.
    fn resolve_hint_ranks(op_set: &OpSet, jobs: &mut [HintJob<'_>]) {
        // which jobs target which object. `BTreeMap` for key order, which
        // for `ObjId` is document order — so one forward-only cursor
        // covers every object
        let mut objs: std::collections::BTreeMap<crate::types::ObjId, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (i, job) in jobs.iter().enumerate() {
            for obj in job.needed.keys() {
                objs.entry(*obj).or_default().push(i);
            }
        }

        let mut obj_id_iter = op_set.obj_id_iter();

        for (obj, interested) in objs {
            let scope = obj_id_iter.seek_to_value(obj);
            // hoisted out of the row loop: one hash lookup per object,
            // not per row
            let targets: Vec<rustc_hash::FxHashSet<crate::types::OpId>> = interested
                .iter()
                .map(|&i| jobs[i].needed.remove(&obj).unwrap_or_default())
                .collect();
            // rank restarts at each object: hints are object-relative
            let mut rank: Vec<u64> = vec![0; interested.len()];
            let mut remaining: Vec<usize> = targets.iter().map(|t| t.len()).collect();
            let mut active: Vec<usize> = (0..interested.len())
                .filter(|&k| remaining[k] > 0)
                .collect();

            for id in op_set.id_iter_range(&scope) {
                let mut k = 0;
                while k < active.len() {
                    let slot = active[k];
                    let job = &mut jobs[interested[slot]];
                    let wanted = targets[slot].contains(&id);
                    let covered = job.clock.covers(&id);
                    // a row that is neither covered nor a target cannot
                    // affect this job at all
                    if !covered && !wanted {
                        k += 1;
                        continue;
                    }
                    // members do not count towards the rank — a receiver
                    // about to apply the change set does not have them yet
                    if job.collector.is_member(id) {
                        k += 1;
                        continue;
                    }
                    if wanted {
                        job.ranks.insert(id, rank[slot]);
                        remaining[slot] -= 1;
                    }
                    if covered {
                        rank[slot] += 1;
                    }
                    if remaining[slot] == 0 {
                        active.swap_remove(k);
                        continue;
                    }
                    k += 1;
                }
                if active.is_empty() {
                    break;
                }
            }
            debug_assert!(active.is_empty(), "hint target missing from its object",);
        }
    }

    /// Read the change set's own ops out of the op set.
    ///
    /// The walk is counter-ranged, so it costs the rows the change set
    /// carries (plus whatever the skip iterator steps over between
    /// them) rather than the document.
    fn collect_ops<'a>(
        op_set: &'a OpSet,
        changes: Vec<ChangeSetMetadata<'a>>,
    ) -> ChangeSetBuilder<'a> {
        let min = changes
            .iter()
            .map(|c| c.start_op as usize)
            .min()
            .unwrap_or(0);
        let max = changes.iter().map(|c| c.max_op as usize).max().unwrap_or(0) + 1;

        let mapper = ActorMapper::new(&op_set.actors);
        let mut collector = ChangeSetBuilder::from_change_meta(changes, mapper);

        for op in op_set.iter_ctr_range(min..max) {
            let op_id = op.id;
            let op_succ: Vec<_> = op.succ().collect();
            collector.process_op(op, &op_succ);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }
        collector
    }

    /// The hash-keyed path, which has no clock and so writes no hints.
    fn storage_from_meta(
        op_set: &OpSet,
        changes: Vec<ChangeSetMetadata<'_>>,
    ) -> ChangeSetStorage<'static, Verified> {
        Self::collect_ops(op_set, changes).finish_with_ranks(&std::collections::HashMap::new())
    }

    /// The inner chunk: the carried changes on their own, with per-column
    /// DEFLATE applied where each column is large enough to benefit.
    /// Falls back to the uncompressed buffer for storage built before the
    /// per-column compression pass (or parsed from input with no
    /// compressed columns).
    /// The column section in its on-disk form.
    fn column_bytes(&self) -> &[u8] {
        match &self.storage.compressed_bytes {
            Some(c) => c,
            None => &self.storage.bytes,
        }
    }

    /// The chunk's bytes: the metadata prefix followed by the column
    /// section.
    pub fn bytes(&self) -> Vec<u8> {
        self.bytes_with(true)
    }

    /// [`Self::bytes`] with the column section left uncompressed. The
    /// builder assembles both forms, so this only chooses between them.
    pub fn bytes_uncompressed(&self) -> Vec<u8> {
        self.bytes_with(false)
    }

    fn bytes_with(&self, deflate: bool) -> Vec<u8> {
        // dedup the actors the prefix references
        fn actor_idx<'x>(actors: &mut Vec<&'x ActorId>, a: &'x ActorId) -> u64 {
            match actors.iter().position(|x| *x == a) {
                Some(i) => i as u64,
                None => {
                    actors.push(a);
                    (actors.len() - 1) as u64
                }
            }
        }
        let mut actors: Vec<&ActorId> = Vec::new();
        let boundary: Vec<(ChangeHash, u64, u64)> = self
            .boundary
            .iter()
            .map(|(h, id)| (*h, actor_idx(&mut actors, id.actor()), id.seq()))
            .collect();
        let deps: Vec<(u64, u64)> = self
            .dep_ids
            .iter()
            .map(|id| (actor_idx(&mut actors, id.actor()), id.seq()))
            .collect();

        let mut data = Vec::new();
        leb128::write::unsigned(&mut data, actors.len() as u64).unwrap();
        for a in &actors {
            length_prefixed_bytes(a.to_bytes(), &mut data);
        }
        leb128::write::unsigned(&mut data, self.heads.len() as u64).unwrap();
        for (h, i) in &self.heads {
            data.extend_from_slice(h.as_bytes());
            leb128::write::unsigned(&mut data, *i as u64).unwrap();
        }
        leb128::write::unsigned(&mut data, self.checkpoints.len() as u64).unwrap();
        for (i, h) in &self.checkpoints {
            leb128::write::unsigned(&mut data, *i as u64).unwrap();
            data.extend_from_slice(h.as_bytes());
        }
        leb128::write::unsigned(&mut data, boundary.len() as u64).unwrap();
        for (h, a, s) in &boundary {
            data.extend_from_slice(h.as_bytes());
            leb128::write::unsigned(&mut data, *a).unwrap();
            leb128::write::unsigned(&mut data, *s).unwrap();
        }
        leb128::write::unsigned(&mut data, deps.len() as u64).unwrap();
        for (a, s) in &deps {
            leb128::write::unsigned(&mut data, *a).unwrap();
            leb128::write::unsigned(&mut data, *s).unwrap();
        }
        data.extend_from_slice(if deflate {
            self.column_bytes()
        } else {
            &self.storage.bytes
        });

        let header = Header::new(ChunkType::ChangeSet, &data);
        let mut out = Vec::with_capacity(header.len() + data.len());
        header.write(&mut out);
        out.extend(data);
        out
    }

    /// Parse the metadata prefix, returning the remaining input (the
    /// carried changes' chunk).
    ///
    /// Every count is a wire-supplied length prefix, so none of them may
    /// be trusted to size an allocation: a 5-byte varint can claim more
    /// entries than the machine has memory. Each is capped by what the
    /// remaining input could possibly hold ([`entry_capacity`]) before it
    /// reserves anything — a claim beyond that just falls out as the
    /// incomplete-input error the entry parse would have produced anyway.
    pub(crate) fn parse_prefix(
        i: parse::Input<'_>,
    ) -> parse::ParseResult<'_, ParsedPrefix, parse::leb128::Error> {
        let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;

        // a head is a 32-byte hash plus a uleb member index
        let (i, n_heads) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut heads = Vec::with_capacity(entry_capacity(&i, n_heads, 33));
        for _ in 0..n_heads {
            let (j, h) = parse::change_hash(i)?;
            let (j, idx) = parse::leb128_u64(j)?;
            heads.push((h, idx as usize));
            i = j;
        }

        // a checkpoint is a uleb index plus a 32-byte hash
        let (mut i, n_checkpoints) = parse::leb128_u64(i)?;
        let mut checkpoints = Vec::with_capacity(entry_capacity(&i, n_checkpoints, 33));
        for _ in 0..n_checkpoints {
            let (j, idx) = parse::leb128_u64(i)?;
            let (j, h) = parse::change_hash(j)?;
            checkpoints.push((idx as usize, h));
            i = j;
        }

        // a boundary entry is a 32-byte hash plus two ulebs
        let (i, n_boundary) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut boundary = Vec::with_capacity(entry_capacity(&i, n_boundary, 34));
        for _ in 0..n_boundary {
            let (j, h) = parse::change_hash(i)?;
            let (j, a) = parse::leb128_u64(j)?;
            let (j, s) = parse::leb128_u64(j)?;
            boundary.push((h, a, s));
            i = j;
        }

        // a dep id is two ulebs
        let (i, n_deps) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut deps = Vec::with_capacity(entry_capacity(&i, n_deps, 2));
        for _ in 0..n_deps {
            let (j, a) = parse::leb128_u64(i)?;
            let (j, s) = parse::leb128_u64(j)?;
            deps.push((a, s));
            i = j;
        }

        Ok((
            i,
            ParsedPrefix {
                actors,
                heads,
                checkpoints,
                boundary,
                deps,
            },
        ))
    }
}

/// One change set's state during the shared hint walk: its collected ops,
/// the targets it still needs a rank for, and the running count of
/// clock-covered non-member rows seen so far.
struct HintJob<'a> {
    collector: ChangeSetBuilder<'a>,
    clock: crate::clock::Clock,
    /// targets grouped by the object they live in — the walk visits one
    /// object at a time and only consults the jobs targeting it
    needed:
        std::collections::HashMap<crate::types::ObjId, rustc_hash::FxHashSet<crate::types::OpId>>,
    ranks: std::collections::HashMap<crate::types::OpId, u64>,
}

/// How many entries it is safe to reserve for a wire-supplied `count`:
/// the claim, clamped to what the unconsumed input could hold at
/// `min_entry_bytes` each. Reserving beyond that could never be filled,
/// so clamping loses nothing and keeps a hostile count from turning into
/// an allocation the size it asked for.
fn entry_capacity(i: &parse::Input<'_>, count: u64, min_entry_bytes: usize) -> usize {
    let ceiling = i.unconsumed_bytes().len() / min_entry_bytes;
    usize::try_from(count).unwrap_or(usize::MAX).min(ceiling)
}

/// The decoded metadata prefix of a [`ChangeSet`] chunk, with actors
/// still in index form.
#[derive(Debug)]
pub(crate) struct ParsedPrefix {
    actors: Vec<ActorId>,
    heads: Vec<(ChangeHash, usize)>,
    checkpoints: Vec<(usize, ChangeHash)>,
    boundary: Vec<(ChangeHash, u64, u64)>,
    deps: Vec<(u64, u64)>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid change_set: {0}")]
pub struct InvalidChangeSet(pub(crate) String);

/// Parse the inner (carried-changes) chunk of a change set.
fn parse_change_set_columns(bytes: &[u8]) -> Result<ChangeSetStorage<'static, Verified>, String> {
    let input = parse::Input::new(bytes);
    let (_i, stored) = ChangeSetStorage::parse_columns(input)
        .map_err(|e| format!("invalid carried changes: {}", e))?;
    let verified = stored
        .verify()
        .map_err(|e| format!("unable to verify ops: {}", e))?;
    Ok(verified.into_owned())
}

/// Metadata for one change carried by a change set.
#[derive(Clone, Debug)]
pub struct ChangeSetChange<'a> {
    pub actor: usize,
    pub author: Option<usize>,
    pub seq: u64,
    pub start_op: u64,
    pub max_op: u64,
    pub timestamp: i64,
    pub message: Option<Cow<'a, str>>,
    pub deps: Vec<u64>,
    pub extra: Cow<'a, [u8]>,
}

impl ChangeSetChange<'_> {
    /// Detach from the buffer the metadata was decoded out of, so a
    /// parsed change set can cache its change metadata alongside the bytes
    /// it came from. Only `message` and `extra` borrow, and both are
    /// usually absent.
    pub(crate) fn into_owned(self) -> ChangeSetChange<'static> {
        ChangeSetChange {
            actor: self.actor,
            author: self.author,
            seq: self.seq,
            start_op: self.start_op,
            max_op: self.max_op,
            timestamp: self.timestamp,
            message: self.message.map(|m| Cow::Owned(m.into_owned())),
            deps: self.deps,
            extra: Cow::Owned(self.extra.into_owned()),
        }
    }
}

impl<'a> From<ChangeSetChange<'a>> for BuildChangeMetadata<'a> {
    fn from(change_set: ChangeSetChange<'a>) -> Self {
        BuildChangeMetadata {
            actor: change_set.actor,
            seq: change_set.seq,
            start_op: change_set.start_op,
            max_op: change_set.max_op,
            timestamp: change_set.timestamp,
            message: change_set.message,
            deps: change_set.deps,
            extra: change_set.extra,
            builder: 0,
        }
    }
}

impl TryFrom<&[u8]> for ChangeSet {
    type Error = InvalidChangeSet;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let input = parse::Input::new(bytes);
        let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
            .map_err(|e| InvalidChangeSet(format!("invalid header: {}", e)))?;
        if header.chunk_type() != ChunkType::ChangeSet {
            return Err(InvalidChangeSet("not a change_set chunk".to_string()));
        }
        Self::parse_after_header(i)
    }
}

impl ChangeSet {
    /// The whole change set, from the input the chunk parser leaves after the
    /// header.
    ///
    /// The chunk parser calls this rather than validating the columns and
    /// throwing the result away: parsing a change set's columns is most of
    /// what loading one costs, and doing it twice doubled the load.
    pub(crate) fn parse_after_header(i: parse::Input<'_>) -> Result<Self, InvalidChangeSet> {
        let bad = |s: &str| InvalidChangeSet(s.to_string());
        let (i, prefix) = Self::parse_prefix(i)
            .map_err(|e| InvalidChangeSet(format!("invalid prefix: {}", e)))?;

        let resolve = |a: u64| -> Result<ActorId, InvalidChangeSet> {
            prefix
                .actors
                .get(a as usize)
                .cloned()
                .ok_or_else(|| bad("bad actor index"))
        };
        // change sequence numbers are 1-based everywhere; rejecting zero
        // here is what lets `ChangeId` take a `NonZeroU64` and stay total
        let seq = |s: u64| NonZeroU64::new(s).ok_or_else(|| bad("change sequence number is zero"));
        let change_id = |a: u64, s: u64| Ok(ChangeId::new(seq(s)?, resolve(a)?, 0));
        let boundary = prefix
            .boundary
            .iter()
            .map(|(h, a, s)| Ok((*h, change_id(*a, *s)?)))
            .collect::<Result<Vec<_>, InvalidChangeSet>>()?;
        let dep_ids = prefix
            .deps
            .iter()
            .map(|(a, s)| change_id(*a, *s))
            .collect::<Result<Vec<_>, InvalidChangeSet>>()?;

        let storage = parse_change_set_columns(i.unconsumed_bytes())
            .map_err(|e| InvalidChangeSet(format!("invalid carried changes: {}", e)))?;

        // all shape errors are caught here, at parse time — a Change set
        // that exists is well formed and appliers need no bounds checks
        if dep_ids.len() != storage.deps().len() {
            return Err(bad("dep ids do not match the carried changes' deps"));
        }
        // the members are validated columnwise — actor indexes in range,
        // sequence numbers non-zero, `start_op <= max_op` — which is all
        // the appliers need taken on trust. Their full metadata is only
        // decoded if someone asks for it (`to_changes`, audit mode).
        let (member_actors, member_seqs) = storage
            .member_ids()
            .map_err(|e| InvalidChangeSet(format!("invalid change metadata: {}", e)))?;
        let num_members = member_seqs.len();
        if prefix.heads.is_empty() {
            return Err(bad("a change_set must deliver at least one head"));
        }
        if prefix.heads.iter().any(|(_, i)| *i >= num_members) {
            return Err(bad("head index out of range"));
        }
        if prefix.checkpoints.iter().any(|(i, _)| *i >= num_members) {
            return Err(bad("checkpoint index out of range"));
        }
        // soundness only: completeness would need every member's hash,
        // which a parse does not compute
        if prefix
            .checkpoints
            .iter()
            .any(|(_, h)| h.fragment_level() == 0)
        {
            return Err(bad("checkpoint is not a fragment head"));
        }
        if prefix
            .checkpoints
            .iter()
            .any(|(_, c)| prefix.heads.iter().any(|(h, _)| h == c))
        {
            return Err(bad("checkpoint duplicates a delivered head"));
        }

        Ok(ChangeSet {
            heads: prefix.heads,
            checkpoints: prefix.checkpoints,
            boundary,
            dep_ids,
            member_actors,
            member_seqs,
            storage,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::MAGIC_BYTES;

    /// Every change set carries its fragment-level members as
    /// checkpoints, without which it cannot serve as a Fragment.
    #[test]
    fn change_sets_carry_their_fragment_level_members_as_checkpoints() {
        use crate::transaction::{CommitOptions, Transactable};
        use crate::{ActorId, AutoCommit, ROOT};

        let mut doc = AutoCommit::new().with_actor(ActorId::from(&b"aaaa"[..]));
        for i in 0..2000 {
            doc.put(ROOT, "k", i as i64).unwrap();
            doc.commit_with(CommitOptions::default().with_time(0));
        }
        let _ = doc.save();
        for i in 0..1000 {
            doc.put(ROOT, "k", 100_000 + i as i64).unwrap();
            doc.commit_with(CommitOptions::default().with_time(0));
        }

        let bytes = doc.save_incremental();
        let cs = ChangeSet::try_from(bytes.as_slice()).unwrap();

        let heads: Vec<ChangeHash> = cs.heads().collect();
        let mut expected: Vec<ChangeHash> = cs
            .to_changes()
            .unwrap()
            .iter()
            .map(|c| c.hash())
            .filter(|h| h.fragment_level() > 0 && !heads.contains(h))
            .collect();
        let mut got: Vec<ChangeHash> = cs.checkpoints.iter().map(|(_, h)| *h).collect();
        expected.sort_unstable();
        got.sort_unstable();
        assert_eq!(
            got, expected,
            "a change set must carry exactly its non-head fragment-level members",
        );
        // and every checkpoint must index a real member
        assert!(cs
            .checkpoints
            .iter()
            .all(|(i, _)| *i < cs.member_seqs.len()));
    }

    /// A change set's bytes must be a function of its content, and batched
    /// bundling must agree with bundling one fragment at a time.
    ///
    /// Guards the fix for `flush_deletes` emitting a key group's
    /// concurrent deletes in `HashMap` order, which is seeded per
    /// instance — the same fragment change setd twice in one process gave
    /// different bytes. NOTE: this fixture does not reproduce that
    /// (it needs several concurrent deletes pending in one group at
    /// once, which turned out to be rare — 1 fragment in 191 on the
    /// A1 corpus document). It is kept as a property guard; the
    /// reproduction lives in the corpus.
    #[test]
    fn bundling_is_deterministic() {
        use crate::transaction::Transactable;
        use crate::{AutoCommit, ObjType, ROOT};

        // The pending-delete map is keyed by op id within one (obj, key)
        // group, so it holds more than one entry only when several
        // *concurrent* deletes land on the same element — that is the
        // shape whose emission order was unstable. Concurrent text
        // editing produces it naturally.
        let mut doc = AutoCommit::new().with_actor(crate::ActorId::from(&[0u8][..]));
        doc.enable_audit_mode().unwrap();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, &"abcdefghij".repeat(8))
            .unwrap();
        doc.commit();

        for round in 0..6u8 {
            for a in 1..=6u8 {
                let mut fork = doc
                    .fork()
                    .with_actor(crate::ActorId::from(&[round * 16 + a][..]));
                fork.enable_audit_mode().unwrap();
                // every fork deletes the same span, concurrently
                let _ = fork.splice_text(&text, 0, 4, "");
                let _ = fork.splice_text(&text, 2, 3, "X");
                fork.commit();
                doc.merge(&mut fork).unwrap();
            }
        }
        doc.put(ROOT, "done", true).unwrap();
        doc.commit();

        let fragments = doc.document().fragments(..);
        assert!(!fragments.is_empty());
        for f in &fragments {
            let first = doc.document().change_set_for_fragment(f).unwrap().bytes();
            for _ in 0..4 {
                assert_eq!(
                    doc.document().change_set_for_fragment(f).unwrap().bytes(),
                    first,
                    "change_set bytes vary between identical calls"
                );
            }
        }

        // ...and bundling the whole set in one batch must agree with
        // bundling each fragment on its own
        let batched = doc
            .document()
            .change_sets_for_fragments(fragments.clone())
            .unwrap();
        let singly: Vec<Vec<u8>> = fragments
            .iter()
            .map(|f| doc.document().change_set_for_fragment(f).unwrap().bytes())
            .collect();
        assert_eq!(batched, singly, "batched bundling diverges");
    }

    fn leb(mut n: u64, out: &mut Vec<u8>) {
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            out.push(b);
            if n == 0 {
                break;
            }
        }
    }

    /// A chunk header around `data`. The checksum is never verified by
    /// `ChangeSet::try_from`, so it is left blank — the point of these
    /// tests is what the parser does with hostile *content*.
    fn chunk(data: Vec<u8>) -> Vec<u8> {
        let mut out = MAGIC_BYTES.to_vec();
        out.extend([0u8; 4]);
        out.push(u8::from(ChunkType::ChangeSet));
        leb(data.len() as u64, &mut out);
        out.extend(data);
        out
    }

    /// A prefix whose counts are wire-supplied lies. Each of the four
    /// counted sections (heads, checkpoints, boundary, deps) claims more
    /// entries than exist; none may turn into an allocation of the size
    /// it asked for.
    #[test]
    fn absurd_counts_error_instead_of_allocating() {
        for section in 0..4 {
            let mut data = Vec::new();
            leb(0, &mut data); // no actors
            for s in 0..4 {
                // the section under test claims u64::MAX/64 entries; the
                // ones before it are empty so parsing reaches it
                leb(if s == section { u64::MAX / 64 } else { 0 }, &mut data);
                if s == section {
                    break;
                }
            }
            let err = ChangeSet::try_from(&chunk(data)[..]);
            assert!(err.is_err(), "section {section} should not parse");
        }
    }

    /// A change set has to deliver something.
    #[test]
    fn a_change_set_with_no_heads_is_rejected() {
        let mut doc = crate::Automerge::new();
        doc.enable_audit_mode().unwrap();
        {
            use crate::transaction::Transactable;
            let mut tx = doc.transaction();
            tx.put(crate::ROOT, "k", 1).unwrap();
            tx.commit();
        }
        let f = &doc.fragments(..)[0];
        let good = doc.change_set_for_fragment(f).unwrap();
        assert_eq!(good.heads().len(), 1);

        // rebuild the prefix with an empty head list
        let mut b = ChangeSet::try_from(&good.bytes()[..]).unwrap();
        b.heads.clear();
        let err = ChangeSet::try_from(&b.bytes()[..]).expect_err("no heads must be rejected");
        assert!(err.0.contains("at least one head"), "got {err}");
    }

    /// The counts are only trusted as far as the remaining bytes could
    /// possibly satisfy them.
    #[test]
    fn entry_capacity_is_clamped_to_the_input() {
        let bytes = [0u8; 100];
        let i = parse::Input::new(&bytes);
        assert_eq!(entry_capacity(&i, 3, 33), 3, "an honest count is kept");
        assert_eq!(entry_capacity(&i, u64::MAX, 33), 3, "a lie is clamped");
        assert_eq!(entry_capacity(&i, u64::MAX, 2), 50);
    }

    /// Change sequence numbers are 1-based, so a zero on the wire is a
    /// parse error — not a panic deep in `ChangeId`.
    #[test]
    fn zero_sequence_numbers_are_rejected() {
        use crate::transaction::Transactable;
        use crate::{Automerge, ROOT};

        // pinned actor + timestamp: the fragment layout this test indexes
        // into depends on which commits hash to fragment heads
        let mut src = Automerge::new().with_actor(crate::ActorId::from(&b"zsn"[..]));
        src.enable_audit_mode().unwrap();
        for i in 0..6 {
            let mut tx = src.transaction();
            tx.put(ROOT, "k", i).unwrap();
            tx.commit_with(crate::transaction::CommitOptions::default().with_time(0));
        }
        let frags = src.fragments(0..=0);
        // the second fragment has a boundary entry (its dep on the first)
        let mut bytes = src.change_set_for_fragment(&frags[1]).unwrap().bytes();
        assert!(ChangeSet::try_from(&bytes[..]).is_ok(), "baseline parses");

        // walk the prefix to the first boundary entry's seq and zero it
        let mut at = MAGIC_BYTES.len() + 4 + 1;
        let read = |b: &[u8], at: &mut usize| -> u64 {
            let mut r = 0u64;
            let mut shift = 0;
            loop {
                let x = b[*at];
                *at += 1;
                r |= u64::from(x & 0x7f) << shift;
                if x & 0x80 == 0 {
                    return r;
                }
                shift += 7;
            }
        };
        read(&bytes, &mut at); // chunk length
        let n_actors = read(&bytes, &mut at);
        for _ in 0..n_actors {
            let n = read(&bytes, &mut at) as usize;
            at += n;
        }
        let n_heads = read(&bytes, &mut at);
        assert!(n_heads > 0, "fixture needs a head");
        for _ in 0..n_heads {
            at += 32; // head hash
            read(&bytes, &mut at); // head member index
        }
        let n_checkpoints = read(&bytes, &mut at);
        for _ in 0..n_checkpoints {
            read(&bytes, &mut at);
            at += 32;
        }
        let n_boundary = read(&bytes, &mut at);
        assert!(n_boundary > 0, "fixture needs a boundary entry");
        at += 32; // boundary hash
        read(&bytes, &mut at); // boundary actor
        let seq_at = at;
        let seq = read(&bytes, &mut at);
        assert_eq!(at, seq_at + 1, "fixture seq is a single byte");
        assert_ne!(seq, 0);

        bytes[seq_at] = 0;
        let err = ChangeSet::try_from(&bytes[..]).expect_err("zero seq must be rejected");
        assert!(err.0.contains("sequence number"), "got {err}");
    }
}
