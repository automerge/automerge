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
mod storage;

pub(crate) use builder::ops;
pub(crate) use builder::{
    ChangeSetBuilder, ChangeSetChangeCols, ChangeSetChangeIterUnverified, ChangeSetChangeWriter,
    ChangeSetOpWriter, DeltaRunGrouper, ManifoldOp, ManifoldOps, OpIterUnverified,
};
pub(crate) use error::ParseError;
pub(crate) use storage::ChangeSetStorage;

/// EXPERIMENTAL: A set of changes in compressed columnar form, plus the
/// fragment metadata needed to apply them to a document that does not know
/// every change's hash.
///
/// The wire layout after the chunk header is:
///
/// ```text
/// actors      uleb count, then length-prefixed actor ids (only the
///             actors the prefix itself references)
/// heads       uleb count, then per entry: 32-byte hash + uleb member index
/// checkpoints uleb count, then per entry: uleb member index + 32-byte hash
/// retained    uleb count, then per entry: uleb member index + 32-byte hash
/// boundary    uleb count, then per entry: 32-byte hash + uleb actor + uleb seq
/// deps        uleb count, then per entry: uleb actor + uleb seq
/// columns     the carried changes' columns: external dep hashes, actors,
///             change metadata columns, op columns
/// ```
///
/// Member indexes refer to the carried changes in topological order.
/// `heads` are the members nothing else depends on, `checkpoints` the other
/// fragment-level members, and `retained` the level-0 member hashes the
/// receiver keeps. `boundary` pairs external dependency hashes with their
/// change ids, and `deps` gives the change id of each external dep in the
/// column section, in the same order.
///
/// The format may still change — do not use it in systems where you expect
/// data to stick around.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// `(hash, member index)`
    pub(crate) heads: Vec<(ChangeHash, usize)>,
    /// `(member index, hash)`
    pub(crate) checkpoints: Vec<(usize, ChangeHash)>,
    /// `(member index, hash)`, level-0 only and disjoint from `heads`.
    pub(crate) retained: Vec<(usize, ChangeHash)>,
    pub(crate) boundary: Vec<(ChangeHash, ChangeId)>,
    /// Aligned with [`Self::deps`].
    pub(crate) dep_ids: Vec<ChangeId>,
    /// Validated at parse time; aligned with the members.
    pub(crate) member_actors: Vec<ActorIdx>,
    pub(crate) member_seqs: Vec<NonZeroU64>,
    pub(crate) storage: ChangeSetStorage<'static, Verified>,
}

impl ChangeSet {
    pub(crate) fn change_cols(&self) -> ChangeSetChangeCols<'_> {
        self.storage
            .change_cols()
            .expect("a parsed change_set's change columns are well formed")
    }

    /// The members no other member of the change set depends on.
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
    /// decoding its ops. The metadata is decoded on the first call, which
    /// fails if it is malformed.
    pub fn iter_changes(
        &self,
    ) -> Result<std::slice::Iter<'_, ChangeSetChange<'static>>, AutomergeError> {
        Ok(self.changes()?.iter())
    }

    pub(crate) fn take_change_set_ops(&mut self) -> Result<OpSet, AutomergeError> {
        self.storage
            .take_change_set_ops()
            .map_err(|e| AutomergeError::DecodeChangeSet(Box::new(e)))
    }

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

    #[cfg(debug_assertions)]
    pub(crate) fn storage_for_hashes<I, H: crate::hash_retention::HashRetention>(
        op_set: &OpSet,
        change_graph: &ChangeGraph<H>,
        hashes: I,
    ) -> Result<ChangeSetStorage<'static, Verified>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let mut nodes = hashes
            .into_iter()
            .map(|h| change_graph.node_by_hash(&h))
            .collect::<Option<Vec<_>>>()
            .ok_or(crate::change_graph::MissingDep)?;
        nodes.sort_unstable();
        Self::storage_for_nodes(op_set, change_graph, nodes)
    }

    /// `nodes` must be sorted ascending.
    pub(crate) fn storage_for_nodes<H: crate::hash_retention::HashRetention>(
        op_set: &OpSet,
        change_graph: &ChangeGraph<H>,
        nodes: Vec<crate::change_graph::NodeIdx>,
    ) -> Result<ChangeSetStorage<'static, Verified>, AutomergeError> {
        Ok(
            Self::storage_for_node_sets(op_set, change_graph, vec![nodes])?
                .pop()
                .expect("one set in, one out"),
        )
    }

    /// Copies every op row verbatim, so the op set must hold no
    /// uncommitted ops.
    pub(crate) fn storage_for_document<H: crate::hash_retention::HashRetention>(
        op_set: &OpSet,
        change_graph: &ChangeGraph<H>,
        nodes: &[crate::change_graph::NodeIdx],
    ) -> Result<ChangeSetStorage<'static, Verified>, AutomergeError> {
        let mut mapper = ActorMapper::new(&op_set.actors);
        let mut change_writer = ChangeSetChangeWriter::new(nodes.len());
        change_graph.write_change_set_changes(nodes, &mut change_writer, &mut mapper)?;
        let mut op_writer = ChangeSetOpWriter::default();
        op_set.write_all_change_set_ops(&mut op_writer, &mut mapper);
        Ok(
            ChangeSetBuilder::from_writers(mapper, change_writer, op_writer)
                .finish_with_ranks(&std::collections::HashMap::new()),
        )
    }

    /// [`Self::storage_for_nodes`] for several change sets at once. Hint
    /// ranks need a walk of the document's id column; batching shares one
    /// walk rather than paying O(change sets × document).
    pub(crate) fn storage_for_node_sets<H: crate::hash_retention::HashRetention>(
        op_set: &OpSet,
        change_graph: &ChangeGraph<H>,
        node_sets: Vec<Vec<crate::change_graph::NodeIdx>>,
    ) -> Result<Vec<ChangeSetStorage<'static, Verified>>, AutomergeError> {
        let mut jobs = Vec::with_capacity(node_sets.len());
        for nodes in node_sets {
            let clock = change_graph.clock_for_nodes(nodes.clone());
            let mapper = ActorMapper::new(&op_set.actors);
            let collector = ChangeSetBuilder::from_graph(op_set, change_graph, &nodes, mapper)?;
            let collector = Self::walk_ops(op_set, collector);
            jobs.push(HintJob {
                targets_by_obj: collector.hint_targets_by_obj(),
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

    fn resolve_hint_ranks(op_set: &OpSet, jobs: &mut [HintJob<'_>]) {
        // `ObjId` order is document order, so one forward-only cursor
        // covers every object
        let mut objs: std::collections::BTreeMap<crate::types::ObjId, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (i, job) in jobs.iter().enumerate() {
            for obj in job.targets_by_obj.keys() {
                objs.entry(*obj).or_default().push(i);
            }
        }

        let mut obj_id_iter = op_set.obj_id_iter();

        for (obj, interested) in objs {
            let scope = obj_id_iter.seek_to_value(obj);
            let targets: Vec<rustc_hash::FxHashSet<crate::types::OpId>> = interested
                .iter()
                .map(|&i| jobs[i].targets_by_obj.remove(&obj).unwrap_or_default())
                .collect();
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
                    if !covered && !wanted {
                        k += 1;
                        continue;
                    }
                    // a receiver applying the change set does not have its
                    // members yet, so they do not count towards the rank
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

    fn walk_ops<'a>(
        op_set: &'a OpSet,
        mut collector: ChangeSetBuilder<'a>,
    ) -> ChangeSetBuilder<'a> {
        for op in op_set.iter_ctr_range(collector.op_counters()) {
            let op_id = op.id;
            let op_succ: Vec<_> = op.succ().collect();
            collector.process_op(op, &op_succ);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }
        collector
    }

    /// The inner chunk: the carried changes on their own, with per-column
    /// DEFLATE applied where each column is large enough to benefit.
    /// Falls back to the uncompressed buffer for storage built before the
    /// per-column compression pass (or parsed from input with no
    /// compressed columns).
    fn compressed_column_bytes(&self) -> &[u8] {
        match &self.storage.compressed_bytes {
            Some(c) => c,
            None => &self.storage.bytes,
        }
    }

    /// The encoded chunk, with large columns DEFLATE-compressed.
    pub fn bytes(&self) -> Vec<u8> {
        self.bytes_with(true)
    }

    /// [`Self::bytes`] with no column compressed.
    pub fn bytes_uncompressed(&self) -> Vec<u8> {
        self.bytes_with(false)
    }

    fn bytes_with(&self, deflate: bool) -> Vec<u8> {
        fn intern_actor<'x>(actors: &mut Vec<&'x ActorId>, a: &'x ActorId) -> u64 {
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
            .map(|(h, id)| (*h, intern_actor(&mut actors, id.actor()), id.seq()))
            .collect();
        let deps: Vec<(u64, u64)> = self
            .dep_ids
            .iter()
            .map(|id| (intern_actor(&mut actors, id.actor()), id.seq()))
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
        leb128::write::unsigned(&mut data, self.retained.len() as u64).unwrap();
        for (i, h) in &self.retained {
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
            self.compressed_column_bytes()
        } else {
            &self.storage.bytes
        });

        let header = Header::new(ChunkType::ChangeSet, &data);
        let mut out = Vec::with_capacity(header.len() + data.len());
        header.write(&mut out);
        out.extend(data);
        out
    }

    pub(crate) fn parse_prefix(
        i: parse::Input<'_>,
    ) -> parse::ParseResult<'_, ParsedPrefix, parse::leb128::Error> {
        let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;

        let (i, n_heads) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut heads = Vec::with_capacity(entry_capacity(&i, n_heads, HASH + MIN_ULEB));
        for _ in 0..n_heads {
            let (j, h) = parse::change_hash(i)?;
            let (j, idx) = parse::leb128_u64(j)?;
            heads.push((h, idx as usize));
            i = j;
        }

        let (i, checkpoints) = parse_indexed_hashes(i)?;
        let (i, retained) = parse_indexed_hashes(i)?;

        let (i, n_boundary) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut boundary = Vec::with_capacity(entry_capacity(&i, n_boundary, HASH + 2 * MIN_ULEB));
        for _ in 0..n_boundary {
            let (j, h) = parse::change_hash(i)?;
            let (j, a) = parse::leb128_u64(j)?;
            let (j, s) = parse::leb128_u64(j)?;
            boundary.push((h, a, s));
            i = j;
        }

        let (i, n_deps) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut deps = Vec::with_capacity(entry_capacity(&i, n_deps, 2 * MIN_ULEB));
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
                retained,
                boundary,
                deps,
            },
        ))
    }
}

struct HintJob<'a> {
    collector: ChangeSetBuilder<'a>,
    clock: crate::clock::Clock,
    targets_by_obj:
        std::collections::HashMap<crate::types::ObjId, rustc_hash::FxHashSet<crate::types::OpId>>,
    ranks: std::collections::HashMap<crate::types::OpId, u64>,
}

const HASH: usize = 32;
const MIN_ULEB: usize = 1;

fn parse_indexed_hashes(
    i: parse::Input<'_>,
) -> parse::ParseResult<'_, Vec<(usize, ChangeHash)>, parse::leb128::Error> {
    let (mut i, count) = parse::leb128_u64(i)?;
    let mut out = Vec::with_capacity(entry_capacity(&i, count, MIN_ULEB + HASH));
    for _ in 0..count {
        let (j, idx) = parse::leb128_u64(i)?;
        let (j, h) = parse::change_hash(j)?;
        out.push((idx as usize, h));
        i = j;
    }
    Ok((i, out))
}

/// A wire-supplied `count` clamped to what the remaining input could hold,
/// so a hostile count cannot force a huge allocation.
fn entry_capacity(i: &parse::Input<'_>, count: u64, min_entry_bytes: usize) -> usize {
    let ceiling = i.unconsumed_bytes().len() / min_entry_bytes;
    usize::try_from(count).unwrap_or(usize::MAX).min(ceiling)
}

#[derive(Debug)]
pub(crate) struct ParsedPrefix {
    actors: Vec<ActorId>,
    heads: Vec<(ChangeHash, usize)>,
    checkpoints: Vec<(usize, ChangeHash)>,
    retained: Vec<(usize, ChangeHash)>,
    boundary: Vec<(ChangeHash, u64, u64)>,
    deps: Vec<(u64, u64)>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid change_set: {0}")]
pub struct InvalidChangeSet(pub(crate) String);

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

        if dep_ids.len() != storage.deps().len() {
            return Err(bad("dep ids do not match the carried changes' deps"));
        }
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
        // completeness is not checked: it would need every member's hash
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
        if prefix.retained.iter().any(|(i, _)| *i >= num_members) {
            return Err(bad("retained hash index out of range"));
        }
        // a level > 0 retained hash would bypass the checkpoint checks
        if prefix.retained.iter().any(|(_, h)| h.fragment_level() > 0) {
            return Err(bad("retained hash is a fragment head"));
        }
        if prefix
            .retained
            .iter()
            .any(|(_, r)| prefix.heads.iter().any(|(h, _)| h == r))
        {
            return Err(bad("retained hash duplicates a delivered head"));
        }

        Ok(ChangeSet {
            heads: prefix.heads,
            checkpoints: prefix.checkpoints,
            retained: prefix.retained,
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

    #[test]
    fn change_sets_carry_their_fragment_level_members_as_checkpoints() {
        use crate::autocommit::AutoCommit;
        use crate::tx::{CommitOptions, Transactable};
        use crate::{ActorId, ROOT};

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
        assert!(cs
            .checkpoints
            .iter()
            .all(|(i, _)| *i < cs.member_seqs.len()));
    }

    /// A change set's bytes must be a function of its content, and batched
    /// bundling must agree with bundling one fragment at a time.
    #[test]
    fn bundling_is_deterministic() {
        use crate::autocommit::AutoCommit;
        use crate::tx::Transactable;
        use crate::{ObjType, ROOT};

        // concurrent deletes of one span put several pending deletes in
        // the same key group
        let mut doc = AutoCommit::new()
            .with_actor(crate::ActorId::from(&[0u8][..]))
            .enable_audit_mode()
            .unwrap();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, &"abcdefghij".repeat(8))
            .unwrap();
        doc.commit();

        for round in 0..6u8 {
            for a in 1..=6u8 {
                let mut fork = doc
                    .fork()
                    .with_actor(crate::ActorId::from(&[round * 16 + a][..]));
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

    /// `ChangeSet::try_from` does not verify the checksum, so it is blank.
    fn chunk(data: Vec<u8>) -> Vec<u8> {
        let mut out = MAGIC_BYTES.to_vec();
        out.extend([0u8; 4]);
        out.push(u8::from(ChunkType::ChangeSet));
        leb(data.len() as u64, &mut out);
        out.extend(data);
        out
    }

    #[test]
    fn absurd_counts_error_instead_of_allocating() {
        for section in 0..5 {
            let mut data = Vec::new();
            leb(0, &mut data); // no actors
            for s in 0..5 {
                leb(if s == section { u64::MAX / 64 } else { 0 }, &mut data);
                if s == section {
                    break;
                }
            }
            let err = ChangeSet::try_from(&chunk(data)[..]);
            assert!(err.is_err(), "section {section} should not parse");
        }
    }

    #[test]
    fn a_change_set_with_no_heads_is_rejected() {
        let mut doc = crate::automerge::Automerge::new()
            .enable_audit_mode()
            .unwrap();
        {
            use crate::tx::Transactable;
            let mut tx = doc.transaction();
            tx.put(crate::ROOT, "k", 1).unwrap();
            tx.commit();
        }
        let f = &doc.fragments(..)[0];
        let good = doc.change_set_for_fragment(f).unwrap();
        assert_eq!(good.heads().len(), 1);

        let mut b = ChangeSet::try_from(&good.bytes()[..]).unwrap();
        b.heads.clear();
        let err = ChangeSet::try_from(&b.bytes()[..]).expect_err("no heads must be rejected");
        assert!(err.0.contains("at least one head"), "got {err}");
    }

    #[test]
    fn entry_capacity_is_clamped_to_the_input() {
        let bytes = [0u8; 100];
        let i = parse::Input::new(&bytes);
        assert_eq!(entry_capacity(&i, 3, 33), 3, "an honest count is kept");
        assert_eq!(entry_capacity(&i, u64::MAX, 33), 3, "a lie is clamped");
        assert_eq!(entry_capacity(&i, u64::MAX, 2), 50);
    }

    /// A zero seq must be a parse error, not a panic in `ChangeId`.
    #[test]
    fn zero_sequence_numbers_are_rejected() {
        use crate::automerge::Automerge;
        use crate::tx::Transactable;
        use crate::ROOT;

        // pinned actor and timestamp fix which commits are fragment heads
        let mut src = Automerge::new()
            .with_actor(crate::ActorId::from(&b"zsn"[..]))
            .enable_audit_mode()
            .unwrap();
        for i in 0..6 {
            let mut tx = src.transaction();
            tx.put(ROOT, "k", i).unwrap();
            tx.commit_with(crate::tx::CommitOptions::default().with_time(0));
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
        let n_retained = read(&bytes, &mut at);
        for _ in 0..n_retained {
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
