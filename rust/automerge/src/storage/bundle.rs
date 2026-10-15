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
    BundleBuilder, BundleChangeCols, BundleChangeIterUnverified, BundleChangeWriter,
    BundleOpWriter, DeltaRunGrouper, ManifoldOp, ManifoldOps,
};
pub(crate) use error::ParseError;
pub(crate) use storage::BundleStorage;

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
pub struct Bundle {
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
    pub(crate) storage: BundleStorage<'static, Verified>,
}

/// A change with at most this many ops is smaller than its hash.
pub(crate) const TINY_CHANGE_OPS: u64 = 8;
/// A load rehashes a bundle within both limits in about a millisecond.
pub(crate) const REHASHABLE_CHANGES: usize = 500;
pub(crate) const REHASHABLE_OPS: u64 = 5_000;

pub(crate) fn rehashable(num_changes: usize, num_ops: u64) -> bool {
    num_changes <= REHASHABLE_CHANGES && num_ops <= REHASHABLE_OPS
}

impl Bundle {
    /// Whether a load may rehash the members whose hashes this bundle
    /// leaves out: it depends on nothing outside itself and is small.
    pub(crate) fn is_rehashable(&self) -> bool {
        // decoding the members' metadata is O(members), so count them first
        self.deps().is_empty()
            && self.boundary.is_empty()
            && self.member_seqs.len() <= REHASHABLE_CHANGES
            && self.changes().is_ok_and(|changes| {
                rehashable(
                    changes.len(),
                    changes
                        .iter()
                        .map(|c| (c.max_op + 1).saturating_sub(c.start_op))
                        .sum(),
                )
            })
    }

    /// Every member's hash, in member order: `known[i]` where given,
    /// otherwise rebuilt from this bundle's ops. Reads every op row.
    pub(crate) fn member_hashes(
        &self,
        known: &[Option<ChangeHash>],
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        self.storage
            .member_hashes(known)
            .map_err(|e| AutomergeError::DecodeBundle(Box::new(e)))
    }

    pub(crate) fn change_cols(&self) -> BundleChangeCols<'_> {
        self.storage
            .change_cols()
            .expect("a parsed bundle's change columns are well formed")
    }

    /// The members no other member of the bundle depends on.
    pub fn heads(&self) -> impl ExactSizeIterator<Item = ChangeHash> + '_ {
        self.heads.iter().map(|(h, _)| *h)
    }

    /// The changes carried by the bundle, in topological order.
    pub fn to_changes(&self) -> Result<Vec<Change>, AutomergeError> {
        self.storage
            .to_changes()
            .map_err(|e| AutomergeError::DecodeBundle(Box::new(e)))
    }

    /// What [`Self::to_changes`] would return, or with `M = ChangeHash` just
    /// the changes' hashes, in topological order, after checking that
    /// everything the bundle says about them is true. Two things cannot
    /// be checked without the sender's document: that it retains every hash
    /// a receiver will need, and its op hints.
    ///
    /// Hashes every carried change; `M = Change` also rebuilds each one.
    pub fn audit<M: AuditedMember>(&self) -> Result<Vec<M>, MalformedBundle> {
        use std::collections::HashSet;

        let members = M::decode(self)?;
        let hash = |i: usize| members[i].hash();
        let num_members = members.len();
        let decode = |e| MalformedBundle::Decode(Box::new(e));

        // decoding checked that the actors are unique, so an index names one
        let mut member_ids = HashSet::with_capacity(num_members);
        let mut depended_on = vec![false; num_members];
        let mut used_deps = vec![false; self.deps().len()];
        let mut num_meta = 0;
        for (i, m) in self.storage.iter_change_meta().map_err(decode)?.enumerate() {
            let m = m.map_err(decode)?;
            num_meta += 1;
            if m.actor >= self.actors().len() {
                return Err(MalformedBundle::Invalid("member actor out of range"));
            }
            if !member_ids.insert((m.actor, m.seq)) {
                let actor = self.actors()[m.actor].clone();
                return Err(MalformedBundle::DuplicateMember(ChangeId::from_doc_seq(
                    m.seq, actor, 0,
                )));
            }
            for d in &m.deps {
                let d = *d as usize;
                match d.checked_sub(num_members) {
                    None if d < i => depended_on[d] = true,
                    None => return Err(MalformedBundle::NotTopological(i)),
                    Some(e) => {
                        *used_deps
                            .get_mut(e)
                            .ok_or(MalformedBundle::Invalid("member dep out of range"))? = true
                    }
                }
            }
        }
        if num_meta != num_members {
            return Err(MalformedBundle::Invalid("member count mismatch"));
        }
        let is_member = |id: &ChangeId| {
            self.actors()
                .binary_search(id.actor())
                .is_ok_and(|actor| member_ids.contains(&(actor, id.seq())))
        };

        let names = |index: usize, h: ChangeHash| index < num_members && hash(index) == h;
        for &(hash, index) in &self.heads {
            if !names(index, hash) {
                return Err(MalformedBundle::WrongHeadHash { hash, index });
            }
        }
        for &(index, hash) in &self.checkpoints {
            if !names(index, hash) {
                return Err(MalformedBundle::WrongCheckpointHash { hash, index });
            }
        }
        for &(index, hash) in &self.retained {
            if !names(index, hash) {
                return Err(MalformedBundle::WrongRetainedHash { hash, index });
            }
        }

        let mut declared_heads: Vec<usize> = self.heads.iter().map(|(_, i)| *i).collect();
        declared_heads.sort_unstable();
        let actual_heads: Vec<usize> = (0..num_members).filter(|i| !depended_on[*i]).collect();
        if declared_heads != actual_heads {
            return Err(MalformedBundle::WrongHeads);
        }

        let carried = |hashes: &HashSet<ChangeHash>| {
            if hashes.is_empty() {
                return None;
            }
            (0..num_members).map(hash).find(|h| hashes.contains(h))
        };
        let declared_deps: HashSet<ChangeHash> = self.deps().iter().copied().collect();
        if declared_deps.len() != self.deps().len()
            || used_deps.contains(&false)
            || carried(&declared_deps).is_some()
        {
            return Err(MalformedBundle::WrongDeps);
        }

        let mut declared_checkpoints: Vec<usize> =
            self.checkpoints.iter().map(|(i, _)| *i).collect();
        declared_checkpoints.sort_unstable();
        let actual_checkpoints: Vec<usize> = (0..num_members)
            .filter(|i| hash(*i).fragment_level() > 0 && depended_on[*i])
            .collect();
        if declared_checkpoints != actual_checkpoints {
            return Err(MalformedBundle::WrongCheckpoints);
        }

        if let Some(id) = self.dep_ids.iter().find(|id| is_member(id)) {
            return Err(MalformedBundle::ExternalIdIsMember(id.clone()));
        }
        let boundary: HashSet<ChangeHash> = self.boundary.iter().map(|(h, _)| *h).collect();
        if let Some(hash) = carried(&boundary) {
            return Err(MalformedBundle::BoundaryIsMember(hash));
        }
        for (hash, id) in &self.boundary {
            if is_member(id) {
                return Err(MalformedBundle::ExternalIdIsMember(id.clone()));
            }
            if let Some(k) = self.deps().iter().position(|d| d == hash) {
                if self.dep_ids[k] != *id {
                    return Err(MalformedBundle::BoundaryDisagreesWithDep(*hash));
                }
            }
        }

        Ok(members)
    }

    /// Metadata for each carried change, in topological order, without
    /// decoding its ops. The metadata is decoded on the first call, which
    /// fails if it is malformed.
    pub fn iter_changes(
        &self,
    ) -> Result<std::slice::Iter<'_, BundleChange<'static>>, AutomergeError> {
        Ok(self.changes()?.iter())
    }

    pub(crate) fn take_bundle_ops(&mut self) -> Result<OpSet, AutomergeError> {
        self.storage
            .take_bundle_ops()
            .map_err(|e| AutomergeError::DecodeBundle(Box::new(e)))
    }

    pub(crate) fn changes(&self) -> Result<&[BundleChange<'static>], AutomergeError> {
        self.storage
            .changes()
            .map_err(|e| AutomergeError::DecodeBundle(Box::new(e)))
    }

    /// The hashes this bundle depends on but does not carry.
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
    ) -> Result<BundleStorage<'static, Verified>, AutomergeError>
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
    ) -> Result<BundleStorage<'static, Verified>, AutomergeError> {
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
    ) -> Result<BundleStorage<'static, Verified>, AutomergeError> {
        let mut mapper = ActorMapper::new(&op_set.actors);
        let mut change_writer = BundleChangeWriter::new(nodes.len());
        change_graph.write_bundle_changes(nodes, &mut change_writer, &mut mapper)?;
        let mut op_writer = BundleOpWriter::default();
        op_set.write_all_bundle_ops(&mut op_writer, &mut mapper);
        Ok(
            BundleBuilder::from_writers(mapper, change_writer, op_writer)
                .finish_with_ranks(&std::collections::HashMap::new()),
        )
    }

    /// [`Self::storage_for_nodes`] for several bundles at once. Hint
    /// ranks need a walk of the document's id column; batching shares one
    /// walk rather than paying O(bundles × document).
    pub(crate) fn storage_for_node_sets<H: crate::hash_retention::HashRetention>(
        op_set: &OpSet,
        change_graph: &ChangeGraph<H>,
        node_sets: Vec<Vec<crate::change_graph::NodeIdx>>,
    ) -> Result<Vec<BundleStorage<'static, Verified>>, AutomergeError> {
        let mut jobs = Vec::with_capacity(node_sets.len());
        for nodes in node_sets {
            let clock = change_graph.clock_for_nodes(nodes.clone());
            let mapper = ActorMapper::new(&op_set.actors);
            let collector = BundleBuilder::from_graph(op_set, change_graph, &nodes, mapper)?;
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
                    // a receiver applying the bundle does not have its
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

    fn walk_ops<'a>(op_set: &'a OpSet, mut collector: BundleBuilder<'a>) -> BundleBuilder<'a> {
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

        let header = Header::new(ChunkType::Bundle, &data);
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
    collector: BundleBuilder<'a>,
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
#[error("invalid bundle: {0}")]
pub struct InvalidBundle(pub(crate) String);

/// Why a bundle was rejected as malformed.
#[derive(Debug, thiserror::Error)]
pub enum MalformedBundle {
    #[error("malformed bundle: {0}")]
    Invalid(&'static str),
    #[error("malformed bundle: unable to rebuild the carried changes: {0}")]
    Decode(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("malformed bundle: change {0} is carried twice")]
    DuplicateMember(ChangeId),
    #[error("malformed bundle: member {0} depends on a member after it")]
    NotTopological(usize),
    #[error("malformed bundle: head {hash} does not name member {index}")]
    WrongHeadHash { hash: ChangeHash, index: usize },
    #[error("malformed bundle: checkpoint {hash} does not name member {index}")]
    WrongCheckpointHash { hash: ChangeHash, index: usize },
    #[error("malformed bundle: retained hash {hash} does not name member {index}")]
    WrongRetainedHash { hash: ChangeHash, index: usize },
    #[error("malformed bundle: the heads are not the members nothing else depends on")]
    WrongHeads,
    #[error("malformed bundle: the deps are not what the members depend on but do not carry")]
    WrongDeps,
    #[error("malformed bundle: the checkpoints are not the non-head fragment-level members")]
    WrongCheckpoints,
    #[error("malformed bundle: boundary {0} is a carried change")]
    BoundaryIsMember(ChangeHash),
    #[error("malformed bundle: dep or boundary id {0} is a carried change")]
    ExternalIdIsMember(ChangeId),
    #[error("malformed bundle: boundary {0} and the dep with its hash have different change ids")]
    BoundaryDisagreesWithDep(ChangeHash),
}

/// What [`Bundle::audit`] returns for each carried change: the whole
/// [`Change`], or only its [`ChangeHash`].
pub trait AuditedMember: Sized + sealed::Sealed {
    #[doc(hidden)]
    fn decode(bundle: &Bundle) -> Result<Vec<Self>, MalformedBundle>;
    #[doc(hidden)]
    fn hash(&self) -> ChangeHash;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::Change {}
    impl Sealed for crate::ChangeHash {}
}

impl AuditedMember for Change {
    fn decode(bundle: &Bundle) -> Result<Vec<Self>, MalformedBundle> {
        bundle
            .storage
            .to_changes()
            .map_err(|e| MalformedBundle::Decode(Box::new(e)))
    }

    fn hash(&self) -> ChangeHash {
        Change::hash(self)
    }
}

impl AuditedMember for ChangeHash {
    fn decode(bundle: &Bundle) -> Result<Vec<Self>, MalformedBundle> {
        bundle
            .storage
            .hash_members()
            .map_err(|e| MalformedBundle::Decode(Box::new(e)))
    }

    fn hash(&self) -> ChangeHash {
        *self
    }
}

fn parse_bundle_columns(bytes: &[u8]) -> Result<BundleStorage<'static, Verified>, String> {
    let input = parse::Input::new(bytes);
    let (_i, stored) = BundleStorage::parse_columns(input)
        .map_err(|e| format!("invalid carried changes: {}", e))?;
    let verified = stored
        .verify()
        .map_err(|e| format!("unable to verify ops: {}", e))?;
    Ok(verified.into_owned())
}

/// Metadata for one change carried by a bundle.
#[derive(Clone, Debug)]
pub struct BundleChange<'a> {
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

impl BundleChange<'_> {
    pub(crate) fn into_owned(self) -> BundleChange<'static> {
        BundleChange {
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

impl<'a> From<BundleChange<'a>> for BuildChangeMetadata<'a> {
    fn from(bundle: BundleChange<'a>) -> Self {
        BuildChangeMetadata {
            actor: bundle.actor,
            seq: bundle.seq,
            start_op: bundle.start_op,
            max_op: bundle.max_op,
            timestamp: bundle.timestamp,
            message: bundle.message,
            deps: bundle.deps,
            extra: bundle.extra,
            builder: 0,
        }
    }
}

impl TryFrom<&[u8]> for Bundle {
    type Error = InvalidBundle;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let input = parse::Input::new(bytes);
        let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
            .map_err(|e| InvalidBundle(format!("invalid header: {}", e)))?;
        if header.chunk_type() != ChunkType::Bundle {
            return Err(InvalidBundle("not a bundle chunk".to_string()));
        }
        if !header.checksum_valid() {
            return Err(InvalidBundle("bad checksum".to_string()));
        }
        Self::parse_after_header(i)
    }
}

impl Bundle {
    pub(crate) fn parse_after_header(i: parse::Input<'_>) -> Result<Self, InvalidBundle> {
        let bad = |s: &str| InvalidBundle(s.to_string());
        let (i, prefix) =
            Self::parse_prefix(i).map_err(|e| InvalidBundle(format!("invalid prefix: {}", e)))?;

        let resolve = |a: u64| -> Result<ActorId, InvalidBundle> {
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
            .collect::<Result<Vec<_>, InvalidBundle>>()?;
        let dep_ids = prefix
            .deps
            .iter()
            .map(|(a, s)| change_id(*a, *s))
            .collect::<Result<Vec<_>, InvalidBundle>>()?;

        let storage = parse_bundle_columns(i.unconsumed_bytes())
            .map_err(|e| InvalidBundle(format!("invalid carried changes: {}", e)))?;

        if dep_ids.len() != storage.deps().len() {
            return Err(bad("dep ids do not match the carried changes' deps"));
        }
        let (member_actors, member_seqs) = storage
            .member_ids()
            .map_err(|e| InvalidBundle(format!("invalid change metadata: {}", e)))?;
        let num_members = member_seqs.len();
        if prefix.heads.is_empty() {
            return Err(bad("a bundle must deliver at least one head"));
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

        Ok(Bundle {
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
    fn bundles_carry_their_fragment_level_members_as_checkpoints() {
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
        let cs = Bundle::try_from(bytes.as_slice()).unwrap();

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
            "a bundle must carry exactly its non-head fragment-level members",
        );
        assert!(cs
            .checkpoints
            .iter()
            .all(|(i, _)| *i < cs.member_seqs.len()));
    }

    /// A bundle's bytes must be a function of its content, and batched
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
            let first = doc.document().bundle_for_fragment(f).unwrap().bytes();
            for _ in 0..4 {
                assert_eq!(
                    doc.document().bundle_for_fragment(f).unwrap().bytes(),
                    first,
                    "bundle bytes vary between identical calls"
                );
            }
        }

        let batched = doc
            .document()
            .bundles_for_fragments(fragments.clone())
            .unwrap();
        let singly: Vec<Vec<u8>> = fragments
            .iter()
            .map(|f| doc.document().bundle_for_fragment(f).unwrap().bytes())
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

    /// `Bundle::try_from` does not verify the checksum, so it is blank.
    fn chunk(data: Vec<u8>) -> Vec<u8> {
        let mut out = MAGIC_BYTES.to_vec();
        out.extend([0u8; 4]);
        out.push(u8::from(ChunkType::Bundle));
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
            let err = Bundle::try_from(&chunk(data)[..]);
            assert!(err.is_err(), "section {section} should not parse");
        }
    }

    #[test]
    fn a_bundle_with_no_heads_is_rejected() {
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
        let good = doc.bundle_for_fragment(f).unwrap();
        assert_eq!(good.heads().len(), 1);

        let mut b = Bundle::try_from(&good.bytes()[..]).unwrap();
        b.heads.clear();
        let err = Bundle::try_from(&b.bytes()[..]).expect_err("no heads must be rejected");
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
        let mut bytes = src.bundle_for_fragment(&frags[1]).unwrap().bytes();
        assert!(Bundle::try_from(&bytes[..]).is_ok(), "baseline parses");

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
        let err = Bundle::try_from(&with_recomputed_checksum(&bytes)[..])
            .expect_err("zero seq must be rejected");
        assert!(err.0.contains("sequence number"), "got {err}");
    }

    fn audited(actor: &[u8]) -> crate::automerge::Automerge<crate::hash_retention::Full> {
        crate::automerge::Automerge::new()
            .with_actor(crate::ActorId::from(actor))
            .enable_audit_mode()
            .unwrap()
    }

    fn put(doc: &mut crate::automerge::Automerge<crate::hash_retention::Full>, i: i64) {
        use crate::tx::{CommitOptions, Transactable};
        let mut tx = doc.transaction();
        tx.put(crate::ROOT, "k", i).unwrap();
        tx.commit_with(CommitOptions::default().with_time(0));
    }

    #[test]
    fn an_audited_apply_skips_members_it_has() {
        let src = audit_fixture();
        let mut doc = audited(b"rcv");
        let first = src.bundle_for_fragment(&src.fragments(..)[0]).unwrap();
        doc.apply_bundle(first).unwrap();
        doc.apply_bundle(src.bundle_document().unwrap()).unwrap();
        assert_eq!(doc.get_heads(), src.get_heads());
        assert_eq!(doc.save(), src.save());
    }

    #[test]
    fn an_audited_apply_rejects_a_forked_actor() {
        let mut ours = audited(b"zsn");
        put(&mut ours, 1);
        let mut theirs = audited(b"zsn");
        put(&mut theirs, 2);
        put(&mut theirs, 3);
        let err = ours
            .apply_bundle(theirs.bundle_document().unwrap())
            .unwrap_err();
        assert!(
            matches!(err, AutomergeError::DuplicateSeqNumber(1, _)),
            "{err}"
        );
    }

    #[test]
    fn an_audited_apply_rejects_a_dep_id_naming_another_change() {
        let mut base = audited(b"aaa");
        put(&mut base, 1);
        put(&mut base, 2);
        let mut src = base.fork().with_actor(crate::ActorId::from(&b"bbb"[..]));
        put(&mut src, 3);
        let at = |seq| {
            crate::ChangeId::from_parts(
                crate::ActorId::from(&b"aaa"[..]),
                NonZeroU64::new(seq).unwrap(),
            )
        };
        let mut cs = src.bundle_after(&[at(2)]).unwrap().unwrap();
        assert_eq!(cs.dep_ids, vec![at(2)]);
        cs.dep_ids[0] = at(1);
        for (_, id) in &mut cs.boundary {
            *id = at(1);
        }
        cs.audit::<ChangeHash>().unwrap();
        let err = base.apply_bundle(cs).unwrap_err();
        assert!(
            matches!(
                err,
                AutomergeError::MalformedBundle(MalformedBundle::Invalid(_))
            ),
            "{err}"
        );
        assert_eq!(base.get_changes(&[]).unwrap().len(), 2);
    }

    fn delivers_queued_changes<H: crate::hash_retention::HashRetention>(
        mut doc: crate::automerge::Automerge<H>,
    ) {
        let mut src = audited(b"src");
        for i in 0..3 {
            put(&mut src, i);
        }
        let changes = src.get_changes(&[]).unwrap();
        doc.apply_changes(changes[1..].to_vec()).unwrap();
        assert!(!doc.queue.is_empty());
        doc.apply_bundle(src.bundle_document().unwrap()).unwrap();
        assert!(doc.queue.is_empty());
        doc.apply_changes(changes).unwrap();
        assert_eq!(doc.get_heads(), src.get_heads());
    }

    #[test]
    fn a_bundle_delivers_queued_changes() {
        delivers_queued_changes(audited(b"rcv"));
        delivers_queued_changes(
            crate::automerge::Automerge::<crate::hash_retention::Retained>::new(),
        );
    }

    fn audit_fixture() -> crate::automerge::Automerge<crate::hash_retention::Full> {
        use crate::automerge::Automerge;
        use crate::tx::{CommitOptions, Transactable};
        use crate::ROOT;

        // pinned actors and timestamps fix which commits are fragment heads
        let mut doc = Automerge::new()
            .with_actor(crate::ActorId::from(&b"zsn"[..]))
            .enable_audit_mode()
            .unwrap();
        let commit = |doc: &mut Automerge<_>, k: &str, i: i64| {
            let mut tx = doc.transaction();
            tx.put(ROOT, k, i).unwrap();
            tx.commit_with(CommitOptions::default().with_time(0));
        };
        for i in 0..300 {
            commit(&mut doc, "k", i);
        }
        let mut fork = doc.fork().with_actor(crate::ActorId::from(&b"other"[..]));
        for i in 0..4 {
            commit(&mut doc, "k", 10 + i);
            commit(&mut fork, "j", 20 + i);
        }
        doc.merge(&mut fork).unwrap();
        commit(&mut doc, "k", 30);
        doc
    }

    fn first_change() -> crate::ChangeId {
        crate::ChangeId::from_parts(
            crate::ActorId::from(&b"zsn"[..]),
            NonZeroU64::new(1).unwrap(),
        )
    }

    fn assert_audits(cs: &Bundle) {
        let audited = cs
            .audit::<Change>()
            .unwrap_or_else(|e| panic!("audit failed: {e}"));
        let decoded = cs.to_changes().unwrap();
        let hashes = decoded.iter().map(|c| c.hash()).collect::<Vec<_>>();
        assert_eq!(audited.iter().map(|c| c.hash()).collect::<Vec<_>>(), hashes);
        assert_eq!(cs.audit::<ChangeHash>().unwrap(), hashes);
    }

    #[test]
    fn bundles_from_every_builder_pass_audit() {
        let doc = audit_fixture();
        let frags = doc.fragments(..);
        assert!(frags.len() > 1, "fixture needs several fragments");

        assert_audits(&doc.bundle_document().unwrap());
        for f in &frags {
            assert_audits(&doc.bundle_for_fragment(f).unwrap());
            assert_audits(&doc.make_bundle_from_fragment(f).unwrap());
        }
        for cs in doc.bundles_for_fragments(frags.clone()).unwrap() {
            assert_audits(&Bundle::try_from(&cs[..]).unwrap());
        }
        assert_audits(&doc.bundle_after(&[first_change()]).unwrap().unwrap());

        let retained = crate::automerge::Automerge::load(&doc.save()).unwrap();
        assert_audits(&retained.bundle_document().unwrap());
        for f in &retained.fragments(..) {
            assert_audits(&retained.make_bundle_from_fragment(f).unwrap());
        }
    }

    #[test]
    fn audit_rejects_a_bundle_that_misdescribes_its_changes() {
        let doc = audit_fixture();
        let good = doc.bundle_after(&[first_change()]).unwrap().unwrap();
        assert_audits(&good);
        assert!(!good.boundary.is_empty(), "fixture needs a boundary");
        assert!(!good.checkpoints.is_empty(), "fixture needs a checkpoint");
        let changes = good.to_changes().unwrap();
        macro_rules! rejects {
            ($cs:expr, $why:literal, $err:pat) => {
                let err = $cs
                    .audit::<Change>()
                    .expect_err(concat!("audit should reject ", $why));
                assert!(matches!(err, $err), "{}: got {err}", $why);
                let err = $cs
                    .audit::<ChangeHash>()
                    .expect_err(concat!("audit should reject ", $why));
                assert!(matches!(err, $err), "{}: got {err}", $why);
            };
        }

        let mut cs = good.clone();
        cs.heads[0].0 = changes[0].hash();
        rejects!(
            cs,
            "a head hash that names another member",
            MalformedBundle::WrongHeadHash { .. }
        );

        let mut cs = good.clone();
        cs.heads[0].1 = 0;
        cs.heads[0].0 = changes[0].hash();
        rejects!(
            cs,
            "a head that other members depend on",
            MalformedBundle::WrongHeads
        );

        let mut cs = good.clone();
        cs.heads.push((changes[0].hash(), 0));
        rejects!(cs, "an extra head", MalformedBundle::WrongHeads);

        let mut cs = good.clone();
        cs.retained.push((0, changes[1].hash()));
        rejects!(
            cs,
            "a retained hash that names another member",
            MalformedBundle::WrongRetainedHash { .. }
        );

        let mut cs = good.clone();
        cs.checkpoints.push((0, changes[0].hash()));
        rejects!(
            cs,
            "a checkpoint that is not fragment-level",
            MalformedBundle::WrongCheckpoints
        );

        let mut cs = good.clone();
        cs.checkpoints.pop();
        rejects!(
            cs,
            "a missing checkpoint",
            MalformedBundle::WrongCheckpoints
        );

        let mut cs = good.clone();
        cs.boundary
            .push((changes[0].hash(), cs.boundary[0].1.clone()));
        rejects!(
            cs,
            "a boundary entry that names a member",
            MalformedBundle::BoundaryIsMember(_)
        );

        let mut cs = good.clone();
        let member = changes[0].clone();
        cs.boundary[0].1 = crate::ChangeId::from_parts(
            member.actor_id().clone(),
            NonZeroU64::new(member.seq()).unwrap(),
        );
        rejects!(
            cs,
            "a boundary id that names a member",
            MalformedBundle::ExternalIdIsMember(_)
        );

        let mut cs = good.clone();
        let other = cs.dep_ids[0].clone();
        cs.dep_ids[0] = crate::ChangeId::from_parts(
            other.actor().clone(),
            NonZeroU64::new(other.seq() + 1000).unwrap(),
        );
        rejects!(
            cs,
            "a dep id that disagrees with the boundary",
            MalformedBundle::BoundaryDisagreesWithDep(_)
        );
    }

    fn with_recomputed_checksum(bytes: &[u8]) -> Vec<u8> {
        let (_, header) =
            Header::parse::<crate::storage::chunk::error::Header>(parse::Input::new(bytes))
                .unwrap();
        let data = &bytes[header.data_bytes()];
        let header = Header::new(ChunkType::Bundle, data);
        let mut out = Vec::new();
        header.write(&mut out);
        out.extend(data);
        out
    }

    #[test]
    fn a_bad_checksum_is_rejected() {
        let mut bytes = audit_fixture().save();
        assert_eq!(bytes[8], u8::from(ChunkType::Bundle));
        bytes[4] ^= 0xff;

        let err = Bundle::try_from(&bytes[..]).expect_err("try_from should reject it");
        assert!(err.0.contains("checksum"), "got {err}");
        assert!(matches!(
            crate::automerge::Automerge::load(&bytes),
            Err(AutomergeError::Load(
                crate::storage::load::Error::BadChecksum
            ))
        ));
        assert!(matches!(
            crate::audit::Automerge::load(&bytes),
            Err(AutomergeError::Load(
                crate::storage::load::Error::BadChecksum
            ))
        ));
    }

    #[test]
    fn an_audit_load_rejects_a_bundle_that_fails_audit() {
        let doc = audit_fixture();
        let mut cs = Bundle::try_from(&doc.save()[..]).unwrap();
        cs.heads[0].0 = cs.to_changes().unwrap()[0].hash();

        let err = crate::audit::Automerge::load(&cs.bytes()).expect_err("audit load should fail");
        assert!(
            matches!(
                err,
                AutomergeError::MalformedBundle(MalformedBundle::WrongHeadHash { .. })
            ),
            "got {err}"
        );
    }
}
