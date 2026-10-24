use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Add;
use std::ops::{Range, RangeBounds};

use crate::actor::{ActorIndexed, ActorRefs, ActorRemoval, ActorShift, ActorTable};
use crate::author::Authors;
use crate::change_id::ChangeId;
use crate::hash_retention::{Full, HashRetention, Retained};
use crate::types::SmallHashMap;
use crate::{
    clock::{Clock, SeqClock},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorIdx, ValueMeta},
    storage::columns::compression::Uncompressed,
    storage::columns::BadColumnLayout,
    storage::document::ReconstructError as LoadError,
    storage::{Columns, Document, RawColumn, RawColumns},
    types::OpId,
    Change, ChangeHash, MalformedBundle,
};

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache
#[derive(Debug, Clone)]
pub struct ChangeGraph<H: HashRetention = Retained> {
    hashes: H,
    actors: ActorRefs<Vec<ActorIdx>>,
    /// `dep_range[n]` is the `(offset, count)` of node `n`'s parents within
    /// `dep_target`. Plain `Vec`s, not hexane columns: `parents()` is hot in
    /// the clock-cache ancestry walks.
    dep_range: Vec<(u32, u32)>,
    dep_target: Vec<NodeIdx>,
    seq: Vec<u32>,
    /// A mapping from a [`NodeIdx`] to the maximum op-counter in a given
    /// commit.
    max_ops: Vec<u32>,
    max_op: u32,
    num_ops: hexane::Column<u64>,
    timestamps: hexane::DeltaColumn<i64>,
    messages: hexane::Column<Option<String>>,
    extra_bytes_meta: hexane::PrefixColumn<ValueMeta>,
    extra_bytes_raw: Vec<u8>,
    heads: BTreeSet<ChangeHash>,
    nodes_by_hash: SmallHashMap<ChangeHash, NodeIdx>,
    clock_cache: SmallHashMap<NodeIdx, SeqClock>,
    seq_index: ActorIndexed<Vec<NodeIdx>>,
    /// Each actor's level > 0 changes in seq order: where fragment
    /// boundaries sit, whether or not a fragment still heads at them.
    leveled: ActorIndexed<Vec<NodeIdx>>,
    fragment_top: SeqClock,
    fragments: Vec<FragmentNode>,
    gc_mode: crate::automerge::GcMode,
    gc_owed: bool,
    pins: Pins,
}

#[derive(Debug)]
pub(crate) struct ChangeGraphCols {
    graph: ChangeGraph,
}

const CACHE_STEP: u32 = 16;

/// Sorted, distinct nodes, and the order a bundle lists them in.
#[derive(Debug)]
pub struct Members<'a> {
    nodes: &'a [NodeIdx],
    contiguous: bool,
    /// the bundle order, and each sorted index's place in it; `None` when
    /// that is `nodes` itself
    order: Option<(Vec<NodeIdx>, Vec<u32>)>,
}

impl<'a> Members<'a> {
    /// Listed in node order.
    pub(crate) fn new(nodes: &'a [NodeIdx]) -> Self {
        debug_assert!(
            nodes.windows(2).all(|w| w[0] < w[1]),
            "members must be sorted and distinct"
        );
        let contiguous = match (nodes.first(), nodes.last()) {
            (Some(first), Some(last)) => (last.0 - first.0) as usize + 1 == nodes.len(),
            _ => true,
        };
        Members {
            nodes,
            contiguous,
            order: None,
        }
    }

    pub(crate) fn nodes(&self) -> &'a [NodeIdx] {
        self.nodes
    }

    /// The members in bundle order.
    pub(crate) fn in_order(&self) -> &[NodeIdx] {
        match &self.order {
            Some((order, _)) => order,
            None => self.nodes,
        }
    }

    /// `n`'s place in bundle order.
    pub(crate) fn position(&self, n: NodeIdx) -> Option<usize> {
        let i = self.sorted_index(n)?;
        Some(match &self.order {
            Some((_, rank)) => rank[i] as usize,
            None => i,
        })
    }

    fn sorted_index(&self, n: NodeIdx) -> Option<usize> {
        if self.contiguous {
            let i = n.0.checked_sub(self.nodes.first()?.0)? as usize;
            (i < self.nodes.len()).then_some(i)
        } else {
            self.nodes.binary_search(&n).ok()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyLookup {
    Present,
    Absent,
    /// A different change already occupies the change's `(actor, seq)`
    Equivocation,
}

#[derive(Debug, thiserror::Error)]
#[error("this operation needs change hashes that are not retained, call enable_audit_mode() first")]
pub struct UncheckedHashes;

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphLoadError {
    #[error("actor {actor} has an author footer at seq {seq}")]
    AuthorOnNonInitialSeq { seq: u64, actor: usize },
}

impl From<UncheckedHashes> for AutomergeError {
    fn from(_: UncheckedHashes) -> Self {
        AutomergeError::AuditModeRequired
    }
}

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeIdx(pub(crate) u32);

impl Add<usize> for NodeIdx {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        NodeIdx(self.0 + other as u32)
    }
}

/// An elided column decodes as empty and stands for a column of defaults.
fn pad<T: Clone>(iter: impl Iterator<Item = T>, default: T, n: usize) -> impl Iterator<Item = T> {
    iter.chain(std::iter::repeat(default)).take(n)
}

const NO_EXTRA: ValueMeta = ValueMeta::bytes(0);

#[derive(Debug, Clone)]
pub(crate) struct BundleMember<'a> {
    /// The member's actor as a document actor index
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) num_ops: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) deps: Vec<BundleDep>,
}

/// `Member` is a position in the bundle's (topologically ordered) member list.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BundleDep {
    Member(usize),
    Node(NodeIdx),
}

impl<H: HashRetention> ChangeGraph<H> {
    pub(crate) fn new(actors: &ActorTable) -> Self {
        Self {
            gc_mode: crate::automerge::GcMode::default(),
            gc_owed: false,
            pins: Pins::default(),
            nodes_by_hash: SmallHashMap::default(),
            hashes: H::default(),
            actors: ActorRefs::default(),
            max_ops: Vec::new(),
            max_op: 0,
            num_ops: hexane::Column::new(),
            seq: Vec::new(),
            dep_range: Vec::new(),
            dep_target: Vec::new(),
            messages: hexane::Column::new(),
            timestamps: hexane::DeltaColumn::new(),
            extra_bytes_meta: hexane::PrefixColumn::new(),
            extra_bytes_raw: Vec::new(),
            heads: BTreeSet::new(),
            clock_cache: SmallHashMap::default(),
            seq_index: ActorIndexed::new(actors),
            leveled: ActorIndexed::new(actors),
            fragments: vec![],
            fragment_top: SeqClock::new(actors),
        }
    }

    pub(crate) fn all_actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index.iter().enumerate().map(|(i, _)| i)
    }

    pub(crate) fn actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if !v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn unused_actors(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.heads.iter().cloned()
    }

    /// In the same order as [`Self::heads`].
    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.heads.iter().map(|h| {
            self.nodes_by_hash
                .get(h)
                .expect("every head has a known node")
                .0 as u64
        })
    }

    pub(crate) fn empty_clock(&self) -> SeqClock {
        self.fragment_top.empty_like()
    }

    /// Ascending, which is topological order.
    pub(crate) fn all_nodes(&self) -> Vec<NodeIdx> {
        (0..self.len() as u32).map(NodeIdx).collect()
    }

    /// Make room for an actor just inserted into the document's actor table.
    pub(crate) fn insert_actor(&mut self, shift: &ActorShift) {
        self.actors.shift_actors(shift);

        for clock in self.clock_cache.values_mut() {
            clock.shift_actor(shift)
        }
        for f in &mut self.fragments {
            f.clock.shift_actor(shift)
        }
        self.fragment_top.shift_actor(shift);
        self.seq_index.insert(shift, vec![]);
        self.leveled.insert(shift, vec![]);
    }

    /// The actor must have no changes in the graph.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) {
        self.actors
            .remove_actor(removal)
            .expect("removed actor still has changes in the graph");
        let removed = self.seq_index.remove(removal);
        assert!(removed.is_empty());
        self.leveled.remove(removal);
        for clock in self.clock_cache.values_mut() {
            clock.remove_actor(removal)
        }
        for fragment in &mut self.fragments {
            fragment.clock.remove_actor(removal)
        }
        self.fragment_top.remove_actor(removal);
    }

    pub(crate) fn len(&self) -> usize {
        self.actors.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    pub(crate) fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<ChangeHash> {
        self.hashes.get(NodeIdx(index as u32))
    }

    pub(crate) fn try_index_to_hash(&self, index: usize) -> Result<ChangeHash, UncheckedHashes> {
        self.hashes.try_get(NodeIdx(index as u32))
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.max_op as u64
    }

    pub(crate) fn max_op_for_actor(&self, actor_index: usize) -> u64 {
        self.seq_index
            .get(actor_index)
            .and_then(|s| s.last())
            .and_then(|index| self.max_ops.get(index.0 as usize).cloned())
            .unwrap_or(0) as u64
    }

    pub(crate) fn seq_for_actor(&self, actor: usize) -> u64 {
        self.seq_index
            .get(actor)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
    }

    pub(crate) fn current_clock(&self) -> Clock {
        self.fragment_top
            .clock_for_each_actor(|a| self.max_op_for_actor(a) as u32)
    }

    pub(crate) fn current_seq_clock(&self) -> SeqClock {
        let mut clock = self.empty_clock();
        for (a, seqs) in self.seq_index.iter().enumerate() {
            clock.include(a, u32::try_from(seqs.len()).ok().filter(|n| *n > 0));
        }
        clock
    }

    fn deps_iter(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.node_ids().flat_map(|n| self.parents(n))
    }

    fn num_deps(&self) -> impl Iterator<Item = usize> + '_ {
        self.node_ids().map(|n| self.parents(n).count())
    }

    fn node_ids(&self) -> impl Iterator<Item = NodeIdx> {
        let end = self.len() as u32;
        (0..end).map(NodeIdx)
    }

    fn is_covered_by(&self, n: NodeIdx, frontier: &SeqClock) -> bool {
        let i = n.0 as usize;
        let actor = usize::from(self.actors[i]);
        frontier.get_for_actor(&actor) >= NonZeroU32::new(self.seq[i])
    }

    #[cfg(test)]
    pub(crate) fn pinned_without_hash(&self) -> Vec<(NodeIdx, usize, u32)> {
        self.pins
            .count
            .keys()
            .filter(|n| self.hashes.get(**n).is_none())
            .map(|n| {
                (
                    *n,
                    usize::from(self.actors[n.0 as usize]),
                    self.seq[n.0 as usize],
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn held_hashes(&self) -> BTreeSet<ChangeHash> {
        self.hashes.iter().map(|(_, h)| h).collect()
    }

    /// Each actor's nodes above `frontier`.
    fn uncovered_tails<'a>(
        &'a self,
        frontier: &'a SeqClock,
    ) -> impl Iterator<Item = &'a [NodeIdx]> + 'a {
        self.seq_index.iter().enumerate().map(|(a, seqs)| {
            let covered = covered_seq(frontier, a).min(seqs.len());
            &seqs[covered..]
        })
    }

    /// May include anchors and actor tips outside `candidates`.
    fn retained_from(
        &self,
        candidates: impl Iterator<Item = NodeIdx>,
        frontier: &SeqClock,
    ) -> BTreeSet<NodeIdx> {
        let mut keep = BTreeSet::new();
        for n in candidates {
            if self.fragment_level(n) > 0 {
                keep.insert(n);
            } else if !self.is_covered_by(n, frontier) {
                keep.insert(n);
                keep.extend(self.anchors_of(n, frontier));
            }
        }
        // committing as an actor names its latest change by hash
        keep.extend(self.seq_index.iter().filter_map(|c| c.last()).copied());
        keep
    }

    /// The covered level-0 parents of loose commit `n`, which its future
    /// fragment boundary names.
    fn anchors_of<'a>(
        &'a self,
        n: NodeIdx,
        frontier: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a {
        self.parents(n)
            .filter(|p| self.is_covered_by(*p, frontier) && self.fragment_level(*p) == 0)
    }

    /// 0 when the hash was freed, which never happens to a fragment head.
    fn fragment_level(&self, node: NodeIdx) -> usize {
        self.hashes.get(node).map_or(0, |h| h.fragment_level())
    }

    /// The hashes among `nodes` a receiver of them must retain, each paired
    /// with its position in `nodes`: the ones a document without audit mode
    /// would hold. With
    /// [`NameHashes::UntagTiny`](crate::automerge::NameHashes::UntagTiny)
    /// and few enough `nodes` to rehash on load, tiny level-0 changes are
    /// left out.
    pub(crate) fn hashes_to_retain(
        &self,
        members: &Members<'_>,
        names: crate::automerge::NameHashes,
    ) -> Result<ReceiverHashes, UnretainedHashes> {
        use crate::storage::bundle::{rehashable, REHASHABLE_CHANGES, TINY_CHANGE_OPS};
        let nodes = members.nodes();
        let num_ops = |n: &NodeIdx| self.num_ops.get(n.0 as usize).unwrap_or_default();
        let untag_tiny = names == crate::automerge::NameHashes::UntagTiny
            && nodes.len() <= REHASHABLE_CHANGES
            && rehashable(nodes.len(), nodes.iter().map(num_ops).sum());
        let rehashed_on_load = |n: &NodeIdx, h: Option<ChangeHash>| {
            untag_tiny && h.is_none_or(|h| h.fragment_level() == 0) && num_ops(n) <= TINY_CHANGE_OPS
        };
        let carried: Vec<NodeIdx> = self.leveled_members(members).map(|(n, _)| n).collect();
        let frontier = self.calculate_clock(carried.clone());
        let loose = self.loose_members(members, &frontier);
        let mut required = self.retained_from(carried.into_iter().chain(loose), &frontier);
        required.extend(self.member_tips(nodes));
        // a receiver pins these when it caches the fragment, and may have
        // freed those outside the bundle: a concurrent fragment can cover
        // them first
        let mut outside = BTreeSet::new();
        for f in &self.fragments {
            if members.position(f.head).is_some() {
                for p in self.fragment_pins(f) {
                    if members.position(p).is_some() {
                        required.insert(p);
                    } else {
                        outside.insert(p);
                    }
                }
            }
        }
        let outside = outside
            .into_iter()
            .map(|n| self.hashes.get(n))
            .collect::<Option<Vec<_>>>()
            .ok_or(UnretainedHashes {
                count: 1,
                what: "changes outside the bundle its fragments need",
            })?;
        required.retain(|n| members.position(*n).is_some());
        let unheld = required
            .iter()
            .filter(|n| self.hashes.get(**n).is_none() && !rehashed_on_load(n, None))
            .count();
        if unheld > 0 {
            return Err(UnretainedHashes {
                count: unheld,
                what: "changes its receiver must retain",
            });
        }
        let mut named: Vec<(usize, ChangeHash)> = required
            .into_iter()
            .filter_map(|n| Some((members.position(n)?, n, self.hashes.get(n)?)))
            .filter(|(_, n, h)| !rehashed_on_load(n, Some(*h)))
            .map(|(i, _, h)| (i, h))
            .collect();
        named.sort_unstable();
        Ok(ReceiverHashes {
            retained: named,
            outside,
        })
    }

    /// Each actor's last change among `nodes`: the tip of a receiver that
    /// holds no later change of that actor.
    fn member_tips(&self, nodes: &[NodeIdx]) -> impl Iterator<Item = NodeIdx> {
        let mut last: Vec<Option<NodeIdx>> = Vec::new();
        for n in nodes {
            let actor = usize::from(self.actors[n.0 as usize]);
            if actor >= last.len() {
                last.resize(actor + 1, None);
            }
            last[actor] = Some(*n);
        }
        last.into_iter().flatten()
    }

    /// `nodes` (sorted) in the order every peer holding them lists them,
    /// whatever order they arrived in: parents first; then a change's
    /// successor by its own actor when emitting it freed one; otherwise
    /// the least ready `(actor, seq)`. O(nodes) when only one order is
    /// causal, O(nodes · log width) otherwise.
    pub(crate) fn bundle_members<'a>(&self, nodes: &'a [NodeIdx]) -> Members<'a> {
        let mut members = Members::new(nodes);
        let linear = nodes
            .windows(2)
            .all(|w| self.parent_slice(w[1]).contains(&w[0]));
        if linear {
            return members;
        }
        let m = nodes.len();
        let mut waiting = vec![0u32; m];
        let mut child_start = vec![0u32; m + 1];
        for (i, n) in nodes.iter().enumerate() {
            for p in self.parent_slice(*n) {
                if let Some(j) = members.sorted_index(*p) {
                    waiting[i] += 1;
                    child_start[j + 1] += 1;
                }
            }
        }
        for j in 0..m {
            child_start[j + 1] += child_start[j];
        }
        let mut fill = child_start.clone();
        let mut children = vec![0u32; child_start[m] as usize];
        for (i, n) in nodes.iter().enumerate() {
            for p in self.parent_slice(*n) {
                if let Some(j) = members.sorted_index(*p) {
                    children[fill[j] as usize] = i as u32;
                    fill[j] += 1;
                }
            }
        }
        let key = |i: u32| {
            let n = nodes[i as usize].0 as usize;
            (self.actors[n], self.seq[n], i)
        };
        let mut ready: std::collections::BinaryHeap<_> = (0..m as u32)
            .filter(|i| waiting[*i as usize] == 0)
            .map(|i| std::cmp::Reverse(key(i)))
            .collect();
        let mut order = Vec::with_capacity(m);
        let mut rank = vec![0u32; m];
        let mut next = None;
        while let Some(i) = next.take().or_else(|| ready.pop().map(|r| r.0 .2)) {
            rank[i as usize] = order.len() as u32;
            order.push(nodes[i as usize]);
            let (actor, ..) = key(i);
            for c in
                &children[child_start[i as usize] as usize..child_start[i as usize + 1] as usize]
            {
                waiting[*c as usize] -= 1;
                if waiting[*c as usize] > 0 {
                    continue;
                }
                let k = key(*c);
                match next {
                    _ if k.0 != actor => ready.push(std::cmp::Reverse(k)),
                    Some(n) if key(n) < k => ready.push(std::cmp::Reverse(k)),
                    Some(n) => {
                        ready.push(std::cmp::Reverse(key(n)));
                        next = Some(*c);
                    }
                    None => next = Some(*c),
                }
            }
        }
        debug_assert_eq!(order.len(), m, "members hold a cycle");
        if order.as_slice() != nodes {
            members.order = Some((order, rank));
        }
        members
    }

    /// Each node's actor, through `actor_index`, and seq.
    pub(crate) fn actors_and_seqs(
        &self,
        members: &Members<'_>,
        actor_index: &[ActorIdx],
    ) -> (Vec<ActorIdx>, Vec<std::num::NonZeroU64>) {
        members
            .in_order()
            .iter()
            .map(|n| {
                let i = n.0 as usize;
                let seq = std::num::NonZeroU64::new(self.seq[i] as u64)
                    .expect("a change's seq is at least 1");
                (actor_index[usize::from(self.actors[i])], seq)
            })
            .unzip()
    }

    fn loose_members(&self, members: &Members<'_>, frontier: &SeqClock) -> Vec<NodeIdx> {
        let tails: Vec<&[NodeIdx]> = self.uncovered_tails(frontier).collect();
        if tails.iter().map(|t| t.len()).sum::<usize>() <= members.nodes().len() {
            tails
                .into_iter()
                .flatten()
                .copied()
                .filter(|n| members.position(*n).is_some())
                .collect()
        } else {
            members
                .nodes()
                .iter()
                .copied()
                .filter(|n| !self.is_covered_by(*n, frontier))
                .collect()
        }
    }

    /// Fragment heads and checkpoints among `members`, in node order.
    pub(crate) fn leveled_members<'a>(
        &'a self,
        members: &'a Members<'a>,
    ) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + 'a {
        self.hashes.leveled_members(members).into_iter()
    }

    /// Whether `n` is its actor's latest change: committing as that actor
    /// names it by hash.
    fn is_actor_tip(&self, n: NodeIdx) -> bool {
        self.seq_index[usize::from(self.actors[n.0 as usize])].last() == Some(&n)
    }

    /// Appends `node` as `actor`'s tip, which must keep its hash; the old
    /// tip's hash may now be freed.
    fn push_tip(&mut self, actor: usize, seq: u64, node: NodeIdx) {
        assert!(actor < self.seq_index.len());
        assert_eq!(self.seq_index[actor].len() + 1, seq as usize);
        if Self::tracks_pins() {
            if let Some(old) = self.seq_index[actor].last() {
                self.pins.candidates.push(*old);
            }
            self.pins.fresh.push(node);
        }
        self.seq_index.as_mut_slice()[actor].push(node);
    }

    /// Removes the head `hash` of `node`, whose hash may now be freed.
    fn demote_head(&mut self, node: NodeIdx, hash: &ChangeHash) {
        if self.heads.remove(hash) && Self::tracks_pins() {
            self.pins.candidates.push(node);
        }
    }

    /// Whether the hash of `n` (if held) must stay.
    fn must_keep(&self, n: NodeIdx, hash: &ChangeHash) -> bool {
        self.pins.is_pinned(n)
            || hash.fragment_level() > 0
            || self.is_actor_tip(n)
            || self.heads.contains(hash)
    }

    /// The hashes this document must retain but lacks, among the nodes
    /// pinned or made actor tips since the last check.
    pub(crate) fn missing_retained_hashes(&mut self) -> Vec<NodeIdx> {
        if !Self::tracks_pins() {
            return Vec::new();
        }
        debug_assert!(self.pins.deferred_from.is_none(), "loose pins deferred");
        let mut missing = std::mem::take(&mut self.pins.fresh);
        missing.retain(|n| {
            (self.pins.is_pinned(*n) || self.is_actor_tip(*n)) && self.hashes.get(*n).is_none()
        });
        missing.sort_unstable();
        missing.dedup();
        missing
    }

    /// Every pin count, recomputed from the holders. Panics on a mismatch,
    /// naming the first wrong node.
    #[cfg(feature = "slow_path_assertions")]
    fn verify_pins(&self) {
        let mut expect: SmallHashMap<NodeIdx, u32> = SmallHashMap::default();
        for p in self.pin_holders() {
            *expect.entry(p).or_default() += 1;
        }
        for (n, c) in &expect {
            assert_eq!(
                self.pins.count.get(n).map(|c| c.get()),
                Some(*c),
                "pin count of {n:?}"
            );
        }
        assert_eq!(expect.len(), self.pins.count.len(), "stray pins");
    }

    /// Every pin count from scratch, for a graph that wasn't tracking them.
    fn rebuild_pins(&mut self) {
        self.pins = Pins::default();
        for p in self.pin_holders() {
            self.pins.pin(p);
        }
    }

    /// One entry per pin a holder takes: the fragments', then the loose
    /// nodes'.
    fn pin_holders(&self) -> Vec<NodeIdx> {
        let mut pins: Vec<NodeIdx> = self
            .fragments
            .iter()
            .flat_map(|f| self.fragment_pins(f))
            .collect();
        for n in self.uncovered_tails(&self.fragment_top).flatten() {
            pins.push(*n);
            pins.extend(self.parents(*n));
        }
        pins
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> RawColumns<Uncompressed> {
        use hexane::EncoderApi;
        use ids::*;

        let actor = hexane::Encoder::<ActorIdx>::encode_to(out, self.actors.iter().copied());
        let seq =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.seq.iter().map(|s| *s as usize));
        let max_op =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.max_ops.iter().map(|m| *m as usize));
        let time_start = out.len();
        out.extend_from_slice(&self.timestamps.save());
        let time = time_start..out.len();
        let message = self.messages.save_to_unless(out, None);

        let num_deps = hexane::Encoder::<usize>::encode_to(out, self.num_deps());
        let deps =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.deps_iter().map(|n| n.0 as usize));

        // FIXME - we could eliminate this column if empty but meta isnt all null
        let meta = self.extra_bytes_meta.save_to(out);
        let raw = out.len()..out.len() + self.extra_bytes_raw.len();
        out.extend(&self.extra_bytes_raw);

        let cols = vec![
            RawColumn::new(ACTOR_COL_SPEC, actor),
            RawColumn::new(SEQ_COL_SPEC, seq),
            RawColumn::new(MAX_OP_COL_SPEC, max_op),
            RawColumn::new(TIME_COL_SPEC, time),
            RawColumn::new(MESSAGE_COL_SPEC, message),
            RawColumn::new(DEPS_COUNT_COL_SPEC, num_deps),
            RawColumn::new(DEPS_VAL_COL_SPEC, deps),
            RawColumn::new(EXTRA_META_COL_SPEC, meta),
            RawColumn::new(EXTRA_VAL_COL_SPEC, raw),
        ];

        cols.into_iter().collect()
    }

    pub(crate) fn validate(
        bytes: usize,
        cols: &RawColumns<Uncompressed>,
    ) -> Result<RawColumns<Uncompressed>, BadColumnLayout> {
        use ids::*;
        let _ = Columns::parse2(bytes, cols.iter())?;
        Ok(cols
            .iter()
            .filter(|col| {
                matches!(
                    col.spec(),
                    ACTOR_COL_SPEC
                        | SEQ_COL_SPEC
                        | MAX_OP_COL_SPEC
                        | TIME_COL_SPEC
                        | MESSAGE_COL_SPEC
                        | DEPS_COUNT_COL_SPEC
                        | DEPS_VAL_COL_SPEC
                        | EXTRA_META_COL_SPEC
                        | EXTRA_VAL_COL_SPEC
                )
            })
            .cloned()
            .collect())
    }

    fn opid_to_node(&self, id: OpId) -> Option<NodeIdx> {
        let actor_indices = self.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = self.num_ops.get(i).unwrap_or_default();
                let max_op = self.max_ops[i];
                let start = max_op as u64 - num_ops + 1;
                if counter < start {
                    Ordering::Greater
                } else if (max_op as u64) < counter {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
            .ok()?;
        Some(actor_indices[index])
    }

    /// `Err` when the graph is unchecked, so it can't tell whether the hash
    /// is a change.
    fn lookup_hash(&self, hash: &ChangeHash) -> Result<Option<NodeIdx>, UncheckedHashes> {
        match self.nodes_by_hash.get(hash) {
            Some(n) => Ok(Some(*n)),
            None if self.hashes.has_every_hash() => Ok(None),
            None => Err(UncheckedHashes),
        }
    }

    /// Like [`Self::has_change`], but an unknown hash is resolved through the
    /// change's `(actor, seq)`: a covered slot whose hash was freed is
    /// trusted to be this change.
    pub(crate) fn lookup_change_for_apply(
        &self,
        hash: &ChangeHash,
        id: &ChangeId,
        actors: &ActorTable,
    ) -> ApplyLookup {
        if self.nodes_by_hash.contains_key(hash) {
            return ApplyLookup::Present;
        }
        let Some(node) = self.node_for_change_id(id, actors) else {
            return ApplyLookup::Absent;
        };
        match self.hashes.get(node) {
            Some(_) => ApplyLookup::Equivocation,
            None => ApplyLookup::Present,
        }
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> Result<bool, UncheckedHashes> {
        Ok(self.lookup_hash(hash)?.is_some())
    }

    /// An author footer is only valid on an actor's first change: skipping a
    /// later one would leave its ops unattributed, so masking could not hide
    /// them.
    fn assign_authors(
        &self,
        nodes: Range<usize>,
        authors: &mut Authors,
    ) -> Result<(), GraphLoadError> {
        // a prefix column's `get` walks from the start: iterate to stay linear
        for (meta, i) in self.extra_bytes_meta.iter_range(nodes.clone()).zip(nodes) {
            let extra = &self.extra_bytes_raw[meta.prefix() as usize..meta.total() as usize];
            let Some(author) = crate::change::decode_author_footer(extra) else {
                continue;
            };
            let actor = self.actors[i].into();
            if self.seq[i] != 1 {
                return Err(GraphLoadError::AuthorOnNonInitialSeq {
                    seq: self.seq[i] as u64,
                    actor,
                });
            }
            authors.assign_author(author.into_owned(), actor);
        }
        Ok(())
    }

    /// The authors `members` (`(actor index, seq, extra bytes)`) assign.
    /// Errors if a member names a second author for an actor.
    fn member_authors<'b>(
        members: impl Iterator<Item = (usize, u64, &'b [u8])>,
        authors: &Authors,
        actor_ids: &ActorTable,
    ) -> Result<Vec<(usize, crate::Author<'b>)>, AutomergeError> {
        let mut claimed = HashSet::new();
        let mut assigned = Vec::new();
        for (actor, seq, extra) in members {
            if extra.is_empty() {
                continue;
            }
            let Some(author) = crate::change::decode_author_footer(extra) else {
                continue;
            };
            let existing = authors.get_author_for_actor(actor);
            if existing.is_some_and(|a| a != author) || !claimed.insert(actor) {
                return Err(AutomergeError::DuplicateAuthor(
                    author.into_owned(),
                    actor_ids[actor].clone(),
                    seq,
                ));
            }
            if seq != 1 {
                return Err(AutomergeError::AuthorOnNonInitialSeq(
                    seq,
                    actor_ids[actor].clone(),
                ));
            }
            assigned.push((actor, author));
        }
        Ok(assigned)
    }

    /// A dep outside `nodes` is written by hash, so its hash must be retained.
    pub(crate) fn write_bundle_changes<'a>(
        &'a self,
        members: &Members<'_>,
        writer: &mut crate::storage::BundleChangeWriter<'a>,
        mapper: &mut crate::op_set2::change::ActorMapper<'_>,
    ) -> Result<(), UnretainedHashes> {
        use crate::storage::DeltaRunGrouper;
        let (mut seq, mut max_op, mut deps) =
            <(DeltaRunGrouper, DeltaRunGrouper, DeltaRunGrouper)>::default();
        let mut ranges = node_ranges(members.in_order());
        let span = match (members.nodes().first(), members.nodes().last()) {
            (Some(lo), Some(hi)) => (hi.0 - lo.0) as usize + 1,
            _ => 0,
        };
        // a run's seeks cost about as much as decoding this many rows
        const SEEK_ROWS: usize = 32;
        let mut unnamed = 0;
        if ranges.len() > 1 && ranges.len() * SEEK_ROWS >= span {
            unnamed = self.write_gathered(members, writer, mapper);
            ranges.clear();
        }
        for range in ranges {
            for run in self.actors[range.clone()].chunk_by(|a, b| a == b) {
                mapper.process_actor(usize::from(run[0]));
                writer.actor.append_n(run[0], run.len());
            }
            DeltaRunGrouper::extend(
                &mut seq,
                &mut writer.seq,
                self.seq[range.clone()].iter().map(|s| *s as i64),
            );
            DeltaRunGrouper::extend(
                &mut max_op,
                &mut writer.max_op,
                self.max_ops[range.clone()].iter().map(|m| *m as i64),
            );
            for run in self.num_ops.iter_range(range.clone()).runs() {
                writer.num_ops.append_n(run.value, run.count);
            }
            for run in self.timestamps.iter_range(range.clone()).runs() {
                writer.timestamp.append_run(run);
            }
            for run in self.messages.iter_range(range.clone()).runs() {
                writer.message.append_n(run.value, run.count);
            }
            for run in self.extra_bytes_meta.iter_range(range.clone()).runs() {
                writer.extra_meta.append_n(run.value, run.count);
            }
            let start = self.extra_bytes_meta.get_prefix(range.start) as usize;
            let end = self.extra_bytes_meta.get_prefix(range.end) as usize;
            writer
                .extra
                .extend_from_slice(&self.extra_bytes_raw[start..end]);
            let mut counts = range
                .clone()
                .map(|i| self.parent_slice(NodeIdx(i as u32)).len());
            if let Some(mut count) = counts.next() {
                let mut run = 1;
                for c in counts {
                    if c == count {
                        run += 1;
                    } else {
                        writer.dep_count.append_n(count as u32, run);
                        (count, run) = (c, 1);
                    }
                }
                writer.dep_count.append_n(count as u32, run);
            }
            for i in range {
                unnamed += self.write_member_deps(NodeIdx(i as u32), members, writer, &mut deps);
            }
        }
        if unnamed > 0 {
            return Err(UnretainedHashes {
                count: unnamed,
                what: "changes outside the bundle that its members depend on",
            });
        }
        seq.flush(&mut writer.seq);
        max_op.flush(&mut writer.max_op);
        deps.flush(&mut writer.deps);
        Ok(())
    }

    /// Writes members whose bundle order breaks into many short runs of
    /// nodes: every column is decoded once over their span rather than
    /// sought for each run. Returns the number of deps it could not name.
    fn write_gathered<'a>(
        &'a self,
        members: &Members<'_>,
        writer: &mut crate::storage::BundleChangeWriter<'a>,
        mapper: &mut crate::op_set2::change::ActorMapper<'_>,
    ) -> usize {
        let nodes = members.nodes();
        let lo = nodes[0].0 as usize;
        let span = lo..nodes[nodes.len() - 1].0 as usize + 1;
        let num_ops: Vec<_> = self.num_ops.iter_range(span.clone()).collect();
        let timestamps: Vec<_> = self.timestamps.iter_range(span.clone()).collect();
        let messages: Vec<_> = self.messages.iter_range(span.clone()).collect();
        let extra: Vec<_> = self
            .extra_bytes_meta
            .iter_range(span)
            .map(|m| (m.value, m.prefix() as usize..m.total() as usize))
            .collect();
        use crate::storage::DeltaRunGrouper;
        let (mut seq, mut max_op, mut deps, mut timestamp) = <(
            DeltaRunGrouper,
            DeltaRunGrouper,
            DeltaRunGrouper,
            DeltaRunGrouper,
        )>::default();
        let mut actor = Run(None);
        let mut ops = Run(None);
        let mut message = Run(None);
        let mut meta = Run(None);
        let mut dep_count = Run(None);
        let mut unnamed = 0;
        for n in members.in_order() {
            let i = n.0 as usize;
            let j = i - lo;
            let a = self.actors[i];
            if actor.0.is_none_or(|(last, _)| last != a) {
                mapper.process_actor(usize::from(a));
            }
            if let Some((v, k)) = actor.push(a) {
                writer.actor.append_n(v, k);
            }
            seq.push(&mut writer.seq, self.seq[i] as i64);
            max_op.push(&mut writer.max_op, self.max_ops[i] as i64);
            if let Some((v, k)) = ops.push(num_ops[j]) {
                writer.num_ops.append_n(v, k);
            }
            timestamp.push(&mut writer.timestamp, timestamps[j]);
            if let Some((v, k)) = message.push(messages[j]) {
                writer.message.append_n(v, k);
            }
            if let Some((v, k)) = meta.push(extra[j].0) {
                writer.extra_meta.append_n(v, k);
            }
            writer
                .extra
                .extend_from_slice(&self.extra_bytes_raw[extra[j].1.clone()]);
            if let Some((v, k)) = dep_count.push(self.parent_slice(*n).len() as u32) {
                writer.dep_count.append_n(v, k);
            }
            unnamed += self.write_member_deps(*n, members, writer, &mut deps);
        }
        if let Some((v, k)) = actor.0 {
            writer.actor.append_n(v, k);
        }
        if let Some((v, k)) = ops.0 {
            writer.num_ops.append_n(v, k);
        }
        if let Some((v, k)) = message.0 {
            writer.message.append_n(v, k);
        }
        if let Some((v, k)) = meta.0 {
            writer.extra_meta.append_n(v, k);
        }
        if let Some((v, k)) = dep_count.0 {
            writer.dep_count.append_n(v, k);
        }
        seq.flush(&mut writer.seq);
        max_op.flush(&mut writer.max_op);
        deps.flush(&mut writer.deps);
        timestamp.flush(&mut writer.timestamp);
        unnamed
    }

    /// The number of `n`'s parents it could not name.
    fn write_member_deps(
        &self,
        n: NodeIdx,
        members: &Members<'_>,
        writer: &mut crate::storage::BundleChangeWriter<'_>,
        deps: &mut crate::storage::DeltaRunGrouper,
    ) -> usize {
        let mut unnamed = 0;
        for p in self.parent_slice(n) {
            let idx = match (members.position(*p), self.hashes.get(*p)) {
                (Some(pos), _) => pos as i64,
                (None, Some(hash)) => writer.external_dep_index(hash),
                (None, None) => {
                    unnamed += 1;
                    continue;
                }
            };
            deps.push(&mut writer.deps, idx);
        }
        unnamed
    }

    /// `(actor, seq, start_op, max_op)` for each node.
    pub(crate) fn op_spans<'a>(
        &'a self,
        nodes: &'a [NodeIdx],
    ) -> impl Iterator<Item = (usize, u64, u64, u64)> + 'a {
        node_ranges(nodes).into_iter().flat_map(move |range| {
            range
                .clone()
                .zip(self.num_ops.iter_range(range))
                .map(move |(i, num_ops)| {
                    let max_op = self.max_ops[i] as u64;
                    let start_op = max_op + 1 - num_ops;
                    (
                        usize::from(self.actors[i]),
                        self.seq[i] as u64,
                        start_op,
                        max_op,
                    )
                })
        })
    }

    pub(crate) fn get_build_metadata<I>(
        &self,
        hashes: I,
    ) -> Result<Vec<BuildChangeMetadata<'_>>, crate::AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let indexes: Vec<_> = hashes
            .into_iter()
            .map(|hash| {
                self.lookup_hash(&hash)?
                    .ok_or(AutomergeError::from(MissingDep))
            })
            .collect::<Result<_, _>>()?;

        Ok(self.get_build_metadata_for_indexes(indexes))
    }

    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        ChangeIter {
            index: 0,
            actors: self.actors.iter(),
            seq: self.seq.iter(),
            max_ops: self.max_ops.iter(),
            num_ops: self.num_ops.iter(),
            timestamps: self.timestamps.iter(),
            messages: self.messages.iter(),
            extra_bytes_meta: self
                .extra_bytes_meta
                .iter_range(0..self.extra_bytes_meta.len()),
            len: self.len(),
            extra_bytes_raw: &self.extra_bytes_raw,
            dep_range: &self.dep_range,
            dep_target: &self.dep_target,
        }
    }

    fn get_build_metadata_for_indexes<I>(&self, indexes: I) -> Vec<BuildChangeMetadata<'_>>
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let changes = indexes
            .into_iter()
            .map(|index| {
                let i = index.0 as usize;
                let actor = self.actors[i].into();
                let timestamp = self.timestamps.get(i).unwrap_or_default();
                let max_op = self.max_ops[i] as u64;
                let num_ops = self.num_ops.get(i).unwrap_or_default();
                let message = self.messages.get(i).flatten().map(Cow::Borrowed);

                let meta = self.extra_bytes_meta.get(i).unwrap();
                let meta_range = meta.prefix() as usize..meta.total() as usize;
                let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

                let deps = self.parents(index).map(|p| p.0 as u64).collect::<Vec<_>>();
                let start_op = max_op - num_ops + 1;
                let seq = self.seq[i] as u64;
                BuildChangeMetadata {
                    actor,
                    seq,
                    start_op,
                    max_op,
                    timestamp,
                    message,
                    extra,
                    deps,
                    builder: i,
                }
            })
            .collect();
        changes
    }

    /// `seeds` and every ancestor whose hash was freed, ascending. Pass a
    /// whole run at once: separate calls re-walk shared ancestors.
    pub(crate) fn nodes_back_to_retained(
        &self,
        seeds: impl IntoIterator<Item = NodeIdx>,
    ) -> Vec<NodeIdx> {
        self.ancestors_while(seeds, |p| self.hashes.get(p).is_none())
    }

    /// The nodes `clock` doesn't cover, ascending, widened when the GC has
    /// freed a hash their bundle would name as a dep: back past every
    /// unhashed change behind those deps.
    pub(crate) fn nodes_above(&self, mut clock: SeqClock) -> Vec<NodeIdx> {
        loop {
            let nodes = self.get_build_indexes(clock.clone());
            if self.hashes.has_every_hash() {
                return nodes;
            }
            let unnamed: Vec<NodeIdx> = nodes
                .iter()
                .flat_map(|n| self.parent_slice(*n))
                .filter(|p| nodes.binary_search(p).is_err() && self.hashes.get(**p).is_none())
                .copied()
                .collect();
            if unnamed.is_empty() {
                return nodes;
            }
            // descendants go too, so the clock stays causally closed
            let mut gone: HashSet<NodeIdx> =
                self.nodes_back_to_retained(unnamed).into_iter().collect();
            let first = gone.iter().min().copied().expect("an unnamed dep");
            let covered = clock.clone();
            for n in (first.0..self.len() as u32).map(NodeIdx) {
                if !self.is_covered_by(n, &covered) {
                    continue;
                }
                // uncovering a change uncovers its actor's later ones too
                if gone.contains(&n)
                    || !self.is_covered_by(n, &clock)
                    || self.parents(n).any(|p| gone.contains(&p))
                {
                    gone.insert(n);
                    clock.uncover(
                        usize::from(self.actors[n.0 as usize]),
                        self.seq[n.0 as usize],
                    );
                }
            }
        }
    }

    /// The nodes `clock` covers, ascending.
    pub(crate) fn nodes_covered_by(&self, clock: &SeqClock) -> Vec<NodeIdx> {
        let mut nodes: Vec<NodeIdx> = self
            .seq_index
            .iter()
            .enumerate()
            .flat_map(|(actor, changes)| &changes[..covered_seq(clock, actor)])
            .copied()
            .collect();
        nodes.sort_unstable();
        nodes
    }

    /// The nodes of `nodes` (ascending) that none of the others depends on.
    pub(crate) fn heads_of(&self, nodes: &[NodeIdx]) -> Vec<NodeIdx> {
        let mut depended_on = vec![false; nodes.len()];
        for n in nodes {
            for p in self.parent_slice(*n) {
                if let Ok(i) = nodes.binary_search(p) {
                    depended_on[i] = true;
                }
            }
        }
        nodes
            .iter()
            .zip(depended_on)
            .filter(|(_, d)| !d)
            .map(|(n, _)| *n)
            .collect()
    }

    /// The nodes `clock` does *not* cover, ascending.
    pub(crate) fn get_build_indexes(&self, clock: SeqClock) -> Vec<NodeIdx> {
        let mut change_indexes: Vec<NodeIdx> = Vec::new();
        // walk the state from the given deps clock and add them into the vec
        for (actor_index, actor_changes) in self.seq_index.iter().enumerate() {
            if let Some(seq) = clock.get_for_actor(&actor_index) {
                // find the change in this actors sequence of changes that corresponds to the max_op
                // recorded for them in the clock
                change_indexes.extend(&actor_changes[seq.get() as usize..]);
            } else {
                change_indexes.extend(&actor_changes[..]);
            }
        }

        // ensure the changes are still in sorted order
        change_indexes.sort_unstable();

        change_indexes
    }

    pub(crate) fn get_hashes(
        &self,
        have_deps: &[ChangeHash],
    ) -> Result<Cow<'_, [ChangeHash]>, UncheckedHashes> {
        match (self.hashes.all(), have_deps.is_empty()) {
            (Some(all), true) => Ok(Cow::Borrowed(all)),
            (None, true) => Err(UncheckedHashes),
            _ => {
                let clock = self.seq_clock_for_heads(have_deps)?;
                Ok(Cow::Owned(
                    self.get_build_indexes(clock)
                        .into_iter()
                        .map(|node| self.hashes.try_get(node))
                        .collect::<Result<_, _>>()?,
                ))
            }
        }
    }

    pub(crate) fn get_build_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> Result<Vec<BuildChangeMetadata<'_>>, UncheckedHashes> {
        let clock = self.seq_clock_for_heads(have_deps)?;
        Ok(self.get_build_metadata_for_seq_clock(clock))
    }

    pub(crate) fn get_build_metadata_for_seq_clock(
        &self,
        clock: SeqClock,
    ) -> Vec<BuildChangeMetadata<'_>> {
        let change_indexes = self.get_build_indexes(clock);
        self.get_build_metadata_for_indexes(change_indexes)
    }

    fn update_heads(&mut self, change: &Change) {
        for d in change.deps() {
            if let Some(node) = self.nodes_by_hash.get(d).copied() {
                self.demote_head(node, d);
            }
        }
        self.heads.insert(change.hash());
    }

    pub(crate) fn add_nodes<
        'a,
        I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone,
    >(
        &mut self,
        iter: I,
    ) {
        self.actors
            .extend(iter.clone().map(|(_, a)| ActorIdx::from(a)));
        self.seq.extend(iter.clone().map(|(c, _)| c.seq() as u32));
        self.max_ops
            .extend(iter.clone().map(|(c, _)| c.max_op() as u32));
        self.num_ops
            .extend(iter.clone().map(|(c, _)| c.len() as u64));
        self.timestamps
            .extend(iter.clone().map(|(c, _)| c.timestamp()));
        self.messages.extend(iter.clone().map(|(c, _)| c.message()));
        self.extra_bytes_meta
            .extend(iter.clone().map(|(c, _)| ValueMeta::from(c.extra_bytes())));
        for (c, _) in iter {
            self.extra_bytes_raw.extend_from_slice(c.extra_bytes());
        }
    }

    /// Whether a new fragment owes a [`Self::gc_after_batch`].
    pub(crate) fn add_changes<
        'a,
        I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone,
    >(
        &mut self,
        iter: I,
        authors: &mut Authors,
    ) -> Result<bool, AddChangeError> {
        let node = NodeIdx(self.len() as u32);
        let mut new_fragment = false;

        self.add_nodes(iter.clone());
        self.assign_authors(node.0 as usize..self.len(), authors)?;

        for (i, (change, actor)) in iter.enumerate() {
            let node_idx = node + i;
            let hash = change.hash();
            self.max_op = std::cmp::max(self.max_op, change.max_op() as u32);
            self.hashes.push(hash);
            debug_assert!(!self.nodes_by_hash.contains_key(&hash));
            self.nodes_by_hash.insert(hash, node_idx);
            self.update_heads(change);

            self.push_tip(actor, change.seq(), node_idx);

            let parents = change
                .deps()
                .iter()
                .map(|h| self.lookup_hash(h)?.ok_or(MissingDep.into()))
                .collect::<Result<Vec<_>, AddChangeError>>()?;
            self.push_parents(node_idx, parents);
            if Self::tracks_pins() {
                self.pins.candidates.push(node_idx);
                self.pin_loose(node_idx);
            }

            if (node_idx + 1).0.is_multiple_of(CACHE_STEP) {
                self.cache_clock(node_idx);
            }

            // GC deferred to the end of the batch: later changes in
            // this batch may still resolve their deps by hash
            new_fragment |= self.cache_fragment_inner(node_idx);
        }
        Ok(new_fragment)
    }

    pub(crate) fn get_fragment(&self, head: ChangeHash, actors: &ActorTable) -> Option<Fragment> {
        let n = self.nodes_by_hash.get(&head).copied()?;
        if head.fragment_level() == 0 {
            self.loose_commit(n, actors)
        } else {
            debug_assert!(self.fragments.is_sorted_by_key(|f| f.sort_key()));
            let key = (std::cmp::Reverse(head.fragment_level()), n);
            self.fragments
                .binary_search_by_key(&key, |f| f.sort_key())
                .ok()
                .map(|i| self.cached_fragment(&self.fragments[i], actors))
        }
    }

    pub(crate) fn actor_seq(&self, n: NodeIdx) -> (usize, u32) {
        let i = n.0 as usize;
        (usize::from(self.actors[i]), self.seq[i])
    }

    pub(crate) fn change_id(&self, n: NodeIdx, actors: &ActorTable) -> ChangeId {
        let i = n.0 as usize;
        let actor_idx = usize::from(self.actors[i]);
        ChangeId::from_doc_seq(self.seq[i] as u64, actors[actor_idx].clone(), actor_idx)
    }

    pub(crate) fn node_for_change_id(&self, id: &ChangeId, actors: &ActorTable) -> Option<NodeIdx> {
        let hint = id.actor_idx_hint();
        let actor_idx = if actors.get(hint) == Some(id.actor()) {
            hint
        } else {
            actors.lookup(id.actor())?
        };
        self.seq_index
            .get(actor_idx)?
            .get(id.seq() as usize - 1)
            .copied()
    }

    /// [`AutomergeError::InvalidSeq`] when there is no such change,
    /// [`AutomergeError::AuditModeRequired`] when its hash was freed.
    pub(crate) fn get_hash_for_change_id(
        &self,
        id: &ChangeId,
        actors: &ActorTable,
    ) -> Result<ChangeHash, AutomergeError> {
        let node = self
            .node_for_change_id(id, actors)
            .ok_or(AutomergeError::InvalidSeq(id.seq()))?;
        self.hashes
            .try_get(node)
            .map_err(|_| AutomergeError::AuditModeRequired)
    }

    pub(crate) fn opid_to_change_id(&self, id: OpId, actors: &ActorTable) -> Option<ChangeId> {
        let node = self.opid_to_node(id)?;
        Some(self.change_id(node, actors))
    }

    pub(crate) fn change_id_for_hash(
        &self,
        hash: &ChangeHash,
        actors: &ActorTable,
    ) -> Result<Option<ChangeId>, UncheckedHashes> {
        Ok(self.lookup_hash(hash)?.map(|n| self.change_id(n, actors)))
    }

    /// Sorted, so documents with equal heads give identical lists.
    pub(crate) fn head_change_ids(&self, actors: &ActorTable) -> Vec<ChangeId> {
        let mut ids: Vec<ChangeId> = self
            .heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .map(|n| self.change_id(*n, actors))
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Ignores order and duplicates.
    pub(crate) fn nodes_are_heads(&self, nodes: &[NodeIdx]) -> bool {
        let head_nodes: std::collections::BTreeSet<NodeIdx> = self
            .heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .copied()
            .collect();
        let given: std::collections::BTreeSet<NodeIdx> = nodes.iter().copied().collect();
        head_nodes == given
    }

    /// A loose commit as its own single-member fragment.
    fn loose_commit(&self, n: NodeIdx, actors: &ActorTable) -> Option<Fragment> {
        let head = self.hashes.get(n)?;
        assert_eq!(head.fragment_level(), 0);
        let boundary = self
            .parents(n)
            .map(|p| self.hashes.get(p))
            .collect::<Option<Vec<_>>>()?;
        Some(self.export_fragment(head, 0, boundary, &[n], actors))
    }

    fn cached_fragment(&self, f: &FragmentNode, actors: &ActorTable) -> Fragment {
        let expect = "fragment index requires the fragment-hashes state";
        let head = self.hashes.get(f.head).expect(expect);
        let mut boundary: Vec<ChangeHash> = f
            .deps
            .iter()
            .map(|d| self.hashes.get(*d).expect(expect))
            .collect();
        // node order depends on arrival order
        boundary.sort_unstable();
        let clock = self.calculate_clock(f.deps.clone());
        let nodes = self.members_between([f.head], &clock);
        self.export_fragment(head, f.level, boundary, &nodes, actors)
    }

    fn export_fragment(
        &self,
        head: ChangeHash,
        level: usize,
        boundary: Vec<ChangeHash>,
        nodes: &[NodeIdx],
        actors: &ActorTable,
    ) -> Fragment {
        // interior hashes may be freed; checkpoint hashes never are
        let mut checkpoints: Vec<ChangeHash> = nodes
            .iter()
            .filter_map(|n| self.hashes.get(*n))
            .filter(|h| *h != head && h.fragment_level() > 0)
            .collect();
        checkpoints.sort_unstable();
        let members = nodes.iter().map(|n| self.change_id(*n, actors)).collect();
        Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        }
    }

    /// The fragments covering `heads` at `levels`, in apply order: coarsest
    /// first, oldest first within a level.
    pub(crate) fn fragments<R: RangeBounds<usize>>(
        &self,
        heads: &[ChangeHash],
        levels: R,
        actors: &ActorTable,
    ) -> Vec<Fragment> {
        let mut out: Vec<Fragment> = self
            .fragments
            .iter()
            .filter(|f| levels.contains(&f.level))
            .map(|f| self.cached_fragment(f, actors))
            .collect();
        if levels.contains(&0) {
            out.extend(
                self.loose_nodes(heads)
                    .into_iter()
                    .filter_map(|n| self.loose_commit(n, actors)),
            );
        }
        debug_assert!(
            out.is_sorted_by_key(|f| (std::cmp::Reverse(f.level), self.node_by_hash(&f.head))),
            "fragments are not in apply order",
        );
        out
    }

    /// The fragments, loose commits included, holding a node `covered`
    /// does not cover, in apply order: as [`Self::fragments`] lists them.
    pub(crate) fn fragments_after(
        &self,
        heads: &[ChangeHash],
        covered: &SeqClock,
        actors: &ActorTable,
    ) -> Vec<Fragment> {
        let fresh = |n: &NodeIdx| !self.is_covered_by(*n, covered);
        #[cfg(feature = "slow_path_assertions")]
        assert!(
            !self
                .fragments
                .iter()
                .any(|f| fresh(&f.head) && self.inside_higher_fragment(f)),
            "a fragment lies inside a larger one"
        );
        let mut out: Vec<Fragment> = self
            .fragments
            .iter()
            .filter(|f| fresh(&f.head))
            .map(|f| self.cached_fragment(f, actors))
            .collect();
        out.extend(
            self.loose_nodes(heads)
                .into_iter()
                .filter(fresh)
                .filter_map(|n| self.loose_commit(n, actors)),
        );
        out
    }

    /// Levels nest, so `f` lies inside a higher-level fragment exactly
    /// when its head is one of that fragment's members.
    #[cfg(feature = "slow_path_assertions")]
    fn inside_higher_fragment(&self, f: &FragmentNode) -> bool {
        self.fragments
            .iter()
            .filter(|k| k.level > f.level)
            .any(|k| {
                self.is_covered_by(f.head, &k.clock)
                    && !self.is_covered_by(f.head, &self.calculate_clock(k.deps.clone()))
            })
    }

    /// The loose commits above the fragment index, oldest first.
    fn loose_nodes(&self, heads: &[ChangeHash]) -> Vec<NodeIdx> {
        let tips = heads
            .iter()
            .filter(|h| h.fragment_level() == 0)
            .filter_map(|h| self.nodes_by_hash.get(h).copied());
        self.members_between(tips, &self.fragment_top)
    }

    /// Everything reachable from `heads` that `dep_clock` does not
    /// cover, oldest first.
    pub(crate) fn members_between(
        &self,
        heads: impl IntoIterator<Item = NodeIdx>,
        dep_clock: &SeqClock,
    ) -> Vec<NodeIdx> {
        self.ancestors_while(heads, |p| !self.is_covered_by(p, dep_clock))
    }

    /// `seeds` and the ancestors reached through parents `follow` accepts,
    /// ascending.
    fn ancestors_while(
        &self,
        seeds: impl IntoIterator<Item = NodeIdx>,
        follow: impl Fn(NodeIdx) -> bool,
    ) -> Vec<NodeIdx> {
        // pops in descending node index and a parent's index is always
        // lower than its child's, so the frontier alone dedupes
        let mut to_visit: BTreeSet<NodeIdx> = seeds.into_iter().collect();
        let mut nodes = Vec::new();
        while let Some(n) = to_visit.pop_last() {
            to_visit.extend(self.parents(n).filter(|p| follow(*p)));
            nodes.push(n);
        }
        nodes.reverse();
        nodes
    }

    pub(crate) fn cache_fragments(&mut self) {
        self.fragments.clear();
        self.fragment_top = self.empty_clock();
        for leveled in self.leveled.as_mut_slice() {
            leveled.clear();
        }
        for n in 0..self.hashes.len() {
            self.cache_fragment_inner(NodeIdx(n as u32));
        }
    }

    /// Run once at the end of a batch: a GC mid-batch would free dep hashes
    /// that later changes in the batch still resolve by hash.
    pub(crate) fn gc_after_batch(&mut self) {
        if self.gc_mode == crate::automerge::GcMode::Manual {
            self.gc_owed = true;
            return;
        }
        H::collect_garbage(self);
    }

    pub(crate) fn run_gc(&mut self) {
        self.gc_owed = false;
        H::collect_garbage(self);
    }

    pub(crate) fn gc_mode(&self) -> crate::automerge::GcMode {
        self.gc_mode
    }

    pub(crate) fn set_gc_mode(&mut self, mode: crate::automerge::GcMode) {
        self.gc_mode = mode;
    }

    pub(crate) fn gc_owed(&self) -> bool {
        self.gc_owed
    }

    /// Whether a fragment was cached, in which case the caller owes a
    /// [`Self::gc_after_batch`].
    fn cache_fragment_inner(&mut self, head: NodeIdx) -> bool {
        let Some(hash) = self.hashes.get(head) else {
            return false;
        };
        let level = hash.fragment_level();
        if level == 0 {
            return false;
        }
        debug_assert!(
            {
                let actor = usize::from(self.actors[head.0 as usize]);
                let seq = NonZeroU32::new(self.seq[head.0 as usize]);
                self.fragments
                    .iter()
                    .all(|f| f.clock.get_for_actor(&actor) < seq)
            },
            "fragments cached out of graph order"
        );
        let actor = usize::from(self.actors[head.0 as usize]);
        let at = self.leveled[actor]
            .partition_point(|n| self.seq[n.0 as usize] < self.seq[head.0 as usize]);
        self.leveled.as_mut_slice()[actor].insert(at, head);
        let clock = self.calculate_clock(vec![head]);
        let (deps, boundary) = self.fragment_boundary(head, &clock, level);
        let supercede: Vec<usize> = (0..self.fragments.len())
            .rev()
            .filter(|i| {
                let f = &self.fragments[*i];
                f.level < level && clock.covers(&f.clock)
            })
            .collect();
        let superseded: Vec<FragmentNode> = supercede
            .into_iter()
            .map(|i| self.fragments.remove(i))
            .collect();
        let old_top = Self::tracks_pins().then(|| self.fragment_top.clone());
        SeqClock::merge(&mut self.fragment_top, &clock);
        let node = FragmentNode {
            head,
            level,
            deps,
            clock,
        };
        if let Some(old_top) = old_top {
            // pin before releasing, so a node held by both never reaches zero
            for p in self.fragment_pins_within(&node, &boundary) {
                self.pins.pin(p);
                if self.hashes.get(p).is_none() && self.pins.deferred_from.is_none_or(|d| p < d) {
                    self.pins.unhashed.push(p);
                }
            }
            for f in &superseded {
                self.unpin_fragment(f);
            }
            self.release_covered(&old_top);
        }
        let pos = self
            .fragments
            .partition_point(|f| f.sort_key() < node.sort_key());
        self.fragments.insert(pos, node);
        true
    }

    /// The nearest changes of level >= `level` in `head`'s ancestry, and
    /// their merged clock: each actor's latest such change `clock` covers,
    /// less those in another's ancestry.
    fn fragment_boundary(
        &self,
        head: NodeIdx,
        clock: &SeqClock,
        level: usize,
    ) -> (Vec<NodeIdx>, SeqClock) {
        let head_actor = usize::from(self.actors[head.0 as usize]);
        let mut candidates = vec![];
        for (a, leveled) in self.leveled.iter().enumerate() {
            let Some(limit) = clock.get_for_actor(&a) else {
                continue;
            };
            let limit = limit.get() - u32::from(a == head_actor);
            let end = leveled.partition_point(|n| self.seq[n.0 as usize] <= limit);
            if let Some(n) = leveled[..end]
                .iter()
                .rev()
                .find(|n| self.fragment_level(**n) >= level)
            {
                candidates.push(*n);
            }
        }
        let clocks: Vec<SeqClock> = candidates
            .iter()
            .map(|c| self.calculate_clock(vec![*c]))
            .collect();
        let mut boundary = self.empty_clock();
        for c in &clocks {
            SeqClock::merge(&mut boundary, c);
        }
        let deps = candidates
            .iter()
            .enumerate()
            .filter(|(i, c)| {
                !clocks
                    .iter()
                    .enumerate()
                    .any(|(j, other)| j != *i && self.is_covered_by(**c, other))
            })
            .map(|(_, c)| *c)
            .collect();
        (deps, boundary)
    }

    /// Changes this document already had that a new fragment pins after the
    /// GC freed their hashes: a fork inside one fragment whose other branch
    /// sat in a concurrent one, interior to both until a fragment covering
    /// them both put it on its boundary. Only the document can rebuild them.
    pub(crate) fn take_unhashed_fragment_pins(&mut self) -> Vec<NodeIdx> {
        let mut pins = std::mem::take(&mut self.pins.unhashed);
        // a fragment a later one superseded in the same bundle released its pins
        pins.retain(|p| self.pins.is_pinned(*p) && self.hashes.get(*p).is_none());
        pins.sort_unstable();
        pins.dedup();
        pins
    }

    fn tracks_pins() -> bool {
        H::AUDIT == crate::automerge::AuditMode::Disabled
    }

    /// The nodes whose hashes exporting `f` needs beyond level > 0 ones:
    /// its members' parents outside it, and each actor's last member (the
    /// receiver's tip for that actor). Sorted, without duplicates.
    fn fragment_pins(&self, f: &FragmentNode) -> Vec<NodeIdx> {
        self.fragment_pins_within(f, &self.calculate_clock(f.deps.clone()))
    }

    /// [`Self::fragment_pins`], given the merged clock of `f.deps`.
    fn fragment_pins_within(&self, f: &FragmentNode, boundary: &SeqClock) -> Vec<NodeIdx> {
        let mut pins = vec![];
        // members are each actor's seqs between the boundary and the clock,
        // so no ancestry walk
        for (a, seqs) in self.seq_index.iter().enumerate() {
            let from = covered_seq(boundary, a);
            let to = covered_seq(&f.clock, a);
            if from >= to {
                continue;
            }
            pins.push(seqs[to - 1]);
            for m in &seqs[from..to] {
                pins.extend(
                    self.parents(*m)
                        .filter(|p| self.is_covered_by(*p, boundary)),
                );
            }
        }
        pins.sort_unstable();
        pins.dedup();
        pins
    }

    fn unpin_fragment(&mut self, f: &FragmentNode) {
        for p in self.fragment_pins(f) {
            self.pins.unpin(p);
        }
    }

    /// A node is loose from when it's added until `fragment_top` covers it,
    /// and pins itself and its parents meanwhile.
    fn pin_loose(&mut self, n: NodeIdx) {
        self.pins.pin(n);
        let (off, count) = self.dep_range[n.0 as usize];
        for i in off..off + count {
            self.pins.pin(self.dep_target[i as usize]);
        }
    }

    fn unpin_loose(&mut self, n: NodeIdx) {
        self.pins.unpin(n);
        let (off, count) = self.dep_range[n.0 as usize];
        for p in &self.dep_target[off as usize..(off + count) as usize] {
            self.pins.unpin(*p);
        }
    }

    /// Releases the loose nodes `fragment_top` covers but `old_top` didn't:
    /// nodes are added above `fragment_top`, so these are exactly the ones
    /// pinned as loose.
    fn release_covered(&mut self, old_top: &SeqClock) {
        for a in 0..self.seq_index.len() {
            let from = covered_seq(old_top, a);
            let to = covered_seq(&self.fragment_top, a);
            for i in from..to {
                let n = self.seq_index[a][i];
                if self.pins.deferred_from.is_none_or(|d| n < d) {
                    self.unpin_loose(n);
                }
            }
        }
    }

    /// Pins the nodes a bundle added that its fragments left loose. Call
    /// once the bundle's hashes are all recorded, before anything reads the
    /// pins.
    pub(crate) fn pin_deferred_loose(&mut self) {
        let Some(from) = self.pins.deferred_from else {
            return;
        };
        for n in from.0..self.len() as u32 {
            if !self.is_covered_by(NodeIdx(n), &self.fragment_top) {
                self.pin_loose(NodeIdx(n));
            }
        }
        self.pins.deferred_from = None;
    }

    /// `None` for a hash the graph does not hold, including a freed one.
    pub(crate) fn node_by_hash(&self, hash: &ChangeHash) -> Option<NodeIdx> {
        self.nodes_by_hash.get(hash).copied()
    }

    pub(crate) fn hash_for_node(&self, node: NodeIdx) -> Option<ChangeHash> {
        self.hashes.get(node)
    }

    /// Record a node's hash, returning whether that formed a new fragment,
    /// in which case the caller owes a [`Self::gc_after_batch`].
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_node_hash(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        // a chain apply re-records fragment heads; re-caching one would cost
        // an O(graph) clock walk and a duplicate index entry
        if let Some(known) = self.nodes_by_hash.get(&hash) {
            debug_assert_eq!(*known, node, "hash recorded for two nodes");
            return false;
        }
        if !self.hashes.learn(node, hash) {
            return false;
        }
        self.nodes_by_hash.insert(hash, node);
        if Self::tracks_pins() {
            self.pins.candidates.push(node);
        }
        self.cache_fragment_inner(node)
    }

    /// [`Self::record_node_hash`], also adding the hash to the heads.
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_fragment_head(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        let cached = self.record_node_hash(node, hash);
        self.heads.insert(hash);
        cached
    }

    /// Whether a new fragment owes a [`Self::gc_after_batch`].
    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor: usize,
        authors: &mut Authors,
    ) -> Result<bool, AddChangeError> {
        let hash = change.hash();

        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(false);
        }

        for h in change.deps().iter() {
            if !self.has_change(h)? {
                return Err(MissingDep.into());
            }
        }

        self.add_changes([(change, actor)].into_iter(), authors)
    }

    fn cache_clock(&mut self, node_idx: NodeIdx) -> SeqClock {
        let mut clock = self.empty_clock();
        let mut to_visit = BTreeSet::from([node_idx]);

        self.calculate_clock_inner(&mut clock, &mut to_visit, CACHE_STEP as usize * 2);

        for n in to_visit {
            let sub = self.cache_clock(n);
            SeqClock::merge(&mut clock, &sub);
        }

        self.clock_cache.insert(node_idx, clock.clone());

        clock
    }

    fn push_parents(&mut self, child_idx: NodeIdx, parents: impl IntoIterator<Item = NodeIdx>) {
        debug_assert_eq!(
            self.dep_range.len(),
            child_idx.0 as usize,
            "nodes must receive their parents in order"
        );
        let off = self.dep_target.len() as u32;
        for parent_idx in parents {
            debug_assert!(parent_idx < child_idx, "parent added after its child");
            self.dep_target.push(parent_idx);
        }
        self.dep_range
            .push((off, self.dep_target.len() as u32 - off));
    }

    pub(crate) fn deps(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = Result<ChangeHash, UncheckedHashes>> + '_ {
        let mut iter = self.nodes_by_hash.get(hash).map(|node| self.parents(*node));
        std::iter::from_fn(move || {
            let next = iter.as_mut()?.next()?;
            Some(self.hashes.try_get(next))
        })
    }

    /// Skips parents whose hash was freed.
    pub(crate) fn parent_hashes(&self, node_idx: NodeIdx) -> Vec<ChangeHash> {
        self.parents(node_idx)
            .filter_map(|p| self.hashes.get(p))
            .collect()
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        self.parent_slice(node_idx).iter().copied()
    }

    /// Empty for a node whose edges aren't written yet: in a bulk append the
    /// node columns run ahead of `dep_range`.
    fn parent_slice(&self, node_idx: NodeIdx) -> &[NodeIdx] {
        match self.dep_range.get(node_idx.0 as usize) {
            Some(&(off, count)) => &self.dep_target[off as usize..off as usize + count as usize],
            None => &[],
        }
    }

    /// Skips hashes that are definitely not in this document.
    fn heads_to_nodes(&self, heads: &[ChangeHash]) -> Result<Vec<NodeIdx>, UncheckedHashes> {
        heads
            .iter()
            .filter_map(|h| self.lookup_hash(h).transpose())
            .collect()
    }

    pub(crate) fn clock_for_nodes(&self, nodes: Vec<NodeIdx>) -> Clock {
        self.calculate_clock(nodes).map_to_clock(|actor, seq| {
            self.seq_index
                .get(actor)
                .and_then(|v| v.get(seq.get() as usize - 1))
                .and_then(|i| self.max_ops.get(i.0 as usize))
                .copied()
        })
    }

    pub(crate) fn seq_clock_for_heads(
        &self,
        heads: &[ChangeHash],
    ) -> Result<SeqClock, UncheckedHashes> {
        let nodes = self.heads_to_nodes(heads)?;
        Ok(self.calculate_clock(nodes))
    }

    fn clock_data_for(&self, idx: NodeIdx) -> Option<u32> {
        Some(*self.seq.get(idx.0 as usize)?)
    }

    pub(crate) fn calculate_clock(&self, nodes: Vec<NodeIdx>) -> SeqClock {
        let mut clock = self.empty_clock();
        let mut to_visit = nodes.into_iter().collect::<BTreeSet<_>>();

        self.calculate_clock_inner(&mut clock, &mut to_visit, usize::MAX);

        assert!(to_visit.is_empty());

        clock
    }

    fn calculate_clock_inner(
        &self,
        clock: &mut SeqClock,
        to_visit: &mut BTreeSet<NodeIdx>,
        limit: usize,
    ) {
        // The merge of every cached clock absorbed so far: a node it covers
        // is already accounted for, subtree and all. Without this pruning the
        // walk fans out exponentially on merge-heavy graphs.
        let mut covered = self.empty_clock();
        let mut visited = 0;

        // parents sort below their children, so a popped node never returns
        while let Some(idx) = to_visit.pop_last() {
            visited += 1;

            let actor = self.actors[idx.0 as usize];
            let data = self.clock_data_for(idx);

            if let (Some(d), Some(c)) = (data, covered.get_for_actor(&actor.into())) {
                if d <= c.get() {
                    continue;
                }
            }

            clock.include(actor.into(), data);

            if let Some(cached) = self.clock_cache.get(&idx) {
                SeqClock::merge(clock, cached);
                SeqClock::merge(&mut covered, cached);
            } else {
                to_visit.extend(self.parents(idx));
                if visited > limit {
                    break;
                }
            }
        }
    }

    /// `Err` is a hash the graph held that the recomputed `hashes`
    /// contradict.
    pub(crate) fn verify_hashes(&self, hashes: &[ChangeHash]) -> Result<(), ChangeHash> {
        assert_eq!(hashes.len(), self.len(), "one hash per node");

        for idx in self.node_ids() {
            if let Some(known) = self.hashes.get(idx) {
                if hashes[idx.0 as usize] != known {
                    return Err(known);
                }
            }
        }

        let has_child = self.has_child_mask();
        let computed_heads: BTreeSet<ChangeHash> = (0..self.len())
            .filter(|n| !has_child[*n])
            .map(|n| hashes[n])
            .collect();
        if computed_heads != self.heads {
            let bad = self
                .heads
                .difference(&computed_heads)
                .next()
                .or_else(|| computed_heads.difference(&self.heads).next())
                .copied()
                .expect("unequal sets differ somewhere");
            return Err(bad);
        }

        Ok(())
    }

    pub(crate) fn into_full(self, hashes: Vec<ChangeHash>) -> ChangeGraph<Full> {
        assert_eq!(hashes.len(), self.len(), "one hash per node");
        let nodes_by_hash = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, NodeIdx(i as u32)))
            .collect();
        let mut graph = self.with_hashes(Full(hashes));
        graph.nodes_by_hash = nodes_by_hash;
        graph.pins = Pins::default();
        graph.cache_fragments();
        graph
    }

    fn has_child_mask(&self) -> Vec<bool> {
        let mut has_child = vec![false; self.len()];
        for target in &self.dep_target {
            has_child[target.0 as usize] = true;
        }
        has_child
    }

    /// `nodes_by_hash` is the caller's to keep in step with `hashes`.
    fn with_hashes<H2: HashRetention>(self, hashes: H2) -> ChangeGraph<H2> {
        let ChangeGraph {
            hashes: _,
            actors,
            dep_range,
            dep_target,
            seq,
            max_ops,
            max_op,
            num_ops,
            timestamps,
            messages,
            extra_bytes_meta,
            extra_bytes_raw,
            heads,
            nodes_by_hash,
            clock_cache,
            seq_index,
            leveled,
            fragment_top,
            fragments,
            gc_mode,
            gc_owed,
            pins,
        } = self;
        ChangeGraph {
            hashes,
            actors,
            dep_range,
            dep_target,
            seq,
            max_ops,
            max_op,
            num_ops,
            timestamps,
            messages,
            extra_bytes_meta,
            extra_bytes_raw,
            heads,
            nodes_by_hash,
            clock_cache,
            seq_index,
            leveled,
            fragment_top,
            fragments,
            gc_mode,
            gc_owed,
            pins,
        }
    }

    /// Populate `clock_cache` with the clock of every `CACHE_STEP`th node
    /// from `base` on. Memory is bounded by the graph's width, not its size;
    /// costs one full clock walk per node before `base` they depend on.
    fn cache_clocks_from(&mut self, base: usize) {
        let n = self.len();
        // a clock is cached at every `CACHE_STEP`th node only
        let first_cached = (base as u32 + 1).next_multiple_of(CACHE_STEP) - 1;
        if first_cached as usize >= n {
            return;
        }

        fn alloc(pool: &mut Vec<SeqClock>, free: &mut Vec<u32>, like: &SeqClock) -> u32 {
            free.pop().unwrap_or_else(|| {
                pool.push(like.empty_like());
                (pool.len() - 1) as u32
            })
        }

        fn two_rows(pool: &mut [SeqClock], dst: usize, src: usize) -> (&mut SeqClock, &SeqClock) {
            debug_assert_ne!(dst, src);
            if dst < src {
                let (lo, hi) = pool.split_at_mut(src);
                (&mut lo[dst], &hi[0])
            } else {
                let (lo, hi) = pool.split_at_mut(dst);
                (&mut hi[0], &lo[src])
            }
        }

        let template = self.empty_clock();

        const DEAD: u32 = u32::MAX;
        // indexed by node - base, so a small apply costs O(apply), not O(graph)
        let mut slot_of = vec![DEAD; n - base]; // node -> pool slot while its row is live
        let mut pending_children = vec![0u32; n - base];
        let mut pool: Vec<SeqClock> = Vec::new();
        let mut free: Vec<u32> = Vec::new();
        let mut parent_buf: Vec<usize> = Vec::new();

        // a parent from before `base` keeps its row for the whole sweep
        let mut seed_slot: SmallHashMap<usize, u32> = SmallHashMap::default();
        for i in base..n {
            for p in self.parents(NodeIdx(i as u32)) {
                let p = p.0 as usize;
                if p >= base {
                    pending_children[p - base] += 1;
                } else if let std::collections::hash_map::Entry::Vacant(e) = seed_slot.entry(p) {
                    pool.push(self.calculate_clock(vec![NodeIdx(p as u32)]));
                    e.insert((pool.len() - 1) as u32);
                }
            }
        }
        let slot_for = |slot_of: &[u32], p: usize| {
            if p >= base {
                slot_of[p - base]
            } else {
                seed_slot[&p]
            }
        };

        for i in base..n {
            let idx = NodeIdx(i as u32);
            parent_buf.clear();
            for p in self.parents(idx) {
                let p = p.0 as usize;
                debug_assert!(p < i, "change graph is topologically ordered");
                parent_buf.push(p);
            }

            let slot = match parent_buf.split_first() {
                Some((&first, rest)) => {
                    let first_slot = slot_for(&slot_of, first);
                    debug_assert_ne!(first_slot, DEAD);
                    let slot = if first >= base && pending_children[first - base] == 1 {
                        // sole remaining child: reuse the row
                        slot_of[first - base] = DEAD;
                        first_slot
                    } else {
                        let s = alloc(&mut pool, &mut free, &template);
                        let (dst, src) = two_rows(&mut pool, s as usize, first_slot as usize);
                        dst.copy_from(src);
                        s
                    };
                    for &p in rest {
                        let p_slot = slot_for(&slot_of, p);
                        if p_slot == DEAD || p_slot == slot {
                            continue; // duplicate dep
                        }
                        let (dst, src) = two_rows(&mut pool, slot as usize, p_slot as usize);
                        SeqClock::merge(dst, src);
                    }
                    slot
                }
                None => {
                    let s = alloc(&mut pool, &mut free, &template);
                    pool[s as usize].clear();
                    s
                }
            };

            for &p in &parent_buf {
                if p < base {
                    continue;
                }
                let pending = &mut pending_children[p - base];
                *pending -= 1;
                if *pending == 0 && slot_of[p - base] != DEAD {
                    free.push(slot_of[p - base]);
                    slot_of[p - base] = DEAD;
                }
            }

            let actor = self.actors[i];
            pool[slot as usize].include(actor.into(), self.clock_data_for(idx));

            if (i as u32 + 1).is_multiple_of(CACHE_STEP) {
                self.clock_cache.insert(idx, pool[slot as usize].clone());
            }

            if pending_children[i - base] == 0 {
                free.push(slot);
            } else {
                slot_of[i - base] = slot;
            }
        }
    }

    pub(crate) fn remove_ancestors(
        &self,
        changes: &mut BTreeSet<ChangeHash>,
        heads: &[ChangeHash],
    ) -> Result<(), UncheckedHashes> {
        let nodes = self.heads_to_nodes(heads)?;
        let mut unchecked = false;
        self.traverse_ancestors(nodes, |idx| {
            match self.hashes.get(idx) {
                Some(hash) => {
                    changes.remove(&hash);
                }
                None => unchecked = true,
            }
            true
        });
        if unchecked {
            Err(UncheckedHashes)
        } else {
            Ok(())
        }
    }

    fn traverse_ancestors<F: FnMut(NodeIdx) -> bool>(&self, mut to_visit: Vec<NodeIdx>, mut f: F) {
        let mut visited = BTreeSet::new();

        while let Some(idx) = to_visit.pop() {
            if visited.contains(&idx) {
                continue;
            } else {
                visited.insert(idx);
            }
            if f(idx) {
                to_visit.extend(self.parents(idx));
            }
        }
    }

    /// Append a bundle's members, with their `hashes` if given. Members
    /// must be in topological order, each extending its actor's seq chain.
    /// Whether a new fragment owes a [`Self::gc_after_batch`].
    pub(crate) fn add_bundle_members(
        &mut self,
        members: Vec<BundleMember<'_>>,
        hashes: Option<&[ChangeHash]>,
        authors: &mut Authors,
        actor_ids: &ActorTable,
    ) -> Result<bool, AutomergeError> {
        let new_authors = Self::member_authors(
            members.iter().map(|m| (m.actor, m.seq, m.extra.as_ref())),
            authors,
            actor_ids,
        )?;
        let base = NodeIdx(self.len() as u32);

        self.hashes.extend(members.len(), hashes);

        self.actors
            .extend(members.iter().map(|m| ActorIdx::from(m.actor)));
        self.seq.extend(members.iter().map(|m| m.seq as u32));
        self.max_ops.extend(members.iter().map(|m| m.max_op as u32));
        self.num_ops.extend(members.iter().map(|m| m.num_ops));
        self.timestamps.extend(members.iter().map(|m| m.timestamp));
        self.messages
            .extend(members.iter().map(|m| m.message.clone()));
        self.extra_bytes_meta
            .extend(members.iter().map(|m| ValueMeta::from(m.extra.as_ref())));
        for m in &members {
            self.extra_bytes_raw.extend_from_slice(&m.extra);
        }

        let mut parent_buf: Vec<NodeIdx> = Vec::new();
        for (i, m) in members.iter().enumerate() {
            let node_idx = base + i;
            self.max_op = std::cmp::max(self.max_op, m.max_op as u32);

            self.push_tip(m.actor, m.seq, node_idx);

            parent_buf.clear();
            parent_buf.extend(m.deps.iter().map(|d| match d {
                BundleDep::Member(j) => {
                    debug_assert!(*j < i);
                    base + *j
                }
                BundleDep::Node(n) => *n,
            }));
            for &parent in &parent_buf {
                if let Some(h) = self.hashes.get(parent) {
                    self.demote_head(parent, &h);
                }
            }
            self.push_parents(node_idx, parent_buf.iter().copied());
        }
        Ok(self.finish_members(base, hashes, new_authors, authors))
    }

    fn finish_members(
        &mut self,
        base: NodeIdx,
        hashes: Option<&[ChangeHash]>,
        new_authors: Vec<(usize, crate::Author<'_>)>,
        authors: &mut Authors,
    ) -> bool {
        self.cache_clocks_from(base.0 as usize);
        for (actor, author) in new_authors {
            authors.assign_author(author.into_owned(), actor);
        }
        if Self::tracks_pins() {
            // most of a bundle is covered by its own fragments, so pinning
            // it loose now would only be released below
            self.pins.deferred_from.get_or_insert(base);
            let learned = hashes.map_or(0, |h| h.len()) as u32;
            self.pins
                .candidates
                .extend((base.0..base.0 + learned).map(NodeIdx));
        }
        let mut new_fragment = false;
        for (i, hash) in hashes.into_iter().flatten().enumerate() {
            self.nodes_by_hash.insert(*hash, base + i);
            new_fragment |= self.cache_fragment_inner(base + i);
        }
        new_fragment
    }

    /// [`Self::add_bundle_members`] straight from the bundle's columns.
    /// Only valid when *every* member is kept. `ext_nodes` holds the node
    /// of each external dep, in the bundle's dep order. A malformed bundle
    /// leaves the graph untouched.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_bundle_members_cols(
        &mut self,
        cols: &crate::storage::BundleChangeCols<'_>,
        member_actors: &[ActorIdx],
        member_seqs: &[NonZeroU64],
        actor_map: &[usize],
        ext_nodes: &[NodeIdx],
        hashes: Option<&[ChangeHash]>,
        authors: &mut Authors,
        actor_ids: &ActorTable,
    ) -> Result<bool, AutomergeError> {
        let bad = |s: &'static str| AutomergeError::from(MalformedBundle::Invalid(s));
        let base = NodeIdx(self.len() as u32);
        let n = member_actors.len();
        debug_assert_eq!(n, member_seqs.len());

        // already in the graph's encodings: spliced in below without decoding
        let opts = hexane::LoadOpts::new().with_length(n);
        let num_ops = hexane::Column::<u64>::load_with(cols.num_ops, opts.with_fill(1u64))
            .map_err(|_| bad("invalid member op-count column"))?;
        let timestamps =
            hexane::DeltaColumn::<i64>::load_with(cols.timestamp, opts.with_fill(0i64))
                .map_err(|_| bad("invalid member timestamp column"))?;
        let messages =
            hexane::Column::<Option<String>>::load_with(cols.message, opts.with_fill(None))
                .map_err(|_| bad("invalid member message column"))?;
        let extra_meta =
            hexane::PrefixColumn::<ValueMeta>::load_with(cols.extra_meta, opts.with_fill(NO_EXTRA))
                .map_err(|_| bad("invalid member extra column"))?;
        let extra_end = extra_meta.sum_range(0..n) as usize;
        if extra_end > cols.extra.len() {
            return Err(bad("member extra bytes overrun the column"));
        }
        let new_authors = Self::member_authors(
            extra_meta.iter().take(n).enumerate().map(|(i, m)| {
                (
                    actor_map[usize::from(member_actors[i])],
                    member_seqs[i].get(),
                    &cols.extra[m.prefix() as usize..m.total() as usize],
                )
            }),
            authors,
            actor_ids,
        )?;

        let mut max_ops = Vec::with_capacity(n);
        for m in cols.max_ops().take(n) {
            let Some(m) = m else {
                return Err(bad("short member max_op column"));
            };
            max_ops.push(m as u32);
        }
        if max_ops.len() != n {
            return Err(bad("short member max_op column"));
        }

        let mut dep_range = Vec::with_capacity(n);
        let mut dep_target: Vec<NodeIdx> = Vec::new();
        let mut dep_values = cols.dep_values();
        let dep_base = self.dep_target.len() as u32;
        for (i, count) in pad(cols.dep_counts().map(|c| c.unwrap_or(0)), 0, n).enumerate() {
            let off = dep_base + dep_target.len() as u32;
            for _ in 0..count {
                let Some(Some(d)) = dep_values.next() else {
                    return Err(bad("short member dep column"));
                };
                let d = d as usize;
                let parent = if d < n {
                    if d >= i {
                        return Err(bad("member dep is not an earlier member"));
                    }
                    base + d
                } else {
                    let Some(node) = ext_nodes.get(d - n).copied() else {
                        return Err(bad("member dep index out of range"));
                    };
                    node
                };
                dep_target.push(parent);
            }
            dep_range.push((off, dep_target.len() as u32 + dep_base - off));
        }

        self.hashes.extend(n, hashes);
        self.actors.extend(
            member_actors
                .iter()
                .map(|a| ActorIdx::from(actor_map[usize::from(*a)])),
        );
        self.seq.extend(member_seqs.iter().map(|s| s.get() as u32));
        self.max_op = std::cmp::max(self.max_op, max_ops.iter().copied().max().unwrap_or(0));
        self.max_ops.extend(max_ops);

        let tail = hexane::Splice {
            pos: base.0 as usize,
            ..Default::default()
        };
        self.num_ops.copy_ranges(num_ops, [tail.clone()]);
        self.timestamps.copy_ranges(timestamps, [tail.clone()]);
        self.messages.copy_ranges(messages, [tail.clone()]);
        self.extra_bytes_meta.copy_ranges(extra_meta, [tail]);
        self.extra_bytes_raw
            .extend_from_slice(&cols.extra[..extra_end]);

        debug_assert_eq!(self.dep_range.len(), base.0 as usize);
        for parent in &dep_target {
            if *parent < base {
                if let Some(h) = self.hashes.get(*parent) {
                    self.demote_head(*parent, &h);
                }
            }
        }
        self.dep_range.extend(dep_range);
        self.dep_target.extend(dep_target);

        for i in 0..n {
            let actor = actor_map[usize::from(member_actors[i])];
            self.push_tip(actor, member_seqs[i].get(), base + i);
        }
        Ok(self.finish_members(base, hashes, new_authors, authors))
    }
}

impl ChangeGraph<Retained> {
    /// Frees the hashes no holder needs any more. Costs O(hashes learned or
    /// released since the last GC), not O(graph).
    ///
    /// Panics if a hash this document must retain is already gone.
    pub(crate) fn retain_hashes_only(&mut self) {
        let gone = self.missing_retained_hashes().len();
        assert!(
            gone == 0,
            "{gone} change hashes this document must retain are already gone"
        );
        let mut candidates = std::mem::take(&mut self.pins.candidates);
        candidates.sort_unstable();
        candidates.dedup();
        for n in candidates {
            let Some(hash) = self.hashes.get(n) else {
                continue;
            };
            if self.must_keep(n, &hash) {
                // re-queued by `unpin`, `push_tip` or `demote_head`; level > 0
                // hashes stay for good
                continue;
            }
            self.hashes.forget(n);
            self.nodes_by_hash.remove(&hash);
        }
        #[cfg(feature = "slow_path_assertions")]
        self.verify_pins();
    }
}

impl ChangeGraph<Full> {
    pub(crate) fn into_retained(mut self) -> ChangeGraph<Retained> {
        self.rebuild_pins();
        let len = self.hashes.len();
        let map: SmallHashMap<NodeIdx, ChangeHash> = self
            .hashes
            .iter()
            .filter(|(n, hash)| self.must_keep(*n, hash))
            .collect();
        let mut graph = self.with_hashes(Retained { map, len });
        let ChangeGraph {
            hashes,
            nodes_by_hash,
            ..
        } = &mut graph;
        nodes_by_hash.retain(|_, n| hashes.get(*n).is_some());
        // a Full graph keeps no candidates: every hash left is kept
        graph.pins.candidates.clear();
        graph.pins.fresh.clear();
        graph
    }
}

impl ChangeGraphCols {
    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        self.graph.iter()
    }

    pub(crate) fn finalize(
        self,
        computed: crate::op_set2::change::CollectedHashes,
        authors: &mut Authors,
    ) -> Result<ChangeGraph<Full>, GraphLoadError> {
        debug_assert!(self.graph.hashes.len() == 0);
        let crate::op_set2::change::CollectedHashes {
            hashes, num_ops, ..
        } = computed;
        let mut graph = self
            .graph
            .with_hashes(Full(Vec::with_capacity(hashes.len())));
        graph.assign_authors(0..graph.len(), authors)?;
        debug_assert_eq!(hashes.len(), graph.len());

        // The encoded change columns only contain each change's maximum op.
        // `load()` estimates op counts from dependencies, but that is ambiguous
        // for an isolated actor whose first change can start above counter 1.
        // Reconstruction counted each change's ops, so use those.
        graph.num_ops = num_ops.into_iter().collect();

        for (i, hash) in hashes.into_iter().enumerate() {
            let node_idx = NodeIdx(i as u32);
            graph.nodes_by_hash.insert(hash, node_idx);
            graph.hashes.push(hash);
        }

        // the header's heads are untrusted
        let has_child = graph.has_child_mask();
        graph.heads = (0..graph.len() as u32)
            .filter(|n| !has_child[*n as usize])
            .filter_map(|n| graph.hashes.get(NodeIdx(n)))
            .collect();

        graph.cache_clocks_from(0);

        graph.cache_fragments();

        Ok(graph)
    }

    pub(crate) fn load(doc: &Document<'_>, actor_table: &ActorTable) -> Result<Self, LoadError> {
        use ids::*;

        let num_actors = actor_table.len();
        let meta = doc.change_meta();
        let bytes = doc.change_bytes();

        let actor_bytes = meta.bytes(ACTOR_COL_SPEC, bytes);
        let seq_bytes = meta.bytes(SEQ_COL_SPEC, bytes);
        let max_op_bytes = meta.bytes(MAX_OP_COL_SPEC, bytes);
        let time_bytes = meta.bytes(TIME_COL_SPEC, bytes);
        let message_bytes = meta.bytes(MESSAGE_COL_SPEC, bytes);
        let deps_count_bytes = meta.bytes(DEPS_COUNT_COL_SPEC, bytes);
        let deps_val_bytes = meta.bytes(DEPS_VAL_COL_SPEC, bytes);
        let extra_meta_bytes = meta.bytes(EXTRA_META_COL_SPEC, bytes);

        let extra_bytes_raw = meta.bytes(EXTRA_VAL_COL_SPEC, bytes).to_vec();

        // the decoders below panic on malformed bytes
        fn check<T: hexane::ColumnValueRef>(bytes: &[u8]) -> Result<(), hexane::PackError> {
            use hexane::ColumnEncoding;
            T::Encoding::<hexane::Leb128>::validate_encoding(bytes).map(|_| ())
        }
        check::<ActorIdx>(actor_bytes)?;
        check::<u32>(deps_count_bytes)?;
        for delta in [max_op_bytes, seq_bytes, deps_val_bytes] {
            check::<i64>(delta)?;
        }

        let actors: ActorRefs<Vec<ActorIdx>> =
            ActorRefs(hexane::decoder::<ActorIdx>(actor_bytes).collect());
        let max_ops: Vec<u32> = hexane::DeltaDecoder::<u32>::new(max_op_bytes).collect();
        let max_op = max_ops.iter().copied().max().unwrap_or(0);
        let seq: Vec<u32> = hexane::DeltaDecoder::<u32>::new(seq_bytes).collect();

        if let Some(a) = actors.iter().copied().map(usize::from).max() {
            if a >= num_actors {
                return Err(LoadError::InvalidActorId(a));
            }
        }

        let len = actors.len();

        let opts = hexane::LoadOpts::new().with_length(len);

        let timestamps = hexane::DeltaColumn::<i64>::load_with(time_bytes, opts.with_fill(0i64))?;
        let messages =
            hexane::Column::<Option<String>>::load_with(message_bytes, opts.with_fill(None))?;
        let extra_bytes_meta =
            hexane::PrefixColumn::<ValueMeta>::load_with(extra_meta_bytes, opts)?;

        if max_ops.len() != len {
            return Err(LoadError::InvalidColumnLength(MAX_OP_COL_SPEC));
        }
        if seq.len() != len {
            return Err(LoadError::InvalidColumnLength(SEQ_COL_SPEC));
        }
        if timestamps.len() != len {
            return Err(LoadError::InvalidColumnLength(TIME_COL_SPEC));
        }
        if messages.len() != len {
            return Err(LoadError::InvalidColumnLength(MESSAGE_COL_SPEC));
        }

        let mut seq_index: ActorIndexed<Vec<NodeIdx>> = ActorIndexed::new(actor_table);
        for (i, actor) in actors.iter().enumerate() {
            let actor = actor.0 as usize;
            seq_index.as_mut_slice()[actor].push(NodeIdx(i as u32));
        }

        let mut dep_range: Vec<(u32, u32)> = Vec::with_capacity(len);
        let mut dep_target: Vec<NodeIdx> = Vec::new();

        let deps_count: Vec<u32> = hexane::decoder::<u32>(deps_count_bytes).collect();
        let mut deps_val_iter = hexane::DeltaDecoder::<u32>::new(deps_val_bytes);

        let mut num_ops_vec = Vec::with_capacity(len);
        for (i, d) in deps_count.iter().enumerate() {
            let d = *d as usize;
            if d == 0 {
                num_ops_vec.push(max_ops[i] as u64);
                dep_range.push((dep_target.len() as u32, 0));
                continue;
            }

            let off = dep_target.len() as u32;
            let mut last_max_op = 0;
            for _ in 0..d {
                let dep = deps_val_iter
                    .next()
                    .ok_or(LoadError::InvalidColumnLength(DEPS_VAL_COL_SPEC))?;
                // untrusted input: everything downstream assumes parents precede children
                if dep as usize >= i {
                    return Err(LoadError::InvalidDepIndex);
                }
                last_max_op = std::cmp::max(last_max_op, max_ops[dep as usize]);
                dep_target.push(NodeIdx(dep));
            }
            dep_range.push((off, d as u32));
            if last_max_op > max_ops[i] {
                return Err(LoadError::InvalidMaxOp);
            }
            num_ops_vec.push(max_ops[i] as u64 - last_max_op as u64);
        }
        let num_ops: hexane::Column<u64> = num_ops_vec.into_iter().collect();

        let heads = doc.heads().iter().copied().collect();

        if dep_range.len() != len {
            return Err(LoadError::InvalidColumnLength(DEPS_COUNT_COL_SPEC));
        }

        Ok(ChangeGraphCols {
            graph: ChangeGraph {
                actors,
                dep_range,
                dep_target,
                seq,
                max_ops,
                max_op,
                num_ops,
                timestamps,
                messages,
                extra_bytes_meta,
                extra_bytes_raw,
                heads,
                seq_index,
                ..ChangeGraph::new(actor_table)
            },
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep;

#[derive(Debug, thiserror::Error)]
#[error("cannot write bundle: {count} {what} have no retained hash")]
pub struct UnretainedHashes {
    count: usize,
    what: &'static str,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AddChangeError {
    #[error(transparent)]
    MissingDep(#[from] MissingDep),
    #[error(transparent)]
    Unchecked(#[from] UncheckedHashes),
    #[error(transparent)]
    Author(#[from] GraphLoadError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        make_rng,
        op_set2::{change::build_change, op_set::ResolvedAction, OpSet, TxOp},
        tx::Transactable,
        types::{ObjMeta, OpId, OpType},
        ActorId, TextEncoding, ROOT,
    };

    use crate::autocommit::AutoCommit;

    use crate::automerge::Automerge;
    use rand::RngExt;

    use super::*;

    #[test]
    fn cache_clocks_sweep_matches_backward_walk() {
        let mut builder = TestGraphBuilder::new();
        let a = builder.actor();
        let b = builder.actor();
        let c = builder.actor();

        // long single-actor chains exercise the row-reuse path
        let mut last_a = builder.change(&a, 1, &[]);
        let mut last_b = builder.change(&b, 1, &[]);
        for i in 0..20 {
            last_a = builder.change(&a, 1, &[last_a, last_b]);
            last_b = builder.change(&b, 1, &[last_b, last_a]);
            if i % 5 == 0 {
                for _ in 0..7 {
                    last_a = builder.change(&a, 1, &[last_a]);
                }
            }
        }
        let mut last_c = builder.change(&c, 1, &[last_a, last_b]);
        for _ in 0..20 {
            last_c = builder.change(&c, 1, &[last_c]);
        }

        let graph = builder.build();
        assert!(graph.len() > 2 * CACHE_STEP as usize);

        let mut swept = graph.clone();
        swept.clock_cache.clear();
        swept.cache_clocks_from(0);

        let mut bare = graph.clone();
        bare.clock_cache.clear();

        assert_eq!(swept.clock_cache.len(), graph.len() / CACHE_STEP as usize);
        for (idx, clock) in &swept.clock_cache {
            assert_eq!((idx.0 + 1) % CACHE_STEP, 0);
            assert_eq!(clock, &bare.calculate_clock(vec![*idx]), "node {idx:?}");
        }
    }

    #[test]
    fn clock_by_heads() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let change4 = builder.change(&actor1, 10, &[change2, change3]);
        let graph = builder.build();

        // todo - why 4?
        let mut expected_clock = SeqClock::new(&builder.actors);
        expected_clock.include(builder.index(&actor1), Some(2));
        expected_clock.include(builder.index(&actor2), Some(1));
        expected_clock.include(builder.index(&actor3), Some(1));

        let clock = graph.seq_clock_for_heads(&[change4]).unwrap();
        assert_eq!(clock, expected_clock);
    }

    /// An author footer on a seq!=1 change must fail the load, as the same
    /// change applied directly does: skipping it would leave the ops
    /// unattributed, so masking could not hide them. The public API cannot
    /// produce such bytes, so the footer is forged into the graph's columns.
    #[test]
    fn finalize_rejects_author_footer_at_non_initial_seq() {
        use crate::{Author, ExpandedChange};

        let mut doc = AutoCommit::new();
        doc.put(ROOT, "k1", "v1").unwrap();
        doc.commit();
        doc.put(ROOT, "k2", "v2").unwrap();
        doc.commit();
        let mut changes = doc.get_changes(&[]).unwrap();
        changes.sort_by_key(Change::seq);
        let author = Author::try_from("ffff").unwrap();
        let mut expanded: ExpandedChange = (&changes[1]).into();
        expanded.extra_bytes = vec![1, author.as_bytes().len() as u8];
        expanded.extra_bytes.extend_from_slice(author.as_bytes());
        let forged: Change = expanded.into();
        assert!(forged.author().is_some());
        let forged_changes = vec![changes[0].clone(), forged];

        let actors = ActorTable::from_sorted(vec![changes[0].actor_id().clone()]).unwrap();
        let mut graph: ChangeGraph = ChangeGraph::new(&actors);
        graph.add_nodes(forged_changes.iter().map(|c| (c, 0usize)));
        let cols = ChangeGraphCols { graph };

        let mut authors = Authors::new(&actors);
        let computed = crate::op_set2::change::CollectedHashes {
            hashes: forged_changes.iter().map(Change::hash).collect(),
            num_ops: forged_changes.iter().map(|c| c.len() as u64).collect(),
            heads: Default::default(),
        };
        let err = cols
            .finalize(computed, &mut authors)
            .expect_err("an author footer at seq 2 must fail the load");
        assert!(
            matches!(
                err,
                GraphLoadError::AuthorOnNonInitialSeq { seq: 2, actor: 0 }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn remove_ancestors() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let change4 = builder.change(&actor1, 10, &[change2, change3]);
        let graph = builder.build();

        let mut changes = vec![change1, change2, change3, change4]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let heads = vec![change2];
        graph.remove_ancestors(&mut changes, &heads).unwrap();

        let expected_changes = vec![change3, change4].into_iter().collect::<BTreeSet<_>>();

        assert_eq!(changes, expected_changes);
    }

    struct TestGraphBuilder {
        actors: ActorTable,
        changes: Vec<Change>,
        graph: ChangeGraph<Full>,
        seqs_by_actor: BTreeMap<ActorId, u64>,
        rng: rand::rngs::SmallRng,
    }

    impl TestGraphBuilder {
        fn new() -> Self {
            // random hashes can form fragments, which would free hashes the tests resolve
            let actors = ActorTable::new();
            let graph = ChangeGraph::<Full>::new(&actors);
            TestGraphBuilder {
                actors,
                changes: Vec::new(),
                graph,
                seqs_by_actor: BTreeMap::new(),
                rng: crate::make_rng(),
            }
        }

        fn actor(&mut self) -> ActorId {
            use rand::RngExt;
            let actor = ActorId::from(self.rng.random::<[u8; 16]>().to_vec());
            match self.actors.insert(actor.clone()) {
                crate::actor::ActorInsert::Inserted(shift) => self.graph.insert_actor(&shift),
                crate::actor::ActorInsert::Existing(_) => panic!("random actor already present"),
            }
            actor
        }

        fn index(&self, actor: &ActorId) -> usize {
            self.actors.lookup(actor).unwrap()
        }

        /// Create a change with `num_new_ops` and `parents` for `actor`
        ///
        /// The `start_op` and `seq` of the change will be computed from the
        /// previous changes for the same actor.
        fn change(
            &mut self,
            actor: &ActorId,
            num_new_ops: usize,
            parents: &[ChangeHash],
        ) -> ChangeHash {
            let osd = OpSet::from_actors(self.actors.to_vec(), TextEncoding::platform_default());

            let start_op = parents
                .iter()
                .map(|c| {
                    self.changes
                        .iter()
                        .find(|change| change.hash() == *c)
                        .unwrap()
                        .max_op()
                })
                .max()
                .unwrap_or(0)
                + 1;

            let actor_idx = self.index(actor);
            let ops = (0..num_new_ops)
                .map(|opnum| {
                    TxOp::map(
                        OpId::new(start_op + opnum as u64, actor_idx),
                        ObjMeta::root(),
                        0,
                        ResolvedAction::VisibleUpdate(OpType::Put("value".into())),
                        "key".to_string(),
                        vec![],
                    )
                })
                .collect::<Vec<_>>();

            let timestamp = 0;
            let seq = self.seqs_by_actor.entry(actor.clone()).or_insert(1);
            let meta = BuildChangeMetadata {
                actor: actor_idx,
                builder: 0,
                deps: parents
                    .iter()
                    .map(|h| self.graph.hash_to_index(h).unwrap() as u64)
                    .collect(),
                seq: *seq,
                max_op: start_op + ops.len() as u64 - 1,
                start_op,
                timestamp,
                message: None,
                extra: Cow::Owned(vec![]),
            };
            let change = Change::new(build_change(&ops, &meta, &self.graph, &osd.actors));
            *seq = seq.checked_add(1).unwrap();
            let hash = change.hash();
            self.graph
                .add_change(&change, actor_idx, &mut Authors::new(&self.actors))
                .unwrap();
            self.changes.push(change);
            hash
        }

        fn build(&self) -> ChangeGraph<Full> {
            let mut graph = ChangeGraph::<Full>::new(&self.actors);
            let mut authors = Authors::new(&self.actors);
            for change in &self.changes {
                let actor_idx = self.index(change.actor_id());
                graph.add_change(change, actor_idx, &mut authors).unwrap();
            }
            graph
        }

        fn all_hashes(&self) -> Vec<ChangeHash> {
            self.changes.iter().map(|c| c.hash()).collect()
        }

        fn all_change_ids(&self) -> Vec<ChangeId> {
            self.changes
                .iter()
                .map(|c| ChangeId::from_doc_seq(c.seq(), c.actor_id().clone(), 0))
                .collect()
        }

        fn hash_of(&self) -> BTreeMap<(ActorId, u64), ChangeHash> {
            self.changes
                .iter()
                .map(|c| ((c.actor_id().clone(), c.seq()), c.hash()))
                .collect()
        }
    }

    #[test]
    fn member_authors_rejects_a_second_author() {
        let actors = ActorTable::from_actors([ActorId::from(&[1][..]), ActorId::from(&[2][..])]);
        let x = crate::Author::from(vec![1, 1]);
        let y = crate::Author::from(vec![2, 2]);
        let footer = |a: &crate::Author<'static>| {
            crate::change::encode_author_footer(&Some(a.clone())).into_owned()
        };
        let (fx, fy) = (footer(&x), footer(&y));

        // non-footer extra bytes are not claims
        let authors = Authors::with_actors(2);
        let ok = [(0, 1, &fx[..]), (1, 1, &fy[..]), (0, 2, &[9, 9][..])];
        <ChangeGraph>::member_authors(ok.into_iter(), &authors, &actors).unwrap();

        // the actor already has an author
        let mut authors = Authors::with_actors(2);
        authors.assign_author(x.clone(), 0);
        let err = <ChangeGraph>::member_authors([(0, 2, &fy[..])].into_iter(), &authors, &actors)
            .unwrap_err();
        assert!(
            matches!(err, AutomergeError::DuplicateAuthor(a, actor, 2) if a == y && actor == actors[0])
        );

        // two members of the bundle claim the same actor
        let authors = Authors::with_actors(2);
        let twice = [(1, 1, &fx[..]), (1, 2, &fy[..])];
        let err = <ChangeGraph>::member_authors(twice.into_iter(), &authors, &actors).unwrap_err();
        assert!(matches!(err, AutomergeError::DuplicateAuthor(_, actor, 2) if actor == actors[1]));
    }

    fn member_hash(hash_of: &BTreeMap<(ActorId, u64), ChangeHash>, id: &ChangeId) -> ChangeHash {
        hash_of[&(id.actor().clone(), id.seq())]
    }

    #[test]
    fn fragments_cover_all_changes() {
        // Create a long linear chain — with ~1000 changes, we expect several
        // with fragment_level >= 1 (roughly 1 in 256).
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let all_ids: BTreeSet<_> = builder
            .all_change_ids()
            .into_iter()
            .map(|id| (id.actor().clone(), id.seq()))
            .collect();
        let heads: Vec<_> = graph.heads().collect();

        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor().clone(), m.seq()));
            }
        }

        // Every change must appear in at least one fragment
        let missing: Vec<_> = all_ids.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );
    }

    fn assert_fragment_invariants(
        fragments: &[Fragment],
        hash_of: &BTreeMap<(ActorId, u64), ChangeHash>,
    ) {
        for f in fragments {
            // level must match the fragment_level of the id hash
            assert_eq!(
                f.level,
                f.head.fragment_level(),
                "fragment level mismatch for {:?}",
                f.head
            );

            // id must be in members
            assert!(
                f.members.iter().any(|m| member_hash(hash_of, m) == f.head),
                "fragment id {:?} not found in its own members",
                f.head
            );

            // Checkpoints must exclude both the head and the boundary.
            assert!(!f.checkpoints.contains(&f.head));
            for boundary in &f.boundary {
                assert!(!f.checkpoints.contains(boundary));
            }
            for checkpoint in &f.checkpoints {
                assert!(f
                    .members
                    .iter()
                    .any(|m| member_hash(hash_of, m) == *checkpoint));
                assert!(checkpoint.fragment_level() > 0);
            }

            // deps must be equal or higher level than the fragment
            for dep in &f.boundary {
                assert!(
                    dep.fragment_level() >= f.level,
                    "fragment {:?} (level {}) has dep {:?} with lower level {}",
                    f.head,
                    f.level,
                    dep,
                    dep.fragment_level(),
                );
            }

            // members must not contain a change with a higher level than the id
            for m in &f.members {
                let h = member_hash(hash_of, m);
                assert!(
                    h.fragment_level() <= f.level,
                    "fragment {:?} (level {}) contains {:?} with higher level {}",
                    f.head,
                    f.level,
                    h,
                    h.fragment_level(),
                );
            }
        }
    }

    #[test]
    fn fragment_id_and_level_consistent() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

        assert_fragment_invariants(&fragments, &builder.hash_of());
    }

    #[test]
    fn fragment_checkpoints_exclude_head_and_boundary() {
        // Fix the actor and change contents so fragment levels are deterministic.
        let mut doc = AutoCommit::new().with_actor(ActorId::from(&[1][..]));
        for value in 0..1500 {
            doc.put(ROOT, "counter", value).unwrap();
            doc.commit();
        }

        let fragments = doc.fragments(1..);
        assert!(!fragments.is_empty(), "expected bundled fragments");
        assert!(
            fragments.iter().any(|f| !f.boundary.is_empty()),
            "expected a fragment with a boundary to exercise boundary exclusion",
        );
        for fragment in fragments {
            let head_id = doc.hash_to_change_id(&fragment.head).unwrap().unwrap();
            assert!(fragment.members.contains(&head_id));
            assert!(
                !fragment.checkpoints.contains(&fragment.head),
                "fragment {:?} contains its own head as a checkpoint",
                fragment.head,
            );
            for boundary in &fragment.boundary {
                assert!(
                    !fragment.checkpoints.contains(boundary),
                    "fragment {:?} contains boundary {:?} as a checkpoint",
                    fragment.head,
                    boundary,
                );
            }
            assert_eq!(doc.get_fragment(fragment.head), Some(fragment));
        }
    }

    #[test]
    fn fragments_work_with_concurrent_actors() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();

        // Build two concurrent chains that merge periodically
        let root = builder.change(&actor1, 1, &[]);
        let mut tip1 = root;
        let mut tip2 = root;
        for i in 0..500 {
            tip1 = builder.change(&actor1, 1, &[tip1]);
            tip2 = builder.change(&actor2, 1, &[tip2]);
            if i % 50 == 49 {
                // merge
                let merge = builder.change(&actor1, 1, &[tip1, tip2]);
                tip1 = merge;
                tip2 = merge;
            }
        }
        let graph = builder.build();
        let all_ids: BTreeSet<_> = builder
            .all_change_ids()
            .into_iter()
            .map(|id| (id.actor().clone(), id.seq()))
            .collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor().clone(), m.seq()));
            }
        }

        let missing: Vec<_> = all_ids.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );

        assert_fragment_invariants(&fragments, &builder.hash_of());
    }

    /// Concurrent branches are what make apply order non-trivial.
    #[test]
    fn fragments_are_returned_in_apply_order() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();

        // long enough for a branch to grow its own fragments before the merge
        let root = builder.change(&actor1, 1, &[]);
        let mut tips = [root, root, root];
        for i in 0..1_200 {
            tips[0] = builder.change(&actor1, 1, &[tips[0]]);
            tips[1] = builder.change(&actor2, 1, &[tips[1]]);
            tips[2] = builder.change(&actor3, 1, &[tips[2]]);
            if i % 400 == 399 {
                let merge = builder.change(&actor1, 1, &tips);
                tips = [merge, merge, merge];
            }
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();
        let fragments = graph.fragments(&heads, .., &builder.actors);
        let hash_of = builder.hash_of();

        let mut applied: BTreeSet<ChangeHash> = BTreeSet::new();
        for f in &fragments {
            let members: BTreeSet<ChangeHash> =
                f.members.iter().map(|m| member_hash(&hash_of, m)).collect();
            for dep in &f.boundary {
                assert!(
                    applied.contains(dep) || members.contains(dep),
                    "fragment {:?} applies before its boundary {:?}",
                    f.head,
                    dep,
                );
            }
            for m in &members {
                for d in graph.deps(m) {
                    let d = d.unwrap();
                    assert!(
                        members.contains(&d) || applied.contains(&d),
                        "fragment {:?} member {:?} applies before its dep {:?}",
                        f.head,
                        m,
                        d,
                    );
                }
            }
            applied.extend(members);
        }
        assert_eq!(
            applied,
            builder.all_hashes().into_iter().collect::<BTreeSet<_>>(),
            "fragments do not cover every change",
        );
    }

    #[test]
    fn fragment_deps_reference_known_hashes() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let all_hashes: BTreeSet<_> = builder.all_hashes().into_iter().collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);
        let fragment_ids: BTreeSet<_> = fragments.iter().map(|f| f.head).collect();

        for f in &fragments {
            for dep in &f.boundary {
                assert!(
                    all_hashes.contains(dep),
                    "fragment {:?} has dep {:?} not in change graph",
                    f.head,
                    dep
                );
                // Deps of cached fragments (level > 0) should point to other fragment ids
                // Deps of loose fragments (level == 0) point to change-level parents
                if f.level > 0 {
                    assert!(
                        fragment_ids.contains(dep) || dep.fragment_level() == 0,
                        "cached fragment {:?} has dep {:?} that is not a fragment id",
                        f.head,
                        dep
                    );
                }
            }
        }
    }

    #[test]
    fn fragments_filtered_by_levels() {
        // 5000 changes gives ~20 expected level-1 fragments (1 hash in 256)
        // so seeing zero cached fragments would be extraordinarily unlikely.
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..5000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();

        let all: Vec<_> = graph.fragments(&heads, .., &builder.actors);
        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);

        // loose + cached partition the full range
        assert_eq!(loose.len() + cached.len(), all.len());
        assert!(
            !cached.is_empty(),
            "expected at least one cached fragment from 5000 changes",
        );
        // ~1 run in 256 the head hash itself has a leading zero byte:
        // the head is then a cached fragment head, every change is
        // covered, and an empty loose set is the correct answer
        if heads.iter().all(|h| h.fragment_level() > 0) {
            assert!(loose.is_empty(), "cached head must cover everything");
        } else {
            assert!(!loose.is_empty());
        }

        for f in &loose {
            assert_eq!(f.level, 0, "0..=0 returned a non-zero level fragment");
        }
        for f in &cached {
            assert!(f.level >= 1, "1.. returned a level-0 fragment");
        }

        // empty range yields nothing
        assert_eq!(graph.fragments(&heads, 0..0, &builder.actors).len(), 0);
    }

    #[test]
    fn fragments_with_cached_head() {
        // force the rare branch the two tests above only hit by luck:
        // rebuild small graphs until the head hash has a leading zero
        // byte (expected ~256 tries), then loose must be empty and the
        // cached fragments must cover every change
        for _ in 0..10_000 {
            let mut builder = TestGraphBuilder::new();
            let actor = builder.actor();
            let mut prev = vec![];
            for _ in 0..8 {
                let h = builder.change(&actor, 1, &prev);
                prev = vec![h];
            }
            if prev[0].fragment_level() == 0 {
                continue;
            }
            let graph = builder.build();
            let heads: Vec<_> = graph.heads().collect();

            let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
            assert!(loose.is_empty(), "cached head must cover everything");

            let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);
            let hash_of = builder.hash_of();
            let covered: BTreeSet<ChangeHash> = cached
                .iter()
                .flat_map(|f| f.members.iter().map(|m| member_hash(&hash_of, m)))
                .collect();
            let all: BTreeSet<ChangeHash> = builder.all_hashes().into_iter().collect();
            assert_eq!(covered, all, "cached fragments must cover all changes");
            return;
        }
        panic!("no level>=1 head hash in 10k tries (p ~ 1e-17)");
    }

    #[test]
    fn get_fragment_returns_loose_and_cached() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..5000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();

        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);
        assert!(!cached.is_empty(), "expected at least one cached fragment");

        // get_fragment on a loose (level 0) commit hash returns an
        // equivalent Fragment. Loose can be legitimately empty (~1 run
        // in 256) when the head hash itself is a fragment head
        if let Some(l) = loose.first() {
            let got = graph.get_fragment(l.head, &builder.actors).unwrap();
            assert_eq!(got, *l);
        } else {
            assert!(heads.iter().all(|h| h.fragment_level() > 0));
        }

        // get_fragment on a cached (level >= 1) fragment id returns an equivalent Fragment
        let c = &cached[0];
        let got = graph.get_fragment(c.head, &builder.actors).unwrap();
        assert_eq!(got, *c);

        // unknown hash returns None
        assert!(graph
            .get_fragment(ChangeHash([0xff; 32]), &builder.actors)
            .is_none());
    }

    #[test]
    fn bundles_for_fragments_roundtrips_through_load_incremental() {
        let mut rng = make_rng();
        let mut doc = Automerge::new();

        for _ in 0..1_000 {
            let key = format!("k{}", rng.random::<u32>() % 32);
            let value = (rng.random::<u32>() % 1000) as i64;
            let mut tx = doc.transaction();
            tx.put(ROOT, key, value).unwrap();
            tx.commit();
        }

        let fragments = doc.fragments(..);

        let bundles = doc.bundles_for_fragments(fragments).unwrap();

        let joined: Vec<u8> = bundles.into_iter().flatten().collect();

        let mut loaded = AutoCommit::new();
        loaded.load_incremental(&joined).unwrap();

        assert_eq!(doc.get_heads(), loaded.get_heads());

        let a = doc.save();
        let b = loaded.save();
        assert_eq!(a, b);
    }

    /// `doc.bundle_after(heads)` applied one bundle at a time to `at`, a
    /// save of `doc` at `heads`: each must apply as it arrives, no
    /// fragment may lie inside another, and the result must hold `doc`'s
    /// changes, as an audit-mode load confirms.
    /// Returns how many fragments straddle `heads`.
    fn assert_bundle_after_rebuilds(doc: &Automerge, heads: &[ChangeId], at: &[u8]) -> usize {
        let fragments = doc.fragments_after(heads).unwrap();
        let bundles = doc.bundle_after(heads).unwrap();
        assert_eq!(
            bundles,
            doc.bundles_for_fragments(fragments.clone()).unwrap()
        );
        for f in &fragments {
            let inside = fragments
                .iter()
                .any(|g| g.head != f.head && f.members.iter().all(|m| g.members.contains(m)));
            assert!(!inside, "{f:?} lies inside another fragment");
        }
        let mut fork = Automerge::load(at).unwrap();
        let straddling = fragments
            .iter()
            .filter(|f| f.members.iter().any(|m| fork.has_change_id(m)))
            .count();
        assert_eq!(fork.get_heads(), {
            let mut h = heads.to_vec();
            h.sort();
            h
        });
        for (f, bundle) in fragments.iter().zip(&bundles) {
            fork.load_incremental(bundle).unwrap();
            assert!(
                fork.has_change(&f.head).unwrap(),
                "{f:?} waits on a later one"
            );
        }
        assert_eq!(fork.get_heads(), doc.get_heads());
        assert_eq!(fork.hydrate(None).unwrap(), doc.hydrate(None).unwrap());
        let audited = Automerge::load(&fork.save())
            .unwrap()
            .enable_audit_mode()
            .unwrap();
        assert_eq!(audited.get_heads(), doc.get_heads());
        straddling
    }

    #[test]
    fn bundle_after_rebuilds_the_document_from_any_heads() {
        let mut rng = make_rng();
        let mut doc = Automerge::new();
        let cuts = [1, 300, 999, 1_500, 1_999];
        let mut snapshots = vec![(vec![], vec![])];
        for i in 1..=2_000 {
            let key = format!("k{}", rng.random::<u32>() % 32);
            let mut tx = doc.transaction();
            tx.put(ROOT, key, rng.random::<u32>() as i64).unwrap();
            tx.commit();
            if cuts.contains(&i) {
                snapshots.push((doc.get_heads(), doc.save()));
            }
        }
        assert!(
            doc.fragments(1..).len() > 2,
            "the history spans several fragments"
        );
        let straddling: usize = snapshots
            .iter()
            .map(|(heads, at)| assert_bundle_after_rebuilds(&doc, heads, at))
            .sum();
        assert!(straddling > 0, "a fragment straddling heads is sent whole");
        assert!(doc.fragments_after(&doc.get_heads()).unwrap().is_empty());
        assert!(doc.bundle_after(&doc.get_heads()).unwrap().is_empty());
    }

    #[test]
    fn bundle_after_brings_in_a_concurrent_branch() {
        let mut rng = make_rng();
        let mut doc = Automerge::new();
        for i in 0..600 {
            let mut tx = doc.transaction();
            tx.put(ROOT, "base", i as i64).unwrap();
            tx.commit();
        }
        let mut other = doc.fork();
        for doc in [&mut doc, &mut other] {
            for _ in 0..700 {
                let key = format!("k{}", rng.random::<u32>() % 32);
                let mut tx = doc.transaction();
                tx.put(ROOT, key, rng.random::<u32>() as i64).unwrap();
                tx.commit();
            }
        }
        let (ours, ours_at) = (doc.get_heads(), doc.save());
        let (theirs, theirs_at) = (other.get_heads(), other.save());
        doc.merge(&mut other).unwrap();
        let straddling = assert_bundle_after_rebuilds(&doc, &ours, &ours_at)
            + assert_bundle_after_rebuilds(&doc, &theirs, &theirs_at);
        assert!(straddling > 0, "a fragment straddling heads is sent whole");
    }

    /// A loose commit whose parent lies in a fragment whose head is not
    /// its ancestor must still be listed after that fragment.
    #[test]
    fn fragments_after_applies_one_at_a_time() {
        let mut b = AutoCommit::new().with_actor(crate::ActorId::from(&[1u8][..]));
        b.put(ROOT, "p", 0).unwrap();
        b.commit();
        let mut a = b.fork().with_actor(crate::ActorId::from(&[2u8][..]));
        let mut i = 0;
        while a.document().get_head_hashes()[0].fragment_level() == 0 {
            a.put(ROOT, "a", i).unwrap();
            a.commit();
            i += 1;
        }
        b.put(ROOT, "loose", 0).unwrap();
        b.commit();
        b.merge(&mut a).unwrap();
        let doc = b.document();
        let fragments = doc.fragments_after(&[]).unwrap();
        assert!(fragments.iter().any(|f| f.level > 0));
        let mut z = Automerge::new();
        for (i, bundle) in doc.bundle_after(&[]).unwrap().iter().enumerate() {
            z.load_incremental(bundle)
                .unwrap_or_else(|e| panic!("bundle {i} applied out of order: {e}"));
        }
        assert_eq!(z.get_heads(), doc.get_heads());
    }

    #[test]
    fn exports_do_not_depend_on_arrival_order() {
        let mut rng = make_rng();
        let mut base = AutoCommit::new().with_actor(crate::ActorId::from(&[0u8][..]));
        let text = base.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        for _ in 0..50 {
            base.splice_text(&text, 0, 0, "ab").unwrap();
            base.commit();
        }
        let mut branches: Vec<AutoCommit> = (1..=3u8)
            .map(|a| base.fork().with_actor(crate::ActorId::from(&[a][..])))
            .collect();
        let (mut x, mut y) = (base.fork(), base.fork());
        for round in 0..4 {
            for b in branches.iter_mut() {
                for i in 0..40 {
                    let len = crate::read::ReadDoc::length(b, &text);
                    let pos = rng.random::<u32>() as usize % (len + 1);
                    if i % 3 == 0 && len > 0 {
                        b.splice_text(&text, pos.min(len - 1), 1, "").unwrap();
                    } else {
                        b.splice_text(&text, pos, 0, "x").unwrap();
                    }
                    b.put(ROOT, format!("k{}", i % 7), i as i64).unwrap();
                    b.commit();
                }
            }
            for i in [0, 1, 2] {
                x.merge(&mut branches[i]).unwrap();
            }
            for i in [2, 1, 0] {
                y.merge(&mut branches[i]).unwrap();
            }
            // some branches see each other between rounds
            let (a, b) = branches.split_at_mut(round % 2 + 1);
            a[0].merge(&mut b[0]).unwrap();
        }
        let (x, y) = (x.document(), y.document());
        assert_eq!(x.get_heads(), y.get_heads());
        let by_head = |doc: &Automerge| {
            let mut f = doc.fragments(..);
            f.sort_by_key(|f| (f.head, f.level));
            f
        };
        let (fx, fy) = (by_head(x), by_head(y));
        assert_eq!(fx.len(), fy.len());
        assert!(fx.len() > 1);
        let bx = x.bundles_for_fragments(fx).unwrap();
        let by = y.bundles_for_fragments(fy).unwrap();
        for (i, (a, b)) in bx.iter().zip(&by).enumerate() {
            assert!(a == b, "fragment {i} differs");
        }
        assert!(x.save() == y.save(), "saves differ");
        // list order follows node order, which arrival order may change
        let sorted = |mut v: Vec<Vec<u8>>| {
            v.sort();
            v
        };
        assert!(
            sorted(x.bundle_after(&[]).unwrap()) == sorted(y.bundle_after(&[]).unwrap()),
            "bundle_after differs"
        );
        let after = |doc: &Automerge| {
            let mut f = doc.fragments_after(&[]).unwrap();
            f.sort_by_key(|f| (f.head, f.level));
            for f in &mut f {
                f.members.sort_by_key(|m| (m.actor().clone(), m.seq()));
            }
            f
        };
        assert_eq!(after(x), after(y));
        for doc in [x, y] {
            let mut z = Automerge::new();
            for (i, b) in doc.bundle_after(&[]).unwrap().iter().enumerate() {
                z.load_incremental(b)
                    .unwrap_or_else(|e| panic!("bundle {i} applied out of order: {e}"));
            }
            assert_eq!(z.get_heads(), doc.get_heads());
        }
    }
}

impl ExactSizeIterator for ChangeIter<'_> {
    fn len(&self) -> usize {
        self.len - self.index
    }
}

pub(crate) struct ChangeIter<'a> {
    index: usize,
    actors: std::slice::Iter<'a, ActorIdx>,
    seq: std::slice::Iter<'a, u32>,
    max_ops: std::slice::Iter<'a, u32>,
    num_ops: hexane::Iter<'a, u64>,
    timestamps: hexane::DeltaIter<'a, i64>,
    messages: hexane::Iter<'a, Option<String>>,
    extra_bytes_meta: hexane::prefix::PrefixIter<'a, ValueMeta>,
    len: usize,
    extra_bytes_raw: &'a [u8],
    dep_range: &'a [(u32, u32)],
    dep_target: &'a [NodeIdx],
}

impl ChangeIter<'_> {
    fn deps(&self, i: usize) -> Vec<u64> {
        match self.dep_range.get(i) {
            Some(&(off, count)) => self.dep_target[off as usize..(off + count) as usize]
                .iter()
                .map(|n| n.0 as u64)
                .collect(),
            None => Vec::new(),
        }
    }
}

impl<'a> Iterator for ChangeIter<'a> {
    type Item = BuildChangeMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;
        self.index += 1;
        let actor = (*self.actors.next()?).into();
        let seq = *self.seq.next()? as u64;
        let max_op = *self.max_ops.next()? as u64;
        let num_ops = self.num_ops.next().unwrap_or_default();
        let timestamp = self.timestamps.next().unwrap_or_default();
        let message = self.messages.next().flatten().map(Cow::Borrowed);

        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.next()?;
        let meta_range = meta.prefix() as usize..meta.total() as usize;
        let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);
        let deps = self.deps(i);
        Some(BuildChangeMetadata {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            extra,
            deps,
            builder: 0,
        })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.index + n;
        self.index += n + 1;

        let actor = (*self.actors.nth(n)?).into();
        let seq = *self.seq.nth(n)? as u64;
        let max_op = *self.max_ops.nth(0)? as u64;
        let num_ops = self.num_ops.next().unwrap_or_default();
        let timestamp = self.timestamps.next().unwrap_or_default();
        let message = self.messages.next().flatten().map(Cow::Borrowed);

        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.delta_nth(n)?;
        let meta_start = meta.delta as usize;
        let meta_range = meta_start..(meta_start + meta.pv.value.length());
        let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

        let deps = self.deps(i);

        Some(BuildChangeMetadata {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            extra,
            deps,
            builder: 0,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
struct FragmentNode {
    head: NodeIdx,
    level: usize,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

/// Outside audit mode, how many holders need each node's hash. Level > 0
/// hashes, heads and actor tips are kept without pins.
#[derive(Debug, Clone, Default)]
struct Pins {
    count: SmallHashMap<NodeIdx, NonZeroU32>,
    /// Pinned since the last check that every pin has its hash.
    fresh: Vec<NodeIdx>,
    /// Hashes the next GC may free: learned, or unpinned to zero, since.
    candidates: Vec<NodeIdx>,
    /// Nodes from here on don't hold their loose pins yet (see
    /// [`ChangeGraph::pin_deferred_loose`]).
    deferred_from: Option<NodeIdx>,
    /// See [`ChangeGraph::take_unhashed_fragment_pins`].
    unhashed: Vec<NodeIdx>,
}

impl Pins {
    fn pin(&mut self, n: NodeIdx) {
        match self.count.entry(n) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() = e.get().checked_add(1).expect("pin count overflow");
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(NonZeroU32::MIN);
                self.fresh.push(n);
            }
        }
    }

    /// Panics if `n` isn't pinned.
    fn unpin(&mut self, n: NodeIdx) {
        let std::collections::hash_map::Entry::Occupied(mut e) = self.count.entry(n) else {
            panic!("unpinned {n:?}, which no holder pinned");
        };
        match NonZeroU32::new(e.get().get() - 1) {
            Some(c) => *e.get_mut() = c,
            None => {
                e.remove();
                self.candidates.push(n);
            }
        }
    }

    fn is_pinned(&self, n: NodeIdx) -> bool {
        self.count.contains_key(&n)
    }
}

/// What a bundle tells its receiver to keep.
pub(crate) struct ReceiverHashes {
    /// `(member index, hash)`
    pub(crate) retained: Vec<(usize, ChangeHash)>,
    /// Outside the bundle: what its fragments pin.
    pub(crate) outside: Vec<ChangeHash>,
}

impl FragmentNode {
    /// Also the apply order.
    fn sort_key(&self) -> (std::cmp::Reverse<usize>, NodeIdx) {
        (std::cmp::Reverse(self.level), self.head)
    }
}

/// EXPERIMENTAL: A section of the change graph identified by its head hash.
///
/// This is an experimental API, it may change or be removed without warning.
#[doc(hidden)]
#[derive(Debug, PartialEq, Clone)]
pub struct Fragment {
    pub head: ChangeHash,
    pub level: usize,
    pub boundary: Vec<ChangeHash>,
    /// Non-zero-level members of the fragment, excluding its head, sorted.
    pub checkpoints: Vec<ChangeHash>,
    /// By [`ChangeId`], since interior change hashes may have been freed.
    /// A set: their order depends on the order the changes arrived in.
    pub members: Vec<ChangeId>,
}

/// Equal values gathered into runs, so an encoder takes a run at a time.
struct Run<V>(Option<(V, usize)>);

impl<V: Copy + PartialEq> Run<V> {
    /// The run `value` ends, if it starts a new one.
    fn push(&mut self, value: V) -> Option<(V, usize)> {
        match &mut self.0 {
            Some((v, n)) if *v == value => {
                *n += 1;
                None
            }
            run => run.replace((value, 1)),
        }
    }
}

fn covered_seq(clock: &SeqClock, actor: usize) -> usize {
    clock.get_for_actor(&actor).map_or(0, |s| s.get() as usize)
}

/// The maximal runs of consecutive indexes in `nodes`.
fn node_ranges(nodes: &[NodeIdx]) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    for n in nodes {
        let i = n.0 as usize;
        match ranges.last_mut() {
            Some(r) if r.end == i => r.end = i + 1,
            _ => ranges.push(i..i + 1),
        }
    }
    ranges
}

#[rustfmt::skip]
pub(crate) mod ids {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    const ACTOR_COL_ID: ColumnId = ColumnId::new(0);
    const SEQ_COL_ID: ColumnId = ColumnId::new(0);
    const MAX_OP_COL_ID: ColumnId = ColumnId::new(1);
    const TIME_COL_ID: ColumnId = ColumnId::new(2);
    const MESSAGE_COL_ID: ColumnId = ColumnId::new(3);
    const DEPS_COL_ID: ColumnId = ColumnId::new(4);
    const EXTRA_COL_ID: ColumnId = ColumnId::new(5);

    pub(super) const ACTOR_COL_SPEC:      ColumnSpec = ColumnSpec::new_actor(ACTOR_COL_ID);
    pub(super) const SEQ_COL_SPEC:        ColumnSpec = ColumnSpec::new_delta(SEQ_COL_ID);
    pub(super) const MAX_OP_COL_SPEC:     ColumnSpec = ColumnSpec::new_delta(MAX_OP_COL_ID);
    pub(super) const TIME_COL_SPEC:       ColumnSpec = ColumnSpec::new_delta(TIME_COL_ID);
    pub(super) const MESSAGE_COL_SPEC:    ColumnSpec = ColumnSpec::new_string(MESSAGE_COL_ID);
    pub(super) const DEPS_COUNT_COL_SPEC: ColumnSpec = ColumnSpec::new_group(DEPS_COL_ID);
    pub(super) const DEPS_VAL_COL_SPEC:   ColumnSpec = ColumnSpec::new_delta(DEPS_COL_ID);
    pub(super) const EXTRA_META_COL_SPEC: ColumnSpec = ColumnSpec::new_value_metadata(EXTRA_COL_ID);
    pub(super) const EXTRA_VAL_COL_SPEC:  ColumnSpec = ColumnSpec::new_value(EXTRA_COL_ID);
    // ColumnId 6 (a retired change-hash column) still parses and `validate`
    // filters it out: don't reuse it.
}
