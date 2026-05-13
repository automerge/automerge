use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::ops::Add;

use hexane::{
    ColGroupIter, ColumnCursor, ColumnData, ColumnDataIter, DeltaCursor, PackError, StrCursor,
    UIntCursor,
};

use crate::filter::{Filter, ResolvedRule, Rule};
use crate::storage::BundleMetadata;
use crate::{
    clock::{Clock as OpClock, SeqClock},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorCursor, ActorIdx, MetaCursor, ValueMeta},
    storage::columns::compression::Uncompressed,
    storage::columns::BadColumnLayout,
    storage::document::ReconstructError as LoadError,
    storage::{Columns, Document, RawColumn, RawColumns},
    types::{ActorId, Author, AuthorIdx, OpId},
    Change, ChangeHash,
};

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache

#[derive(Debug, PartialEq, Default, Clone)]
pub(crate) struct ChangeGraph {
    edges: Vec<Edge>,
    hashes: Vec<ChangeHash>,
    actors: Vec<ActorIdx>,
    authors: Vec<Author>,
    author: Option<AuthorIdx>,
    /// The visibility filter: which changes the document is willing to
    /// render. The rule set is the user-facing source of truth; whenever
    /// it changes the resolved per-actor mask in `resolved` is recomputed.
    rules: Filter,
    /// Materialised form of `rules`. Driven entirely by `rules` plus the
    /// graph's own state (which actors exist, which authors they carry,
    /// which heads have arrived); never mutated independently.
    resolved: ResolvedFilter,
    actor_author: Vec<Option<AuthorIdx>>,
    parents: Vec<Option<EdgeIdx>>,
    seq: Vec<u32>,
    max_ops: Vec<u32>,
    max_op: u32,
    num_ops: ColumnData<UIntCursor>,
    timestamps: ColumnData<DeltaCursor>,
    messages: ColumnData<StrCursor>,
    extra_bytes_meta: ColumnData<MetaCursor>,
    extra_bytes_raw: Vec<u8>,
    heads: BTreeSet<ChangeHash>,
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    clock_cache: HashMap<NodeIdx, SeqClock>,
    seq_index: Vec<Vec<NodeIdx>>,
}

/// Materialised form of [`Filter`]. The user-facing rule set lives on
/// [`ChangeGraph::rules`]; this struct carries the derived state that the
/// op-set index and slow read paths actually consume.
///
/// * `actor_rules` — the public filter's `actors: BTreeMap<ActorId, Rule>`
///   compiled to use [`ActorIdx`] keys. The change graph never needs the
///   `ActorId` table again after `set_filter`/`insert_actor` have run.
/// * `actor_mask` — for each actor, the largest seq that is still visible
///   (`None` means nothing visible). Missing keys mean the actor is
///   unrestricted. Drives the apply-time `filtered_out` flag on incoming
///   ops.
/// * `cached_clock` — the same information re-keyed from seq to op-counter,
///   ready to be passed to `OpSet::recompute_indexes` and to slow-path read
///   helpers that bypass the op-set index.
/// * `pending_heads` — heads referenced by an `AllowUpTo` rule that aren't
///   in the graph yet. Once such a head lands we re-derive the mask.
#[derive(Debug, PartialEq, Default, Clone)]
struct ResolvedFilter {
    actor_rules: HashMap<ActorIdx, Rule>,
    actor_mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    cached_clock: OpClock,
    pending_heads: HashSet<ChangeHash>,
}

impl ResolvedFilter {
    fn new(num_actors: usize) -> Self {
        Self {
            actor_rules: HashMap::new(),
            actor_mask: HashMap::new(),
            cached_clock: OpClock(vec![u32::MAX; num_actors]),
            pending_heads: HashSet::new(),
        }
    }

    /// Whether `(actor, seq)` is rejected by the filter. Used at apply time
    /// to stamp the `filtered_out` flag on freshly-imported ops, and on
    /// slow read paths that have an `OpId` rather than a position.
    fn is_filtered_out(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.actor_mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    /// Per-actor mask iterator. Used when composing a head-derived clock
    /// with the filter's restrictions.
    fn iter_mask(&self) -> impl Iterator<Item = (ActorIdx, Option<NonZeroU32>)> + '_ {
        self.actor_mask.iter().map(|(a, v)| (*a, *v))
    }

    /// Take a head out of the pending set if it was waiting for it. Returns
    /// true when the caller needs to re-derive the mask because a previously
    /// unknown head just became known.
    fn take_pending(&mut self, hash: &ChangeHash) -> bool {
        self.pending_heads.remove(hash)
    }

    /// Adjust the per-actor maps and the cached clock when a new actor
    /// slot is inserted at `idx`. The new slot starts unrestricted; the
    /// caller (`ChangeGraph::insert_actor`) writes its rule and mask
    /// entries afterwards if needed.
    fn insert_actor(&mut self, idx: usize) {
        shift_keys_up(&mut self.actor_rules, idx);
        shift_keys_up(&mut self.actor_mask, idx);
        self.cached_clock.0.insert(idx, u32::MAX);
    }

    /// Adjust the per-actor maps and the cached clock when actor slot
    /// `idx` is removed.
    fn remove_actor(&mut self, idx: usize) {
        self.cached_clock.0.remove(idx);
        shift_keys_down(&mut self.actor_rules, idx);
        shift_keys_down(&mut self.actor_mask, idx);
    }
}

/// Shift every `ActorIdx` key in `map` that is `>= idx` up by one, in
/// place. Used when a new actor slot is inserted at `idx`.
fn shift_keys_up<V>(map: &mut HashMap<ActorIdx, V>, idx: usize) {
    *map = std::mem::take(map)
        .into_iter()
        .map(|(a, v)| {
            let shifted = if a.0 >= idx as u32 {
                ActorIdx(a.0 + 1)
            } else {
                a
            };
            (shifted, v)
        })
        .collect();
}

/// Drop the entry at `idx` and shift every key `> idx` down by one.
/// Used when an actor slot is removed.
fn shift_keys_down<V>(map: &mut HashMap<ActorIdx, V>, idx: usize) {
    *map = std::mem::take(map)
        .into_iter()
        .filter_map(|(a, v)| match a.0.cmp(&(idx as u32)) {
            std::cmp::Ordering::Less => Some((a, v)),
            std::cmp::Ordering::Equal => None,
            std::cmp::Ordering::Greater => Some((ActorIdx(a.0 - 1), v)),
        })
        .collect();
}

pub(crate) struct ChangeGraphCols(ChangeGraph);

const CACHE_STEP: u32 = 16;

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeIdx(u32);

impl Add<usize> for NodeIdx {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        NodeIdx(self.0 + other as u32)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdx(NonZeroU32);

impl EdgeIdx {
    fn new(value: usize) -> Self {
        EdgeIdx(NonZeroU32::new(value as u32 + 1).unwrap())
    }
    fn get(&self) -> usize {
        self.0.get() as usize - 1
    }
}

#[derive(PartialEq, Debug, Clone)]
struct Edge {
    // Edges are always child -> parent so we only store the target, the child is implicit
    // as you get the edge from the child
    target: NodeIdx,
    next: Option<EdgeIdx>,
}

impl ChangeGraph {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            edges: Vec::new(),
            nodes_by_hash: HashMap::new(),
            hashes: Vec::new(),
            actors: Vec::new(),
            authors: Vec::new(),
            actor_author: Vec::new(),
            author: None,
            rules: Filter::default(),
            resolved: ResolvedFilter::new(num_actors),
            max_ops: Vec::new(),
            max_op: 0,
            num_ops: ColumnData::new(),
            seq: Vec::new(),
            parents: Vec::new(),
            messages: ColumnData::new(),
            timestamps: ColumnData::new(),
            extra_bytes_meta: ColumnData::new(),
            extra_bytes_raw: Vec::new(),
            heads: BTreeSet::new(),
            clock_cache: HashMap::new(),
            seq_index: vec![vec![]; num_actors],
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

    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.heads
            .iter()
            .map(|h| self.nodes_by_hash.get(h).unwrap().0 as u64)
    }

    pub(crate) fn num_actors(&self) -> usize {
        self.seq_index.len()
    }

    /// Replace the visibility filter and re-derive the per-actor mask.
    ///
    /// `actor_ids` is the op-set's `ActorId` table. We consult it once
    /// here to compile the public filter's `actors: BTreeMap<ActorId, _>`
    /// down to an `ActorIdx`-keyed map; from then on every internal path
    /// that resolves a rule for an actor works in `ActorIdx` space and
    /// never needs the `ActorId` table again.
    ///
    /// Heads referenced by a rule but not yet in the graph are remembered
    /// in `pending_heads`; the rule takes effect for those heads as soon
    /// as they arrive (see `add_changes`).
    pub(crate) fn set_filter(&mut self, filter: Filter, actor_ids: &[ActorId]) {
        self.rules = filter;

        // Compile `rules.actors` (keyed by ActorId) into an ActorIdx-keyed
        // map. ActorIds in `rules.actors` that aren't in the document yet
        // are simply ignored — if such an actor arrives later,
        // `insert_actor` will pick the rule up.
        self.resolved.actor_rules.clear();
        for (actor_id, rule) in &self.rules.actors {
            if let Ok(idx) = actor_ids.binary_search(actor_id) {
                self.resolved.actor_rules.insert(idx.into(), rule.clone());
            }
        }

        // Note which `AllowUpTo` heads we don't yet have so they can be
        // re-resolved when they arrive.
        self.resolved.pending_heads.clear();
        for h in self.rules.referenced_heads() {
            if !self.nodes_by_hash.contains_key(h) {
                self.resolved.pending_heads.insert(*h);
            }
        }

        self.recomp_actor_masks();
    }

    /// The current filter rule set.
    pub(crate) fn filter(&self) -> &Filter {
        &self.rules
    }

    /// Re-derive every actor's mask entry from the already-compiled
    /// `resolved.actor_rules`, the per-actor author assignments, and the
    /// `default` rule. Callers that only need to refresh one actor (e.g.
    /// `assign_author`, `insert_actor`) call `recomp_actor_mask` directly.
    fn recomp_actor_masks(&mut self) {
        self.resolved.actor_mask.clear();
        for actor_idx in 0..self.num_actors() {
            self.recomp_actor_mask(actor_idx);
        }
        self.rebuild_filter_cached_clock();
    }

    /// Resolve the rule for one actor and write its mask entry. Does *not*
    /// rebuild the cached op-counter clock; callers are expected to do
    /// that afterwards (often after multiple actors have been
    /// recomputed). Most-specific-wins: actor rule → author rule →
    /// default.
    fn recomp_actor_mask(&mut self, actor_idx: usize) {
        let rule: ResolvedRule<'_> =
            if let Some(r) = self.resolved.actor_rules.get(&actor_idx.into()) {
                r.into()
            } else {
                let author = self
                    .actor_author
                    .get(actor_idx)
                    .and_then(|a| a.as_ref())
                    .and_then(|a| self.authors.get(a.as_usize()));
                match author.and_then(|a| self.rules.authors.get(a)) {
                    Some(r) => r.into(),
                    None => (&self.rules.default).into(),
                }
            };
        // The mask entry has three states:
        //   missing key       -> actor is unrestricted (Allow)
        //   Some(None)        -> actor has nothing visible (Deny)
        //   Some(Some(seq))   -> actor visible up to seq inclusive (AllowUpTo)
        match rule {
            ResolvedRule::Allow => {
                self.resolved.actor_mask.remove(&actor_idx.into());
            }
            ResolvedRule::Deny => {
                self.resolved.actor_mask.insert(actor_idx.into(), None);
            }
            ResolvedRule::AllowUpTo(heads) => {
                let clock = self.calculate_clock(self.heads_to_nodes(heads));
                self.resolved
                    .actor_mask
                    .insert(actor_idx.into(), clock.get_for_actor(&actor_idx));
            }
        }
    }

    /// The clock used to filter rejected ops on slow paths that bypass the
    /// op-set index (e.g. `visible_slow`). `None` when the filter is not
    /// rejecting anything — slow paths can then skip clock-based filtering.
    /// Borrowed callers do not have to copy the clock.
    pub(crate) fn active_filter_clock(&self) -> Option<&OpClock> {
        if self.rules.is_noop() {
            None
        } else {
            Some(&self.resolved.cached_clock)
        }
    }

    /// Rebuild `ResolvedFilter::cached_clock` from
    /// `ResolvedFilter::actor_mask`. The mask is keyed by actor and stores
    /// the largest accepted *seq* for each actor; the cached clock indexes
    /// ops by their global op counter, so we have to convert the seq into
    /// the max op counter of the change at that seq (just like
    /// `to_op_clock` does).
    fn rebuild_filter_cached_clock(&mut self) {
        self.resolved.cached_clock = (0_u32..self.num_actors() as u32)
            .map(|actor| {
                let actor_usize = actor as usize;
                if let Some(mask) = self.resolved.actor_mask.get(&ActorIdx(actor)) {
                    mask.and_then(|seq| {
                        self.seq_index
                            .get(actor_usize)
                            .and_then(|v| v.get(seq.get() as usize - 1))
                            .and_then(|n| self.max_ops.get(n.0 as usize))
                            .copied()
                    })
                } else {
                    Some(u32::MAX)
                }
            })
            .collect();
    }

    /// Whether `(actor, seq)` is rejected by the current filter. Used at
    /// apply time to stamp the `filtered_out` flag on freshly-imported
    /// ops.
    pub(crate) fn is_filtered_out(&self, actor: ActorIdx, seq: u64) -> bool {
        self.resolved.is_filtered_out(actor, seq)
    }

    pub(crate) fn get_authors(&self) -> &[Author] {
        &self.authors
    }

    pub(crate) fn get_author_for_actor(&self, actor: usize) -> Option<&Author> {
        let author = self.actor_author.get(actor)?.as_ref()?.as_usize();
        self.authors.get(author)
    }

    pub(crate) fn get_actors_for_author(
        &self,
        author: &Author,
    ) -> impl Iterator<Item = usize> + '_ {
        get_actors_for_author(&self.authors, &self.actor_author, author)
    }

    pub(crate) fn assign_author(&mut self, author: Author, actor: usize) {
        let author_id = self.put_author(author);
        self.actor_author[actor] = Some(author_id);
        // The author rule (if any) may now apply to this actor. Resolution
        // is `ActorIdx`-only because `actor_rules` is already compiled,
        // so we don't need the op-set's `ActorId` table here.
        self.recomp_actor_mask(actor);
        self.rebuild_filter_cached_clock();
    }

    pub(crate) fn put_author(&mut self, author: Author) -> AuthorIdx {
        match self.authors.binary_search(&author) {
            Err(index) => {
                self.authors.insert(index, author);
                for a in self.actor_author.iter_mut().flatten() {
                    a.with_new_author(index)
                }
                if let Some(a) = self.author.as_mut() {
                    a.with_new_author(index);
                }
                index.into()
            }
            Ok(index) => index.into(),
        }
    }

    /// Insert a new actor slot at `idx`. `actor_id` is consulted *only*
    /// to check whether the public filter has an `actors`-keyed rule for
    /// the new actor; the resolved state is otherwise driven entirely by
    /// `ActorIdx`.
    pub(crate) fn insert_actor(&mut self, idx: usize, actor_id: &ActorId) {
        if self.seq_index.len() != idx {
            for actor_index in &mut self.actors {
                if actor_index.0 >= idx as u32 {
                    actor_index.0 += 1;
                }
            }
            // Shift the per-actor maps to make room for the new slot.
            self.resolved.insert_actor(idx);
        } else {
            // Even when no existing actor needs reindexing, the cached clock
            // length must stay in lockstep with the actor count.
            self.resolved.cached_clock.0.insert(idx, u32::MAX);
        }
        for clock in self.clock_cache.values_mut() {
            clock.rewrite_with_new_actor(idx)
        }
        self.seq_index.insert(idx, vec![]);
        self.actor_author.insert(idx, None);
        // If the public filter has a rule keyed on this actor's `ActorId`,
        // record it now so future resolution stays in `ActorIdx` space.
        if let Some(rule) = self.rules.actors.get(actor_id) {
            self.resolved
                .actor_rules
                .insert(idx.into(), rule.clone());
        }
        self.recomp_actor_mask(idx);
        self.rebuild_filter_cached_clock();
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        for actor_index in &mut self.actors {
            if actor_index.0 > idx as u32 {
                actor_index.0 -= 1;
            }
        }
        if self.seq_index.get(idx).is_some() {
            assert!(self.seq_index[idx].is_empty());
            self.seq_index.remove(idx);
            self.actor_author.remove(idx);
            self.resolved.remove_actor(idx);
        }
        for clock in &mut self.clock_cache.values_mut() {
            clock.remove_actor(idx)
        }
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

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.hashes.get(index)
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.max_op as u64
    }

    pub(crate) fn max_op_for_actor(&mut self, actor_index: usize) -> u64 {
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

    fn deps_iter(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.node_ids().flat_map(|n| self.parents(n))
    }

    fn num_deps(&self) -> impl Iterator<Item = usize> + '_ {
        self.node_ids().map(|n| self.parents(n).count())
    }

    fn node_ids(&self) -> impl Iterator<Item = NodeIdx> {
        let end = self.hashes.len() as u32;
        (0..end).map(NodeIdx)
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> RawColumns<Uncompressed> {
        use ids::*;

        let actor_iter = self.actors.iter().map(as_actor);
        let actor = ActorCursor::encode_unless_empty(out, actor_iter);

        let seq_iter = self.seq.iter().map(as_seq);
        let seq = DeltaCursor::encode_unless_empty(out, seq_iter);

        let max_op_iter = self.max_ops.iter().map(as_max_op);
        let max_op = DeltaCursor::encode_unless_empty(out, max_op_iter);

        let time = self.timestamps.save_to_unless_empty(out);

        let message = self.messages.save_to_unless_empty(out);

        let num_deps_iter = self.num_deps().map(as_num_deps);
        let num_deps = UIntCursor::encode_unless_empty(out, num_deps_iter);

        let deps_iter = self.deps_iter().map(as_deps);
        let deps = DeltaCursor::encode_unless_empty(out, deps_iter);

        // FIXME - we could eliminate this column if empty but meta isnt all null
        let meta = self.extra_bytes_meta.save_to_unless_empty(out);
        let raw = out.len()..out.len() + self.extra_bytes_raw.len();
        out.extend(&self.extra_bytes_raw);

        [
            RawColumn::new(ACTOR_COL_SPEC, actor),
            RawColumn::new(SEQ_COL_SPEC, seq),
            RawColumn::new(MAX_OP_COL_SPEC, max_op),
            RawColumn::new(TIME_COL_SPEC, time),
            RawColumn::new(MESSAGE_COL_SPEC, message),
            RawColumn::new(DEPS_COUNT_COL_SPEC, num_deps),
            RawColumn::new(DEPS_VAL_COL_SPEC, deps),
            RawColumn::new(EXTRA_META_COL_SPEC, meta),
            RawColumn::new(EXTRA_VAL_COL_SPEC, raw),
        ]
        .into_iter()
        .collect()
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

    pub(crate) fn opid_to_hash(&self, id: OpId) -> Option<ChangeHash> {
        let actor_indices = self.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
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
        let node_idx = actor_indices[index];
        self.hashes.get(node_idx.0 as usize).cloned()
    }

    pub(crate) fn deps_for_hash(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let node_idx = self.nodes_by_hash.get(hash);
        let mut edge_idx = node_idx.and_then(|n| self.parents[n.0 as usize]);
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            let hash = self.hashes[edge.target.0 as usize];
            Some(hash)
        })
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> bool {
        self.nodes_by_hash.contains_key(hash)
    }

    pub(crate) fn get_bundle_metadata<I>(
        &self,
        hashes: I,
    ) -> impl Iterator<Item = Result<BundleMetadata<'_>, MissingDep>>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        hashes.into_iter().map(|hash| {
            let index = self
                .nodes_by_hash
                .get(&hash)
                .cloned()
                .ok_or(MissingDep(hash))?;
            let i = index.0 as usize;
            let actor = self.actors[i].into();
            let timestamp = *self.timestamps.get(i).flatten().unwrap_or_default();
            let max_op = self.max_ops[i] as u64;
            let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
            let message = self.messages.get(i).flatten();

            // FIXME - this needs a test
            let meta = self.extra_bytes_meta.get_with_acc(i).unwrap();
            let meta_range =
                meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
            let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

            let deps = self
                .parents(index)
                .map(|p| self.hashes[p.0 as usize])
                .collect::<Vec<_>>();

            let start_op = max_op - num_ops + 1;
            let seq = self.seq[i] as u64;
            Ok(BundleMetadata {
                hash,
                actor,
                seq,
                start_op,
                max_op,
                timestamp,
                message,
                extra,
                deps,
                builder: i,
            })
        })
    }

    pub(crate) fn get_build_metadata<I>(
        &self,
        hashes: I,
    ) -> Result<Vec<BuildChangeMetadata<'_>>, MissingDep>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let indexes: Vec<_> = hashes
            .into_iter()
            .map(|hash| {
                self.nodes_by_hash
                    .get(&hash)
                    .cloned()
                    .ok_or(MissingDep(hash))
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
            extra_bytes_meta: self.extra_bytes_meta.iter().with_acc(),
            graph: self,
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
                let timestamp = *self.timestamps.get(i).flatten().unwrap_or_default();
                let max_op = self.max_ops[i] as u64;
                let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
                let message = self.messages.get(i).flatten();

                // FIXME - this needs a test
                let meta = self.extra_bytes_meta.get_with_acc(i).unwrap();
                let meta_range =
                    meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
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

    fn get_build_indexes(&self, clock: SeqClock) -> Vec<NodeIdx> {
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

    pub(crate) fn get_hashes(&self, have_deps: &[ChangeHash]) -> Cow<'_, [ChangeHash]> {
        if have_deps.is_empty() {
            Cow::Borrowed(&self.hashes)
        } else {
            let clock = self.seq_clock_for_heads(have_deps);
            Cow::Owned(
                self.get_build_indexes(clock)
                    .into_iter()
                    .filter_map(|node| self.hashes.get(node.0 as usize))
                    .copied()
                    .collect(),
            )
        }
    }

    pub(crate) fn get_build_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> Vec<BuildChangeMetadata<'_>> {
        let clock = self.seq_clock_for_heads(have_deps);
        let change_indexes = self.get_build_indexes(clock);
        self.get_build_metadata_for_indexes(change_indexes)
    }

    pub(crate) fn get_hash_for_actor_seq(
        &self,
        actor: usize,
        seq: u64,
    ) -> Result<ChangeHash, AutomergeError> {
        self.seq_index
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|i| self.hashes.get(i.0 as usize))
            .ok_or(AutomergeError::InvalidSeq(seq))
            .copied()
    }

    fn update_heads(&mut self, change: &Change) {
        for d in change.deps() {
            self.heads.remove(d);
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
        self.messages
            .extend(iter.clone().map(|(c, _)| c.message().cloned()));
        self.extra_bytes_meta
            .extend(iter.clone().map(|(c, _)| ValueMeta::from(c.extra_bytes())));
        self.parents.extend(std::iter::repeat_n(None, iter.len()));
        for (c, _) in iter {
            self.extra_bytes_raw.extend_from_slice(c.extra_bytes());
        }
    }

    fn add_changes<'a, I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone>(
        &mut self,
        iter: I,
    ) -> Result<(), MissingDep> {
        let node = NodeIdx(self.hashes.len() as u32);
        let mut recomp_filter = false;

        self.add_nodes(iter.clone());

        for (i, (change, actor)) in iter.enumerate() {
            let node_idx = node + i;
            let hash = change.hash();
            self.max_op = std::cmp::max(self.max_op, change.max_op() as u32);
            self.hashes.push(hash);
            debug_assert!(!self.nodes_by_hash.contains_key(&hash));
            recomp_filter = recomp_filter || self.resolved.take_pending(&hash);
            self.nodes_by_hash.insert(hash, node_idx);
            self.update_heads(change);

            if let Some(a) = change.author() {
                // This is validated in Automerge::apply_changes_batch_log_patches
                assert!(change.seq() == 1);
                if change.seq() == 1 {
                    self.assign_author(a.into(), actor)
                }
            }

            assert!(actor < self.seq_index.len());
            assert_eq!(self.seq_index[actor].len() + 1, change.seq() as usize);
            self.seq_index[actor].push(node_idx);

            for parent_hash in change.deps().iter() {
                self.add_parent(node_idx, parent_hash);
            }

            if (node_idx + 1).0 % CACHE_STEP == 0 {
                self.cache_clock(node_idx);
            }
        }

        if recomp_filter {
            // A pending head just landed: every `AllowUpTo` rule that
            // referenced it may now resolve to a different mask entry.
            // `actor_rules` doesn't depend on heads, so no recompilation
            // is needed — just refresh the per-actor masks.
            self.recomp_actor_masks();
        }

        Ok(())
    }

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor: usize,
    ) -> Result<(), MissingDep> {
        let hash = change.hash();

        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }

        for h in change.deps().iter() {
            if !self.nodes_by_hash.contains_key(h) {
                return Err(MissingDep(*h));
            }
        }

        self.add_changes([(change, actor)].into_iter())
    }

    fn cache_clock(&mut self, node_idx: NodeIdx) -> SeqClock {
        let mut clock = SeqClock::new(self.num_actors());
        let mut to_visit = BTreeSet::from([node_idx]);

        self.calculate_clock_inner(&mut clock, &mut to_visit, CACHE_STEP as usize * 2);

        for n in to_visit {
            let sub = self.cache_clock(n);
            SeqClock::merge(&mut clock, &sub);
        }

        self.clock_cache.insert(node_idx, clock.clone());

        clock
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_hash: &ChangeHash) {
        debug_assert!(self.nodes_by_hash.contains_key(parent_hash));
        let parent_idx = *self.nodes_by_hash.get(parent_hash).unwrap();
        let new_edge_idx = EdgeIdx::new(self.edges.len());
        self.edges.push(Edge {
            target: parent_idx,
            next: None,
        });

        let child = &mut self.parents[child_idx.0 as usize];
        if let Some(edge_idx) = child {
            let mut edge = &mut self.edges[edge_idx.get()];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.get()];
            }
            edge.next = Some(new_edge_idx);
        } else {
            *child = Some(new_edge_idx);
        }
    }

    pub(crate) fn deps(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let mut iter = self.nodes_by_hash.get(hash).map(|node| self.parents(*node));
        std::iter::from_fn(move || {
            let next = iter.as_mut()?.next()?;
            self.hashes.get(next.0 as usize).copied()
        })
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.parents[node_idx.0 as usize];
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            Some(edge.target)
        })
    }

    fn heads_to_nodes(&self, heads: &[ChangeHash]) -> Vec<NodeIdx> {
        heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .copied()
            .collect()
    }

    pub(crate) fn clock_for_heads(&self, heads: &[ChangeHash]) -> OpClock {
        let nodes = self.heads_to_nodes(heads);
        let mut clock = self.calculate_clock(nodes);
        for (actor, seq) in self.resolved.iter_mask() {
            clock.mask(usize::from(actor), seq);
        }
        self.to_op_clock(clock)
    }

    fn to_op_clock(&self, c: SeqClock) -> OpClock {
        c.iter()
            .map(|(actor, seq)| {
                self.seq_index
                    .get(actor)
                    .and_then(|v| v.get(seq?.get() as usize - 1))
                    .and_then(|i| self.max_ops.get(i.0 as usize))
                    .copied()
            })
            .collect()
    }

    fn seq_clock_for_heads(&self, heads: &[ChangeHash]) -> SeqClock {
        let nodes = self.heads_to_nodes(heads);
        self.calculate_clock(nodes)
    }

    fn clock_data_for(&self, idx: NodeIdx) -> Option<u32> {
        Some(*self.seq.get(idx.0 as usize)?)
    }

    fn calculate_clock(&self, nodes: Vec<NodeIdx>) -> SeqClock {
        let mut clock = SeqClock::new(self.num_actors());
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
        let mut visited = BTreeSet::new();

        while let Some(idx) = to_visit.pop_last() {
            assert!(!visited.contains(&idx));
            assert!(visited.len() <= self.hashes.len());
            visited.insert(idx);

            let actor = self.actors[idx.0 as usize];
            let data = self.clock_data_for(idx);
            clock.include(actor.into(), data);

            if let Some(cached) = self.clock_cache.get(&idx) {
                SeqClock::merge(clock, cached);
            } else {
                to_visit.extend(self.parents(idx).filter(|p| !visited.contains(p)));
                if visited.len() > limit {
                    break;
                }
            }
        }
    }

    pub(crate) fn remove_ancestors(
        &self,
        changes: &mut BTreeSet<ChangeHash>,
        heads: &[ChangeHash],
    ) {
        let nodes = self.heads_to_nodes(heads);
        self.traverse_ancestors(nodes, |idx| {
            let hash = &self.hashes[idx.0 as usize];
            changes.remove(hash);
            true
        });
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
}

impl ChangeGraphCols {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        self.0.iter()
    }

    pub(crate) fn finalize(self, changes: &[Change]) -> ChangeGraph {
        let mut graph = self.0;
        debug_assert_eq!(changes.len(), graph.len());
        debug_assert!(graph.hashes.is_empty());

        for c in changes {
            let hash = c.hash();
            let idx = graph.hashes.len();
            let node_idx = NodeIdx(idx as u32);
            graph.nodes_by_hash.insert(hash, node_idx);
            graph.hashes.push(hash);
            if let Some(author) = c.author() {
                // Saved documents written by an honest encoder only carry the
                // author footer on seq=1. Skip rather than panic on bad data
                // — any further validation is the apply path's job.
                if c.seq() == 1 {
                    graph.assign_author(author.into(), graph.actors[idx].into())
                }
            }
        }

        for n in 0..(graph.len() as u32) {
            if (n + 1) % CACHE_STEP == 0 {
                graph.cache_clock(NodeIdx(n));
            }
        }

        graph
    }

    pub(crate) fn load(doc: &Document<'_>) -> Result<Self, LoadError> {
        use ids::*;

        let num_actors = doc.actors().len();
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

        let actors = to_vec(ActorCursor::iter(actor_bytes))?;
        let max_ops = to_u32_vec(DeltaCursor::iter(max_op_bytes))?;
        let max_op = max_ops.iter().copied().max().unwrap_or(0);
        let seq = to_u32_vec(DeltaCursor::iter(seq_bytes))?;

        if let Some(a) = actors.iter().copied().map(usize::from).max() {
            if a >= num_actors {
                return Err(LoadError::InvalidActorId(a));
            }
        }

        let len = actors.len();

        let timestamps = ColumnData::load_unless_empty(time_bytes, len)?;
        let messages = ColumnData::load_unless_empty(message_bytes, len)?;
        let extra_bytes_meta = ColumnData::load_unless_empty(extra_meta_bytes, len)?;

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

        let mut seq_index = vec![vec![]; num_actors];
        for (i, actor) in actors.iter().enumerate() {
            let actor = actor.0 as usize;
            seq_index[actor].push(NodeIdx(i as u32));
        }

        let mut parents = Vec::with_capacity(len);
        let mut edges = vec![];

        let deps_count = UIntCursor::iter(deps_count_bytes).map(to_u32);
        let mut deps_val = DeltaCursor::iter(deps_val_bytes).map(to_u32);

        let mut num_ops = Vec::with_capacity(len);
        for (i, d) in deps_count.enumerate() {
            let d = d? as usize;
            if d == 0 {
                num_ops.push(max_ops[i] as u64);
                parents.push(None);
                continue;
            }

            parents.push(Some(EdgeIdx::new(edges.len())));
            let mut last_max_op = 0;
            for e in 0..d {
                let dep = deps_val.next();
                let dep = dep.ok_or(LoadError::InvalidColumnLength(DEPS_VAL_COL_SPEC))??;
                let target = NodeIdx(dep);
                let next = EdgeIdx::new(edges.len() + 1);
                let next = if e + 1 == d { None } else { Some(next) };
                last_max_op = std::cmp::max(last_max_op, max_ops[dep as usize]);
                edges.push(Edge { target, next })
            }
            if last_max_op > max_ops[i] {
                return Err(LoadError::InvalidMaxOp);
            }
            num_ops.push(max_ops[i] as u64 - last_max_op as u64);
        }
        let num_ops = num_ops.into_iter().collect();

        let heads = doc.heads().iter().copied().collect();

        if parents.len() != len {
            return Err(LoadError::InvalidColumnLength(DEPS_COUNT_COL_SPEC));
        }

        // blank - to be filled out later
        let clock_cache = HashMap::default();
        let hashes = vec![];
        let nodes_by_hash = HashMap::new();

        let author = None;
        let authors = vec![];
        let actor_author = vec![None; num_actors];

        Ok(ChangeGraphCols(ChangeGraph {
            edges,
            hashes,
            actors,
            authors,
            author,
            actor_author,
            rules: Filter::default(),
            resolved: ResolvedFilter::new(num_actors),
            parents,
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
        }))
    }
}

fn as_num_deps(num: usize) -> Option<Cow<'static, u64>> {
    Some(Cow::Owned(num as u64))
}

fn as_seq(seq: &u32) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*seq as i64))
}

fn as_actor(actor_index: &ActorIdx) -> Option<Cow<'_, ActorIdx>> {
    Some(Cow::Borrowed(actor_index))
}

fn as_max_op(m: &u32) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*m as i64))
}

fn as_deps(n: NodeIdx) -> Option<Cow<'static, i64>> {
    Some(Cow::Owned(n.0 as i64))
}

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep(ChangeHash);

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        op_set2::{change::build_change, op_set::ResolvedAction, OpSet, TxOp},
        types::{ObjMeta, OpId, OpType},
        ActorId, TextEncoding,
    };

    use super::*;

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
        let mut expected_clock = SeqClock::new(3);
        expected_clock.include(builder.index(&actor1), Some(2));
        expected_clock.include(builder.index(&actor2), Some(1));
        expected_clock.include(builder.index(&actor3), Some(1));

        let clock = graph.seq_clock_for_heads(&[change4]);
        assert_eq!(clock, expected_clock);
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
        graph.remove_ancestors(&mut changes, &heads);

        let expected_changes = vec![change3, change4].into_iter().collect::<BTreeSet<_>>();

        assert_eq!(changes, expected_changes);
    }

    struct TestGraphBuilder {
        actors: Vec<ActorId>,
        changes: Vec<Change>,
        graph: ChangeGraph,
        seqs_by_actor: BTreeMap<ActorId, u64>,
    }

    impl TestGraphBuilder {
        fn new() -> Self {
            TestGraphBuilder {
                actors: Vec::new(),
                changes: Vec::new(),
                graph: ChangeGraph::new(0),
                seqs_by_actor: BTreeMap::new(),
            }
        }

        fn actor(&mut self) -> ActorId {
            let actor = ActorId::random();
            let idx = self.actors.len();
            self.graph.insert_actor(idx, &actor);
            self.actors.push(actor.clone());
            actor
        }

        fn index(&self, actor: &ActorId) -> usize {
            self.actors.iter().position(|a| a == actor).unwrap()
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
            let osd = OpSet::from_actors(self.actors.clone(), TextEncoding::platform_default());

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

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
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
            self.graph.add_change(&change, actor_idx).unwrap();
            self.changes.push(change);
            hash
        }

        fn build(&self) -> ChangeGraph {
            let mut graph = ChangeGraph::new(self.actors.len());
            for change in &self.changes {
                let actor_idx = self.index(change.actor_id());
                graph.add_change(change, actor_idx).unwrap();
            }
            graph
        }
    }
}

fn to_vec<'a, I, T>(iter: I) -> Result<Vec<T>, PackError>
where
    I: Iterator<Item = Result<Option<Cow<'a, T>>, PackError>>,
    T: Copy + Default + 'a,
{
    iter.map(squish).collect()
}

fn squish<T>(i: Result<Option<Cow<'_, T>>, PackError>) -> Result<T, PackError>
where
    T: Copy + Default,
{
    match i {
        Err(e) => Err(e),
        Ok(Some(i)) => Ok(*i),
        Ok(None) => Ok(T::default()),
    }
}

fn to_u32<T>(i: Result<Option<Cow<'_, T>>, PackError>) -> Result<u32, PackError>
where
    T: TryInto<u32> + Copy + Default,
{
    match i {
        Err(e) => Err(e),
        Ok(Some(i)) => Ok((*i).try_into().unwrap_or(0)),
        Ok(None) => Ok(0),
    }
}

fn to_u32_vec<'a, I, T>(iter: I) -> Result<Vec<u32>, PackError>
where
    I: Iterator<Item = Result<Option<Cow<'a, T>>, PackError>>,
    T: TryInto<u32> + Copy + Default + 'a,
{
    iter.map(to_u32).collect()
}

pub(crate) struct ChangeIter<'a> {
    index: usize,
    actors: std::slice::Iter<'a, ActorIdx>,
    seq: std::slice::Iter<'a, u32>,
    max_ops: std::slice::Iter<'a, u32>,
    num_ops: ColumnDataIter<'a, UIntCursor>,
    timestamps: ColumnDataIter<'a, DeltaCursor>,
    messages: ColumnDataIter<'a, StrCursor>,
    extra_bytes_meta: ColGroupIter<'a, MetaCursor>,
    graph: &'a ChangeGraph,
}

impl<'a> Iterator for ChangeIter<'a> {
    type Item = BuildChangeMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;
        self.index += 1;
        let actor = (*self.actors.next()?).into();
        let seq = *self.seq.next()? as u64;
        let max_op = *self.max_ops.next()? as u64;
        let num_ops = *self.num_ops.next().flatten().unwrap_or_default();
        let timestamp = *self.timestamps.next().flatten().unwrap_or_default();
        let message = self.messages.next().flatten();
        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.next()?;
        let meta_range = meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
        let extra = Cow::Borrowed(&self.graph.extra_bytes_raw[meta_range]);
        let deps = self
            .graph
            .parents(NodeIdx(i as u32))
            .map(|n| n.0 as u64)
            .collect();
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
        let num_ops = *self.num_ops.nth(0).flatten().unwrap_or_default();
        let timestamp = *self.timestamps.nth(0).flatten().unwrap_or_default();
        let message = self.messages.nth(0).flatten();
        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.shift_acc(0)?;
        let meta_range = meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
        let extra = Cow::Borrowed(&self.graph.extra_bytes_raw[meta_range]);

        let deps = self
            .graph
            .parents(NodeIdx(i as u32))
            .map(|n| n.0 as u64)
            .collect();

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
}

fn get_actors_for_author<'a>(
    authors: &'a [Author],
    actor_author: &'a [Option<AuthorIdx>],
    author: &Author,
) -> impl Iterator<Item = usize> + 'a {
    authors
        .binary_search(author)
        .ok()
        .map(|idx| {
            let idx = AuthorIdx::from(idx);
            actor_author.iter().enumerate().filter_map(move |(i, a)| {
                if a.as_ref()? == &idx {
                    Some(i)
                } else {
                    None
                }
            })
        })
        .into_iter()
        .flatten()
}
