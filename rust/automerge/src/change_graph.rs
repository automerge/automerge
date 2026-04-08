use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::ops::Add;

use hexane::{
    ColGroupIter, ColumnCursor, ColumnData, ColumnDataIter, DeltaCursor, PackError, StrCursor,
    UIntCursor,
};

use crate::storage::BundleMetadata;
use crate::{
    clock::{Clock as OpClock, SeqClock},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorCursor, ActorIdx, MetaCursor, ValueMeta},
    storage::columns::compression::Uncompressed,
    storage::columns::BadColumnLayout,
    storage::document::ReconstructError as LoadError,
    storage::{Columns, Document, RawColumn, RawColumns},
    types::{Author, AuthorIdx, OpId},
    Change, ChangeHash,
};

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache

#[derive(Debug, PartialEq, Default, Clone)]
struct CommitStore {
    edges: Vec<Edge>,
    hashes: Vec<ChangeHash>,
    actors: Vec<ActorIdx>,
    parents: Vec<Option<EdgeIdx>>,
    seq: Vec<u32>,
    max_ops: Vec<u32>,
    num_ops: ColumnData<UIntCursor>,
    timestamps: ColumnData<DeltaCursor>,
    messages: ColumnData<StrCursor>,
    extra_bytes_meta: ColumnData<MetaCursor>,
    extra_bytes_raw: Vec<u8>,
}

#[derive(Debug, PartialEq, Default, Clone)]
struct CommitIndex {
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    heads: BTreeSet<ChangeHash>,
    max_op: u32,
    seq_index: Vec<Vec<NodeIdx>>,
    clock_cache: HashMap<NodeIdx, SeqClock>,
}

#[derive(Debug, PartialEq, Default, Clone)]
struct AuthorMap {
    authors: Vec<Author>,
    local: Option<AuthorIdx>,
    actor_author: Vec<Option<AuthorIdx>>,
}

#[derive(Debug, PartialEq, Default, Clone)]
struct RevocationState {
    rules: HashMap<Author, Vec<ChangeHash>>,
    pending: HashSet<ChangeHash>,
    mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    dirty: HashSet<Author>,
    all_dirty: bool,
}

#[derive(Debug, PartialEq, Default, Clone)]
pub(crate) struct ChangeGraph {
    store: CommitStore,
    index: CommitIndex,
    author_map: AuthorMap,
    revocations: RevocationState,
}

impl RevocationState {
    fn on_actor_assigned(&mut self, author: &Author) {
        if self.rules.contains_key(author) {
            self.dirty.insert(author.clone());
        }
    }
}

impl AuthorMap {
    fn put_author(&mut self, author: Author) -> AuthorIdx {
        match self.authors.binary_search(&author) {
            Err(index) => {
                self.authors.insert(index, author);
                for a in self.actor_author.iter_mut().flatten() {
                    a.with_new_author(index)
                }
                if let Some(a) = self.local.as_mut() {
                    a.with_new_author(index);
                }
                index.into()
            }
            Ok(index) => index.into(),
        }
    }
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
            store: CommitStore {
                edges: Vec::new(),
                hashes: Vec::new(),
                actors: Vec::new(),
                parents: Vec::new(),
                seq: Vec::new(),
                max_ops: Vec::new(),
                num_ops: ColumnData::new(),
                timestamps: ColumnData::new(),
                messages: ColumnData::new(),
                extra_bytes_meta: ColumnData::new(),
                extra_bytes_raw: Vec::new(),
            },
            index: CommitIndex {
                nodes_by_hash: HashMap::new(),
                heads: BTreeSet::new(),
                max_op: 0,
                seq_index: vec![vec![]; num_actors],
                clock_cache: HashMap::new(),
            },
            author_map: AuthorMap {
                authors: Vec::new(),
                local: None,
                actor_author: vec![None; num_actors],
            },
            revocations: RevocationState {
                rules: HashMap::new(),
                pending: HashSet::new(),
                mask: HashMap::new(),
                dirty: HashSet::new(),
                all_dirty: false,
            },
        }
    }

    pub(crate) fn all_actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.index.seq_index.iter().enumerate().map(|(i, _)| i)
    }

    pub(crate) fn actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.index
            .seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if !v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn unused_actors(&self) -> impl Iterator<Item = usize> + '_ {
        self.index
            .seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.index.heads.iter().cloned()
    }

    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.index
            .heads
            .iter()
            .map(|h| self.index.nodes_by_hash.get(h).unwrap().0 as u64)
    }

    pub(crate) fn num_actors(&self) -> usize {
        self.index.seq_index.len()
    }

    pub(crate) fn revoke(&mut self, author: Author, heads: Vec<ChangeHash>) {
        for h in &heads {
            if !self.index.nodes_by_hash.contains_key(h) {
                self.revocations.pending.insert(*h);
            }
        }
        self.revocations.dirty.insert(author.clone());
        self.revocations.rules.insert(author, heads);
        self.recompute_revocations();
    }

    pub(crate) fn set_revocations(&mut self, revocations: HashMap<Author, Vec<ChangeHash>>) {
        self.revocations.pending.clear();
        self.revocations.mask.clear();
        self.revocations.rules.clear();
        self.revocations.dirty.clear();
        self.revocations.all_dirty = false;
        for (author, heads) in revocations {
            for h in &heads {
                if !self.index.nodes_by_hash.contains_key(h) {
                    self.revocations.pending.insert(*h);
                }
            }
            self.revocations.dirty.insert(author.clone());
            self.revocations.rules.insert(author, heads);
        }
        self.recompute_revocations();
    }

    fn recompute_revocations(&mut self) {
        if !self.revocations.all_dirty && self.revocations.dirty.is_empty() {
            return;
        }

        if self.revocations.all_dirty {
            self.revocations.dirty.clear();
            self.revocations.all_dirty = false;
            for (author, heads) in &self.revocations.rules {
                let clock = self.calculate_clock(self.heads_to_nodes(heads));
                for a in get_actors_for_author(
                    &self.author_map.authors,
                    &self.author_map.actor_author,
                    author,
                ) {
                    self.revocations
                        .mask
                        .insert(a.into(), clock.get_for_actor(&a));
                }
            }
            // Clean up stale mask entries from unrevoked authors
            let authors = &self.author_map.authors;
            let actor_author = &self.author_map.actor_author;
            let rules = &self.revocations.rules;
            self.revocations.mask.retain(|actor, _| {
                let actor_usize: usize = (*actor).into();
                actor_author
                    .get(actor_usize)
                    .and_then(|a| a.as_ref())
                    .and_then(|a| authors.get(a.as_usize()))
                    .is_some_and(|a| rules.contains_key(a))
            });
        } else {
            let dirty: Vec<_> = self.revocations.dirty.drain().collect();
            for author in dirty {
                match self.revocations.rules.get(&author) {
                    Some(heads) => {
                        let clock = self.calculate_clock(self.heads_to_nodes(heads));
                        for a in get_actors_for_author(
                            &self.author_map.authors,
                            &self.author_map.actor_author,
                            &author,
                        ) {
                            self.revocations
                                .mask
                                .insert(a.into(), clock.get_for_actor(&a));
                        }
                    }
                    None => {
                        for a in get_actors_for_author(
                            &self.author_map.authors,
                            &self.author_map.actor_author,
                            &author,
                        ) {
                            self.revocations.mask.remove(&a.into());
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn unrevoke(&mut self, author: &Author) {
        self.revocations.rules.remove(author);
        self.revocations.dirty.insert(author.clone());
        self.recompute_revocations();
    }

    pub(crate) fn is_revoked(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.revocations.mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    pub(crate) fn get_revocations(&self) -> &HashMap<Author, Vec<ChangeHash>> {
        &self.revocations.rules
    }

    pub(crate) fn get_authors(&self) -> &[Author] {
        &self.author_map.authors
    }

    pub(crate) fn get_author_for_actor(&self, actor: usize) -> Option<&Author> {
        let author = self
            .author_map
            .actor_author
            .get(actor)?
            .as_ref()?
            .as_usize();
        self.author_map.authors.get(author)
    }

    pub(crate) fn get_actors_for_author(
        &self,
        author: &Author,
    ) -> impl Iterator<Item = usize> + '_ {
        get_actors_for_author(
            &self.author_map.authors,
            &self.author_map.actor_author,
            author,
        )
    }

    pub(crate) fn assign_author(&mut self, author: Author, actor: usize) {
        self.assign_author_deferred(author, actor);
        self.recompute_revocations();
    }

    fn assign_author_deferred(&mut self, author: Author, actor: usize) {
        self.revocations.on_actor_assigned(&author);
        let author_id = self.author_map.put_author(author);
        self.author_map.actor_author[actor] = Some(author_id);
    }

    pub(crate) fn insert_actor(&mut self, idx: usize) {
        if self.index.seq_index.len() != idx {
            for actor_index in &mut self.store.actors {
                if actor_index.0 >= idx as u32 {
                    actor_index.0 += 1;
                }
            }
        }
        for clock in self.index.clock_cache.values_mut() {
            clock.rewrite_with_new_actor(idx)
        }
        self.index.seq_index.insert(idx, vec![]);
        self.author_map.actor_author.insert(idx, None);
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        for actor_index in &mut self.store.actors {
            if actor_index.0 > idx as u32 {
                actor_index.0 -= 1;
            }
        }
        if self.index.seq_index.get(idx).is_some() {
            assert!(self.index.seq_index[idx].is_empty());
            self.index.seq_index.remove(idx);
            self.author_map.actor_author.remove(idx);
        }
        for clock in &mut self.index.clock_cache.values_mut() {
            clock.remove_actor(idx)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.store.actors.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.store.actors.is_empty()
    }

    pub(crate) fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.index.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.store.hashes.get(index)
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.index.max_op as u64
    }

    pub(crate) fn max_op_for_actor(&mut self, actor_index: usize) -> u64 {
        self.index
            .seq_index
            .get(actor_index)
            .and_then(|s| s.last())
            .and_then(|index| self.store.max_ops.get(index.0 as usize).cloned())
            .unwrap_or(0) as u64
    }

    pub(crate) fn seq_for_actor(&self, actor: usize) -> u64 {
        self.index
            .seq_index
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
        let end = self.store.hashes.len() as u32;
        (0..end).map(NodeIdx)
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> RawColumns<Uncompressed> {
        use ids::*;

        let actor_iter = self.store.actors.iter().map(as_actor);
        let actor = ActorCursor::encode_unless_empty(out, actor_iter);

        let seq_iter = self.store.seq.iter().map(as_seq);
        let seq = DeltaCursor::encode_unless_empty(out, seq_iter);

        let max_op_iter = self.store.max_ops.iter().map(as_max_op);
        let max_op = DeltaCursor::encode_unless_empty(out, max_op_iter);

        let time = self.store.timestamps.save_to_unless_empty(out);

        let message = self.store.messages.save_to_unless_empty(out);

        let num_deps_iter = self.num_deps().map(as_num_deps);
        let num_deps = UIntCursor::encode_unless_empty(out, num_deps_iter);

        let deps_iter = self.deps_iter().map(as_deps);
        let deps = DeltaCursor::encode_unless_empty(out, deps_iter);

        // FIXME - we could eliminate this column if empty but meta isnt all null
        let meta = self.store.extra_bytes_meta.save_to_unless_empty(out);
        let raw = out.len()..out.len() + self.store.extra_bytes_raw.len();
        out.extend(&self.store.extra_bytes_raw);

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
        let actor_indices = self.index.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = *self.store.num_ops.get(i).flatten().unwrap_or_default();
                let max_op = self.store.max_ops[i];
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
        self.store.hashes.get(node_idx.0 as usize).cloned()
    }

    pub(crate) fn deps_for_hash(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let node_idx = self.index.nodes_by_hash.get(hash);
        let mut edge_idx = node_idx.and_then(|n| self.store.parents[n.0 as usize]);
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.store.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            let hash = self.store.hashes[edge.target.0 as usize];
            Some(hash)
        })
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> bool {
        self.index.nodes_by_hash.contains_key(hash)
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
                .index
                .nodes_by_hash
                .get(&hash)
                .cloned()
                .ok_or(MissingDep(hash))?;
            let i = index.0 as usize;
            let actor = self.store.actors[i].into();
            let timestamp = *self.store.timestamps.get(i).flatten().unwrap_or_default();
            let max_op = self.store.max_ops[i] as u64;
            let num_ops = *self.store.num_ops.get(i).flatten().unwrap_or_default();
            let message = self.store.messages.get(i).flatten();

            // FIXME - this needs a test
            let meta = self.store.extra_bytes_meta.get_with_acc(i).unwrap();
            let meta_range =
                meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
            let extra = Cow::Borrowed(&self.store.extra_bytes_raw[meta_range]);

            let deps = self
                .parents(index)
                .map(|p| self.store.hashes[p.0 as usize])
                .collect::<Vec<_>>();

            let start_op = max_op - num_ops + 1;
            let seq = self.store.seq[i] as u64;
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
                self.index
                    .nodes_by_hash
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
            actors: self.store.actors.iter(),
            seq: self.store.seq.iter(),
            max_ops: self.store.max_ops.iter(),
            num_ops: self.store.num_ops.iter(),
            timestamps: self.store.timestamps.iter(),
            messages: self.store.messages.iter(),
            extra_bytes_meta: self.store.extra_bytes_meta.iter().with_acc(),
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
                let actor = self.store.actors[i].into();
                let timestamp = *self.store.timestamps.get(i).flatten().unwrap_or_default();
                let max_op = self.store.max_ops[i] as u64;
                let num_ops = *self.store.num_ops.get(i).flatten().unwrap_or_default();
                let message = self.store.messages.get(i).flatten();

                // FIXME - this needs a test
                let meta = self.store.extra_bytes_meta.get_with_acc(i).unwrap();
                let meta_range =
                    meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
                let extra = Cow::Borrowed(&self.store.extra_bytes_raw[meta_range]);

                let deps = self.parents(index).map(|p| p.0 as u64).collect::<Vec<_>>();
                let start_op = max_op - num_ops + 1;
                let seq = self.store.seq[i] as u64;
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
        for (actor_index, actor_changes) in self.index.seq_index.iter().enumerate() {
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
            Cow::Borrowed(&self.store.hashes)
        } else {
            let clock = self.seq_clock_for_heads(have_deps);
            Cow::Owned(
                self.get_build_indexes(clock)
                    .into_iter()
                    .filter_map(|node| self.store.hashes.get(node.0 as usize))
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
        self.index
            .seq_index
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|i| self.store.hashes.get(i.0 as usize))
            .ok_or(AutomergeError::InvalidSeq(seq))
            .copied()
    }

    fn update_heads(&mut self, change: &Change) {
        for d in change.deps() {
            self.index.heads.remove(d);
        }
        self.index.heads.insert(change.hash());
    }

    pub(crate) fn add_nodes<
        'a,
        I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone,
    >(
        &mut self,
        iter: I,
    ) {
        self.store
            .actors
            .extend(iter.clone().map(|(_, a)| ActorIdx::from(a)));
        self.store
            .seq
            .extend(iter.clone().map(|(c, _)| c.seq() as u32));
        self.store
            .max_ops
            .extend(iter.clone().map(|(c, _)| c.max_op() as u32));
        self.store
            .num_ops
            .extend(iter.clone().map(|(c, _)| c.len() as u64));
        self.store
            .timestamps
            .extend(iter.clone().map(|(c, _)| c.timestamp()));
        self.store
            .messages
            .extend(iter.clone().map(|(c, _)| c.message().cloned()));
        self.store
            .extra_bytes_meta
            .extend(iter.clone().map(|(c, _)| ValueMeta::from(c.extra_bytes())));
        self.store
            .parents
            .extend(std::iter::repeat_n(None, iter.len()));
        for (c, _) in iter {
            self.store
                .extra_bytes_raw
                .extend_from_slice(c.extra_bytes());
        }
    }

    fn add_changes<'a, I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone>(
        &mut self,
        iter: I,
    ) -> Result<(), MissingDep> {
        let node = NodeIdx(self.store.hashes.len() as u32);

        self.add_nodes(iter.clone());

        for (i, (change, actor)) in iter.enumerate() {
            let node_idx = node + i;
            let hash = change.hash();
            self.index.max_op = std::cmp::max(self.index.max_op, change.max_op() as u32);
            self.store.hashes.push(hash);
            debug_assert!(!self.index.nodes_by_hash.contains_key(&hash));
            if self.revocations.pending.remove(&hash) {
                self.revocations.all_dirty = true;
            }
            self.index.nodes_by_hash.insert(hash, node_idx);
            self.update_heads(change);

            if let Some(a) = change.author() {
                assert!(change.seq() == 1);
                self.assign_author_deferred(a.into(), actor)
            }

            assert!(actor < self.index.seq_index.len());
            assert_eq!(self.index.seq_index[actor].len() + 1, change.seq() as usize);
            self.index.seq_index[actor].push(node_idx);

            for parent_hash in change.deps().iter() {
                self.add_parent(node_idx, parent_hash);
            }

            if (node_idx + 1).0 % CACHE_STEP == 0 {
                self.cache_clock(node_idx);
            }
        }

        self.recompute_revocations();

        Ok(())
    }

    pub(crate) fn add_change(&mut self, change: &Change, actor: usize) -> Result<(), MissingDep> {
        let hash = change.hash();

        if self.index.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }

        for h in change.deps().iter() {
            if !self.index.nodes_by_hash.contains_key(h) {
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

        self.index.clock_cache.insert(node_idx, clock.clone());

        clock
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_hash: &ChangeHash) {
        debug_assert!(self.index.nodes_by_hash.contains_key(parent_hash));
        let parent_idx = *self.index.nodes_by_hash.get(parent_hash).unwrap();
        let new_edge_idx = EdgeIdx::new(self.store.edges.len());
        self.store.edges.push(Edge {
            target: parent_idx,
            next: None,
        });

        let child = &mut self.store.parents[child_idx.0 as usize];
        if let Some(edge_idx) = child {
            let mut edge = &mut self.store.edges[edge_idx.get()];
            while let Some(next) = edge.next {
                edge = &mut self.store.edges[next.get()];
            }
            edge.next = Some(new_edge_idx);
        } else {
            *child = Some(new_edge_idx);
        }
    }

    pub(crate) fn deps(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let mut iter = self
            .index
            .nodes_by_hash
            .get(hash)
            .map(|node| self.parents(*node));
        std::iter::from_fn(move || {
            let next = iter.as_mut()?.next()?;
            self.store.hashes.get(next.0 as usize).copied()
        })
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.store.parents[node_idx.0 as usize];
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.store.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            Some(edge.target)
        })
    }

    fn heads_to_nodes(&self, heads: &[ChangeHash]) -> Vec<NodeIdx> {
        heads
            .iter()
            .filter_map(|h| self.index.nodes_by_hash.get(h))
            .copied()
            .collect()
    }

    pub(crate) fn clock_for_heads(&self, heads: &[ChangeHash]) -> OpClock {
        let nodes = self.heads_to_nodes(heads);
        let mut clock = self.calculate_clock(nodes);
        for (actor, seq) in self.revocations.mask.iter() {
            clock.mask(usize::from(*actor), *seq);
        }
        self.to_op_clock(clock)
    }

    pub(crate) fn revocation_clock(&self) -> OpClock {
        (0_u32..self.num_actors() as u32)
            .map(|actor_idx| {
                let actor_idx = ActorIdx(actor_idx);
                if let Some(mask) = self.revocations.mask.get(&actor_idx) {
                    mask.map(|c| c.into())
                } else {
                    Some(u32::MAX)
                }
            })
            .collect()
    }

    fn to_op_clock(&self, c: SeqClock) -> OpClock {
        c.iter()
            .map(|(actor, seq)| {
                self.index
                    .seq_index
                    .get(actor)
                    .and_then(|v| v.get(seq?.get() as usize - 1))
                    .and_then(|i| self.store.max_ops.get(i.0 as usize))
                    .copied()
            })
            .collect()
    }

    fn seq_clock_for_heads(&self, heads: &[ChangeHash]) -> SeqClock {
        let nodes = self.heads_to_nodes(heads);
        self.calculate_clock(nodes)
    }

    fn clock_data_for(&self, idx: NodeIdx) -> Option<u32> {
        Some(*self.store.seq.get(idx.0 as usize)?)
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
            assert!(visited.len() <= self.store.hashes.len());
            visited.insert(idx);

            let actor = self.store.actors[idx.0 as usize];
            let data = self.clock_data_for(idx);
            clock.include(actor.into(), data);

            if let Some(cached) = self.index.clock_cache.get(&idx) {
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
            let hash = &self.store.hashes[idx.0 as usize];
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
        debug_assert!(graph.store.hashes.is_empty());

        for c in changes {
            let hash = c.hash();
            let idx = graph.store.hashes.len();
            let node_idx = NodeIdx(idx as u32);
            graph.index.nodes_by_hash.insert(hash, node_idx);
            graph.store.hashes.push(hash);
            if let Some(author) = c.author() {
                graph.assign_author(author.into(), graph.store.actors[idx].into())
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
        let revocations = HashMap::default();
        let revocations_mask = HashMap::default();
        let pending_revoke = HashSet::default();

        Ok(ChangeGraphCols(ChangeGraph {
            store: CommitStore {
                edges,
                hashes,
                actors,
                parents,
                seq,
                max_ops,
                num_ops,
                timestamps,
                messages,
                extra_bytes_meta,
                extra_bytes_raw,
            },
            index: CommitIndex {
                nodes_by_hash,
                heads,
                max_op,
                seq_index,
                clock_cache,
            },
            author_map: AuthorMap {
                authors,
                local: author,
                actor_author,
            },
            revocations: RevocationState {
                rules: revocations,
                pending: pending_revoke,
                mask: revocations_mask,
                dirty: HashSet::new(),
                all_dirty: false,
            },
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
            self.graph.insert_actor(self.actors.len());
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
        let extra = Cow::Borrowed(&self.graph.store.extra_bytes_raw[meta_range]);
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
        let extra = Cow::Borrowed(&self.graph.store.extra_bytes_raw[meta_range]);

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
