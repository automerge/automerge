//! Generic B+tree slab index.  Generic over an aggregate type `A`
//! implementing [`SlabAggregate`], so the same tree can back multiple
//! column types:
//!
//! * `DeltaColumn` uses `SlabBTree<SlabAgg>` with min/max
//!   pruning for value queries.
//! * `PrefixColumn` uses `SlabBTree<PrefixSlabWeight<P>>` as a direct
//!   replacement for the Fenwick BIT — stores len + prefix sum per slab.
//!
//! Each leaf holds up to `B` slab aggregates in slab-index order.  Each
//! internal node holds up to `B` child entries, where each entry carries
//! the child's merged aggregate and its slab count.
//!
//! Mutation API ([`splice`](SlabBTree::splice)) matches what
//! `Column::splice_inner` produces: a slab-range to replace + a stream
//! of new aggregates.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Range, SubAssign};

/// Branching factor.  Smaller = deeper tree + cheaper per-node rebalance;
/// larger = fewer nodes + more per-node cost.  `B = 16` gives height ≈ 5
/// for 1 M slabs.
const B: usize = 64;

// ── SlabAggregate trait ─────────────────────────────────────────────────────

/// A per-slab value that aggregates associatively (in left-to-right
/// order) into per-subtree values.  Used both as the leaf's per-slab
/// payload and the internal node's per-subtree summary.
///
/// The `merge` operation does **not** need to be commutative — only
/// associative and order-preserving.  Specifically, `merge(a, b) != merge(b, a)`
/// is fine (e.g. delta-running-min semantics) as long as the tree
/// always walks children left-to-right.
pub trait SlabAggregate: Clone + Default + std::fmt::Debug {
    /// Combine two adjacent subtree aggregates into one.
    fn merge(l: &Self, r: &Self) -> Self;
    /// Number of items covered by this aggregate.  Used by generic
    /// tree-walking helpers (e.g. `find_by_prefix`); concrete impls
    /// typically access a `.len` field directly.
    fn len(&self) -> usize;
}

/// Aggregate that additionally carries a prefix-sum value per slab,
/// enabling [`find_slab_at_item`](SlabBTree::find_slab_at_item) and
/// [`find_slab_at_prefix`](SlabBTree::find_slab_at_prefix) to work
/// aggregate-agnostically.
///
/// Implemented by [`PrefixSlabWeight<P>`](super::prefix::PrefixSlabWeight)
/// (prefix lives in `.prefix`) and [`SlabAgg`] (prefix lives in `.total`).
pub trait PrefixAggregate: SlabAggregate {
    type Prefix: Copy + Default + PartialOrd + Add<Output = Self::Prefix> + Debug;
    fn prefix(&self) -> Self::Prefix;
}

fn merge_all<'a, A: SlabAggregate + 'a>(aggs: impl IntoIterator<Item = &'a A>) -> A {
    let mut acc = A::default();
    for a in aggs {
        acc = A::merge(&acc, a);
    }
    acc
}

// ── SlabAgg (used by DeltaColumn) ───────────────────────────────────────────

/// Running min/max of a delta column's prefix-sum range.  The specific
/// aggregate used by `DeltaColumn`'s default `IndexedDeltaWeightFn` for
/// value-range pruning.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SlabAgg {
    pub len: usize,
    pub total: i64,
    pub min_offset: i64,
    pub max_offset: i64,
}

impl SlabAggregate for SlabAgg {
    fn merge(l: &Self, r: &Self) -> Self {
        if l.len == 0 {
            return *r;
        }
        if r.len == 0 {
            return *l;
        }
        SlabAgg {
            len: l.len + r.len,
            total: l.total + r.total,
            min_offset: l.min_offset.min(l.total + r.min_offset),
            max_offset: l.max_offset.max(l.total + r.max_offset),
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl PrefixAggregate for SlabAgg {
    type Prefix = i64;
    fn prefix(&self) -> i64 {
        self.total
    }
}

impl SlabAgg {
    fn contains(&self, target: i64, prefix_before: i64) -> bool {
        self.len > 0
            && target >= prefix_before + self.min_offset
            && target <= prefix_before + self.max_offset
    }

    fn overlaps(&self, lo: i64, hi: i64, prefix_before: i64) -> bool {
        self.len > 0
            && prefix_before + self.max_offset >= lo
            && prefix_before + self.min_offset <= hi
    }
}

// ── SlabAggregate for usize (LenWeight) ─────────────────────────────────────

impl SlabAggregate for usize {
    fn merge(l: &Self, r: &Self) -> Self {
        *l + *r
    }
    fn len(&self) -> usize {
        *self
    }
}

// ── SlabAggregate for PrefixSlabWeight ──────────────────────────────────────

impl<P> SlabAggregate for super::prefix::PrefixSlabWeight<P>
where
    P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    fn merge(l: &Self, r: &Self) -> Self {
        let mut out = *l;
        out += *r;
        out
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl<P> PrefixAggregate for super::prefix::PrefixSlabWeight<P>
where
    P: Copy + Default + std::fmt::Debug + AddAssign + SubAssign + PartialOrd + Add<Output = P>,
{
    type Prefix = P;
    fn prefix(&self) -> P {
        self.prefix
    }
}

// ── Nodes ───────────────────────────────────────────────────────────────────

type NodeId = u32;

#[derive(Debug, Clone)]
enum Node<A: SlabAggregate> {
    Leaf(Leaf<A>),
    Internal(Internal<A>),
}

#[derive(Debug, Clone)]
struct Leaf<A: SlabAggregate> {
    aggs: Vec<A>,
}

impl<A: SlabAggregate> Default for Leaf<A> {
    fn default() -> Self {
        Self { aggs: Vec::new() }
    }
}

#[derive(Debug, Clone)]
struct Internal<A: SlabAggregate> {
    children: Vec<ChildSlot<A>>,
}

/// One entry in an internal node: pointer to a child subtree plus that
/// subtree's aggregate and slab count (redundant with what the child
/// root could recompute, but cached here so top-down walks are
/// constant-time per node).
#[derive(Debug, Copy, Clone)]
struct ChildSlot<A: SlabAggregate> {
    id: NodeId,
    agg: A,
    slab_count: usize,
}

// ── Tree ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SlabBTree<A: SlabAggregate> {
    nodes: Vec<Option<Node<A>>>,
    free: Vec<NodeId>,
    root: NodeId,
    total_slabs: usize,
    _phantom: PhantomData<A>,
}

impl<A: SlabAggregate> Default for SlabBTree<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: SlabAggregate> SlabBTree<A> {
    pub(crate) fn new() -> Self {
        let mut nodes = Vec::with_capacity(8);
        nodes.push(Some(Node::Leaf(Leaf::default())));
        Self {
            nodes,
            free: Vec::new(),
            root: 0,
            total_slabs: 0,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.total_slabs
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.total_slabs == 0
    }

    #[allow(dead_code)]
    pub(crate) fn root_agg(&self) -> A {
        match self.node(self.root) {
            Node::Leaf(l) => merge_all(&l.aggs),
            Node::Internal(n) => merge_all(n.children.iter().map(|c| &c.agg)),
        }
    }

    // ── Arena helpers ───────────────────────────────────────────────────

    fn alloc(&mut self, node: Node<A>) -> NodeId {
        if let Some(id) = self.free.pop() {
            self.nodes[id as usize] = Some(node);
            id
        } else {
            let id = self.nodes.len() as NodeId;
            self.nodes.push(Some(node));
            id
        }
    }

    fn free(&mut self, id: NodeId) {
        self.nodes[id as usize] = None;
        self.free.push(id);
    }

    fn node(&self, id: NodeId) -> &Node<A> {
        self.nodes[id as usize].as_ref().expect("freed node id")
    }

    fn node_mut(&mut self, id: NodeId) -> &mut Node<A> {
        self.nodes[id as usize].as_mut().expect("freed node id")
    }

    /// Aggregate of all slabs under node `id`.
    fn agg_of(&self, id: NodeId) -> A {
        match self.node(id) {
            Node::Leaf(l) => merge_all(&l.aggs),
            Node::Internal(n) => merge_all(n.children.iter().map(|c| &c.agg)),
        }
    }

    // ── Bulk construction ───────────────────────────────────────────────

    pub(crate) fn from_iter<I: IntoIterator<Item = A>>(iter: I) -> Self {
        let aggs: Vec<A> = iter.into_iter().collect();
        let mut tree = Self::new();
        if aggs.is_empty() {
            return tree;
        }
        tree.total_slabs = aggs.len();

        let mut level: Vec<(NodeId, A, usize)> = aggs
            .chunks(B)
            .map(|chunk| {
                let agg = merge_all(chunk);
                let slab_count = chunk.len();
                let leaf = Leaf {
                    aggs: chunk.to_vec(),
                };
                let id = tree.alloc(Node::Leaf(leaf));
                (id, agg, slab_count)
            })
            .collect();

        while level.len() > 1 {
            level = level
                .chunks(B)
                .map(|chunk| {
                    let children: Vec<ChildSlot<A>> = chunk
                        .iter()
                        .map(|(id, agg, slab_count)| ChildSlot {
                            id: *id,
                            agg: agg.clone(),
                            slab_count: *slab_count,
                        })
                        .collect();
                    let agg = merge_all(children.iter().map(|c| &c.agg));
                    let slab_count = children.iter().map(|c| c.slab_count).sum();
                    let id = tree.alloc(Node::Internal(Internal { children }));
                    (id, agg, slab_count)
                })
                .collect();
        }

        let (root_id, _, _) = level[0];
        tree.free(tree.root);
        tree.root = root_id;
        tree
    }

    // ── Splice ──────────────────────────────────────────────────────────

    pub(crate) fn update_slab(&mut self, slab_idx: usize, new_agg: A) {
        assert!(slab_idx < self.total_slabs, "update_slab out of bounds");
        // Descend to the leaf, recording each (node, child_idx_taken).
        let mut path: Vec<(NodeId, usize)> = Vec::with_capacity(8);
        let mut remaining = slab_idx;
        let mut node_id = self.root;
        loop {
            let next = match self.node(node_id) {
                Node::Leaf(_) => break,
                Node::Internal(n) => {
                    let mut picked = None;
                    for (i, c) in n.children.iter().enumerate() {
                        if remaining < c.slab_count {
                            picked = Some((i, c.id));
                            break;
                        }
                        remaining -= c.slab_count;
                    }
                    picked.expect("slab_idx should have been bounds-checked")
                }
            };
            let (child_idx, child_id) = next;
            path.push((node_id, child_idx));
            node_id = child_id;
        }

        // Update the leaf slot.
        if let Node::Leaf(l) = self.node_mut(node_id) {
            l.aggs[remaining] = new_agg;
        }

        // Walk back up, refreshing each internal node's cached child agg.
        for (parent_id, child_idx) in path.into_iter().rev() {
            // Find which child id this was in the parent (we only stored child_idx).
            let child_id = match self.node(parent_id) {
                Node::Internal(n) => n.children[child_idx].id,
                Node::Leaf(_) => unreachable!("leaf can't be a parent"),
            };
            let new_child_agg = self.agg_of(child_id);
            if let Node::Internal(p) = self.node_mut(parent_id) {
                p.children[child_idx].agg = new_child_agg;
                // slab_count unchanged — we don't add/remove slabs here.
            }
        }
    }

    #[inline(never)]
    pub(crate) fn splice<I: IntoIterator<Item = A>>(&mut self, range: Range<usize>, new_aggs: I) {
        assert!(range.end <= self.total_slabs, "splice out of bounds");
        assert!(range.start <= range.end, "invalid range");
        let new: Vec<A> = new_aggs.into_iter().collect();

        if range.is_empty() && new.is_empty() {
            return;
        }

        // Decompose into overwrite + structural delta.  When the number
        // of removed and inserted aggs match (the common case for
        // replace-style mutations), the structural delta is empty and
        // the overwrite is a pure in-place rewrite with zero allocation.
        let common = range.len().min(new.len());
        if common > 0 {
            self.overwrite_range(range.start, &new[..common]);
        }

        let delta_start = range.start + common;
        let delta_range = delta_start..range.end;
        let delta_new = &new[common..];
        if delta_range.is_empty() && delta_new.is_empty() {
            return;
        }

        self.splice_structural(&delta_range, delta_new);
    }

    /// Overwrite existing agg values in-place without changing tree
    /// structure.  Walks across leaf boundaries as needed.  O(k + log n).
    fn overwrite_range(&mut self, start: usize, new: &[A]) {
        let mut cursor = start;
        let mut i = 0;
        while i < new.len() {
            let Some((path, leaf_id, leaf_start)) = self.locate_leaf_for_start(cursor) else {
                return;
            };
            let leaf_len = match self.node(leaf_id) {
                Node::Leaf(l) => l.aggs.len(),
                _ => unreachable!(),
            };
            let local_start = cursor - leaf_start;
            let count = (leaf_len - local_start).min(new.len() - i);
            if let Node::Leaf(l) = self.node_mut(leaf_id) {
                l.aggs[local_start..local_start + count].clone_from_slice(&new[i..i + count]);
            }
            self.update_ancestor_aggregates(&path, leaf_id);
            cursor += count;
            i += count;
        }
    }

    /// Handle the structural part of a splice (the delta after the common
    /// prefix has been overwritten).  Usually tiny (0-2 items).
    fn splice_structural(&mut self, range: &Range<usize>, new: &[A]) {
        if self.total_slabs == 0 {
            self.splice_into_empty(new);
            return;
        }
        if self.try_single_leaf_splice(range, new) {
            return;
        }
        self.splice_via_lca(range, new);
    }

    /// General multi-leaf splice via the lowest common ancestor (LCA).
    ///
    /// Locates the LCA of the start and end positions, collects
    /// aggregates from the affected subtrees, applies the splice, builds
    /// replacement subtrees at the correct height, and replaces the
    /// LCA's child span.  O(log n + k) — no full-tree rebuild.
    fn splice_via_lca(&mut self, range: &Range<usize>, new: &[A]) {
        let Some((start_path, _start_leaf, _)) = self.locate_leaf_for_start(range.start) else {
            return;
        };
        let end_query = if range.is_empty() {
            range.start
        } else {
            range.end - 1
        };
        let Some((end_path, _end_leaf, _)) = self.locate_leaf_for_start(end_query) else {
            return;
        };

        // Find the divergence point — where start and end paths go to
        // different children of the same node.  That node is the LCA.
        let diverge = start_path
            .iter()
            .zip(end_path.iter())
            .position(|(a, b)| a.1 != b.1);
        let Some(d) = diverge else {
            // Paths identical — single leaf.  Shouldn't reach here.
            // Defensive: rebuild.
            let mut all: Vec<A> = Vec::with_capacity(self.total_slabs + new.len() - range.len());
            self.collect_into(self.root, &mut all);
            all.splice(range.clone(), new.iter().cloned());
            *self = Self::from_iter(all);
            return;
        };

        debug_assert_eq!(start_path[d].0, end_path[d].0);
        let lca_id = start_path[d].0;
        let left_child_idx = start_path[d].1;
        let right_child_idx = end_path[d].1;
        let lca_path = &start_path[..d];

        // Compute the absolute slab offset of LCA.children[left_child_idx].
        let mut lca_abs_offset: usize = 0;
        for &(ancestor_id, child_idx) in lca_path {
            let n = match self.node(ancestor_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            lca_abs_offset += n.children[..child_idx]
                .iter()
                .map(|c| c.slab_count)
                .sum::<usize>();
        }
        let left_child_offset: usize = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children[..left_child_idx]
                .iter()
                .map(|c| c.slab_count)
                .sum()
        };
        let combined_abs_start = lca_abs_offset + left_child_offset;

        // Determine height of the affected children.
        let child_height = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            self.subtree_height(n.children[left_child_idx].id)
        };

        // Collect aggregates from the affected subtrees + free them.
        let child_ids: Vec<NodeId> = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children[left_child_idx..=right_child_idx]
                .iter()
                .map(|c| c.id)
                .collect()
        };
        let mut combined: Vec<A> = Vec::new();
        for &cid in &child_ids {
            self.collect_into(cid, &mut combined);
        }
        for &cid in &child_ids {
            self.free_subtree(cid);
        }

        // Apply the splice in local coordinates.
        let local_start = range.start - combined_abs_start;
        let local_end = range.end - combined_abs_start;
        debug_assert!(local_end <= combined.len());
        combined.splice(local_start..local_end, new.iter().cloned());

        self.total_slabs = self.total_slabs + new.len() - range.len();

        // Build fresh subtrees at the same height as the old ones.
        let new_children = self.build_children_at_height(&combined, child_height);

        // Replace child span in LCA.
        let new_lca_len = {
            let n = match self.node_mut(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children
                .splice(left_child_idx..=right_child_idx, new_children);
            n.children.len()
        };

        // Propagate up.
        let lca_path_vec = lca_path.to_vec();
        if new_lca_len <= B && new_lca_len > 0 {
            let (agg, count) = self.internal_summary(lca_id);
            let replacement = vec![ChildSlot {
                id: lca_id,
                agg,
                slab_count: count,
            }];
            self.propagate_splits(lca_path_vec, replacement);
        } else if new_lca_len > B {
            self.split_and_propagate(lca_id, lca_path_vec);
        }
    }

    fn free_subtree(&mut self, id: NodeId) {
        let child_ids: Vec<NodeId> = match self.node(id) {
            Node::Internal(n) => n.children.iter().map(|c| c.id).collect(),
            Node::Leaf(_) => Vec::new(),
        };
        for cid in child_ids {
            self.free_subtree(cid);
        }
        self.free(id);
    }

    fn subtree_height(&self, id: NodeId) -> usize {
        match self.node(id) {
            Node::Leaf(_) => 0,
            Node::Internal(n) => {
                if n.children.is_empty() {
                    0
                } else {
                    1 + self.subtree_height(n.children[0].id)
                }
            }
        }
    }

    fn build_children_at_height(&mut self, aggs: &[A], height: usize) -> Vec<ChildSlot<A>> {
        if aggs.is_empty() {
            return Vec::new();
        }
        if height == 0 {
            return self.make_fresh_leaves(aggs);
        }
        let below = self.build_children_at_height(aggs, height - 1);
        if below.is_empty() {
            return Vec::new();
        }
        let num = below.len().div_ceil(B);
        let base = below.len() / num;
        let rem = below.len() % num;
        let mut result = Vec::with_capacity(num);
        let mut i = 0;
        for c in 0..num {
            let size = base + if c < rem { 1 } else { 0 };
            let children: Vec<ChildSlot<A>> = below[i..i + size].to_vec();
            let agg = merge_all(children.iter().map(|c| &c.agg));
            let count: usize = children.iter().map(|c| c.slab_count).sum();
            let id = self.alloc(Node::Internal(Internal { children }));
            result.push(ChildSlot {
                id,
                agg,
                slab_count: count,
            });
            i += size;
        }
        result
    }

    fn make_fresh_leaves(&mut self, aggs: &[A]) -> Vec<ChildSlot<A>> {
        if aggs.is_empty() {
            return Vec::new();
        }
        let total = aggs.len();
        let num = total.div_ceil(B);
        let base = total / num;
        let rem = total % num;
        let mut result = Vec::with_capacity(num);
        let mut i = 0;
        for c in 0..num {
            let size = base + if c < rem { 1 } else { 0 };
            let chunk = &aggs[i..i + size];
            let agg = merge_all(chunk);
            let leaf = Leaf {
                aggs: chunk.to_vec(),
            };
            let id = self.alloc(Node::Leaf(leaf));
            result.push(ChildSlot {
                id,
                agg,
                slab_count: size,
            });
            i += size;
        }
        result
    }

    fn split_and_propagate(&mut self, node_id: NodeId, path: Vec<(NodeId, usize)>) {
        let total = match self.node(node_id) {
            Node::Internal(n) => n.children.len(),
            _ => unreachable!(),
        };
        let num_chunks = total.div_ceil(B);
        let base = total / num_chunks;
        let rem = total % num_chunks;
        let first_size = base + if rem > 0 { 1 } else { 0 };

        let drained: Vec<ChildSlot<A>> = {
            let n = match self.node_mut(node_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children.drain(first_size..).collect()
        };

        let mut replacement: Vec<ChildSlot<A>> = Vec::new();
        let (first_agg, first_count) = self.internal_summary(node_id);
        replacement.push(ChildSlot {
            id: node_id,
            agg: first_agg,
            slab_count: first_count,
        });

        let mut iter = drained.into_iter();
        for c in 1..num_chunks {
            let size = base + if c < rem { 1 } else { 0 };
            let children: Vec<ChildSlot<A>> = iter.by_ref().take(size).collect();
            let agg = merge_all(children.iter().map(|c| &c.agg));
            let count: usize = children.iter().map(|c| c.slab_count).sum();
            let id = self.alloc(Node::Internal(Internal { children }));
            replacement.push(ChildSlot {
                id,
                agg,
                slab_count: count,
            });
        }

        self.propagate_splits(path, replacement);
    }

    /// Splice into an empty tree.  If ≤ B new aggregates, fill the root
    /// leaf; otherwise bulk-build.
    fn splice_into_empty(&mut self, new: &[A]) {
        if new.is_empty() {
            return;
        }
        if new.len() <= B {
            if let Node::Leaf(l) = self.node_mut(self.root) {
                l.aggs.extend_from_slice(new);
            }
            self.total_slabs = new.len();
            return;
        }
        *self = Self::from_iter(new.iter().cloned());
    }

    /// Try a single-leaf splice.  Returns `false` if `range` spans
    /// multiple leaves (caller falls back to rebuild).
    ///
    /// Handles two sub-cases:
    ///   * result ≤ B — in-place edit, O(log n) ancestor aggregate update.
    ///   * result > B — split the leaf into ⌈len/B⌉ pieces, cascade
    ///     splits up the parent chain, grow the root if it overflows.
    fn try_single_leaf_splice(&mut self, range: &Range<usize>, new: &[A]) -> bool {
        let Some((path, leaf_id, leaf_start)) = self.locate_leaf_for_start(range.start) else {
            return false;
        };

        let leaf_aggs_len = match self.node(leaf_id) {
            Node::Leaf(l) => l.aggs.len(),
            _ => unreachable!(),
        };
        let leaf_end = leaf_start + leaf_aggs_len;

        // Must fit entirely in this one leaf.
        if range.start < leaf_start || range.end > leaf_end {
            return false;
        }

        let local_start = range.start - leaf_start;
        let local_end = range.end - leaf_start;
        let new_leaf_len = leaf_aggs_len + new.len() - range.len();

        self.total_slabs = self.total_slabs + new.len() - range.len();

        // Fast path: result fits in one leaf.  Just splice in place and
        // walk the path updating aggregates.
        if new_leaf_len <= B {
            if new_leaf_len == 0 && leaf_id != self.root {
                // Empty non-root leaf would break the B+tree invariant.
                // Fall back to rebuild.
                self.total_slabs = self.total_slabs + range.len() - new.len();
                return false;
            }
            if let Node::Leaf(l) = self.node_mut(leaf_id) {
                l.aggs.splice(local_start..local_end, new.iter().cloned());
            }
            self.update_ancestor_aggregates(&path, leaf_id);
            return true;
        }

        // Overflow: build the combined vector, split it into ≤ B-sized
        // chunks, reuse `leaf_id` for the first chunk and allocate new
        // leaves for the rest.  Then propagate up.
        let combined: Vec<A> = {
            let l = match self.node(leaf_id) {
                Node::Leaf(l) => l,
                _ => unreachable!(),
            };
            let mut v = Vec::with_capacity(new_leaf_len);
            v.extend_from_slice(&l.aggs[..local_start]);
            v.extend_from_slice(new);
            v.extend_from_slice(&l.aggs[local_end..]);
            v
        };

        let new_children = self.make_leaves_from_aggs(leaf_id, &combined);
        self.propagate_splits(path, new_children);
        true
    }

    /// Walk from the root toward the leaf containing slab index `start`.
    /// Returns the ancestor path (`(parent_id, child_idx)` stack), the
    /// leaf id, and `leaf_start` (the slab index of the leaf's first
    /// entry).  Handles the append case (`start == total_slabs`) by
    /// landing in the last leaf.
    #[allow(clippy::type_complexity)]
    fn locate_leaf_for_start(&self, start: usize) -> Option<(Vec<(NodeId, usize)>, NodeId, usize)> {
        let mut path: Vec<(NodeId, usize)> = Vec::with_capacity(8);
        let mut remaining = start;
        let mut leaf_start = 0usize;
        let mut node_id = self.root;
        loop {
            match self.node(node_id) {
                Node::Leaf(_) => return Some((path, node_id, leaf_start)),
                Node::Internal(n) => {
                    if n.children.is_empty() {
                        return None;
                    }
                    let last_idx = n.children.len() - 1;
                    let mut descended = false;
                    for (i, c) in n.children.iter().enumerate() {
                        // Descend into this child if `remaining` falls
                        // strictly inside its range, or if we're at the
                        // last child and hit its right boundary (append).
                        if remaining < c.slab_count || (i == last_idx && remaining == c.slab_count)
                        {
                            path.push((node_id, i));
                            node_id = c.id;
                            descended = true;
                            break;
                        }
                        remaining -= c.slab_count;
                        leaf_start += c.slab_count;
                    }
                    if !descended {
                        return None;
                    }
                }
            }
        }
    }

    /// After an in-place leaf edit, walk up `path` recomputing each
    /// ancestor's `agg` + `slab_count` from scratch.  O(log n) nodes,
    /// each summary is O(B) ≤ 16 ops.
    fn update_ancestor_aggregates(&mut self, path: &[(NodeId, usize)], leaf_id: NodeId) {
        let mut current = leaf_id;
        for &(parent_id, child_idx) in path.iter().rev() {
            let new_agg = self.agg_of(current);
            let new_count = self.slab_count_of(current);
            if let Node::Internal(p) = self.node_mut(parent_id) {
                p.children[child_idx].agg = new_agg;
                p.children[child_idx].slab_count = new_count;
            }
            current = parent_id;
        }
    }

    /// Split `aggs` into ⌈len/B⌉ chunks as evenly as possible, reusing
    /// `first_leaf_id` for the first chunk and allocating new leaves for
    /// the rest.  Returns the resulting child slots in slab order.
    fn make_leaves_from_aggs(&mut self, first_leaf_id: NodeId, aggs: &[A]) -> Vec<ChildSlot<A>> {
        debug_assert!(!aggs.is_empty());
        let total = aggs.len();
        let num_leaves = total.div_ceil(B);
        let base = total / num_leaves;
        let rem = total % num_leaves;

        let mut result = Vec::with_capacity(num_leaves);
        let mut i = 0;
        for c in 0..num_leaves {
            let size = base + if c < rem { 1 } else { 0 };
            let chunk = &aggs[i..i + size];
            let agg = merge_all(chunk);
            let slab_count = size;
            let id = if c == 0 {
                if let Node::Leaf(l) = self.node_mut(first_leaf_id) {
                    l.aggs.clear();
                    l.aggs.extend_from_slice(chunk);
                }
                first_leaf_id
            } else {
                let leaf = Leaf {
                    aggs: chunk.to_vec(),
                };
                self.alloc(Node::Leaf(leaf))
            };
            result.push(ChildSlot {
                id,
                agg,
                slab_count,
            });
            i += size;
        }
        result
    }

    /// Cascade structural changes up the ancestor path.  At each level,
    /// `replacement` is the set of child slots that should replace the
    /// single child slot at `path[level]`.  If that overflows the
    /// parent, split the parent and produce a new replacement set for
    /// the next level up.  If we run out of path (reached the root) and
    /// still have multiple replacements, grow a new root.
    fn propagate_splits(&mut self, path: Vec<(NodeId, usize)>, mut replacement: Vec<ChildSlot<A>>) {
        for (parent_id, child_idx) in path.into_iter().rev() {
            let new_total = {
                let p = match self.node_mut(parent_id) {
                    Node::Internal(n) => n,
                    _ => unreachable!(),
                };
                p.children
                    .splice(child_idx..child_idx + 1, std::mem::take(&mut replacement));
                p.children.len()
            };

            if new_total <= B {
                let (agg, count) = self.internal_summary(parent_id);
                replacement.push(ChildSlot {
                    id: parent_id,
                    agg,
                    slab_count: count,
                });
                continue;
            }

            // Parent overflowed — split into ⌈new_total/B⌉ internals.
            let num_chunks = new_total.div_ceil(B);
            let base = new_total / num_chunks;
            let rem = new_total % num_chunks;
            let first_size = base + if rem > 0 { 1 } else { 0 };

            // Drain the overflow tail out of parent.
            let drained: Vec<ChildSlot<A>> = {
                let p = match self.node_mut(parent_id) {
                    Node::Internal(n) => n,
                    _ => unreachable!(),
                };
                p.children.drain(first_size..).collect()
            };

            let (first_agg, first_count) = self.internal_summary(parent_id);
            replacement.push(ChildSlot {
                id: parent_id,
                agg: first_agg,
                slab_count: first_count,
            });

            let mut iter = drained.into_iter();
            for c in 1..num_chunks {
                let size = base + if c < rem { 1 } else { 0 };
                let children: Vec<ChildSlot<A>> = iter.by_ref().take(size).collect();
                debug_assert_eq!(children.len(), size);
                let agg = merge_all(children.iter().map(|c| &c.agg));
                let count: usize = children.iter().map(|c| c.slab_count).sum();
                let id = self.alloc(Node::Internal(Internal { children }));
                replacement.push(ChildSlot {
                    id,
                    agg,
                    slab_count: count,
                });
            }
            debug_assert!(iter.next().is_none());
        }

        // Path exhausted — decide what to do at the root.
        match replacement.len() {
            0 => {}
            1 => {
                let slot = replacement[0].clone();
                if slot.id != self.root {
                    self.free(self.root);
                    self.root = slot.id;
                }
            }
            _ => {
                // Root overflowed — grow a new root one level up.
                let new_root = Internal {
                    children: replacement,
                };
                let new_root_id = self.alloc(Node::Internal(new_root));
                self.root = new_root_id;
            }
        }
    }

    fn internal_summary(&self, id: NodeId) -> (A, usize) {
        match self.node(id) {
            Node::Internal(n) => {
                let agg = merge_all(n.children.iter().map(|c| &c.agg));
                let count: usize = n.children.iter().map(|c| c.slab_count).sum();
                (agg, count)
            }
            _ => unreachable!(),
        }
    }

    fn slab_count_of(&self, id: NodeId) -> usize {
        match self.node(id) {
            Node::Leaf(l) => l.aggs.len(),
            Node::Internal(n) => n.children.iter().map(|c| c.slab_count).sum(),
        }
    }

    fn collect_into(&self, id: NodeId, out: &mut Vec<A>) {
        match self.node(id) {
            Node::Leaf(l) => out.extend_from_slice(&l.aggs),
            Node::Internal(n) => {
                for c in &n.children {
                    self.collect_into(c.id, out);
                }
            }
        }
    }

    // ── Positional lookup ───────────────────────────────────────────────

    /// Find the slab index containing the `pos`-th item and the
    /// item-count of items strictly before that slab.  Returns `None`
    /// if `pos >= total_items`.
    #[inline(never)]
    pub(crate) fn find_by_prefix(&self, pos: usize) -> Option<(usize, usize)> {
        let root_agg = self.root_agg();
        if pos >= root_agg.len() {
            return None;
        }
        let mut node = self.root;
        let mut pos = pos;
        let mut slab_idx_base = 0;
        let mut items_before = 0;
        loop {
            match self.node(node) {
                Node::Leaf(l) => {
                    for (i, a) in l.aggs.iter().enumerate() {
                        if pos < a.len() {
                            return Some((slab_idx_base + i, items_before));
                        }
                        pos -= a.len();
                        items_before += a.len();
                    }
                    return None;
                }
                Node::Internal(n) => {
                    for c in &n.children {
                        if pos < c.agg.len() {
                            node = c.id;
                            break;
                        }
                        pos -= c.agg.len();
                        items_before += c.agg.len();
                        slab_idx_base += c.slab_count;
                    }
                }
            }
        }
    }
}

// ── Value-range queries (SlabAgg-specific) ──────────────────────────────────

impl SlabBTree<SlabAgg> {
    #[inline(never)]
    pub(crate) fn find_by_value(&self, target: i64) -> FindByValue<'_> {
        FindByValue::new(self, target)
    }

    #[inline(never)]
    pub(crate) fn find_by_value_range(&self, lo: i64, hi: i64) -> FindByValueRange<'_> {
        FindByValueRange::new(self, lo, hi)
    }
}

pub struct FindByValue<'a> {
    tree: &'a SlabBTree<SlabAgg>,
    target: i64,
    stack: Vec<Frame>,
}

#[derive(Copy, Clone)]
struct Frame {
    node: NodeId,
    next_child: usize,
    prefix_before: i64,
    slab_base: usize,
    items_before: usize,
}

impl<'a> FindByValue<'a> {
    fn new(tree: &'a SlabBTree<SlabAgg>, target: i64) -> Self {
        let mut s = Self {
            tree,
            target,
            stack: Vec::with_capacity(16),
        };
        s.stack.push(Frame {
            node: tree.root,
            next_child: 0,
            prefix_before: 0,
            slab_base: 0,
            items_before: 0,
        });
        s
    }
}

impl Iterator for FindByValue<'_> {
    /// `(slab_idx, items_before_slab, prefix_before_slab)`.
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            match self.tree.node(frame.node) {
                Node::Leaf(l) => {
                    while frame.next_child < l.aggs.len() {
                        let i = frame.next_child;
                        let agg = &l.aggs[i];
                        let prefix = frame.prefix_before;
                        let slab_idx = frame.slab_base + i;
                        let items_before = frame.items_before;
                        frame.next_child += 1;
                        frame.prefix_before += agg.total;
                        frame.items_before += agg.len;
                        if agg.contains(self.target, prefix) {
                            return Some((slab_idx, items_before, prefix));
                        }
                    }
                    self.stack.pop();
                }
                Node::Internal(n) => {
                    if frame.next_child >= n.children.len() {
                        self.stack.pop();
                        continue;
                    }
                    let child = n.children[frame.next_child];
                    let prefix = frame.prefix_before;
                    let slab_base = frame.slab_base;
                    let items_before = frame.items_before;
                    frame.next_child += 1;
                    frame.prefix_before += child.agg.total;
                    frame.slab_base += child.slab_count;
                    frame.items_before += child.agg.len;
                    if child.agg.contains(self.target, prefix) {
                        self.stack.push(Frame {
                            node: child.id,
                            next_child: 0,
                            prefix_before: prefix,
                            slab_base,
                            items_before,
                        });
                    }
                }
            }
        }
        None
    }
}

pub struct FindByValueRange<'a> {
    tree: &'a SlabBTree<SlabAgg>,
    lo: i64,
    hi: i64,
    stack: Vec<Frame>,
}

impl<'a> FindByValueRange<'a> {
    fn new(tree: &'a SlabBTree<SlabAgg>, lo: i64, hi: i64) -> Self {
        let mut s = Self {
            tree,
            lo,
            hi,
            stack: Vec::with_capacity(16),
        };
        s.stack.push(Frame {
            node: tree.root,
            next_child: 0,
            prefix_before: 0,
            slab_base: 0,
            items_before: 0,
        });
        s
    }
}

impl Iterator for FindByValueRange<'_> {
    /// `(slab_idx, items_before_slab, prefix_before_slab)`.
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            match self.tree.node(frame.node) {
                Node::Leaf(l) => {
                    while frame.next_child < l.aggs.len() {
                        let i = frame.next_child;
                        let agg = &l.aggs[i];
                        let prefix = frame.prefix_before;
                        let slab_idx = frame.slab_base + i;
                        let items_before = frame.items_before;
                        frame.next_child += 1;
                        frame.prefix_before += agg.total;
                        frame.items_before += agg.len;
                        if agg.overlaps(self.lo, self.hi, prefix) {
                            return Some((slab_idx, items_before, prefix));
                        }
                    }
                    self.stack.pop();
                }
                Node::Internal(n) => {
                    if frame.next_child >= n.children.len() {
                        self.stack.pop();
                        continue;
                    }
                    let child = n.children[frame.next_child];
                    let prefix = frame.prefix_before;
                    let slab_base = frame.slab_base;
                    let items_before = frame.items_before;
                    frame.next_child += 1;
                    frame.prefix_before += child.agg.total;
                    frame.slab_base += child.slab_count;
                    frame.items_before += child.agg.len;
                    if child.agg.overlaps(self.lo, self.hi, prefix) {
                        self.stack.push(Frame {
                            node: child.id,
                            next_child: 0,
                            prefix_before: prefix,
                            slab_base,
                            items_before,
                        });
                    }
                }
            }
        }
        None
    }
}

// ── Prefix-sum queries (generic over PrefixAggregate) ───────────────────────

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct FoundSlab<A: PrefixAggregate> {
    pub(crate) index: usize,
    pub(crate) prefix: A::Prefix,
    //  pub(crate) weight: A,
    pub(crate) pos: usize,
}

impl<A: PrefixAggregate> FoundSlab<A> {
    #[cfg(test)]
    pub(crate) fn decompose(&self) -> (usize, A::Prefix, usize) {
        (self.index, self.prefix, self.pos)
    }
}

impl<A: PrefixAggregate> SlabBTree<A> {
    /// Find the landing slab for `item_idx`: returns
    /// `(slab_idx, prefix_before_slab, items_before_slab)`.  The
    /// landing slab is the one containing item `item_idx`; if
    /// `item_idx >= total_items` returns the final position with the
    /// full prefix/items totals.
    ///
    /// O(log n) — descends the tree by `len`, accumulating `prefix`
    /// and `len` of children visited before the descent.
    #[inline(never)]
    pub(crate) fn find_slab_at_item(&self, item_idx: usize) -> FoundSlab<A> {
        let mut prefix = A::Prefix::default();
        let mut pos = 0usize;
        let mut slab_base = 0usize;
        let mut remaining = item_idx;
        let mut node = self.root;
        loop {
            match self.node(node) {
                Node::Leaf(l) => {
                    for (i, a) in l.aggs.iter().enumerate() {
                        let l_len = a.len();
                        if remaining < l_len {
                            return FoundSlab {
                                index: slab_base + i,
                                prefix,
                                pos,
                            };
                        }
                        prefix = prefix + a.prefix();
                        pos += l_len;
                        remaining -= l_len;
                    }
                    return FoundSlab {
                        index: slab_base + l.aggs.len(),
                        prefix,
                        pos,
                    };
                }
                Node::Internal(n) => {
                    let mut descended = false;
                    for c in &n.children {
                        let c_len = c.agg.len();
                        if remaining < c_len {
                            node = c.id;
                            descended = true;
                            break;
                        }
                        prefix = prefix + c.agg.prefix();
                        pos += c_len;
                        slab_base += c.slab_count;
                        remaining -= c_len;
                    }
                    if !descended {
                        return FoundSlab {
                            index: slab_base,
                            prefix,
                            pos,
                        };
                    }
                }
            }
        }
    }

    /// Find the slab whose prefix sum first reaches or exceeds `target`.
    /// Returns `(slab_idx, prefix_before_slab, items_before_slab)`.  If
    /// `target` exceeds the total prefix, returns one-past-the-end.
    ///
    /// O(log n) — descends by comparing `target` against running prefix
    /// sums of sibling subtrees.
    #[inline(never)]
    pub(crate) fn find_slab_at_prefix(&self, target: A::Prefix) -> (usize, A::Prefix, usize) {
        let mut acc_prefix = A::Prefix::default();
        let mut acc_items = 0usize;
        let mut slab_base = 0usize;
        let mut node = self.root;
        loop {
            match self.node(node) {
                Node::Leaf(l) => {
                    for (i, a) in l.aggs.iter().enumerate() {
                        let after = acc_prefix + a.prefix();
                        if target <= after {
                            return (slab_base + i, acc_prefix, acc_items);
                        }
                        acc_prefix = after;
                        acc_items += a.len();
                    }
                    return (slab_base + l.aggs.len(), acc_prefix, acc_items);
                }
                Node::Internal(n) => {
                    let mut descended = false;
                    for c in &n.children {
                        let after = acc_prefix + c.agg.prefix();
                        if target <= after {
                            node = c.id;
                            descended = true;
                            break;
                        }
                        acc_prefix = after;
                        acc_items += c.agg.len();
                        slab_base += c.slab_count;
                    }
                    if !descended {
                        return (slab_base, acc_prefix, acc_items);
                    }
                }
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Self(seed.max(1))
        }
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn reference_find_by_value(aggs: &[SlabAgg], target: i64) -> Vec<(usize, usize, i64)> {
        let mut out = Vec::new();
        let mut prefix = 0i64;
        let mut items_before = 0usize;
        for (i, a) in aggs.iter().enumerate() {
            if a.contains(target, prefix) {
                out.push((i, items_before, prefix));
            }
            prefix += a.total;
            items_before += a.len;
        }
        out
    }

    fn reference_find_by_range(aggs: &[SlabAgg], lo: i64, hi: i64) -> Vec<(usize, usize, i64)> {
        let mut out = Vec::new();
        let mut prefix = 0i64;
        let mut items_before = 0usize;
        for (i, a) in aggs.iter().enumerate() {
            if a.overlaps(lo, hi, prefix) {
                out.push((i, items_before, prefix));
            }
            prefix += a.total;
            items_before += a.len;
        }
        out
    }

    fn simple_agg(total: i64, len: usize) -> SlabAgg {
        SlabAgg {
            len,
            total,
            min_offset: total.min(0),
            max_offset: total.max(0),
        }
    }

    #[test]
    fn empty_tree() {
        let t: SlabBTree<SlabAgg> = SlabBTree::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.root_agg(), SlabAgg::default());
        assert_eq!(t.find_by_value(0).collect::<Vec<_>>(), vec![]);
    }

    #[test]
    fn from_iter_single() {
        let aggs = vec![simple_agg(10, 5)];
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        assert_eq!(t.len(), 1);
        assert_eq!(t.root_agg().len, 5);
        assert_eq!(t.root_agg().total, 10);
    }

    #[test]
    fn from_iter_many_levels() {
        let aggs: Vec<_> = (0..100).map(|i| simple_agg((i + 1) as i64, 3)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        assert_eq!(t.len(), 100);
        assert_eq!(t.root_agg().len, 300);
        for target in [1i64, 100, 5050, 9999] {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want, "target={target}");
        }
    }

    #[test]
    fn find_by_prefix_basic() {
        let aggs: Vec<_> = (0..10).map(|_| simple_agg(10, 5)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(t.find_by_prefix(0), Some((0, 0)));
        assert_eq!(t.find_by_prefix(4), Some((0, 0)));
        assert_eq!(t.find_by_prefix(5), Some((1, 5)));
        assert_eq!(t.find_by_prefix(49), Some((9, 45)));
        assert_eq!(t.find_by_prefix(50), None);
    }

    #[test]
    fn splice_no_op() {
        let aggs: Vec<_> = (0..20).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        t.splice(5..5, std::iter::empty());
        assert_eq!(t.len(), 20);
        for target in [3i64, 10, 100] {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want);
        }
    }

    #[test]
    fn splice_replace_single() {
        let mut aggs: Vec<_> = (0..20).map(|i| simple_agg(i as i64, 2)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        let new_agg = simple_agg(999, 3);
        t.splice(10..11, std::iter::once(new_agg));
        aggs[10] = new_agg;
        let prefix_before_10: i64 = aggs[..10].iter().map(|a| a.total).sum();
        let target = prefix_before_10 + 999;
        let got: Vec<_> = t.find_by_value(target).collect();
        let want = reference_find_by_value(&aggs, target);
        assert_eq!(got, want);
    }

    #[test]
    fn splice_grow_and_shrink() {
        let mut aggs: Vec<_> = (0..10).map(|i| simple_agg(i as i64, 2)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());

        let new: Vec<_> = (100..105).map(|v| simple_agg(v, 1)).collect();
        t.splice(3..3, new.clone());
        aggs.splice(3..3, new);
        assert_eq!(t.len(), 15);

        t.splice(7..11, std::iter::empty());
        aggs.drain(7..11);
        assert_eq!(t.len(), 11);

        for target in -10i64..200 {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want, "target={target}");
        }
    }

    #[test]
    fn find_by_value_unique_hit() {
        let mut aggs: Vec<_> = (0..1000).map(|_| simple_agg(1, 1)).collect();
        aggs[500] = simple_agg(10_000, 1);
        let t = SlabBTree::<SlabAgg>::from_iter(aggs);
        let target = 500 + 10_000;
        let mut iter = t.find_by_value(target);
        let next = iter.next();
        assert_eq!(next.map(|(si, ib, _)| (si, ib)), Some((500, 500)));
        drop(iter);
    }

    #[test]
    fn find_by_range_returns_iterator() {
        let aggs: Vec<_> = (0..50).map(|i| simple_agg((i + 1) * 10, 1)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        let got: Vec<_> = t.find_by_value_range(100, 500).collect();
        let want = reference_find_by_range(&aggs, 100, 500);
        assert_eq!(got, want);
    }

    #[test]
    fn fuzz_splice_vs_reference() {
        let mut rng = Rng::new(0xC0FFEE);
        let mut reference: Vec<SlabAgg> = (0..30)
            .map(|_| simple_agg(rand_total(&mut rng), 1))
            .collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(reference.clone());

        for _ in 0..2_000 {
            let op = rng.next() % 3;
            let len = reference.len();
            match op {
                0 => {
                    let at = (rng.next() as usize) % (len + 1);
                    let count = 1 + (rng.next() as usize) % 5;
                    let new: Vec<_> = (0..count)
                        .map(|_| simple_agg(rand_total(&mut rng), 1))
                        .collect();
                    tree.splice(at..at, new.clone());
                    reference.splice(at..at, new);
                }
                1 if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    let count = 1 + (rng.next() as usize) % (len - at).min(5);
                    tree.splice(at..at + count, std::iter::empty());
                    reference.drain(at..at + count);
                }
                _ if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    let del = 1 + (rng.next() as usize) % (len - at).min(3);
                    let count = 1 + (rng.next() as usize) % 4;
                    let new: Vec<_> = (0..count)
                        .map(|_| simple_agg(rand_total(&mut rng), 1))
                        .collect();
                    tree.splice(at..at + del, new.clone());
                    reference.splice(at..at + del, new);
                }
                _ => {}
            }
            assert_eq!(tree.len(), reference.len());
            for _ in 0..4 {
                let t: i64 = (rng.next() as i64) % 10_000 - 5_000;
                let got: Vec<_> = tree.find_by_value(t).collect();
                let want = reference_find_by_value(&reference, t);
                assert_eq!(got, want, "target={t}");
            }
        }
    }

    fn rand_total(rng: &mut Rng) -> i64 {
        (rng.next() as i64 % 200) - 100
    }

    // ── Prefix-sum B-tree smoke test ────────────────────────────────────

    use super::super::prefix::PrefixSlabWeight;

    fn psw(len: usize, prefix: i64) -> PrefixSlabWeight<i64> {
        PrefixSlabWeight { len, prefix }
    }

    #[test]
    fn update_slab_single_slab_no_descend() {
        let mut tree = SlabBTree::<SlabAgg>::from_iter(vec![simple_agg(10, 3)]);
        tree.update_slab(0, simple_agg(20, 5));
        let got: Vec<_> = tree.find_by_value(20).collect();
        assert_eq!(got, vec![(0, 0, 0)]);
    }

    #[test]
    fn update_slab_deep_tree() {
        // 200 slabs forces multi-level tree at B=16.
        let aggs: Vec<_> = (0..200).map(|i| simple_agg((i + 1) as i64, 1)).collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        // Update slab 100 to carry a unique large value.
        let prefix_before_100: i64 = aggs[..100].iter().map(|a| a.total).sum();
        tree.update_slab(100, simple_agg(999_999, 1));
        let target = prefix_before_100 + 999_999;
        let mut iter = tree.find_by_value(target);
        let first = iter.next().unwrap();
        assert_eq!(first.0, 100);
    }

    #[test]
    fn update_slab_root_agg_refreshed() {
        let aggs: Vec<_> = (0..50).map(|_| simple_agg(1, 1)).collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(tree.root_agg().total, 50);
        tree.update_slab(25, simple_agg(100, 1));
        // Total went from 50 to 50 - 1 + 100 = 149.
        assert_eq!(tree.root_agg().total, 149);
    }

    #[test]
    fn prefix_btree_find_slab_at_item() {
        // 10 slabs, each len=5, prefix values chosen to be distinct.
        let weights: Vec<_> = (0..10).map(|i| psw(5, (i + 1) as i64 * 10)).collect();
        let tree = SlabBTree::from_iter(weights.clone());
        assert_eq!(tree.len(), 10);
        // Item 0 is in slab 0; nothing before → (0, 0, 0).
        assert_eq!(tree.find_slab_at_item(0).decompose(), (0, 0, 0));
        // Item 5 is the start of slab 1 → prefix = weights[0].prefix = 10,
        // items_before = 5.
        assert_eq!(tree.find_slab_at_item(5).decompose(), (1, 10, 5));
        // Item 24 is inside slab 4 → prefix = sum of slabs 0..=3 = 100,
        // items_before = 20.
        assert_eq!(tree.find_slab_at_item(24).decompose(), (4, 100, 20));
    }

    #[test]
    fn prefix_btree_find_slab_at_prefix() {
        let weights: Vec<_> = (0..10).map(|i| psw(5, (i + 1) as i64 * 10)).collect();
        let tree = SlabBTree::from_iter(weights);
        // Prefix sum of slab 0 is 10; target=10 lands in slab 0 (≤ 10).
        assert_eq!(tree.find_slab_at_prefix(10), (0, 0, 0));
        // Target 15: crosses into slab 1 (0+10 < 15 <= 0+10+20=30).
        assert_eq!(tree.find_slab_at_prefix(15), (1, 10, 5));
        // Target 100: prefix_before slab 3 = 60; slab 3 ends at 100; target=100 ≤ 100 → slab 3.
        assert_eq!(tree.find_slab_at_prefix(100), (3, 60, 15));
        // Target past total (sum = 550): one-past-end.
        assert_eq!(tree.find_slab_at_prefix(9999), (10, 550, 50));
    }

    // ── Fuzz: find_slab, find_slab_at_item, find_slab_at_prefix, root_agg ──

    fn reference_find_by_prefix(aggs: &[SlabAgg], pos: usize) -> Option<(usize, usize)> {
        let mut items_before = 0;
        for (i, a) in aggs.iter().enumerate() {
            if pos < items_before + a.len {
                return Some((i, items_before));
            }
            items_before += a.len;
        }
        None
    }

    fn reference_find_slab_at_item(aggs: &[SlabAgg], item_idx: usize) -> (usize, i64, usize) {
        let mut prefix: i64 = 0;
        let mut items_before = 0;
        for (i, a) in aggs.iter().enumerate() {
            if item_idx < items_before + a.len {
                return (i, prefix, items_before);
            }
            prefix += a.total;
            items_before += a.len;
        }
        (aggs.len(), prefix, items_before)
    }

    fn reference_find_slab_at_prefix(aggs: &[SlabAgg], target: i64) -> (usize, i64, usize) {
        let mut prefix: i64 = 0;
        let mut items_before = 0;
        for (i, a) in aggs.iter().enumerate() {
            if prefix + a.total >= target {
                return (i, prefix, items_before);
            }
            prefix += a.total;
            items_before += a.len;
        }
        (aggs.len(), prefix, items_before)
    }

    fn reference_root_agg(aggs: &[SlabAgg]) -> SlabAgg {
        aggs.iter()
            .fold(SlabAgg::default(), |acc, a| SlabAgg::merge(&acc, a))
    }

    #[test]
    fn fuzz_find_slab_and_prefix() {
        let mut rng = Rng::new(0xDEADBEEF);

        for trial in 0..500 {
            let mut reference: Vec<SlabAgg> = (0..10)
                .map(|_| simple_agg(rand_total(&mut rng), 1 + (rng.next() as usize) % 10))
                .collect();
            let mut tree = SlabBTree::<SlabAgg>::from_iter(reference.clone());

            for step in 0..200 {
                let len = reference.len();
                let op = rng.next() % 3;
                match op {
                    0 => {
                        let at = (rng.next() as usize) % (len + 1);
                        let count = 1 + (rng.next() as usize) % 3;
                        let new: Vec<_> = (0..count)
                            .map(|_| {
                                simple_agg(rand_total(&mut rng), 1 + (rng.next() as usize) % 10)
                            })
                            .collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    1 if len > 1 => {
                        let at = (rng.next() as usize) % len;
                        let count = 1 + (rng.next() as usize) % (len - at).min(3);
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    _ if len > 0 => {
                        let at = (rng.next() as usize) % len;
                        let new_agg =
                            simple_agg(rand_total(&mut rng), 1 + (rng.next() as usize) % 10);
                        tree.update_slab(at, new_agg);
                        reference[at] = new_agg;
                    }
                    _ => {}
                }

                assert_eq!(
                    tree.len(),
                    reference.len(),
                    "trial={trial} step={step}: slab count"
                );

                // Verify root_agg
                let expected_root = reference_root_agg(&reference);
                assert_eq!(
                    tree.root_agg(),
                    expected_root,
                    "trial={trial} step={step}: root_agg"
                );

                let total_items: usize = reference.iter().map(|a| a.len).sum();

                // find_by_prefix (positional slab lookup)
                if total_items > 0 {
                    for _ in 0..3 {
                        let pos = (rng.next() as usize) % total_items;
                        let got = tree.find_by_prefix(pos);
                        let want = reference_find_by_prefix(&reference, pos);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_by_prefix({pos})"
                        );
                    }
                    // Edge: last item
                    let got = tree.find_by_prefix(total_items - 1);
                    let want = reference_find_by_prefix(&reference, total_items - 1);
                    assert_eq!(got, want, "trial={trial} step={step}: find_by_prefix(last)");
                    // Edge: past end
                    assert_eq!(tree.find_by_prefix(total_items), None);
                }

                // find_slab_at_item (prefix-aware positional lookup)
                if total_items > 0 {
                    for _ in 0..3 {
                        let idx = (rng.next() as usize) % total_items;
                        let got = tree.find_slab_at_item(idx).decompose();
                        let want = reference_find_slab_at_item(&reference, idx);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_slab_at_item({idx})"
                        );
                    }
                }

                // find_slab_at_prefix (prefix-sum based lookup)
                let total_prefix: i64 = reference.iter().map(|a| a.total).sum();
                if total_prefix > 0 {
                    for _ in 0..3 {
                        let target = 1 + (rng.next() as i64).abs() % total_prefix;
                        let got = tree.find_slab_at_prefix(target);
                        let want = reference_find_slab_at_prefix(&reference, target);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_slab_at_prefix({target})"
                        );
                    }
                }

                // find_by_value (existing fuzz coverage, but now also after
                // update_slab which wasn't fuzzed before)
                for _ in 0..2 {
                    let t: i64 = (rng.next() as i64) % 500 - 250;
                    let got: Vec<_> = tree.find_by_value(t).collect();
                    let want = reference_find_by_value(&reference, t);
                    assert_eq!(got, want, "trial={trial} step={step}: find_by_value({t})");
                }

                // find_by_value_range
                {
                    let lo: i64 = (rng.next() as i64) % 400 - 200;
                    let hi = lo + (rng.next() as i64).abs() % 200;
                    let got: Vec<_> = tree.find_by_value_range(lo, hi).collect();
                    let want = reference_find_by_range(&reference, lo, hi);
                    assert_eq!(
                        got, want,
                        "trial={trial} step={step}: find_by_value_range({lo}..{hi})"
                    );
                }
            }
        }
    }

    #[test]
    fn fuzz_btree_with_prefix_weights() {
        use super::super::prefix::PrefixSlabWeight;
        type W = PrefixSlabWeight<u64>;
        type Tree = SlabBTree<W>;

        let mut rng = Rng::new(0xCAFEBABE);

        fn rand_weight(rng: &mut Rng) -> W {
            let len = 1 + (rng.next() as usize) % 8;
            let prefix = rng.next() % 50;
            W { len, prefix }
        }

        for trial in 0..500 {
            let mut reference: Vec<W> = (0..10).map(|_| rand_weight(&mut rng)).collect();
            let mut tree = Tree::from_iter(reference.clone());

            for step in 0..200 {
                let len = reference.len();
                let op = rng.next() % 4;
                match op {
                    0 => {
                        let at = (rng.next() as usize) % (len + 1);
                        let count = 1 + (rng.next() as usize) % 3;
                        let new: Vec<_> = (0..count).map(|_| rand_weight(&mut rng)).collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    1 if len > 1 => {
                        let at = (rng.next() as usize) % len;
                        let count = 1 + (rng.next() as usize) % (len - at).min(3);
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    2 if len > 0 => {
                        let at = (rng.next() as usize) % len;
                        let new = rand_weight(&mut rng);
                        tree.update_slab(at, new);
                        reference[at] = new;
                    }
                    _ if len > 0 => {
                        let at = (rng.next() as usize) % len;
                        let del = 1 + (rng.next() as usize) % (len - at).min(2);
                        let ins = 1 + (rng.next() as usize) % 3;
                        let new: Vec<_> = (0..ins).map(|_| rand_weight(&mut rng)).collect();
                        tree.splice(at..at + del, new.clone());
                        reference.splice(at..at + del, new);
                    }
                    _ => {}
                }

                assert_eq!(
                    tree.len(),
                    reference.len(),
                    "trial={trial} step={step}: len"
                );

                // Verify root aggregate matches
                let expected_root = reference.iter().copied().fold(W::default(), |mut acc, w| {
                    acc += w;
                    acc
                });
                let root = tree.root_agg();
                assert_eq!(
                    root.len, expected_root.len,
                    "trial={trial} step={step}: root_agg.len"
                );
                assert_eq!(
                    root.prefix, expected_root.prefix,
                    "trial={trial} step={step}: root_agg.prefix"
                );

                let total_items: usize = reference.iter().map(|w| w.len).sum();
                let total_prefix: u64 = reference.iter().map(|w| w.prefix).sum();

                // find_by_prefix (positional)
                if total_items > 0 {
                    let pos = (rng.next() as usize) % total_items;
                    let got = tree.find_by_prefix(pos);
                    let mut items = 0;
                    let mut expected = None;
                    for (i, w) in reference.iter().enumerate() {
                        if pos < items + w.len {
                            expected = Some((i, items));
                            break;
                        }
                        items += w.len;
                    }
                    assert_eq!(
                        got, expected,
                        "trial={trial} step={step}: find_by_prefix({pos})"
                    );
                }

                // find_slab_at_item
                if total_items > 0 {
                    let idx = (rng.next() as usize) % total_items;
                    let found = tree.find_slab_at_item(idx);
                    let mut exp_prefix = 0u64;
                    let mut exp_items = 0;
                    let mut exp_si = reference.len();
                    for (i, w) in reference.iter().enumerate() {
                        if idx < exp_items + w.len {
                            exp_si = i;
                            break;
                        }
                        exp_prefix += w.prefix;
                        exp_items += w.len;
                    }
                    assert_eq!(
                        (found.index, found.prefix, found.pos),
                        (exp_si, exp_prefix, exp_items),
                        "trial={trial} step={step}: find_slab_at_item({idx})"
                    );
                }

                // find_slab_at_prefix
                if total_prefix > 0 {
                    let target = 1 + (rng.next() % total_prefix);
                    let (si, prefix_before, items_before) = tree.find_slab_at_prefix(target);
                    let mut exp_prefix = 0u64;
                    let mut exp_items = 0;
                    let mut exp_si = reference.len();
                    for (i, w) in reference.iter().enumerate() {
                        if exp_prefix + w.prefix >= target {
                            exp_si = i;
                            break;
                        }
                        exp_prefix += w.prefix;
                        exp_items += w.len;
                    }
                    assert_eq!(
                        (si, prefix_before, items_before),
                        (exp_si, exp_prefix, exp_items),
                        "trial={trial} step={step}: find_slab_at_prefix({target})"
                    );
                }
            }
        }
    }
}
