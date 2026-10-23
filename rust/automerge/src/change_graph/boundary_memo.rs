//! Memoised fragment boundaries.
//!
//! The boundary of a fragment head `h` at level `L` is the set of ancestors
//! with level `>= L` that are reachable from `h` along paths whose interior
//! nodes all have level `< L`. Defining, for any node `n`,
//!
//! ```text
//! summary_L(n) = ∪ { {p}          if level(p) >= L
//!                  { summary_L(p) otherwise        for each parent p of n }
//! ```
//!
//! the boundary is just `summary_L(h)`. Summaries depend only on the immutable
//! ancestry of a node, so they can be memoised for ever: nodes are append-only
//! and never renumbered. We keep one table per level, mapping each node to a
//! shared, sorted summary. Most nodes have a single below-level parent and
//! simply share that parent's summary, so the number of distinct summaries is
//! far smaller than the number of nodes.

use std::sync::Arc;

use super::{ChangeHash, Edge, EdgeIdx, NodeIdx};

const UNKNOWN: u32 = u32::MAX;

#[derive(Clone, Debug, Default)]
pub(super) struct BoundaryMemo {
    /// Indexed by level. Level 0 is never queried and stays empty.
    levels: Vec<LevelMemo>,
}

#[derive(Clone, Debug, Default)]
struct LevelMemo {
    /// For each node, the index into `sets` of its summary, or `UNKNOWN`.
    of_node: Vec<u32>,
    /// Distinct summaries, each sorted by `NodeIdx`.
    sets: Vec<Arc<[NodeIdx]>>,
}

/// Borrowed view of the graph structure the memo needs.
pub(super) struct Graph<'a> {
    pub(super) hashes: &'a [ChangeHash],
    pub(super) parents: &'a [Option<EdgeIdx>],
    pub(super) edges: &'a [Edge],
}

impl Graph<'_> {
    fn parents(&self, node: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge = self.parents[node.0 as usize];
        std::iter::from_fn(move || {
            let e = &self.edges[edge?.get()];
            edge = e.next;
            Some(e.target)
        })
    }

    fn level(&self, node: NodeIdx) -> usize {
        self.hashes[node.0 as usize].fragment_level()
    }
}

impl BoundaryMemo {
    /// The boundary of `head` at `level`, sorted by change hash.
    pub(super) fn boundary(
        &mut self,
        graph: &Graph<'_>,
        head: NodeIdx,
        level: usize,
    ) -> Vec<NodeIdx> {
        assert!(level > 0);
        if self.levels.len() <= level {
            self.levels.resize_with(level + 1, LevelMemo::default);
        }
        let memo = &mut self.levels[level];
        let set = memo.summary(graph, head, level);
        let mut boundary = memo.sets[set as usize].to_vec();
        boundary.sort_unstable_by_key(|n| graph.hashes[n.0 as usize]);
        boundary
    }

    #[cfg(test)]
    pub(super) fn footprint(&self) -> (usize, usize) {
        let sets = self.levels.iter().map(|l| l.sets.len()).sum();
        let bytes = self
            .levels
            .iter()
            .map(|l| {
                l.of_node.capacity() * 4
                    + l.sets.capacity() * std::mem::size_of::<Arc<[NodeIdx]>>()
                    + l.sets.iter().map(|s| s.len() * 4).sum::<usize>()
            })
            .sum();
        (sets, bytes)
    }
}

impl LevelMemo {
    fn get(&self, node: NodeIdx) -> Option<u32> {
        self.of_node
            .get(node.0 as usize)
            .copied()
            .filter(|&s| s != UNKNOWN)
    }

    /// Index into `self.sets` of `summary_level(root)`, computing and
    /// memoising it (and every unknown summary it depends on) if needed.
    ///
    /// Iterative post-order: a node is pushed, then its unknown below-level
    /// parents are pushed on top of it; once all of them are known the node
    /// itself is computed.
    fn summary(&mut self, graph: &Graph<'_>, root: NodeIdx, level: usize) -> u32 {
        if self.of_node.len() < graph.hashes.len() {
            self.of_node.resize(graph.hashes.len(), UNKNOWN);
        }
        let mut stack = vec![root];
        let mut union = Vec::new();
        while let Some(&node) = stack.last() {
            if self.get(node).is_some() {
                stack.pop();
                continue;
            }
            let before = stack.len();
            stack.extend(
                graph
                    .parents(node)
                    .filter(|&p| graph.level(p) < level && self.get(p).is_none()),
            );
            if stack.len() != before {
                continue;
            }
            let set = self.compute(graph, node, level, &mut union);
            self.of_node[node.0 as usize] = set;
            stack.pop();
        }
        self.get(root).unwrap()
    }

    /// Compute a node's summary from its parents, all of whose summaries are
    /// known. Reuses an existing set wherever the union adds nothing to it.
    fn compute(
        &mut self,
        graph: &Graph<'_>,
        node: NodeIdx,
        level: usize,
        union: &mut Vec<NodeIdx>,
    ) -> u32 {
        union.clear();
        // The largest input set, if the union turns out to equal it.
        let mut largest: Option<u32> = None;
        for p in graph.parents(node) {
            if graph.level(p) >= level {
                merge_into(union, &[p]);
            } else {
                let s = self.get(p).unwrap();
                if largest == Some(s) {
                    // Both parents share a summary; merging again is a no-op.
                    continue;
                }
                let set = &self.sets[s as usize];
                if largest.is_none_or(|l| self.sets[l as usize].len() < set.len()) {
                    largest = Some(s);
                }
                merge_into(union, set);
            }
        }
        if let Some(l) = largest {
            if self.sets[l as usize].len() == union.len() {
                return l;
            }
        }
        self.sets.push(Arc::from(union.as_slice()));
        (self.sets.len() - 1) as u32
    }
}

/// `dst = dst ∪ src`, both sorted and deduplicated.
fn merge_into(dst: &mut Vec<NodeIdx>, src: &[NodeIdx]) {
    if dst.is_empty() {
        dst.extend_from_slice(src);
        return;
    }
    let mut merged = Vec::with_capacity(dst.len() + src.len());
    let (mut i, mut j) = (0, 0);
    while i < dst.len() && j < src.len() {
        match dst[i].cmp(&src[j]) {
            std::cmp::Ordering::Less => {
                merged.push(dst[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(src[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(dst[i]);
                i += 1;
                j += 1;
            }
        }
    }
    merged.extend_from_slice(&dst[i..]);
    merged.extend_from_slice(&src[j..]);
    *dst = merged;
}
