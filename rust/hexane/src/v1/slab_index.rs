//! Slab index data structures for O(log S) positional access.
//!
//! All implementations support:
//! - `find(index)` — locate which slab contains a logical item index
//! - `splice(range, slabs)` — update after structural changes to slabs
//!
//! Three implementations:
//! - [`FenwickIndex`] — Fenwick tree (BIT). Fast point updates, O(S) structural changes.
//! - [`BTreeIndex`] — Lightweight B-tree with cumulative counts at each node.
//! - [`OSTIndex`] — Order-statistic tree (augmented BST).

use super::column::{Slab, SlabWeight, WeightFn};
use super::ColumnValueRef;
use std::ops::Range;

/// Trait for slab index structures that support positional access.
pub(crate) trait SlabIndex<T: ColumnValueRef, WF: WeightFn<T>>:
    SlabIndexCore + Clone
{
    /// Build from a slice of slabs.
    fn build(slabs: &[Slab]) -> Self;

    /// Notify that slabs in `range` have changed (inserted, removed, or modified).
    /// `slabs` is the full slab list after the change.
    fn splice(&mut self, range: Range<usize>, slabs: &[Slab]);
}

/// Core operations that don't need `T` or `WF` type params.
pub(crate) trait SlabIndexCore {
    /// Find the slab containing logical `index`. Returns (slab_idx, offset_within_slab).
    fn find(&self, index: usize, n: usize) -> (usize, usize);
}

// ── Fenwick tree ────────────────────────────────────────────────────────────

/// Fenwick tree (Binary Indexed Tree) index.
/// O(log S) find and point update. O(S) structural changes (rebuild).
#[derive(Clone, Debug)]
pub(crate) struct FenwickIndex<W: SlabWeight> {
    bit: Vec<W>,
}

impl<W: SlabWeight> SlabIndexCore for FenwickIndex<W> {
    #[inline]
    fn find(&self, index: usize, n: usize) -> (usize, usize) {
        if n == 0 {
            return (0, 0);
        }
        let mut pos = 0usize;
        let mut idx = 0usize;
        let mut k = 1;
        while k <= n {
            k <<= 1;
        }
        k >>= 1;
        while k > 0 {
            let next = idx + k;
            if next <= n && pos + self.bit[next].len() <= index {
                pos += self.bit[next].len();
                idx = next;
            }
            k >>= 1;
        }
        (idx, index - pos)
    }
}

impl<T: ColumnValueRef, WF: WeightFn<T>> SlabIndex<T, WF> for FenwickIndex<WF::Weight> {
    fn build(slabs: &[Slab]) -> Self {
        let n = slabs.len();
        let mut bit = vec![WF::Weight::default(); n + 1];
        for i in 0..n {
            bit[i + 1] = WF::compute(&slabs[i]);
        }
        for i in 1..=n {
            let parent = i + (i & i.wrapping_neg());
            if parent <= n {
                let child = bit[i];
                bit[parent] += child;
            }
        }
        FenwickIndex { bit }
    }

    fn splice(&mut self, range: Range<usize>, slabs: &[Slab]) {
        let n = slabs.len();
        let old_n = self.bit.len() - 1;

        if n == old_n && range.len() == 1 {
            // Single slab changed — point update.
            let si = range.start;
            let new_w = WF::compute(&slabs[si]);
            // Compute old weight by querying prefix(si+1) - prefix(si).
            let old_w = {
                let after = self.prefix_sum(si + 1);
                let before = self.prefix_sum(si);
                let mut w = after;
                w -= before;
                w
            };
            let mut i = si + 1;
            while i <= n {
                self.bit[i] -= old_w;
                self.bit[i] += new_w;
                i += i & i.wrapping_neg();
            }
        } else {
            // Structural change — rebuild in place.
            self.bit.clear();
            self.bit.resize(n + 1, WF::Weight::default());
            for i in 0..n {
                self.bit[i + 1] = WF::compute(&slabs[i]);
            }
            for i in 1..=n {
                let parent = i + (i & i.wrapping_neg());
                if parent <= n {
                    let child = self.bit[i];
                    self.bit[parent] += child;
                }
            }
        }
    }
}

impl<W: SlabWeight> FenwickIndex<W> {
    fn prefix_sum(&self, idx: usize) -> W {
        let mut sum = W::default();
        let mut i = idx;
        while i > 0 {
            sum += self.bit[i];
            i -= i & i.wrapping_neg();
        }
        sum
    }
}

// ── B-tree index ────────────────────────────────────────────────────────────

/// Lightweight B-tree with cumulative lengths at internal nodes.
/// O(log S) for all operations including structural changes.
///
/// Each leaf holds one slab weight. Internal nodes hold the sum of their
/// children's weights. The tree is stored as a flat array in BFS order.
/// B=16 means each internal node has up to 16 children.
#[derive(Clone, Debug)]
pub(crate) struct BTreeIndex<W: SlabWeight> {
    /// Leaf weights in slab order.
    weights: Vec<W>,
    /// Cumulative sums at each level, bottom-up. Level 0 = groups of B leaves.
    levels: Vec<Vec<W>>,
}

const B: usize = 16;

impl<W: SlabWeight> SlabIndexCore for BTreeIndex<W> {
    fn find(&self, index: usize, n: usize) -> (usize, usize) {
        if n == 0 {
            return (0, 0);
        }
        let mut pos = 0usize;
        // child_start tracks which entry in the *next level down* (or leaves)
        // the selected group expands to.
        let mut child_start = 0usize;

        // Walk top-down. levels[L-1] is the coarsest, levels[0] groups leaves by B.
        for level in self.levels.iter().rev() {
            // The selected group at the previous level narrowed us to
            // child_start..child_end in *this* level.
            let g_start = child_start / B;
            let g_end = level.len(); // scan to the end (B entries max from parent narrowing)

            let mut found = false;
            for g in g_start..g_end {
                let w = level[g].len();
                if pos + w > index {
                    // This group contains our target.
                    // Its children in the next level down start at g * B.
                    child_start = g * B;
                    found = true;
                    break;
                }
                pos += w;
            }
            if !found {
                return (n, index - pos);
            }
        }

        // Walk leaves within the narrowed range.
        let leaf_end = (child_start + B).min(n);
        for i in child_start..leaf_end {
            let w = self.weights[i].len();
            if pos + w > index {
                return (i, index - pos);
            }
            pos += w;
        }
        (n, index - pos)
    }
}

impl<T: ColumnValueRef, WF: WeightFn<T>> SlabIndex<T, WF> for BTreeIndex<WF::Weight> {
    fn build(slabs: &[Slab]) -> Self {
        let weights: Vec<WF::Weight> = slabs.iter().map(WF::compute).collect();
        let levels = build_levels::<WF::Weight>(&weights);
        BTreeIndex { weights, levels }
    }

    fn splice(&mut self, range: Range<usize>, slabs: &[Slab]) {
        let n = slabs.len();
        let old_n = self.weights.len();

        if n == old_n && range.len() == 1 {
            // Point update — update leaf weight and propagate through levels.
            let si = range.start;
            self.weights[si] = WF::compute(&slabs[si]);

            // Level 0 groups weights by B. Level k groups level[k-1] by B.
            let mut idx = si;
            for li in 0..self.levels.len() {
                let g = idx / B;
                if g >= self.levels[li].len() {
                    break;
                }
                // Recompute group sum from its children.
                let child_start = g * B;
                let mut sum = WF::Weight::default();
                if li == 0 {
                    let child_end = (child_start + B).min(self.weights.len());
                    for i in child_start..child_end {
                        sum += self.weights[i];
                    }
                } else {
                    let child_end = (child_start + B).min(self.levels[li - 1].len());
                    for i in child_start..child_end {
                        sum += self.levels[li - 1][i];
                    }
                }
                self.levels[li][g] = sum;
                idx = g;
            }
        } else {
            // Structural change — rebuild.
            self.weights.clear();
            self.weights.extend(slabs.iter().map(WF::compute));
            self.levels = build_levels::<WF::Weight>(&self.weights);
        }
    }
}

fn build_levels<W: SlabWeight>(weights: &[W]) -> Vec<Vec<W>> {
    let mut levels = Vec::new();
    let mut current = weights;
    let mut buf;
    loop {
        let n = current.len();
        if n <= 1 {
            break;
        }
        let groups = n.div_ceil(B);
        buf = Vec::with_capacity(groups);
        for g in 0..groups {
            let start = g * B;
            let end = (start + B).min(n);
            let mut sum = W::default();
            for i in start..end {
                sum += current[i];
            }
            buf.push(sum);
        }
        levels.push(buf.clone());
        if groups <= 1 {
            break;
        }
        // next level
        let prev = levels.last().unwrap();
        current = prev;
    }
    levels
}

// ── Order-Statistic Tree ────────────────────────────────────────────────────

/// Order-statistic tree using an implicit balanced binary tree (like a segment tree).
/// O(log S) for all operations.
///
/// Stored as a flat array where node `i` has children `2*i+1` and `2*i+2`.
/// Each node stores the sum of weights in its subtree.
#[derive(Clone, Debug)]
pub(crate) struct OSTIndex<W: SlabWeight> {
    /// Segment tree array. Leaves at positions [offset..offset+n).
    tree: Vec<W>,
    /// Number of leaf slots (rounded up to next power of 2).
    capacity: usize,
    /// Actual number of slabs.
    n: usize,
}

impl<W: SlabWeight> SlabIndexCore for OSTIndex<W> {
    fn find(&self, index: usize, n: usize) -> (usize, usize) {
        if n == 0 {
            return (0, 0);
        }
        let mut pos = 0usize;
        let mut node = 1; // root

        // Walk down the tree.
        while node < self.capacity {
            let left = 2 * node;
            let left_len = self.tree[left].len();
            if pos + left_len <= index {
                pos += left_len;
                node = left + 1; // go right
            } else {
                node = left; // go left
            }
        }

        let slab_idx = node - self.capacity;
        if slab_idx >= n {
            (n, index - pos)
        } else {
            (slab_idx, index - pos)
        }
    }
}

impl<T: ColumnValueRef, WF: WeightFn<T>> SlabIndex<T, WF> for OSTIndex<WF::Weight> {
    fn build(slabs: &[Slab]) -> Self {
        let n = slabs.len();
        let capacity = n.next_power_of_two().max(1);
        let mut tree = vec![WF::Weight::default(); 2 * capacity];
        // Fill leaves.
        for i in 0..n {
            tree[capacity + i] = WF::compute(&slabs[i]);
        }
        // Build internal nodes bottom-up.
        for i in (1..capacity).rev() {
            let left = tree[2 * i];
            let right = tree[2 * i + 1];
            tree[i] = left;
            tree[i] += right;
        }
        OSTIndex { tree, capacity, n }
    }

    fn splice(&mut self, range: Range<usize>, slabs: &[Slab]) {
        let new_n = slabs.len();

        if new_n != self.n {
            // Size changed — rebuild.
            self.rebuild_from_weights(slabs.iter().map(WF::compute), new_n);
            return;
        }

        // Same size — update only the changed range.
        for i in range {
            let leaf = self.capacity + i;
            self.tree[leaf] = WF::compute(&slabs[i]);
            // Propagate up.
            let mut node = leaf >> 1;
            while node >= 1 {
                let left = self.tree[2 * node];
                let right = self.tree[2 * node + 1];
                self.tree[node] = left;
                self.tree[node] += right;
                node >>= 1;
            }
        }
        self.n = new_n;
    }
}

impl<W: SlabWeight> OSTIndex<W> {
    fn rebuild_from_weights(&mut self, weights: impl Iterator<Item = W>, n: usize) {
        self.n = n;
        self.capacity = n.next_power_of_two().max(1);
        self.tree.clear();
        self.tree.resize(2 * self.capacity, W::default());
        for (i, w) in weights.enumerate() {
            self.tree[self.capacity + i] = w;
        }
        for i in (1..self.capacity).rev() {
            let left = self.tree[2 * i];
            let right = self.tree[2 * i + 1];
            self.tree[i] = left;
            self.tree[i] += right;
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::column::LenWeight;
    use crate::v1::ValidBuf;

    fn make_slabs(lens: &[usize]) -> Vec<Slab> {
        lens.iter()
            .map(|&len| Slab {
                data: ValidBuf::new(vec![]),
                len,
                segments: 0,
            })
            .collect()
    }

    fn test_index<I: SlabIndex<u64, LenWeight>>(name: &str) {
        // Basic find.
        let slabs = make_slabs(&[10, 20, 30, 40]);
        let idx = I::build(&slabs);

        assert_eq!(idx.find(0, 4), (0, 0), "{name}: find(0)");
        assert_eq!(idx.find(9, 4), (0, 9), "{name}: find(9)");
        assert_eq!(idx.find(10, 4), (1, 0), "{name}: find(10)");
        assert_eq!(idx.find(29, 4), (1, 19), "{name}: find(29)");
        assert_eq!(idx.find(30, 4), (2, 0), "{name}: find(30)");
        assert_eq!(idx.find(99, 4), (3, 39), "{name}: find(99)");

        // Splice: modify one slab.
        let mut idx = idx;
        let mut slabs = slabs;
        slabs[1].len = 5; // was 20
        idx.splice(1..2, &slabs);
        assert_eq!(idx.find(10, 4), (1, 0), "{name}: after splice find(10)");
        assert_eq!(idx.find(14, 4), (1, 4), "{name}: after splice find(14)");
        assert_eq!(idx.find(15, 4), (2, 0), "{name}: after splice find(15)");

        // Splice: insert a slab.
        slabs.insert(
            2,
            Slab {
                data: ValidBuf::new(vec![]),
                len: 7,
                segments: 0,
            },
        );
        idx.splice(2..3, &slabs);
        assert_eq!(idx.find(15, 5), (2, 0), "{name}: after insert find(15)");
        assert_eq!(idx.find(22, 5), (3, 0), "{name}: after insert find(22)");

        // Splice: remove a slab.
        slabs.remove(1);
        idx.splice(1..1, &slabs);
        assert_eq!(idx.find(10, 4), (1, 0), "{name}: after remove find(10)");

        // Empty.
        let empty: Vec<Slab> = vec![];
        let idx2 = I::build(&empty);
        assert_eq!(idx2.find(0, 0), (0, 0), "{name}: empty find(0)");
    }

    #[test]
    fn fenwick_basic() {
        test_index::<FenwickIndex<usize>>("fenwick");
    }

    #[test]
    fn btree_basic() {
        test_index::<BTreeIndex<usize>>("btree");
    }

    #[test]
    fn ost_basic() {
        test_index::<OSTIndex<usize>>("ost");
    }

    #[test]
    fn all_agree_random() {
        use rand::{rng, RngCore};
        let mut r = rng();

        for _ in 0..100 {
            let n = (r.next_u32() % 200 + 1) as usize;
            let lens: Vec<usize> = (0..n).map(|_| (r.next_u32() % 100 + 1) as usize).collect();
            let slabs = make_slabs(&lens);
            let total: usize = lens.iter().sum();

            let f = <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
            let b = <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
            let o = <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);

            for _ in 0..50 {
                let idx = r.next_u64() as usize % total;
                let rf = f.find(idx, n);
                let rb = b.find(idx, n);
                let ro = o.find(idx, n);
                assert_eq!(rf, rb, "fenwick vs btree at idx={idx} n={n}");
                assert_eq!(rf, ro, "fenwick vs ost at idx={idx} n={n}");
            }
        }
    }

    fn bench_at_size(n: usize) {
        use rand::{rng, RngCore};
        use std::time::Instant;
        let mut r = rng();

        let lens: Vec<usize> = (0..n).map(|_| (r.next_u32() % 100 + 1) as usize).collect();
        let slabs = make_slabs(&lens);
        let total: usize = lens.iter().sum();
        let queries: Vec<usize> = (0..10_000).map(|_| r.next_u64() as usize % total).collect();

        let build_iters: u128 = if n <= 10_000 { 1000 } else { 100 };
        let point_iters: u128 = 1000;
        let struct_iters: u128 = if n <= 10_000 { 100 } else { 20 };

        // Build
        let start = Instant::now();
        for _ in 0..build_iters {
            let _ = <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        }
        let f_build = start.elapsed().as_nanos() / build_iters;

        let start = Instant::now();
        for _ in 0..build_iters {
            let _ = <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        }
        let b_build = start.elapsed().as_nanos() / build_iters;

        let start = Instant::now();
        for _ in 0..build_iters {
            let _ = <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        }
        let o_build = start.elapsed().as_nanos() / build_iters;

        eprintln!("build ({n} slabs): fenwick={f_build}ns  btree={b_build}ns  ost={o_build}ns");

        // Find
        let f = <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        let b = <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        let o = <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);

        let start = Instant::now();
        for &q in &queries {
            let _ = f.find(q, n);
        }
        let f_find = start.elapsed().as_nanos() / queries.len() as u128;

        let start = Instant::now();
        for &q in &queries {
            let _ = b.find(q, n);
        }
        let b_find = start.elapsed().as_nanos() / queries.len() as u128;

        let start = Instant::now();
        for &q in &queries {
            let _ = o.find(q, n);
        }
        let o_find = start.elapsed().as_nanos() / queries.len() as u128;

        eprintln!("find ({n} slabs): fenwick={f_find}ns  btree={b_find}ns  ost={o_find}ns");

        // Splice — point update (same slab count)
        let mut f = <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        let mut b = <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        let mut o = <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
        let mut slabs_mut = slabs.clone();

        let start = Instant::now();
        for i in 0..point_iters as usize {
            let si = i % n;
            slabs_mut[si].len = (i % 100) + 1;
            <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::splice(
                &mut f,
                si..si + 1,
                &slabs_mut,
            );
        }
        let f_point = start.elapsed().as_nanos() / point_iters;

        slabs_mut = slabs.clone();
        let start = Instant::now();
        for i in 0..point_iters as usize {
            let si = i % n;
            slabs_mut[si].len = (i % 100) + 1;
            <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::splice(
                &mut b,
                si..si + 1,
                &slabs_mut,
            );
        }
        let b_point = start.elapsed().as_nanos() / point_iters;

        slabs_mut = slabs.clone();
        let start = Instant::now();
        for i in 0..point_iters as usize {
            let si = i % n;
            slabs_mut[si].len = (i % 100) + 1;
            <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::splice(&mut o, si..si + 1, &slabs_mut);
        }
        let o_point = start.elapsed().as_nanos() / point_iters;

        eprintln!(
            "splice point ({n} slabs): fenwick={f_point}ns  btree={b_point}ns  ost={o_point}ns"
        );

        // Splice — structural (slab count changes)
        let start = Instant::now();
        for _ in 0..struct_iters {
            let mut f = <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
            <FenwickIndex<usize> as SlabIndex<u64, LenWeight>>::splice(&mut f, 500..500, &slabs);
        }
        let f_struct = start.elapsed().as_nanos() / struct_iters;

        let start = Instant::now();
        for _ in 0..struct_iters {
            let mut b = <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
            <BTreeIndex<usize> as SlabIndex<u64, LenWeight>>::splice(&mut b, 500..500, &slabs);
        }
        let b_struct = start.elapsed().as_nanos() / struct_iters;

        let start = Instant::now();
        for _ in 0..struct_iters {
            let mut o = <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::build(&slabs);
            <OSTIndex<usize> as SlabIndex<u64, LenWeight>>::splice(&mut o, 500..500, &slabs);
        }
        let o_struct = start.elapsed().as_nanos() / struct_iters;

        eprintln!("splice structural ({n} slabs): fenwick={f_struct}ns  btree={b_struct}ns  ost={o_struct}ns");
    }

    #[test]
    fn benchmark_comparison() {
        for &n in &[1_000, 10_000, 100_000] {
            eprintln!("\n=== {n} slabs ===");
            bench_at_size(n);
        }
    }
}
