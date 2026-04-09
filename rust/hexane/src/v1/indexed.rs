use std::marker::PhantomData;

use super::delta::{DeltaColumn, DeltaValue};
use crate::PackError;

// ── Segment tree node ───────────────────────────────────────────────────────

/// A segment tree node storing the value-range metadata for a contiguous range
/// of items in a delta column.
///
/// `total` is the sum of all deltas in the range.  `min_offset` and
/// `max_offset` are the minimum and maximum *running prefix sums* relative to
/// the range's start.  Given `prefix_before` (the absolute prefix sum of all
/// items before this range), the absolute realized-value range is:
///
/// ```text
/// [prefix_before + min_offset, prefix_before + max_offset]
/// ```
#[derive(Copy, Clone, Debug, Default)]
struct RangeNode {
    /// Sum of all deltas in this range.
    total: i64,
    /// Minimum running prefix sum relative to range start.
    min_offset: i64,
    /// Maximum running prefix sum relative to range start.
    max_offset: i64,
    /// Number of items in this range.
    len: usize,
}

impl RangeNode {
    /// Merge two adjacent ranges.
    ///
    /// Right child's offsets are relative to its own start, which is
    /// `left.total` from the merged range's start.
    #[inline]
    fn merge(left: &RangeNode, right: &RangeNode) -> RangeNode {
        if left.len == 0 {
            return *right;
        }
        if right.len == 0 {
            return *left;
        }
        RangeNode {
            total: left.total + right.total,
            min_offset: left.min_offset.min(left.total + right.min_offset),
            max_offset: left.max_offset.max(left.total + right.max_offset),
            len: left.len + right.len,
        }
    }

    /// Can the target realized value exist anywhere in this range?
    #[inline]
    fn contains(&self, target: i64, prefix_before: i64) -> bool {
        self.len > 0
            && target >= prefix_before + self.min_offset
            && target <= prefix_before + self.max_offset
    }
}

// ── Compute leaf from slab data ─────────────────────────────────────────────

use super::leb::{read_signed, read_unsigned};

/// Compute a [`RangeNode`] from an RLE-encoded slab of signed deltas (i64).
///
/// Walks the slab in O(segments) — repeat runs are handled in O(1) each
/// because a monotonic run's min/max are always at the endpoints.
fn compute_leaf_i64(data: &[u8]) -> RangeNode {
    let mut byte_pos = 0;
    let mut partial = 0i64;
    let mut min_off = i64::MAX;
    let mut max_off = i64::MIN;
    let mut len = 0usize;

    while byte_pos < data.len() {
        let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
            Some(v) => v,
            None => break,
        };

        match count_raw {
            n if n > 0 => {
                // Repeat run: value repeated n times.
                let count = n as usize;
                let vs = byte_pos + cb;
                let (vl, val) = match read_signed(&data[vs..]) {
                    Some(v) => v,
                    None => break,
                };
                // Monotonic run: endpoints are first and last partial sums.
                let first = partial + val;
                let last = partial + val * count as i64;
                min_off = min_off.min(first.min(last));
                max_off = max_off.max(first.max(last));
                partial = last;
                len += count;
                byte_pos = vs + vl;
            }
            n if n < 0 => {
                // Literal run: -n individual values.
                let total = (-n) as usize;
                let mut scan = byte_pos + cb;
                for _ in 0..total {
                    let (vl, val) = match read_signed(&data[scan..]) {
                        Some(v) => v,
                        None => break,
                    };
                    partial += val;
                    min_off = min_off.min(partial);
                    max_off = max_off.max(partial);
                    len += 1;
                    scan += vl;
                }
                byte_pos = scan;
            }
            _ => {
                // Null run: 0 header followed by null count.
                let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                    Some(v) => v,
                    None => break,
                };
                // Nulls contribute zero delta — partial unchanged.
                // But each null item has the current partial as its "realized" offset.
                // Since partial doesn't change, we just check it once.
                if null_count > 0 {
                    min_off = min_off.min(partial);
                    max_off = max_off.max(partial);
                }
                len += null_count as usize;
                byte_pos += cb + ncb;
            }
        }
    }

    if len == 0 {
        return RangeNode::default();
    }

    RangeNode {
        total: partial,
        min_offset: min_off,
        max_offset: max_off,
        len,
    }
}

// ── IndexedDeltaColumn ──────────────────────────────────────────────────────

/// A [`DeltaColumn`] augmented with a segment tree for O(log n) value lookup.
///
/// Supports `find_all(target)` — returning all indices where the realized
/// value equals `target`.  The segment tree stores per-slab min/max of
/// realized-value offsets, enabling top-down pruning that skips subtrees
/// whose value ranges exclude the target.
///
/// For the common automerge pattern (monotonically increasing values with
/// each value appearing at most once), `find_all` runs in **O(log n)**.
/// For random data it degrades gracefully to O(n).
pub struct IndexedDeltaColumn<T: DeltaValue> {
    col: DeltaColumn<T>,
    /// Array-based segment tree.  1-indexed: `tree[1]` is the root.
    /// Leaves at `[tree_n .. 2*tree_n)` map to slabs (padded to power of 2).
    tree: Vec<RangeNode>,
    /// Number of leaves (next power of 2 ≥ slab count).
    tree_n: usize,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue> Default for IndexedDeltaColumn<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DeltaValue> IndexedDeltaColumn<T> {
    /// Create an empty indexed delta column.
    pub fn new() -> Self {
        Self {
            col: DeltaColumn::new(),
            tree: vec![RangeNode::default(); 2],
            tree_n: 1,
            _phantom: PhantomData,
        }
    }

    /// Create an empty indexed delta column with a custom segment budget.
    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            col: DeltaColumn::with_max_segments(max_segments),
            tree: vec![RangeNode::default(); 2],
            tree_n: 1,
            _phantom: PhantomData,
        }
    }

    /// Bulk-construct from a Vec of realized values.
    pub fn from_values(values: Vec<T>) -> Self {
        let col = DeltaColumn::from_values(values);
        let (tree, tree_n) = build_tree(&col);
        Self {
            col,
            tree,
            tree_n,
            _phantom: PhantomData,
        }
    }

    /// Deserialize from bytes produced by [`save`](IndexedDeltaColumn::save).
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        let col = DeltaColumn::load(data)?;
        let (tree, tree_n) = build_tree(&col);
        Ok(Self {
            col,
            tree,
            tree_n,
            _phantom: PhantomData,
        })
    }

    /// Deserialize with options (applied to the inner delta column).
    /// See [`LoadOpts`](super::LoadOpts).
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T::Inner>) -> Result<Self, PackError> {
        let col = DeltaColumn::load_with(data, opts)?;
        let (tree, tree_n) = build_tree(&col);
        Ok(Self {
            col,
            tree,
            tree_n,
            _phantom: PhantomData,
        })
    }

    // ── Delegated read methods ───────────────────────────────────────────

    /// Total number of items in the column.
    pub fn len(&self) -> usize {
        self.col.len()
    }

    /// Returns `true` if the column contains no items.
    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    /// Returns the realized value at `index`, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<T> {
        self.col.get(index)
    }

    /// Serialize the column into a byte array.
    pub fn save(&self) -> Vec<u8> {
        self.col.save()
    }

    /// Number of slabs in the column.
    pub fn slab_count(&self) -> usize {
        self.col.slab_count()
    }

    // ── Mutations (rebuild segment tree after) ───────────────────────────

    /// Inserts `value` at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index > self.len()`.
    pub fn insert(&mut self, index: usize, value: T) {
        self.col.insert(index, value);
        self.rebuild_tree();
    }

    /// Removes the value at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len()`.
    pub fn remove(&mut self, index: usize) {
        self.col.remove(index);
        self.rebuild_tree();
    }

    /// Appends `value` to the end of the column.
    pub fn push(&mut self, value: T) {
        self.col.push(value);
        self.rebuild_tree();
    }

    /// Removes and returns the last realized value, or `None` if empty.
    pub fn pop(&mut self) -> Option<T> {
        let val = self.col.pop();
        self.rebuild_tree();
        val
    }

    /// Returns the first realized value, or `None` if empty.
    pub fn first(&self) -> Option<T> {
        self.col.first()
    }

    /// Returns the last realized value, or `None` if empty.
    pub fn last(&self) -> Option<T> {
        self.col.last()
    }

    /// Removes all elements from the column.
    pub fn clear(&mut self) {
        self.col.clear();
        self.rebuild_tree();
    }

    /// Shortens the column to `len` elements.
    ///
    /// If `len >= self.len()`, this is a no-op.
    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.col.truncate(len);
            self.rebuild_tree();
        }
    }

    /// Removes `del` elements starting at `index` and inserts `values` in their place.
    ///
    /// # Panics
    ///
    /// Panics if `index + del > self.len()`.
    pub fn splice(&mut self, index: usize, del: usize, values: impl IntoIterator<Item = T>) {
        self.col.splice(index, del, values);
        self.rebuild_tree();
    }

    // ── Value lookup ─────────────────────────────────────────────────────

    /// Find all indices where the realized value equals `target`.
    ///
    /// For monotonic data with unique values: **O(log n)**.
    /// For data with many slabs containing `target` in their range: degrades
    /// proportionally to the number of candidate slabs.
    pub fn find(&self, target: T) -> Vec<usize> {
        let Some(target_i64) = target.to_i64() else {
            // Searching for null — scan linearly for null entries.
            return self.find_nulls();
        };

        let mut results = Vec::new();
        if self.col.is_empty() {
            return results;
        }
        self.search(1, target_i64, 0, 0, &mut results);
        results
    }

    /// Find the first index where the realized value equals `target`, or `None`.
    pub fn find_first(&self, target: T) -> Option<usize> {
        let target_i64 = target.to_i64()?;
        if self.col.is_empty() {
            return None;
        }
        let mut result = None;
        self.search_first(1, target_i64, 0, 0, &mut result);
        result
    }

    // ── Internal tree operations ─────────────────────────────────────────

    fn rebuild_tree(&mut self) {
        let (tree, tree_n) = build_tree(&self.col);
        self.tree = tree;
        self.tree_n = tree_n;
    }

    /// Recursive top-down search for all indices with realized value == target.
    fn search(
        &self,
        node: usize,
        target: i64,
        prefix_before: i64,
        items_before: usize,
        results: &mut Vec<usize>,
    ) {
        if node >= self.tree.len() {
            return;
        }
        let n = &self.tree[node];
        if !n.contains(target, prefix_before) {
            return;
        }

        // Leaf node — scan the slab.
        if node >= self.tree_n {
            let slab_idx = node - self.tree_n;
            if slab_idx < self.col.slab_count() {
                self.scan_slab(slab_idx, target, prefix_before, items_before, results);
            }
            return;
        }

        let left = 2 * node;
        let right = 2 * node + 1;
        let left_node = if left < self.tree.len() {
            &self.tree[left]
        } else {
            return;
        };
        self.search(left, target, prefix_before, items_before, results);
        self.search(
            right,
            target,
            prefix_before + left_node.total,
            items_before + left_node.len,
            results,
        );
    }

    /// Like `search` but stops after finding the first match.
    fn search_first(
        &self,
        node: usize,
        target: i64,
        prefix_before: i64,
        items_before: usize,
        result: &mut Option<usize>,
    ) {
        if result.is_some() || node >= self.tree.len() {
            return;
        }
        let n = &self.tree[node];
        if !n.contains(target, prefix_before) {
            return;
        }

        if node >= self.tree_n {
            let slab_idx = node - self.tree_n;
            if slab_idx < self.col.slab_count() {
                let mut results = Vec::new();
                self.scan_slab(slab_idx, target, prefix_before, items_before, &mut results);
                if let Some(&first) = results.first() {
                    *result = Some(first);
                }
            }
            return;
        }

        let left = 2 * node;
        let right = 2 * node + 1;
        let left_node = if left < self.tree.len() {
            &self.tree[left]
        } else {
            return;
        };
        self.search_first(left, target, prefix_before, items_before, result);
        self.search_first(
            right,
            target,
            prefix_before + left_node.total,
            items_before + left_node.len,
            result,
        );
    }

    /// Scan a single slab for items whose realized value == target.
    fn scan_slab(
        &self,
        slab_idx: usize,
        target: i64,
        prefix_before: i64,
        items_before: usize,
        results: &mut Vec<usize>,
    ) {
        let local_target = target - prefix_before;

        // Access the slab data through the inner PrefixColumn -> Column.
        let slab_data = self.col.inner().values().slab_data();
        let data = &slab_data[slab_idx];

        let mut byte_pos = 0;
        let mut item_idx = 0usize;
        let mut partial = 0i64;

        while byte_pos < data.len() {
            let (cb, count_raw) = match read_signed(&data[byte_pos..]) {
                Some(v) => v,
                None => break,
            };

            match count_raw {
                n if n > 0 => {
                    let count = n as usize;
                    let vs = byte_pos + cb;
                    let (vl, val) = match read_signed(&data[vs..]) {
                        Some(v) => v,
                        None => break,
                    };
                    // Repeat run: value `val` repeated `count` times.
                    // Realized offsets: partial+val, partial+2*val, ..., partial+count*val
                    // We want partial + k*val == local_target, i.e. k = (local_target - partial) / val
                    if val == 0 {
                        // All items in run have realized offset == partial.
                        if partial == local_target {
                            for k in 0..count {
                                results.push(items_before + item_idx + k);
                            }
                        }
                    } else {
                        let diff = local_target - partial;
                        if diff % val == 0 {
                            let k = diff / val;
                            if k >= 1 && k <= count as i64 {
                                results.push(items_before + item_idx + (k - 1) as usize);
                            }
                        }
                    }
                    partial += val * count as i64;
                    item_idx += count;
                    byte_pos = vs + vl;
                }
                n if n < 0 => {
                    let total = (-n) as usize;
                    let mut scan = byte_pos + cb;
                    for _ in 0..total {
                        let (vl, val) = match read_signed(&data[scan..]) {
                            Some(v) => v,
                            None => break,
                        };
                        partial += val;
                        if partial == local_target {
                            results.push(items_before + item_idx);
                        }
                        item_idx += 1;
                        scan += vl;
                    }
                    byte_pos = scan;
                }
                _ => {
                    // Null run.
                    let (ncb, null_count) = match read_unsigned(&data[byte_pos + cb..]) {
                        Some(v) => v,
                        None => break,
                    };
                    // Null items have realized offset == partial (delta is 0).
                    // But they're null — don't match non-null targets.
                    // (Null searching is handled by find_nulls.)
                    item_idx += null_count as usize;
                    byte_pos += cb + ncb;
                }
            }
        }
    }

    /// Linear scan for null entries (for nullable delta columns).
    fn find_nulls(&self) -> Vec<usize> {
        let mut results = Vec::new();
        for i in 0..self.col.len() {
            if let Some(v) = self.col.get(i) {
                if v.to_i64().is_none() {
                    results.push(i);
                }
            }
        }
        results
    }
}

// ── Trait impls ─────────────────────────────────────────────────────────────

impl<T: DeltaValue> FromIterator<T> for IndexedDeltaColumn<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<T: DeltaValue> Extend<T> for IndexedDeltaColumn<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let vals: Vec<T> = iter.into_iter().collect();
        if !vals.is_empty() {
            let len = self.len();
            self.splice(len, 0, vals);
        }
    }
}

// ── Tree construction ───────────────────────────────────────────────────────

/// Build the segment tree from the current state of a DeltaColumn.
/// Returns `(tree, tree_n)` where `tree_n` is the number of leaves.
fn build_tree<T: DeltaValue>(col: &DeltaColumn<T>) -> (Vec<RangeNode>, usize) {
    let s = col.slab_count();
    if s == 0 {
        return (vec![RangeNode::default(); 2], 1);
    }

    // Round up to next power of 2.
    let mut tree_n = 1;
    while tree_n < s {
        tree_n <<= 1;
    }

    let mut tree = vec![RangeNode::default(); 2 * tree_n];

    // Fill leaves from slab data.
    let slab_data = col.inner().values().slab_data();
    for (i, data) in slab_data.iter().enumerate() {
        tree[tree_n + i] = compute_leaf_i64(data);
    }

    // Build internal nodes bottom-up.
    for i in (1..tree_n).rev() {
        tree[i] = RangeNode::merge(&tree[2 * i], &tree[2 * i + 1]);
    }

    (tree, tree_n)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_empty() {
        let col = IndexedDeltaColumn::<u64>::new();
        assert_eq!(col.find(42), vec![]);
        assert_eq!(col.find_first(42), None);
    }

    #[test]
    fn find_single() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![42]);
        assert_eq!(col.find(42), vec![0]);
        assert_eq!(col.find(99), vec![]);
        assert_eq!(col.find_first(42), Some(0));
        assert_eq!(col.find_first(99), None);
    }

    #[test]
    fn find_monotonic() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        assert_eq!(col.find(10), vec![0]);
        assert_eq!(col.find(30), vec![2]);
        assert_eq!(col.find(50), vec![4]);
        assert_eq!(col.find(25), vec![]);
    }

    #[test]
    fn find_sequential() {
        // Common automerge pattern: values incrementing by 1
        let values: Vec<u64> = (0..100).collect();
        let col = IndexedDeltaColumn::<u64>::from_values(values);
        for i in 0..100u64 {
            assert_eq!(col.find(i), vec![i as usize], "find({i})");
            assert_eq!(col.find_first(i), Some(i as usize), "find_first({i})");
        }
        assert_eq!(col.find(100), vec![]);
    }

    #[test]
    fn find_with_duplicates() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![1, 2, 2, 3, 3, 3, 4]);
        assert_eq!(col.find(1), vec![0]);
        assert_eq!(col.find(2), vec![1, 2]);
        assert_eq!(col.find(3), vec![3, 4, 5]);
        assert_eq!(col.find(4), vec![6]);
    }

    #[test]
    fn find_non_monotonic() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![100, 50, 200, 10, 300]);
        assert_eq!(col.find(100), vec![0]);
        assert_eq!(col.find(50), vec![1]);
        assert_eq!(col.find(200), vec![2]);
        assert_eq!(col.find(10), vec![3]);
        assert_eq!(col.find(300), vec![4]);
        assert_eq!(col.find(999), vec![]);
    }

    #[test]
    fn find_after_insert() {
        let mut col = IndexedDeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(1, 15);
        // Values: [10, 15, 20, 30]
        assert_eq!(col.find(10), vec![0]);
        assert_eq!(col.find(15), vec![1]);
        assert_eq!(col.find(20), vec![2]);
        assert_eq!(col.find(30), vec![3]);
    }

    #[test]
    fn find_after_remove() {
        let mut col = IndexedDeltaColumn::<u64>::from_values(vec![10, 20, 30, 40]);
        col.remove(1);
        // Values: [10, 30, 40]
        assert_eq!(col.find(10), vec![0]);
        assert_eq!(col.find(20), vec![]);
        assert_eq!(col.find(30), vec![1]);
        assert_eq!(col.find(40), vec![2]);
    }

    #[test]
    fn find_after_splice() {
        let mut col = IndexedDeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        col.splice(1, 2, [25, 35]);
        // Values: [10, 25, 35, 40, 50]
        assert_eq!(col.find(10), vec![0]);
        assert_eq!(col.find(25), vec![1]);
        assert_eq!(col.find(35), vec![2]);
        assert_eq!(col.find(20), vec![]);
        assert_eq!(col.find(30), vec![]);
    }

    #[test]
    fn find_i64_signed() {
        let col = IndexedDeltaColumn::<i64>::from_values(vec![5, -3, 8, -1, 10]);
        assert_eq!(col.find(5), vec![0]);
        assert_eq!(col.find(-3), vec![1]);
        assert_eq!(col.find(8), vec![2]);
        assert_eq!(col.find(-1), vec![3]);
        assert_eq!(col.find(10), vec![4]);
        assert_eq!(col.find(0), vec![]);
    }

    #[test]
    fn find_u32_type() {
        let col = IndexedDeltaColumn::<u32>::from_values(vec![1, 2, 3, 4, 5]);
        assert_eq!(col.find(3), vec![2]);
        assert_eq!(col.find(6), vec![]);
    }

    #[test]
    fn find_nullable() {
        let col = IndexedDeltaColumn::<Option<u64>>::from_values(vec![
            Some(5),
            None,
            Some(8),
            None,
            Some(12),
        ]);
        assert_eq!(col.find(Some(5)), vec![0]);
        assert_eq!(col.find(Some(8)), vec![2]);
        assert_eq!(col.find(Some(12)), vec![4]);
        assert_eq!(col.find(Some(99)), vec![]);
        // find(None) should return null indices
        assert_eq!(col.find(None), vec![1, 3]);
    }

    #[test]
    fn find_multi_slab() {
        // Force multiple slabs: alternating strides create many literal runs
        let values: Vec<u64> = (0..50)
            .map(|i| if i % 2 == 0 { i * 3 } else { i * 7 })
            .collect();
        let mut col = IndexedDeltaColumn::<u64>::with_max_segments(4);
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        assert!(col.slab_count() > 1, "should have multiple slabs");
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(col.find(v), vec![i], "find({v}) in multi-slab");
        }
        assert_eq!(col.find(9999), vec![]);
    }

    #[test]
    fn find_large_monotonic() {
        let values: Vec<u64> = (0..1000).collect();
        let col = IndexedDeltaColumn::<u64>::from_values(values);
        // Spot-check
        assert_eq!(col.find(0), vec![0]);
        assert_eq!(col.find(500), vec![500]);
        assert_eq!(col.find(999), vec![999]);
        assert_eq!(col.find(1000), vec![]);
    }

    #[test]
    fn find_constant_value() {
        // All same value — duplicates everywhere
        let col = IndexedDeltaColumn::<u64>::from_values(vec![7, 7, 7, 7, 7]);
        assert_eq!(col.find(7), vec![0, 1, 2, 3, 4]);
        assert_eq!(col.find(0), vec![]);
    }

    #[test]
    fn find_first_basic() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![5, 5, 10, 10, 15]);
        assert_eq!(col.find_first(5), Some(0));
        assert_eq!(col.find_first(10), Some(2));
        assert_eq!(col.find_first(15), Some(4));
        assert_eq!(col.find_first(99), None);
    }

    #[test]
    fn save_load_roundtrip() {
        let col = IndexedDeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        let bytes = col.save();
        let loaded = IndexedDeltaColumn::<u64>::load(&bytes).unwrap();
        for i in 0..5 {
            assert_eq!(loaded.get(i), col.get(i));
        }
        assert_eq!(loaded.find(30), vec![2]);
    }

    #[test]
    fn find_zero_delta_run() {
        // Values: [5, 5, 5, 10, 10]
        // Deltas: [5, 0, 0, 5, 0]  — zero-delta repeat run
        let col = IndexedDeltaColumn::<u64>::from_values(vec![5, 5, 5, 10, 10]);
        assert_eq!(col.find(5), vec![0, 1, 2]);
        assert_eq!(col.find(10), vec![3, 4]);
    }
}
