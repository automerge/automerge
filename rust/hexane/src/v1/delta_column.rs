use std::marker::PhantomData;

use super::prefix_column::PrefixValue;
use super::ColumnValueRef;
use super::PrefixColumn;
use crate::PackError;

// ── DeltaValue trait ────────────────────────────────────────────────────────

/// Trait for value types that can be stored in a delta-encoded column.
///
/// All types store deltas internally using `Self::Inner` — `i64` for
/// non-nullable types, `Option<i64>` for nullable types.
///
/// | External type   | `Inner`       |
/// |-----------------|---------------|
/// | `u32`           | `i64`         |
/// | `u64`           | `i64`         |
/// | `i32`           | `i64`         |
/// | `i64`           | `i64`         |
/// | `Option<u32>`   | `Option<i64>` |
/// | `Option<u64>`   | `Option<i64>` |
/// | `Option<i32>`   | `Option<i64>` |
/// | `Option<i64>`   | `Option<i64>` |
pub trait DeltaValue: Copy {
    /// The inner column value type for storing deltas.
    type Inner: PrefixValue + Copy;

    /// Whether this type supports null values.
    const NULLABLE: bool;

    /// Convert to `i64` for delta computation. Returns `None` for null values.
    fn to_i64(self) -> Option<i64>;

    /// Convert from an `i64` realized value back to this type.
    fn from_i64(v: i64) -> Self;

    /// Create a null value. Panics for non-nullable types.
    fn null_value() -> Self;

    /// Create an inner delta value from an `Option<i64>` delta.
    /// For non-nullable types, `None` input panics.
    fn make_inner(delta: Option<i64>) -> Self::Inner;

    /// Extract `Option<i64>` from an inner column get result.
    /// Returns `None` for null deltas, `Some(d)` for non-null.
    fn get_inner(inner: <Self::Inner as ColumnValueRef>::Get<'_>) -> Option<i64>;

    /// Convert the prefix sum type to `i64`.
    fn prefix_to_i64(p: <Self::Inner as PrefixValue>::Prefix) -> i64;
}

// ── Non-nullable impls ──────────────────────────────────────────────────────

impl DeltaValue for u32 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as u32
    }
    fn null_value() -> Self {
        panic!("non-nullable u32")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable u32")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for u64 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as u64
    }
    fn null_value() -> Self {
        panic!("non-nullable u64")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable u64")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for i32 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self as i64)
    }
    fn from_i64(v: i64) -> Self {
        v as i32
    }
    fn null_value() -> Self {
        panic!("non-nullable i32")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable i32")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for i64 {
    type Inner = i64;
    const NULLABLE: bool = false;
    fn to_i64(self) -> Option<i64> {
        Some(self)
    }
    fn from_i64(v: i64) -> Self {
        v
    }
    fn null_value() -> Self {
        panic!("non-nullable i64")
    }
    fn make_inner(delta: Option<i64>) -> i64 {
        delta.expect("non-nullable i64")
    }
    fn get_inner(inner: i64) -> Option<i64> {
        Some(inner)
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

// ── Nullable impls ──────────────────────────────────────────────────────────

impl DeltaValue for Option<u32> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as u32)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<u64> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as u64)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<i32> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self.map(|v| v as i64)
    }
    fn from_i64(v: i64) -> Self {
        Some(v as i32)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

impl DeltaValue for Option<i64> {
    type Inner = Option<i64>;
    const NULLABLE: bool = true;
    fn to_i64(self) -> Option<i64> {
        self
    }
    fn from_i64(v: i64) -> Self {
        Some(v)
    }
    fn null_value() -> Self {
        None
    }
    fn make_inner(delta: Option<i64>) -> Option<i64> {
        delta
    }
    fn get_inner(inner: Option<i64>) -> Option<i64> {
        inner
    }
    fn prefix_to_i64(p: i128) -> i64 {
        p as i64
    }
}

// ── DeltaColumn ─────────────────────────────────────────────────────────────

/// A column that stores values using delta encoding, wrapping a
/// [`Column`](super::Column) of deltas.
///
/// Externally presents absolute values of type `T`, but internally stores
/// deltas in a `Column<T::Inner>`.  Prefix sums of deltas yield realized
/// values.
///
/// For a sequence `[6, 7, 8, 9]`, the stored deltas are `[6, 1, 1, 1]`.
/// Constant-stride sequences compress beautifully with RLE.
///
/// Mutations adjust neighboring deltas to maintain consistency:
/// - Inserting value `V` at index `I` stores `delta = V - prev_realized` and
///   adjusts the following delta so all subsequent realized values remain
///   unchanged.
/// - Removing index `I` absorbs its delta into the following element.
pub struct DeltaColumn<T: DeltaValue> {
    inner: PrefixColumn<T::Inner>,
    _phantom: PhantomData<T>,
}

impl<T: DeltaValue> Default for DeltaColumn<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DeltaValue> DeltaColumn<T> {
    pub fn new() -> Self {
        Self {
            inner: PrefixColumn::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            inner: PrefixColumn::with_max_segments(max_segments),
            _phantom: PhantomData,
        }
    }

    /// Bulk-construct from a Vec of realized values.
    pub fn from_values(values: Vec<T>) -> Self {
        if values.is_empty() {
            return Self::new();
        }
        let deltas = values_to_deltas::<T>(&values);
        Self {
            inner: PrefixColumn::from_values(deltas),
            _phantom: PhantomData,
        }
    }

    /// Deserialize from bytes produced by [`save`](DeltaColumn::save).
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        let col = super::Column::<T::Inner>::load(data)?;
        Ok(Self {
            inner: PrefixColumn::from_column(col),
            _phantom: PhantomData,
        })
    }

    /// Deserialize with options (applied to the inner delta column).
    /// See [`LoadOpts`](super::LoadOpts).
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T::Inner>) -> Result<Self, PackError>
    where
        T::Inner: super::ColumnDefault,
    {
        let col = super::Column::<T::Inner>::load_with(data, opts)?;
        Ok(Self {
            inner: PrefixColumn::from_column(col),
            _phantom: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn slab_count(&self) -> usize {
        self.inner.slab_count()
    }

    pub fn validate_encoding(&self) {
        self.inner.validate_encoding();
    }

    /// Serialize the delta-encoded column to bytes.
    pub fn save(&self) -> Vec<u8> {
        self.inner.save()
    }

    /// Returns the realized value at `index`, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<T> {
        self.iter().nth(index)
    }

    /// Returns an iterator over all realized values.
    pub fn iter(&self) -> DeltaIter<'_, T> {
        DeltaIter {
            inner: self.inner.iter(),
            _phantom: PhantomData,
        }
    }

    /// Returns an iterator over realized values in `range`.
    pub fn iter_range(&self, range: std::ops::Range<usize>) -> DeltaIter<'_, T> {
        DeltaIter {
            inner: self.inner.iter_range(range),
            _phantom: PhantomData,
        }
    }

    /// Inserts `value` at `index`, adjusting the following delta.
    /// Panics if `index > self.len()`.
    pub fn insert(&mut self, index: usize, value: T) {
        let len = self.inner.len();
        assert!(index <= len, "insert index out of bounds");

        match value.to_i64() {
            None => {
                self.inner.insert(index, T::make_inner(None));
            }
            Some(v) => {
                let prev = self.prev_realized(index);
                let new_delta = v - prev;

                if index >= len {
                    self.inner.insert(index, T::make_inner(Some(new_delta)));
                    return;
                }

                let current = T::get_inner(self.inner.get_value(index).unwrap());

                match current {
                    Some(d) => {
                        self.inner.splice(
                            index,
                            1,
                            [
                                T::make_inner(Some(new_delta)),
                                T::make_inner(Some(d - new_delta)),
                            ],
                        );
                    }
                    None => {
                        self.insert_before_null_run(index, new_delta);
                    }
                }
            }
        }
    }

    /// Removes the value at `index`, absorbing its delta into the next element.
    /// Panics if `index >= self.len()`.
    pub fn remove(&mut self, index: usize) {
        let len = self.inner.len();
        assert!(index < len, "remove index out of bounds");

        let delta = T::get_inner(self.inner.get_value(index).unwrap());

        match delta {
            None => {
                self.inner.remove(index);
            }
            Some(d) => {
                if index + 1 >= len {
                    self.inner.remove(index);
                    return;
                }
                self.remove_and_absorb(index, d);
            }
        }
    }

    /// Removes `del` elements starting at `index` and inserts `values` in their place.
    /// Panics if `index + del > self.len()`.
    pub fn splice(&mut self, index: usize, del: usize, values: impl IntoIterator<Item = T>) {
        let len = self.inner.len();
        assert!(index + del <= len, "splice range out of bounds");

        let values: Vec<T> = values.into_iter().collect();
        if del == 0 && values.is_empty() {
            return;
        }

        let prev = self.prev_realized(index);

        let new_deltas = values_to_deltas_from::<T>(&values, prev);

        let new_prefix_end = {
            let mut p = prev;
            for v in &values {
                if let Some(r) = v.to_i64() {
                    p = r;
                }
            }
            p
        };

        let splice_end = index + del;
        if splice_end < len {
            let old_prefix_at_end = self.prefix_sum(splice_end);
            let adjustment = old_prefix_at_end - new_prefix_end;

            let (extra_del, mut boundary_deltas) = self.find_nonnull_from(splice_end, adjustment);

            let mut all_deltas: Vec<T::Inner> = new_deltas;
            all_deltas.append(&mut boundary_deltas);
            self.inner.splice(index, del + extra_del, all_deltas);
        } else {
            self.inner.splice(index, del, new_deltas);
        }
    }

    /// Access the inner PrefixColumn.
    pub(crate) fn inner(&self) -> &PrefixColumn<T::Inner> {
        &self.inner
    }

    // ── Prefix sum (O(log S) via Fenwick tree) ──────────────────────────────

    fn prefix_sum(&self, count: usize) -> i64 {
        T::prefix_to_i64(self.inner.get_prefix(count))
    }

    fn prev_realized(&self, index: usize) -> i64 {
        self.prefix_sum(index)
    }

    // ── Null-aware helpers ──────────────────────────────────────────────────

    fn insert_before_null_run(&mut self, index: usize, new_delta: i64) {
        let len = self.inner.len();
        let mut j = index;
        while j < len {
            if T::get_inner(self.inner.get_value(j).unwrap()).is_some() {
                break;
            }
            j += 1;
        }

        if j < len {
            let d_j = T::get_inner(self.inner.get_value(j).unwrap()).unwrap();
            let adjusted = d_j - new_delta;
            let null_count = j - index;
            let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 2);
            vals.push(T::make_inner(Some(new_delta)));
            for _ in 0..null_count {
                vals.push(T::make_inner(None));
            }
            vals.push(T::make_inner(Some(adjusted)));
            self.inner.splice(index, j - index + 1, vals);
        } else {
            self.inner.insert(index, T::make_inner(Some(new_delta)));
        }
    }

    fn remove_and_absorb(&mut self, index: usize, delta: i64) {
        let len = self.inner.len();
        debug_assert!(index + 1 < len);

        if !T::NULLABLE {
            let d_next = T::get_inner(self.inner.get_value(index + 1).unwrap()).unwrap();
            self.inner
                .splice(index, 2, [T::make_inner(Some(delta + d_next))]);
            return;
        }

        let mut j = index + 1;
        while j < len {
            if let Some(d_j) = T::get_inner(self.inner.get_value(j).unwrap()) {
                let adjusted = delta + d_j;
                let null_count = j - index - 1;
                let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 1);
                for _ in 0..null_count {
                    vals.push(T::make_inner(None));
                }
                vals.push(T::make_inner(Some(adjusted)));
                self.inner.splice(index, j - index + 1, vals);
                return;
            }
            j += 1;
        }
        self.inner.remove(index);
    }

    fn find_nonnull_from(&self, from: usize, adjustment: i64) -> (usize, Vec<T::Inner>) {
        if adjustment == 0 && !T::NULLABLE {
            let d = T::get_inner(self.inner.get_value(from).unwrap()).unwrap();
            return (1, vec![T::make_inner(Some(d))]);
        }
        if adjustment == 0 {
            return (0, vec![]);
        }

        let len = self.inner.len();
        if !T::NULLABLE {
            let d = T::get_inner(self.inner.get_value(from).unwrap()).unwrap();
            return (1, vec![T::make_inner(Some(d + adjustment))]);
        }

        let mut j = from;
        while j < len {
            if let Some(d) = T::get_inner(self.inner.get_value(j).unwrap()) {
                let adjusted = d + adjustment;
                let null_count = j - from;
                let mut vals: Vec<T::Inner> = Vec::with_capacity(null_count + 1);
                for _ in 0..null_count {
                    vals.push(T::make_inner(None));
                }
                vals.push(T::make_inner(Some(adjusted)));
                return (j - from + 1, vals);
            }
            j += 1;
        }
        (0, vec![])
    }
}

// ── DeltaIter ───────────────────────────────────────────────────────────────

/// Iterator over realized values in a [`DeltaColumn`].
///
/// Each yielded value is the prefix sum of deltas through that position,
/// converted back to the external type `T`.
pub struct DeltaIter<'a, T: DeltaValue> {
    inner: super::PrefixIter<'a, T::Inner>,
    _phantom: PhantomData<T>,
}

impl<'a, T: DeltaValue> Iterator for DeltaIter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        let (prefix, raw) = self.inner.next()?;
        if T::get_inner(raw).is_none() {
            Some(T::null_value())
        } else {
            Some(T::from_i64(T::prefix_to_i64(prefix)))
        }
    }

    fn nth(&mut self, n: usize) -> Option<T> {
        let (prefix, raw) = self.inner.nth(n)?;
        if T::get_inner(raw).is_none() {
            Some(T::null_value())
        } else {
            Some(T::from_i64(T::prefix_to_i64(prefix)))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T: DeltaValue> ExactSizeIterator for DeltaIter<'_, T> {}

// ── FromIterator ────────────────────────────────────────────────────────────

impl<T: DeltaValue> FromIterator<T> for DeltaColumn<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

// ── Free functions ──────────────────────────────────────────────────────────

fn values_to_deltas<T: DeltaValue>(values: &[T]) -> Vec<T::Inner> {
    values_to_deltas_from::<T>(values, 0)
}

fn values_to_deltas_from<T: DeltaValue>(values: &[T], prev_realized: i64) -> Vec<T::Inner> {
    let mut deltas = Vec::with_capacity(values.len());
    let mut prev = prev_realized;
    for v in values {
        match v.to_i64() {
            None => deltas.push(T::make_inner(None)),
            Some(r) => {
                deltas.push(T::make_inner(Some(r - prev)));
                prev = r;
            }
        }
    }
    deltas
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_col<T: DeltaValue + PartialEq + std::fmt::Debug>(
        col: &DeltaColumn<T>,
        expected: &[T],
    ) {
        assert_eq!(col.len(), expected.len(), "length mismatch");
        for (i, exp) in expected.iter().enumerate() {
            assert_eq!(col.get(i).as_ref(), Some(exp), "mismatch at index {i}");
        }
    }

    // ── Basic ───────────────────────────────────────────────────────────────

    #[test]
    fn empty() {
        let col = DeltaColumn::<u64>::new();
        assert_eq!(col.len(), 0);
        assert_eq!(col.get(0), None);
    }

    #[test]
    fn single() {
        let col = DeltaColumn::<u64>::from_values(vec![42]);
        assert_col(&col, &[42]);
    }

    #[test]
    fn constant_stride() {
        let col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40]);
        assert_col(&col, &[10, 20, 30, 40]);
    }

    #[test]
    fn non_monotonic() {
        let col = DeltaColumn::<i64>::from_values(vec![5, 3, 8, 2, 10]);
        assert_col(&col, &[5, 3, 8, 2, 10]);
    }

    #[test]
    fn unsigned_non_monotonic() {
        let col = DeltaColumn::<u64>::from_values(vec![100, 50, 200, 10]);
        assert_col(&col, &[100, 50, 200, 10]);
    }

    // ── Insert ──────────────────────────────────────────────────────────────

    #[test]
    fn insert_at_start() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(0, 5);
        assert_col(&col, &[5, 10, 20, 30]);
    }

    #[test]
    fn insert_at_end() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(3, 40);
        assert_col(&col, &[10, 20, 30, 40]);
    }

    #[test]
    fn insert_in_middle() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.insert(1, 15);
        assert_col(&col, &[10, 15, 20, 30]);
    }

    #[test]
    fn insert_preserves_subsequent() {
        let mut col = DeltaColumn::<i64>::from_values(vec![6, 7, 8, 9]);
        col.insert(2, 10);
        assert_col(&col, &[6, 7, 10, 8, 9]);
    }

    #[test]
    fn sequential_inserts() {
        let mut col = DeltaColumn::<u64>::new();
        for i in 0..20 {
            col.insert(i, (i + 1) as u64 * 100);
        }
        let expected: Vec<u64> = (1..=20).map(|i| i * 100).collect();
        assert_col(&col, &expected);
    }

    // ── Remove ──────────────────────────────────────────────────────────────

    #[test]
    fn remove_first() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(0);
        assert_col(&col, &[20, 30]);
    }

    #[test]
    fn remove_last() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(2);
        assert_col(&col, &[10, 20]);
    }

    #[test]
    fn remove_middle() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(1);
        assert_col(&col, &[10, 30]);
    }

    #[test]
    fn remove_preserves_subsequent() {
        let mut col = DeltaColumn::<i64>::from_values(vec![6, 7, 8, 9]);
        col.remove(1);
        assert_col(&col, &[6, 8, 9]);
    }

    #[test]
    fn remove_all() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.remove(0);
        col.remove(0);
        col.remove(0);
        assert_eq!(col.len(), 0);
    }

    // ── Splice ──────────────────────────────────────────────────────────────

    #[test]
    fn splice_replace() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        col.splice(1, 2, [25, 35]);
        assert_col(&col, &[10, 25, 35, 40, 50]);
    }

    #[test]
    fn splice_insert_only() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.splice(1, 0, [15, 17]);
        assert_col(&col, &[10, 15, 17, 20, 30]);
    }

    #[test]
    fn splice_delete_only() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30, 40, 50]);
        col.splice(1, 3, std::iter::empty());
        assert_col(&col, &[10, 50]);
    }

    #[test]
    fn splice_at_end() {
        let mut col = DeltaColumn::<u64>::from_values(vec![10, 20, 30]);
        col.splice(2, 1, [100, 200]);
        assert_col(&col, &[10, 20, 100, 200]);
    }

    // ── Save / Load ─────────────────────────────────────────────────────────

    #[test]
    fn save_load_roundtrip() {
        let col = DeltaColumn::<u64>::from_values(vec![100, 200, 300, 400, 500]);
        let bytes = col.save();
        let loaded = DeltaColumn::<u64>::load(&bytes).unwrap();
        assert_col(&loaded, &[100, 200, 300, 400, 500]);
    }

    #[test]
    fn save_load_negative_deltas() {
        let col = DeltaColumn::<i64>::from_values(vec![10, 5, 15, 3, 20]);
        let bytes = col.save();
        let loaded = DeltaColumn::<i64>::load(&bytes).unwrap();
        assert_col(&loaded, &[10, 5, 15, 3, 20]);
    }

    // ── Multi-slab ──────────────────────────────────────────────────────────

    #[test]
    fn multi_slab_get() {
        let values: Vec<u64> = (0..50)
            .map(|i| if i % 2 == 0 { i * 3 } else { i * 7 })
            .collect();
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        assert!(col.slab_count() > 1, "should have multiple slabs");
        assert_col(&col, &values);
    }

    #[test]
    fn multi_slab_insert_remove() {
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        let mut mirror: Vec<u64> = Vec::new();
        for i in 0..30 {
            let v = (i * 7 + 3) as u64;
            col.insert(i, v);
            mirror.push(v);
        }
        assert_col(&col, &mirror);
        for _ in 0..5 {
            col.remove(10);
            mirror.remove(10);
        }
        assert_col(&col, &mirror);
        for i in 0..5 {
            let v = (1000 + i) as u64;
            col.insert(5, v);
            mirror.insert(5, v);
        }
        assert_col(&col, &mirror);
    }

    #[test]
    fn multi_slab_save_load() {
        let mut col = DeltaColumn::<u64>::with_max_segments(4);
        let values: Vec<u64> = (1..=100).collect();
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
        }
        let bytes = col.save();
        let loaded = DeltaColumn::<u64>::load(&bytes).unwrap();
        assert_col(&loaded, &values);
    }

    // ── Type variants ───────────────────────────────────────────────────────

    #[test]
    fn u32_type() {
        let col = DeltaColumn::<u32>::from_values(vec![1, 2, 3, 4, 5]);
        assert_col(&col, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn i32_type() {
        let col = DeltaColumn::<i32>::from_values(vec![10, -5, 20, -10, 0]);
        assert_col(&col, &[10, -5, 20, -10, 0]);
    }

    // ── Nullable ────────────────────────────────────────────────────────────

    #[test]
    fn nullable_basic() {
        let col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8), Some(10)]);
        assert_col(&col, &[Some(5), None, Some(8), Some(10)]);
    }

    #[test]
    fn nullable_insert_before_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8)]);
        col.insert(1, Some(6));
        assert_col(&col, &[Some(5), Some(6), None, Some(8)]);
    }

    #[test]
    fn nullable_insert_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), Some(10)]);
        col.insert(1, None);
        assert_col(&col, &[Some(5), None, Some(8), Some(10)]);
    }

    #[test]
    fn nullable_remove_null() {
        let mut col = DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8)]);
        col.remove(1);
        assert_col(&col, &[Some(5), Some(8)]);
    }

    #[test]
    fn nullable_remove_before_null() {
        let mut col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), None, Some(12)]);
        col.remove(1);
        assert_col(&col, &[Some(5), None, Some(12)]);
    }

    #[test]
    fn nullable_splice() {
        let mut col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), Some(8), Some(10), Some(15)]);
        col.splice(1, 2, [None, Some(12)]);
        assert_col(&col, &[Some(5), None, Some(12), Some(15)]);
    }

    #[test]
    fn nullable_all_null() {
        let col = DeltaColumn::<Option<u64>>::from_values(vec![None, None, None]);
        assert_col(&col, &[None, None, None]);
    }

    #[test]
    fn nullable_save_load() {
        let col =
            DeltaColumn::<Option<u64>>::from_values(vec![Some(5), None, Some(8), None, Some(12)]);
        let bytes = col.save();
        let loaded = DeltaColumn::<Option<u64>>::load(&bytes).unwrap();
        assert_col(&loaded, &[Some(5), None, Some(8), None, Some(12)]);
    }

    // ── v0 save compatibility ───────────────────────────────────────────────

    /// Build a v0 DeltaCursor column and a v1 DeltaColumn from the same
    /// realized values and assert their serialized bytes are identical.
    fn assert_v0_v1_save_match(values: &[Option<i64>]) {
        use crate::ColumnData as V0ColumnData;
        use crate::DeltaCursor;

        // v0
        let mut v0: V0ColumnData<DeltaCursor> = V0ColumnData::new();
        v0.splice(0, 0, values.to_vec());
        let v0_bytes = v0.save();

        // v1
        let v1 = DeltaColumn::<Option<i64>>::from_values(values.to_vec());
        let v1_bytes = v1.save();

        assert_eq!(
            v0_bytes, v1_bytes,
            "v0 vs v1 save mismatch for values: {:?}",
            values
        );

        // Also verify v1 reads back correctly.
        let loaded = DeltaColumn::<Option<i64>>::load(&v1_bytes).unwrap();
        for (i, exp) in values.iter().enumerate() {
            assert_eq!(loaded.get(i).as_ref(), Some(exp), "reload mismatch at {i}");
        }
    }

    #[test]
    fn v0_v1_save_empty() {
        assert_v0_v1_save_match(&[]);
    }

    #[test]
    fn v0_v1_save_single() {
        assert_v0_v1_save_match(&[Some(42)]);
    }

    #[test]
    fn v0_v1_save_constant_stride() {
        assert_v0_v1_save_match(&[Some(10), Some(20), Some(30), Some(40)]);
    }

    #[test]
    fn v0_v1_save_with_nulls() {
        assert_v0_v1_save_match(&[None, Some(0), Some(2), Some(3)]);
    }

    #[test]
    fn v0_v1_save_mixed() {
        assert_v0_v1_save_match(&[
            Some(1),
            Some(10),
            Some(2),
            Some(11),
            Some(4),
            Some(27),
            Some(19),
            Some(3),
            Some(21),
            Some(14),
            Some(2),
            Some(8),
        ]);
    }

    #[test]
    fn v0_v1_save_runs() {
        assert_v0_v1_save_match(&[
            Some(1),
            Some(2),
            Some(4),
            Some(6),
            Some(9),
            Some(12),
            Some(16),
            Some(20),
            Some(25),
            Some(30),
        ]);
    }

    #[test]
    fn v0_v1_save_nulls_and_values() {
        assert_v0_v1_save_match(&[
            None,
            Some(0),
            Some(2),
            Some(3),
            Some(4),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(7),
            Some(8),
            Some(9),
        ]);
    }

    #[test]
    fn v0_v1_save_fuzz() {
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(12345);

        for _ in 0..200 {
            let len = rng.random_range(0..50);
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            for _ in 0..len {
                let r: u32 = rng.random_range(0..10);
                if r == 0 {
                    values.push(None);
                } else {
                    values.push(Some(rng.random_range(0..100)));
                }
            }
            assert_v0_v1_save_match(&values);
        }
    }

    // ── Stress / fuzz ───────────────────────────────────────────────────────

    #[test]
    fn fuzz_insert_remove() {
        let mut col = DeltaColumn::<i64>::new();
        let mut mirror: Vec<i64> = Vec::new();
        let values: Vec<i64> = vec![
            100, 50, 200, 150, 300, 10, 500, 250, 400, 350, 80, 90, 70, 60, 40, 30, 20, 15, 5, 1,
        ];
        for (i, &v) in values.iter().enumerate() {
            col.insert(i, v);
            mirror.push(v);
            assert_col(&col, &mirror);
        }
        for &idx in &[5, 0, 10, 3, 7, 1, 0, 0, 0, 0] {
            if idx < col.len() {
                col.remove(idx);
                mirror.remove(idx);
                assert_col(&col, &mirror);
            }
        }
        for (i, &v) in [999, 888, 777, 666].iter().enumerate() {
            let pos = i.min(col.len());
            col.insert(pos, v);
            mirror.insert(pos, v);
            assert_col(&col, &mirror);
        }
    }

    #[test]
    fn fuzz_splice() {
        let mut col = DeltaColumn::<u64>::from_values((1..=20).collect());
        let mut mirror: Vec<u64> = (1..=20).collect();
        for iter in 0..10 {
            let len = col.len();
            if len < 4 {
                break;
            }
            let pos = (iter * 3 + 1) % (len - 2);
            let del = 2.min(len - pos);
            let new_vals: Vec<u64> = (0..3).map(|j| (iter * 100 + j + 1) as u64).collect();
            col.splice(pos, del, new_vals.clone());
            mirror.splice(pos..pos + del, new_vals);
            assert_col(&col, &mirror);
        }
    }
}
