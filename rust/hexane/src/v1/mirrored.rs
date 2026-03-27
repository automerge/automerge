//! Mirrored columns for v0/v1 cross-validation.
//!
//! A `MirroredColumn<T>` wraps both a v1 `Column<T>` and a v0 `ColumnData<C>`,
//! applying every mutation to both and asserting equivalence on every operation:
//!
//! - `get()` asserts both return the same value
//! - `save()` / `save_to()` asserts both produce identical bytes
//! - `MirrorIter` advances both iterators in lockstep, comparing every item
//!
//! The API is identical to `Column<T>` / `PrefixColumn<T>` — swap
//! `MirroredColumn<T>` for `Column<T>` to drop the validation overhead.

use std::borrow::Cow;
use std::ops::Range;

use crate::cursor::ColumnCursor;
use crate::ColumnData;
use crate::PackError;

use super::column::Column;
use super::prefix_column::{PrefixColumn, PrefixIter, PrefixValue};
use super::{AsColumnRef, ColumnDefault, ColumnValueRef};

// ── Mirrorable trait ────────────────────────────────────────────────────────

/// Bridge between a v1 value type and its v0 cursor counterpart.
///
/// Provides comparison methods and v0 column operations so that generic code
/// in `MirroredColumn`/`MirroredPrefixColumn` doesn't need HRTB bounds.
pub trait Mirrorable: ColumnValueRef + Sized {
    type V0Cursor: ColumnCursor;

    /// Compare a v0 `get()` result with a v1 `get()` result.
    fn compare_get(
        v0: Option<Option<Cow<'_, <Self::V0Cursor as ColumnCursor>::Item>>>,
        v1: Option<Self::Get<'_>>,
    ) -> bool;

    /// Compare a v0 iterator item with a v1 iterator item.
    fn compare_iter(
        v0: &Option<Cow<'_, <Self::V0Cursor as ColumnCursor>::Item>>,
        v1: Self::Get<'_>,
    ) -> bool;

    /// Apply a splice to a v0 ColumnData using borrowed `Get` values.
    fn v0_splice(
        col: &mut ColumnData<Self::V0Cursor>,
        index: usize,
        del: usize,
        values: &[Self::Get<'_>],
    );

    /// Collect borrowed `Get` values into a v0 ColumnData.
    fn v0_from_values(values: &[Self::Get<'_>]) -> ColumnData<Self::V0Cursor>;
}

// ── Mirrorable impls ────────────────────────────────────────────────────────

impl Mirrorable for Option<u64> {
    type V0Cursor = crate::UIntCursor;

    fn compare_get(v0: Option<Option<Cow<'_, u64>>>, v1: Option<Option<u64>>) -> bool {
        match (v0, v1) {
            (None, None) | (Some(None), Some(None)) => true,
            (Some(Some(c)), Some(Some(v))) => *c == v,
            _ => false,
        }
    }

    fn compare_iter(v0: &Option<Cow<'_, u64>>, v1: Option<u64>) -> bool {
        match (v0, v1) {
            (None, None) => true,
            (Some(c), Some(v)) => **c == v,
            _ => false,
        }
    }

    fn v0_splice(
        col: &mut ColumnData<Self::V0Cursor>,
        index: usize,
        del: usize,
        values: &[Option<u64>],
    ) {
        col.splice(index, del, values.iter().copied());
    }

    fn v0_from_values(values: &[Option<u64>]) -> ColumnData<Self::V0Cursor> {
        values.iter().copied().collect()
    }
}

impl Mirrorable for Option<i64> {
    type V0Cursor = crate::IntCursor;

    fn compare_get(v0: Option<Option<Cow<'_, i64>>>, v1: Option<Option<i64>>) -> bool {
        match (v0, v1) {
            (None, None) | (Some(None), Some(None)) => true,
            (Some(Some(c)), Some(Some(v))) => *c == v,
            _ => false,
        }
    }

    fn compare_iter(v0: &Option<Cow<'_, i64>>, v1: Option<i64>) -> bool {
        match (v0, v1) {
            (None, None) => true,
            (Some(c), Some(v)) => **c == v,
            _ => false,
        }
    }

    fn v0_splice(
        col: &mut ColumnData<Self::V0Cursor>,
        index: usize,
        del: usize,
        values: &[Option<i64>],
    ) {
        col.splice(index, del, values.iter().copied());
    }

    fn v0_from_values(values: &[Option<i64>]) -> ColumnData<Self::V0Cursor> {
        values.iter().copied().collect()
    }
}

impl Mirrorable for Option<String> {
    type V0Cursor = crate::StrCursor;

    fn compare_get(v0: Option<Option<Cow<'_, str>>>, v1: Option<Option<&str>>) -> bool {
        match (v0, v1) {
            (None, None) | (Some(None), Some(None)) => true,
            (Some(Some(c)), Some(Some(v))) => &*c == v,
            _ => false,
        }
    }

    fn compare_iter(v0: &Option<Cow<'_, str>>, v1: Option<&str>) -> bool {
        match (v0, v1) {
            (None, None) => true,
            (Some(c), Some(v)) => &**c == v,
            _ => false,
        }
    }

    fn v0_splice(
        col: &mut ColumnData<Self::V0Cursor>,
        index: usize,
        del: usize,
        values: &[Option<&str>],
    ) {
        col.splice(index, del, values.iter().copied());
    }

    fn v0_from_values(values: &[Option<&str>]) -> ColumnData<Self::V0Cursor> {
        values.iter().copied().collect()
    }
}

impl Mirrorable for Option<Vec<u8>> {
    type V0Cursor = crate::ByteCursor;

    fn compare_get(v0: Option<Option<Cow<'_, [u8]>>>, v1: Option<Option<&[u8]>>) -> bool {
        match (v0, v1) {
            (None, None) | (Some(None), Some(None)) => true,
            (Some(Some(c)), Some(Some(v))) => &*c == v,
            _ => false,
        }
    }

    fn compare_iter(v0: &Option<Cow<'_, [u8]>>, v1: Option<&[u8]>) -> bool {
        match (v0, v1) {
            (None, None) => true,
            (Some(c), Some(v)) => &**c == v,
            _ => false,
        }
    }

    fn v0_splice(
        col: &mut ColumnData<Self::V0Cursor>,
        index: usize,
        del: usize,
        values: &[Option<&[u8]>],
    ) {
        col.splice(index, del, values.iter().copied());
    }

    fn v0_from_values(values: &[Option<&[u8]>]) -> ColumnData<Self::V0Cursor> {
        values.iter().copied().collect()
    }
}

impl Mirrorable for bool {
    type V0Cursor = crate::BooleanCursor;

    fn compare_get(v0: Option<Option<Cow<'_, bool>>>, v1: Option<bool>) -> bool {
        match (v0, v1) {
            (None, None) => true,
            (Some(Some(c)), Some(v)) => *c == v,
            _ => false,
        }
    }

    fn compare_iter(v0: &Option<Cow<'_, bool>>, v1: bool) -> bool {
        match v0 {
            Some(c) => **c == v1,
            None => false,
        }
    }

    fn v0_splice(col: &mut ColumnData<Self::V0Cursor>, index: usize, del: usize, values: &[bool]) {
        col.splice(index, del, values.iter().copied());
    }

    fn v0_from_values(values: &[bool]) -> ColumnData<Self::V0Cursor> {
        values.iter().copied().collect()
    }
}

// ── MirroredColumn ──────────────────────────────────────────────────────────

/// A column that mirrors every operation to both v1 and v0, asserting
/// equivalence on every get, save, and iteration.
///
/// Drop-in replacement for `Column<T>` — swap to `Column<T>` when confident.
#[derive(Clone)]
pub struct MirroredColumn<T: Mirrorable> {
    v1: Column<T>,
    v0: ColumnData<T::V0Cursor>,
}

impl<T: Mirrorable> std::fmt::Debug for MirroredColumn<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirroredColumn")
            .field("v0", &self.v0)
            .field("len", &self.v1.len())
            .finish()
    }
}

impl<T: Mirrorable> Default for MirroredColumn<T> {
    fn default() -> Self {
        Self {
            v1: Column::new(),
            v0: ColumnData::new(),
        }
    }
}

impl<T: Mirrorable> MirroredColumn<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.v1.len()
    }

    pub fn is_empty(&self) -> bool {
        self.v1.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<T::Get<'_>> {
        let v1_result = self.v1.get(index);
        let v0_result = self.v0.get(index);
        assert!(
            T::compare_get(v0_result, v1_result),
            "get({index}) mismatch",
        );
        v1_result
    }

    pub fn insert(&mut self, index: usize, value: impl AsColumnRef<T>) {
        {
            let get = value.as_column_ref();
            T::v0_splice(&mut self.v0, index, 0, std::slice::from_ref(&get));
        }
        self.v1.insert(index, value);
    }

    pub fn remove(&mut self, index: usize) {
        self.v1.remove(index);
        T::v0_splice(&mut self.v0, index, 1, &[]);
    }

    pub fn splice<V: AsColumnRef<T>>(
        &mut self,
        index: usize,
        del: usize,
        values: impl IntoIterator<Item = V>,
    ) {
        let vals: Vec<V> = values.into_iter().collect();
        {
            let gets: Vec<T::Get<'_>> = vals.iter().map(|v| v.as_column_ref()).collect();
            T::v0_splice(&mut self.v0, index, del, &gets);
        }
        self.v1.splice(index, del, vals);
    }

    pub fn save(&self) -> Vec<u8> {
        let v0_bytes = self.v0.save();
        let v1_bytes = self.v1.save();
        assert_eq!(v0_bytes, v1_bytes, "v0/v1 save byte mismatch");
        v1_bytes
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let range = self.v1.save_to(out);
        let mut tmp = Vec::new();
        self.v0.save_to(&mut tmp);
        assert_eq!(&out[range.clone()], &tmp[..], "v0/v1 save_to byte mismatch");
        range
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        let v1 = Column::load(data)?;
        let v0 = ColumnData::load(data)?;
        assert_eq!(v1.len(), v0.len(), "load() len mismatch");
        Ok(Self { v1, v0 })
    }

    pub fn from_values(values: Vec<T>) -> Self {
        let v0 = {
            let gets: Vec<T::Get<'_>> = values.iter().map(|v| v.as_column_ref()).collect();
            T::v0_from_values(&gets)
        };
        let v1 = Column::from_values(values);
        Self { v1, v0 }
    }

    pub fn iter(&self) -> MirrorIter<'_, T> {
        MirrorIter {
            v1: self.v1.iter(),
            v0: self.v0.iter(),
        }
    }

    pub fn iter_range(&self, range: Range<usize>) -> MirrorIter<'_, T> {
        MirrorIter {
            v1: self.v1.iter_range(range.clone()),
            v0: self.v0.iter_range(range),
        }
    }

    pub fn slab_count(&self) -> usize {
        self.v1.slab_count()
    }

    /// Collect all values into a Vec.
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.v1.iter().collect()
    }
}

impl<T: Mirrorable + ColumnDefault> MirroredColumn<T> {
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T>) -> Result<Self, PackError> {
        let v1 = Column::load_with(data, opts)?;
        let v0 = if data.is_empty() {
            match opts.length {
                Some(0) | None => ColumnData::new(),
                Some(len) => ColumnData::init_empty(len),
            }
        } else {
            ColumnData::load(data)?
        };
        assert_eq!(v1.len(), v0.len(), "load_with() len mismatch");
        Ok(Self { v1, v0 })
    }

    pub fn is_default(&self) -> bool {
        self.v1.is_default()
    }

    pub fn save_to_unless_default(&self, out: &mut Vec<u8>) -> Range<usize> {
        if self.is_default() {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    pub fn init_default(len: usize) -> Self {
        let v1 = Column::init_default(len);
        let v0 = if len == 0 {
            ColumnData::new()
        } else {
            ColumnData::init_empty(len)
        };
        Self { v1, v0 }
    }
}

// ── MirrorIter ──────────────────────────────────────────────────────────────

/// Iterator that advances both v0 and v1 iterators in lockstep, asserting
/// value equality at every step.
pub struct MirrorIter<'a, T: Mirrorable> {
    v1: super::column::Iter<'a, T>,
    v0: crate::columndata::ColumnDataIter<'a, T::V0Cursor>,
}

impl<'a, T: Mirrorable> Iterator for MirrorIter<'a, T> {
    type Item = T::Get<'a>;

    fn next(&mut self) -> Option<T::Get<'a>> {
        let v1_val = self.v1.next();
        let v0_val = self.v0.next();
        match (v0_val, v1_val) {
            (None, None) => None,
            (Some(v0), Some(v1)) => {
                assert!(
                    T::compare_iter(&v0, v1),
                    "iter next() mismatch at pos {}",
                    self.v1.pos() - 1,
                );
                Some(v1)
            }
            (v0, v1) => panic!(
                "iter next() exhaustion mismatch at pos {}: v0={}, v1={}",
                self.v1.pos(),
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    fn nth(&mut self, n: usize) -> Option<T::Get<'a>> {
        let v1_val = self.v1.nth(n);
        let v0_val = self.v0.nth(n);
        match (v0_val, v1_val) {
            (None, None) => None,
            (Some(v0), Some(v1)) => {
                assert!(
                    T::compare_iter(&v0, v1),
                    "iter nth({n}) mismatch at pos {}",
                    self.v1.pos() - 1,
                );
                assert_eq!(self.v0.pos(), self.v1.pos(), "pos mismatch after nth({n})",);
                Some(v1)
            }
            (v0, v1) => panic!(
                "iter nth({n}) exhaustion mismatch: v0={}, v1={}",
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.v1.size_hint()
    }
}

impl<T: Mirrorable> ExactSizeIterator for MirrorIter<'_, T> {}

impl<T: Mirrorable> Clone for MirrorIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            v1: self.v1.clone(),
            v0: self.v0.clone(),
        }
    }
}

impl<'a, T: Mirrorable> MirrorIter<'a, T> {
    pub fn pos(&self) -> usize {
        let v1_pos = self.v1.pos();
        let v0_pos = self.v0.pos();
        assert_eq!(v1_pos, v0_pos, "pos() mismatch: v1={v1_pos}, v0={v0_pos}");
        v1_pos
    }

    /// Returns the next run of identical values.
    ///
    /// Delegates to the v1 iterator and advances the v0 iterator to stay
    /// in sync.
    pub fn next_run(&mut self) -> Option<super::Run<T::Get<'a>>> {
        let run = self.v1.next_run()?;
        // Advance v0 by the same count to keep in sync
        if run.count > 1 {
            self.v0.nth(run.count - 1);
        } else {
            self.v0.next();
        }
        Some(run)
    }

    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        let v1_val = self.v1.shift_next(range.clone());
        let v0_val = self.v0.shift_next(range.clone());
        match (v0_val, v1_val) {
            (None, None) => None,
            (Some(v0), Some(v1)) => {
                assert!(
                    T::compare_iter(&v0, v1),
                    "shift_next({:?}) mismatch at pos {}",
                    range,
                    self.v1.pos() - 1,
                );
                assert_eq!(
                    self.v0.pos(),
                    self.v1.pos(),
                    "pos mismatch after shift_next({:?})",
                    range,
                );
                Some(v1)
            }
            (v0, v1) => panic!(
                "shift_next({range:?}) exhaustion mismatch: v0={}, v1={}",
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    /// Captures the current iterator position for later resumption.
    pub fn suspend(&self) -> MirrorIterState<T> {
        MirrorIterState {
            v1: self.v1.suspend(),
            v0: self.v0.suspend(),
        }
    }
}

// ── MirrorIterState (suspend / resume) ──────────────────────────────────────

/// Serializable snapshot of a [`MirrorIter`] position.
pub struct MirrorIterState<T: Mirrorable> {
    v1: super::column::IterState,
    v0: crate::columndata::ColumnDataIterState<T::V0Cursor>,
}

impl<T: Mirrorable> MirrorIterState<T> {
    /// Restores the iterator position in `column`.
    ///
    /// Returns [`PackError::InvalidResume`] if `column` was mutated since suspend.
    pub fn try_resume<'a>(
        &self,
        column: &'a MirroredColumn<T>,
    ) -> Result<MirrorIter<'a, T>, PackError> {
        let v1 = self.v1.try_resume(&column.v1)?;
        let v0 = self.v0.try_resume(&column.v0)?;
        Ok(MirrorIter { v1, v0 })
    }

    /// Restores the iterator position in a [`MirroredPrefixColumn`],
    /// resuming as a value-only iterator (no prefix sums).
    pub fn try_resume_prefix<'a>(
        &self,
        column: &'a MirroredPrefixColumn<T>,
    ) -> Result<MirrorIter<'a, T>, PackError>
    where
        T: PrefixValue + PrefixToAcc,
    {
        let v1 = self.v1.try_resume(column.v1.inner())?;
        let v0 = self.v0.try_resume(&column.v0)?;
        Ok(MirrorIter { v1, v0 })
    }
}

// ── MirroredPrefixColumn ────────────────────────────────────────────────────

/// Prefix column that mirrors every operation to both v1 and v0.
///
/// Drop-in replacement for `PrefixColumn<T>` — swap when confident.
pub struct MirroredPrefixColumn<T: Mirrorable + PrefixValue + PrefixToAcc> {
    v1: PrefixColumn<T>,
    v0: ColumnData<T::V0Cursor>,
}

/// Maps a v1 `T::Prefix` to a v0 `Acc` for comparison.
pub trait PrefixToAcc: PrefixValue {
    fn prefix_to_acc(prefix: Self::Prefix) -> crate::Acc;
    fn acc_to_prefix(acc: crate::Acc) -> Self::Prefix;
}

impl PrefixToAcc for Option<u64> {
    fn prefix_to_acc(prefix: u128) -> crate::Acc {
        crate::Acc(prefix as u64)
    }
    fn acc_to_prefix(acc: crate::Acc) -> u128 {
        acc.as_u64() as u128
    }
}

impl PrefixToAcc for Option<i64> {
    fn prefix_to_acc(prefix: i128) -> crate::Acc {
        crate::Acc(prefix as u64)
    }
    fn acc_to_prefix(acc: crate::Acc) -> i128 {
        acc.as_u64() as i128
    }
}

impl PrefixToAcc for bool {
    fn prefix_to_acc(prefix: u32) -> crate::Acc {
        crate::Acc(prefix as u64)
    }
    fn acc_to_prefix(acc: crate::Acc) -> u32 {
        acc.as_u64() as u32
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc + Clone> Clone for MirroredPrefixColumn<T> {
    fn clone(&self) -> Self {
        Self {
            v1: self.v1.clone(),
            v0: self.v0.clone(),
        }
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> std::fmt::Debug for MirroredPrefixColumn<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirroredPrefixColumn")
            .field("v0", &self.v0)
            .field("len", &self.v1.len())
            .finish()
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> Default for MirroredPrefixColumn<T> {
    fn default() -> Self {
        Self {
            v1: PrefixColumn::new(),
            v0: ColumnData::new(),
        }
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> MirroredPrefixColumn<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.v1.len()
    }

    pub fn is_empty(&self) -> bool {
        self.v1.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<T::Get<'_>> {
        let v1_result = self.v1.get(index);
        let v0_result = self.v0.get(index);
        assert!(
            T::compare_get(v0_result, v1_result),
            "prefix get({index}) mismatch",
        );
        v1_result
    }

    pub fn get_prefix(&self, index: usize) -> T::Prefix {
        let v1_prefix = self.v1.get_prefix(index);
        let v0_acc = self.v0.get_acc(index);
        assert_eq!(
            v1_prefix,
            T::acc_to_prefix(v0_acc),
            "get_prefix({index}) mismatch",
        );
        v1_prefix
    }

    pub fn get_with_prefix(&self, index: usize) -> Option<(T::Get<'_>, T::Prefix)> {
        let v1_result = self.v1.get_with_prefix(index);
        let v0_value = self.v0.get(index);
        let v0_acc = self.v0.get_acc(index);
        match (v0_value, v1_result) {
            (None, None) => None,
            (Some(_), Some((v1_val, v1_prefix))) => {
                assert_eq!(
                    v1_prefix,
                    T::acc_to_prefix(v0_acc),
                    "get_with_prefix({index}) prefix mismatch",
                );
                Some((v1_val, v1_prefix))
            }
            _ => panic!("get_with_prefix({index}) existence mismatch"),
        }
    }

    pub fn get_total(&self, index: usize) -> T::Prefix {
        self.v1.get_total(index)
    }

    pub fn get_index_for_prefix(&self, target: T::Prefix) -> usize {
        self.v1.get_index_for_prefix(target)
    }

    pub fn get_index_for_total(&self, target: T::Prefix) -> usize {
        self.v1.get_index_for_total(target)
    }

    pub fn insert(&mut self, index: usize, value: impl AsColumnRef<T>) {
        {
            let get = value.as_column_ref();
            T::v0_splice(&mut self.v0, index, 0, std::slice::from_ref(&get));
        }
        self.v1.insert(index, value);
    }

    pub fn remove(&mut self, index: usize) {
        self.v1.remove(index);
        T::v0_splice(&mut self.v0, index, 1, &[]);
    }

    pub fn splice<V: AsColumnRef<T>>(
        &mut self,
        index: usize,
        del: usize,
        values: impl IntoIterator<Item = V>,
    ) {
        let vals: Vec<V> = values.into_iter().collect();
        {
            let gets: Vec<T::Get<'_>> = vals.iter().map(|v| v.as_column_ref()).collect();
            T::v0_splice(&mut self.v0, index, del, &gets);
        }
        self.v1.splice(index, del, vals);
    }

    pub fn save(&self) -> Vec<u8> {
        let v0_bytes = self.v0.save();
        let v1_bytes = self.v1.save();
        assert_eq!(v0_bytes, v1_bytes, "v0/v1 prefix save byte mismatch");
        v1_bytes
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let range = self.v1.save_to(out);
        let mut tmp = Vec::new();
        self.v0.save_to(&mut tmp);
        assert_eq!(
            &out[range.clone()],
            &tmp[..],
            "v0/v1 prefix save_to byte mismatch"
        );
        range
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        let v1 = PrefixColumn::load(data)?;
        let v0 = ColumnData::load(data)?;
        assert_eq!(v1.len(), v0.len(), "prefix load() len mismatch");
        Ok(Self { v1, v0 })
    }

    pub fn from_values(values: Vec<T>) -> Self {
        let v0 = {
            let gets: Vec<T::Get<'_>> = values.iter().map(|v| v.as_column_ref()).collect();
            T::v0_from_values(&gets)
        };
        let v1 = PrefixColumn::from_values(values);
        Self { v1, v0 }
    }

    pub fn iter(&self) -> MirrorPrefixIter<'_, T> {
        MirrorPrefixIter {
            v1: self.v1.iter(),
            v0: self.v0.iter(),
        }
    }

    pub fn iter_range(&self, range: Range<usize>) -> MirrorPrefixIter<'_, T> {
        MirrorPrefixIter {
            v1: self.v1.iter_range(range.clone()),
            v0: self.v0.iter_range(range),
        }
    }

    pub fn value_iter(&self) -> MirrorIter<'_, T> {
        MirrorIter {
            v1: self.v1.value_iter(),
            v0: self.v0.iter(),
        }
    }

    pub fn value_iter_range(&self, range: Range<usize>) -> MirrorIter<'_, T> {
        MirrorIter {
            v1: self.v1.value_iter_range(range.clone()),
            v0: self.v0.iter_range(range),
        }
    }

    /// Collect all values into a Vec (without prefix sums).
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.v1.value_iter().collect()
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc + super::ColumnDefault> MirroredPrefixColumn<T> {
    pub fn load_with(data: &[u8], opts: super::LoadOpts<T>) -> Result<Self, PackError> {
        let v1 = PrefixColumn::load_with(data, opts)?;
        let v0 = if data.is_empty() {
            match opts.length {
                Some(0) | None => ColumnData::new(),
                Some(len) => ColumnData::init_empty(len),
            }
        } else {
            ColumnData::load(data)?
        };
        assert_eq!(v1.len(), v0.len(), "prefix load_with() len mismatch");
        Ok(Self { v1, v0 })
    }

    pub fn is_default(&self) -> bool {
        self.v1.is_default()
    }

    pub fn save_to_unless_default(&self, out: &mut Vec<u8>) -> Range<usize> {
        if self.is_default() {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    pub fn init_default(len: usize) -> Self {
        let v1 = PrefixColumn::init_default(len);
        let v0 = if len == 0 {
            ColumnData::new()
        } else {
            ColumnData::init_empty(len)
        };
        Self { v1, v0 }
    }
}

// ── MirrorPrefixIter ────────────────────────────────────────────────────────

/// Iterator that advances both v0 and v1 iterators in lockstep, asserting
/// both values and prefix sums match at every step.
pub struct MirrorPrefixIter<'a, T: Mirrorable + PrefixValue + PrefixToAcc> {
    v1: PrefixIter<'a, T>,
    v0: crate::columndata::ColumnDataIter<'a, T::V0Cursor>,
}

impl<'a, T: Mirrorable + PrefixValue + PrefixToAcc> Iterator for MirrorPrefixIter<'a, T> {
    type Item = (T::Prefix, T::Get<'a>);

    fn next(&mut self) -> Option<(T::Prefix, T::Get<'a>)> {
        let v0_item = self.v0.next();
        let v1_result = self.v1.next();
        match (v0_item, v1_result) {
            (None, None) => None,
            (Some(v0_val), Some((v1_total, v1_value))) => {
                assert!(
                    T::compare_iter(&v0_val, v1_value),
                    "prefix iter next() value mismatch at pos {}",
                    self.v0.pos() - 1,
                );
                // v0 calculate_acc() = inclusive sum through consumed item
                let v0_acc = self.v0.calculate_acc();
                assert_eq!(
                    v1_total,
                    T::acc_to_prefix(v0_acc),
                    "prefix iter next() total mismatch at pos {}",
                    self.v0.pos() - 1,
                );
                Some((v1_total, v1_value))
            }
            (v0, v1) => panic!(
                "prefix iter next() exhaustion mismatch: v0={}, v1={}",
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    fn nth(&mut self, n: usize) -> Option<(T::Prefix, T::Get<'a>)> {
        let v1_result = self.v1.nth(n);
        let v0_item = self.v0.nth(n);
        match (v0_item, v1_result) {
            (None, None) => None,
            (Some(v0_val), Some((v1_total, v1_value))) => {
                assert!(
                    T::compare_iter(&v0_val, v1_value),
                    "prefix iter nth({n}) value mismatch at pos {}",
                    self.v1.pos() - 1,
                );
                let v0_acc = self.v0.calculate_acc();
                assert_eq!(
                    v1_total,
                    T::acc_to_prefix(v0_acc),
                    "prefix iter nth({n}) total mismatch",
                );
                Some((v1_total, v1_value))
            }
            (v0, v1) => panic!(
                "prefix iter nth({n}) exhaustion mismatch: v0={}, v1={}",
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.v1.size_hint()
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> ExactSizeIterator for MirrorPrefixIter<'_, T> {}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> Clone for MirrorPrefixIter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            v1: self.v1.clone(),
            v0: self.v0.clone(),
        }
    }
}

impl<T: Mirrorable + PrefixValue + PrefixToAcc> std::fmt::Debug for MirrorPrefixIter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorPrefixIter")
            .field("pos", &self.v1.pos())
            .finish()
    }
}

impl<'a, T: Mirrorable + PrefixValue + PrefixToAcc> MirrorPrefixIter<'a, T> {
    pub fn pos(&self) -> usize {
        self.v1.pos()
    }

    /// Returns the next run of identical values with prefix sums.
    ///
    /// Delegates to the v1 prefix iterator and advances v0 to stay in sync.
    pub fn next_run(&mut self) -> Option<super::Run<(T::Prefix, T::Get<'a>)>> {
        let run = self.v1.next_run()?;
        // Advance v0 by the same count to keep in sync
        if run.count > 1 {
            self.v0.nth(run.count - 1);
        } else {
            self.v0.next();
        }
        Some(run)
    }

    pub fn shift_next(&mut self, range: Range<usize>) -> Option<(T::Prefix, T::Get<'a>)> {
        let v1_result = self.v1.shift_next(range.clone());
        let v0_item = self.v0.shift_next(range.clone());
        match (v0_item, v1_result) {
            (None, None) => None,
            (Some(v0_val), Some((v1_total, v1_value))) => {
                assert!(
                    T::compare_iter(&v0_val, v1_value),
                    "prefix shift_next({:?}) value mismatch",
                    range,
                );
                let v0_acc = self.v0.calculate_acc();
                assert_eq!(
                    v1_total,
                    T::acc_to_prefix(v0_acc),
                    "prefix shift_next({:?}) total mismatch",
                    range,
                );
                Some((v1_total, v1_value))
            }
            (v0, v1) => panic!(
                "prefix shift_next({range:?}) exhaustion mismatch: v0={}, v1={}",
                v0.is_some(),
                v1.is_some(),
            ),
        }
    }

    pub fn advance_total(&mut self, val: T::Prefix) -> Option<(T::Prefix, T::Get<'a>)> {
        let v1_result = self.v1.advance_total(val);
        match v1_result {
            None => {
                // Exhaust v0 to stay in sync
                while self.v0.next().is_some() {}
                None
            }
            Some((v1_total, v1_value)) => {
                // Sync v0 iterator to the same position as v1.
                let item_index = self.v1.pos() - 1;
                let skip = item_index - self.v0.pos();
                let v0_val = self.v0.nth(skip);
                match v0_val {
                    Some(v0) => {
                        assert!(
                            T::compare_iter(&v0, v1_value),
                            "advance_total({val:?}) value mismatch at pos {item_index}",
                        );
                        let v0_acc = self.v0.calculate_acc();
                        assert_eq!(
                            v1_total,
                            T::acc_to_prefix(v0_acc),
                            "advance_total({val:?}) total mismatch at pos {item_index}",
                        );
                    }
                    None => panic!(
                        "advance_total({val:?}): v0 exhausted at pos {} but v1 returned item at {item_index}",
                        self.v0.pos(),
                    ),
                }
                Some((v1_total, v1_value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirrored_column_basic_operations() {
        let mut col = MirroredColumn::<Option<u64>>::new();
        col.insert(0, Some(10));
        col.insert(1, Some(20));
        col.insert(2, None);
        col.insert(1, Some(15));

        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), Some(Some(10)));
        assert_eq!(col.get(1), Some(Some(15)));
        assert_eq!(col.get(2), Some(Some(20)));
        assert_eq!(col.get(3), Some(None));

        col.remove(1);
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Some(Some(10)));
        assert_eq!(col.get(1), Some(Some(20)));
    }

    #[test]
    fn mirrored_column_splice() {
        let mut col = MirroredColumn::<Option<u64>>::new();
        col.splice(0, 0, vec![Some(1), Some(2), Some(3)]);
        assert_eq!(col.len(), 3);

        col.splice(1, 1, vec![Some(10), Some(20)]);
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(0), Some(Some(1)));
        assert_eq!(col.get(1), Some(Some(10)));
        assert_eq!(col.get(2), Some(Some(20)));
        assert_eq!(col.get(3), Some(Some(3)));
    }

    #[test]
    fn mirrored_column_save_load_roundtrip() {
        let mut col = MirroredColumn::<Option<u64>>::new();
        col.splice(0, 0, vec![Some(1), None, Some(3), Some(3), Some(3)]);

        let bytes = col.save();
        let loaded = MirroredColumn::<Option<u64>>::load(&bytes).unwrap();
        assert_eq!(loaded.len(), 5);
        for i in 0..5 {
            assert_eq!(loaded.get(i), col.get(i));
        }
    }

    #[test]
    fn mirrored_column_from_values() {
        let col = MirroredColumn::<Option<u64>>::from_values(vec![Some(1), Some(2), None, Some(4)]);
        assert_eq!(col.len(), 4);
        assert_eq!(col.get(2), Some(None));
    }

    #[test]
    fn mirrored_column_bool() {
        let mut col = MirroredColumn::<bool>::new();
        col.insert(0, true);
        col.insert(1, false);
        col.insert(2, true);
        assert_eq!(col.get(0), Some(true));
        assert_eq!(col.get(1), Some(false));
        assert_eq!(col.get(2), Some(true));
    }

    #[test]
    fn mirrored_column_string() {
        let mut col = MirroredColumn::<Option<String>>::new();
        col.insert(0, Some("hello".to_string()));
        col.insert(1, Option::<String>::None);
        col.insert(2, Some("world".to_string()));
        assert_eq!(col.get(0), Some(Some("hello")));
        assert_eq!(col.get(1), Some(None));
        assert_eq!(col.get(2), Some(Some("world")));
    }

    #[test]
    fn mirror_iter_basic() {
        let col = MirroredColumn::<Option<u64>>::from_values(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]);
        let vals: Vec<_> = col.iter().collect();
        assert_eq!(vals, vec![Some(1), Some(2), None, Some(4), Some(5)]);
    }

    #[test]
    fn mirror_iter_nth() {
        let col = MirroredColumn::<Option<u64>>::from_values(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]);
        let mut iter = col.iter();
        assert_eq!(iter.nth(2), Some(Some(3)));
        assert_eq!(iter.pos(), 3);
        assert_eq!(iter.next(), Some(Some(4)));
    }

    #[test]
    fn mirror_iter_shift_next() {
        let col = MirroredColumn::<Option<u64>>::from_values(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(50),
        ]);
        let mut iter = col.iter();
        assert_eq!(iter.shift_next(2..5), Some(Some(30)));
        assert_eq!(iter.pos(), 3);
        assert_eq!(iter.next(), Some(Some(40)));
        assert_eq!(iter.next(), Some(Some(50)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn mirror_iter_range() {
        let col = MirroredColumn::<Option<u64>>::from_values(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(50),
        ]);
        let vals: Vec<_> = col.iter_range(1..4).collect();
        assert_eq!(vals, vec![Some(20), Some(30), Some(40)]);
    }

    #[test]
    fn mirrored_prefix_column_basic() {
        let mut col = MirroredPrefixColumn::<Option<u64>>::new();
        col.insert(0, Some(10));
        col.insert(1, Some(20));
        col.insert(2, Some(30));

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Some(Some(10)));
        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 10);
        assert_eq!(col.get_prefix(2), 30);
        assert_eq!(col.get_prefix(3), 60);
    }

    #[test]
    fn mirrored_prefix_column_bool() {
        let mut col = MirroredPrefixColumn::<bool>::new();
        col.insert(0, true);
        col.insert(1, false);
        col.insert(2, true);
        col.insert(3, true);

        assert_eq!(col.get_prefix(0), 0);
        assert_eq!(col.get_prefix(1), 1);
        assert_eq!(col.get_prefix(2), 1);
        assert_eq!(col.get_prefix(3), 2);
        assert_eq!(col.get_prefix(4), 3);
    }

    #[test]
    fn mirror_prefix_iter_basic() {
        let col = MirroredPrefixColumn::<Option<u64>>::from_values(vec![
            Some(10),
            Some(20),
            None,
            Some(30),
        ]);
        let items: Vec<_> = col.iter().collect();
        // v1 PrefixIter yields inclusive prefix (through current item)
        assert_eq!(items[0], (10, Some(10)));
        assert_eq!(items[1], (30, Some(20)));
        assert_eq!(items[2], (30, None));
        assert_eq!(items[3], (60, Some(30)));
    }

    #[test]
    fn mirror_prefix_iter_shift_next() {
        let col = MirroredPrefixColumn::<Option<u64>>::from_values(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(50),
        ]);
        let mut iter = col.iter();
        let item = iter.shift_next(2..5).unwrap();
        // inclusive prefix through item 2 = 10+20+30 = 60
        assert_eq!(item, (60, Some(30)));
        assert_eq!(iter.pos(), 3);
    }

    #[test]
    fn mirror_prefix_iter_advance_total() {
        let col = MirroredPrefixColumn::<bool>::from_values(vec![
            true, false, false, true, true, false, true,
        ]);
        let mut iter = col.iter();
        // inclusive prefix: [1, 1, 1, 2, 3, 3, 4]
        let item = iter.advance_total(1).unwrap();
        assert_eq!(item, (1, true));
        let item = iter.advance_total(1).unwrap();
        assert_eq!(item, (2, true));
    }

    #[test]
    fn mirrored_column_large_fuzz() {
        use rand::{Rng, SeedableRng};
        let mut r = rand::rngs::StdRng::seed_from_u64(42);
        let mut col = MirroredColumn::<Option<u64>>::new();

        for _ in 0..200 {
            let len = col.len();
            match r.random_range(0..4) {
                0 if len < 500 => {
                    let idx = r.random_range(0..=len);
                    let val = if r.random_bool(0.8) {
                        Some(r.random_range(0..100))
                    } else {
                        None
                    };
                    col.insert(idx, val);
                }
                1 if len > 2 => {
                    // Splice: replace 1-2 items with 1-3 new items
                    let idx = r.random_range(0..len - 1);
                    let del = r.random_range(1..=2.min(len - idx));
                    let new_count = r.random_range(1..=3);
                    let vals: Vec<_> = (0..new_count)
                        .map(|_| {
                            if r.random_bool(0.8) {
                                Some(r.random_range(0..100))
                            } else {
                                None
                            }
                        })
                        .collect();
                    col.splice(idx, del, vals);
                }
                _ if len > 0 => {
                    let idx = r.random_range(0..len);
                    col.get(idx);
                }
                _ => {
                    col.insert(0, Some(r.random_range(0..100)));
                }
            }
        }

        // Final full iteration check
        let _: Vec<_> = col.iter().collect();
    }
}
