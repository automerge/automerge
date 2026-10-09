//! Lazy actor renumbering: inserting an actor mid-list would renumber
//! every actor index in the op columns, an O(document) re-encode. Instead
//! the columns keep their stored codes and an [`ActorMap`] translates
//! stored <-> logical (sorted) indexes. A bijection preserves run equality,
//! so slabs are untouched.

use crate::op_set2::types::ActorIdx;
use std::fmt::Debug;
use std::sync::Arc;

/// The stored <-> logical actor-index bijection, shared by the actor
/// columns of an op set.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ActorMap {
    /// empty while the map is identity
    to_logical: Vec<u32>,
    /// empty while the map is identity
    to_stored: Vec<u32>,
    /// bumped on every non-append change; not unique across independent maps
    version: u64,
}

impl ActorMap {
    pub(crate) fn identity() -> Arc<Self> {
        Arc::new(ActorMap::default())
    }

    pub(crate) fn is_identity(&self) -> bool {
        self.to_logical.is_empty()
    }

    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    #[inline]
    pub(crate) fn logical(&self, s: ActorIdx) -> ActorIdx {
        if self.to_logical.is_empty() {
            s
        } else {
            ActorIdx(self.to_logical[s.0 as usize])
        }
    }

    #[inline]
    pub(crate) fn stored(&self, l: ActorIdx) -> ActorIdx {
        if self.to_stored.is_empty() {
            l
        } else {
            ActorIdx(self.to_stored[l.0 as usize])
        }
    }

    /// The map after inserting a new actor at sorted position `logical`,
    /// with `len` actors existing before the insert. O(actors).
    pub(crate) fn insert(&self, logical: usize, len: usize) -> Arc<Self> {
        if self.is_identity() && logical == len {
            return Arc::new(self.clone());
        }
        let mut to_logical = if self.to_logical.is_empty() {
            (0..len as u32).collect::<Vec<_>>()
        } else {
            self.to_logical.clone()
        };
        let mut to_stored = if self.to_stored.is_empty() {
            (0..len as u32).collect::<Vec<_>>()
        } else {
            self.to_stored.clone()
        };
        debug_assert_eq!(to_logical.len(), len);
        for l in to_logical.iter_mut() {
            if *l >= logical as u32 {
                *l += 1;
            }
        }
        to_logical.push(logical as u32);
        to_stored.insert(logical, len as u32);
        Arc::new(ActorMap {
            to_logical,
            to_stored,
            version: self.version + 1,
        })
    }
}

/// Value shapes carrying an actor index the map applies to.
pub(crate) trait MapActor: Copy + PartialEq + Debug {
    fn map_actor(self, f: impl Fn(ActorIdx) -> ActorIdx) -> Self;
}

impl MapActor for ActorIdx {
    #[inline]
    fn map_actor(self, f: impl Fn(ActorIdx) -> ActorIdx) -> Self {
        f(self)
    }
}

impl MapActor for Option<ActorIdx> {
    #[inline]
    fn map_actor(self, f: impl Fn(ActorIdx) -> ActorIdx) -> Self {
        self.map(f)
    }
}

/// An actor column whose reads and writes are in logical space.
#[derive(Debug, Clone, Default)]
pub(crate) struct MappedColumn<T>
where
    T: MapActor + hexane::ColumnValueRef,
{
    col: hexane::Column<T>,
    map: Arc<ActorMap>,
}

impl<T> MappedColumn<T>
where
    T: MapActor + Default + hexane::ColumnValueRef,
{
    pub(crate) fn new() -> Self {
        MappedColumn {
            col: hexane::Column::new(),
            map: ActorMap::identity(),
        }
    }

    /// Wrap a column whose values are already logical.
    pub(crate) fn identity(col: hexane::Column<T>) -> Self {
        MappedColumn {
            col,
            map: ActorMap::identity(),
        }
    }

    pub(crate) fn load(data: &[u8]) -> Result<Self, hexane::PackError> {
        Ok(Self::identity(hexane::Column::load(data)?))
    }

    pub(crate) fn load_with<'a, F>(
        data: &'a [u8],
        opts: hexane::LoadOpts<F>,
    ) -> Result<Self, hexane::PackError>
    where
        F: hexane::MaybeFill<T::Get<'a>>,
    {
        Ok(Self::identity(hexane::Column::load_with(data, opts)?))
    }

    pub(crate) fn actor_map(&self) -> &Arc<ActorMap> {
        &self.map
    }

    /// The raw column, for a cursor edit.
    ///
    /// Panics unless the map is identity.
    pub(crate) fn identity_mut(&mut self) -> &mut hexane::Column<T> {
        assert!(self.map.is_identity(), "cursor edit on a mapped column");
        &mut self.col
    }

    /// O(1): stored codes are unchanged, only their interpretation shifts.
    pub(crate) fn set_map(&mut self, map: Arc<ActorMap>) {
        self.map = map;
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.col.len()
    }
}

impl<T> MappedColumn<T>
where
    T: MapActor + Default + Ord,
    T: for<'a> hexane::ColumnValueRef<Get<'a> = T>,
    T: hexane::AsColumnRef<T>,
{
    /// Build from logical values, adopting `map`.
    pub(crate) fn from_logical_values(vals: Vec<T>, map: Arc<ActorMap>) -> Self {
        let col = if map.is_identity() {
            hexane::Column::from_values(vals)
        } else {
            let m = map.clone();
            hexane::Column::from_values(
                vals.into_iter()
                    .map(|v| v.map_actor(|a| m.stored(a)))
                    .collect::<Vec<_>>(),
            )
        };
        MappedColumn { col, map }
    }

    #[inline]
    pub(crate) fn get(&self, pos: usize) -> Option<T> {
        self.col.get(pos).map(|v| self.log_val(v))
    }

    #[inline]
    pub(crate) fn log_val(&self, v: T) -> T {
        if self.map.is_identity() {
            v
        } else {
            let map = &self.map;
            v.map_actor(|a| map.logical(a))
        }
    }

    #[inline]
    pub(crate) fn iter(&self) -> MappedIter<'_, T> {
        MappedIter {
            iter: self.col.iter(),
            map: self.map.clone(),
        }
    }

    /// Yields stored values: map them through [`Self::log_val`].
    pub(crate) fn stored_iter(&self) -> hexane::Iter<'_, T> {
        self.col.iter()
    }

    #[inline]
    pub(crate) fn iter_range(&self, range: std::ops::Range<usize>) -> MappedIter<'_, T> {
        MappedIter {
            iter: self.col.iter_range(range),
            map: self.map.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn to_vec(&self) -> Vec<T> {
        self.iter().collect()
    }

    pub(crate) fn splice<I>(&mut self, index: usize, del: usize, values: I)
    where
        I: IntoIterator<Item = T>,
        T::Encoding<hexane::Leb128>: hexane::edit::SlabEdit<Value = T>,
    {
        if self.map.is_identity() {
            self.col.splice(index, del, values);
        } else {
            let map = self.map.clone();
            self.col.splice(
                index,
                del,
                values
                    .into_iter()
                    .map(move |v| v.map_actor(|a| map.stored(a))),
            );
        }
    }

    /// Multi-point copy. Adopts `src`'s slabs when both maps agree;
    /// otherwise translates value by value, which is much slower.
    pub(crate) fn copy_ranges<I>(&mut self, src: MappedColumn<T>, splices: I)
    where
        I: IntoIterator<Item = hexane::Splice>,
        T::Encoding<hexane::Leb128>: hexane::edit::SlabEdit<Value = T>,
    {
        if Arc::ptr_eq(&self.map, &src.map) || self.map == src.map {
            self.col.copy_ranges(src.col, splices);
        } else {
            let mut shift = 0isize;
            for sp in splices {
                let at = (sp.pos as isize + shift) as usize;
                let vals: Vec<T> = src
                    .iter_range(sp.range.clone())
                    .map(|v| {
                        if self.map.is_identity() {
                            v
                        } else {
                            v.map_actor(|a| self.map.stored(a))
                        }
                    })
                    .collect();
                shift += vals.len() as isize - sp.delete as isize;
                self.col.splice(at, sp.delete, vals);
            }
        }
    }

    /// Map every logical value through `f` and adopt `target`'s map.
    pub(crate) fn remap_into<F>(&mut self, f: &F, target: Arc<ActorMap>)
    where
        F: Fn(ActorIdx) -> ActorIdx,
    {
        let map = self.map.clone();
        self.col
            .remap(|v: T| v.map_actor(|a| target.stored(f(map.logical(a)))));
        self.map = target;
    }

    /// Map every logical value through `f`, keeping the current map.
    pub(crate) fn remap<F>(&mut self, f: &F)
    where
        F: Fn(ActorIdx) -> ActorIdx,
    {
        let target = self.map.clone();
        self.remap_into(f, target);
    }

    fn canonical(&self) -> hexane::Column<T> {
        let mut c = self.col.clone();
        if !self.map.is_identity() {
            let map = self.map.clone();
            c.remap(move |v: T| v.map_actor(|a| map.logical(a)));
        }
        c
    }

    /// Rewrite stored codes to logical and drop the map. O(column) unless
    /// the map is already identity.
    pub(crate) fn flush(&mut self) {
        if !self.map.is_identity() {
            let map = self.map.clone();
            self.col.remap(move |v: T| v.map_actor(|a| map.logical(a)));
        }
        self.map = ActorMap::identity();
    }

    pub(crate) fn try_resume(
        &self,
        state: &MappedIterState,
    ) -> Result<MappedIter<'_, T>, hexane::PackError> {
        if state.version != self.map.version() {
            return Err(hexane::PackError::InvalidResume);
        }
        Ok(MappedIter {
            iter: state.state.try_resume(&self.col)?,
            map: self.map.clone(),
        })
    }

    pub(crate) fn save(&self) -> Vec<u8> {
        self.canonical().save()
    }

    pub(crate) fn save_to(&self, out: &mut Vec<u8>) -> std::ops::Range<usize> {
        self.canonical().save_to(out)
    }

    pub(crate) fn save_to_unless(&self, out: &mut Vec<u8>, unless: T) -> std::ops::Range<usize> {
        self.canonical().save_to_unless(out, unless)
    }

    /// Narrow `range`, sorted by logical value, to the rows holding
    /// `value`. O(log n) for an identity map, O(runs in range) otherwise.
    pub(crate) fn scope_to_value(
        &self,
        value: T,
        range: impl std::ops::RangeBounds<usize>,
    ) -> std::ops::Range<usize> {
        if self.map.is_identity() {
            return self.col.scope_to_value(value, range);
        }
        let start = match range.start_bound() {
            std::ops::Bound::Unbounded => 0,
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Unbounded => self.col.len(),
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
        };
        let mut it = self.col.iter_range(start..end);
        let mut pos = start;
        let mut found: Option<std::ops::Range<usize>> = None;
        while let Some(run) = it.next_run() {
            match run.value.map_actor(|a| self.map.logical(a)).cmp(&value) {
                std::cmp::Ordering::Less => pos += run.count,
                std::cmp::Ordering::Equal => {
                    let f = found.get_or_insert(pos..pos);
                    pos += run.count;
                    f.end = pos;
                }
                std::cmp::Ordering::Greater => break,
            }
        }
        found.unwrap_or(pos..pos)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MappedIter<'a, T>
where
    T: MapActor + hexane::ColumnValueRef,
{
    iter: hexane::Iter<'a, T>,
    map: Arc<ActorMap>,
}

impl<'a, T> MappedIter<'a, T>
where
    T: MapActor + Default + Ord,
    T: for<'x> hexane::ColumnValueRef<Get<'x> = T>,
    T: hexane::AsColumnRef<T>,
{
    #[inline]
    pub(crate) fn log_val(&self, v: T) -> T {
        if self.map.is_identity() {
            v
        } else {
            let map = &self.map;
            v.map_actor(|a| map.logical(a))
        }
    }

    #[inline]
    pub(crate) fn pos(&self) -> usize {
        self.iter.pos()
    }

    #[inline]
    pub(crate) fn end_pos(&self) -> usize {
        self.iter.end_pos()
    }

    #[inline]
    pub(crate) fn set_max(&mut self, pos: usize) {
        self.iter.set_max(pos)
    }

    #[inline]
    pub(crate) fn advance_to(&mut self, target: usize) {
        self.iter.advance_to(target)
    }

    #[inline]
    pub(crate) fn advance_by(&mut self, amount: usize) {
        self.iter.advance_by(amount)
    }

    #[inline]
    pub(crate) fn shift_next(&mut self, range: std::ops::Range<usize>) -> Option<T> {
        self.iter.set_max(range.end);
        let n = range.start - self.iter.pos();
        Iterator::nth(self, n)
    }

    #[inline]
    pub(crate) fn scan_to_pos(&mut self, pos: usize) -> Option<T> {
        let n = pos - self.iter.pos();
        Iterator::nth(self, n)
    }

    #[inline]
    pub(crate) fn next_run(&mut self) -> Option<hexane::Run<T>> {
        let run = self.iter.next_run()?;
        Some(hexane::Run {
            count: run.count,
            value: self.log_val(run.value),
        })
    }

    /// The sub-range of `range` holding `target`, or an empty range at the
    /// insertion point on a miss; leaves the iterator at its start.
    /// O(log n) for an identity map, O(runs in range) otherwise.
    pub(crate) fn seek_to_value(
        &mut self,
        target: T,
        range: impl std::ops::RangeBounds<usize>,
    ) -> std::ops::Range<usize> {
        if self.map.is_identity() {
            return self.iter.seek_to_value(target, range);
        }
        let start = match range.start_bound() {
            std::ops::Bound::Unbounded => 0,
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Unbounded => self.iter.end_pos(),
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
        };
        if start > self.iter.pos() {
            self.iter.advance_to(start);
        }
        let mut checkpoint = self.iter.clone();
        checkpoint.set_max(end);
        let mut pos = checkpoint.pos();
        let mut found: Option<std::ops::Range<usize>> = None;
        while let Some(run) = checkpoint.next_run() {
            match run.value.map_actor(|a| self.map.logical(a)).cmp(&target) {
                std::cmp::Ordering::Less => pos += run.count,
                std::cmp::Ordering::Equal => {
                    let f = found.get_or_insert(pos..pos);
                    pos += run.count;
                    f.end = pos;
                }
                std::cmp::Ordering::Greater => break,
            }
        }
        let range = found.unwrap_or(pos..pos);
        self.iter.advance_to(range.start);
        range
    }
}

pub(crate) struct MappedIterState {
    state: hexane::column::IterState,
    version: u64,
}

impl<'a, T> MappedIter<'a, T>
where
    T: MapActor + Default + Ord,
    T: for<'x> hexane::ColumnValueRef<Get<'x> = T>,
    T: hexane::AsColumnRef<T>,
{
    #[cfg(test)]
    pub(crate) fn raw(iter: hexane::Iter<'a, T>) -> Self {
        MappedIter {
            iter,
            map: ActorMap::identity(),
        }
    }

    pub(crate) fn suspend(&self) -> MappedIterState {
        MappedIterState {
            state: self.iter.suspend(),
            version: self.map.version(),
        }
    }
}

impl<T> Default for MappedIter<'_, T>
where
    T: MapActor + hexane::ColumnValueRef,
{
    fn default() -> Self {
        MappedIter {
            iter: Default::default(),
            map: ActorMap::identity(),
        }
    }
}

impl<T> Iterator for MappedIter<'_, T>
where
    T: MapActor + Default + Ord,
    T: for<'x> hexane::ColumnValueRef<Get<'x> = T>,
    T: hexane::AsColumnRef<T>,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        let v = self.iter.next()?;
        Some(self.log_val(v))
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<T> {
        let v = self.iter.nth(n)?;
        Some(self.log_val(v))
    }
}

impl<T> hexane::Shiftable for MappedIter<'_, T>
where
    T: MapActor + Default + Ord + Debug,
    T: for<'x> hexane::ColumnValueRef<Get<'x> = T>,
    T: hexane::AsColumnRef<T>,
{
    #[inline]
    fn get_pos(&self) -> usize {
        self.iter.get_pos()
    }

    #[inline]
    fn get_max(&self) -> usize {
        self.iter.get_max()
    }

    fn set_max(&mut self, pos: usize) {
        self.iter.set_max(pos)
    }
}

impl<'a, T> hexane::RunSrc<'a, T> for MappedIter<'a, T>
where
    T: MapActor + Default + Ord + Debug,
    T: for<'x> hexane::ColumnValueRef<Get<'x> = T>,
    T: hexane::AsColumnRef<T>,
{
    fn try_next_run(&mut self) -> Result<Option<hexane::Run<T>>, hexane::PackError> {
        Ok(self.next_run())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_appends_stay_identity() {
        let m = ActorMap::identity();
        let m = m.insert(0, 0);
        let m = m.insert(1, 1);
        assert!(m.is_identity());
        assert_eq!(m.logical(ActorIdx(1)), ActorIdx(1));
    }

    #[test]
    fn mid_insert_shifts() {
        let m = ActorMap::identity();
        let m = m.insert(0, 0); // actor A -> stored 0, logical 0
        let m = m.insert(1, 1); // actor C -> stored 1, logical 1
        let m = m.insert(1, 2); // actor B between them -> stored 2, logical 1
        assert!(!m.is_identity());
        assert_eq!(m.logical(ActorIdx(0)), ActorIdx(0)); // A
        assert_eq!(m.logical(ActorIdx(1)), ActorIdx(2)); // C shifted
        assert_eq!(m.logical(ActorIdx(2)), ActorIdx(1)); // B fresh
        for l in 0..3u32 {
            assert_eq!(m.logical(m.stored(ActorIdx(l))), ActorIdx(l));
        }
    }

    #[test]
    fn same_version_maps_are_not_interchangeable() {
        let a = ActorMap::identity().insert(0, 2);
        let b = ActorMap::identity().insert(1, 2);
        assert_eq!(a.version(), b.version(), "versions collide");
        assert!(!Arc::ptr_eq(&a, &b));
        assert_ne!(a.logical(ActorIdx(0)), b.logical(ActorIdx(0)));
    }

    #[test]
    fn copy_ranges_translates_across_distinct_maps() {
        let dst_map = ActorMap::identity().insert(0, 2);
        let src_map = ActorMap::identity().insert(1, 2);
        assert_eq!(dst_map.version(), src_map.version());

        let logical = |v: &[u32]| -> Vec<ActorIdx> { v.iter().map(|&x| ActorIdx(x)).collect() };

        let mut dst = MappedColumn::from_logical_values(logical(&[0, 1]), dst_map.clone());
        let src = MappedColumn::from_logical_values(logical(&[2, 1, 0]), src_map);

        dst.copy_ranges(
            src,
            [hexane::Splice {
                pos: 2,
                delete: 0,
                range: 1..3,
            }],
        );

        assert_eq!(dst.to_vec(), logical(&[0, 1, 1, 0]));
        assert!(Arc::ptr_eq(dst.actor_map(), &dst_map), "map unchanged");
    }

    #[test]
    fn mapped_column_round_trip() {
        // two actors (stored 0, 1), then a third sorts between them
        let raw: Vec<ActorIdx> = [0u32, 0, 1, 1, 0].iter().map(|&v| ActorIdx(v)).collect();
        let mut col = MappedColumn::identity(hexane::Column::from_values(raw));
        let map = col.actor_map().insert(1, 2); // logical: 0->0, 1->2, new=1
        col.set_map(map);

        let logical: Vec<ActorIdx> = [0u32, 0, 2, 2, 0].iter().map(|&v| ActorIdx(v)).collect();
        assert_eq!(col.to_vec(), logical);
        assert_eq!(col.get(2), Some(ActorIdx(2)));
        assert_eq!(col.iter().collect::<Vec<_>>(), logical);

        col.splice(5, 0, [ActorIdx(1)]);
        assert_eq!(col.get(5), Some(ActorIdx(1)));

        let runs: Vec<_> = {
            let mut it = col.iter();
            std::iter::from_fn(move || it.next_run()).collect()
        };
        let flat: Vec<ActorIdx> = runs
            .iter()
            .flat_map(|r| std::iter::repeat_n(r.value, r.count))
            .collect();
        assert_eq!(flat, col.to_vec());

        assert_eq!(col.scope_to_value(ActorIdx(2), 2..4), 2..4);
        assert_eq!(col.scope_to_value(ActorIdx(1), 2..4), 2..2);

        let plain = hexane::Column::<ActorIdx>::from_values(col.to_vec());
        assert_eq!(col.save(), plain.save());

        let before = col.to_vec();
        col.flush();
        assert!(col.actor_map().is_identity());
        assert_eq!(col.to_vec(), before);
    }
}
