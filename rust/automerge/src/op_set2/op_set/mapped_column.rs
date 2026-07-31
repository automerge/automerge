//! Lazy actor renumbering.
//!
//! The op columns compare actors by index, which requires indexes to
//! order consistently with the (sorted) actor list. Inserting an actor
//! mid-list renumbers every index at or after it — historically an
//! O(document) re-encode of all four actor columns per new actor.
//!
//! [`ActorMap`] defers that: the columns keep their existing *stored*
//! codes (a new actor takes a fresh appended code), and an O(#actors)
//! side table maps stored ↔ *logical* (the true sorted index). This is
//! sound because the actor columns are RLE — a bijection on values
//! preserves equality, so runs, slabs and tree structure are all
//! remap-invariant; only the value interpretation shifts.
//!
//! [`MappedColumn`] / [`MappedIter`] wrap the raw hexane column and
//! iterator behind the same surface, translating at the boundary:
//! reads come out logical, writes go in logical, equality searches
//! translate the target once, order searches walk runs translating per
//! run (their windows are counter-narrowed and tiny). After a load the
//! map is identity (the wire is canonical) and everything delegates
//! straight through; only documents with deferred inserts pay the
//! translation.

use crate::op_set2::types::ActorIdx;
use std::fmt::Debug;
use std::sync::Arc;

/// The stored ↔ logical actor-index bijection, shared (via `Arc`) by
/// the four actor columns of an op set. Copy-on-write: an actor
/// insert builds the successor map and the op set swaps the `Arc`s.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ActorMap {
    /// stored code -> logical index; empty while the map is identity
    to_logical: Vec<u32>,
    /// logical index -> stored code; empty while the map is identity
    to_stored: Vec<u32>,
    /// bumped on every non-append change — [`MappedColumn::copy_ranges`]
    /// requires equal versions before adopting slabs
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

    /// stored -> logical
    #[inline]
    pub(crate) fn log(&self, s: ActorIdx) -> ActorIdx {
        if self.to_logical.is_empty() {
            s
        } else {
            ActorIdx(self.to_logical[s.0 as usize])
        }
    }

    /// logical -> stored
    #[inline]
    pub(crate) fn sto(&self, l: ActorIdx) -> ActorIdx {
        if self.to_stored.is_empty() {
            l
        } else {
            ActorIdx(self.to_stored[l.0 as usize])
        }
    }

    /// The successor map after inserting a new actor at sorted position
    /// `logical`, with `len` actors existing before the insert. The new
    /// actor takes a fresh appended stored code; every logical index at
    /// or after the insertion point shifts up. Appending (`logical ==
    /// len`) onto an identity map stays identity.
    pub(crate) fn insert(&self, logical: usize, len: usize) -> Arc<Self> {
        if self.is_identity() && logical == len {
            // appended actors keep stored == logical
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
        // fresh stored code for the new actor
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

/// An actor column in *stored* space presenting a logical-space
/// surface, duck-typed to the subset of the raw [`hexane::Column`] API
/// the op columns use.
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

    /// Wrap a column whose values are already canonical (identity map)
    /// — the load paths.
    pub(crate) fn identity(col: hexane::Column<T>) -> Self {
        MappedColumn {
            col,
            map: ActorMap::identity(),
        }
    }

    /// [`hexane::Column::load`], wrapped with the identity map (the
    /// wire is canonical).
    pub(crate) fn load(data: &[u8]) -> Result<Self, hexane::PackError> {
        Ok(Self::identity(hexane::Column::load(data)?))
    }

    /// [`hexane::Column::load_with`], wrapped with the identity map.
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

    /// The column itself, for a cursor edit. Identity-mapped only: a
    /// value written through the cursor goes in verbatim, which is only
    /// right where stored and logical space are the same — the fragment
    /// load paths, which are the only editors.
    pub(crate) fn identity_mut(&mut self) -> &mut hexane::Column<T> {
        assert!(self.map.is_identity(), "cursor edit on a mapped column");
        &mut self.col
    }

    /// Install the successor map after an actor insert — O(1), no slab
    /// access. Stored codes are unchanged; their interpretation shifts.
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
    /// Build from logical-space values, adopting `map` (translating
    /// each value to stored space on the way in).
    pub(crate) fn from_logical_values(vals: Vec<T>, map: Arc<ActorMap>) -> Self {
        let col = if map.is_identity() {
            hexane::Column::from_values(vals)
        } else {
            let m = map.clone();
            hexane::Column::from_values(
                vals.into_iter()
                    .map(|v| v.map_actor(|a| m.sto(a)))
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
    fn log_val(&self, v: T) -> T {
        if self.map.is_identity() {
            v
        } else {
            let map = &self.map;
            v.map_actor(|a| map.log(a))
        }
    }

    #[inline]
    pub(crate) fn iter(&self) -> MappedIter<'_, T> {
        MappedIter {
            iter: self.col.iter(),
            map: self.map.clone(),
        }
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
    {
        if self.map.is_identity() {
            self.col.splice(index, del, values);
        } else {
            let map = self.map.clone();
            self.col.splice(
                index,
                del,
                values.into_iter().map(move |v| v.map_actor(|a| map.sto(a))),
            );
        }
    }

    /// Multi-point copy. Adopting the source's slabs is only sound when
    /// both sides read their stored codes through the same bijection —
    /// so the test is that the maps *agree*, not merely that they have
    /// the same `version()`. (Versions count edits to one lineage, so
    /// two maps built independently can share a version and disagree on
    /// every code.)
    ///
    /// Pointer equality is the common case — every producer shares the
    /// document's map — but it is not the only one: an empty document
    /// and a freshly loaded fragment each mint their own identity map,
    /// and those are interchangeable. Comparing the maps catches that,
    /// and costs nothing when both are identity (two empty vecs).
    /// Getting this wrong is silent and expensive rather than incorrect:
    /// the fallback below translates per value.
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
                            v.map_actor(|a| self.map.sto(a))
                        }
                    })
                    .collect();
                shift += vals.len() as isize - sp.delete as isize;
                self.col.splice(at, sp.delete, vals);
            }
        }
    }

    /// Rewrite into a (possibly different) target space: values become
    /// `target.sto(f(logical))` and the column adopts `target`'s map.
    /// This is the fragment-load remap — the same single O(fragment)
    /// pass the bundle→doc translation always needed.
    pub(crate) fn remap_into<F>(&mut self, f: &F, target: Arc<ActorMap>)
    where
        F: Fn(ActorIdx) -> ActorIdx,
    {
        let map = self.map.clone();
        self.col
            .remap(|v: T| v.map_actor(|a| target.sto(f(map.log(a)))));
        self.map = target;
    }

    /// Logical-space rewrite in place (keeps the current map).
    pub(crate) fn remap<F>(&mut self, f: &F)
    where
        F: Fn(ActorIdx) -> ActorIdx,
    {
        let target = self.map.clone();
        self.remap_into(f, target);
    }

    /// A canonical (logical-space) column — the save paths. Identity
    /// maps hand back a cheap clone (Arc'd slabs).
    fn canonical(&self) -> hexane::Column<T> {
        let mut c = self.col.clone();
        if !self.map.is_identity() {
            let map = self.map.clone();
            c.remap(move |v: T| v.map_actor(|a| map.log(a)));
        }
        c
    }

    /// Rewrite stored codes to logical and drop the map. Rare paths
    /// (actor removal) that must see raw == logical call this first.
    pub(crate) fn flush(&mut self) {
        if !self.map.is_identity() {
            let map = self.map.clone();
            self.col.remap(move |v: T| v.map_actor(|a| map.log(a)));
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

    /// Narrow `range` to the rows holding `value`. Rows in the range
    /// are sorted by *logical* value (a counter-narrowed group), so the
    /// mapped path walks its (few) runs translating per run; identity
    /// delegates to the raw binary search.
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
            match run.value.map_actor(|a| self.map.log(a)).cmp(&value) {
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

// ── the mapped iterator: hexane::Iter's surface, logical values ──────

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
    fn log_val(&self, v: T) -> T {
        if self.map.is_identity() {
            v
        } else {
            let map = &self.map;
            v.map_actor(|a| map.log(a))
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

    /// Order search within a (small, counter-narrowed) window: walk
    /// runs translating per run; identity delegates to the raw binary
    /// search. Returns the matching sub-range, or an empty range at
    /// the insertion point on a miss.
    pub(crate) fn seek_to_value(
        &mut self,
        target: T,
        range: impl std::ops::RangeBounds<usize>,
    ) -> std::ops::Range<usize> {
        if self.map.is_identity() {
            return self.iter.seek_to_value(target, range);
        }
        // mirror the raw path exactly: scan on a max-clamped clone,
        // leave self parked at the found range's start
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
            match run.value.map_actor(|a| self.map.log(a)).cmp(&target) {
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
    /// Wrap a raw iterator with the identity map (test helpers).
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
        assert_eq!(m.log(ActorIdx(1)), ActorIdx(1));
    }

    #[test]
    fn mid_insert_shifts() {
        let m = ActorMap::identity();
        let m = m.insert(0, 0); // actor A -> stored 0, logical 0
        let m = m.insert(1, 1); // actor C -> stored 1, logical 1
        let m = m.insert(1, 2); // actor B between them -> stored 2, logical 1
        assert!(!m.is_identity());
        assert_eq!(m.log(ActorIdx(0)), ActorIdx(0)); // A
        assert_eq!(m.log(ActorIdx(1)), ActorIdx(2)); // C shifted
        assert_eq!(m.log(ActorIdx(2)), ActorIdx(1)); // B fresh
        for l in 0..3u32 {
            assert_eq!(m.log(m.sto(ActorIdx(l))), ActorIdx(l));
        }
    }

    /// Two maps built independently from the same starting point end up
    /// with the same `version` but disagree on every code — the reason
    /// [`MappedColumn::copy_ranges`] tests identity with `Arc::ptr_eq`
    /// and not version equality.
    #[test]
    fn same_version_maps_are_not_interchangeable() {
        let a = ActorMap::identity().insert(0, 2);
        let b = ActorMap::identity().insert(1, 2);
        assert_eq!(a.version(), b.version(), "versions collide");
        assert!(!Arc::ptr_eq(&a, &b));
        // ...and they really are different bijections
        assert_ne!(a.log(ActorIdx(0)), b.log(ActorIdx(0)));
    }

    /// A copy between columns on different maps must translate rather
    /// than adopt slabs: the destination's logical values are what the
    /// caller wrote on either side, never the raw stored codes.
    #[test]
    fn copy_ranges_translates_across_distinct_maps() {
        let dst_map = ActorMap::identity().insert(0, 2);
        let src_map = ActorMap::identity().insert(1, 2);
        assert_eq!(dst_map.version(), src_map.version());

        let logical = |v: &[u32]| -> Vec<ActorIdx> { v.iter().map(|&x| ActorIdx(x)).collect() };

        let mut dst = MappedColumn::from_logical_values(logical(&[0, 1]), dst_map.clone());
        let src = MappedColumn::from_logical_values(logical(&[2, 1, 0]), src_map);

        // append src's rows 1..3 to the end of dst
        dst.copy_ranges(
            src,
            [hexane::Splice {
                pos: 2,
                delete: 0,
                range: 1..3,
            }],
        );

        // the values that went in are the values that come out — a
        // version-equality check here would have adopted src's slabs and
        // reinterpreted its stored codes through dst's map
        assert_eq!(dst.to_vec(), logical(&[0, 1, 1, 0]));
        assert!(Arc::ptr_eq(dst.actor_map(), &dst_map), "map unchanged");
    }

    /// A column written pre-insert, read post-insert: the stored slabs
    /// never change, the surface renumbers, and every access path
    /// (get, iter, run walk, order search, save) agrees.
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

        // writes go in logical, come back logical
        col.splice(5, 0, [ActorIdx(1)]);
        assert_eq!(col.get(5), Some(ActorIdx(1)));

        // run walk translates per run
        let runs: Vec<_> = {
            let mut it = col.iter();
            std::iter::from_fn(move || it.next_run()).collect()
        };
        let flat: Vec<ActorIdx> = runs
            .iter()
            .flat_map(|r| std::iter::repeat_n(r.value, r.count))
            .collect();
        assert_eq!(flat, col.to_vec());

        // order search in a sorted window: rows 2..3 hold logical 2
        assert_eq!(col.scope_to_value(ActorIdx(2), 2..4), 2..4);
        assert_eq!(col.scope_to_value(ActorIdx(1), 2..4), 2..2);

        // save is canonical (logical) — equal to a plain column's
        let plain = hexane::Column::<ActorIdx>::from_values(col.to_vec());
        assert_eq!(col.save(), plain.save());

        // flush rewrites in place without changing the surface
        let before = col.to_vec();
        col.flush();
        assert!(col.actor_map().is_identity());
        assert_eq!(col.to_vec(), before);
    }
}
