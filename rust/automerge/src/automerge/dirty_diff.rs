use std::collections::HashMap;
use std::ops::Range;

use hexane::PrefixIter;

use crate::clock::ClockRange;
use crate::iter::{ListDiff, MapDiff, SpansDiff};
use crate::op_set2::op_set::{MarkIdx, OpSet};
use crate::patches::{Patch, PatchAccumulator};
use crate::types::{ObjId, ObjMeta, ObjType, OpId, TextEncoding};

use super::Automerge;
use crate::ChangeId;

#[derive(Debug, PartialEq)]
pub(crate) enum DirtyDiffError {
    MissingObject(usize),
    UnknownObject(ObjId),
    InvalidHeads(crate::AutomergeError),
}

#[derive(Debug, Clone)]
struct DirtyObject {
    obj: ObjId,
    typ: ObjType,
    range: Range<usize>,
}

/// Caches the object lookup while scanning dirty runs: consecutive
/// dirty rows almost always land in the same object.
#[derive(Default, Debug, Clone)]
struct DirtyObjectContext {
    object: Option<DirtyObject>,
}

impl DirtyObjectContext {
    fn object_containing(
        &mut self,
        doc: &Automerge,
        pos: usize,
    ) -> Result<DirtyObject, DirtyDiffError> {
        if let Some(object) = &self.object {
            if object.range.contains(&pos) {
                return Ok(object.clone());
            }
        }

        let (obj, range) = doc
            .ops()
            .obj_range_containing(pos)
            .ok_or(DirtyDiffError::MissingObject(pos))?;
        let typ = doc
            .ops()
            .object_type(&obj)
            .ok_or(DirtyDiffError::UnknownObject(obj))?;
        let object = DirtyObject { obj, typ, range };
        self.object = Some(object.clone());
        Ok(object)
    }
}

/// The diff pipeline for one dirty pass: one iterator per object type,
/// plus the two index cursors, all long-lived.
///
/// Dirty ranges arrive in document order — `normalize_dirty_object_ranges`
/// sorts by object and then by range, and an object's ops are contiguous
/// and ascending — so every cursor here only ever moves forward. A range
/// is a `shift_next` onto its object type's iterator, and the index
/// cursors carry their running totals across ranges instead of
/// recounting from the object's first row.
struct DirtyIters<'a> {
    map: MapDiff<'a>,
    list: ListDiff<'a>,
    spans: SpansDiff<'a>,
    /// running count of visible elements: the list index
    top: PrefixIter<'a, bool>,
    /// running text width: the character index
    text: PrefixIter<'a, Option<u32>>,
    object: Option<ObjId>,
}

impl<'a> DirtyIters<'a> {
    fn new(op_set: &'a OpSet, clock: ClockRange, encoding: TextEncoding) -> Self {
        // built over an empty window at row zero: constructing over a
        // real range would draw a lookahead item (`Unshift::new`), and
        // the visibility skipper behind it reads ahead far enough that
        // the first `shift` could then be asked to move backwards
        let empty = 0..0;
        Self {
            map: MapDiff::new(op_set, empty.clone(), clock.clone()),
            list: ListDiff::new(op_set, empty.clone(), clock.clone()),
            spans: SpansDiff::new(op_set, empty, clock, encoding),
            top: op_set.top_prefix_iter(),
            text: op_set.text_prefix_iter(),
            object: None,
        }
    }

    /// Indexes are relative to their object, so zero the running totals
    /// at each new object's first row.
    fn enter(&mut self, object: &DirtyObject) {
        if self.object == Some(object.obj) {
            return;
        }
        self.top.advance_to(object.range.start);
        self.top.reset_prefix();
        self.text.advance_to(object.range.start);
        self.text.reset_prefix();
        self.object = Some(object.obj);
    }

    fn list_index(&mut self, pos: usize) -> usize {
        self.top.advance_to(pos);
        self.top.total()
    }

    fn text_index(&mut self, pos: usize) -> usize {
        self.text.advance_to(pos);
        self.text.total() as usize
    }
}

impl Automerge {
    pub(crate) fn dirty_diff_patches(
        &self,
        before_heads: &[ChangeId],
        after_heads: &[ChangeId],
    ) -> Result<Vec<Patch>, DirtyDiffError> {
        if before_heads.is_empty() && after_heads == self.get_heads() {
            let mut patch_accumulator = PatchAccumulator::event_log();
            patch_accumulator.heads_clock = None;
            self.log_current_state(ObjMeta::root(), &mut patch_accumulator, true);
            return Ok(patch_accumulator.make_patches(self));
        }

        let current_heads = self.get_heads();
        let clock = if after_heads == current_heads.as_slice() {
            let before = self
                .nodes_for_change_ids(before_heads)
                .map_err(DirtyDiffError::InvalidHeads)?;
            ClockRange::diff_to_current(self.change_graph.clock_for_nodes(before))
        } else {
            self.clock_range(before_heads, after_heads)
                .map_err(DirtyDiffError::InvalidHeads)?
        };
        let mut patch_accumulator = PatchAccumulator::event_log();
        patch_accumulator.heads_clock = clock.after_clock();
        self.log_dirty_diff(clock, &mut patch_accumulator)?;
        Ok(patch_accumulator.make_patches(self))
    }

    pub(crate) fn dirty_diff_patches_and_clear(
        &mut self,
        before_heads: &[ChangeId],
        after_heads: &[ChangeId],
    ) -> Result<Vec<Patch>, DirtyDiffError> {
        let current_heads = self.get_heads();
        debug_assert_eq!(
            after_heads,
            current_heads.as_slice(),
            "clearing dirty diff state is only valid after diffing to current heads"
        );
        let patches = self.dirty_diff_patches(before_heads, after_heads)?;
        self.clear_dirty();
        Ok(patches)
    }

    fn log_dirty_diff(
        &self,
        clock: ClockRange,
        patch_accumulator: &mut PatchAccumulator,
    ) -> Result<(), DirtyDiffError> {
        let encoding = self.text_encoding();
        let ranges = self.dirty_ranges_by_object()?;
        let mut iters = DirtyIters::new(self.ops(), clock, encoding);
        for (object, range) in ranges {
            iters.enter(&object);
            let obj = object.obj;
            match object.typ {
                ObjType::Map | ObjType::Table => {
                    // `MapDiff` reads conflict, expose and the winner by
                    // counting within a key's register, so a partial one
                    // yields a wrong patch rather than a failure. This
                    // checks `dirty_ranges_by_object`'s widening, not the
                    // op set — a key column that isn't sorted by key is
                    // past saving here.
                    debug_assert!(
                        self.ops()
                            .map_range_is_on_key_boundaries(&range, object.range.clone()),
                        "dirty range {range:?} splits a key register",
                    );
                    iters.map.shift(range);
                    for item in iters.map.by_ref() {
                        item.log(obj, patch_accumulator, encoding);
                    }
                }
                ObjType::List => {
                    debug_assert!(
                        self.ops()
                            .list_range_is_on_register_boundaries(&range, object.range.clone()),
                        "dirty range {range:?} splits a list register",
                    );
                    let index = iters.list_index(range.start);
                    iters.list.shift_with_index(range, index);
                    for item in iters.list.by_ref() {
                        item.log(obj, patch_accumulator, encoding);
                    }
                }
                ObjType::Text => {
                    let index = iters.text_index(range.start);
                    iters.spans.shift_with_index(range, index);
                    for item in iters.spans.by_ref() {
                        item.log(obj, patch_accumulator, encoding);
                    }
                }
            }
        }
        Ok(())
    }

    fn dirty_ranges_by_object(&self) -> Result<Vec<(DirtyObject, Range<usize>)>, DirtyDiffError> {
        let mut context = DirtyObjectContext::default();
        let mut ranges = Vec::new();
        for dirty in self.ops().dirty_runs() {
            let mut start = dirty.start;
            while start < dirty.end {
                let object = context.object_containing(self, start)?;
                let end = dirty.end.min(object.range.end);
                let mut range = start..end;
                // dirty bits are marked row-at-a-time; the diff iterators
                // work register-at-a-time, so widen to register boundaries
                match object.typ {
                    ObjType::Map | ObjType::Table => {
                        range.start = self
                            .ops()
                            .map_key_register_at_pos(range.start, object.range.clone())
                            .start;
                        range.end = self
                            .ops()
                            .map_key_register_at_pos(range.end - 1, object.range.clone())
                            .end
                            .max(range.end);
                    }
                    ObjType::List | ObjType::Text => {
                        range = self
                            .ops()
                            .expand_to_seq_register_boundaries(range, object.range.clone());
                    }
                }
                ranges.push((object, range));
                start = end;
            }
        }
        self.widen_to_mark_extents(&mut ranges);
        Ok(Self::normalize_dirty_object_ranges(ranges))
    }

    /// Widen text ranges to cover the span of every dirty mark.
    ///
    /// A mark op that appeared changes the formatting of everything it
    /// brackets, not just the row it sits on, and the patch for that is
    /// produced by walking those spans — so the span between the two
    /// ends has to be in range.
    ///
    /// Finding the other end needs no search: a mark's begin and end are
    /// written by one transaction, so either both rows are dirty or
    /// neither is, and the end always sorts after the begin. Pairing
    /// them off the ranges already collected is enough. The extents go
    /// in as further ranges and
    /// [`Self::normalize_dirty_object_ranges`] merges them.
    fn widen_to_mark_extents(&self, ranges: &mut Vec<(DirtyObject, Range<usize>)>) {
        if !self.ops().has_marks() {
            return;
        }
        let mut extents = Vec::new();
        let mut open: HashMap<OpId, usize> = HashMap::new();
        for (object, range) in ranges.iter() {
            if object.typ != ObjType::Text {
                continue;
            }
            for (pos, idx) in self.ops().mark_index_entries(range.clone()) {
                match idx {
                    MarkIdx::Start(id) => {
                        open.insert(id, pos);
                    }
                    MarkIdx::End(id) => {
                        let Some(start) = open.remove(&id) else {
                            // the begin is dirty whenever the end is, so
                            // a miss means the pair invariant broke
                            debug_assert!(false, "dirty mark end {id:?} without its begin");
                            continue;
                        };
                        let extent = self.ops().expand_to_seq_register_boundaries(
                            start..pos + 1,
                            object.range.clone(),
                        );
                        extents.push((object.clone(), extent));
                    }
                }
            }
        }
        debug_assert!(open.is_empty(), "dirty mark begin without its end");
        ranges.append(&mut extents);
    }

    fn normalize_dirty_object_ranges(
        mut ranges: Vec<(DirtyObject, Range<usize>)>,
    ) -> Vec<(DirtyObject, Range<usize>)> {
        ranges.retain(|(_, range)| range.start < range.end);
        ranges.sort_unstable_by(|(left_obj, left_range), (right_obj, right_range)| {
            left_obj
                .range
                .start
                .cmp(&right_obj.range.start)
                .then_with(|| left_range.start.cmp(&right_range.start))
                .then_with(|| left_range.end.cmp(&right_range.end))
        });

        let mut normalized: Vec<(DirtyObject, Range<usize>)> = Vec::with_capacity(ranges.len());
        for (object, range) in ranges {
            if let Some((last_object, last_range)) = normalized.last_mut() {
                if last_object.obj == object.obj && range.start <= last_range.end {
                    last_range.end = last_range.end.max(range.end);
                    continue;
                }
            }
            normalized.push((object, range));
        }
        normalized
    }
}
