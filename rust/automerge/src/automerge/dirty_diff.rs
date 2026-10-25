use std::collections::HashMap;
use std::ops::Range;

use hexane::PrefixIter;

use crate::iter::{ListDiff, MapDiff, SpansDiff};
use crate::op_set2::op_set::{MarkIdx, OpSet};
use crate::patches::{Patch, PatchAccumulator};
use crate::types::{ObjId, ObjMeta, ObjType, OpId, TextEncoding};

use super::view::{ClockRange, Viewpoint};
use super::Automerge;

#[derive(Debug, Clone)]
struct DirtyObject {
    obj: ObjId,
    typ: ObjType,
    range: Range<usize>,
}

/// Dirty ranges arrive in document order, so every cursor here only moves
/// forward and the index totals carry across ranges.
struct DirtyIters<'a> {
    map: MapDiff<'a>,
    list: ListDiff<'a>,
    spans: SpansDiff<'a>,
    list_index: PrefixIter<'a, bool>,
    text_index: PrefixIter<'a, Option<u32>>,
    object: Option<ObjId>,
}

impl<'a> DirtyIters<'a> {
    fn new(op_set: &'a OpSet, clock: ClockRange<'a>, encoding: TextEncoding) -> Self {
        // a non-empty window draws a lookahead item, after which the first
        // `shift` could have to move backwards
        let empty = 0..0;
        Self {
            map: MapDiff::new(op_set, empty.clone(), clock.clone()),
            list: ListDiff::new(op_set, empty.clone(), clock.clone()),
            spans: SpansDiff::new(op_set, empty, clock, encoding),
            list_index: op_set.top_prefix_iter(),
            text_index: op_set.text_prefix_iter(),
            object: None,
        }
    }

    fn enter(&mut self, object: &DirtyObject) {
        if self.object == Some(object.obj) {
            return;
        }
        self.list_index.advance_to(object.range.start);
        self.list_index.reset_prefix();
        self.text_index.advance_to(object.range.start);
        self.text_index.reset_prefix();
        self.object = Some(object.obj);
    }

    fn list_index(&mut self, pos: usize) -> usize {
        self.list_index.advance_to(pos);
        self.list_index.total()
    }

    fn text_index(&mut self, pos: usize) -> usize {
        self.text_index.advance_to(pos);
        self.text_index.total() as usize
    }
}

impl<H: crate::hash_retention::HashRetention> Automerge<H> {
    /// Patches from `before` to the current state, walking only what changed
    /// since the dirty bits were last cleared: `before` must be the view they
    /// were cleared at.
    ///
    /// Panics if `before` names heads this document does not hold.
    pub(crate) fn dirty_diff_patches(&self, before: &Viewpoint) -> Vec<Patch> {
        let mut patch_accumulator = PatchAccumulator::new(None);
        if before.heads.is_empty() {
            self.log_current_state(ObjMeta::root(), &mut patch_accumulator, true);
        } else {
            let before = self
                .visible_for_viewpoint(before)
                .expect("dirty diff starts at this document's own heads");
            let clock = ClockRange::between_current(before, self.read_current());
            self.log_dirty_diff(clock, &mut patch_accumulator);
        }
        patch_accumulator.make_patches(self)
    }

    fn log_dirty_diff(&self, clock: ClockRange<'_>, patch_accumulator: &mut PatchAccumulator) {
        let encoding = self.text_encoding();
        let ranges = self.dirty_ranges_by_object();
        let mut iters = DirtyIters::new(self.ops(), clock, encoding);
        for (object, range) in ranges {
            iters.enter(&object);
            let obj = object.obj;
            match object.typ {
                ObjType::Map | ObjType::Table => {
                    // a partial key register yields a wrong patch, not a failure
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
    }

    fn widen_to_registers(&self, object: &DirtyObject, range: Range<usize>) -> Range<usize> {
        match object.typ {
            ObjType::Map | ObjType::Table => {
                let start = self
                    .ops()
                    .map_key_register_at_pos(range.start, object.range.clone())
                    .start;
                let end = self
                    .ops()
                    .map_key_register_at_pos(range.end - 1, object.range.clone())
                    .end
                    .max(range.end);
                start..end
            }
            ObjType::List | ObjType::Text => self
                .ops()
                .expand_to_seq_register_boundaries(range, object.range.clone()),
        }
    }

    fn dirty_ranges_by_object(&self) -> Vec<(DirtyObject, Range<usize>)> {
        let mut object: Option<DirtyObject> = None;
        let mut ranges = Vec::new();
        for dirty in self.ops().dirty_runs() {
            let mut start = dirty.start;
            while start < dirty.end {
                let object = match &object {
                    Some(o) if o.range.contains(&start) => o.clone(),
                    _ => object.insert(self.dirty_object_at(start)).clone(),
                };
                let end = dirty.end.min(object.range.end);
                let range = self.widen_to_registers(&object, start..end);
                ranges.push((object, range));
                start = end;
            }
        }
        self.widen_to_mark_extents(&mut ranges);
        Self::normalize_dirty_object_ranges(ranges)
    }

    fn dirty_object_at(&self, pos: usize) -> DirtyObject {
        let (obj, range) = self
            .ops()
            .obj_range_containing(pos)
            .expect("a dirty row lies inside an object");
        let typ = self
            .ops()
            .object_type(&obj)
            .expect("an object in the op set has a type");
        DirtyObject { obj, typ, range }
    }

    /// A dirty mark changes the formatting of its whole span, so the span
    /// must be diffed too. A mark's begin and end are written by one
    /// transaction, so both are dirty or neither is.
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
