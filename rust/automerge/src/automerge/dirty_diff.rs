use std::ops::Range;

use crate::clock::ClockRange;
use crate::iter::{Diff, ListDiff, MapDiff, RichTextDiff, SpansDiff};
use crate::op_set2::types::{Action, MarkData};
use crate::patches::{Patch, PatchAccumulator};
use crate::types::{ChangeHash, ObjId, ObjMeta, ObjType};

use super::Automerge;

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirtyDiffError {
    MissingObject(usize),
    UnknownObject(ObjId),
    UnsupportedObjectType(ObjType),
}

#[derive(Debug, Clone)]
struct DirtyObject {
    obj: ObjId,
    typ: ObjType,
    range: Range<usize>,
}

#[derive(Default, Debug, Clone)]
struct DirtyObjectContext {
    object: Option<DirtyObject>,
    list_pos: usize,
    list_index: usize,
    text_pos: usize,
    text_index: usize,
    text_marks: RichTextDiff<'static>,
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
        self.reset_for_object(&object);
        self.object = Some(object.clone());
        Ok(object)
    }

    fn reset_for_object(&mut self, object: &DirtyObject) {
        self.list_pos = object.range.start;
        self.list_index = 0;
        self.text_pos = object.range.start;
        self.text_index = 0;
        self.text_marks = RichTextDiff::default();
    }

    fn ensure_object(&mut self, object: &DirtyObject) {
        if self
            .object
            .as_ref()
            .is_none_or(|current| current.obj != object.obj || current.range != object.range)
        {
            self.object = Some(object.clone());
            self.reset_for_object(object);
        }
    }

    fn list_index_at(&mut self, doc: &Automerge, object: &DirtyObject, pos: usize) -> usize {
        self.advance_list_to(doc, object, pos);
        self.list_index
    }

    fn advance_list_to(&mut self, doc: &Automerge, object: &DirtyObject, pos: usize) {
        self.ensure_object(object);

        let pos = pos.min(object.range.end);
        if pos < self.list_pos {
            self.list_pos = object.range.start;
            self.list_index = 0;
        }
        if pos > self.list_pos {
            self.list_index += doc.ops().list_visible_items_in_range(self.list_pos..pos);
            self.list_pos = pos;
        }
    }

    fn text_index_at(
        &mut self,
        doc: &Automerge,
        object: &DirtyObject,
        pos: usize,
        clock: &ClockRange,
    ) -> usize {
        self.advance_text_to(doc, object, pos, clock);
        self.text_index
    }

    fn text_marks(&self) -> RichTextDiff<'static> {
        self.text_marks.clone()
    }

    fn advance_text_to(
        &mut self,
        doc: &Automerge,
        object: &DirtyObject,
        pos: usize,
        clock: &ClockRange,
    ) {
        self.ensure_object(object);

        let pos = pos.min(object.range.end);
        if pos < self.text_pos {
            self.text_pos = object.range.start;
            self.text_index = 0;
            self.text_marks = RichTextDiff::default();
        }
        if pos > self.text_pos {
            let range = self.text_pos..pos;
            if doc.ops().has_marks() {
                self.advance_text_marks(doc, range.clone(), clock);
            }
            self.text_index += doc.ops().text_visible_width_in_range(range);
            self.text_pos = pos;
        }
    }

    fn advance_text_marks(&mut self, doc: &Automerge, range: Range<usize>, clock: &ClockRange) {
        for op in doc.ops().iter_range(&range) {
            if op.action != Action::Mark {
                continue;
            }
            let diff = match (clock.visible_before(&op.id), clock.visible_after(&op.id)) {
                (true, true) => Diff::Same,
                (true, false) => Diff::Del,
                (false, true) => Diff::Add,
                (false, false) => continue,
            };
            if let Some(name) = op.mark_name {
                let data = MarkData {
                    name: name.to_owned().into(),
                    value: op.value.into_owned(),
                };
                self.text_marks.mark_begin_diff(diff, op.id, data);
            } else {
                self.text_marks.mark_end_diff(diff, op.id);
            }
        }
    }
}

impl Automerge {
    #[allow(dead_code)]
    pub(crate) fn dirty_diff_patches(
        &self,
        before_heads: &[ChangeHash],
        after_heads: &[ChangeHash],
    ) -> Result<Vec<Patch>, DirtyDiffError> {
        if before_heads.is_empty() && after_heads == self.get_heads() {
            let mut patch_accumulator = PatchAccumulator::event_log();
            patch_accumulator.heads = None;
            self.log_current_state(ObjMeta::root(), &mut patch_accumulator, true);
            return Ok(patch_accumulator.make_patches(self));
        }

        let current_heads = self.get_heads();
        let use_baseline_before = before_heads == self.dirty_diff_base.as_slice()
            && after_heads == current_heads.as_slice();
        let clock = if after_heads == current_heads.as_slice() {
            ClockRange::diff_to_current(self.clock_at(before_heads))
        } else {
            self.clock_range(before_heads, after_heads)
        };
        let mut patch_accumulator = PatchAccumulator::event_log();
        patch_accumulator.heads = Some(after_heads.to_vec());
        self.log_dirty_diff(clock, use_baseline_before, &mut patch_accumulator)?;
        Ok(patch_accumulator.make_patches(self))
    }

    #[allow(dead_code)]
    pub(crate) fn dirty_diff_patches_and_clear(
        &mut self,
        before_heads: &[ChangeHash],
        after_heads: &[ChangeHash],
    ) -> Result<Vec<Patch>, DirtyDiffError> {
        let current_heads = self.get_heads();
        debug_assert_eq!(
            after_heads,
            current_heads.as_slice(),
            "clearing dirty diff state is only valid after diffing to current heads"
        );
        let patches = self.dirty_diff_patches(before_heads, after_heads)?;
        self.clear_dirty_and_reset_diff_baseline(current_heads);
        Ok(patches)
    }

    #[allow(dead_code)]
    fn log_dirty_diff(
        &self,
        clock: ClockRange,
        use_baseline_before: bool,
        patch_accumulator: &mut PatchAccumulator,
    ) -> Result<(), DirtyDiffError> {
        let encoding = self.text_encoding();
        let ranges = self.dirty_ranges_by_object()?;
        let mut context = DirtyObjectContext::default();
        for (object, range) in ranges {
            match object.typ {
                ObjType::Map | ObjType::Table => {
                    if !self.ops().map_range_is_on_key_boundaries(&range) {
                        return Err(DirtyDiffError::UnsupportedObjectType(object.typ));
                    }
                    let iter = if use_baseline_before {
                        MapDiff::new_with_baseline_before(self.ops(), range, clock.clone())
                    } else {
                        MapDiff::new(self.ops(), range, clock.clone())
                    };
                    for item in iter {
                        item.log(object.obj, patch_accumulator, encoding);
                    }
                }
                ObjType::List => {
                    if !self
                        .ops()
                        .list_range_is_on_register_boundaries(&range, object.range.clone())
                    {
                        return Err(DirtyDiffError::UnsupportedObjectType(object.typ));
                    }
                    let base_index = context.list_index_at(self, &object, range.start);
                    let iter = if use_baseline_before {
                        ListDiff::new_with_baseline_before(
                            self.ops(),
                            range.clone(),
                            clock.clone(),
                            base_index,
                        )
                    } else {
                        ListDiff::new_with_index(
                            self.ops(),
                            range.clone(),
                            clock.clone(),
                            base_index,
                        )
                    };
                    for item in iter {
                        item.log(object.obj, patch_accumulator, encoding);
                    }
                    context.advance_list_to(self, &object, range.end);
                }
                ObjType::Text => {
                    let base_index = context.text_index_at(self, &object, range.start, &clock);
                    let marks = context.text_marks();
                    let iter = if use_baseline_before {
                        SpansDiff::new_with_baseline_before(
                            self.ops(),
                            range.clone(),
                            clock.clone(),
                            encoding,
                            base_index,
                            marks,
                        )
                    } else {
                        SpansDiff::new_with_index_and_marks(
                            self.ops(),
                            range.clone(),
                            clock.clone(),
                            encoding,
                            base_index,
                            marks,
                        )
                    };
                    for item in iter {
                        item.log(object.obj, patch_accumulator, encoding);
                    }
                    context.advance_text_to(self, &object, range.end, &clock);
                }
            }
        }
        Ok(())
    }

    fn dirty_ranges_by_object(&self) -> Result<Vec<(DirtyObject, Range<usize>)>, DirtyDiffError> {
        let mut context = DirtyObjectContext::default();
        let mut ranges = Vec::new();
        for dirty in self.ops().dirty_runs() {
            let mut start = dirty.range.start;
            while start < dirty.range.end {
                let object = context.object_containing(self, start)?;
                let end = dirty.range.end.min(object.range.end);
                let mut range = start..end;
                if object.typ == ObjType::Text
                    && range != object.range
                    && self.ops().has_marks()
                    && self.ops().range_has_mark(range.clone())
                {
                    range = object.range.clone();
                }
                ranges.push((object, range));
                start = end;
            }
        }
        Ok(Self::normalize_dirty_object_ranges(ranges))
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
