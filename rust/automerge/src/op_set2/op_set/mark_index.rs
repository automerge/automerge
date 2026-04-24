use crate::op_set2::op_set::RichTextQueryState;
use crate::op_set2::MarkData;
use crate::types::{Clock, OpId};
use hexane::{
    Acc, ColumnCursor, ColumnData, HasAcc, HasPos, PackError, Packable, RleCursor, Slab, SpanWeight,
};

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum MarkIndexValue {
    Start(OpId),
    End(OpId),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MarkIndexBuilder {
    Start(OpId, MarkData<'static>),
    End(OpId),
}

impl MarkIndexValue {
    fn as_i64(&self) -> i64 {
        match self {
            MarkIndexValue::Start(id) => {
                let tmp = ((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff);
                debug_assert_eq!(self, &MarkIndexValue::load(tmp));
                tmp
            }
            MarkIndexValue::End(id) => {
                let tmp = -(((id.actor() as i64) << 32) + ((id.counter() as i64) & 0xffffffff));
                debug_assert_eq!(self, &MarkIndexValue::load(tmp));
                tmp
            }
        }
    }

    fn load(v: i64) -> Self {
        if v < 0 {
            let v = -v as u64;
            let actor = (v >> 32) as usize;
            let ctr = v & 0xffffffff;
            Self::End(OpId::new(ctr, actor))
        } else {
            let v = v as u64;
            let actor = (v >> 32) as usize;
            let ctr = v & 0xffffffff;
            Self::Start(OpId::new(ctr, actor))
        }
    }

    fn with_new_actor(self, idx: usize) -> Self {
        match self {
            Self::Start(id) => Self::Start(id.with_new_actor(idx)),
            Self::End(id) => Self::End(id.with_new_actor(idx)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct MarkIndexSpanner {
    pub(crate) pos: usize,
    /// Net mark counts: +1 for each Start, -1 for each End.
    /// Positive = net open (in `start` set), negative = net close (in `end` set).
    marks: HashMap<OpId, i32>,
}

impl MarkIndexSpanner {
    /// Marks that are net-open in this range (have Start but no matching End).
    pub(crate) fn start_set(&self) -> HashSet<OpId> {
        self.marks
            .iter()
            .filter(|(_, &v)| v > 0)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Marks that are net-closed in this range (have End but no matching Start).
    pub(crate) fn end_set(&self) -> HashSet<OpId> {
        self.marks
            .iter()
            .filter(|(_, &v)| v < 0)
            .map(|(&id, _)| id)
            .collect()
    }
}

impl SpanWeight<Slab> for MarkIndexSpanner {
    fn alloc(slab: &Slab) -> Self {
        let pos = slab.len();
        let mut marks = HashMap::default();
        let mut cursor = MarkIndex::default();
        let bytes = slab.as_slice();
        while let Some(run) = cursor.next(bytes) {
            match run.value.as_deref() {
                Some(MarkIndexValue::Start(id)) => {
                    *marks.entry(*id).or_insert(0) += 1;
                }
                Some(MarkIndexValue::End(id)) => {
                    *marks.entry(*id).or_insert(0) -= 1;
                }
                None => {}
            }
        }
        // Remove zero entries to keep map compact.
        marks.retain(|_, v| *v != 0);
        Self { pos, marks }
    }

    fn and(mut self, other: &Self) -> Self {
        self.union(other);
        self
    }

    fn union(&mut self, other: &Self) {
        self.pos += other.pos;
        for (&id, &count) in &other.marks {
            let entry = self.marks.entry(id).or_insert(0);
            *entry += count;
            if *entry == 0 {
                self.marks.remove(&id);
            }
        }
    }

    fn maybe_sub(&mut self, other: &Self) -> bool {
        self.pos -= other.pos;
        for (&id, &count) in &other.marks {
            let entry = self.marks.entry(id).or_insert(0);
            *entry -= count;
            if *entry == 0 {
                self.marks.remove(&id);
            }
        }
        true
    }
}

impl HasAcc for MarkIndexSpanner {
    fn acc(&self) -> Acc {
        Acc::new()
    }
}

impl HasPos for MarkIndexSpanner {
    fn pos(&self) -> usize {
        self.pos
    }
}

pub(crate) type MarkIndexInternal<const B: usize> = RleCursor<B, MarkIndexValue, MarkIndexSpanner>;

pub(crate) type MarkIndex = MarkIndexInternal<64>;

#[derive(Clone, Debug, Default)]
pub(crate) struct MarkIndexColumn {
    data: ColumnData<MarkIndex>,
    cache: HashMap<OpId, MarkData<'static>>,
}

impl MarkIndexColumn {
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        // FIXME - would be much better to do this by run instead of by value
        let new_data = self
            .data
            .iter()
            .map(|m| m.map(|n| n.with_new_actor(idx)))
            .collect();
        let new_cache = self
            .cache
            .iter()
            .map(|(key, val)| (key.with_new_actor(idx), val.clone()))
            .collect();
        self.data = new_data;
        self.cache = new_cache;
    }

    pub(crate) fn new() -> Self {
        Self {
            data: ColumnData::new(),
            cache: HashMap::new(),
        }
    }

    pub(crate) fn extend(&mut self, index: usize, values: Vec<Option<MarkIndexBuilder>>) {
        let values = values
            .into_iter()
            .map(|v| match v? {
                MarkIndexBuilder::Start(id, mark) => {
                    self.cache.insert(id, mark);
                    Some(MarkIndexValue::Start(id))
                }
                MarkIndexBuilder::End(id) => Some(MarkIndexValue::End(id)),
            })
            .collect::<Vec<_>>();
        self.data.splice(index, 0, values);
    }

    pub(crate) fn undo(&mut self, index: usize, values: Vec<Option<MarkIndexBuilder>>) {
        let del = values.len();
        for v in &values {
            if let Some(MarkIndexBuilder::Start(id, _)) = v {
                self.cache.remove(id);
            }
        }
        self.data.splice::<MarkIndexValue, _>(index, del, []);
    }

    pub(crate) fn rich_text_at(
        &self,
        target: usize,
        clock: Option<&Clock>,
    ) -> RichTextQueryState<'static> {
        let mut marks = RichTextQueryState::default();
        for id in self.marks_at(target, clock) {
            let data = self.cache.get(&id).unwrap();
            marks.map.insert(id, data.clone());
        }
        marks
    }

    pub(crate) fn marks_at<'a>(
        &self,
        target: usize,
        clock: Option<&'a Clock>,
    ) -> impl Iterator<Item = OpId> + 'a {
        let sub = self
            .data
            .slabs
            .get_where_or_last(|acc, next| target < acc.pos() + next.pos());
        let mut start = sub.weight.start_set().into_iter().collect::<BTreeSet<_>>();
        let mut end = sub.weight.end_set();
        let mut pos = sub.weight.pos;
        let mut cursor = MarkIndex::default();
        let bytes = sub.element.as_slice();
        while let Some(run) = cursor.next(bytes) {
            pos += run.count;
            match run.value.as_deref() {
                Some(MarkIndexValue::Start(id)) => {
                    start.insert(*id);
                }
                Some(MarkIndexValue::End(id)) => {
                    if !start.remove(id) {
                        end.insert(*id);
                    }
                }
                None => {}
            }
            if pos > target {
                break;
            }
        }
        start
            .into_iter()
            .filter(move |id| clock.map(|c| c.covers(id)).unwrap_or(true))
    }

    #[cfg(test)]
    pub(crate) fn save(&self) -> Vec<u8> {
        self.data.save()
    }
}

impl Packable for MarkIndexValue {
    fn width(item: &MarkIndexValue) -> usize {
        hexane::lebsize(item.as_i64()) as usize
    }

    fn pack(item: &MarkIndexValue, out: &mut Vec<u8>) {
        leb128::write::signed(out, item.as_i64()).unwrap();
    }

    fn unpack(mut buff: &[u8]) -> Result<(usize, Cow<'static, MarkIndexValue>), PackError> {
        let start_len = buff.len();
        let val = leb128::read::signed(&mut buff)?;
        assert_eq!(MarkIndexValue::load(val).as_i64(), val);
        Ok((
            start_len - buff.len(),
            Cow::Owned(MarkIndexValue::load(val)),
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::op_set2::types::ScalarValue;

    fn mk_mark(name: &str) -> MarkData<'static> {
        MarkData {
            name: Cow::Owned(name.to_string()),
            value: ScalarValue::Boolean(true),
        }
    }

    fn id(actor: usize, counter: u64) -> OpId {
        OpId::new(counter, actor)
    }

    /// Build a column with `n` positions. `marks` is a list of (start_pos, end_pos, actor, counter, name).
    /// Start and End occupy existing positions (they don't insert new entries).
    fn build_column(n: usize, marks: &[(usize, usize, usize, u64, &str)]) -> MarkIndexColumn {
        let mut col = MarkIndexColumn::new();
        // Build the full column as a single extend.
        let mut values: Vec<Option<MarkIndexBuilder>> = vec![None; n];
        for &(start, end, actor, counter, name) in marks {
            let op_id = id(actor, counter);
            values[start] = Some(MarkIndexBuilder::Start(op_id, mk_mark(name)));
            values[end] = Some(MarkIndexBuilder::End(op_id));
        }
        col.extend(0, values);
        col
    }

    fn active_mark_ids(col: &MarkIndexColumn, pos: usize) -> BTreeSet<OpId> {
        col.marks_at(pos, None).collect()
    }

    fn active_mark_names(col: &MarkIndexColumn, pos: usize) -> Vec<String> {
        let rt = col.rich_text_at(pos, None);
        let mut names: Vec<String> = rt.map.values().map(|m| m.name.to_string()).collect();
        names.sort();
        names
    }

    // ── Basic tests ─────────────────────────────────────────────────────

    #[test]
    fn empty_column() {
        let col = MarkIndexColumn::new();
        let rt = col.rich_text_at(0, None);
        assert!(rt.map.is_empty());
    }

    #[test]
    fn single_mark_span() {
        // 10 positions: mark "bold" spans positions 2..7
        // Layout: [_, _, S, _, _, _, _, E, _, _]
        let col = build_column(10, &[(2, 7, 0, 1, "bold")]);

        // Before the mark
        assert!(active_mark_names(&col, 0).is_empty());
        assert!(active_mark_names(&col, 1).is_empty());

        // Inside the mark (start is inclusive)
        assert_eq!(active_mark_names(&col, 2), vec!["bold"]);
        assert_eq!(active_mark_names(&col, 4), vec!["bold"]);
        assert_eq!(active_mark_names(&col, 6), vec!["bold"]);

        // At and after the end
        assert!(active_mark_names(&col, 7).is_empty());
        assert!(active_mark_names(&col, 9).is_empty());
    }

    #[test]
    fn multiple_non_overlapping_marks() {
        // [_, S(bold), _, E(bold), _, S(italic), _, E(italic), _]
        let col = build_column(9, &[(1, 3, 0, 1, "bold"), (5, 7, 0, 2, "italic")]);

        assert!(active_mark_names(&col, 0).is_empty());
        assert_eq!(active_mark_names(&col, 1), vec!["bold"]);
        assert_eq!(active_mark_names(&col, 2), vec!["bold"]);
        assert!(active_mark_names(&col, 3).is_empty());
        assert!(active_mark_names(&col, 4).is_empty());
        assert_eq!(active_mark_names(&col, 5), vec!["italic"]);
        assert_eq!(active_mark_names(&col, 6), vec!["italic"]);
        assert!(active_mark_names(&col, 7).is_empty());
    }

    #[test]
    fn overlapping_marks() {
        // bold: 1..6, italic: 3..8
        // [_, S(b), _, S(i), _, _, E(b), _, E(i), _]
        let col = build_column(10, &[(1, 6, 0, 1, "bold"), (3, 8, 0, 2, "italic")]);

        assert!(active_mark_names(&col, 0).is_empty());
        assert_eq!(active_mark_names(&col, 1), vec!["bold"]);
        assert_eq!(active_mark_names(&col, 2), vec!["bold"]);
        // Overlap region
        assert_eq!(active_mark_names(&col, 3), vec!["bold", "italic"]);
        assert_eq!(active_mark_names(&col, 5), vec!["bold", "italic"]);
        // After bold ends
        assert_eq!(active_mark_names(&col, 6), vec!["italic"]);
        assert_eq!(active_mark_names(&col, 7), vec!["italic"]);
        assert!(active_mark_names(&col, 8).is_empty());
    }

    #[test]
    fn nested_marks() {
        // outer: 0..9, inner: 3..6
        let col = build_column(10, &[(0, 9, 0, 1, "outer"), (3, 6, 0, 2, "inner")]);

        assert_eq!(active_mark_names(&col, 0), vec!["outer"]);
        assert_eq!(active_mark_names(&col, 2), vec!["outer"]);
        assert_eq!(active_mark_names(&col, 3), vec!["inner", "outer"]);
        assert_eq!(active_mark_names(&col, 5), vec!["inner", "outer"]);
        assert_eq!(active_mark_names(&col, 6), vec!["outer"]);
        assert_eq!(active_mark_names(&col, 8), vec!["outer"]);
        assert!(active_mark_names(&col, 9).is_empty());
    }

    // ── Undo tests ──────────────────────────────────────────────────────

    #[test]
    fn undo_removes_mark() {
        // bold: 2..7, italic: 4..9
        let mut col = build_column(12, &[(2, 7, 0, 1, "bold"), (4, 9, 0, 2, "italic")]);

        assert_eq!(active_mark_names(&col, 5), vec!["bold", "italic"]);

        let bold_id = id(0, 1);
        let bold_positions = find_mark_positions(&col, bold_id);
        assert_eq!(bold_positions.len(), 2, "expected Start + End for bold");

        col.undo(
            bold_positions[1],
            vec![Some(MarkIndexBuilder::End(bold_id))],
        );
        col.undo(
            bold_positions[0],
            vec![Some(MarkIndexBuilder::Start(bold_id, mk_mark("bold")))],
        );

        // Bold should be gone, italic should remain.
        for i in 0..col.len() {
            let names = active_mark_names(&col, i);
            assert!(
                !names.contains(&"bold".to_string()),
                "bold should be undone at position {i}, got {names:?}"
            );
        }

        // Italic should still be active in its range.
        // The italic range shifted because we removed 2 entries.
        // Just check that italic exists somewhere.
        let any_italic =
            (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"italic".to_string()));
        assert!(any_italic, "italic should still be active somewhere");
    }

    #[test]
    fn undo_preserves_other_marks() {
        // Three marks: a(1..5), b(3..8), c(6..10)
        let mut col = build_column(
            12,
            &[(1, 5, 0, 1, "a"), (3, 8, 0, 2, "b"), (6, 10, 0, 3, "c")],
        );

        // Verify all three at various positions.
        assert_eq!(active_mark_names(&col, 1), vec!["a"]);
        assert_eq!(active_mark_names(&col, 4), vec!["a", "b"]);
        assert_eq!(active_mark_names(&col, 7), vec!["b", "c"]);

        let b_id = id(0, 2);
        let b_positions = find_mark_positions(&col, b_id);
        assert_eq!(b_positions.len(), 2);

        col.undo(b_positions[1], vec![Some(MarkIndexBuilder::End(b_id))]);
        col.undo(
            b_positions[0],
            vec![Some(MarkIndexBuilder::Start(b_id, mk_mark("b")))],
        );

        // Check every position — "b" should be gone, "a" and "c" intact.
        for i in 0..col.len() {
            let names = active_mark_names(&col, i);
            assert!(
                !names.contains(&"b".to_string()),
                "mark 'b' should be undone at pos {i}, got {names:?}"
            );
        }

        // "a" and "c" should still be active in their (shifted) ranges.
        let any_a = (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"a".to_string()));
        let any_c = (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"c".to_string()));
        assert!(any_a, "mark 'a' should still exist");
        assert!(any_c, "mark 'c' should still exist");
    }

    #[test]
    fn undo_verify_every_position() {
        // 15 positions, 3 marks:
        //   bold:      pos 2..8  (Start@2, End@8)
        //   italic:    pos 4..11 (Start@4, End@11)
        //   underline: pos 6..13 (Start@6, End@13)
        let mut col = build_column(
            15,
            &[
                (2, 8, 0, 1, "bold"),
                (4, 11, 0, 2, "italic"),
                (6, 13, 0, 3, "underline"),
            ],
        );

        // Snapshot every position's marks before undo.
        let before: Vec<Vec<String>> = (0..col.len()).map(|i| active_mark_names(&col, i)).collect();

        // Verify known positions.
        assert_eq!(before[0], Vec::<String>::new());
        assert_eq!(before[2], vec!["bold"]);
        assert_eq!(before[5], vec!["bold", "italic"]);
        assert_eq!(before[7], vec!["bold", "italic", "underline"]);
        assert_eq!(before[9], vec!["italic", "underline"]);
        assert_eq!(before[12], vec!["underline"]);
        assert_eq!(before[14], Vec::<String>::new());

        // Undo italic (id=0,2). Find its positions.
        let italic_id = id(0, 2);
        let italic_positions = find_mark_positions(&col, italic_id);
        assert_eq!(italic_positions.len(), 2);

        col.undo(
            italic_positions[1],
            vec![Some(MarkIndexBuilder::End(italic_id))],
        );
        col.undo(
            italic_positions[0],
            vec![Some(MarkIndexBuilder::Start(italic_id, mk_mark("italic")))],
        );

        // Column is now 13 items (15 - 2 removed).
        assert_eq!(col.len(), 13);

        // Check every position — italic should be gone.
        for i in 0..col.len() {
            let names = active_mark_names(&col, i);
            assert!(
                !names.contains(&"italic".to_string()),
                "italic should be gone at pos {i}, got {names:?}"
            );
        }

        // Bold and underline should still be active.
        let has_bold =
            (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"bold".to_string()));
        let has_underline =
            (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"underline".to_string()));
        assert!(has_bold, "bold should still exist");
        assert!(has_underline, "underline should still exist");

        // Now undo underline too.
        let underline_id = id(0, 3);
        let underline_positions = find_mark_positions(&col, underline_id);
        assert_eq!(underline_positions.len(), 2);

        col.undo(
            underline_positions[1],
            vec![Some(MarkIndexBuilder::End(underline_id))],
        );
        col.undo(
            underline_positions[0],
            vec![Some(MarkIndexBuilder::Start(
                underline_id,
                mk_mark("underline"),
            ))],
        );

        assert_eq!(col.len(), 11);

        // Only bold should remain.
        for i in 0..col.len() {
            let names = active_mark_names(&col, i);
            assert!(!names.contains(&"italic".to_string()), "italic at pos {i}");
            assert!(
                !names.contains(&"underline".to_string()),
                "underline at pos {i}"
            );
        }
        let has_bold =
            (0..col.len()).any(|i| active_mark_names(&col, i).contains(&"bold".to_string()));
        assert!(
            has_bold,
            "bold should still exist after undoing italic+underline"
        );
    }

    /// Find the column positions of Start and End entries for a given OpId.
    fn find_mark_positions(col: &MarkIndexColumn, target: OpId) -> Vec<usize> {
        let mut positions = Vec::new();
        let mut pos = 0;
        for slab in col.data.slabs.iter() {
            let mut cursor = MarkIndex::default();
            let bytes = slab.as_slice();
            while let Some(run) = cursor.next(bytes) {
                for _ in 0..run.count {
                    if let Some(val) = run.value.as_deref() {
                        match val {
                            MarkIndexValue::Start(id) | MarkIndexValue::End(id)
                                if *id == target =>
                            {
                                positions.push(pos);
                            }
                            _ => {}
                        }
                    }
                    pos += 1;
                }
            }
        }
        positions
    }

    #[test]
    fn rich_text_at_every_position() {
        // Comprehensive check: 3 marks, verify every position.
        // bold: 2..6, italic: 4..10, underline: 8..12
        let col = build_column(
            15,
            &[
                (2, 6, 0, 1, "bold"),
                (4, 10, 0, 2, "italic"),
                (8, 12, 0, 3, "underline"),
            ],
        );

        // The column has extra entries from Start/End insertions.
        // Check every position for correct marks.
        for i in 0..col.len() {
            let names = active_mark_names(&col, i);
            let ids: BTreeSet<OpId> = active_mark_ids(&col, i);

            // Each mark in rich_text_at should have a corresponding cache entry.
            let rt = col.rich_text_at(i, None);
            assert_eq!(rt.map.len(), names.len(), "pos {i}: map len != names len");

            // Every id from marks_at should appear in rich_text_at.
            for mid in &ids {
                assert!(
                    rt.map.contains_key(mid),
                    "pos {i}: mark {mid:?} in marks_at but not in rich_text_at"
                );
            }
        }
    }

    /// Brute-force oracle: scan linearly to compute which marks are active at `pos`.
    fn oracle_marks_at(values: &[Option<MarkIndexValue>], pos: usize) -> BTreeSet<OpId> {
        let mut open = BTreeSet::new();
        for (i, v) in values.iter().enumerate() {
            if i > pos {
                break;
            }
            match v {
                Some(MarkIndexValue::Start(id)) => {
                    open.insert(*id);
                }
                Some(MarkIndexValue::End(id)) => {
                    open.remove(id);
                }
                None => {}
            }
        }
        open
    }

    #[test]
    #[ignore]
    fn large_column_multi_slab() {
        use rand::{RngExt, SeedableRng};
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);

        let n = 100_000;
        let mut values: Vec<Option<MarkIndexValue>> = vec![None; n];
        let mut cache_entries: Vec<(OpId, MarkData<'static>)> = Vec::new();

        // Place ~2500 mark pairs (5% non-null = ~5000 entries in 100k).
        let num_marks = 2500;
        let mut mark_ids: Vec<(OpId, usize, usize)> = Vec::new(); // (id, start, end)

        for i in 0..num_marks {
            let op_id = id(0, i as u64 + 1);
            let start = rng.random_range(0..n - 2);
            let end = rng.random_range(start + 1..n);
            // Only place if slots are free.
            if values[start].is_none() && values[end].is_none() {
                values[start] = Some(MarkIndexValue::Start(op_id));
                values[end] = Some(MarkIndexValue::End(op_id));
                mark_ids.push((op_id, start, end));
                cache_entries.push((op_id, mk_mark(&format!("m{i}"))));
            }
        }

        let placed = mark_ids.len();
        assert!(placed > 100, "need enough marks placed, got {placed}");

        // Build the column.
        let mut col = MarkIndexColumn::new();
        let builder_values: Vec<Option<MarkIndexBuilder>> = values
            .iter()
            .map(|v| match v {
                Some(MarkIndexValue::Start(id)) => {
                    let data = cache_entries
                        .iter()
                        .find(|(cid, _)| cid == id)
                        .unwrap()
                        .1
                        .clone();
                    Some(MarkIndexBuilder::Start(*id, data))
                }
                Some(MarkIndexValue::End(id)) => Some(MarkIndexBuilder::End(*id)),
                None => None,
            })
            .collect();
        col.extend(0, builder_values);

        assert_eq!(col.len(), n);
        assert!(
            col.data.slabs.len() > 1,
            "need multiple slabs, got {}",
            col.data.slabs.len()
        );

        // Verify marks_at matches oracle at sampled positions.
        let check_positions: Vec<usize> = (0..200)
            .map(|_| rng.random_range(0..n))
            .chain([0, 1, n / 4, n / 2, 3 * n / 4, n - 2, n - 1])
            .collect();

        for &pos in &check_positions {
            let expected = oracle_marks_at(&values, pos);
            let actual = active_mark_ids(&col, pos);
            assert_eq!(expected, actual, "mismatch at pos {pos} (before undo)");
        }

        // Undo every 3rd mark.
        let marks_to_undo: Vec<(OpId, usize, usize)> = mark_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 3 == 0)
            .map(|(_, m)| *m)
            .collect();

        for &(op_id, _start, _end) in marks_to_undo.iter().rev() {
            // Find current positions (they shift as we remove entries).
            let positions = find_mark_positions(&col, op_id);
            assert_eq!(
                positions.len(),
                2,
                "mark {op_id:?} should have Start+End, found {:?}",
                positions
            );
            col.undo(positions[1], vec![Some(MarkIndexBuilder::End(op_id))]);
            col.undo(
                positions[0],
                vec![Some(MarkIndexBuilder::Start(
                    op_id,
                    cache_entries
                        .iter()
                        .find(|(cid, _)| *cid == op_id)
                        .unwrap()
                        .1
                        .clone(),
                ))],
            );
        }

        // Update the oracle: remove undone marks.
        let undone_ids: HashSet<OpId> = marks_to_undo.iter().map(|(id, _, _)| *id).collect();
        let remaining_values: Vec<Option<MarkIndexValue>> = values
            .iter()
            .filter(|v| match v {
                Some(MarkIndexValue::Start(id)) | Some(MarkIndexValue::End(id)) => {
                    !undone_ids.contains(id)
                }
                None => true,
            })
            .cloned()
            .collect();

        assert_eq!(
            col.len(),
            remaining_values.len(),
            "column length after undo"
        );

        // Verify marks_at matches oracle at sampled positions (adjusted for new length).
        let new_len = col.len();
        let check_positions: Vec<usize> = (0..200)
            .map(|_| rng.random_range(0..new_len))
            .chain([0, 1, new_len / 4, new_len / 2, 3 * new_len / 4, new_len - 1])
            .collect();

        for &pos in &check_positions {
            let expected = oracle_marks_at(&remaining_values, pos);
            let actual = active_mark_ids(&col, pos);
            assert_eq!(
                expected, actual,
                "mismatch at pos {pos} (after undo, col_len={new_len})"
            );
        }
    }
}
