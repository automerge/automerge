use crate::exid::ExId;
use crate::op_tree::{OpSetMetadata, OpTree, OpTreeNode};
use crate::types::{
    Clock, Counter, Key, ListEncoding, Op, OpId, OpType, ScalarValue, TextEncoding,
};
use fxhash::FxBuildHasher;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

mod attribute;
mod attribute2;
mod elem_id_pos;
mod insert;
mod keys;
mod keys_at;
mod len;
mod len_at;
mod list_range;
mod list_range_at;
mod list_vals;
mod list_vals_at;
mod map_range;
mod map_range_at;
mod nth;
mod nth_at;
mod opid;
mod opid_vis;
mod prop;
mod prop_at;
mod raw_spans;
mod seek_mark;
mod seek_op;
mod seek_op_with_patch;
mod spans;

pub(crate) use attribute::{Attribute, ChangeSet};
pub(crate) use attribute2::{Attribute2, ChangeSet2};
pub(crate) use elem_id_pos::ElemIdPos;
pub(crate) use insert::InsertNth;
pub(crate) use keys::Keys;
pub(crate) use keys_at::KeysAt;
pub(crate) use len::Len;
pub(crate) use len_at::LenAt;
pub(crate) use list_range::ListRange;
pub(crate) use list_range_at::ListRangeAt;
pub(crate) use list_vals::ListVals;
pub(crate) use list_vals_at::ListValsAt;
pub(crate) use map_range::MapRange;
pub(crate) use map_range_at::MapRangeAt;
pub(crate) use nth::Nth;
pub(crate) use nth_at::NthAt;
pub(crate) use opid::OpIdSearch;
pub(crate) use opid_vis::OpIdVisSearch;
pub(crate) use prop::Prop;
pub(crate) use prop_at::PropAt;
pub(crate) use raw_spans::RawSpans;
pub(crate) use seek_mark::SeekMark;
pub(crate) use seek_op::SeekOp;
pub(crate) use seek_op_with_patch::SeekOpWithPatch;
pub(crate) use spans::{Span, Spans};

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SpanInfo {
    pub id: ExId,
    pub start: usize,
    pub end: usize,
    pub key: String,
    pub value: ScalarValue,
}

// use a struct for the args for clarity as they are passed up the update chain in the optree
#[derive(Debug, Clone)]
pub(crate) struct ChangeVisibility<'a> {
    pub(crate) old_vis: bool,
    pub(crate) new_vis: bool,
    pub(crate) op: &'a Op,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
}

pub(crate) trait TreeQuery<'a>: Clone + Debug {
    fn equiv(&mut self, _other: &Self) -> bool {
        false
    }

    fn can_shortcut_search(&mut self, _tree: &'a OpTree) -> bool {
        false
    }

    #[inline(always)]
    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        _m: &OpSetMetadata,
        ops: &[Op],
    ) -> QueryResult {
        self.query_node(child, ops)
    }

    fn query_node(&mut self, _child: &'a OpTreeNode, _ops: &[Op]) -> QueryResult {
        QueryResult::Descend
    }

    #[inline(always)]
    fn query_element_with_metadata(&mut self, element: &'a Op, _m: &OpSetMetadata) -> QueryResult {
        self.query_element(element)
    }

    fn query_element(&mut self, _element: &'a Op) -> QueryResult {
        panic!("invalid element query")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryResult {
    Next,
    /// Skip this many elements, only allowed from the root node.
    Skip(usize),
    Descend,
    Finish,
}

#[derive(Clone, Debug, PartialEq)]
struct TextWidth {
    utf8: usize,
    utf16: usize,
}

impl TextWidth {
    fn add_op(&mut self, op: &Op) {
        self.utf8 += op.width(ListEncoding::Text(TextEncoding::Utf8));
        self.utf16 += op.width(ListEncoding::Text(TextEncoding::Utf16));
    }

    fn remove_op(&mut self, op: &Op) {
        // Why are we using saturating_sub here? Shouldn't this always be greater than 0?
        //
        // In the case of objects which are _not_ `Text` we may end up subtracting more than the
        // current width. This can happen if the elements in a list are `ScalarValue::str` and
        // there are conflicting elements for the same index in the list. Like so:
        //
        // ```notrust
        // [
        //     "element",
        //     ["conflict1", "conflict2_longer"],
        //     "element"
        // ]
        // ```
        //
        // Where there are two conflicted elements at index 1
        //
        // in `Index::insert` and `Index::change_visibility` we add the width of the inserted op in
        // utf8 and utf16 to the current width, but only if there was not a previous element for
        // that index. Imagine that we encounter the "conflict1" op first, then we will add the
        // length of 'conflict1' to the text widths. When 'conflict2_longer' is added we don't do
        // anything because we've already seen an op for this index. Imagine that later we remove
        // the `conflict2_longer` op, then we will end up subtracting the length of
        // 'conflict2_longer' from the text widths, hence, `saturating_sub`. This isn't a problem
        // because for non text objects we don't need the text widths to be accurate anyway.
        //
        // Really this is a sign that we should be tracking the type of the Index (List or Text) at
        // the type level, but for now we just look the other way.
        self.utf8 = self
            .utf8
            .saturating_sub(op.width(ListEncoding::Text(TextEncoding::Utf8)));
        self.utf16 = self
            .utf16
            .saturating_sub(op.width(ListEncoding::Text(TextEncoding::Utf16)));
    }

    fn merge(&mut self, other: &TextWidth) {
        self.utf8 += other.utf8;
        self.utf16 += other.utf16;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Index {
    /// The map of visible keys to the number of visible operations for that key.
    visible: HashMap<Key, usize, FxBuildHasher>,
    visible_text: TextWidth,
    /// Set of opids found in this node and below.
    ops: HashSet<OpId, FxBuildHasher>,
}

impl Index {
    pub(crate) fn new() -> Self {
        Index {
            visible: Default::default(),
            visible_text: TextWidth { utf8: 0, utf16: 0 },
            ops: Default::default(),
        }
    }

    /// Get the number of visible elements in this index.
    pub(crate) fn visible_len(&self, encoding: ListEncoding) -> usize {
        match encoding {
            ListEncoding::List => self.visible.len(),
            ListEncoding::Text(TextEncoding::Utf8) => self.visible_text.utf8,
            ListEncoding::Text(TextEncoding::Utf16) => self.visible_text.utf16,
        }
    }

    pub(crate) fn has_visible(&self, seen: &Key) -> bool {
        self.visible.contains_key(seen)
    }

    /// Whether `opid` is in this node or any below it
    pub(crate) fn has_op(&self, opid: &OpId) -> bool {
        self.ops.contains(opid)
    }

    pub(crate) fn change_vis<'a>(
        &mut self,
        change_vis: ChangeVisibility<'a>,
    ) -> ChangeVisibility<'a> {
        let ChangeVisibility {
            old_vis,
            new_vis,
            op,
        } = &change_vis;
        let key = op.elemid_or_key();
        match (old_vis, new_vis) {
            (true, false) => match self.visible.get(&key).copied() {
                Some(n) if n == 1 => {
                    self.visible.remove(&key);
                    self.visible_text.remove_op(op);
                }
                Some(n) => {
                    self.visible.insert(key, n - 1);
                }
                None => panic!("remove overun in index"),
            },
            (false, true) => {
                if let Some(n) = self.visible.get(&key) {
                    self.visible.insert(key, n + 1);
                } else {
                    self.visible.insert(key, 1);
                    self.visible_text.add_op(op);
                }
            }
            _ => {}
        }
        change_vis
    }

    pub(crate) fn insert(&mut self, op: &Op) {
        self.ops.insert(op.id);
        if op.visible() {
            let key = op.elemid_or_key();
            if let Some(n) = self.visible.get(&key) {
                self.visible.insert(key, n + 1);
            } else {
                self.visible.insert(key, 1);
                self.visible_text.add_op(op);
            }
        }
    }

    pub(crate) fn remove(&mut self, op: &Op) {
        self.ops.remove(&op.id);
        if op.visible() {
            let key = op.elemid_or_key();
            match self.visible.get(&key).copied() {
                Some(n) if n == 1 => {
                    self.visible.remove(&key);
                    self.visible_text.remove_op(op);
                }
                Some(n) => {
                    self.visible.insert(key, n - 1);
                }
                None => panic!("remove overun in index"),
            }
        }
    }

    pub(crate) fn merge(&mut self, other: &Index) {
        for id in &other.ops {
            self.ops.insert(*id);
        }
        for (elem, other_len) in other.visible.iter() {
            self.visible
                .entry(*elem)
                .and_modify(|len| *len += *other_len)
                .or_insert(*other_len);
        }
        self.visible_text.merge(&other.visible_text);
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct VisWindow {
    counters: HashMap<OpId, CounterData>,
}

impl VisWindow {
    fn visible_at(&mut self, op: &Op, pos: usize, clock: &Clock) -> bool {
        if !clock.covers(&op.id) {
            return false;
        }

        let mut visible = false;
        match op.action {
            OpType::Put(ScalarValue::Counter(Counter { start, .. })) => {
                self.counters.insert(
                    op.id,
                    CounterData {
                        pos,
                        val: start,
                        succ: op.succ.into_iter().cloned().collect(),
                        op: op.clone(),
                    },
                );
                if !op.succ.into_iter().any(|i| clock.covers(i)) {
                    visible = true;
                }
            }
            OpType::Increment(inc_val) => {
                for id in &op.pred {
                    // pred is always before op.id so we can see them
                    if let Some(mut entry) = self.counters.get_mut(id) {
                        entry.succ.remove(&op.id);
                        entry.val += inc_val;
                        entry.op.action = OpType::Put(ScalarValue::counter(entry.val));
                        if !entry.succ.iter().any(|i| clock.covers(i)) {
                            visible = true;
                        }
                    }
                }
            }
            _ => {
                if !op.succ.into_iter().any(|i| clock.covers(i)) {
                    visible = true;
                }
            }
        };
        visible
    }

    pub(crate) fn seen_op(&self, op: &Op, pos: usize) -> Vec<(usize, Op)> {
        let mut result = vec![];
        for pred in &op.pred {
            if let Some(entry) = self.counters.get(pred) {
                result.push((entry.pos, entry.op.clone()));
            }
        }
        if result.is_empty() {
            result.push((pos, op.clone()));
        }
        result
    }
}

pub(crate) fn binary_search_by<F>(node: &OpTreeNode, ops: &[Op], f: F) -> usize
where
    F: Fn(&Op) -> Ordering,
{
    let mut right = node.len();
    let mut left = 0;
    while left < right {
        let seq = (left + right) / 2;
        if f(&ops[node.get(seq).unwrap()]) == Ordering::Less {
            left = seq + 1;
        } else {
            right = seq;
        }
    }
    left
}
