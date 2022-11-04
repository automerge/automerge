use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::types::{Clock, Counter, Key, Op, OpId, OpType, ScalarValue};
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

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
mod seek_op;
mod seek_op_with_patch;

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
pub(crate) use seek_op::SeekOp;
pub(crate) use seek_op_with_patch::SeekOpWithPatch;

// use a struct for the args for clarity as they are passed up the update chain in the optree
#[derive(Debug, Clone)]
pub(crate) struct ReplaceArgs {
    pub(crate) old_id: OpId,
    pub(crate) new_id: OpId,
    pub(crate) old_visible: bool,
    pub(crate) new_visible: bool,
    pub(crate) new_key: Key,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
}

pub(crate) trait TreeQuery<'a> {
    #[inline(always)]
    fn query_node_with_metadata(
        &mut self,
        child: &'a OpTreeNode,
        _m: &OpSetMetadata,
    ) -> QueryResult {
        self.query_node(child)
    }

    fn query_node(&mut self, _child: &'a OpTreeNode) -> QueryResult {
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
pub(crate) struct Index {
    /// The map of visible keys to the number of visible operations for that key.
    pub(crate) visible: HashMap<Key, usize, FxBuildHasher>,
    /// Set of opids found in this node and below.
    pub(crate) ops: HashSet<OpId, FxBuildHasher>,
}

impl Index {
    pub(crate) fn new() -> Self {
        Index {
            visible: Default::default(),
            ops: Default::default(),
        }
    }

    /// Get the number of visible elements in this index.
    pub(crate) fn visible_len(&self) -> usize {
        self.visible.len()
    }

    pub(crate) fn has_visible(&self, seen: &Key) -> bool {
        self.visible.contains_key(seen)
    }

    pub(crate) fn replace(
        &mut self,
        ReplaceArgs {
            old_id,
            new_id,
            old_visible,
            new_visible,
            new_key,
        }: &ReplaceArgs,
    ) {
        if old_id != new_id {
            self.ops.remove(old_id);
            self.ops.insert(*new_id);
        }

        match (new_visible, old_visible, new_key) {
            (false, true, key) => match self.visible.get(key).copied() {
                Some(n) if n == 1 => {
                    self.visible.remove(key);
                }
                Some(n) => {
                    self.visible.insert(*key, n - 1);
                }
                None => panic!("remove overun in index"),
            },
            (true, false, key) => *self.visible.entry(*key).or_default() += 1,
            _ => {}
        }
    }

    pub(crate) fn insert(&mut self, op: &Op) {
        self.ops.insert(op.id);
        if op.visible() {
            *self.visible.entry(op.elemid_or_key()).or_default() += 1;
        }
    }

    pub(crate) fn remove(&mut self, op: &Op) {
        self.ops.remove(&op.id);
        if op.visible() {
            let key = op.elemid_or_key();
            match self.visible.get(&key).copied() {
                Some(n) if n == 1 => {
                    self.visible.remove(&key);
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
        for (elem, n) in other.visible.iter() {
            *self.visible.entry(*elem).or_default() += n;
        }
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

pub(crate) fn binary_search_by<F>(node: &OpTreeNode, f: F) -> usize
where
    F: Fn(&Op) -> Ordering,
{
    let mut right = node.len();
    let mut left = 0;
    while left < right {
        let seq = (left + right) / 2;
        if f(node.get(seq).unwrap()) == Ordering::Less {
            left = seq + 1;
        } else {
            right = seq;
        }
    }
    left
}
