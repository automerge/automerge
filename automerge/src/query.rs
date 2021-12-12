#![allow(dead_code)]

use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::{ElemId, ObjId, Op, OpId, ScalarValue};
use automerge_protocol as amp;
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

mod insert;
mod list_vals;
mod nth;
mod opid;
mod prop;
mod seek_op;

pub(crate) use insert::InsertNth;
pub(crate) use list_vals::ListVals;
pub(crate) use nth::Nth;
#[allow(unused_imports)]
pub(crate) use opid::OpIdQuery;
pub(crate) use prop::Prop;
#[allow(unused_imports)]
pub(crate) use seek_op::SeekOp;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
}

pub(crate) trait TreeQuery<const B: usize> {
    #[inline(always)]
    fn query_node_with_metadata(
        &mut self,
        child: &OpTreeNode<B>,
        _m: &OpSetMetadata,
    ) -> QueryResult {
        self.query_node(child)
    }

    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        panic!("invalid node query")
    }

    #[inline(always)]
    fn query_element_with_metadata(&mut self, element: &Op, _m: &OpSetMetadata) -> QueryResult {
        self.query_element(element)
    }

    fn query_element(&mut self, _element: &Op) -> QueryResult {
        panic!("invalid element query")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryResult {
    Next,
    Decend,
    Finish,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Index {
    pub visible: HashMap<ObjId, HashMap<ElemId, usize, FxBuildHasher>, FxBuildHasher>,
    pub lens: HashMap<ObjId, usize, FxBuildHasher>,
    pub ops: HashSet<OpId, FxBuildHasher>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            visible: Default::default(),
            lens: Default::default(),
            ops: Default::default(),
        }
    }

    pub fn has(&self, obj: &ObjId, e: &Option<ElemId>) -> bool {
        if let Some(seen) = e {
            if let Some(sub) = self.visible.get(obj) {
                return sub.contains_key(seen);
            }
        }
        false
    }

    pub fn insert(&mut self, op: &Op) {
        self.ops.insert(op.id);
        if op.succ.is_empty() {
            if let Some(elem) = op.elemid() {
                let sub = self.visible.entry(op.obj).or_default();
                match sub.get(&elem).copied() {
                    None => {
                        sub.insert(elem, 1);
                        self.lens.entry(op.obj).and_modify(|n| *n += 1).or_insert(1);
                    }
                    Some(n) => {
                        sub.insert(elem, n + 1);
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, op: &Op) {
        self.ops.remove(&op.id);
        if op.succ.is_empty() {
            let mut sub_empty = false;
            if let Some(elem) = op.elemid() {
                if let Some(c) = self.visible.get_mut(&op.obj) {
                    if let Some(d) = c.get(&elem).copied() {
                        if d == 1 {
                            c.remove(&elem);
                            self.lens.entry(op.obj).and_modify(|n| *n -= 1);
                            sub_empty = c.is_empty();
                        } else {
                            c.insert(elem, d - 1);
                        }
                    }
                }
            }
            if sub_empty {
                self.visible.remove(&op.obj);
                self.lens.remove(&op.obj);
            }
        }
    }

    pub fn merge(&mut self, other: &Index) {
        for id in &other.ops {
            self.ops.insert(*id);
        }
        for (obj, sub) in other.visible.iter() {
            let local_obj = self.visible.entry(*obj).or_default();
            for (elem, n) in sub.iter() {
                match local_obj.get(elem).cloned() {
                    None => {
                        local_obj.insert(*elem, 1);
                        self.lens.entry(*obj).and_modify(|o| *o += 1).or_insert(1);
                    }
                    Some(m) => {
                        local_obj.insert(*elem, m + n);
                    }
                }
            }
        }
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn visible_pos(op: &Op, pos: usize, counters: &HashMap<OpId, CounterData>) -> usize {
    for pred in &op.pred {
        if let Some(entry) = counters.get(pred) {
            return entry.pos;
        }
    }
    pos
}

pub(crate) fn is_visible(op: &Op, pos: usize, counters: &mut HashMap<OpId, CounterData>) -> bool {
    let mut visible = false;
    match op.action {
        amp::OpType::Set(amp::ScalarValue::Counter(val)) => {
            counters.insert(
                op.id,
                CounterData {
                    pos,
                    val,
                    succ: op.succ.iter().cloned().collect(),
                    op: op.clone(),
                },
            );
            if op.succ.is_empty() {
                visible = true;
            }
        }
        amp::OpType::Inc(inc_val) => {
            for id in &op.pred {
                if let Some(mut entry) = counters.get_mut(id) {
                    entry.succ.remove(&op.id);
                    entry.val += inc_val;
                    entry.op.action = amp::OpType::Set(ScalarValue::Counter(entry.val));
                    if entry.succ.is_empty() {
                        visible = true;
                    }
                }
            }
        }
        _ => {
            if op.succ.is_empty() {
                visible = true;
            }
        }
    };
    visible
}

pub(crate) fn visible_op(
    op: &Op,
    pos: usize,
    counters: &HashMap<OpId, CounterData>,
) -> Vec<(usize, Op)> {
    let mut result = vec![];
    for pred in &op.pred {
        if let Some(entry) = counters.get(pred) {
            result.push((entry.pos, entry.op.clone()));
        }
    }
    if result.is_empty() {
        vec![(pos, op.clone())]
    } else {
        result
    }
}

pub(crate) fn binary_search_by<F, const B: usize>(node: &OpTreeNode<B>, f: F) -> usize
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
