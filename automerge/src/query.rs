#![allow(dead_code)]

use crate::op_tree::{OpTreeNode, QueryResult, TreeQuery};
use crate::{ElemId, ObjId, Op, OpId, ScalarValue};
use automerge_protocol as amp;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpIdQuery {
    target: OpId,
    index: usize,
    finish: bool,
    pub ops: Vec<Op>,
    counters: HashMap<OpId, CounterData>,
}

impl OpIdQuery {
    pub fn new(target: OpId) -> Self {
        OpIdQuery {
            target,
            index: 0,
            finish: false,
            ops: vec![],
            counters: HashMap::new(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for OpIdQuery {
    fn query_child(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        if child.index.ops.contains(&self.target) {
            QueryResult::Decend
        } else {
            self.index += child.len();
            QueryResult::Next
        }
    }

    fn done(&self) -> bool {
        self.finish
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        if element.id == self.target {
            self.finish = true;
            QueryResult::Finish
        } else {
            self.index += 1;
            QueryResult::Next
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NthQuery {
    obj: ObjId,
    target: usize,
    index: usize,
    seen: usize,
    pub insert: usize,
    pub ops: Vec<Op>,
    last_seen: Option<ElemId>,
    counters: HashMap<OpId, CounterData>,
    pub element_seeks: usize,
    pub child_seeks: usize,
}

impl NthQuery {
    pub fn new(obj: ObjId, target: usize) -> Self {
        NthQuery {
            obj,
            target,
            index: 0,
            seen: 0,
            last_seen: None,
            ops: vec![],
            insert: 0,
            counters: HashMap::new(),
            element_seeks: 0,
            child_seeks: 0,
        }
    }
}

impl<const B: usize> TreeQuery<B> for NthQuery {
    fn query_child(&mut self, child: &OpTreeNode<B>) -> QueryResult {
        self.child_seeks += 1;
        let index = &child.index;
        if let Some(mut num_vis) = index.lens.get(&self.obj).copied() {
            // num vis is the number of keys in the index
            // minus one if we're counting last_seen
            //let mut num_vis = s.keys().count();
            if let Some(true) = self.last_seen.map(|seen| {
                index
                    .visible
                    .get(&self.obj)
                    .map(|sub| sub.contains_key(&seen))
                    .unwrap_or(false)
            }) {
                num_vis -= 1;
            }
            if self.seen + num_vis >= self.target {
                QueryResult::Decend
            } else {
                self.index += child.len();
                self.seen += num_vis;
                self.last_seen = child.get(child.len() - 1).and_then(|op| op.elemid());
                QueryResult::Next
            }
        } else {
            QueryResult::Next
        }
    }

    fn done(&self) -> bool {
        self.seen > self.target
    }

    //fn query_span(&mut self, index: &Index, span: &Span) -> QueryResult {
    //}

    fn query_element(&mut self, element: &Op) -> QueryResult {
        self.element_seeks += 1;
        self.index += 1;
        if element.obj != self.obj {
            return if self.seen > self.target {
                QueryResult::Finish
            } else {
                QueryResult::Next
            };
        }
        if element.insert {
            if self.seen > self.target {
                return QueryResult::Finish;
            };
            self.insert = self.index - 1;
            self.last_seen = None
        }
        let visible = is_visible(element, self.index - 1, &mut self.counters);
        if visible && self.last_seen.is_none() {
            self.seen += 1;
            self.last_seen = element.elemid()
        }
        if self.seen == self.target + 1 && visible {
            let vop = visible_op(element, &self.counters);
            self.ops.push(vop)
        }
        QueryResult::Next
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
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

pub(crate) fn visible_op(op: &Op, counters: &HashMap<OpId, CounterData>) -> Op {
    for pred in &op.pred {
        if let Some(entry) = counters.get(pred) {
            return entry.op.clone();
        }
    }
    op.clone()
}
