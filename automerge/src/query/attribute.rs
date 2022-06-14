use crate::clock::Clock;
use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, Op};
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Attribute {
    pos: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    baseline: Clock,
    pub(crate) change_sets: Vec<ChangeSet>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSet {
    clock: Clock,
    next_add: Option<Range<usize>>,
    next_del: Option<(usize, String)>,
    pub add: Vec<Range<usize>>,
    pub del: Vec<(usize, String)>,
}

impl From<Clock> for ChangeSet {
    fn from(clock: Clock) -> Self {
        ChangeSet {
            clock,
            next_add: None,
            next_del: None,
            add: Vec::new(),
            del: Vec::new(),
        }
    }
}

impl ChangeSet {
    fn cut_add(&mut self) {
        if let Some(add) = self.next_add.take() {
            self.add.push(add)
        }
    }

    fn cut_del(&mut self) {
        if let Some(del) = self.next_del.take() {
            self.del.push(del)
        }
    }
}

impl Attribute {
    pub(crate) fn new(baseline: Clock, change_sets: Vec<Clock>) -> Self {
        Attribute {
            pos: 0,
            seen: 0,
            last_seen: None,
            baseline,
            change_sets: change_sets.into_iter().map(|c| c.into()).collect(),
        }
    }

    fn update_add(&mut self, element: &Op) {
        let baseline = self.baseline.covers(&element.id);
        for cs in &mut self.change_sets {
            if !baseline && cs.clock.covers(&element.id) {
                // is part of the change_set
                if let Some(range) = &mut cs.next_add {
                    range.end += 1;
                } else {
                    cs.next_add = Some(Range {
                        start: self.seen,
                        end: self.seen + 1,
                    });
                }
            } else {
                cs.cut_add();
            }
            cs.cut_del();
        }
    }

    // id is in baseline
    // succ is not in baseline but is in cs

    fn update_del(&mut self, element: &Op) {
        if !self.baseline.covers(&element.id)
            || element.succ.iter().any(|id| self.baseline.covers(id))
        {
            return;
        }
        for cs in &mut self.change_sets {
            if element.succ.iter().any(|id| cs.clock.covers(id)) {
                // was deleted by change set
                if let Some(s) = element.as_string() {
                    if let Some((_, span)) = &mut cs.next_del {
                        span.push_str(&s);
                    } else {
                        cs.next_del = Some((self.seen, s))
                    }
                }
            }
        }
    }

    pub(crate) fn finish(&mut self) {
        for cs in &mut self.change_sets {
            cs.cut_add();
            cs.cut_del();
        }
    }
}

impl<'a> TreeQuery<'a> for Attribute {
    fn query_element_with_metadata(&mut self, element: &Op, _m: &OpSetMetadata) -> QueryResult {
        if element.insert {
            self.last_seen = None;
        }
        if self.last_seen.is_none() && element.visible() {
            self.update_add(element);
            self.seen += 1;
            self.last_seen = element.elemid();
        }
        if !element.succ.is_empty() {
            self.update_del(element);
        }
        self.pos += 1;
        QueryResult::Next
    }
}
