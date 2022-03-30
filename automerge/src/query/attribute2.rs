use crate::clock::Clock;
use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, Op};
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Attribute2<const B: usize> {
    pos: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    baseline: Clock,
    pub change_sets: Vec<ChangeSet2>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSet2 {
    clock: Clock,
    next_add: Option<CS2Add>,
    next_del: Option<CS2Del>,
    pub add: Vec<CS2Add>,
    pub del: Vec<CS2Del>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CS2Add {
    pub actor: usize,
    pub range: Range<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CS2Del {
    pub pos: usize,
    pub actor: usize,
    pub span: String,
}

impl From<Clock> for ChangeSet2 {
    fn from(clock: Clock) -> Self {
        ChangeSet2 {
            clock,
            next_add: None,
            next_del: None,
            add: Vec::new(),
            del: Vec::new(),
        }
    }
}

impl ChangeSet2 {
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

impl<const B: usize> Attribute2<B> {
    pub fn new(baseline: Clock, change_sets: Vec<Clock>) -> Self {
        Attribute2 {
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
                if let Some(CS2Add { range, actor }) = &mut cs.next_add {
                    if *actor == element.id.actor() {
                        range.end += 1;
                    } else {
                        cs.cut_add();
                        cs.next_add = Some(CS2Add {
                            actor: element.id.actor(),
                            range: Range {
                                start: self.seen,
                                end: self.seen + 1,
                            },
                        });
                    }
                } else {
                    cs.next_add = Some(CS2Add {
                        actor: element.id.actor(),
                        range: Range {
                            start: self.seen,
                            end: self.seen + 1,
                        },
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
            let succ: Vec<_> = element
                .succ
                .iter()
                .filter(|id| cs.clock.covers(id))
                .collect();
            // was deleted by change set
            if let Some(suc) = succ.get(0) {
                if let Some(s) = element.as_string() {
                    if let Some(CS2Del { actor, span, .. }) = &mut cs.next_del {
                        if suc.actor() == *actor {
                            span.push_str(&s);
                        } else {
                            cs.cut_del();
                            cs.next_del = Some(CS2Del {
                                pos: self.seen,
                                actor: suc.actor(),
                                span: s,
                            })
                        }
                    } else {
                        cs.next_del = Some(CS2Del {
                            pos: self.seen,
                            actor: suc.actor(),
                            span: s,
                        })
                    }
                }
            }
        }
    }

    pub fn finish(&mut self) {
        for cs in &mut self.change_sets {
            cs.cut_add();
            cs.cut_del();
        }
    }
}

impl<const B: usize> TreeQuery<B> for Attribute2<B> {
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
