use crate::marks::Mark;
use crate::op_tree::OpSetMetadata;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, MarkName, Op, OpId, OpType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekMark {
    /// the mark we are looking for
    id: OpId,
    end: usize,
    encoding: ListEncoding,
    found: bool,
    mark_name: MarkName,
    next_mark: Mark,
    pos: usize,
    seen: usize,
    last_seen: Option<Key>,
    super_marks: HashMap<OpId, MarkName>,
    pub(crate) marks: Vec<Mark>,
}

impl SeekMark {
    pub(crate) fn new(id: OpId, end: usize, encoding: ListEncoding) -> Self {
        SeekMark {
            id,
            encoding,
            end,
            found: false,
            next_mark: Default::default(),
            mark_name: MarkName::from_prop_index(0),
            pos: 0,
            seen: 0,
            last_seen: None,
            super_marks: Default::default(),
            marks: Default::default(),
        }
    }

    fn count_visible(&mut self, e: &Op) {
        if e.insert {
            self.last_seen = None
        }
        if e.visible() && self.last_seen.is_none() {
            self.seen += e.width(self.encoding);
            self.last_seen = Some(e.elemid_or_key())
        }
    }
}

impl TreeQuery<'_> for SeekMark {
    // this is missing an index - active marks
    /*
        fn query_node_with_metadata(
            &mut self,
            child: &OpTreeNode,
            _m: &OpSetMetadata,
            ops: &[Op],
        ) -> QueryResult {
            if self.found {
                log!("node found decend");
                QueryResult::Descend
            } else if child.index.ops.contains(&self.id) {
                log!("node contains decend");
                QueryResult::Descend
            } else {
                self.pos += child.len();

                let mut num_vis = child.index.visible_len(self.encoding);
                if num_vis > 0 {
                    if let Some(last_seen) = self.last_seen {
                        if child.index.has_visible(&last_seen) {
                            num_vis -= 1;
                        }
                    }
                    self.seen += num_vis;
                    self.last_seen = Some(ops[child.last()].elemid_or_key());
                }
                log!("node next");
                QueryResult::Next
            }
        }
    */

    fn query_element_with_metadata(&mut self, op: &Op, m: &OpSetMetadata) -> QueryResult {
        match &op.action {
            OpType::MarkBegin(mark) if op.id == self.id => {
                if !op.succ.is_empty() {
                    return QueryResult::Finish;
                }
                self.found = true;
                self.mark_name = mark.name;
                // retain the name and the value
                self.next_mark.name = m.props.get(mark.name.props_index()).clone();
                self.next_mark.value = mark.value.clone();
                // change id to the end id
                self.id = self.id.next();
                // begin a new mark if nothing supersedes us
                if self.super_marks.is_empty() {
                    self.next_mark.start = self.seen;
                }
                // remove all marks that dont match
                self.super_marks.retain(|_, v| v == &mark.name);
            }
            OpType::MarkBegin(mark) => {
                if m.lamport_cmp(op.id, self.id) == Ordering::Greater {
                    if self.found {
                        // gather marks of the same type that supersede us
                        if mark.name == self.mark_name {
                            self.super_marks.insert(op.id.next(), mark.name);
                            if self.super_marks.len() == 1 {
                                // complete a mark
                                self.next_mark.end = self.seen;
                                self.marks.push(self.next_mark.clone());
                            }
                        }
                    } else {
                        // gather all marks until we know what our mark's name is
                        self.super_marks.insert(op.id.next(), mark.name);
                    }
                }
            }
            OpType::MarkEnd(_) if self.end == self.pos => {
                if self.super_marks.is_empty() {
                    // complete a mark
                    self.next_mark.end = self.seen;
                    self.marks.push(self.next_mark.clone());
                }
                return QueryResult::Finish;
            }
            OpType::MarkEnd(_) if self.super_marks.contains_key(&op.id) => {
                self.super_marks.remove(&op.id);
                if self.found && self.super_marks.len() == 0 {
                    // begin a new mark
                    self.next_mark.start = self.seen;
                }
            }
            _ => {}
        }
        // the end op hasn't been inserted yet so we need to work off the position
        if self.end == self.pos {
            if self.super_marks.is_empty() {
                // complete a mark
                self.next_mark.end = self.seen;
                self.marks.push(self.next_mark.clone());
            }
            return QueryResult::Finish;
        }

        self.pos += 1;
        self.count_visible(&op);
        QueryResult::Next
    }
}
