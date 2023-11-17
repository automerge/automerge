use crate::marks::Mark;
use crate::op_set::Op;
use crate::op_tree::OpSetData;
use crate::query::{Index, ListState, OpTreeNode, QueryResult, TreeQuery};
use crate::types::{ListEncoding, OpId, OpType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekMark<'a> {
    /// the mark we are looking for
    idx: ListState,
    id: OpId,
    end: usize,
    found_begin: bool,
    found_end: bool,
    mark_name: smol_str::SmolStr,
    next_mark: Option<Mark<'a>>,
    super_marks: HashMap<OpId, smol_str::SmolStr>,
    marks: Vec<Mark<'a>>,
}

// should be able to use MarkStateMachine here now - FIXME

impl<'a> SeekMark<'a> {
    pub(crate) fn new(id: OpId, end: usize, encoding: ListEncoding) -> Self {
        SeekMark {
            idx: ListState::new(encoding, usize::MAX),
            id,
            end,
            found_begin: false,
            found_end: false,
            next_mark: None,
            mark_name: "".into(),
            super_marks: Default::default(),
            marks: Default::default(),
        }
    }

    // Called once the the query has finished to account for the situation where the op we are
    // inserting is a MarkEnd op at the end of the text sequence. In this case
    // query_element_with_metadata won't be called
    pub(crate) fn finish(mut self) -> Vec<Mark<'a>> {
        // If we searched every element in the sequence and we didn't find an end mark and there
        // are no superceding marks then we are inserting the MarkEnd as the last element in the
        // sequence.
        if self.idx.pos() == self.end && !self.found_end && self.super_marks.is_empty() {
            if let Some(next_mark) = &mut self.next_mark {
                next_mark.end = self.idx.index();
                self.marks.push(next_mark.clone());
            }
        }
        self.marks
    }
}

impl<'a> TreeQuery<'a> for SeekMark<'a> {
    fn query_node(
        &mut self,
        _child: &OpTreeNode,
        index: &Index,
        _osd: &'a OpSetData,
    ) -> QueryResult {
        self.idx.check_if_node_is_clean(index);
        QueryResult::Descend
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        match op.action() {
            OpType::MarkBegin(_, data) if op.id() == &self.id => {
                if op.succ().len() > 0 {
                    return QueryResult::Finish;
                }
                self.found_begin = true;
                self.mark_name = data.name.clone();
                // retain the name and the value
                self.next_mark = Some(Mark::from_data(self.idx.index(), self.idx.index(), data));
                // change id to the end id
                self.id = self.id.next();
                // remove all marks that dont match
                self.super_marks.retain(|_, v| v == &data.name);
            }
            OpType::MarkBegin(_, mark) => {
                if op.lamport_cmp(self.id) == Ordering::Greater {
                    if let Some(next_mark) = &mut self.next_mark {
                        // gather marks of the same type that supersede us
                        if mark.name == self.mark_name {
                            self.super_marks.insert(op.id().next(), mark.name.clone());
                            if self.super_marks.len() == 1 {
                                // complete a mark
                                next_mark.end = self.idx.index();
                                self.marks.push(next_mark.clone());
                            }
                        }
                    } else {
                        // gather all marks until we know what our mark's name is
                        self.super_marks.insert(op.id().next(), mark.name.clone());
                    }
                }
            }
            OpType::MarkEnd(_) if self.end == self.idx.pos() => {
                if self.super_marks.is_empty() {
                    // complete a mark
                    if let Some(next_mark) = &mut self.next_mark {
                        next_mark.end = self.idx.index();
                        self.marks.push(next_mark.clone());
                    }
                }
                self.found_end = true;
                return QueryResult::Finish;
            }
            OpType::MarkEnd(_) if self.super_marks.contains_key(op.id()) => {
                self.super_marks.remove(op.id());
                if let Some(next_mark) = &mut self.next_mark {
                    if self.super_marks.is_empty() {
                        // begin a new mark
                        next_mark.start = self.idx.index();
                    }
                }
            }
            _ => {}
        }
        // the end op hasn't been inserted yet so we need to work off the position
        if self.end == self.idx.pos() {
            self.found_end = true;
            if self.super_marks.is_empty() {
                // complete a mark
                if let Some(next_mark) = &mut self.next_mark {
                    next_mark.end = self.idx.index();
                    self.marks.push(next_mark.clone());
                }
            }
            return QueryResult::Finish;
        }

        let elemid = op.elemid_or_key();
        let visible = op.visible(); // why is this not *AT* FIXME
        self.idx.process_op(op, elemid, visible);
        QueryResult::Next
    }
}
