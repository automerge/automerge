use crate::clock::Clock;
use crate::marks::MarkData;
use crate::op_set::{Op, OpSetData};
use crate::op_tree::{LastInsert, OpTreeNode};
use crate::query::{Index, QueryResult};
use crate::types::{Key, ListEncoding, OpId, OpType};
use crate::ObjType;
use fxhash::FxBuildHasher;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RichTextQueryState<'a> {
    map: HashMap<OpId, &'a MarkData, FxBuildHasher>,
    block: Option<OpId>,
}

impl<'a> RichTextQueryState<'a> {
    pub(crate) fn process(&mut self, op: Op<'a>, clock: Option<&Clock>) {
        if !(clock.map(|c| c.covers(op.id())).unwrap_or(true)) {
            // if the op is not visible in the current clock
            // we can ignore it
            return;
        }
        match op.action() {
            OpType::MarkBegin(_, data) => {
                self.map.insert(*op.id(), data);
            }
            OpType::MarkEnd(_) => {
                self.map.remove(&op.id().prev());
            }
            OpType::Make(ObjType::Map) => {
                self.block = Some(*op.id());
            }
            _ => {}
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&OpId, &&MarkData)> {
        self.map.iter()
    }

    pub(crate) fn insert(&mut self, op: OpId, data: &'a MarkData) {
        self.map.insert(op, data);
    }

    pub(crate) fn remove(&mut self, op: &OpId) {
        self.map.remove(op);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ListState {
    encoding: ListEncoding,
    last_seen: Option<Key>,
    last_width: usize,
    never_seen_puts: bool,
    target: usize,
    index: usize,
    pos: usize,
}

// There are two indexes being tracked in lists
// pos: this is the position in the opset.  A list of 100 items may have 1000 ops.  Each op has a position
// index: this is the logical index of the list of visible values.  Conflicted items will have the same index.
//        The index is affected by utf8/utf16 encoding for text, etc

impl ListState {
    pub(crate) fn new(encoding: ListEncoding, target: usize) -> Self {
        ListState {
            encoding,
            target,
            last_seen: None,
            last_width: 0,
            index: 0,
            pos: 0,
            never_seen_puts: true,
        }
    }

    pub(crate) fn was_last_seen(&self, key: Key) -> bool {
        self.last_seen == Some(key)
    }

    // lists that have never seen puts (only inserts and deletes)
    // can take advantage of a faster codepath
    pub(crate) fn check_if_node_is_clean(&mut self, index: &Index) {
        self.never_seen_puts &= index.has_never_seen_puts();
    }

    pub(crate) fn process_node<'a>(
        &mut self,
        node: &'a OpTreeNode,
        index: &'a Index,
        osd: &OpSetData,
        marks: Option<&mut RichTextQueryState<'a>>,
    ) -> QueryResult {
        if self.encoding == ListEncoding::List {
            self.process_list_node(node, index, osd, marks)
        } else if self.never_seen_puts {
            // text node is clean - use the indexes
            self.process_text_node(node, index, marks)
        } else {
            // text nodes are intended to only be interacted with splice()
            // meaning all ops are inserts or deleted inserts
            // the indexes are written with this assumption in mind
            // if conflicted put()'s with different character widths exist
            // we cannot trust the indexs and need to descend
            QueryResult::Descend
        }
    }

    fn process_marks<'a>(&mut self, index: &'a Index, marks: Option<&mut RichTextQueryState<'a>>) {
        if let Some(marks) = marks {
            for (id, data) in index.mark_begin.iter() {
                marks.insert(*id, data);
            }
            for id in index.mark_end.iter() {
                marks.remove(&id.prev());
            }
        }
    }

    fn process_text_node<'a>(
        &mut self,
        node: &'a OpTreeNode,
        index: &'a Index,
        marks: Option<&mut RichTextQueryState<'a>>,
    ) -> QueryResult {
        let num_vis = index.visible_len(self.encoding);
        if self.index + num_vis >= self.target {
            return QueryResult::Descend;
        }
        self.index += num_vis;
        self.pos += node.len();
        self.process_marks(index, marks);
        QueryResult::Next
    }

    fn process_list_node<'a>(
        &mut self,
        node: &'a OpTreeNode,
        index: &'a Index,
        osd: &OpSetData,
        marks: Option<&mut RichTextQueryState<'a>>,
    ) -> QueryResult {
        let mut num_vis = index.visible.len();
        if let Some(last_seen) = self.last_seen {
            // the elemid `last_seen` is counted in this node's index
            // but since we've already seen it we dont want to count it again
            // as this is only for lists we can subtract 1
            if index.has_visible(&last_seen) {
                num_vis -= 1;
            }
        }

        if self.index + num_vis >= self.target {
            // if we've reached out target - decend
            return QueryResult::Descend;
        }

        self.index += num_vis;
        self.pos += node.len();

        // scenario 1: the last elemid is in the index - record it
        // scenario 2: the last elemid this node is not in the index
        // scenario 2a: and its different than the previous elemid - last_seen = None
        //              as there is no possability of visible elements spanning the
        //              node boundary
        // scenario 2b: it is the same as the previous last_seen - do nothing

        let last_elemid = node.last().as_op(osd).elemid_or_key();
        if index.has_visible(&last_elemid) {
            self.last_seen = Some(last_elemid);
        } else if self.last_seen.is_some() && Some(last_elemid) != self.last_seen {
            self.last_seen = None;
        }
        self.process_marks(index, marks);
        QueryResult::Next
    }

    pub(crate) fn process_op(&mut self, op: Op<'_>, current: Key, visible: bool) {
        if visible {
            if self.never_seen_puts {
                // clean sequnces are simple - only insert and deletes
                self.last_width = op.width(self.encoding);
                self.index += self.last_width;
            } else {
                let current_width = op.width(self.encoding);
                if self.last_seen != Some(current) {
                    // new value - progess
                    self.last_width = current_width;
                    self.index += self.last_width;
                    self.last_seen = Some(current);
                } else if current_width != self.last_width {
                    // width is always 1 for lists so this
                    // will only trigger if there are conflicting unicode characters
                    // of different lengths
                    self.index = self.index + current_width - self.last_width;
                    self.last_width = current_width;
                }
            }
        }
        self.pos += 1;
    }

    pub(crate) fn target(&self) -> usize {
        self.target
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn last_index(&self) -> usize {
        self.index - self.last_width
    }

    pub(crate) fn done(&self) -> bool {
        self.index >= self.target
    }

    pub(crate) fn seek(&mut self, last: &LastInsert) {
        self.last_width = last.width;
        self.index = last.index + last.width;
        self.pos = last.pos + 1;
    }
}
