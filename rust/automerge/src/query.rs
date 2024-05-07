use crate::marks::MarkData;
use crate::op_set::Op;
use crate::op_tree::{OpSetData, OpTree, OpTreeNode};
use crate::types::{Key, ListEncoding, OpId, OpType};
use fxhash::FxBuildHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

mod insert;
mod list_state;
mod nth;
mod opid;
mod seek_mark;

pub(crate) use insert::InsertNth;
pub(crate) use list_state::{ListState, RichTextQueryState};
pub(crate) use nth::Nth;
pub(crate) use opid::{OpIdSearch, SimpleOpIdSearch};
pub(crate) use seek_mark::SeekMark;

// use a struct for the args for clarity as they are passed up the update chain in the optree
#[derive(Debug, Clone)]
pub(crate) struct ChangeVisibility<'a> {
    pub(crate) old_vis: bool,
    pub(crate) new_vis: bool,
    pub(crate) op: Op<'a>,
}

pub(crate) trait TreeQuery<'a>: Clone + Debug {
    fn can_shortcut_search(&mut self, _tree: &'a OpTree, _osd: &'a OpSetData) -> bool {
        false
    }

    fn query_node(
        &mut self,
        _child: &'a OpTreeNode,
        _index: &'a Index,
        _osd: &'a OpSetData,
    ) -> QueryResult {
        QueryResult::Descend
    }

    fn query_element(&mut self, _op: Op<'a>) -> QueryResult {
        panic!("invalid element query")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryResult {
    Next,
    Descend,
    Finish,
}

#[derive(Clone, Debug, PartialEq)]
struct TextWidth {
    width: usize,
}

impl TextWidth {
    fn add_op(&mut self, op: Op<'_>) {
        self.width += op.width(ListEncoding::Text);
    }

    fn remove_op(&mut self, op: Op<'_>) {
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
        self.width = self.width.saturating_sub(op.width(ListEncoding::Text));
    }

    fn merge(&mut self, other: &TextWidth) {
        self.width += other.width;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Index {
    /// The map of visible keys to the number of visible operations for that key.
    visible: HashMap<Key, usize, FxBuildHasher>,
    visible_text: TextWidth,
    /// Set of opids found in this node and below.
    ops: HashSet<OpId, FxBuildHasher>,
    never_seen_puts: bool,
    mark_begin: HashMap<OpId, MarkData, FxBuildHasher>,
    mark_end: Vec<OpId>,
    /// The ID of the last block in this index, if any
    pub(crate) block: Option<OpId>,
}

impl Index {
    pub(crate) fn has_never_seen_puts(&self) -> bool {
        self.never_seen_puts
    }

    pub(crate) fn new() -> Self {
        Index {
            visible: Default::default(),
            visible_text: TextWidth { width: 0 },
            ops: Default::default(),
            never_seen_puts: true,
            mark_begin: Default::default(),
            mark_end: Default::default(),
            block: None,
        }
    }

    /// Get the number of visible elements in this index.
    pub(crate) fn visible_len(&self, encoding: ListEncoding) -> usize {
        match encoding {
            ListEncoding::List => self.visible.len(),
            ListEncoding::Text => self.visible_text.width,
        }
    }

    pub(crate) fn has_visible(&self, seen: &Key) -> bool {
        self.visible.contains_key(seen)
    }

    pub(crate) fn change_vis<'a>(
        &mut self,
        change_vis: ChangeVisibility<'a>,
    ) -> ChangeVisibility<'a> {
        let ChangeVisibility {
            old_vis,
            new_vis,
            op,
        } = change_vis;
        let key = op.elemid_or_key();
        match (old_vis, new_vis) {
            (true, false) => match self.visible.get(&key).copied() {
                Some(1) => {
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

    pub(crate) fn insert(&mut self, op: Op<'_>) {
        self.never_seen_puts &= op.insert();

        // opids
        self.ops.insert(*op.id());

        // marks
        match op.action() {
            OpType::MarkBegin(_, data) => {
                self.mark_begin.insert(*op.id(), data.clone());
            }
            OpType::MarkEnd(_) => {
                if self.mark_begin.remove(&op.id().prev()).is_none() {
                    self.mark_end.push(*op.id())
                }
            }
            _ => {}
        }

        // visible ops
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

    pub(crate) fn remove(&mut self, op: Op<'_>) {
        // op ids
        self.ops.remove(op.id());

        // marks
        match op.action() {
            OpType::MarkBegin(_, _) => {
                self.mark_begin.remove(op.id());
            }
            OpType::MarkEnd(_) => {
                self.mark_end.retain(|id| id != op.id());
            }
            _ => {}
        }

        // visible ops
        if op.visible() {
            let key = op.elemid_or_key();
            match self.visible.get(&key).copied() {
                Some(1) => {
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
        self.mark_begin.extend(other.mark_begin.clone()); // can I remove this clone?
        self.mark_end.extend(&other.mark_end);
        self.visible_text.merge(&other.visible_text);
        self.block = other.block;
        self.never_seen_puts &= other.never_seen_puts;
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}
