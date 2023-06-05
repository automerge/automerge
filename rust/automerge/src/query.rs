use crate::op_tree::{OpSetMetadata, OpTree, OpTreeNode};
use crate::types::{Key, ListEncoding, Op, OpId};
use fxhash::FxBuildHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

mod insert;
mod list_state;
mod nth;
mod opid;
mod prop;
mod seek_mark;

pub(crate) use insert::InsertNth;
pub(crate) use list_state::ListState;
pub(crate) use nth::Nth;
pub(crate) use opid::{OpIdSearch, SimpleOpIdSearch};
pub(crate) use prop::Prop;
pub(crate) use seek_mark::SeekMark;

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
    Descend,
    Finish,
}

#[derive(Clone, Debug, PartialEq)]
struct TextWidth {
    width: usize,
}

impl TextWidth {
    fn add_op(&mut self, op: &Op) {
        self.width += op.width(ListEncoding::Text);
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
        self.never_seen_puts &= op.insert;
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
        self.never_seen_puts &= other.never_seen_puts;
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}
