use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::types::{Key, OpId};
use crate::{Prop, Value};
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct Range<'a, R: RangeBounds<Prop>> {
    range: R,
    index: usize,
    /// number of visible elements seen.
    seen: usize,
    last_key: Option<Key>,
    root_child: &'a OpTreeNode,
    meta: &'a OpSetMetadata,
}

impl<'a, R: RangeBounds<Prop>> Range<'a, R> {
    pub(crate) fn new(range: R, root_child: &'a OpTreeNode, meta: &'a OpSetMetadata) -> Self {
        Self {
            range,
            index: 0,
            seen: 0,
            last_key: None,
            root_child,
            meta,
        }
    }
}

impl<'a, 'm, R: RangeBounds<Prop>> Iterator for Range<'a, R> {
    type Item = (Key, Value, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.root_child.len() {
            let op = self.root_child.get(i)?;
            println!("{} {:?}", self.index, op);
            self.index += 1;
            if Some(op.elemid_or_key()) != self.last_key && op.visible() {
                self.last_key = Some(op.elemid_or_key());
                let contains = match op.key {
                    Key::Map(m) => self
                        .range
                        .contains(&Prop::Map(self.meta.props.get(m).clone())),
                    Key::Seq(_) => self.range.contains(&Prop::Seq(self.seen)),
                };
                println!("{} {}", self.seen, contains);
                self.seen += 1;
                if contains {
                    return Some((op.elemid_or_key(), op.value(), op.id));
                }
            }
        }
        None
    }
}
