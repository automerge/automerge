use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::types::{Key, OpId};
use crate::Value;
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct Range<'a, R: RangeBounds<String>> {
    range: R,
    index: usize,
    last_key: Option<Key>,
    index_back: usize,
    last_key_back: Option<Key>,
    root_child: &'a OpTreeNode,
    meta: &'a OpSetMetadata,
}

impl<'a, R: RangeBounds<String>> Range<'a, R> {
    pub(crate) fn new(range: R, root_child: &'a OpTreeNode, meta: &'a OpSetMetadata) -> Self {
        Self {
            range,
            index: 0,
            last_key: None,
            index_back: root_child.len(),
            last_key_back: None,
            root_child,
            meta,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for Range<'a, R> {
    type Item = (&'a str, Value<'a>, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            self.index += 1;
            if Some(op.key) != self.last_key && op.visible() {
                self.last_key = Some(op.key);
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => panic!("found list op in range query"),
                };
                if self.range.contains(prop) {
                    return Some((prop, op.value(), op.id));
                }
            }
        }
        None
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for Range<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in (self.index..self.index_back).rev() {
            let op = self.root_child.get(i)?;
            self.index_back -= 1;
            if Some(op.key) != self.last_key_back && op.visible() {
                self.last_key_back = Some(op.key);
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => panic!("can't iterate through lists backwards"),
                };
                if self.range.contains(prop) {
                    return Some((prop, op.value(), op.id));
                }
            }
        }
        None
    }
}
