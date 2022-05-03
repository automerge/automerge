use crate::exid::ExId;
use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::types::{Key, OpId};
use crate::values::ValueIter;
use crate::{Automerge, Value};
use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug)]
pub(crate) struct MapRange<'a, R: RangeBounds<String>> {
    range: R,
    index: usize,
    last_key: Option<Key>,
    next_result: Option<(&'a str, Value<'a>, OpId)>,
    index_back: usize,
    last_key_back: Option<Key>,
    root_child: &'a OpTreeNode,
    meta: &'a OpSetMetadata,
}

impl<'a, R: RangeBounds<String>> ValueIter<'a> for MapRange<'a, R> {
    fn next_value(&mut self, doc: &'a Automerge) -> Option<(Value<'a>, ExId)> {
        self.next().map(|(_, val, id)| (val, doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new(range: R, root_child: &'a OpTreeNode, meta: &'a OpSetMetadata) -> Self {
        Self {
            range,
            index: 0,
            last_key: None,
            next_result: None,
            index_back: root_child.len(),
            last_key_back: None,
            root_child,
            meta,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = (&'a str, Value<'a>, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
            self.index += 1;
            if op.visible() {
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => return None, // this is a list
                };
                if self.range.contains(prop) {
                    let result = self.next_result.replace((prop, op.value(), op.id));
                    if Some(op.key) != self.last_key {
                        self.last_key = Some(op.key);
                        if result.is_some() {
                            return result;
                        }
                    }
                }
            }
        }
        self.next_result.take()
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for MapRange<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in (self.index..self.index_back).rev() {
            let op = self.root_child.get(i)?;
            self.index_back -= 1;
            if Some(op.key) != self.last_key_back && op.visible() {
                self.last_key_back = Some(op.key);
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => return None, // this is a list
                };
                if self.range.contains(prop) {
                    return Some((prop, op.value(), op.id));
                }
            }
        }
        None
    }
}
