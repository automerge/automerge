use crate::{
    exid::ExId,
    op_set2::Value,
};

use super::{Op, OpIter};

use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: &'a str,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
}

pub struct MapRange<'a, R: RangeBounds<String>> {
    //iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
    iter: Box<dyn Iterator<Item = Op<'a>> + 'a>,
    range: Option<R>,
    op_set: Option<&'a super::OpSet>,
}

impl<'a, R: RangeBounds<String>> Default for MapRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: Box::new(OpIter::default()),
            range: None,
            op_set: None,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let op_set = self.op_set?;
        let range = self.range.as_ref()?;
        while let Some(op) = self.iter.next() {
            let key = op.key.map_key()?;
            let s_key = key.to_string(); // FIXME
            if !range.contains(&s_key) {
                // return None if > end
                continue;
            }
            let value = op.value();
            let id = op_set.id_to_exid(op.id);
            let conflict = op.conflict;
            return Some(MapRangeItem {
                key,
                value,
                id,
                conflict,
            });
        }
        None
    }
}

impl<'a, R: RangeBounds<String>> MapRange<'a, R> {
    pub(crate) fn new<I: Iterator<Item = Op<'a>> + 'a>(
        //iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
        iter: I,
        range: R,
        op_set: &'a super::OpSet,
    ) -> Self {
        Self {
            iter: Box::new(iter),
            range: Some(range),
            op_set: Some(op_set),
        }
    }
}

