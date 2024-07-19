use crate::exid::ExId;
use crate::Value;

use super::OpQueryTerm;

use std::fmt::Debug;
use std::ops::RangeBounds;

#[derive(Debug, PartialEq)]
pub struct MapRangeItem<'a> {
    pub key: &'a str,
    pub value: Value<'a>,
    pub id: ExId,
    pub conflict: bool,
}

#[derive(Debug)]
pub struct MapRange<'a, R: RangeBounds<String>> {
    iter: Option<Box<dyn OpQueryTerm<'a> + 'a>>,
    range: Option<R>,
}

impl<'a, R: RangeBounds<String>> Default for MapRange<'a, R> {
    fn default() -> Self {
        Self {
            iter: None,
            range: None,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRange<'a, R> {
    type Item = MapRangeItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let range = self.range.as_ref()?;
        let iter = self.iter.as_mut()?;
        while let Some(op) = iter.next() {
            let key = op.key.map_key()?;
            let s_key = key.to_string(); // FIXME
            if !range.contains(&s_key) {
                // return None if > end
                continue;
            }
            let value = op.value().into();
            let id = op.exid();
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
    pub(crate) fn new<I: OpQueryTerm<'a> + 'a>(iter: I, range: R) -> Self {
        Self {
            iter: Some(Box::new(iter)),
            range: Some(range),
        }
    }
}
