use crate::{
    exid::ExId,
    op_set2::types::{ActorIdx, Key, ScalarValue},
    types,
    types::{Clock},
};

use super::{ Op, OpIter };

use std::fmt::Debug;
use std::sync::Arc;

pub struct Values<'a> {
    iter: Box<dyn Iterator<Item = Op<'a>> + 'a>,
    op_set: Option<&'a super::OpSet>,
}

impl<'a> Default for Values<'a> {
    fn default() -> Self {
        Self {
            iter: Box::new(OpIter::default()),
            op_set: None,
        }
    }
}

impl<'a> Values<'a> {
    pub(crate) fn new<I: Iterator<Item = Op<'a>> + 'a>(
        iter: I,
        clock: Option<Clock>,
        op_set: &'a super::OpSet,
    ) -> Self {
        Self {
            iter: Box::new(iter),
            op_set: Some(op_set),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (types::Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.next()?;
        let value = op.value().into_owned();
        let op_set = self.op_set?;
        let id = op_set.id_to_exid(op.id);
        Some((value, id))
    }
}

