use crate::{exid::ExId, types, types::Clock};

use super::OpQueryTerm;

use std::fmt::Debug;

#[derive(Default, Debug)]
pub struct Values<'a> {
    iter: Option<Box<dyn OpQueryTerm<'a> + 'a>>,
}

impl<'a> Values<'a> {
    // FIXME - ignore clock?
    pub(crate) fn new<I: OpQueryTerm<'a> + 'a>(iter: I, _clock: Option<Clock>) -> Self {
        Self {
            iter: Some(Box::new(iter)),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (types::Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.as_mut()?.next()?;
        let value = op.value().into_owned();
        let op_set = &self.iter.as_ref()?.get_opiter().op_set;
        let id = op_set.id_to_exid(op.id);
        Some((value, id))
    }
}
