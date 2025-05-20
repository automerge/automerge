use std::ops::Range;

use super::{DeltaRange, RleRange};
use crate::{
    columnar::{encoding::raw, SpliceError},
    convert,
};

#[derive(Debug, Clone)]
pub(crate) struct OpIdRange {
    actor: RleRange<u64>,
    counter: DeltaRange,
}

impl OpIdRange {
    pub(crate) fn new(actor: RleRange<u64>, counter: DeltaRange) -> Self {
        Self { actor, counter }
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &DeltaRange {
        &self.counter
    }

    /*
        pub(crate) fn encode<I, O>(opids: I, out: &mut Vec<u8>) -> Self
        where
            O: convert::OpId<usize>,
            I: Iterator<Item = O> + Clone,
        {
            let actor = RleRange::encode(opids.clone().map(|o| Some(o.actor() as u64)), out);
            let counter = DeltaRange::encode(opids.map(|o| Some(o.counter() as i64)), out);
            Self { actor, counter }
        }
    */

    #[allow(dead_code)]
    pub(crate) fn splice<I, E, O>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<Self, SpliceError<raw::Error, E>>
    where
        O: convert::OpId<usize>,
        E: std::error::Error,
        I: Iterator<Item = Result<O, E>> + Clone,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with
                .clone()
                .map(|i| i.map(|i| Some(i.actor() as u64))),
            out,
        )?;
        let counter = self.counter.splice(
            data,
            replace,
            replace_with.map(|i| i.map(|i| Some(i.counter() as i64))),
            out,
        )?;
        Ok(Self { actor, counter })
    }
}
