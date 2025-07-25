use std::borrow::Cow;

use crate::types::ChangeHash;

#[derive(Clone, Debug)]
pub(crate) struct BundleMetadata<'a> {
    pub(crate) hash: ChangeHash,
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) start_op: u64,
    pub(crate) builder: usize,
}

impl BundleMetadata<'_> {
    pub(crate) fn num_ops(&self) -> usize {
        (1 + self.max_op - self.start_op) as usize
    }
}
