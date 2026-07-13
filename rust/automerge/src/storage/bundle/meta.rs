use std::borrow::Cow;

use crate::types::ChangeHash;

/// A change's dependency as the bundle builder consumes it: either
/// another member of the bundle (by its position in the member list,
/// which is topological order) or a change outside the bundle (by
/// hash). Resolving deps to positions up front is what lets bundles be
/// built without knowing the members' own hashes — only boundary
/// (external) hashes are needed.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DepRef {
    Internal(usize),
    External(ChangeHash),
}

#[derive(Clone, Debug)]
pub(crate) struct BundleMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<DepRef>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) start_op: u64,
    pub(crate) builder: usize,
}

impl BundleMetadata<'_> {
    pub(crate) fn num_ops(&self) -> usize {
        (1 + self.max_op - self.start_op) as usize
    }
}
