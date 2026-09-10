use crate::{revocation::RevocationState, ChangeHash};

#[cfg(test)]
mod tests;

/// A view observed by internal patch bookkeeping. Public heads-based reads and
/// diffs always use current revocations; they do not expose this coordinate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct View {
    pub(crate) heads: Vec<ChangeHash>,
    pub(crate) revocations: RevocationState,
}
