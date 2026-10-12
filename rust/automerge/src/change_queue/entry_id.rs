/// A stable handle to a queued change.
///
/// Ids are handed out in arrival order and never reused, so the indexes in
/// [`ChangeQueue`] stay valid when other entries are removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct EntryId(u64);

impl EntryId {
    pub(super) fn zero() -> Self {
        Self(0)
    }

    pub(super) fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
