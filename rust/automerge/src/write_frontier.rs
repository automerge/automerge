use std::collections::HashMap;

use crate::{Author, ChangeHash};

/// A record of write-frontiers for a set of [`Author`]s.
///
/// A frontier is a set of heads, i.e. [`Vec<ChangeHash>`]. A write-frontier is
/// then an inclusive bound at which the Automerge document will materialize for
/// the given author. Any changes after this frontier will be considered
/// invisible.
///
/// Each author is uniquely masked, and the heads are the inclusive bounds at
/// which the Automerge document will materialize for this author. Any changes
/// after these changes will be considered invisible.
#[derive(Debug, Default, Clone)]
pub(crate) struct WriteFrontier {
    /// Inclusive heads bounding each author's contribution to the view.
    author_frontier: HashMap<Author<'static>, Vec<ChangeHash>>,
}

impl WriteFrontier {
    /// Initialize the [`WriteFrontier`] beginning with no [`Author`]s being masked.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been masked.
    pub(crate) fn is_empty(&self) -> bool {
        self.author_frontier.is_empty()
    }

    /// Return at which changes the [`Author`] was masked at, if any.
    pub(crate) fn get_write_frontier_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&[ChangeHash]> {
        self.author_frontier
            .get(author)
            .map(|heads| heads.as_slice())
    }

    /// Returns `true` if the given `author` is being masked by the write-frontier.
    pub(crate) fn is_author_masked(&self, author: &Author<'_>) -> bool {
        self.author_frontier.contains_key(author)
    }

    /// Return the set of write-frontiers, per [`Author`].
    pub(crate) fn get_write_frontier(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.author_frontier
    }

    /// Mask the given [`Author`] at the given `heads`.
    pub(crate) fn mask_author(&mut self, author: Author<'static>, heads: Vec<ChangeHash>) {
        self.author_frontier.insert(author, heads);
    }

    /// Reveal the given [`Author`], so that their changes can be
    /// materialized in the Automerge document again.
    pub(crate) fn reveal_author(&mut self, author: &Author<'static>) {
        self.author_frontier.remove(author);
    }
}

impl From<HashMap<Author<'static>, Vec<ChangeHash>>> for WriteFrontier {
    fn from(write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>) -> Self {
        Self {
            author_frontier: write_frontier,
        }
    }
}
