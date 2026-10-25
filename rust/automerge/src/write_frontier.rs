use std::collections::HashMap;

use crate::{Author, ChangeHash, ChangeId};

/// A change at which an author's write-frontier stops.
///
/// The audited API names changes by hash, and a hash the document has not
/// seen yet cannot be turned into an id, so both forms are kept.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FrontierHead {
    Id(ChangeId),
    Hash(ChangeHash),
}

/// For each masked [`Author`], the inclusive heads beyond which their
/// changes are hidden.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct WriteFrontier {
    author_frontier: HashMap<Author<'static>, Vec<FrontierHead>>,
}

impl WriteFrontier {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_ids(frontier: HashMap<Author<'static>, Vec<ChangeId>>) -> Self {
        Self {
            author_frontier: frontier
                .into_iter()
                .map(|(author, ids)| (author, ids.into_iter().map(FrontierHead::Id).collect()))
                .collect(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.author_frontier.is_empty()
    }

    pub(crate) fn is_author_masked(&self, author: &Author<'_>) -> bool {
        self.author_frontier.contains_key(author)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Author<'static>, &[FrontierHead])> {
        self.author_frontier
            .iter()
            .map(|(author, heads)| (author, heads.as_slice()))
    }

    /// The boundaries named by id; a hash boundary not yet resolved is left out.
    pub(crate) fn ids(&self) -> HashMap<Author<'static>, Vec<ChangeId>> {
        self.author_frontier
            .iter()
            .map(|(author, heads)| {
                let ids = heads
                    .iter()
                    .filter_map(|boundary| match boundary {
                        FrontierHead::Id(id) => Some(id.clone()),
                        FrontierHead::Hash(_) => None,
                    })
                    .collect();
                (author.clone(), ids)
            })
            .collect()
    }

    pub(crate) fn heads_mut(&mut self) -> impl Iterator<Item = &mut FrontierHead> {
        self.author_frontier.values_mut().flatten()
    }

    pub(crate) fn mask_author(&mut self, author: Author<'static>, heads: Vec<FrontierHead>) {
        self.author_frontier.insert(author, heads);
    }

    pub(crate) fn reveal_author(&mut self, author: &Author<'static>) {
        self.author_frontier.remove(author);
    }
}

impl From<HashMap<Author<'static>, Vec<ChangeHash>>> for WriteFrontier {
    fn from(write_frontier: HashMap<Author<'static>, Vec<ChangeHash>>) -> Self {
        Self {
            author_frontier: write_frontier
                .into_iter()
                .map(|(author, hashes)| {
                    (author, hashes.into_iter().map(FrontierHead::Hash).collect())
                })
                .collect(),
        }
    }
}
