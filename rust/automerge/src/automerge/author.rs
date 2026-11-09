//! Register authors for new actors and refresh their derived visibility.
//!
//! Borrow matching document fields with [`AuthorRegistration::new`], then use
//! [`AuthorRegistration::register`] before the actors record operations.

use crate::actor::ActorTable;
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::write_frontier::WriteFrontier;
use crate::Author;

use super::visibility::FrontierVisibility;

/// Associate new actors with authors under the existing write-frontier policy.
///
/// Actors must already be in the actor table and have no recorded operations.
/// Refreshing their visibility therefore needs no operation-index rebuild or
/// patch emission.
pub(super) struct AuthorRegistration<'a> {
    authors: &'a mut Authors,
    visibility: &'a mut FrontierVisibility,
    graph: &'a ChangeGraph,
    actors: &'a ActorTable,
    policy: &'a WriteFrontier,
}

impl<'a> AuthorRegistration<'a> {
    /// Borrow the matching registration fields of one document.
    pub(super) fn new(
        authors: &'a mut Authors,
        visibility: &'a mut FrontierVisibility,
        graph: &'a ChangeGraph,
        actors: &'a ActorTable,
        policy: &'a WriteFrontier,
    ) -> Self {
        Self {
            authors,
            visibility,
            graph,
            actors,
            policy,
        }
    }

    /// Assign all actor-author pairs, then derive visibility once.
    ///
    /// An empty batch leaves authors and visibility unchanged. Each actor must
    /// have no recorded changes or operations yet.
    pub(super) fn register(&mut self, pairs: impl IntoIterator<Item = (Author<'static>, usize)>) {
        let mut pairs = pairs.into_iter().peekable();
        let should_derive = pairs.peek().is_some();
        for (author, actor) in pairs {
            debug_assert_eq!(self.graph.seq_for_actor(actor), 0);
            self.authors.assign_author(author, actor);
        }
        if should_derive {
            *self.visibility =
                FrontierVisibility::new(self.policy, self.graph, self.authors, self.actors);
        }
    }
}
