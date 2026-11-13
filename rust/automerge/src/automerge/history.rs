//! Update the change graph, heads, and author associations together.
//!
//! Borrow the matching document fields with [`History::new`], then record a
//! change through [`History::update`]. The returned [`HistoryUpdate`] tells the
//! caller whether it must publish a resolved write-frontier boundary.

use std::collections::HashSet;

use crate::actor::ActorTable;
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::{Change, ChangeHash};

use super::signing::SigningState;
use super::visibility::FrontierVisibility;
use super::HistoryUpdate;

/// Record changes in a document's history without applying their operations.
///
/// The actor and dependencies must already be present. Visibility is inspected
/// but not changed. Callers publish a resolved boundary once operations are in
/// a consistent state.
pub(super) struct History<'a> {
    graph: &'a mut ChangeGraph,
    heads: &'a mut HashSet<ChangeHash>,
    authors: &'a mut Authors,
    actors: &'a ActorTable,
    visibility: &'a FrontierVisibility,
    signing: &'a mut SigningState,
}

impl<'a> History<'a> {
    /// Borrow the matching history fields of one document.
    pub(super) fn new(
        graph: &'a mut ChangeGraph,
        heads: &'a mut HashSet<ChangeHash>,
        authors: &'a mut Authors,
        actors: &'a ActorTable,
        visibility: &'a FrontierVisibility,
        signing: &'a mut SigningState,
    ) -> Self {
        Self {
            graph,
            heads,
            authors,
            actors,
            visibility,
            signing,
        }
    }

    /// Record a change and report whether it resolves a pending boundary.
    ///
    /// The actor must exist and dependencies must already be in the graph.
    pub(super) fn update(&mut self, change: &Change) -> HistoryUpdate {
        self.update_heads(change);

        let actor_index = self
            .actors
            .lookup(change.actor_id())
            .expect("Change's actor not already in the document");

        let hash = self
            .graph
            .add_change(change, actor_index, self.authors)
            .expect("Change's deps should already be in the document");

        self.signing
            .history_changed(self.graph, self.authors, &hash);

        // Report the boundary without publishing visibility before operations
        // have reached a consistent state.
        if self.visibility.is_pending(&hash) {
            HistoryUpdate::BoundaryResolved
        } else {
            HistoryUpdate::Unchanged
        }
    }

    fn update_heads(&mut self, change: &Change) {
        for dependency in change.deps() {
            self.heads.remove(dependency);
        }
        self.heads.insert(change.hash());
    }
}
