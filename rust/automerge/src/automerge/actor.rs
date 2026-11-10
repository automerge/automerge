//! Coordinate actor-table changes across the document's actor-indexed state.
//!
//! Borrow the matching fields through [`ActorMaintenance::new`], then use
//! [`ActorMaintenance::insert`] or [`ActorMaintenance::remove`] to update all
//! affected actor indices together.

use crate::actor::ActorInsert;
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::op_set2::OpSet;
use crate::ActorId;

use super::visibility::FrontierVisibility;
use super::Actor;

/// Maintain actor indices together without borrowing the whole document.
///
/// Construct from fields belonging to the same document. The handle coordinates
/// index shifts only. It does not register authors.
pub(super) struct ActorMaintenance<'a> {
    ops: &'a mut OpSet,
    graph: &'a mut ChangeGraph,
    local_actor: &'a mut Actor,
    authors: &'a mut Authors,
    visibility: &'a mut FrontierVisibility,
}

impl<'a> ActorMaintenance<'a> {
    /// Borrow the matching actor-indexed fields of one document.
    pub(super) fn new(
        ops: &'a mut OpSet,
        graph: &'a mut ChangeGraph,
        local_actor: &'a mut Actor,
        authors: &'a mut Authors,
        visibility: &'a mut FrontierVisibility,
    ) -> Self {
        Self {
            ops,
            graph,
            local_actor,
            authors,
            visibility,
        }
    }

    /// Ensure an actor is present, shifting all affected indices if inserted.
    pub(super) fn insert(&mut self, actor: ActorId) -> usize {
        match self.ops.insert_actor(actor) {
            ActorInsert::Existing(index) => index,
            ActorInsert::Inserted(shift) => {
                self.graph.insert_actor(&shift);
                self.local_actor.shift(&shift);
                self.authors.insert_actor(&shift);
                self.visibility.insert_actor(&shift);
                shift.index()
            }
        }
    }

    /// Remove an actor with no changes in the document from all indexed state.
    pub(super) fn remove(&mut self, index: usize) {
        let (removed, removal) = self.ops.remove_actor(index);
        self.local_actor.remove_actor(&removal, removed);
        self.graph.remove_actor(&removal);
        self.authors.remove_actor(&removal);
        self.visibility.remove_actor(&removal);
    }

    /// Find all unused actors, according to the change graph, and remove them
    /// from the document state.
    pub(super) fn remove_unused(&mut self) {
        while let Some(index) = self.graph.unused_actors().last() {
            self.remove(index);
        }
    }
}
