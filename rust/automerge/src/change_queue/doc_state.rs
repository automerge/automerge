use crate::{
    author::Authors, change_graph::ChangeGraph, op_set2::OpSet, ActorId, Author, Change, ChangeHash,
};

/// The parts of a document the queue consults to decide what may enter it.
///
/// This is basically a convenience to avoid passing in these three arguments
/// to every method on [`ChangeQueue`]. The fields are all held by
/// [`Automerge`] but we can't pass a reference to [`Automerge`] in because it
/// also holds [`ChangeQueue`] and the borrow checker would get upset.
pub(crate) struct DocState<'a> {
    pub(crate) graph: &'a ChangeGraph,
    pub(crate) authors: &'a Authors,
    pub(crate) ops: &'a OpSet,
}

impl DocState<'_> {
    pub(super) fn author_of(&self, actor: &ActorId) -> Option<Author<'_>> {
        let index = self.ops.lookup_actor(actor)?;
        self.authors.get_author_for_actor(index)
    }

    pub(super) fn actor_of(&self, hash: &ChangeHash) -> Option<&ActorId> {
        let (index, _) = self.graph.actor_seq_for_hash(hash)?;
        self.ops.actors.get(index)
    }

    /// Whether the document already holds this actor sequence.
    pub(super) fn has_actor_seq(&self, change: &Change) -> bool {
        self.ops
            .lookup_actor(change.actor_id())
            .is_some_and(|index| self.graph.seq_for_actor(index) >= change.seq())
    }
}
