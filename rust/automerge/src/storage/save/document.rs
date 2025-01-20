use std::borrow::{Borrow, Cow};
use std::{collections::HashMap, iter::Iterator};

use crate::{
    change_graph::ChangeGraph,
    indexed_cache::IndexedCache,
    op_set2::OpSet,
    storage::{change::DEFLATE_MIN_SIZE, CompressConfig, Document},
    types::ActorId,
    Change, ChangeHash,
};

/// # Panics
///
/// * If any of the `heads` are not in `changes`
/// * If any of ops in `ops` reference an actor which is not in `actors`
/// * If any of ops in `ops` reference a property which is not in `props`
/// * If any of the changes reference a dependency index which is not in `changes`
pub(crate) fn save_document(
    op_set: &OpSet,
    change_graph: &ChangeGraph,
    config: Option<CompressConfig>,
) -> Vec<u8> {
    let mut op_set = Cow::Borrowed(op_set);
    let mut change_graph = Cow::Borrowed(change_graph);

    let actors = op_set.actors.clone().into_iter().collect::<Vec<_>>();

    let actor_ids = change_graph
        .actor_ids()
        .map(|i| actors.get(i).cloned().unwrap())
        .collect::<Vec<_>>();

    //assert_eq!(actor_ids, actor_ids2);

    // I really dont like this current implementation
    // This is needed b/c sometimes an actor is added but not used
    // and it should not be present in the final file
    // save() should not be &mut as well so we cant edit op_set
    // better solutions:
    // 1.  supply an optional actor_id mapping to op_set.export()
    // 2a. only add new actorIds when the transaction is successfull and incorporated
    // 3b. Use Arc<ActorId> instead of actor_idx in
    //     HashGraph and history so they cant get out of sync
    for (index, actor_id) in actor_ids.iter().enumerate() {
        loop {
            let actor_idx = op_set.lookup_actor(actor_id).unwrap();
            if actor_idx != index {
                op_set.to_mut().remove_actor(actor_idx - 1);
                change_graph.to_mut().remove_actor(actor_idx - 1);
                continue;
            }
            break;
        }
    }

    let doc = Document::new(
        op_set.borrow(),
        change_graph.borrow(),
        config.unwrap_or(CompressConfig::Threshold(DEFLATE_MIN_SIZE)),
    );
    doc.into_bytes()
}
