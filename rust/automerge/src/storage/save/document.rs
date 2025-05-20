use std::iter::Iterator;

use crate::{
    change_graph::ChangeGraph,
    op_set2::OpSet,
    storage::{change::DEFLATE_MIN_SIZE, CompressConfig, Document},
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
    assert_eq!(op_set.actors.len(), change_graph.actor_ids().count());

    let config = config.unwrap_or(CompressConfig::Threshold(DEFLATE_MIN_SIZE));

    let doc = Document::new(op_set, change_graph, config);

    doc.into_bytes()
}
