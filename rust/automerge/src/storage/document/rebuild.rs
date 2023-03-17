use std::collections::{HashMap, HashSet};

use crate::{
    change_graph::ChangeGraph,
    storage::convert::op_as_actor_id,
    types::{ObjId, Op},
    ChangeHash,
};

pub(crate) struct Rebuilt {
    pub(crate) changes: Vec<crate::Change>,
    pub(crate) change_graph: ChangeGraph,
    pub(crate) heads: Vec<(ChangeHash, usize)>,
}

/// Reconstruct the stored changes from the ops in `doc`
pub(crate) fn rebuild_changelog(doc: &crate::Automerge) -> Rebuilt {
    let mut changes = Vec::new();
    let mut change_graph = ChangeGraph::new();
    let mut heads = HashSet::new();
    let mut old_to_new: HashMap<ChangeHash, ChangeHash> = HashMap::new();

    for change in doc.changes_topo() {
        let ops = ops_for_change(doc, change);
        let deps = change.deps().iter().map(|h| old_to_new[h]).collect();
        let new_change = crate::storage::Change::builder()
            .with_actor(change.actor_id().clone())
            .with_seq(change.seq())
            .with_message(change.message().cloned())
            .with_extra_bytes(change.extra_bytes().to_vec())
            .with_timestamp(change.timestamp())
            .with_start_op(change.start_op())
            .with_dependencies(deps)
            .build(ops.map(|(op, obj)| op_as_actor_id(obj, op, &doc.ops().m)))
            .unwrap()
            .into_owned();
        old_to_new.insert(change.hash(), new_change.hash());
        for dep in new_change.dependencies() {
            heads.remove(dep);
        }
        heads.insert(new_change.hash());
        let new_change = crate::Change::new(new_change);
        let actor_idx = doc.ops().m.actors.lookup(change.actor_id()).unwrap();
        change_graph.add_change(&new_change, actor_idx).unwrap();
        changes.push(new_change);
    }

    let mut heads_vec: Vec<(ChangeHash, usize)> = heads
        .into_iter()
        .map(|h| (h, changes.iter().position(|c| c.hash() == h).unwrap()))
        .collect();

    heads_vec.sort_by_key(|(_, pos)| *pos);

    Rebuilt {
        changes,
        change_graph,
        heads: heads_vec,
    }
}

fn ops_for_change<'a>(
    doc: &'a crate::Automerge,
    change: &'a crate::Change,
) -> impl Iterator<Item = (&'a Op, &'a ObjId)> + Clone {
    let start_op = u64::from(change.start_op());
    let end_op = start_op + (change.len() as u64);
    let actor_idx = doc.ops().m.actors.lookup(change.actor_id()).unwrap();
    doc.ops().iter().filter_map(move |(obj, _ty, op)| {
        if op.id.counter() >= start_op && op.id.counter() < end_op && op.id.actor() == actor_idx {
            Some((op, obj))
        } else {
            None
        }
    })
}
