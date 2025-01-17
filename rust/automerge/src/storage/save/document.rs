use std::borrow::{Borrow, Cow};
use std::{collections::HashMap, iter::Iterator};

use fxhash::FxBuildHasher;
use itertools::Itertools;

use crate::{
    change_graph::ChangeGraph,
    indexed_cache::IndexedCache,
    op_set2::OpSet,
    storage::{change::DEFLATE_MIN_SIZE, AsChangeMeta, CompressConfig, Document},
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
    //changes: I,
    op_set: &OpSet,
    change_graph: &ChangeGraph,
    heads: &[ChangeHash],
    config: Option<CompressConfig>,
) -> Vec<u8>
//where I: Iterator<Item = &'a Change> + Clone + 'a,
{
    let mut op_set = Cow::Borrowed(op_set);
    let mut change_graph = Cow::Borrowed(change_graph);

    /*
        let mut actor_lookup = HashMap::with_capacity(op_set.actors.len());
        let mut actor_ids = changes
            .clone()
            .map(|c| c.actor_id().clone())
            .unique()
            .collect::<Vec<_>>();
        actor_ids.sort();
    */

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
            //actor_lookup.insert(actor_idx, index);
            break;
        }
    }

    //let hash_graph = HashGraph::new(changes.clone());
    /*
        let changes = changes.map(|c| ChangeWithGraph {
            actors: &actors,
            actor_lookup: &actor_lookup,
            change: c,
            graph: &hash_graph,
        });
    */

    let doc_ops = op_set.iter();

    let doc = Document::new(
        op_set.borrow(),
        change_graph.borrow(),
        //hash_graph.heads_with_indices(heads.to_vec()),
        //doc_ops,
        //changes,
        config.unwrap_or(CompressConfig::Threshold(DEFLATE_MIN_SIZE)),
    );
    doc.into_bytes()
}

struct HashGraph {
    index_by_hash: HashMap<ChangeHash, usize, FxBuildHasher>,
}

impl HashGraph {
    fn new<'a, I>(changes: I) -> Self
    where
        I: Iterator<Item = &'a Change>,
    {
        let mut index_by_hash: HashMap<_, _, _> = Default::default();
        for (index, change) in changes.enumerate() {
            index_by_hash.insert(change.hash(), index);
        }
        Self { index_by_hash }
    }

    fn change_index(&self, hash: &ChangeHash) -> usize {
        self.index_by_hash[hash]
    }

    fn heads_with_indices(&self, heads: Vec<ChangeHash>) -> Vec<(ChangeHash, usize)> {
        heads
            .into_iter()
            .map(|h| (h, self.index_by_hash[&h]))
            .collect()
    }
}

struct ChangeWithGraph<'a> {
    change: &'a Change,
    graph: &'a HashGraph,
    actor_lookup: &'a HashMap<usize, usize>,
    actors: &'a IndexedCache<ActorId>,
}

impl<'a> AsChangeMeta<'a> for ChangeWithGraph<'a> {
    type DepsIter = ChangeDepsIter<'a>;

    fn actor(&self) -> u64 {
        self.actor_lookup[&self.actors.lookup(self.change.actor_id()).unwrap()] as u64
    }

    fn seq(&self) -> u64 {
        self.change.seq()
    }

    fn deps(&self) -> Self::DepsIter {
        ChangeDepsIter {
            change: self.change,
            graph: self.graph,
            offset: 0,
        }
    }

    fn extra(&self) -> Cow<'a, [u8]> {
        self.change.extra_bytes().into()
    }

    fn max_op(&self) -> u64 {
        self.change.max_op()
    }

    fn message(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        self.change.message().map(|m| Cow::Owned(m.into()))
    }

    fn timestamp(&self) -> i64 {
        self.change.timestamp()
    }
}

struct ChangeDepsIter<'a> {
    change: &'a Change,
    graph: &'a HashGraph,
    offset: usize,
}

impl<'a> ExactSizeIterator for ChangeDepsIter<'a> {
    fn len(&self) -> usize {
        self.change.deps().len()
    }
}

impl<'a> Iterator for ChangeDepsIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(dep) = self.change.deps().get(self.offset) {
            self.offset += 1;
            Some(self.graph.change_index(dep) as u64)
        } else {
            None
        }
    }
}
