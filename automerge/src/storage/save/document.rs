use std::{borrow::Cow, collections::BTreeMap, iter::Iterator};

use crate::{
    indexed_cache::IndexedCache,
    storage::{
        change::DEFLATE_MIN_SIZE, convert::op_as_docop, AsChangeMeta, CompressConfig, Document,
    },
    types::{ActorId, ObjId, Op},
    Change, ChangeHash,
};

/// # Panics
///
/// * If any of the `heads` are not in `changes`
/// * If any of ops in `ops` reference an actor which is not in `actors`
/// * If any of ops in `ops` reference a property which is not in `props`
/// * If any of the changes reference a dependency index which is not in `changes`
#[tracing::instrument(skip(changes, ops, actors, props, config))]
pub(crate) fn save_document<'a, I, O>(
    changes: I,
    ops: O,
    actors: &'a IndexedCache<ActorId>,
    props: &IndexedCache<String>,
    heads: &[ChangeHash],
    config: Option<CompressConfig>,
) -> Vec<u8>
where
    I: Iterator<Item = &'a Change> + Clone + 'a,
    O: Iterator<Item = (&'a ObjId, &'a Op)> + Clone + ExactSizeIterator,
{
    let actor_lookup = actors.encode_index();
    let doc_ops = ops.map(|(obj, op)| op_as_docop(&actor_lookup, props, obj, op));

    let hash_graph = HashGraph::new(changes.clone());
    let changes = changes.map(|c| ChangeWithGraph {
        actors,
        actor_lookup: &actor_lookup,
        change: c,
        graph: &hash_graph,
    });

    let doc = Document::new(
        actors.sorted().cache,
        hash_graph.heads_with_indices(heads.to_vec()),
        doc_ops,
        changes,
        config.unwrap_or(CompressConfig::Threshold(DEFLATE_MIN_SIZE)),
    );
    doc.into_bytes()
}

struct HashGraph {
    index_by_hash: BTreeMap<ChangeHash, usize>,
}

impl HashGraph {
    fn new<'a, I>(changes: I) -> Self
    where
        I: Iterator<Item = &'a Change>,
    {
        let mut index_by_hash = BTreeMap::new();
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
    actor_lookup: &'a [usize],
    actors: &'a IndexedCache<ActorId>,
}

impl<'a> AsChangeMeta<'a> for ChangeWithGraph<'a> {
    type DepsIter = ChangeDepsIter<'a>;

    fn actor(&self) -> u64 {
        self.actor_lookup[self.actors.lookup(self.change.actor_id()).unwrap()] as u64
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
