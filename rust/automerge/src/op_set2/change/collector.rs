use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use crate::change_graph::ChangeGraph;
use crate::error::AutomergeError;
use crate::op_set2::op_set::IndexBuilder;
use crate::storage::document::ReadChangeError;
use crate::storage::load::change_collector::Error;
use crate::{
    change::Change,
    op_set2::{ChangeMetadata, KeyRef, Op, OpBuilder, OpSet},
    storage::DocChangeMetadata,
    types::{ActorId, ChangeHash, ObjId, OpId},
};

pub(crate) struct IndexedChangeCollector<'a> {
    pub(crate) index: IndexBuilder,
    pub(crate) collector: ChangeCollector<'a>,
}

impl<'a> IndexedChangeCollector<'a> {
    pub(crate) fn process_succ(&mut self, op_id: OpId, succ_id: OpId, is_counter: bool) {
        self.index.process_succ(is_counter, succ_id);
        self.collector.process_succ(op_id, succ_id);
    }

    pub(crate) fn build_changegraph(
        self,
        op_set: &OpSet,
    ) -> Result<(IndexBuilder, CollectedChanges), Error> {
        let index = self.index;
        let changes = self.collector.build_changegraph(op_set)?;
        Ok((index, changes))
    }

    pub(crate) fn process_op(&mut self, op: Op<'a>) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.collector.last != next;
        if flush {
            self.index.flush();
        }
        self.index.process_op(&op);
        self.collector.process_op_internal(op, flush);
        if flush {
            self.collector.last = next;
        }
    }
}

pub(crate) struct ChangeCollector<'a> {
    changes: Vec<BuildChangeMetadata<'a>>,
    builders: Vec<ChangeBuilder<'a>>,
    last: Option<(ObjId, KeyRef<'a>)>,
    preds: HashMap<OpId, Vec<OpId>>,
    max_op: u64,
    num_deps: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct BuildChangeMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<u64>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) start_op: u64,
    pub(crate) builder: usize,
}

impl BuildChangeMetadata<'_> {
    fn num_ops(&self) -> usize {
        (1 + self.max_op - self.start_op) as usize
    }

    pub(crate) fn message_str(&self) -> &str {
        self.message.as_deref().unwrap_or_default()
    }
}

#[derive(Debug)]
struct ChangeBuilder<'a> {
    actor: usize,
    seq: u64,
    change: usize,
    start_op: u64,
    ops: Vec<Option<OpBuilder<'a>>>,
}

impl<'a> ChangeBuilder<'a> {
    pub(crate) fn get_ops(&self) -> Result<&[Option<OpBuilder<'a>>], Error> {
        let start_pos = self.ops.iter().position(|op| op.is_some()).unwrap_or(0);
        let ops = &self.ops[start_pos..];

        if ops.iter().any(|o| o.is_none()) {
            return Err(Error::MissingOps);
        }

        Ok(ops)
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.start_op + self.ops.len() as u64 - 1
    }

    pub(crate) fn add(&mut self, op: OpBuilder<'a>) {
        let counter = op.id.counter();
        //if counter >= self.start_op && counter <= self.max_op() && op.id.actor() == self.actor {
        self.ops[(counter - self.start_op) as usize] = Some(op);
        //}
    }
}

impl<'a> ChangeCollector<'a> {
    pub(crate) fn with_index(self, index: IndexBuilder) -> IndexedChangeCollector<'a> {
        IndexedChangeCollector {
            collector: self,
            index,
        }
    }

    pub(crate) fn new<I>(changes: I) -> Result<ChangeCollector<'a>, ReadChangeError>
    where
        I: Iterator<Item = Result<DocChangeMetadata<'a>, ReadChangeError>>,
    {
        let mut num_deps = 0;
        let mut changes: Vec<_> = changes
            .map(|m| {
                m.map(|meta| BuildChangeMetadata {
                    actor: meta.actor,
                    seq: meta.seq,
                    max_op: meta.max_op,
                    timestamp: meta.timestamp,
                    message: meta.message,
                    deps: meta.deps,
                    extra: meta.extra,
                    start_op: 0,
                    builder: 0,
                })
            })
            .collect::<Result<_, _>>()?;

        for i in 0..changes.len() {
            changes[i].start_op = changes[i]
                .deps
                .iter()
                .map(|i| changes[*i as usize].max_op)
                .max()
                .unwrap_or(0)
                + 1;
            if changes[i].start_op > changes[i].max_op + 1 {
                return Err(ReadChangeError::InvalidMaxOp);
            }
            num_deps += changes[i].deps.len();
        }
        Ok(Self::from_change_meta(changes, num_deps))
    }

    fn from_change_meta(
        mut changes: Vec<BuildChangeMetadata<'a>>,
        num_deps: usize,
    ) -> ChangeCollector<'a> {
        let mut builders: Vec<_> = changes
            .iter()
            .enumerate()
            .map(|(index, e)| ChangeBuilder {
                actor: e.actor,
                seq: e.seq,
                change: index,
                start_op: e.start_op,
                ops: vec![None; e.num_ops()],
            })
            .collect();

        builders.sort_unstable_by(|a, b| a.actor.cmp(&b.actor).then(a.seq.cmp(&b.seq)));

        builders
            .iter()
            .enumerate()
            .for_each(|(index, b)| changes[b.change].builder = index);

        ChangeCollector {
            changes,
            builders,
            last: None,
            preds: HashMap::default(),
            max_op: 0,
            num_deps,
        }
    }

    pub(crate) fn exclude_hashes(
        op_set: &OpSet,
        change_graph: &'a ChangeGraph,
        have_deps: &[ChangeHash],
    ) -> Vec<Change> {
        let (changes, num_deps) = change_graph.get_build_metadata_clock(have_deps);
        Self::from_build_meta(op_set, change_graph, changes, num_deps)
    }

    pub(crate) fn exclude_hashes_meta(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        have_deps: &[ChangeHash],
    ) -> Vec<ChangeMetadata<'a>> {
        let (changes, _) = change_graph.get_build_metadata_clock(have_deps);
        changes
            .into_iter()
            .map(|c| ChangeMetadata {
                actor: Cow::Borrowed(&op_set.actors[c.actor]),
                seq: c.seq,
                start_op: c.start_op,
                max_op: c.max_op,
                timestamp: c.timestamp,
                message: c.message,
                deps: c
                    .deps
                    .iter()
                    .filter_map(|n| change_graph.index_to_hash(*n as usize).cloned())
                    .collect(),
                hash: change_graph.index_to_hash(c.builder).cloned().unwrap(),
                extra: c.extra,
            })
            .collect()
    }

    pub(crate) fn meta_for_hashes<I>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        hashes: I,
    ) -> Result<Vec<ChangeMetadata<'a>>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let (changes, _) = change_graph.get_build_metadata(hashes)?;
        Ok(changes
            .into_iter()
            .map(|c| ChangeMetadata {
                actor: Cow::Borrowed(&op_set.actors[c.actor]),
                seq: c.seq,
                start_op: c.start_op,
                max_op: c.max_op,
                timestamp: c.timestamp,
                message: c.message,
                deps: c
                    .deps
                    .iter()
                    .filter_map(|n| change_graph.index_to_hash(*n as usize).cloned())
                    .collect(),
                hash: change_graph.index_to_hash(c.builder).cloned().unwrap(),
                extra: c.extra,
            })
            .collect())
    }

    pub(crate) fn for_hashes<I>(
        op_set: &OpSet,
        change_graph: &'a ChangeGraph,
        hashes: I,
    ) -> Result<Vec<Change>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let (changes, num_deps) = change_graph.get_build_metadata(hashes)?;
        Ok(Self::from_build_meta(
            op_set,
            change_graph,
            changes,
            num_deps,
        ))
    }

    fn from_build_meta(
        op_set: &OpSet,
        change_graph: &'a ChangeGraph,
        changes: Vec<BuildChangeMetadata<'a>>,
        num_deps: usize,
    ) -> Vec<Change> {
        let r1 = Self::from_build_meta1(op_set, change_graph, changes.clone(), num_deps);
        #[cfg(debug_assertions)]
        let r2 = Self::from_build_meta2(op_set, change_graph, changes, num_deps);
        #[cfg(debug_assertions)]
        assert_eq!(r1, r2);
        r1
    }

    fn from_build_meta1(
        op_set: &OpSet,
        change_graph: &'a ChangeGraph,
        changes: Vec<BuildChangeMetadata<'a>>,
        num_deps: usize,
    ) -> Vec<Change> {
        let min = changes
            .iter()
            .map(|c| c.start_op as usize)
            .min()
            .unwrap_or(0);
        let max = changes.iter().map(|c| c.max_op as usize).max().unwrap_or(0) + 1;

        let mut collector = Self::from_change_meta(changes, num_deps);

        for op in op_set.iter_ctr_range(min..max) {
            let op_id = op.id;
            let op_succ = op.succ();
            collector.process_op(op);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }

        // this can error on load but should never on a live document
        collector.finish(change_graph, &op_set.actors).unwrap()
    }

    #[cfg(debug_assertions)]
    fn from_build_meta2(
        op_set: &OpSet,
        change_graph: &'a ChangeGraph,
        changes: Vec<BuildChangeMetadata<'a>>,
        num_deps: usize,
    ) -> Vec<Change> {
        let mut collector = Self::from_change_meta(changes, num_deps);

        for op in op_set.iter() {
            let op_id = op.id;
            let op_succ = op.succ();
            collector.process_op(op);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }

        // this can error on load but should never on a live document
        collector.finish(change_graph, &op_set.actors).unwrap()
    }

    pub(crate) fn process_succ(&mut self, op_id: OpId, succ_id: OpId) {
        self.max_op = std::cmp::max(self.max_op, succ_id.counter());
        self.preds.entry(succ_id).or_default().push(op_id);
    }

    pub(crate) fn process_op(&mut self, op: Op<'a>) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.last != next;

        self.process_op_internal(op, flush);

        if flush {
            self.last = next;
        }
    }

    fn process_op_internal(&mut self, op: Op<'a>, flush: bool) {
        self.max_op = std::cmp::max(self.max_op, op.id.counter());

        if flush {
            self.flush_deletes();
        }

        let pred = self.preds.remove(&op.id).unwrap_or_default();

        let op = op.build(pred);

        if let Some(index) = self.builders_index(op.id) {
            self.builders[index].add(op);
        }
    }

    fn builders_index(&self, id: OpId) -> Option<usize> {
        self.builders
            .binary_search_by(|builder| {
                builder
                    .actor
                    .cmp(&id.actor())
                    .then_with(|| match id.counter() {
                        c if c < builder.start_op => Ordering::Greater,
                        c if c > builder.max_op() => Ordering::Less,
                        _ => Ordering::Equal,
                    })
            })
            .ok()
    }

    pub(crate) fn flush_deletes(&mut self) {
        if let Some((obj, key)) = self.last.take() {
            for (id, pred) in &self.preds {
                let op = Op::del(*id, obj, key.clone());
                let op = op.build(pred.to_vec());
                if let Some(index) = self.builders_index(op.id) {
                    self.builders[index].add(op);
                }
            }
            self.preds.clear();
        }
    }

    pub(crate) fn finish(
        self,
        change_graph: &ChangeGraph,
        actors: &[ActorId],
    ) -> Result<Vec<Change>, Error> {
        self.finish_inner(change_graph, actors, None)
    }

    fn finish_inner(
        mut self,
        graph: &ChangeGraph,
        actors: &[ActorId],
        index: Option<&mut IndexBuilder>,
    ) -> Result<Vec<Change>, Error> {
        self.flush_deletes();
        if let Some(i) = index {
            i.flush()
        }

        let mut changes = Vec::with_capacity(self.changes.len());

        let mut mapper = super::ActorMapper::new(actors);

        for change in self.changes.into_iter() {
            let actor = change.actor;

            if actor >= actors.len() {
                return Err(Error::MissingActor);
            }

            let ops = self.builders[change.builder].get_ops()?;

            if let Some(Some(last)) = ops.last() {
                assert_eq!(last.id.counter(), change.max_op);
            }

            let finished = super::build_change_inner(ops, &change, graph, &mut mapper);

            changes.push(Change::new(finished));
        }

        Ok(changes)
    }

    pub(crate) fn build_changegraph(mut self, op_set: &OpSet) -> Result<CollectedChanges, Error> {
        self.flush_deletes();

        let num_actors = op_set.actors.len();
        let mut max_ops = vec![0; num_actors];
        let mut seq = vec![0; num_actors];
        let mut changes = Vec::with_capacity(self.changes.len());
        let mut heads = BTreeSet::new();

        let mut actors = Vec::with_capacity(self.changes.len());
        let mut mapper = super::ActorMapper::new(&op_set.actors);

        for change in self.changes.into_iter() {
            let actor = change.actor;

            if actor >= num_actors {
                return Err(Error::MissingActor);
            }

            if seq[actor] + 1 != change.seq {
                return Err(Error::ChangesOutOfOrder);
            }

            seq[actor] = change.seq;

            let builder = change.builder;
            let max_op = change.max_op;

            if change.start_op == 0 || max_op < max_ops[actor] {
                return Err(Error::IncorrectMaxOp);
            }

            max_ops[actor] = max_op;

            let ops = self.builders[builder].get_ops()?;

            if let Some(Some(last)) = ops.last() {
                assert_eq!(last.id.counter(), max_op);
            }

            let finished = super::build_change_inner(ops, &change, &changes, &mut mapper);

            let hash = finished.hash();

            for dep in finished.dependencies() {
                heads.remove(dep);
            }

            heads.insert(hash);

            let change = Change::new(finished);

            changes.push(change);
            actors.push(actor);
        }

        let max_op = self.max_op;

        let change_graph = ChangeGraph::from_iter(
            changes.iter().zip(actors.into_iter()),
            self.num_deps,
            num_actors,
        )?;

        Ok(CollectedChanges {
            changes,
            heads,
            max_op,
            change_graph,
        })
    }
}

pub(crate) struct CollectedChanges {
    pub(crate) changes: Vec<Change>,
    pub(crate) heads: BTreeSet<ChangeHash>,
    pub(crate) max_op: u64,
    pub(crate) change_graph: ChangeGraph,
}
