use std::{borrow::Cow, collections::{BTreeSet, HashMap}};

use tracing::instrument;

use crate::{
    indexed_cache::IndexedCache,
    columnar_2::{
        rowblock::{
            change_op_columns::{ChangeOp, ChangeOpsColumns},
            doc_change_columns::ChangeMetadata,
            Key as StoredKey, PrimVal,
        },
        storage::Change as StoredChange,
    },
    types::{ActorId, ChangeHash, ElemId, Key, Op, ObjId},
    OpType,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("a change referenced an actor index we couldn't find")]
    MissingActor,
    #[error("changes out of order")]
    ChangesOutOfOrder,
    #[error("missing change")]
    MissingChange,
    #[error("some ops were missing")]
    MissingOps,
    #[error("unable to read change metadata: {0}")]
    ReadChange(Box<dyn std::error::Error>),
}

pub(crate) struct ChangeCollector<'a> {
    changes_by_actor: HashMap<usize, Vec<PartialChange<'a>>>,
}

pub(crate) struct CollectedChanges<'a> {
    pub(crate) history: Vec<StoredChange<'a>>,
    pub(crate) history_index: HashMap<ChangeHash, usize>,
    pub(crate) actor_to_history: HashMap<usize, Vec<usize>>,
    pub(crate) heads: BTreeSet<ChangeHash>,
}

impl<'a> ChangeCollector<'a> {
    pub(crate) fn new<E: std::error::Error + 'static, I>(
        changes: I,
    ) -> Result<ChangeCollector<'a>, Error>
    where
        I: IntoIterator<Item = Result<ChangeMetadata<'a>, E>>,
    {
        let mut changes_by_actor: HashMap<usize, Vec<PartialChange<'_>>> = HashMap::new();
        for (index, change) in changes.into_iter().enumerate() {
            tracing::trace!(?change, "importing change metadata");
            let change = change.map_err(|e| Error::ReadChange(Box::new(e)))?;
            let actor_changes = changes_by_actor.entry(change.actor).or_default();
            if let Some(prev) = actor_changes.last() {
                if prev.max_op >= change.max_op {
                    return Err(Error::ChangesOutOfOrder);
                }
            }
            actor_changes.push(PartialChange {
                index,
                deps: change.deps,
                actor: change.actor,
                seq: change.seq,
                timestamp: change.timestamp,
                max_op: change.max_op,
                message: change.message,
                extra_bytes: change.extra,
                ops: Vec::new(),
            })
        }
        let num_changes: usize = changes_by_actor.values().map(|v| v.len()).sum();
        tracing::trace!(num_changes, ?changes_by_actor, "change collection context created");
        Ok(ChangeCollector { changes_by_actor })
    }

    #[instrument(skip(self))]
    pub(crate) fn collect(&mut self, obj: ObjId, op: Op) -> Result<(), Error> {
        let actor_changes = self
            .changes_by_actor
            .get_mut(&op.id.actor())
            .ok_or_else(||{
                tracing::error!(missing_actor=op.id.actor(), "missing actor for op");
                Error::MissingActor
            })?;
        let change_index = actor_changes.partition_point(|c| c.max_op < op.id.counter());
        let change = actor_changes
            .get_mut(change_index)
            .ok_or_else(||{
                tracing::error!(missing_change_index=change_index, "missing change for op");
                Error::MissingChange
            })?;
        change.ops.push((obj, op));
        Ok(())
    }

    #[instrument(skip(self, actors, props))]
    pub(crate) fn finish(
        self,
        actors: &IndexedCache<ActorId>,
        props: &IndexedCache<String>,
    ) -> Result<CollectedChanges<'static>, Error> {
        let mut changes_in_order =
            Vec::with_capacity(self.changes_by_actor.values().map(|c| c.len()).sum());
        for (_, changes) in self.changes_by_actor {
            let mut start_op = 0;
            let mut seq = None;
            for change in changes {
                if change.max_op != start_op + (change.ops.len() as u64) {
                    tracing::error!(?change, start_op, "missing operations");
                    return Err(Error::MissingOps);
                } else {
                    start_op = change.max_op;
                }
                if let Some(seq) = seq {
                    if seq != change.seq - 1 {
                        return Err(Error::ChangesOutOfOrder);
                    }
                } else if change.seq != 1 {
                    return Err(Error::ChangesOutOfOrder);
                }
                seq = Some(change.seq);
                changes_in_order.push(change);
            }
        }
        changes_in_order.sort_by_key(|c| c.index);

        let mut hashes_by_index = HashMap::new();
        let mut history = Vec::new();
        let mut actor_to_history: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut heads = BTreeSet::new();
        for (index, change) in changes_in_order.into_iter().enumerate() {
            actor_to_history
                .entry(change.actor)
                .or_default()
                .push(index);
            let finished = change.finish(&hashes_by_index, actors, props)?;
            let hash = finished.hash();
            hashes_by_index.insert(index, hash);
            for dep in &finished.dependencies {
                heads.remove(dep);
            }
            tracing::trace!(?hash, "processing change hash");
            heads.insert(hash);
            history.push(finished.into_owned());
        }

        let indices_by_hash = hashes_by_index.into_iter().map(|(k, v)| (v, k)).collect();
        Ok(CollectedChanges {
            history,
            history_index: indices_by_hash,
            actor_to_history,
            heads,
        })
    }
}

#[derive(Debug)]
struct PartialChange<'a> {
    index: usize,
    deps: Vec<u64>,
    actor: usize,
    seq: u64,
    max_op: u64,
    timestamp: i64,
    message: Option<smol_str::SmolStr>,
    extra_bytes: Cow<'a, [u8]>,
    ops: Vec<(ObjId, Op)>,
}

impl<'a> PartialChange<'a> {
    /// # Panics
    ///
    /// If any op references a property index which is not in `props`
    #[instrument(skip(self, known_changes, actors, props))]
    fn finish(
        self,
        known_changes: &HashMap<usize, ChangeHash>,
        actors: &IndexedCache<ActorId>,
        props: &IndexedCache<String>,
    ) -> Result<StoredChange<'a>, Error> {
        let deps_len = self.deps.len();
        let mut deps =
            self.deps
                .into_iter()
                .try_fold(Vec::with_capacity(deps_len), |mut acc, dep| {
                    acc.push(
                        known_changes
                            .get(&(dep as usize))
                            .cloned()
                            .ok_or_else(|| {
                                tracing::error!(dependent_index=self.index, dep_index=dep, "could not find dependency");
                                Error::MissingChange
                            })?,
                    );
                    Ok(acc)
                })?;
        deps.sort();
        let other_actors =
            self.ops
                .iter()
                .try_fold(Vec::with_capacity(self.ops.len()), |mut acc, (_, op)| {
                    match op.key {
                        Key::Seq(ElemId(elem)) => {
                            if elem.actor() != self.actor {
                                acc.push(
                                    actors
                                        .safe_get(elem.actor())
                                        .cloned()
                                        .ok_or(Error::MissingActor)?,
                                );
                            }
                        }
                        Key::Map(_) => {}
                    };
                    Ok(acc)
                })?;
        let mut ops_data = Vec::new();
        let num_ops = self.ops.len() as u64;
        let columns = ChangeOpsColumns::empty().encode(
            self.ops.into_iter().map(|(obj, op)| {
                let action_index = op.action.action_index();
                ChangeOp {
                    key: match op.key {
                        // SAFETY: The caller must ensure that all props in the ops are in the propmap
                        Key::Map(idx) => StoredKey::Prop(props.safe_get(idx).unwrap().into()),
                        Key::Seq(elem) => StoredKey::Elem(elem),
                    },
                    insert: op.insert,
                    val: match op.action {
                        OpType::Make(_) | OpType::Del => PrimVal::Null,
                        OpType::Inc(i) => PrimVal::Int(i),
                        OpType::Set(v) => v.into(),
                    },
                    action: action_index,
                    pred: op.pred,
                    obj,
                }
            }),
            &mut ops_data,
        );
        Ok(StoredChange {
            dependencies: deps,
            actor: actors
                .safe_get(self.actor)
                .cloned()
                .ok_or(Error::MissingActor)?,
            other_actors,
            seq: self.seq,
            start_op: self.max_op - num_ops,
            timestamp: self.timestamp,
            message: self.message.map(|s| s.to_string()),
            ops_meta: columns.metadata(),
            ops_data: Cow::Owned(ops_data),
            extra_bytes: self.extra_bytes,
        })
    }
}
