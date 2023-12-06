use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    num::NonZeroU64,
};

use tracing::instrument;

use crate::{
    op_set::OpIdx,
    op_tree::OpSetData,
    storage::{
        change::{PredOutOfOrder, Verified},
        convert::op_as_actor_id,
        Change as StoredChange, ChangeMetadata,
    },
    types::{ChangeHash, OpId},
};

use fxhash::FxBuildHasher;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("a change referenced an actor index we couldn't find")]
    MissingActor,
    #[error("changes out of order")]
    ChangesOutOfOrder,
    #[error("missing change")]
    MissingChange,
    #[error("unable to read change osd: {0}")]
    ReadChange(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("incorrect max op")]
    IncorrectMaxOp,
    #[error("missing ops")]
    MissingOps,
}

pub(crate) struct ChangeCollector<'a> {
    changes_by_actor: HashMap<usize, Vec<PartialChange<'a>>, FxBuildHasher>,
}

pub(crate) struct CollectedChanges<'a> {
    pub(crate) history: Vec<StoredChange<'a, Verified>>,
    pub(crate) heads: BTreeSet<ChangeHash>,
}

impl<'a> ChangeCollector<'a> {
    pub(crate) fn new<E: std::error::Error + Send + Sync + 'static, I>(
        changes: I,
    ) -> Result<ChangeCollector<'a>, Error>
    where
        I: IntoIterator<Item = Result<ChangeMetadata<'a>, E>>,
    {
        let mut changes_by_actor: HashMap<usize, Vec<PartialChange<'_>>, FxBuildHasher> =
            HashMap::default();
        for (index, change) in changes.into_iter().enumerate() {
            tracing::trace!(?change, "importing change osd");
            let change = change.map_err(|e| Error::ReadChange(Box::new(e)))?;
            let actor_changes = changes_by_actor.entry(change.actor).or_default();
            if let Some(prev) = actor_changes.last() {
                // Note that we allow max_op to be equal to the previous max_op in case the
                // previous change had no ops (which is permitted)
                if prev.max_op > change.max_op {
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
        tracing::trace!(num_changes, "change collection context created");
        Ok(ChangeCollector { changes_by_actor })
    }

    #[instrument(skip(self))]
    pub(crate) fn collect(&mut self, opid: OpId, idx: OpIdx) -> Result<(), Error> {
        let actor_changes = self
            .changes_by_actor
            .get_mut(&opid.actor())
            .ok_or_else(|| {
                tracing::error!(missing_actor = opid.actor(), "missing actor for op");
                Error::MissingActor
            })?;
        let change_index = actor_changes.partition_point(|c| c.max_op < opid.counter());
        let change = actor_changes.get_mut(change_index).ok_or_else(|| {
            tracing::error!(missing_change_index = change_index, "missing change for op");
            Error::MissingChange
        })?;
        change.ops.push(idx);
        Ok(())
    }

    #[instrument(skip(self, osd))]
    pub(crate) fn finish(self, osd: &OpSetData) -> Result<CollectedChanges<'static>, Error> {
        let mut changes_in_order =
            Vec::with_capacity(self.changes_by_actor.values().map(|c| c.len()).sum());
        for (_, changes) in self.changes_by_actor {
            let mut seq = None;
            for change in changes {
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

        let mut hashes_by_index = HashMap::default();
        let mut history = Vec::new();
        let mut heads = BTreeSet::new();
        for (index, change) in changes_in_order.into_iter().enumerate() {
            let finished = change.finish(&hashes_by_index, osd)?;
            let hash = finished.hash();
            hashes_by_index.insert(index, hash);
            for dep in finished.dependencies() {
                heads.remove(dep);
            }
            heads.insert(hash);
            history.push(finished.into_owned());
        }

        Ok(CollectedChanges { history, heads })
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
    ops: Vec<OpIdx>,
}

impl<'a> PartialChange<'a> {
    /// # Panics
    ///
    /// * If any op references a property index which is not in `props`
    /// * If any op references an actor index which is not in `actors`
    #[instrument(skip(self, known_changes, osd))]
    fn finish(
        mut self,
        known_changes: &HashMap<usize, ChangeHash, FxBuildHasher>,
        osd: &OpSetData,
    ) -> Result<StoredChange<'a, Verified>, Error> {
        let deps_len = self.deps.len();
        let mut deps = self.deps.into_iter().try_fold::<_, _, Result<_, Error>>(
            Vec::with_capacity(deps_len),
            |mut acc, dep| {
                acc.push(known_changes.get(&(dep as usize)).cloned().ok_or_else(|| {
                    tracing::error!(
                        dependent_index = self.index,
                        dep_index = dep,
                        "could not find dependency"
                    );
                    Error::MissingChange
                })?);
                Ok(acc)
            },
        )?;
        deps.sort();
        let num_ops = self.ops.len() as u64;
        self.ops.sort_by_key(|o| o.as_op(osd).id().counter());
        let converted_ops = self.ops.iter().map(|idx| op_as_actor_id(idx.as_op(osd)));
        let actor = osd
            .actors
            .safe_get(self.actor)
            .ok_or_else(|| {
                tracing::error!(actor_index = self.actor, "actor out of bounds");
                Error::MissingActor
            })?
            .clone();

        if num_ops > self.max_op {
            return Err(Error::IncorrectMaxOp);
        }

        let change = match StoredChange::builder()
            .with_dependencies(deps)
            .with_actor(actor)
            .with_seq(self.seq)
            .with_start_op(NonZeroU64::new(self.max_op - num_ops + 1).ok_or(Error::MissingOps)?)
            .with_timestamp(self.timestamp)
            .with_message(self.message.map(|s| s.to_string()))
            .with_extra_bytes(self.extra_bytes.into_owned())
            .build(converted_ops)
        {
            Ok(s) => s,
            Err(PredOutOfOrder) => {
                // SAFETY: types::OpBuilder::preds is `types::OpIds` which ensures ops are always sorted
                panic!("preds out of order");
            }
        };
        #[cfg(not(debug_assertions))]
        tracing::trace!(?change, hash=?change.hash(), "collected change");
        #[cfg(debug_assertions)]
        {
            tracing::trace!(?change, ops=?self.ops, hash=?change.hash(), "collected change");
        }
        Ok(change)
    }
}
