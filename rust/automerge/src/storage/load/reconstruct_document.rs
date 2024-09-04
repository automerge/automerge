use super::change_collector::ChangeCollector;
use std::collections::{BTreeSet, HashMap};

use crate::{
    change::Change,
    op_set2::{OpBuilder2, OpSet, PackError},
    storage::{change::Verified, Change as StoredChange, Document},
    types::{ChangeHash, OpId},
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("the document contained ops which were out of order")]
    OpsOutOfOrder,
    #[error("error reading operation: {0:?}")]
    ReadOp(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("an operation referenced a missing actor id")]
    MissingActor,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
    #[error("mismatching heads")]
    MismatchingHeads(MismatchedHeads),
    #[error("succ out of order")]
    SuccOutOfOrder,
    #[error(transparent)]
    InvalidOp(#[from] crate::error::InvalidOpType),
    #[error(transparent)]
    PackError(#[from] PackError),
}

pub(crate) struct MismatchedHeads {
    changes: Vec<StoredChange<'static, Verified>>,
    expected_heads: BTreeSet<ChangeHash>,
    derived_heads: BTreeSet<ChangeHash>,
}

impl std::fmt::Debug for MismatchedHeads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MismatchedHeads")
            .field("changes", &self.changes.len())
            .field("expected_heads", &self.expected_heads)
            .field("derived_heads", &self.derived_heads)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum VerificationMode {
    Check,
    DontCheck,
}

pub(crate) fn reconstruct_opset<'a>(
    doc: &'a Document<'a>,
    mode: VerificationMode,
) -> Result<ReconOpSet, Error> {
    let op_set = OpSet::new(doc)?;
    let mut change_collector = ChangeCollector::new(doc.iter_changes())?;
    let mut max_op = 0;
    let mut preds = HashMap::new();
    let mut last = None;
    for op in op_set.iter() {
        let next = Some((op.obj, op.elemid_or_key()));
        if last != next {
            if let Some((obj, key)) = last.take() {
                for (id, pred) in preds.drain() {
                    let del = OpBuilder2::del(id, obj.into(), key.into_owned(), pred);
                    change_collector.collect(del)?;
                }
            }
            last = next;
        }
        for id in op.succ() {
            preds.entry(id).or_default().push(op.id);
        }
        max_op = std::cmp::max(max_op, op.id.counter());

        let pred = preds.remove(&op.id);

        change_collector.collect(op.build(pred))?;
    }
    let (changes, heads) = flush_changes(change_collector, doc, mode, &op_set)?;

    Ok(ReconOpSet {
        changes,
        max_op,
        op_set,
        heads,
    })
}

// create all binary changes
// look for mismatched heads

fn flush_changes(
    change_collector: ChangeCollector<'_>,
    doc: &Document<'_>,
    mode: VerificationMode,
    op_set: &OpSet,
) -> Result<(Vec<Change>, BTreeSet<ChangeHash>), Error> {
    let super::change_collector::CollectedChanges { history, heads } =
        change_collector.finish(op_set)?;
    if matches!(mode, VerificationMode::Check) {
        let expected_heads: BTreeSet<_> = doc.heads().iter().cloned().collect();
        if expected_heads != heads {
            tracing::error!(?expected_heads, ?heads, "mismatching heads");
            return Err(Error::MismatchingHeads(MismatchedHeads {
                changes: history,
                expected_heads,
                derived_heads: heads,
            }));
        }
    }
    let changes = history.into_iter().map(Change::new).collect();
    Ok((changes, heads))
}

pub(crate) struct ReconOpSet {
    pub(crate) changes: Vec<Change>,
    pub(crate) max_op: u64,
    pub(crate) op_set: OpSet,
    pub(crate) heads: BTreeSet<ChangeHash>,
}

/// We construct the OpSet directly from the vector of actors which are encoded in the
/// start of the document. Therefore we need to check for each opid in the docuemnt that the actor
/// ID which it references actually exists in the op set data.
fn check_opid(op_set: &OpSet, opid: OpId) -> Result<OpId, Error> {
    match op_set.get_actor_safe(opid.actor()) {
        Some(_) => Ok(opid),
        None => {
            tracing::error!("missing actor");
            Err(Error::MissingActor)
        }
    }
}
