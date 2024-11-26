use super::change_collector::ChangeCollector;
use std::collections::BTreeSet;

use crate::{
    change::Change,
    op_set2::{OpSet, PackError, ReadOpError},
    storage::{change::Verified, Change as StoredChange, Document},
    types::ChangeHash,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // FIXME - I need to do this check
    //#[error("the document contained ops which were out of order")]
    //OpsOutOfOrder,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
    #[error("mismatched max_op ops={0}, changes={0}")]
    MismatchingMaxOp(u64, u64),
    #[error("mismatching heads")]
    MismatchingHeads(MismatchedHeads),
    // FIXME - i need to do this check
    //#[error("succ out of order")]
    //SuccOutOfOrder,
    #[error(transparent)]
    InvalidOp(#[from] crate::error::InvalidOpType),
    #[error(transparent)]
    PackErr(#[from] PackError),
    #[error(transparent)]
    ReadOpErr(#[from] ReadOpError),
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
    let mut op_set = OpSet::new(doc)?;
    let mut change_collector = ChangeCollector::new(doc.iter_changes())?;
    let mut iter = op_set.iter();
    let mut index_builder = op_set.index_builder();

    while let Some(op) = iter.try_next()? {
        let op_id = op.id;
        let op_is_counter = op.is_counter();
        let op_succ = op.succ();
        index_builder.process_op(&op);
        change_collector.process_op(op)?;

        for id in op_succ {
            change_collector.process_succ(op_id, id);
            index_builder.process_succ(op_is_counter, id);
        }
    }

    let (changes, heads, max_op) = flush_changes(change_collector, doc, mode, &op_set)?;

    op_set.set_indexes(index_builder);

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
) -> Result<(Vec<Change>, BTreeSet<ChangeHash>, u64), Error> {
    let super::change_collector::CollectedChanges {
        history,
        heads,
        max_op,
    } = change_collector.finish(op_set)?;
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
    let changes = history.into_iter().map(Change::new).collect::<Vec<_>>();
    let max_op2 = changes.iter().map(|c| c.max_op()).max().unwrap_or(0);
    if max_op != max_op2 {
        return Err(Error::MismatchingMaxOp(max_op, max_op2));
    }
    Ok((changes, heads, max_op))
}

pub(crate) struct ReconOpSet {
    pub(crate) changes: Vec<Change>,
    pub(crate) max_op: u64,
    pub(crate) op_set: OpSet,
    pub(crate) heads: BTreeSet<ChangeHash>,
}
