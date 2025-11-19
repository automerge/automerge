use crate::change_graph::ChangeGraph;
use crate::op_set2::change::{ChangeCollector, CollectedChanges};
use crate::storage::document::ReadChangeError;
use std::collections::BTreeSet;

use crate::types::TextEncoding;
use crate::{
    change::Change,
    op_set2::{OpSet, PackError, ReadOpError},
    storage::Document,
    types::ChangeHash,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // FIXME - I need to do this check
    //#[error("the document contained ops which were out of order")]
    //OpsOutOfOrder,
    #[error("invalid changes: {0}")]
    InvalidChanges(#[from] super::change_collector::Error),
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
    #[error(transparent)]
    ReadChange(#[from] ReadChangeError),
}

pub(crate) struct MismatchedHeads {
    //changes: Vec<StoredChange<'static, Verified>>,
    changes: Vec<Change>,
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
    text_encoding: TextEncoding,
) -> Result<ReconOpSet, Error> {
    let mut op_set = OpSet::from_doc(doc, text_encoding)?;
    let index_builder2 = op_set.index_builder();
    let mut change_collector = ChangeCollector::new(
        doc.iter_changes().collect::<Result<Vec<_>, _>>()?,
        &op_set.actors,
    )?
    .with_index(index_builder2);
    let mut iter = op_set.iter();

    while let Some(op) = iter.try_next()? {
        let op_id = op.id;
        let op_is_counter = op.is_counter();
        let op_succ = op.succ();

        change_collector.process_op(op);

        for id in op_succ {
            change_collector.process_succ(op_id, id, op_is_counter);
        }
    }

    let (index, changes) = change_collector.build_changegraph(&op_set)?;

    verify_changes(&changes, doc, mode)?;

    op_set.set_indexes(index);

    debug_assert!(op_set.validate_top_index());

    Ok(ReconOpSet {
        changes: changes.changes,
        max_op: changes.max_op,
        op_set,
        heads: changes.heads,
        change_graph: changes.change_graph,
    })
}

// create all binary changes
// look for mismatched heads

fn verify_changes(
    cc: &CollectedChanges,
    doc: &Document<'_>,
    mode: VerificationMode,
) -> Result<(), Error> {
    if matches!(mode, VerificationMode::Check) {
        let expected_heads: BTreeSet<_> = doc.heads().iter().cloned().collect();
        if expected_heads != cc.heads {
            tracing::error!(?expected_heads, ?cc.heads, "mismatching heads");
            return Err(Error::MismatchingHeads(MismatchedHeads {
                changes: cc.changes.clone(),
                expected_heads,
                derived_heads: cc.heads.clone(),
            }));
        }
    }
    Ok(())
}

pub(crate) struct ReconOpSet {
    pub(crate) changes: Vec<Change>,
    pub(crate) max_op: u64,
    pub(crate) op_set: OpSet,
    pub(crate) heads: BTreeSet<ChangeHash>,
    pub(crate) change_graph: ChangeGraph,
}
