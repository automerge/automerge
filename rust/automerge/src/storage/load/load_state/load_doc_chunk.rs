use crate::{
    op_set2::{change::ChangeCollector, op_set::OpIterState},
    storage::{
        document::ReconstructError,
        load::{Error as LoadError, VerificationMode},
        Document,
    },
    Automerge, TextEncoding,
};

use super::collector_cell::{CollectorCell, CollectorOwner};
use super::StepResult;

#[cfg(test)]
const BATCH_SIZE: usize = 250;
#[cfg(not(test))]
const BATCH_SIZE: usize = 100_000;

/// Step-able loader for a single document chunk.
///
/// Owns the parsed [`Document`], plus a [`CollectorCell`] that bundles the
/// `OpSet`, change-graph columns, index builder and a live change collector
/// borrowing from them. Between [`step`](Self::step) calls the op iterator is
/// suspended into an [`OpIterState`]; the collector stays live inside the
/// cell.
pub(crate) struct LoadingDocChunk<'a> {
    encoding: TextEncoding,
    verification_mode: VerificationMode,
    doc: Document<'a>,
    phase: Phase,
}

enum Phase {
    NotStarted,
    /// Iterating ops. Boxed because [`OpIterState`] is significantly larger
    /// than the other variants.
    Processing(Box<ProcessingPhase>),
    /// Iteration complete; ready to collect changes. Boxed so this variant is
    /// pointer-sized rather than the size of a [`CollectorCell`].
    Finishing(Box<CollectorCell>),
}

struct ProcessingPhase {
    cell: CollectorCell,
    iter_state: OpIterState,
}

impl<'a> LoadingDocChunk<'a> {
    pub(crate) fn new(
        doc: Document<'a>,
        encoding: TextEncoding,
        verification_mode: VerificationMode,
    ) -> Self {
        Self {
            encoding,
            verification_mode,
            doc,
            phase: Phase::NotStarted,
        }
    }

    pub(crate) fn step(mut self) -> Result<StepResult<Self, Automerge>, LoadError> {
        let phase = std::mem::replace(&mut self.phase, Phase::NotStarted);
        match phase {
            Phase::NotStarted => {
                let (cell, iter_state) = build(&self.doc, self.encoding)?;
                self.phase = Phase::Processing(Box::new(ProcessingPhase { cell, iter_state }));
                Ok(StepResult::Loading(self))
            }
            Phase::Processing(mut state) => {
                let done = run_batch(&mut state.cell, &mut state.iter_state)?;
                self.phase = if done {
                    Phase::Finishing(Box::new(state.cell))
                } else {
                    Phase::Processing(state)
                };
                Ok(StepResult::Loading(self))
            }
            Phase::Finishing(cell) => Ok(StepResult::Ready(self.finish(*cell)?)),
        }
    }

    fn finish(self, cell: CollectorCell) -> Result<Automerge, LoadError> {
        let (changes, owner) = cell.consume(|collector, op_set| {
            collector
                .collect(op_set)
                .map_err(|e| reconstruct_error(e.into()))
        })?;

        self.doc
            .verify_changes(&changes, self.verification_mode)
            .map_err(reconstruct_error)?;

        let CollectorOwner {
            mut op_set,
            change_cols,
            index_builder,
        } = owner;

        op_set.set_indexes(index_builder);

        let change_graph = change_cols.finalize(&changes.changes);

        debug_assert_eq!(changes.changes.len(), change_graph.len());
        debug_assert!(op_set.validate_top_index());

        Ok(Automerge::from_parts(op_set, change_graph))
    }
}

fn build(
    doc: &Document<'_>,
    encoding: TextEncoding,
) -> Result<(CollectorCell, OpIterState), LoadError> {
    use crate::change_graph::ChangeGraphCols;
    use crate::op_set2::OpSet;

    let op_set = OpSet::load(doc, encoding).map_err(|e| reconstruct_error(e.into()))?;
    let change_cols = ChangeGraphCols::load(doc).map_err(reconstruct_error)?;
    let index_builder = op_set.index_builder();
    // Initial iterator state: a fresh iter that hasn't advanced. The first
    // `run_batch` will resume from position 0.
    let iter_state = op_set.iter().suspend();

    let owner = CollectorOwner {
        op_set,
        change_cols,
        index_builder,
    };
    let cell = CollectorCell::try_new(owner, |change_cols, op_set, index_builder| {
        let collector = ChangeCollector::try_new(change_cols, op_set)
            .map_err(|e| reconstruct_error(e.into()))?;
        Ok::<_, LoadError>(collector.with_index(index_builder))
    })?;

    Ok((cell, iter_state))
}

/// Run one batch of op iteration. Returns `true` when the iterator has been
/// exhausted (the caller should advance to the finalization phase).
fn run_batch(cell: &mut CollectorCell, iter_state: &mut OpIterState) -> Result<bool, LoadError> {
    cell.with_mut(|collector, owner| {
        let mut iter = iter_state
            .try_resume(&owner.op_set)
            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;

        for _ in 0..BATCH_SIZE {
            match iter
                .try_next()
                .map_err(|e| LoadError::InflateDocument(Box::new(e)))?
            {
                None => {
                    *iter_state = iter.suspend();
                    return Ok(true);
                }
                Some(op) => {
                    let op_id = op.id;
                    let op_is_counter = op.is_counter();
                    let op_succ = op.succ();

                    collector.process_op(op);

                    for id in op_succ {
                        collector.index.process_succ(op_is_counter, id);
                        collector.collector.process_succ(op_id, id);
                    }
                }
            }
        }

        *iter_state = iter.suspend();
        Ok(false)
    })
}

fn reconstruct_error(e: ReconstructError) -> LoadError {
    LoadError::InflateDocument(Box::new(e))
}
