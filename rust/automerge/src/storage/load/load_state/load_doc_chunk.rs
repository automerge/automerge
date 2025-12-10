use std::{marker::PhantomPinned, mem::ManuallyDrop, pin::Pin, ptr::NonNull};

use crate::{
    automerge::ChangeCollector,
    op_set2::{
        change::collector::IndexedChangeCollector, columns::Columns, op_set::IndexBuilder, OpIter,
        OpSet,
    },
    storage::{
        document::DocChangeColumnIter,
        load::{reconstruct_document::verify_changes, Error as LoadError, ReconOpSet},
        DocChangeMetadata, Document,
    },
    ActorId, StepResult, TextEncoding, VerificationMode,
};

// This structure loads a document chunk in two phases:
// 1. Iterate over change metadata (LoadingChanges state)
// 2. Iterate over ops (LoadingOps state)
//
// The Document is pinned throughout the entire process. All iterators and
// collectors that reference the document's data are stored as raw pointers
// with erased lifetimes, and are guaranteed to be dropped before the document.
//
// Memory layout and drop order:
// - `state` is declared first and dropped first (contains iterators/collectors)
// - `data` is declared second and dropped second (contains Document)
//
// This ensures all references are dropped before the data they reference.
/// Pinned data that persists throughout the loading process
struct LoadingDocChunkData<'a> {
    doc: Document<'a>,
    /// Columns for op iteration, created when transitioning to LoadingOps phase.
    /// Option because it's not available during the LoadingChanges phase.
    columns: Option<Columns>,
    encoding: TextEncoding,
    verification_mode: VerificationMode,
    _pin: PhantomPinned,
}

/// State machine for the loading phases
enum LoadingDocChunkState {
    /// Phase 1: Iterating over change metadata
    LoadingChanges {
        iter: Option<NonNull<DocChangeColumnIter<'static>>>,
        changes: Vec<DocChangeMetadata<'static>>,
    },
    /// Phase 2: Iterating over ops
    LoadingOps {
        iter: Option<NonNull<OpIter<'static>>>,
        collector: Option<NonNull<IndexedChangeCollector<'static>>>,
    },
    /// Terminal state after extraction
    Done,
}

pub(super) struct LoadingDocChunk<'a> {
    // IMPORTANT: field order determines drop order. `state` drops first, then `data`.
    state: LoadingDocChunkState,
    data: Pin<Box<LoadingDocChunkData<'a>>>,
}

impl<'a> LoadingDocChunk<'a> {
    pub(super) fn new(
        doc: Document<'a>,
        encoding: TextEncoding,
        verification_mode: VerificationMode,
    ) -> Self {
        let data = Box::pin(LoadingDocChunkData {
            doc,
            columns: None,
            encoding,
            verification_mode,
            _pin: PhantomPinned,
        });

        // Create the change iterator referencing the pinned document's bytes.
        // SAFETY: The document is pinned in a Box, so its address is stable.
        // The iterator is heap-allocated and stored as a raw pointer.
        // Drop order guarantees the iterator is dropped before the document.
        let iter = data.doc.iter_changes();
        let iter: DocChangeColumnIter<'static> = unsafe { std::mem::transmute(iter) };
        let iter = NonNull::new(Box::into_raw(Box::new(iter))).unwrap();

        Self {
            state: LoadingDocChunkState::LoadingChanges {
                iter: Some(iter),
                changes: Vec::new(),
            },
            data,
        }
    }

    pub(super) fn step(self) -> Result<StepResult<Self, ReconOpSet>, LoadError> {
        match self.state {
            LoadingDocChunkState::LoadingChanges { .. } => self.step_loading_changes(),
            LoadingDocChunkState::LoadingOps { .. } => self.step_loading_ops(),
            LoadingDocChunkState::Done => {
                unreachable!("step called on Done state")
            }
        }
    }

    fn step_loading_changes(mut self) -> Result<StepResult<Self, ReconOpSet>, LoadError> {
        let (iter_opt, changes) = match &mut self.state {
            LoadingDocChunkState::LoadingChanges { iter, changes } => (iter, changes),
            _ => unreachable!(),
        };

        const BATCH_SIZE: usize = 100000;
        let mut iterations = 0;

        // SAFETY: We have exclusive access to self, and the iterator is valid
        // because the pinned data it references is still alive.
        let iter = unsafe { iter_opt.as_mut().unwrap().as_mut() };

        loop {
            let Some(change_meta) = iter.next() else {
                // Done iterating changes, transition to LoadingOps phase
                return self.transition_to_loading_ops();
            };

            let meta = change_meta.map_err(|e| {
                LoadError::InflateDocument(Box::new(
                    crate::storage::load::reconstruct_document::Error::from(e),
                ))
            })?;
            // SAFETY: meta references data in the pinned Document, which won't move.
            changes.push(unsafe {
                std::mem::transmute::<DocChangeMetadata<'_>, DocChangeMetadata<'_>>(meta)
            });

            iterations += 1;
            if iterations >= BATCH_SIZE {
                return Ok(StepResult::Loading(self));
            }
        }
    }

    fn transition_to_loading_ops(mut self) -> Result<StepResult<Self, ReconOpSet>, LoadError> {
        // Extract changes from the current state
        let changes = match &mut self.state {
            LoadingDocChunkState::LoadingChanges { iter, changes } => {
                // Free the change iterator (take sets it to None so Drop won't free again)
                if let Some(iter_ptr) = iter.take() {
                    unsafe {
                        let _ = Box::from_raw(iter_ptr.as_ptr());
                    }
                }
                std::mem::take(changes)
            }
            _ => unreachable!(),
        };

        // Build columns from the document
        let raw_cols = self.data.doc.op_metadata.clone();
        let cols = Columns::load(
            raw_cols.iter(),
            self.data.doc.op_raw_bytes(),
            self.data.doc.actors(),
        )
        .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;
        let num_rows = cols.len();

        // Store columns in the pinned data
        // SAFETY: We're modifying a field through Pin, but we're not moving the struct.
        unsafe {
            let data_mut = Pin::get_unchecked_mut(self.data.as_mut());
            // ensure that no other code has already stored columns which would
            // be dropped in the assignment
            assert!(data_mut.columns.is_none());
            data_mut.columns = Some(cols);
        }

        // Create the op iterator referencing the pinned columns
        // SAFETY: The columns are now stored in the pinned data, so their address is stable.
        let cols_ref: &Columns = self.data.columns.as_ref().unwrap();
        let cols_ref: &'static Columns = unsafe { std::mem::transmute(cols_ref) };
        let op_iter = OpIter::from_columns(cols_ref, &(0..num_rows));
        let op_iter: OpIter<'static> = unsafe { std::mem::transmute(op_iter) };
        let op_iter = NonNull::new(Box::into_raw(Box::new(op_iter))).unwrap();

        // Create the change collector referencing the pinned doc's actors
        // SAFETY: The document is pinned, so its actors slice address is stable.
        let actors: &[ActorId] = self.data.doc.actors();
        let actors: &'static [ActorId] = unsafe { std::mem::transmute(actors) };
        let index_builder = IndexBuilder::new(cols_ref, self.data.encoding);
        let collector = ChangeCollector::new(changes, actors)
            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?
            .with_index(index_builder);
        let collector: IndexedChangeCollector<'static> = unsafe { std::mem::transmute(collector) };
        let collector = NonNull::new(Box::into_raw(Box::new(collector))).unwrap();

        // Transition to LoadingOps state
        self.state = LoadingDocChunkState::LoadingOps {
            iter: Some(op_iter),
            collector: Some(collector),
        };

        Ok(StepResult::Loading(self))
    }

    fn step_loading_ops(mut self) -> Result<StepResult<Self, ReconOpSet>, LoadError> {
        let (iter_opt, collector_opt) = match &mut self.state {
            LoadingDocChunkState::LoadingOps { iter, collector } => (iter, collector),
            _ => unreachable!(),
        };

        // SAFETY: We have exclusive access to self, and the iterator/collector
        // are valid because the pinned data they reference is still alive.
        let iter = unsafe { iter_opt.as_mut().unwrap().as_mut() };
        let collector = unsafe { collector_opt.as_mut().unwrap().as_mut() };

        const BATCH_SIZE: usize = 100000;
        let mut iterations = 0;

        while let Some(op) = iter
            .try_next()
            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?
        {
            iterations += 1;
            let op_id = op.id;
            let op_is_counter = op.is_counter();
            let op_succ = op.succ();

            collector.process_op(op);

            for id in op_succ {
                collector.process_succ(op_id, id, op_is_counter);
            }
            if iterations >= BATCH_SIZE {
                return Ok(StepResult::Loading(self));
            }
        }

        // Done iterating ops - finalize
        self.finalize()
    }

    fn finalize(mut self) -> Result<StepResult<Self, ReconOpSet>, LoadError> {
        let mode = self.data.verification_mode;
        let encoding = self.data.encoding;

        // Extract collector and iterator from state
        let collector = match &mut self.state {
            LoadingDocChunkState::LoadingOps { iter, collector } => {
                // Free the op iterator
                if let Some(iter_ptr) = iter.take() {
                    unsafe {
                        let _ = Box::from_raw(iter_ptr.as_ptr());
                    }
                }
                // Take the collector
                let collector_ptr = collector.take().unwrap();
                let collector: IndexedChangeCollector<'a> =
                    unsafe { std::mem::transmute(*Box::from_raw(collector_ptr.as_ptr())) };
                collector
            }
            _ => unreachable!(),
        };

        // Mark state as Done to prevent Drop from freeing already-freed pointers
        self.state = LoadingDocChunkState::Done;

        // Extract data from the pinned box
        // SAFETY: All iterators/collectors have been freed, so we can safely unpin.
        // We use ManuallyDrop to prevent self.data from being dropped when we move it out.
        let this = ManuallyDrop::new(self);
        let data = unsafe { Pin::into_inner_unchecked(std::ptr::read(&this.data)) };
        let columns = data.columns.unwrap();
        let doc = data.doc;
        // this.state is Done, so its Drop is a no-op. We don't need to do anything else.

        let mut op_set = OpSet::from_parts(columns, doc.actors().to_vec(), encoding);

        let (index, changes) = collector
            .build_changegraph(&op_set)
            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;

        op_set.set_indexes(index);

        verify_changes(&changes, &doc, mode)
            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;

        debug_assert!(op_set.validate_top_index());

        let recon = ReconOpSet {
            changes: changes.changes,
            max_op: changes.max_op,
            op_set,
            heads: changes.heads,
            change_graph: changes.change_graph,
        };

        Ok(StepResult::Ready(recon))
    }
}

impl Drop for LoadingDocChunkState {
    fn drop(&mut self) {
        match self {
            LoadingDocChunkState::LoadingChanges { iter, changes: _ } => {
                // Free the change iterator if present
                if let Some(iter_ptr) = iter.take() {
                    unsafe {
                        let _ = Box::from_raw(iter_ptr.as_ptr());
                    }
                }
            }
            LoadingDocChunkState::LoadingOps { iter, collector } => {
                // Free the op iterator if present
                if let Some(iter_ptr) = iter.take() {
                    unsafe {
                        let _ = Box::from_raw(iter_ptr.as_ptr());
                    }
                }
                // Free the collector if present
                if let Some(collector_ptr) = collector.take() {
                    unsafe {
                        let _ = Box::from_raw(collector_ptr.as_ptr());
                    }
                }
            }
            LoadingDocChunkState::Done => {
                // Nothing to free
            }
        }
    }
}
