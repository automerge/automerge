use std::{borrow::Cow, marker::PhantomPinned, ops::Range, pin::Pin, ptr::NonNull};

use crate::{
    automerge::ChangeCollector,
    op_set2::change::BuildChangeMetadata,
    storage::{
        bundle::{BundleChangeIterUnverified, OpIterUnverified, ParseError},
        change::Unverified,
        columns::compression::Uncompressed,
        BundleStorage, Header, RawColumns,
    },
    ActorId, Change, ChangeHash, StepResult,
};

/// Pinned data that iterators and collectors will reference. Once pinned, this
/// cannot move, which allows us to safely create iterators that reference it.
struct LoadingBundleData<'a> {
    bytes: Cow<'a, [u8]>,
    #[allow(dead_code)]
    header: Header,
    deps: Vec<ChangeHash>,
    actors: Vec<ActorId>,
    ops_meta: RawColumns<Uncompressed>,
    ops_data: Range<usize>,
    changes_meta: RawColumns<Uncompressed>,
    changes_data: Range<usize>,
    /// Marker to opt out of `Unpin`, making `Pin` meaningful
    _pin: PhantomPinned,
}

/// State for the loading phase.
///
/// # Safety
///
/// The iterators and collectors here reference memory in a sibling
/// `Pin<Box<LoadingBundleData>>`. This is safe because:
/// 1. The data is pinned and cannot move
/// 2. This enum is dropped before the data (field ordering in `LoadingBundleChanges`)
/// 3. We never expose the iterators outside this module
enum LoadingState {
    Changes {
        iter: NonNull<BundleChangeIterUnverified<'static>>,
        changes: Vec<BuildChangeMetadata<'static>>,
        batch_size: Option<usize>,
    },
    Ops {
        /// Option so we can take the iterator in step() without double-free on Drop
        iter: Option<NonNull<OpIterUnverified<'static>>>,
        /// Option so we can take the collector in step() without double-free on Drop.
        /// Stored as NonNull to avoid transmuting references to 'static, which
        /// violates miri's Stacked Borrows model.
        collector: Option<NonNull<ChangeCollector<'static>>>,
        batch_size: Option<usize>,
    },
    Done,
}

impl Drop for LoadingState {
    fn drop(&mut self) {
        match self {
            LoadingState::Changes { iter, .. } => {
                // SAFETY: iter was created via Box::into_raw
                unsafe {
                    drop(Box::from_raw(iter.as_ptr()));
                }
            }
            LoadingState::Ops {
                iter, collector, ..
            } => {
                // Free iterator if it hasn't been taken yet
                if let Some(iter_ptr) = iter.take() {
                    // SAFETY: iter was created via Box::into_raw and is never moved afterwards
                    unsafe {
                        drop(Box::from_raw(iter_ptr.as_ptr()));
                    }
                }
                // Free collector if it hasn't been taken yet
                if let Some(collector_ptr) = collector.take() {
                    // SAFETY: collector was created via Box::into_raw and is never moved afterwards
                    unsafe {
                        drop(Box::from_raw(collector_ptr.as_ptr()));
                    }
                }
            }
            LoadingState::Done => {}
        }
    }
}

/// Interruptible bundle change loading state machine.
///
/// This struct uses pinning and raw pointers to safely store iterators
/// alongside the data they reference.
pub(crate) struct LoadingBundleChanges<'a> {
    // IMPORTANT: Field order determines drop order.
    // `state` contains iterators/collectors referencing `data`, so must be dropped first.
    state: LoadingState,
    data: Pin<Box<LoadingBundleData<'a>>>,
}

impl<'a> LoadingBundleChanges<'a> {
    pub(super) fn new(
        storage: BundleStorage<'a, Unverified>,
        _ops_batch_size: Option<usize>,
        change_batch_size: Option<usize>,
    ) -> Self {
        // Pin the data so its address is stable
        let data = Box::pin(LoadingBundleData {
            bytes: storage.bytes,
            header: storage.header,
            deps: storage.deps,
            actors: storage.actors,
            ops_meta: storage.ops_meta,
            ops_data: storage.ops_data,
            changes_meta: storage.changes_meta,
            changes_data: storage.changes_data,
            _pin: PhantomPinned,
        });

        // Create the change metadata iterator referencing the pinned data
        // SAFETY: data is pinned and won't move. The 'static lifetime is a lie -
        // the true lifetime is "until data is dropped" - but we guarantee the
        // iterator is dropped first via field ordering.
        let iter = unsafe {
            let data_ref = data.as_ref().get_ref();
            let change_data: &'static [u8] =
                std::mem::transmute(&data_ref.bytes[data_ref.changes_data.clone()]);
            let changes_meta: &'static RawColumns<Uncompressed> =
                std::mem::transmute(&data_ref.changes_meta);

            let iter = BundleChangeIterUnverified::new(changes_meta, change_data);
            NonNull::new_unchecked(Box::into_raw(Box::new(iter)))
        };

        LoadingBundleChanges {
            state: LoadingState::Changes {
                iter,
                changes: Vec::new(),
                batch_size: change_batch_size,
            },
            data,
        }
    }

    /// Create an op iterator referencing the pinned data.
    ///
    /// # Safety
    ///
    /// The returned iterator has a fake 'static lifetime but actually references
    /// `self.data`. Caller must ensure the iterator is dropped before `self.data`.
    unsafe fn create_op_iter(&self) -> NonNull<OpIterUnverified<'static>> {
        let data_ref = self.data.as_ref().get_ref();
        let ops_data: &'static [u8] =
            std::mem::transmute(&data_ref.bytes[data_ref.ops_data.clone()]);
        let ops_meta: &'static RawColumns<Uncompressed> = std::mem::transmute(&data_ref.ops_meta);

        let iter = OpIterUnverified::new(ops_meta, ops_data);
        NonNull::new_unchecked(Box::into_raw(Box::new(iter)))
    }

    /// Create a collector referencing the pinned actors data.
    ///
    /// # Safety
    ///
    /// The returned NonNull points to a heap-allocated ChangeCollector that
    /// references `self.data.actors`. Caller must ensure the collector is
    /// dropped (via Box::from_raw) before `self.data` is dropped.
    unsafe fn create_collector(
        &self,
        changes: Vec<BuildChangeMetadata<'static>>,
    ) -> NonNull<ChangeCollector<'static>> {
        let data_ref = self.data.as_ref().get_ref();
        // Create the collector with actual lifetime, then immediately box it.
        // The NonNull erases the lifetime at the pointer level, avoiding the
        // need to transmute references which violates Stacked Borrows.
        let collector = ChangeCollector::from_change_meta(changes, &data_ref.actors);
        // Transmute the collector to 'static. This is safe because:
        // 1. The collector is immediately boxed and stored as a raw pointer
        // 2. We guarantee via drop ordering that it's freed before actors
        let collector: ChangeCollector<'static> = std::mem::transmute(collector);
        NonNull::new_unchecked(Box::into_raw(Box::new(collector)))
    }

    pub(crate) fn step(mut self) -> Result<StepResult<Self, Vec<Change>>, ParseError> {
        match &mut self.state {
            LoadingState::Changes {
                iter,
                changes,
                batch_size,
            } => {
                let mut iterations = 0;
                // SAFETY: iter points to a valid, live iterator
                let iter_ref = unsafe { iter.as_mut() };

                for change_meta_result in iter_ref.by_ref() {
                    // Verification happens here - we propagate any parse errors
                    let change_meta = change_meta_result?;
                    changes.push(change_meta.into());
                    iterations += 1;
                    if let Some(bs) = *batch_size {
                        if iterations >= bs {
                            return Ok(StepResult::Loading(self));
                        }
                    }
                }

                // Transition to Ops phase
                let old_batch_size = *batch_size;

                // Take ownership of changes before replacing state
                let changes = std::mem::take(changes);

                // Replace state to free the change iterator
                let old_state = std::mem::replace(&mut self.state, LoadingState::Done);
                drop(old_state);

                // Create the collector and op iterator
                // SAFETY: Both collector and iterator reference pinned data.
                // They will be stored in self.state which is dropped before self.data.
                let collector = unsafe { self.create_collector(changes) };
                let op_iter = unsafe { self.create_op_iter() };

                self.state = LoadingState::Ops {
                    iter: Some(op_iter),
                    collector: Some(collector),
                    batch_size: old_batch_size,
                };

                Ok(StepResult::Loading(self))
            }
            LoadingState::Ops {
                iter,
                collector,
                batch_size,
            } => {
                let mut iterations = 0;
                // SAFETY: The only place we move or invalidate iter and
                // collector is in this function after the iter_ref returns None
                // when we drop them both and transition to a new state. That
                // means that if iter and collector are Some (and we'll panic if
                // they're not so we don't get UB) then they are valid
                let iter_ref = unsafe { iter.as_mut().unwrap().as_mut() };
                let collector_ref = unsafe { collector.as_mut().unwrap().as_mut() };

                for op_result in iter_ref.by_ref() {
                    // Verification happens here - we propagate any parse errors
                    let op = op_result?;
                    collector_ref.add(op);
                    iterations += 1;
                    if let Some(bs) = *batch_size {
                        if iterations >= bs {
                            return Ok(StepResult::Loading(self));
                        }
                    }
                }

                // Take ownership of iterator and collector before transitioning state.
                // Using Option::take() means Drop won't try to free them again.
                let iter_ptr = iter.take().unwrap();
                let collector_ptr = collector.take().unwrap();

                // Free the iterator
                unsafe {
                    drop(Box::from_raw(iter_ptr.as_ptr()));
                }

                // Free the collector
                let collector = unsafe { *Box::from_raw(collector_ptr.as_ptr()) };

                // Transition to Done
                self.state = LoadingState::Done;

                // Get references to deps and actors for unbundle
                // SAFETY: We've freed the iterators, but collector still references actors.
                // However, unbundle() consumes the collector and returns owned Changes,
                // so after this call no references to the pinned data remain.
                let data_ref = self.data.as_ref().get_ref();

                let changes = collector
                    .unbundle(&data_ref.actors, &data_ref.deps)
                    .map_err(|e| ParseError::Unbundle(Box::new(e)))?;

                Ok(StepResult::Ready(changes))
            }
            LoadingState::Done => {
                panic!("step() called on completed LoadingBundleChanges");
            }
        }
    }
}
