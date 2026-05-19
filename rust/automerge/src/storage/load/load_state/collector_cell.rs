//! Safe wrapper around the self-referential pairing of an
//! [`IndexedChangeCollector`] with the data it borrows from.
//!
//! `IndexedChangeCollector<'a>` borrows from an [`OpSet`], a
//! [`ChangeGraphCols`], and an [`IndexBuilder`]. To keep all of these alive
//! together across yield points we need a self-referential struct. Rust's type
//! system can't directly express self-references, so this module hides the
//! required `unsafe` behind a closure-based API: callers never see the
//! `'static` lifetime erasure used internally.
//!
//! All `unsafe` code in the interruptible loader lives here.

use std::mem::ManuallyDrop;

use crate::change_graph::ChangeGraphCols;
use crate::op_set2::{change::collector::IndexedChangeCollector, op_set::IndexBuilder, OpSet};

/// The owned data that an [`IndexedChangeCollector`] borrows from.
pub(crate) struct CollectorOwner {
    pub(crate) op_set: OpSet,
    pub(crate) change_cols: ChangeGraphCols,
    pub(crate) index_builder: IndexBuilder,
}

/// A live [`IndexedChangeCollector`] paired with the [`CollectorOwner`] it
/// borrows from.
///
/// The owner sits in a `Box` so the addresses of its fields are stable; the
/// collector's actual borrows point into that box. The collector is stored
/// internally with its lifetime erased to `'static`; access is only ever via
/// the closure-based methods below, which re-narrow the lifetime to a borrow
/// tied to `&self` / `&mut self`.
pub(crate) struct CollectorCell {
    // Field declaration order matters for `Drop`. `collector` references
    // `owner` and must be dropped first. Do not reorder these fields.
    collector: ManuallyDrop<IndexedChangeCollector<'static>>,
    owner: ManuallyDrop<Box<CollectorOwner>>,
}

impl CollectorCell {
    /// Build a new cell from an owned [`CollectorOwner`] plus a constructor
    /// closure. The closure is handed split borrows into the (now heap-pinned)
    /// owner and returns the live collector.
    pub(crate) fn try_new<E>(
        owner: CollectorOwner,
        build: impl for<'a> FnOnce(
            &'a ChangeGraphCols,
            &'a OpSet,
            &'a mut IndexBuilder,
        ) -> Result<IndexedChangeCollector<'a>, E>,
    ) -> Result<Self, E> {
        let mut owner = Box::new(owner);
        let collector = {
            let CollectorOwner {
                op_set,
                change_cols,
                index_builder,
            } = &mut *owner;
            build(change_cols, op_set, index_builder)?
        };
        // SAFETY: `collector` borrows from `*owner`. `owner` lives in a `Box`,
        // so its address is stable, and we own it for the lifetime of `self`.
        // We erase the lifetime to `'static` purely for storage; every
        // accessor below re-narrows it to a borrow tied to `&self` / `&mut
        // self`. The `Drop` impl drops the collector before the owner.
        let collector: IndexedChangeCollector<'static> = unsafe { std::mem::transmute(collector) };
        Ok(Self {
            collector: ManuallyDrop::new(collector),
            owner: ManuallyDrop::new(owner),
        })
    }

    /// Access the collector and owner together with mutually consistent
    /// lifetimes. The HRTB on `f` prevents references from escaping.
    pub(crate) fn with_mut<R>(
        &mut self,
        f: impl for<'a> FnOnce(&'a mut IndexedChangeCollector<'a>, &'a CollectorOwner) -> R,
    ) -> R {
        // SAFETY: re-narrow the stored `'static` lifetime to a borrow tied to
        // `&mut self`. The collector borrows from `*self.owner`, which is
        // shared-borrowed for the duration of `f`. The HRTB ensures the
        // closure can't smuggle a reference out.
        let collector: &mut IndexedChangeCollector<'_> =
            unsafe { std::mem::transmute(&mut *self.collector) };
        f(collector, &self.owner)
    }

    /// Consume the cell. Calls `f` with the live collector and a borrow of
    /// the owner's [`OpSet`]; once `f` returns, hands back its result along
    /// with the unboxed [`CollectorOwner`].
    pub(crate) fn consume<R, E>(
        self,
        f: impl for<'a> FnOnce(IndexedChangeCollector<'a>, &'a OpSet) -> Result<R, E>,
    ) -> Result<(R, CollectorOwner), E> {
        // Suppress the normal `Drop` so we can take the fields out by value.
        let mut this = ManuallyDrop::new(self);
        // SAFETY: we never touch `this.collector` after this take.
        let collector = unsafe { ManuallyDrop::take(&mut this.collector) };
        // SAFETY: we never touch `this.owner` after this take.
        let owner_box = unsafe { ManuallyDrop::take(&mut this.owner) };

        // SAFETY: `collector` references `*owner_box`. The box stays alive on
        // this stack frame until after `f` runs; the closure consumes the
        // collector (or drops it if it panics), so by the time we unbox the
        // owner the collector is gone. The HRTB on `f` prevents references
        // from escaping.
        let collector: IndexedChangeCollector<'_> = unsafe { std::mem::transmute(collector) };

        let result = f(collector, &owner_box.op_set)?;
        Ok((result, *owner_box))
    }
}

impl Drop for CollectorCell {
    fn drop(&mut self) {
        // SAFETY: `collector` borrows from `*owner`, so we drop it first. We
        // do not touch either field after dropping it.
        unsafe {
            ManuallyDrop::drop(&mut self.collector);
            ManuallyDrop::drop(&mut self.owner);
        }
    }
}
