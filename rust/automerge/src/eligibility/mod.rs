//! EXPERIMENTAL prototype A: external interpretation of whole-change
//! eligibility. Disposable; not a production API.
//!
//! Pure core: [`evidence`] (authority/eligibility evaluation) and
//! [`Selection`]/[`ViewSpec`] (a captured whole-change classification at
//! explicit heads). Shell: [`Session`] (staged delivery groups, captures,
//! transitions).

mod evidence;
mod session;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::clock::{Clock, ClockRange, OpMask};
use crate::exid::ExId;
use crate::iter::DiffIter;
use crate::patches::PatchLog;
use crate::types::{ChangeHash, ObjMeta};
use crate::{Automerge, AutomergeError, Patch, Prop, Value};

pub use evidence::{
    authorities, evaluate, Authority, AuthorizationContextId, ChangeFacts, ContextBinding,
    ContextKind, Decision, Eligibility, EventId, Evidence, EvidenceError, EvidenceLog, GraphFacts,
    Reason,
};
pub use session::{
    Capture, Envelope, Input, Session, SessionError, SessionId, StatusDelta, Transition, ViewId,
};

/// Whole-change classification of every integrated change (hash-keyed;
/// non-prefix selections are expressible).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection(Arc<BTreeMap<ChangeHash, Eligibility>>);

impl Selection {
    pub fn new(map: BTreeMap<ChangeHash, Eligibility>) -> Self {
        Self(Arc::new(map))
    }

    pub fn get(&self, hash: &ChangeHash) -> Option<Eligibility> {
        self.0.get(hash).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChangeHash, &Eligibility)> {
        self.0.iter()
    }
}

/// Content heads plus the selection to interpret them under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewSpec {
    pub heads: Vec<ChangeHash>,
    pub selection: Selection,
}

#[derive(Debug, thiserror::Error)]
pub enum ViewError {
    #[error("head {0} is not integrated in this document")]
    ForeignHeads(ChangeHash),
    #[error("integrated change {0} has no classification in the selection")]
    UnclassifiedChange(ChangeHash),
    #[error(transparent)]
    Automerge(#[from] AutomergeError),
}

impl Automerge {
    /// Compile a [`ViewSpec`] into a read scope against the current actor
    /// table. Excluded and pending changes are masked out; the structural
    /// clock is the ordinary clock at `heads`. Never cached across imports.
    pub(crate) fn scope_for(&self, view: &ViewSpec) -> Result<Clock, ViewError> {
        for h in &view.heads {
            if !self.change_graph.has_change(h) {
                return Err(ViewError::ForeignHeads(*h));
            }
        }
        let mut mask = OpMask::new(self.ops.actors.len());
        for (hash, eligibility) in view.selection.iter() {
            if *eligibility == Eligibility::Eligible {
                continue;
            }
            if let Some((actor, range)) = self.change_graph.op_range(hash) {
                mask.exclude(actor, range);
            }
        }
        // Every integrated change within the ancestry of `heads` must be
        // classified. Changes outside (e.g. integrated after this view was
        // captured) are structurally absent from the view and need none.
        for hash in self.change_graph.iter_hashes() {
            if view.selection.get(&hash).is_none()
                && self.change_graph.in_ancestry(&view.heads, &hash) == Some(true)
            {
                return Err(ViewError::UnclassifiedChange(hash));
            }
        }
        let clock = self.change_graph.clock_at(&view.heads);
        Ok(clock.with_mask(Some(Arc::new(mask))))
    }

    /// Materialize the whole document under `view`.
    pub fn hydrate_view(&self, view: &ViewSpec) -> Result<crate::hydrate::Value, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self.hydrate_map(&crate::types::ObjId::root(), Some(&clock)))
    }

    /// All visible candidates for a property under `view` (conflict
    /// inspection).
    pub fn get_all_view<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        view: &ViewSpec,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self.get_all_for(obj, prop, Some(clock))?)
    }

    /// Content patches from `before` to `after`, both compiled against this
    /// document's current graph and actor table.
    pub fn diff_view(&self, before: &ViewSpec, after: &ViewSpec) -> Result<Vec<Patch>, ViewError> {
        let before_clock = self.scope_for(before)?;
        let after_clock = self.scope_for(after)?;
        let range = ClockRange::Diff(before_clock, after_clock.clone());
        let mut patch_log = PatchLog::active();
        DiffIter::log(self, ObjMeta::root(), range, &mut patch_log, true);
        patch_log.scope = Some(after_clock);
        Ok(patch_log.make_patches(self))
    }
}

impl Automerge {
    /// EXPERIMENTAL: start a transaction whose reads (predecessor and index
    /// selection, object checks) are scoped to `view`. Dependencies are the
    /// captured heads; the actor is isolated with the baseline structural
    /// precondition (`isolate_actor`); the transaction's own new ops are made
    /// visible by isolating the actor in the scope clock while the
    /// eligibility mask of previously classified changes is retained.
    pub fn transaction_view(
        &mut self,
        view: &ViewSpec,
    ) -> Result<crate::transaction::Transaction<'_>, ViewError> {
        // Validate the view first (heads present, classification complete).
        self.scope_for(view)?;
        // Actor allocation/isolation may insert actors and shift indices, so
        // the actor-indexed mask must be compiled *after* it, against the
        // resulting actor table.
        let mut args = self.transaction_args(Some(&view.heads));
        let mask = self.scope_for(view)?.mask().cloned();
        // `transaction_args(Some(heads))` produced the structural isolation
        // clock (heads clock with this actor isolated). Attach the mask.
        args.scope = args.scope.take().map(|c| c.with_mask(mask));
        Ok(crate::transaction::Transaction::new(
            self,
            args,
            PatchLog::inactive(),
        ))
    }
}

impl Automerge {
    /// Visible list/text elements under `view`: `(index, value, value_op_id)`.
    /// The id is the selected *value* operation (a replacement's own id), not
    /// the insertion element identity; see [`Self::list_elements_view`] for both.
    pub fn list_view<O: AsRef<ExId>>(
        &self,
        view: &ViewSpec,
        obj: O,
    ) -> Result<Vec<(usize, Value<'static>, ExId)>, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self
            .list_range_for(obj.as_ref(), .., Some(clock))
            .map(|item| {
                let id = item.id();
                (item.index, item.value.into_owned().into(), id)
            })
            .collect())
    }

    pub fn text_view<O: AsRef<ExId>>(&self, view: &ViewSpec, obj: O) -> Result<String, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self.text_for(obj.as_ref(), Some(clock))?)
    }

    pub fn spans_view<O: AsRef<ExId>>(
        &self,
        view: &ViewSpec,
        obj: O,
    ) -> Result<Vec<crate::iter::Span>, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self.spans_for(obj.as_ref(), Some(clock))?.collect())
    }

    pub fn marks_view<O: AsRef<ExId>>(
        &self,
        view: &ViewSpec,
        obj: O,
    ) -> Result<Vec<crate::marks::Mark>, ViewError> {
        let clock = self.scope_for(view)?;
        Ok(self.marks_for(obj.as_ref(), Some(clock))?)
    }
}

impl Automerge {
    /// Visible list elements under `view` with both identities:
    /// `(index, value, value_op_id, element_id)`. The value op id is the
    /// selected value operation (a replacement's own id); the element id is
    /// the original insertion the sequence key addresses.
    pub fn list_elements_view<O: AsRef<ExId>>(
        &self,
        view: &ViewSpec,
        obj: O,
    ) -> Result<Vec<(usize, Value<'static>, ExId, ExId)>, ViewError> {
        let clock = self.scope_for(view)?;
        let mut out = Vec::new();
        for item in self.list_range_for(obj.as_ref(), .., Some(clock.clone())) {
            let value_id = item.id();
            let value_op = item.op_id();
            let elem = self
                .ops()
                .find_op_by_id_and_vis(&value_op, None)
                .and_then(|(op, _)| op.cursor().ok())
                .map(|e| e.0)
                .unwrap_or(value_op);
            out.push((
                item.index,
                item.value.into_owned().into(),
                value_id,
                self.id_to_exid(elem),
            ));
        }
        Ok(out)
    }
}
