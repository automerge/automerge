//! Describe how values transitions for one map property or sequence element
//! during a batch. These descriptions are then used to update the tracking
//! [`PatchLog`].
//!
//! A [`ValueState`] accumulates the visible candidates before and after the
//! batch into two [`CandidateSummary`]'s. These are handed to
//! [`ValueTransition::new`], which emits the result via either
//! [`ValueTransition::emit_map`] or [`ValueTransition::emit_sequence`].
//!
//! [`ValueState`]: super::ValueState

use crate::hydrate::Value;
use crate::iter::RichTextDiff;
use crate::types::{ObjId, OpId, Prop, ScalarValue, SequenceType};
use crate::{PatchLog, TextEncoding};

/// Whether this candidate is present in the document before this batch, or is
/// part of the incoming batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateOrigin {
    Existing,
    Incoming,
}

/// A value candidate for a map property or sequence element.
#[derive(Debug, Clone)]
struct Candidate {
    /// The [`OpId`] of the operation.
    id: OpId,
    /// A shallow value. Objects are populated by child patches or explicit
    /// exposure.
    value: Value,
    /// Where this candidate is coming from during processing of the batch.
    origin: CandidateOrigin,
}

/// The visible candidates of a nonempty summary: the winner and how many
/// others lost to it.
#[derive(Debug, Clone)]
struct PresentCandidates {
    /// The candidate with the greatest [`OpId`].
    winner: Candidate,
    /// Candidates other than the winner, including losing conflicts.
    other_candidates: usize,
}

impl PresentCandidates {
    /// Returns `true` if there is more than one candidate.
    fn conflicted(&self) -> bool {
        self.other_candidates > 0
    }
}

#[derive(Debug, Default, Clone)]
enum Summary {
    #[default]
    Empty,
    Present(PresentCandidates),
}

/// Summarizes the visible value candidates for one map property or sequence
/// element at one point in time.
///
/// Retains the candidate with the greatest [`OpId`] and counts all candidates,
/// including losing conflicts. Each candidate must be added exactly once. A
/// nonempty summary always has a winner.
#[derive(Debug, Default, Clone)]
pub(super) struct CandidateSummary(Summary);

impl CandidateSummary {
    /// Add a candidate that is present in the document before this batch.
    pub(super) fn add_existing(&mut self, id: OpId, value: Value) {
        self.add(Candidate {
            id,
            value,
            origin: CandidateOrigin::Existing,
        });
    }

    /// Add a candidate that is part of the incoming batch.
    pub(super) fn add_incoming(&mut self, id: OpId, value: Value) {
        self.add(Candidate {
            id,
            value,
            origin: CandidateOrigin::Incoming,
        });
    }

    /// Add a candidate, updating the winner if its [`OpId`] is greater than the
    /// current winner’s.
    fn add(&mut self, candidate: Candidate) {
        match &mut self.0 {
            Summary::Empty => {
                self.0 = Summary::Present(PresentCandidates {
                    winner: candidate,
                    other_candidates: 0,
                })
            }
            Summary::Present(summary) => {
                summary.other_candidates += 1;
                if candidate.id > summary.winner.id {
                    summary.winner = candidate;
                }
            }
        }
    }
}

/// How the winning identity of a property or element changed across the batch.
#[derive(Debug)]
enum Transition {
    /// No candidate before or after.
    Absent,
    /// No candidate before, and a winner after.
    Appeared { after: PresentCandidates },
    /// A winner before, and no candidate after.
    Disappeared { before: PresentCandidates },
    /// Winners before and after with different identities.
    WinnerReplaced {
        before: PresentCandidates,
        after: PresentCandidates,
    },
    /// The same winner before and after.
    ///
    /// Note that its value or conflict state may still have changed, and text
    /// formatting may change independently.
    WinnerUnchanged {
        before: PresentCandidates,
        after: PresentCandidates,
    },
}

/// The semantic transition of one map property or sequence element, ready to
/// be encoded into the patch log.
#[derive(Debug)]
pub(super) struct ValueTransition(Transition);

impl ValueTransition {
    /// Classify the change between the before and after summaries by presence
    /// and winner identity.
    ///
    /// The before winner may have been deleted even if an existing candidate
    /// survives, so a surviving lower ID is a replacement.
    pub(super) fn new(before: CandidateSummary, after: CandidateSummary) -> Self {
        let transition = match (before.0, after.0) {
            (Summary::Empty, Summary::Empty) => Transition::Absent,
            (Summary::Empty, Summary::Present(after)) => Transition::Appeared { after },
            (Summary::Present(before), Summary::Empty) => Transition::Disappeared { before },
            (Summary::Present(before), Summary::Present(after)) => {
                if before.winner.id == after.winner.id {
                    Transition::WinnerUnchanged { before, after }
                } else {
                    Transition::WinnerReplaced { before, after }
                }
            }
        };
        Self(transition)
    }

    /// Encode the transition of map property `key` into the patch log.
    pub(super) fn emit_map(self, obj: ObjId, key: &str, log: &mut PatchLog) {
        match self.0 {
            Transition::Absent => {}
            Transition::Appeared { after } | Transition::WinnerReplaced { after, .. } => {
                put_map(obj, key, after, log);
            }
            Transition::Disappeared { .. } => log.delete_map(obj, key),
            Transition::WinnerUnchanged { before, after } => {
                match WinnerUnchangedPatch::new(&before, &after) {
                    WinnerUnchangedPatch::Unchanged => {}
                    WinnerUnchangedPatch::Put => put_map(obj, key, after, log),
                    WinnerUnchangedPatch::Increment {
                        delta,
                        conflict_appeared,
                    } => {
                        log.increment_map(obj, key, delta, after.winner.id);
                        if conflict_appeared {
                            log.flag_conflict(obj, &Prop::from(key));
                        }
                    }
                    WinnerUnchangedPatch::ConflictAppeared => {
                        log.flag_conflict(obj, &Prop::from(key))
                    }
                }
            }
        }
    }

    /// Encode the transition of the element at `index` into the patch log.
    ///
    /// `marks` is the parent's read-only formatting context at this element;
    /// it is only consulted for text.
    pub(super) fn emit_sequence(
        self,
        obj: ObjId,
        index: usize,
        seq_type: SequenceType,
        encoding: TextEncoding,
        marks: &RichTextDiff<'_>,
        log: &mut PatchLog,
    ) {
        let is_text = matches!(seq_type, SequenceType::Text);
        match self.0 {
            Transition::Absent => {}
            Transition::Appeared { after } => {
                let conflict = after.conflicted();
                let expose = expose(&after.winner);
                let winner = after.winner;
                if is_text && winner.value.is_scalar() {
                    // FIXME: This preserves an existing bug: newly visible text
                    // needs complete after-marks, not a delta. Unchanged marks
                    // (for example, bold across a concurrent delete/put) are lost.
                    log.splice(obj, index, winner.value.as_str(), marks.current().export());
                } else {
                    log.insert_and_maybe_expose(
                        obj,
                        index,
                        winner.value,
                        winner.id,
                        conflict,
                        expose,
                    );
                }
            }
            Transition::Disappeared { before } => {
                log.delete_seq(obj, index, before.winner.value.width(seq_type, encoding));
            }
            Transition::WinnerReplaced { before, after } => {
                replace(obj, index, &before, after, seq_type, encoding, marks, log);
            }
            Transition::WinnerUnchanged { before, after } => {
                let patch = WinnerUnchangedPatch::new(&before, &after);
                // A text element can still acquire mark changes, independently
                // of counter or conflict changes. A replacement carries its
                // complete format itself instead.
                if is_text && patch != WinnerUnchangedPatch::Put {
                    emit_text_marks(obj, index, &after.winner.value, encoding, marks, log);
                }
                match patch {
                    WinnerUnchangedPatch::Unchanged => {}
                    // A replacement inserts fresh text, even for an unchanged
                    // winner whose conflict clears.
                    WinnerUnchangedPatch::Put => {
                        replace(obj, index, &before, after, seq_type, encoding, marks, log);
                    }
                    WinnerUnchangedPatch::Increment {
                        delta,
                        conflict_appeared,
                    } => {
                        // Counters render as an unchanged replacement
                        // character in text.
                        if !is_text {
                            log.increment_seq(obj, index, delta, after.winner.id);
                        }
                        if conflict_appeared {
                            log.flag_conflict(obj, &Prop::from(index));
                        }
                    }
                    WinnerUnchangedPatch::ConflictAppeared => {
                        log.flag_conflict(obj, &Prop::from(index))
                    }
                }
            }
        }
    }
}

/// Whether a `Put` of this candidate must request full after-state exposure:
/// it is an object that existed in the document before the batch, so a
/// consumer may recreate its container and needs its complete contents. The
/// object may also receive ordinary child patches in the same batch; the patch
/// log's existing exposure coordination prevents applying both to one subtree.
fn expose(candidate: &Candidate) -> bool {
    matches!(candidate.origin, CandidateOrigin::Existing) && candidate.value.is_object()
}

/// Put the after winner at `key` with its final conflict state.
fn put_map(obj: ObjId, key: &str, after: PresentCandidates, log: &mut PatchLog) {
    let conflict = after.conflicted();
    let expose = expose(&after.winner);
    let winner = after.winner;
    log.put_map(obj, key, winner.value, winner.id, conflict, expose);
}

/// Replace the actual before-value at `index` with the after winner, using the
/// before-value's full encoded width. For text, carries the complete after
/// format because the replacement inserts fresh text.
#[allow(clippy::too_many_arguments)]
fn replace(
    obj: ObjId,
    index: usize,
    before: &PresentCandidates,
    after: PresentCandidates,
    seq_type: SequenceType,
    encoding: TextEncoding,
    marks: &RichTextDiff<'_>,
    log: &mut PatchLog,
) {
    let conflict = after.conflicted();
    let expose = expose(&after.winner);
    let winner = after.winner;
    log.replace_seq(
        obj,
        index,
        &before.winner.value,
        winner.value,
        winner.id,
        conflict,
        expose,
        seq_type,
        encoding,
        marks.after.current().cloned(),
    );
}

/// Log the mark delta over the rendered width of `value`, if any.
fn emit_text_marks(
    obj: ObjId,
    index: usize,
    value: &Value,
    encoding: TextEncoding,
    marks: &RichTextDiff<'_>,
    log: &mut PatchLog,
) {
    if let Some(delta) = marks.current().export() {
        log.mark(
            obj,
            index,
            value.width(SequenceType::Text, encoding),
            &delta,
        );
    }
}

/// The existing patch instructions that express a winner's value and conflict
/// changes.
///
/// An unchanged winner is semantically unchanged in identity, but the patch
/// vocabulary cannot clear a conflict with an increment, so a clearing conflict
/// is encoded as a `Put` of the final value even when the value is a counter
/// with a nonzero delta.
#[derive(Debug, PartialEq, Eq)]
enum WinnerUnchangedPatch {
    /// No value or conflict patch is required.
    Unchanged,
    /// Put the final after-value with its conflict state; this clears a
    /// conflict and does not also emit a counter delta.
    Put,
    /// The same counter has a nonzero delta and no conflict cleared. Also flag
    /// the conflict if one appeared.
    Increment { delta: i64, conflict_appeared: bool },
    /// Only a conflict appeared.
    ConflictAppeared,
}

impl WinnerUnchangedPatch {
    /// Decide how to encode an unchanged winner from its concrete endpoints.
    fn new(before: &PresentCandidates, after: &PresentCandidates) -> WinnerUnchangedPatch {
        if before.conflicted() && !after.conflicted() {
            return WinnerUnchangedPatch::Put;
        }
        let conflict_appeared = !before.conflicted() && after.conflicted();
        if let (
            Value::Scalar(ScalarValue::Counter(old)),
            Value::Scalar(ScalarValue::Counter(new)),
        ) = (&before.winner.value, &after.winner.value)
        {
            let delta = new.current - old.current;
            if delta != 0 {
                return WinnerUnchangedPatch::Increment {
                    delta,
                    conflict_appeared,
                };
            }
        }
        if conflict_appeared {
            WinnerUnchangedPatch::ConflictAppeared
        } else {
            WinnerUnchangedPatch::Unchanged
        }
    }
}
