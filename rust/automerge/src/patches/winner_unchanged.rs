//! Encode a winner that is *unchanged* across a transition into patch-log
//! events.
//!
//! A map property or sequence element has an unchanged winner when the same
//! op wins on both sides of a transition (`Transition::WinnerUnchanged` in
//! the batch walk). Only the winner's identity is fixed: its visible counter
//! total and its conflict state may each have moved, and they may move
//! together. Callers describe what they observed as [`Facts`] and call
//! [`Facts::emit_map`] or [`Facts::emit_sequence`]; this module decides
//! which events the patch vocabulary needs and writes them. (The private
//! `Encoding::Unchanged` is the narrower case where nothing observable moved
//! at all.)
//!
//! Every path that materialises a transition routes unchanged winners here:
//! the batch walk (`op_set2::change::batch::transition`), `diff()`,
//! isolate/integrate and write-frontier changes (`iter::map_range`,
//! `iter::list_range`). Keeping the decision in one place is what stops the
//! encodings drifting apart.

use crate::hydrate::Value;
use crate::iter::RichTextDiff;
use crate::types::{ObjId, OpId, SequenceType};
use crate::TextEncoding;

use super::Events;

/// What a caller observed about an unchanged winner at the two endpoints of a
/// transition.
///
/// Only the facts the encoding depends on: whether the register (property
/// or element) held more than one visible candidate at each endpoint, and
/// how the winner's visible counter total moved. The values themselves are
/// supplied at emission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Facts {
    /// Whether the register held more than one visible candidate at each
    /// endpoint.
    pub(crate) conflict: Conflict,
    /// Movement of the winner's visible counter total.
    pub(crate) counter_delta: CounterDelta,
}

/// Whether the register was conflicted before and after the transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Conflict {
    /// There was no conflict before, nor after.
    None,
    /// There was a conflict before, but not after.
    Before,
    /// There was a conflict after, but not before.
    After,
    /// There was a conflict before and after.
    Both,
}

impl Conflict {
    /// Construct a new [`Conflict`] given a `before` and `after` side.
    pub(crate) fn new(before: bool, after: bool) -> Self {
        match (before, after) {
            (true, true) => Self::Both,
            (true, false) => Self::Before,
            (false, true) => Self::After,
            (false, false) => Self::None,
        }
    }

    /// Record the after-side conflict state, replacing what was observed before.
    pub(crate) fn with_after(self, after: bool) -> Self {
        Self::new(matches!(self, Self::Before | Self::Both), after)
    }

    /// Returns `true` if there is an after conflict.
    pub(crate) fn after_conflicted(&self) -> bool {
        matches!(self, Self::After | Self::Both)
    }
}

/// How an unchanged counter winner's visible total moved across a transition.
///
/// [`CounterDelta::empty`] and a zero delta are the same value: neither
/// produces an increment. Callers with a non-counter winner use `empty()`;
/// callers that always compute a difference can pass it to `new()` and let
/// zero collapse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CounterDelta(Option<i64>);

impl CounterDelta {
    pub(crate) fn new(value: i64) -> Self {
        Self((value != 0).then_some(value))
    }

    #[inline]
    pub(crate) const fn empty() -> Self {
        Self(None)
    }

    fn get(self) -> Option<i64> {
        self.0
    }
}

impl Facts {
    /// Encode the unchanged winner at `key` into `log`.
    ///
    /// `value` is the winner's after-value with its visible counter total
    /// folded in; it is only written when the encoding is a put.
    pub(crate) fn emit_map(
        self,
        obj: ObjId,
        key: &str,
        id: OpId,
        value: Value,
        log: &mut Events<'_>,
    ) {
        Encoding::new(self).emit_map(obj, key, id, value, log);
    }

    /// Encode the unchanged winner at `index` into `log`.
    ///
    /// `elem` carries the after-value and, for text, the before-value and
    /// formatting context; see [`Seq`].
    pub(crate) fn emit_sequence(
        self,
        obj: ObjId,
        index: usize,
        id: OpId,
        elem: Seq<'_>,
        log: &mut Events<'_>,
    ) {
        Encoding::new(self).emit_sequence(obj, index, id, elem, log);
    }
}

/// The patch events that express an unchanged winner's transition.
///
/// The vocabulary has no way to clear a conflict flag except by re-putting
/// the value, so a clearing conflict is always `Put` — even for a counter
/// with a nonzero delta, whose final total the put then carries.
#[derive(Debug, PartialEq, Eq)]
enum Encoding {
    /// Nothing observable changed.
    Unchanged,
    /// The conflict cleared: re-put the after-value.
    Put,
    /// The counter total moved and no conflict cleared.
    Increment { delta: i64, conflict_appeared: bool },
    /// Only a conflict appeared.
    ConflictAppeared,
}

impl Encoding {
    /// Resolve facts to an encoding. First match wins:
    ///
    /// | before | after | delta | encoding                                 |
    /// |--------|-------|-------|------------------------------------------|
    /// | yes    | no    | any   | `Put`                                    |
    /// | any    | any   | ≠ 0   | `Increment { delta, conflict_appeared }` |
    /// | no     | yes   | 0     | `ConflictAppeared`                       |
    /// |        |       |       | `Unchanged`                              |
    fn new(
        Facts {
            conflict,
            counter_delta,
        }: Facts,
    ) -> Self {
        match (conflict, counter_delta.get()) {
            (Conflict::None, None) => Encoding::Unchanged,
            (Conflict::None, Some(delta)) => Encoding::Increment {
                delta,
                conflict_appeared: false,
            },
            (Conflict::Before, _) => Encoding::Put,
            (Conflict::After, None) => Encoding::ConflictAppeared,
            (Conflict::After, Some(delta)) => Encoding::Increment {
                delta,
                conflict_appeared: true,
            },
            (Conflict::Both, None) => Encoding::Unchanged,
            (Conflict::Both, Some(delta)) => Encoding::Increment {
                delta,
                conflict_appeared: false,
            },
        }
    }

    fn emit_map(self, obj: ObjId, key: &str, id: OpId, value: Value, log: &mut Events<'_>) {
        match self {
            Self::Unchanged => {}
            Self::Put => {
                // The conflict cleared, and an unchanged winner already
                // existed, so expose: an object winner's unchanged children
                // emit nothing of their own. `put_map` ignores `expose` for
                // scalars.
                log.put_map(obj, key, value, id, false, true);
            }
            Self::Increment {
                delta,
                conflict_appeared,
            } => {
                log.increment_map(obj, key, delta, id);
                if conflict_appeared {
                    log.flag_conflict_map(obj, key);
                }
            }
            Self::ConflictAppeared => log.flag_conflict_map(obj, key),
        }
    }

    fn emit_sequence(
        self,
        obj: ObjId,
        index: usize,
        id: OpId,
        elem: Seq<'_>,
        log: &mut Events<'_>,
    ) {
        match (self, elem) {
            (Self::Unchanged, Seq::List { .. }) => {}
            (
                Self::Unchanged,
                Seq::Text {
                    after,
                    encoding,
                    marks,
                    ..
                },
            ) => {
                emit_text_marks(obj, index, &after, encoding, marks, log);
            }

            (Self::Put, Seq::List { after }) => {
                // As for the map put: the conflict cleared; expose so an
                // object winner re-emits its children.
                log.put_seq(obj, index, after, id, false, true);
            }
            (
                Self::Put,
                Seq::Text {
                    before,
                    after,
                    encoding,
                    marks,
                },
            ) => {
                // A replacement inserts fresh text carrying its full format;
                // no separate mark delta.
                log.replace_seq(
                    obj,
                    index,
                    before,
                    after,
                    id,
                    false,
                    true,
                    SequenceType::Text,
                    encoding,
                    marks.after.current().cloned(),
                );
            }

            (
                Self::Increment {
                    delta,
                    conflict_appeared,
                },
                Seq::List { .. },
            ) => {
                log.increment_seq(obj, index, delta, id);
                if conflict_appeared {
                    log.flag_conflict_seq(obj, index);
                }
            }
            (
                Self::Increment {
                    conflict_appeared, ..
                },
                Seq::Text {
                    after,
                    encoding,
                    marks,
                    ..
                },
            ) => {
                // Counters render as an unchanged replacement character in text:
                // no delta event, but marks may still have moved.
                emit_text_marks(obj, index, &after, encoding, marks, log);
                if conflict_appeared {
                    log.flag_conflict_seq(obj, index);
                }
            }

            (Self::ConflictAppeared, Seq::List { .. }) => log.flag_conflict_seq(obj, index),
            (
                Self::ConflictAppeared,
                Seq::Text {
                    after,
                    encoding,
                    marks,
                    ..
                },
            ) => {
                emit_text_marks(obj, index, &after, encoding, marks, log);
                log.flag_conflict_seq(obj, index);
            }
        }
    }
}

/// Log the mark delta over the retained rendered width of `value`, if any.
fn emit_text_marks(
    obj: ObjId,
    index: usize,
    value: &Value,
    encoding: TextEncoding,
    marks: &RichTextDiff<'_>,
    log: &mut Events<'_>,
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

/// The payload of an unchanged sequence winner, by container.
///
/// The variant also fixes the event shape: a list element is re-put in
/// place, a text element is replaced by deleting its before-width and
/// splicing fresh text with its complete format.
pub(crate) enum Seq<'a> {
    /// A list element. `after` is the winner's after-value with its visible
    /// counter total folded in.
    List { after: Value },
    /// A text element. `before` is measured for the deletion width; `marks`
    /// supplies the retained mark delta, or the complete after-format on a
    /// replacement.
    Text {
        before: &'a Value,
        after: Value,
        encoding: TextEncoding,
        marks: &'a RichTextDiff<'a>,
    },
}

#[cfg(test)]
mod tests;
