//! Public-API regressions for batch patch replay, grouped by observable behavior.
//! Each scenario keeps its replica history explicit; support only handles setup,
//! delivery schedules and replay checks.

#[path = "batch_transitions/counters.rs"]
mod counters;
#[path = "batch_transitions/objects.rs"]
mod objects;
#[path = "batch_transitions/support.rs"]
mod support;
#[path = "batch_transitions/text.rs"]
mod text;
#[path = "batch_transitions/winners.rs"]
mod winners;
