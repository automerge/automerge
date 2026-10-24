/// Replays patches over a materialized document, so tests can check that two
/// patch streams land the same state.
#[cfg(test)]
pub(crate) mod effect;
mod patch;
mod patch_accumulator;
mod patch_builder;
pub use patch::{Patch, PatchAction};
pub(crate) use patch_accumulator::Event;
pub(crate) use patch_accumulator::PatchAccumulator;
pub(crate) use patch_builder::PatchBuilder;
