/// Patch-effect model: materializes a document and replays patches over
/// it, so two patch streams can be checked to land the same state. Only
/// the tests use it now, so the `cfg` here matches the `cfg(test)` on
/// every remaining call site; a wider gate leaves it dead in ordinary
/// debug builds and trips `clippy -D warnings`.
#[cfg(test)]
pub(crate) mod effect;
mod patch;
mod patch_accumulator;
mod patch_builder;
pub use patch::{Patch, PatchAction};
pub(crate) use patch_accumulator::Event;
pub(crate) use patch_accumulator::PatchAccumulator;
pub(crate) use patch_builder::PatchBuilder;
