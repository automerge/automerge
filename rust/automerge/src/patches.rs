/// Patch-effect model: materializes a document and replays patches over
/// it, so two patch streams can be checked to land the same state. Only
/// the assertion paths use it, so it is compiled out of release builds
/// (matching the `cfg` on every call site).
#[cfg(any(test, debug_assertions, feature = "patchless_assertions"))]
pub(crate) mod effect;
mod patch;
mod patch_accumulator;
mod patch_builder;
pub use patch::{Patch, PatchAction};
pub(crate) use patch_accumulator::Event;
pub(crate) use patch_accumulator::PatchAccumulator;
pub(crate) use patch_builder::PatchBuilder;
