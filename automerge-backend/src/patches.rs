mod incremental_diff;
mod from_scratch_diff;
mod patch_workshop;
mod gen_value_diff;
mod edits;

pub(crate) use edits::Edits;
pub(crate) use incremental_diff::{PendingDiff, IncrementalPatch};
pub(crate) use from_scratch_diff::generate_from_scratch_diff;
pub(crate) use patch_workshop::PatchWorkshop;
