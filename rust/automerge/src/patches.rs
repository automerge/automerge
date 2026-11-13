mod patch;
mod patch_builder;
mod patch_log;
pub(crate) mod winner_unchanged;

pub(crate) use patch_builder::PatchBuilder;
pub(crate) use patch_log::{Event, Events};

pub use patch::{Patch, PatchAction};
pub use patch_log::PatchLog;
