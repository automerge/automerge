mod patch;
mod patch_builder;
mod patch_log;
pub(crate) use patch::AttributionLookup;
pub use patch::{NoAttribution, Patch, PatchAction, PatchWithAttribution};
pub(crate) use patch_builder::PatchBuilder;
pub use patch_log::PatchLog;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TextRepresentation {
    Array,
    String,
}

impl TextRepresentation {
    pub fn is_array(&self) -> bool {
        matches!(self, TextRepresentation::Array)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, TextRepresentation::String)
    }
}

impl std::default::Default for TextRepresentation {
    fn default() -> Self {
        TextRepresentation::Array // FIXME
    }
}
