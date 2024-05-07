mod patch;
mod patch_builder;
mod patch_log;
pub use patch::{Patch, PatchAction};
pub(crate) use patch_builder::PatchBuilder;
pub use patch_log::PatchLog;

use crate::{types::ListEncoding, ObjType};

#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub enum TextRepresentation {
    Array,
    #[default]
    String,
}

impl TextRepresentation {
    pub(crate) fn encoding(&self, typ: ObjType) -> ListEncoding {
        match (self, typ) {
            (&Self::String, ObjType::Text) => ListEncoding::Text,
            _ => ListEncoding::List,
        }
    }

    pub fn is_array(&self) -> bool {
        matches!(self, TextRepresentation::Array)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, TextRepresentation::String)
    }
}
