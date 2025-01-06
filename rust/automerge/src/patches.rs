mod patch;
mod patch_builder;
mod patch_log;
pub use patch::{Patch, PatchAction};
pub(crate) use patch_builder::PatchBuilder;
pub use patch_log::PatchLog;

use crate::{
    types::{ListEncoding, TextEncoding},
    ObjType,
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TextRepresentation {
    Array,
    String(TextEncoding),
}

impl From<ListEncoding> for TextRepresentation {
    fn from(encoding: ListEncoding) -> Self {
        match encoding {
            ListEncoding::Text(encoding) => Self::String(encoding),
            ListEncoding::List => Self::Array,
        }
    }
}

impl From<TextEncoding> for TextRepresentation {
    fn from(value: TextEncoding) -> Self {
        Self::String(value)
    }
}

impl TextRepresentation {
    pub(crate) fn encoding(&self, typ: ObjType) -> ListEncoding {
        match (self, typ) {
            (&Self::String(encoding), ObjType::Text) => ListEncoding::Text(encoding),
            _ => ListEncoding::List,
        }
    }

    pub fn is_array(&self) -> bool {
        matches!(self, TextRepresentation::Array)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, TextRepresentation::String(_))
    }
}
