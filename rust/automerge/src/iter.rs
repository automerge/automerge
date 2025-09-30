mod doc;
mod keys;
mod list_range;
mod map_range;
mod spans;
mod values;

pub(crate) mod tools;

pub use doc::{DocItem, DocIter, DocObjItem};
pub use keys::Keys;
pub use list_range::{ListRange, ListRangeItem};
pub use map_range::{MapRange, MapRangeItem};
pub use spans::{Span, Spans};
pub use values::Values;

pub(crate) use doc::DiffIter;
pub(crate) use list_range::{ListDiff, ListDiffItem};
pub(crate) use map_range::{MapDiff, MapDiffItem};
pub(crate) use tools::Diff;

pub(crate) use spans::{RichTextDiff, SpanDiff, SpanInternal, SpansDiff, SpansInternal};

#[cfg(test)]
pub(crate) use keys::KeyOpIter;
