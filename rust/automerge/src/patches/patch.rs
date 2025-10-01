use crate::{
    marks::{Mark, MarkSet},
    text_value::ConcreteTextValue,
    ObjId, Prop, Value,
};
use core::fmt::Debug;
use std::fmt;

use crate::sequence_tree::SequenceTree;

/// A change to the current state of the document
///
/// [`Patch`]es are obtained from a [`PatchLog`](super::PatchLog) which has been passed to any of
/// the various methods which mutate a document and add incremental changes to the
/// [`PatchLog`](super::PatchLog)
#[derive(Debug, Clone, PartialEq)]
pub struct Patch {
    /// The object this patch modifies
    pub obj: ObjId,
    /// The path to the property in the parent object where this object lives
    pub path: Vec<(ObjId, Prop)>,
    /// The change this patch represents
    pub action: PatchAction,
}

impl Patch {
    pub(crate) fn has(&self, obj: &ObjId, recursive: bool) -> bool {
        &self.obj == obj && (recursive || self.path.iter().any(|(o, _)| o == obj))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchAction {
    /// A key was created or updated in a map
    PutMap {
        key: String,
        /// The value that was inserted and the object ID of the object that was inserted. Note
        /// that the Object ID is only meaningful for `Value::Obj` values
        value: (Value<'static>, ObjId),
        /// Whether there is a conflict at this key. If there is a conflict this patch represents
        /// the "winning" value of the conflict. The conflicting values can be obtained with
        /// [`crate::ReadDoc::get_all`]
        conflict: bool,
    },
    /// An index in a sequence was updated
    PutSeq {
        index: usize,
        /// The value that was set and the object ID of the object that was set. Note that the
        /// Object ID is only meaningful for `Value::Obj` values
        value: (Value<'static>, ObjId),
        /// Whether there is a conflict at this index. If there is a conflict this patch represents
        /// the "winning" value of the conflict. The conflicting values can be obtained with
        /// [`crate::ReadDoc::get_all`]
        conflict: bool,
    },
    /// One or more elements were inserted into a sequence
    Insert {
        index: usize,
        /// The values that were inserted, in order that they appear. As with [`Self::PutMap`] and
        /// [`Self::PutSeq`] the object ID is only meaningful for `Value::Obj` values
        values: SequenceTree<(Value<'static>, ObjId, bool)>,
    },
    /// Some text was spliced into a text object
    SpliceText {
        index: usize,
        /// The text that was inserted
        value: ConcreteTextValue,
        /// All marks currently active for this span of text
        marks: Option<MarkSet>,
    },
    /// A counter was incremented
    Increment {
        /// The property of the counter that was incremented within the target object
        prop: Prop,
        /// The amount incremented, may be negative
        value: i64,
    },
    /// A new conflict has appeared
    Conflict {
        /// The conflicted property
        prop: Prop,
    },
    /// A key was deleted from a map
    DeleteMap { key: String },
    /// One or more indices were removed from a sequence
    DeleteSeq { index: usize, length: usize },
    /// Some marks within a text object were added or removed
    Mark { marks: Vec<Mark> },
}

impl fmt::Display for PatchAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
