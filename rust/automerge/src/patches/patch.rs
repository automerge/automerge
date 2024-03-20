use crate::{
    marks::{Mark, MarkSet},
    ObjId, Prop, Value,
};
use core::fmt::Debug;
use std::collections::HashMap;
use std::fmt;

use crate::op_tree::OpSetData;
use crate::sequence_tree::SequenceTree;
use crate::text_value::TextValue;
use crate::types::{ActorId, OpId};

/// A change to the current state of the document
///
/// [`Patch`]es are obtained from a [`PatchLog`](super::PatchLog) which has been passed to any of
/// the various methods which mutate a document and add incremental changes to the
/// [`PatchLog`](super::PatchLog)

pub type Patch = PatchWithAttribution<'static, NoAttribution>;

#[allow(missing_debug_implementations)]
#[derive(Clone, PartialEq)]
pub struct NoAttribution {}

#[derive(Clone, PartialEq)]
pub struct PatchWithAttribution<'a, T: PartialEq> {
    /// The object this patch modifies
    pub obj: ObjId,
    /// The path to the property in the parent object where this object lives
    pub path: Vec<(ObjId, Prop)>,
    /// user attribution to the patch
    pub attribute: Option<&'a T>,
    /// The change this patch represents
    pub action: PatchAction,
}

// FIXME - I can remove all these partial eq and use reference equality
// *IF* i changed the calling format to diff_with_attr to take a vec[value] instead of a hashmap<actor,value>

/*
impl<'a, T: PartialEq> PartialEq for PatchWithAttribution<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        let attr_eq = match (self.attribute, other.attribute) {
            (None, None) => true,
            (Some(a), Some(b)) => std::ptr::eq(a, b),
            _ => false,
        };
        attr_eq && self.obj == other.obj && self.path == other.path && self.action == other.action
    }
}
*/

#[derive(Default)]
pub(crate) struct AttributionLookup<'a, T: PartialEq> {
    cache: Vec<Option<&'a T>>,
}

impl<'a, T: PartialEq> AttributionLookup<'a, T> {
    pub(crate) fn new(attr: &'a HashMap<ActorId, T>, osd: &OpSetData) -> Self {
        Self {
            cache: osd.actors.cache.iter().map(|a| attr.get(a)).collect(),
        }
    }

    pub(crate) fn empty() -> Self {
        Self { cache: vec![] }
    }

    pub(crate) fn get(&self, id: Option<OpId>) -> Option<&'a T> {
        self.cache.get(id?.actor()).cloned().flatten()
    }
}

impl fmt::Debug for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Patch")
            .field("obj", &self.obj)
            .field("path", &self.path)
            .field("action", &self.action)
            .finish()
    }
}

impl<'a, T: PartialEq> fmt::Debug for PatchWithAttribution<'a, T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PatchWithAttribution")
            .field("obj", &self.obj)
            .field("path", &self.path)
            .field("attribute", &self.attribute)
            .field("action", &self.action)
            .finish()
    }
}

impl Patch {
    pub fn new(obj: ObjId, path: Vec<(ObjId, Prop)>, action: PatchAction) -> Self {
        Patch {
            obj,
            path,
            action,
            attribute: None,
        }
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
        /// All marks currently active for these values
        marks: Option<MarkSet>,
    },
    /// Some text was spliced into a text object
    SpliceText {
        index: usize,
        /// The text that was inserted
        value: TextValue,
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
    DeleteSeq {
        index: usize,
        length: usize,
        value: String,
    },
    /// Some marks within a text object were added or removed
    Mark { marks: Vec<Mark<'static>> },
}

impl fmt::Display for PatchAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
