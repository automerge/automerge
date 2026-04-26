mod serde_impls;
mod utility_impls;

use std::num::NonZeroU64;

pub(crate) use crate::types::{ActorId, Author, ChangeHash, ObjType, ScalarValue};
pub(crate) use crate::value::DataType;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MapType {
    Map,
    Table,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SequenceType {
    List,
    Text,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct OpId(pub u64, pub ActorId);

impl OpId {
    pub fn new(seq: u64, actor: &ActorId) -> OpId {
        OpId(seq, actor.clone())
    }

    pub fn actor(&self) -> &ActorId {
        &self.1
    }

    pub fn counter(&self) -> u64 {
        self.0
    }

    pub fn increment_by(&self, by: u64) -> OpId {
        OpId(self.0 + by, self.1.clone())
    }

    /// Returns true if `other` has the same actor ID, and their counter is `delta` greater than
    /// ours.
    pub fn delta(&self, other: &Self, delta: u64) -> bool {
        self.1 == other.1 && self.0 + delta == other.0
    }
}

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectId {
    Id(OpId),
    Root,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum ElementId {
    Head,
    Id(OpId),
}

impl ElementId {
    pub fn as_opid(&self) -> Option<&OpId> {
        match self {
            ElementId::Head => None,
            ElementId::Id(opid) => Some(opid),
        }
    }

    pub fn into_key(self) -> Key {
        Key::Seq(self)
    }

    pub fn not_head(&self) -> bool {
        match self {
            ElementId::Head => false,
            ElementId::Id(_) => true,
        }
    }

    pub fn increment_by(&self, by: u64) -> Option<Self> {
        match self {
            ElementId::Head => None,
            ElementId::Id(id) => Some(ElementId::Id(id.increment_by(by))),
        }
    }
}

#[derive(Serialize, PartialEq, Eq, Debug, Hash, Clone)]
#[serde(untagged)]
pub enum Key {
    Map(SmolStr),
    Seq(ElementId),
}

impl Key {
    pub fn head() -> Key {
        Key::Seq(ElementId::Head)
    }

    pub fn is_map_key(&self) -> bool {
        match self {
            Key::Map(_) => true,
            Key::Seq(_) => false,
        }
    }

    pub fn as_element_id(&self) -> Option<ElementId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => Some(eid.clone()),
        }
    }

    pub fn to_opid(&self) -> Option<OpId> {
        match self.as_element_id()? {
            ElementId::Id(id) => Some(id),
            ElementId::Head => None,
        }
    }
    pub fn increment_by(&self, by: u64) -> Option<Self> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => eid.increment_by(by).map(Key::Seq),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SortedVec<T>(Vec<T>);

impl<T> SortedVec<T> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.0.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.0.get_mut(index)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Ord> From<Vec<T>> for SortedVec<T> {
    fn from(mut other: Vec<T>) -> Self {
        other.sort_unstable();
        Self(other)
    }
}

impl<T: Ord> FromIterator<T> for SortedVec<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: std::iter::IntoIterator<Item = T>,
    {
        let mut inner: Vec<T> = iter.into_iter().collect();
        inner.sort_unstable();
        Self(inner)
    }
}

impl<T> IntoIterator for SortedVec<T> {
    type Item = T;

    type IntoIter = <Vec<T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'de, T> serde::Deserialize<'de> for SortedVec<T>
where
    T: serde::Deserialize<'de> + Ord,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut v = Vec::deserialize(deserializer)?;
        v.sort_unstable();
        Ok(Self(v))
    }
}

pub(crate) struct OpTypeParts {
    pub(crate) action: u64,
    pub(crate) value: ScalarValue,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<smol_str::SmolStr>,
}

// Like `types::OpType` except using a String for mark names
#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    Delete,
    Increment(i64),
    Put(ScalarValue),
    MarkBegin(MarkData),
    MarkEnd(bool),
}

impl OpType {
    /// Create a new legacy OpType
    ///
    /// This is really only meant to be used to convert from a crate::Change to a
    /// crate::legacy::Change, so the arguments should all have been validated. Consequently it
    /// does not return an error and instead panics on the following conditions
    ///
    /// # Panics
    ///
    /// * If The action index is unrecognized
    /// * If the action index indicates that the value should be numeric but the value is not a
    ///   number
    pub(crate) fn from_parts(
        OpTypeParts {
            action,
            value,
            expand,
            mark_name,
        }: OpTypeParts,
    ) -> Self {
        match action {
            0 => Self::Make(ObjType::Map),
            1 => Self::Put(value),
            2 => Self::Make(ObjType::List),
            3 => Self::Delete,
            4 => Self::Make(ObjType::Text),
            5 => match value {
                ScalarValue::Int(i) => Self::Increment(i),
                ScalarValue::Uint(i) => Self::Increment(i as i64),
                _ => panic!("non numeric value for integer action"),
            },
            6 => Self::Make(ObjType::Table),
            7 => match mark_name {
                Some(name) => Self::MarkBegin(MarkData {
                    name,
                    value,
                    expand,
                }),
                None => Self::MarkEnd(expand),
            },
            other => panic!("unknown action type {}", other),
        }
    }

    pub(crate) fn action_index(&self) -> u64 {
        match self {
            Self::Make(ObjType::Map) => 0,
            Self::Put(_) => 1,
            Self::Make(ObjType::List) => 2,
            Self::Delete => 3,
            Self::Make(ObjType::Text) => 4,
            Self::Increment(_) => 5,
            Self::Make(ObjType::Table) => 6,
            Self::MarkBegin(_) | Self::MarkEnd(_) => 7,
        }
    }

    pub(crate) fn expand(&self) -> bool {
        matches!(
            self,
            Self::MarkBegin(MarkData { expand: true, .. }) | Self::MarkEnd(true)
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct MarkData {
    pub name: smol_str::SmolStr,
    pub value: ScalarValue,
    pub expand: bool,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Op {
    pub action: OpType,
    pub obj: ObjectId,
    pub key: Key,
    pub pred: SortedVec<OpId>,
    pub insert: bool,
}

impl Op {
    pub fn primitive_value(&self) -> Option<ScalarValue> {
        match &self.action {
            OpType::Put(v) => Some(v.clone()),
            OpType::MarkBegin(MarkData { value, .. }) => Some(value.clone()),
            OpType::Increment(i) => Some(ScalarValue::Int(*i)),
            _ => None,
        }
    }

    pub fn obj_type(&self) -> Option<ObjType> {
        match self.action {
            OpType::Make(o) => Some(o),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.primitive_value().as_ref().and_then(|v| v.to_i64())
    }
}

/// A change represents a group of operations performed by an actor.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Change {
    /// The operations performed in this change.
    #[serde(rename = "ops")]
    pub operations: Vec<Op>,
    /// The actor that performed this change.
    #[serde(rename = "actor")]
    pub actor_id: ActorId,
    /// The hash of this change.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hash: Option<ChangeHash>,
    /// The index of this change in the changes from this actor.
    pub seq: u64,
    /// The start operation index. Starts at 1.
    #[serde(rename = "startOp")]
    pub start_op: NonZeroU64,
    /// The time that this change was committed.
    pub time: i64,
    /// The message of this change.
    pub message: Option<String>,
    /// The dependencies of this change.
    pub deps: Vec<ChangeHash>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub extra_bytes: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub author: Option<Author>,
}

impl PartialEq for Change {
    // everything but hash (its computed and not always present)
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
            && self.actor_id == other.actor_id
            && self.seq == other.seq
            && self.start_op == other.start_op
            && self.time == other.time
            && self.message == other.message
            && self.deps == other.deps
            && self.extra_bytes == other.extra_bytes
    }
}
