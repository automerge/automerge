use automerge_protocol as amp;
use serde::Serialize;
use smol_str::SmolStr;

use super::Cursor;

/// A primitive value, leaf nodes of the document tree.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub enum Primitive {
    Bytes(Vec<u8>),
    Str(SmolStr),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Cursor(Cursor),
    Null,
}

impl Primitive {
    /// Return whether the [`Primitive`] is bytes.
    pub fn is_bytes(&self) -> bool {
        matches!(self, Self::Bytes(_))
    }

    /// Extract the `&[u8]` in this [`Primitive`] if it represents bytes.
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a string.
    pub fn is_str(&self) -> bool {
        matches!(self, Self::Str(_))
    }

    /// Extract the [`&str`] in this [`Primitive`] if it represents a string.
    pub fn str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is an int.
    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    /// Extract the [`i64`] in this [`Primitive`] if it represents an int.
    pub fn int(&self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a uint.
    pub fn is_uint(&self) -> bool {
        matches!(self, Self::Uint(_))
    }

    /// Extract the [`u64`] in this [`Primitive`] if it represents a uint.
    pub fn uint(&self) -> Option<u64> {
        match self {
            Self::Uint(u) => Some(*u),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a float.
    pub fn is_f64(&self) -> bool {
        matches!(self, Self::F64(_))
    }

    /// Extract the [`f64`] in this [`Primitive`] if it represents a float.
    pub fn f64(&self) -> Option<f64> {
        match self {
            Self::F64(f) => Some(*f),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a counter.
    pub fn is_counter(&self) -> bool {
        matches!(self, Self::Counter(_))
    }

    /// Extract the [`i64`] in this [`Primitive`] if it represents a counter.
    pub fn counter(&self) -> Option<i64> {
        match self {
            Self::Counter(c) => Some(*c),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a timestamp.
    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp(_))
    }

    /// Extract the [`i64`] in this [`Primitive`] if it represents a timestamp.
    pub fn timestamp(&self) -> Option<i64> {
        match self {
            Self::Timestamp(c) => Some(*c),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a boolean.
    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean(_))
    }

    /// Extract the [`bool`] in this [`Primitive`] if it represents a boolean.
    pub fn boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is a cursor.
    pub fn is_cursor(&self) -> bool {
        matches!(self, Self::Cursor(_))
    }

    /// Extract the [`Cursor`] in this [`Primitive`] if it represents a cursor.
    pub fn cursor(&self) -> Option<&Cursor> {
        match self {
            Self::Cursor(c) => Some(c),
            _ => None,
        }
    }

    /// Return whether the [`Primitive`] is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl From<&amp::CursorDiff> for Primitive {
    fn from(diff: &amp::CursorDiff) -> Self {
        Primitive::Cursor(Cursor {
            index: diff.index,
            object: diff.object_id.clone(),
            elem_opid: diff.elem_id.clone(),
        })
    }
}

impl From<&Primitive> for amp::ScalarValue {
    fn from(p: &Primitive) -> Self {
        match p {
            Primitive::Bytes(b) => amp::ScalarValue::Bytes(b.clone()),
            Primitive::Str(s) => amp::ScalarValue::Str(s.clone()),
            Primitive::Int(i) => amp::ScalarValue::Int(*i),
            Primitive::Uint(u) => amp::ScalarValue::Uint(*u),
            Primitive::F64(f) => amp::ScalarValue::F64(*f),
            Primitive::Counter(i) => amp::ScalarValue::Counter(*i),
            Primitive::Timestamp(i) => amp::ScalarValue::Timestamp(*i),
            Primitive::Boolean(b) => amp::ScalarValue::Boolean(*b),
            Primitive::Null => amp::ScalarValue::Null,
            Primitive::Cursor(c) => amp::ScalarValue::Cursor(c.elem_opid.clone()),
        }
    }
}
