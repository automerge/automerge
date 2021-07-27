mod list;
mod map;
mod root;
mod sorted_map;
mod table;
mod text;

pub use list::ListRef;
pub use map::MapRef;
pub use root::RootRef;
pub use sorted_map::SortedMapRef;
pub use table::TableRef;
pub use text::TextRef;

use crate::{
    state_tree::{StateTreeComposite, StateTreeValue},
    Primitive, Value,
};

/// A ValueRef represents a way to interact with the frontend's state lazily rather than creating
/// a full [`Value`](crate::Value) from the internal state.
///
/// This aims to make operations cheaper by only extracting the needed data.
///
/// A `ValueRef` can be obtained from the [`value_ref`](crate::Frontend::value_ref) method on a
/// [`Frontend`](crate::Frontend).

#[derive(Clone, Debug)]
pub enum ValueRef<'a> {
    Primitive(&'a Primitive),
    Map(MapRef<'a>),
    SortedMap(SortedMapRef<'a>),
    Table(TableRef<'a>),
    List(ListRef<'a>),
    Text(TextRef<'a>),
}

impl<'a> ValueRef<'a> {
    pub(crate) fn new(stv: &'a StateTreeValue) -> Self {
        match stv {
            StateTreeValue::Leaf(p) => Self::Primitive(p),
            StateTreeValue::Composite(StateTreeComposite::Map(m)) => Self::Map(MapRef::new(m)),
            StateTreeValue::Composite(StateTreeComposite::SortedMap(m)) => {
                Self::SortedMap(SortedMapRef::new(m))
            }
            StateTreeValue::Composite(StateTreeComposite::Table(t)) => {
                Self::Table(TableRef::new(t))
            }
            StateTreeValue::Composite(StateTreeComposite::List(l)) => Self::List(ListRef::new(l)),
            StateTreeValue::Composite(StateTreeComposite::Text(t)) => Self::Text(TextRef::new(t)),
        }
    }

    pub fn map(&self) -> Option<&MapRef<'a>> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn sorted_map(&self) -> Option<&SortedMapRef<'a>> {
        match self {
            Self::SortedMap(m) => Some(m),
            _ => None,
        }
    }

    pub fn table(&self) -> Option<&TableRef<'a>> {
        match self {
            Self::Table(t) => Some(t),
            _ => None,
        }
    }

    pub fn list(&self) -> Option<&ListRef<'a>> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn text(&self) -> Option<&TextRef<'a>> {
        match self {
            Self::Text(t) => Some(t),
            _ => None,
        }
    }

    pub fn primitive(&self) -> Option<&Primitive> {
        match self {
            Self::Primitive(p) => Some(p),
            _ => None,
        }
    }

    pub fn value(&self) -> Value {
        match self {
            ValueRef::Primitive(p) => Value::from((*p).clone()),
            ValueRef::Map(m) => m.value(),
            ValueRef::SortedMap(m) => m.value(),
            ValueRef::Table(t) => t.value(),
            ValueRef::List(l) => l.value(),
            ValueRef::Text(t) => t.value(),
        }
    }
}
