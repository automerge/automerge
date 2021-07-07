mod list;
mod map;
mod root;
mod table;
mod text;

pub use list::ListProxy;
pub use map::MapProxy;
pub use root::RootProxy;
pub use table::TableProxy;
pub use text::TextProxy;

use crate::{
    state_tree::{StateTreeComposite, StateTreeValue},
    Primitive, Value,
};

/// A ValueProxy represents a way to interact with the frontend's state lazily rather than creating
/// a full [`Value`](crate::Value) from the internal state.
///
/// This aims to make operations cheaper by only extracting the needed data.
///
/// A `ValueProxy` can be obtained from the [`proxy`](crate::Frontend::proxy) method on a
/// [`Frontend`](crate::Frontend).

#[derive(Clone, Debug)]
pub enum ValueProxy<'a> {
    Primitive(&'a Primitive),
    Map(MapProxy<'a>),
    Table(TableProxy<'a>),
    List(ListProxy<'a>),
    Text(TextProxy<'a>),
}

impl<'a> ValueProxy<'a> {
    pub(crate) fn new(stv: &'a StateTreeValue) -> Self {
        match stv {
            StateTreeValue::Leaf(p) => Self::Primitive(p),
            StateTreeValue::Composite(StateTreeComposite::Map(m)) => Self::Map(MapProxy::new(m)),
            StateTreeValue::Composite(StateTreeComposite::Table(t)) => {
                Self::Table(TableProxy::new(t))
            }
            StateTreeValue::Composite(StateTreeComposite::List(l)) => Self::List(ListProxy::new(l)),
            StateTreeValue::Composite(StateTreeComposite::Text(t)) => Self::Text(TextProxy::new(t)),
        }
    }

    pub fn map(&self) -> Option<&MapProxy<'a>> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn table(&self) -> Option<&TableProxy<'a>> {
        match self {
            Self::Table(t) => Some(t),
            _ => None,
        }
    }

    pub fn list(&self) -> Option<&ListProxy<'a>> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn text(&self) -> Option<&TextProxy<'a>> {
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
            ValueProxy::Primitive(p) => Value::from((*p).clone()),
            ValueProxy::Map(m) => m.value(),
            ValueProxy::Table(t) => t.value(),
            ValueProxy::List(l) => l.value(),
            ValueProxy::Text(t) => t.value(),
        }
    }
}
