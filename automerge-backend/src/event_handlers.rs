use std::fmt::Debug;

use crate::Change;

#[derive(Clone, Copy)]
pub struct EventHandlerId(usize);

/// A sequence of event handlers.
///
/// This maintains the order of insertion so handlers will be called in a consistent order.
#[derive(Debug)]
pub struct EventHandlers<H: EventHandler>(Vec<H>);

impl<H: EventHandler> Default for EventHandlers<H> {
    fn default() -> Self {
        Self(Vec::default())
    }
}

impl<H: EventHandler> Clone for EventHandlers<H> {
    fn clone(&self) -> Self {
        EventHandlers(Vec::new())
    }
}

impl<H: EventHandler> EventHandlers<H> {
    pub(crate) fn before_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            handler.before_apply_change(change)
        }
    }

    pub(crate) fn after_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            handler.after_apply_change(change)
        }
    }

    /// Adds the event handler and returns the id of the handler.
    pub fn add_handler(&mut self, handler: H) -> EventHandlerId {
        self.0.push(handler);
        EventHandlerId(self.0.len() - 1)
    }

    /// Remove the handler with the given id, returning whether it removed a handler or not.
    pub fn remove_handler(&mut self, id: EventHandlerId) -> bool {
        if id.0 < self.0.len() {
            self.0.remove(id.0);
            true
        } else {
            false
        }
    }
}

mod private {
    pub trait Sealed {}

    impl Sealed for super::UnsendableEventHandler {}
    impl Sealed for super::SendableEventHandler {}
}

pub trait EventHandler: private::Sealed {
    fn before_apply_change(&mut self, change: &Change);

    fn after_apply_change(&mut self, change: &Change);
}

/// An general event handler.
pub enum UnsendableEventHandler {
    /// An event handler that gets called before a change is applied to the history.
    BeforeApplyChange(Box<dyn FnMut(&Change)>),
    /// An event handler that gets called after a change has been applied to the history.
    AfterApplyChange(Box<dyn FnMut(&Change)>),
}

impl EventHandler for UnsendableEventHandler {
    fn before_apply_change(&mut self, change: &Change) {
        if let Self::BeforeApplyChange(f) = self {
            f(change)
        }
    }

    fn after_apply_change(&mut self, change: &Change) {
        if let Self::AfterApplyChange(f) = self {
            f(change)
        }
    }
}

impl Debug for UnsendableEventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::BeforeApplyChange(_) => write!(f, "BeforeApplyChange"),
            Self::AfterApplyChange(_) => write!(f, "AfterApplyChange"),
        }
    }
}

/// An general event handler for sendable functions.
pub enum SendableEventHandler {
    /// An event handler that gets called before a change is applied to the history.
    BeforeApplyChange(Box<dyn FnMut(&Change) + Send>),
    /// An event handler that gets called after a change has been applied to the history.
    AfterApplyChange(Box<dyn FnMut(&Change) + Send>),
}

impl EventHandler for SendableEventHandler {
    fn before_apply_change(&mut self, change: &Change) {
        if let Self::BeforeApplyChange(f) = self {
            f(change)
        }
    }

    fn after_apply_change(&mut self, change: &Change) {
        if let Self::AfterApplyChange(f) = self {
            f(change)
        }
    }
}

impl Debug for SendableEventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::BeforeApplyChange(_) => write!(f, "BeforeApplyChange"),
            Self::AfterApplyChange(_) => write!(f, "AfterApplyChange"),
        }
    }
}
