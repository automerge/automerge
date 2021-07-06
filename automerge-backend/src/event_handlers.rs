use std::fmt::Debug;

use crate::Change;

#[derive(Clone, Copy)]
pub struct EventHandlerId(usize);

/// A sequence of event handlers.
///
/// This maintains the order of insertion so handlers will be called in a consistent order.
#[derive(Debug, Default)]
pub struct EventHandlers(Vec<EventHandler>);

impl Clone for EventHandlers {
    fn clone(&self) -> Self {
        EventHandlers(Vec::new())
    }
}

impl EventHandlers {
    pub(crate) fn before_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            if let EventHandler::BeforeApplyChange(f) = handler {
                f.0(change);
            }
        }
    }

    pub(crate) fn after_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            if let EventHandler::AfterApplyChange(f) = handler {
                f.0(change);
            }
        }
    }

    /// Adds the event handler and returns the id of the handler.
    pub fn add_handler(&mut self, handler: EventHandler) -> EventHandlerId {
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

/// A handler for changes.
pub struct ChangeEventHandler(pub Box<dyn FnMut(&Change) + Send>);

/// An general event handler.
pub enum EventHandler {
    /// An event handler that gets called before a change is applied to the history.
    BeforeApplyChange(ChangeEventHandler),
    /// An event handler that gets called after a change has been applied to the history.
    AfterApplyChange(ChangeEventHandler),
}

impl Debug for EventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::BeforeApplyChange(_) => write!(f, "BeforeApplyChange"),
            Self::AfterApplyChange(_) => write!(f, "AfterApplyChange"),
        }
    }
}
