use std::fmt::Debug;

use crate::Change;

#[derive(Debug, Default, PartialEq)]
pub struct EventHandlers(Vec<EventHandler>);

impl Clone for EventHandlers {
    fn clone(&self) -> Self {
        EventHandlers(Vec::new())
    }
}

impl EventHandlers {
    pub fn before_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            if let EventHandler::BeforeApplyChange(f) = handler {
                f(change)
            }
        }
    }

    pub fn after_apply_change(&mut self, change: &Change) {
        for handler in &mut self.0 {
            if let EventHandler::AfterApplyChange(f) = handler {
                f(change)
            }
        }
    }

    pub fn add_before_apply_change_handler(&mut self, handler: ChangeEventHandler) {
        self.0.push(EventHandler::BeforeApplyChange(handler))
    }

    pub fn add_after_apply_change_handler(&mut self, handler: ChangeEventHandler) {
        self.0.push(EventHandler::AfterApplyChange(handler))
    }
}

type ChangeEventHandler = Box<dyn FnMut(&Change) + Send>;

enum EventHandler {
    BeforeApplyChange(ChangeEventHandler),
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

impl PartialEq for EventHandler {
    fn eq(&self, _other: &Self) -> bool {
        // TODO: check if this seems sensible to do
        false
    }
}
