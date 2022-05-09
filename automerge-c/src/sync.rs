use automerge as am;

/// \struct AMsyncMessage
/// \brief A synchronization message for a peer.
pub struct AMsyncMessage(am::sync::Message);

impl AMsyncMessage {
    pub fn new(message: am::sync::Message) -> Self {
        Self(message)
    }
}

impl AsRef<am::sync::Message> for AMsyncMessage {
    fn as_ref(&self) -> &am::sync::Message {
        &self.0
    }
}

/// \struct AMsyncState
/// \brief The state of synchronization with a peer.
pub struct AMsyncState(am::sync::State);

impl AMsyncState {
    pub fn new(state: am::sync::State) -> Self {
        Self(state)
    }
}

impl AsMut<am::sync::State> for AMsyncState {
    fn as_mut(&mut self) -> &mut am::sync::State {
        &mut self.0
    }
}

impl From<AMsyncState> for *mut AMsyncState {
    fn from(b: AMsyncState) -> Self {
        Box::into_raw(Box::new(b))
    }
}
