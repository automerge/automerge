use std::collections::HashMap;

use automerge_backend::{Backend, Change, SyncMessage, SyncState};
use automerge_frontend::{value_ref::RootRef, Frontend, MutableDocument, Path, Value};
use automerge_protocol as amp;
use automerge_protocol::OpId;
use thiserror::Error;

use crate::{BackendError, FrontendError, InvalidPatch};

#[derive(Debug, Error)]
pub enum AutomergeError {
    #[error(transparent)]
    BackendError(#[from] BackendError),
    #[error(transparent)]
    FrontendError(#[from] FrontendError),
    #[error(transparent)]
    InvalidPatch(#[from] InvalidPatch),
}

/// A more advanced way of building an [`Automerge`] from constituent parts.
pub struct AutomergeBuilder {
    frontend: Option<Frontend>,
    backend: Option<Backend>,
}

impl Default for AutomergeBuilder {
    fn default() -> Self {
        Self {
            frontend: Default::default(),
            backend: Default::default(),
        }
    }
}

impl AutomergeBuilder {
    /// Set the frontend, consuming the builder and returning it for chaining.
    pub fn with_frontend(mut self, frontend: Frontend) -> Self {
        self.frontend = Some(frontend);
        self
    }

    /// Set the frontend, taking a mutable reference to the builder and returning it.
    pub fn set_frontend(&mut self, frontend: Frontend) -> &mut Self {
        self.frontend = Some(frontend);
        self
    }

    /// Set the backend, consuming the builder and returning it for chaining.
    pub fn with_backend(mut self, backend: Backend) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Set the backend, taking a mutable reference to the builder and returning it.
    pub fn set_backend(&mut self, backend: Backend) -> &mut Self {
        self.backend = Some(backend);
        self
    }

    /// Build this builder, setting any unset fields to their defaults.
    pub fn build(self) -> Automerge {
        Automerge {
            frontend: self.frontend.unwrap_or_default(),
            backend: self.backend.unwrap_or_default(),
        }
    }
}

/// The core automerge document.
///
/// In reality this combines both the [`Frontend`] and [`Backend`] and handles synchronising the frontend
/// with updates from the backend caused by local changes.
#[derive(Debug, Default)]
pub struct Automerge {
    frontend: Frontend,
    backend: Backend,
}

impl Automerge {
    /// Construct a new, empty, automerge instance.
    ///
    /// To build a new document from a previously saved one see [`load`](Self::load).
    ///
    /// For using a pre-built frontend and/or backend see [`AutomergeBuilder`].
    pub fn new() -> Self {
        Self {
            frontend: Frontend::new(),
            backend: Backend::new(),
        }
    }

    /// Get the current value that this document stores.
    pub fn state(&mut self) -> &Value {
        self.frontend.state()
    }

    /// Get a reference to the value that this document stores, without building it all.
    pub fn value_ref(&self) -> RootRef {
        self.frontend.value_ref()
    }

    /// Make a change to this document.
    ///
    /// Internally, this applies the updates to the frontend to get a change.
    /// This change is then applied to the backend in return for a patch.
    /// This patch is then used to update the frontend's bookkeeping.
    pub fn change<F, O, E>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<(O, Option<&Change>), E>
    where
        E: std::error::Error,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        let (out, change) = self.frontend.change(message, change_closure)?;
        if let Some(change) = change {
            let (patch, change) = self
                .backend
                .apply_local_change(change)
                .expect("Applied an invalid change");
            self.frontend
                .apply_patch(patch)
                .expect("Applied an invalid patch");
            Ok((out, Some(change)))
        } else {
            Ok((out, None))
        }
    }

    /// Get any current conflicts at the given path.
    pub fn get_conflicts(&self, path: &Path) -> Option<HashMap<OpId, Value>> {
        self.frontend.get_conflicts(path)
    }

    /// Get the value at a specific path in the document.
    ///
    /// This will construct the value, the [`ValueRef`](Self::value_ref) API is useful for avoiding
    /// that.
    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.frontend.get_value(path)
    }

    /// Load all of the changes from a previous document into this document.
    pub fn load_changes(&mut self, changes: Vec<Change>) -> Result<(), BackendError> {
        self.backend.load_changes(changes)
    }

    /// Apply changes from a remote document onto this one.
    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        let patch = self.backend.apply_changes(changes)?;
        self.frontend.apply_patch(patch)?;
        Ok(())
    }

    /// Get the current heads of the hash graph used for synchronising this document.
    pub fn get_heads(&self) -> Vec<amp::ChangeHash> {
        self.backend.get_heads()
    }

    /// Get all of the changes which have occurred since `have_deps` in this document.
    pub fn get_changes(&self, have_deps: &[amp::ChangeHash]) -> Vec<&Change> {
        self.backend.get_changes(have_deps)
    }

    /// Save this document into a compact byte representation.
    pub fn save(&self) -> Result<Vec<u8>, BackendError> {
        self.backend.save()
    }

    /// Load a new document from a previously saved one.
    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let backend = Backend::load(data)?;
        let patch = backend.get_patch()?;
        let mut frontend = Frontend::new();
        frontend.apply_patch(patch)?;
        Ok(Self { frontend, backend })
    }

    /// Generate a message to synchronise another peer.
    pub fn generate_sync_message(&self, sync_state: &mut SyncState) -> Option<SyncMessage> {
        self.backend.generate_sync_message(sync_state)
    }

    /// Receive a sync message from a peer and apply any updates that they sent.
    pub fn receive_sync_message(
        &mut self,
        sync_state: &mut SyncState,
        message: SyncMessage,
    ) -> Result<(), AutomergeError> {
        let patch = self.backend.receive_sync_message(sync_state, message)?;
        if let Some(patch) = patch {
            self.frontend.apply_patch(patch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use automerge_frontend::{LocalChange, Primitive};

    use super::*;

    #[test]
    fn change() {
        let mut a = Automerge::new();

        let path = Path::root().key("a");
        let value = Value::Primitive(Primitive::Str("test".into()));

        let ((), _change) = a
            .change(None, |doc| {
                doc.add_change(LocalChange::set(path.clone(), value.clone()))
            })
            .unwrap();

        let found_value = a.get_value(&path);
        assert_eq!(found_value, Some(value));
    }

    #[test]
    fn save_load() {
        let mut a = Automerge::new();

        let path = Path::root().key("a");
        let value = Value::Primitive(Primitive::Str("test".into()));

        let ((), _change) = a
            .change(None, |doc| {
                doc.add_change(LocalChange::set(path.clone(), value.clone()))
            })
            .unwrap();

        let found_value = a.get_value(&path);
        assert_eq!(found_value, Some(value));

        let bytes = a.save().unwrap();
        let b = Automerge::load(bytes).unwrap();

        assert_eq!(b.get_value(&Path::root()), a.get_value(&Path::root()))
    }

    #[test]
    fn foreign_change() {
        let mut a = Automerge::new();

        let path = Path::root().key("a");
        let value = Value::Primitive(Primitive::Str("test".into()));

        let ((), _change) = a
            .change(None, |doc| {
                doc.add_change(LocalChange::set(path.clone(), value.clone()))
            })
            .unwrap();

        let found_value = a.get_value(&path);
        assert_eq!(found_value, Some(value));

        let mut b = Automerge::new();
        b.apply_changes(a.get_changes(&[]).into_iter().cloned().collect())
            .unwrap();

        assert_eq!(b.get_value(&Path::root()), a.get_value(&Path::root()))
    }

    #[test]
    fn sync() {
        let mut a = Automerge::new();

        let path = Path::root().key("a");
        let value = Value::Primitive(Primitive::Str("test".into()));

        let ((), _change) = a
            .change(None, |doc| {
                doc.add_change(LocalChange::set(path.clone(), value.clone()))
            })
            .unwrap();

        let found_value = a.get_value(&path);
        assert_eq!(found_value, Some(value));

        let mut a_sync_state = SyncState::default();
        let mut b_sync_state = SyncState::default();

        let mut b = Automerge::new();

        let msg = b.generate_sync_message(&mut a_sync_state).unwrap();
        a.receive_sync_message(&mut b_sync_state, msg).unwrap();

        let msg = a.generate_sync_message(&mut b_sync_state).unwrap();
        b.receive_sync_message(&mut a_sync_state, msg).unwrap();

        assert_eq!(b.get_value(&Path::root()), a.get_value(&Path::root()))
    }
}
