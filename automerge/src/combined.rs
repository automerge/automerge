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
    #[error(transparent)]
    UserError(#[from] Box<dyn std::error::Error>),
}

#[derive(Debug, Default)]
pub struct Automerge {
    frontend: Frontend,
    backend: Backend,
}

impl Automerge {
    pub fn new() -> Self {
        Self {
            frontend: Frontend::new(),
            backend: Backend::new(),
        }
    }

    pub fn state(&mut self) -> &Value {
        self.frontend.state()
    }

    pub fn value_ref(&self) -> RootRef {
        self.frontend.value_ref()
    }

    pub fn change<F, O, E>(
        &mut self,
        message: Option<String>,
        change_closure: F,
    ) -> Result<(O, Option<&Change>), AutomergeError>
    where
        E: std::error::Error + 'static,
        F: FnOnce(&mut dyn MutableDocument) -> Result<O, E>,
    {
        let (out, change) = self
            .frontend
            .change(message, change_closure)
            .map_err(|e| AutomergeError::UserError(Box::new(e)))?;
        if let Some(change) = change {
            let (patch, change) = self.backend.apply_local_change(change)?;
            self.frontend.apply_patch(patch)?;
            Ok((out, Some(change)))
        } else {
            Ok((out, None))
        }
    }

    pub fn get_conflicts(&self, path: &Path) -> Option<HashMap<OpId, Value>> {
        self.frontend.get_conflicts(path)
    }

    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.frontend.get_value(path)
    }

    pub fn load_changes(&mut self, changes: Vec<Change>) -> Result<(), BackendError> {
        self.backend.load_changes(changes)
    }

    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        let patch = self.backend.apply_changes(changes)?;
        self.frontend.apply_patch(patch)?;
        Ok(())
    }

    pub fn get_heads(&self) -> Vec<amp::ChangeHash> {
        self.backend.get_heads()
    }

    pub fn get_changes(&self, have_deps: &[amp::ChangeHash]) -> Vec<&Change> {
        self.backend.get_changes(have_deps)
    }

    pub fn save(&self) -> Result<Vec<u8>, BackendError> {
        self.backend.save()
    }

    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let backend = Backend::load(data)?;
        let patch = backend.get_patch()?;
        let mut frontend = Frontend::new();
        frontend.apply_patch(patch)?;
        Ok(Self { frontend, backend })
    }

    pub fn generate_sync_message(&self, sync_state: &mut SyncState) -> Option<SyncMessage> {
        self.backend.generate_sync_message(sync_state)
    }

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
