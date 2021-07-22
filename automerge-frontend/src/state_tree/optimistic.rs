use std::ops::{Deref, DerefMut};

use super::{MultiGrapheme, MultiValue, ResolvedPathMut, StateTree};
use crate::{path::PathElement, Path};

/// Contains the required data to undo an operation on the state tree.
#[derive(Clone, Debug)]
pub(crate) enum LocalOperationForRollback {
    Set { old: Option<MultiValue> },
    SetList { old: MultiValue },
    SetText { old: MultiGrapheme },
    Delete { old: MultiValue },
    DeleteText { old: MultiGrapheme },
    Insert,
    InsertMany { count: usize },
    Increment { by: i64 },
}

/// Keeps track of the changes made to a state tree and allows rolling back changes.
#[derive(Clone, Debug)]
pub(crate) struct OptimisticStateTree {
    state: StateTree,
    copies_for_rollback: Vec<(Path, LocalOperationForRollback)>,
}

impl OptimisticStateTree {
    pub(crate) fn new(state_tree: StateTree) -> Self {
        Self {
            state: state_tree,
            copies_for_rollback: Vec::new(),
        }
    }

    pub(crate) fn take_state(&mut self) -> StateTree {
        std::mem::take(&mut self.state)
    }

    /// Commit the operations, making it possible to roll them back.
    pub(crate) fn commit_operations(&mut self, ops: Vec<(Path, LocalOperationForRollback)>) {
        self.copies_for_rollback.extend(ops)
    }

    /// Rollback a list of operations that have been applied.
    pub(crate) fn rollback_operations(&mut self, ops: Vec<(Path, LocalOperationForRollback)>) {
        self.rollback(ops.into_iter())
    }

    /// Rollback all applied operations.
    pub fn rollback_all(&mut self) {
        let rollback_ops = std::mem::take(&mut self.copies_for_rollback);
        self.rollback(rollback_ops.into_iter())
    }

    /// Undo the operations applied to this document.
    ///
    /// This is used in the case of an error to undo the already applied changes.
    fn rollback(
        &mut self,
        ops: impl DoubleEndedIterator<Item = (Path, LocalOperationForRollback)>,
    ) {
        for (path, op) in ops.rev() {
            match op {
                LocalOperationForRollback::Set { old } => {
                    if let Some(key) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match (key, parent) {
                                (PathElement::Key(key), ResolvedPathMut::Root(mut map)) => {
                                    map.rollback_set(key.clone(), old)
                                }
                                (PathElement::Key(key), ResolvedPathMut::Map(mut map)) => {
                                    map.rollback_set(key.clone(), old)
                                }
                                (PathElement::Key(key), ResolvedPathMut::Table(mut table)) => {
                                    table.rollback_set(key.clone(), old)
                                }
                                (PathElement::Key(_), ResolvedPathMut::List(_))
                                | (PathElement::Key(_), ResolvedPathMut::Text(_))
                                | (PathElement::Key(_), ResolvedPathMut::Character(_))
                                | (PathElement::Key(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Key(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non object with key")
                                }
                                (PathElement::Index(_), ResolvedPathMut::List(_))
                                | (PathElement::Index(_), ResolvedPathMut::Text(_))
                                | (PathElement::Index(_), ResolvedPathMut::Root(_))
                                | (PathElement::Index(_), ResolvedPathMut::Map(_))
                                | (PathElement::Index(_), ResolvedPathMut::Table(_))
                                | (PathElement::Index(_), ResolvedPathMut::Character(_))
                                | (PathElement::Index(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Index(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found index element while rolling back a set")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::SetList { old } => {
                    if let Some(key) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match (key, parent) {
                                (PathElement::Key(_), _) => {
                                    unreachable!("found key element while rolling back a setlist")
                                }
                                (PathElement::Index(i), ResolvedPathMut::List(mut list)) => {
                                    list.rollback_set(*i as usize, old)
                                }
                                (PathElement::Index(_), ResolvedPathMut::Text(_))
                                | (PathElement::Index(_), ResolvedPathMut::Root(_))
                                | (PathElement::Index(_), ResolvedPathMut::Map(_))
                                | (PathElement::Index(_), ResolvedPathMut::Table(_))
                                | (PathElement::Index(_), ResolvedPathMut::Character(_))
                                | (PathElement::Index(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Index(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non list with index")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::SetText { old } => {
                    if let Some(key) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match (key, parent) {
                                (PathElement::Key(_), _) => {
                                    unreachable!("found key element while rolling back a settext")
                                }
                                (PathElement::Index(i), ResolvedPathMut::Text(mut text)) => {
                                    text.rollback_set(*i as usize, old)
                                }
                                (PathElement::Index(_), ResolvedPathMut::List(_))
                                | (PathElement::Index(_), ResolvedPathMut::Root(_))
                                | (PathElement::Index(_), ResolvedPathMut::Map(_))
                                | (PathElement::Index(_), ResolvedPathMut::Table(_))
                                | (PathElement::Index(_), ResolvedPathMut::Character(_))
                                | (PathElement::Index(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Index(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non text with index")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::Delete { old } => {
                    if let Some(key) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match (key, parent) {
                                (PathElement::Key(key), ResolvedPathMut::Root(mut map)) => {
                                    map.rollback_delete(key.clone(), old)
                                }
                                (PathElement::Key(key), ResolvedPathMut::Map(mut map)) => {
                                    map.rollback_delete(key.clone(), old)
                                }
                                (PathElement::Key(key), ResolvedPathMut::Table(mut table)) => {
                                    table.rollback_delete(key.clone(), old)
                                }
                                (PathElement::Key(_), ResolvedPathMut::List(_))
                                | (PathElement::Key(_), ResolvedPathMut::Text(_))
                                | (PathElement::Key(_), ResolvedPathMut::Character(_))
                                | (PathElement::Key(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Key(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non object with key")
                                }
                                (PathElement::Index(i), ResolvedPathMut::List(mut list)) => {
                                    list.rollback_delete(*i as usize, old)
                                }
                                (PathElement::Index(_), ResolvedPathMut::Text(_))
                                | (PathElement::Index(_), ResolvedPathMut::Root(_))
                                | (PathElement::Index(_), ResolvedPathMut::Map(_))
                                | (PathElement::Index(_), ResolvedPathMut::Table(_))
                                | (PathElement::Index(_), ResolvedPathMut::Character(_))
                                | (PathElement::Index(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Index(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non list with index")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::DeleteText { old } => {
                    if let Some(key) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match (key, parent) {
                                (PathElement::Key(_), ResolvedPathMut::Root(_))
                                | (PathElement::Key(_), ResolvedPathMut::Map(_))
                                | (PathElement::Key(_), ResolvedPathMut::Table(_))
                                | (PathElement::Key(_), ResolvedPathMut::List(_))
                                | (PathElement::Key(_), ResolvedPathMut::Text(_))
                                | (PathElement::Key(_), ResolvedPathMut::Character(_))
                                | (PathElement::Key(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Key(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found key for SetText")
                                }
                                (PathElement::Index(i), ResolvedPathMut::Text(mut text)) => {
                                    text.rollback_delete(*i as usize, old)
                                }
                                (PathElement::Index(_), ResolvedPathMut::List(_))
                                | (PathElement::Index(_), ResolvedPathMut::Root(_))
                                | (PathElement::Index(_), ResolvedPathMut::Map(_))
                                | (PathElement::Index(_), ResolvedPathMut::Table(_))
                                | (PathElement::Index(_), ResolvedPathMut::Character(_))
                                | (PathElement::Index(_), ResolvedPathMut::Counter(_))
                                | (PathElement::Index(_), ResolvedPathMut::Primitive(_)) => {
                                    unreachable!("found non text with index")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::Insert => {
                    if let Some(PathElement::Index(index)) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match parent {
                                ResolvedPathMut::List(mut list) => {
                                    list.rollback_insert(*index as usize)
                                }
                                ResolvedPathMut::Text(mut text) => {
                                    text.rollback_insert(*index as usize)
                                }
                                ResolvedPathMut::Root(_)
                                | ResolvedPathMut::Map(_)
                                | ResolvedPathMut::Table(_)
                                | ResolvedPathMut::Character(_)
                                | ResolvedPathMut::Counter(_)
                                | ResolvedPathMut::Primitive(_) => {
                                    unreachable!("Found non list object in rollback insert")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::InsertMany { count } => {
                    if let Some(PathElement::Index(index)) = path.name() {
                        if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                            match parent {
                                ResolvedPathMut::List(mut list) => {
                                    for _ in 0..count {
                                        list.rollback_insert(*index as usize)
                                    }
                                }
                                ResolvedPathMut::Text(mut text) => {
                                    for _ in 0..count {
                                        text.rollback_insert(*index as usize)
                                    }
                                }
                                ResolvedPathMut::Root(_)
                                | ResolvedPathMut::Map(_)
                                | ResolvedPathMut::Table(_)
                                | ResolvedPathMut::Character(_)
                                | ResolvedPathMut::Counter(_)
                                | ResolvedPathMut::Primitive(_) => {
                                    unreachable!("Found non list object in rollback insert")
                                }
                            }
                        }
                    }
                }
                LocalOperationForRollback::Increment { by } => {
                    if path.name().is_some() {
                        if let Some(ResolvedPathMut::Counter(mut counter)) =
                            self.state.resolve_path_mut(&path)
                        {
                            counter.rollback_increment(by)
                        }
                    }
                }
            }
        }
    }
}

impl Deref for OptimisticStateTree {
    type Target = StateTree;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for OptimisticStateTree {
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        &mut self.state
    }
}
