use automerge_protocol as amp;
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    error::InvalidChangeRequest,
    state_tree::{
        LocalOperationResult, ResolvedPath, ResolvedPathMut, SetOrInsertPayload, StateTree,
    },
    value::{Cursor, Primitive, Value},
    Path, PathElement,
};

pub trait MutableDocument {
    fn value_at_path(&self, path: &Path) -> Option<Value>;
    fn cursor_to_path(&self, path: &Path) -> Option<Cursor>;
    fn add_change(&mut self, change: LocalChange) -> Result<(), InvalidChangeRequest>;
}

#[derive(Debug, PartialEq, Clone)]
pub enum LocalOperation {
    Set(Value),
    Delete,
    Increment(i64),
    Insert(Value),
    InsertMany(Vec<Value>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct LocalChange {
    path: Path,
    operation: LocalOperation,
}

impl LocalChange {
    /// Set the value at `path` to `value`
    pub fn set<TV>(path: Path, value: TV) -> LocalChange
    where
        TV: Into<Value>,
    {
        LocalChange {
            path,
            operation: LocalOperation::Set(value.into()),
        }
    }

    /// Delete the entry at `path`
    pub fn delete(path: Path) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::Delete,
        }
    }

    /// Increment the counter at `path` by 1
    pub fn increment(path: Path) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::Increment(1),
        }
    }

    /// Increment the counter at path by a (possibly negative) amount `by`
    pub fn increment_by(path: Path, by: i64) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::Increment(by),
        }
    }

    pub fn insert(path: Path, value: Value) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::Insert(value),
        }
    }

    pub fn insert_many(path: Path, values: Vec<Value>) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::InsertMany(values),
        }
    }
}

/// `MutationTracker` is used as the context in which a mutation closure is
/// applied. The mutation tracker implements `MutableDocument`, which is how it
/// captures the changes that the mutation closure is making.
///
/// For each operation in the mutation closure the `MutationTracker` generates
/// a diff and immediately applies it to the `StateTree` it is constructed
/// with. It also adds the change to a set of operations. This set of operations
/// is used to generate a `ChangeRequest` once the closure is completed.
pub struct MutationTracker<'a> {
    state: &'a mut StateTree,
    ops: Vec<amp::Op>,
    pub max_op: u64,
    actor_id: amp::ActorId,
}

impl<'a> MutationTracker<'a> {
    pub(crate) fn new(state_tree: &'a mut StateTree, max_op: u64, actor_id: amp::ActorId) -> Self {
        Self {
            state: state_tree,
            ops: Vec::new(),
            max_op,
            actor_id,
        }
    }

    pub fn ops(self) -> Vec<amp::Op> {
        self.ops
    }

    /// If the `value` is a map, individually assign each k,v in it to a key in
    /// the root object
    fn wrap_root_assignment(&mut self, value: Value) -> Result<(), InvalidChangeRequest> {
        match value {
            Value::Map(kvs) => {
                for (k, v) in kvs.iter() {
                    self.add_change(LocalChange::set(Path::root().key(k), v.clone()))?;
                }
                Ok(())
            }
            _ => Err(InvalidChangeRequest::CannotSetNonMapObjectAsRoot {
                value: value.clone(),
            }),
        }
    }

    fn apply_state_change(&mut self, change: LocalOperationResult) {
        self.max_op += change.new_ops.len() as u64;
        self.ops.extend(change.new_ops);
    }

    fn insert_helper<I>(&mut self, path: &Path, values: I) -> Result<(), InvalidChangeRequest>
    where
        I: ExactSizeIterator<Item = Value>,
    {
        if let Some(name) = path.name() {
            let index = match name {
                PathElement::Index(i) => i,
                _ => {
                    return Err(InvalidChangeRequest::InsertWithNonSequencePath {
                        path: path.clone(),
                    })
                }
            };
            if let Some(parent) = self.state.resolve_path_mut(&path.parent()) {
                match parent {
                    ResolvedPathMut::List(mut list_target) => {
                        let payload = SetOrInsertPayload {
                            start_op: self.max_op + 1,
                            actor: &self.actor_id.clone(),
                            value: values,
                        };
                        let res = list_target.insert_many(*index, payload)?;
                        self.apply_state_change(res);
                    }
                    ResolvedPathMut::Text(mut text_target) => {
                        let mut chars = Vec::with_capacity(values.len());
                        for value in values {
                            match value {
                                Value::Primitive(Primitive::Str(s)) => {
                                    if s.graphemes(true).count() == 1 {
                                        chars.push(s.clone())
                                    } else {
                                        return Err(
                                            InvalidChangeRequest::InsertNonTextInTextObject {
                                                path: path.clone(),
                                                object: Value::Primitive(Primitive::Str(s)),
                                            },
                                        );
                                    }
                                }
                                _ => {
                                    return Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                        path: path.clone(),
                                        object: value.clone(),
                                    })
                                }
                            }
                        }
                        let payload = SetOrInsertPayload {
                            start_op: self.max_op + 1,
                            actor: &self.actor_id.clone(),
                            value: chars.into_iter(),
                        };
                        let res = text_target.insert_many(*index, payload)?;
                        self.apply_state_change(res);
                    }
                    _ => return Err(InvalidChangeRequest::NoSuchPathError { path: path.clone() }),
                };
                Ok(())
            } else {
                Err(InvalidChangeRequest::InsertForNonSequenceObject { path: path.clone() })
            }
        } else {
            Err(InvalidChangeRequest::NoSuchPathError { path: path.clone() })
        }
    }
}

impl<'a> MutableDocument for MutationTracker<'a> {
    fn value_at_path(&self, path: &Path) -> Option<Value> {
        self.state.resolve_path(path).map(|r| r.default_value())
    }

    fn cursor_to_path(&self, path: &Path) -> Option<Cursor> {
        if let Some(PathElement::Index(i)) = path.name() {
            if let Some(parent) = self.state.resolve_path(&path.parent()) {
                match parent {
                    ResolvedPath::List(list_target) => list_target.get_cursor(*i).ok(),
                    ResolvedPath::Text(text_target) => text_target.get_cursor(*i).ok(),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    fn add_change(&mut self, change: LocalChange) -> Result<(), InvalidChangeRequest> {
        match change.operation {
            LocalOperation::Set(value) => {
                //TODO double resolving is ugly here
                if let Some(ResolvedPath::Counter(_)) = self.state.resolve_path(&change.path) {
                    return Err(InvalidChangeRequest::CannotOverwriteCounter { path: change.path });
                };
                if let Some(name) = change.path.name() {
                    if let Some(parent) = self.state.resolve_path_mut(&change.path.parent()) {
                        match (name, parent) {
                            (
                                PathElement::Key(ref k),
                                ResolvedPathMut::Root(ref mut root_target),
                            ) => {
                                let payload = SetOrInsertPayload {
                                    start_op: self.max_op + 1,
                                    actor: &self.actor_id.clone(),
                                    value,
                                };
                                let res = root_target.set_key(k, payload);
                                self.apply_state_change(res);
                                Ok(())
                            }
                            (PathElement::Key(ref k), ResolvedPathMut::Map(ref mut maptarget)) => {
                                let payload = SetOrInsertPayload {
                                    start_op: self.max_op + 1,
                                    actor: &self.actor_id.clone(),
                                    value,
                                };
                                let res = maptarget.set_key(k, payload);
                                self.apply_state_change(res);
                                Ok(())
                            }
                            (
                                PathElement::Key(ref k),
                                ResolvedPathMut::Table(ref mut tabletarget),
                            ) => {
                                let payload = SetOrInsertPayload {
                                    start_op: self.max_op + 1,
                                    actor: &self.actor_id.clone(),
                                    value,
                                };
                                let res = tabletarget.set_key(k, payload);
                                self.apply_state_change(res);
                                Ok(())
                            }
                            // In this case we are trying to modify a key in something which is not
                            // an object or a table, so the path does not exist
                            (PathElement::Key(_), _) => {
                                Err(InvalidChangeRequest::NoSuchPathError { path: change.path })
                            }
                            (PathElement::Index(i), ResolvedPathMut::List(ref mut list_target)) => {
                                let payload = SetOrInsertPayload {
                                    start_op: self.max_op + 1,
                                    actor: &self.actor_id.clone(),
                                    value,
                                };
                                let res = list_target.set(*i, payload)?;
                                self.apply_state_change(res);
                                Ok(())
                            }
                            (PathElement::Index(i), ResolvedPathMut::Text(ref mut text)) => {
                                match value {
                                    Value::Primitive(Primitive::Str(s)) => {
                                        if s.graphemes(true).count() == 1 {
                                            let payload = SetOrInsertPayload {
                                                start_op: self.max_op + 1,
                                                actor: &self.actor_id.clone(),
                                                value: s,
                                            };
                                            let res = text.set(*i, payload)?;
                                            self.apply_state_change(res);
                                            Ok(())
                                        } else {
                                            Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                                path: change.path.clone(),
                                                object: Value::Primitive(Primitive::Str(s)),
                                            })
                                        }
                                    }
                                    _ => Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                        path: change.path.clone(),
                                        object: value.clone(),
                                    }),
                                }
                            }
                            (PathElement::Index(_), _) => {
                                Err(InvalidChangeRequest::InsertWithNonSequencePath {
                                    path: change.path.clone(),
                                })
                            }
                        }
                    } else {
                        Err(InvalidChangeRequest::NoSuchPathError { path: change.path })
                    }
                } else {
                    self.wrap_root_assignment(value)
                }
            }
            LocalOperation::Delete => {
                if let Some(name) = change.path.name() {
                    if let Some(pr) = self.state.resolve_path_mut(&change.path.parent()) {
                        let state_change = match pr {
                            ResolvedPathMut::Counter(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPathMut::List(mut l) => match name {
                                PathElement::Index(i) => l.remove(*i)?,
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPathMut::Text(mut t) => match name {
                                PathElement::Index(i) => t.remove(*i)?,
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPathMut::Primitive(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPathMut::Map(mut m) => match name {
                                PathElement::Key(k) => m.delete_key(k),
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPathMut::Table(mut t) => match name {
                                PathElement::Key(k) => t.delete_key(k),
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPathMut::Character(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPathMut::Root(mut r) => match name {
                                PathElement::Key(k) => r.delete_key(k),
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                        };
                        self.apply_state_change(state_change);
                        Ok(())
                    } else {
                        Err(InvalidChangeRequest::NoSuchPathError { path: change.path })
                    }
                } else {
                    Err(InvalidChangeRequest::CannotDeleteRootObject)
                }
            }
            LocalOperation::Increment(by) => {
                if change.path.name().is_some() {
                    if let Some(pr) = self.state.resolve_path_mut(&change.path) {
                        match pr {
                            ResolvedPathMut::Counter(mut counter_target) => {
                                let res = counter_target.increment(by);
                                self.apply_state_change(res);
                                Ok(())
                            }
                            _ => Err(InvalidChangeRequest::IncrementForNonCounterObject {
                                path: change.path.clone(),
                            }),
                        }
                    } else {
                        Err(InvalidChangeRequest::NoSuchPathError { path: change.path })
                    }
                } else {
                    Err(InvalidChangeRequest::IncrementForNonCounterObject {
                        path: change.path.clone(),
                    })
                }
            }
            LocalOperation::Insert(value) => {
                self.insert_helper(&change.path, std::iter::once(value))
            }
            LocalOperation::InsertMany(values) => {
                self.insert_helper(&change.path, values.into_iter())
            }
        }
    }
}
