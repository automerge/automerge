use crate::error::InvalidChangeRequest;
use crate::state_tree::{LocalOperationResult, ResolvedPath, SetOrInsertPayload, StateTree};
use crate::value::Value;
use crate::{Path, PathElement};
use automerge_protocol as amp;

pub trait MutableDocument {
    fn value_at_path(&self, path: &Path) -> Option<Value>;
    fn add_change(&mut self, change: LocalChange) -> Result<(), InvalidChangeRequest>;
}

pub(crate) enum LocalOperation {
    Set(Value),
    Delete,
    Increment(u32),
    Insert(Value),
}

pub struct LocalChange {
    path: Path,
    operation: LocalOperation,
}

impl LocalChange {
    /// Set the value at `path` to `value`
    pub fn set(path: Path, value: Value) -> LocalChange {
        LocalChange {
            path,
            operation: LocalOperation::Set(value),
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
    pub fn increment_by(path: Path, by: u32) -> LocalChange {
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
}

/// `MutationTracker` is used as the context in which a mutation closure is
/// applied. The mutation tracker implements `MutableDocument`, which is how it
/// captures the changes that the mutation closure is making.
///
/// For each operation in the mutation closure the `MutationTracker` generates
/// a diff and immediately applies it to the `StateTree` it is constructed
/// with. It also adds the change to a set of operations. This set of operations
/// is used to generate a `ChangeRequest` once the closure is completed.
pub struct MutationTracker {
    pub(crate) state: StateTree,
    pub(crate) ops: Vec<amp::Op>,
    pub max_op: u64,
    actor_id: amp::ActorID,
}

impl MutationTracker {
    pub(crate) fn new(
        state_tree: StateTree,
        max_op: u64,
        actor_id: amp::ActorID,
    ) -> MutationTracker {
        MutationTracker {
            state: state_tree,
            ops: Vec::new(),
            max_op,
            actor_id,
        }
    }

    pub fn ops(&self) -> Option<Vec<amp::Op>> {
        if !self.ops.is_empty() {
            Some(self.ops.clone())
        } else {
            None
        }
    }

    /// If the `value` is a map, individually assign each k,v in it to a key in
    /// the root object
    fn wrap_root_assignment(&mut self, value: &Value) -> Result<(), InvalidChangeRequest> {
        match value {
            Value::Map(kvs, amp::MapType::Map) => {
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
        self.state = change.new_state;
        self.max_op += change.new_ops.len() as u64;
        self.ops.extend(change.new_ops);
    }
}

impl MutableDocument for MutationTracker {
    fn value_at_path(&self, path: &Path) -> Option<Value> {
        self.state.resolve_path(path).map(|r| r.default_value())
    }

    fn add_change(&mut self, change: LocalChange) -> Result<(), InvalidChangeRequest> {
        match &change.operation {
            LocalOperation::Set(value) => {
                //TODO double resolving is ugly here
                if let Some(target) = self.state.resolve_path(&change.path) {
                    if let ResolvedPath::Counter(_) = target {
                        return Err(InvalidChangeRequest::CannotOverwriteCounter {
                            path: change.path,
                        });
                    }
                };
                if let Some(name) = change.path.name() {
                    if let Some(parent) = self.state.resolve_path(&change.path.parent()) {
                        let payload = SetOrInsertPayload {
                            start_op: self.max_op + 1,
                            actor: &self.actor_id.clone(),
                            value,
                        };
                        match (name, parent) {
                            (PathElement::Key(ref k), ResolvedPath::Root(ref root_target)) => {
                                self.apply_state_change(root_target.set_key(k, payload));
                                Ok(())
                            }
                            (PathElement::Key(ref k), ResolvedPath::Map(ref maptarget)) => {
                                self.apply_state_change(maptarget.set_key(k, payload));
                                Ok(())
                            }
                            (PathElement::Key(ref k), ResolvedPath::Table(ref tabletarget)) => {
                                self.apply_state_change(tabletarget.set_key(k, payload));
                                Ok(())
                            }
                            // In this case we are trying to modify a key in something which is not
                            // an object or a table, so the path does not exist
                            (PathElement::Key(_), _) => {
                                Err(InvalidChangeRequest::NoSuchPathError { path: change.path })
                            }
                            (PathElement::Index(i), ResolvedPath::List(ref list_target)) => {
                                self.apply_state_change(list_target.set(*i, payload)?);
                                Ok(())
                            }
                            (PathElement::Index(i), ResolvedPath::Text(ref text)) => match value {
                                Value::Primitive(amp::ScalarValue::Str(s)) => {
                                    if s.len() == 1 {
                                        let payload = SetOrInsertPayload {
                                            start_op: self.max_op + 1,
                                            actor: &self.actor_id.clone(),
                                            value: s.chars().next().unwrap(),
                                        };
                                        self.apply_state_change(text.set(*i, payload)?);
                                        Ok(())
                                    } else {
                                        Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                            path: change.path.clone(),
                                            object: value.clone(),
                                        })
                                    }
                                }
                                _ => Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                    path: change.path.clone(),
                                    object: value.clone(),
                                }),
                            },
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
                    if let Some(pr) = self.state.resolve_path(&change.path.parent()) {
                        let state_change = match pr {
                            ResolvedPath::Counter(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPath::List(l) => match name {
                                PathElement::Index(i) => l.remove(*i)?,
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPath::Text(t) => match name {
                                PathElement::Index(i) => t.remove(*i)?,
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPath::Primitive(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPath::Map(m) => match name {
                                PathElement::Key(k) => m.delete_key(k),
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPath::Table(t) => match name {
                                PathElement::Key(k) => t.delete_key(k),
                                _ => {
                                    return Err(InvalidChangeRequest::NoSuchPathError {
                                        path: change.path,
                                    })
                                }
                            },
                            ResolvedPath::Character(_) => {
                                return Err(InvalidChangeRequest::NoSuchPathError {
                                    path: change.path,
                                })
                            }
                            ResolvedPath::Root(r) => match name {
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
                    if let Some(pr) = self.state.resolve_path(&change.path) {
                        match pr {
                            ResolvedPath::Counter(counter_target) => {
                                self.apply_state_change(counter_target.increment(*by as i64));
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
                if let Some(name) = change.path.name() {
                    let index = match name {
                        PathElement::Index(i) => i,
                        _ => {
                            return Err(InvalidChangeRequest::InsertWithNonSequencePath {
                                path: change.path,
                            })
                        }
                    };
                    if let Some(parent) = self.state.resolve_path(&change.path.parent()) {
                        match (parent, value) {
                            (ResolvedPath::List(list_target), _) => {
                                let payload = SetOrInsertPayload {
                                    start_op: self.max_op + 1,
                                    actor: &self.actor_id.clone(),
                                    value,
                                };
                                self.apply_state_change(list_target.insert(*index, payload)?);
                                Ok(())
                            }
                            (ResolvedPath::Text(text_target), val) => match val {
                                Value::Primitive(amp::ScalarValue::Str(s)) => {
                                    if s.len() == 1 {
                                        let payload = SetOrInsertPayload {
                                            start_op: self.max_op + 1,
                                            actor: &self.actor_id.clone(),
                                            value: s.chars().next().unwrap(),
                                        };
                                        self.apply_state_change(
                                            text_target.insert(*index, payload)?,
                                        );
                                        Ok(())
                                    } else {
                                        Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                            path: change.path,
                                            object: value.clone(),
                                        })
                                    }
                                }
                                _ => Err(InvalidChangeRequest::InsertNonTextInTextObject {
                                    path: change.path,
                                    object: value.clone(),
                                }),
                            },
                            _ => Err(InvalidChangeRequest::NoSuchPathError {
                                path: change.path.clone(),
                            }),
                        }
                    } else {
                        Err(InvalidChangeRequest::InsertForNonSequenceObject { path: change.path })
                    }
                } else {
                    Err(InvalidChangeRequest::NoSuchPathError {
                        path: change.path.clone(),
                    })
                }
            }
        }
    }
}
