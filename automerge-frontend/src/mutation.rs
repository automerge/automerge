use crate::{Value, AutomergeFrontendError, ope};
use crate::change_context::ChangeContext;
use crate::object::Object;
use crate::value::value_to_op_requests;
use automerge_backend as amb;

pub trait MutableDocument {
    fn value_at_path(&self, path: &Path) -> Option<Value>;
    fn add_change(&mut self, change: LocalChange) -> Result<(), AutomergeFrontendError>;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PathElement {
    Key(String),
    Index(usize),
}

impl PathElement {
    pub(crate) fn to_request_key(&self) -> amb::RequestKey {
        match self {
            PathElement::Key(s) => amb::RequestKey::Str(s.into()),
            PathElement::Index(i) => amb::RequestKey::Num(*i as u64),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Path(Vec<PathElement>);

impl Path {
    pub fn root() -> Path {
        Path(Vec::new())
    }

    pub fn index(mut self, index: usize) -> Self {
        self.0.push(PathElement::Index(index));
        self
    }

    pub fn key<S: Into<String>>(mut self, key: S) -> Path {
        self.0.push(PathElement::Key(key.into()));
        self
    }
    
    pub fn parent(&self) -> Self {
        if self.0.len() == 0 {
            Path(Vec::new())
        } else {
            let mut new_path = self.0.clone();
            new_path.pop();
            Path(new_path)
        }
    }

    /// Get the final component of the path, if any
    pub(crate) fn name(&self) -> Option<&PathElement> {
        self.0.last()
    }
}

pub(crate) enum LocalOperation {
    Set(Value),
    Delete,
}

pub struct LocalChange {
    path: Path,
    operation: LocalOperation,
}

impl LocalChange {
    pub fn set(path: Path, value: Value) -> LocalChange {
        LocalChange{
            path,
            operation: LocalOperation::Set(value)
        }
    }

    pub fn delete(path: Path) -> LocalChange {
        LocalChange{
            path,
            operation: LocalOperation::Delete,
        }
    }
}

/// `MutationTracker` is used as the context in which a mutation closure is 
/// applied. The mutation tracker implements `MutableDocument`, which is how it
/// captures the changes that the mutation closure is making.
///
/// For each operation in the mutation closure the `MutationTracker` generates
/// a diff and immediately applies it to the `ChangeContext` it is constructed
/// with. It also adds the change to a set of operations. This set of operations
/// is used to generate a `ChangeRequest` once the closure is completed.
///
/// If the mutation closure is successful then the changes it has enacted can
/// be applied using `ChangeContext::commit`
pub struct MutationTracker<'a, 'b> {
    change_context: &'a mut ChangeContext<'b>,
    ops: Vec<amb::OpRequest>,
}

impl<'a,'b> MutationTracker<'a, 'b> {
    pub fn new(change_context: &'a mut ChangeContext<'b>) -> MutationTracker<'a,'b> {
        MutationTracker{
            change_context,
            ops: Vec::new(),
        }
    }

    pub fn change_request(&self) -> Option<amb::ChangeRequest> {
        panic!("not implemented")
    }

    fn parent_object_id(&self, path: &Path) -> Option<amb::ObjectID> {
        let mut stack = path.clone().0;
        stack.pop();
        stack.reverse();
        let mut current_obj = self.change_context.value_for_object_id(&amb::ObjectID::Root).unwrap();
        while let Some(next_elem) = stack.pop() {
            match (next_elem, *current_obj.borrow()) {
                (PathElement::Key(k), Object::Map(oid, vals, _)) => {
                    if let Some(target) = vals.get(&amb::Key(k)) {
                        current_obj = target.default_value();
                    } else {
                        return None
                    }
                },
                (PathElement::Index(i), Object::Sequence(oid, vals, _)) => {
                    if let Some(Some(target)) = vals.get(i) {
                        current_obj = target.default_value();
                    } else {
                        return None
                    }
                },
                _ => return None
            }
        };
        match *current_obj.borrow() {
            Object::Map(oid, _, _) => Some(oid),
            Object::Sequence(oid, _, _) => Some(oid),
            _ => None,
        }
    }

}

impl<'a, 'b> MutableDocument for MutationTracker<'a, 'b> {
    fn value_at_path(&self, path: &Path) -> Option<Value> {
        panic!("not implemented")
    }

    fn add_change(&mut self, change: LocalChange) -> Result<(), AutomergeFrontendError> {
        match change.operation {
            LocalOperation::Set(value) => {
                if let Some(oid) = self.parent_object_id(&change.path) {
                    let insert = match *self.change_context.value_for_object_id(oid).borrow() {
                        Some(Object::Sequence(_, _, _)) => true,
                        _ => false,
                    };
                    let ops = value_to_op_requests(
                        oid.to_string(),
                        change.path.name().clone(),
                        value,
                        insert,
                    );
                } else {
                    Err(AutomergeFrontendError::NoSuchPathError(change.path))
                }
            }
            LocalOperation::Delete => panic!("delete not implemented")
        }
    }
}
