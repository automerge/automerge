use crate::change_context::ChangeContext;
use crate::object::Object;
use crate::value::{value_to_op_requests, random_op_id, MapType};
use crate::{AutomergeFrontendError, Value};
use automerge_backend as amb;
use std::{collections::HashMap, rc::Rc};
use maplit::hashmap;

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

    pub(crate) fn to_diff_key(&self) -> amb::Key {
        match self {
            PathElement::Key(s) => amb::Key(s.clone()),
            PathElement::Index(i) => amb::Key(i.to_string()),
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

    pub(crate) fn first_key(&self) -> Option<amb::Key> {
        self.0.first().map(|o| o.to_diff_key())
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
        LocalChange {
            path,
            operation: LocalOperation::Set(value),
        }
    }

    pub fn delete(path: Path) -> LocalChange {
        LocalChange {
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

impl<'a, 'b> MutationTracker<'a, 'b> {
    pub fn new(change_context: &'a mut ChangeContext<'b>) -> MutationTracker<'a, 'b> {
        MutationTracker {
            change_context,
            ops: Vec::new(),
        }
    }

    pub fn ops(&self) -> Option<Vec<amb::OpRequest>> {
        if self.ops.len() > 0 {
            Some(self.ops.clone())
        } else {
            None
        }
    }

    fn parent_object<'c>(&'a self, path: &'c Path) -> Option<Rc<Object>> {
        self.value_for_path(&path.parent())
    }

    fn value_for_path<'c>(&'a self, path: &'c Path) -> Option<Rc<Object>> {
        let mut stack = path.clone().0;
        stack.pop();
        stack.reverse();
        let mut current_obj: Rc<Object> = self
            .change_context
            .value_for_object_id(&amb::ObjectID::Root)
            .unwrap();
        while let Some(next_elem) = stack.pop() {
            match (next_elem, &*current_obj) {
                (PathElement::Key(ref k), Object::Map(_, ref vals, _)) => {
                    if let Some(target) = vals.get(&amb::Key(k.clone())) {
                        current_obj = Rc::new(target.default_value().borrow().clone());
                    } else {
                        return None;
                    }
                }
                (PathElement::Index(i), Object::Sequence(_, vals, _)) => {
                    if let Some(Some(target)) = vals.get(i) {
                        current_obj = Rc::new(target.default_value().borrow().clone());
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current_obj)
    }

    /// Given `subdiff` which is intended to be applied at the object pointed
    /// to by `path`, construct a diff which starts at the root object and
    /// contains all the diffs between the root and the target path.
    ///
    /// For example, given an object structure like the following:
    ///
    /// ```json
    /// {
    ///     birds: [{name: magpie, flightless: false}]
    /// }
    /// ```
    ///
    /// And a diff like this, which is intended to be applied to the inner object:
    ///
    /// ```json
    /// {
    ///     {likes: "shiny things"}
    /// }
    /// ```
    ///
    /// We produce a diff like this:
    ///
    /// ```json
    /// {
    ///     objectId: <ROOT>,
    ///     type: 'object',
    ///     props: {
    ///         birds: {
    ///             <OPID>: {
    ///                 objectId: <birds list object ID>,
    ///                 type: 'list',
    ///                 props: {
    ///                     <OPID>: {
    ///                         0: {
    ///                             objectId: <magbie object ID>,
    ///                             type: 'object',
    ///                             props: {
    ///                                 <OPID>: {
    ///                                     likes: "shiny things"
    ///                                 }
    ///                             }
    ///                         }
    ///                     }
    ///                 }
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// The OPIDs at each step are autogenerated.
    fn diff_at_path<'c>(&'a self, path: &'c Path, subdiff: amb::DiffLink) -> Option<amb::Diff> {
        // This code duplicates a lot of logic from the `value_for_path` method.
        // This is because it needs to build up a set of intermediate
        // values to build diffs out of as it traverses the path

        // We'll use these once we've reached the end of the path to generate
        // the enclosing diffs
        let mut intermediates: Vec<(amb::ObjectID, amb::Key, amb::ObjType)> = Vec::new();

        // This is just the logic for reducing the path
        let mut stack = path.0.clone();
        // We don't need the final element as that's where we're applying this
        // diff (I think).
        stack.pop();
        stack.reverse();
        let mut current_obj: Rc<Object> = self
            .change_context
            .value_for_object_id(&amb::ObjectID::Root)
            .unwrap();
        while let Some(next_elem) = stack.pop() {
            match (&next_elem, &*current_obj) {
                (PathElement::Key(ref k), Object::Map(_, ref vals, _)) => {
                    if let Some(target) = vals.get(&amb::Key(k.clone())) {
                        current_obj = Rc::new(target.default_value().borrow().clone());
                        intermediates.push((current_obj.id().unwrap(), next_elem.to_diff_key(), current_obj.backend_type().unwrap())) //TODO fix unwraps
                    } else {
                        return None;
                    }
                }
                (PathElement::Index(i), Object::Sequence(_, vals, _)) => {
                    if let Some(Some(target)) = vals.get(*i) {
                        current_obj = Rc::new(target.default_value().borrow().clone());
                        intermediates.push((current_obj.id().unwrap(), next_elem.to_diff_key(), current_obj.backend_type().unwrap())) //TODO fix unwraps
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        };

        // Okay, we've followed the path and built the intermediate states we
        // need, now we can use them to create a set of diffs. We rfold
        // because we start with the smallest diff and keep enclosing it in 
        // larger ones
        let difflink = intermediates.into_iter().rfold(subdiff, |diff_so_far, (next_obj_id, next_key, next_obj_type)|{
            amb::DiffLink::Link(amb::Diff{
                object_id: next_obj_id,
                edits: None,
                obj_type: next_obj_type,
                props: Some(hashmap!{next_key => hashmap!{random_op_id() => diff_so_far}})
            })
        });
        Some(amb::Diff{
            object_id: amb::ObjectID::Root,
            edits: None,
            obj_type: amb::ObjType::Map,
            props: Some(hashmap!{path.first_key().unwrap() => hashmap!{random_op_id() => difflink}})
        })
    }

    /// If the `value` is a map, individually assign each k,v in it to a key in
    /// the root object
    fn wrap_root_assignment(&mut self, value: &Value) -> Result<(), AutomergeFrontendError> {
        match value {
            Value::Map(kvs, MapType::Map) => {
                for (k, v) in kvs.into_iter() {
                    self.add_change(LocalChange::set(Path::root().key(k), v.clone()))?;
                };
                Ok(())
            }
            _ => Err(AutomergeFrontendError::InvalidChangeRequest)
        }
    }

}

impl<'a, 'b> MutableDocument for MutationTracker<'a, 'b> {
    fn value_at_path(&self, path: &Path) -> Option<Value> {
        self.value_for_path(path).map(|o| o.value())
    }

    fn add_change(&mut self, change: LocalChange) -> Result<(), AutomergeFrontendError> {
        match &change.operation {
            LocalOperation::Set(value) => {
                if change.path == Path::root() {
                    return self.wrap_root_assignment(value)
                }
                if let Some(oid) = self.parent_object(&change.path).and_then(|o| o.id()) {
                    // We are not inserting unless this path references an
                    // existing index in a sequence
                    let insert = self
                        .change_context
                        .value_for_object_id(&oid)
                        .map(|o| match o.as_ref() {
                                Object::Sequence(_, vals, _) => change
                                    .path
                                    .name()
                                    .map(|elem| match elem {
                                        PathElement::Index(i) => vals.len() > *i,
                                        _ => false,
                                    })
                                    .unwrap_or(false),
                                _ => false,
                            }
                        )
                        .unwrap_or(false);
                    let (ops, difflink) = value_to_op_requests(
                        oid.to_string(),
                        change
                            .path
                            .name()
                            .ok_or_else(|| {
                                AutomergeFrontendError::NoSuchPathError(change.path.clone())
                            })?
                            .clone(),
                        &value,
                        insert,
                    );
                    let diff = self.diff_at_path(&change.path, difflink).unwrap(); //TODO fix unwrap
                    self.change_context.apply_diff(&diff)?;
                    self.ops.extend(ops.into_iter());
                    Ok(())
                } else {
                    Err(AutomergeFrontendError::NoSuchPathError(change.path))
                }
            }
            LocalOperation::Delete => panic!("delete not implemented"),
        }
    }

}


pub(crate) fn resolve_path(path: &Path, objects: &HashMap<amb::ObjectID, Rc<Object>>) -> Option<Rc<Object>> {
    let mut stack = path.clone().0;
    stack.reverse();
    let mut current_obj: Rc<Object> = objects.get(&amb::ObjectID::Root).unwrap().clone();
    while let Some(next_elem) = stack.pop() {
        match (next_elem, &*current_obj) {
            (PathElement::Key(ref k), Object::Map(_, ref vals, _)) => {
                if let Some(target) = vals.get(&amb::Key(k.clone())) {
                    current_obj = Rc::new(target.default_value().borrow().clone());
                } else {
                    return None;
                }
            }
            (PathElement::Index(i), Object::Sequence(_, vals, _)) => {
                if let Some(Some(target)) = vals.get(i) {
                    current_obj = Rc::new(target.default_value().borrow().clone());
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(current_obj)
}
