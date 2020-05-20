use crate::change_context::ChangeContext;
use crate::object::Object;
use crate::value::{random_op_id, value_to_op_requests, MapType, SequenceType};
use crate::{AutomergeFrontendError, Value};
use automerge_protocol as amp;
use maplit::hashmap;
use std::{collections::HashMap, fmt, rc::Rc};

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
    pub(crate) fn to_request_key(&self) -> amp::RequestKey {
        match self {
            PathElement::Key(s) => amp::RequestKey::Str(s.into()),
            PathElement::Index(i) => amp::RequestKey::Num(*i as u64),
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
        if self.0.is_empty() {
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

impl fmt::Display for PathElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathElement::Key(k) => write!(f, "{}", k),
            PathElement::Index(i) => write!(f, "{}", i),
        }
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
    ops: Vec<amp::OpRequest>,
}

impl<'a, 'b> MutationTracker<'a, 'b> {
    pub fn new(change_context: &'a mut ChangeContext<'b>) -> MutationTracker<'a, 'b> {
        MutationTracker {
            change_context,
            ops: Vec::new(),
        }
    }

    pub fn ops(&self) -> Option<Vec<amp::OpRequest>> {
        if !self.ops.is_empty() {
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
        stack.reverse();
        let mut current_obj: Rc<Object> = self
            .change_context
            .value_for_object_id(&amp::ObjectID::Root)
            .unwrap();
        while let Some(next_elem) = stack.pop() {
            match (next_elem, &*current_obj) {
                (PathElement::Key(ref k), Object::Map(_, ref vals, _)) => {
                    if let Some(target) = vals.get(k) {
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
    fn diff_at_path<'c>(&'a self, path: &'c Path, subdiff: amp::Diff) -> Option<amp::Diff> {
        // This code duplicates a lot of logic from the `value_for_path` method.
        // This is because it needs to build up a set of intermediate
        // values to build diffs out of as it traverses the path

        // We'll use these once we've reached the end of the path to generate
        // the enclosing diffs
        #[derive(Debug)]
        enum Intermediate {
            Map(amp::ObjectID, MapType, String, Option<amp::OpID>),
            Seq(amp::ObjectID, SequenceType, usize, Option<amp::OpID>),
        };
        let mut intermediates: Vec<Intermediate> = Vec::new();

        // This is just the logic for reducing the path
        let mut stack = path.0.clone();
        stack.reverse();
        let mut current_obj: Rc<Object> = self
            .change_context
            .value_for_object_id(&amp::ObjectID::Root)
            .unwrap();
        while let Some(next_elem) = stack.pop() {
            match (&next_elem, &*current_obj) {
                (PathElement::Key(ref k), Object::Map(oid, ref vals, map_type)) => {
                    if let Some(target) = vals.get(k) {
                        intermediates.push(Intermediate::Map(
                            oid.clone(),
                            map_type.clone(),
                            k.clone(),
                            current_obj.default_op_id_for_key(amp::RequestKey::Str(k.clone())),
                        ));
                        current_obj = Rc::new(target.default_value().borrow().clone());
                    } else {
                        // This key does not exist, but it's the last element in
                        // the path, so we can still make a diff as we're
                        // creating this key
                        if stack.is_empty() {
                            intermediates.push(Intermediate::Map(
                                oid.clone(),
                                map_type.clone(),
                                k.clone(),
                                current_obj.default_op_id_for_key(amp::RequestKey::Str(k.clone())),
                            ))
                        } else {
                            return None;
                        }
                    }
                }
                (PathElement::Index(i), Object::Sequence(oid, vals, seq_type)) => {
                    if let Some(Some(target)) = vals.get(*i) {
                        intermediates.push(Intermediate::Seq(
                            oid.clone(),
                            seq_type.clone(),
                            *i,
                            current_obj.default_op_id_for_key(amp::RequestKey::Num(*i as u64)),
                        ));
                        current_obj = Rc::new(target.default_value().borrow().clone());
                    } else {
                        // This index does not exist, but it's the last element
                        // in the path so we can create a diff for it as we're
                        // setting this index
                        // TODO should we check if this is an `insert` subdiff
                        // first?
                        if stack.is_empty() {
                            intermediates.push(Intermediate::Seq(
                                oid.clone(),
                                seq_type.clone(),
                                *i,
                                current_obj.default_op_id_for_key(amp::RequestKey::Num(*i as u64)),
                            ))
                        } else {
                            return None;
                        }
                    }
                }
                _ => return None,
            }
        }

        // Okay, we've followed the path and built the intermediate states we
        // need, now we can use them to create a set of diffs. We rfold
        // because we start with the smallest diff and keep enclosing it in
        // larger ones
        let diff = intermediates
            .into_iter()
            .rfold(subdiff, |diff_so_far, intermediate| match intermediate {
                Intermediate::Map(oid, map_type, k, opid) => amp::Diff::Map(amp::MapDiff {
                    object_id: oid.to_string(),
                    obj_type: map_type.to_obj_type(),
                    props: hashmap! {k => hashmap!{opid.unwrap_or_else(random_op_id).to_string() => diff_so_far}},
                }),
                Intermediate::Seq(oid, seq_type, index, opid) => amp::Diff::Seq(amp::SeqDiff {
                    object_id: oid.to_string(),
                    obj_type: seq_type.to_obj_type(),
                    edits: Vec::new(),
                    props: hashmap! {index => hashmap!{opid.unwrap_or_else(random_op_id).to_string() => diff_so_far}},
                }),
            });
        Some(diff)
    }

    /// If the `value` is a map, individually assign each k,v in it to a key in
    /// the root object
    fn wrap_root_assignment(&mut self, value: &Value) -> Result<(), AutomergeFrontendError> {
        match value {
            Value::Map(kvs, MapType::Map) => {
                for (k, v) in kvs.iter() {
                    self.add_change(LocalChange::set(Path::root().key(k), v.clone()))?;
                }
                Ok(())
            }
            _ => Err(AutomergeFrontendError::InvalidChangeRequest),
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
                    return self.wrap_root_assignment(value);
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
                        })
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
            LocalOperation::Delete => {
                if self.value_for_path(&change.path).is_some() {
                    // Unwrap is fine as we know the parent object exists from the above
                    let parent_obj = self.value_for_path(&change.path.parent()).unwrap();
                    let op = amp::OpRequest {
                        action: amp::ReqOpType::Del,
                        // This unwrap is fine because we know the parent
                        // is a container
                        obj: parent_obj.id().unwrap().to_string(),
                        key: change.path.name().unwrap().to_request_key(),
                        child: None,
                        insert: false,
                        value: None,
                        datatype: None,
                    };
                    let diff = match &*parent_obj {
                        Object::Map(oid, _, map_type) => amp::Diff::Map(amp::MapDiff {
                            object_id: oid.to_string(),
                            obj_type: map_type.to_obj_type(),
                            props: hashmap! {change.path.name().unwrap().to_string() => HashMap::new()},
                        }),
                        Object::Sequence(oid, _, seq_type) => {
                            if let Some(PathElement::Index(i)) = change.path.name() {
                                amp::Diff::Seq(amp::SeqDiff {
                                    object_id: oid.to_string(),
                                    obj_type: seq_type.to_obj_type(),
                                    edits: vec![amp::DiffEdit::Remove { index: *i }],
                                    props: hashmap! {*i => HashMap::new()},
                                })
                            } else {
                                return Err(AutomergeFrontendError::NoSuchPathError(change.path));
                            }
                        }
                        Object::Primitive(..) => panic!("parent object was primitive"),
                    };
                    self.ops.push(op);
                    self.change_context.apply_diff(&diff)?;
                    Ok(())
                } else {
                    Err(AutomergeFrontendError::NoSuchPathError(change.path))
                }
            }
        }
    }
}

pub(crate) fn resolve_path(
    path: &Path,
    objects: &HashMap<amp::ObjectID, Rc<Object>>,
) -> Option<Rc<Object>> {
    let mut stack = path.clone().0;
    stack.reverse();
    let mut current_obj: Rc<Object> = objects.get(&amp::ObjectID::Root).unwrap().clone();
    while let Some(next_elem) = stack.pop() {
        match (next_elem, &*current_obj) {
            (PathElement::Key(ref k), Object::Map(_, ref vals, _)) => {
                if let Some(target) = vals.get(k) {
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
