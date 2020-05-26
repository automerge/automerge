use crate::change_context::ChangeContext;
use crate::object::Object;
use crate::value::{random_op_id, value_to_op_requests};
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
    Increment(i64),
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
    ops: Vec<amp::Op>,
}

impl<'a, 'b> MutationTracker<'a, 'b> {
    pub fn new(change_context: &'a mut ChangeContext<'b>) -> MutationTracker<'a, 'b> {
        MutationTracker {
            change_context,
            ops: Vec::new(),
        }
    }

    pub fn ops(&self) -> Option<Vec<amp::Op>> {
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
            Map(amp::ObjectID, amp::MapType, String, Option<amp::OpID>),
            Seq(amp::ObjectID, amp::SequenceType, usize, Option<amp::OpID>),
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
                            *map_type,
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
                                *map_type,
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
                            *seq_type,
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
                                *seq_type,
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
                    object_id: oid,
                    obj_type: map_type,
                    props: hashmap! {k => hashmap!{opid.unwrap_or_else(random_op_id) => diff_so_far}},
                }),
                Intermediate::Seq(oid, seq_type, index, opid) => amp::Diff::Seq(amp::SeqDiff {
                    object_id: oid,
                    obj_type: seq_type,
                    edits: Vec::new(),
                    props: hashmap! {index => hashmap!{opid.unwrap_or_else(random_op_id) => diff_so_far}},
                }),
            });
        Some(diff)
    }

    /// If the `value` is a map, individually assign each k,v in it to a key in
    /// the root object
    fn wrap_root_assignment(&mut self, value: &Value) -> Result<(), AutomergeFrontendError> {
        match value {
            Value::Map(kvs, amp::MapType::Map) => {
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
                if let Some(o) = self.value_for_path(&change.path) {
                    if let Object::Primitive(amp::Value::Counter(_)) = &*o {
                        return Err(AutomergeFrontendError::CannotOverwriteCounter);
                    }
                };
                if let Some(oid) = self.parent_object(&change.path).and_then(|o| o.id()) {
                    let (ops, difflink) = value_to_op_requests(
                        oid,
                        change
                            .path
                            .name()
                            .ok_or_else(|| {
                                AutomergeFrontendError::NoSuchPathError(change.path.clone())
                            })?
                            .clone(),
                        &value,
                        false,
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
                    let op = amp::Op {
                        action: amp::OpType::Del,
                        // This unwrap is fine because we know the parent
                        // is a container
                        obj: parent_obj.id().unwrap().to_string(),
                        // Is this unwrap fine? I think so as we intercept assignments
                        // to the root path at the top of this function so we know
                        // this path has at least one element
                        key: change.path.name().unwrap().to_request_key(),
                        child: None,
                        insert: false,
                        value: None,
                        datatype: None,
                    };
                    let diff = match &*parent_obj {
                        Object::Map(oid, _, map_type) => amp::Diff::Map(amp::MapDiff {
                            object_id: oid.clone(),
                            obj_type: *map_type,
                            props: hashmap! {change.path.name().unwrap().to_string() => HashMap::new()},
                        }),
                        Object::Sequence(oid, _, seq_type) => {
                            if let Some(PathElement::Index(i)) = change.path.name() {
                                amp::Diff::Seq(amp::SeqDiff {
                                    object_id: oid.clone(),
                                    obj_type: *seq_type,
                                    edits: vec![amp::DiffEdit::Remove { index: *i }],
                                    props: HashMap::new(),
                                })
                            } else {
                                return Err(AutomergeFrontendError::NoSuchPathError(change.path));
                            }
                        }
                        Object::Primitive(..) => panic!("parent object was primitive"),
                    };
                    // TODO fix unwrap
                    let diff = self.diff_at_path(&change.path.parent(), diff).unwrap();
                    self.ops.push(op);
                    self.change_context.apply_diff(&diff)?;
                    Ok(())
                } else {
                    Err(AutomergeFrontendError::NoSuchPathError(change.path))
                }
            }
            LocalOperation::Increment(by) => {
                if let Some(val) = self.value_for_path(&change.path) {
                    let current_val = match &*val {
                        Object::Primitive(amp::Value::Counter(i)) => i,
                        _ => return Err(AutomergeFrontendError::PathIsNotCounter),
                    };
                    // Unwrap is fine as we know the parent object exists from the above
                    let parent_obj = self.value_for_path(&change.path.parent()).unwrap();
                    let op = amp::Op {
                        action: amp::OpType::Inc,
                        // This unwrap is fine because we know the parent
                        // is a container
                        obj: parent_obj.id().unwrap().to_string(),
                        key: change.path.name().unwrap().to_request_key(),
                        child: None,
                        insert: false,
                        value: Some(amp::Value::Int(*by)),
                        datatype: Some(amp::DataType::Counter),
                    };
                    let diff = amp::Diff::Value(amp::Value::Counter(current_val + by));
                    let diff = self.diff_at_path(&change.path, diff).unwrap();
                    self.ops.push(op);
                    self.change_context.apply_diff(&diff)?;
                    Ok(())
                } else {
                    Err(AutomergeFrontendError::NoSuchPathError(change.path))
                }
            }
            LocalOperation::Insert(value) => {
                let index = match change.path.name() {
                    Some(PathElement::Index(i)) => i,
                    // TODO make this error more descriptive, probably need
                    // a specific branch for invalid insert paths
                    _ => return Err(AutomergeFrontendError::InvalidChangeRequest),
                };
                if let Some(parent) = self.value_for_path(&change.path.parent()) {
                    match &*parent {
                        // TODO make this error more descriptive
                        Object::Map(..) | Object::Primitive(..) => {
                            Err(AutomergeFrontendError::InvalidChangeRequest)
                        }
                        Object::Sequence(oid, vals, seq_type) => {
                            if *index > vals.len() {
                                return Err(AutomergeFrontendError::InvalidChangeRequest);
                            }
                            let (ops, diff) = value_to_op_requests(
                                oid.clone(),
                                change
                                    .path
                                    .name()
                                    .ok_or_else(|| {
                                        AutomergeFrontendError::NoSuchPathError(change.path.clone())
                                    })?
                                    .clone(),
                                &value,
                                true,
                            );
                            let seqdiff = amp::Diff::Seq(amp::SeqDiff {
                                object_id: oid.clone(),
                                obj_type: *seq_type,
                                edits: vec![amp::DiffEdit::Insert { index: *index }],
                                props: hashmap! {
                                    *index => hashmap!{
                                        random_op_id() => diff,
                                    }
                                },
                            });
                            let diff = self.diff_at_path(&change.path.parent(), seqdiff).unwrap(); //TODO fix unwrap
                            self.ops.extend(ops);
                            self.change_context.apply_diff(&diff)?;
                            Ok(())
                        }
                    }
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
