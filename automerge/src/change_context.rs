/// This module handles creating changes. Most of the machinery here is related
/// to resolving paths from ChangeRequests, and generating operations to create
/// and modify data in the op set.
use crate::change_request::{ChangeRequest, ListIndex, Path, PathElement};
use crate::error::InvalidChangeRequest;
use automerge_backend::list_ops_in_order;
use automerge_backend::ActorHistories;
use automerge_backend::ObjectHistory;
use automerge_backend::ObjectStore;
use automerge_backend::OperationWithMetadata;
use automerge_backend::Value;
use automerge_backend::{
    ActorID, Change, Clock, ElementID, Key, ObjectID, Operation, PrimitiveValue,
};
use std::convert::TryInto;

#[derive(Clone, Debug)]
enum ResolvedPathElement {
    Map(ObjectID),
    List(ObjectID, u32),
    Key(Key),
    Index(ElementID),
    Value(PrimitiveValue),
    MissingKey(Key),
}

/// Represents a resolved path
#[derive(Debug, Clone)]
struct ResolvedPath(Vec<ResolvedPathElement>);

impl ResolvedPath {
    fn new(elements: Vec<ResolvedPathElement>) -> ResolvedPath {
        ResolvedPath(elements)
    }

    fn as_set_target(&self) -> Option<SetTarget> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Value(_)] => Some(SetTarget{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::MissingKey(_)] => Some(SetTarget{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::Value(_)] => Some(SetTarget{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                }),
                _ => None
            }
        })
    }

    fn as_move_source(&self) -> Option<MoveSource> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Map(c)] => Some(MoveSource::Reference{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                    contained_object_id: c.clone()
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::List(l, _)] => Some(MoveSource::Reference{
                    containing_object_id: o.clone(),
                    key: k.clone(),
                    contained_object_id: l.clone()
                }),
                [ResolvedPathElement::Map(o), ResolvedPathElement::Key(k), ResolvedPathElement::Value(v)] => Some(MoveSource::Value{
                    containing_object_id: o.clone(),
                    value: v.clone(),
                    key: k.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::Map(m)] => Some(MoveSource::Reference{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                    contained_object_id: m.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(elem_id), ResolvedPathElement::List(l2, _)] => Some(MoveSource::Reference{
                    containing_object_id: l.clone(),
                    key: elem_id.as_key(),
                    contained_object_id: l2.clone(),
                }),
                [ResolvedPathElement::List(l, _), ResolvedPathElement::Index(i), ResolvedPathElement::Value(v)] => Some(MoveSource::Value{
                    containing_object_id: l.clone(),
                    value: v.clone(),
                    key: i.as_key(),
                }),
                _ => None
            }
        })
    }

    fn as_insert_after_target(&self) -> Option<InsertAfterTarget> {
        self.last_n(3).and_then(|last_three| {
            match &last_three[..] {
                [ResolvedPathElement::List(l, m), ResolvedPathElement::Index(e), ResolvedPathElement::Value(_)] => Some(InsertAfterTarget{
                    list_id: l.clone(),
                    element_id: e.clone(),
                    max_elem: *m,
                }),
                [_, ResolvedPathElement::List(l, m), ResolvedPathElement::Index(e)] => Some(InsertAfterTarget{
                    list_id: l.clone(),
                    element_id: e.clone(),
                    max_elem: *m,
                }),
                _ => None,
            }
        })
    }

    fn last_n(&self, n: usize) -> Option<Box<[ResolvedPathElement]>> {
        if self.0.len() < n {
            None
        } else {
            Some(
                self.0
                    .iter()
                    .skip(self.0.len() - n)
                    .cloned()
                    .collect::<Vec<ResolvedPathElement>>()
                    .into_boxed_slice(),
            )
        }
    }
}

/// Represents the target of a "set" change request.
#[derive(Debug, Clone)]
struct SetTarget {
    containing_object_id: ObjectID,
    key: Key,
}

/// Represents a path which can be moved.
enum MoveSource {
    Reference {
        containing_object_id: ObjectID,
        key: Key,
        contained_object_id: ObjectID,
    },
    Value {
        containing_object_id: ObjectID,
        key: Key,
        value: PrimitiveValue,
    },
}

impl MoveSource {
    fn delete_op(&self) -> Operation {
        match self {
            MoveSource::Reference {
                containing_object_id,
                key,
                ..
            }
            | MoveSource::Value {
                containing_object_id,
                key,
                ..
            } => Operation::Delete {
                object_id: containing_object_id.clone(),
                key: key.clone(),
            },
        }
    }
}

#[derive(Debug)]
struct InsertAfterTarget {
    list_id: ObjectID,
    element_id: ElementID,
    max_elem: u32,
}

/// The ChangeContext is responsible for taking the current state of the opset
/// (which is an ObjectStore, and a clock), and an actor ID and generating a
/// new change for a given set of ChangeRequests. The ObjectStore which the
/// ChangeContext manages is a copy of the OpSet's ObjectStore, this is because
/// in order to process ChangeRequests the ChangeContext needs to update the
/// ObjectStore.
///
/// For example, if we have several ChangeRequests which are inserting elements
/// into a list, one after another, then we need to know the element IDs of the
/// newly inserted elements to generate the correct operations.
pub struct ChangeContext<'a> {
    object_store: ObjectStore,
    actor_id: ActorID,
    actor_histories: &'a ActorHistories,
    clock: Clock,
}

impl<'a> ChangeContext<'a> {
    pub fn new(
        object_store: &ObjectStore,
        actor_id: ActorID,
        actor_histories: &'a ActorHistories,
        clock: Clock,
    ) -> ChangeContext<'a> {
        ChangeContext {
            object_store: object_store.clone(),
            actor_histories,
            actor_id,
            clock,
        }
    }

    fn get_operations_for_object_id(&self, object_id: &ObjectID) -> Option<&ObjectHistory> {
        self.object_store.history_for_object_id(object_id)
    }

    pub(crate) fn create_change<I>(
        &mut self,
        requests: I,
        message: Option<String>,
    ) -> Result<Change, InvalidChangeRequest>
    where
        I: IntoIterator<Item = ChangeRequest>,
    {
        let ops_with_errors: Vec<Result<Vec<Operation>, InvalidChangeRequest>> = requests
            .into_iter()
            .map(|request| {
                let ops = match request {
                    ChangeRequest::Set {
                        ref path,
                        ref value,
                    } => self.create_set_operations(&self.actor_id, path, value),
                    ChangeRequest::Delete { ref path } => {
                        self.create_delete_operation(path).map(|o| vec![o])
                    }
                    ChangeRequest::Increment {
                        ref path,
                        ref value,
                    } => self
                        .create_increment_operation(path, value.clone())
                        .map(|o| vec![o]),
                    ChangeRequest::Move { ref from, ref to } => {
                        self.create_move_operations(from, to)
                    }
                    ChangeRequest::InsertAfter {
                        ref path,
                        ref value,
                    } => self.create_insert_operation(&self.actor_id, path, value),
                };
                // We have to apply each operation to the object store so that
                // operations which reference earlier operations within this
                // change set have the correct data to refer to.
                ops.iter().for_each(|inner_ops| {
                    inner_ops.iter().for_each(|op| {
                        let op_with_meta = OperationWithMetadata {
                            sequence: self.clock.seq_for(&self.actor_id) + 1,
                            actor_id: self.actor_id.clone(),
                            operation: op.clone(),
                        };
                        self.object_store
                            .apply_operation(self.actor_histories, op_with_meta)
                            .unwrap();
                    });
                });
                ops
            })
            .collect();
        let nested_ops = ops_with_errors
            .into_iter()
            .collect::<Result<Vec<Vec<Operation>>, InvalidChangeRequest>>()?;
        let ops = nested_ops.into_iter().flatten().collect::<Vec<Operation>>();
        let dependencies = self.clock.clone();
        let seq = self.clock.seq_for(&self.actor_id) + 1;
        let change = Change {
            actor_id: self.actor_id.clone(),
            operations: ops,
            seq,
            message,
            dependencies,
        };
        Ok(change)
    }

    pub(crate) fn create_set_operations(
        &self,
        actor_id: &ActorID,
        path: &Path,
        value: &Value,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        // If we're setting a map as the root object we actually want to set
        // each key of the map to the corresponding key in the root object
        if let Value::Map(kvs) = value.clone() {
            if path.is_root() {
                let mut ops = Vec::new();
                for (key, value) in kvs.into_iter() {
                    let key_path = path.key(key);
                    let mut this_key_ops =
                        self.create_set_operations(actor_id, &key_path, &value)?;
                    ops.append(&mut this_key_ops)
                }
                return Ok(ops);
            }
        };
        self.resolve_path(path)
            .and_then(|r| r.as_set_target())
            .map(|path_resolution| match value {
                Value::Map { .. } | Value::List { .. } => {
                    let (new_object_id, mut create_ops) = value_to_ops(actor_id, &value);
                    let link_op = Operation::Link {
                        object_id: path_resolution.containing_object_id.clone(),
                        key: path_resolution.key.clone(),
                        value: new_object_id,
                    };
                    create_ops.push(link_op);
                    create_ops
                }
                Value::Str { .. } | Value::Number { .. } | Value::Boolean { .. } | Value::Null => {
                    vec![create_prim(
                        path_resolution.containing_object_id.clone(),
                        path_resolution.key,
                        &value,
                    )]
                }
            })
            .ok_or(InvalidChangeRequest(format!("Missing path: {:?}", path)))
    }

    pub(crate) fn create_move_operations(
        &self,
        from: &Path,
        to: &Path,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        let resolved_from = self.resolve_path(from).ok_or(InvalidChangeRequest(format!(
            "Missing from path: {:?}",
            from
        )))?;
        let resolved_to = self
            .resolve_path(to)
            .ok_or(InvalidChangeRequest(format!("Missing to path: {:?}", to)))?;

        let move_source = resolved_from
            .as_move_source()
            .ok_or(InvalidChangeRequest(format!(
                "Invalid move source path: {:?}",
                from
            )))?;
        let target = resolved_to
            .as_set_target()
            .ok_or(InvalidChangeRequest(format!("Invalid to path: {:?}", to)))?;

        let delete_op = move_source.delete_op();

        let insert_op = match (move_source, target) {
            (
                MoveSource::Value { value: v, .. },
                SetTarget {
                    containing_object_id,
                    key,
                },
            ) => Operation::Set {
                object_id: containing_object_id,
                key,
                value: v,
                datatype: None,
            },
            (
                MoveSource::Reference {
                    contained_object_id,
                    ..
                },
                SetTarget {
                    containing_object_id: target_container_id,
                    key: target_key,
                },
            ) => Operation::Link {
                object_id: target_container_id,
                key: target_key,
                value: contained_object_id,
            },
        };

        Ok(vec![delete_op, insert_op])
    }

    pub(crate) fn create_delete_operation(
        &self,
        path: &Path,
    ) -> Result<Operation, InvalidChangeRequest> {
        self.resolve_path(path)
            .and_then(|r| r.as_move_source())
            .map(|source| source.delete_op())
            .ok_or(InvalidChangeRequest(format!(
                "Invalid delete path: {:?}",
                path
            )))
    }

    pub(crate) fn create_increment_operation(
        &self,
        _path: &Path,
        _value: f64,
    ) -> Result<Operation, InvalidChangeRequest> {
        Err(InvalidChangeRequest(
            "create_increment_operation not implemented".to_string(),
        ))
    }

    pub(crate) fn create_insert_operation(
        &self,
        actor_id: &ActorID,
        after: &Path,
        value: &Value,
    ) -> Result<Vec<Operation>, InvalidChangeRequest> {
        let after_target = self
            .resolve_path(after)
            .and_then(|p| p.as_insert_after_target())
            .ok_or(InvalidChangeRequest(format!(
                "Invalid insert after path: {:?}",
                after
            )))?;
        let next_elem_id =
            ElementID::SpecificElementID(actor_id.clone(), after_target.max_elem + 1);
        let insert_op = Operation::Insert {
            list_id: after_target.list_id.clone(),
            key: after_target.element_id,
            elem: after_target.max_elem + 1,
        };
        let mut ops = vec![insert_op];
        match value {
            Value::Map { .. } | Value::List { .. } => {
                let (new_object_id, create_ops) = value_to_ops(actor_id, &value);
                ops.extend(create_ops);
                let link_op = Operation::Link {
                    object_id: after_target.list_id.clone(),
                    key: next_elem_id.as_key(),
                    value: new_object_id,
                };
                ops.push(link_op);
            }
            Value::Str { .. } | Value::Number { .. } | Value::Boolean { .. } | Value::Null => {
                ops.push(create_prim(
                    after_target.list_id.clone(),
                    next_elem_id.as_key(),
                    &value,
                ));
            }
        };
        Ok(ops)
    }

    fn resolve_path(&self, path: &Path) -> Option<ResolvedPath> {
        let mut resolved_elements: Vec<ResolvedPathElement> = Vec::new();
        let mut containing_object_id = ObjectID::Root;
        for next_elem in path {
            match resolved_elements.last() {
                Some(ResolvedPathElement::MissingKey(_)) => return None,
                Some(ResolvedPathElement::Index(ElementID::Head)) => return None,
                _ => {}
            }
            match next_elem {
                PathElement::Root => {
                    resolved_elements.push(ResolvedPathElement::Map(ObjectID::Root))
                }
                PathElement::Key(key) => {
                    resolved_elements.push(ResolvedPathElement::Key(Key(key.to_string())));
                    let op = self
                        .get_operations_for_object_id(&containing_object_id)
                        .and_then(|history| match history {
                            ObjectHistory::Map { operations_by_key } => Some(operations_by_key),
                            ObjectHistory::List { .. } => None,
                        })
                        .and_then(|kvs| kvs.get(key))
                        .and_then(|cops| cops.active_op())
                        .map(|o| o.operation.clone());
                    match op {
                        Some(Operation::Set { value, .. }) => {
                            resolved_elements.push(ResolvedPathElement::Value(value))
                        }
                        Some(Operation::Link { value, .. }) => {
                            match self.get_operations_for_object_id(&value) {
                                None => return None,
                                Some(ObjectHistory::Map { .. }) => {
                                    resolved_elements.push(ResolvedPathElement::Map(value.clone()));
                                    containing_object_id = value.clone()
                                }
                                Some(ObjectHistory::List { max_elem, .. }) => {
                                    resolved_elements
                                        .push(ResolvedPathElement::List(value.clone(), *max_elem));
                                    containing_object_id = value.clone()
                                }
                            }
                        }
                        None => resolved_elements
                            .push(ResolvedPathElement::MissingKey(Key(key.to_string()))),
                        _ => return None,
                    }
                }
                PathElement::Index(index) => match index {
                    ListIndex::Head => {
                        match self.get_operations_for_object_id(&containing_object_id) {
                            Some(ObjectHistory::List { .. }) => {
                                resolved_elements.push(ResolvedPathElement::Index(ElementID::Head))
                            }
                            _ => return None,
                        };
                    }
                    ListIndex::Index(i) => {
                        let op = self
                            .get_operations_for_object_id(&containing_object_id)
                            .and_then(|history| match history {
                                ObjectHistory::List {
                                    operations_by_elemid,
                                    following,
                                    ..
                                } => list_ops_in_order(operations_by_elemid, following).ok(),
                                ObjectHistory::Map { .. } => None,
                            })
                            .and_then(|ops| ops.get(*i).map(|o| o.clone()))
                            .and_then(|(element_id, cops)| {
                                cops.active_op().map(|o| (element_id, o.operation.clone()))
                            });
                        match op {
                            Some((elem_id, Operation::Set { value, .. })) => {
                                resolved_elements.push(ResolvedPathElement::Index(elem_id));
                                resolved_elements.push(ResolvedPathElement::Value(value));
                            }
                            Some((_, Operation::Link { value, .. })) => {
                                match self.get_operations_for_object_id(&value) {
                                    None => return None,
                                    Some(ObjectHistory::Map { .. }) => {
                                        resolved_elements
                                            .push(ResolvedPathElement::Map(value.clone()));
                                        containing_object_id = value
                                    }
                                    Some(ObjectHistory::List { max_elem, .. }) => {
                                        resolved_elements.push(ResolvedPathElement::List(
                                            value.clone(),
                                            *max_elem,
                                        ));
                                        containing_object_id = value
                                    }
                                }
                            }
                            _ => return None,
                        }
                    }
                },
            }
        }
        Some(ResolvedPath::new(resolved_elements))
    }
}

fn value_to_ops(actor_id: &ActorID, v: &Value) -> (ObjectID, Vec<Operation>) {
    match v {
        Value::List(vs) => {
            let list_id = ObjectID::ID(uuid::Uuid::new_v4().to_string());
            let mut ops = vec![Operation::MakeList {
                object_id: list_id.clone(),
            }];
            let mut elem_ops: Vec<Operation> = vs
                .into_iter()
                .enumerate()
                .map(|(index, elem_value)| {
                    let elem: u32 = (index + 1).try_into().unwrap();
                    let previous_elemid = match index {
                        0 => ElementID::Head,
                        _ => ElementID::SpecificElementID(actor_id.clone(), elem - 1),
                    };
                    let insert_op = Operation::Insert {
                        list_id: list_id.clone(),
                        elem,
                        key: previous_elemid,
                    };
                    let elem_id = ElementID::SpecificElementID(actor_id.clone(), elem);
                    let mut elem_value_ops: Vec<Operation> = match elem_value {
                        Value::Boolean { .. }
                        | Value::Str { .. }
                        | Value::Number { .. }
                        | Value::Null { .. } => {
                            vec![create_prim(list_id.clone(), elem_id.as_key(), elem_value)]
                        }
                        Value::Map { .. } | Value::List { .. } => {
                            let (linked_object_id, mut value_ops) =
                                value_to_ops(actor_id, elem_value);
                            value_ops.push(Operation::Link {
                                object_id: list_id.clone(),
                                key: elem_id.as_key(),
                                value: linked_object_id,
                            });
                            value_ops
                        }
                    };
                    let mut result = Vec::new();
                    result.push(insert_op);
                    result.append(&mut elem_value_ops);
                    result
                })
                .flatten()
                .collect();
            ops.append(&mut elem_ops);
            (list_id, ops)
        }
        Value::Map(kvs) => {
            let object_id = ObjectID::ID(uuid::Uuid::new_v4().to_string());
            let mut ops = vec![Operation::MakeMap {
                object_id: object_id.clone(),
            }];
            let mut key_ops: Vec<Operation> = kvs
                .iter()
                .map(|(k, v)| match v {
                    Value::Boolean { .. }
                    | Value::Str { .. }
                    | Value::Number { .. }
                    | Value::Null { .. } => vec![create_prim(object_id.clone(), Key(k.clone()), v)],
                    Value::Map { .. } | Value::List { .. } => {
                        let (linked_object_id, mut value_ops) = value_to_ops(actor_id, v);
                        value_ops.push(Operation::Link {
                            object_id: object_id.clone(),
                            key: Key(k.clone()),
                            value: linked_object_id,
                        });
                        value_ops
                    }
                })
                .flatten()
                .collect();
            ops.append(&mut key_ops);
            (object_id, ops)
        }
        _ => panic!("Only a map or list can be the top level object in value_to_ops".to_string()),
    }
}

fn create_prim(object_id: ObjectID, key: Key, value: &Value) -> Operation {
    let prim_value = match value {
        Value::Number(n) => PrimitiveValue::Number(*n),
        Value::Boolean(b) => PrimitiveValue::Boolean(*b),
        Value::Str(s) => PrimitiveValue::Str(s.to_string()),
        Value::Null => PrimitiveValue::Null,
        _ => panic!("Non primitive value passed to create_prim"),
    };
    Operation::Set {
        object_id,
        key,
        value: prim_value,
        datatype: None,
    }
}
