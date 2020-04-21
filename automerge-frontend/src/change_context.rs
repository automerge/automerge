use automerge_backend::{
    DataType, Diff, Key, ObjType, ObjectID, OpID, OpRequest, PrimitiveValue, ReqOpType,
    DiffLink, DiffEdit
};
use uuid;
//use crate::AutomergeFrontendError;
use crate::{AutomergeFrontendError, MapType, PathElement, SequenceType, Value};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use std::str::FromStr;

pub(crate) fn value_to_op_requests(
    parent_object: String,
    key: PathElement,
    v: &Value,
    insert: bool,
) -> Vec<OpRequest> {
    match v {
        Value::Sequence(vs, seq_type) => {
            let make_action = match seq_type {
                SequenceType::List => ReqOpType::MakeList,
                SequenceType::Text => ReqOpType::MakeText,
            };
            let list_id = new_object_id();
            let make_op = OpRequest {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(list_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests: Vec<OpRequest> = vs
                .iter()
                .enumerate()
                .flat_map(|(index, v)| {
                    value_to_op_requests(list_id.clone(), PathElement::Index(index), v, true)
                })
                .collect();
            let mut result = vec![make_op];
            result.extend(child_requests);
            result
        }
        Value::Map(kvs, map_type) => {
            let make_action = match map_type {
                MapType::Map => ReqOpType::MakeMap,
                MapType::Table => ReqOpType::MakeTable,
            };
            let map_id = new_object_id();
            let make_op = OpRequest {
                action: make_action,
                obj: parent_object,
                key: key.to_request_key(),
                child: Some(map_id.clone()),
                value: None,
                datatype: None,
                insert,
            };
            let child_requests: Vec<OpRequest> = kvs
                .iter()
                .flat_map(|(k, v)| {
                    value_to_op_requests(map_id.clone(), PathElement::Key(k.clone()), v, false)
                })
                .collect();
            let mut result = vec![make_op];
            result.extend(child_requests);
            result
        }
        Value::Primitive(prim_value, datatype) => vec![OpRequest {
            action: ReqOpType::Set,
            obj: parent_object,
            key: key.to_request_key(),
            child: None,
            value: Some(prim_value.clone()),
            datatype: Some(*datatype),
            insert,
        }],
    }
}

fn new_object_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Represents the set of conflicting values for a register in an automerge
/// document. 
#[derive(Clone)]
pub struct Values(HashMap<OpID, Rc<RefCell<Object>>>);

impl Values {
    fn default_value(&self) -> Rc<RefCell<Object>> {
        let mut op_ids: Vec<&OpID> = self.0.keys().collect();
        op_ids.sort();
        let op_id = op_ids.first().unwrap();
        Rc::clone(self.0.get(op_id).unwrap())
    }

    fn update_for_opid(&mut self, opid: OpID, value: Rc<RefCell<Object>>) {
        self.0.insert(opid, value);
    }
}


#[derive(Clone)]
pub enum Object {
    Sequence(ObjectID, Vec<Option<Values>>, SequenceType),
    Map(ObjectID, HashMap<Key, Values>, MapType),
    Primitive(PrimitiveValue, DataType),
}

impl Object {
    fn value(&self) -> Value {
        match self {
            Object::Sequence(_, vals, seq_type) => Value::Sequence(
                vals.iter().filter_map(|v| v.clone().map(|v2| v2.default_value().borrow().value())).collect(),
                seq_type.clone(),
            ),
            Object::Map(_, vals, map_type) => Value::Map(
                vals.iter()
                    .map(|(k, v)| (k.to_string(), v.default_value().borrow().value()))
                    .collect(),
                map_type.clone(),
            ),
            Object::Primitive(v, d) => Value::Primitive(v.clone(), *d),
        }
    }
}

pub struct ChangeContext {
    original_objects: HashMap<ObjectID, Object>,
    updated: RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
}

impl ChangeContext {
    pub fn new() -> ChangeContext {
        ChangeContext {
            original_objects: HashMap::new(),
            updated: RefCell::new(HashMap::new()),
        }
    }

    /// How do we apply a diff?
    ///
    /// There are two composite data types to consider, maps and sequences,
    /// then there are the primitive types. We recieve diffs, which look like
    /// this:
    ///
    /// ```rust,ignore
    /// struct Diff {
    ///     pub edits: Option<Vec<DiffEdit>>,
    ///     pub object_id: ObjectID,
    ///     pub obj_type: ObjType,
    ///     pub props: Option<HashMap<Key, HashMap<OpID, DiffLink>>>,
    /// }
    ///
    /// pub enum DiffEdit {
    ///     Insert { index: usize },
    ///     Remove { index: usize },
    /// }
    ///
    /// pub enum DiffLink {
    ///    Link(Diff),
    ///    Val(DiffValue),
    /// }
    /// ```
    ///
    /// Diffs are nested structures, the structure of the diff mirrors the
    /// structure of the data. For example, a diff adding a key "somekey" to
    /// an existing object "parent" will look a bit like this:
    ///
    /// ```rust,ignore
    /// Diff {
    ///     edits: None,
    ///     object_id: ObjectID::ID("someid"),
    ///     obj_type: ObjType::Map,
    ///     props: hashmap!(
    ///         "parent" => hashmap!(
    ///             OpID("1@actorid") => DiffLink::Link(Diff{
    ///                 edits: None,
    ///                 object_id: ObjectID::ID(OpID::from_str("2@actor").unwrap()),
    ///                 object_type: ObjType::Map,
    ///                 props: hashmap!(
    ///                     "somekey" => hashmap!(
    ///                         OpID("2@actorid") => DiffLink::Val(DiffValue{
    ///                             value: "somevalue".to_string(),
    ///                             datatype: DataType::Undefined
    ///                         })
    ///                     )
    ///                 )
    ///             })
    ///         )
    ///     )
    /// }
    /// ```
    ///
    /// The end result that we want after having applied this diff is twofold:
    ///
    /// 1. Return the object which results from applying the diff.
    /// 2. Store any updates made on the change context, so that future diffs
    ///    can reference the changes made by this diff
    ///
    /// Notice that the `props` hashmap is a map from `Key`s to a map of
    /// `DiffLink`s. This is important for two reasons:
    ///
    /// - In the case of list ops, we will need to transform the keys into
    ///   indexes. If we receive malformed diff keys which cannot be turned
    ///   into indexes then  we panic.
    /// - The map off OpIDs to DiffLinks is important because we need to apply
    ///   every change in it. The idea is that if there are two competing
    ///   assignments to the same key or list position then we compute the
    ///   result of both of them. Then we sort the different values by their
    ///   OpID and store the first one as the resolved value, and the remainder
    ///   as "conflicts"
    ///
    /// So, these conflicts? What do they look like? We store them in the
    /// `Values` struct, which effectively stores all the possible values of
    /// a key at once, along with a method for extracting the default value.
    ///
    /// The obvious way of achieving this is to have an object store
    /// represented as a kind of tree. Then to apply a diff you lookup the
    /// object the diff applies to, apply any `edits`, then apply any `props`,
    /// then finally return the updated object.
    ///
    /// This all has to be done in a way that supports rolling back a set of
    /// changes if one of them turns out to be invalid. This is the purpose of
    /// the `original_objects` and `updated` maps. We construct a change
    /// context with a set of existing objects, but we never change those.
    /// Instead we accumulate changes by copying and objects which have been
    /// modified to the `updated` map before modifying them. Then, once the
    /// change is complete we can `commit` the change context.
    ///
    pub fn apply_diff(&mut self, diff: &Diff) -> Result<(), AutomergeFrontendError> {
        Self::apply_diff_helper(
            &self.original_objects,
            &self.updated,
            diff,
        )?;
        Ok(())
    }

    fn apply_diff_helper<'a>(
        original_objects: &'a HashMap<ObjectID, Object>,
        updated: &'a RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
        diff: &Diff
    ) -> Result<Rc<RefCell<Object>>, AutomergeFrontendError> {
        match diff.obj_type {
            ObjType::Map => {
                let obj = Self::get_or_create_object(
                    &diff.object_id,
                    original_objects,
                    updated,
                    || Object::Map(diff.object_id.clone(), HashMap::new(), MapType::Map),
                );
                if let Some(diffprops) = &diff.props {
                    match &mut*obj.borrow_mut() {
                        Object::Map(_, ref mut kvs, MapType::Map) => {
                            for (key, prop_diffs) in diffprops {
                                let values = kvs.entry(key.clone()).or_insert_with(|| Values(HashMap::new()));
                                for (opid, difflink) in prop_diffs.iter() {
                                    let object = match difflink {
                                        DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                            original_objects,
                                            updated,
                                            subpatch
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(Object::Primitive(v.value.clone(), v.datatype)))
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                };
                                if prop_diffs.len() == 0 {
                                    kvs.remove(key);
                                }
                            }
                        }
                        _ => panic!("Invalid object type when applying diff"),
                    }
                };
                Ok(obj)
            },
            ObjType::Table => {
                let obj = Self::get_or_create_object(
                    &diff.object_id,
                    original_objects,
                    updated,
                    || Object::Map(diff.object_id.clone(), HashMap::new(), MapType::Table),
                );
                if let Some(diffprops) = &diff.props {
                    match &mut*obj.borrow_mut() {
                        Object::Map(_, ref mut kvs, MapType::Table) => {
                            for (key, prop_diffs) in diffprops {
                                let values = kvs.entry(key.clone()).or_insert_with(|| Values(HashMap::new()));
                                let prop_diffs_vec: Vec<(&OpID, &DiffLink)> = prop_diffs.into_iter().collect();
                                match prop_diffs_vec[..] {
                                    [] => {kvs.remove(key);},
                                    [(opid, difflink)] => {
                                        let object = match difflink {
                                            DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                                original_objects,
                                                updated,
                                                subpatch
                                            )?,
                                            DiffLink::Val(v) => Rc::new(RefCell::new(Object::Primitive(v.value.clone(), v.datatype)))
                                        };
                                        values.update_for_opid(opid.clone(), object);
                                    },
                                    _ => return Err(AutomergeFrontendError::InvalidChangeRequest)
                                };
                            }
                        },
                        _ => panic!("Invalid object type when applying diff"),
                    };
                };
                Ok(obj)
            },
            ObjType::List => {
                let obj = Self::get_or_create_object(
                    &diff.object_id,
                    original_objects,
                    updated,
                    || Object::Sequence(diff.object_id.clone(), Vec::new(), SequenceType::List)
                );
                match &mut*obj.borrow_mut() {
                    Object::Sequence(_, ref mut elems, SequenceType::List) => {
                        if let Some(edits) = &diff.edits {
                            for edit in edits {
                                match edit {
                                    DiffEdit::Insert{index} => elems.insert(*index, None),
                                    DiffEdit::Remove{index} => {elems.remove(*index);},
                                };
                            }
                        };
                        if let Some(diffprops) = &diff.props {
                            for (key, prop_diffs) in diffprops {
                                let index = Self::key_to_index(key)?;
                                let values = match elems[index].as_mut() {
                                    Some(v) => v,
                                    None => {
                                        let to_insert = Some(Values(HashMap::new()));
                                        elems[index] = to_insert;
                                        elems[index].as_mut().unwrap()
                                    }
                                };
                                for (opid, difflink) in prop_diffs.iter() {
                                    let object = match difflink {
                                        DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                            original_objects,
                                            updated,
                                            subpatch
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(Object::Primitive(v.value.clone(), v.datatype)))
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                };
                            }
                        };
                    },
                    _ => panic!("Invalid object type when applying diff"),
                };
                Ok(obj)
            },
            ObjType::Text => {
                let obj = Self::get_or_create_object(
                    &diff.object_id,
                    original_objects,
                    updated,
                    || Object::Sequence(diff.object_id.clone(), Vec::new(), SequenceType::List)
                );
                match &mut*obj.borrow_mut() {
                    Object::Sequence(_, ref mut elems, SequenceType::Text) => {
                        if let Some(edits) = &diff.edits {
                            for edit in edits {
                                match edit {
                                    DiffEdit::Insert{index} => elems.insert(*index, None),
                                    DiffEdit::Remove{index} => {elems.remove(*index);},
                                };
                            }
                        };
                        if let Some(diffprops) = &diff.props {
                            for (key, prop_diffs) in diffprops {
                                let index = Self::key_to_index(key)?;
                                let values = match elems[index].as_mut() {
                                    Some(v) => v,
                                    None => {
                                        let to_insert = Some(Values(HashMap::new()));
                                        elems[index] = to_insert;
                                        elems[index].as_mut().unwrap()
                                    }
                                };
                                for (opid, difflink) in prop_diffs.iter() {
                                    let object = match difflink {
                                        DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                            original_objects,
                                            updated,
                                            subpatch
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(Object::Primitive(v.value.clone(), v.datatype)))
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                };
                            }
                        };
                    },
                    _ => panic!("Invalid object type when applying diff"),
                };
                Ok(obj)
            }
        }
    }

    fn key_to_index(key: &Key) -> Result<usize, AutomergeFrontendError> {
        usize::from_str(key.to_string().as_str()).map_err(|_| AutomergeFrontendError::InvalidChangeRequest)
    }


    fn get_or_create_object<'a, F>(
        object_id: &ObjectID,
        original: &'a HashMap<ObjectID, Object>,
        updated: &'a RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
        create_new: F
    ) -> Rc<RefCell<Object>> 
        where F: FnOnce() -> Object
    {
        updated
            .borrow_mut()
            .entry(object_id.clone())
            .or_insert_with(|| {
                original
                    .get(object_id)
                    .cloned()
                    .map(|o| Rc::new(RefCell::new(o)))
                    .unwrap_or_else(|| Rc::new(RefCell::new(create_new())))
                    //.unwrap_or_else(|| {
                        //Rc::new(RefCell::new(Object::Map(object_id.clone(), HashMap::new(), MapType::Map)))
                    //})
            }).clone()
    }

    pub fn value_for_object(&self, object_id: &ObjectID) -> Option<Value> {
        if let Some(updated) = self.updated.borrow().get(object_id) {
            return Some(updated.borrow().value())
        }
        self.original_objects.get(object_id).map(|o| o.value())
    }
}
