use automerge_backend::{
    Diff, DiffEdit, DiffLink, Key, ObjType, ObjectID, OpID
};
//use crate::AutomergeFrontendError;
use crate::object::{Object, Values};
use crate::{
    AutomergeFrontendError, MapType, PrimitiveValue, SequenceType,
    Value
};
use std::str::FromStr;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

/// A `ChangeContext` represents some kind of change which has not been applied
/// yet. This is usefule in two contexts:
///
/// 1. When applying the diffs in a patch.
/// 2. When generating changes in a mutation closure (passed to Frontend::change)
///
/// In both of these cases we have an initial set of objects and then diffs
/// which we want to apply to those objects under the constraint that if the
/// change fails we want to be able to roll everything back. This is implemented
/// by accumulating all the changes in a separate set of objects, then only
/// actually mutating the original object set (which you will note is captured
/// by mutable reference) when `commit` is called.
pub struct ChangeContext<'a> {
    original_objects: &'a mut HashMap<ObjectID, Rc<Object>>,
    updated: RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
}

impl<'a> ChangeContext<'a> {
    pub fn new(original_objects: &'a mut HashMap<ObjectID, Rc<Object>>) -> ChangeContext {
        ChangeContext {
            original_objects,
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
        Self::apply_diff_helper(&self.original_objects, &self.updated, diff)?;
        Ok(())
    }

    fn apply_diff_helper<'b>(
        original_objects: &'b HashMap<ObjectID, Rc<Object>>,
        updated: &'b RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
        diff: &Diff,
    ) -> Result<Rc<RefCell<Object>>, AutomergeFrontendError> {
        match diff.obj_type {
            ObjType::Map => {
                let obj =
                    Self::get_or_create_object(&diff.object_id, original_objects, updated, || {
                        Object::Map(diff.object_id.clone(), HashMap::new(), MapType::Map)
                    });
                if let Some(diffprops) = &diff.props {
                    match &mut *obj.borrow_mut() {
                        Object::Map(_, ref mut kvs, MapType::Map) => {
                            for (key, prop_diffs) in diffprops {
                                let values = kvs
                                    .entry(key.clone())
                                    .or_insert_with(|| Values(HashMap::new()));
                                for (opid, difflink) in prop_diffs.iter() {
                                    let object = match difflink {
                                        DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                            original_objects,
                                            updated,
                                            subpatch,
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(
                                            Object::Primitive(PrimitiveValue::from_backend_values(
                                                v.value.clone(),
                                                v.datatype,
                                            )),
                                        )),
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                }
                                if prop_diffs.len() == 0 {
                                    kvs.remove(key);
                                }
                            }
                        }
                        _ => panic!("Invalid object type when applying diff"),
                    }
                };
                Ok(obj)
            }
            ObjType::Table => {
                let obj =
                    Self::get_or_create_object(&diff.object_id, original_objects, updated, || {
                        Object::Map(diff.object_id.clone(), HashMap::new(), MapType::Table)
                    });
                if let Some(diffprops) = &diff.props {
                    match &mut *obj.borrow_mut() {
                        Object::Map(_, ref mut kvs, MapType::Table) => {
                            for (key, prop_diffs) in diffprops {
                                let values = kvs
                                    .entry(key.clone())
                                    .or_insert_with(|| Values(HashMap::new()));
                                let prop_diffs_vec: Vec<(&OpID, &DiffLink)> =
                                    prop_diffs.into_iter().collect();
                                match prop_diffs_vec[..] {
                                    [] => {
                                        kvs.remove(key);
                                    }
                                    [(opid, difflink)] => {
                                        let object = match difflink {
                                            DiffLink::Link(subpatch) => Self::apply_diff_helper(
                                                original_objects,
                                                updated,
                                                subpatch,
                                            )?,
                                            DiffLink::Val(v) => {
                                                Rc::new(RefCell::new(Object::Primitive(
                                                    PrimitiveValue::from_backend_values(
                                                        v.value.clone(),
                                                        v.datatype,
                                                    ),
                                                )))
                                            }
                                        };
                                        values.update_for_opid(opid.clone(), object);
                                    }
                                    _ => return Err(AutomergeFrontendError::InvalidChangeRequest),
                                };
                            }
                        }
                        _ => panic!("Invalid object type when applying diff"),
                    };
                };
                Ok(obj)
            }
            ObjType::List => {
                let obj =
                    Self::get_or_create_object(&diff.object_id, original_objects, updated, || {
                        Object::Sequence(diff.object_id.clone(), Vec::new(), SequenceType::List)
                    });
                match &mut *obj.borrow_mut() {
                    Object::Sequence(_, ref mut elems, SequenceType::List) => {
                        if let Some(edits) = &diff.edits {
                            for edit in edits {
                                match edit {
                                    DiffEdit::Insert { index } => elems.insert(*index, None),
                                    DiffEdit::Remove { index } => {
                                        elems.remove(*index);
                                    }
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
                                            subpatch,
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(
                                            Object::Primitive(PrimitiveValue::from_backend_values(
                                                v.value.clone(),
                                                v.datatype,
                                            )),
                                        )),
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                }
                            }
                        };
                    }
                    _ => panic!("Invalid object type when applying diff"),
                };
                Ok(obj)
            }
            ObjType::Text => {
                let obj =
                    Self::get_or_create_object(&diff.object_id, original_objects, updated, || {
                        Object::Sequence(diff.object_id.clone(), Vec::new(), SequenceType::List)
                    });
                match &mut *obj.borrow_mut() {
                    Object::Sequence(_, ref mut elems, SequenceType::Text) => {
                        if let Some(edits) = &diff.edits {
                            for edit in edits {
                                match edit {
                                    DiffEdit::Insert { index } => elems.insert(*index, None),
                                    DiffEdit::Remove { index } => {
                                        elems.remove(*index);
                                    }
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
                                            subpatch,
                                        )?,
                                        DiffLink::Val(v) => Rc::new(RefCell::new(
                                            Object::Primitive(PrimitiveValue::from_backend_values(
                                                v.value.clone(),
                                                v.datatype,
                                            )),
                                        )),
                                    };
                                    values.update_for_opid(opid.clone(), object);
                                }
                            }
                        };
                    }
                    _ => panic!("Invalid object type when applying diff"),
                };
                Ok(obj)
            }
        }
    }

    fn key_to_index(key: &Key) -> Result<usize, AutomergeFrontendError> {
        usize::from_str(key.to_string().as_str())
            .map_err(|_| AutomergeFrontendError::InvalidChangeRequest)
    }

    fn get_or_create_object<'b, F>(
        object_id: &ObjectID,
        original: &'b HashMap<ObjectID, Rc<Object>>,
        updated: &'b RefCell<HashMap<ObjectID, Rc<RefCell<Object>>>>,
        create_new: F,
    ) -> Rc<RefCell<Object>>
    where
        F: FnOnce() -> Object,
    {
        updated
            .borrow_mut()
            .entry(object_id.clone())
            .or_insert_with(|| {
                original
                    .get(object_id)
                    .map(|o| Rc::new(RefCell::new(o.as_ref().clone())))
                    .unwrap_or_else(|| Rc::new(RefCell::new(create_new())))
            })
            .clone()
    }

    pub(crate) fn value_for_object_id(&self, object_id: &ObjectID) -> Option<Rc<Object>> {
        if let Some(updated) = self.updated.borrow().get(object_id) {
            // This is irritating. Ideally we would return `impl Deref<Object>` but
            // unfortunately the type checker won't allow returning different
            // types for the same impl (in this case Rc<T> and RefCell::Ref<T>)
            // and so instead we clone and wrap.
            return Some(Rc::new(updated.borrow().clone()))
        }
        self.original_objects.get(object_id).map(|r| r.clone())
    }

    pub fn commit(self) -> Result<Value, AutomergeFrontendError> {
        for (object_id, object) in self.updated.into_inner().into_iter() {
            let cloned_object = object.borrow().clone();
            self.original_objects
                .insert(object_id.clone(), Rc::new(cloned_object));
        }
        // The root ID must be in result by this point so we can unwrap
        let state = self.original_objects.get(&ObjectID::Root).unwrap().value();
        Ok(state)
    }
}

