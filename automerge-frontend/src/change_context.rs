use automerge_backend::{
    Key, ObjectID, Operation, PrimitiveValue, OpRequest, ReqOpType
};
use uuid;
//use crate::AutomergeFrontendError;
use crate::{Value, PathElement, SequenceType};



    //fn resolve_path(&self, path: &Path) -> Option<ResolvedPath> {
        //let mut resolved_elements: Vec<ResolvedPathElement> = Vec::new();
        //let mut containing_object_id = ObjectID::Root;
        //for next_elem in path {
            //match resolved_elements.last() {
                //Some(ResolvedPathElement::MissingKey(_)) => return None,
                //Some(ResolvedPathElement::Index(ElementID::Head)) => return None,
                //_ => {}
            //}
            //match next_elem {
                //PathElement::Root => {
                    //resolved_elements.push(ResolvedPathElement::Map(ObjectID::Root))
                //}
                //PathElement::Key(key) => {
                    //resolved_elements.push(ResolvedPathElement::Key(Key(key.to_string())));
                    //let op = self
                        //.get_operations_for_object_id(&containing_object_id)
                        //.and_then(|history| match history {
                            //ObjectState::Map(MapState {
                                //operations_by_key, ..
                            //}) => Some(operations_by_key),
                            //ObjectState::List { .. } => None,
                        //})
                        //.and_then(|kvs| kvs.get(&Key(key.to_string())))
                        //.and_then(|cops| cops.active_op())
                        //.map(|o| o.operation.clone());
                    //match op {
                        //Some(Operation::Set { value, .. }) => {
                            //resolved_elements.push(ResolvedPathElement::Value(value))
                        //}
                        //Some(Operation::Link { value, .. }) => {
                            //match self.get_operations_for_object_id(&value) {
                                //None => return None,
                                //Some(ObjectState::Map { .. }) => {
                                    //resolved_elements.push(ResolvedPathElement::Map(value.clone()));
                                    //containing_object_id = value.clone()
                                //}
                                //Some(ObjectState::List(ListState { max_elem, .. })) => {
                                    //resolved_elements
                                        //.push(ResolvedPathElement::List(value.clone(), *max_elem));
                                    //containing_object_id = value.clone()
                                //}
                            //}
                        //}
                        //None => resolved_elements
                            //.push(ResolvedPathElement::MissingKey(Key(key.to_string()))),
                        //_ => return None,
                    //}
                //}
                //PathElement::Index(index) => match index {
                    //ListIndex::Head => {
                        //match self.get_operations_for_object_id(&containing_object_id) {
                            //Some(ObjectState::List { .. }) => {
                                //resolved_elements.push(ResolvedPathElement::Index(ElementID::Head))
                            //}
                            //_ => return None,
                        //};
                    //}
                    //ListIndex::Index(i) => {
                        //let op = self
                            //.get_operations_for_object_id(&containing_object_id)
                            //.and_then(|history| match history {
                                //ObjectState::List(ListState {
                                    //operations_by_elemid,
                                    //following,
                                    //..
                                //}) => list_ops_in_order(operations_by_elemid, following).ok(),
                                //ObjectState::Map { .. } => None,
                            //})
                            //.and_then(|ops| ops.get(*i).cloned())
                            //.and_then(|(element_id, cops)| {
                                //cops.active_op().map(|o| (element_id, o.operation.clone()))
                            //});
                        //match op {
                            //Some((elem_id, Operation::Set { value, .. })) => {
                                //resolved_elements.push(ResolvedPathElement::Index(elem_id));
                                //resolved_elements.push(ResolvedPathElement::Value(value));
                            //}
                            //Some((_, Operation::Link { value, .. })) => {
                                //match self.get_operations_for_object_id(&value) {
                                    //None => return None,
                                    //Some(ObjectState::Map { .. }) => {
                                        //resolved_elements
                                            //.push(ResolvedPathElement::Map(value.clone()));
                                        //containing_object_id = value
                                    //}
                                    //Some(ObjectState::List(ListState { max_elem, .. })) => {
                                        //resolved_elements.push(ResolvedPathElement::List(
                                            //value.clone(),
                                            //*max_elem,
                                        //));
                                        //containing_object_id = value
                                    //}
                                //}
                            //}
                            //_ => return None,
                        //}
                    //}
                //},
            //}
        //}
        //Some(ResolvedPath::new(resolved_elements))
    //}
//}

fn value_to_op_requests(parent_object: ObjectID, key: PathElement, v: &Value, insert: bool) -> Vec<OpRequest> {
    match v {
        Value::Sequence(vs, seq_type) => {
            let make_action = match seq_type {
                SequenceType::List => ReqOpType::MakeList,
                SequenceType::Text => ReqOpType::MakeText,
            };
            let list_id = ObjectID::from(new_object_id());
            let make_op = OpRequest{
                action: make_action,
                obj: String::from(&parent_object),
                key: key.to_request_key(),
                child: Some((&list_id).into()),
                value: None,
                datatype: None,
                insert: false,
            };
            let child_requests = vs.iter().enumerate().map(|index, v| {
                value_to_op_requests(
            }).collect();
            Vec::new()
        }
        Value::Map(kvs, map_type) => {
        }
        _ => panic!("Only a map or list can be the top level object in value_to_op_requests".to_string()),
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

fn new_object_id() -> ObjectID {
    ObjectID::Str(uuid::Uuid::new_v4().to_string())
}
