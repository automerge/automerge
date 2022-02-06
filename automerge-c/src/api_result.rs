
use automerge as am;
use automerge::{Value, ScalarValue, ObjType };
use crate::Datatype;

#[derive(Debug)]
pub(crate) enum ApiResult {
    ObjId(am::ObjId),
    Value(am::Value,am::ObjId),
}

impl ApiResult {
    pub fn datatype(&self) -> Option<Datatype> {
        match self {
            ApiResult::Value(v,_) => Some(v.into()),
            _ => None,
        }
    }
    pub fn to_bytes(self) -> Vec<u8> {
        println!("to_bytes {:?}",self);
        match self {
            ApiResult::ObjId(_id) => {
                let mut buff = "id".to_string().into_bytes();
                buff.push(0);
                buff
            },
            ApiResult::Value(Value::Scalar(ScalarValue::Str(s)),_) => {
                let mut buff = s.to_string().into_bytes();
                buff.push(0);
                buff
            },
            ApiResult::Value(Value::Object(ObjType::List),_id) => {
                let mut buff = "list".to_string().into_bytes();
                buff.push(0);
                buff
            },
            _ => unimplemented!(),
        }
    }
}

impl From<am::ObjId> for ApiResult {
    fn from(obj: am::ObjId) -> Self {
        ApiResult::ObjId(obj)
    }
}

impl From<(am::Value,am::ObjId)> for ApiResult {
    fn from(vo: (am::Value, am::ObjId)) -> Self {
        ApiResult::Value(vo.0,vo.1)
    }
}

